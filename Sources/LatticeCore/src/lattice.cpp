#include "recovery_producer_continuity.hpp"
// lattice.cpp - Implementation moved to header (templates)
// This file kept for potential non-template implementations

#include "lattice/lattice.hpp"
#include "lattice/ipc.hpp"
#include <cstdlib>
#include <set>
#include <limits>
#include <exception>
#include <unordered_set>
#include <cstring>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

namespace lattice {

namespace {
// Maintenance already refuses contended topology. Do the same for its old
// writer rather than wait under topology behind a legacy SQLite callback.
struct managed_writer_publication_hold {
    sqlite3_mutex* mutex;
    managed_writer_publication_hold(sqlite3* connection, const std::atomic<size_t>& active)
        : mutex(connection ? sqlite3_db_mutex(connection) : nullptr) {
#ifndef __EMSCRIPTEN__
        if (connection && !mutex) throw db_error("writer maintenance requires a serialized connection");
#endif
        if (sqlite3_mutex_try(mutex) != SQLITE_OK)
            throw db_error("writer maintenance refused while writer is busy");
        if (active.load(std::memory_order_acquire) != 0) {
            sqlite3_mutex_leave(mutex);
            throw db_error("writer maintenance refused during managed scalar delivery");
        }
    }
    ~managed_writer_publication_hold() { sqlite3_mutex_leave(mutex); }
    managed_writer_publication_hold(const managed_writer_publication_hold&) = delete;
    managed_writer_publication_hold& operator=(const managed_writer_publication_hold&) = delete;
};
}

thread_local detail::managed_route_scope* detail::managed_route_scope::current_ = nullptr;

bool detail::managed_route_scope::active_for(const database* db) noexcept {
    for (auto* frame = current_; frame; frame = frame->previous_)
        if (frame->db_ == db) return true;
    return false;
}

bool detail::managed_route_scope::active_for(const lattice_db* owner) noexcept {
    for (auto* frame = current_; frame; frame = frame->previous_)
        if (frame->owner_ == owner) return true;
    return false;
}

detail::managed_route_scope::managed_route_scope(database* db, lattice_db* owner,
    const std::string& table, int64_t token, const std::weak_ptr<database>& captured_writer, bool vector_write) {
    // Explicitly assigned ownerless fields retain their raw database contract.
    if (!db || !owner) return;
    if (vector_write && active_for(db))
        throw db_error("managed field: reentrant vector mutation during scalar access");
    // Keep the common main-model getter free of parsing, atomics and locks.
    if (token == 0 && table.find('.') == std::string::npos &&
        (table.empty() || table.front() != '"')) return;
    const auto route = managed_route(table);
    if (token == 0 && (route.schema_sql == "main" || route.schema_sql == "\"main\"")) return;
    if (token <= 0) throw db_error("managed field: missing attachment generation");

    // Lock the captured weak control block, not a current writer found by
    // raw address. Expired old handles cannot revive after allocator reuse.
    writer_owner_ = captured_writer.lock();
    if (!writer_owner_ || writer_owner_.get() != db)
        throw db_error("managed field: detached or retired attachment writer");

    // Memory vec0 maintenance owns its existing store gate before SQLite.
    // A SQLite callback already inside a scalar scope must not wait for it.
#ifdef __EMSCRIPTEN__
    const bool vector_gate_needed = vector_write;
#else
    const bool vector_gate_needed = vector_write && owner->config_.is_in_memory();
#endif
    if (vector_gate_needed) {
        if (active_for(db))
            throw db_error("managed field: reentrant vector mutation during scalar access");
        auto& gate = owner->store_write_gate_ ? *owner->store_write_gate_ : owner->vec0_memory_maintenance_gate_;
        vector_gate_ = std::unique_lock<std::recursive_timed_mutex>(gate);
    }
    auto* mutex = sqlite3_db_mutex(db->internal_handle());
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("managed field: attachment writer is not serialized");
#endif
    sqlite3_mutex_enter(mutex);
    try {
        if (owner->closed_.load() || db->is_closed())
            throw db_error("managed field: attachment writer is closed");
        auto view = std::atomic_load(&owner->managed_attachment_view_);
        const lattice_db::managed_attachment_binding* current = nullptr;
        if (view) for (const auto& binding : *view) {
            if (binding->token == token && !binding->writer.owner_before(captured_writer) &&
                !captured_writer.owner_before(binding->writer) &&
                binding->valid.load(std::memory_order_acquire)) {
                current = binding.get();
                break;
            }
        }
        if (!current || (route.schema_sql != managed_quote_identifier(current->alias) &&
                         route.schema_sql != current->alias))
            throw db_error("managed field: detached or stale attachment route");
        // Like checked bulk mutation, detect an unbookkept different-file
        // binding. No PRAGMA/authorizer action is added to a scalar read.
        // Raw same-file detach/reattach cannot establish a new Core token.
        const char* filename = sqlite3_db_filename(db->internal_handle(), current->alias.c_str());
        if (!filename) throw db_error("managed field: attachment is missing");
        std::string actual(filename);
        if (actual != current->filename) {
            std::error_code error;
            const auto canonical = actual.empty() ? std::filesystem::path{} :
                std::filesystem::weakly_canonical(actual, error);
            if (error || canonical.string() != current->filename)
                throw db_error("managed field: attachment physical database changed");
        }
        db_ = db;
        owner_ = owner;
        mutex_ = mutex;
        exceptions_ = std::uncaught_exceptions();
        previous_ = current_;
        owner_->active_managed_attachment_operations_.fetch_add(1, std::memory_order_acq_rel);
        current_ = this;
    } catch (...) {
        sqlite3_mutex_leave(mutex);
        throw;
    }
}

detail::managed_route_scope::~managed_route_scope() noexcept(false) {
    if (!db_) return;
    struct complete_operation {
        std::atomic<size_t>& active;
        ~complete_operation() { active.fetch_sub(1, std::memory_order_release); }
    } complete{owner_->active_managed_attachment_operations_};
    current_ = previous_;
    sqlite3_mutex_leave(mutex_);
    if (vector_gate_.owns_lock()) vector_gate_.unlock();
    // Nested scopes leave delivery to their outer successful scope. A failed
    // operation retains the existing query/update exception-tail behavior.
    if (std::uncaught_exceptions() == exceptions_) db_->drain_if_settled();
}

void lattice_db::publish_managed_attachment(const std::string& alias, const std::string& path,
    int64_t token, const std::shared_ptr<database>& writer) {
    auto next = std::make_shared<managed_attachment_view>();
    if (auto old = std::atomic_load(&managed_attachment_view_)) {
        for (const auto& binding : *old)
            if (binding->valid.load(std::memory_order_acquire)) next->push_back(binding);
    }
    std::string filename;
    if (!configuration::path_is_memory(path)) {
        std::error_code error;
        filename = std::filesystem::weakly_canonical(path, error).string();
        if (error) throw db_error("cannot capture managed attachment filename");
    }
    next->push_back(std::make_shared<managed_attachment_binding>(alias, filename, token, writer));
    std::atomic_store(&managed_attachment_view_, std::shared_ptr<const managed_attachment_view>(std::move(next)));
}

std::shared_ptr<const lattice_db::managed_attachment_view> lattice_db::managed_view_for_writer(
    const std::shared_ptr<database>& writer) const {
    auto next = std::make_shared<managed_attachment_view>();
    // Rebuild from authoritative topology, including close_write followed by
    // reopen, when the retired writer's published view has already been cleared.
    for (const auto& [alias, path] : attached_dbs_) {
        auto token = attached_route_tokens_.find(alias);
        if (token == attached_route_tokens_.end()) continue;
        std::string filename;
        if (!configuration::path_is_memory(path)) {
            std::error_code error;
            filename = std::filesystem::weakly_canonical(path, error).string();
            if (error) throw db_error("cannot restore managed attachment filename");
        }
        next->push_back(std::make_shared<managed_attachment_binding>(alias, filename, token->second, writer));
    }
    return next;
}

// Single definition of the global log level (declared extern in log.hpp).
// Seed from the LATTICE_LOG_LEVEL env var (0=off..4=debug) so logging can be
// enabled for a test/run without code changes, e.g. `LATTICE_LOG_LEVEL=4 swift test`.
static log_level initial_log_level() {
    if (const char* s = std::getenv("LATTICE_LOG_LEVEL")) {
        int v = std::atoi(s);
        if (v >= 0 && v <= static_cast<int>(log_level::debug)) {
            return static_cast<log_level>(v);
        }
    }
    return log_level::off;
}
std::atomic<log_level> g_log_level{initial_log_level()};

// Singleton instance - defined here to ensure single copy across all translation units
instance_registry& instance_registry::instance() {
    // Intentionally leaked: prevents use-after-destroy when GCD callbacks
    // fire during process teardown (after atexit handlers run).
    static instance_registry* reg = new instance_registry();
    return *reg;
}

cross_process_notifier* instance_registry::get_or_create_notifier(const std::string& path) {
    // No cross-PROCESS notifier for any memory path (incl. named shared-cache
    // URIs): memory DBs are process-local; same-process cross-instance
    // delivery rides the instance registry (keyed on the canonical URI).
    if (configuration::path_is_memory(path)) return nullptr;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = shared_notifiers_.find(path);
    if (it != shared_notifiers_.end()) {
        return it->second.get();
    }

    auto notifier = make_cross_process_notifier(path);
    if (!notifier) return nullptr;

    notifier->start_listening([path] {
        int notified = 0;
        instance_registry::instance().for_each_alive(path,
            [&notified](lattice_db* inst) {
                inst->handle_cross_process_notification();
                ++notified;
            });
        LOG_DEBUG("xproc", "Notified %d alive instances for path", notified);
    });

    auto* raw = notifier.get();
    shared_notifiers_[path] = std::move(notifier);
    return raw;
}

void lattice_db::projection_pressure_map_deleter::operator()(const projection_pressure_map* value) const noexcept {
    delete value;
}

void lattice_db::publish_projection_pressure(std::unique_ptr<const projection_pressure_map> next) {
    // Caller serializes publishers with attach_mutex_ (or is constructing the
    // instance before hooks exist). No SQLite/registry/service lock is held.
    const unsigned old = projection_pressure_slot_.load();
    const unsigned fresh = 1 - old;
    projection_pressure_owners_[fresh].reset(next.release());
    projection_pressure_maps_[fresh].store(projection_pressure_owners_[fresh].get());
    projection_pressure_slot_.store(fresh);
    // New readers use fresh. A stale pre-increment reader rechecks the slot
    // before dereferencing; validated old readers keep this count nonzero.
    while (projection_pressure_readers_[old].load() != 0) std::this_thread::yield();
    projection_pressure_maps_[old].store(nullptr);
    projection_pressure_owners_[old].reset();
}
void lattice_db::replace_projection_pressure_source(const std::string& schema,
                                                   std::shared_ptr<projection_pressure_source> source) {
    const auto* current = projection_pressure_maps_[projection_pressure_slot_.load()].load();
    auto next = current ? std::make_unique<projection_pressure_map>(*current) : std::make_unique<projection_pressure_map>();
    auto prior = next->find(schema);
    auto retired = prior == next->end() ? std::shared_ptr<projection_pressure_source>{} : prior->second;
    if (source) { source->active.store(true); (*next)[schema] = std::move(source); }
    else next->erase(schema);
    publish_projection_pressure(std::move(next));
    if (retired) retired->active.store(false);
}
void lattice_db::raise_projection_pressure(const char* schema) noexcept {
    for (;;) {
        const unsigned slot = projection_pressure_slot_.load();
        projection_pressure_readers_[slot].fetch_add(1);
        if (slot != projection_pressure_slot_.load()) {
            projection_pressure_readers_[slot].fetch_sub(1); continue;
        }
        const auto* map = projection_pressure_maps_[slot].load();
        if (map) {
            const auto found = map->find(schema ? schema : "main");
            if (found != map->end()) found->second->raise();
        }
        projection_pressure_readers_[slot].fetch_sub(1);
        return;
    }
}
std::vector<std::shared_ptr<projection_pressure_source>> lattice_db::projection_pressure_sources() const {
    for (;;) {
        const unsigned slot = projection_pressure_slot_.load();
        projection_pressure_readers_[slot].fetch_add(1);
        if (slot != projection_pressure_slot_.load()) {
            projection_pressure_readers_[slot].fetch_sub(1); continue;
        }
        struct release { std::atomic<uint64_t>& readers; ~release() { readers.fetch_sub(1); } } release{projection_pressure_readers_[slot]};
        std::vector<std::shared_ptr<projection_pressure_source>> sources;
        const auto* map = projection_pressure_maps_[slot].load();
        if (map) for (const auto& [_, source] : *map) sources.push_back(source);
        return sources;
    }
}
void lattice_db::setup_projection_pressure() {
    if (config_.is_in_memory()) return;
    // Best effort: legacy opens keep working on unsupported VFS/filesystems.
    // Projected reads later require a validated identity before admission.
    try {
        auto source = make_projection_pressure_source(db_->physical_identity());
        if (source) {
            std::lock_guard<std::mutex> lock(attach_mutex_);
            if (wal_eviction_pending_.load()) source->raise();
            replace_projection_pressure_source("main", std::move(source));
        }
    } catch (...) {}
}
void lattice_db::deactivate_projection_pressure() {
    std::lock_guard<std::mutex> lock(attach_mutex_);
    // Publisher serialization keeps this map alive; destruction/close needs
    // neither allocation nor a reader grace section to deactivate its atoms.
    const auto* current = projection_pressure_maps_[projection_pressure_slot_.load()].load();
    if (current) for (const auto& [_, source] : *current) source->active.store(false);
}

std::shared_ptr<database> lattice_db::borrow_read_connection() {
    std::shared_ptr<database> writer, reader;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
        reader = read_db_;
    }
    // No SQLite call under the publication mutex. A retired fallback writer
    // stays owned through this test and the caller's entire query.
    if (reader && writer &&
        txn_owner_thread_.load(std::memory_order_acquire) == std::this_thread::get_id() &&
        writer->is_in_transaction()) {
        return writer;
    }
    if (reader) return reader;
    if (writer) return writer;
    throw db_error("read connection unavailable during maintenance");
}

std::shared_ptr<database> lattice_db::borrow_xproc_read_connection() {
    std::shared_ptr<database> writer, reader, xproc;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
        reader = read_db_;
        xproc = xproc_read_db_;
    }
    if (xproc) return xproc;
    if (reader && writer &&
        txn_owner_thread_.load(std::memory_order_acquire) == std::this_thread::get_id() &&
        writer->is_in_transaction()) {
        return writer;
    }
    if (reader) return reader;
    if (writer) return writer;
    throw db_error("xproc read connection unavailable during maintenance");
}

std::vector<database::row_t> lattice_db::query_read(
    const std::string& sql, const std::vector<column_value_t>& params) {
    auto connection = borrow_read_connection();
    return connection->query(sql, params);
}

std::vector<database::row_t> lattice_db::query_xproc(
    const std::string& sql, const std::vector<column_value_t>& params) {
    auto connection = borrow_xproc_read_connection();
    return connection->query(sql, params);
}

void lattice_db::close_read_db() {
    std::shared_ptr<database> reader, xproc;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        ++connection_revision_;
        reader.swap(read_db_);
        xproc.swap(xproc_read_db_);
    }
    // In particular, do not wait for an xproc borrower: its next statement can
    // need the writer mutex already held by the maintenance caller.
}

void lattice_db::close_write_db() {
    if (active_recovery_install_operations_.load(std::memory_order_acquire) != 0)
        throw db_error("writer close refused during owned recovery install");
    if (detail::managed_route_scope::active_for(this) ||
        active_managed_attachment_operations_.load(std::memory_order_acquire) != 0)
        throw db_error("writer close refused during managed scalar access or delivery");
    // Preserve broad projection cancellation/grace before retiring the writer.
    pause_projection_reads();
    retire_all_read_generations();
    deactivate_projection_pressure();
    std::shared_ptr<database> writer;
    uint64_t revision;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
        revision = connection_revision_;
    }
    {
        std::unique_lock<std::mutex> attach_lock(attach_mutex_, std::try_to_lock);
        if (!attach_lock.owns_lock())
            throw db_error("writer close refused while attachment topology is busy");
        managed_writer_publication_hold publication(
            writer ? writer->internal_handle() : nullptr, active_managed_attachment_operations_);
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (revision != connection_revision_ || db_ != writer)
            throw db_error("writer close invalidated by concurrent maintenance");
        ++connection_revision_;
        db_.reset();
        std::atomic_store(&managed_attachment_view_, std::shared_ptr<const managed_attachment_view>{});
    }
    wal_eviction_pending_.store(false);
    // No publication/attachment/writer lock survives the retired owner's release.
}

std::vector<std::shared_ptr<database>> lattice_db::view_handles() {
    std::vector<std::shared_ptr<database>> handles;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (db_) handles.push_back(db_);
        if (read_db_) handles.push_back(read_db_);
    }
    return handles;
}

void lattice_db::restore_attached_views(database& connection) {
    // Caller holds attachment admission; this connection is not published.
    if (!attachment_topology_valid_)
        throw db_error("cannot reopen an incomplete attachment topology");
    for (const auto& [alias, path] : attached_dbs_) {
        std::string quoted = "\"";
        for (char c : alias) { quoted += c; if (c == '"') quoted += '"'; }
        quoted += '"';
        connection.execute("ATTACH DATABASE ? AS " + quoted, {path});
        const auto expected = attached_projection_identities_.find(alias);
        if (expected != attached_projection_identities_.end() && expected->second) {
            auto actual = connection.physical_identity(alias, {}, true);
            if (!actual || !(*actual == *expected->second))
                throw db_error("attached physical file changed during connection maintenance");
        }
    }
    for (const auto& [_, sql] : attached_view_sql_) connection.execute(sql);
}

void lattice_db::reopen_write_db() {
    if(recovery_continuous_)throw db_error("continuous writer replacement requires a new admitted facade");
    if (active_recovery_install_operations_.load(std::memory_order_acquire) != 0)
        throw db_error("writer reopen refused during owned recovery install");
    if (detail::managed_route_scope::active_for(this) ||
        active_managed_attachment_operations_.load(std::memory_order_acquire) != 0)
        throw db_error("writer reopen refused during managed scalar access or delivery");
    pause_projection_reads();
    retire_all_read_generations();
    uint64_t revision;
    std::shared_ptr<database> previous_writer;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst))
            throw db_error("cannot reopen a closed lattice");
        revision = connection_revision_;
        previous_writer = db_;
    }
    auto staged = std::make_shared<database>(config_.path,
        database::open_mode::read_write, config_.busy_timeout_ms);
    register_sql_functions(*staged);
    {
        // Never wait behind topology while a caller may own the writer mutex.
        std::unique_lock<std::mutex> attach_lock(attach_mutex_, std::try_to_lock);
        if (!attach_lock.owns_lock())
            throw db_error("connection reopen refused while attachment topology is busy");
        const auto* current = projection_pressure_maps_[projection_pressure_slot_.load()].load();
        if (current) for (const auto& [_, source] : *current) source->active.store(false);
        restore_attached_views(*staged);
        detail::prepare_recovery_local_producer(*this, staged);
        // This overload installs only hooks; pressure setup below runs after
        // publication and outside attachment admission, as on the broad base.
        setup_change_hook(*staged);
        auto managed_view = managed_view_for_writer(staged);
        managed_writer_publication_hold publication(
            previous_writer ? previous_writer->internal_handle() : nullptr,
            active_managed_attachment_operations_);
        {
            std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
            if (closed_.load(std::memory_order_seq_cst) || revision != connection_revision_ ||
                db_ != previous_writer)
                throw db_error("write reopen invalidated by concurrent maintenance");
            ++connection_revision_;
            if (previous_writer) {
                if (auto allowed = std::atomic_load(&previous_writer->local_producer_write_allowed_))
                    allowed->store(false, std::memory_order_release);
            }
            staged.swap(db_);
            detail::publish_recovery_local_producer(*this, *db_);
            std::atomic_store(&managed_attachment_view_, std::move(managed_view));
        }
        wal_eviction_pending_.store(false);
    }
    staged.reset();
    previous_writer.reset(); // release the retired writer off all acquired locks
    setup_projection_pressure();
    {
        std::lock_guard<std::mutex> lock(attach_mutex_);
        const auto* current = projection_pressure_maps_[projection_pressure_slot_.load()].load();
        if (current) for (const auto& [schema, source] : *current) {
            if (schema != "main") {
                source->acknowledge(source->raised.load());
                source->active.store(true);
            }
        }
    }
    // Any failed reopen keeps admission paused; success creates a fresh service.
    resume_projection_reads();
}

void lattice_db::reopen_read_db() {
    if (config_.is_in_memory()) return;
    if (config_.read_only) { request_recovery_refresh(); return; }
    uint64_t revision;
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst))
            throw db_error("cannot reopen a closed lattice");
        revision = connection_revision_;
    }
    // No half-pair publication if either open or topology restoration fails.
    auto reader = std::make_shared<database>(config_.path,
        database::open_mode::read_only, config_.busy_timeout_ms);
    auto xproc = std::make_shared<database>(config_.path,
        database::open_mode::read_only, config_.busy_timeout_ms);
    // A maintenance caller can already own the writer SQLite mutex. Never
    // wait behind attach while it may be waiting for that writer. A busy
    // topology is an explicit failed reopen, with prior publication intact.
    std::unique_lock<std::mutex> attach_lock(attach_mutex_, std::try_to_lock);
    if (!attach_lock.owns_lock())
        throw db_error("connection reopen refused while attachment topology is busy");
    restore_attached_views(*reader);
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        if (closed_.load(std::memory_order_seq_cst) || revision != connection_revision_)
            throw db_error("read reopen invalidated by concurrent maintenance");
        ++connection_revision_;
        reader.swap(read_db_);
        xproc.swap(xproc_read_db_);
    }
    attach_lock.unlock();
    // Keep the prior acknowledged witness. Reopen must catch a missed install,
    // never seed its local cursor to the latest generation without delivery.
    request_recovery_refresh();
}

void lattice_db::setup_change_hook(database& connection) {
    LOG_DEBUG("setup_change_hook", "Setting up hooks for path: %s", config_.path.c_str());

    // Cache the page size for the WAL-threshold eviction check (results spec
    // §3.4): the WAL hook receives the log's FRAME count and may not run SQL
    // from its C frame, so bytes-per-frame must be known up front.
    try {
        auto rows = connection.query("PRAGMA page_size");
        if (!rows.empty()) {
            auto it = rows[0].find("page_size");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                auto ps = std::get<int64_t>(it->second);
                if (ps > 0) wal_page_size_ = ps;
            }
        }
    } catch (const db_error&) {
        // keep the 4096 default
    }

    // Update hook - buffers changes (called for each row change)
    // Installation/replacement follows the existing writer ownership rules.
    // Retain the registered address through moves; destructor uninstalls the
    // hook before releasing this connection-owned context.
    if (!connection.lattice_update_hook_context_)
        connection.lattice_update_hook_context_ = std::make_unique<database::lattice_update_hook_context>();
    auto& hook_context = *connection.lattice_update_hook_context_;
    hook_context.owner = this;
    hook_context.connection = connection.internal_handle();
    sqlite3_update_hook(connection.internal_handle(),
        [](void* user_data, int operation, const char* db_name, const char* table_name, sqlite3_int64 rowid) {
            auto* context = static_cast<database::lattice_update_hook_context*>(user_data);
            auto* self = context->owner;
            database::update_hook_scope callback_scope(context->connection);

            std::string op;
            switch (operation) {
                case SQLITE_INSERT: op = "INSERT"; break;
                case SQLITE_UPDATE: op = "UPDATE"; break;
                case SQLITE_DELETE: op = "DELETE"; break;
                default: return;
            }

            LOG_DEBUG("update_hook", "table=%s op=%s rowid=%lld", table_name, op.c_str(), (long long)rowid);

            // Handle internal tables specially
            std::string table(table_name);
            if (table == "_SyncControl" || table == "_lattice_sync_set" || table == "_lattice_sync_state") {
                LOG_DEBUG("update_hook", "Skipping internal table %s", table_name);
                return;
            }

            // For AuditLog changes:
            // - In-memory DBs: buffer + mark dirty (WAL hook won't fire; the
            //   post-statement drain flushes at transaction close)
            // - Emscripten: same (uses DELETE journal mode, no WAL)
            // - File DBs: skip (let flush_changes handle it via WAL hook to avoid double notification)
            if (table == "AuditLog") {
#ifdef __EMSCRIPTEN__
                // Emscripten uses DELETE journal mode — WAL hook never fires,
                // so always use the deferred-drain path.
                constexpr bool use_deferred_drain = true;
#else
                const bool use_deferred_drain = self->config_.is_in_memory();
#endif
                if (use_deferred_drain || context->recovery_delivery_deferred) {
                    // Private recovery batches capture actual AuditLog INSERTs
                    // on every storage kind. Deriving them later from a model
                    // row would mistake an old audit for a suppressed write.
                    // Deferred delivery (docs/design-deferred-memory-delivery.md):
                    // buffer the AuditLog INSERT like any other row and let the
                    // post-statement drain deliver it once the transaction
                    // settles. globalId resolution moves to flush time — the
                    // row is committed and readable there, which also removes
                    // a same-connection SELECT from inside this hook.
                    if (operation == SQLITE_INSERT) {
                        LOG_DEBUG("update_hook", "AuditLog change (in-memory), buffering for txn-settled drain");
                        self->append_to_change_buffer("AuditLog", "INSERT",
                                                      static_cast<int64_t>(rowid), "", db_name && std::strcmp(db_name, "main") == 0);
                        self->db_->mark_txn_dirty();
                    }
                } else {
                    LOG_DEBUG("update_hook", "AuditLog change (file DB), skipping - WAL hook will handle");
                    // Advance the cross-process cursor on ALL instances sharing
                    // this path INSIDE the transaction, before the WAL commit
                    // makes this entry visible to readers. This prevents
                    // handle_cross_process_notification (on the inotify/GCD
                    // thread) from seeing local entries and firing duplicate
                    // observer callbacks — both on this instance and on other
                    // same-process instances sharing the same database file.
                    if (operation == SQLITE_INSERT) {
                        if (context->entry_cursor_active && db_name &&
                            std::strcmp(db_name, "main") == 0) {
                            // A rolled-back entry may free this ID for an
                            // external writer. Publish only after RELEASE.
                            context->entry_cursor_last = static_cast<int64_t>(rowid);
                            context->entry_cursor_present = true;
                        } else {
                            self->publish_local_audit_id_(static_cast<int64_t>(rowid));
                        }
                    }
                }
                return;
            }

            // Skip SQLite internal tables (triggered during VACUUM, etc.)
            if (table.rfind("sqlite_", 0) == 0) {
                return;
            }

            // Link tables start with underscore — they don't have a globalId column
            // but we still buffer them so flush_changes() can find their AuditLog
            // entries and notify the synchronizer.
            bool is_link_table = !table.empty() && table[0] == '_';

            // Get the globalId for this row (only for model tables)
            std::string global_id;
            if (!is_link_table && operation != SQLITE_DELETE) {
                // The update hook identifies a physical schema. A logical
                // attached-union view can contain the same local id from
                // another store, so resolve only the row that SQLite changed.
                const auto quote_identifier = [](const std::string& name) {
                    std::string quoted = "\"";
                    for (const char c : name) { quoted += c; if (c == '\"') quoted += '\"'; }
                    return quoted + '\"';
                };
                const std::string schema = db_name ? db_name : "main";
                const std::string sql = "SELECT globalId FROM " + quote_identifier(schema) +
                    "." + quote_identifier(table) + " WHERE id = ?";
                auto rows = self->db_->query(sql, {static_cast<int64_t>(rowid)});
                if (!rows.empty()) {
                    auto it = rows[0].find("globalId");
                    if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                        global_id = std::get<std::string>(it->second);
                    }
                }
            }

            // Buffer the change instead of notifying immediately
            LOG_DEBUG("update_hook", "Buffering change: table=%s op=%s rowid=%lld globalId=%s",
                   table.c_str(), op.c_str(), (long long)rowid, global_id.c_str());
            self->append_to_change_buffer(table, op, static_cast<int64_t>(rowid), global_id,
                db_name && std::strcmp(db_name, "main") == 0);

            // For in-memory databases the WAL hook won't fire, so mark the
            // connection dirty; the post-statement drain in db.cpp flushes at
            // transaction close — never from inside this hook, which would
            // deliver mid-transaction on SQLite's C frames
            // (docs/design-deferred-memory-delivery.md). Emscripten is the
            // same: DELETE journal mode, no WAL hook.
#ifdef __EMSCRIPTEN__
            LOG_DEBUG("update_hook", "Emscripten: marking txn dirty for post-statement drain");
            self->db_->mark_txn_dirty();
#else
            if (self->config_.is_in_memory()) {
                LOG_DEBUG("update_hook", "In-memory DB, marking txn dirty for post-statement drain");
                self->db_->mark_txn_dirty();
            } else {
                LOG_DEBUG("update_hook", "File DB (path=%s), waiting for WAL hook", self->config_.path.c_str());
            }
#endif
        },
        connection.lattice_update_hook_context_.get()
    );

    // Revoke continuation admission before COMMIT can open a successor. This
    // hook runs for every actual write transaction, including BEGIN IMMEDIATE
    // with no changed pages. It deliberately does not mark a commit successful:
    // SQLite can still fail after this point. WAL/rollback retain that role.
    sqlite3_commit_hook(connection.internal_handle(),
        [](void* user_data) -> int {
            auto* context = static_cast<database::lattice_update_hook_context*>(user_data);
            if (auto* settlement = context->sync_chunk) {
                settlement->commit_attempted = true;
                if (settlement->policy == database::sync_apply_chunk_state::commit_policy::owner_body) {
                    // Enforce the private body's existing no-settlement
                    // contract. SQLite invokes rollback after this veto.
                    // No SQL, allocation, or application callback here.
                    settlement->premature_commit = true;
                    return 1;
                }
            }
            return 0;
        },
        connection.lattice_update_hook_context_.get());

    // WAL hook - flushes buffered changes on transaction commit (file-based DBs only)
    sqlite3_wal_hook(connection.internal_handle(),
        [](void* user_data, sqlite3* connection, const char* schema, int nframes) -> int {
            auto* context = static_cast<database::lattice_update_hook_context*>(user_data);
            auto* self = context->owner;
#if defined(LATTICE_SYNC_COMMIT_PROBE)
            sync_commit_probe_detail::record(self, connection, schema);
#endif
            bool unexpected_owned_commit = false;
            if (context->connection == connection && schema && std::strcmp(schema, "main") == 0) {
                auto* settlement = context->sync_chunk;
                unexpected_owned_commit = settlement && settlement->policy ==
                    database::sync_apply_chunk_state::commit_policy::owner_body;
                context->note_settled(true);
                // An unsupported replaced commit hook may have bypassed the
                // veto. Record only the physical outcome; never publish an
                // unvalidated private batch or retain its reservation across
                // a later successor. No general hook-replacement guarantee.
                if (unexpected_owned_commit && context->consume_recovery_reservation(settlement)) {
                    std::lock_guard<std::mutex> lock(self->change_buffer_mutex_);
                    self->change_buffer_.clear();
                    self->recovery_change_buffer_reserved_ = false;
                }
            }

            // WAL-threshold keeper eviction (results spec §3.4): nframes is
            // the log's total frame count after this commit. Crossing the
            // threshold sets the per-instance eviction flag — a single
            // atomic store, legal in this C hook frame. Readers/maintenance
            // aggregate the flag per path and open the coordinated reader
            // gap (retire ALL same-path keepers, TRUNCATE-else-PASSIVE,
            // re-pin) off this thread.
            const int64_t threshold = self->wal_keeper_eviction_threshold_bytes_
                                          .load(std::memory_order_relaxed);
            if (threshold > 0 &&
                static_cast<int64_t>(nframes) * self->wal_page_size_ > threshold) {
                self->raise_projection_pressure(schema);
                self->wal_eviction_pending_.store(true, std::memory_order_seq_cst);
            }

            // The private recovery call owns this commit's detached tail.
            // Generic dirty-state drain must never consume that batch.
            if (context->recovery_delivery_deferred || unexpected_owned_commit) return SQLITE_OK;

            const bool delivered = self->flush_changes();
            if (!delivered) {
                // §2.3 epoch semantics: EVERY settled commit signals,
                // observed tables or not — each commit grows the WAL that a
                // keeper pins, including commits whose change buffer was
                // empty (bookkeeping tables such as _lattice_sync_state,
                // mark-synced AuditLog UPDATEs). The empty payload only
                // means "no shape caches to drop"; the epoch bump — and the
                // §3.4 advance the flag above requests — still delivers.
                self->fire_invalidation_hooks({}, invalidation_reason::commit);
            }
            return SQLITE_OK;
        },
        connection.lattice_update_hook_context_.get()
    );

    // Transaction-settled drain + rollback discard
    // (docs/design-deferred-memory-delivery.md). The settled hook drains the
    // change buffer after any successful statement that leaves the connection
    // in autocommit mode — the memory/Emscripten replacement for the WAL
    // hook. Harmless for file DBs: only the memory path ever sets the dirty
    // flag. The rollback hook applies to ALL storage kinds — a rolled-back
    // transaction must discard its buffered rows, or the next flush delivers
    // them as phantoms.
    //
    // The rollback path ALSO signals the invalidation hooks (results spec
    // §2.3): a rolled-back transaction delivers no change batch by design,
    // so without this signal a memory-family capture that raced the
    // transaction could serve a poisoned id vector forever (§4.1) — the
    // rollback bump guarantees the next access re-captures. Runs inside
    // sqlite3_rollback_hook's C frame: hook bodies are atomics-only by
    // contract, and every lock on this path (change_buffer_mutex_, the
    // hook-list and registry mutexes) is a leaf lock never held across SQL.
    connection.set_txn_hooks_owned_(
        [this] { flush_changes(); },
        [this, context = connection.lattice_update_hook_context_.get()] {
#if defined(LATTICE_SYNC_COMMIT_PROBE)
            sync_commit_probe_detail::rolled_back(this, context->connection);
#endif
            auto* settlement = context->sync_chunk;
            context->note_settled(false);
            // Retire the original transaction's event/cursor reservation in
            // the rollback callback, BEFORE any subsequent body statement can
            // begin a successor. Teardown may no longer clear its dirty state.
            if (context->consume_recovery_reservation(settlement)) {
                std::lock_guard<std::mutex> lock(change_buffer_mutex_);
                change_buffer_.clear();
                recovery_change_buffer_reserved_ = false;
            } else discard_change_buffer();
            fire_invalidation_hooks({}, invalidation_reason::rollback);
        });
}
void lattice_db::publish_local_audit_id_(int64_t row_id) {
    instance_registry::instance().for_each_alive(config_.path,
        [row_id](lattice_db* inst) {
            inst->last_seen_audit_id_.store(row_id, std::memory_order_release);
        });
}
void lattice_db::setup_cross_process_notifier() {
    // Use the shared per-path notifier from instance_registry.
    // Only ONE Darwin listener per path per process — prevents N^2
    // notification amplification when multiple lattice_db instances
    // share the same DB file (e.g. via LatticeThreadSafeReference.resolve()).
    shared_xproc_notifier_ = instance_registry::instance().get_or_create_notifier(config_.path);
    if (!shared_xproc_notifier_) return;

    // Initialize this instance's cursor to current max AuditLog id
    auto max_rows = query_read("SELECT MAX(id) AS max_id FROM AuditLog");
    if (!max_rows.empty()) {
        auto it = max_rows[0].find("max_id");
        if (it != max_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            last_seen_audit_id_ = std::get<int64_t>(it->second);
        }
    }

    LOG_DEBUG("xproc", "Initialized cursor at audit id=%lld for path: %s",
              (long long)last_seen_audit_id_, config_.path.c_str());
}

void lattice_db::handle_cross_process_notification() {
    // KILL-SWITCH: set LATTICE_DISABLE_XPROC=1 to suppress cross-process observer dispatch.
    // Used to diagnose whether xproc notifications cause main-thread stalls.
    static bool disabled = (std::getenv("LATTICE_DISABLE_XPROC") != nullptr);
    if (disabled) return;

    // Recovery has a durable witness independent of outgoing AuditLog. Queue
    // its private drain before the legacy empty-audit exit.
    request_recovery_refresh();

    auto cursor = last_seen_audit_id_.load(std::memory_order_acquire);
    LOG_DEBUG("xproc", "Cross-process notification received, last_seen=%lld", (long long)cursor);

    // During process teardown the database files may already be deleted while
    // this callback is still queued on the dispatch queue. Catch db_error to
    // prevent an uncaught exception from aborting the process.
    try {
        // Query AuditLog for entries newer than our cursor.
        // Uses the dedicated xproc read connection to avoid SQLite lock contention
        // with observer callbacks running on the scheduler (MainActor), which use
        // read_db() for existence checks.
        auto rows = query_xproc(
            "SELECT id, tableName, operation, rowId, globalRowId, changedFieldsNames FROM AuditLog WHERE id > ? ORDER BY id ASC",
            {cursor}
        );

        if (rows.empty()) {
            // No new AuditLog entries, but the notification may indicate sync
            // state changes (e.g., daemon marked entries as isSynchronized=1).
            // Fire the dedicated idle hint callback directly on this thread —
            // NOT through notify_changes_batched / scheduler, which would
            // dispatch synthetic observer callbacks to MainActor and cause
            // thread pile-ups under rapid notification load.
            std::function<void()> hint;
            {
                std::lock_guard<std::mutex> lock(xproc_idle_mutex_);
                hint = on_xproc_idle_;
            }
            if (hint) {
                LOG_DEBUG("xproc", "No new AuditLog entries — firing xproc idle hint");
                hint();
            } else {
                LOG_DEBUG("xproc", "No new AuditLog entries — no idle hint registered");
            }
            return;
        }

        // Re-check cursor: the update_hook may have advanced it while we were
        // querying (local write committed between our cursor read and the
        // SELECT). Filter out entries that are now below the updated cursor
        // to avoid duplicate notifications for local changes.
        auto updated_cursor = last_seen_audit_id_.load(std::memory_order_acquire);
        if (updated_cursor > cursor) {
            rows.erase(
                std::remove_if(rows.begin(), rows.end(), [updated_cursor](const auto& row) {
                    auto id_it = row.find("id");
                    return id_it != row.end() &&
                           std::holds_alternative<int64_t>(id_it->second) &&
                           std::get<int64_t>(id_it->second) <= updated_cursor;
                }),
                rows.end()
            );
            if (rows.empty()) {
                LOG_DEBUG("xproc", "All entries filtered by advanced cursor (local write race)");
                return;
            }
        }

        LOG_DEBUG("xproc", "Found %zu new AuditLog entries from other process", rows.size());

        // Only notify THIS instance's observers. Each instance sharing the
        // same database path has its own cross-process notifier, so each
        // independently receives the event and processes it. Notifying all
        // instances here would cause N^2 observer callbacks when N instances
        // share a path (e.g., migration tests that open the same DB twice).
        //
        // Batch all changes into a single scheduler dispatch to avoid
        // creating N separate Tasks on the main actor (each with overhead).
        std::vector<std::tuple<std::string, std::string, int64_t, std::string, std::string>> changes;
        changes.reserve(rows.size() * 2);  // model change + AuditLog change per row

        for (const auto& row : rows) {
            auto id_it = row.find("id");
            auto table_it = row.find("tableName");
            auto op_it = row.find("operation");
            auto rowid_it = row.find("rowId");
            auto growid_it = row.find("globalRowId");
            auto cfn_it = row.find("changedFieldsNames");

            if (id_it == row.end() || !std::holds_alternative<int64_t>(id_it->second)) continue;

            int64_t audit_id = std::get<int64_t>(id_it->second);
            std::string table = (table_it != row.end() && std::holds_alternative<std::string>(table_it->second))
                ? std::get<std::string>(table_it->second) : "";
            std::string op = (op_it != row.end() && std::holds_alternative<std::string>(op_it->second))
                ? std::get<std::string>(op_it->second) : "";
            int64_t row_id = (rowid_it != row.end() && std::holds_alternative<int64_t>(rowid_it->second))
                ? std::get<int64_t>(rowid_it->second) : 0;
            std::string global_row_id = (growid_it != row.end() && std::holds_alternative<std::string>(growid_it->second))
                ? std::get<std::string>(growid_it->second) : "";
            std::string changed_fields_names = (cfn_it != row.end() && std::holds_alternative<std::string>(cfn_it->second))
                ? std::get<std::string>(cfn_it->second) : "";

            // Collect model table change
            changes.emplace_back(table, op, row_id, global_row_id, changed_fields_names);

            // Collect AuditLog change
            auto audit_gid_rows = query_xproc(
                "SELECT globalId FROM AuditLog WHERE id = ?", {audit_id}
            );
            std::string audit_global_id;
            if (!audit_gid_rows.empty()) {
                auto git = audit_gid_rows[0].find("globalId");
                if (git != audit_gid_rows[0].end() && std::holds_alternative<std::string>(git->second)) {
                    audit_global_id = std::get<std::string>(git->second);
                }
            }
            changes.emplace_back("AuditLog", "INSERT", audit_id, audit_global_id, "");

            last_seen_audit_id_.store(audit_id, std::memory_order_release);
        }

        // Resolve internal tables (link tables, geo_bounds list tables) to their
        // parent tables. Internal table changes are translated to parent UPDATE
        // notifications — e.g. a link table INSERT becomes a parent table UPDATE.
        // This mirrors the same resolution done in flush_changes() for same-process writes.
        std::unordered_map<std::string, std::string> internal_table_parents;
        for (const auto& [table, op, row_id, global_id, cfn] : changes) {
            if (table == "AuditLog" || internal_table_parents.count(table)) continue;
            auto meta = query_xproc(
                "SELECT value FROM _lattice_meta WHERE key = ?",
                {"internal_table:" + table}
            );
            if (!meta.empty()) {
                auto val_it = meta[0].find("value");
                if (val_it != meta[0].end() && std::holds_alternative<std::string>(val_it->second)) {
                    internal_table_parents[table] = std::get<std::string>(val_it->second);
                }
            }
        }

        // Rewrite internal table entries to parent UPDATE notifications.
        // Resolve the parent's actual rowId via the link table's lhs column
        // (parent globalId) so Swift table observers can validate the object.
        if (!internal_table_parents.empty()) {
            for (auto& change : changes) {
                auto& table = std::get<0>(change);
                auto it = internal_table_parents.find(table);
                if (it != internal_table_parents.end() && !it->second.empty()) {
                    auto& meta_value = it->second;
                    auto colon_pos = meta_value.find(':');
                    std::string parent_table = (colon_pos != std::string::npos)
                        ? meta_value.substr(0, colon_pos) : meta_value;
                    std::string property_name = (colon_pos != std::string::npos)
                        ? meta_value.substr(colon_pos + 1) : "";
                    std::string changed_fields = property_name.empty()
                        ? "" : "[\"" + property_name + "\"]";

                    // Resolve parent rowId: AuditLog changedFields has {"lhs": "parent-globalId", ...}
                    // Link table triggers store rowId=0 (no id column), so we extract
                    // the parent globalId from changedFields JSON and look up the parent row.
                    int64_t parent_row_id = 0;
                    std::string parent_global_id;
                    auto& link_global_id = std::get<3>(change);
                    if (!link_global_id.empty()) {
                        auto cf_rows = query_xproc(
                            "SELECT json_extract(changedFields, '$.lhs') AS lhs FROM AuditLog "
                            "WHERE globalRowId = ? AND tableName = ?",
                            {link_global_id, table}
                        );
                        if (!cf_rows.empty()) {
                            auto lhs_it = cf_rows[0].find("lhs");
                            if (lhs_it != cf_rows[0].end() && std::holds_alternative<std::string>(lhs_it->second)) {
                                parent_global_id = std::get<std::string>(lhs_it->second);
                                auto pid_rows = query_xproc(
                                    "SELECT id FROM \"" + parent_table + "\" WHERE globalId = ?",
                                    {parent_global_id}
                                );
                                if (!pid_rows.empty()) {
                                    auto pid_it = pid_rows[0].find("id");
                                    if (pid_it != pid_rows[0].end() && std::holds_alternative<int64_t>(pid_it->second)) {
                                        parent_row_id = std::get<int64_t>(pid_it->second);
                                    }
                                }
                            }
                        }
                    }

                    LOG_DEBUG("xproc", "Internal table %s -> parent UPDATE %s (prop=%s, parentRowId=%lld)",
                              table.c_str(), parent_table.c_str(), property_name.c_str(), (long long)parent_row_id);
                    table = parent_table;
                    std::get<1>(change) = "UPDATE";
                    std::get<2>(change) = parent_row_id;
                    std::get<3>(change) = parent_global_id;
                    std::get<4>(change) = changed_fields;
                }
            }
        }

        // Reconcile vec0 virtual tables for synced vector columns.
        // vec0 maintains per-connection internal state — shadow table writes
        // from the sync db connection are not visible to this connection's
        // vec0 virtual table. Re-insert on this connection so nearest()
        // queries return synced data.
        for (const auto& [table, op, row_id, global_id, cfn] : changes) {
            if (table == "AuditLog" || table.empty() || table[0] == '_') continue;
            if (op != "INSERT" && op != "UPDATE") continue;
            if (global_id.empty()) continue;

            auto vec_tables = db().query(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE ?",
                {"_" + table + "_%_vec"});

            for (const auto& vt_row : vec_tables) {
                auto name_it = vt_row.find("name");
                if (name_it == vt_row.end() || !std::holds_alternative<std::string>(name_it->second)) continue;
                auto& vec_table = std::get<std::string>(name_it->second);

                // Extract column name from vec table name: _Table_Column_vec
                auto prefix_len = table.size() + 2; // "_" + table + "_"
                auto suffix_len = 4; // "_vec"
                if (vec_table.size() <= prefix_len + suffix_len) continue;
                auto col = vec_table.substr(prefix_len, vec_table.size() - prefix_len - suffix_len);

                auto data_rows = db().query(
                    "SELECT " + col + " FROM " + table + " WHERE globalId = ?",
                    {global_id});
                if (data_rows.empty()) continue;

                auto col_it = data_rows[0].find(col);
                if (col_it == data_rows[0].end() ||
                    !std::holds_alternative<std::vector<uint8_t>>(col_it->second)) continue;
                auto& vec_data = std::get<std::vector<uint8_t>>(col_it->second);
                if (vec_data.empty()) continue;

                try {
                    // Idempotent + atomic (refresh_vec0_row): the writer's
                    // triggers already indexed this row in the common case,
                    // so this is usually a lock-free read that skips. The
                    // old blind DELETE+INSERT here — run by EVERY process
                    // receiving the xproc notification — interleaved across
                    // processes into 28K UNIQUE failures and a write-lock
                    // storm that livelocked IPC sync (Aug 13 incident).
                    refresh_vec0_row(vec_table, global_id, vec_data);
                } catch (const std::exception& e) {
                    // Expected under write bursts (lock contention); the
                    // open-path gap reconcile and knn's count-mismatch
                    // self-heal cover any row missed here.
                    LOG_DEBUG("xproc", "vec0 reconcile failed for %s: %s", vec_table.c_str(), e.what());
                }
            }
        }

        // Single batched dispatch — all observer callbacks run in one Task
        if (!changes.empty()) {
            notify_changes_batched(changes);
        }

        LOG_DEBUG("xproc", "Cross-process notification handled, cursor now at %lld", (long long)last_seen_audit_id_);
    } catch (const db_error&) {
        LOG_DEBUG("xproc", "Database unavailable during cross-process notification (likely teardown)");
    }
}

// Synchronizer registry — at most one synchronizer per {path, key}
// Used for both WSS (key = websocket_url) and IPC (key = channel name).
static std::mutex& sync_registry_mutex() {
    // Intentionally leaked: prevents use-after-destroy when destructors
    // run during process teardown (static destruction order is undefined).
    static auto* m = new std::mutex();
    return *m;
}
static std::set<std::pair<std::string, std::string>>& active_sync_keys() {
    // Intentionally leaked: same reason as sync_registry_mutex.
    static auto* s = new std::set<std::pair<std::string, std::string>>();
    return *s;
}

bool lattice_db::try_register_sync_key(const std::string& path, const std::string& key) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    return active_sync_keys().emplace(path, key).second;
}

void lattice_db::unregister_sync_key(const std::string& path, const std::string& key) {
    std::lock_guard<std::mutex> lock(sync_registry_mutex());
    active_sync_keys().erase({path, key});
}

// Collect all sync_ids for this database (WSS + all IPC channels).
// Used to populate all_active_sync_ids for per-synchronizer sync state.
static std::vector<std::string> collect_all_sync_ids(const configuration& config) {
    std::vector<std::string> ids;
    if (config.is_sync_enabled()) {
        ids.push_back("wss:" + config.websocket_url);
    }
    for (const auto& target : config.ipc_targets) {
        ids.push_back("ipc:" + target.channel);
    }
    return ids;
}

void lattice_db::setup_sync_if_configured() {
    if(recovery_continuous_) {if(config_.is_sync_enabled())detail::recovery_continuous_producer::setup_configured_route(*this);return;}
    if (!config_.is_sync_enabled()) {
        return;
    }

#ifndef __EMSCRIPTEN__
    // Cross-process flock: only one in-process lattice_db at a time may
    // own the WSS synchronizer for a given path. If the lock is held, we
    // distinguish three cases:
    //   1. Held by another *process* — silent skip (today's behavior).
    //   2. Held by an in-process sibling on the SAME wssEndpoint — silent
    //      skip (legitimate dormant duplicate, e.g. cross-actor cache miss).
    //   3. Held by an in-process sibling on a DIFFERENT wssEndpoint — this
    //      is a URL-change scenario (e.g. peer rejoining after host
    //      republished). Kick the sibling and take over.
    //
    // Skipped on WASM — single-threaded, single-process environment.
    if (sync_lock_fd_ < 0) {
        const std::string lock_path = config_.path + ".sync.lock";

        auto try_acquire_lock = [&]() -> bool {
            int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
            if (fd < 0) return false;
            if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                ::close(fd);
                return false;
            }
            sync_lock_fd_ = fd;
            return true;
        };

        if (!try_acquire_lock()) {
            // Try to find a kickable in-process sibling — one that owns
            // sync (synchronizer_ != nullptr) on a DIFFERENT URL.
            //
            // The kick must happen INSIDE the registry callback so the
            // registry's notify_refcount keeps the victim alive while
            // teardown_sync runs. fire_handoff=false prevents the victim's
            // own teardown from waking another same-URL sibling, which
            // would re-hold the flock against our new URL.
            bool kicked = false;
            instance_registry::instance().for_each_alive(config_.path,
                [&](lattice_db* sibling) {
                    if (kicked) return;
                    if (sibling == this) return;
                    if (sibling->synchronizer_ == nullptr) return;
                    if (sibling->config_.websocket_url == config_.websocket_url) return;
                    LOG_INFO("lattice_db",
                             "kicking sibling=%p (URL=%s) for URL change to %s",
                             (void*)sibling, sibling->config_.websocket_url.c_str(),
                             config_.websocket_url.c_str());
                    sibling->teardown_sync(/*fire_handoff=*/false);
                    kicked = true;
                });
            if (!kicked || !try_acquire_lock()) {
                LOG_DEBUG("lattice_db", "WSS sync lock held, skipping setup");
                return;
            }
        }
    }
#endif

    // Only one synchronizer per {path, websocket_url} across all instances
    if (!try_register_sync_key(config_.path, config_.websocket_url)) {
        LOG_DEBUG("lattice_db", "Synchronizer already active for this path, skipping");
        return;
    }

    // Create sync config from our configuration
    sync_config sync_cfg;
    sync_cfg.websocket_url = config_.websocket_url;
    sync_cfg.authorization_token = config_.authorization_token;
    sync_cfg.sync_filter = config_.sync_filter;

    // Always use per-synchronizer sync state
    auto all_ids = collect_all_sync_ids(config_);
    sync_cfg.sync_id = "wss:" + config_.websocket_url;
    {
        // Log-only identity (see sync_config::log_label).
        const auto slash = config_.path.find_last_of('/');
        sync_cfg.log_label = sync_cfg.sync_id + "@" +
            (slash == std::string::npos ? config_.path : config_.path.substr(slash + 1));
    }
    sync_cfg.all_active_sync_ids = all_ids;
    sync_cfg.is_observer = config_.sync_is_observer;
    config_.tuning.apply(sync_cfg);
    // Upload coalescing stays at the library default (0 = legacy immediate
    // dispatch): with the upload floor + classify gating, passes are cheap,
    // and enabling pacing here measurably collapsed relay-chain catch-up
    // throughput (each enumeration window paid the full coalesce delay).
    // Consumers with pathological tick rates can opt in via sync_config.

#ifdef __EMSCRIPTEN__
    // Emscripten: single-threaded, so the synchronizer borrows our connection
    // directly. This avoids opening a second connection to the same OPFS file,
    // which would fail due to exclusive locking.
    LOG_INFO("lattice_db", "sync using shared db (Emscripten), path=%s", config_.path.c_str());
#else
    // Native: create a dedicated lattice_db for the synchronizer (separate
    // connection on its own thread via std_thread_scheduler).
    std::string sync_path = resolve_path(config_);
    configuration sync_db_config(sync_path,
                                 std::make_shared<std_thread_scheduler>());
    sync_db_config.target_schema_version = config_.target_schema_version;
    sync_db_config.migration_block = config_.migration_block;
    auto sync_db = std::make_unique<lattice_db>(sync_db_config);

    // Debug: verify sync db can see model tables
    LOG_INFO("lattice_db", "sync_db created for path=%s", sync_db_config.path.c_str());
    try {
        auto tables = sync_db->db().query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name");
        std::string table_list;
        for (const auto& row : tables) {
            auto it = row.find("name");
            if (it != row.end() && std::holds_alternative<std::string>(it->second)) {
                if (!table_list.empty()) table_list += ", ";
                table_list += std::get<std::string>(it->second);
            }
        }
        LOG_INFO("lattice_db", "sync_db tables: [%s]", table_list.c_str());
    } catch (const std::exception& e) {
        LOG_ERROR("lattice_db", "sync_db table list query failed: %s", e.what());
    }
#endif

    // Create synchronizer
#ifdef __EMSCRIPTEN__
    synchronizer_ = std::make_unique<synchronizer>(*this, sync_cfg);
#else
    synchronizer_ = std::make_unique<synchronizer>(std::move(sync_db), sync_cfg);
#endif

    // Wire up callbacks if set
    if (on_sync_state_change_) {
        synchronizer_->set_on_state_change(on_sync_state_change_);
    }
    if (on_sync_error_) {
        synchronizer_->set_on_error(on_sync_error_);
    }

    // Auto-connect (like Swift's Lattice.init)
    synchronizer_->connect();
}

void lattice_db::setup_ipc_if_configured() {
    if(config_.is_ipc_enabled())detail::recovery_continuous_producer::require_no_continuous_route(*this);
#ifndef __EMSCRIPTEN__
    if (!config_.is_ipc_enabled()) {
        return;
    }

    // Only one IPC endpoint per {path, channel} across all instances
    // (mirrors the WSS dedup via try_register_sync_key).
    // Filter out channels that are already registered by another instance.
    std::vector<size_t> active_indices;
    for (size_t i = 0; i < config_.ipc_targets.size(); ++i) {
        const auto& channel = config_.ipc_targets[i].channel;
        if (try_register_sync_key(config_.path, "ipc:" + channel)) {
            active_indices.push_back(i);
        } else {
            LOG_DEBUG("lattice_db", "IPC synchronizer already active for %s on channel %s, skipping",
                      config_.path.c_str(), channel.c_str());
        }
    }
    if (active_indices.empty()) return;

    auto all_ids = collect_all_sync_ids(config_);

    // Phase 1: Create all endpoints and store them in the vector.
    // We must do this BEFORE starting any endpoint, because:
    // - The server callback fires asynchronously on the accept thread
    // - Capturing &state.sync from a local that is later moved = dangling pointer
    // - reserve() ensures push_back won't invalidate references
    ipc_synchronizers_.reserve(active_indices.size());

    for (size_t idx : active_indices) {
        ipc_sync_state state;
        state.endpoint = std::make_unique<ipc_endpoint>(config_.ipc_targets[idx].channel,
                                                        config_.ipc_targets[idx].socket_path);

        // Acquire per-channel flock — mirrors sync_lock_fd_ for WSS.
        // Set during construction so is_sync_agent() returns true before
        // the accept callback creates the synchronizer (avoids timing races).
        const auto& target_cfg = config_.ipc_targets[idx];
        std::string sock_path = target_cfg.socket_path.value_or(
            resolve_ipc_socket_path(target_cfg.channel));
        std::string lock_path = sock_path + ".lock";
        int lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
        if (lock_fd >= 0) {
            if (::flock(lock_fd, LOCK_EX | LOCK_NB) == 0) {
                state.lock_fd = lock_fd;
                LOG_INFO("ipc_sync", "Acquired IPC lock: %s (fd=%d)", lock_path.c_str(), lock_fd);
            } else {
                // Another process holds this channel — still proceed (IPC handles roles via bind)
                // but don't set lock_fd so is_sync_agent() reflects the true owner.
                ::close(lock_fd);
                LOG_INFO("ipc_sync", "IPC lock held by another process: %s", lock_path.c_str());
            }
        }

        ipc_synchronizers_.push_back(std::move(state));
    }

    // Phase 2: Start endpoints using stable references into the vector.
    for (size_t i = 0; i < ipc_synchronizers_.size(); ++i) {
        const auto& target = config_.ipc_targets[active_indices[i]];
        std::string sync_id = "ipc:" + target.channel;
        auto& sync_slot = ipc_synchronizers_[i].sync;

        ipc_synchronizers_[i].endpoint->start(
            [this, sync_id, all_ids, &target, &sync_slot,
             ep = ipc_synchronizers_[i].endpoint.get()](std::unique_ptr<ipc_socket_client> transport) {
                LOG_INFO("ipc_sync", "[%s] Accept callback fired (sync_slot=%s, db=%s)",
                         sync_id.c_str(), sync_slot ? "OCCUPIED" : "empty",
                         config_.path.c_str());

                sync_config ipc_cfg;
                ipc_cfg.sync_id = sync_id;
                // Log-only identity: hub and spoke share the same sync_id on
                // an IPC channel; role + db basename is what makes their
                // interleaved lines in one process log tell apart.
                const auto slash = config_.path.find_last_of('/');
                ipc_cfg.log_label = sync_id + (ep->is_server() ? "#srv@" : "#cli@") +
                    (slash == std::string::npos ? config_.path : config_.path.substr(slash + 1));
                ipc_cfg.all_active_sync_ids = all_ids;
                ipc_cfg.is_observer = config_.sync_is_observer;
                ipc_cfg.sync_filter = target.sync_filter;
                ipc_cfg.narrowing_emits_removals = target.narrowing_emits_removals;
                config_.tuning.apply(ipc_cfg);
                // Coalescing off by default — see the WSS note above.

                // Create dedicated lattice_db for this IPC synchronizer
                configuration ipc_db_config(config_.path,
                                            std::make_shared<std_thread_scheduler>());
                ipc_db_config.target_schema_version = config_.target_schema_version;
                ipc_db_config.migration_block = config_.migration_block;
                auto ipc_db = std::make_unique<lattice_db>(ipc_db_config);

                LOG_INFO("ipc_sync", "[%s] Creating synchronizer (replacing old=%p)",
                         sync_id.c_str(), sync_slot ? (void*)sync_slot.get() : nullptr);

                auto sync = std::make_unique<synchronizer>(
                    std::move(ipc_db), ipc_cfg, std::move(transport));

                // Lock ipc_callbacks_mutex_ to synchronize with set_on_sync_progress/
                // set_on_sync_state_change/set_on_sync_error on the main thread.
                // Without this, the accept thread and main thread can race: both
                // check-then-act on {on_sync_progress_, sync_slot} and miss each other.
                {
                    std::lock_guard<std::mutex> lock(ipc_callbacks_mutex_);

                    // Apply stored callbacks to the newly-created synchronizer
                    if (on_sync_progress_) {
                        sync->set_on_progress(on_sync_progress_);
                    }
                    if (on_sync_state_change_) {
                        sync->set_on_state_change(on_sync_state_change_);
                    }
                    if (on_sync_error_) {
                        sync->set_on_error(on_sync_error_);
                    }

                    sync->connect();
                    sync_slot = std::move(sync);
                }

                LOG_INFO("ipc_sync", "[%s] New synchronizer installed (this=%p)",
                         sync_id.c_str(), (void*)sync_slot.get());
            });
    }
#endif // !__EMSCRIPTEN__
}

std::vector<std::string> lattice_db::attachment_column_names(
    database* db, const std::string& schema_sql, const std::string& table_name) {
    // schema_sql is main or a caller-built escaped attachment qualifier.
    // PRAGMA's argument is a string literal, not an unquoted model identifier.
    std::string argument = "'";
    for (char c : table_name) { argument += c; if (c == '\'') argument += '\''; }
    argument += "'";
    auto cols = db->query_attachment_text_metadata(
        "PRAGMA " + schema_sql + ".table_info(" + argument + ")", "name");
    std::sort(cols.begin(), cols.end());
    return cols;
}

namespace {
std::string attach_alias_for(const std::string& path) {
    std::filesystem::path p = path;
    return p.filename().replace_extension().string();
}

// Attachment aliases come from caller-controlled path stems. Use the same
// SQLite identifier escaping for view qualifiers and DETACH.
std::string quoted_attach_alias(const std::string& alias) {
    std::string quoted = "\"";
    for (char c : alias) {
        quoted += c;
        if (c == '"') quoted += '"';
    }
    quoted += '"';
    return quoted;
}

// Model tables only — excludes sqlite internals, _-prefixed virtual/shadow
// tables, and Lattice bookkeeping. %s is the sqlite_master to scan.
constexpr const char* kAttachTableFilter =
    "SELECT name FROM %s WHERE type='table' "
    "AND name NOT LIKE 'sqlite_%%' "
    "AND name NOT LIKE '\\_%%' ESCAPE '\\' "
    "AND name NOT IN ('AuditLog')";

} // namespace

std::unordered_set<std::string> lattice_db::attachment_model_tables(database* db, const char* master) {
    char sql[512];
    snprintf(sql, sizeof(sql), kAttachTableFilter, master);
    std::unordered_set<std::string> out;
    for (auto& name : db->query_attachment_text_metadata(sql, "name"))
        out.insert(std::move(name));
    return out;
}

void lattice_db::attach(lattice_db &lattice) {
    attach_with_metadata(lattice, {});
}

void lattice_db::attach_with_metadata(lattice_db& lattice, std::shared_ptr<const void> metadata) {
    if(recovery_continuous_||lattice.recovery_continuous_)throw db_error("continuous attachment topology requires admitted same-file facades");
    if (detail::managed_route_scope::active_for(this))
        throw db_error("attach: topology mutation during managed scalar access");
    std::shared_ptr<database> other, writer;
    {
        std::lock_guard<std::mutex> lock(lattice.connection_ownership_mutex_);
        other = lattice.db_;
    }
    std::vector<std::shared_ptr<database>> handles;
    // Resolve source metadata BEFORE taking recipient locks; no foreign parent
    // or SQLite lock can nest beneath this recipient's topology/service locks.
    std::shared_ptr<const physical_store_identity> source_identity;
    std::shared_ptr<projection_pressure_source> pressure_source;
    try {
        if (other) source_identity = other->physical_identity("main", {}, true);
        pressure_source = make_projection_pressure_source(source_identity);
        if (pressure_source) pressure_source->active.store(false);
    } catch (...) {} // Unsupported identity must not break legacy attachment.
    // Serialize attach/detach: the already-attached consult below is
    // check-then-act, and view regeneration must not interleave with a
    // concurrent attach/detach on another thread.
    std::lock_guard<std::mutex> attach_lock(attach_mutex_);
    handles = view_handles();
    {
        std::lock_guard<std::mutex> lock(connection_ownership_mutex_);
        writer = db_;
    }

    if (closed_.load() || !writer || !other || handles.empty() ||
        handles.front().get() != writer.get()) {
        throw std::runtime_error("attach: a database is closed");
    }

    const std::string alias = attach_alias_for(lattice.config_.path);

    // Idempotence: same alias + same path is a no-op; same alias for a
    // different path is a caller error.
    for (const auto& [existing_alias, existing_path] : attached_dbs_) {
        if (existing_alias == alias) {
            if (existing_path == lattice.config_.path) {
                if (!attached_route_tokens_.count(alias))
                    throw std::runtime_error("attach: incomplete prior topology change; detach before retry");
                return;
            }
            throw std::runtime_error(
                "attach: alias '" + alias + "' is already attached to a different database (" +
                existing_path + ")");
        }
    }

    // Validate schema overlap BEFORE any side effect, reading the other
    // lattice's schema through ITS OWN connection — a mismatch throws with
    // this lattice untouched (no dangling ATTACH, no half-created views).
    {
        if (handles.empty()) {
            throw std::runtime_error("attach: this database is closed");
        }
        auto main_set = attachment_model_tables(handles.front().get(), "main.sqlite_master");
        auto other_set = attachment_model_tables(other.get(), "main.sqlite_master");
        auto validate_metadata_names = [](const auto& columns, const std::string& table) {
            for (const auto& name : columns) {
                if (name == "_source" || name == "_lattice_attach_token")
                    throw std::runtime_error("attach: reserved routing column '" + name +
                                             "' in model table '" + table + "'");
            }
        };
        for (const auto& table_name : main_set)
            validate_metadata_names(attachment_column_names(handles.front().get(), "main", table_name), table_name);
        for (const auto& table_name : other_set) {
            auto other_cols = attachment_column_names(other.get(), "main", table_name);
            validate_metadata_names(other_cols, table_name);
            if (!main_set.count(table_name)) continue;
            auto main_cols = attachment_column_names(handles.front().get(), "main", table_name);
            if (main_cols != other_cols) {
                LOG_ERROR("db", "Schema mismatch for table '%s' between main and attached DB '%s'",
                          table_name.c_str(), alias.c_str());
                throw std::runtime_error(
                    "Schema mismatch for table '" + table_name +
                    "' between main database and attached database '" + alias + "'");
            }
        }
    }

    // ATTACH on every view-bearing handle (null-tolerant — sync-enabled and
    // in-memory lattices have no read_db_; the old code null-dereferenced).
    // Bookkeeping already handles idempotence. An unbookkept duplicate alias
    // may be a partial earlier attach or a raw SQL binding to another file;
    // let SQLite reject it rather than minting provenance for the wrong file.
    // Escape single quotes in the path for the SQL literal (paths and named-
    // memory names are caller-controlled strings).
    std::string escaped_path;
    escaped_path.reserve(lattice.config_.path.size());
    for (char c : lattice.config_.path) {
        escaped_path += c;
        if (c == '\'') escaped_path += '\'';
    }
    if (next_attachment_token_ == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("attach: attachment generation exhausted");
    const int64_t token = next_attachment_token_++;
    attachment_topology_valid_ = false;
    cancel_projection_reads(projection_status::snapshot_expired);
    try {
        // Publish before ATTACH permits writes to this alias. No hook takes
        // topology locks or observes a map being mutated.
        if (pressure_source) replace_projection_pressure_source(alias, pressure_source);
        std::shared_ptr<const physical_store_identity> actual_identity;
        for (const auto& handle : handles) {
            std::string escaped_alias;
            escaped_alias.reserve(alias.size());
            for (char c : alias) {
                escaped_alias += c;
                if (c == '\"') escaped_alias += '\"';
            }
            const std::string attach_sql = "ATTACH DATABASE '" + escaped_path + "' AS \"" + escaped_alias + "\"";
            if (handle.get() == writer.get())
                actual_identity = handle->attach_and_capture_identity(attach_sql, alias);
            else
                handle->execute(attach_sql);
        }

        if (source_identity && actual_identity && *source_identity == *actual_identity)
            attached_projection_identities_[alias] = source_identity;
        else {
            attached_projection_identities_[alias] = {};
            replace_projection_pressure_source(alias, {});
        }
        attached_route_tokens_[alias] = token;
        attached_dbs_.emplace_back(alias, lattice.config_.path);
        attached_aliases_.push_back(alias);
        rebuild_attached_views(handles);
        if (metadata) attached_route_metadata_[token] = std::move(metadata);
        publish_managed_attachment(alias, lattice.config_.path, token, writer);
        attachment_topology_valid_ = true;
    } catch (...) {
        // Existing attach is not cross-connection atomic. A partially changed
        // topology must never authorize a checked mutation through this alias.
        invalidate_attachment_route(alias);
        attached_projection_identities_.erase(alias);
        replace_projection_pressure_source(alias, {});
        throw;
    }
}

void lattice_db::detach(lattice_db &lattice) {
    if (detail::managed_route_scope::active_for(this))
        throw db_error("detach: topology mutation during managed scalar access");
    // Resolve by PATH, not by recomputed alias: alias derivation truncates at
    // the filename stem, so two different paths (or dotted memory names) can
    // share an alias — a never-attached path must be a clean no-op, never a
    // detach of a same-stem victim.
    std::string alias;
    int64_t token = -1;
    {
        std::lock_guard<std::mutex> attach_lock(attach_mutex_);
        auto it = std::find_if(attached_dbs_.begin(), attached_dbs_.end(),
            [&](const auto& entry) { return entry.second == lattice.config_.path; });
        if (it == attached_dbs_.end()) return;  // not attached: idempotent no-op
        alias = it->first;
        auto route = attached_route_tokens_.find(alias);
        if (route != attached_route_tokens_.end()) token = route->second;
    }
    detach_alias_if_current(alias, lattice.config_.path, token);
}

void lattice_db::detach_alias(const std::string& alias) {
    detach_alias_if_current(alias, std::nullopt, std::nullopt);
}

void lattice_db::detach_alias_if_current(const std::string& alias,
                                        const std::optional<std::string>& expected_path,
                                        std::optional<int64_t> expected_token) {
    if (detail::managed_route_scope::active_for(this))
        throw db_error("detach: topology mutation during managed scalar access");
    std::vector<std::shared_ptr<database>> handles;
    std::lock_guard<std::mutex> attach_lock(attach_mutex_);
    handles = view_handles();

    auto it = std::find_if(attached_dbs_.begin(), attached_dbs_.end(),
        [&](const auto& entry) { return entry.first == alias; });
    if (it == attached_dbs_.end()) return;  // idempotent no-op
    auto route = attached_route_tokens_.find(alias);
    const int64_t token = route == attached_route_tokens_.end() ? -1 : route->second;
    if ((expected_path && it->second != *expected_path) ||
        (expected_token && token != *expected_token)) return;

    // Invalidate before the first side effect, including when a later DROP or
    // DETACH fails. Old row handles stay invalid after an alias is reused.
    attachment_topology_valid_ = false;
    cancel_projection_reads(projection_status::snapshot_expired);
    invalidate_attachment_route(alias);

    // Views reference the alias — drop them all first (regeneration for the
    // remaining aliases happens after the DETACH).
    for (const auto& handle : handles) {
        for (const auto& view : attached_view_names_) {
            handle->execute("DROP VIEW IF EXISTS \"" + view + "\"");
        }
    }
    attached_view_names_.clear();
    attached_view_sql_.clear();

    // DETACH with a bounded retry: there is no prepared-statement cache —
    // every query prepares/finalizes in-call — so the only blocker is an
    // in-flight statement on another thread transiently locking the schema.
    for (const auto& handle : handles) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        for (;;) {
            try {
                handle->execute("DETACH DATABASE " + quoted_attach_alias(alias));
                break;
            } catch (const db_error& e) {
                const std::string msg = e.what();
                const bool transient = msg.find("locked") != std::string::npos ||
                                       msg.find("busy") != std::string::npos;
                if (!transient || std::chrono::steady_clock::now() >= deadline) throw;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    attached_projection_identities_.erase(alias);
    replace_projection_pressure_source(alias, {});
    attached_dbs_.erase(it);
    attached_aliases_.erase(
        std::remove(attached_aliases_.begin(), attached_aliases_.end(), alias),
        attached_aliases_.end());
    rebuild_attached_views(handles);
    attachment_topology_valid_ = true;
}

// Caller holds attach_mutex_. Drops every view attach created, then
// regenerates from the CURRENT alias set — this makes multi-alias
// attach/detach order-independent (the old CREATE ... IF NOT EXISTS meant a
// second alias sharing a table name silently never joined the union view)
// and restores main-table visibility after the last detach.
void lattice_db::rebuild_attached_views(const std::vector<std::shared_ptr<database>>& handles) {
    attached_view_sql_.clear();
    std::map<std::string, std::string> rebuilt_sql;
    for (const auto& handle : handles) {
        for (const auto& view : attached_view_names_) {
            handle->execute("DROP VIEW IF EXISTS \"" + view + "\"");
        }
    }
    attached_view_names_.clear();
    if (attached_dbs_.empty()) return;

    for (const auto& handle : handles) {
        auto main_set = attachment_model_tables(handle.get(), "main.sqlite_master");

        // table → arms. An arm is (schema-qualifier, _source label).
        std::map<std::string, std::vector<std::pair<std::string, std::string>>> arms;
        for (const auto& [alias, _] : attached_dbs_) {
            const std::string quoted = quoted_attach_alias(alias);
            char master[600];
            snprintf(master, sizeof(master), "%s.sqlite_master", quoted.c_str());
            for (const auto& table_name : attachment_model_tables(handle.get(), master)) {
                arms[table_name].emplace_back(quoted, quoted);
            }
        }

        for (const auto& [table_name, alias_arms] : arms) {
            const bool in_main = main_set.count(table_name) > 0;
            // UNION ALL matches columns BY POSITION, and each database's
            // physical column order is whatever its create-time schema
            // iteration produced (an unordered map — effectively random
            // per file). `SELECT *` arms therefore SCRAMBLE values across
            // same-named columns whenever two files disagree — observed
            // live: a WHERE over the view read an attached row's
            // modifiedAt as deletedAt and filtered every spoke row out
            // of recall. Project every arm's columns explicitly, in one
            // canonical order, so the view maps by NAME. A column
            // missing from some arm then fails view creation loudly
            // instead of scrambling silently.
            const std::string first_schema =
                in_main ? "main" : alias_arms.front().first;
            auto ordered_cols = [&]() {
                // Preserve physical column order here; only comparison above sorts.
                std::string argument = "'";
                for (char c : table_name) { argument += c; if (c == '\'') argument += '\''; }
                argument += "'";
                const auto names = handle->query_attachment_text_metadata(
                    "PRAGMA " + first_schema + ".table_info(" + argument + ")", "name");
                std::string cols;
                for (const auto& name : names) {
                    if (!cols.empty()) cols += ", ";
                    cols += "\"" + name + "\"";
                }
                return cols;
            }();
            std::string sql = "CREATE TEMP VIEW IF NOT EXISTS " + table_name + " AS ";
            bool first = true;
            if (in_main) {
                sql += "SELECT " + ordered_cols + ", 'main' AS _source, 0 AS _lattice_attach_token FROM main." + table_name;
                first = false;
            }
            for (const auto& [qualifier, label] : alias_arms) {
                if (!first) sql += " UNION ALL ";
                // The label is caller-controlled (derived from the attached
                // path) — escape single quotes for the SQL string literal.
                std::string escaped_label;
                escaped_label.reserve(label.size());
                for (char c : label) {
                    escaped_label += c;
                    if (c == '\'') escaped_label += '\'';
                }
                // qualifier/label are escaped SQL identifiers. Resolve the
                // token from that same spelling; absent means failed topology.
                int64_t token = -1;
                for (const auto& [alias, candidate] : attached_route_tokens_) {
                    std::string quoted = "\"";
                    for (char c : alias) { quoted += c; if (c == '\"') quoted += '\"'; }
                    quoted += "\"";
                    if (quoted == qualifier) { token = candidate; break; }
                }
                sql += "SELECT " + ordered_cols + ", '" + escaped_label +
                       "' AS _source, " + std::to_string(token) +
                       " AS _lattice_attach_token FROM " + qualifier + "." + table_name;
                first = false;
            }
            handle->execute(sql);
            attached_view_names_.insert(table_name);
            rebuilt_sql[table_name] = sql;
        }
    }
    attached_view_sql_ = std::move(rebuilt_sql);
}

// ============================================================================
// managed<std::vector<geo_bounds*>> method implementations
// ============================================================================

size_t managed<std::vector<geo_bounds*>>::size() const {
    if (!is_bound()) return unmanaged_value.size();
    load_if_needed();
    return cached_objects_.size();
}

void managed<std::vector<geo_bounds*>>::load_if_needed() const {
    if (loaded_ || !is_bound()) return;

    cached_objects_.clear();
    std::string sql = "SELECT id, minLat, maxLat, minLon, maxLon FROM " + list_table_ +
                      " WHERE parent_id = ? ORDER BY id";
    auto rows = db->query(sql, {parent_global_id_});

    for (const auto& row : rows) {
        auto get_double = [&](const std::string& col) -> double {
            auto it = row.find(col);
            if (it != row.end() && std::holds_alternative<double>(it->second)) {
                return std::get<double>(it->second);
            }
            return 0.0;
        };
        auto get_int64 = [&](const std::string& col) -> int64_t {
            auto it = row.find(col);
            if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
                return std::get<int64_t>(it->second);
            }
            return 0;
        };

        geo_bounds bounds(
            get_double("minLat"),
            get_double("maxLat"),
            get_double("minLon"),
            get_double("maxLon")
        );
        int64_t row_id = get_int64("id");

        auto wrapper = std::make_shared<managed<geo_bounds*>>(bounds);
        wrapper->bind_to_list_row(db, lattice, list_table_, rtree_table_, row_id, parent_global_id_);
        cached_objects_.push_back(wrapper);
    }
    loaded_ = true;
}

void managed<std::vector<geo_bounds*>>::push_back(const geo_bounds& bounds) {
    if (!is_bound()) {
        unmanaged_value.push_back(bounds);
        return;
    }

    // Ensure the list table exists
    if (lattice) {
        lattice->ensure_geo_bounds_list_table(table_name, column_name);
    }

    // list_table_ is already a physical, schema-qualified target. insert()
    // prefixes main for logical model names. RETURNING also captures this
    // row's identity before the settled tail can invoke callbacks that write.
    const auto inserted = db->query(
        "INSERT INTO " + list_table_ +
        " (parent_id, minLat, maxLat, minLon, maxLon) VALUES (?, ?, ?, ?, ?) RETURNING id",
        {parent_global_id_, bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon});
    if (inserted.size() != 1) {
        throw db_error("Geo bounds insert did not return one row id");
    }
    const auto id = inserted[0].find("id");
    if (id == inserted[0].end() || !std::holds_alternative<int64_t>(id->second)) {
        throw db_error("Geo bounds insert returned an invalid row id");
    }
    const primary_key_t new_row_id = std::get<int64_t>(id->second);

    // Create cached wrapper
    auto wrapper = std::make_shared<managed<geo_bounds*>>(bounds);
    wrapper->bind_to_list_row(db, lattice, list_table_, rtree_table_, new_row_id, parent_global_id_);
    cached_objects_.push_back(wrapper);
}

void managed<std::vector<geo_bounds*>>::erase(size_t index) {
    if (!is_bound()) {
        if (index < unmanaged_value.size()) {
            unmanaged_value.erase(unmanaged_value.begin() + static_cast<std::ptrdiff_t>(index));
        }
        return;
    }

    load_if_needed();
    if (index >= cached_objects_.size()) return;

    // Get the row id from the cached wrapper
    int64_t row_id_to_delete = cached_objects_[index]->list_row_id_;

    // Delete from database
    db->execute("DELETE FROM " + list_table_ + " WHERE id = ?", {row_id_to_delete});

    // Update cache
    cached_objects_.erase(cached_objects_.begin() + static_cast<std::ptrdiff_t>(index));
}

void managed<std::vector<geo_bounds*>>::clear() {
    if (!is_bound()) {
        unmanaged_value.clear();
        return;
    }

    // Delete all entries for this parent
    std::string sql = "DELETE FROM " + list_table_ + " WHERE parent_id = ?";
    db->execute(sql, {parent_global_id_});

    // Clear cache
    cached_objects_.clear();
    loaded_ = true;  // Mark as loaded (empty)
}

managed<std::vector<geo_bounds*>>::iterator managed<std::vector<geo_bounds*>>::begin() {
    load_if_needed();
    return iterator(cached_objects_.begin());
}

managed<std::vector<geo_bounds*>>::iterator managed<std::vector<geo_bounds*>>::end() {
    load_if_needed();
    return iterator(cached_objects_.end());
}

managed<std::vector<geo_bounds*>>::element_proxy
managed<std::vector<geo_bounds*>>::operator[](size_t index) {
    return element_proxy(this, index);
}

geo_bounds managed<std::vector<geo_bounds*>>::operator[](size_t index) const {
    if (!is_bound()) {
        return unmanaged_value[index];
    }
    load_if_needed();
    return cached_objects_[index]->detach();
}

// Element proxy implementations
managed<std::vector<geo_bounds*>>::element_proxy::operator managed<geo_bounds*>&() const {
    list_->load_if_needed();
    return *list_->cached_objects_[index_];
}

managed<geo_bounds*>*
managed<std::vector<geo_bounds*>>::element_proxy::operator->() const {
    list_->load_if_needed();
    return list_->cached_objects_[index_].get();
}

managed<std::vector<geo_bounds*>>::element_proxy&
managed<std::vector<geo_bounds*>>::element_proxy::operator=(const geo_bounds& bounds) {
    list_->load_if_needed();
    *list_->cached_objects_[index_] = bounds;
    return *this;
}

} // namespace lattice
