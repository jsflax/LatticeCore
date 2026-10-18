#include "recovery_refresh.hpp"
#include <limits>
#include <set>
#include <map>
#include <chrono>
#include <condition_variable>

namespace lattice::detail {
namespace {
struct listener {
    std::atomic<bool> active{true};
    std::function<void()> callback;
    explicit listener(std::function<void()> f) : callback(std::move(f)) {}
};
// Matches instance_registry's admission protocol. Only the heap guard is
// accessed after user callbacks (which may close/destroy the facade).
struct owner_hold {
    std::shared_ptr<instance_guard> guard;
    bool admitted = false;
    explicit owner_hold(std::shared_ptr<instance_guard> g) : guard(std::move(g)) {
        ++instance_guard::tls_depths()[guard.get()]; // allocate before publishing
        guard->notify_refcount.fetch_add(1, std::memory_order_seq_cst);
        admitted = guard->alive.load(std::memory_order_seq_cst);
    }
    ~owner_hold() {
        auto& depths = instance_guard::tls_depths();
        if (--depths[guard.get()] == 0) depths.erase(guard.get());
        guard->notify_refcount.fetch_sub(1, std::memory_order_seq_cst);
    }
};
thread_local size_t* preparation_writer_inspections = nullptr; // private test seam; inert otherwise
std::string text(const database::row_t& row, const char* key) {
    const auto found = row.find(key);
    if (found == row.end() || !std::holds_alternative<std::string>(found->second))
        throw db_error("recovery refresh schema text unavailable");
    return std::get<std::string>(found->second);
}
using field_map = std::map<std::string, std::set<std::string>>;
field_map fields_for(database& reader, const std::set<std::string>& subscribed) {
    field_map fields;
    for (const auto& table : subscribed) {
        const auto columns = reader.query("SELECT name FROM pragma_table_info(?, 'main') ORDER BY cid", {table});
        if (columns.empty()) throw db_error("recovery refresh subscribed schema missing");
        std::set<std::string> physical;
        for (const auto& column : columns) physical.insert(text(column, "name"));
        auto& names = fields[table];
        for (const auto& name : physical) {
            if (name != "id" && name != "globalId") names.insert(name);
            constexpr std::string_view suffix = "_minLat";
            if (name.size() <= suffix.size() || !name.ends_with(suffix)) continue;
            const auto prefix = name.substr(0, name.size() - suffix.size());
            if (physical.contains(prefix + "_maxLat") && physical.contains(prefix + "_minLon") &&
                physical.contains(prefix + "_maxLon")) names.insert(prefix);
        }
    }
    if (subscribed.empty()) return fields;
    // Schema work, not a row scan. Keep complete logical relation names for
    // current held parents, including union tables with several parent maps.
    const auto mappings = reader.query("SELECT value FROM main._lattice_meta WHERE key LIKE 'internal_table:%'");
    for (const auto& row : mappings) {
        const auto mapping = text(row, "value");
        size_t start = 0;
        while (start < mapping.size()) {
            const auto end = mapping.find(';', start);
            const auto item = mapping.substr(start, end == std::string::npos ? end : end - start);
            const auto colon = item.find(':');
            if (colon != std::string::npos && colon + 1 < item.size()) {
                const auto table = item.substr(0, colon);
                if (subscribed.contains(table)) fields[table].insert(item.substr(colon + 1));
            }
            if (end == std::string::npos) break;
            start = end + 1;
        }
    }
    return fields;
}
} // namespace

struct recovery_refresh_access::statement_guard {
    store_physical_native_scope physical_call;
    sqlite3_mutex* mutex;
    bool owned;
    explicit statement_guard(database& db) : physical_call(db),
        mutex(physical_call.entered()?sqlite3_db_mutex(db.internal_handle()):nullptr),
        owned(physical_call.entered()&&sqlite3_mutex_try(mutex)==SQLITE_OK) {}
    ~statement_guard() { if (owned) sqlite3_mutex_leave(mutex); }
};
bool recovery_refresh_access::settled(database& writer, sqlite3* h, std::shared_ptr<const physical_store_identity>* identity) {
    if (preparation_writer_inspections) ++*preparation_writer_inspections;
    if(h!=writer.internal_handle())return false;
    statement_guard guard(writer);
    if (!guard.owned || writer.is_closed() || sqlite3_get_autocommit(h) == 0) return false;
    for (auto* s = sqlite3_next_stmt(h, nullptr); s; s = sqlite3_next_stmt(h, s))
        if (sqlite3_stmt_busy(s)) return false;
    if (identity) *identity = writer.physical_identity("main", {}, true);
    return true;
}

struct recovery_refresh_signal;
struct recovery_refresh_state {
    std::mutex mutex; // leaf: never SQL, scheduler calls, or callback destruction
    uint64_t revision = 0, acknowledged_revision = 0, next_id = 1;
    uint64_t managed_revision = 0, acknowledged_managed_revision = 0;
    size_t managed_registrations = 0;
    bool managed_active = false;
    std::shared_ptr<const managed_observation_state::interest_callback> managed_hook;
    bool in_flight = false;
    bool requested = false, manual = false;
    // Independent of the witness delivery ticket: a scheduler may keep that
    // ticket queued indefinitely without starving storage maintenance.
    bool automatic_enabled = false, automatic_enrolling = false, automatic_running = false;
    size_t automatic_pauses = 0;
    std::chrono::steady_clock::time_point automatic_next_poll{};
    uint64_t automatic_attempts = 0, automatic_quiet = 0, automatic_deferred = 0, automatic_completed = 0;
    lattice_db* owner = nullptr; // guarded by the independent heap lifetime guard
    std::shared_ptr<instance_guard> guard;
    std::weak_ptr<scheduler> delivery_scheduler; // never keep a queued-work cycle alive
    std::weak_ptr<recovery_refresh_signal> wake;
    std::chrono::steady_clock::time_point next_poll{};
    bool witness_was_missing = false;
    std::optional<recovery_witness> acknowledged;
    std::map<uint64_t, std::shared_ptr<listener>> listeners;
    std::shared_ptr<database> probe_reader; // private, one admitted drain only
    bool interested() const { return !listeners.empty() || managed_active; } // mutex held
};

struct recovery_refresh_prepared {
    uint64_t subscription_revision, connection_revision;
    uint64_t managed_revision;
    std::optional<recovery_witness> witness;
    field_map names;
};
#ifndef __EMSCRIPTEN__
struct recovery_refresh_signal {
    std::mutex mutex; // leaf; callbacks/SQL/destruction always outside
    std::condition_variable changed;
    bool stopped = false;
    uint64_t revision = 0;
    std::map<recovery_refresh_state*, std::shared_ptr<recovery_refresh_state>> owners;
    void notify() noexcept {
        { std::lock_guard<std::mutex> lock(mutex); ++revision; }
        changed.notify_one();
    }
};
struct recovery_refresh_worker {
    std::shared_ptr<recovery_refresh_signal> signal = std::make_shared<recovery_refresh_signal>();
    std_thread_scheduler executor;
    static recovery_refresh_worker& instance() { static recovery_refresh_worker worker; return worker; }
    recovery_refresh_worker() { executor.invoke([keep = signal] { loop(keep); }); }
    ~recovery_refresh_worker() {
        { std::lock_guard<std::mutex> lock(signal->mutex); signal->stopped = true; }
        signal->changed.notify_one(); executor.shutdown();
    }
    void add(const std::shared_ptr<recovery_refresh_state>& shared) {
        { std::lock_guard<std::mutex> lock(shared->mutex); shared->wake = signal; }
        { std::lock_guard<std::mutex> lock(signal->mutex); signal->owners.emplace(shared.get(), shared); ++signal->revision; }
        signal->changed.notify_one();
    }
    static void loop(const std::shared_ptr<recovery_refresh_signal>& signal) {
        uint64_t seen = 0;
        for (;;) {
          try {
            std::vector<std::shared_ptr<recovery_refresh_state>> work, retired;
            bool stopped;
            {
                std::unique_lock<std::mutex> lock(signal->mutex);
                signal->changed.wait_for(lock, std::chrono::seconds(1), [&] { return signal->stopped || seen != signal->revision; });
                stopped = signal->stopped;
                seen = signal->revision;
                // Registration never holds an owner-state lock while acquiring
                // this registry lock. No SQL or callback destructor under either.
                for (auto i = signal->owners.begin(); i != signal->owners.end();) {
                    auto& shared = i->second;
                    std::lock_guard<std::mutex> state_lock(shared->mutex);
                    if (stopped || !shared->guard->alive.load() ||
                        (!shared->automatic_enabled && !shared->automatic_enrolling &&
                         !shared->in_flight && !shared->managed_registrations && !shared->interested())) {
                        retired.push_back(shared); i = signal->owners.erase(i);
                    } else { work.push_back(shared); ++i; }
                }
            }
            for (const auto& shared : retired) {
                std::shared_ptr<database> reader;
                { std::lock_guard<std::mutex> lock(shared->mutex); reader.swap(shared->probe_reader); }
                // A cancelled/closed owner's private reader is released here,
                // not by a queued callback on the owner scheduler.
                // Closed owners can be pruned even if a custom scheduler keeps
                // a queued delivery forever: that job contains no SQLite view.
            }
            if (stopped) return;
            for (const auto& shared : work) recovery_refresh_access::process(shared);
          } catch (...) {
            // A snapshot allocation failure must not permanently kill the
            // one process worker. Keep retained entries for a bounded retry.
            std::unique_lock<std::mutex> lock(signal->mutex);
            signal->changed.wait_for(lock, std::chrono::seconds(1));
          }
        }
    }
};
#endif


namespace {
struct enrollment_failure { bool armed = true; };
thread_local std::weak_ptr<enrollment_failure> next_enrollment_failure;
void increment(uint64_t& value) noexcept { if (value != UINT64_MAX) ++value; }
}

bool recovery_refresh_access::enable_automatic_maintenance(lattice_db& owner) {
    ordinary_require_creator(owner.config_.ordinary_context);
    owner.require_catalog_callback_absent();
    if (owner.closed_.load(std::memory_order_seq_cst)) throw db_error("automatic maintenance owner closed");
    if (owner.config_.is_in_memory() || owner.config_.read_only) return false;
#ifdef __EMSCRIPTEN__
    throw db_error("automatic maintenance requires a native process worker");
#else
    // Existing enabled originals must not allocate a discarded state or
    // acquire a new startup failure on an idempotent cached-alias call.
    auto shared = state(owner, false);
    if (!shared) shared = state(owner, true); // state() rechecks under ownership.
    if (!shared) throw db_error("automatic maintenance owner closed");
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->automatic_enabled) return true;
        if (shared->manual || shared->automatic_enrolling)
            throw db_error("automatic maintenance registration unavailable");
        shared->automatic_enrolling = true; // Pins registry admission, no SQL interest yet.
    }
    try {
        if (auto fault = next_enrollment_failure.lock(); fault && fault->armed) {
            fault->armed = false;
            throw std::bad_alloc();
        }
        // Construct/start the real worker and admit this state before success.
        recovery_refresh_worker::instance().add(shared);
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (!shared->guard->alive.load(std::memory_order_seq_cst))
                throw db_error("automatic maintenance owner closed during registration");
            shared->automatic_enabled = true;
            shared->automatic_enrolling = false;
        }
        signal(shared);
        return true;
    } catch (...) {
        std::shared_ptr<recovery_refresh_signal> wake;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->automatic_enrolling = false;
            wake = shared->wake.lock();
        }
        // Never remove/reset existing witness registrations, acknowledgements,
        // or interest. A now-uninterested entry is pruned by the actual worker.
        if (wake) wake->notify();
        throw;
    }
#endif
}

void recovery_refresh_access::maintain(const std::shared_ptr<recovery_refresh_state>& shared) noexcept {
#ifndef __EMSCRIPTEN__
    try {
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            const auto now = std::chrono::steady_clock::now();
            if (!shared->automatic_enabled || shared->automatic_running || shared->automatic_pauses ||
                now < shared->automatic_next_poll) return;
            shared->automatic_running = true;
            shared->automatic_next_poll = now + std::chrono::seconds(1);
            increment(shared->automatic_attempts);
        }
        struct turn {
            std::shared_ptr<recovery_refresh_state> shared;
            ~turn() {
                std::lock_guard<std::mutex> lock(shared->mutex);
                shared->automatic_running = false;
            }
        } ticket{shared};
        owner_hold hold(shared->guard);
        if (!hold.admitted) return;
        auto outcome = maintenance_result::deferred;
        try { outcome = maintain_owner(*shared->owner); } catch (...) { /* Retain all obligations. */ }
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (outcome == maintenance_result::quiet) increment(shared->automatic_quiet);
        else if (outcome == maintenance_result::completed) increment(shared->automatic_completed);
        else increment(shared->automatic_deferred);
    } catch (...) { /* No exception can terminate the one process worker. */ }
#else
    (void)shared;
#endif
}

bool recovery_refresh_access::retire_due_generations(lattice_db& owner, bool pressure, bool& work) {
    std::vector<std::shared_ptr<lattice_db::read_generation>> due;
    {
        std::unique_lock<std::mutex> lock(owner.read_pool_mutex_, std::try_to_lock);
        if (!lock.owns_lock()) return false;
        const auto now = std::chrono::steady_clock::now();
        const auto now_ms = lattice_db::steady_ms_now();
        const auto ttl = owner.read_generation_ttl_ms_.load(std::memory_order_relaxed);
        const auto max_age = owner.read_generation_max_age_ms_.load(std::memory_order_relaxed);
        for (const auto& gen : owner.live_generations_) {
            const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - gen->pinned_at).count();
            const bool idle = gen->in_flight.load(std::memory_order_acquire) == 0;
            const bool ttl_due = ttl > 0 && idle && now_ms - gen->last_read_steady_ms.load() >= ttl;
            if (pressure || gen->retiring.load() || ttl_due || (max_age > 0 && age >= max_age)) {
                due.push_back(gen); // Allocate BEFORE changing actual retirement state.
            }
        }
        for (const auto& gen : due) gen->retiring.store(true, std::memory_order_release);
    }
    bool ended_all = true;
    for (const auto& gen : due) {
        work = true;
        const auto conn = gen->conn;
        if (!conn) { ended_all = false; continue; }
        // Keep the generation charged and visible while its exact original
        // remains in a transaction. A failed COMMIT is never a pool/refund.
        if (gen->in_flight.load(std::memory_order_acquire)) conn->interrupt();
        auto* h = conn->internal_handle();
        if (!h) { ended_all = false; continue; }
        statement_guard native(*conn);
        if (!native.owned) { ended_all = false; continue; }
        conn->require_creator_();
        conn->require_catalog_clean_locked_();
        if (conn->is_closed() || conn->channel_reset_unsettled_.load(std::memory_order_acquire) ||
            sqlite3_txn_state(h, nullptr) == SQLITE_TXN_WRITE) { ended_all = false; continue; }
        bool busy = false;
        for (auto* stmt = sqlite3_next_stmt(h, nullptr); stmt; stmt = sqlite3_next_stmt(h, stmt))
            if (sqlite3_stmt_busy(stmt)) { busy = true; break; }
        if (busy) { ended_all = false; continue; }
        if (sqlite3_get_autocommit(h) == 0) {
            database::record_statement();
            // Private read keeper only, under uninterrupted try-admitted
            // SQLite custody. The existing observed native path preserves
            // statement/finalize errors without a topology wait under Native.
            const int rc = conn->execute_observed_sql_("COMMIT");
            if (rc != SQLITE_OK || sqlite3_get_autocommit(h) == 0) { ended_all = false; continue; }
        }
        // The actual known ending precedes removal. A bookkeeping race keeps
        // it conservatively counted for a later turn; never restart its txn.
        std::unique_lock<std::mutex> lock(owner.read_pool_mutex_, std::try_to_lock);
        if (!lock.owns_lock()) { ended_all = false; continue; }
        const auto found = std::find(owner.live_generations_.begin(), owner.live_generations_.end(), gen);
        if (found == owner.live_generations_.end()) continue; // Another genuine retire path owns its tail.
        if (!owner.closed_.load() && gen->in_flight.load(std::memory_order_acquire) == 0 &&
            owner.idle_read_pool_.size() + owner.live_generations_.size() - 1 < owner.read_pool_capacity_ &&
            std::find(owner.idle_read_pool_.begin(), owner.idle_read_pool_.end(), conn) == owner.idle_read_pool_.end())
            owner.idle_read_pool_.push_back(conn); // Allocation failure leaves it counted.
        owner.live_generations_.erase(found);
    }
    return ended_all;
}

recovery_refresh_access::maintenance_result recovery_refresh_access::maintain_owner(lattice_db& owner) {
    ordinary_require_creator(owner.config_.ordinary_context);
    bool pending = false;
    instance_registry::instance().for_each_alive(owner.config_.path, [&](lattice_db* inst) {
        pending = pending || inst->wal_eviction_pending_.load(std::memory_order_acquire);
    });
    bool work = false, readers_ended = true;
    if (!pending) {
        // No SQL, writer-mutex inspection, connection mint or projection epoch
        // change on the quiet path. Only an actually due keeper may do work.
        readers_ended = retire_due_generations(owner, false, work);
        if (!work) return readers_ended ? maintenance_result::quiet : maintenance_result::deferred;
        return readers_ended ? maintenance_result::completed : maintenance_result::deferred;
    }
    std::vector<std::pair<std::shared_ptr<projection_pressure_source>, uint64_t>> pressure;
    instance_registry::instance().for_each_alive(owner.config_.path, [&](lattice_db* inst) {
        for (const auto& source : inst->projection_pressure_sources())
            if (source->pending()) pressure.emplace_back(source, source->raised.load(std::memory_order_acquire));
        if (!retire_due_generations(*inst, true, work)) readers_ended = false;
    });
    for (const auto& [source, _] : pressure) retire_projection_store(source->identity);
    for (const auto& [source, _] : pressure)
        if (projection_store_readers(source->identity) != 0) readers_ended = false;
    instance_registry::instance().for_each_alive(owner.config_.path, [&](lattice_db* inst) {
        std::unique_lock<std::mutex> lock(inst->read_pool_mutex_, std::try_to_lock);
        if (!lock.owns_lock() || !inst->live_generations_.empty()) readers_ended = false;
    });
    if (!readers_ended) return maintenance_result::deferred;
    std::shared_ptr<database> writer;
    {
        std::unique_lock<std::mutex> lock(owner.connection_ownership_mutex_, std::try_to_lock);
        if (!lock.owns_lock() || owner.closed_.load() || owner.config_.read_only) return maintenance_result::deferred;
        writer = owner.db_;
    }
    if (!writer) return maintenance_result::deferred;
    // Unlike a preflight probe, this scope retains the successful Native try
    // admission through policy mutation, checkpoint, and restoration. A busy
    // user's Native mutex or transaction defers without blocking other owners.
    database::maintenance_scope::admission admission;
    database::maintenance_scope native(*writer, &admission);
    if (admission != database::maintenance_scope::admission::ready) return maintenance_result::deferred;
    const auto result = writer->wal_checkpoint(true, 250);
    if (result.rc != SQLITE_OK || result.busy != 0) return maintenance_result::deferred;
    for (const auto& [source, through] : pressure) source->acknowledge(through);
    instance_registry::instance().for_each_alive(owner.config_.path, [](lattice_db* inst) {
        inst->wal_eviction_pending_.store(false, std::memory_order_seq_cst);
        for (const auto& source : inst->projection_pressure_sources())
            if (source->pending()) inst->wal_eviction_pending_.store(true, std::memory_order_seq_cst);
    });
    return maintenance_result::completed;
}

std::shared_ptr<void> recovery_refresh_test_access::pause_automatic_maintenance(lattice_db& owner) {
    owner.require_catalog_callback_absent();
    auto shared = recovery_refresh_access::state(owner, false);
    if (!shared) throw db_error("automatic pause requires enrolled live owner");
    struct pause {
        std::shared_ptr<recovery_refresh_state> shared;
        bool admitted = false;
        ~pause() {
            if (!admitted) return;
            std::shared_ptr<recovery_refresh_signal> wake;
            { std::lock_guard<std::mutex> lock(shared->mutex); --shared->automatic_pauses; wake = shared->wake.lock(); }
#ifndef __EMSCRIPTEN__
            if (wake) wake->notify();
#endif
        }
    };
    auto token = std::make_shared<pause>(); token->shared = shared;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (!shared->guard->alive.load() || !shared->automatic_enabled || shared->automatic_enrolling)
            throw db_error("automatic pause requires enrolled live owner");
        if (shared->automatic_running) return {};
        if (shared->automatic_pauses == SIZE_MAX) throw db_error("automatic pause count exhausted");
        ++shared->automatic_pauses; token->admitted = true;
    }
    return token;
}
automatic_maintenance_snapshot recovery_refresh_test_access::automatic_snapshot(lattice_db& owner) {
    owner.require_catalog_callback_absent();
    auto shared = recovery_refresh_access::state(owner, false);
    if (!shared) return {};
    std::lock_guard<std::mutex> lock(shared->mutex);
    return {shared->automatic_enabled, shared->automatic_running, shared->automatic_pauses,
        shared->automatic_attempts, shared->automatic_quiet, shared->automatic_deferred, shared->automatic_completed};
}
std::shared_ptr<void> recovery_refresh_test_access::fail_next_automatic_enrollment() {
    if (auto prior = next_enrollment_failure.lock(); prior && prior->armed)
        throw db_error("automatic enrollment fault already armed");
    auto token = std::make_shared<enrollment_failure>();
    next_enrollment_failure = token;
    return token;
}
std::shared_ptr<database> recovery_refresh_access::test_keeper(lattice_db& owner, uint64_t id) {
    owner.require_catalog_callback_absent();
    ordinary_require_creator(owner.config_.ordinary_context);
    std::lock_guard<std::mutex> lock(owner.read_pool_mutex_);
    for (const auto& gen : owner.live_generations_) if (gen->id == id) return gen->conn;
    return {};
}
std::shared_ptr<database> recovery_refresh_test_access::actual_read_keeper(lattice_db& owner, uint64_t id) {
    return recovery_refresh_access::test_keeper(owner, id);
}

registry_depth_failure_observation recovery_refresh_access::test_registry_depth_failure(lattice_db& owner, bool creator_bound) {
    owner.require_catalog_callback_absent();
    ordinary_require_creator(owner.config_.ordinary_context);
    auto& registry = instance_registry::instance();
    if (registry.test_fail_next_depth_publication_) throw db_error("registry exception mechanism already armed");
    registry_depth_failure_observation result;
    result.holds_before = owner.guard_->notify_refcount.load(std::memory_order_seq_cst);
    struct disarm { bool& armed; ~disarm() { armed = false; } } reset{registry.test_fail_next_depth_publication_};
    registry.test_fail_next_depth_publication_ = true;
    try {
        auto visit = [&](lattice_db*) { result.callback_entered = true; };
        if (creator_bound) registry.for_each_alive_creator_bound(owner.config_.path, visit);
        else registry.for_each_alive(owner.config_.path, visit);
    } catch (const std::bad_alloc&) { result.caught_bad_alloc = true; }
    result.depth_publication_reached = !registry.test_fail_next_depth_publication_;
    result.holds_after = owner.guard_->notify_refcount.load(std::memory_order_seq_cst);
    return result;
}
registry_depth_failure_observation recovery_refresh_test_access::registry_depth_failure(lattice_db& owner, bool creator_bound) {
    return recovery_refresh_access::test_registry_depth_failure(owner, creator_bound);
}

std::shared_ptr<recovery_refresh_state> recovery_refresh_access::state(lattice_db& owner, bool create) {
    ordinary_require_creator(owner.config_.ordinary_context);
    ordinary_before_open(owner.config_.path,owner.config_.ordinary_context);
    auto fresh = create ? std::make_shared<recovery_refresh_state>() : nullptr;
    std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
    if (owner.closed_.load()) return {};
    if (!owner.recovery_refresh_ && fresh) {
        fresh->owner = &owner; fresh->guard = owner.guard_; fresh->delivery_scheduler = owner.scheduler_;
        owner.recovery_refresh_ = std::move(fresh);
    }
    return owner.recovery_refresh_;
}
uint64_t recovery_refresh_access::subscribe(lattice_db& owner, std::function<void()> callback) {
    if (!callback) throw db_error("recovery refresh callback required");
    auto item = std::make_shared<listener>(std::move(callback));
    auto shared = state(owner, true);
    if (!shared) throw db_error("recovery refresh owner closed");
    uint64_t id;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->revision == UINT64_MAX || shared->next_id == UINT64_MAX)
            throw db_error("recovery refresh subscription counter exhausted");
        id = shared->next_id++;
        shared->listeners.emplace(id, item);
        ++shared->revision;
    }
#ifndef __EMSCRIPTEN__
    try {
        bool manual;
        { std::lock_guard<std::mutex> lock(shared->mutex); manual = shared->manual; }
        if (!manual && !owner.config_.is_in_memory()) recovery_refresh_worker::instance().add(shared);
    } catch (...) { unsubscribe(owner, id); throw; }
#endif
    request(owner);
    return id;
}
void recovery_refresh_access::unsubscribe(lattice_db& owner, uint64_t id) {
    // Removal remains available after logical close. The caller still owns the
    // facade; queued work uses only the separate guard/state admission fence.
    std::shared_ptr<recovery_refresh_state> shared;
    { std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_); shared = owner.recovery_refresh_; }
    if (!shared) return;
    std::shared_ptr<listener> removed;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        const auto found = shared->listeners.find(id);
        if (found == shared->listeners.end()) return;
        removed.swap(found->second);
        removed->active.store(false, std::memory_order_release);
        shared->listeners.erase(found);
    }
}
void recovery_refresh_access::subscriptions_changed(lattice_db& owner) {
    auto shared = state(owner, false);
    if (!shared) return; // ordinary-only observers incur no witness reads
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->revision == UINT64_MAX) throw db_error("recovery refresh subscription counter exhausted");
        ++shared->revision;
    }
    request(owner);
}
void recovery_refresh_access::request(lattice_db& owner) noexcept {
    if(!ordinary_creator_current({}))return;
#ifndef __EMSCRIPTEN__
    try {
        auto shared = state(owner, false);
        if (!shared || owner.config_.is_in_memory()) return;
        signal(shared);
    } catch (...) { /* Periodic retry remains authoritative, not this hint. */ }
#else
    (void)owner;
#endif
}
void recovery_refresh_access::signal(const std::shared_ptr<recovery_refresh_state>& shared) noexcept {
    if(!ordinary_creator_current({}))return;
#ifndef __EMSCRIPTEN__
    try {
        bool manual;
        std::shared_ptr<recovery_refresh_signal> wake;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->requested = true; manual = shared->manual; wake = shared->wake.lock();
        }
        if (manual) process(shared);
        else if (wake) wake->notify();
    } catch (...) {} // Periodic retry remains authoritative.
#else
    (void)shared;
#endif
}
notification_token recovery_refresh_access::observe_managed(lattice_db& owner, const std::string& table,
    int64_t row, const std::string& global_id, std::vector<std::string> fallback,
    managed_observation_state::callback callback) {
#ifndef __EMSCRIPTEN__
    if (!owner.config_.is_in_memory() && callback) {
        auto shared = state(owner, true);
        if (!shared) throw db_error("recovery refresh owner closed");
        const auto weak = std::weak_ptr<recovery_refresh_state>(shared);
        auto hook = std::make_shared<const managed_observation_state::interest_callback>(
            [weak](uint64_t revision, bool active) noexcept {
                if (auto shared = weak.lock()) {
                    {
                        std::lock_guard<std::mutex> lock(shared->mutex);
                        if (revision < shared->managed_revision) return;
                        shared->managed_revision = revision; shared->managed_active = active;
                    }
                    signal(shared);
                }
            });
        bool manual;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (shared->managed_registrations == SIZE_MAX) throw db_error("recovery registrations exhausted");
            ++shared->managed_registrations;
            if (!shared->managed_hook) shared->managed_hook = hook;
            hook = shared->managed_hook; manual = shared->manual;
        }
        struct registration {
            std::shared_ptr<recovery_refresh_state> shared;
            ~registration() {
                std::shared_ptr<recovery_refresh_signal> wake;
                { std::lock_guard<std::mutex> lock(shared->mutex);
                  --shared->managed_registrations; wake = shared->wake.lock(); }
                if (wake) wake->notify();
            }
        } pin{shared};
        // Pin before registry admission: its prune pass cannot remove a new
        // registration between worker admission and the typed slot insertion.
        owner.managed_observers_.set_interest_observer(hook);
        if (!manual) recovery_refresh_worker::instance().add(shared);
        return owner.managed_observers_.observe(table, row, global_id, std::move(fallback), std::move(callback));
    }
#endif
    return owner.managed_observers_.observe(table, row, global_id, std::move(fallback), std::move(callback));
}
void recovery_refresh_test_access::use_manual_preparation(lattice_db& owner) {
    auto shared = recovery_refresh_access::state(owner, true);
    if (!shared) throw db_error("manual recovery preparation requires live owner");
    std::lock_guard<std::mutex> lock(shared->mutex);
    if (shared->automatic_enabled || shared->automatic_enrolling || shared->interested() ||
        shared->managed_registrations || shared->in_flight || !shared->wake.expired())
        throw db_error("configure manual preparation before recovery subscription");
    shared->manual = true;
}
void recovery_refresh_access::process(const std::shared_ptr<recovery_refresh_state>& shared) noexcept {
    if(!ordinary_creator_current({}))return;
    maintain(shared); // Separate admission/clock, before witness in_flight checks.
#ifndef __EMSCRIPTEN__
    try {
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            const auto now = std::chrono::steady_clock::now();
            if (shared->in_flight || !shared->interested() || (!shared->requested && now < shared->next_poll)) return;
            shared->in_flight = true; shared->requested = false;
            shared->next_poll = now + std::chrono::seconds(1);
        }
        struct ticket {
            std::shared_ptr<recovery_refresh_state> shared;
            explicit ticket(std::shared_ptr<recovery_refresh_state> s) : shared(std::move(s)) {}
            ~ticket() {
                std::shared_ptr<recovery_refresh_signal> wake;
                { std::lock_guard<std::mutex> lock(shared->mutex); shared->in_flight = false; wake = shared->wake.lock(); }
                if (wake) wake->notify();
            }
        };
        std::shared_ptr<ticket> work;
        try { work = std::make_shared<ticket>(shared); }
        catch (...) { std::lock_guard<std::mutex> lock(shared->mutex); shared->in_flight = false; throw; }
        std::optional<recovery_refresh_prepared> prepared;
        {
            owner_hold hold(shared->guard);
            if (!hold.admitted) return;
            prepared = prepare(*shared->owner, shared);
        }
        if (!prepared) return;
        auto scheduler = shared->delivery_scheduler.lock();
        if (!scheduler) return;
        scheduler->invoke([shared, work, prepared = std::move(*prepared)] {
            try {
                owner_hold hold(shared->guard);
                if (hold.admitted) deliver(*shared->owner, shared, prepared);
            } catch (...) { /* Leave acknowledgement pending for the next poll. */ }
        });
    } catch (...) { /* Discarded/failed work never acknowledges. */ }
#else
    (void)shared;
#endif
}

std::optional<recovery_refresh_prepared> recovery_refresh_access::prepare(lattice_db& owner, const std::shared_ptr<recovery_refresh_state>& shared) {
    ordinary_require_creator(owner.config_.ordinary_context);
    ordinary_before_open(owner.config_.path,owner.config_.ordinary_context);
    uint64_t subscription_revision, connection_revision, managed_revision;
    std::optional<recovery_witness> acknowledged;
    std::shared_ptr<database> probe, writer;
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (!shared->interested()) return std::nullopt;
        subscription_revision = shared->revision;
        managed_revision = shared->managed_revision;
        acknowledged = shared->acknowledged;
        probe = shared->probe_reader;
    }
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load()) return std::nullopt;
        connection_revision = owner.connection_revision_;
        writer = owner.db_;
    }
    if (!writer) return std::nullopt;
    if (!probe) {
        owner.note_history_uncaptured_connection(); // Before this reader exists off the published connection fields.
        probe = database::make_owned_connection(owner.config_.path, database::open_mode::read_only, 0 /* background probe retries instead of sleeping in SQLite busy handling */, std::shared_ptr<database_read_control>{}, owner.config_.ordinary_context,owner.connection_ledger_);
        owner.bind_observer_delivery_read_(*probe);
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->probe_reader = probe;
    }
    probe->execute("BEGIN");
    bool reading = true;
    std::optional<recovery_witness> witness;
    field_map names;
    try {
        witness = read_recovery_witness(*probe);
        bool same;
        { std::lock_guard<std::mutex> lock(shared->mutex);
          if (!witness && acknowledged) {
              // Initial legacy absence is inert. Losing a previously observed
              // witness is not evidence that content stayed unchanged. Keep
              // the old acknowledgement and require refresh after restoration,
              // even if the restored tuple happens to equal the old one.
              shared->witness_was_missing = true;
              throw db_error("previously observed recovery witness disappeared");
          }
          same = !shared->witness_was_missing && witness == acknowledged &&
              shared->acknowledged_revision == subscription_revision && shared->acknowledged_managed_revision == managed_revision; }
        if (witness && !same) {
            std::set<std::string> tables;
            { std::lock_guard<std::mutex> lock(owner.object_observers_mutex_);
              for (const auto& [table, _] : owner.object_observers_) tables.insert(table); }
            const auto typed = owner.managed_observers_.observed_tables();
            tables.insert(typed.begin(), typed.end());
            names = fields_for(*probe, tables);
        }
        probe->execute("COMMIT"); reading = false;
        if (same || !witness) {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->acknowledged = witness; shared->acknowledged_revision = subscription_revision;
            shared->acknowledged_managed_revision = managed_revision;
            return std::nullopt;
        }
    } catch (...) {
        if (reading) {
            try { probe->execute("ROLLBACK"); }
            catch (...) {
                // A failed private reader cleanup must not retain an old view
                // or make every subsequent BEGIN fail. Retire it off-lock.
                std::shared_ptr<database> retired;
                { std::lock_guard<std::mutex> lock(shared->mutex);
                  if (shared->probe_reader == probe) retired.swap(shared->probe_reader); }
            }
        }
        throw;
    }
    // Absent/unchanged witness polls above use only the private reader. Even
    // a momentary writer-mutex inspection would make idle foreground install
    // admission spuriously refuse. Actual refresh still validates its writer,
    // explicit transaction/statement snapshot and physical file before publish.
    std::shared_ptr<const physical_store_identity> writer_identity;
    if (!settled(*writer, writer->internal_handle(), &writer_identity)) return std::nullopt;
    const auto probe_identity = probe->physical_identity("main", {}, true);
    if (!writer_identity || !probe_identity || !(*probe_identity == *writer_identity))
        throw db_error("recovery refresh physical store changed");
    owner.note_history_uncaptured_connection(); // Retired/replacement readers require a future per-open ledger.
    auto fresh = database::make_owned_connection(owner.config_.path, database::open_mode::read_only, 0 /* background probe retries instead of sleeping in SQLite busy handling */, std::shared_ptr<database_read_control>{}, owner.config_.ordinary_context,owner.connection_ledger_);
    owner.bind_observer_delivery_read_(*fresh);
    const auto fresh_identity = fresh->physical_identity("main", {}, true);
    if (!fresh_identity || !(*fresh_identity == *writer_identity))
        throw db_error("recovery refresh reader physical store changed");
    {
        // Same non-waiting topology/publication discipline as reopen_read_db.
        std::unique_lock<std::mutex> topology(owner.attach_mutex_, std::try_to_lock);
        if (!topology.owns_lock()) return std::nullopt;
        owner.restore_attached_views(*fresh);
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load() || owner.connection_revision_ != connection_revision || owner.db_ != writer) return std::nullopt;
        connection_revision = ++owner.connection_revision_;
        fresh.swap(owner.read_db_); // old borrowers retain their own old view
    }
    // Release the old reader outside locks. No retired-reader list is retained.
    fresh.reset();
    if (!settled(*writer, writer->internal_handle())) return std::nullopt;

    return recovery_refresh_prepared{subscription_revision, connection_revision, managed_revision, std::move(witness), std::move(names)};
}
bool recovery_refresh_test_access::prepare_once(lattice_db& owner, size_t& writer_inspections) {
    auto shared = recovery_refresh_access::state(owner, false);
    if (!shared) throw db_error("deterministic recovery preparation needs subscribed state");
    { std::lock_guard<std::mutex> lock(shared->mutex);
      if (!shared->manual || shared->in_flight) throw db_error("deterministic recovery preparation needs idle manual state"); }
    writer_inspections = 0;
    struct count_scope {
        size_t* previous = preparation_writer_inspections;
        explicit count_scope(size_t& count) { preparation_writer_inspections = &count; }
        ~count_scope() { preparation_writer_inspections = previous; }
    } count{writer_inspections};
    return recovery_refresh_access::prepare(owner, shared).has_value();
}
void recovery_refresh_access::deliver(lattice_db& owner, const std::shared_ptr<recovery_refresh_state>& shared,
                                     const recovery_refresh_prepared& prepared) {
    const auto& names = prepared.names;
    const auto& witness = prepared.witness;
    const auto subscription_revision = prepared.subscription_revision;
    std::shared_ptr<database> writer;
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load() || owner.connection_revision_ != prepared.connection_revision) return;
        writer = owner.db_;
    }
    // Nonwaiting admission only. All witness/schema SQL, reader creation,
    // topology restoration and retired-reader destruction ran on the worker.
    if (!writer || !settled(*writer, writer->internal_handle())) return;
    writer.reset();
    std::vector<lattice_db::invalidation_hook_detailed_fn> invalidations;
    std::vector<std::function<void()>> objects;
    std::vector<std::shared_ptr<listener>> listeners;
    auto guard = owner.guard_;
    {
        std::lock_guard<std::mutex> lock(owner.invalidation_hooks_mutex_);
        for (const auto& [_, fn] : owner.invalidation_hooks_) invalidations.push_back(fn);
    }
    {
        std::lock_guard<std::mutex> lock(owner.object_observers_mutex_);
        for (const auto& [table, rows] : owner.object_observers_) {
            const auto found = names.find(table);
            if (found == names.end()) continue; // later registration retains newer revision
            const auto fields = nlohmann::json(found->second).dump();
            for (const auto& [_, entries] : rows) for (const auto& [id, fn] : entries)
                objects.push_back([fn, fields] { fn(fields); });
        }
    }
    {
        std::lock_guard<std::mutex> lock(shared->mutex);
        for (const auto& [_, item] : shared->listeners) listeners.push_back(item);
    }
    for (const auto& [table, fields] : names)
        owner.managed_observers_.append(table, "UPDATE", 0, "", nlohmann::json(fields).dump(), objects);
    // No owner access after this point: a callback may release its last owner.
    // Coalescible payload-free refreshes can repeat after a failed callback.
    bool success = true;
    auto invoke = [&](auto&& fn) {
        if (!guard->alive.load(std::memory_order_seq_cst)) { success = false; return; }
        try { fn(); } catch (...) { success = false; }
    };
    for (const auto& fn : invalidations) invoke([&] { fn({}, lattice_db::invalidation_reason::recovery); });
    for (const auto& fn : objects) invoke(fn);
    for (const auto& item : listeners) if (item->active.load(std::memory_order_acquire)) invoke(item->callback);
    if (success) {
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->acknowledged = witness; shared->acknowledged_revision = subscription_revision;
        shared->acknowledged_managed_revision = prepared.managed_revision;
        shared->witness_was_missing = false;
    }
}
} // namespace lattice::detail

namespace lattice {
uint64_t lattice_db::add_recovery_refresh_observer(std::function<void()> callback) {
    return detail::recovery_refresh_access::subscribe(*this, std::move(callback));
}
void lattice_db::remove_recovery_refresh_observer(uint64_t token) { detail::recovery_refresh_access::unsubscribe(*this, token); }
void lattice_db::request_recovery_refresh() noexcept { detail::recovery_refresh_access::request(*this); }
bool lattice_db::enable_automatic_read_pool_maintenance() { return detail::recovery_refresh_access::enable_automatic_maintenance(*this); }
void lattice_db::recovery_subscriptions_changed() { detail::recovery_refresh_access::subscriptions_changed(*this); }
} // namespace lattice
