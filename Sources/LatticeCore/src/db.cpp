#include "lattice/db.hpp"
#include "lattice/log.hpp"
#include <sqlite-vec.h>
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <exception>
#include <memory>

namespace lattice {

namespace {
template<typename Cancelled>
int cancellable_busy_wait(int previous_calls, int budget_ms,
        std::chrono::steady_clock::time_point& deadline, Cancelled&& cancelled) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (previous_calls == 0) deadline = now + std::chrono::milliseconds(budget_ms);
    if (cancelled() || now >= deadline) return 0;
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    if (remaining < 1) return 0;
    // Short slices make cancellation observable while retaining the configured
    // complete wait budget for operations that are allowed to continue.
    sqlite3_sleep(static_cast<int>(remaining < 5 ? remaining : 5));
    return !cancelled() && std::chrono::steady_clock::now() < deadline;
}
}

struct database::bound_cancellation {
    std::shared_ptr<const std::atomic<bool>> token;
    int wait_budget_ms = 0;
    std::chrono::steady_clock::time_point wait_deadline{};
    static int busy(void* context, int previous_calls) noexcept {
        auto& binding = *static_cast<bound_cancellation*>(context);
        return cancellable_busy_wait(previous_calls, binding.wait_budget_ms,
            binding.wait_deadline, [&] { return binding.token->load(std::memory_order_acquire); });
    }
};

void database::bind_cancellation_token(std::shared_ptr<const std::atomic<bool>> cancelled) {
    if (!cancelled) throw db_error("cancellation binding requires a token");
    if (cancellable_operation::current_)
        throw db_error("cannot bind cancellation inside a cancellable operation");
    auto binding = std::make_unique<bound_cancellation>();
    binding->token = std::move(cancelled);
    auto* mutex = db_ ? sqlite3_db_mutex(db_) : nullptr;
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("cancellation binding requires a serialized connection");
#endif
    if (sqlite3_mutex_try(mutex) != SQLITE_OK)
        throw db_error("cancellation binding connection is busy");
    struct unlock { sqlite3_mutex* mutex; ~unlock() { sqlite3_mutex_leave(mutex); } } lock{mutex};
    if (!maintenance_scope::idle(*this))
        throw db_error("cancellation binding requires an idle connection");
    if (bound_cancellation_) throw db_error("connection cancellation is already bound");
    if (binding->token->load(std::memory_order_acquire))
        throw db_error("database operation cancelled");
    bound_cancellation_ = std::move(binding);
    configure_busy_timeout(busy_timeout_ms_);
}

database::cancellable_operation::cancellable_operation(
        database& owner, const std::atomic<bool>& cancelled)
    : owner_(owner), mutex_(owner.db_ ? sqlite3_db_mutex(owner.db_) : nullptr),
      cancelled_(cancelled) {
    if (current_) throw db_error("cancellable database operations cannot nest");
#ifndef __EMSCRIPTEN__
    if (!mutex_) throw db_error("cancellable operation requires a serialized connection");
#endif
    if (sqlite3_mutex_try(mutex_) != SQLITE_OK)
        throw db_error("cancellable operation connection is busy");
    if (!maintenance_scope::idle(owner_)) {
        sqlite3_mutex_leave(mutex_);
        throw db_error("cancellable operation requires an idle connection");
    }
    if (cancelled_.load(std::memory_order_acquire) || owner_.operation_cancelled()) {
        sqlite3_mutex_leave(mutex_);
        throw db_error("database operation cancelled");
    }
    current_ = this;
    set_wait_budget(owner_.busy_timeout_ms_);
}

database::cancellable_operation::~cancellable_operation() noexcept {
    // Moving this scope to another thread would unlock a mutex it does not own.
    if (current_ != this) std::terminate();
    current_ = nullptr;
    owner_.configure_busy_timeout(owner_.busy_timeout_ms_);
    sqlite3_mutex_leave(mutex_);
}

void database::cancellable_operation::set_wait_budget(int timeout_ms) noexcept {
    wait_budget_ms_ = timeout_ms > 0 ? timeout_ms : 0;
    sqlite3_busy_handler(owner_.db_, &cancellable_operation::busy, this);
}

int database::cancellable_operation::busy(void* context, int previous_calls) noexcept {
    auto& scope = *static_cast<cancellable_operation*>(context);
    return cancellable_busy_wait(previous_calls, scope.wait_budget_ms_,
        scope.wait_deadline_, [&] { return scope.owner_.operation_cancelled(); });
}

void database::configure_busy_timeout(int timeout_ms) noexcept {
    auto* operation = cancellable_operation::current_;
    if (operation && &operation->owner_ == this) {
        operation->set_wait_budget(timeout_ms);
    } else if (bound_cancellation_) {
        // SQLite invokes the busy callback while holding this connection's
        // recursive mutex. Protect its context update as well as installation;
        // sqlite3_busy_handler's own lock would otherwise come too late.
        auto* mutex = sqlite3_db_mutex(db_);
        sqlite3_mutex_enter(mutex);
        bound_cancellation_->wait_budget_ms = timeout_ms > 0 ? timeout_ms : 0;
        sqlite3_busy_handler(db_, &bound_cancellation::busy, bound_cancellation_.get());
        sqlite3_mutex_leave(mutex);
    } else {
        sqlite3_busy_timeout(db_, timeout_ms);
    }
}

bool database::operation_cancelled() const noexcept {
    const auto* operation = cancellable_operation::current_;
    return (operation && &operation->owner_ == this &&
            operation->cancelled_.load(std::memory_order_acquire)) ||
           (bound_cancellation_ && bound_cancellation_->token->load(std::memory_order_acquire));
}

database::statement_scope::statement_scope(database& db) noexcept
    : owner(db), mutex(sqlite3_db_mutex(db.db_)), previous(current) {
    sqlite3_mutex_enter(mutex);
    current = this;
}

database::statement_scope::~statement_scope() noexcept {
    sqlite3_finalize(statement);
    // Finalize may commit an implicit RETURNING write during exception unwind.
    // READ is allowed: an unrelated active SELECT does not uncommit that write.
    owner.seal_if_settled_locked();
    current = previous;
    sqlite3_mutex_leave(mutex);
}

bool database::statement_scope::active_for(sqlite3* connection) noexcept {
    for (auto* scope = current; scope; scope = scope->previous) {
        if (scope->owner.db_ == connection) return true;
    }
    return false;
}

// Process-global statement counter (see db.hpp::total_statement_count).
static std::atomic<uint64_t> g_statement_count{0};
// Thread-local twin: exact statement budgets for single-threaded read paths,
// immune to parallel test suites sharing the process.
static thread_local uint64_t t_statement_count = 0;

uint64_t database::total_statement_count() {
    return g_statement_count.load(std::memory_order_relaxed);
}

uint64_t database::thread_statement_count() {
    return t_statement_count;
}


database::database(const std::string& path, open_mode mode, int busy_timeout_ms,
                   std::shared_ptr<const std::atomic<bool>> cancellation)
    : database(path, mode, busy_timeout_ms, initialization_key(false), std::move(cancellation)) {}

std::shared_ptr<database> database::make_read_keeper(const std::string& path,
                                                   int busy_timeout_ms) {
    return std::make_shared<database>(path, open_mode::read_only, busy_timeout_ms,
                                     initialization_key(true));
}

database::database(const std::string& path, open_mode mode, int busy_timeout_ms,
                   initialization_key key,
                   std::shared_ptr<const std::atomic<bool>> cancellation)
    : path_(path), mode_(mode), busy_timeout_ms_(busy_timeout_ms) {
    if (cancellation && cancellation->load(std::memory_order_acquire))
        throw db_error("database operation cancelled");
    // Determine SQLite open flags based on mode
    int flags = SQLITE_OPEN_FULLMUTEX;  // Always use serialized threading mode
    int rc;

    if (mode == open_mode::read_only) {
        // WAL-aware read-only reader: a plain SQLITE_OPEN_READONLY connection
        // joins a concurrent writer's WAL and sees committed-but-not-yet-
        // checkpointed rows. Never immutable=1 — that ignores the -wal entirely,
        // which is wrong for any live, WAL-backed Lattice database. Modern SQLite
        // (>=3.22) reads a read-only WAL database even on read-only media by
        // falling back to a heap-memory wal-index, so this also covers bundled DBs.
        flags |= SQLITE_OPEN_READONLY;
        // URI handling is unconditional: SQLite only URI-parses names that
        // START with "file:" even when the flag is set, so plain paths are
        // unaffected — and ATTACH through this connection can then resolve
        // named-memory URIs ("file:<name>?mode=memory&cache=shared") instead
        // of creating a literal file of that name.
        flags |= SQLITE_OPEN_URI;
        rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    } else {
        flags |= SQLITE_OPEN_READWRITE;
        if (mode == open_mode::read_write) flags |= SQLITE_OPEN_CREATE;
        flags |= SQLITE_OPEN_URI;  // see read-only branch note
        rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    }
    // A throwing constructor never reaches ~database(). Keep ownership local
    // through every initialization statement, including failing PRAGMAs.
    std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)> opening(db_, sqlite3_close_v2);
    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        db_ = nullptr;
        LOG_ERROR("db", "Failed to open database: %s", error.c_str());
        throw db_error("Failed to open database: " + error);
    }

    // Statement-level busy timeout MUST be installed before ANY statement runs.
    // It used to be set after the open-time pragmas, so the very first
    // `PRAGMA journal_mode = WAL` had no busy handler — a concurrent open or
    // an in-flight writer on the same file made it throw
    // "database is locked" instantly instead of waiting. Instances now
    // genuinely close and reopen (weak instance cache), so open-time races
    // are common rather than exceptional.
    if (cancellation) bind_cancellation_token(std::move(cancellation));
    else configure_busy_timeout(busy_timeout_ms_);

    // Register virtual-table modules before any statement can load sqlite_schema.
    // SQLite classifies their shadow tables while loading the schema; registering
    // vec0 afterward can leave them classified as ordinary tables on this handle.
    // Storage writer guards must never be installed on virtual-table shadows.
    int vec_rc = sqlite3_vec_init(db_, nullptr, nullptr);
    if (vec_rc != SQLITE_OK) {
        db_ = nullptr;
        LOG_ERROR("db", "Failed to initialize sqlite-vec extension");
        throw db_error("Failed to initialize sqlite-vec extension");
    }

    // Even without an explicit checkpoint, SQLite can checkpoint the last WAL writer on close.
    // Strict existing opens must remain byte-preserving when later admission rejects the handle.
    if (mode == open_mode::read_write_existing) {
        const int configured = sqlite3_db_config(db_, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, nullptr);
        if (configured != SQLITE_OK) throw db_error("Cannot disable checkpoint-on-close for existing open");
    }

    // Enable foreign keys
    execute("PRAGMA foreign_keys = ON");

#ifdef __EMSCRIPTEN__
    // WASM/OPFS mode: Use DELETE journal mode (WAL requires mmap/shm which OPFS doesn't support)
    // Also skip mmap since OPFS uses SyncAccessHandle instead
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = DELETE");
    }
    execute(key.keeper_cache_ ? "PRAGMA cache_size = 2000"
                              : "PRAGMA cache_size = 50000");
    execute("PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#else
    // Native mode: Enable WAL mode for better concurrency (only on read-write connection)
    if (mode == open_mode::read_write) {
        enable_wal_mode();
    }

    // Performance optimizations (matching Lattice.swift)
    execute(key.keeper_cache_ ? "PRAGMA cache_size = 2000"
                              : "PRAGMA cache_size = 50000");
    execute("PRAGMA mmap_size = 300000000");    // Memory-mapped I/O (~300MB)
    execute("PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#endif

    // (busy timeout installed immediately after open, above — before the
    // journal-mode/cache pragmas, which are themselves subject to locking.)

    if (mode == open_mode::read_write) {
        // No ANALYZE here: it scans every index (O(GB) on large databases) and
        // takes the write lock at the worst possible moment — open. Stats are
        // refreshed incrementally via "PRAGMA optimize" (dtor + maintenance
        // paths); analysis_limit bounds the cost of any future stats scan.
        sqlite3_exec(db_, "PRAGMA analysis_limit=400", nullptr, nullptr, nullptr);
        // Bound the WAL file: successful TRUNCATE/RESTART checkpoints shrink
        // the -wal file back to this size instead of leaving it fully allocated.
        sqlite3_exec(db_, "PRAGMA journal_size_limit=268435456", nullptr, nullptr, nullptr);
        // Materialize the WAL index (-shm/-wal) with a no-op read transaction.
        // Read-only connections CANNOT create the -shm file — on a fresh
        // database they fail with "unable to open database file" unless a
        // writable connection has started a transaction first. (ANALYZE used
        // to do this as a side effect.)
        sqlite3_exec(db_, "SELECT count(*) FROM sqlite_master", nullptr, nullptr, nullptr);
    }

    // Apple's libsqlite3 compiles SQLITE_ENABLE_STMT_SCANSTATUS and ships the
    // runtime toggle ON, which makes every WhereBegin opcode run the
    // sqlite3WhereAddExplainText/sqlite3_str_appendf explain-text pass — the
    // exact top frames of the Aug 2026 Engram SIGBUS — and measurably fattens
    // every prepare. We never read scanstatus, so turn it off per connection.
    // Stock amalgamations without the feature don't define the constant (and
    // the call would be a no-op there anyway).
#ifdef SQLITE_DBCONFIG_STMT_SCANSTATUS
    sqlite3_db_config(db_, SQLITE_DBCONFIG_STMT_SCANSTATUS, 0, nullptr);
#endif

    opening.release();  // Fully initialized: ~database() now owns the connection.
}

void database::enable_wal_mode() {
    // Two fresh opens can both read the rollback-journal header before either
    // upgrades to WAL. SQLite may return BUSY without invoking its busy handler
    // to avoid a read-to-write lock deadlock. Retry only this idempotent bootstrap
    // PRAGMA, releasing each failed statement/read lock before waiting again.
    // One deadline includes time already spent inside SQLite's busy handler.
    const auto budget = std::chrono::milliseconds(busy_timeout_ms_ > 0 ? busy_timeout_ms_ : 0);
    const auto deadline = std::chrono::steady_clock::now() + budget;
    constexpr const char* sql = "PRAGMA journal_mode = WAL";
    try {
        for (;;) {
            if (operation_cancelled()) throw db_error("database operation cancelled");
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now()).count();
            configure_busy_timeout(remaining > 0 ? static_cast<int>(remaining) : 0);
            char* raw_error = nullptr;
            g_statement_count.fetch_add(1, std::memory_order_relaxed);
            ++t_statement_count;
            const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &raw_error);
            std::unique_ptr<char, decltype(&sqlite3_free)> error_owner(raw_error, sqlite3_free);
            const std::string error = raw_error ? raw_error : sqlite3_errmsg(db_);
            error_owner.reset();
            // sqlite3_exec has finalized its statement on both success/failure.
            // A cancelled constructor must not publish a connection even if the
            // journal transition happened just before cancellation arrived.
            if (operation_cancelled()) throw db_error("database operation cancelled");
            if (rc == SQLITE_OK) break;
            if ((rc & 0xff) != SQLITE_BUSY || std::chrono::steady_clock::now() >= deadline)
                throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
            sqlite3_sleep(1);
            // Zero/expired budgets never get another attempt. Preserve the
            // failed SQL's error rather than replacing it with a timeout label.
            if (operation_cancelled()) throw db_error("database operation cancelled");
            if (std::chrono::steady_clock::now() >= deadline)
                throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
        }
    } catch (...) {
        configure_busy_timeout(busy_timeout_ms_);
        throw;
    }
    configure_busy_timeout(busy_timeout_ms_);
}

database::~database() {
    if (db_) {
        if (mode_ == open_mode::read_write) {
            // The connection is closing — silence change/commit hooks first.
            // PRAGMA optimize below may write sqlite_stat rows; firing hooks
            // into an owner that is mid-destruction locks destroyed mutexes.
            sqlite3_update_hook(db_, nullptr, nullptr);
            sqlite3_wal_hook(db_, nullptr, nullptr);
            sqlite3_commit_hook(db_, nullptr, nullptr);
            sqlite3_rollback_hook(db_, nullptr, nullptr);
            // Best-effort incremental stats refresh (bounded by analysis_limit).
            // Only re-analyzes tables this connection queried whose stats are
            // missing or stale. Never throw from a destructor.
            int orc = sqlite3_exec(db_, "PRAGMA optimize", nullptr, nullptr, nullptr);
            if (orc != SQLITE_OK) {
                LOG_DEBUG("db", "~database optimize skipped: rc=%d, path=%s", orc, path_.c_str());
            }
            int nLog = 0, nCkpt = 0;
            int rc = sqlite3_wal_checkpoint_v2(db_, nullptr, SQLITE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
            LOG_DEBUG("db", "~database checkpoint: rc=%d, nLog=%d, nCkpt=%d, path=%s", rc, nLog, nCkpt, path_.c_str());
        }
        // A raw outstanding statement can defer sqlite3_close_v2's final
        // cleanup. It must not retain our soon-to-die callback context.
        sqlite3_busy_handler(db_, nullptr, nullptr);
        int rc = sqlite3_close_v2(db_);
        if (rc != SQLITE_OK) {
            LOG_ERROR("db", "~database close failed: rc=%d (%s), path=%s", rc, sqlite3_errmsg(db_), path_.c_str());
        } else {
            LOG_DEBUG("db", "~database closed: path=%s", path_.c_str());
        }
    }
}

void database::close() {
    // Logical close: ops short-circuit after this. The sqlite3* itself is freed in
    // ~database (single-threaded), so a concurrent reader holding this wrapper can
    // never deref a freed handle — it either sees closed_ and returns empty, or runs
    // a final query on the still-open connection. An already admitted private
    // maintenance scope likewise settles its complete transaction on its
    // owning thread; logical close never strands that transaction halfway.
    closed_.store(true, std::memory_order_release);
}

void database::require_main_file_unmoved(const std::string& expected_path) {
    if (expected_path.empty() || expected_path.size() > 4096 || expected_path.find('\0') != std::string::npos)
        throw db_error("Main file currentness expected path is invalid");
    if (!db_ || is_closed()) throw db_error("Main file connection is closed");
    // FULLMUTEX serializes access to SQLite's actual open-file/VFS state. Unlike
    // statement_scope, this probe must not seal/deliver unrelated transaction work.
    auto* mutex = sqlite3_db_mutex(db_);
    sqlite3_mutex_enter(mutex);
    struct unlock { sqlite3_mutex* value; ~unlock() { sqlite3_mutex_leave(value); } } held{mutex};
    if (is_closed()) throw db_error("Main file connection is closed");
    const char* filename = sqlite3_db_filename(db_, "main");
    if (!filename || !*filename) throw db_error("Main file currentness requires a file database");
    // Compare SQLite's opened filename, not the configured URL or a newly resolved alias.
    if (expected_path != filename) throw db_error("Main file currentness expected path does not match the open database");
#ifdef SQLITE_FCNTL_HAS_MOVED
    int moved = -1; // A successful opcode that fails to fill its output is not admission.
    const int result = sqlite3_file_control(db_, "main", SQLITE_FCNTL_HAS_MOVED, &moved);
    if (result != SQLITE_OK) throw db_error("Main file currentness is unsupported or failed (SQLite " + std::to_string(result) + ")");
    if (moved != 0) throw db_error("The open main file no longer matches its pathname");
#else
    throw db_error("Main file currentness is unsupported by SQLite");
#endif
    if (is_closed()) throw db_error("Main file connection closed during currentness check");
}

void database::set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back) {
    set_txn_hooks(std::move(settled), std::move(rolled_back), {});
}

void database::set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back,
                              std::function<void()> sealed) {
    on_txn_settled_ = std::move(settled);
    on_txn_rolled_back_ = std::move(rolled_back);
    on_txn_sealed_ = std::move(sealed);
    sqlite3_rollback_hook(db_, [](void* self_ptr) {
        auto* self = static_cast<database*>(self_ptr);
        self->txn_dirty_.store(false, std::memory_order_relaxed);
        // Committed rows may still await the outer query's unlock. Preserve
        // them even when a subsequent transaction rolls back on this handle.
        if (self->on_txn_rolled_back_) self->on_txn_rolled_back_();
    }, this);
}

void database::seal_if_settled_locked() noexcept {
    // Autocommit is also true during an unfinished INSERT ... RETURNING.
    // SQLite reports WRITE until that statement finalizes; a held SELECT may
    // instead keep READ after a separate writer has committed successfully.
    if (update_hook_scope::active_for(db_) || !db_ ||
        sqlite3_get_autocommit(db_) == 0 ||
        sqlite3_txn_state(db_, nullptr) == SQLITE_TXN_WRITE) return;
    if (!txn_dirty_.exchange(false, std::memory_order_relaxed)) return;
    if (on_txn_sealed_) on_txn_sealed_();
    committed_pending_.store(true, std::memory_order_relaxed);
}

void database::drain_if_settled() {
    if (!db_) return;
    auto* mutex = sqlite3_db_mutex(db_);
    sqlite3_mutex_enter(mutex);
    seal_if_settled_locked();
    const bool eligible = !statement_scope::active_for(db_) &&
                          !(cancellable_operation::current_ &&
                            &cancellable_operation::current_->owner_ == this) &&
        !update_hook_scope::active_for(db_) && !maintenance_scope::active_for(db_) &&
        sqlite3_get_autocommit(db_) != 0 && sqlite3_txn_state(db_, nullptr) != SQLITE_TXN_WRITE;
    const bool deliver = eligible && on_txn_settled_ &&
        committed_pending_.exchange(false, std::memory_order_relaxed);
    sqlite3_mutex_leave(mutex);
    if (deliver) on_txn_settled_();
}

void database::discard_if_rolled_back() {
    // SQLite's rollback hook is the authority for discarding the current
    // transaction. An error in a later read is not evidence that an earlier
    // committed write rolled back. Preserve and deliver sealed work.
    drain_if_settled();
}

database::database(database&& other) noexcept
    : db_(other.db_), path_(std::move(other.path_)), mode_(other.mode_),
      busy_timeout_ms_(other.busy_timeout_ms_),
      bound_cancellation_(std::move(other.bound_cancellation_)) {
    if (cancellable_operation::current_ &&
        &cancellable_operation::current_->owner_ == &other) std::terminate();
    other.db_ = nullptr;
}

database& database::operator=(database&& other) noexcept {
    if (this != &other) {
        if (cancellable_operation::current_ &&
            (&cancellable_operation::current_->owner_ == this ||
             &cancellable_operation::current_->owner_ == &other)) std::terminate();
        if (db_) {
            sqlite3_busy_handler(db_, nullptr, nullptr);
            sqlite3_close_v2(db_);
        }
        db_ = other.db_;
        path_ = std::move(other.path_);
        mode_ = other.mode_;
        busy_timeout_ms_ = other.busy_timeout_ms_;
        bound_cancellation_ = std::move(other.bound_cancellation_);
        other.db_ = nullptr;
    }
    return *this;
}

int64_t database::changes() const {
    if (closed_.load(std::memory_order_acquire) || !db_) return 0;
    return sqlite3_changes64(db_);
}

primary_key_t database::execute_statement(const char* sql,
        const std::vector<column_value_t>& params, const char** tail, bool returning_rowid,
        const char* prepare_error, const char* step_error) {
    primary_key_t rowid = 0;
    bool prepare_failed = false;
    std::string error;
    int rc = SQLITE_OK;
    try {
        statement_scope scope(*this);
        rc = sqlite3_prepare_v2(db_, sql, -1, &scope.statement, tail);
        prepare_failed = rc != SQLITE_OK;
        if (rc == SQLITE_OK && scope.statement) {
            int index = 1;
            for (const auto& value : params) bind_value(scope.statement, index++, value);
            bool first_row = true;
            while ((rc = sqlite3_step(scope.statement)) == SQLITE_ROW) {
                if (returning_rowid && first_row) rowid = sqlite3_column_int64(scope.statement, 0);
                first_row = false;
            }
            if (rc == SQLITE_DONE) {
                if (!returning_rowid) rowid = sqlite3_last_insert_rowid(db_);
            } else error = sqlite3_errmsg(db_);
        } else if (rc != SQLITE_OK) error = sqlite3_errmsg(db_);
    } catch (...) {
        const auto failure = std::current_exception();
        try { drain_if_settled(); } catch (...) {}
        std::rethrow_exception(failure);
    }
    if (rc != SQLITE_OK && rc != SQLITE_DONE) {
        // A SQL error is primary even if delivery of earlier committed work
        // also throws. Neither failure changes its durable commit outcome.
        try { drain_if_settled(); } catch (...) {}
        const char* prefix = prepare_failed ? prepare_error : step_error;
        if (prefix) throw db_error(std::string(prefix) + error);
        throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
    }
    drain_if_settled();
    return rowid;
}

void database::execute(const std::string& sql, const std::vector<column_value_t>& params) {
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_)) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (!params.empty()) {
        execute_statement(sql.c_str(), params, nullptr);
        return;
    }
    // Match sqlite3_exec's multi-statement behavior, but finalize and seal each
    // successful statement before the next can fail or roll back a later txn.
    // The outer scope keeps the complete batch owned and postpones delivery.
    try {
        statement_scope batch(*this);
        const char* next = sql.c_str();
        while (*next) {
            const char* tail = nullptr;
            execute_statement(next, {}, &tail);
            if (!tail || tail == next) break;
            next = tail;
        }
    } catch (...) {
        const auto failure = std::current_exception();
        try { drain_if_settled(); } catch (...) {}
        std::rethrow_exception(failure);
    }
    drain_if_settled();
}

bool database::table_exists(const std::string& name) const {
    const char* sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_exists statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare statement");
    }

    sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
    sqlite3_finalize(stmt);

    return exists;
}

std::unordered_map<std::string, std::string> database::get_table_info(const std::string& table) const {
    std::unordered_map<std::string, std::string> columns;

    std::string sql = "PRAGMA table_info(" + table + ")";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare table_info statement: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare table_info statement");
    }

    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const char* type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

        if (name && type) {
            // Normalize type to uppercase for comparison
            std::string type_str(type);
            for (char& c : type_str) {
                c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            }
            columns[name] = type_str;
        }
    }

    sqlite3_finalize(stmt);
    return columns;
}

void database::create_table(const table_schema& schema) {
    std::ostringstream sql;
    sql << "CREATE TABLE " << schema.name << " (";

    bool first = true;
    if (!schema.is_link_table) {
        // Regular tables get id and globalId
        sql << "id INTEGER PRIMARY KEY AUTOINCREMENT, ";
        sql << "globalId TEXT UNIQUE NOT NULL";
        first = false;
    }

    for (const auto& col : schema.columns) {
        // Skip id and globalId - they're already added above for non-link tables
        if (!schema.is_link_table && (col.name == "id" || col.name == "globalId")) {
            continue;
        }
        if (!first) sql << ", ";
        sql << col.name << " ";
        first = false;

        switch (col.type) {
            case column_type::integer: sql << "INTEGER"; break;
            case column_type::real: sql << "REAL"; break;
            case column_type::text: sql << "TEXT"; break;
            case column_type::blob: sql << "BLOB"; break;
        }

        if (!col.nullable) {
            sql << " NOT NULL";
        }
        if (col.is_unique) {
            sql << " UNIQUE";
        }
        if (col.foreign_key_table) {
            sql << " REFERENCES " << *col.foreign_key_table
                << "(" << col.foreign_key_column.value_or("id") << ")";
        }
    }

    sql << ")";
    execute(sql.str());
}

void database::ensure_table(const table_schema& schema) {
    if (!table_exists(schema.name)) {
        create_table(schema);
    }
}

void database::bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value) {
    std::visit([&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) {
            sqlite3_bind_null(stmt, index);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            sqlite3_bind_int64(stmt, index, v);
        } else if constexpr (std::is_same_v<T, double>) {
            sqlite3_bind_double(stmt, index, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            // Preserve every UTF-8 byte, including embedded NULs. Keep the
            // existing unchecked bind-return policy; error custody is separate.
            sqlite3_bind_text64(stmt, index, v.c_str(),
                                static_cast<sqlite3_uint64>(v.size()),
                                SQLITE_TRANSIENT, SQLITE_UTF8);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            if (v.empty()) {
                sqlite3_bind_zeroblob(stmt, index, 0);
            } else {
                sqlite3_bind_blob(stmt, index, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT);
            }
        }
    }, value);
}

column_value_t database::extract_column(sqlite3_stmt* stmt, int index) {
    int type = sqlite3_column_type(stmt, index);
    switch (type) {
        case SQLITE_INTEGER:
            return sqlite3_column_int64(stmt, index);
        case SQLITE_FLOAT:
            return sqlite3_column_double(stmt, index);
        case SQLITE_TEXT: {
            const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
            if (!text) return std::string{};  // Preserve conversion-failure fallback.
            const int size = sqlite3_column_bytes(stmt, index);
            return std::string(text, static_cast<size_t>(size));
        }
        case SQLITE_BLOB: {
            const void* data = sqlite3_column_blob(stmt, index);
            int size = sqlite3_column_bytes(stmt, index);
            const uint8_t* bytes = static_cast<const uint8_t*>(data);
            return std::vector<uint8_t>(bytes, bytes + size);
        }
        case SQLITE_NULL:
        default:
            return nullptr;
    }
}

primary_key_t database::insert(const std::string& table,
                               const std::vector<std::pair<std::string, column_value_t>>& values,
                               const std::vector<std::string>& conflict_columns) {
    if (closed_.load(std::memory_order_acquire)) return {};
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    std::ostringstream sql;
    sql << "INSERT INTO main." << table << " (";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col;
        first = false;
    }

    sql << ") VALUES (";
    first = true;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!first) sql << ", ";
        sql << "?";
        first = false;
    }
    sql << ")";

    // Add ON CONFLICT clause for upsert if conflict_columns provided
    if (!conflict_columns.empty()) {
        sql << " ON CONFLICT (";
        first = true;
        for (const auto& col : conflict_columns) {
            if (!first) sql << ", ";
            sql << col;
            first = false;
        }
        sql << ")";
        std::ostringstream set_clause;
        first = true;
        for (const auto& [col, _] : values) {
            // Skip conflict columns and globalId in UPDATE
            if (col == "globalId") continue;
            bool is_conflict = false;
            for (const auto& cc : conflict_columns) {
                if (cc == col) { is_conflict = true; break; }
            }
            if (is_conflict) continue;
            if (!first) set_clause << ", ";
            set_clause << col << " = excluded." << col;
            first = false;
        }
        if (first) {
            // No columns to update — every non-globalId column is part of the conflict key.
            sql << " DO NOTHING";
        } else {
            sql << " DO UPDATE SET " << set_clause.str();
        }
        // Truthful row identity for upserts. last_insert_rowid() is NOT
        // updated when the DO UPDATE path runs — it keeps the rowid of the
        // last unrelated INSERT on this connection, silently binding the
        // caller's object to the wrong row. RETURNING reports the rowid of
        // the row actually inserted OR updated; DO NOTHING returns no row,
        // which we surface as 0 so the caller can look the row up by its
        // conflict key.
        sql << " RETURNING rowid";
    }

    std::vector<column_value_t> params;
    params.reserve(values.size());
    for (const auto& entry : values) params.push_back(entry.second);
    return execute_statement(sql.str().c_str(), params, nullptr, !conflict_columns.empty(),
                             "Failed to prepare insert: ", "Insert failed: ");
}

void database::update(const std::string& table,
                       primary_key_t id,
                       const std::vector<std::pair<std::string, column_value_t>>& values) {
    if (closed_.load(std::memory_order_acquire)) return;
    if (values.empty()) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;

    std::ostringstream sql;
    sql << "UPDATE " << table << " SET ";

    bool first = true;
    for (const auto& [col, _] : values) {
        if (!first) sql << ", ";
        sql << col << " = ?";
        first = false;
    }

    sql << " WHERE id = ?";

    std::vector<column_value_t> params;
    params.reserve(values.size() + 1);
    for (const auto& entry : values) params.push_back(entry.second);
    params.push_back(id);
    execute_statement(sql.str().c_str(), params, nullptr, false,
                      "Failed to prepare update: ", "Update failed: ");
}

void database::remove(const std::string& table, primary_key_t id) {
    if (closed_.load(std::memory_order_acquire)) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    std::string sql = "DELETE FROM " + table + " WHERE id = ?";

    execute_statement(sql.c_str(), {id}, nullptr, false,
                      "Failed to prepare delete: ", "Delete failed: ");
}

std::vector<database::row_t> database::query(const std::string& sql,
                                             const std::vector<column_value_t>& params) {
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_)) return {};

    enum class failure { none, prepare, column_name, step } failed = failure::none;
    std::string error;
    std::vector<row_t> results;
    std::exception_ptr materialization_failure;
    try {
        statement_scope scope(*this);
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &scope.statement, nullptr);
        auto* stmt = scope.statement;
        if (rc != SQLITE_OK) {
            // Capture connection-local error state before finalize or unlock.
            error = sqlite3_errmsg(db_);
            failed = failure::prepare;
        } else {
            int index = 1;
            for (const auto& param : params) bind_value(stmt, index++, param);

            const int col_count = sqlite3_column_count(stmt);
            // Names are lazy SQLite allocations; copy once while this statement
            // owns the connection, preserving the existing explicit OOM check.
            std::vector<std::string> col_names;
            col_names.reserve(static_cast<size_t>(col_count));
            for (int i = 0; i < col_count; ++i) {
                const char* name = sqlite3_column_name(stmt, i);
                if (!name) {
                    failed = failure::column_name;
                    break;
                }
                col_names.emplace_back(name);
            }
            if (failed == failure::none) {
                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                    if (test_hook_query_row_) test_hook_query_row_(stmt);
                    row_t row;
                    for (int i = 0; i < col_count; ++i) {
                        row[col_names[static_cast<size_t>(i)]] = extract_column(stmt, i);
                    }
                    results.push_back(std::move(row));
                }
                if (rc != SQLITE_DONE) {
                    error = sqlite3_errmsg(db_);
                    failed = failure::step;
                }
            }
        }
        // scope finalizes on every exit, including C++ exceptions, then unlocks.
    } catch (...) {
        materialization_failure = std::current_exception();
    }

    if (materialization_failure) {
        // A nested autocommit write, or RETURNING finalized during unwinding,
        // may already have committed. Deliver its dirty state after unlock;
        // an extraction failure is not evidence that those writes rolled back.
        // An explicit transaction remains open for its owning caller to settle.
        try {
            drain_if_settled();
        } catch (...) {
            // Preserve the initiating C++ exception even if delivery/logging
            // also fails. Dirty-state consumption keeps the existing policy.
            try { LOG_ERROR("db", "Settled delivery also failed after query materialization failure"); }
            catch (...) {}
        }
        std::rethrow_exception(materialization_failure);
    }

    // Preserve committed nested work on every SQL failure. Rollback hooks
    // already discarded only the uncommitted transaction's buffer.
    if (failed != failure::none) {
        try { drain_if_settled(); } catch (...) {}
        if (failed == failure::prepare) throw db_error("Failed to prepare query: " + error);
        if (failed == failure::column_name) throw db_error("Query failed: out of memory reading column name");
        throw db_error("Query failed: " + error);
    }

    drain_if_settled();
    return results;
}

void database::refresh_wal_snapshot() {
    // A no-op SELECT forces SQLite to release the old WAL read snapshot
    // and acquire a fresh one on the next query.
    sqlite3_exec(db_, "SELECT 1", nullptr, nullptr, nullptr);
}

void database::interrupt() {
    if (db_) sqlite3_interrupt(db_);
}

database::checkpoint_result database::wal_checkpoint(bool truncate, int busy_budget_ms) {
    checkpoint_result result;
#ifdef __EMSCRIPTEN__
    // DELETE journal mode — there is no WAL to checkpoint.
    (void)truncate; (void)busy_budget_ms;
    return result;
#else
    if (closed_.load(std::memory_order_acquire) || mode_ != open_mode::read_write || !db_) {
        return result;
    }
    // PRAGMA (not the C API) so the (busy, log, checkpointed) row comes back
    // through the ordinary query path; same style as the Swift bridge's
    // checkpoint(). Bound the wait: TRUNCATE holds the writer lock while
    // waiting out readers, so a held snapshot must fail fast (retry next
    // cycle) rather than stall every writer behind it.
    configure_busy_timeout(truncate ? busy_budget_ms : 0);
    try {
        auto rows = query(truncate ? "PRAGMA wal_checkpoint(TRUNCATE)"
                                   : "PRAGMA wal_checkpoint(PASSIVE)");
        result.rc = SQLITE_OK;
        if (!rows.empty()) {
            auto get = [&](const char* key) -> int64_t {
                auto it = rows[0].find(key);
                if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    return std::get<int64_t>(it->second);
                }
                return -1;
            };
            result.busy = static_cast<int>(get("busy"));
            result.log_frames = get("log");
            result.checkpointed = get("checkpointed");
        }
    } catch (const std::exception& e) {
        result.rc = SQLITE_ERROR;
        LOG_DEBUG("db", "wal_checkpoint(%s) failed: %s, path=%s",
                  truncate ? "TRUNCATE" : "PASSIVE", e.what(), path_.c_str());
    }
    configure_busy_timeout(busy_timeout_ms_);  // restore statement-level timeout
    return result;
#endif
}

bool database::maintenance_scope::idle(database& db) noexcept {
    if (!db.db_ || db.closed_.load(std::memory_order_acquire) ||
        sqlite3_get_autocommit(db.db_) == 0 ||
        sqlite3_txn_state(db.db_, nullptr) != SQLITE_TXN_NONE ||
        update_hook_scope::active_for(db.db_)) return false;
    for (auto* statement = sqlite3_next_stmt(db.db_, nullptr);
         statement; statement = sqlite3_next_stmt(db.db_, statement)) {
        if (sqlite3_stmt_busy(statement)) return false;
    }
    return true;
}

void database::maintenance_scope::probe_before_store_gate(database& db) {
    auto* mutex = db.db_ ? sqlite3_db_mutex(db.db_) : nullptr;
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("audit maintenance requires a serialized connection");
#endif
    // A SQLite callback may already own this recursive mutex while another
    // writer owns the shared-memory gate and is waiting for SQLite. Reject
    // callback/statement reentry BEFORE waiting for that gate. This probe
    // acquires no gate and never waits for another SQLite thread.
    if (sqlite3_mutex_try(mutex) != SQLITE_OK)
        throw db_error("audit maintenance connection is busy");
    const bool available = idle(db);
    sqlite3_mutex_leave(mutex);
    if (!available) throw db_error("audit maintenance requires an idle connection");
}

database::maintenance_scope::maintenance_scope(database& db)
    : owner(db), mutex(db.db_ ? sqlite3_db_mutex(db.db_) : nullptr) {
#ifndef __EMSCRIPTEN__
    if (!mutex) throw db_error("audit maintenance requires a serialized connection");
#endif
    sqlite3_mutex_enter(mutex);
    // Recheck after admission: the pre-gate probe is not an ownership lease.
    // Never wait for another transaction while retaining its owner's mutex.
    if (!idle(db)) {
        sqlite3_mutex_leave(mutex);
        throw db_error("audit maintenance requires an idle connection");
    }
    previous = current;
    current = this;
}

database::maintenance_scope::~maintenance_scope() noexcept {
    current = previous;
    sqlite3_mutex_leave(mutex);
}

void database::begin_transaction(bool exclusive) {
    if (closed_.load(std::memory_order_acquire) && !maintenance_scope::active_for(db_)) return;
    if (operation_cancelled()) throw db_error("database operation cancelled");
    // IMMEDIATE: acquires write lock, readers still allowed (WAL mode).
    // EXCLUSIVE: acquires write lock AND blocks all readers.
    // Use exclusive for migrations so stale connections can't read mid-migration.
    const char* sql = exclusive ? "BEGIN EXCLUSIVE" : "BEGIN IMMEDIATE";

    // Wall-clock budget for acquiring the transaction. Each attempt blocks
    // INSIDE SQLite's busy handler for the remaining budget — the handler
    // re-polls the lock with sub-millisecond cadence, so the lock is acquired
    // the moment it frees. (A previous version zeroed the statement timeout
    // and slept between attempts; under heavy writer traffic that sparse
    // polling starves — long write transactions hand the lock to whoever is
    // inside the busy handler, never to a sleeper.) The deadline, not the
    // per-attempt timeout, bounds the total wait: attempts repeat only for
    // same-connection transaction races, which resolve quickly.
    //
    // The budget is the configured busy timeout, NOT a constant: an
    // interactive process that opened with busy_timeout_ms=2000 must not
    // discover its explicit BEGINs still park for 30s (Aug 2026 hook-wedge
    // incident — two such waits ate a 60s harness deadline whole).
    const int budget_ms = busy_timeout_ms_ > 0 ? busy_timeout_ms_ : kDefaultBusyTimeoutMs;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_ms);
    int rc;
    for (;;) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining < 1) remaining = 1;
        configure_busy_timeout(static_cast<int>(remaining));

        rc = sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
        if (rc == SQLITE_OK) break;
        // A cancelled busy handler returns SQLITE_BUSY. Do not interpret that
        // as permission to reinstall another wait and retry until the deadline.
        if (operation_cancelled()) break;

        const bool past_deadline = std::chrono::steady_clock::now() >= deadline;
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            // The busy handler already waited out `remaining` — only retry if
            // wall clock says budget is left (e.g. spurious early return).
            if (past_deadline) break;
            continue;
        }
        if (rc == SQLITE_ERROR && is_in_transaction()) {
            // "cannot start a transaction within a transaction" — another
            // thread on this serialized connection (SQLITE_OPEN_FULLMUTEX)
            // holds a transaction. Brief sleep; it finishes shortly.
            if (past_deadline) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        break;  // non-retryable error
    }

    configure_busy_timeout(busy_timeout_ms_);  // Restore statement-level timeout

    if (rc != SQLITE_OK) {
        if (operation_cancelled()) throw db_error("database operation cancelled");
        auto error = std::string(sqlite3_errmsg(db_));
        int ext = sqlite3_extended_errcode(db_);
        LOG_ERROR("db", "Failed to begin transaction (rc=%d, ext=%d, db=%p, path=%s): %s",
                  rc, ext, (void*)db_, path_.c_str(), error.c_str());
        throw db_error("Failed to begin transaction: " + error);
    }
}

bool database::try_begin_immediate(int /*timeout_ms*/) {
    if (closed_.load(std::memory_order_acquire)) return false;
    if (operation_cancelled()) return false;
    // Non-blocking: temporarily set busy timeout to 0, try BEGIN IMMEDIATE once,
    // then restore the original timeout. This never sleeps.
    configure_busy_timeout(0);
    int rc = sqlite3_exec(db_, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
    configure_busy_timeout(busy_timeout_ms_);  // Restore configured timeout
    return rc == SQLITE_OK;
}

void database::commit() {
    execute("COMMIT");
}

void database::rollback() {
    execute("ROLLBACK");
}

bool database::is_in_transaction() const {
    // sqlite3_get_autocommit returns 0 if a transaction is active, non-zero otherwise
    return sqlite3_get_autocommit(db_) == 0;
}

// Transaction RAII guard
transaction::transaction(database& db, bool exclusive) : db_(db) {
    db_.begin_transaction(exclusive);
}

transaction::~transaction() {
    if (!completed_) {
        try {
            db_.rollback();
        } catch (...) {
            // Suppress exceptions in destructor
        }
    }
}

void transaction::commit() {
    db_.commit();
    completed_ = true;
}

void transaction::rollback() {
    db_.rollback();
    completed_ = true;
}

} // namespace lattice
