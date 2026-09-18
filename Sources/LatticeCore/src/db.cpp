#include "lattice/db.hpp"
#include "lattice/projection.hpp"
#include "lattice/log.hpp"
#include <sqlite-vec.h>
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <sys/stat.h>
#include <exception>

namespace lattice {

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


bool database_read_control::stopped() noexcept {
    if (stop_code.load(std::memory_order_acquire) != 0) return true;
    if (std::chrono::steady_clock::now() < deadline) return false;
    int32_t expected = 0;
    stop_code.compare_exchange_strong(expected, static_cast<int32_t>(projection_status::deadline_exceeded));
    return true;
}
void database_read_control::stop(int32_t reason) noexcept {
    int32_t expected = 0;
    stop_code.compare_exchange_strong(expected, reason, std::memory_order_acq_rel);
    std::lock_guard<std::mutex> lock(target_mutex);
    if (target) sqlite3_interrupt(target);
}
void database_read_control::publish(sqlite3* handle) noexcept {
    std::lock_guard<std::mutex> lock(target_mutex);
    target = handle;
    if (stopped()) sqlite3_interrupt(target);
}
void database_read_control::unpublish(sqlite3* handle) noexcept {
    std::lock_guard<std::mutex> lock(target_mutex);
    if (target == handle) target = nullptr;
}
void database::record_statement() {
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
}

std::shared_ptr<const physical_store_identity> database::physical_identity(
    const std::string& schema, const std::shared_ptr<database_read_control>& control,
    bool validate_current) const {
#if defined(__EMSCRIPTEN__) || (!defined(__APPLE__) && !defined(__linux__))
    return {};
#else
    if (schema == "main" && !validate_current) {
        if (auto cached = std::atomic_load(&main_physical_identity_)) return cached;
    }
    if (!db_) return {};
    auto* mutex = sqlite3_db_mutex(db_);
    if (!mutex) return {};
    const auto wait_end = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max(0, busy_timeout_ms_));
    while (sqlite3_mutex_try(mutex) != SQLITE_OK) {
        if ((control && control->stopped()) || (!control && std::chrono::steady_clock::now() >= wait_end)) return {};
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    struct unlock { sqlite3_mutex* mutex; ~unlock() { sqlite3_mutex_leave(mutex); } } unlock{mutex};
    return physical_identity_locked(schema, control);
#endif
}

std::shared_ptr<const physical_store_identity> database::physical_identity_locked(
    const std::string& schema, const std::shared_ptr<database_read_control>& control) const {
#if defined(__EMSCRIPTEN__) || (!defined(__APPLE__) && !defined(__linux__))
    return {};
#else
    if (control && control->stopped()) return {};
    const char* filename = sqlite3_db_filename(db_, schema.c_str());
    if (!filename || !*filename || sqlite3_uri_boolean(filename, "immutable", 0)) return {};
    sqlite3_vfs* vfs = nullptr;
    if (sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_VFS_POINTER, &vfs) != SQLITE_OK ||
        !vfs || !vfs->zName || (std::string(vfs->zName) != "unix" && std::string(vfs->zName) != "unix-excl")) return {};
    int moved = 1;
    if (sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_HAS_MOVED, &moved) != SQLITE_OK || moved) return {};
    std::error_code error;
    const auto canonical = std::filesystem::canonical(filename, error);
    if (error) return {};
    struct stat before{}, after{};
    if (::stat(canonical.c_str(), &before) != 0 || !S_ISREG(before.st_mode)) return {};
    moved = 1;
    if (sqlite3_file_control(db_, schema.c_str(), SQLITE_FCNTL_HAS_MOVED, &moved) != SQLITE_OK || moved ||
        ::stat(canonical.c_str(), &after) != 0 || before.st_dev != after.st_dev || before.st_ino != after.st_ino) return {};
    if (control && control->stopped()) return {};
    auto identity = std::make_shared<physical_store_identity>();
    identity->device = static_cast<uint64_t>(after.st_dev);
    identity->inode = static_cast<uint64_t>(after.st_ino);
    identity->filename = canonical.string();
    if (schema == "main") {
        auto cached = std::atomic_load(&main_physical_identity_);
        if (cached && !(*cached == *identity)) return {};
        if (!cached) std::atomic_store(&main_physical_identity_, std::shared_ptr<const physical_store_identity>(identity));
    }
    return identity;
#endif
}


std::shared_ptr<const physical_store_identity> database::attach_and_capture_identity(
    const std::string& attach_sql, const std::string& schema) {
    if (closed_.load(std::memory_order_acquire)) return {};
    struct capture_context {
        database* owner;
        const std::string* schema;
        std::shared_ptr<const physical_store_identity> identity;
        std::exception_ptr error;
    } context{this, &schema, {}, {}};
    // ATTACH produces no rows. This single constant row provides an internal
    // capture point inside the same sqlite3_exec as the successful ATTACH.
    const std::string sql = attach_sql + "; SELECT 1";
    record_statement(); // ATTACH attempt; the executed SELECT is counted below.
    char* message = nullptr;
    const int rc = sqlite3_exec(db_, sql.c_str(),
        [](void* raw, int columns, char**, char**) noexcept -> int {
            auto& capture = *static_cast<capture_context*>(raw);
            try {
                // Also tolerate legacy PRAGMA empty_result_callbacks: the
                // rowless ATTACH itself must never act as the capture point.
                if (columns != 1) return 0;
                database::record_statement();
                auto* mutex = sqlite3_db_mutex(capture.owner->db_);
                // FULLMUTEX connections have a recursive mutex. Verify usable
                // ownership without polling; unsupported configurations retain
                // legacy attachment but cannot authorize projected provenance.
                if (!mutex || sqlite3_mutex_try(mutex) != SQLITE_OK) return 0;
                struct unlock {
                    sqlite3_mutex* mutex;
                    ~unlock() { sqlite3_mutex_leave(mutex); }
                } release{mutex};
                capture.identity = capture.owner->physical_identity_locked(*capture.schema, {});
                return 0;
            } catch (...) {
                capture.error = std::current_exception();
                return 1; // SQLite finalizes before C++ propagates this error.
            }
        }, &context, &message);
    const std::unique_ptr<char, decltype(&sqlite3_free)> free_message(message, &sqlite3_free);
    if (rc != SQLITE_OK) {
        discard_if_rolled_back();
        if (context.error) std::rethrow_exception(context.error);
        throw db_error("SQL execution failed: " + std::string(message ? message : "Unknown error") +
                       " (SQL: " + attach_sql + ")");
    }
    drain_if_settled();
    return context.identity;
}

std::vector<std::string> database::query_attachment_text_metadata(
    const std::string& sql, const std::string& column) {
    if (closed_.load(std::memory_order_acquire)) return {};
    struct metadata_context {
        database* owner;
        const std::string* sql;
        const std::string* column;
        std::vector<std::string> values;
        std::exception_ptr error;
        bool entered = false;
        bool statement_failed = false;
    } context{this, &sql, &column, {}, {}};
    // Do not use pragma_* table-valued functions: ordinary tables can shadow
    // those names. The nested original PRAGMA/SELECT also preserves real SQLite
    // types, which sqlite3_exec's string-only result callback would erase.
    record_statement(); // Constant rendezvous statement.
    char* message = nullptr;
    const int rc = sqlite3_exec(db_, "SELECT 1",
        [](void* raw, int columns, char** values, char**) noexcept -> int {
            auto& capture = *static_cast<metadata_context*>(raw);
            try {
                if (!values) return 0; // Legacy empty_result_callbacks.
                if (columns != 1 || capture.entered)
                    throw db_error("Unexpected attachment metadata rendezvous shape");
                capture.entered = true;
                auto* mutex = sqlite3_db_mutex(capture.owner->db_);
                if (mutex && sqlite3_mutex_try(mutex) != SQLITE_OK)
                    throw db_error("Attachment metadata execution scope unavailable");
                struct unlock {
                    sqlite3_mutex* mutex;
                    ~unlock() { if (mutex) sqlite3_mutex_leave(mutex); }
                } release{mutex};
                // NULL means the connection has no SQLite mutex (e.g. a
                // single-threaded WASM build); preserve its existing mode.
                database::record_statement(); // Actual metadata attempt.
                sqlite3_stmt* raw_statement = nullptr;
                const int prepare_rc = sqlite3_prepare_v2(capture.owner->db_,
                    capture.sql->c_str(), -1, &raw_statement, nullptr);
                const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>
                    statement(raw_statement, &sqlite3_finalize);
                if (prepare_rc != SQLITE_OK)
                    throw db_error("Failed to prepare attachment metadata: " +
                        std::string(sqlite3_errmsg(capture.owner->db_)));
                if (!statement || !sqlite3_stmt_readonly(statement.get()))
                    throw db_error("Attachment metadata requires a read-only statement");
                int selected = -1;
                const int count = sqlite3_column_count(statement.get());
                for (int index = 0; index < count; ++index) {
                    const char* name = sqlite3_column_name(statement.get(), index);
                    if (!name) throw db_error("Attachment metadata column name allocation failed");
                    if (*capture.column == name) selected = index;
                }
                int step_rc = SQLITE_OK;
                while ((step_rc = sqlite3_step(statement.get())) == SQLITE_ROW) {
                    if (selected < 0 || sqlite3_column_type(statement.get(), selected) != SQLITE_TEXT)
                        continue; // Existing callers ignore missing/non-TEXT name values.
                    const auto* value = sqlite3_column_text(statement.get(), selected);
                    if (!value) throw db_error("Attachment metadata text allocation failed");
                    // Match extract_column's current C-string semantics. This
                    // is metadata only, not a new general binary/text codec.
                    capture.values.emplace_back(reinterpret_cast<const char*>(value));
                }
                if (step_rc != SQLITE_DONE) {
                    capture.statement_failed = true;
                    throw db_error("Attachment metadata query failed: " +
                        std::string(sqlite3_errmsg(capture.owner->db_)));
                }
                return 0; // Statement finalizes before releasing the recursive scope.
            } catch (...) {
                capture.error = std::current_exception();
                return 1;
            }
        }, &context, &message);
    const std::unique_ptr<char, decltype(&sqlite3_free)> free_message(message, &sqlite3_free);
    if (rc != SQLITE_OK || context.error) {
        if (context.statement_failed) discard_if_rolled_back();
        if (context.error) std::rethrow_exception(context.error);
        throw db_error("Attachment metadata execution failed: " +
            std::string(message ? message : "Unknown error"));
    }
    if (!context.entered) throw db_error("Attachment metadata rendezvous did not execute");
    drain_if_settled();
    return std::move(context.values);
}

database::database(const std::string& path, open_mode mode, int busy_timeout_ms,
                   std::shared_ptr<database_read_control> read_control)
    : path_(path), mode_(mode), busy_timeout_ms_(busy_timeout_ms), read_control_(std::move(read_control)) {
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
        flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        flags |= SQLITE_OPEN_URI;  // see read-only branch note
        rc = sqlite3_open_v2(path.c_str(), &db_, flags, nullptr);
    }
    if (rc != SQLITE_OK) {
        std::string error = sqlite3_errmsg(db_);
        sqlite3_close_v2(db_);
        db_ = nullptr;
        LOG_ERROR("db", "Failed to open database: %s", error.c_str());
        throw db_error("Failed to open database: " + error);
    }

    try {
    // Statement-level busy timeout MUST be installed before ANY statement runs.
    // It used to be set after the open-time pragmas, so the very first
    // `PRAGMA journal_mode = WAL` had no busy handler — a concurrent open or
    // an in-flight writer on the same file made it throw
    // "database is locked" instantly instead of waiting. Instances now
    // genuinely close and reopen (weak instance cache), so open-time races
    // are common rather than exceptional.
    if (read_control_) {
        read_control_->publish(db_);
        sqlite3_progress_handler(db_, 1000, [](void* context) -> int {
            return static_cast<database_read_control*>(context)->stopped() ? 1 : 0;
        }, read_control_.get());
        sqlite3_busy_handler(db_, [](void* context, int) -> int {
            auto* control = static_cast<database_read_control*>(context);
            if (control->stopped()) return 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return control->stopped() ? 0 : 1;
        }, read_control_.get());
        if (read_control_->stopped()) throw db_error("owned read cancelled before initialization");
    } else {
        sqlite3_busy_timeout(db_, busy_timeout_ms_);
    }

    // Enable foreign keys
    execute("PRAGMA foreign_keys = ON");

#ifdef __EMSCRIPTEN__
    // WASM/OPFS mode: Use DELETE journal mode (WAL requires mmap/shm which OPFS doesn't support)
    // Also skip mmap since OPFS uses SyncAccessHandle instead
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = DELETE");
    }
    execute(read_control_ ? "PRAGMA cache_size = 2000" : "PRAGMA cache_size = 50000");       // Large cache for performance
    execute(read_control_ ? "PRAGMA temp_store = FILE" : "PRAGMA temp_store = MEMORY");      // Temp tables in RAM
#else
    // Native mode: Enable WAL mode for better concurrency (only on read-write connection)
    if (mode == open_mode::read_write) {
        execute("PRAGMA journal_mode = WAL");
    }

    // Performance optimizations (matching Lattice.swift)
    execute(read_control_ ? "PRAGMA cache_size = 2000" : "PRAGMA cache_size = 50000");       // Large cache for performance
    execute(read_control_ ? "PRAGMA mmap_size = 0" : "PRAGMA mmap_size = 300000000");    // Memory-mapped I/O (~300MB)
    execute(read_control_ ? "PRAGMA temp_store = FILE" : "PRAGMA temp_store = MEMORY");      // Temp tables in RAM
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

    // Initialize sqlite-vec extension for vector search
    int vec_rc = sqlite3_vec_init(db_, nullptr, nullptr);
    if (vec_rc != SQLITE_OK) {
        if (read_control_) read_control_->unpublish(db_);
        sqlite3_close_v2(db_);
        db_ = nullptr;
        LOG_ERROR("db", "Failed to initialize sqlite-vec extension");
        throw db_error("Failed to initialize sqlite-vec extension");
    }
    } catch (...) {
        if (read_control_) read_control_->unpublish(db_);
        if (db_) sqlite3_close_v2(db_);
        db_ = nullptr;
        throw;
    }
}

database::~database() {
    if (read_control_) read_control_->unpublish(db_);
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
    // a final query on the still-open connection.
    closed_.store(true, std::memory_order_release);
}

void database::set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back) {
    on_txn_settled_ = std::move(settled);
    on_txn_rolled_back_ = std::move(rolled_back);
    // Rollback hook: fires inside SQLite's C frames, so the trampoline only
    // clears a flag and (via rolled_back) a C++ vector — no SQLite calls,
    // nothing that can throw. Applies to every storage kind: without it a
    // rolled-back transaction's buffered rows linger and the NEXT flush
    // delivers them as phantoms (latent file-DB bug, see the design doc).
    sqlite3_rollback_hook(db_,
        [](void* self_ptr) {
            auto* self = static_cast<database*>(self_ptr);
            self->txn_dirty_.store(false, std::memory_order_relaxed);
            if (self->on_txn_rolled_back_) self->on_txn_rolled_back_();
        },
        this);
}

void database::drain_if_settled() {
    // A nested query from the update hook can finish before the outer
    // implicit statement does. Autocommit alone does not identify that
    // callback frame. Leave dirty state for the actual statement's tail.
    if (update_hook_scope::active_for(db_)) return;
    // Post-statement drain point (docs/design-deferred-memory-delivery.md):
    // after a successful statement, autocommit != 0 means the top-level
    // transaction just closed (implicit, or the explicit COMMIT that funnels
    // through execute()) and every lock is released — the exact post-commit
    // point the WAL hook gives file DBs, but reached through plain C++
    // frames, so observer exceptions propagate to the writer instead of
    // unwinding through sqlite3_step. Clear the flag BEFORE draining so a
    // callback's own writes re-arm it rather than re-entering.
    if (!on_txn_settled_ || !db_ || !txn_dirty_.load(std::memory_order_relaxed)) return;
    auto* mutex = sqlite3_db_mutex(db_);
    sqlite3_mutex_enter(mutex);
    const bool deliver = sqlite3_get_autocommit(db_) != 0 &&
        txn_dirty_.exchange(false, std::memory_order_relaxed);
    sqlite3_mutex_leave(mutex);
    // Claim under SQLite, deliver after releasing it. Unrelated active read
    // cursors do not defer an already committed writer's notifications.
    if (deliver) on_txn_settled_();
}

void database::discard_if_rolled_back() {
    // Failed statement with autocommit restored: the implicit transaction
    // (if any) already rolled back. SQLite's rollback hook covers most of
    // these paths; clear defensively so a hook-buffered row from the failed
    // statement can't surface as a phantom in the next flush. Inside an
    // explicit transaction (autocommit == 0) nothing is cleared — the
    // transaction may still commit.
    if (sqlite3_get_autocommit(db_) != 0) {
        txn_dirty_.store(false, std::memory_order_relaxed);
        if (on_txn_rolled_back_) on_txn_rolled_back_();
    }
}

database::database(database&& other) noexcept
    : db_(other.db_), path_(std::move(other.path_)), mode_(other.mode_),
      busy_timeout_ms_(other.busy_timeout_ms_), read_control_(std::move(other.read_control_)),
      main_physical_identity_(std::atomic_load(&other.main_physical_identity_)) {
    other.db_ = nullptr;
}

database& database::operator=(database&& other) noexcept {
    if (this != &other) {
        if (read_control_) read_control_->unpublish(db_);
        if (db_) {
            sqlite3_close_v2(db_);
        }
        db_ = other.db_;
        mode_ = other.mode_;
        busy_timeout_ms_ = other.busy_timeout_ms_;
        read_control_ = std::move(other.read_control_);
        std::atomic_store(&main_physical_identity_, std::atomic_load(&other.main_physical_identity_));
        path_ = std::move(other.path_);
        other.db_ = nullptr;
    }
    return *this;
}

int64_t database::changes() const {
    if (closed_.load(std::memory_order_acquire) || !db_) return 0;
    return sqlite3_changes64(db_);
}

void database::execute(const std::string& sql, const std::vector<column_value_t>& params) {
    if (closed_.load(std::memory_order_acquire)) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (params.empty()) {
        // Fast path for parameterless queries
        char* errmsg = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errmsg);
        if (rc != SQLITE_OK) {
            std::string error = errmsg ? errmsg : "Unknown error";
            sqlite3_free(errmsg);
            LOG_ERROR("db", "SQL execution failed: %s (SQL: %s)", error.c_str(), sql.c_str());
            discard_if_rolled_back();
            throw db_error("SQL execution failed: " + error + " (SQL: " + sql + ")");
        }
        drain_if_settled();
    } else {
        // Prepared statement path for parameterized queries
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            LOG_ERROR("db", "Failed to prepare statement: %s (SQL: %s)", sqlite3_errmsg(db_), sql.c_str());
            throw db_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db_)));
        }

        int index = 1;
        for (const auto& param : params) {
            bind_value(stmt, index++, param);
        }

        // Step until SQLITE_DONE. Virtual tables (e.g. vec0) may return
        // SQLITE_ROW for DML statements (DELETE returns the deleted row).
        // Drain all rows before expecting SQLITE_DONE.
        do {
            rc = sqlite3_step(stmt);
        } while (rc == SQLITE_ROW);

        // Capture error message BEFORE finalize, which resets connection error state
        std::string errmsg_str;
        if (rc != SQLITE_DONE) {
            errmsg_str = sqlite3_errmsg(db_) ? sqlite3_errmsg(db_) : "Unknown error";
        }

        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE) {
            LOG_ERROR("db", "Execution failed: %s (SQL: %s)", errmsg_str.c_str(), sql.c_str());
            discard_if_rolled_back();
            throw db_error("Execution failed: " + errmsg_str);
        }
        drain_if_settled();
    }
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
            sqlite3_bind_text(stmt, index, v.c_str(), -1, SQLITE_TRANSIENT);
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
            return std::string(text ? text : "");
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

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare insert: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare insert: " + std::string(sqlite3_errmsg(db_)));
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }

    rc = sqlite3_step(stmt);

    if (!conflict_columns.empty()) {
        primary_key_t affected_rowid = 0;
        if (rc == SQLITE_ROW) {
            affected_rowid = sqlite3_column_int64(stmt, 0);
            rc = sqlite3_step(stmt);  // drain RETURNING
        }
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) {
            int extended_rc = sqlite3_extended_errcode(db_);
            auto err = std::string(sqlite3_errmsg(db_));
            LOG_ERROR("db", "Upsert failed (rc=%d, ext=%d, db=%p, path=%s): %s",
                      rc, extended_rc, (void*)db_, path_.c_str(), err.c_str());
            discard_if_rolled_back();
            throw db_error("Insert failed: " + err);
        }
        drain_if_settled();
        return affected_rowid;
    }

    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        int extended_rc = sqlite3_extended_errcode(db_);
        auto err = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Insert failed (rc=%d, ext=%d, db=%p, path=%s): %s",
                  rc, extended_rc, (void*)db_, path_.c_str(), err.c_str());
        discard_if_rolled_back();
        throw db_error("Insert failed: " + err);
    }

    // Capture the rowid BEFORE draining: the drain runs observer callbacks,
    // whose own writes would clobber last_insert_rowid on this connection.
    auto rowid = sqlite3_last_insert_rowid(db_);
    drain_if_settled();
    return rowid;
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

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Failed to prepare update: %s", error.c_str());
        throw db_error("Failed to prepare update: " + error);
    }

    int index = 1;
    for (const auto& [_, val] : values) {
        bind_value(stmt, index++, val);
    }
    sqlite3_bind_int64(stmt, index, id);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        auto errmsg = sqlite3_errmsg(db_);
        LOG_ERROR("db", "Update failed: %s", errmsg);
        discard_if_rolled_back();
        throw db_error("Update failed: " + std::string(errmsg));
    }
    drain_if_settled();
}

void database::remove(const std::string& table, primary_key_t id) {
    if (closed_.load(std::memory_order_acquire)) return;
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    std::string sql = "DELETE FROM " + table + " WHERE id = ?";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        LOG_ERROR("db", "Failed to prepare delete: %s", sqlite3_errmsg(db_));
        throw db_error("Failed to prepare delete: " + std::string(sqlite3_errmsg(db_)));
    }

    sqlite3_bind_int64(stmt, 1, id);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        LOG_ERROR("db", "Delete failed: %s", sqlite3_errmsg(db_));
        discard_if_rolled_back();
        throw db_error("Delete failed: " + std::string(sqlite3_errmsg(db_)));
    }
    drain_if_settled();
}

std::vector<database::row_t> database::query(const std::string& sql,
                                             const std::vector<column_value_t>& params) {
    g_statement_count.fetch_add(1, std::memory_order_relaxed);
    ++t_statement_count;
    if (closed_.load(std::memory_order_acquire)) return {};
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        auto errmsg = sqlite3_errmsg(db_);
        LOG_ERROR("db", "%s in %s", errmsg, sql.c_str());
        std::cerr<<"db: "<<errmsg<<" "<<sql.c_str()<<std::endl;
        throw db_error("Failed to prepare query: " + std::string(errmsg));
    }

    int index = 1;
    for (const auto& param : params) {
        bind_value(stmt, index++, param);
    }

    std::vector<row_t> results;
    int col_count = sqlite3_column_count(stmt);

    // Capture column names ONCE, before stepping. sqlite3_column_name
    // materializes the name lazily on first access and returns NULL if that
    // allocation fails — under system memory pressure (Apple's purgeable
    // page cache purging mid-scan) this is a real, observed failure, and
    // constructing the row-map key from NULL is a segfault. Names are stable
    // for the life of the statement, so per-row capture was also wasted work.
    std::vector<std::string> col_names;
    col_names.reserve(static_cast<size_t>(col_count));
    for (int i = 0; i < col_count; ++i) {
        const char* name = sqlite3_column_name(stmt, i);
        if (!name) {
            sqlite3_finalize(stmt);
            LOG_ERROR("db", "column_name OOM in %s", sql.c_str());
            throw db_error("Query failed: out of memory reading column name");
        }
        col_names.emplace_back(name);
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        row_t row;
        for (int i = 0; i < col_count; ++i) {
            row[col_names[static_cast<size_t>(i)]] = extract_column(stmt, i);
        }
        results.push_back(std::move(row));
    }

    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        auto error = std::string(sqlite3_errmsg(db_));
        LOG_ERROR("db", "Query failed: %s", error.c_str());
        discard_if_rolled_back();
        throw db_error("Query failed: " + error);
    }

    // A plain SELECT can't close a transaction, but DML-via-RETURNING issued
    // through query() can — one relaxed load of insurance (see design doc).
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
    sqlite3_busy_timeout(db_, truncate ? busy_budget_ms : 0);
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
    sqlite3_busy_timeout(db_, busy_timeout_ms_);  // restore statement-level timeout
    if (truncate && result.busy != 0) {
        // Signal only after the checkpoint statement returned. This does not
        // claim the current BUSY attempt succeeded; a later retry may truncate.
        try { retire_projection_store(physical_identity()); } catch (...) {}
    }
    return result;
#endif
}

void database::begin_transaction(bool exclusive) {
    if (closed_.load(std::memory_order_acquire)) return;
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
        sqlite3_busy_timeout(db_, static_cast<int>(remaining));

        rc = sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
        if (rc == SQLITE_OK) break;

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

    sqlite3_busy_timeout(db_, busy_timeout_ms_);  // Restore statement-level timeout

    if (rc != SQLITE_OK) {
        auto error = std::string(sqlite3_errmsg(db_));
        int ext = sqlite3_extended_errcode(db_);
        LOG_ERROR("db", "Failed to begin transaction (rc=%d, ext=%d, db=%p, path=%s): %s",
                  rc, ext, (void*)db_, path_.c_str(), error.c_str());
        throw db_error("Failed to begin transaction: " + error);
    }
}

bool database::try_begin_immediate(int /*timeout_ms*/) {
    if (closed_.load(std::memory_order_acquire)) return false;
    // Non-blocking: temporarily set busy timeout to 0, try BEGIN IMMEDIATE once,
    // then restore the original timeout. This never sleeps.
    sqlite3_busy_timeout(db_, 0);
    int rc = sqlite3_exec(db_, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
    sqlite3_busy_timeout(db_, busy_timeout_ms_);  // Restore configured timeout
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
