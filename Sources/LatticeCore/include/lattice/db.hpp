#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include <sqlite3.h>
#include <stdexcept>
#include <unordered_map>
#include <atomic>
#include <functional>
#include <chrono>
#include <memory>
#include <mutex>

namespace lattice {

/// Default statement-level busy timeout. Headless/server processes tolerate long
/// waits; interactive apps should pass a smaller value (e.g. 5000) via
/// configuration::busy_timeout_ms so a stuck writer can't hang the UI thread.
inline constexpr int kDefaultBusyTimeoutMs = 30000;

class db_error : public std::runtime_error {
public:
    explicit db_error(const std::string& msg) : std::runtime_error(msg) {}
};

/// Only installed on a connection exclusively owned by one read operation.
/// Target publication protects sqlite3_interrupt from late cancellation/UAF.
struct database_read_control {
    std::atomic<int32_t> stop_code{0};
    std::chrono::steady_clock::time_point deadline;
    std::mutex target_mutex;
    sqlite3* target = nullptr;
    bool stopped() noexcept;
    void stop(int32_t reason) noexcept;
    void publish(sqlite3* handle) noexcept;
    void unpublish(sqlite3* handle) noexcept;
};

/// Native live-file identity. filename is SQLite's decoded absolute filename,
/// canonicalized for diagnostics; matching uses device/inode, not spelling.
/// Capture validates pathname stability with HAS_MOVED, not an atomic fstat of
/// SQLite's descriptor. Concurrent external replacement during capture is
/// unsupported; detected movement/replacement fails projected reads closed.
struct physical_store_identity {
    uint64_t device = 0, inode = 0;
    std::string filename;
    bool operator==(const physical_store_identity& other) const noexcept {
        return device == other.device && inode == other.inode;
    }
};

class database {
    friend class lattice_db;
    template<typename T, typename Enable> friend struct managed;
    // The update hook may query globalId through database::query(). That
    // nested query must not drain a prior row's dirty state while the outer
    // SQLite statement still owns its connection mutex. Track the actual
    // callback frame, including callers that step SQLite directly.
    struct update_hook_scope {
        sqlite3* connection;
        update_hook_scope* previous;
        static inline thread_local update_hook_scope* current = nullptr;
        explicit update_hook_scope(database& db) noexcept
            : connection(db.db_), previous(current) { current = this; }
        ~update_hook_scope() noexcept { current = previous; }
        update_hook_scope(const update_hook_scope&) = delete;
        update_hook_scope& operator=(const update_hook_scope&) = delete;
        static bool active_for(sqlite3* connection) noexcept {
            for (auto* frame = current; frame; frame = frame->previous) {
                if (frame->connection == connection) return true;
            }
            return false;
        }
    };

public:
    /// Open mode for database connections
    enum class open_mode {
        read_write,  ///< Full read/write access (default)
        read_only    ///< Read-only; joins a concurrent writer's WAL (sees committed WAL rows)
    };

    explicit database(const std::string& path, open_mode mode = open_mode::read_write,
                      int busy_timeout_ms = kDefaultBusyTimeoutMs,
                      std::shared_ptr<database_read_control> read_control = {});
    ~database();

    /// No SQL statements. Best-effort for legacy callers; nullptr means an
    /// unsupported/moved/nonfilesystem store or interrupted metadata wait.
    /// The main identity is cached lazily; validation is required on a newly
    /// opened private lease. Callers never hold a global registry lock here.
    std::shared_ptr<const physical_store_identity> physical_identity(
        const std::string& schema = "main",
        const std::shared_ptr<database_read_control>& control = {},
        bool validate_current = false) const;


    /// Logically close the connection: subsequent ops short-circuit to empty/no-op.
    /// The underlying sqlite3* is NOT freed here — it is released in ~database (which
    /// is single-threaded), so a reader on another thread can never deref a freed
    /// handle. Use instead of destroying the wrapper while readers may still hold it.
    void close();

    // Non-copyable
    database(const database&) = delete;
    database& operator=(const database&) = delete;

    // Moveable
    database(database&& other) noexcept;
    database& operator=(database&& other) noexcept;

    // Schema management
    void create_table(const table_schema& schema);
    void ensure_table(const table_schema& schema);
    bool table_exists(const std::string& name) const;

    // Get existing column names and types from a table (for migration)
    // Returns map of column_name -> SQL_TYPE (uppercase)
    std::unordered_map<std::string, std::string> get_table_info(const std::string& table) const;

    // CRUD operations
    // conflict_columns: if non-empty, generates ON CONFLICT (...) DO UPDATE SET for upsert
    primary_key_t insert(const std::string& table,
                         const std::vector<std::pair<std::string, column_value_t>>& values,
                         const std::vector<std::string>& conflict_columns = {});

    void update(const std::string& table,
                primary_key_t id,
                const std::vector<std::pair<std::string, column_value_t>>& values);

    void remove(const std::string& table, primary_key_t id);

    // Query - returns rows as vector of column maps
    using row_t = std::unordered_map<std::string, column_value_t>;
    std::vector<row_t> query(const std::string& sql,
                             const std::vector<column_value_t>& params = {});

    // Transaction support
    void begin_transaction(bool exclusive = false);
    /// Try to begin an IMMEDIATE transaction with a short timeout.
    /// Returns true if the transaction was started, false if the DB is busy.
    /// Use for optional write paths (e.g. vec0 reconciliation) where blocking is worse than skipping.
    bool try_begin_immediate(int timeout_ms = 100);
    void commit();
    void rollback();
    bool is_in_transaction() const;

    // Execute SQL with optional params (for INSERT/UPDATE/DELETE without return)
    void execute(const std::string& sql,
                 const std::vector<column_value_t>& params = {});

    /// Rows changed by the most recent INSERT/UPDATE/DELETE on this
    /// connection (sqlite3_changes64). With value-guarded writes (a DO
    /// UPDATE arm or UPDATE gated on actual value inequality) 0 means the
    /// statement was a genuine no-op — the sync apply path uses this to
    /// suppress relay minting for value-identical redundant deliveries.
    int64_t changes() const;

    /// Advance this connection's WAL read snapshot to see the latest committed data.
    /// Needed when another connection wrote and this connection's mmap'd WAL index is stale.
    void refresh_wal_snapshot();

    /// Interrupt any in-flight statement on this connection
    /// (sqlite3_interrupt). Safe to call from another thread. Used by the
    /// read-generation force-retire protocol: SQLite refuses COMMIT while
    /// statements are in progress, so a wedged in-flight read must be kicked
    /// before the keeper transaction can close (results spec §3.4).
    void interrupt();

    /// Result of a wal_checkpoint() call. rc is the PRAGMA's SQLite result
    /// code; busy is 1 when the checkpoint could not complete because a
    /// reader/writer held the WAL; log_frames/checkpointed mirror the PRAGMA
    /// row (-1 when unavailable).
    struct checkpoint_result {
        int rc = 0;
        int busy = 1;
        int64_t log_frames = -1;
        int64_t checkpointed = -1;
    };

    /// Run a WAL checkpoint on this (read-write) connection.
    /// PASSIVE (truncate=false) backfills as far as the oldest live reader
    /// allows and never blocks anyone. TRUNCATE (truncate=true) additionally
    /// resets the -wal file to zero length, but must wait out readers — the
    /// busy_budget_ms bounds that wait so a held snapshot makes it FAIL FAST
    /// instead of stalling writers. No-op (busy=1) on read-only connections,
    /// closed connections, and Emscripten (DELETE journal mode).
    checkpoint_result wal_checkpoint(bool truncate, int busy_budget_ms = 250);

    /// Process-global count of SQL statements issued through the public
    /// funnels (query/execute/insert/update/remove) across ALL connections.
    /// Test/bench primitive: recall-style code paths span multiple
    /// connections (read/write/xproc, attached lattices), so a per-connection
    /// counter undercounts — tests assert on deltas of this global.
    static uint64_t total_statement_count();

    /// Thread-local twin of total_statement_count(): counts only statements
    /// issued from the calling thread. Exact budgets for single-threaded
    /// read paths, immune to parallel test suites in the same process.
    static uint64_t thread_statement_count();
    /// Raw bounded read cursors use the same statement accounting funnel.
    static void record_statement();

    /// Mark this connection dirty: buffered row changes await delivery once
    /// the enclosing transaction settles. Relaxed store — callable from inside
    /// sqlite3_update_hook (C frame: no locks, nothing that can throw).
    void mark_txn_dirty() { txn_dirty_.store(true, std::memory_order_relaxed); }

    /// Install the transaction-settled drain and rollback-discard callbacks
    /// (docs/design-deferred-memory-delivery.md). `settled` runs after any
    /// successful statement that leaves the connection in autocommit mode
    /// with the dirty flag set — i.e. at the close of every top-level
    /// transaction (including the explicit COMMIT, which funnels through
    /// execute()), on the writing thread, outside all SQLite frames.
    /// `rolled_back` is invoked from sqlite3_rollback_hook (C frame — it must
    /// only clear state, never touch SQLite or throw) and defensively on
    /// failed statements whose implicit transaction already rolled back.
    void set_txn_hooks(std::function<void()> settled, std::function<void()> rolled_back);

    // Raw access (use sparingly)
    sqlite3* handle() const { return db_; }

    // Bind a value to a prepared statement (public for lattice_db bulk insert)
    void bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value);

    /// Whether close() has been called. Ops check this and short-circuit.
    bool is_closed() const { return closed_.load(std::memory_order_acquire); }

private:
    sqlite3* db_ = nullptr;
    std::string path_;
    open_mode mode_;
    // Set by close(); ops short-circuit when set. db_ stays valid until ~database,
    // so this is a logical-close flag, not a lifetime guard.
    std::atomic<bool> closed_{false};
    int busy_timeout_ms_ = kDefaultBusyTimeoutMs;
    std::shared_ptr<database_read_control> read_control_;
    mutable std::shared_ptr<const physical_store_identity> main_physical_identity_;
    // Deferred delivery (docs/design-deferred-memory-delivery.md): set by the
    // update hook via mark_txn_dirty(); consumed by drain_if_settled() at the
    // success tail of every statement wrapper; cleared by the rollback hook.
    std::atomic<bool> txn_dirty_{false};
    std::function<void()> on_txn_settled_;
    std::function<void()> on_txn_rolled_back_;
    column_value_t extract_column(sqlite3_stmt* stmt, int index);
    // Internal live primitive getter path. Preserve query()'s first-row/name
    // and stored-type conventions without building generic result containers.
    // Empty optional means no matching first-row cell; a present nullptr is
    // SQL NULL. The owning connection, fresh statement and settled tail remain.
    std::optional<column_value_t> query_managed_cell(
        const std::string& sql, const std::string& column, primary_key_t row_id);
    // ATTACH-only internal operation. Capture metadata in the same SQLite
    // execution scope, before a competing writer can win a second acquisition.
    // This captures only internal metadata; deferred user delivery stays after it.
    std::shared_ptr<const physical_store_identity> attach_and_capture_identity(
        const std::string& attach_sql, const std::string& schema);
    // Caller owns this handle's recursive SQLite mutex.
    std::shared_ptr<const physical_store_identity> physical_identity_locked(
        const std::string& schema,
        const std::shared_ptr<database_read_control>& control) const;
    // Attachment schema metadata only. Run the existing single read statement
    // inside one SQLite execution scope; keep original SQLite types and names.
    std::vector<std::string> query_attachment_text_metadata(
        const std::string& sql, const std::string& column);
    void drain_if_settled();
    void discard_if_rolled_back();
};

// RAII transaction guard
class transaction {
public:
    explicit transaction(database& db, bool exclusive = false);
    ~transaction();

    void commit();
    void rollback();

private:
    database& db_;
    bool completed_ = false;
};

} // namespace lattice

#endif // __cplusplus
