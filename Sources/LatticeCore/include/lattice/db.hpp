#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include <sqlite3.h>
#include <stdexcept>
#include <unordered_map>
#include <atomic>
#include <functional>

namespace lattice {

/// Default statement-level busy timeout. Headless/server processes tolerate long
/// waits; interactive apps should pass a smaller value (e.g. 5000) via
/// configuration::busy_timeout_ms so a stuck writer can't hang the UI thread.
inline constexpr int kDefaultBusyTimeoutMs = 30000;

class db_error : public std::runtime_error {
public:
    explicit db_error(const std::string& msg) : std::runtime_error(msg) {}
};

class database {
public:
    /// Open mode for database connections
    enum class open_mode {
        read_write,  ///< Full read/write access (default)
        read_only    ///< Read-only; joins a concurrent writer's WAL (sees committed WAL rows)
    };

    explicit database(const std::string& path, open_mode mode = open_mode::read_write,
                      int busy_timeout_ms = kDefaultBusyTimeoutMs);
    ~database();

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
    // Deferred delivery (docs/design-deferred-memory-delivery.md): set by the
    // update hook via mark_txn_dirty(); consumed by drain_if_settled() at the
    // success tail of every statement wrapper; cleared by the rollback hook.
    std::atomic<bool> txn_dirty_{false};
    std::function<void()> on_txn_settled_;
    std::function<void()> on_txn_rolled_back_;
    column_value_t extract_column(sqlite3_stmt* stmt, int index);
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
