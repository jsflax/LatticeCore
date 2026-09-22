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

class lattice_db;
class database;
namespace detail {
struct exact_vector_rows_access;
struct recovery_writer_access;
struct recovery_witness_access;
struct recovery_refresh_access;
struct receive_delivery_guard_access;
class canonical_writer_adapter;
struct canonical_writer_custody_test_access;
class recovery_local_producer_adapter;
class recovery_continuous_producer;
struct recovery_continuous_admission;
void require_continuous_raw_handle_absent(database&);
void require_continuous_legacy_export_absent(database&);
class recovery_obligation_producer_store;
void require_canonical_relation(database&, const std::string&);
bool prepare_recovery_local_producer(lattice_db&, const std::shared_ptr<database>&);
void publish_recovery_local_producer(lattice_db&, database&) noexcept;
bool preserve_recovery_local_producer_relation(database&, const std::string&);
void require_recovery_local_producer_maintenance_absent(database&);
void reset_sync_channel_with_producer_fence(lattice_db&, const std::string&, bool retire);
void initialize_receive_guard_schema(database&, bool legacy_origin);
bool receive_guard_manages_cursor(database&);
void require_receive_guard_history_unblocked(database&);

// One ordinary attached-field operation. Main/manual database fields keep
// their existing path. The implementation never acquires a topology mutex
// beneath SQLite; it validates the published generation while holding SQLite.
class managed_route_scope {
    database* db_ = nullptr;
    std::shared_ptr<database> writer_owner_;
    lattice_db* owner_ = nullptr;
    sqlite3_mutex* mutex_ = nullptr;
    std::unique_lock<std::recursive_timed_mutex> vector_gate_;
    managed_route_scope* previous_ = nullptr;
    int exceptions_ = 0;
    static thread_local managed_route_scope* current_;
public:
    managed_route_scope(database*, lattice_db*, const std::string& table,
                        int64_t token, const std::weak_ptr<database>& writer,
                        bool vector_write = false);
    ~managed_route_scope() noexcept(false);
    managed_route_scope(const managed_route_scope&) = delete;
    managed_route_scope& operator=(const managed_route_scope&) = delete;
    static bool active_for(const database*) noexcept;
    static bool active_for(const lattice_db*) noexcept;
};
}

class database {
    friend class lattice_db;
    friend struct detail::exact_vector_rows_access;
    friend struct detail::recovery_writer_access;
    friend struct detail::recovery_witness_access;
    friend struct detail::recovery_refresh_access;
    friend struct detail::receive_delivery_guard_access;
    friend class detail::canonical_writer_adapter;
    friend struct detail::canonical_writer_custody_test_access;
    friend class detail::recovery_local_producer_adapter;
    friend class detail::recovery_continuous_producer;
    friend void detail::require_continuous_raw_handle_absent(database&);
    friend void detail::require_continuous_legacy_export_absent(database&);
    friend class detail::recovery_obligation_producer_store;
    friend void detail::require_canonical_relation(database&, const std::string&);
    friend bool detail::preserve_recovery_local_producer_relation(database&, const std::string&);
    friend void detail::require_recovery_local_producer_maintenance_absent(database&);
    // Private fixed-scope trigger qualification lacks upstream receipt settlement.
    bool canonical_trigger_only_ = false;
    std::shared_ptr<void> canonical_callback_custody_;
    std::shared_ptr<std::atomic<bool>> canonical_write_allowed_;
    // Physical policy custody, serialized by SQLite's connection mutex.
    // Bootstrap closes the interval before the canonical context is published.
    bool canonical_custody_bootstrap_ = false;
    // Failed producer bootstrap must not let optional close-time ANALYZE
    // mutate the schema it just refused. This policy follows the physical
    // handle through moves, even before producer callback custody exists.
    bool suppress_destructor_optimize_ = false;
    // Writable constructors classify eagerly. Read-only internal/keeper paths
    // defer until a guarded raw/legacy export boundary. Unknown never means
    // absence; this denial-only state cannot grant producer/source authority.
    enum class continuous_classification { unknown, ordinary, protected_file };
    std::atomic<continuous_classification> continuous_file_{continuous_classification::unknown};
    bool txn_hooks_external_ = false;
    void set_txn_hooks_owned_(std::function<void()>, std::function<void()>);
    void rebind_txn_hooks_owned_() noexcept;
    // Private receiver producer admission. Heap custody follows the physical
    // connection across wrapper moves; no all-route capability is implied.
    std::shared_ptr<void> local_producer_callback_custody_;
    std::shared_ptr<std::atomic<bool>> local_producer_write_allowed_;
    // A failed channel-reset cleanup cannot leave partial work committable.
    // Only an explicit successful rollback clears this physical-writer fence.
    std::atomic<bool> channel_reset_unsettled_{false};
    int step_statement_(sqlite3_stmt*) const;
    friend class detail::managed_route_scope;
    // Only database can construct this key. The keyed overload remains
    // accessible to make_shared so keepers retain its single allocation.
    class initialization_key {
        friend class database;
        friend class detail::recovery_continuous_producer;
        const bool keeper_cache_;
        std::shared_ptr<detail::recovery_continuous_admission> continuous_;
        explicit initialization_key(std::shared_ptr<detail::recovery_continuous_admission> value) : keeper_cache_(false), continuous_(std::move(value)) {}
        explicit initialization_key(bool keeper_cache) : keeper_cache_(keeper_cache) {}
    public:
        initialization_key(const initialization_key&) = default;
    };
    static std::shared_ptr<database> make_read_keeper(const std::string& path,
                                                    int busy_timeout_ms);
    template<typename T, typename Enable> friend struct managed;
    friend class swift_lattice;
    friend class projection_service;
    friend struct projection_operation_state;
    friend struct database_projection_capture;
    // The update hook may query globalId through database::query(). That
    // nested query must not drain a prior row's dirty state while the outer
    // SQLite statement still owns its connection mutex. Track the actual
    // callback frame, including callers that step SQLite directly.
    struct update_hook_scope {
        sqlite3* connection;
        update_hook_scope* previous;
        static inline thread_local update_hook_scope* current = nullptr;
        explicit update_hook_scope(database& db) noexcept
            : update_hook_scope(db.db_) {}
        explicit update_hook_scope(sqlite3* handle) noexcept
            : connection(handle), previous(current) { current = this; }
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

    // SQLite retains this address as update-hook userdata. Heap ownership
    // keeps it stable when a database wrapper moves; physical identity does
    // not depend on which writer the lattice currently publishes. The raw
    // lattice owner must still outlive all uses of the connection.
    struct sync_apply_chunk_state {
        enum class phase { not_started, active, committed, rolled_back };
        phase state = phase::not_started;
        // A pre-commit callback is not proof of durable settlement. It only
        // prevents an admitted reset from following COMMIT into a successor,
        // including memory and no-write transactions with no WAL callback.
        bool commit_attempted = false;
        // Only self-owned private maintenance uses this restriction. Sync and
        // caller-owned reset markers remain observational by default.
        enum class commit_policy { observational, owner_body, owner_finalizing };
        commit_policy policy = commit_policy::observational;
        bool premature_commit = false;
        // A rollback consumes this reservation before a successor can write.
        // The operation's separate owner-lifetime hold lasts through unwind.
        bool owns_recovery_reservation = false;
    };
    struct lattice_update_hook_context {
        lattice_db* owner = nullptr;
        sqlite3* connection = nullptr;
        // Only touched while owning this physical connection's SQLite mutex.
        // The added hook path uses POD, with no allocation/SQL/user callback.
        sync_apply_chunk_state* sync_chunk = nullptr;
        bool recovery_delivery_deferred = false;
        bool entry_cursor_active = false;
        bool entry_cursor_present = false;
        int64_t entry_cursor_last = 0;
        bool consume_recovery_reservation(sync_apply_chunk_state* settlement) noexcept {
            if (!settlement || !settlement->owns_recovery_reservation) return false;
            settlement->owns_recovery_reservation = false;
            entry_cursor_active = false;
            entry_cursor_present = false;
            recovery_delivery_deferred = false;
            return true;
        }
        void note_settled(bool committed) noexcept {
            if (!sync_chunk) return;
            sync_chunk->state = committed ? sync_apply_chunk_state::phase::committed
                                          : sync_apply_chunk_state::phase::rolled_back;
            // Consume before callbacks can open a successor transaction.
            sync_chunk = nullptr;
        }
    };
    std::unique_ptr<lattice_update_hook_context> lattice_update_hook_context_;

    // Private multi-statement maintenance ownership. FULLMUTEX alone only
    // serializes individual SQLite calls; another thread must not join this
    // owner's transaction between its safety reads and writes. The caller
    // takes any store gate first and drains notifications after both scopes.
    struct maintenance_scope {
        database& owner;
        sqlite3_mutex* mutex;
        maintenance_scope* previous = nullptr;
        static inline thread_local maintenance_scope* current = nullptr;
        static bool idle(database& db) noexcept;
        static void probe_before_store_gate(database& db);
        // False is only the initial no-effect SQLite mutex contention probe.
        static bool try_probe_before_store_gate(database& db);
        explicit maintenance_scope(database& db);
        ~maintenance_scope() noexcept;
        maintenance_scope(const maintenance_scope&) = delete;
        maintenance_scope& operator=(const maintenance_scope&) = delete;
        static bool active_for(sqlite3* connection) noexcept {
            for (auto* frame = current; frame; frame = frame->previous) {
                if (frame->owner.db_ == connection) return true;
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
    // Private construction capability; no caller can manufacture the key.
    database(const std::string& path, open_mode mode, int busy_timeout_ms,
             std::shared_ptr<database_read_control> read_control, initialization_key key);
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

    /// Result of a wal_checkpoint() call. rc retains the PRAGMA-style result
    /// code; busy is 1 when the checkpoint could not complete because a
    /// reader/writer held the WAL; log_frames/checkpointed mirror the PRAGMA
    /// row (-1 when unavailable). Native SQLITE_BUSY maps to rc=SQLITE_OK,
    /// busy=1; other native errors retain rc=SQLITE_ERROR and unavailable frames.
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

    /// Raw access retires canonical admission before pointer publication and
    /// refuses during its bootstrap, transaction or active statement. Public
    /// transaction-hook replacement likewise refuses attached policy custody.
    /// Raw access permanently opts this connection out of strict borrowed
    /// memory projection capture: external SQLite handlers cannot be restored
    /// or proven read-only. Waits behind an active capture before exposing it.
    sqlite3* handle() const;

    // Bind a value to a prepared statement (public for lattice_db bulk insert)
    void bind_value(sqlite3_stmt* stmt, int index, const column_value_t& value);

    /// Whether close() has been called. Ops check this and short-circuit.
    bool is_closed() const { return closed_.load(std::memory_order_acquire); }

private:
    // Trusted Core/bridge callers only; never return this pointer to a client.
    sqlite3* internal_handle() const noexcept { return db_; }
    sqlite3* db_ = nullptr;
    mutable std::atomic<bool> raw_handle_escaped_{false};
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
    struct txn_hook_callbacks {
        std::function<void()> settled, rolled_back;
        txn_hook_callbacks(std::function<void()>&& success, std::function<void()>&& rollback)
            : settled(std::move(success)), rolled_back(std::move(rollback)) {}
    };
    // Construct/destroy callable targets outside SQLite. Under its mutex only
    // shared_ptr ownership moves; std::function moves/swaps may run user code.
    std::shared_ptr<txn_hook_callbacks> txn_hooks_;
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
