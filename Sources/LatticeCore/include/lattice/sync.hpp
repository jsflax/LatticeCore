#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include "db.hpp"
#include "network.hpp"
#include "scheduler.hpp"
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <chrono>
#include <functional>
#include <memory>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

namespace lattice {

// Forward declaration
class lattice_db;

// ============================================================================
// AnyProperty - matches Swift's AnyProperty enum
// ============================================================================

enum class any_property_kind : int {
    int_kind = 0,
    int64_kind = 1,
    string_kind = 2,
    date_kind = 3,
    null_kind = 4,
    float_kind = 5,
    data_kind = 6,
    double_kind = 7
};

struct any_property {
    using value_type = std::variant<
        std::nullptr_t,     // null
        int64_t,            // int, int64
        double,             // float, double, date (as timestamp)
        std::string,        // string
        std::vector<uint8_t> // data
    >;

    any_property_kind kind = any_property_kind::null_kind;
    value_type value = nullptr;

    // Constructors
    any_property() = default;
    any_property(std::nullptr_t) : kind(any_property_kind::null_kind), value(nullptr) {}
    any_property(int v) : kind(any_property_kind::int_kind), value(static_cast<int64_t>(v)) {}
    any_property(int64_t v) : kind(any_property_kind::int64_kind), value(v) {}
    any_property(float v) : kind(any_property_kind::float_kind), value(static_cast<double>(v)) {}
    any_property(double v) : kind(any_property_kind::double_kind), value(v) {}
    any_property(const std::string& v) : kind(any_property_kind::string_kind), value(v) {}
    any_property(std::string&& v) : kind(any_property_kind::string_kind), value(std::move(v)) {}
    any_property(const char* v) : kind(any_property_kind::string_kind), value(std::string(v)) {}
    any_property(const std::vector<uint8_t>& v) : kind(any_property_kind::data_kind), value(v) {}
    any_property(std::vector<uint8_t>&& v) : kind(any_property_kind::data_kind), value(std::move(v)) {}

    // Factory for date (stored as double timestamp)
    static any_property date(double timestamp) {
        any_property p;
        p.kind = any_property_kind::date_kind;
        p.value = timestamp;
        return p;
    }

    // Check if null
    bool is_null() const { return kind == any_property_kind::null_kind; }

    // Convert to column_value_t for database operations
    column_value_t to_column_value() const;

    // Create from column_value_t
    static any_property from_column_value(const column_value_t& v);
};

// Type alias for changed fields map
using changed_fields_map = std::unordered_map<std::string, any_property>;

// ============================================================================
// AuditLog Entry - matches Lattice.swift's AuditLog model
// ============================================================================

struct audit_log_entry {
    int64_t id = 0;
    std::string global_id;
    std::string table_name;
    std::string operation;  // "INSERT", "UPDATE", "DELETE"
    int64_t row_id = 0;
    std::string global_row_id;
    changed_fields_map changed_fields;       // Map of property name -> any_property
    std::vector<std::string> changed_fields_names; // List of changed field names
    std::string timestamp;
    bool is_from_remote = false;
    bool is_synchronized = false;

    // Generate SQL instruction from this audit entry
    // Returns {sql, params} for executing the change
    // schema maps column_name -> column_type for decoding hex-encoded BLOBs
    std::pair<std::string, std::vector<column_value_t>> generate_instruction(
        const std::unordered_map<std::string, column_type>& schema = {}) const;

    // Serialize to JSON
    std::string to_json() const;

    // Deserialize from JSON
    static std::optional<audit_log_entry> from_json(const std::string& json);

    // Serialize changed_fields to JSON string (for database storage)
    std::string changed_fields_to_json() const;

    // Serialize changed_fields_names to JSON string (for database storage)
    std::string changed_fields_names_to_json() const;

    // Parse changed_fields from JSON string (from database)
    static changed_fields_map parse_changed_fields(const std::string& json);

    // Parse changed_fields_names from JSON string (from database)
    static std::vector<std::string> parse_changed_fields_names(const std::string& json);
};

// Type alias for Swift interop (can't specialize templates from Swift)
using AuditLogEntryVector = std::vector<audit_log_entry>;

// ============================================================================
// ServerSentEvent - matches Lattice.swift's ServerSentEvent enum
// ============================================================================

struct server_sent_event {
    enum class type { audit_log, ack, replay_request };

    type event_type;
    std::vector<audit_log_entry> audit_logs;  // For audit_log type
    std::vector<std::string> acked_ids;       // For ack type (UUIDs)

    // Serialize to JSON
    std::string to_json() const;

    // Deserialize from JSON
    static std::optional<server_sent_event> from_json(const std::string& json);

    // Factory methods
    static server_sent_event make_audit_log(std::vector<audit_log_entry> logs) {
        return {type::audit_log, std::move(logs), {}};
    }

    static server_sent_event make_ack(std::vector<std::string> ids) {
        return {type::ack, {}, std::move(ids)};
    }

    /// Fresh-peer catch-up request. IPC transports ONLY — never sent over WSS
    /// (deployed servers on older protocol versions would fail to parse it).
    static server_sent_event make_replay_request() {
        return {type::replay_request, {}, {}};
    }
};

// ============================================================================
// Sync Configuration
// ============================================================================

// ============================================================================
// Sync Filter
// ============================================================================

struct sync_filter_entry {
    std::string table_name;
    std::optional<std::string> where_clause;  // nullopt = all rows
};

struct sync_config {
    std::string websocket_url;
    std::string authorization_token;
    int max_reconnect_attempts = 0;  // 0 = unlimited
    double base_delay_seconds = 1.0;
    double max_delay_seconds = 60.0;
    size_t chunk_size = 1000;  // Max events per message

    /// A connection must stay open at least this long before a subsequent
    /// drop resets the reconnect backoff. Guards against flapping endpoints
    /// (open→error every cycle) re-arming ~1s reconnect storms.
    int64_t stable_connection_ms = 60'000;

    /// Upload-tick coalescing window. The first request after an idle period
    /// dispatches immediately (leading edge — zero added latency for isolated
    /// writes and synchronous sync_now() semantics); further requests inside
    /// the window collapse into ONE trailing-edge tick. 0 = legacy behavior:
    /// every request dispatches its own scheduler pass — which, combined with
    /// four zero-delay re-invocation sites, busy-spun the daemon at 90-160%
    /// CPU. Engram sets WSS=750ms (each tick re-diffs the audit log; pacing
    /// it is pure win) and IPC=50ms (relay stays effectively latency-free via
    /// the leading edge). No pacer thread on Emscripten — requests degrade to
    /// direct dispatch.
    int upload_coalesce_ms = 0;

    /// Periodic WAL maintenance, driven by the pacer thread (requires
    /// upload_coalesce_ms > 0; no-op on Emscripten). PASSIVE checkpoints run
    /// unconditionally on this cadence, even while disconnected, which is
    /// precisely when the WAL previously grew unbounded (observed 1.1GB:
    /// long-lived sync connections pinned the autocheckpoint while the
    /// daemon busy-spun). TRUNCATE additionally runs when the synchronizer
    /// is idle (nothing in flight) to return the file to ~zero. 0 disables.
    int checkpoint_passive_interval_ms = 60'000;
    int checkpoint_truncate_interval_ms = 300'000;

    /// Upload filter. nullopt = sync everything (default).
    /// Empty vector = sync nothing. Non-empty = whitelist.
    std::optional<std::vector<sync_filter_entry>> sync_filter;

    /// Unique identifier for this synchronizer instance (e.g. "wss:<url>" or "ipc:<channel>").
    /// When set, per-synchronizer sync state is tracked in _lattice_sync_state
    /// instead of using the single isSynchronized column on AuditLog.
    /// This enables multiple synchronizers per database without interference.
    std::string sync_id;

    /// All active sync_ids on this database (including this one).
    /// Used for eager cleanup: when all sync_ids have synced an entry,
    /// _lattice_sync_state rows are deleted and isSynchronized=1 is set.
    std::vector<std::string> all_active_sync_ids;
};

// ============================================================================
// Synchronizer - matches Lattice.swift's Synchronizer actor
// ============================================================================

class synchronizer_base {
public:
    using on_sync_complete_handler = std::function<void(const std::vector<std::string>& synced_ids)>;
    using on_error_handler = std::function<void(const std::string& error)>;
    using on_state_change_handler = std::function<void(bool connected)>;

    // ============================================================================
    // Sync Progress
    // ============================================================================

    struct sync_progress {
        int64_t pending_upload = 0;
        int64_t total_upload = 0;    // snapshot at start of batch
        int64_t acked = 0;
        int64_t received = 0;        // cumulative downloads
    };

    using on_progress_handler = std::function<void(const sync_progress&)>;

    virtual ~synchronizer_base();

protected:
    synchronizer_base() = default;

public:
    // Non-copyable, non-moveable
    synchronizer_base(const synchronizer_base&) = delete;
    synchronizer_base& operator=(const synchronizer_base&) = delete;
    synchronizer_base(synchronizer_base&&) = delete;
    synchronizer_base& operator=(synchronizer_base&&) = delete;

    // Connection management
    void connect();
    void disconnect();
    bool is_connected() const { return is_connected_; }

    // Manual sync trigger (uploads pending changes)
    void sync_now();

    /// Liveness guard for the detached ack-timeout thread. The thread holds
    /// a shared_ptr; the destructor flips `alive` under the mutex BEFORE
    /// teardown, and the thread only touches the synchronizer while holding
    /// the same mutex with alive==true — no use-after-free window.
    struct ack_retry_guard {
        std::mutex m;
        bool alive = true;
    };

    /// Bounded flush before teardown: if connected, run one upload pass and
    /// wait until all sent entries are ACKed or the deadline passes. Dropping
    /// the last reference to a Lattice mid-write must not cut in-flight sync
    /// entries — the daemon shutting down, a task-scoped instance going out
    /// of scope, and the A→B handoff all rely on this.
    /// Never blocks past `deadline`; returns immediately when disconnected.
    void drain(std::chrono::steady_clock::time_point deadline);

    std::shared_ptr<ack_retry_guard> ack_guard_ = std::make_shared<ack_retry_guard>();

    // Sync filter management
    void update_sync_filter(std::vector<sync_filter_entry> filter);
    void clear_sync_filter();

    // Event handlers
    void set_on_sync_complete(on_sync_complete_handler handler) { on_sync_complete_ = std::move(handler); }
    void set_on_error(on_error_handler handler) { on_error_ = std::move(handler); }
    void set_on_state_change(on_state_change_handler handler) {
        on_state_change_ = std::move(handler);
        if (on_state_change_ && is_connected_) {
            scheduler_->invoke([this] { on_state_change_(true); });
        }
    }

    // Progress
    void set_on_progress(on_progress_handler handler);

    sync_progress get_progress() const;

protected:
    /// Database accessor — set by subclass constructors.
    lattice_db& db() { return *db_ptr_; }
    lattice_db* db_ptr_ = nullptr;

    /// Owned database (native only). Stored in the base class so it outlives
    /// ~synchronizer_base() — base members are destroyed after the base
    /// destructor body, avoiding use-after-free on db_ptr_.
    std::unique_ptr<lattice_db> owned_db_;

    /// Common init — call from subclass constructors after db is set up.
    void init_sync(const sync_config& config, std::shared_ptr<scheduler> sched);
    void init_sync(const sync_config& config, std::shared_ptr<scheduler> sched,
                   std::unique_ptr<sync_transport> transport);

    sync_config config_;
    std::shared_ptr<scheduler> scheduler_;
    std::unique_ptr<sync_transport> ws_client_;

    std::atomic<bool> is_connected_{false};
    std::atomic<bool> is_destroyed_{false};  // Set in destructor; guards scheduled lambdas
    std::atomic<bool> should_reconnect_{true};  // Set false on explicit disconnect
    std::atomic<int> reconnect_attempts_{0};
    // steady_clock ms of the last successful open. Backoff resets only after a
    // connection proved STABLE (open ≥ config_.stable_connection_ms before
    // dropping) — resetting on open re-armed ~1s reconnect storms against a
    // flapping endpoint, and a schedule-time check re-arms during any outage
    // longer than the stability window. The check therefore lives in
    // on_websocket_close/error.
    std::atomic<int64_t> last_open_time_ms_{0};
    /// Reset reconnect_attempts_ iff a genuinely-open connection survived
    /// ≥ config_.stable_connection_ms. Called from on_websocket_close/error
    /// with was_open = is_connected_.exchange(false).
    void maybe_reset_backoff_after_stable_connection(bool was_open);

    /// Coalesced upload trigger — replaces direct scheduler dispatch at the
    /// hot re-invocation sites (observer, post-ACK, ack-timeout resend,
    /// sync_now). Leading edge dispatches inline; in-window requests are
    /// absorbed by the pacer thread's single trailing-edge tick. Thread-safe;
    /// no-op after destruction begins.
    void request_upload();

    /// Consecutive ack-timeout failures (no ACK before the resend deadline).
    /// Grows the resend deadline (10s, 20s, 40s… capped) so a stalled server
    /// is not re-hammered with the same window at a fixed cadence. Reset on
    /// any ACK.
    std::atomic<int> ack_resend_failures_{0};

#ifndef __EMSCRIPTEN__
    // Pacer thread: owns trailing-edge coalescing and periodic WAL
    // maintenance. Started by init_sync when upload_coalesce_ms > 0; joined
    // in the destructor BEFORE scheduler shutdown (it only ever enqueues to
    // the scheduler, never blocks on it).
    std::thread pacer_thread_;
    std::mutex pacer_mutex_;
    std::condition_variable pacer_cv_;
    bool pacer_stop_ = false;
    std::chrono::steady_clock::time_point next_allowed_tick_{};
    std::chrono::steady_clock::time_point last_passive_ckpt_{};
    std::chrono::steady_clock::time_point last_truncate_ckpt_{};
    void start_pacer();
    void stop_pacer();
    /// Called from the pacer loop on every wakeup (request or timeout).
    /// Dispatches PASSIVE/TRUNCATE checkpoints per the config cadences;
    /// TRUNCATE only when idle (in-flight empty — pending mirrors it).
    void maybe_checkpoint();
#endif
    std::atomic<bool> upload_requested_{false};  // Coalesces observer-triggered uploads
    std::atomic<uint64_t> filter_version_{0};     // Bumped on each update_sync_filter; reconcile checks before acting

    // In-flight tracking: entries sent but not yet ACK'd.
    // Prevents upload_pending_changes from re-sending entries on each cycle.
    // Accessed from scheduler thread (upload) and WebSocket/IPC thread (ACK) — needs mutex.
    std::mutex in_flight_mutex_;
    std::unordered_set<std::string> in_flight_ids_;

    on_sync_complete_handler on_sync_complete_;
    on_error_handler on_error_;
    on_state_change_handler on_state_change_;
    mutable std::mutex progress_handler_mutex_;
    on_progress_handler on_progress_;

    // Progress tracking
    std::atomic<int64_t> progress_pending_upload_{0};
    std::atomic<int64_t> progress_total_upload_{0};
    std::atomic<int64_t> progress_acked_{0};
    std::atomic<int64_t> progress_received_{0};

    void fire_progress();

    // Observer for AuditLog changes (triggers upload when new local entries appear)
    uint64_t audit_log_observer_id_{0};

    // Constructor helpers
    void setup_transport_handlers();
    void setup_observer();

    // Internal handlers
    void on_websocket_open();
    void on_transport_message(const transport_message& msg);
    void on_websocket_error(const std::string& error);
    void on_websocket_close(int code, const std::string& reason);

    // Sync operations
    void upload_pending_changes();
    std::vector<std::string> apply_remote_changes(const std::vector<audit_log_entry>& entries);
    void mark_as_synced(const std::vector<std::string>& global_ids);

    // upload_pending_changes decomposed phases
    struct classified_entries {
        std::vector<audit_log_entry> to_send;
        std::vector<int64_t> to_mark_synced;
    };
    std::vector<audit_log_entry> query_pending_entries();
    classified_entries classify_entries(std::vector<audit_log_entry>& entries);
    void classify_delete(audit_log_entry& entry, classified_entries& result);
    void classify_insert_or_update(audit_log_entry& entry, const std::string& filter_table, bool is_link_table, classified_entries& result);
    void mark_skipped_synced(const std::vector<int64_t>& to_mark_synced);
    void send_entries(std::vector<audit_log_entry>& entries);

    // Sync filter helpers
    // Returns nullopt if table not in filter; otherwise returns the where_clause (which may itself be nullopt for "all rows")
    std::optional<std::optional<std::string>> get_filter_for_table(const std::string& table_name) const;
    bool is_table_in_filter(const std::string& table_name) const;
    bool row_matches_filter(const std::string& table_name, const std::string& global_row_id);
    std::optional<audit_log_entry> build_insert_entry_from_current_row(const std::string& table_name, const std::string& global_row_id);

    // Sync set management
    void sync_set_add(const std::string& table_name, const std::string& global_row_id);
    void sync_set_remove(const std::string& table_name, const std::string& global_row_id);
    bool sync_set_contains(const std::string& table_name, const std::string& global_row_id);

    // Reconciliation (called on filter change)
    void reconcile_sync_filter();

    // Reconnection
    void schedule_reconnect();

    // Get last received event ID for checkpoint
    std::optional<std::string> get_last_received_event_id();
};

// ============================================================================
// Platform-specific synchronizer subclasses
// ============================================================================

#ifdef __EMSCRIPTEN__

/// Emscripten: borrows the parent's lattice_db (single-threaded, no need for
/// a separate connection — avoids OPFS exclusive lock conflicts).
class synchronizer : public synchronizer_base {
public:
    synchronizer(lattice_db& db_ref, const sync_config& config);
    synchronizer(lattice_db& db_ref, const sync_config& config,
                 std::unique_ptr<sync_transport> transport);
};

#else

/// Native: owns a dedicated lattice_db (separate connection on its own thread).
class synchronizer : public synchronizer_base {
public:
    synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config);
    synchronizer(std::unique_ptr<lattice_db> db, const sync_config& config,
                 std::unique_ptr<sync_transport> transport);
};

#endif

// ============================================================================
// Helper: Query AuditLog entries from database
// ============================================================================

std::vector<audit_log_entry> query_audit_log(database& db,
    bool only_unsynced = false,
    std::optional<std::string> after_global_id = std::nullopt);

/// Query unsynced audit log entries for a specific synchronizer using _lattice_sync_state.
/// Returns entries that have no sync_state row (or is_synchronized=0) for the given sync_id.
/// Unlike the single-sync query, this does NOT filter on isFromRemote — any entry not yet
/// synced by this sync_id is returned, enabling cross-transport relay.
std::vector<audit_log_entry> query_audit_log_for_sync(
    database& db,
    const std::string& sync_id,
    const std::optional<std::vector<sync_filter_entry>>& sync_filter = std::nullopt);

// Mark audit entries as synchronized (with observer notification)
void mark_audit_entries_synced(lattice_db& db, const std::vector<std::string>& global_ids);

/// Mark audit entries as synchronized for a specific sync_id in _lattice_sync_state.
/// Also performs eager cleanup: when all active sync_ids have synced an entry,
/// deletes _lattice_sync_state rows and sets isSynchronized=1 on AuditLog.
void mark_audit_entries_synced_for(lattice_db& db,
                                   const std::vector<std::string>& global_ids,
                                   const std::string& sync_id,
                                   const std::vector<std::string>& all_active_sync_ids);

// Get events after a checkpoint (for server-side sync)
std::vector<audit_log_entry> events_after(database& db, const std::optional<std::string>& checkpoint_global_id);

// Apply remote audit log entries to a database (server-side receive path)
// Disables sync triggers, executes model SQL, inserts AuditLog records, re-enables triggers.
// Returns global IDs of successfully applied entries (entries that failed SQL execution are excluded).
std::vector<std::string> apply_remote_changes(lattice_db& db, const std::vector<audit_log_entry>& entries);

/// Apply remote audit log entries and mark them as synced for the receiving sync_id.
/// Unlike apply_remote_changes, this uses per-synchronizer sync state so that OTHER
/// synchronizers (WSS, BLE, etc.) see the entries as pending and relay them.
/// The receiving sync_id's _lattice_sync_state is set to 1 (preventing re-upload = loop prevention).
/// Returns global IDs of successfully applied entries.
std::vector<std::string> apply_remote_changes_for(lattice_db& db,
                              const std::vector<audit_log_entry>& entries,
                              const std::string& sync_id);

// Replication slot management for sync-safe compaction.
// Each synchronizer registers a slot; compaction only deletes entries
// below the minimum confirmed_audit_id across all active slots.

/// Register (or touch) a replication slot for the given sync_id.
void register_replication_slot(database& db, const std::string& sync_id);

/// Advance a replication slot's confirmed cursor (monotonically forward only).
void advance_replication_slot(database& db, const std::string& sync_id, int64_t confirmed_audit_id);

/// Remove a replication slot.
void remove_replication_slot(database& db, const std::string& sync_id);

} // namespace lattice

#endif // __cplusplus
