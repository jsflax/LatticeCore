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
#include <variant>
#include <unordered_map>

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
    enum class type { audit_log, ack };

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
    int max_reconnect_attempts = 6;
    double base_delay_seconds = 1.0;
    size_t chunk_size = 1000;  // Max events per message

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

class synchronizer {
public:
    using on_sync_complete_handler = std::function<void(const std::vector<std::string>& synced_ids)>;
    using on_error_handler = std::function<void(const std::string& error)>;
    using on_state_change_handler = std::function<void(bool connected)>;

    synchronizer(lattice_db& db, const sync_config& config,
                 std::shared_ptr<scheduler> scheduler = nullptr);

    /// Construct with an externally-provided transport (for IPC).
    /// The transport is already connected or will be connected via connect().
    synchronizer(lattice_db& db, const sync_config& config,
                 std::unique_ptr<sync_transport> transport,
                 std::shared_ptr<scheduler> scheduler = nullptr);

    ~synchronizer();

    // Non-copyable, non-moveable
    synchronizer(const synchronizer&) = delete;
    synchronizer& operator=(const synchronizer&) = delete;
    synchronizer(synchronizer&&) = delete;
    synchronizer& operator=(synchronizer&&) = delete;

    // Connection management
    void connect();
    void disconnect();
    bool is_connected() const { return is_connected_; }

    // Manual sync trigger (uploads pending changes)
    void sync_now();

    // Sync filter management
    void update_sync_filter(std::vector<sync_filter_entry> filter);
    void clear_sync_filter();

    // Event handlers
    void set_on_sync_complete(on_sync_complete_handler handler) { on_sync_complete_ = std::move(handler); }
    void set_on_error(on_error_handler handler) { on_error_ = std::move(handler); }
    void set_on_state_change(on_state_change_handler handler) { on_state_change_ = std::move(handler); }

private:
    lattice_db& db_;
    sync_config config_;
    std::shared_ptr<scheduler> scheduler_;
    std::unique_ptr<sync_transport> ws_client_;

    std::atomic<bool> is_connected_{false};
    std::atomic<bool> should_reconnect_{true};  // Set false on explicit disconnect
    std::atomic<int> reconnect_attempts_{0};

    on_sync_complete_handler on_sync_complete_;
    on_error_handler on_error_;
    on_state_change_handler on_state_change_;

    // Observer for AuditLog changes (triggers upload when new local entries appear)
    uint64_t audit_log_observer_id_{0};

    // Internal handlers
    void on_websocket_open();
    void on_transport_message(const transport_message& msg);
    void on_websocket_error(const std::string& error);
    void on_websocket_close(int code, const std::string& reason);

    // Sync operations
    void upload_pending_changes();
    void apply_remote_changes(const std::vector<audit_log_entry>& entries);
    void mark_as_synced(const std::vector<std::string>& global_ids);

    // Sync filter helpers
    // Returns nullopt if table not in filter; otherwise returns the where_clause (which may itself be nullopt for "all rows")
    std::optional<std::optional<std::string>> get_filter_for_table(const std::string& table_name) const;
    bool is_table_in_filter(const std::string& table_name) const;
    bool row_matches_filter(const std::string& table_name, const std::string& global_row_id);
    audit_log_entry build_insert_entry_from_current_row(const std::string& table_name, const std::string& global_row_id);

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
// Helper: Query AuditLog entries from database
// ============================================================================

std::vector<audit_log_entry> query_audit_log(database& db,
    bool only_unsynced = false,
    std::optional<std::string> after_global_id = std::nullopt);

/// Query unsynced audit log entries for a specific synchronizer using _lattice_sync_state.
/// Returns entries that have no sync_state row (or is_synchronized=0) for the given sync_id.
/// Unlike the single-sync query, this does NOT filter on isFromRemote — any entry not yet
/// synced by this sync_id is returned, enabling cross-transport relay.
std::vector<audit_log_entry> query_audit_log_for_sync(database& db, const std::string& sync_id);

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
void apply_remote_changes(lattice_db& db, const std::vector<audit_log_entry>& entries);

/// Apply remote audit log entries and mark them as synced for the receiving sync_id.
/// Unlike apply_remote_changes, this uses per-synchronizer sync state so that OTHER
/// synchronizers (WSS, BLE, etc.) see the entries as pending and relay them.
/// The receiving sync_id's _lattice_sync_state is set to 1 (preventing re-upload = loop prevention).
void apply_remote_changes_for(lattice_db& db,
                              const std::vector<audit_log_entry>& entries,
                              const std::string& sync_id);

} // namespace lattice

#endif // __cplusplus
