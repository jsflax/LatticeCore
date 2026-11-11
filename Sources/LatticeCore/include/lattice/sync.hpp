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
#include <set>
#include <mutex>
#include <limits>
#include <stdexcept>
#include <utility>
#include <type_traits>

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

    /// A5 — provenance marker for full-row snapshot entries synthesized from
    /// LOCAL state (reconcile Phase-2 additions, classify's UPDATE→INSERT
    /// conversion, nuclear-compact history regeneration) rather than captured
    /// from a live write. Synthesized rows may be STALE relative to a peer
    /// that has newer edits; apply treats a synthesized INSERT for an
    /// already-existing row as insert-if-absent (skip, ack) instead of the
    /// unconditional every-column upsert — re-expose/re-join/nuclear-compact
    /// must never resurrect a peer's tombstones or revert newer edits.
    /// Serialized as an optional "synthesized" JSON key; old receivers
    /// ignore it (legacy upsert behavior — acceptable, they predate groups).
    bool synthesized = false;

    /// Transport-only completeness metadata: these named no_history fields
    /// contain an observed SQL NULL, not a raw UPDATE-history placeholder.
    /// Optional on the wire; not persisted in AuditLog. Old peers ignore the
    /// key and conservatively drop these clears. Receivers still check their
    /// installed no_history flags and actual column nullability.
    std::vector<std::string> no_history_nulls;

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
    enum class type { audit_log, ack, replay_request, nack, catch_up_complete };

    type event_type;
    std::vector<audit_log_entry> audit_logs;  // For audit_log type
    std::vector<std::string> acked_ids;       // For ack type (UUIDs)
    std::vector<std::string> nacked_ids;      // Rejected uploads; never acknowledgments
    std::string nack_reason;

    // Serialize to JSON
    std::string to_json() const;

    // Deserialize from JSON
    static std::optional<server_sent_event> from_json(const std::string& json);
    /// Optional payload-free diagnostic: fixed category and, for an invalid
    /// audit entry, its zero-based position. Never includes wire values or
    /// parser exception text. Cleared on a successful parse.
    static std::optional<server_sent_event> from_json(
        const std::string& json, std::string* error_out);

    /// Strict value shapes for opt-in immutable receive, composed with the
    /// ordinary parser's complete-frame and control-message checks.
    static std::optional<server_sent_event> from_json_immutable(const std::string& json);

    // Only emitted by a server after the negotiated initial replay.
    static server_sent_event make_catch_up_complete() {
        return {type::catch_up_complete, {}, {}, {}, {}};
    }

    // Factory methods
    static server_sent_event make_audit_log(std::vector<audit_log_entry> logs) {
        return {type::audit_log, std::move(logs), {}, {}, {}};
    }

    static server_sent_event make_ack(std::vector<std::string> ids) {
        return {type::ack, {}, std::move(ids), {}, {}};
    }

    static server_sent_event make_nack(std::vector<std::string> ids, std::string reason) {
        return {type::nack, {}, {}, std::move(ids), std::move(reason)};
    }

    /// Fresh-peer catch-up request. IPC transports ONLY — never sent over WSS
    /// (deployed servers on older protocol versions would fail to parse it).
    static server_sent_event make_replay_request() {
        return {type::replay_request, {}, {}, {}, {}};
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

// Audit JSON keeps the existing wire protocol. This bounds an encoded message,
// independently of event count; the byte-accounted upload window is twice this.
inline constexpr size_t sync_max_encoded_message_bytes = 4 * 1024 * 1024;

struct encoded_audit_log_prefix {
    std::string json;
    size_t count = 0;
    // Conservative per-entry reservation (each carries its own envelope charge).
    // Sum of outstanding reservations bounds bytes even after partial ACKs.
    std::vector<size_t> entry_charges;
};

/// Encode only a contiguous prefix. A first row that cannot fit a whole message
/// throws length_error; it is never silently skipped. A temporarily full byte
/// window returns count 0. No audit/floor/in-flight state is changed here.
encoded_audit_log_prefix encode_audit_log_prefix(
    const std::vector<audit_log_entry>& entries, size_t start, size_t max_count,
    size_t max_bytes = sync_max_encoded_message_bytes,
    size_t available_window_bytes = 2 * sync_max_encoded_message_bytes);

namespace detail {
// Recover the exact row bytes from a single-entry bounded encoder result.
// This does not serialize again or cache across admission/currentness checks.
std::string single_encoded_audit_log_json(const encoded_audit_log_prefix& encoded);

// Admission is synchronized, but no gate lock is held over SQLite/transport/user work. Holding
// such a lock in an observer called under SQLite would invert the scheduler's database locks.
struct sync_callback_gate {
    std::mutex mutex;
    std::condition_variable drained;
    bool accepting=true;
    size_t active=0;
    std::map<std::thread::id,size_t> threads;
    std::shared_ptr<void> channel_lifetime;
    const std::weak_ptr<void> owner;
    const bool owner_required;
    explicit sync_callback_gate(std::shared_ptr<void> lifetime,std::weak_ptr<void> owner={},bool required=false)
        :channel_lifetime(std::move(lifetime)),owner(std::move(owner)),owner_required(required) {}
    bool enter() {
        std::lock_guard<std::mutex> lock(mutex);
        if(!accepting) return false;
        ++threads[std::this_thread::get_id()];++active;return true;
    }
    void leave() {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found=threads.find(std::this_thread::get_id());
        if(--found->second==0) threads.erase(found);
        if(--active==0) drained.notify_all();
    }
    bool revoke_and_wait() {
        std::unique_lock<std::mutex> lock(mutex);accepting=false;
        // A raw/stack owner deleting itself from its callback cannot be kept alive by a mutex.
        // Report this unresolved ownership path to the destructor; never deadlock or abort here.
        if(threads.count(std::this_thread::get_id())) return false;
        drained.wait(lock,[&] {return active==0;});return true;
    }
};
// Keep this non-template cleanup owner outside the generic callback lambda so
// its destructor has an ordinary inline definition on every supported toolchain.
struct sync_callback_ticket {
    std::shared_ptr<sync_callback_gate> gate;
    ~sync_callback_ticket() {gate->leave();}
};
template<class Function>
auto guard_sync_callback(std::shared_ptr<sync_callback_gate> gate,Function function) {
    return [gate=std::move(gate),function=std::move(function)](auto&&... args) mutable {
        if(!gate) return;
        auto owner=gate->owner.lock();
        if(gate->owner_required && !owner) return;
        if(!gate->enter()) return;
        sync_callback_ticket admitted{gate};
        function(std::forward<decltype(args)>(args)...);
    };
}
std::shared_ptr<void> acquire_sync_channel_lifetime(lattice_db&,const std::string& sync_id);
void with_quiescent_sync_channel(lattice_db&,const std::string& sync_id,const std::function<void()>& operation);
inline std::string sync_reset_meta_key(const std::string& sync_id) {
    if(sync_id.size()>4096 || sync_id.find('\0')!=std::string::npos)
        throw std::invalid_argument("Sync channel identity must contain at most 4096 bytes without NUL");
    return "sync_reset_through_v1:"+sync_id;
}
inline int64_t parse_sync_reset_cutoff(const std::string& text) {
    if(text.empty() || text.size()>19 || (text.size()>1 && text[0]=='0'))
        throw std::runtime_error("Invalid sync reset cutoff");
    int64_t value=0;
    for(char c:text) {
        if(c<'0' || c>'9' || value>(std::numeric_limits<int64_t>::max()-(c-'0'))/10)
            throw std::runtime_error("Invalid sync reset cutoff");
        value=value*10+(c-'0');
    }
    return value;
}
struct sync_delivery {
    int64_t receipt_id = 0; // durable storage identity, independent of optional legacy provenance
    int64_t source_id = 0;
    std::string source_global_id;
    audit_log_entry wire;
    std::string wire_json;
    std::string routing_key; // exact bounded filter configuration last admitted for this receipt
    int membership = 0; // -1 remove, 0 unchanged, 1 add; committed with ACK
    size_t charge = 0;
    bool legacy_receipt = false; // released v7 receipt may outlive its compacted source
};
}

struct sync_config {
    // Internal owner admission only; never part of a wire frame or persisted routing policy.
    std::shared_ptr<std::atomic<bool>> owner_active;

    std::string websocket_url;
    std::string authorization_token;
    std::string pinned_server_certificate_sha256;
    bool uploads_enabled = true;
    int max_reconnect_attempts = 0;  // 0 = unlimited
    double base_delay_seconds = 1.0;
    double max_delay_seconds = 60.0;
    size_t chunk_size = 1000;  // Max events per message

    /// Base ACK-progress deadline for the resend watchdog (doubles with
    /// consecutive failures, capped at 5 min). The default reproduces the
    /// former hard-coded 10s exactly; tests and benches shrink it so
    /// deadline-release scenarios run in milliseconds instead of tens of
    /// seconds.
    int ack_timeout_base_ms = 10'000;

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

    /// Incremental upload cursor. When true, query_audit_log_for_sync's
    /// no-sync-state branch is bounded below by the slot's upload_floor and
    /// the whole query is LIMITed to one send window — per-pass cost becomes
    /// O(window) instead of O(entire AuditLog) (202k-row scans per tick were
    /// the daemon's main CPU burn). The floor is only ever a scan bound;
    /// disabling this restores the exact unbounded pre-floor queries.
    bool use_upload_floor = true;

    /// This connection is an OBSERVER (read-only dial): its replication slot
    /// is flagged is_observer=1, excluded from every compaction floor, and
    /// evicted on a clean disconnect. See configuration::sync_is_observer.
    bool is_observer = false;

    /// One fresh WSS connection, fixed upload prefix, negotiated initial replay.
    /// No automatic reconnect. The owner supplies deadline and cleanup.
    bool finite_cycle = false;

    /// Upload filter. nullopt = sync everything (default).
    /// Empty vector = sync nothing. Non-empty = whitelist.
    std::optional<std::vector<sync_filter_entry>> sync_filter;

    /// Unique identifier for this synchronizer instance (e.g. "wss:<url>" or "ipc:<channel>").
    /// When set, per-synchronizer sync state is tracked in _lattice_sync_state
    /// instead of using the single isSynchronized column on AuditLog.
    /// This enables multiple synchronizers per database without interference.
    std::string sync_id;

    /// Display identity for log lines ONLY — never used as a persistence key.
    /// Defaults to sync_id when empty. IPC sets it to
    /// "<sync_id>#<srv|cli>@<db-basename>": hub and spoke sides of one IPC
    /// channel share the same sync_id, and their interleaved log lines under
    /// one bare "[ipc:<channel>]" label cost hours of misdiagnosis across two
    /// incidents (Aug 2026).
    std::string log_label;

    /// All active sync_ids on this database (including this one).
    /// Used for eager cleanup: when all sync_ids have synced an entry,
    /// _lattice_sync_state rows are deleted and isSynchronized=1 is set.
    std::vector<std::string> all_active_sync_ids;

    /// A4 — whether narrowing this channel's filter synthesizes marked
    /// filter-removal DELETEs for rows that left the filter (reconcile
    /// Phase 1). true (default): current behavior — the paired spoke's
    /// mirror is cleared (personal-sync semantics: un-synced projects leave
    /// the synced DB). false: narrowing is BOOKKEEPING-ONLY — rows leave
    /// this channel's sync set but no DELETE is emitted; the paired spoke
    /// remains a full mirror (group-channel semantics: un-exposing stops
    /// NEW sharing, never removes already-shared rows — a mirror-clear
    /// would let later peer edits apply against deleted rows as acked
    /// no-ops and permanently diverge). Row-level filter unmatch in
    /// classify (e.g. a row edit that stops matching) is NOT affected —
    /// that DELETE is the deliberate retract path.
    bool narrowing_emits_removals = true;
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
        /// Which channel this update belongs to. Multiple synchronizers on
        /// one database multiplex through one handler — without the label,
        /// interleaved counters from different channels are undiagnosable
        /// (the Aug 2026 incident's log was exactly this).
        std::string sync_id;
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

    // Internal work receipt; work_complete is not a transport/SQLite cleanup receipt.
    enum class finite_cycle_phase { inactive, running, work_complete, failed, cancelled };
    enum class finite_cycle_failure {
        none, transport, invalid_frame, partial_apply, storage, configuration_changed
    };
    struct finite_cycle_status {
        finite_cycle_phase phase = finite_cycle_phase::inactive;
        finite_cycle_failure failure = finite_cycle_failure::none;
        uint64_t connection_generation = 0;
        bool horizon_captured = false;
        int64_t upload_horizon = 0;
        bool catch_up_received = false;
    };
    // Metadata-only: safe under the native store's short ownership lock.
    finite_cycle_status get_finite_cycle_status() const;
    void cancel_finite_cycle();

    // Connection management
    void connect();
    void disconnect();
    bool is_connected() const { return is_connected_; }

    // Manual sync trigger. Native immediate dispatch runs inline when idle;
    // overlapping/reentrant work queues without waiting for the active pass.
    // Use drain(deadline) from outside callbacks to wait for upload progress.
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
    void set_on_sync_complete(on_sync_complete_handler handler) {
        const auto call_owner=callback_owner_.lock();
        {std::lock_guard<std::mutex> lock(event_handler_mutex_);on_sync_complete_.swap(handler);}
    }
    // Replacing a handler does not cancel a callback that already copied it. Snapshots are
    // invoked outside this mutex, so callbacks may replace handlers or close their owner.
    void set_on_error(on_error_handler handler) {
        const auto call_owner=callback_owner_.lock();
        {std::lock_guard<std::mutex> lock(event_handler_mutex_);on_error_.swap(handler);}
    }
    void set_on_state_change(on_state_change_handler handler) {
        const auto call_owner=callback_owner_.lock();
        const bool present=static_cast<bool>(handler);
        {std::lock_guard<std::mutex> lock(event_handler_mutex_);on_state_change_.swap(handler);}
        if(present && is_connected_) {
            const auto generation=connection_generation_.load();
            invoke_on_scheduler([this,generation] {
                if(!is_destroyed_ && connection_generation_==generation && is_connected_)
                    fire_state_change(true);
            });
        }
    }

    // Progress
    void set_on_progress(on_progress_handler handler);

    sync_progress get_progress() const;

protected:
    /// Database accessor — set by subclass constructors.
    lattice_db& db() { return *db_ptr_; }
    lattice_db* db_ptr_ = nullptr;

    // Both remain alive until after the dedicated connection/scheduler teardown.
    std::shared_ptr<void> owner_resources_;
    std::shared_ptr<void> channel_lifetime_;

    /// Owned database (native only). Stored in the base class so it outlives
    /// ~synchronizer_base() — base members are destroyed after the base
    /// destructor body, avoiding use-after-free on db_ptr_.
    std::unique_ptr<lattice_db> owned_db_;

    /// Common init — call from subclass constructors after db is set up.
    void init_sync(const sync_config& config, std::shared_ptr<scheduler> sched);
    void init_sync(const sync_config& config, std::shared_ptr<scheduler> sched,
                   std::unique_ptr<sync_transport> transport);

    std::shared_ptr<detail::sync_callback_gate> callback_gate_;
    std::weak_ptr<void> callback_owner_;
    bool callback_owner_required_=false;
    bool counted_sync_=false;
    void invoke_on_scheduler(std::function<void()> work);
    sync_config config_;
    std::shared_ptr<scheduler> scheduler_;
    std::unique_ptr<sync_transport> ws_client_;

    /// Log-line identity: config_.log_label when set, else sync_id. Cached
    /// so LOG_ macros can take a stable c_str(). Set by init_sync.
    std::string log_label_cache_;
    const char* log_id() const {
        return log_label_cache_.empty() ? config_.sync_id.c_str()
                                        : log_label_cache_.c_str();
    }

    std::atomic<bool> is_connected_{false};
    std::atomic<bool> is_connecting_{false};
    std::atomic<bool> is_destroyed_{false};  // Set in destructor; guards scheduled lambdas
    std::atomic<bool> should_reconnect_{true};  // Set false on explicit disconnect
    // Explicit disconnect/reconnect invalidates delayed retries before another
    // transport call can start. Recursive for transports that callback inline.
    std::recursive_mutex connection_mutex_;
    // Short filter-request/frame-admission boundary. Never held over a transport or user call.
    // A request revokes future admissions; an already-reserved bounded frame may finish enqueue.
    std::mutex policy_admission_mutex_;
    std::atomic<uint64_t> connection_generation_{0};
    // A receive gap fences this connection's queued frames until a new open.
    std::atomic<bool> receive_halted_{true};
    // Error and close callbacks for one attempt share a single delayed retry.
    std::atomic<bool> reconnect_pending_{false};
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
    /// is not re-hammered with the same window at a fixed cadence. Reset
    /// ONLY by an ACK that matched in-flight entries — unmatched ack floods
    /// (re-acks of history, another device's traffic on the channel) kept
    /// resetting this during the Aug 2026 incident, pinning the resend loop
    /// hot at its base cadence indefinitely.
    std::atomic<int> ack_resend_failures_{0};

    /// steady_clock nanos of the last ACK that resolved in-flight entries.
    /// The resend watchdog treats this as PROGRESS and extends its deadline:
    /// first-delivery acks return only after the peer synchronously applies
    /// a full frame (11-20s+ per 1000 entries observed), so a fixed
    /// send-time deadline re-sent every window before its acks could land —
    /// ~16-26x upload amplification in production. A window being actively
    /// consumed is never re-sent; a truly silent peer still times out.
    std::atomic<int64_t> last_matching_ack_ns_{0};

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
    bool reconciliation_pending_ = false; // bounded synthetic admission resumes after ACK/upload
    uint64_t applied_filter_version_ = 0; // scheduler-owned; requests revoke sends before configuration applies
    std::vector<detail::sync_delivery> delivery_outbox_; // scheduler-owned, reload after every durable change
    std::atomic<uint64_t> filter_version_{0};     // Bumped on each update_sync_filter; reconcile checks before acting

    // In-flight tracking: entries sent but not yet ACK'd, mapping the entry's
    // global_id to its AuditLog id (0 for synthetic entries, which have no
    // audit row — they must never gate the upload floor).
    // Prevents upload_pending_changes from re-sending entries on each cycle.
    // Accessed from scheduler thread (upload) and WebSocket/IPC thread (ACK) — needs mutex.
    std::mutex in_flight_mutex_;
    struct in_flight_entry { int64_t audit_id; size_t encoded_bytes; };
    std::unordered_map<std::string, in_flight_entry> in_flight_ids_;

    // Upload-floor bookkeeping (guarded by in_flight_mutex_). open_audit_ids_
    // holds every real audit id enumerated but not yet resolved (ACKed or
    // skipped); the floor advances to min(open)-1, or to the highest id ever
    // enumerated when the set drains. NOT cleared on disconnect — unACKed
    // entries must stay above the floor so the (now bounded) reconnect
    // re-query finds exactly them. The floor itself is read from the DB each
    // pass rather than cached: the accept-side IPC synchronizer is replaced
    // on every client reconnect, and the monotonic UPDATE makes brief
    // old/new coexistence benign.
    std::set<int64_t> open_audit_ids_;
    int64_t last_enumerated_id_ = 0;
    // Poison-entry observability: if min(open) hasn't moved for this long,
    // some entry is permanently failing to resolve (pre-existing apply-failure
    // loop) — the floor makes it visible instead of silent.
    int64_t stall_min_open_ = -1;
    std::chrono::steady_clock::time_point stall_since_{};
    /// Erase resolved audit ids from the open set and persist the advanced
    /// floor (monotonic). Call ONLY after the resolving transactions have
    /// committed. Takes in_flight_mutex_ internally.
    void resolve_audit_ids(const std::vector<int64_t>& audit_ids);

    /// Sweep the resolved prefix of open_audit_ids_ against DATABASE truth:
    /// min(open) is erased while the database says it can never be pending —
    /// synchronized for this channel, globally synchronized (cross-channel
    /// collapse deletes the per-channel rows), or the AuditLog row no longer
    /// exists. Self-heals every state where a row was marked/removed without
    /// the in-memory resolution running (late acks, prior-tenure acks,
    /// out-of-band history wipes) — each of which otherwise pins the upload
    /// floor for the life of the connection and vetoes all compaction.
    void reconcile_open_with_db();

    mutable std::mutex event_handler_mutex_;
    on_sync_complete_handler on_sync_complete_;
    on_error_handler on_error_;
    on_state_change_handler on_state_change_;
    void fire_sync_complete(const std::vector<std::string>& ids);
    void fire_error(const std::string& error);
    void fire_state_change(bool connected);
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

    mutable std::mutex finite_cycle_mutex_;
    finite_cycle_status finite_cycle_status_;
    bool finite_cycle_ready_ = false; // guarded by finite_cycle_mutex_
    void fail_finite_cycle(finite_cycle_failure reason, uint64_t generation);
    bool finite_cycle_accepts_work(uint64_t generation, bool download = false) const;
    void complete_finite_cycle_if_drained();

    // Sync operations
    void upload_pending_changes();
    void upload_pending_changes_impl();
    std::vector<std::string> apply_remote_changes(const std::vector<audit_log_entry>& entries);
    void mark_as_synced(const std::vector<std::string>& global_ids);

    // upload_pending_changes decomposed phases
    struct classified_entries {
        std::vector<audit_log_entry> to_send;
        std::vector<int64_t> to_mark_synced;
        std::unordered_map<std::string, detail::sync_delivery> to_admit;
    };
    // Continuation is a result of this specific pass, never shared state.
    std::vector<audit_log_entry> query_pending_entries(bool& enumeration_hit_limit);
    classified_entries classify_entries(std::vector<audit_log_entry>& entries);
    void mark_skipped_synced(const std::vector<int64_t>& to_mark_synced);
    void send_entries(std::vector<audit_log_entry>& entries,
                      const std::unordered_map<std::string, detail::sync_delivery>* admissions = nullptr);
    std::string current_routing_key() const;
    void refresh_delivery_outbox();
    void cancel_disallowed_deliveries();
    void resend_delivery_outbox();
    bool delivery_content_allowed(const detail::sync_delivery& delivery);
    bool has_pending_delivery(const std::string& table, const std::string& row) const;

    // Channel-private deliveries whose classification changes membership.
    // Persist before resolving their source; only a matching ACK removes them.
    void sync_outbox_transaction(const std::function<void()>& body);

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

    // Failed downloads preserve the durable cursor and the transport's retry policy.
    void download_failed(uint64_t generation, finite_cycle_failure failure, const std::string& error);

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

    // Native owner slots use this factory so admitted callbacks retain both the synchronizer
    // and its dedicated database until return. Standalone raw/stack constructors retain their
    // caller-managed lifetime contract. The weak owner is installed BEFORE callbacks start.
    static std::shared_ptr<synchronizer> create_owned(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                       std::shared_ptr<void> resources={});
    static std::shared_ptr<synchronizer> create_owned(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                    std::unique_ptr<sync_transport> transport,
                                                    std::shared_ptr<void> resources={});
    // A derived transport's unique_ptr also converts to shared_ptr<void>. Prefer the transport
    // path explicitly without making callers erase its type or accidentally retain it as resources.
    template<class Transport, std::enable_if_t<std::is_convertible_v<Transport*,sync_transport*> &&
                                               !std::is_same_v<Transport,sync_transport>,int> = 0>
    static std::shared_ptr<synchronizer> create_owned(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                    std::unique_ptr<Transport> transport,
                                                    std::shared_ptr<void> resources={}) {
        return create_owned(std::move(db),config,std::unique_ptr<sync_transport>(std::move(transport)),std::move(resources));
    }
private:
    struct deferred_init {};
    synchronizer(std::unique_ptr<lattice_db> db,deferred_init);
    static std::shared_ptr<synchronizer> create_owned_impl(std::unique_ptr<lattice_db> db,const sync_config& config,
                                                         std::unique_ptr<sync_transport> transport,bool supplied,
                                                         std::shared_ptr<void> resources);
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
/// min_id_exclusive bounds the no-sync-state branch (`a.id > ?`) — pass the
/// slot's upload_floor. limit (0 = unbounded) caps the result to one send
/// window; entries are ordered by id ASC so the window is always the oldest
/// pending work. max_id_inclusive bounds BOTH branches for a finite cycle;
/// zero is a real empty prefix, while nullopt preserves the unbounded query.
std::vector<audit_log_entry> query_audit_log_for_sync(
    database& db,
    const std::string& sync_id,
    const std::optional<std::vector<sync_filter_entry>>& sync_filter = std::nullopt,
    int64_t min_id_exclusive = 0,
    size_t limit = 0,
    std::optional<int64_t> max_id_inclusive = std::nullopt,
    size_t materialized_byte_limit = 0,
    bool* hit_byte_limit = nullptr);

/// Read the slot's upload_floor (0 when absent). Monotonic advance helper.
int64_t read_upload_floor(database& db, const std::string& sync_id);
void advance_upload_floor(database& db, const std::string& sync_id, int64_t floor);

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

/// no_history late-binding (1.5.0). A column flagged `no_history` records
/// THAT it changed in its UPDATE audit rows, never its value (see
/// property_descriptor::no_history). Anything that ships audit entries to a
/// peer must therefore fill such columns with the row's CURRENT value first:
/// for every UPDATE entry whose table schema marks columns no_history and
/// whose changedFieldsNames lists them with a null/missing value, read the
/// live row and fill them in (marking observed NULLs in noHistoryNulls); if
/// the row is gone, drop the column from both
/// changedFields and changedFieldsNames (its DELETE entry follows). Invariant
/// a consumer can rely on: a no_history column syncs its latest value at
/// upload/push time, not every intermediate value. Called by
/// query_audit_log_for_sync and events_after; exposed for other shippers.
/// Full INSERT payloads also mark their explicit no_history NULLs.
void late_bind_no_history(database& db, std::vector<audit_log_entry>& entries);

/// The same fill for ONE row, as the wire JSON object ({"col": {"kind":..,
/// "value":..}}) — for shippers that serialize audit rows outside core (the
/// Swift relay's observer push). Missing row ⇒ "{}".
std::string no_history_live_values_json(database& db, const std::string& table_name,
                                        const std::string& global_row_id,
                                        const std::vector<std::string>& columns);

namespace test_hooks {
/// Test-only failure injection: invoked with chunk_start at the top of every
/// apply chunk transaction, BEFORE its BEGIN. A test installs a throwing hook
/// to deterministically simulate a chunk-level failure (lock timeout at
/// BEGIN/COMMIT) and pin the containment contract: earlier committed chunks'
/// entries are still returned (= acked), the failed chunk is retried once,
/// then the delivery stops. nullptr (the default) in production.
extern std::function<void(size_t chunk_start)> apply_chunk_begin;
/// Test-only rollback seam after durable-delivery INSERT, before membership
/// and source resolution. Throwing must roll the whole staging transaction back.
extern std::function<void()> sync_outbox_after_insert;
}  // namespace test_hooks

// Apply remote audit log entries to a database (server-side receive path)
// Disables sync triggers, executes model SQL, inserts AuditLog records, re-enables triggers.
// Returns global IDs of successfully applied entries (entries that failed SQL execution are excluded).
std::vector<std::string> apply_remote_changes(lattice_db& db, const std::vector<audit_log_entry>& entries);

/// Opt-in append-only model rows: complete INSERT snapshots, identical replays
/// only. Validation and all entries share the existing owned write transaction.
/// Empty tables and no authorization preserve ordinary receive semantics.
std::vector<std::string> apply_remote_changes_immutable(lattice_db& db,
    const std::vector<audit_log_entry>& entries,
    const std::vector<std::string>& immutable_tables,
    const std::function<bool()>& authorize = {});

/// ACK bookkeeping under the same checked authority boundary. No ACK is echoed.
void mark_audit_entries_synced_authorized(lattice_db& db,
    const std::vector<std::string>& global_ids, const std::function<bool()>& authorize);

/// Apply remote audit log entries and mark them as synced for the receiving sync_id.
/// Unlike apply_remote_changes, this uses per-synchronizer sync state so that OTHER
/// synchronizers (WSS, BLE, etc.) see the entries as pending and relay them.
/// The receiving sync_id's _lattice_sync_state is set to 1 (preventing re-upload = loop prevention).
/// Returns global IDs of successfully applied entries.
std::vector<std::string> apply_remote_changes_for(lattice_db& db,
                              const std::vector<audit_log_entry>& entries,
                              const std::string& sync_id);

/// Rescue copied, unacknowledged local audit entries without acknowledging a
/// receiving channel. Each unique input ID has one outcome. Duplicate IDs and
/// unsupported schemas are rejected before entry effects. New entries retain
/// upload eligibility, including value-identical writes; filter-removal DELETEs
/// remain synchronized. The source store and its acknowledgements are untouched.
struct pending_import_result {
    std::vector<std::string> imported_ids;
    std::vector<std::string> already_present_ids;
    std::vector<std::string> rejected_ids;
};
pending_import_result apply_pending_changes(lattice_db& db,
                              const std::vector<audit_log_entry>& entries);

/// Bounded exact-identity audit capture for transports. Decode and late-bind
/// actual rows in a read snapshot, retaining Core provenance and Int64 values.
/// Missing/malformed rows or exceeded limits throw; no partial result is returned.
/// At most 4096 identities and 16 MiB raw/expanded bytes. max_bytes may lower
/// that budget so a caller can bound a sequence of captures cumulatively.
std::vector<audit_log_entry> query_audit_log_selected(database& db,
    const std::vector<std::pair<int64_t, std::string>>& identities,
    size_t max_bytes = 16 * 1024 * 1024);

// Replication slot management for sync-safe compaction.
// Each synchronizer registers a slot; compaction only deletes entries
// below the minimum confirmed_audit_id across all active slots.

/// Register (or touch) a replication slot for the given sync_id. `is_observer`
/// marks a read-only dial whose floor never advances (excluded from compaction
/// bounds); re-registration updates the flag so a connection that changes
/// role is re-classified.
void register_replication_slot(database& db, const std::string& sync_id,
                               bool is_observer = false);

/// Guarded migration: adds _lattice_replication_slots.is_observer (default 0)
/// the same lazy way ensure_cursor_column adds the download cursor — no schema
/// epoch bump, no slow-path cost. Idempotent and cheap.
void ensure_observer_column(database& db);

/// Flip a slot's observer flag (a relay that learns a connection's scope
/// after registration).
void set_replication_slot_observer(database& db, const std::string& sync_id, bool is_observer);

/// Guarded migration: adds _lattice_replication_slots.last_received_event_id
/// (the externalized download-resume cursor) and eagerly seeds it from the
/// legacy newest-isFromRemote AuditLog row. Idempotent and cheap; called from
/// every cursor read/write entry point.
void ensure_cursor_column(database& db);

/// Advance a replication slot's confirmed cursor (monotonically forward only).
void advance_replication_slot(database& db, const std::string& sync_id, int64_t confirmed_audit_id);

/// Remove a replication slot.
void remove_replication_slot(database& db, const std::string& sync_id);

} // namespace lattice

#endif // __cplusplus
