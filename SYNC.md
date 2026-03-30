# LatticeCore Sync System

LatticeCore's sync system provides bidirectional data synchronization between SQLite databases using an audit-log-based change tracking mechanism. Local changes are captured automatically via SQL triggers into an `AuditLog` table, serialized as JSON, and transmitted over pluggable transports (WebSocket, Unix domain sockets, or custom). Remote changes are applied idempotently with conflict resolution. The system supports multiple concurrent transports with per-synchronizer state tracking, filtered sync with dynamic reconciliation, and slot-aware audit log compaction.

## Architecture

```
Local write ──► SQL trigger ──► AuditLog INSERT
                                      │
                                 Observer fires
                                      │
                              upload_pending_changes()
                                      │
                               classify_entries()
                              ╱                    ╲
                         to_send              to_mark_synced
                            │                       │
                   send via transport      mark_skipped_synced()
                            │
                       Remote peer
                            │
                      ACK response
                            │
                     mark_as_synced()
```

### Multi-Transport Relay

```
[Process A]                          [Process B]
 lattice_db                           lattice_db
     │                                    │
 IPC endpoint ──── Unix socket ──── IPC endpoint
     │                                    │
 sync_id="ipc:ch"                  sync_id="wss:url"
                                          │
                                     WebSocket ──── Cloud Server
```

Per-synchronizer state (`_lattice_sync_state`) tracks sync status independently per transport. An entry received via IPC is marked as synced for the IPC sync_id but remains pending for WSS, enabling automatic relay without loop prevention logic.

---

## Quick Reference

| Concept | Header | Implementation |
|---------|--------|----------------|
| Sync engine | `sync.hpp` | `sync.cpp` |
| Transport interface | `network.hpp` | Platform-specific |
| IPC transport | `ipc.hpp` | `ipc.cpp` |
| AuditLog / triggers | `lattice.hpp` | Inline |
| Compaction | `lattice.hpp` | Inline |
| Swift API | `../lattice/.../Lattice.swift` | `Sync.swift`, `AuditLog.swift` |

All C++ headers are under `Sources/LatticeCore/include/lattice/`.
All C++ sources are under `Sources/LatticeCore/src/`.

---

## Database Schema

### AuditLog

Every model table change is recorded here by SQL triggers.

```sql
CREATE TABLE IF NOT EXISTS AuditLog(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    globalId         TEXT UNIQUE COLLATE NOCASE DEFAULT (lower(hex(randomblob(4))) || '-' || ...),
    tableName        TEXT,
    operation        TEXT,           -- "INSERT", "UPDATE", "DELETE"
    rowId            INTEGER,
    globalRowId      TEXT,
    changedFields    TEXT,           -- JSON: {"field": {"kind": N, "value": V}, ...}
    changedFieldsNames TEXT,         -- JSON: ["field1", "field2", ...]
    isFromRemote     INTEGER DEFAULT 0,
    isSynchronized   INTEGER DEFAULT 0,
    timestamp        REAL DEFAULT (unixepoch('subsec'))
)
```

### _SyncControl

Single-row table gating trigger execution during remote apply.

```sql
CREATE TABLE IF NOT EXISTS _SyncControl (
    id       INTEGER PRIMARY KEY CHECK(id=1),
    disabled INTEGER NOT NULL DEFAULT 0
)
```

### _lattice_sync_state

Per-synchronizer sync tracking. Enables multiple transports without interference.

```sql
CREATE TABLE IF NOT EXISTS _lattice_sync_state (
    audit_entry_id  INTEGER NOT NULL,
    sync_id         TEXT NOT NULL,
    is_synchronized INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (audit_entry_id, sync_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_state_pending
    ON _lattice_sync_state(sync_id, is_synchronized)
    WHERE is_synchronized = 0;
```

### _lattice_sync_set

Tracks which rows are in scope for a sync filter. Used to detect rows entering/leaving the filter.

```sql
CREATE TABLE IF NOT EXISTS _lattice_sync_set (
    table_name    TEXT NOT NULL,
    global_row_id TEXT NOT NULL,
    PRIMARY KEY (table_name, global_row_id)
)
```

### _lattice_replication_slots

Prevents premature audit log compaction. Each synchronizer registers a slot.

```sql
CREATE TABLE IF NOT EXISTS _lattice_replication_slots (
    sync_id            TEXT PRIMARY KEY,
    confirmed_audit_id INTEGER NOT NULL DEFAULT 0,
    last_active_at     TEXT NOT NULL DEFAULT (datetime('now'))
)
```

---

## SQL Triggers

Three triggers are created for every model table by `create_audit_triggers()`:

### INSERT Trigger (`Audit<Table>Insert`)

Fires `AFTER INSERT` when `sync_disabled() = 0`. Captures all column values into `changedFields` as JSON.

### UPDATE Trigger (`AuditLog_Update_<Table>`)

Fires `AFTER UPDATE` when `sync_disabled() = 0` and at least one column actually changed (`OLD.col IS NOT NEW.col`). Records only the changed columns.

### DELETE Trigger (`Audit<Table>Delete`)

Fires `AFTER DELETE` when `sync_disabled() = 0`. Captures all column values of the deleted row.

### sync_disabled() Function

A custom SQLite function registered at database open. Reads `_SyncControl.disabled` to gate all audit triggers. Set to 1 during remote change application to prevent feedback loops.

```cpp
// Disable triggers
db.execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
// ... apply remote changes ...
// Re-enable triggers
db.execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
```

---

## Core Data Types

### any_property (`sync.hpp`)

Variant type for sync-safe transmission of column values.

```cpp
enum class any_property_kind : int {
    int_kind    = 0,
    int64_kind  = 1,
    string_kind = 2,
    date_kind   = 3,   // double (Unix timestamp)
    null_kind   = 4,
    float_kind  = 5,
    double_kind = 7,
    data_kind   = 6    // BLOB, hex-encoded in JSON
};

struct any_property {
    any_property_kind kind;
    std::variant<std::nullptr_t, int64_t, double, std::string, std::vector<uint8_t>> value;
};
```

### audit_log_entry (`sync.hpp`)

Represents a single change record.

```cpp
struct audit_log_entry {
    int64_t id;
    std::string global_id;           // UUID, used for ACK
    std::string table_name;
    std::string operation;           // "INSERT", "UPDATE", "DELETE"
    int64_t row_id;
    std::string global_row_id;       // Cross-DB object identity
    changed_fields_map changed_fields;
    std::vector<std::string> changed_fields_names;
    std::string timestamp;
    bool is_from_remote;
    bool is_synchronized;

    // Generate SQL from this entry (INSERT...ON CONFLICT, UPDATE, DELETE)
    std::pair<std::string, std::vector<column_value_t>> generate_instruction(
        const std::unordered_map<std::string, column_type>& schema = {}) const;
};
```

### server_sent_event (`sync.hpp`)

Polymorphic message envelope -- either a batch of changes or an acknowledgment.

```cpp
struct server_sent_event {
    enum class type { audit_log, ack };

    type event_type;
    std::vector<audit_log_entry> audit_logs;  // For audit_log type
    std::vector<std::string> acked_ids;       // For ack type

    static server_sent_event make_audit_log(std::vector<audit_log_entry> logs);
    static server_sent_event make_ack(std::vector<std::string> ids);
};
```

### sync_config (`sync.hpp`)

```cpp
struct sync_config {
    std::string websocket_url;
    std::string authorization_token;
    int max_reconnect_attempts = 0;      // 0 = unlimited
    double base_delay_seconds = 1.0;
    double max_delay_seconds = 60.0;
    size_t chunk_size = 1000;            // Max events per message

    std::optional<std::vector<sync_filter_entry>> sync_filter;  // nullopt = all
    std::string sync_id;                 // e.g. "wss:<url>" or "ipc:<channel>"
    std::vector<std::string> all_active_sync_ids;
};
```

### sync_filter_entry (`sync.hpp`)

```cpp
struct sync_filter_entry {
    std::string table_name;
    std::optional<std::string> where_clause;  // nullopt = all rows of this table
};
```

---

## Transport Layer

### sync_transport (`network.hpp`)

Abstract bidirectional transport interface.

```cpp
class sync_transport {
public:
    virtual void connect(const std::string& url,
                        const std::map<std::string, std::string>& headers = {}) = 0;
    virtual void disconnect() = 0;
    virtual transport_state state() const = 0;
    virtual void send(const transport_message& message) = 0;

    virtual void set_on_open(on_open_handler handler) = 0;
    virtual void set_on_message(on_message_handler handler) = 0;
    virtual void set_on_error(on_error_handler handler) = 0;
    virtual void set_on_close(on_close_handler handler) = 0;
};
```

### generic_sync_transport (`network.hpp`)

Injectable transport via C function pointers, used by Swift to provide platform-native WebSocket (URLSessionWebSocketTask on Apple, WebSocketKit/NIO on Linux).

```cpp
class generic_sync_transport : public sync_transport {
public:
    generic_sync_transport(void* user_data,
                          connect_fn_ptr connect,
                          disconnect_fn_ptr disconnect,
                          state_fn_ptr state,
                          send_fn_ptr send);

    // Called by the external transport implementation:
    void trigger_on_open();
    void trigger_on_message(const transport_message& msg);
    void trigger_on_error(const std::string& error);
    void trigger_on_close(int code, const std::string& reason);
};
```

### IPC Transport (`ipc.hpp`)

Unix domain socket transport implementing `sync_transport`.

**Framing**: Length-prefixed -- `[4 bytes big-endian length][JSON payload]`. Max frame size 256 MB.

**ipc_socket_client**: Two construction modes:
- Client-side: `ipc_socket_client(socket_path)` -- connects to existing server
- Server-side: `ipc_socket_client(accepted_fd)` -- wraps an already-accepted file descriptor

Background read thread runs `read_length_prefixed()` in a loop, dispatching `on_message` callbacks.

**ipc_server**: Listens on a Unix socket, runs background accept thread, hands each connection as `unique_ptr<ipc_socket_client>` to the accept callback.

**ipc_endpoint**: Auto-negotiates server/client role. On `start()`:
1. Tries to `bind()` (become server)
2. If `EADDRINUSE`, probes the socket to distinguish live server from stale socket
3. Stale sockets are unlinked and re-bound

**Socket paths**:
- macOS: `~/Library/Caches/Lattice/ipc/<channel>.sock`
- Linux: `$XDG_RUNTIME_DIR/lattice/<channel>.sock` (fallback `/tmp/lattice-<uid>/<channel>.sock`)

---

## Synchronizer Engine

### synchronizer_base (`sync.hpp`)

Core sync engine. Non-copyable, non-moveable. All sync work is serialized through a `scheduler` thread.

**Platform variants**:
- **Native** (macOS/Linux/iOS): Synchronizer owns a dedicated `lattice_db` (separate SQLite connection on its own thread)
- **Emscripten** (WASM): Borrows the parent's `lattice_db` (single-threaded, OPFS exclusive lock prevents second connection)

**Lifecycle**:
1. `init_sync()` -- sets up transport handlers and registers AuditLog table observer
2. `connect()` -- opens transport (WSS: appends `?last-event-id=<checkpoint>` and `Authorization` header)
3. `on_websocket_open()` -- registers replication slot, runs `reconcile_sync_filter()`, calls `upload_pending_changes()`
4. Steady state -- observer triggers uploads on new AuditLog entries; incoming messages trigger apply/ACK
5. `disconnect()` -- sets `should_reconnect_=false`, closes transport
6. Destructor -- sets `is_destroyed_=true`, removes observer, disconnects

**Observer** (`sync.cpp`): Registered on the `"AuditLog"` table. On INSERT, checks if entry is pending for this `sync_id`. If pending, sets `upload_requested_` atomic and schedules `upload_pending_changes()`.

**In-flight tracking**: `std::unordered_set<std::string> in_flight_ids_` (mutex-protected) tracks entries sent but not yet ACK'd, preventing re-sends during upload cycles. Cleared on error/close for retry after reconnect.

---

## Upload Flow

### 1. query_pending_entries()

Calls `query_audit_log_for_sync()` with this synchronizer's `sync_id` and optional filter. Returns entries where:
- `_lattice_sync_state` row exists with `is_synchronized=0`, OR
- No `_lattice_sync_state` row AND `AuditLog.isSynchronized=0`

Excludes entries already in `in_flight_ids_`. Does NOT filter on `isFromRemote` -- any unsynced entry is eligible, enabling relay.

### 2. classify_entries()

With no filter, all entries go to `to_send`. With a filter active:

| Operation | Matches Filter | In Sync Set | Action |
|-----------|---------------|-------------|--------|
| INSERT/UPDATE | Yes | No | Add to sync set, send |
| INSERT/UPDATE | Yes | Yes | Send as-is |
| INSERT/UPDATE | No | Yes | Send synthetic DELETE, remove from sync set |
| INSERT/UPDATE | No | No | Skip (mark synced) |
| DELETE | - | Yes | Send, remove from sync set |
| DELETE | - | No | Skip (mark synced) |

Link tables (prefixed with `_`) follow their parent model table's filter.

### 3. send_entries()

- Adds entries to `in_flight_ids_`
- Chunks by `config_.chunk_size` (default 1000)
- Wraps in `server_sent_event::make_audit_log()`
- Serializes to JSON and sends as binary `transport_message`

### 4. On ACK

- Parses `server_sent_event` with type `ack`
- Calls `mark_as_synced()` which:
  - Removes from `in_flight_ids_`
  - Updates `_lattice_sync_state` for this `sync_id`
  - Advances replication slot cursor
  - Performs eager cleanup (see Per-Synchronizer State)
  - Updates progress counters

---

## Download Flow

### 1. Receive Message

`on_transport_message()` parses JSON as `server_sent_event`:
- **audit_log**: Dispatches to scheduler thread, calls `apply_remote_changes()`
- **ack**: Dispatches to scheduler thread, calls `mark_as_synced()`

### 2. apply_remote_changes_impl()

Processes in chunks of 50 entries within transactions:

1. Disable triggers: `UPDATE _SyncControl SET disabled = 1`
2. For each entry:
   - Check for duplicate `globalId` in AuditLog (idempotence)
   - Ensure link tables / vec0 vector tables exist if needed
   - Generate SQL via `entry.generate_instruction(schema)`
   - Resolve local `rowId` (may differ due to `INSERT...ON CONFLICT DO UPDATE`)
   - Execute the SQL
   - Insert AuditLog record with `isFromRemote=1`
3. Re-enable triggers: `UPDATE _SyncControl SET disabled = 0`

**Per-sync mode** (relay): When `receiving_sync_id` is provided, sets `isSynchronized=0` on AuditLog and marks only the receiving `sync_id` in `_lattice_sync_state`. Other sync_ids see the entry as pending and relay it.

### 3. Send ACK

After successful apply, sends `server_sent_event::make_ack(applied_ids)` back to the sender.

---

## Per-Synchronizer Sync State

### sync_id Convention

- WebSocket: `"wss:<websocket_url>"`
- IPC: `"ipc:<channel>"`

### How It Works

The `_lattice_sync_state` table tracks sync status independently per `sync_id`:

```
query_audit_log_for_sync(sync_id):
  Case 1: sync_state row exists, is_synchronized=1  → EXCLUDED (already synced)
  Case 2: sync_state row exists, is_synchronized=0  → INCLUDED (pending)
  Case 3: No row, AuditLog.isSynchronized=0          → INCLUDED (never tracked)
  Case 4: No row, AuditLog.isSynchronized=1          → EXCLUDED (eagerly cleaned up)
```

### Eager Cleanup

When ALL active `sync_ids` have synced an entry:
1. Delete all `_lattice_sync_state` rows for that entry
2. Set `AuditLog.isSynchronized=1`

This prevents unbounded growth of the sync state table while maintaining the per-synchronizer invariant.

### Relay Pattern

```
1. Process A writes a row → trigger → AuditLog entry (isFromRemote=0, isSynchronized=0)
2. IPC synchronizer uploads to Process B
3. Process B: apply_remote_changes_for(entries, "ipc:channel")
   → Executes SQL, inserts AuditLog with isFromRemote=1, isSynchronized=0
   → Marks "ipc:channel" in _lattice_sync_state as synced
4. WSS synchronizer in Process B sees entry as pending (no row for "wss:url")
5. WSS uploads to cloud server
6. Cloud ACKs → mark_as_synced for "wss:url" → eager cleanup
```

---

## Filtered Sync

### Configuration

- `nullopt` = sync everything (default)
- Empty vector = sync nothing
- Non-empty = whitelist of `sync_filter_entry` (table + optional WHERE)

### Sync Set

`_lattice_sync_set` tracks which `(table_name, global_row_id)` pairs are currently in sync scope. This enables detecting when rows enter or leave the filter.

### reconcile_sync_filter()

Called on connect and on runtime filter update via `update_sync_filter()`. Two phases:

**Phase 1 -- Removals**: Query current matching rows per filter entry. Any `_lattice_sync_set` entry NOT in current matches gets a synthetic DELETE in AuditLog, and is removed from the sync set.

**Phase 2 -- Additions**: Per filter entry, batch-fetch matching rows NOT in sync set. Synthesize INSERT audit entries with full row data. Add to sync set.

### Version Guard

`filter_version_` is bumped atomically before scheduling reconciliation. The scheduled lambda checks the version before acting -- stale versions skip reconciliation to prevent DELETE+INSERT churn from rapid filter toggles.

---

## Reconnection

### Exponential Backoff

```
delay = min(2^attempts * base_delay_seconds, max_delay_seconds)
```

Interruptible: sleeps in 50ms increments, checking `is_destroyed_` each cycle. Only applies to WSS transports (IPC sets `should_reconnect_=false`).

### Checkpoint Resume

On WebSocket reconnect, the URL includes `?last-event-id=<globalId>` where `globalId` is the most recent `isFromRemote=1` AuditLog entry. The server uses this to resume streaming from the correct point.

### Error Recovery

On transport error or close:
1. `in_flight_ids_` cleared (entries can be re-sent)
2. `is_connected_` set to false
3. `on_state_change_` callback fired
4. `schedule_reconnect()` called (if `should_reconnect_`)

---

## Audit Log Compaction

### force_compact_audit_log()

Nuclear compaction. Deletes all history and regenerates a fresh snapshot.

1. Disable triggers
2. `DELETE FROM AuditLog`
3. `DELETE FROM _lattice_sync_state`
4. `DELETE FROM _lattice_sync_set`
5. Reset AUTOINCREMENT counter
6. Reset all replication slot cursors to 0
7. Re-enable triggers
8. `generate_history()` -- creates INSERT entries for all current objects

Active synchronizers will re-sync all data after this operation.

### safe_compact_audit_log(stale_threshold_seconds)

Slot-aware compaction. Only deletes entries confirmed by ALL registered synchronizers.

1. Optionally evict stale slots (inactive longer than `stale_threshold_seconds`)
2. Query `MIN(confirmed_audit_id)` across all active slots
3. `DELETE FROM AuditLog WHERE id <= safe_id`
4. `DELETE FROM _lattice_sync_state WHERE audit_entry_id <= safe_id`

Returns count of deleted entries, or -1 if no slots exist.

### Replication Slots

- **register_replication_slot(sync_id)**: Called on connect. INSERT OR REPLACE with `last_active_at = datetime('now')`.
- **advance_replication_slot(sync_id, confirmed_audit_id)**: Called after ACK batches. Monotonically forward: `SET confirmed_audit_id = MAX(confirmed_audit_id, ?)`.
- **remove_replication_slot(sync_id)**: Called on explicit teardown.

---

## Progress Tracking

### sync_progress

```cpp
struct sync_progress {
    int64_t pending_upload = 0;    // Entries awaiting ACK (decrements on ACK)
    int64_t total_upload = 0;      // Snapshot at batch start
    int64_t acked = 0;             // Cumulative ACKs received
    int64_t received = 0;          // Cumulative remote entries applied
};
```

Updated via atomics at each stage:
- **Classification**: Sets `pending_upload` and `total_upload`
- **ACK received**: Decrements `pending_upload`, increments `acked`
- **Remote apply**: Increments `received`

### Aggregation

`lattice_db::get_sync_progress()` walks all sibling instances via `instance_registry` and sums progress from all synchronizers (WSS + IPC).

---

## Swift API

### Configuration

```swift
let config = Lattice.Configuration(
    fileURL: dbURL,
    wssEndpoint: URL(string: "wss://sync.example.com/v1"),
    authorizationToken: "Bearer ...",
    syncFilter: filter,
    ipcTargets: [.init(channel: "synced", syncFilter: ipcFilter)]
)
```

### SyncFilter

```swift
var filter = Lattice.SyncFilter()
filter.include(Person.self)                                    // All rows
filter.include(Memory.self, where: { $0.isPrivate == false })  // With predicate
```

- `nil` = sync everything (default)
- Empty `SyncFilter()` = sync nothing
- With entries = whitelist

### IPCSyncTarget

```swift
Lattice.IPCSyncTarget(
    channel: "synced",           // Unix socket channel name
    syncFilter: filter,          // Optional per-channel filter
    socketPath: "/custom/path"   // Optional explicit socket path
)
```

### SyncProgress

```swift
public struct SyncProgress: Sendable {
    public let pendingUpload: Int
    public let totalUpload: Int
    public let acked: Int
    public let received: Int
    public var uploadFraction: Double    // acked / totalUpload (1.0 if nothing to upload)
    public var isUploading: Bool         // pendingUpload > 0
}
```

### Callbacks and Streams

```swift
// Callback-based
lattice.onSyncProgress { progress in ... }
lattice.onSyncError { error in ... }
lattice.onSyncStateChange { connected in ... }

// Async stream
for await progress in lattice.syncProgressStream { ... }

// Combine publisher
lattice.syncProgressPublisher.sink { progress in ... }
```

### State Queries

```swift
lattice.isSyncConnected      // Bool: WebSocket connection status
```

### Runtime Filter Updates

```swift
lattice.updateSyncFilter(newFilter)   // Triggers reconciliation
lattice.updateSyncFilter(nil)         // Clear filter, sync everything
```

### Server-Side Sync

```swift
// Receive changes from a client
let ackedIds = try lattice.receive(jsonData)    // Returns [UUID]

// Query events after a checkpoint (for streaming to clients)
let events = lattice.eventsAfter(globalId: lastSeenId)
```

### AuditLog Model

```swift
@Model @Codable public class AuditLog {
    public enum Operation: String, Codable { case insert, update, delete }

    public var tableName: String
    public var operation: Operation
    public var rowId: Int64
    public var globalRowId: UUID?
    public var changedFields: [String: AnyProperty]
    public var changedFieldsNames: [String?]?
    public var timestamp: Date
    public var isFromRemote: Bool
    public var isSynchronized: Bool
}
```

Observable via `lattice.observe { entries in ... }` or `lattice.changeStream`.

---

## Wire Protocol

### audit_log Message

```json
{
  "kind": "auditLog",
  "auditLog": [
    {
      "id": 42,
      "globalId": "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
      "tableName": "Person",
      "operation": "INSERT",
      "rowId": 7,
      "globalRowId": "f6e5d4c3-b2a1-4098-8765-432109876543",
      "changedFields": {
        "name": {"kind": 2, "value": "Alice"},
        "age": {"kind": 0, "value": 30}
      },
      "changedFieldsNames": ["name", "age"],
      "timestamp": "1711800000.123",
      "isFromRemote": false,
      "isSynchronized": false
    }
  ]
}
```

### ack Message

```json
{
  "kind": "ack",
  "ack": ["a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"]
}
```

### any_property_kind Mapping

| Kind | Value | JSON Value Type |
|------|-------|-----------------|
| `int_kind` | 0 | number |
| `int64_kind` | 1 | number |
| `string_kind` | 2 | string |
| `date_kind` | 3 | number (Unix timestamp) |
| `null_kind` | 4 | null |
| `float_kind` | 5 | number |
| `data_kind` | 6 | string (hex-encoded) |
| `double_kind` | 7 | number |

---

## Platform Differences

| Aspect | Native (macOS/Linux/iOS) | Emscripten (WASM) |
|--------|--------------------------|-------------------|
| SQLite connection | Dedicated (separate connection for sync) | Shared (single connection, OPFS lock) |
| Scheduler | `std_thread_scheduler` (background thread) | `immediate_scheduler` (inline) |
| Synchronizer | Owns `unique_ptr<lattice_db>` | Borrows `lattice_db&` reference |
| IPC | Supported (Unix domain sockets) | Not available |

**IPC socket paths**:
- macOS: `~/Library/Caches/Lattice/ipc/<channel>.sock`
- Linux: `$XDG_RUNTIME_DIR/lattice/<channel>.sock` (fallback `/tmp/lattice-<uid>/<channel>.sock`)

---

## Invariants and Rules

1. **One instance per DB owns writes AND relay.** Two `lattice_db` instances on the same SQLite file have independent `_lattice_sync_set` state. The instance that writes must also own the IPC relay. Enforced by `flock()` on `<path>.sync.lock` / `<socket>.lock`.

2. **Per-synchronizer state enables relay without loop prevention.** `query_audit_log_for_sync()` does NOT filter on `isFromRemote`. The receiving `sync_id`'s `_lattice_sync_state` row prevents re-upload to the same transport.

3. **Replication slots prevent premature compaction.** `safe_compact_audit_log()` only deletes entries below `MIN(confirmed_audit_id)` across all active slots.

4. **Chunked processing.** Remote apply: 50 entries per transaction. Upload sends: configurable via `chunk_size` (default 1000). Mark-synced: 50 entries per batch.

5. **Triggers are gated by sync_disabled().** All audit triggers check `sync_disabled() = 0` before firing. Set to 1 during `apply_remote_changes` to prevent feedback loops.

6. **All sync work is serialized through the scheduler.** Transport callbacks dispatch to the scheduler thread. No concurrent access to the sync `lattice_db`.

7. **is_destroyed_ guards post-destruction execution.** All scheduled lambdas check this atomic before accessing synchronizer state, preventing use-after-free.

8. **In-flight tracking prevents double-sends.** Entries added to `in_flight_ids_` before sending, removed on ACK. Cleared on error/close for retry after reconnect.

9. **Cross-process coordination.** `try_register_sync_key()` ensures at most one synchronizer per `{db_path, sync_target}` in-process. On teardown, ownership is handed off to an eligible sibling instance.
