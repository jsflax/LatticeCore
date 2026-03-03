# Changelog

## [0.7.0] - 2026-03-03

### Added
- **Sync Progress API**: Real-time upload/download progress tracking
  - `synchronizer::sync_progress` struct with `pending_upload`, `total_upload`, `acked`, `received` counters
  - `get_progress()` polling and `set_on_progress()` callback APIs
  - `lattice_db::get_sync_progress()` aggregates progress across all synchronizers (WSS + IPC)
  - `lattice_db::set_on_sync_progress()` callback fires on synchronizer thread
  - Swift bridge: `sync_progress_*()` polling methods and `set_on_sync_progress()` C callback
- **Owned-DB Synchronizer Architecture**: Each synchronizer owns a dedicated `lattice_db` connection
  - Constructor takes `std::unique_ptr<lattice_db>` instead of `lattice_db&`
  - Each synchronizer runs on its own `std_thread_scheduler`, isolating sync I/O from the caller's thread
  - Cross-process notifications bridge writes between parent and sync connections
  - Eliminates SQLite write contention between sync operations and application writes
- **Instance Guard System**: Safe cross-instance observer notifications
  - `instance_guard` with atomic `alive` flag and `notify_refcount` for in-flight call tracking
  - `instance_registry::for_each_alive()` replaces raw `get_instances()` — prevents use-after-free when instances are destroyed during notification
  - Destructor spins on `notify_refcount` before proceeding, ensuring in-flight `notify_change()` calls complete
- **Scheduler Resilience**
  - `std_thread_scheduler::run_loop()` catches exceptions from work items (logs and continues instead of killing the thread)
  - `scheduler::shutdown()` virtual method for orderly drain — blocks until in-flight work completes
  - Synchronizer destructor calls `scheduler_->shutdown()` to drain in-flight lambdas before member destruction
  - `is_destroyed_` atomic flag guards all scheduled lambdas against post-destruction execution
- **In-Flight Tracking**: Prevents duplicate upload of entries awaiting ACK
  - `in_flight_ids_` set with mutex tracks sent-but-unacknowledged entries
  - `query_pending_entries()` filters out in-flight entries before upload
  - ACK handler removes acknowledged entries from in-flight set
- **Upload Coalescing**: Observer-triggered uploads batched through scheduler
  - `upload_requested_` atomic flag coalesces rapid AuditLog INSERTs into a single upload cycle
  - Tail-call re-scheduling: if new entries arrive during upload, re-queues without starving other work
- **IPC Stale Socket Recovery**: Probe logic detects and cleans up sockets from crashed processes
  - `ECONNREFUSED` on probe → unlink stale socket file, retry bind as server
  - Zombie detection: connected probe polls for `POLLHUP` / `POLLIN` with `recv(MSG_PEEK)` to distinguish dead peers from live servers sending data
- **Deferred Sync Setup**: `lattice_db(config, defer_sync=true)` delays sync/IPC initialization
  - `swift_lattice` uses deferred mode so all Swift model tables are created before sync starts
- **Sync Filter Versioning**: Prevents stale filter reconciliation from rapid toggles
  - `filter_version_` atomic counter — scheduled reconcile checks version before acting
- Synchronizer destroy-during-scheduled-work race condition test (`test_synchronizer_destroy_race`)
- Lifecycle logging: `lattice_db`, `std_thread_scheduler`, and `synchronizer` log CREATED/DESTROYED with alive counts

### Changed
- **Synchronizer constructor**: Takes `std::unique_ptr<lattice_db>` (owned) instead of `lattice_db&` (borrowed)
  - Legacy single-sync path removed — all synchronizers use per-synchronizer `sync_id` state
  - `setup_observer()` and `setup_transport_handlers()` extracted as shared helpers
- **Teardown ordering**: `close()` and `~lattice_db()` follow strict phase ordering:
  1. Mark `guard_->alive = false` (prevents new notifications)
  2. Spin on `guard_->notify_refcount` (waits for in-flight notifications)
  3. Disconnect ALL synchronizers before destroying any (prevents cross-sync use-after-free in flush_changes)
  4. Unregister from instance registry
- **IPC synchronizer dedup**: `try_register_sync_key()` now covers IPC channels (not just WSS), preventing duplicate synchronizers across instances sharing the same database
- **Sync filter reconciliation**: Inserts synthetic AuditLog entries instead of sending directly, leveraging the normal upload pipeline (in-flight tracking, chunking, ACK confirmation)
- `apply_remote_changes()` and `apply_remote_changes_for()` return `std::vector<std::string>` of successfully applied global IDs (previously void)
- `build_insert_entry_from_current_row()` returns `std::optional` instead of throwing when the row is deleted
- `mark_audit_entries_synced_for()` processes in chunks of 50 to avoid holding write lock
- `on_transport_message()` dispatches apply and ACK to scheduler thread (prevents SQLite contention with IPC/NIO read thread)
- `on_websocket_open()` dispatches initial reconcile + upload to scheduler (avoids blocking IPC accept thread)
- `schedule_reconnect()` uses interruptible 50ms sleep loop (checks `is_destroyed_`) instead of a single blocking sleep
- WAL checkpoint on close changed from `TRUNCATE` to `PASSIVE` (non-blocking)
- `ANALYZE` on init uses raw `sqlite3_exec` to avoid retry-timeout on busy databases
- `_lattice_sync_state` excluded from update hook (internal table)
- `update_sync_filter()` / `clear_sync_filter()` propagate to IPC synchronizers (not just WSS)
- `LatticeRefCacheKey` includes `schema_version` — differentiates pre/post migration instances
- Cache no longer skips entries for `target_schema_version > 1` (migration is idempotent)
- `query_audit_log_for_sync()` accepts optional `sync_filter` parameter for pre-filtering at query time

### Fixed
- **IPC probe misidentifying live server as dead**: `POLLIN` set by real sync data during 50ms probe window caused the target to incorrectly become a second server. Now uses `recv(MSG_PEEK)` to distinguish EOF from real data.
- **IPC SIGPIPE crash**: `suppress_sigpipe()` applied to all IPC sockets (server, accepted client, connected client). Without `SO_NOSIGPIPE` (macOS), writing to a closed peer kills the process.
- **IPC send interleaving**: `send()` uses `send_mutex_` to prevent interleaved length-prefix frames from concurrent callers. `fd_` changed to `std::atomic<int>` for safe concurrent read/close.
- **Scheduler thread death on exception**: Any exception from a work item (e.g., `SQLITE_BUSY` after retry timeout) killed the scheduler thread silently, dropping all future work (ACKs, uploads, applies). Tests hung waiting for conditions that required the dead scheduler.
- **Infinite resend loop**: `query_audit_log_for_sync()` WHERE clause now includes `AND a.isSynchronized = 0` to prevent eagerly-cleaned entries (deleted `_lattice_sync_state` rows) from being re-queried as pending
- **Enum deserialization crash**: `getField` returns `defaultValue` instead of `fatalError` for unknown raw values (graceful handling of schema evolution)
- Hex-encoded BLOB fields from `build_insert_entry_from_current_row()` stored as `string_kind` (matching AuditLog trigger format) instead of `data_kind`
- `any_property::to_column_value()` handles hex-encoded BLOB strings stored in `data_kind` variant
- `on_transport_message()` guards against use-after-free from late NIO/IPC callbacks via `is_destroyed_` check

## [0.6.0] - 2026-02-28

### Added
- **IPC Transport**: Cross-process sync via Unix domain sockets
  - `ipc_server` — binds and listens on a channel socket
  - `ipc_socket_client` — implements `sync_transport` interface
  - Length-prefix framing over `SOCK_STREAM`
  - Channel-based naming: `resolve_ipc_socket_path()` maps channel names to platform paths
  - macOS: `~/Library/Caches/Lattice/ipc/<channel>.sock`
  - Linux: `$XDG_RUNTIME_DIR/lattice/<channel>.sock` (fallback: `/tmp/lattice-<uid>/`)
- **Per-Synchronizer Sync State**: `_lattice_sync_state` table
  - Independent sync tracking per transport via `sync_id`
  - `query_audit_log_for_sync()` — returns pending entries for a specific synchronizer
  - Backwards compatible: single-synchronizer mode still uses `isSynchronized` column
- **Cloud Relay Mode**: `sync_apply_mode::relay`
  - IPC-received entries written with `isFromRemote=1, isSynchronized=0`
  - Receiving synchronizer's own `sync_id` marked as synced (loop prevention)
  - Other synchronizers (WSS, other IPC) see entries as pending
- **Filtered Sync**: Per-table upload filtering
  - `sync_filter_entry` with table name + SQL predicate
  - `reconcile_sync_filter()` — generates initial sync set from filter
  - Applied at upload time; downloads always accepted
- `collect_all_sync_ids()` — gathers WSS + IPC sync IDs for multi-transport
- IPC integration tests (C++ test suite)

### Changed
- `websocket_client` interface renamed to `sync_transport`
- `synchronizer` now accepts `sync_transport*` instead of `websocket_client*`
- `upload_pending_changes()` uses `_lattice_sync_state` JOIN when `sync_id` is set

### Fixed
- Linux `resolve_ipc_socket_path` test assertion for Docker environments without `XDG_RUNTIME_DIR`

## [0.5.1] - 2026-02-27

### Added
- Preserved global ID support for object insertion

### Fixed
- Cache eviction crash
- Migration guard
- Attached DB vector queries

## [0.5.0] - 2026-02-26

### Added
- `@Indexed` attribute support for non-unique column indexes
