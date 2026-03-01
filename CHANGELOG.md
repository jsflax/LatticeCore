# Changelog

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
