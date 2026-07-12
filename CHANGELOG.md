# Changelog

## [Unreleased]

## [0.10.9] - 2026-07-12

### Fixed
- **Transaction-settled observer delivery for memory DBs**: in-memory (and
  Emscripten) update hooks now only buffer; delivery drains after the
  statement that closes the transaction — observers no longer run inside the
  writer's open transaction (shared-cache readers can't hit SQLITE_LOCKED;
  callback exceptions no longer unwind through SQLite's C hook frame).
  Memory-DB observers now see ONE batch per logical transaction in commit
  order, matching file DBs. A rollback hook discards buffered changes (fixes
  a latent file-DB phantom-notification-on-rollback bug), and flush_changes
  drains until empty (fixes stranded observer write-backs).
- **Sync fan-out for named memory**: synced-status AuditLog updates now reach
  all same-name shared-cache handles (new storage_shared_across_instances()
  policy helper); isolated `:memory:` stays local-only.
- **vec0 gap-reconcile reads `main.<table>`**: on an attached clone
  connection the bare table name resolved to the TEMP union view, healing
  the attached lattice's rows into main's vec index as permanent orphans
  (scheduling-dependent; surfaced as a CI flake).

## [0.10.8] - 2026-07-11

### Fixed
- **Path-exact detach**: `lattice_db::detach(lattice&)` resolved the alias by
  recomputing it from the filename stem with no path verification — detaching
  a never-attached lattice whose stem matched an attached one silently
  DETACHed the wrong database. Now resolves by exact path; unknown path is a
  clean no-op.
- **Uncached clone factory**: new `LatticeCache::create_uncached` /
  `swift_lattice_ref.createUncached` for query-only clone connections — the
  keyed cache deduped a clone whose stripped config matched its parent and
  returned the PARENT instance, so the clone's ATTACH mutated the parent's
  own connection.
- ATTACH alias identifier double-quote escaped; `sync_tuning::apply()` drops
  `chunk_size` 0 and nonpositive backoff delays; bridge delay setters require
  `> 0`; `path_is_memory` only classifies `file:`-prefixed URIs (an on-disk
  path merely containing `mode=memory` is a regular file).

## [0.10.7] - 2026-07-11

### Fixed
- **URI-capable connections everywhere**: `SQLITE_OPEN_URI` is set on every
  connection (SQLite only URI-parses names starting with `file:`, so plain
  paths are unaffected). Fixes attaching a named-memory lattice to a
  file-backed lattice, which previously created and attached an empty
  LITERAL file named `file:<name>?mode=memory&cache=shared`.
- **attach SQL escaping**: the attached path and the union views'
  `'<label>' AS _source` literal are single-quote escaped.
- **Sync tuning validation**: `swift_configuration.set_sync_*` setters ignore
  nonsensical values (`chunk_size <= 0` would permanently stall uploads;
  negative delays/windows dropped).
- **Instance-cache tuning fingerprint**: two opens of the same path with
  different sync tuning no longer silently share the first opener's
  synchronizer.
- **`last_attach_error()` thread safety**: mutex-guarded.

## [0.10.6] - 2026-07-11

### Added
- **Sync tuning plumb-through (1.0 item I1)**: `configuration::sync_tuning` —
  an all-optional overlay (chunk_size, reconnect/backoff knobs,
  stable_connection_ms, upload_coalesce_ms, checkpoint intervals,
  use_upload_floor) forwarded into every synchronizer the database creates
  (WSS and IPC). Unset fields keep sync.hpp's defaults. Bridge:
  `swift_configuration.set_sync_*` setters expose every knob to Swift.
- **Named in-memory identity (1.0 item E1, core side)**:
  `configuration::path_is_memory()` / `is_in_memory()` recognize named memory
  URIs (`file:<name>?mode=memory&cache=shared`). Same-name handles share one
  same-process database with exactly-once cross-instance observation via the
  URI-keyed instance registry; distinct names are isolated. Read-only side
  connections (`read_db_`, `xproc_read_db_`) stay null for all memory forms,
  and no cross-process notifier is created for memory URIs (process-local).

## [0.10.5] - 2026-07-11

(Note: the apply-path scan-storm and destroy-from-callback fixes formerly
listed here actually shipped in 0.10.4 — see that entry.)


### Added
- **attach/detach rework (1.0 items ATT-1..ATT-3)**: `lattice_db::attach` is
  now idempotent (repeat attach of the same (alias, path) is a no-op; duplicate
  SQLite alias tolerated), serialized behind an attach mutex, validates the
  peer's schema through its own live handle BEFORE attaching (mismatch throws
  cleanly with no side effects), and is null-tolerant on sync-enabled lattices
  (fixes a `read_db_` null-deref crash). New `detach(lattice_db&)` /
  `detach_alias()` drop the attachment's union views, DETACH with bounded
  retry on transient locks, and rebuild remaining views order-independently
  via `rebuild_attached_views()`. Bridge: `swift_lattice::attach/detach`
  return `bool` with `last_attach_error()` — C++ exceptions no longer cross
  the Swift interop boundary (consumed by lattice's throwing ATT-4 surface).

## [0.10.4] - 2026-07-08

### Added
- **Upload pacing machinery (ships default-off)**: the hot upload-trigger sites
  (AuditLog observer, upload tail-call, post-ACK next-window kick, ack-timeout
  resend) now route through `request_upload()` — the first request after an idle
  period dispatches immediately (leading edge), while requests inside the
  coalescing window collapse into one trailing-edge tick fired by a
  per-synchronizer pacer thread. `sync_config::upload_coalesce_ms` defaults to
  `0` (exact legacy immediate dispatch) for the built-in WSS/IPC synchronizers;
  consumers that measure a need can opt in.
- **Periodic WAL checkpointing**: the pacer drives a PASSIVE checkpoint every
  `checkpoint_passive_interval_ms` (default 60s) — unconditionally, including
  while disconnected — and an idle-gated TRUNCATE every
  `checkpoint_truncate_interval_ms` (default 300s) with a busy budget and a
  same-cycle PASSIVE fallback so backfill progress always happens. New
  `database::wal_checkpoint(truncate, busy_budget_ms)` primitive returns
  `(rc, busy, log, checkpointed)`. Fixes unbounded `-wal` growth under
  long-lived daemon connections (1.1 GB observed).
- **Incremental upload floor**: a per-slot `upload_floor` cursor on
  `_lattice_replication_slots` (guarded ALTER; schema format epoch 1 → 2) turns
  each upload pass from O(AuditLog) into O(send window).
  `query_audit_log_for_sync` gains `(min_id_exclusive, limit)` — the pending
  query is a PK range scan bounded to one send window even against a stalled
  server. The floor is only a scan bound (sync-state rows stay the source of
  truth): it advances only past resolved entries (ACK or skip), and a stale-low
  floor merely re-scans a window that filters back out. The send window now caps
  total in-flight, not sends-per-invocation. Kill-switch:
  `sync_config::use_upload_floor` (default true) restores the unbounded queries.
- **Opt-in row cache on `dynamic_object`**: `enableRowCache()` puts an object in
  materialized-read mode — property gets serve from the hydrated snapshot (zero
  SQL for query-fetched objects; `refreshRowCache()` is one `SELECT *`), with a
  known-NULL contract (`nullptr` in the snapshot = known NULL; an absent key
  falls through to the live per-column path — slower, never wrong). Writes stay
  write-through and mirror stored forms into the snapshot; links/lists/unions
  always read live; `disableRowCache()` restores the default path, which is
  untouched when the cache is off. Ref-level forwards expose the API to Swift,
  and materialized reads serve the primary key from the handle itself —
  `detached()` on a materialized object is 0 SQL statements.
- `dynamic_object::increment_int_field()` — atomic SQL-side
  `SET col = col + ?` increment under the write lock: no read-modify-write race
  and half the statements of get+set.
- SQL statement counters for statement-budget regression tests: process-global
  `database::total_statement_count()` (spans all connections) and thread-local
  `database::thread_statement_count()` (race-free under parallel test suites).

### Changed
- `classify_entries` preloads are gated on batch size: batches of ≤64 entries
  use the indexed per-row helpers instead of scanning all of
  `_lattice_sync_set` and every filtered table — steady-state paced ticks carry
  a handful of entries; bulk catch-up batches preload exactly as before.
- Catch-up continuation: when an upload pass fills its send window and makes
  progress (sent or skip-resolved), the next pass dispatches immediately instead
  of waiting for a coalesce/ACK signal — a floor-bounded backlog drains without
  per-window added latency, and a stalled server still can't spin the loop.
- The ack-timeout resend gains consecutive-failure backoff (10s → 20s → …
  capped at 5 min, reset by any ACK) — a stalled server is no longer
  re-hammered with the same window at a fixed cadence.

### Fixed
- **Self-hang when an observer callback releases the last lattice
  reference**: the destructor's in-flight-callback drain wait now excludes
  the current thread's own holds (it spun on itself forever — the
  long-standing intermittent test-suite hang), and a scheduler shutdown
  reached from its own worker detaches instead of self-joining.
- **Apply-path scan storm**: per-sync-mode applies write one
  `_lattice_sync_state` row per entry, and `flush_changes`' change→audit
  lookup reverse-scanned the entire AuditLog for each (no covering index) —
  1000-entry IPC batches against a 187k-row AuditLog burned minutes of CPU
  per batch, starving ACKs into a permanent resend storm. Pass 2 now skips
  never-audited bookkeeping tables, and AuditLog gains
  `idx_audit_log_change_lookup` (schema format epoch 2 → 3, same unreleased
  train).
- **Pacer ABBA deadlock**: the pacer thread held its coordination mutex while
  performing DB work (WAL checkpoints, inline coalesced ticks under an
  immediate scheduler). A writer mid-`sqlite3_step` holds the connection mutex
  and its change-hook observer calls `request_upload()`, which takes the pacer
  mutex — the two orders deadlock whenever a checkpoint heartbeat coincides
  with a commit. The pacer now releases its mutex around all DB work.
- **Reconnect backoff no longer resets on flappy opens**: `reconnect_attempts_`
  was reset on every `on_websocket_open`, so an endpoint flapping open→error
  re-armed a ~1s reconnect storm forever. The reset now fires on close/error and
  only when the connection was genuinely up for at least
  `sync_config::stable_connection_ms` (default 60s), so the exponential ladder
  escalates as intended.
- **Exact windowed upload counters**: progress `pending` was incremented with
  the full classified backlog before sends were windowed, so multi-window
  catch-ups re-counted the un-sent remainder every pass — `pending` ratcheted
  unboundedly and never returned to 0, permanently blocking `drain()` and any
  idle-gated maintenance. Invariant now: `pending` == in-flight
  (sent-but-unACKed), maintained at every in-flight mutation; `total` counts
  only entries actually sent. `drain()` can exit mid-backlog.
- The pacer thread starts whenever checkpointing is enabled, not only when
  coalescing is — the WAL checkpoint heartbeat no longer silently dies with
  coalescing off.
- BridgeTests macOS compile: migrated to `shared_for_lattice` and by-ref
  `push_back` after the underlying API changes.

## [0.10.3] - 2026-07-04

### Fixed
- **Windowed uploads**: `send_entries` blasted the entire backlog in one burst
  (25 × ~1 MB frames in seconds), blowing up relay backpressure until the server
  reset the connection mid-send. Sends are now windowed to 2 chunks per
  invocation — entries beyond the window stay pending (not in-flight) and the
  ACK-drain path ships the next window; the ack-timeout resend covers a lost one.
- **Reconnect after a transport error without a close event**: a connection
  reset left `is_connected_` true, so the reconnect gate refused to run and the
  synchronizer sat dead until process restart. `on_websocket_error` now marks
  the transport disconnected before scheduling the reconnect.

## [0.10.2] - 2026-07-04

### Fixed
- Linux/Docker build: the bridge header's WAL maintenance calls
  (`sqlite3_wal_checkpoint_v2`, `sqlite3_wal_autocheckpoint`, `sqlite3_exec`)
  are replaced with their exact SQL equivalents (`PRAGMA
  wal_checkpoint(TRUNCATE)`, `PRAGMA wal_autocheckpoint=N`, `PRAGMA optimize`) —
  in translation units that include `sqlite3ext.h` every `sqlite3_*` name is a
  macro over an undeclared extension pointer, breaking the build. `checkpoint()`
  keeps its busy/log/checkpointed telemetry from the pragma's result row.

## [0.10.1] - 2026-07-03

### Fixed
- Value-path (pre-FRT) migration backlink API:
  `migration_take_backlink_result` was declared FRT-pointer-only and
  `migration_lookup_backlinks` was defined only inside the FRT branch — below
  the FRT floor (iOS < 16.4) the first failed to import correctly in Swift and
  the second was a linker hole. Both now follow the dual-declaration
  convention, with value-path definitions returning an empty ref
  (`isValid() == false`) when absent. No behavior change on the FRT path.

## [0.10.0] - 2026-07-03

### Added
- **iOS 15 support — `LATTICE_HAS_FRT` floor switch**: Swift foreign-reference
  types are hard-floored at iOS 16.4 / macOS 13.3 and don't back-deploy. Below
  the floor the `*_ref` handle family now compiles as plain copyable value types
  over the same `shared_ptr`-owned state (factories return by value; the
  delegating surface is `const` so Swift holds the handles in `let`s), so the
  existing C++-interop Swift code runs on iOS 15 without a separate C backend.
  `std::format` is gated behind availability (`LATTICE_HAS_STD_FORMAT`) and the
  minimum iOS version drops to 15.
- **Schemaless (dynamic) open**: the full Swift schema (property descriptors,
  unions, constraints) is persisted into `_lattice_meta` on the DDL path and
  reconstructed on an open with no compile-time types (with a
  `sqlite_master`/PRAGMA fallback). `swift_lattice_ref.create_dynamic(config:)`
  opens read-only with no DDL; `dynamic_table_names()` /
  `dynamic_properties_for()` expose the schema to Swift. Backs the Lattice MCP
  query server, which now reads live rooms directly — no checkpoint required.
- **Union type support**: schema, storage, and bridge layer for discriminated
  union fields; union table schema evolution (rebuild on case addition); union
  values set in migration callbacks are persisted correctly (tables created
  before the migration loop, values persisted after the table rebuild).
- **IVF vector search**: sqlite-vec upgraded to v0.1.10; count-only
  nearest-query path; nearest-query CTE builder extracted.
- **`@Unique(compoundedWith:)` on link-kind fields**: link columns live in a
  separate table, so the UNIQUE index is built on shadow `<field>__link_gid`
  columns kept in sync by link-table triggers (shadow columns never ride sync).
  Adding a `@Unique` constraint over existing duplicate data auto-deduplicates
  (keeps the newest row per group) before retrying index creation, and a bare
  `@Unique` (no arguments) now generates its constraint.
- **Fresh-peer replay handshake (IPC)**: a recreated synced DB used to receive
  nothing on reconnect (the peer's sync set marked every row already-synced).
  An IPC synchronizer that has never applied a remote entry now announces a
  `replay_request` on connect, and the peer re-arms its filtered snapshot
  (reset + reconcile + upload). IPC only — never WSS.
- **Bounded teardown drain**: `drain(deadline)` runs one upload pass and waits
  until everything sent is ACKed, the connection dies, or the deadline passes;
  teardown runs it with a single 2s budget shared across all synchronizers.
  Dropping the last reference right after a write no longer cuts the in-flight
  sync entry. Definitively idle synchronizers skip the drain round-trip
  entirely.
- `find_indices()` bridge method: position-only list queries from joined SQL,
  skipping dangling links.
- Batched `generate_history()` (per-table batches in their own transactions,
  with a transient dedup index) and `cxx_error` propagation of C++ exceptions
  across the Swift bridge.
- `LATTICE_LOG_LEVEL` env var seeds the log level (0=off … 4=debug) without
  code changes.
- Comprehensive sync system documentation.

### Changed
- **No more `ANALYZE` at open.** `database::database` no longer runs `ANALYZE` on
  every read-write open — on multi-GB databases this scanned every index (seconds
  of I/O) while holding the write lock at the worst possible moment. Stats now
  refresh incrementally: `PRAGMA analysis_limit=400` is set at open and a
  best-effort `PRAGMA optimize` runs in the destructor. Long-lived processes that
  never destruct cleanly should run `PRAGMA optimize` from their own maintenance
  paths.
- **WAL bounded via `journal_size_limit`.** Read-write connections set
  `PRAGMA journal_size_limit=256MB` so successful TRUNCATE/RESTART checkpoints
  shrink the `-wal` file instead of leaving it fully allocated.
- **Busy timeout is configurable and defaults to 30s.** New
  `configuration::busy_timeout_ms` (default `kDefaultBusyTimeoutMs` = 30000,
  previously hardcoded 5000) applies to all connections of a `lattice_db`.
  Interactive apps that write on the main thread should set a small value
  (e.g. 5000). `begin_transaction` now zeroes the statement-level timeout during
  its retry loop and uses a wall-clock 30s deadline — previously each BEGIN
  attempt busy-waited the full statement timeout *and* the loop only counted its
  own sleeps, multiplying the worst-case wait far past the intended budget.
- **LatticeCache: build on-disk instances outside the cache lock.** `get_or_create`
  no longer holds `mutex_` across `swift_lattice` construction (DB open, migration,
  `ensure_swift_tables` COMMIT, sync/IPC setup). Concurrent opens of the same key now
  de-dup via a per-key in-flight `std::shared_future` instead of serializing the whole
  function, so `get_by_pointer` (the per-object materialization / `resolve()` hot path)
  and unrelated opens no longer block while another connection is opening or migrating.
  The `:memory:` path is unchanged (still constructs under the lock; each open stays a
  distinct instance).
- **Read-only opens are WAL-aware; `immutable` mode removed.** SQLite's
  `immutable=1` structurally ignores the `-wal` file, so a reader saw only the
  last checkpoint (a live DB with a 213 MB WAL read as 0 rows).
  `open_mode::read_only_immutable` is gone; read-only opens join a concurrent
  writer's WAL and see committed-but-not-yet-checkpointed rows.
- **Cascade delete overhauled**: indexed lookups (link / virtual-link / list
  tables by target) replace per-table try/catch scans, with bulk DELETE per
  cascade target; the parent DELETE and cascade cleanup commit in a single
  transaction; cascade audits are delivered to table observers in one batch.
- **Batched table observers**: `add_table_observer` callbacks now take
  `const std::vector<change_event>&`; `flush_changes` dispatches once per WAL
  flush with each table's slice, so a 3-row transaction delivers one 3-element
  batch and wire-relay consumers ship one frame per logical transaction.
- AuditLog observers fire once per row, not coalesced per batch — consumers
  that yield the specific delivered row no longer strand the rest of a
  multi-row transaction with `isSynchronized=0`.
- Upserts emit `DO NOTHING` when the `DO UPDATE SET` clause would be empty
  (previously invalid SQL), and the conflict path re-reads `globalId` from the
  write connection so link writes never reference nonexistent globalIds.
- Preserved globalIds are lowercased at ingestion (`bind_managed`): uppercase
  `UUID.uuidString` values broke case-sensitive TEXT joins through link tables
  (`@Relation` backlinks silently filtered synced rows out).
- Logging is thread-safe (mutex-protected `fprintf` + `fflush`).

### Fixed
- **Sync state collapses against the live slot registry (+ startup heal)**:
  audit entries collapse to `isSynchronized=1` once every sync channel has
  acknowledged them, but the check compared against an init-time config
  snapshot — a configured-but-never-running channel made collapse unreachable,
  orphaning `_lattice_sync_state` rows and wedging the passive pending count.
  Both mark paths now compare against the live `_lattice_replication_slots`
  registry, and `heal_collapsed_sync_state()` at open repairs existing
  databases.
- **Unshare no longer fleet-deletes**: narrowing a sync filter emitted plain
  DELETE audit entries that rode the normal upload pipeline — the server
  applied and relayed them, destroying every peer's copy. Filter-removal
  DELETEs now carry a `__lattice_filter_removal` marker and are scoped to one
  IPC hop; WSS receivers skip applying marked DELETEs but still ACK them.
- **At-least-once delivery**: a sent frame can vanish without any transport
  error. After `send_entries`, a timer waits up to 10s for the batch's ACKs and
  releases + re-enqueues any entries still in-flight, so dropped frames recover
  without waiting for a reconnect. Re-delivery is safe (applies are idempotent
  via `ON CONFLICT(globalId) DO UPDATE`).
- **IPC dialer auto-reconnect**: IPC synchronizers hard-disabled reconnection
  and stayed dead forever after a peer restart. `sync_transport::
  supports_reconnect()` now drives the behavior — dialers (with a socket path)
  redial; server-accepted fd wrappers never do.
- **Busy timeout installed before open-time pragmas**: the busy handler was
  configured after `PRAGMA journal_mode`/`cache_size` ran, so the very first
  statement of every connection had no lock protection — a concurrent open or
  in-flight writer made open throw "database is locked" instantly.
- **Close-after-use safety**: an atomic `closed_` flag on the database wrapper
  is checked at the top of each SQL op, `close()` only sets the flag
  (`sqlite3_close_v2`; the connection is freed solely in the destructor after
  sync/scheduler threads join), and the Swift write funnels early-return when
  closed — a reader on another thread can never dereference a freed handle
  (previously a logout-time `Lattice.delete()` under a live `@LatticeQuery`
  crashed). `LatticeCache::evict()` no longer erases `ptr_cache_` (use-after-
  free when delete/close raced a concurrent read).
- `dynamic_object::get_object()` on an unset to-one link returned by segfault:
  a nil link now yields an empty (unmanaged) object, and missing target tables
  in partially reconstructed schemas are null-checked.
- LatticeCache re-entrancy deadlock: SQLite's commit hook fires synchronously
  inside `swift_lattice` construction and re-entered the cache mutex via
  sibling observer notification; the mutex is now recursive.
- Conflicting-URL same-process siblings are kicked off the sync flock
  (`teardown_sync(fire_handoff: false)`) so a new opener with a different
  `wssEndpoint` can acquire sync ownership; same-URL dormants still skip
  silently.
- Cascade-clean of link and union tables on row delete: the cascade filter
  skipped every link table, so deletes left orphaned link rows whose traversal
  segfaulted on peers; union tables (no `rhs` column) get a dedicated
  post-cascade pass, and are skipped by the generic rhs cascade.
- vec0: correct table names for schema-qualified (attached) model tables; the
  vector CTE joins the per-schema model table instead of the UNION ALL view;
  IVF reconcile uses a LEFT JOIN on the `_rowids` shadow table.
- WASM/browser build: no-thread guards for the reconnect backoff loop (ran
  inline under `immediate_scheduler` and froze the tab), the ack-timeout
  resend, and vec0 reconciliation; the bridge try/catch utility is gated behind
  `__BLOCKS__` for non-blocks compilers.

## [0.9.2] - 2026-03-27

### Added
- `lattice_set_log_level()` in the C API.

### Fixed
- vec0 tables created lazily at query time are backfilled from the model table
  on creation — rows inserted before the vec0 table existed (C API / Python /
  Kotlin path) are searchable again (restores the reconciliation removed in
  0.9.1's move to init-time).

## [0.9.1] - 2026-03-26

### Added
- `rebuild_vec0()` / `vacuum_vec0()` APIs for explicit vector-index
  maintenance.
- `apply_remote` diagnostic logging for sync debugging.

### Changed
- vec0 reconciliation moved from per-query to init-time
  (`ensure_swift_tables`), filling gaps from cross-connection IPC sync writes
  without taxing every query.

### Fixed
- `has_value()` falls through to a DB check when the property isn't in the
  `properties_` map — Optional property loading works on models not passed to
  Lattice init (e.g. AuditLog queried via the ORM).
- Defensive vec0 orphan cleanup on delete handles entries left behind by
  unreliable DELETE triggers under write contention.
- `get_table_name` uses the proper accessor.

## [0.9.0] - 2026-03-16

- Re-release of 0.8.6 under a new minor version — no source changes (the tag
  points at the same commit as 0.8.6).

## [0.8.6] - 2026-03-16

### Added
- **VirtualList / VirtualLink**: polymorphic relationships via discriminated
  link tables with `rhs_type` columns; the sync receiver creates virtual link
  tables (with `rhs_type`) when the audit entry carries `rhs_type` in its
  changed fields.
- **Link table observation**: internal-table changes fire both link-table and
  parent-table observer notifications with changed field names, enabling Swift
  Observation property tracking on relationship mutations.
- Explicit IPC socket path support (cross-platform IPC between processes with
  different HOMEs).
- vec0 write-path reconciliation (`reconcile_vec0`) with hardened embedding
  triggers.
- Expanded C API surface, plus a native C++ unit-test suite (vendored
  GoogleTest + CMake build): Bridge, CascadeDelete, InstanceLifecycle,
  MultiConnection, Observation, Query, Schema, SyncUnit, and Vec0 tests.

### Changed
- Junction tables are eagerly created at startup (fixes relay/downstream
  nodes that receive link rows before ever writing one).
- `COLLATE NOCASE` on all link-table globalId queries for case-insensitive
  matching.
- List `size()` uses `load_if_needed()` for Collection conformance
  consistency.

### Fixed
- Cascade delete removes referencing link-table entries when objects are
  deleted.
- Cross-process notifications resolve internal tables to a parent UPDATE with
  property names and a parent rowId lookup.

## [0.8.5] - 2026-03-06

### Added
- `_lattice_post_cross_process_notification()` test helper for posting Darwin
  notifications from external writers.

### Fixed
- Cross-process sync progress observer not firing for AuditLog UPDATEs: when
  the sync daemon marks entries synced (`isSynchronized=1`) no new audit rows
  appear, and the cursor-based notification handler returned early — passive
  progress observers never re-queried `pending_sync_entry_count()`. A synthetic
  AuditLog UPDATE notification now fires in the empty-rows path.

## [0.8.4] - 2026-03-05

### Changed
- **Unlimited reconnect with capped exponential backoff**: the synchronizer
  used to give up after 6 attempts (~63s), permanently killing sync in
  long-lived daemons on a transient outage. `max_reconnect_attempts` now
  defaults to 0 (unlimited) with the delay capped at 60s
  (1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, …).

## [0.8.3] - 2026-03-05

- Re-release of 0.8.2 — no source changes (the tag points at the same commit
  as 0.8.2).

## [0.8.2] - 2026-03-05

### Fixed
- Deadlock in `mark_as_synced` under `immediate_scheduler`: the in-flight
  mutex was held across a synchronous `scheduler->invoke()` whose upload path
  re-locked the same non-recursive mutex; the lock is released before invoking.
- `receive_sync_data` crash: C++ exceptions are caught at the Swift boundary
  (previously `std::terminate` → SIGTRAP) and surfaced via the
  `last_receive_error()` accessor.
- Migration lookup crash when the target model has new columns: schema
  properties are filtered to columns that exist in the DB so `detach()` no
  longer SELECTs not-yet-added columns ("no such column").

## [0.8.1] - 2026-03-04

### Added
- `LATTICE_DISABLE_XPROC` env-var kill-switch for cross-process notifications.
- `backdate_replication_slots()` test helper for deterministic stale-slot
  eviction.

### Changed
- `notify_changes_batched` coalesces AuditLog table observers to fire at most
  once per batch (model table observers still fire per row).

## [0.8.0] - 2026-03-04

### Added
- **Replication slots** for slot-aware audit log compaction: `safe_compact`
  deletes only entries confirmed by all synchronizers; `force_compact`
  regenerates snapshots and resets slot cursors; stale-slot eviction with a
  configurable threshold.
- `is_sync_agent()` — file-lock-based sync ownership detection.
- Sync error and state-change callbacks (`set_on_sync_error`,
  `set_on_sync_state_change`).
- `pending_sync_entry_count()` for cross-process progress observation.
- `distinct_by` support on `count()` and `objects()`; `combinedNearestQuery`
  supports `distinctBy` for nearest results.
- C++ try/catch bridge utility.

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
