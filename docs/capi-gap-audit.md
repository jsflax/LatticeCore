# C-ABI Gap Audit — bridge header vs `lattice.h`

**1.0 roadmap item C1, first deliverable** (the plan's "first commit: systematic
bridge-header-vs-lattice.h diff audit").

- Audited at: `f5f7cd7` (2026-07-12, branch HEAD)
- C ABI: `Sources/LatticeCAPI/include/lattice.h` — **95 functions declared, 93
  implemented** in `Sources/LatticeCAPI/src/lattice.cpp` (2 declared-but-missing, see B-9).
  Header last touched `e8b5917` **2026-03-27** (frozen since, as the plan states).
- Bridge surface: `Sources/LatticeSwiftCppBridge/include/lattice.hpp` (3,143 lines;
  last touched **2026-07-12**), plus `dynamic_object.hpp`, `list.hpp`,
  `geo_bounds.hpp`, `unmanaged_object.hpp`, `managed_object.hpp`, `LatticeBridge.hpp`.
- Structural note that shapes the whole audit: **the C ABI is implemented ON TOP of
  the bridge** (`lattice_db_t` = `swift_lattice_ref`, `lattice_object_t` =
  `dynamic_object_ref`, `lattice_link_list_t` = `link_list_ref`; see
  `LatticeCAPI/src/lattice.cpp:24-38`). Every bridge capability is therefore
  *mechanically* exposable; the gaps below are purely "nobody wrote the wrapper"
  plus a handful of places where the existing wrappers drifted.
- `swift_lattice` publicly inherits `lattice_db`, so Swift can also reach core
  methods that have no explicit forwarder (`sync_now`, `connect_sync`,
  `disconnect_sync`, `storage_shared_across_instances`, …). Those count as bridge
  surface here, marked *(inherited)*.

Flags used in the tables:

- **PLAN** — explicitly listed in the plan's C1 section, with the plan's disposition.
- **UNENUMERATED** — existed on the bridge before the plan was written (≤ Jul 9)
  but is absent from the plan's C1 list. The plan predicted these ("assume more exist").
- **NEW-SINCE-PLAN** — landed on the bridge Jul 10–12, after the plan's C1 list froze.

---

## A. Bridge capabilities with NO C-ABI equivalent

### A1. Schema & data model

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 1 | Compound unique constraints + `allows_upsert` (`swift_constraint`, `swift_schema_entry.constraints`) | lattice.hpp:152-174 | **Unreachable.** `lattice_unique_constraint_t` is *declared* in lattice.h:79-84 but **no C function accepts it**; `convert_schemas()` (capi lattice.cpp:117) never populates constraints → upsert semantics cannot be declared from C at all | **UNENUMERATED** — dangling type in the frozen header |
| 2 | Union types: `property_kind::union_type` (=5), `union_value` typed get/set, `get_union`/`set_union` on objects | lattice.hpp:59-149, dynamic_object.hpp:365-366, 564-565 | None. C `lattice_property_kind_t` stops at `VIRTUAL_LINK` (=4); `lattice_property_t` has no union descriptor; no union accessors on `lattice_object_t` | **UNENUMERATED** |
| 3 | geo_bounds **value** accessors: `get/set_geo_bounds`, `has_geo_bounds`, geo lists (`add/clear/remove/get_at`), `geo_bounds_list_ref` (incl. `find_where`) | dynamic_object.hpp:299-362, geo_bounds.hpp | None. C can *declare* `is_geo_bounds` and *query* R\*Tree (`lattice_db_query_within_bounds`) but **cannot read or write a geo_bounds value** — the feature is write-dead from C | **UNENUMERATED** — arguably the largest silent parity hole after sync |
| 4 | Virtual list/link **access**: bridge `get_link_list` resolves virtual kinds | dynamic_object.hpp:204-209 | Partial/unverified: C can declare `VIRTUAL_LIST`/`VIRTUAL_LINK` kinds and `lattice_object_get_link_list` wraps the same bridge call, so reads *may* work; never tested from C (no C-level test target exists) | UNENUMERATED (verification item for C1's test target) |
| 5 | `add_bulk` (single-transaction bulk insert, 3 overloads + ref forwarder) | lattice.hpp:675-691, 2833-2836 | None — C callers loop `lattice_db_add` (one tx each unless hand-wrapped) | **UNENUMERATED** |
| 6 | Named-memory URIs (`file:<name>?mode=memory&cache=shared`, core `path_is_memory`) | core lattice.hpp:540 | *Reachable but undocumented*: the URI passes through `lattice_db_create_with_schemas(path=…)`; lattice.h's comment ("NULL or \":memory:\"") predates the named form | **NEW-SINCE-PLAN** (E1.5 memory-model work) — docs-level gap |
| 7 | Dynamic (schema-from-file, read-only) open: `create_dynamic`, `dynamic_table_names`, `dynamic_properties_for`, `configuration.read_only` | lattice.hpp:2686-2697, 2858-2864 | None (no read-only flag anywhere in C) | **UNENUMERATED** |

### A2. Object read/write performance & introspection

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 8 | Row cache: `enable_row_cache` / `disable_row_cache` / `is_row_cache_enabled` / `refresh_row_cache` (object + ref) | dynamic_object.hpp:123-131, 541-544 | None | **PLAN** — "row cache enable/disable/refresh/is-enabled" (S) |
| 9 | `increment_int_field(name, delta)` — SQL-side atomic increment | dynamic_object.hpp:278-295, 545-547 | None | **PLAN** — `lattice_object_increment_int` (S) |
| 10 | Statement counters: `total_sql_statement_count()`, `thread_sql_statement_count()` (static) | lattice.hpp:831-838 | None | **PLAN** — total/thread counters (S) |
| 11 | `lattice_object_to_json(max_depth)` (planned; Detached one-liner) | — (Swift-side only; no C++ impl exists) | None | **PLAN** — sized **M** (no C++ object-graph→JSON machinery exists yet) |
| 12 | Versioning/introspection: `lattice_capi_version[_string]()`, `lattice_schema_format_epoch()` (`kLatticeSchemaFormatEpoch = 3`, core lattice.hpp:4989), `lattice_capi_has_feature(name)` | — | None (exists nowhere yet) | **PLAN** |
| 13 | `lattice_get_log_level()` (C has set only) | lattice.hpp:36 | None | UNENUMERATED (trivial) |
| 14 | Handle conveniences: `path()`, `valid()`, `hash_value()` | lattice.hpp:2796-2805 | None | UNENUMERATED (trivial) |

### A3. Maintenance & storage

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 15 | `checkpoint()` (WAL TRUNCATE w/ logged outcome) | lattice.hpp:843-865, 2896 | None | **PLAN** — `lattice_db_checkpoint(mode)`; note bridge is TRUNCATE-only today, the planned C fn takes a `mode` |
| 16 | `optimize()` (PRAGMA optimize) | lattice.hpp:871-873, 2897 | None | **UNENUMERATED** |
| 17 | `vacuum()` (with read-connection close/reopen) | lattice.hpp:1008-1017, 2890 | None | **UNENUMERATED** |
| 18 | Audit-log variants: `safe_compact_audit_log(stale_threshold_s)`, `force_compact_audit_log`, `backdate_replication_slots(seconds)` | lattice.hpp:811-821, 2891-2895 | C has only plain `lattice_db_compact_audit_log` + `lattice_db_generate_history` | **UNENUMERATED** |
| 19 | vec0 maintenance: `upsert_vector`, `delete_vector`, `rebuild_vec0`, `vacuum_vec0`, `train_untrained_vec0_tables`, `wait_for_vec0_training` | lattice.hpp:561-566, 875-905, 1152-1164, 2885-2889 | None | **UNENUMERATED** |
| 20 | `storage_shared_across_instances()` *(inherited from core)* | core lattice.hpp:1398 | None | **NEW-SINCE-PLAN** (Jul 11-12 memory-model work) |
| 21 | `create_uncached(config, schemas)` — cache-bypassing clone for query-only attach connections | lattice.hpp:2752-2762 | None (all C creators go through the keyed LatticeCache) | **NEW-SINCE-PLAN** (Jul 11, `443ad8d`) |

### A4. Sync — configuration, observability, control

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 22 | Sync-tuning knobs (9): `set_sync_chunk_size`, `max_reconnect_attempts`, `base_delay_seconds`, `max_delay_seconds`, `stable_connection_ms`, `upload_coalesce_ms`, `checkpoint_passive_interval_ms`, `checkpoint_truncate_interval_ms`, `use_upload_floor` on `swift_configuration` | lattice.hpp:446-464 | None — C `lattice_db_create_with_sync` is positional (path, schemas, scheduler, url, token) and can't grow | **PLAN** — size-prefixed `lattice_sync_options_t` (`struct_size` first, `_init()` defaults) (M). Bridge side landed Jul 11 (`b91a228`, I1) |
| 23 | Sync progress: `set_on_sync_progress` callback + `synchronizer::sync_progress {pending_upload, total_upload, acked, received}` (obj + ref) | lattice.hpp:949-970, 3013-3021; core sync.hpp:255-260 | None | **PLAN** — "sync progress struct + observe callback (0.7-era gap)" |
| 24 | `set_on_sync_error(cb)` | lattice.hpp:972-987, 3022 | None (C can only poll `lattice_db_is_sync_connected`) | **UNENUMERATED** (adjacent to #23; should ride the same commit) |
| 25 | `set_on_sync_state_change(cb)` | lattice.hpp:989-1004, 3027 | None | **UNENUMERATED** |
| 26 | `set_on_xproc_idle(cb)` | lattice.hpp:932-947, 3032 | None | **UNENUMERATED** |
| 27 | `is_sync_agent()` | lattice.hpp:913, 2904 | None | **UNENUMERATED** |
| 28 | `pending_sync_entry_count()` | lattice.hpp:916-927, 3010 | None | **UNENUMERATED** |
| 29 | `detach(other)` (idempotent, exception-safe) | lattice.hpp:1047, 2969-2975 | **None — C has attach but no detach at all** | **NEW-SINCE-PLAN** (ATT-3, Jul 11 `acb37c1`) |
| 30 | `last_attach_error()` (mutex-guarded accessor) | lattice.hpp:1049-1052, 2976 | None (and unreachable — see B-1) | **NEW-SINCE-PLAN** (Jul 11) |
| 31 | `last_receive_error()` (receive_sync_data failure detail) | lattice.hpp:2124, 2980 | Partial — C sets thread-local `lattice_last_error()` on *total* failure, but cannot report per-batch partial state (see B-2) | UNENUMERATED |
| 32 | `lattice_db_read_upload_floor` (planned conformance debug hook; `upload_floor` column exists in `_lattice_replication_slots`, core lattice.hpp:4899-4910) | — | None (nowhere yet) | **PLAN** |
| 33 | Unified `lattice_db_open(const lattice_open_options_t*)` | — | None (five non-composable creators) | **PLAN** — DECIDED IN with the rc−2wk drop-gate on the migration-ABI redesign |

### A5. Query surface

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 34 | Combined multi-constraint nearest: `combined_nearest_query` / `combined_nearest_query_count` (bounds[] + vectors[] + geos[] + texts[] + where + sort_descriptor + group/distinct, per-column distances) | lattice.hpp:1859-2077, 2934-2963 | None. C has only single-constraint `lattice_db_query_nearest` (IDs+distance), `query_fts`, `query_within_bounds` | **UNENUMERATED** — the entire post-0.9 query engine |
| 35 | `nearest_neighbors` (objects+distances in one call) | lattice.hpp:1169-1190 | C returns global-ids only; caller must N× `lattice_db_find_by_global_id` | UNENUMERATED (workaround exists) |
| 36 | `geo_nearest` (radius + Haversine sort, distances returned) | lattice.hpp:1345-1437 | None | **UNENUMERATED** |
| 37 | `count_within_bbox` | lattice.hpp:1293-1339, 2923 | None (`query_within_bounds` + client count only) | UNENUMERATED |
| 38 | `objects_within_bbox` extras: `order_by`, `offset`, `group_by` params | lattice.hpp:1210-1220 | C `lattice_db_query_within_bounds` has where+limit only | UNENUMERATED (partial) |
| 39 | `union_objects` (polymorphic multi-table UNION query with `_type` dispatch) | lattice.hpp:745-775, 2866 | None | **UNENUMERATED** |
| 40 | `objects()` `group_by` parameter | lattice.hpp:717-724 | C `lattice_db_query` lacks it; `lattice_db_query_distinct` covers `distinct_by` only (`count_distinct` does take group_by) | UNENUMERATED (partial) |
| 41 | Link-list queries: `find_index`, `find_where(predicate)`, `find_indices(predicate, orderBy, ascending)`, `get_link_table_name` | list.hpp:143-164, 305-319 | C link list = size/get/push_back/erase/clear only | **UNENUMERATED** |

### A6. Observation & migration

| # | Bridge capability | Anchor | C-ABI status | Disposition / flag |
|---|---|---|---|---|
| 42 | `remove_all_object_observers(table, row)` | lattice.hpp:1143-1145 | None (token-wise removal only) | UNENUMERATED |
| 43 | Multi-version migration: `swift_configuration::addMigrationSchema(version, table, from, to)`, `setRowMigrationCallback`, `migration_get_current_version`, `migration_lookup[_by_global_id]`, `migration_lookup_backlinks`, `migration_take_*`, `migration_get_old/new_row`, full `swift_migration_context_ref` (pending_changes, enumerate_objects, rename_property, delete_all, execute_sql, query_sql, set_row_value) | lattice.hpp:355-475, 2153-2298, 3077-3137 | C `lattice_db_create_with_migration` is **single-version, single-table**, JSON-roundtrip rows (lattice.h:615-636) | **PLAN** — this is exactly the drop-gate on unified `lattice_db_open` (multi-version migration array must be design-reviewed by rc−2wk, else 1.1) |
| 44 | `_lattice_post_cross_process_notification(path)` (test hook for external writers) | lattice.hpp:40 | None | UNENUMERATED (test-only; C1's test target may want it) |

**Category A totals: 44 audited capability gaps** — 11 PLAN-assigned, 28 UNENUMERATED
(pre-plan bridge surface the plan's C1 list doesn't name), 5 NEW-SINCE-PLAN
(Jul 10–12: named-memory URI docs, storage_shared_across_instances, create_uncached,
detach, last_attach_error). (Sync tuning A-22 is counted PLAN — `lattice_sync_options_t`
is in the C1 list — even though its bridge half landed Jul 11, after the list froze.)

---

## B. C-ABI functions whose semantics DRIFTED from the bridge equivalent

| # | C function | Drift | Severity |
|---|---|---|---|
| 1 | `lattice_db_attach` (capi lattice.cpp:1775) | **Regression, Jul 11.** It calls `swift_lattice::attach`, which since ATT-3 (`acb37c1`) **never throws** — it returns `bool` and stashes the reason in `last_attach_error_`. The C wrapper **discards the bool** and its `catch` is now dead code → **attach failures return `LATTICE_OK`** and `lattice_last_error()` is never set. C also cannot reach `last_attach_error()`. | **P0** — silent success on failure |
| 2 | `lattice_db_receive_sync_data` (capi lattice.cpp:972) | Bridge routes through `lattice::apply_remote_changes` (per-entry error isolation; returns only successfully-applied ids; failures land in `last_receive_error()`). The C impl **re-implements a simpler loop** (`entry.generate_instruction` + raw `execute`) that is all-or-nothing: the first bad entry throws out of the loop, the function returns `NULL`, and **entries already executed are applied but never reported** — partial apply with no ack trail. | **P1** — sync-correctness drift |
| 3 | `lattice_db_set_sync_filter` (capi lattice.cpp:1941) | **Header/impl key mismatch**: lattice.h:668 documents `{"table": …, "predicate": …}`; the impl parses `entry.value("where_clause", "")` (matching core `sync_filter_entry.where_clause`). A caller following the header gets a filter whose predicate is silently dropped → table syncs **all rows**. | **P1** — documented contract broken |
| 4 | `lattice_object_observer_fn` (lattice.h:299) | Callback is `void(context)` only; the bridge object observer delivers `changed_field_names`. Information silently dropped at the C boundary (capi lattice.cpp:838 discards the `const char*`). | P2 — additive fix possible (new callback type) |
| 5 | `lattice_db_observe_table` (capi lattice.cpp:794) | Bridge delivers **batched** parallel arrays per WAL flush (contract change of Jul 8 era); the C wrapper unrolls to one callback per row — same data, batch boundaries lost (no way to know a transaction's rows arrived together). | P2 |
| 6 | `lattice_db_delete_where` (capi lattice.cpp:701) | Header promises "number deleted"; impl does non-atomic `count(where)` **then** `delete_where` (TOCTOU under concurrent writers), because the bridge's `delete_where` returns `bool`. | P3 |
| 7 | `lattice_db_rollback` (capi lattice.cpp:778) | Executes raw `db().execute("ROLLBACK")` instead of `lattice_db::rollback()`. **Verified equivalent today**: core `database::rollback()` is itself `execute("ROLLBACK")` (db.cpp:770) and the change-buffer discard rides SQLite's rollback *hook* (core lattice.cpp:206), which fires either way. Fragile coupling, not a behavior drift — should still be switched to the member call. | P3 (note only) |
| 8 | `lattice_db_create_with_migration` (capi lattice.cpp:1795) | Beyond the shape gap (A-43): the JSON row round-trip **silently drops BLOB columns** — `std::vector<uint8_t>` variants are skipped when serializing `old_row`, and blob values cannot be expressed in `new_values_json`. A C-driven migration of a table with blob columns loses data silently. | **P1** if C migration is ever exercised |
| 9 | `lattice_db_observe_cross_process` / `lattice_db_remove_cross_process_observer` (lattice.h:679-687) | **Declared in the frozen header, never implemented** — no definition in capi lattice.cpp (confirmed: never existed in git history). Any consumer linking these gets an unresolved symbol; ctypes/JNI lookups fail at runtime. The bridge/core cross-process observation path exists and works. | **P1** — the "frozen" ABI ships 2 phantom symbols |

**Category B totals: 9 findings** (1 × P0, 4 × P1, 2 × P2, 2 × P3).

---

## C. C-ABI functions with no bridge equivalent (deliberate C-only surface)

These wrap **core** types directly and exist because Kotlin/Python cannot touch C++;
Swift reaches the same capabilities through core interop instead. Not gaps — but they
are ABI surface the symbols file and conformance tests must cover.

| Group | Functions | Notes |
|---|---|---|
| Scheduler | `lattice_scheduler_create`, `lattice_scheduler_release` | fn-pointer scheduler; Swift builds `std::shared_ptr<scheduler>` directly |
| WebSocket injection | `lattice_websocket_client_create/release`, `_set_on_{open,message,error,close}`, `_trigger_on_{open,message,error,close}` (10) | platform WebSocket plumbing for JNI/ctypes |
| Network factory | `lattice_network_factory_create/release`, `lattice_set_network_factory`, `lattice_db_set_network_factory` (4) | global + per-db registration |
| Server-side sync JSON helpers | `lattice_create_sync_message`, `lattice_create_ack_message`, `lattice_db_get_pending_audit_log`, `lattice_db_mark_synced` | bridge exposes `events_after`/`receive_sync_data` but not these composers; core free functions exist |
| Memory management | `lattice_string_free`, `lattice_knn_results_free`, `lattice_knn_results_count`, `lattice_results_free`, retain/release pairs | C-infrastructure by nature |

**Category C totals: ~21 functions** in 5 groups.

Parity confirmations worth recording (checked, NOT gaps): `lattice_db_rollback` exists
in C (the Jul 12 `4363ea1` commit only added the *ref-level* forwarder Swift needed);
`lattice_db_add_with_global_id` ⇔ `add_preserving_global_id`; `lattice_db_events_after`
⇔ `events_after` (JSON vs typed vector); `lattice_db_create_with_ipc` ⇔
`set_ipc_targets`; `lattice_db_close` ⇔ `close()` including LatticeCache eviction (both
routes hit `swift_lattice::close`); string returns use `thread_local` buffers
(capi lattice.cpp:19, 427) consistent with the "lattice_last_error stays thread-local"
policy.

---

## Verification results

### V1. Linux `swift build` of LatticeCAPI — modulemap gap

**RESOLVED (C1 slice 3).** Fixed with handwritten `module.modulemap`s for
`LatticeSwiftCppBridge`, `LatticeCAPI`, and `Tests/LatticeCAPIHeaderCheck`
— fix shape (1) below, with one correction the audit missed: an
all-textual bridge map empties the module for the Lattice Swift package's
`import LatticeSwiftCppBridge`, so the bridge map anchors the module on a
Swift-only real header (`swift_module_umbrella.hpp`, never included by
C++) with every other header `textual`. Verified in swift:6.3-noble:
`swift build` (all targets, including the now-ungated LatticeCAPITests)
and `swift run LatticeCAPITests` both green; the Lattice Swift package
still builds on macOS. Enforced by `.github/workflows/capi.yml`.
Original finding follows.

**Still broken as of `f5f7cd7` (static verification; no Linux toolchain/daemon available
on this machine to re-run the build).** Evidence:

- `Package.swift` has not been touched since Jun 12 (`00bd5d3`) — before the Jul 10
  discovery — so nothing has fixed it.
- The load-bearing flag `-fno-implicit-module-maps` is applied only
  `.when(platforms: [.macOS, .iOS])` on LatticeCore, LatticeSwiftCppBridge,
  LatticeCAPI, and LatticeCoreTests (Package.swift:62, 81, 107, 135). On Linux it is
  absent, so clang engages module maps for the bridge headers.
- `Sources/LatticeSwiftCppBridge/include` has **no handwritten `module.modulemap`** —
  SwiftPM synthesizes an umbrella module (`.build/aarch64-unknown-linux-gnu/debug/`
  `LatticeSwiftCppBridge.build/module.modulemap`: `umbrella ".../include"`,
  `export *`) covering C++20-only headers that cannot build as a clang module; the
  LatticeCAPI compile then dies with "module 'LatticeSwiftCppBridge' is needed but has
  not been provided". By contrast **`Sources/LatticeCore/include/module.modulemap`
  exists and marks every C++ header `textual`** — that target has the fix; the bridge
  doesn't.
- The stale containerized Linux build tree confirms it: `LatticeCAPI.build/src/`
  contains only `lattice.cpp.d` (no `.o`), and `LatticeSwiftCppBridge.build/` has no
  object directory at all.
- CI never sees it: `.github/workflows/linux.yml` runs only `swift run
  LatticeCoreTests`.
- Extra wrinkle: the bridge depends on **LatticeSwiftModule (a Swift target)** for
  `SWIFT_CONFORMS_TO_PROTOCOL(LatticeSwiftModule.OptionalProtocol)`
  (unmanaged_object.hpp:17), which drags a generated
  `LatticeSwiftModule-Swift.h` clang module into every downstream C++ compile — one
  more reason the module machinery engages on Linux.

**Fix shape (pick one):**
1. **Handwritten `module.modulemap` in `Sources/LatticeSwiftCppBridge/include`
   declaring the C++ headers `textual`** — the exact pattern LatticeCore already uses.
   Lowest risk; makes the synthesized umbrella disappear. (Do the same for
   LatticeCAPI's own include dir if its umbrella ever bites.)
2. Extend `-fno-implicit-module-maps` to Linux (drop the platform condition) — worth
   trying but weaker: SwiftPM still passes the synthesized module map explicitly via
   `-fmodule-map-file`, so (1) is the robust route.
3. Declare **CMake the only supported LatticeCAPI build** (CMakeLists.txt:109 already
   builds it as the `SHARED` library the siblings consume) and let the new C-ABI CI
   leg use CMake. Legitimate per the plan's own framing, but leaves `swift build`
   (all-targets) permanently red on Linux.

### V2. Does `lattice.h` compile as pure C11 today?

**Yes.** `clang -std=c11 -fsyntax-only -I Sources/LatticeCAPI/include
Sources/LatticeCAPI/include/lattice.h` exits 0 (macOS host clang, 2026-07-12). The
plan's `capi_header_compiles.c` enforcement test does not exist yet, so nothing
*keeps* it true — but there is no current violation.

### V3. `lattice_capi.symbols` situation

**Does not exist.** No `*.symbols` file anywhere in the repo outside `.build`; no
reference in `CMakeLists.txt` or any workflow under `.github/workflows/` (linux.yml /
macos.yml / release.yml). The whole ABI-policy tooling chain from the plan —
`docs/CAPI-STABILITY.md`, the checked-in symbols file diffed vs `nm` in CI,
`LATTICE_EXPORT`/hidden visibility, the `Tests/LatticeCAPITests` GoogleTest target —
is still entirely TODO. Related present-day exposure: because there is no symbols
gate, the two phantom declarations of B-9 have sat in a "frozen" header for 3.5
months without detection.

---

## Priority read-out for Kotlin/Python parity (C2/C3 consumers)

1. **Sync configurability + observability (A-22…A-28)** — zero July sync surface in C:
   no tuning, no progress/error/state callbacks, only `is_sync_connected` polling.
   `lattice_sync_options_t` + progress callback are the plan's own top items.
2. **`lattice_db_attach` silent-success regression (B-1) + missing `detach`/
   `last_attach_error` (A-29/30)** — a C consumer today cannot detect a failed attach.
   One-line fix (`return impl->attach(...) ? LATTICE_OK : LATTICE_ERROR_DATABASE;`
   plus error propagation) should not wait for the rest of C1.
3. **Unions + compound-unique/upsert constraints (A-1/A-2)** — schema parity is
   impossible: Kotlin/Python cannot even declare the Engram schema (which uses both).
4. **geo_bounds write path (A-3)** — geo is query-only *and* write-dead from C; any
   sibling geo feature is DOA without object-level bounds accessors.
5. **`receive_sync_data` semantic drift (B-2) + blob-lossy migration (B-8)** — the two
   places where the C ABI doesn't just *lack* a capability but does something
   *different* from the bridge with the same name; conformance corpus scenarios will
   catch these only if written against the bridge contract.

Honorable mention: the combined multi-constraint query engine (A-34) is the largest
single block of unexposed functionality, but siblings can ship 1.0 parity for
Engram-style workloads with single-constraint KNN/FTS/bbox + the items above.
