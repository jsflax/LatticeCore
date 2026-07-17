# C ABI Stability Policy (`LatticeCAPI`)

**Scope.** The C ABI is `Sources/LatticeCAPI/include/lattice.h` plus the
exported `lattice_*` symbols of the shared `LatticeCAPI` library
(`CMakeLists.txt` builds it `SHARED`; it is the binding surface for Kotlin
(JNI/cinterop) and Python (ctypes) — roadmap items C2/C3). Everything in the
bridge (`LatticeSwiftCppBridge`) and core (`LatticeCore`) headers is
*internal* and carries no stability promise.

**Status.** Pre-1.0 the surface may still change. The ABI **freezes at
`LatticeCore 1.0.0-rc`**; from 1.0 on, the rules below are binding.

## Rules (binding from 1.0)

1. **Additive-only.** No exported function may be removed or renamed, and no
   existing signature, struct layout, or enum value may change, for the whole
   1.x line. New capability = new symbols.
2. **Enums grow at the tail.** New enumerators are appended after the last
   existing value; existing values are never renumbered or reused.
3. **`struct_size` on new pointer-structs.** Every NEW struct passed by
   pointer across the ABI starts with `size_t struct_size` as its first
   member and ships a `_init()` that fills defaults; the implementation reads
   `min(caller_size, sizeof)` so old callers keep working as fields are
   appended. The pre-policy structs — `lattice_property_t`,
   `lattice_unique_constraint_t`, `lattice_schema_t`, `lattice_knn_result_t`
   — are **frozen without one**; they cannot be retrofitted (it would break
   layout) and therefore can never grow. Extending them means introducing a
   successor struct + new functions.
4. **Deprecated wrappers survive 1.x.** When a newer entry point subsumes an
   old one (e.g. the planned unified `lattice_db_open` vs the five creators),
   the old functions remain as thin wrappers for the entire 1.x line, marked
   deprecated in the header.
5. **Error contract.** New fallible functions return `lattice_status_t` and
   report values through out-params or documented return conventions. Detail
   goes to `lattice_last_error()`, which is and stays **thread-local**;
   returned message/string buffers are thread-local too (callers copy before
   the next call on the same thread).
6. **Pure C11 header.** `lattice.h` must compile as self-contained C11 —
   no C++ constructs, no implementation-internal includes. Enforced by
   `Tests/LatticeCAPIHeaderCheck/capi_header_compiles.c`, a translation unit
   compiled with `-std=c11` in the regular test suite
   (`CAPISurfaceTests.HeaderCompilesAndLinksAsC11` proves it stays linked).
7. **No GNU symbol versioning.** Deliberately skipped — it does not exist on
   macOS/Windows and the ABI must behave identically on all three platforms.
   Versioning is carried by `lattice_capi_version()` and the additive-only
   rule instead.
8. **Visibility (adopted at freeze).** The shared library will build with
   default-hidden visibility plus an explicit `LATTICE_EXPORT` on every
   public function, so the export list is exactly the declared surface. Not
   yet applied — rides the rc freeze commit together with the C-ABI CI leg.

## Versioning & introspection

- `lattice_capi_version()` / `lattice_capi_version_string()` — runtime
  version of the linked library; tracks the LatticeCore release train
  (`CHANGELOG.md`). Compile-time counterparts:
  `LATTICE_CAPI_VERSION[_MAJOR|_MINOR|_PATCH|_STRING]`. The integer encoding
  is `MAJOR*1000000 + MINOR*1000 + PATCH`. Compare header vs runtime to
  detect skew.
- `lattice_schema_format_epoch()` — the on-disk schema-format epoch
  (`lattice_db::kLatticeSchemaFormatEpoch`). Processes sharing database
  files must agree on it (see the epoch notes in the core header).
- `lattice_capi_has_feature(name)` — capability probe; returns `false` for
  unknown names so bindings can probe features that do not exist yet.
  - **True today:** `attach`, `checkpoint`, `cross_process_observation`,
    `detach`, `fts`, `geo_query`, `ipc`, `knn`, `migration`, `observation`,
    `rollback`, `row_cache`, `statement_counters`, `sync`, `sync_filter`,
    `sync_progress`, `sync_tuning`, `transactions`.
  - **Reserved (false until the feature lands):** `read_generations`,
    `to_json`, `unified_open`.
  - A feature string is added in the same commit as its functions and is
    never removed once shipped.

## Surface enforcement

`Sources/LatticeCAPI/lattice_capi.symbols` is the canonical, checked-in
export list: one function per line, `LC_ALL=C`-sorted, `#` comments allowed.
`Tests/LatticeCoreTests/CAPISurfaceTests.cpp` (runs in the default suite)
enforces, on every test run:

- every function declared in `lattice.h` is implemented in
  `Sources/LatticeCAPI/src/lattice.cpp` — **no phantom symbols** (two
  declared-but-unimplemented functions shipped undetected for 3.5 months;
  `docs/capi-gap-audit.md` B-9);
- every `extern "C" lattice_*` definition is declared in the header — no
  undocumented exports;
- the symbols file matches the declared surface exactly, is sorted, and has
  no duplicates — any surface change must touch the symbols file
  deliberately, which makes ABI growth reviewable in diffs.

**Regenerating the symbols file** after an intentional surface change
(keep the leading `#` comment block):

```sh
swift build
nm -gU .build/arm64-apple-macosx/debug/LatticeCAPI.build/src/lattice.cpp.o \
  | awk '$2=="T" && $3 ~ /^_lattice_/ {sub(/^_/,"",$3); print $3}' \
  | LC_ALL=C sort -u
# (Linux/CMake shared lib: nm -gD --defined-only libLatticeCAPI.so | awk '$2=="T" && $3 ~ /^lattice_/ {print $3}' | LC_ALL=C sort -u)
```

The planned C-ABI CI leg additionally diffs this file against `nm` of the
built **shared** library (the true export surface, which will matter once
hidden visibility lands); until then the source-level test above is the
gate.

Behavioral coverage lives in `Tests/LatticeCAPITests` — a GoogleTest
executable that LINKS the C surface and exercises it exactly as a
Kotlin/Python binding would (`swift run LatticeCAPITests`; macOS-only under
SwiftPM because the C API library's SwiftPM build is broken on Linux —
`docs/capi-gap-audit.md` V1 — and built everywhere under CMake).

## Known frozen-in quirks (documented, not fixable in-place)

- `lattice_object_observer_fn` receives no changed-field names and
  `lattice_db_observe_table` unrolls batched change events to per-row
  callbacks (audit B-4/B-5) — fixes are additive (new callback types), the
  existing ones stay as-is.
- `lattice_db_delete_where` computes its return count non-atomically
  (count-then-delete; audit B-6).
- `lattice_db_create_with_migration` is single-version/single-table and
  **refuses tables with BLOB columns** (explicit error; previously silent
  data loss — audit B-8). The multi-version, BLOB-capable migration ABI
  arrives with the unified `lattice_db_open` work and must clear its design
  gate before the rc (plan C1).
- `lattice_db_set_sync_filter` accepts both the documented `"predicate"` key
  and the legacy `"where_clause"` key (audit B-3); both are permanent.
