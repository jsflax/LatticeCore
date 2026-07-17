# Design: unified `lattice_db_open(const lattice_open_options_t*)`

**Status: DEFERRED TO 1.1 (drop-gate applied — see §4).** The 1.0 C ABI
ships the existing creators; `lattice_db_open` lands post-1.0 as additive
symbols, which the plan's own ABI policy prices as free (new capability =
new symbols; docs/CAPI-STABILITY.md rule 1).

This document is the design review the plan's C1 drop-gate demands: the
proposed options-struct shape (§2), the migration-array ABI it hinges on
(§3), and the honest gate assessment (§4) with the 1.1 checklist (§5).

## 1. Context

Plan decision 1 (Jul 9): one size-prefixed options struct — embedding sync
options + migration + ipc channels — as the canonical 1.0 entry point, with
the five non-composable creators becoming deprecated wrappers kept through
1.x. Round-2 adversarial review added the **drop-gate**: the current C
migration ABI is single-version/single-table with a JSON row round-trip
that cannot represent BLOBs (`lattice_db_create_with_migration`, audit
A-43/B-8). Embedding *today's* migration shape inside `lattice_db_open`
would freeze a known-inadequate ABI under the additive-only policy — so
unified open ships at rc **only if** the options struct (multi-version
migration array aligned with G3's final `(rowId, [String: ColumnValue])`
shape + per-callback ownership/`destroy_fn` fields) is design-reviewed by
rc minus 2 weeks; otherwise ship the creators and add `lattice_db_open` in
1.1.

The C creator surface at the freeze (all remain, none deprecated at 1.0):

| Creator | Composes with |
|---|---|
| `lattice_db_create_in_memory` / `lattice_db_create_at_path` | nothing |
| `lattice_db_create_with_schemas` | schemas |
| `lattice_db_create_with_scheduler` | + scheduler |
| `lattice_db_create_with_sync` | + WSS endpoint/token |
| `lattice_db_create_with_sync_options` (slice 2) | + size-prefixed `lattice_sync_options_t` |
| `lattice_db_create_with_migration` | + single-version/single-table row migration |
| `lattice_db_create_with_ipc` | + IPC channels |

The non-composability that motivates unified open is real: today a C
caller cannot open a database with sync AND ipc AND a migration at once.
Swift can (one `swift_configuration` carries all three).

## 2. Proposed shape (for the 1.1 review to start from)

```c
// SIZE-PREFIXED (docs/CAPI-STABILITY.md rule 3). Always initialize via
// lattice_open_options_init(), override only what you need.
typedef struct {
    size_t struct_size;                        // set by _init()

    // --- location -----------------------------------------------------------
    const char* path;                          // NULL / ":memory:" / file / named-memory URI
    bool read_only;                            // dynamic (schema-from-file) open

    // --- schema -------------------------------------------------------------
    const lattice_schema_t* schemas;
    size_t schema_count;
    const lattice_unique_constraint_t* constraints;   // finally reachable (audit A-1)
    size_t constraint_count;

    // --- dispatch -----------------------------------------------------------
    lattice_scheduler_t* scheduler;            // NULL = inline/synchronous

    // --- sync ---------------------------------------------------------------
    const char* wss_endpoint;                  // NULL = no WSS sync
    const char* authorization_token;
    const lattice_sync_options_t* sync;        // nested size-prefixed struct; NULL = defaults

    // --- ipc ----------------------------------------------------------------
    const char* const* ipc_channels;
    size_t ipc_channel_count;

    // --- migration (THE GATED PART — see §3/§4) -----------------------------
    int32_t target_schema_version;             // < 0 = unversioned open
    const lattice_migration_step_t* migration_steps;
    size_t migration_step_count;
} lattice_open_options_t;

void lattice_open_options_init(lattice_open_options_t* options);
lattice_db_t* lattice_db_open(const lattice_open_options_t* options);
```

Semantics carried over from the slice-2 precedents: implementation reads
`min(struct_size, sizeof)` so old callers keep working as fields append at
the tail; `struct_size == 0` fails loudly; unknown/nonsensical knob values
follow each embedded struct's own documented rules (`lattice_sync_options_t`
keeps its ignore-and-default semantics).

## 3. The migration-array ABI (the hard part)

### 3.1 Target shape

One step per (version, table), mirroring the bridge's
`swift_configuration::addMigrationSchema(version, table, from, to)` +
`setRowMigrationCallback` machinery (which is already multi-version;
only the C surface is not):

```c
// A single value crossing the migration boundary — BLOB-capable, unlike
// the JSON round-trip in lattice_db_create_with_migration (audit B-8).
typedef struct {
    lattice_column_type_t type;                // INTEGER/REAL/TEXT/BLOB
    bool is_null;
    union {
        int64_t integer_value;
        double real_value;
        struct { const char* data; size_t length; } text;    // NOT NUL-terminated
        struct { const uint8_t* data; size_t length; } blob;
    } value;
} lattice_column_value_t;

// Row access handle valid ONLY inside the callback (thread-confined,
// like the bridge's migration_get_old_row/migration_get_new_row).
typedef struct lattice_migration_row lattice_migration_row_t;

// G3-aligned row callback: (rowId, old row as name→ColumnValue), writes
// staged through the new-row handle.
typedef void (*lattice_migration_row_v2_fn)(
    void* context,
    int64_t row_id,
    const lattice_migration_row_t* old_row,    // typed getters, BLOB-capable
    lattice_migration_row_t* new_row);         // typed setters, BLOB-capable

typedef struct {
    size_t struct_size;
    int32_t version;                           // schema version this step migrates TO
    const char* table;                         // table this step covers
    lattice_migration_row_v2_fn callback;      // NULL = schema-only step
    void* context;                             // per-callback ownership …
    void (*destroy_fn)(void*);                 // … with explicit destroy (plan requirement)
} lattice_migration_step_t;
```

Getter/setter surface on `lattice_migration_row_t` (by-name, typed, plus a
`lattice_column_value_t`-returning generic form) replaces the
`old_values_json` / `char** new_values_json` round-trip entirely — no
encoding, no BLOB loss, no malloc'd-JSON ownership convention.

### 3.2 What is genuinely settled

- Multi-version schema *pairs* per (version, table): the bridge stores and
  looks these up already (`migration_schemas_[version][table_name]`); the C
  array shape above maps 1:1 onto `addMigrationSchema`.
- Per-step `context` + `destroy_fn`: the slice-2 sync-observer pattern
  (register/remove/close-teardown, destroy exactly once) transfers as-is.
- Size-prefixing of the step struct so steps themselves can grow.

### 3.3 The holes (why this fails the gate)

1. **G3 has not landed.** The plan pins the row-callback shape to "G3's
   *final* `(rowId, [String: ColumnValue])` shape". At lattice@HEAD,
   `MigrationContext.enumerateObjects` (Sources/Lattice/Migration.swift)
   takes `(any Model, any Model) -> Void` and its body is a stub whose
   `(rowId, [String: ColumnValue])` marshalling is commented out —
   `ColumnValue` does not exist as a public Swift type yet. There is no
   final shape to align with; aligning with a guess and freezing it
   additive-only is precisely what the drop-gate exists to prevent.
2. **Value-union ownership/lifetime is unreviewed.** For reads, text/blob
   pointers must be documented as callback-scoped — plausible. For
   *writes*, the choice between (a) library-copies-on-set (extra copy per
   value, simple lifetime) and (b) caller-owned-until-commit with a
   per-value destroy (zero-copy, hard to hold correctly from ctypes/JNI)
   changes the signatures. Whichever loses cannot be retrofitted; the
   bindings that consume this (C2/C3) do not exist yet to inform the pick.
3. **Callback fan-out vs the bridge's single slot.** The bridge has ONE
   `row_migration_fn_` receiving only `table_name` — version dispatch
   (which C step's callback fires while core walks versions N→N+1→…)
   requires either a bridge-side change (thread the version through the
   callback) or a C-side dispatcher keyed on a version the bridge does not
   currently expose mid-migration. Unverified against core's stepped
   upgrade order; wrong dispatch silently runs the wrong user callback.
4. **The gate itself is a human review deadline.** "Design-reviewed by rc
   minus 2 weeks" means an actual review of a settled design; with (1)
   unlanded upstream this cannot be satisfied inside this slice, and
   self-certification is not review.

## 4. Decision

**Take the sanctioned fallback: ship the creators at 1.0, add
`lattice_db_open` in 1.1.**

- The plan prices this at zero: "additive post-1.0 is legal under this
  plan's own ABI policy, so the fallback is free."
- `lattice_capi_has_feature("unified_open")` stays **reserved-false**; it
  flips in the release that ships `lattice_db_open` (feature strings are
  added in the same commit as their functions, never removed).
- Nothing is deprecated at 1.0: the deprecated-wrapper clause of
  CAPI-STABILITY rule 4 activates only when the unified entry point
  actually exists.
- Composability loss in the interim is bounded: slice 2's
  `lattice_db_create_with_sync_options` already composes path + schemas +
  scheduler + WSS + tuning + filter, which covers the sibling (Kotlin/
  Python) 1.0 parity workloads; migration and ipc remain separate creators
  exactly as they are today.

## 5. 1.1 checklist (in order)

1. G3 lands in the Swift repo → `ColumnValue` public shape is final.
2. Decide write-path ownership (§3.3 hole 2) WITH the Kotlin/Python
   binding authors — they are the marshalling constituency.
3. Bridge: thread `version` through `setRowMigrationCallback` (additive
   internal change, no C ABI impact).
4. Implement `lattice_db_open` + `lattice_open_options_t` +
   `lattice_migration_step_t` per §2/§3.1; deprecate (do not remove) the
   creators; flip `unified_open`; regenerate `lattice_capi.symbols`.
5. Deprecate `lattice_db_create_with_migration`'s JSON row callback in the
   header text (it already refuses BLOB tables loudly — audit B-8 fix).
