# Lattice Conformance Corpus

The cross-SDK parity contract for the Lattice SDKs (Swift `lattice`,
`latticekotlin`, `latticepython`, `latticejs`). Every scenario in
`corpus/` is a declarative description of behavior that any conforming
SDK must exhibit **through its public API**. A per-SDK *runner* loads the
corpus, maps each operation onto that SDK's public surface, executes every
scenario against a fresh database, and reports per-scenario PASS / FAIL /
SKIP.

The corpus is the contract; the runners are interpreters. Nothing in this
directory is Swift-specific, Python-specific, etc. Anything only one SDK
can express (e.g. Swift's `@LatticeQuery` property wrapper, SwiftUI
integration, Combine publishers, `detached()` graphs, statement counters)
is deliberately **not** in the corpus.

## File format

- Files live in `corpus/*.yaml`, one file per behavioral area.
- File content is restricted to the **JSON subset of YAML** (YAML 1.2 is a
  strict superset of JSON). Every file parses with a stock JSON parser *and*
  any YAML parser — no SDK needs a YAML dependency to run the corpus.
- Scenarios are **byte-reproducible**: no timestamps, no randomness, no
  UUID/globalId assertions, no primary-key-value assertions, no
  locale-dependent strings, and only binary-exact floating point literals
  (values representable exactly in IEEE-754, e.g. `9.5`, `7.25`, `0.5`).

### Top level

```json
{
  "format_version": 1,
  "suite": "crud",
  "scenarios": [ ... ]
}
```

- `format_version` — grammar version of THIS spec. Runners MUST hard-error
  (not skip) on an unknown `format_version`, unknown op, unknown expect
  form, unknown where-operator, or unknown property type. Silent
  under-testing is the failure mode this rule exists to prevent.
- `suite` — the area name; matches the file basename.

### Versioning

The corpus version **is** the LatticeCore version: the corpus lives in this
repository and each SDK pins LatticeCore (submodule / package pin), so the
pin that selects the core also selects the corpus the SDK must satisfy.
Runners declare the `format_version` they implement (currently `1`).
Within one `format_version`, corpus evolution is additive-only: new files,
new scenarios, new *optional* fields. Renaming/removing ops or changing
expect semantics requires a `format_version` bump.

### Scenario

```json
{
  "name": "insert-read-scalars",
  "description": "human-readable intent",
  "capabilities": ["geo"],
  "schema": { "version": 1, "tables": { ... }, "protocols": { ... } },
  "ops":    [ ... ],
  "expect": [ ... ]
}
```

Execution model:

1. The runner creates a **fresh, private, file-backed database** for the
   scenario and opens it with the declared `schema`.
2. `ops` run strictly in order. Any op failure not annotated with
   `expect_error` fails the scenario immediately.
3. `expect` entries are evaluated in order after all ops; each is an
   independent assertion against the final database state and captured
   variables. All must hold.
4. The database (and its sidecar files) is deleted afterwards.

`capabilities` (optional, default `[]`) lists non-core capabilities the
scenario needs (see *Capabilities* below). A runner that has not declared a
required capability reports the scenario as **SKIP** — visibly, never as
PASS.

## Schema

```json
"schema": {
  "version": 1,
  "tables": {
    "CfPerson": {
      "properties": {
        "name":     { "type": "string" },
        "age":      { "type": "int" },
        "score":    { "type": "double" },
        "active":   { "type": "bool" },
        "nickname": { "type": "string", "optional": true },
        "city":     { "type": "string" }
      }
    }
  }
}
```

- `version` — the database schema version this scenario opens at
  (default `1`). Only migration scenarios use anything else.
- Property `type` is one of:
  - `int` — 64-bit signed integer column
  - `double` — IEEE-754 double column
  - `bool` — boolean (integer 0/1 storage)
  - `string` — text
  - `bytes` — blob
  - `vector` — float32 vector; requires `"dims": N`
  - `geo` — geographic bounds/point (lat/lon)
  - `link` — to-one relation; requires `"target": "<Table>"`
  - `list` — ordered to-many relation; requires `"target": "<Table>"`
  - `virtual_link` — to-one polymorphic relation; requires
    `"protocol": "<Proto>"`
  - `virtual_list` — ordered polymorphic to-many; requires
    `"protocol": "<Proto>"`
- Property flags: `"optional": true`, `"indexed": true`, `"unique": true`,
  `"full_text": true`. All default false.
- `protocols` (only in virtual scenarios) declares the polymorphic
  protocols and their shared fields:
  `"protocols": { "CfNoteworthy": { "shared": { "label": "string" } } }`.
  Concrete tables list `"conforms": ["CfNoteworthy"]`.

### The table catalog

Scenario schemas are drawn from a **fixed catalog** of table shapes
(`CfPerson`, `CfPet`, `CfOwner`, `CfCard`, `CfArticle`, `CfDoc`, `CfPlace`,
`CfCounter`, `CfNoteA`, `CfNoteB`, `CfBinder`, `CfWidget`, `CfMigPerson`,
`CfBlobDoc`). The inline schema in each scenario is the source of truth;
statically-typed runners (Swift, Kotlin) may pre-compile one native model
per catalog table + version and MUST validate the scenario's inline
declaration against the compiled shape, hard-erroring on any mismatch.
The `Cf` prefix keeps corpus entity names from colliding with host-process
test models (model registries are keyed by entity name).

## Values

Literal values in `values`, `where`, and `expect`:

- `null`, booleans, integers, doubles, strings — JSON literals.
- bytes: `{ "$hex": "cafef00d" }` — lowercase hex, canonical on read-back.
- vector: array of numbers, e.g. `[1, 0, 0, 0]` (float32 storage).
- geo point: `{ "lat": 10.0, "lon": 20.0 }`.
- object reference: `{ "$ref": "handleName" }` — a handle bound earlier via
  `"as"`. Used for link / virtual-link values and list items.
- id of a handle: `{ "$id_of": "handleName" }` — the runtime primary key of
  a bound handle (primary-key *values* are never asserted; this is only an
  input to `get`).
- previously captured id: `{ "$saved_id": "varName" }` — an id captured via
  `insert.save_id`, for lookups after the source object was deleted (a
  deleted handle no longer carries a primary key).

Reading fields back canonicalizes: `int` columns compare as integers,
`double` as doubles (exact equality — corpus data is binary-exact), `bool`
as true/false, `bytes` as `{"$hex": ...}`, unset optionals and absent links
as `null`.

## Where predicates

```json
{ "field": "age", "op": "ge", "value": 30 }
{ "all": [ w1, w2 ] }
{ "any": [ w1, w2 ] }
{ "not": w }
```

Leaf operators (`op`):

| op | meaning | value |
|---|---|---|
| `eq`, `ne` | equal / not equal | literal |
| `lt`, `le`, `gt`, `ge` | ordered comparison | number |
| `contains` | substring | string |
| `starts_with`, `ends_with` | affix match | string |
| `like` | SQL LIKE pattern (`%`, `_`) | string |
| `in` | set membership | array |
| `between` | inclusive range | `{ "low": a, "high": b }` |
| `is_null`, `is_not_null` | null check | (none) |

Case-sensitivity of string matching is **not pinned** in format 1: corpus
data and patterns are uniformly lower-case so scenarios are insensitive to
that difference. Sort tie-breaking is likewise not pinned: scenarios sort
only on columns whose values are distinct within the matched set.

## Ops

Every op is an object with an `"op"` key. Common optional fields:
`"as"` binds the produced object handle; `"save"` binds a captured value
into the scenario variable environment; `"expect_error"` asserts the op
fails with the given canonical error id (the scenario fails if the op
succeeds or fails with a different id).

| op | fields | semantics |
|---|---|---|
| `insert` | `table`, `values`, `as?`, `save_id?` | Create + persist a new object. Omitted optional properties are null; omitting a non-optional property is a corpus error. `save_id` captures the new primary key for later `$saved_id` lookups (never for assertions). |
| `add_existing` | `ref` | Re-add an already-persisted handle (misuse contract — see errors). |
| `get` | `table`, `id: {"$id_of": r} \| {"$saved_id": v}`, `as?`, `save_found?` | Primary-key lookup. Binds the handle when found; `save_found` captures true/false. |
| `update` | `ref`, `values` | Set properties through the live handle (write-through). Link values: `{"$ref": h}` or `null` to clear. |
| `delete` | `ref` | Delete the object. |
| `delete_where` | `table`, `where?` | Bulk delete; no `where` deletes all rows. Never errors on zero matches. |
| `count` | `table`, `where?`, `save` | Row count. |
| `snapshot` | `table`, `where?`, `sort?`, `limit?`, `offset?`, `distinct_by?`, `columns`, `save` | Ordered point-in-time read; captures `[[col values]]`. `sort`: `{"by": col, "order": "asc"\|"desc"}`. |
| `read` | `ref`, `fields`, `save` | Read fields off a handle; dotted paths traverse links (`"pet.name"`); a null link yields null. |
| `fts` | `table`, `column`, `match`, `limit?`, `columns`, `save` | Full-text search. `match` is a **raw FTS5 match expression** (`"a b"` = AND, `"a OR b"`, `"\"a b\""` phrase, `"pre*"` prefix); runners must use their raw/pass-through path. Result order is not pinned — assert with `unordered`. |
| `knn` | `table`, `column`, `query`, `k`, `metric`, `columns`, `save`, `save_distances?` | k-nearest vectors. `metric`: `l2` (Euclidean) or `cosine` (1 − similarity). Results ordered by ascending distance. Distances are asserted only where exact (integer-coordinate perfect squares under `l2`). |
| `geo_within` | `table`, `column`, `min_lat`, `max_lat`, `min_lon`, `max_lon`, `columns`, `save` | Bounding-box membership (inclusive bounds); order not pinned — assert `unordered`. |
| `list_append` | `ref`, `property`, `item: {"$ref": h}` | Append to a list / virtual list. |
| `list_remove_at` | `ref`, `property`, `index` | Remove by position (0-based). |
| `list_size` | `ref`, `property`, `save` | Element count. |
| `list_read` | `ref`, `property`, `field`, `save` | Ordered element-field values. |
| `transaction` | `ops`, `expect_error?` | Run nested ops inside one explicit transaction (block form — begin/commit/rollback triads are not public on every SDK). Normal completion commits. An `abort` op or an unexpected inner error rolls back; an inner error then surfaces as the transaction's error (matched against `expect_error`). |
| `abort` | | Only inside `transaction`: roll the transaction back and continue the scenario. |
| `materialize` | `ref` | Snapshot-now semantics: capture the row image AND switch the handle to materialized (snapshot-served) reads. |
| `dematerialize` | `ref` | Back to live per-read access. |
| `refresh` | `ref` | Re-capture the row image of a materialized handle. |
| `increment` | `ref`, `field`, `by` | SQL-side atomic `SET field = field + by`. Visible immediately through the handle, including a materialized one (the increment updates the snapshot). |
| `close` | | Close the database. All handles become invalid; scenarios never touch pre-close handles afterwards. |
| `reopen` | `schema`, `migration?`, `save_outcome?` | Re-open the SAME database file with a new schema (see Migrations). `save_outcome` captures `"ok"` or the canonical error id. Without `save_outcome`, a failed reopen fails the scenario. |

## Migrations

`reopen.schema` carries the new `version` and full table shapes. Column
adds are automatic. Added non-optional columns read as their zero value
(`0` for int, `""` for string) for pre-existing rows; added optional
columns read `null`. Pre-existing columns (including `bytes`) are
preserved byte-identical when no row transform touches the table.

Row transforms:

```json
"migration": {
  "transforms": {
    "CfMigPerson": [ { "set": "age", "parse_int_from": "age" } ]
  }
}
```

Transform steps (applied per pre-existing row, in order):

- `{ "set": col, "const": value }` — assign a literal.
- `{ "set": col, "from": oldCol }` — copy the old row's column.
- `{ "set": col, "parse_int_from": oldCol }` — parse the old row's string
  column as a base-10 integer; unparseable → `0`.

**BLOB-explicit-failure contract**: requesting a row transform for a table
that contains `bytes` (or `vector`) columns must NEVER silently lose blob
data. A conforming SDK either (a) refuses the migration with
`migration_blob_unsupported` (the frozen C-ABI contract:
`lattice_db_create_with_migration` fails explicitly because its JSON row
round-trip cannot carry blobs), or (b) performs the transform with all
blob columns preserved byte-identical (SDKs on a blob-capable migration
path, e.g. Swift's typed bridge). The corpus encodes this as the single
sanctioned `one_of` expect — both branches exact; success-with-lost-blobs
fails both.

## Expect

Each entry is one assertion:

| form | semantics |
|---|---|
| `{ "var": name, "equals": v, "unordered?": bool }` | Captured variable equals `v` (deep equality; `unordered` treats a list of rows as a multiset). |
| `{ "count": { "table", "where?", "equals": N } }` | Live count equals N. |
| `{ "rows": { "table", "where?", "sort?", "limit?", "offset?", "distinct_by?", "columns", "equals": [[...]], "unordered?": bool } }` | Live snapshot equals the exact row/column matrix. |
| `{ "field": { "ref", "name", "equals": v } }` | Field (dotted path allowed) of a bound handle equals `v`. |
| `{ "one_of": [ [expect...], [expect...] ] }` | Exactly-one-branch disjunction; a branch passes iff all its entries pass. Reserved for dual-outcome contracts (used once: BLOB-explicit-failure). |

## Canonical error identifiers

Runners map their SDK's native failures onto these ids:

| id | contract | Swift mapping (reference) |
|---|---|---|
| `already_managed` | Re-adding an object that is already persisted fails; nothing is written. | `LatticeError.alreadyManaged` |
| `add_failed` | The backend rejected an insert (e.g. unique-constraint violation); nothing is written and the database stays usable. | `LatticeError.addFailed` |
| `migration_blob_unsupported` | Row-transform migration refused because the migration path cannot carry BLOB columns. | (not produced — Swift's typed path is blob-capable) |

SDKs whose errors are message-based (Python `LatticeError`, Kotlin
`LatticeException`, JS `Error`) classify by their own failure site — the
runner knows which call failed and why; string-matching exact messages is
not required and not recommended.

## Capabilities

Core (undeclarable, required of every runner): CRUD, where/sort/limit/
offset/distinct queries, FTS, KNN, links + lists, transactions, error
contracts, column-add migrations.

| capability | scenarios gated | rationale |
|---|---|---|
| `geo` | `query-geo.yaml` | Geo bounds/R*Tree queries — not yet public in latticejs. |
| `virtual` | `virtuals.yaml` | Polymorphic links/lists — not yet public in latticejs. |
| `migration-row-transform` | parts of `migrations.yaml` | Row-level transforms — latticejs WASM migration context cannot enumerate rows yet. |
| `row-cache` | parts of `rowcache-increment.yaml` | `materialize`/`refresh` — in the C ABI (`lattice_object_*_row_cache`) but not yet wrapped by Python/Kotlin/JS. |
| `increment` | parts of `rowcache-increment.yaml` | Atomic increment — in the C ABI (`lattice_object_increment_int`) but not yet wrapped by Python/Kotlin/JS. |

A capability appearing here means the **core** can express it and the
corpus pins its semantics now; wrappers adopt the scenarios by declaring
the capability when they grow the API. Runners report skipped scenarios
per capability so the parity gap stays visible.

## Known-divergence ledgers (runner-side XFAIL)

The corpus pins the *contract*, including where an SDK currently diverges.
A runner MAY carry an SDK-side expected-divergence ledger (scenario key →
root-cause summary): listed scenarios still execute, their failures are
reported as expected divergences instead of suite failures, and a listed
scenario that starts PASSING must fail the suite loudly so the entry is
removed. Ledgers live with the runner (the divergence is the SDK's state,
not the contract's) and may only shrink by fixing the SDK — never by
editing the corpus.

## Determinism rules (author checklist)

- Fresh database per scenario; file-backed; deleted afterwards.
- Never assert: primary keys, globalIds, timestamps, FTS rank order,
  cosine distance values, geo result order, floating-point values that are
  not binary-exact.
- Strings: lower-case ASCII only (case-sensitivity is unpinned).
- Sorts only on columns with distinct values within the matched set.
- Every `expect` states exact values — no ranges, no "at least".
