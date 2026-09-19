# Owned exact vector rows (internal)

This bridge connects the existing canonical float32 exact-row selector to live
managed objects. It does not enable a public Swift nearest API, change existing
approximate queries, or implement Swift predicate/shape lowering. The new source
and tests require native and importer qualification; source review is not a
runtime or performance result.

`swift_lattice_ref::exact_nearest_rows` captures a strong parent reference before
execution. The private Core lease captures one writer/revision under publication,
then owns its recursive SQLite mutex. It shares the managed scalar TLS frame and
active-operation count, with a no-drain exit policy. Topology is consulted only
with `try_lock` beneath the writer, and publication is briefly rechecked. The
complete typed aliases, tokens, immutable attachment schemas and canonical
filenames are copied before releasing topology. No topology/publication lock
crosses SQL, callbacks, or hydration. A conflicting topology operation returns
`resource_busy`; ordinary writer contention waits.

Admission allows autocommit or the calling thread's Core-owned transaction.
Another thread's transaction and an unowned raw `BEGIN` fail busy. No implicit
transaction or store gate is introduced. The lease validates actual attached
schema membership/filenames, registration and physical model columns. Known
stores without the model contribute no arm; unknown physical stores, missing
metadata, incompatible schemas and unregistered physical columns fail. Attached
models absent from main are supported. Remapped property names and generated or
hidden model columns are explicitly unsupported. Recognized link shadow columns
remain part of the complete physical payload. `id` must be the sole declared
INTEGER primary-key column: a winning `(id, globalId)` payload is unsafe to turn
into a live `WHERE id = ?` handle if that id is not unique. A winning id of zero
also fails: it is SQLite-valid but is the managed wrapper's unbound sentinel.
Negative nonzero integer ids retain their existing managed meaning. This is a
live-hydration restriction; the standalone row selector remains unchanged.

The existing `exact_vector_rows.hpp` supplies eligibility, metric calculation,
best physical replica per binary globalId, deterministic ties, global top-k and
same-statement physical payload selection. Canonical model BLOBs are scanned;
no vec0 sidecar, training, repair or globalId rehydration is used. Requested k
bounds returned rows only, not scan time, SQLite workspace or temporary storage.
Every eligible nonnull vector must satisfy the selector's float32 dimensions
and finite-distance contract, even when that row loses or falls outside top-k.
At k=0 request/schema/prepare validation still occurs, but stored metric
evaluation is not promised.

Hydration authenticates `_source` and `_lattice_attach_token` against the lease
and derives the raw writer pointer plus weak control-block identity from that
same lease. It never rereads mutable owner `db_`. Source-specific Swift property
metadata is copied while the lease remains active. The immutable query row image
is kept separately from live field reads; row-cache mode is not enabled. Final
dynamic objects receive the already captured parent directly, avoiding a
LatticeCache lookup under the SQLite writer mutex. `object_at` later allocates
only a reference wrapper for the existing dynamic object.

Active publication admission extends through result hydration, so writer
close/reopen refuse throughout that interval. Typed detach on another thread
may invalidate a captured token before waiting for the writer; the admitted
SELECT can finish on its original physical schema, but later live field access
on an invalidated result fails. After lease exit, normal writer maintenance may
retire the writer and returned attached fields reject instead of reviving.
Results retain the parent lattice and weak attachment writer identity, not a
strong retired writer. The exact read does not drain deferred callbacks from
earlier writes. Existing scalar getter admission and main fast path remain.

Failure status is independent of diagnostic text and defaults to failure.
Request builders and result accessors are sticky; allocating bodies catch all
C++ exceptions, including request value copies. Both FRT and legacy value object
access are sealed. A successful
empty result has status zero; callers check status after each accessor before
using a value. Result copies share payload, not vectors of managed-row copies.
Status values are internal: success 0, invalid request 1, invalid schema 2,
resource busy 3, database failure 4, bridge failure 5. Diagnostic allocation can
fail without turning an error into empty success.

Limits remain explicit:

- Metadata queries and final selection are not one cross-connection snapshot.
  Supported model schemas remain stable during the operation; concurrent
  external DDL/migration is not supported. The final selection has SQLite's
  statement/attached-file read semantics, not global wall-clock atomicity.
- Core registration distinguishes a vector BLOB but stores no element-width
  tag. Float32 interpretation is a data contract; this API cannot distinguish
  a same-sized Double encoding. No implicit Double conversion is performed.
- The predicate channel is a trusted internal SQL expression with root `m` and
  anonymous bindings. It is not public raw SQL or full Swift AST support.
- Raw same-file detach/rebind, raw callback topology mutation, concurrent full
  lattice close/destruction and broader synchronizer callback lifetimes remain
  outside this change. Main handles retain their existing lifetime contract.
- There is no cancellation, hard deadline, bounded-memory guarantee, new read
  scheduler, stable cross-request cursor, inherited shape handling or public
  exact-search release guarantee here. Those remain separate integration gates.
