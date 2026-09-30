# Live scalar attachment lifetime

Typed `attach` gives each attachment lifetime a generation. Hydrated models and
their scalar field copies retain that generation and a weak identity for the
writer that hydrated them. Typed `detach` invalidates the generation before its
first topology side effect, including when DETACH subsequently fails. Reattaching
the same file or another file under the same alias never revives an old field.

Ordinary primitive, optional, Data/vector, scalar geographic bounds, increment,
and row-cache fallback/refresh paths validate this provenance before SQL or vector
sidecar changes. Valid fields remain live. Explicit cache hits keep their existing
snapshot behavior. Main fields remain physically pinned to main and retain their
existing live semantics; their common getter path adds no SQL, topology admission
or weak-owner acquisition. Metadata access such as `isManaged`, primary key, and a
retained query-row image is not a live-value or route-validity check.

## Admission and invalidation

The existing topology mutex serializes typed attach/detach and route bookkeeping.
Successful attach publishes an immutable view of alias, filename, generation and
weak writer bindings. Each binding has an atomic validity flag. Invalidation flips
that flag before any detach SQL and cannot fail due to allocation. Publication is
the final attach step; allocation failure follows the existing partial-attach
failure invalidation path. It does not authorize the partially changed route.

A scalar operation locks its *captured* weak writer identity, takes that writer's
recursive SQLite mutex, and checks the current published binding while still
holding SQLite. It never waits for the topology mutex below SQLite. Ordinary
writer contention waits normally. Physical detach/rebind SQL cannot pass an
admitted operation's writer hold. Generation invalidation may happen concurrently
after admission; the admitted SQL still finishes against its original physical
binding before detach can acquire SQLite. Subsequent operations reject it.

Memory vector setters acquire the existing vector/store gate before the writer.
Attached vector sidecar setup uses the admitted captured writer, including the
existing vec0 maintenance frame and hook phases. Writer publication remains
excluded until that operation and its deferred tail finish. Its SQL body and
main-vector path are unchanged.
Nested ordinary getters can reenter the recursive writer. Typed topology changes
and vector mutations from a callback inside an attached scalar operation are
rejected before attempting another lock. This includes a callback that tries to
write a main vector through the same writer. This restriction avoids the existing
vector gate followed by writer lock order being reversed by callback reentry.

The field's weak writer identity prevents an expired old pointer from becoming
valid when an allocator reuses its address. Published bindings are weak too: held
models, field copies and old snapshots do not keep retired SQLite connections or
WAL transactions alive. Only an admitted operation pins its writer until its SQL
and normal delivery tail complete. Writer reopen prepares the replacement view
before publication, preserving generation values but changing writer identity.
`reopen_write_db` and `close_write_db` refuse an active attached operation or its
settled tail before beginning maintenance. Final publication also uses a
nonblocking old-writer mutex admission and rechecks the active-operation count;
contended maintenance refuses rather than wait below topology. Its ownership
mutex protects only the revision check and pointer/view publication, not SQLite.
This covers hooks that still consult the owning lattice's current writer. It does
not change full `close()` or destructor behavior, or broaden the existing raw
connection caller's maintenance-serialization contract.
`close_write_db` retirement clears its published view. Main lifetime policy is unchanged.

## Delivery and errors

An active attached scalar scope suppresses the existing
`database::drain_if_settled` tail. Its successful exit releases the writer and any
added vector gate, then performs that tail. Nested scopes leave delivery to the
outer scope. Failure unwinding does not add delivery, matching the existing
query/update exception tails. Existing primitive defaults, stored-type checks,
fresh statements, authorizer decisions and sealed Swift error recording remain.

This specifically defers the existing memory/Emscripten settled-delivery path.
**File WAL observer delivery remains inside its existing SQLite WAL hook.** This
change does not move all observers outside SQLite or coalesce file commit payloads.
Native callbacks must catch exceptions before returning across SQLite C hook
boundaries. Typed topology/vector reentry refusals from such callbacks must be
caught there; Swift bridge entry points retain their sealed error handling. A
general file-WAL delivery redesign is a separate architectural task.

## Scope and compatibility

Only typed topology operations participate in generation tracking. The scalar
guard also checks SQLite's attached filename against its captured route, catching
an ordinary raw replacement with a different filename. That is **not** complete
physical identity or generation proof. Raw SQL/handle detach plus reattach of the
same file, filesystem replacement, or raw topology changes from SQLite callbacks
are outside this contract. They must not be used to mutate a managed owner's
topology while relying on held managed fields. Ownerless manually bound database
fields retain their raw database contract. Owner-bound attached fields require
captured provenance; table name plus id alone is insufficient.

This change does not qualify link/list relationship lifetime, geo list element
routes, arbitrary raw SQL, or owner destruction concurrent with unsupported direct
C++ raw-owner access. Dynamic Swift models retain their existing owning lattice
reference. Selected bulk mutation retains its stronger schema/row/file preflight.

`model_base` gains a weak writer field; `managed_base` gains a generation and weak
writer field, and its internal assignment signature gains provenance arguments.
This changes native/bridge layout and requires jointly rebuilt compatible Core
and Swift C++ bridge artifacts. No persistent database format changes. Wrapper
copies add weak-control-block accounting; attached admission adds weak locking,
snapshot lookup and a writer hold. Exact sizes, allocation/copy costs and runtime
performance are unmeasured until native qualification. The authored regressions
cover stale aliases, same-file reattach, field copies, scalar geo/Data/optional,
cache fallback, writer retirement, contention and callback/race boundaries.
