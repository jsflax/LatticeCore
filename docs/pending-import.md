# Pending-write rescue and selected audit export

This candidate adds `apply_pending_changes` for an application recovering a
retired local replica's unsent history into its replacement. It does not ACK,
delete or alter the source. Upload acknowledgement remains the synchronizer's
job. The caller must retain the original source until its own durable policy
allows removal.

Each distinct input global ID receives exactly one result: imported,
already-present, or rejected. Duplicate IDs anywhere in the batch are rejected
before effects. Existing AuditLog IDs and durable suppression receipts mean
already-present, so a stale rescue cannot revert a newer local edit. New entries
use the regular Core SQL/schema/NoHistory/synthesized rules, with a per-entry
SAVEPOINT covering both data mutation and AuditLog insertion. A journal failure
rolls back the data change and restores its uncommitted observer buffer. Pending
rescue defers early audit-cursor advancement until the existing committed flush
updates all same-path instances. A failed dependency remains rejected and retryable.
A chunk rollback retracts its provisional statuses. An existing caller
transaction is refused without committing or rolling it back.

Imported means **journaled for upload**, not necessarily a materialized row.
An UPDATE whose destination row is absent is still journaled; the original
change can then reach an upstream that owns the row. A value-identical new event
also gets a new upload-eligible journal entry above the destination's upload
floor. Neither case is silently suppressed. Rescue adds no receiving-sync mark,
download cursor, or new no-op receipt. Filter-removal DELETEs retain their
existing synchronized, non-upload policy. Unsupported tables or columns are
rejected instead of receiving ordinary receive's schema-skew ACK. Raw history
placeholders remain raw; the paired NoHistory transport marker is late-bound.

`query_audit_log_selected` captures exact `(local ID, global ID)` identities with
the Core typed codec and provenance, never a JS JSON number conversion. It
refuses missing, duplicate, malformed or oversized entries. Limits are 4096
rows, a positive caller byte budget at most 16 MiB, NoHistory column names at
most 1024 bytes, and at most 4096 marked columns. It checks raw row and metadata
sizes before fetching values; UPDATE live-cell sizes before late binding; and
separate cumulative materialized-input (including metadata) and serialized-output
budgets before retaining each entry. Raw/live-size
preflight is conservative: a near-limit entry can be refused before its final
JSON size is measured. A nested read-only savepoint and connection mutex keep
those checks on one snapshot, preserving any caller read transaction. No full
history scan or source mutation is performed.

Bindings transport serialized native frames as opaque strings. They must keep
payloads out of JS JSON.parse/stringify to preserve signed 64-bit values. Frame
and aggregate limits still apply outside this helper; this API is not a general
permission boundary. Selection is for an already-authorized local store.

Qualification target: `LatticePendingImportTests`, real synthetic Core stores.
Source-level tests cover integer boundaries, nulls, provenance, exact/sparse
selection, byte limits, read-only nesting, no-op uploads, deduplication,
unsupported schema, journal/dependency failures, chunk accounting and caller
transaction ownership. Build/test receipts will identify the exact candidate;
this document alone does not claim installed native/Wasm parity.

Local qualification (2026-10-24): the focused actual-Core target passed all 19
cases, including rejected-only and mixed accepted/rejected observer delivery and
cursor behavior on two same-path owners. The existing 9 nullable-NoHistory
regressions also passed against the same freshly linked candidate. This does
not establish Wasm binding/frame integration or a released runtime pair.
