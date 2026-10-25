# Nullable no-history values on sync

Status: forward-ported from `844c7ac6840d5be4e06c8113280b8d8f492fcd76`
onto the current 2.4 source line. Managed macOS, Linux and C-ABI qualification
of this port is pending. The nine-test receipt below belongs to the earlier
source branch, not this port. No release, dependency floor or live cutover is
claimed.

This port also clears the physical-nullability memo alongside the existing
schema memo after a chunk rollback. A tenth real-SQL regression changes the
physical schema between a failed chunk and its retry and verifies that the
new nullable field is cleared rather than silently acknowledged unchanged.

`@NoHistory` UPDATE triggers retain a field name but store a NULL placeholder instead of its value. Before sending, Core reads that field's current row value. A real SQL NULL was previously indistinguishable from the placeholder at the receiver, so the entry could be ACKed while its nullable field kept an older value.

The optional audit-entry key `noHistoryNulls: [String]` identifies observed NULLs. It is emitted only when nonempty. Every marker must be unique, nonempty, named in `changedFieldsNames`, and present in `changedFields` as an explicit flat `null` or `{ "kind": 4, "value": null }`. Missing/coerced values, invalid lists and markers on DELETE fail decoding; typed Core callers receive the same structural check before deduplication or schema-skew ACKs. Existing AnyProperty tags and event frame kinds are unchanged.

UPDATE late-binding recomputes the list from successful live reads. Missing/deleted rows or unreadable columns remove the value, name and marker. Complete INSERT payloads and synthesized full-row snapshots mark their explicit NULLs as well. AuditLog rows and trigger placeholders are unchanged: the marker is transport-only and is rederived after a relay persists/reopens its store.

The receiver applies a marked NULL to a no-history field only when its installed trigger metadata marks that field and its physical `PRAGMA table_info` column permits NULL (excluding primary keys). Unmarked placeholders and marked NULLs for required columns are dropped while the rest of the entry applies. Ordinary nullable fields continue using ordinary NULL semantics. Synthesized INSERT remains insert-if-absent; the marker does not grant an overwrite.

## Compatibility and cutover

| Sender / relay / receiver | Result |
| --- | --- |
| Qualified new implementations throughout | Nullable clear can propagate. |
| Old sender without a marker to new receiver | NULL remains ambiguous and is conservatively dropped. |
| New sender to old receiver | Optional key is ignored; old guard still misses the clear. |
| New sender through old Swift relay | Old manual Codable drops the marker, and old late binding drops the real NULL. |

There is no feature negotiation for this key. Existing Swift server schema handshakes enforce application schema versions, not this sync capability. A production route must qualify and enforce its complete peer/relay version floor separately. The paired Swift change must be included; a Core-only upgrade is insufficient. Browser manual-apply/import paths must use the same guard without changing pending-upload semantics.

This patch does **not** repair clears already ACKed under an old implementation. Their audit identities are deduplicated on replay. Reconciliation's synthesized snapshots also skip existing rows. Any remediation requires a separately authorized and qualified authoritative reseed or fresh corrective mutation; no owner-store remediation is part of this patch.

## Qualification

`LatticeNoHistoryNullTests` constructs synthetic stores through real Core and tests upload plus `events_after`, codec round trips, legacy/required guards, malformed wire and typed metadata, absent-row cleanup, marker recomputation, INSERT and relay reopen, synthesized snapshot behavior, and the already-ACKed limitation. The paired Swift suite covers manual Codable, C++ JSON conversion, transient schema exclusion, nullable/date-zero late binding and two relay hops through the public receive API. Run only with the paired candidate Core; these are not expected to pass against the old dependency floor.

Historical failing probe bytes remain under `hud-scratch/lattice-nohistory-null/runs/20261024T071422Z`; they are evidence, not fixtures to overwrite. No old binary has been silently relabeled as qualified by removing the new metadata in a test.

### Historical actual-Core receipt

The two-job Debug build reused `hud-scratch/latticecore-hud-migration/build`
with Apple clang 21, the macOS 27 SDK and deployment target 13.3. Evidence is
`hud-scratch/lattice-nohistory-null/runs/20261024T081321Z-core-paired-v2`:
`receipt.json` records full source hashes, dirty candidate patch, exact commands,
toolchain, source stability and artifact digests; `tests.json` and `tests.log`
record **9 tests, 0 failures, 0 errors, 0 disabled** (0.373 seconds). UTC wall
completion was `2026-10-24T08:13:51.224220+00:00`; GTest's raw timestamp uses
Eastern wall time with a `Z` suffix, so it is preserved but not treated as UTC.

The tested candidate was based on `61b7555753fdf9b3262b9ef82ada07d30173c314`
and included separately owned, uncommitted dynamic-object work; the receipt
captures that complete build input. Only the no-history packet is committed
with this document. No source changed during the passing run. The first run
(`20261024T081133Z-core-paired`) preserved a test-only namespace ambiguity;
qualifying `lattice::swift_lattice` resolved it without changing sync logic.

| Artifact | SHA-256 |
| --- | --- |
| `LatticeNoHistoryNullTests` | `f50b40c01d44fc9b1f3ddbbb7d19998ecdd66e78a034524290d9202c6e9ab56e` |
| `libLatticeCore.a` | `c589d37f8a5f5f558d9281228445e2e301673ac4d183bb46cebb4bd96c62e908` |
| `libLatticeSwiftCppBridge.a` | `242a472579784b15aaee4023751119a6fe0e8a0a880e67bc76ba127862d8ec69` |
