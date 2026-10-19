# Disabled Core 1.4 maintenance release preparation

The one-release maintenance path is implemented as dormant source code for
review. **No version is allocated, the profile remains disabled, both approval
registrations are null, and the native gate contract has no final source or
approved inventories.** Operational CLI guards and workflow preflight block
maintenance execution. Source preparation does not approve activation, native
resources, credentials, dispatch, signing, publication or deployment.

## Preserved normal release route

The shared `release_train.py`, its two existing test files and `policy.json`
remain unchanged. The Core wrapper still delegates an omitted or explicit
`main` profile to that protocol with the same remaining arguments. Normal 2.x
version ordering, canonical-main requirement, retry matching, native test
commands, publication behavior and shared publication concurrency are retained.
The reusable native workflows default to the event source for ordinary push/PR
and main-release runs. Maintenance-only inputs and receipt steps are conditional.

The earlier source-admission change and its 73 passing source tests remain the
baseline. The implementation extends that reviewed work; it does not replace the
existing release owner or create another release workflow.

## Prepared product source

The local metadata candidate is a direct child of reviewed notifier backport
`bcf43a8cc789d5330a103fcf1e97ebdfbf7e2b84`, which directly descends from Core
1.4.2 `36b828864cbb1543e945898be31589f9c04d6384`.

Product candidate: `a82742b0e237c378981d3d9a67ed31bea696ecdc`.
Product tree: `e5401d64ff58852411e08ef229f9291110db37c3`.

It changes only `CHANGELOG.md` (an unreleased 1.4.3 entry) and deletes the
legacy product-side `.github/workflows/release.yml`. All runtime source,
tests, public headers, package requirements, deployment floors, CMake and C ABI
version values match the reviewed backport. No Kit port, SDK update or direct
revision dependency override is included.

This identity is a proposed source for review, **not an active registration**.
The prepared local product branch has not been promoted to the policy's existing
`codex/linux-notifier-1-4-backport-20260929` branch. The owner must check the
current remote head before deciding any future promotion. If signing or further
edits change a commit, every final registration and qualification input must use
the resulting exact identity.

## Control and product separation

Only the reviewed main control checkout supplies Python helpers, release policy,
gate contracts and workflow definitions. The product checkout supplies the code
to build and test. Maintenance admission checks distinct clean canonical roots,
committed control-file hashes, full product SHA/tree, base tag peel, exact
backport/metadata parentage and complete registered diffs. It requires the
changelog and removal of the legacy product publisher, rejects hidden index
flags, sparse checkouts and replacement refs, and checks remote branch identities.

The maintenance version is exactly stable 1.4.3. It must exceed every parseable
remote 1.4 version by SemVer precedence, including prereleases and build-metadata
variants. Tag/release inventories include drafts and are fully paginated; exact
absence probes accept only authenticated HTTP 404/JSON Not Found. Read errors do
not establish absence. No other 1.x line or later 1.4 patch is approved.

The dormant workflow carries admitted product SHA/tree and control identity into
each reusable native gate. Each worker obtains its helpers from the control
checkout and verifies the product checkout separately. Product build steps do
not receive publication credentials. Native/validation jobs have read-only
repository permissions; only the publication job has write permission.

## Native receipts and gate contract

`maintenance-gates.json` is the control-owned contract for final source,
expected complete test inventories, required checks and high-descriptor evidence.
Its exact bytes are included in registered control hashes. Its shipped final
values are empty; a caller cannot substitute a smaller unregistered inventory.

Four gates are required: Core Linux, Core macOS, C ABI Linux and C ABI macOS.
Receipts bind the exact product/control commits and trees, profile and policy,
gate contract, workflow run/attempt, gate identity and retained artifact hashes.
GoogleTest discovery inventories are reconciled against executed XML cases;
empty, missing, duplicate, failed or skipped cases cannot become a full-suite
pass. C ABI evidence retains the SwiftPM and CMake shared-library legs, export
comparison, C11 header check and Linux all-target build. Toolchain/platform
observations and source hashes travel with the evidence.

Publication reconciliation verifies the exact release workflow, maintenance
run title, run/attempt and successful required jobs, as well as immutable artifact
metadata and downloaded file hashes. Every uploaded native evidence leaf is
accounted for by the receipt, with the receipt itself as the sole self-exclusion.
A receipt's own `passed` label or a previous attempt's artifacts are insufficient.
The release job itself may still be running while completed native jobs are
verified. The verifier must distinguish those states.

Direct high-file-descriptor evidence remains a separate prerequisite, tied to
the final product identity and an owner-reviewed evidence digest. The four
notifier tests in the backport do not force high descriptor numbers. No actual
native inventory, high-descriptor result or passing native receipt was generated
by this source-only work. Synthetic test fixtures are not qualification receipts.

## Retries and publication semantics

Maintenance retry identity includes repository, profile, version, control and
product source identities and candidate digest; run attempts remain separately
bound. Authenticated, paginated workflow reads reconcile matching runs and all
their attempts before candidate output and again before publication planning.
Only the single current first attempt can proceed through the dormant path.
Any previous matching run, rerun or partial attempt requires a separately
reviewed exception; no exception is registered. Evidence from older or different
attempts cannot satisfy a newer attempt.

The dormant publication path rechecks source/remote/version conditions, verifies
native receipts, and plans an immutable tag at the product commit. The stable
maintenance GitHub release uses `--latest=false`, with exact target, notes and
receipt hashes rechecked immediately before publication. Atomic create-ref rejects
any existing tag, including one already pointing at the same product commit. It never force-updates tags, overwrites existing releases/assets, or
silently repairs a partial publication. Post-publication verification checks the
published product identity and preserves the prior latest release. The normal
2.x publisher retains its existing semantics.

Operational maintenance entry points remain blocked independently of the JSON
profile. The protocol's CLI implementation sits behind a code-level disabled
switch; the existing wrapper preflight also rejects maintenance release commands.
Removing those guards or setting registrations is a separate activation change.
No hosted workflow, release/tag mutation or deployment was executed here.

## Source review commands and validation

`core_release.py maintenance-source-check --profile maintenance-1.4` is the
separate source-admission entry point. It requires explicit full control/product
identities and a separate product checkout; the shipped profile rejects it
before Git/GitHub inspection. A future admitted source result still reports
`maintenance-source-reviewed-not-release-admitted`, with dispatch and publication
false. It is not a native result or release receipt.

Run only local Python/Git fixture tests for this review, directing temporary
outputs under localdev:

```sh
mkdir -p "$HOME/localdev/APPROVED_EVIDENCE/tmp"
TMPDIR="$HOME/localdev/APPROVED_EVIDENCE/tmp" \
  PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s release-train -p 'test_*.py'
```

The tests use synthetic repositories and mocked transport/evidence. Workflow YAML
and shell syntax checks do not execute their native or publication commands.
The legacy 295/295 server-overlay run remains historical evidence for its exact
backport/server pair. It does not qualify this metadata child or normal published
dependency adoption.

## Remaining owner decisions

Jason/jsflax is the proposed release owner, not an approval recorded by these
files. Before activation, the owner must review control and product commits,
choose signing/promotion, approve the exact source/diff/control registrations,
provide the final gate inventories and reviewed high-descriptor evidence binding,
and authorize a bounded native qualification slot. The actual native receipts,
publication decision, stable 1.4.3 tag/release and deployment remain outstanding.

After legitimate publication, the legacy-server owner must resolve the tag
normally, preserving Lattice 1.7.2, Kit 12.5.0 and the unrelated 41 dependency pins,
update its source locks, and qualify the full suite without an overlay. The
broader performance goal remains paused.
