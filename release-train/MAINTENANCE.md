# Inactive Core 1.4 maintenance source admission

This change implements the source-admission portion of a proposed, single Core
1.4.3 release. **The profile is disabled; final product and control approval
registrations are null. No version is allocated.** It does not implement or
authorize maintenance publication, activate a release workflow, reserve a native
resource slot or resume the performance work.

## Compatibility and default behavior

The shared `release_train.py`, its existing tests and `policy.json` remain
unchanged. The Core-specific wrapper delegates an omitted or explicit `main`
profile to the existing runner with the same remaining command arguments.
The release workflow preserves its existing run-name, main/tag source selection,
test dependencies, retry identity, version ordering, publication permissions,
concurrency and tag/release steps. The macOS/Linux/C ABI workflows are unchanged.

An explicit `maintenance-1.4` workflow request stops in preflight before the
native jobs. Every generic maintenance command—including `check`, `candidate`,
`dispatch`, `receipt`, `workflow-receipt` and `verify-package`—is blocked in code.
Changing `enabled` in JSON cannot remove that block. Notes/version suggestions
also cannot select an implicit maintenance exception. Unknown/duplicate profile
arguments fail rather than falling through to the default profile. Environment
variables do not implicitly select a CLI profile; the workflow passes its input
as a quoted explicit argument.

No package, native implementation, public header, symbol, schema, wire contract,
dependency requirement or deployment floor changes in this source branch. There
is no new Python dependency. The legacy notifier candidate remains a distinct
source branch and is not modified by these controls.

## Source-only entry point

The sole maintenance operation is `maintenance-source-check`. Its caller must
provide full control and product commit IDs and a separate existing product
checkout. The runner and policy are always loaded from the control checkout
containing the script. No candidate-side release script or policy is executed.

For a future owner-reviewed registration, its command shape is:

```sh
python3 release-train/core_release.py maintenance-source-check \
  --profile maintenance-1.4 --version 1.4.3 \
  --control-sha FULL_CONTROL_MAIN_COMMIT \
  --expected-sha FULL_PRODUCT_COMMIT \
  --product-root "$HOME/localdev/APPROVED_PRODUCT_CHECKOUT" \
  --output "$HOME/localdev/APPROVED_EVIDENCE/source-review.json"
```

The shipped policy intentionally rejects this command before any Git/GitHub
inspection. Do not fill the placeholders, enable the policy, create release
metadata or treat this example as an activation instruction without a new
reviewed decision. Synthetic source tests use private fixture registrations.

If a future registered source passes, the result is explicitly
`maintenance-source-reviewed-not-release-admitted`, with
`dispatchAdmitted=false` and `publicationAdmitted=false`. It is not a candidate,
native result or release receipt and cannot unlock a workflow. Output must be
outside both source checkouts.

The source checks require the exact repository/branch, explicit approvals,
control file hashes, clean separate source roots, current canonical main,
registered product branch head/SHA/tree, the original 1.4.2 tag identity,
reviewed backport ancestry, complete registered source and metadata diffs,
the 1.4.3 changelog and deletion of the legacy product-side release workflow.
They verify all parseable remote 1.4 versions by SemVer precedence, including
prereleases and build metadata, and reject any existing 1.4.3 tag/release/draft.
Incomplete or failed remote reads do not mean absence.

The fixed product base is Core 1.4.2 at
`36b828864cbb1543e945898be31589f9c04d6384`; the reviewed backport is
`bcf43a8cc789d5330a103fcf1e97ebdfbf7e2b84`, on
`codex/linux-notifier-1-4-backport-20260929`. The final release metadata successor
does not yet have an approved identity. No generic 1.x support policy or later
1.4 patch is admitted by this profile.

## Explicitly outside this change

A separate activation review must implement and qualify the control/product
workflow split, exact product inputs to every native gate, source-bound native
and high-descriptor evidence, schema-2 publication receipts, final rechecks,
profile-aware retry identity, publication-only write permissions and stable
maintenance `--latest=false` behavior. The existing publisher is deliberately
unreachable for this profile until that work is reviewed; it has not been
repurposed to publish 1.4.3.

Jason/jsflax is the proposed release owner. No confirmation, approval reference,
signature or final source registration is inferred from that proposal. The
future product successor needs its own source review and signing record under
the repository's existing conventions. This change is not a signed release.

The retained legacy-server overlay result passed 295 tests, but normal published
dependency consumption and full final-source Core/C ABI qualification remain
pending. Direct high-file-descriptor qualification is also pending. After a
legitimate release, the legacy-server owner must update through ordinary
resolution while retaining Lattice 1.7.2, Kit 12.5.0 and the unrelated 41 pins.
No Kit port or direct-revision shortcut is part of this change.

## Local source validation

Use a scratch directory under the workspace for temporary Git fixtures and
Python cache files, then run the shared regressions plus new profile/wrapper
tests:

```sh
mkdir -p "$HOME/localdev/APPROVED_EVIDENCE/tmp"
TMPDIR="$HOME/localdev/APPROVED_EVIDENCE/tmp" \
  PYTHONDONTWRITEBYTECODE=1 \
  python3 -m unittest discover -s release-train -p 'test_*.py'
```

These are source-level Python/Git fixture tests with mocked GitHub reads. They
are not native, hosted or consumer release qualification. No secrets, workflow
dispatch, tag, release, activation or deployment are required to run them.
