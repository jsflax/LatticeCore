# Explicit transport-managed authentication (candidate)

`configuration.sync_authentication_mode` defaults to `sync_authentication::bearer_token`.
Existing constructors and Swift-derived configuration copies retain that default.
An endpoint without a token remains disabled unless a caller explicitly selects
`sync_authentication::transport_managed`.

This mode delegates authentication to the configured transport. The browser binding
uses it only with its per-owner admission factory, after validating an empty token.
Core does not create a dummy bearer token. Existing `synchronizer_base::connect`
therefore emits no Authorization header for the empty-token managed mode; it retains
the existing noncredential `last-event-id` resume cursor. A supplied real token keeps
its previous header semantics. Mode identity is included in the Swift bridge cache
fingerprint so transport-managed and bearer configurations cannot silently alias.

This is an additive configuration gate, not a gateway or identity provider. The host
must authorize each upgrade, enforce cookie/connection ownership, and reject revoked
or superseded grants. It does not add native retry policies, browser credentials, or
an auth bypass to the default path.

The focused `LatticeTransportAuthenticationTests` target contains four
regressions: default/copy semantics, actual bridge cache identity, actual Core startup
with empty-token disabled versus managed admission, and legacy bearer headers. Its
network factory records synthetic transport calls and never connects to a service.
Set TMPDIR to a localdev scratch directory and build the target with at most two jobs
in the coordinated native window.

On 2026-10-24 the actual native target passed all four tests (0 failures/errors,
0.034 seconds) using Apple clang and the existing Debug CMake graph, with two build
jobs. The command was `cmake --build <scratch>/build --target
LatticeTransportAuthenticationTests --parallel 2`, after reconfiguring the graph,
then the resulting executable with `--gtest_output=json:<evidence>/tests.json`.
Exact source hashes, the patch, configure/build logs, and GTest receipt are retained
at `/Users/jason/localdev/hud-scratch/lattice-transport-auth-v1/`.

The paired LatticeJS candidate also passed its unchanged 39-case actual Wasm gate
and four actual Chrome admission scenarios. Those exercise a loopback HttpOnly ticket
fixture, not the production gateway or HTTPS/Secure-cookie policy. Evidence is under
`/Users/jason/localdev/latticejs-hud/tmp/wasm-sync-admission-v1/`.
No release, published pin, or owner application adoption is claimed. The e7361f9
qualified candidate evidence remains immutable.
