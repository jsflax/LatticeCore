# Foreground WAL checkpoint regression

`WalForeignReaderProbe.cpp` is the exact standalone fixture executed for QA284's
foreground checkpoint correction. It has two controller-driven modes, both given
one explicit isolated database path. `writer` owns a normal LATTICE_SCHEMA model;
`reader` uses a separate process/SQLite connection and holds its read transaction
until the controller acknowledges release. No application profile is involved.

The controller starts the writer and waits for `ready`, starts the reader and
waits for `holding`, then writes `continue` to the writer. The writer performs
eight commit/acquire/read/release cycles with exact row checks and a 100ms
foreground acquisition ceiling. It also checks that maintenance still waits for
its existing bounded budget while the reader is held. On `releaseReader`, the
controller acknowledges and joins the reader before acknowledging the writer.
The writer then proves maintenance truncates the WAL without another write.

The saved exact runner and results are under Orbital iteration284
`wal-acquire-regression-1`; source SHA equality is recorded in the handoff.
The actual test compiled every Core translation unit from this patched tree and
reused only unchanged SqliteVec.o. It did not link the old Core.o.
This is a standalone native regression, not a claim that the full GoogleTest
suite or rebuilt Orbital GUI was executed.

This branch applies only the WAL correction to current upstream main. The saved
regression result above qualifies the retained private Core source, and the
subsequent Orbital consumer run qualifies that private source through the Swift
bridge. Native checks have not been rerun on this contextual upstream commit.

# Pointer-cache lifetime regression

`LatticeCacheExitProbe.cpp` is a standalone, fresh-process regression for the
Swift bridge's process-wide cache. It deliberately avoids the GoogleTest
fixture logger and uses no application profile, provider, test-created worker,
observer-delivery load or sleeps. Ordinary Core initialization may create native
cross-process notification wiring; this probe does not rely on its timing.

Build it against the exact reviewed `LatticeSwiftCppBridge`, `LatticeCore` and
SqliteVec sources/objects, with their matching C++20, deployment-target and
foreign-reference ABI configuration. Rebuild the changed bridge translation
unit; do not link a stale `lattice.cpp` object. This fixture has no SwiftPM test
selector and is not implicitly executed by `LatticeCoreTests`. The executor
must record its concrete compiler/link command, source/object identities and
absolute probe binary before admission. No new package target is required.

Run that absolute binary as a normal subprocess with exactly one argument:
an existing caller-owned directory strictly beneath the canonical
`$HOME/localdev`. For example, after the caller creates its owned artifact root:

```sh
/absolute/localdev/path/to/LatticeCacheExitProbe /absolute/localdev/path/to/owned-artifacts
```

The probe rejects missing/relative/outside-localdev roots and existing fixture
directories; it creates only a fresh `cache-exit-<pid>/fixture.sqlite` beneath
that root. Capture stdout, stderr, exit status and any crash report under the
same owned root. Keep the database/sidecars as evidence until the executor's
normal retention/cleanup step. Neither HOME nor TMPDIR is changed. There is no
fallback to `/tmp`, no logger file, and no test filter that can match zero cases.

The real unregistered control proves that the custom control-block allocator
reports deallocation. A real SQLite fixture is created, read and closed before
exit; its closed object stays strongly retained for the final identity lookup.
A different real object is registered only in the cache's weak pointer map,
then all its strong references are released. Its destruction is required, but
its control block must remain retained by that cache entry until process exit.

The verifier is registered with `atexit` before first cache access. On old code,
normal exit destroys the cache first and the verifier observes the released
weak-control allocation, emitting `weak_registry_destroyed` and exiting70.
It never touches a destroyed cache. Fixed code retains the cache, so the
verifier resolves the closed fixture's exact identity and emits
`late_cache_identity_passed` with exit0. `controls_passed` alone is not success;
all other exit codes, signals, missing terminal receipts or timeouts fail.
The late verifier performs no SQL, schema construction or unrelated teardown.

Required qualification is an unchanged-probe old/new comparison in separate
fresh processes (old source expected70, corrected source expected0), followed
by the exact originally failing Orbital scope with normal process closure.
An address sanitizer is not required for this allocator-based oracle. This
proves the cache's exit-time lifetime contract; it does not establish that
static destruction caused the original ROOT23 observer-delivery crash.

For a native macOS CMake build, after the machine's disk and single-build checks,
the concrete build/link shape is below. The caller supplies the reviewed
checkout, shared repository build directory and owned receipt directory. Select
the runner's pinned Xcode through `DEVELOPER_DIR`; do not change global
`xcode-select`. The CMake archives and probe use the same macOS 14 deployment
target and therefore the same bridge handle layout.

```sh
: "${LATTICE_PROBE_CHECKOUT:?Absolute reviewed LatticeCore checkout}"
: "${LATTICE_PROBE_BUILD:?Shared build directory under HOME/localdev/.build-cache}"
: "${LATTICE_PROBE_RECEIPTS:?Existing owned receipt directory below HOME/localdev}"
: "${DEVELOPER_DIR:?Pinned Xcode Developer directory}"
export DEVELOPER_DIR
cmake -S "$LATTICE_PROBE_CHECKOUT" -B "$LATTICE_PROBE_BUILD" \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_OSX_DEPLOYMENT_TARGET=14.0 \
  -DCMAKE_C_COMPILER="$(xcrun --find clang)" \
  -DCMAKE_CXX_COMPILER="$(xcrun --find clang++)"
cmake --build "$LATTICE_PROBE_BUILD" --target LatticeSwiftCppBridge --parallel 2
xcrun clang++ -std=c++20 -mmacosx-version-min=14.0 \
  -fno-implicit-module-maps \
  -I "$LATTICE_PROBE_CHECKOUT/Sources/LatticeSwiftCppBridge/include" \
  -I "$LATTICE_PROBE_CHECKOUT/Sources/LatticeCore/include" \
  "$LATTICE_PROBE_CHECKOUT/Tests/Regression/LatticeCacheExitProbe.cpp" \
  "$LATTICE_PROBE_BUILD/libLatticeSwiftCppBridge.a" \
  "$LATTICE_PROBE_BUILD/libLatticeCore.a" "$LATTICE_PROBE_BUILD/libSqliteVec.a" \
  -lsqlite3 -o "$LATTICE_PROBE_BUILD/LatticeCacheExitProbe"
"$LATTICE_PROBE_BUILD/LatticeCacheExitProbe" "$LATTICE_PROBE_RECEIPTS"
```

Run the same probe source against the old and corrected `instance()` bodies,
rebuilding the bridge archive each time. Keep only the resulting small receipts
after qualification; build outputs belong in the shared build tree. A successful
compile does not count as executing this probe, and this command has not been
implicitly run by the main test executable.
