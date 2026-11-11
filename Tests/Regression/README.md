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
Swift bridge's process-wide cache. It avoids the GoogleTest fixture logger,
observer-delivery load and sleeps. Ordinary Core initialization may create
cross-process notification wiring; the probe does not rely on its timing.

The CMake `LatticeCacheExitProbe` target is available on Unix platforms. It links
the matching bridge and Core targets and is not executed by `LatticeCoreTests`.
Run its absolute binary with one argument: an existing caller-owned absolute
directory. The probe rejects relative or missing roots and existing fixture
directories. It creates a fresh `cache-exit-<pid>/fixture.sqlite` beneath the
supplied root and does not change HOME or TMPDIR. Capture stdout, stderr and the
exit status, retaining the fixture until the verification record is complete.

The unregistered control proves that the custom control-block allocator reports
deallocation. A real SQLite fixture is created, read and closed before exit;
its closed object stays strongly retained for the final identity lookup. A
different real object is registered only in the cache's weak pointer map, then
all its strong references are released. Its destruction is required, but its
control block must remain retained by that cache entry until process exit.

The verifier is registered with `atexit` before first cache access. With a
static cache object, normal exit destroys the cache first and the verifier
observes the released weak-control allocation, emitting
`weak_registry_destroyed` and exiting 70. It never touches a destroyed cache.
With a process-lived cache, it resolves the closed fixture's exact identity and
emits `late_cache_identity_passed` with exit 0. `controls_passed` alone is not
success; every other exit code, signal, missing terminal receipt or timeout
fails. The late verifier performs no SQL or unrelated static teardown work.

Qualification requires the unchanged probe in two fresh processes, rebuilding
the bridge archive for each cache implementation: the static-object version
must exit 70, and the process-lived version must exit 0. Follow this with the
downstream scope that first failed, including normal process closure. An address
sanitizer is not required for the allocator-based oracle. This proves the cache
exit-lifetime contract; it does not establish the cause of the originally
reported observer-delivery crash.

For a native macOS build, select the intended Xcode with `DEVELOPER_DIR` rather
than changing global `xcode-select`. The libraries and probe must use the same
C++20, deployment-target and foreign-reference ABI configuration. The following
example uses a macOS 14 deployment target and caller-supplied absolute paths:

```sh
: "${LATTICE_PROBE_CHECKOUT:?Absolute reviewed checkout}"
: "${LATTICE_PROBE_BUILD:?Absolute build directory}"
: "${LATTICE_PROBE_RECEIPTS:?Existing caller-owned absolute directory}"
: "${DEVELOPER_DIR:?Selected Xcode Developer directory}"
export DEVELOPER_DIR
cmake -S "$LATTICE_PROBE_CHECKOUT" -B "$LATTICE_PROBE_BUILD" \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_OSX_DEPLOYMENT_TARGET=14.0 \
  -DCMAKE_C_COMPILER="$(xcrun --find clang)" \
  -DCMAKE_CXX_COMPILER="$(xcrun --find clang++)"
cmake --build "$LATTICE_PROBE_BUILD" --target LatticeCacheExitProbe --parallel 2
"$LATTICE_PROBE_BUILD/LatticeCacheExitProbe" "$LATTICE_PROBE_RECEIPTS"
```

If compiling the standalone source manually against those matching archives,
include all three public header directories and retain the same target flags:

```sh
xcrun clang++ -std=c++20 -mmacosx-version-min=14.0 \
  -DSQLITE_CORE -fno-implicit-module-maps \
  -I "$LATTICE_PROBE_CHECKOUT/Sources/LatticeSwiftCppBridge/include" \
  -I "$LATTICE_PROBE_CHECKOUT/Sources/LatticeCore/include" \
  -I "$LATTICE_PROBE_CHECKOUT/Sources/SqliteVec/include" \
  "$LATTICE_PROBE_CHECKOUT/Tests/Regression/LatticeCacheExitProbe.cpp" \
  "$LATTICE_PROBE_BUILD/libLatticeSwiftCppBridge.a" \
  "$LATTICE_PROBE_BUILD/libLatticeCore.a" "$LATTICE_PROBE_BUILD/libSqliteVec.a" \
  -lsqlite3 -o "$LATTICE_PROBE_BUILD/LatticeCacheExitProbe"
```

A successful compile is not a probe execution. Record both old/new exit receipts
before treating the lifetime regression as qualified. The cache is intentionally
never destroyed, so leak-tool output must account for its process-lived storage.
