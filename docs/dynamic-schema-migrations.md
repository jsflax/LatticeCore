# Dynamic schema migration callbacks

`swift_lattice_ref::create_with_migration` applies its generic callback to existing
tables supplied through `SchemaVector`. It reports added, removed and changed
columns, and calls the callback once for the detected dynamic changes before
Swift table DDL. New tables and a version bump without column changes do not
invoke this schema-change callback. Schema changes at the same version retain
the existing automatic-migration behavior and invoke the callback.

The callback runs inside the Swift schema setup transaction. Immediate SQL and
queued row transformations, automatic table migration, schema version, saved
schema and fingerprint markers commit together. A callback or queued SQL failure
aborts that transaction. Queued values targeting new columns apply after those
columns exist. The callback must not manage its own transactions, and its context
is only valid during the synchronous call.

Versioned Swift row hooks retain their existing sequencing. If a configuration
explicitly combines both mechanisms, the generic callback runs first; its queued
values apply after the versioned row transformations and final automatic schema
migration. Callers should avoid conflicting writes between the hooks.
The Swift context's read-only enumeration does not itself queue writes, and
`set_row_value` forwards only explicitly selected fields. Unrelated row-hook
changes therefore survive a generic update to another field.

This is not a whole-open transaction across both schema systems. The base
`lattice_db` constructor initializes globally registered C++ schemas in an earlier
transaction. A mixed C++ registry and dynamic-schema configuration can therefore
invoke the generic callback in each phase. Failure in the later dynamic phase
does not undo the already completed base phase.

`LatticeDynamicMigrationTests` is a focused actual-Core CMake test target covering
callback delivery, pending changes, transforms, replay, downgrade refusal,
rollback/retry, row-hook ordering and FRT context retain cleanup. Supply `TMPDIR`
when running to choose the fixture location. This change does not select or
publish a consuming SDK's dependency version.

The focused native run passed all ten cases on the Core 2.3.1 repair worktree.
Rollback fixtures explicitly establish the separately committed base phase first,
then compare all dynamic data, audit rows, SQLite schema, version/schema metadata
and fingerprint markers. The row-update failure fixture installs a real SQLite
unique index rather than assuming property-index metadata support. An old-source
negative control and a rebuilt consuming Wasm run are still pending; the native
result alone does not qualify LatticeJS or an installed HUD runtime.

## Local qualification record

Source: isolated branch `codex/hud-dynamic-migration`, based on published Core
2.3.1 commit `db7edece4f31e6b00ea537d870c2cb452ef9c409`. The implementation,
focused test and CMake target are committed together with this record; no
dependency pin, installed app, remote service or published package was changed.

The binary diff against that base for the five code/test/build files below has
SHA-256 `4c14c345409518e325021a50429c56b708b523aeda267d68e984a3c879fe5937`
(`git diff --cached --binary -- <the five paths>`, excluding this document).
Exact file SHA-256 values at the passing run:

| Path | SHA-256 |
| --- | --- |
| `CMakeLists.txt` | `404e0a3e37e275fda4231aafcf88cc320a5098b150a79fbe9a09646bd159de6a` |
| `Sources/LatticeCore/include/lattice/lattice.hpp` | `b94e1ec217a576d2a3f259fd57cdad1dbac774ca14eab25481de1f8a3dd27250` |
| `Sources/LatticeSwiftCppBridge/include/lattice.hpp` | `c7a7aaae944d72e46eabc58e108f11efe7c16b4a6e5f469c9f3c0f7b3032aa8d` |
| `Sources/LatticeSwiftCppBridge/src/lattice.cpp` | `fbc6b02b509ed5e6959ed4c2380546e85715615efa6bc4ecc95414a980c1d387` |
| `Tests/LatticeCoreTests/DynamicMigrationTests.cpp` | `64cc0e168b4559a0425b2c78faad1b851f957b38e79cad2de263510ea2f9d864` |

Toolchain: Apple clang 21.0.0 (`clang-2100.3.34.2`), arm64 target,
macOS 27.0 SDK, deployment target 13.3, C++20, Debug configuration;
CMake 3.31.3. Only `LatticeDynamicMigrationTests` and its required libraries were
built, with at most two compilation jobs. The final test process exited 0:
10 tests, 0 failures, 0 disabled, 0 errors, 0.908 seconds reported by GTest.

The run completed at approximately **2026-10-24 04:58 UTC**. The raw GTest JSON
says `2026-10-24T00:58:38Z`, which is the Eastern wall time incorrectly suffixed
with `Z`; the receipt's filesystem timestamp is `2026-10-24T04:58:39.680379+00:00`.
The raw receipt was preserved unchanged.

Commands used (all scratch, fixtures and module cache remain under `localdev`):

```sh
TMPDIR=/Users/jason/localdev/hud-scratch/latticecore-hud-migration/tmp \
CLANG_MODULE_CACHE_PATH=/Users/jason/localdev/hud-scratch/latticecore-hud-migration/module-cache \
cmake -S /Users/jason/localdev/latticecore-hud-migration \
  -B /Users/jason/localdev/hud-scratch/latticecore-hud-migration/build \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/bin/clang \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_OSX_DEPLOYMENT_TARGET=13.3

TMPDIR=/Users/jason/localdev/hud-scratch/latticecore-hud-migration/tmp \
CLANG_MODULE_CACHE_PATH=/Users/jason/localdev/hud-scratch/latticecore-hud-migration/module-cache \
cmake --build /Users/jason/localdev/hud-scratch/latticecore-hud-migration/build \
  --target LatticeDynamicMigrationTests --parallel 2 \
  > /Users/jason/localdev/hud-scratch/latticecore-hud-migration/build.log 2>&1

TMPDIR=/Users/jason/localdev/hud-scratch/latticecore-hud-migration/tmp \
/Users/jason/localdev/hud-scratch/latticecore-hud-migration/build/LatticeDynamicMigrationTests \
  --gtest_output=json:/Users/jason/localdev/hud-scratch/latticecore-hud-migration/native-tests.json \
  > /Users/jason/localdev/hud-scratch/latticecore-hud-migration/native-tests.log 2>&1
```

Receipts under `/Users/jason/localdev/hud-scratch/latticecore-hud-migration`:

| File | SHA-256 |
| --- | --- |
| `native-tests.json` | `eb22a495ef57156a560150a5b02035ad728e9a599183eeeefa2530869aeee2ee` |
| `native-tests.log` | `97f2a86b9da0f33f5dc70c4268c66e53555065b7a2abbaf9458b5845d678b643` |
| `build/LatticeDynamicMigrationTests` | `f0e41269a702bb5e7b96d108daa334db1bcb225ad2a6e13989381c02cdcf1ceb` |

Scope: this proves the focused native dynamic-schema callback, transform,
rollback, version, coexistence and ownership behavior described above. It is
not release qualification, a whole-Core-suite result, a Swift SDK test run or
Wasm/browser parity. The old-source negative control is still pending a shared
build window; no compile or test process from this run remains active.
