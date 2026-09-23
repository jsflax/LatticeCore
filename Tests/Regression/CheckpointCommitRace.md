# Checkpoint and COMMIT on the same connection

`database::wal_checkpoint` shares a serialized SQLite connection with ordinary
writes. The old `query("PRAGMA wal_checkpoint(...)")` path left a non-readonly
statement active between its first `SQLITE_ROW` and the next step/finalize.
A concurrent COMMIT in that gap could fail with `SQL statements in progress`.

The implementation uses `sqlite3_wal_checkpoint_v2` while holding the recursive
SQLite connection mutex across temporary busy-timeout installation, checkpoint,
and timeout restoration. It retains PRAGMA-style result codes, attached-database
coverage, frame counts, and maintenance statement accounting.

`WALCheckpointTests.cpp` includes six ordinary semantic tests and one bounded,
deterministic interleaving test. The latter pauses the old PRAGMA after SQLite
returns its row and releases its per-call mutex, then attempts a transaction on
the same real connection. It calls the real SQLite API; no timing-only race or
fake database is used. On the fixed path, the checkpoint finishes without an
active statement and the transaction commits.

The CMake `LatticeCheckpointInterposedDB` object target renames `sqlite3_step`
only in its test copy of `db.cpp`. `LatticeCheckpointRegressionTests` links that
object with SqliteVec, SQLite, and GoogleTest; the test supplies its own logging
level definition. It does not link the production LatticeCore archive, and
production targets receive neither test macro. The ordinary CMake and SwiftPM
test executable includes the six semantic cases without interposition.

To qualify this upstream checkout, use an isolated build directory:

```sh
cmake -S . -B "$LATTICE_CHECKPOINT_BUILD"
cmake --build "$LATTICE_CHECKPOINT_BUILD" --target LatticeCheckpointRegressionTests --parallel 1
"$LATTICE_CHECKPOINT_BUILD/LatticeCheckpointRegressionTests" --gtest_filter='WALCheckpoint.*'
```

Set `LATTICE_TEST_ARTIFACTS` to an existing directory to choose the temporary
database location; otherwise the tests use the operating system temporary
directory. Each test creates and removes its own unique child directory.

Existing evidence applies to Orbital's vendor snapshot, not this upstream port:
the old implementation at `46381c8efa09cd208005575a126370c6b30892d0` failed the
interleaving test with one active write statement and a failed COMMIT. The fixed
vendor commit `10da9601ad9e995df04a008bb6b6e09729e07500` passed all seven tests with
zero active write statements and a successful COMMIT. The source port preserves
that production method exactly while retaining upstream's unrelated maintenance
changes. This upstream checkout has not been compiled or tested; no whole-app,
device, or historical failure attribution follows from the vendor result.
