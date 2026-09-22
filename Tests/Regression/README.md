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
