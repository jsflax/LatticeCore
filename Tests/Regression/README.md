# Foreground WAL checkpoint regression

`WalForeignReaderProbe.cpp` is a standalone fixture for foreground WAL
checkpoint behavior. It has two controller-driven modes, both given
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

The probe checks that a foreign reader does not delay foreground acquisition,
while maintenance retains its bounded wait and can truncate the WAL after the
reader releases its transaction. Exact row checks cover data visibility across
connection release and reacquisition.

This standalone native regression covers checkpoint behavior only. It does not
qualify the full GoogleTest suite or a consuming application's integration.
