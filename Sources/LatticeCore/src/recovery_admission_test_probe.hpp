#pragma once
#include <lattice/db.hpp>
namespace lattice::detail::recovery_admission_test_hooks {
// A private observation/rendezvous after a real engine query yields a row.
// No provenance is synthesized. Production leaves this thread-local hook null.
inline thread_local void (*after_initial_probe)() = nullptr;
inline thread_local void (*after_read_row)(database&, sqlite3_stmt*) = nullptr;
inline void read_row(database& db, sqlite3_stmt* statement) {
    if (after_read_row) after_read_row(db, statement);
}
}
