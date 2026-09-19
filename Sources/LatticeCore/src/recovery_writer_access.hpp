#pragma once
#include <lattice/lattice.hpp>

namespace lattice::detail {
// Private admission read only. This borrows the caller's explicit transaction;
// it neither acquires ownership nor retains the owner. Keep the owner and its
// transaction alive through the entire operation. Maintenance admission needs
// a separate authority path; an arbitrary active SQLite transaction is not one.
struct recovery_writer_access {
    static database* active_writer(lattice_db& owner) {
        if (owner.is_closed() || !owner.owns_write_transaction()) return nullptr;
        auto& writer = owner.db();
        if (writer.is_closed() || sqlite3_get_autocommit(writer.handle()) != 0 ||
            sqlite3_txn_state(writer.handle(), "main") != SQLITE_TXN_WRITE)
            return nullptr;
        return &writer;
    }
};
} // namespace lattice::detail
