#pragma once
#include <lattice/lattice.hpp>
#include <utility>

// One identical friend-only definition for focused CMake targets and the
// combined SwiftPM test executable. No production injection or public API.
namespace lattice {
struct audit_maintenance_test_access {
    template<class F> static int64_t owned(lattice_db& owner, F&& body) {
        return owner.with_audit_prune_transaction_(std::forward<F>(body));
    }
    static int64_t cursor(const lattice_db& db) { return db.last_seen_audit_id_.load(); }
};
} // namespace lattice
