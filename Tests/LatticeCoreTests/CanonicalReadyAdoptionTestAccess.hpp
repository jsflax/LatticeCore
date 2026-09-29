#pragma once
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"

namespace lattice::detail {
// Defined once in CanonicalDurableReadyTests.cpp through its existing friend.
// Forward only the real private administrative operation; no admission issuer,
// raw writer escape or synthetic owner/producer/transaction custody.
canonical_ready_adoption_result adopt_ready_lifecycle_for_test(std::shared_ptr<lattice_db>,
    const canonical_namespaced_writer_profile&,canonical_upstream_limits,canonical_retention_limits,
    const canonical_ready_profile&,const std::string&,int64_t);
}
