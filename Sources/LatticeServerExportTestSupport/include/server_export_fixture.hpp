#pragma once
#include <lattice.hpp>

// Linked only by explicitly selected qualification tests. No production target
// depends on this module; these helpers confer no source/scope authority.
namespace lattice::server_export_test_support {
struct fixture_facts {
    int32_t status=1;
    int64_t originals=0,claimed=0,stamps=0;
};
// Caller holds the real ref across the synchronous call. One registered model,
// fresh per-test store, fixed explicit fixture budgets; no fabricated SQL stamp.
int32_t enroll(const swift_lattice_ref&,const std::string& model)noexcept;
int32_t freeze(const swift_lattice_ref&)noexcept;
fixture_facts facts(const swift_lattice_ref&)noexcept;
}
