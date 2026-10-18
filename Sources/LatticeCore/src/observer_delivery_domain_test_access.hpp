#pragma once
#include "observer_delivery_domain.hpp"
namespace lattice::detail {
// Test instrumentation only: hold the REAL metadata leaf across fork to prove
// child denial/retirement does not touch it. No resource or SQL grant.
struct observer_delivery_domain_test_access {
    static void lock_metadata(observer_delivery_domain&);
    static void unlock_metadata(observer_delivery_domain&) noexcept;
    // Exact next reservation -> allocation boundary only. No global allocator
    // replacement or invented admission/refund result.
    static void fail_next_client_allocation() noexcept;
    static void fail_next_lease_allocation() noexcept;
};
}
