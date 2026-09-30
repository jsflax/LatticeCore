#pragma once
#include <lattice.hpp>

namespace lattice {
// Inspect only captured provenance. Tests must never repair an old handle by
// assigning whatever generation happens to occupy its alias now.
struct managed_attachment_test_access {
    template<typename T>
    static managed<T> field(const dynamic_object_ref& object, const std::string& name) {
        return object.get()->managed_.get_managed_field<T>(name);
    }
    static bool live(const lattice_db& owner, int64_t token) {
        auto view = std::atomic_load(&owner.managed_attachment_view_);
        if (view) for (const auto& binding : *view)
            if (binding->token == token) return binding->valid.load(std::memory_order_acquire);
        return false;
    }
};
}
