#pragma once
#include <LatticeCore.hpp>
#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include <memory>
#include <mutex>

namespace lattice {
// Shared private test friend: non-owning writer and passive tail DATA only.
// No test-created Native/transaction/refund or SQL/admission authority.
struct observer_delivery_custody_test_access {
    static std::weak_ptr<database> writer(lattice_db& owner) {
        std::lock_guard<std::mutex> leaf(owner.connection_ownership_mutex_);
        return owner.db_;
    }
    static detail::recovery_admission_test_hooks::mutation_tail_data tail(lattice_db& owner) {
        owner.require_creator_process();std::lock_guard<std::mutex> leaf(owner.change_buffer_mutex_);
        return {owner.is_flushing_,owner.recovery_change_buffer_reserved_,
            owner.change_buffer_.size(),owner.topology_delivery_depth_,owner.mutation_delivery_depth_,
            owner.change_buffer_custody_epoch_};
    }
};
} // namespace lattice
