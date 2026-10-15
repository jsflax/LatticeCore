#pragma once
#ifdef LATTICE_MANAGED_IPC_WORKER_QUALIFICATION
#include "ordinary_managed_ipc_workers.hpp"
#include <atomic>
namespace lattice::detail {
// Compile-time qualification only. Holds the actual worker leaf using fixed
// lock-free rendezvous facts; accepts no callback, authority, fd or state DTO.
struct ordinary_managed_ipc_worker_leaf_fixture final {
    static bool hold(const std::shared_ptr<ordinary_managed_ipc_workers>&,
        std::atomic<bool>& entered,std::atomic<bool>& held,const std::atomic<bool>& release,
        std::chrono::steady_clock::time_point original_deadline);
};
}
#endif
