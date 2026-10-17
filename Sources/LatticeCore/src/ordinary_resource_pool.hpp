#pragma once
#include <lattice/ordinary_sync.hpp>
#include <lattice/db.hpp>
#include <atomic>
#include "ordinary_capacity_notification.hpp"

namespace lattice::detail {
// One process pool across strict-v2 and paged-history-v1. Profiles do not
// acquire independent capacity. The reservation precedes source allocation.
inline std::atomic<size_t> ordinary_export_slots{0};
inline std::atomic<size_t> ordinary_receive_slots{0};
template<bool Receiving> class ordinary_resource_reservation final {
    bool active_=false;
    static std::atomic<size_t>& counter()noexcept {
        if constexpr(Receiving)return ordinary_receive_slots;
        else return ordinary_export_slots;
    }
public:
    ordinary_resource_reservation() {
        if constexpr(!Receiving) {
            // Same service-guarded counter used by strict wire, paged history
            // and accepted upstream captures. Typed exhaustion precedes SQL.
            reserve_ordinary_export_capacity();active_=true;return;
        }
        constexpr auto maximum=Receiving?ordinary_sync_limits::simultaneous_receives:ordinary_sync_limits::simultaneous_exports;
        auto& slots=counter();auto used=slots.load(std::memory_order_relaxed);
        do {if(used>=maximum)throw db_error(Receiving?"ordinary receive capacity exhausted":"ordinary export capacity exhausted");}
        while(!slots.compare_exchange_weak(used,used+1,std::memory_order_acq_rel));
        active_=true;
    }
    ordinary_resource_reservation(ordinary_resource_reservation&& other)noexcept:active_(other.active_){other.active_=false;}
    ordinary_resource_reservation(const ordinary_resource_reservation&)=delete;
    ordinary_resource_reservation& operator=(const ordinary_resource_reservation&)=delete;
    ~ordinary_resource_reservation(){
        if(!active_)return;
        if constexpr(Receiving)counter().fetch_sub(1,std::memory_order_release);
        else release_ordinary_export_capacity(); // actual final owner, off-leaf
    }
};
}
