#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
namespace lattice::detail {
uint64_t database_native_process()noexcept;
// One DATA-only monotonic tally shared by a real plan and its issued original.
// It owns no credit/Native/input/callback. Its retirement is never a refund.
class observer_delivery_workspace final {
    const uint64_t creator_,maximum_;
    std::atomic<uint64_t> used_;
    observer_delivery_workspace(uint64_t issuer,uint64_t maximum,uint64_t used)
        :creator_(issuer),maximum_(maximum),used_(used){}
    static void destroy(observer_delivery_workspace* value)noexcept {
        if(value&&value->creator_==database_native_process())delete value;
    }
public:
    static std::shared_ptr<observer_delivery_workspace> create(uint64_t issuer,uint64_t maximum,uint64_t used=0) {
        if(issuer!=database_native_process()||used>maximum)throw std::runtime_error("delivery workspace issuer/declaration differs");
        auto result=std::shared_ptr<observer_delivery_workspace>(new observer_delivery_workspace(issuer,maximum,used),destroy);
        if(issuer!=database_native_process())throw std::runtime_error("inherited delivery workspace allocation");
        return result;
    }
    bool current(uint64_t maximum)const noexcept {
        return creator_==database_native_process()&&maximum_==maximum;
    }
    void claim(uint64_t bytes) {
        if(creator_!=database_native_process())throw std::runtime_error("inherited delivery workspace");
        auto used=used_.load(std::memory_order_acquire);
        do {
            if(used>maximum_||bytes>maximum_-used)throw std::length_error("delivery output exceeds the original admitted workspace");
        }while(!used_.compare_exchange_weak(used,used+bytes,std::memory_order_acq_rel));
    }
    uint64_t remaining()const {
        if(creator_!=database_native_process())throw std::runtime_error("inherited delivery workspace");
        const auto used=used_.load(std::memory_order_acquire);
        if(used>maximum_)throw std::runtime_error("delivery original workspace corrupt");
        return maximum_-used;
    }
};
}
