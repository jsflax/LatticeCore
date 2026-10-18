#pragma once
#include "observer_delivery_domain.hpp"
#include <cstdint>
#include <memory>

namespace lattice::detail {
class observer_delivery_store;
// Single-directory DATA anchor. It owns generic count/byte admission and its
// immutable physical key only. It has no SQL/current_for/Open/Native operation
// and no public issuer. The existing real Store bind is the sole selector.
// A later trusted pre-open issuer must be a separately reviewed typed path;
// raw identity values or this class name never create that authority.
class observer_delivery_data_domain final {
    friend class observer_delivery_store;
    using domain=observer_delivery_domain;
    const std::uint64_t creator;
    const std::uint64_t device,inode;
    const domain::limits limits;
    const domain budget;
    observer_delivery_data_domain(std::uint64_t,std::uint64_t,domain::limits);
    static void destroy(observer_delivery_data_domain*)noexcept;
    static std::shared_ptr<observer_delivery_data_domain> select_(std::uint64_t,
        std::uint64_t,domain::limits);
    observer_delivery_data_domain(const observer_delivery_data_domain&)=delete;
    observer_delivery_data_domain& operator=(const observer_delivery_data_domain&)=delete;
};
}
