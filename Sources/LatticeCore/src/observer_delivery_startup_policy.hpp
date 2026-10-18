#pragma once
#include <chrono>
#include <algorithm>
#include <array>
#include <string_view>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace lattice::detail {
class observer_delivery_startup;
class observer_delivery_open_startup;

// Passive, fixed-size limits. Decoding these fields never enrolls a connection.
struct observer_delivery_startup_limits final {
    std::uint64_t clients=0,waiters=0;
    std::uint64_t guaranteed_count=0,guaranteed_bytes=0,burst_count=0,burst_bytes=0;
    std::uint64_t events=0,event_bytes=0,workspace_bytes=0;
    std::uint64_t read_workspace_bytes=0,read_columns=0,read_rows=0;
    std::uint64_t admission_wait_ms=0,read_duration_ms=0;
    std::uint64_t initialization_statements=0,initialization_workspace_bytes=0;
    bool operator==(const observer_delivery_startup_limits&)const noexcept=default;
};

class observer_delivery_startup_policy;
std::optional<observer_delivery_startup_policy> store_resource_delivery_for_factory(
    const std::string& actual_path,std::chrono::steady_clock::time_point original_end);

// Issued only by the existing image/root-checked normal resource issuer. The
// actual factory still matches every physical open original; this object is
// neither a path-based SQL permission nor an open/ending/refund receipt.
class observer_delivery_startup_policy final {
    friend class observer_delivery_startup;
    friend class observer_delivery_open_startup;
    friend std::optional<observer_delivery_startup_policy> store_resource_delivery_for_factory(
        const std::string&,std::chrono::steady_clock::time_point);
    const std::uint64_t creator_;
    const void* const installation_;
    const void* const root_;
    const std::size_t role_;
    std::array<char,256> member_{};
    std::uint16_t member_size_=0;
    const std::chrono::steady_clock::time_point original_end_;
    const observer_delivery_startup_limits limits_;
    observer_delivery_startup_policy(std::uint64_t issuer,const void* installation,
        const void* root,std::size_t role,std::string_view member,std::chrono::steady_clock::time_point end,
        observer_delivery_startup_limits limits)noexcept
        :creator_(issuer),installation_(installation),root_(root),role_(role),
         original_end_(end),limits_(limits) {
        member_size_=static_cast<std::uint16_t>(member.size());
        std::copy(member.begin(),member.end(),member_.begin());
    }
    void verify_for_factory_(const std::string& actual_path)const;
public:
    observer_delivery_startup_policy(const observer_delivery_startup_policy&)noexcept=default;
    observer_delivery_startup_policy(observer_delivery_startup_policy&&)noexcept=default;
    // DATA only: no inherited shared/weak/Native ownership in destruction.
    ~observer_delivery_startup_policy()noexcept=default;
};
}
