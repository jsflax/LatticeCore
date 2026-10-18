#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>
struct sqlite3;
struct sqlite3_stmt;
namespace lattice::detail {
// Nil-default TEST DATA observation, not a callback or permission. No Native
// owner, arbitrary allocation, SQL execution, policy or cleanup lives here.
struct observer_delivery_registration_observation {
    struct entry {
        std::array<char,256> sql{};
        uint16_t bytes=0;
        bool sql_present=false,truncated=false,readonly=false,busy=false;
    };
    uint64_t creator=0;
    std::array<entry,4> entries{};
    uint8_t count=0;
    bool observed=false,more=false;
    static thread_local observer_delivery_registration_observation* current;
    static void capture(sqlite3*,sqlite3_stmt*)noexcept;
};
static_assert(std::is_trivially_copyable_v<observer_delivery_registration_observation>);
static_assert(sizeof(observer_delivery_registration_observation)<=1152);
}
