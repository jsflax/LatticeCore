#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG) || !defined(__APPLE__)
#error "RAW object observation requires the genuine debug macOS owner image"
#endif
#include <array>
#include <cstdint>
#include <type_traits>
namespace lattice::detail {
// Separate copied fixture DATA. No owner/exception/callback/buffer or setter.
struct ordinary_target_raw_object_observation final {
    struct object_fact {
        std::uintptr_t original=0,state_original=0;
        std::uint64_t owner_count=0,state_owner_count=0,flags=0;
        std::uint8_t kind=255,alias_of=255;
    };
    // object flags: 0 materialized; 1/2 facade ctor observed/returned;
    // 3/4 state ctor observed/returned; 5 counts observed; 6/7 source Native
    // first failure observed/value; 8/9 facade bind attempted/returned;
    // 10/11 state bind attempted/returned; 12/13 extraction attempted/returned;
    // 14/15 whole reset attempted/returned; 16/17 facade/state member tails;
    // 18/19 facade/state partial constructor; 20/21 calls observed/known;
    // 22 this exact object's ending (all SAME actual alias resets returned).
    std::array<object_fact,5> objects{}; // spool, append/current-observation, reader/current-observation
    bool creator_worker=false,receiving_owner_bound=false;
    bool attempted=false,allocation_returned=false,allocation_exception_returned=false,first_failure=false;
    bool preparation_returned=false,returned=false,history_checked=false,history_known=false;
    bool not_issued_observed=false,not_issued=false,physical_close_observed=false;
    std::int64_t physical_close_result=-1;
    int physical_close_error=0;
    int result=-1; // -1 unknown, 0 actual not-issued DATA, 1 all held ended, 2 retained debt
};
static_assert(std::is_trivially_copyable_v<ordinary_target_raw_object_observation>);
static_assert(std::is_standard_layout_v<ordinary_target_raw_object_observation>);
static_assert(sizeof(ordinary_target_raw_object_observation)<=512);
} // namespace lattice::detail
#endif
