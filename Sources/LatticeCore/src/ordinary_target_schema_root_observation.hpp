#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG) || !defined(__APPLE__)
#error "Schema root observation requires the genuine debug macOS owner image"
#endif
#include <array>
#include <cstdint>
#include <type_traits>
namespace lattice::detail {
struct ordinary_target_schema_root_observation final {
    struct root_fact {
        std::uintptr_t original=0;
        std::uint64_t flags=0,owner_count=0;
    };
    // Root flags:0materialized;1/2constructor observed/returned;3count observed;
    // 4/5binding attempted/returned;6actual member tail;7partial construction.
    std::array<root_fact,2> roots{}; // Actual wrapper and actual const state member.
    std::array<std::uint64_t,2> sources{}; // Each real source:bit0extraction attempted;bit1returned.
    std::uintptr_t receiving_state_original=0,request_original=0,request_state_original=0,connection_original=0;
    bool creator_worker=false,receiving_owner_bound=false,originals_observed=false;
    bool attempted=false,allocation_returned=false,allocation_exception_returned=false,first_failure=false;
    bool preparation_returned=false,reset_attempted=false,reset_returned=false;
    bool ending_observation_attempted=false,ending_observation_returned=false,ending_known=false,returned=false;
    bool history_checked=false,history_known=false,source_failure_observed=false,source_failure=false;
    int result=-1; // -1unknown,1actual held schema/state ended,2retained debt; no not-issued ending.
};
static_assert(std::is_trivially_copyable_v<ordinary_target_schema_root_observation>);
static_assert(std::is_standard_layout_v<ordinary_target_schema_root_observation>);
static_assert(sizeof(ordinary_target_schema_root_observation)<=256);
}
#endif
