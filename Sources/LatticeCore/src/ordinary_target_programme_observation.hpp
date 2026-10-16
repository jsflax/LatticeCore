#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG) || !defined(__APPLE__)
#error "Programme observation requires the genuine debug macOS owner image"
#endif
#include <array>
#include <cstdint>
#include <type_traits>
namespace lattice::detail {
// Separate copied DATA, never a programme/owner/native or completion setter.
struct ordinary_target_programme_observation final {
    struct object_fact {
        std::uintptr_t original=0,projection_original=0;
        std::uint64_t flags=0,owner_count=0,bytes=0,items=0,cells=0,bindings=0;
    };
    // Object flags:0 issuance observed;1 issued;2 materialized;3/4 ctor
    // observed/returned;5 count observed;6/7 first error observed/value;
    // 8/9 bind;10/11 extraction;12/13 whole reset;14 member tail;15 partial;
    // 16/17 generation entered observed/value;18/19 generation returned
    // observed/value;20/21/22 validation observed/entered/returned;
    // 23 projection relation observed;24 actual whole-object ending.
    std::array<object_fact,4> objects{}; // catalog/projection/suspension/row
    std::array<std::uintptr_t,4> projection_payloads{};
    std::array<std::uint64_t,4> projection_payload_owner_counts{};
    std::uintptr_t schema_original=0;
    bool creator_worker=false,receiving_owner_bound=false;
    bool attempted=false,allocation_returned=false,allocation_exception_returned=false,first_failure=false;
    bool preparation_returned=false,returned=false,history_checked=false,history_known=false;
    bool source_failure_observed=false,source_failure=false,projection_payloads_observed=false;
    int result=-1; // -1 unknown,1 actual held ended,2 retained debt; no not-issued ending.
};
static_assert(std::is_trivially_copyable_v<ordinary_target_programme_observation>);
static_assert(std::is_standard_layout_v<ordinary_target_programme_observation>);
static_assert(sizeof(ordinary_target_programme_observation)<=512);
}
#endif
