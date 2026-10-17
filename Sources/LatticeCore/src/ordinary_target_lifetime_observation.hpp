#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG) || !defined(__APPLE__)
#error "Receiving lifetime observation requires the debug macOS owner qualification image"
#endif
#include <array>
#include <cstdint>
#include <type_traits>
namespace lattice::detail {
// Qualification DATA only. No method accepts this value as a grant or setter.
// No callback, exception, page, scope or shared owner is retained in this POD.
struct ordinary_target_lifetime_observation final {
    struct root_fact {
        std::uintptr_t original=0,source_original=0,previous_original=0;
        std::uint64_t page_index=0,owner_count=0,native_call_count=0;
        bool materialized=false,source_materialized=false,constructor_observed=false,source_constructor_observed=false;
        bool constructor_returned=false,source_constructor_returned=false;
        bool owner_count_observed=false,source_failure_observed=false,source_failure=false,outstanding_native_call=false;
        bool binding_attempted=false,binding_returned=false,extraction_attempted=false,extraction_returned=false;
        bool source_reset_attempted=false,source_reset_returned=false,release_attempted=false,release_returned=false;
        bool members_destroyed=false,partial_constructor_destroyed=false;
        bool native_returns_observed=false,native_returns_known=false;
        bool scope_history_observed=false,scope_published=false,scope_left=false,scope_failed=false;
        bool scope_leave_returned=false,scope_leave_joined=false,scope_detached=false;
    };
    struct family_fact {
        // result -1 means no first result bank; otherwise exact private enum
        // value: 0=not-held DATA, 1=this family's ended, 2=retained debt.
        bool attempted=false,allocation_returned=false,allocation_exception_returned=false,first_failure=false;
        bool preparation_returned=false,extraction_attempted=false,extraction_returned=false,returned=false;
        bool history_checked=false,history_known=false;
        int result=-1;
        std::array<root_fact,2> roots{};
    };
    bool creator_worker=false,receiving_owner_bound=false,inputs_ended=false,retirement_requested=false,state_first_failure=false;
    bool history_page_active=false,unexpected_predecessor=false;
    std::uint64_t history_pages_issued=0;
    family_fact decoder,phases,outer,pages;
    // Saved actual first exception only; no selector/intent-derived evidence.
    bool state_first_failure_observed=false,state_first_failure_is_page2_injection=false;
    // Appended prefix-compatible fixture DATA: catalog/header/projection/suspension.
    // Slot0 is this actual Native object; slot1 always remains unissued DATA.
    std::array<family_fact,4> static_native{};
    std::array<bool,4> static_programme_failure_observed{},static_programme_failure{};
};
static_assert(std::is_trivially_copyable_v<ordinary_target_lifetime_observation>);
static_assert(std::is_standard_layout_v<ordinary_target_lifetime_observation>);
static_assert(sizeof(ordinary_target_lifetime_observation)<=2048);
} // namespace lattice::detail
#endif
