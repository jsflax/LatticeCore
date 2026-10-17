#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG) || !defined(__APPLE__)
#error "Scope root observations require the genuine debug macOS owner image"
#endif
#include <array>
#include <cstdint>
#include <type_traits>
namespace lattice::detail {
struct ordinary_target_scope_root_observation final {
    struct root_fact {std::uintptr_t original=0;std::uint64_t flags=0,aliases=0;};
    // Root bits0original,1/2ctor observed/returned,3count observed,
    // 4/5bind,6/7extraction,8/9reset,10real tail,11partial ctor.
    std::array<root_fact,10> roots{}; // 3wrappers,4scope states,3call banks.
    std::array<std::uint64_t,6> sources{}; // Actual six source cells attempted/returned.
    std::array<std::uint64_t,4> ledger_sources{};
    std::uint64_t flags=0,peak_bytes=0,scope_count=0,ledger_count=0,resource_count=0,environment_count=0;
    // Overall bits0creator,1owner,2attempt,3allocation return,4allocation error,
    // 5first error,6prepared,7returned,8/9first observation,10first known,
    // 11history observed,12history known. Resource/env counts are pending DATA.
};
static_assert(std::is_trivially_copyable_v<ordinary_target_scope_root_observation>);
static_assert(std::is_standard_layout_v<ordinary_target_scope_root_observation>);
static_assert(sizeof(ordinary_target_scope_root_observation)<=512);
}
#endif
