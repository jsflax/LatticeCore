#pragma once
#include "recovery_writer_access.hpp"
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>

namespace lattice::detail::recovery_cohort_progress_observation {
// Private passive atomic copies. Production leaves current null. Neither this
// record nor a stage grants SQL/WRITE/source/cell/COMMIT authority. No callback,
// foreign mutex, SQL, allocation, clock or wait is invoked by these helpers.
enum class stage : unsigned char { classification=0, install=1, resume=2, none=3 };
struct result_copy {
    std::atomic<uint64_t> returns{0};
    // All fields from ONE actual result are published in one atomic value.
    std::atomic<uint32_t> outcome{0};
};
struct record {
    std::atomic<uint64_t> reservations{0};
    std::array<std::atomic<uint64_t>,3> calls{},bodies{},completed{};
    std::array<result_copy,3> results{};
};
inline thread_local record* current=nullptr;
inline thread_local stage selected=stage::none;
struct scope {
    record* prior;stage previous;
    explicit scope(record& r)noexcept:prior(current),previous(selected){current=&r;selected=stage::none;}
    ~scope(){current=prior;selected=previous;}
    scope(const scope&)=delete;scope& operator=(const scope&)=delete;
};
inline void increment(std::atomic<uint64_t>& n)noexcept {
    const auto old=n.load(std::memory_order_relaxed);
    if(old!=std::numeric_limits<uint64_t>::max())n.store(old+1,std::memory_order_release);
}
inline void reserved()noexcept{if(current)increment(current->reservations);}
inline void before(stage value)noexcept {
    if(current){selected=value;increment(current->calls[static_cast<unsigned>(value)]);}
}
inline void body_entered()noexcept {
    if(current&&selected!=stage::none)increment(current->bodies[static_cast<unsigned>(selected)]);
}
// Called after the actual install helper returns, off its Native/store scopes,
// before existing known() classification. Only presence of exceptions is copied.
inline void returned(const recovery_install_result& actual)noexcept {
    if(!current||selected==stage::none)return;
    auto& out=current->results[static_cast<unsigned>(selected)];
    static_assert(static_cast<unsigned>(recovery_install_state::ownership_lost)<256);
    static_assert(static_cast<unsigned>(recovery_install_deferred::schema_registry)<256);
    const uint32_t flags=(actual.primary_error?1u:0u)|(actual.cleanup_error?2u:0u)|
        (actual.postcommit_error?4u:0u)|(actual.notification_error?8u:0u)|
        (actual.unexpected_commit_observed?16u:0u);
    const uint32_t packed=0x80000000u|static_cast<unsigned>(actual.state)|
        (static_cast<unsigned>(actual.deferred)<<8)|(flags<<16);
    out.outcome.store(packed,std::memory_order_release);increment(out.returns);
}
inline void completed()noexcept {
    if(current&&selected!=stage::none)increment(current->completed[static_cast<unsigned>(selected)]);
    selected=stage::none;
}
} // namespace lattice::detail::recovery_cohort_progress_observation
