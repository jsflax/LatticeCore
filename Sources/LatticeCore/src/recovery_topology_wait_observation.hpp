#pragma once
#include <cstddef>
#include <atomic>
#include <cstdint>
#include <type_traits>

namespace lattice::detail { std::uint64_t database_native_process() noexcept; }
namespace lattice::detail::recovery_topology_wait_observation {
// Private passive DATA only. Production leaves this thread-local pointer null.
// The calling test owns the record on this same thread through query ending;
// neither this record nor its raw identity copies retain/grant any authority.
// Only the last actual selected wait is retained; selected_waits-1 are omitted.
struct record {
    std::uint64_t calls,selected_waits;
    std::uintptr_t wrapper,original;
    std::size_t slot;
    std::uint64_t token,sleeps;
    std::int64_t selection_now,deadline,deadline_exit;
    bool selected,excluded,deadline_observed,returned;
};
static_assert(std::is_trivial_v<record> && std::is_standard_layout_v<record>);
static_assert(std::atomic<bool>::is_always_lock_free);
inline thread_local record* current=nullptr;
inline thread_local std::atomic<bool>* selected_signal=nullptr;
inline thread_local std::uint64_t signal_creator=0;
struct scope {
    record* previous;
    std::atomic<bool>* previous_signal;
    std::uint64_t previous_creator;
    explicit scope(record& actual,std::atomic<bool>* signal=nullptr)noexcept
        :previous(current),previous_signal(selected_signal),previous_creator(signal_creator){
        current=&actual;selected_signal=signal;signal_creator=database_native_process();}
    ~scope()noexcept{current=previous;selected_signal=previous_signal;signal_creator=previous_creator;}
    scope(const scope&)=delete;scope& operator=(const scope&)=delete;
};
}
