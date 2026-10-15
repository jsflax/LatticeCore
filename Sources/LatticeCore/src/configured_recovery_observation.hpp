#pragma once
#ifndef __EMSCRIPTEN__
#include <cstddef>
#include <cstdint>
#include <memory>
#include "configured_retirement.hpp"

namespace lattice::detail {
// Private passive qualification facts and opaque interval comparisons.
// No receipt, owner handle, socket, source grant or completion capability is exposed.
enum class configured_recovery_stage { transport_created, native_completed, collected, logical_closed, transport_error };
struct configured_recovery_observation {
    configured_recovery_stage stage{};
    std::uintptr_t owner=0,attempt=0; // Comparison only during this scoped run.
    configured_retirement_identity interval; // Passive, actual native issued interval.
    size_t commands=0,payloads=0,workers=0;
    bool adapter_complete=false,native_complete=false,quarantined=false;
    bool lane_complete=false,disconnect_returned=false,pacer_present=false,pacer_joined=false,callbacks_settled=false;
    bool wrapper_destroyed=false,route_unregistered=false,receipt_invalid=false;
    bool child_closed=false,scheduler_joined=false;
    int32_t first_error=0;
};
// Separate passive close samples. Each leaf is sampled independently; these
// values do not form an atomic cleanup certificate or issue any admission.
struct configured_close_observation {
    uint32_t point=0; // 1 entry, 2 physical drain returned/skipped, 3 close return.
    std::uintptr_t owner=0,attempt=0; // Private comparison only, never exported.
    configured_retirement_identity interval; // No receipt/completion capability.
    uint32_t sync=0,report_flags=0,state_flags=0,attempt_flags=0,registry_flags=0,service_flags=0;
    size_t commands=0,payloads=0,workers=0,finished_workers=0;
    uint64_t active_callbacks=0;
    size_t collector_busy=0,dispatcher_busy=0;
    int32_t custody_error=0,registry_error=0;
    uint32_t elapsed_ms=0,remaining_ms=0;
    bool time_clamped=false;
    uint32_t sampled_leaves=0; // Zero-valued unsampled fields are unknown.
};
class configured_recovery_observer {
public:
    virtual ~configured_recovery_observer()=default;
    virtual void record_close(const configured_close_observation&)noexcept {}
    virtual void record(const configured_recovery_observation&)noexcept=0;
};
namespace configured_recovery_test_hooks {
// Captured once at real logical-owner creation. Production is null. Observer
// implementations have fixed storage and never throw or return an admission.
extern thread_local std::shared_ptr<configured_recovery_observer> observation;
}
}
#endif
