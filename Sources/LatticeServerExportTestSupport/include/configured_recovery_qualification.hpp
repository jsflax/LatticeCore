#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>

namespace lattice {
struct configured_recovery_qualification_record {
    // 1 transport-created, 2 native-completed, 3 collected, 4 logical-closed,
    // 5 actual native error callback (no error text or source data retained).
    uint32_t stage=0;
    size_t owner=0,attempt=0;
    size_t commands=0,payloads=0,workers=0;
    bool adapter_complete=false,native_complete=false,quarantined=false;
    bool lane_complete=false,disconnect_returned=false,pacer_present=false,pacer_joined=false,callbacks_settled=false;
    bool wrapper_destroyed=false,route_unregistered=false,receipt_invalid=false;
    bool child_closed=false,scheduler_joined=false;
    int32_t first_error=0;
};
// Separate first-close recorder: fixed 24 samples (three points, eight owners).
// Old native cleanup trace/stages and its invalid/overflow flags are unchanged.
struct configured_close_qualification_record {
    uint32_t point=0,sync=0,report_flags=0,state_flags=0,attempt_flags=0,registry_flags=0,service_flags=0;
    size_t owner=0,attempt=0,commands=0,payloads=0,workers=0,finished_workers=0;
    uint64_t active_callbacks=0;
    size_t collector_busy=0,dispatcher_busy=0;
    int32_t custody_error=0,registry_error=0;
    uint32_t elapsed_ms=0,remaining_ms=0;
    bool time_clamped=false;
    uint32_t sampled_leaves=0; // Zero-valued unsampled fields are unknown.
};
// This trace never constructs a transport or completes native/adapter cleanup.
// run() synchronously scopes the existing private successor test permission
// around the caller's ordinary public Lattice initializer. Copies share one
// fixed 64-event trace. No local server/runtime is started by this support API.
class configured_recovery_qualification {
    struct implementation;
    std::shared_ptr<implementation> impl_;
public:
    configured_recovery_qualification();
    void run(void* borrowed_context,void (*borrowed_body)(void*))const;
    size_t count()const noexcept;
    bool overflowed()const noexcept;
    bool invalid()const noexcept;
    size_t close_count()const noexcept;
    bool close_overflowed()const noexcept;
    bool close_invalid()const noexcept;
    bool close_unavailable()const noexcept;
    configured_close_qualification_record close_record(size_t)const noexcept;
    configured_recovery_qualification_record record(size_t)const noexcept;
};
}
