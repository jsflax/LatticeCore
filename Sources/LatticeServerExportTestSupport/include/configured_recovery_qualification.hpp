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
    configured_recovery_qualification_record record(size_t)const noexcept;
};
}
