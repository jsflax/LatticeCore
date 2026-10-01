#pragma once
#ifndef __EMSCRIPTEN__
#include <cstddef>
#include <cstdint>
#include <memory>

namespace lattice::detail {
// Private passive qualification facts. No receipt, owner, socket, source
// grant, callback or completion authority is exposed to the observer.
enum class configured_recovery_stage { transport_created, native_completed, collected, logical_closed, transport_error };
struct configured_recovery_observation {
    configured_recovery_stage stage{};
    std::uintptr_t owner=0,attempt=0; // Comparison only during this scoped run.
    size_t commands=0,payloads=0,workers=0;
    bool adapter_complete=false,native_complete=false,quarantined=false;
    bool lane_complete=false,disconnect_returned=false,pacer_present=false,pacer_joined=false,callbacks_settled=false;
    bool wrapper_destroyed=false,route_unregistered=false,receipt_invalid=false;
    bool child_closed=false,scheduler_joined=false;
    int32_t first_error=0;
};
class configured_recovery_observer {
public:
    virtual ~configured_recovery_observer()=default;
    virtual void record(const configured_recovery_observation&)noexcept=0;
};
namespace configured_recovery_test_hooks {
// Captured once at real logical-owner creation. Production is null. Observer
// implementations have fixed storage and never throw or return an admission.
extern thread_local std::shared_ptr<configured_recovery_observer> observation;
}
}
#endif
