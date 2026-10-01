#pragma once
#include <lattice/configured_platform.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace lattice {
// Actual native registry/command/transport custody for SDK typed-path tests.
// No fixture method supplies an adapter completion or a native success flag.
struct configured_platform_fixture_facts {
    bool issued=false,requested=false,adapter_complete=false,native_complete=false;
    bool quarantined=false,closing=false,collected=false;
    bool construction_entered=false,construction_returned=false,transport_returned=false;
    bool construction_admitted=false,construction_hold_entered=false,construction_hold_released=false;
    bool pre_pointer_entered=false,pre_pointer_released=false,safety_release=false;
    int32_t first_error=0,fixture_error=0;
    uint64_t calls=0,callbacks=0,native_commands=0,native_payloads=0,native_workers=0;
    uint64_t opens=0,errors=0,charged_owners=0;
};
class configured_platform_test_driver {
    struct state;
    std::shared_ptr<state> state_;
public:
    configured_platform_test_driver() noexcept;
    platform_retirement_receipt issued_receipt() const noexcept;
    // Borrows callback context only for this synchronous call. One real native
    // construction command is held before entry until returned owners settle.
    bool construct(void*,create_configured_transport_fn_ptr) const noexcept;
    bool arm_construction_hold() const noexcept;
    void release_construction_hold() const noexcept;
    bool connect(const std::string&) const noexcept;
    // Works before construction/registration. Missing binding remains charged.
    bool request_retirement() const noexcept;
    bool collect_if_settled() const noexcept;
    configured_platform_fixture_facts facts() const noexcept;
    bool arm_pre_pointer_hold() const noexcept;
    void release_pre_pointer_hold() const noexcept;
};

// Dedicated helper process ONLY: install a real global factory whose actual
// displacement releases the supplied context. Consumes the retain on all paths.
bool install_configured_factory_reentry_probe(void*,void(*)(void*)) noexcept;

struct configured_factory_child_result {
    bool launched=false,normal_exit=false,reaped=false,timed_out=false;
    bool custody_lost=false,cleanup_unproved=false,output_overflow=false,io_error=false;
    bool child_slot_unavailable=false;
    int32_t wait_status=0;
    int64_t child_pid=0;
    std::string output;
};
// Exact standalone helper, never a test-runner/filter relaunch. Positive wait
// is 10s; timeout remains failure through a separate bounded 10s cleanup.
configured_factory_child_result run_configured_factory_child(
    const std::string& executable,const std::string& nonce) noexcept;
}
