#pragma once
#include <lattice/network.hpp>
#include <lattice/platform_retirement.hpp>
#include <cstdint>
#include <memory>

namespace lattice {
// Test-only observations of real issued receipts and actual platform transport
// callbacks. No method reports adapter cleanup on the adapter's behalf.
struct platform_retirement_fixture_facts {
    bool issued=false,requested=false,adapter_complete=false,native_complete=false;
    bool quarantined=false,closing=false,collected=false;
    bool hold_entered=false,hold_released=false,safety_release=false;
    int32_t first_error=0;
    uint64_t bridge_uses=0,callback_uses=0,opens=0,messages=0,errors=0,closes=0;
    uint64_t charged_owners=0;
};
class platform_retirement_test_driver {
    struct state;
    std::shared_ptr<state> state_;
public:
    // Reserves the real process-lived registry before Swift allocates a client.
    // Failed or unfinished tests leave actual debt in that bounded registry.
    platform_retirement_test_driver() noexcept;
    platform_retirement_receipt issued_receipt() const noexcept;
    // Consumes a nonnull native pointer on both success and refusal.
    bool install(sync_transport*) const noexcept;
    // Consumes userdata on every path; release runs outside registry locks.
    // request receives a borrowed pointer to an actual issued receipt, valid
    // only during the call. The Swift receiver copies it before returning.
    bool bind_request(void* userdata,void(*request)(void*,const void*),void(*release)(void*)) const noexcept;
    bool connect(const std::string&) const noexcept;
    bool send_text(const std::string&) const noexcept;
    bool disconnect() const noexcept;
    bool request_retirement() const noexcept;
    // Requires actual adapter success and zero fixture calls/callbacks, then
    // relinquishes the fixture's transport alias before native settlement.
    // This is not production synchronizer/pacer/command-borrow integration.
    bool collect_if_settled() const noexcept;
    platform_retirement_fixture_facts facts() const noexcept;
    bool arm_message_hold() const noexcept;
    void release_message_hold() const noexcept;
};
}
