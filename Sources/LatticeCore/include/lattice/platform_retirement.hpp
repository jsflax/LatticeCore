#pragma once
#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
#include <memory>

namespace lattice::detail { class configured_retirement_registry; }
namespace lattice {
// Internal SDK boundary, inactive until a configured owner supplies it. This
// is resource-lifetime provenance, never source/TLS/recovery authority. Copies
// retain only a fixed registry and exact slot/attempt, not a native owner,
// transport, socket or synchronizer. A default value is always invalid.
class platform_retirement_receipt {
    friend class detail::configured_retirement_registry;
    std::shared_ptr<detail::configured_retirement_registry> registry_;
    size_t slot_=0;
    uint64_t owner_=0,attempt_=0;
    platform_retirement_receipt(std::shared_ptr<detail::configured_retirement_registry>,size_t,uint64_t,uint64_t) noexcept;
public:
    platform_retirement_receipt() noexcept=default;
    bool valid() const noexcept;
    bool matches(const platform_retirement_receipt&) const noexcept;
    bool retirement_requested() const noexcept;
    // Report only after actual adapter cleanup AND its admitted callback uses
    // have settled. 0 means successful cleanup; any nonzero code is the first
    // cleanup failure and keeps the native resource charge quarantined.
    // Returns whether this exact completion was accepted, NOT cleanup success.
    // Before native request it returns false without consuming completion;
    // duplicate, stale and invalid signals also return false. Never starts a
    // replacement, releases custody or invokes a native/application callback.
    bool complete_adapter_cleanup(int32_t error_code) const noexcept;
};
}
#endif
