#pragma once
#include <lattice/platform_retirement.hpp>
#include <array>
#include <functional>
#include <mutex>

namespace lattice { class sync_transport; }
namespace lattice::detail {
struct configured_retirement_test_access;
struct configured_retirement_snapshot {
    bool valid=false,requested=false,adapter_complete=false,native_complete=false;
    bool owner_live=false,transport_retained=false,quarantined=false;
    int32_t first_error=0;
};
// Inactive custody primitives. No existing factory, synchronizer or retirement
// lane calls this class. The eventual configured owner must reserve BEFORE any
// factory allocation, deliver requests, settle actual native work/borrows and
// collect on its off-callback executor. This does not yet own the complete
// wrapper/pacer/child bundle or prove those future integration obligations.
class configured_retirement_registry : public std::enable_shared_from_this<configured_retirement_registry> {
    friend class lattice::platform_retirement_receipt;
    friend struct configured_retirement_test_access;
public:
    using request_handler=std::function<void(platform_retirement_receipt)>;
    class reservation {
        friend class configured_retirement_registry;
        std::shared_ptr<configured_retirement_registry> registry_;
        size_t slot_=0;
        uint64_t owner_=0;
        reservation(std::shared_ptr<configured_retirement_registry>,size_t,uint64_t) noexcept;
    public:
        reservation() noexcept=default;
        reservation(reservation&&) noexcept;
        reservation(const reservation&)=delete;
        reservation& operator=(const reservation&)=delete;
        reservation& operator=(reservation&&)=delete;
        ~reservation();
        platform_retirement_receipt begin_attempt();
    };
    static constexpr size_t maximum_owners=64;
    static constexpr int32_t request_delivery_failed=-1;
    // The production registry is process-lived, including abandoned pending
    // or quarantined custody. Exactly 64 slots; no escape allocation or worker.
    static std::shared_ptr<configured_retirement_registry> instance();
    reservation reserve();
    // Register once for the exact attempt. If retirement raced registration,
    // invoke once immediately, off leaf. The adapter request must be nonblocking
    // and retain its cleanup state until completion. No polling is required.
    bool bind_request(const platform_retirement_receipt&,request_handler);
    bool retain_transport(const platform_retirement_receipt&,std::shared_ptr<sync_transport>);
    bool request_retirement(const platform_retirement_receipt&) noexcept;
    // Internal coordinator assertion, not exposed through the SDK receipt.
    // Caller must first settle actual native callbacks, pacer and command
    // borrows. A cancel request or wrapper deletion alone cannot prove this.
    bool complete_native_cleanup(const platform_retirement_receipt&,int32_t) noexcept;
    configured_retirement_snapshot snapshot(const platform_retirement_receipt&) const noexcept;
    // Only the future off-callback collector may call this. Adapter completion
    // NEVER destroys custody. Nonzero first error permanently refuses release.
    // Also supports orphaned successful attempts, without resurrecting owner.
    bool collect_completed(const platform_retirement_receipt&) noexcept;
    size_t charged_owners() const noexcept;
private:
    struct slot {
        bool occupied=false,owner_live=false,requested=false,notified=false;
        bool notifying=false,collecting=false;
        bool adapter_complete=false,native_complete=false;
        uint64_t owner=0,attempt=0;
        int32_t first_error=0;
        std::shared_ptr<const request_handler> request;
        std::shared_ptr<sync_transport> transport;
    };
    mutable std::mutex mutex_;
    std::array<slot,maximum_owners> slots_{};
    size_t capacity_=maximum_owners;
    uint64_t next_owner_=0,next_attempt_=0;
    explicit configured_retirement_registry(size_t);
    bool current_locked(const platform_retirement_receipt&) const noexcept;
    platform_retirement_receipt begin(size_t,uint64_t);
    void abandon(size_t,uint64_t) noexcept;
    bool complete(const platform_retirement_receipt&,int32_t,bool adapter) noexcept;
    void notify(const platform_retirement_receipt&,std::shared_ptr<const request_handler>) noexcept;
};
}
