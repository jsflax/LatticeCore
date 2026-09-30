#pragma once
#include <cstddef>
#include <cstdint>
#include <array>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <thread>

namespace lattice::detail {
enum class configured_bridge_operation {connect,send,disconnect,verify};
// Private per-attempt custody. These are count bounds, not payload-byte bounds.
// All calls and queued payloads share the same actual physical attempt record.
class configured_attempt_custody : public std::enable_shared_from_this<configured_attempt_custody> {
public:
    static constexpr size_t maximum_commands=128,maximum_payloads=4096;
    enum class kind { command,payload };
    struct observation {size_t commands=0,payloads=0;bool closed=false;int32_t first_error=0;size_t workers=0,finished_workers=0;};
    class lease {
        friend class configured_attempt_custody;
        std::shared_ptr<configured_attempt_custody> owner_;
        kind kind_=kind::command;
        lease(std::shared_ptr<configured_attempt_custody> owner,kind value) noexcept
            :owner_(std::move(owner)),kind_(value){}
    public:
        lease() noexcept=default;
        lease(lease&& other) noexcept:owner_(std::move(other.owner_)),kind_(other.kind_){}
        lease(const lease&)=delete;
        lease& operator=(const lease&)=delete;
        ~lease(){if(owner_)owner_->release(kind_);}
        explicit operator bool()const noexcept{return bool(owner_);}
    };
    // Wakeup is immutable after binding and only wakes the bounded coordinator.
    // Capture construction/destruction and invocation all occur off this leaf.
    void bind_wakeup(std::function<void()>);
    // Private actual-use rendezvous; null in ordinary operation. Never issues
    // a receipt, changes admission, or supplies any cleanup/authority result.
    void bind_foreign_probe(std::function<void(configured_bridge_operation)>);
    void before_foreign(configured_bridge_operation) const;
    // The worker payload is reserved before its captures are constructed.
    // Completed OS threads stay owned until the off-scheduler lane joins them.
    void launch_worker(lease,std::function<void()>);
    bool join_finished_workers() noexcept;
    lease admit(kind,bool terminal_payload=false);
    void close() noexcept;
    void seal_payloads() noexcept;
    void fail(int32_t) noexcept;
    observation snapshot() const noexcept;
    void wake() const noexcept;
private:
    mutable std::mutex mutex_;
    size_t commands_=0,payloads_=0;
    bool closed_=false,payloads_sealed_=false;
    int32_t first_error_=0;
    std::shared_ptr<const std::function<void()>> wakeup_;
    std::shared_ptr<const std::function<void(configured_bridge_operation)>> foreign_probe_;
#ifndef __EMSCRIPTEN__
    struct worker_slot {std::thread thread;bool reserved=false,published=false,finished=false,joining=false;};
    std::array<worker_slot,maximum_payloads> workers_;
#endif
    size_t worker_count_=0,finished_worker_count_=0;
    void release(kind) noexcept;
};
// Charge is acquired BEFORE the caller constructs/copies the retained work.
// The returned callable's copies share one payload. Its final destruction
// destroys work and all its captures before releasing the exact charge.
std::function<void()> retain_configured_payload(configured_attempt_custody::lease,std::function<void()>);
}
