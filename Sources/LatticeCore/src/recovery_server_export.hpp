#pragma once
#include "recovery_export_adapter.hpp"

namespace lattice::detail {
// Inactive mechanical qualification only. No source/scope or all-route issuer.
enum class server_export_status : int32_t {
    invalid=0, ready=1, empty=2, unprotected=3, stopped=4, busy=5,
    failed=6, enqueued=7, consumed=8, refused=9
};
struct recovery_server_control;
struct recovery_server_delivery;
struct recovery_server_endpoint_state;
struct recovery_server_page_state;
class recovery_server_export_endpoint;
class recovery_server_export_page;

// These two handles ONLY retain payload-free state. Holding them after IO
// close cannot retain an endpoint, frame, owner, callback context or exception.
class recovery_server_export_stop {
    friend class recovery_server_export_endpoint;
    std::shared_ptr<recovery_server_control> control_;
    explicit recovery_server_export_stop(std::shared_ptr<recovery_server_control>);
public:
    recovery_server_export_stop()=default;
    bool valid()const noexcept {return bool(control_);}
    void request_stop()const noexcept;
    bool stopped()const noexcept;
    bool resources_released()const noexcept;
};
class recovery_server_export_completion {
    friend class recovery_server_export_page;
    std::shared_ptr<recovery_server_delivery> delivery_;
    explicit recovery_server_export_completion(std::shared_ptr<recovery_server_delivery>);
public:
    recovery_server_export_completion()=default;
    bool valid()const noexcept {return bool(delivery_);}
    // Only accepted after the exact page admitted its immutable callback.
    // First result wins. Inline completion cannot release an active turn.
    bool record_result(bool success)const noexcept;
    bool completed()const noexcept;
    bool permits_advance()const noexcept;
};
class recovery_server_export_page {
    friend class recovery_server_export_endpoint;
    std::shared_ptr<recovery_server_page_state> state_;
    server_export_status fallback_=server_export_status::invalid;
    std::exception_ptr fallback_error_; // immutable after publication; native-bearing IO custody
    explicit recovery_server_export_page(server_export_status,std::exception_ptr = {});
    explicit recovery_server_export_page(std::shared_ptr<recovery_server_page_state>);
public:
    recovery_server_export_page()=default;
    server_export_status status()const noexcept;
    uint64_t serial()const noexcept;
    int64_t count()const noexcept;
    std::optional<int64_t> last_audit_id()const noexcept;
    recovery_server_export_completion completion()const noexcept;
    // No endpoint/generation/sink argument; copies share one consumption.
    server_export_status consume()const noexcept;
    // Native-bearing handles must be closed/destroyed on caller's IO lane.
    // Stop/completion handles have no such release restriction.
    void close_on_io()const noexcept;
    std::exception_ptr failure()const noexcept; // private Core diagnostics only
};
class recovery_server_export_endpoint {
    std::shared_ptr<recovery_server_endpoint_state> state_;
    explicit recovery_server_export_endpoint(std::shared_ptr<recovery_server_endpoint_state>);
public:
    using enqueue_fn=int32_t(*)(void*,const uint8_t*,size_t,uint64_t);
    using destroy_fn=void(*)(void*);
    recovery_server_export_endpoint()=default;
    // Ownership transfer REQUIRES a valid nonthrowing destroy callback. If
    // destroy is null, refuse before transfer; caller retains context. With
    // that precondition satisfied, consumes context on EVERY outcome.
    // Callback returns 1 if enqueued, 0 only
    // if definitely not enqueued. Throw/other return is possibly-enqueued:
    // retain credit until explicit promise/cancellation settlement.
    static recovery_server_export_endpoint create_for_qualification(
        std::shared_ptr<lattice_db>,void*,enqueue_fn,destroy_fn,const recovery_export_limits&);
    bool valid()const noexcept;
    recovery_server_export_stop stop_token()const noexcept;
    recovery_server_export_page prepare_history(int64_t after,size_t count)const noexcept;
    void close_on_io()const noexcept;
};
} // namespace lattice::detail
