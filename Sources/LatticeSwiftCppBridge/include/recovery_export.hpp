#pragma once
#ifdef __cplusplus
#include <bridging.hpp>
#include <cstdint>
#include <cstddef>
#include <memory>

namespace lattice::detail {
class recovery_server_export_endpoint;
class recovery_server_export_page;
class recovery_server_export_stop;
class recovery_server_export_completion;
}
namespace lattice {
class swift_lattice_ref;
// Explicit mechanical-qualification budgets; zero defaults refuse. These do
// not establish global admission or a production protected-socket policy.
struct server_export_limits {
    int64_t entries=0,field_bytes=0,raw_bytes=0,wire_bytes=0;
};
// Payload-free value handles on every deployment target. They never retain
// native owner/context/frame state, even after endpoint/page close.
// Check isValid for default-empty handles. Token getters allocate nothing.
class server_export_stop {
    friend class server_export_endpoint;
    std::shared_ptr<detail::recovery_server_export_stop> value_;
public:
    server_export_stop()=default;
    bool valid()const noexcept SWIFT_NAME(isValid());
    void request_stop()const noexcept SWIFT_NAME(requestStop());
    bool stopped()const noexcept;
    bool resources_released()const noexcept SWIFT_NAME(resourcesReleased());
};
class server_export_completion {
    friend class server_export_page;
    std::shared_ptr<detail::recovery_server_export_completion> value_;
public:
    server_export_completion()=default;
    bool valid()const noexcept SWIFT_NAME(isValid());
    bool record_result(bool success)const noexcept SWIFT_NAME(recordResult(success:));
    bool completed()const noexcept;
    bool permits_advance()const noexcept SWIFT_NAME(permitsAdvance());
};
// Copies share one consumption. Keep every native-bearing endpoint/page
// handle on the caller's IO lane, including its final destruction. The SDK
// IO-owned wrapper/admission layer is deliberately not supplied here.
class server_export_page {
    friend class server_export_endpoint;
    std::shared_ptr<detail::recovery_server_export_page> value_;
    std::shared_ptr<detail::recovery_server_export_completion> completion_;
    int32_t failure_=0;
public:
    server_export_page()=default;
    // 0 invalid,1 ready,2 sampled-empty,3 unprotected,4 stopped,5 busy,
    // 6 failed,7 enqueued,8 consumed,9 refused. Failure is never empty.
    int32_t status_code()const noexcept SWIFT_NAME(statusCode());
    uint64_t serial()const noexcept;
    int64_t count()const noexcept;
    bool has_last_audit_id()const noexcept SWIFT_NAME(hasLastAuditId());
    int64_t last_audit_id()const noexcept SWIFT_NAME(lastAuditId());
    server_export_completion completion_token()const noexcept SWIFT_NAME(completionToken());
    int32_t consume()const noexcept;
    void close_on_io()const noexcept SWIFT_NAME(closeOnIO());
};
class server_export_endpoint {
    friend class swift_lattice_ref;
    std::shared_ptr<detail::recovery_server_export_endpoint> value_;
    std::shared_ptr<detail::recovery_server_export_stop> stop_;
public:
    server_export_endpoint()=default;
    bool valid()const noexcept SWIFT_NAME(isValid());
    server_export_stop stop_token()const noexcept SWIFT_NAME(stopToken());
    server_export_page prepare_history(int64_t after_audit_id,int64_t maximum_entries)const noexcept
        SWIFT_NAME(prepareHistory(afterAuditId:maximumEntries:));
    void close_on_io()const noexcept SWIFT_NAME(closeOnIO());
};
} // namespace lattice
#endif
