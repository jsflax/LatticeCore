#pragma once
#ifdef __cplusplus
#include <bridging.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
namespace lattice::detail {
class authenticated_relay_setup;
class authenticated_session_fence;
class authenticated_relay_operation;
}
namespace lattice {
class swift_lattice_ref;
// The stop and operation values retain no owner, SQL state, or app callback.
class relay_recovery_stop {
    friend class relay_recovery_setup;
    std::shared_ptr<detail::authenticated_session_fence> value_;
public:
    relay_recovery_stop()=default;
    void stop()const noexcept;
    bool live()const noexcept;
    bool drained()const noexcept;
};
class relay_recovery_result {
    friend class relay_recovery_setup;
    int32_t status_=0;
    std::vector<std::string> ids_;
    std::shared_ptr<detail::authenticated_relay_operation> operation_;
public:
    relay_recovery_result()=default;
    // 0 invalid,1 completed (possibly partial IDs),2 retired,4 native error.
    // A native error/partial result is never proof that effects did not commit.
    int32_t status_code()const noexcept SWIFT_NAME(statusCode());
    const std::vector<std::string>& ids()const noexcept;
    bool publishable()const noexcept;
};
// An unauthorized actual setup, not a transferable admission. Only the real
// ref creates it; a retained live-route callback is mandatory. The SDK keeps
// this native-bearing handle exclusively on the mount's file IO lane.
class relay_recovery_setup {
    friend class swift_lattice_ref;
    std::shared_ptr<detail::authenticated_relay_setup> value_;
public:
    relay_recovery_setup()=default;
    bool valid()const noexcept;
    std::string descriptor()const noexcept;
    relay_recovery_stop stop_token()const noexcept SWIFT_NAME(stopToken());
    bool finish_authorization(const std::string&)const noexcept SWIFT_NAME(finishAuthorization(_:));
    relay_recovery_result receive(const std::string&)const noexcept;
    void close_on_io()const noexcept SWIFT_NAME(closeOnIO());
};
}
#endif
