#pragma once
#ifdef __cplusplus
#include <bridging.hpp>
#include <cstdint>
#include <array>
#include <exception>
#include <memory>
#include <string>
#include <vector>
namespace lattice::detail {
class authenticated_relay_setup;
class authenticated_session_fence;
class authenticated_relay_operation;
class authenticated_ready_charge;
struct authenticated_lifecycle_adoption_result;
}
namespace lattice {
class swift_lattice_ref;
class relay_ready_charge {
    friend class relay_recovery_stop;
    friend class relay_recovery_setup;
    std::shared_ptr<detail::authenticated_ready_charge> value_;
public:
    relay_ready_charge()=default;
    bool valid()const noexcept{return bool(value_);}
};
// The stop and operation values retain no owner, SQL state, or app callback.
class relay_recovery_stop {
    friend class relay_recovery_setup;
    std::shared_ptr<detail::authenticated_session_fence> value_;
public:
    relay_recovery_stop()=default;
    void stop()const noexcept;
    bool live()const noexcept;
    bool drained()const noexcept;
    relay_ready_charge reserve_ready(uint64_t bytes)const noexcept SWIFT_NAME(reserveReady(bytes:));
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
    // Transfer owned IDs across the Swift boundary without an interior pointer.
    // The result keeps its counted operation until its final copy is released.
    std::vector<std::string> take_ids()noexcept SWIFT_NAME(takeIDs());
    bool publishable()const noexcept;
};
class relay_ready_result {
    friend class relay_recovery_setup;
    int32_t status_=0;
    std::string wire_,request_id_;
    std::shared_ptr<detail::authenticated_relay_operation> operation_;
public:
    relay_ready_result()=default;
    int32_t status_code()const noexcept SWIFT_NAME(statusCode());
    const std::string& wire()const noexcept;
    std::string take_wire()noexcept SWIFT_NAME(takeWire());
    std::string take_request_id()noexcept SWIFT_NAME(takeRequestID());
    const std::string& request_id()const noexcept SWIFT_NAME(requestID());
    bool publishable()const noexcept;
};
// Copied administrative facts only; no owner, callback or receiver authority.
class relay_lifecycle_adoption_result {
    friend class swift_lattice_ref;
    int32_t phase_=0,disposition_=0;
    uint8_t errors_=0;
    bool pending_=false,unexpected_=false;
    std::array<char,769> primary_{},cleanup_{},postcommit_{},notification_{};
    std::string record_;
    std::array<char,37> transition_id_{};
    std::array<char,65> record_digest_{};
    void assign(detail::authenticated_lifecycle_adoption_result&&)noexcept;
    void failure(std::exception_ptr)noexcept;
public:
    relay_lifecycle_adoption_result()=default;
    bool pending()const noexcept{return pending_;}
    int32_t phase()const noexcept{return phase_;}
    // 0 no verified transition, 1 applied, 2 verified existing in this turn.
    int32_t disposition()const noexcept{return disposition_;}
    bool unexpected_commit()const noexcept SWIFT_NAME(unexpectedCommit()){return unexpected_;}
    bool has_error()const noexcept SWIFT_NAME(hasError()){return errors_!=0;}
    std::string primary_error()const noexcept SWIFT_NAME(primaryError());
    std::string cleanup_error()const noexcept SWIFT_NAME(cleanupError());
    std::string postcommit_error()const noexcept SWIFT_NAME(postcommitError());
    std::string notification_error()const noexcept SWIFT_NAME(notificationError());
    std::string take_record()noexcept SWIFT_NAME(takeRecord());
    std::string transition_id()const noexcept SWIFT_NAME(transitionID());
    std::string record_digest()const noexcept SWIFT_NAME(recordDigest());
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
    relay_ready_result ready(const std::string&,const relay_ready_charge&)const noexcept;
    void close_on_io()const noexcept SWIFT_NAME(closeOnIO());
};
}
#endif
