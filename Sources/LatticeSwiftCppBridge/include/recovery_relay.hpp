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
// Opt-in copied diagnostics. Stage families: 0 preparation (10 points),
// 1 read (6), 2 authenticated control (8), 3 resume (7). Point order matches
// the private observation enums. Zero visits means unobserved, not success.
// Each family has its own steady-clock origin; these are relative elapsed
// measurements, not cross-clock absolute timestamps.
// Cost families 0/1 are preparation/read and have 11 inclusive phases; their
// intervals can overlap and must not be summed as disjoint CPU time.
class relay_ready_diagnostics {
    friend class relay_recovery_setup;
    std::array<std::array<uint64_t,10>,4> visits_{},first_us_{},last_us_{};
    std::array<std::array<uint64_t,11>,2> cost_calls_{},cost_us_{};
    std::array<std::array<uint64_t,5>,2> cost_counters_{};
    std::array<int32_t,5> settlement_{{-1,-1,-1,-1,-1}},errors_{},refusal_{};
    std::array<bool,5> unexpected_{};
    std::array<char,37> request_id_{};
public:
    int32_t operation=0,bridge_status=0;
    // Authenticated epoch-ms and retention-session-relative-ms are distinct.
    int64_t authenticated_clock_ms=-1,authenticated_deadline_ms=-1,duration_ms=-1;
    int64_t resume_expiration_ms=-1,resume_expiry_clock_ms=-1,resume_new_deadline_ms=-1;
    int64_t read_deadline_ms=-1,read_clock_before_ms=-1,read_clock_after_ms=-1,read_clock_settled_ms=-1;
    uint64_t prepare_audited_frames=0,read_index=0,read_full_audits=0,read_audited_frames=0,read_audited_bytes=0;
    uint64_t read_positive_receipt_lookups=0,read_addressed_frames=0,read_addressed_bytes=0;
    bool capture_error=false,requires_full_request=false,lease_available=false;
    bool resume_expiration_present=false,resume_expiry_clock_observed=false,resume_transfer_available=false,resume_lease_available=false;
    // Returns an owned bounded UUID or empty. Allocation failure affects only
    // this optional diagnostic value, never the original native result/error.
    std::string request_id()const noexcept SWIFT_NAME(requestID());
    uint64_t stage_visits(uint32_t family,uint32_t point)const noexcept SWIFT_NAME(stageVisits(family:point:));
    uint64_t stage_first_us(uint32_t family,uint32_t point)const noexcept SWIFT_NAME(stageFirstMicroseconds(family:point:));
    uint64_t stage_last_us(uint32_t family,uint32_t point)const noexcept SWIFT_NAME(stageLastMicroseconds(family:point:));
    uint64_t cost_calls(uint32_t family,uint32_t phase)const noexcept SWIFT_NAME(costCalls(family:phase:));
    uint64_t cost_us(uint32_t family,uint32_t phase)const noexcept SWIFT_NAME(costMicroseconds(family:phase:));
    // Counters: receipt batches, receipt IDs, hash input bytes, staged bytes,
    // direct hash blocks. Invalid indexes return zero; no storage is exposed.
    uint64_t cost_counter(uint32_t family,uint32_t index)const noexcept SWIFT_NAME(costCounter(family:index:));
    // Settlements: expiration, preparation, publication, resume, read.
    // State -1 means no copied result. A copied refused/default result does not
    // prove that its owned phase ran: consult the corresponding stage visits.
    // Refusal 0 unobserved, 1 no primary error,
    // 2 expiry, 3 incomplete, 4 changed request/coverage, 5 lease sequence,
    // 6 postimage, 7 other db_error, 8 other exception. Read refusal is not
    // classified by the existing read slot and therefore remains unobserved.
    int32_t settlement_state(uint32_t index)const noexcept SWIFT_NAME(settlementState(_:));
    int32_t settlement_errors(uint32_t index)const noexcept SWIFT_NAME(settlementErrors(_:));
    int32_t settlement_refusal(uint32_t index)const noexcept SWIFT_NAME(settlementRefusal(_:));
    // The reused read slot has no unexpected-commit observation.
    bool settlement_unexpected_commit_observed(uint32_t index)const noexcept SWIFT_NAME(settlementUnexpectedCommitObserved(_:));
    bool settlement_unexpected_commit(uint32_t index)const noexcept SWIFT_NAME(settlementUnexpectedCommit(_:));
};
static_assert(sizeof(relay_ready_diagnostics)<=2048,"READY copied scalar diagnostic bound");
class relay_observed_ready_result {
    friend class relay_recovery_setup;
    relay_ready_result result_;
    relay_ready_diagnostics diagnostics_;
public:
    relay_observed_ready_result()=default;
    relay_ready_result take_result()noexcept SWIFT_NAME(takeResult());
    relay_ready_diagnostics diagnostics()const noexcept{return diagnostics_;}
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
    bool pending_before_enrollment_=false;
public:
    relay_recovery_setup()=default;
    bool valid()const noexcept;
    // Finite no-effect result for the separate automatic entrypoint only.
    // False on every ordinary call and every post-boundary failure.
    bool pending_before_enrollment()const noexcept SWIFT_NAME(pendingBeforeEnrollment()){return pending_before_enrollment_;}
    std::string descriptor()const noexcept;
    relay_recovery_stop stop_token()const noexcept SWIFT_NAME(stopToken());
    bool finish_authorization(const std::string&)const noexcept SWIFT_NAME(finishAuthorization(_:));
    relay_recovery_result receive(const std::string&)const noexcept;
    relay_ready_result ready(const std::string&,const relay_ready_charge&)const noexcept;
    // Separate observed entrypoint: invokes ordinary ready exactly once.
    relay_observed_ready_result ready_observed(const std::string&,const relay_ready_charge&)const noexcept SWIFT_NAME(readyObserved(_:charge:));
    void close_on_io()const noexcept SWIFT_NAME(closeOnIO());
};
}
#endif
