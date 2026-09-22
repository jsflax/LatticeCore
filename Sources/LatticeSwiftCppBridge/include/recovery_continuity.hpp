#pragma once
#ifdef __cplusplus
#include <bridging.hpp>
#include <array>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>
namespace lattice::detail {
class recovery_continuous_barrier;
struct recovery_install_result;
struct recovery_continuous_quiescence;
}
namespace lattice {
class swift_lattice_ref;
// Explicit immutable storage declarations, NOT authenticated issuer input.
// All zero limits refuse. The native factory validates before copying/opening.
struct continuous_contribution {
    std::string channel,authority,source,epoch,scope,schema,profile_digest,receipt_namespace;
    std::vector<std::string> models;
    std::vector<uint8_t> incoming_grant_claim;
};
struct continuous_route { std::string sync_id,endpoint; };
using ContinuousContributions=std::vector<continuous_contribution>;
using ContinuousRoutes=std::vector<continuous_route>;
struct continuous_policy {
    ContinuousContributions contributions;
    ContinuousRoutes routes;
    int64_t scopes=0,records=0,field_bytes=0,journal_bytes=0;
    int64_t channels=0,binding_field_bytes=0,binding_bytes=0;
    int64_t profiles=0,stamps=0,producer_field_bytes=0,manifest_bytes=0,producer_bytes=0;
    int64_t owners=0,physical_routes=0,operations=0,frozen_entries=0,frozen_bytes=0;
};
class continuous_result;
// Opaque actual owner/barrier identity. No public token/proof constructor.
// Keep handles on the same owner lane; copies retain the same actual barrier.
class continuous_barrier {
    friend class continuous_result;
    std::shared_ptr<detail::recovery_continuous_barrier> value_;
public:
    continuous_barrier()=default;
    bool valid()const noexcept SWIFT_NAME(isValid()) {return static_cast<bool>(value_);}
    continuous_result finish()const noexcept;
    continuous_result cancel()const noexcept;
};
// Value diagnostics never authorize an install or manufacture verified UNSENT.
// Phase: 0 refused, 1 rolled back, 2 known committed, 3 unsettled, 4 ownership lost.
class continuous_result {
    friend class swift_lattice_ref;
    friend class continuous_barrier;
    int32_t phase_=0;
    uint8_t errors_=0;
    bool waiting_=false,frozen_=false,unexpected_=false;
    int64_t unsent_count_=0;
    continuous_barrier barrier_;
    std::array<char,769> primary_{},cleanup_{},postcommit_{},notification_{};
    void assign(const detail::recovery_install_result&)noexcept;
    void assign(const detail::recovery_continuous_quiescence&)noexcept;
    void failure(std::exception_ptr)noexcept;
public:
    continuous_result()=default;
    int32_t phase()const noexcept{return phase_;}
    bool waiting()const noexcept{return waiting_;}
    bool frozen()const noexcept{return frozen_;}
    bool unexpected_commit()const noexcept SWIFT_NAME(unexpectedCommit()){return unexpected_;}
    int64_t local_unsent_count()const noexcept SWIFT_NAME(localUnsentCount()){return unsent_count_;}
    bool has_error()const noexcept SWIFT_NAME(hasError());
    continuous_barrier barrier()const noexcept{return barrier_;}
    std::string primary_error()const noexcept SWIFT_NAME(primaryError());
    std::string cleanup_error()const noexcept SWIFT_NAME(cleanupError());
    std::string postcommit_error()const noexcept SWIFT_NAME(postcommitError());
    std::string notification_error()const noexcept SWIFT_NAME(notificationError());
};
}
#endif
