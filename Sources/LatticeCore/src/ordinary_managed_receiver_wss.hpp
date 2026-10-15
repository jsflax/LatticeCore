#pragma once
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <utility>
#include <cstdint>
namespace lattice {struct sync_config;class lattice_db;class swift_owned_receiver_preservation;class swift_managed_receiver_request;}
namespace lattice::detail {
class ordinary_managed_receiver_preservation;
class ordinary_managed_product_credentials;
class ordinary_managed_wss_handle;
class ordinary_managed_wss_admission;
class ordinary_managed_wss_binding;
class ordinary_export_access;
class receiver_directory_completion;
class ordinary_receiver_wss_access;
class ordinary_group_target_receiving_owner;
struct ordinary_receiver_wss_state;
// Borrowed source-private correlation facts, constructed only by the actual
// fixed directory completion. This is not an issuer for source continuity,
// accepted-cut installation, original results, ACK or debt retirement.
class ordinary_receiver_history_expectation final {
    friend class receiver_directory_completion;
    ordinary_receiver_history_expectation(std::string principal,std::optional<std::string> group,
        std::string profile,std::string endpoint,std::string member)
        :principal(std::move(principal)),group(std::move(group)),profile_identifier(std::move(profile)),
         endpoint(std::move(endpoint)),channel("wss:"+this->endpoint),member(std::move(member)){}
public:
    const std::string principal;
    const std::optional<std::string> group;
    const std::string profile_identifier,endpoint,channel,member;
    ordinary_receiver_history_expectation(const ordinary_receiver_history_expectation&)=delete;
    ordinary_receiver_history_expectation& operator=(const ordinary_receiver_history_expectation&)=delete;
};
// Authenticated receive-only role, independent of producer pair/share policy.
// Neither request selectors nor copied directory responses can construct it.
class ordinary_receiver_wss_request final {
    friend class ordinary_export_access; // Existing schema-original issuer reads the same private request state.
    friend class ordinary_receiver_wss_access;
    friend class receiver_directory_completion;
    friend class ordinary_managed_wss_admission;
    friend class ordinary_group_target_receiving_owner;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    friend class ordinary_receiver_reclamation_fixture;
#endif
    const std::shared_ptr<ordinary_receiver_wss_state> state_;
    explicit ordinary_receiver_wss_request(std::shared_ptr<ordinary_receiver_wss_state>);
public:
    ~ordinary_receiver_wss_request();
    ordinary_receiver_wss_request(const ordinary_receiver_wss_request&)=delete;
    ordinary_receiver_wss_request& operator=(const ordinary_receiver_wss_request&)=delete;
    bool current()const noexcept;
};
class ordinary_receiver_wss_access final {
    friend class ::lattice::swift_owned_receiver_preservation;
    friend class ::lattice::swift_managed_receiver_request;
    friend class receiver_directory_completion;
    friend class ordinary_managed_wss_admission;
    friend class ordinary_managed_wss_binding;
    friend class ordinary_export_access;
    friend class ordinary_group_target_receiving_owner;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    friend class ordinary_receiver_reclamation_fixture;
#endif
    static void prepare_group_on_receiving_worker(const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static void require_group_receiving_current(const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static void require_group_history_ingress(const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static bool creator_current()noexcept;
    static std::shared_ptr<ordinary_receiver_wss_request> begin(
        std::shared_ptr<const ordinary_managed_receiver_preservation>,std::shared_ptr<ordinary_managed_product_credentials>);
    static std::string requested_group(const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static std::string admission_nonce(const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static std::uint64_t cookie(const std::shared_ptr<const ordinary_receiver_wss_request>&)noexcept;
    static void cancel(const std::shared_ptr<const ordinary_receiver_wss_request>&)noexcept;
    static std::shared_ptr<ordinary_managed_wss_handle> start(
        const std::shared_ptr<const ordinary_receiver_wss_request>&,const std::shared_ptr<ordinary_managed_product_credentials>&,const std::string&);
    static std::chrono::steady_clock::time_point expires(const std::shared_ptr<const ordinary_receiver_wss_request>&)noexcept;
    static bool matches_admission(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        const std::shared_ptr<lattice_db>&,const std::shared_ptr<lattice_db>&,const ::lattice::sync_config&)noexcept;
    static void validate_admission(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        const std::shared_ptr<lattice_db>&,const std::shared_ptr<lattice_db>&,const ::lattice::sync_config&,std::chrono::steady_clock::time_point);
    static bool has_current_history_expectation(const std::shared_ptr<const ordinary_receiver_wss_request>&)noexcept;
    static const ordinary_receiver_history_expectation& expected_history_relation(
        const std::shared_ptr<const ordinary_receiver_wss_request>&);
    static bool binding_matches(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        const std::shared_ptr<ordinary_managed_product_credentials>&,const std::shared_ptr<lattice_db>&,
        const std::string&,const std::string&,const std::string&)noexcept;
public:
    // Observation only: a positive selection requires the exact child
    // association privately published by the fixed authenticated factory.
    static std::vector<std::string> models_for_actual_child(lattice_db&,const std::string&);
};
}
