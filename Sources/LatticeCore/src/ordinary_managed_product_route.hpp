#pragma once
#include "ordinary_managed_profile_pair.hpp"
#include "ordinary_managed_ipc_route.hpp"
#include "ordinary_managed_ipc_role.hpp"
#include <atomic>
#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace lattice {
class swift_lattice_ref;
struct swift_configuration;
class swift_managed_product_credentials;
class swift_managed_product_request;
class swift_managed_product_route;
}
namespace lattice::detail {
class managed_directory_completion;
struct ordinary_managed_product_state;
class ordinary_managed_wss_binding;
class ordinary_managed_wss_receipt;
class configured_recovery_connection;
class ordinary_group_target_receiving_owner;
class ordinary_managed_wss_admission;

// A selected direction is not authority. Only the closed authenticated
// response + actual enrolled member pair + same-view policy can issue it.
enum class managed_engram_direction : std::uint8_t {
    hub_to_personal=1, personal_to_hub=2, hub_to_group=3, group_to_hub=4
};
struct managed_engram_policy final {
    ordinary_managed_ipc_route_description route;
    std::string source_schema,recipient_schema,policy_seal,principal,group;
    // Actual declaration-derived root AND generated logical-table closure.
    // No underscore/prefix heuristic chooses generated ownership.
    std::vector<std::string> source_tables,recipient_tables;
    bool full_source_partition=false;
    // Always false in this route issuer. A filter miss never grants explicit
    // author privacy retraction or deletion of another lineage's contribution.
    bool grants_privacy_intent=false;
};

class ordinary_managed_product_credentials final {
    friend class ::lattice::swift_managed_product_credentials;
    friend struct ::lattice::swift_configuration;
    friend class ordinary_managed_ipc_access;
    friend class managed_directory_completion;
    friend class ordinary_managed_wss_binding;
    friend class ordinary_managed_wss_admission;
    friend class ordinary_receiver_wss_access;
    friend class receiver_directory_completion;
    friend struct ordinary_receiver_wss_state;
    friend struct ordinary_managed_wss_epoch;
    friend bool managed_wss_configuration_matches(const std::shared_ptr<ordinary_managed_product_credentials>&,const std::string&,const std::string&)noexcept;
    friend struct ordinary_managed_product_state;
    const std::uint64_t creator_;
    const std::string endpoint_,token_digest_;
    mutable std::mutex wss_leaf_;
    std::array<std::weak_ptr<ordinary_managed_wss_binding>,64> wss_;
    std::array<std::weak_ptr<configured_recovery_connection>,64> wss_owners_;
    std::array<std::weak_ptr<ordinary_group_target_receiving_owner>,64> group_receiving_owners_;
    std::shared_ptr<const ordinary_managed_wss_receipt> receipt(
        const ordinary_managed_profile&,const std::string&,const std::string&)const;
    std::shared_ptr<const ordinary_managed_wss_receipt> try_receipt(
        const ordinary_managed_profile&,const std::string&,const std::string&)const;
    std::atomic<bool> live_{true};
    std::mutex principal_leaf_;
    std::string principal_;
    bool bind_principal(const std::string&);
    bool matches_configuration(const std::string&,const std::string&)const noexcept;
    explicit ordinary_managed_product_credentials(std::string,std::string);
    bool current() const noexcept;
    void revoke() noexcept;
};

bool managed_wss_configuration_matches(const std::shared_ptr<ordinary_managed_product_credentials>&,const std::string&,const std::string&)noexcept;
std::shared_ptr<const ordinary_managed_wss_receipt> managed_product_wss_receipt(
    const std::shared_ptr<const ordinary_managed_product_state>&,
    const ordinary_managed_profile&,const std::string&);

std::shared_ptr<const ordinary_managed_wss_receipt> try_managed_product_wss_receipt(
    const std::shared_ptr<const ordinary_managed_product_state>&,const ordinary_managed_profile&,const std::string&);

// Called only by retained route lifetime checks (no SQL or foreign callback).
// Actual policy/schema/physical validation is separate inside the owned turn.
void require_managed_product_live(const std::shared_ptr<const ordinary_managed_product_state>&);
const managed_engram_policy& managed_product_policy(const std::shared_ptr<const ordinary_managed_product_state>&);
}
