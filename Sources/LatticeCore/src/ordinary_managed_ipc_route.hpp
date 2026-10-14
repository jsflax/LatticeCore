#pragma once
#include <lattice/sync.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace lattice { class lattice_db; }
namespace lattice::detail {
class ordinary_managed_profile;
struct ordinary_managed_product_state;
struct managed_engram_policy;
class ordinary_managed_ipc_access;
class ordinary_managed_ipc_role;
class ordinary_managed_ipc_delivery;
class ordinary_export_access;

// Passive description only. Shape validation cannot issue a role or prove
// enrollment, current product membership, the predicate's origin, or a cut.
// The issuer takes these facts from the retained catalog and actual native
// configuration, before starting either endpoint. Wire fields are not input.
struct ordinary_managed_ipc_route_description final {
    std::string route_key, generation, source_member, recipient_member;
    std::int64_t policy_revision=0;
    std::string channel;
    // The closed native catalog scope, not a set inferred from payload rows.
    std::vector<std::string> models;
    // nullopt preserves the actual unfiltered configuration. Empty is an
    // explicit match-none policy; these two cases must not be conflated.
    std::optional<std::vector<sync_filter_entry>> filter;
    // false means retain recipient content when this route narrows. This
    // never changes an actual upstream deletion into a visibility removal.
    bool narrowing_emits_removals=true;

    // No allocation, SQL or callbacks. Returns a conservative logical charge
    // for copying this description, not an allocator/RSS measurement. The
    // actual issuer must reserve the charge BEFORE making its retained copy.
    std::size_t copy_charge() const;
};

// Actual same-process pair only. There is deliberately no fd, path, decoded
// descriptor or remote-profile constructor. A future controller-attested peer
// must have a distinct issuer; this type cannot stand in for that grant.
class ordinary_managed_ipc_local_route final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_role;
    friend class ordinary_managed_ipc_delivery;
    friend class ordinary_export_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;

    // Only the real issuer invokes this after independently validating BOTH
    // post-COMMIT profiles and their actual retained product route. The owner
    // pointers must use the real ownership control blocks; no alias around a
    // borrowed/stack wrapper is permitted. Constructor checks are not issuance.
    ordinary_managed_ipc_local_route(std::shared_ptr<lattice_db>&& source,
        std::shared_ptr<const ordinary_managed_profile>&& source_profile,
        std::shared_ptr<lattice_db>&& recipient,
        std::shared_ptr<const ordinary_managed_profile>&& recipient_profile,
        ordinary_managed_ipc_route_description&&,
        std::shared_ptr<const ordinary_managed_product_state>&&);
    // Creator check precedes owner access. Durable profile/source/physical/
    // schema and policy checks additionally run in each actual local SQL turn.
    void validate_lifetime() const;
    const std::shared_ptr<const ordinary_managed_product_state>& product_authority() const;
    const managed_engram_policy& product_policy() const;
    lattice_db& source_owner() const;
    lattice_db& recipient_owner() const;
    const ordinary_managed_profile& source_profile() const;
    const ordinary_managed_profile& recipient_profile() const;
    const ordinary_managed_ipc_route_description& description() const;
public:
    ~ordinary_managed_ipc_local_route();
    ordinary_managed_ipc_local_route(const ordinary_managed_ipc_local_route&)=delete;
    ordinary_managed_ipc_local_route& operator=(const ordinary_managed_ipc_local_route&)=delete;
};
}
