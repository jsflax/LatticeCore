#pragma once
#include "ordinary_managed_history.hpp"
#include <cstdint>
#include <memory>
#include <string>

namespace lattice { struct physical_store_identity; }
namespace lattice::detail {
// Retention of two actual enrolled local owners in one live admitted child.
// This is not product-principal, membership, filter, partition or IPC authority.
// The closed product issuer must independently possess those facts and reserve
// its real role/endpoint capacity before retaining this pair.
class ordinary_managed_profile_pair final {
    friend class ordinary_export_access;
    friend class ordinary_managed_ipc_access;
    struct implementation;
    std::uint64_t creator_ = 0;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_profile_pair(std::uint64_t, std::unique_ptr<implementation>);
    void require_creator() const;
    // Borrowed only while this exact pair remains retained. Every accessor
    // checks actual creator process before touching owner/profile state.
    const std::shared_ptr<lattice_db>& source_owner() const;
    const std::shared_ptr<lattice_db>& recipient_owner() const;
    const std::shared_ptr<const ordinary_managed_profile>& source_profile() const;
    const std::shared_ptr<const ordinary_managed_profile>& recipient_profile() const;
    const std::string& source_member() const;
    const std::string& recipient_member() const;
    const std::string& generation() const;
public:
    ~ordinary_managed_profile_pair();
    ordinary_managed_profile_pair(ordinary_managed_profile_pair&&) noexcept;
    ordinary_managed_profile_pair& operator=(ordinary_managed_profile_pair&&) noexcept;
    ordinary_managed_profile_pair(const ordinary_managed_profile_pair&) = delete;
    ordinary_managed_profile_pair& operator=(const ordinary_managed_profile_pair&) = delete;
    // Re-enter each actual owner's owned WRITE separately and preserve the
    // original absolute deadline. No foreign-owner SQL nests under a leaf.
    // Retention alone never substitutes for this fresh operation validation.
    void validate(ordinary_launch::deadline) const;
};
}
