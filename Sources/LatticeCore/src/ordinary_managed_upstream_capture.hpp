#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace lattice::detail {
class ordinary_export_access;
class ordinary_managed_ipc_delivery;
// Exact evidence from a retained accepted mapping. This is not a current-row
// projection, route/filter decision, source issuance, privacy/authoring grant,
// materialization receipt or ACK. Decoded bytes cannot construct this type.
class ordinary_managed_upstream_capture final {
    friend class ordinary_export_access;
    friend class ordinary_managed_ipc_delivery;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_upstream_capture(std::unique_ptr<implementation>);
public:
    ~ordinary_managed_upstream_capture();
    ordinary_managed_upstream_capture(ordinary_managed_upstream_capture&&) noexcept;
    ordinary_managed_upstream_capture& operator=(ordinary_managed_upstream_capture&&) noexcept;
    ordinary_managed_upstream_capture(const ordinary_managed_upstream_capture&)=delete;
    ordinary_managed_upstream_capture& operator=(const ordinary_managed_upstream_capture&)=delete;
    // Borrowed immutable observations; copies cannot convey live authority.
    const std::vector<std::uint8_t>& envelope() const;
    const std::vector<std::uint8_t>& descriptor() const;
    const std::vector<std::string>& original_fields() const;
    const std::vector<std::string>& original_names() const;
    const std::string& source_id() const;
    const std::string& scope() const;
    const std::string& schema() const;
    const std::string& record_seal() const;
    std::int64_t accepted_cut() const;
    std::int64_t current_frontier_cut() const;
    std::int64_t local_audit_id() const;
    bool historical_receipt_observed() const;
};
}
