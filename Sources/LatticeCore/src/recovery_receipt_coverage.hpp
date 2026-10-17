#pragma once
#include "lattice/sync.hpp"
#include <optional>

namespace lattice::detail {
// Durable application registration, never inferred from a user, route, file
// name or peer declaration. The real setup consumes the app-authenticated map.
struct recovery_producer_registration {
    std::string registration_id, incarnation;
    void validate() const;
    bool operator==(const recovery_producer_registration&) const = default;
};
struct canonical_coverage_profile {
    std::string cohort_id;
    int64_t revision = 0;
    std::vector<std::string> namespaces;
    // 65536 admitted global origins * all 64 namespace members. Receipts and
    // coverage cells have separate counters; neither evicts the other.
    int64_t maximum_origins = 65536;
    int64_t maximum_origin_bytes = 67108864;
    int64_t maximum_cells = 4194304;
    int64_t maximum_cell_bytes = 1610612736;
    void validate() const;
    bool operator==(const canonical_coverage_profile&) const = default;
};
struct recovery_receipt_binding {
    recovery_producer_registration producer;
    std::string cohort_id;
    int64_t cohort_revision = 0;
    int64_t operation_codec = 1;
    void validate() const;
    bool operator==(const recovery_receipt_binding&) const = default;
};
// Generated originals and received projections share this typed encoding.
// Only changed UPDATE NoHistory values become late-bound markers. Ordinary
// changed values, INSERT/DELETE values, identity and timestamp remain exact.
audit_original_identity make_original_identity(const audit_log_entry&,
    const std::unordered_map<std::string,column_type>&,
    const std::set<std::string>& no_history, const std::string& schema_digest,
    const recovery_producer_registration&,
    const std::vector<std::string>* original_names = nullptr);
void verify_original_identity(const audit_log_entry&,
    const std::unordered_map<std::string,column_type>&,
    const std::set<std::string>& no_history, const std::string& schema_digest,
    const recovery_producer_registration&);
size_t original_identity_bytes(const audit_original_identity&);
}
