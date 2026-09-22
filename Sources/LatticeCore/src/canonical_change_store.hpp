#pragma once
#include <cstdint>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice {
class lattice_db;
class database;
namespace detail {

// Inert storage only: no trigger attachment, authoritative coverage, source
// lease, wire protocol, negative receipt proof, or installation API.
struct canonical_store_binding {
    std::string source, epoch, scope, schema;
    bool operator==(const canonical_store_binding&) const = default;
};
struct canonical_store_limits {
    int64_t markers, marker_bytes, receipts, receipt_bytes;
    int64_t batch_identities, identity_bytes, operation_bytes;
    bool operator==(const canonical_store_limits&) const = default;
};
struct canonical_identity {
    std::string table, global_id;
    bool operator==(const canonical_identity&) const = default;
};
enum class canonical_receipt_outcome : int64_t { applied = 1, no_op = 2, policy = 3 };
// Explicit immutable receipt profile v2. This catalog describes admitted
// provenance; it does not authenticate a caller or prove negative coverage.
struct canonical_namespace_entry {
    std::string namespace_id, coverage_id;
    int64_t revision;
    bool operator==(const canonical_namespace_entry&) const = default;
};
struct canonical_namespace_profile {
    std::string local_namespace;
    std::vector<canonical_namespace_entry> entries;
    void validate() const; // fixed v2 caps: 64 entries, 256 bytes per identity
    bool operator==(const canonical_namespace_profile&) const = default;
};
struct canonical_receipt_request {
    std::string original_id;
    canonical_receipt_outcome outcome;
    std::optional<canonical_identity> target;
    std::optional<std::string> namespace_id; // absent only in legacy receipt v1
    bool operator==(const canonical_receipt_request&) const = default;
};
struct canonical_receipt {
    canonical_receipt_request original;
    int64_t position;
    bool operator==(const canonical_receipt&) const = default;
};
struct canonical_store_state {
    int64_t head = 0, floor = 0;
    int64_t markers = 0, marker_bytes = 0, receipts = 0, receipt_bytes = 0;
    bool operator==(const canonical_store_state&) const = default;
};
struct canonical_record_result {
    int64_t position;
    bool newly_recorded;
    std::optional<canonical_receipt> receipt;
};
enum class canonical_store_error_code {
    transaction_required, invalid_argument, binding_mismatch, limits_mismatch,
    corrupt_state, capacity, sequence_exhausted, base_retired, base_ahead,
    protected_floor, cleanup_failed
};
class canonical_store_error : public std::runtime_error {
public:
    canonical_store_error_code code;
    std::exception_ptr primary_error, cleanup_error;
    canonical_store_error(canonical_store_error_code c, const std::string& message,
                          std::exception_ptr primary = {}, std::exception_ptr cleanup = {})
        : std::runtime_error(message), code(c), primary_error(primary), cleanup_error(cleanup) {}
};

class canonical_change_store {
    lattice_db& owner_;
    canonical_store_binding binding_;
    canonical_store_limits limits_;
    std::optional<canonical_namespace_profile> namespaces_;
    database& connection() const;
    void write_state(const canonical_store_state&, const canonical_store_state&);
public:
    // One explicitly bound scope per database in this first primitive. All IDs
    // are bounded opaque bytes, compared exactly; no UUID/collation migration
    // or case normalization is performed. Admission must validate wire/schema
    // identities separately before attaching this to real writer paths.
    canonical_change_store(lattice_db&, const canonical_store_binding&, canonical_store_limits,
                           const canonical_namespace_profile* = nullptr);
    // Every call checks this thread's actual owned main WRITE transaction.
    // Owner/transaction custody remains with caller. No transferable token.
    void initialize(); // full integrity audit on reopen; no import/reset
    void audit() const;
    canonical_store_state state() const; // cached counters, not a full audit
    std::optional<int64_t> touch(const canonical_identity&) const;
    // nullopt means UNKNOWN, never proof that an operation was not committed.
    std::optional<canonical_receipt> receipt(const std::string& original_id) const;
    // All head/marker/receipt/counter effects share one helper savepoint. For
    // actual model effects caller must dedup BEFORE DML, then record inside
    // the SAME owned entry transaction/savepoint, and roll that boundary back
    // on any refusal. This call cannot retroactively roll back preceding DML.
    // Existing original ID returns its first accepted outcome without touching
    // head/markers or interpreting replacement target/outcome/identity values.
    // Receipt v2 first requires the same enrolled namespace; UUID uniqueness
    // remains global across namespaces. This primitive is not an issuer.
    // No payload parameter exists: NoHistory values cannot be retained here.
    canonical_record_result record(const std::vector<canonical_identity>&,
                                  const std::optional<canonical_receipt_request>& = std::nullopt);
    void require_base(int64_t base) const; // storage range only, not an installed-base proof
    // Caller supplies its separately proved lowest protected base; this helper
    // does not own or manufacture source leases. Atomically advance floor and
    // discard markers <= floor. Receipts are NEVER removed. Full audit first.
    void advance_floor(int64_t floor, int64_t protected_base);
};
} // namespace detail
} // namespace lattice
