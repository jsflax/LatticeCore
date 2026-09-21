#pragma once
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice {
class lattice_db;
class database;
namespace detail {

// Inactive receiver bookkeeping, not authentication or an automatic recovery
// controller. All byte strings are opaque, length-exact identifiers/digests.
struct receive_install_limits {
    int64_t channels;
    int64_t field_bytes;
    int64_t encoded_bytes; // bindings + active/last encoded identities
    bool operator==(const receive_install_limits&) const = default;
};
enum class receive_install_error_code {
    transaction_required, invalid_argument, corrupt_state, limits_mismatch,
    capacity, binding_mismatch, alias, stale, active_conflict, supersession_required,
    sequence_exhausted, cleanup_failed
};
class receive_install_error : public std::runtime_error {
public:
    receive_install_error_code code;
    std::exception_ptr primary_error, cleanup_error;
    receive_install_error(receive_install_error_code c, const std::string& message,
                          std::exception_ptr primary = {}, std::exception_ptr cleanup = {})
        : std::runtime_error(message), code(c), primary_error(primary), cleanup_error(cleanup) {}
};
enum class receive_frontier_kind : int64_t { uninitialized = 0, beginning_null = 1, position = 2 };
struct receive_install_frontier {
    receive_frontier_kind kind = receive_frontier_kind::uninitialized;
    std::optional<int64_t> position;
    bool operator==(const receive_install_frontier&) const = default;
};
// Stable ownership is authority+scope. This first slice conservatively refuses
// replacement of source/epoch/schema; a future explicitly authorized full-state
// replacement may change those while CASing the complete prior installation.
struct receive_install_binding {
    std::string channel, authority, source, epoch, scope, schema;
    bool operator==(const receive_install_binding&) const = default;
};
enum class receive_install_mode : int64_t { full = 0, delta = 1 };
struct receive_install_identity {
    // The attempt key is (immutable binding, sequence), not a reusable wire UUID.
    // Exactly next sequence is admitted. Only committed sequences are nonreused.
    int64_t sequence = 0;
    int64_t expected_revision = 0;
    receive_install_frontier base;
    int64_t head = 0; // explicit numeric H; 0 is an empty source, never NULL
    receive_install_mode mode = receive_install_mode::full;
    // M is retained separately: equal request/receipt/content does not imply
    // an equal manifest (page partition/count and other framing may differ).
    std::string request_digest, receipt_digest, content_digest, manifest_digest;
    bool operator==(const receive_install_identity&) const = default;
};
struct receive_install_snapshot {
    receive_install_binding binding;
    receive_install_frontier frontier;
    int64_t revision = 0, last_sequence = 0;
    std::optional<receive_install_identity> active, last_installed;
    bool operator==(const receive_install_snapshot&) const = default;
};
enum class receive_install_disposition { active, already_installed, installed };
struct receive_install_receipt {
    receive_install_disposition disposition;
    int64_t revision;
    int64_t head;
};
struct receive_install_usage {
    int64_t channels = 0, encoded_bytes = 0;
    bool operator==(const receive_install_usage&) const = default;
};

class receive_install_store {
    friend class recovery_obligation_store;
    // Caller must supply the actual owning shared_ptr, never a no-op-deleter
    // alias for a borrowed/stack object. Every call retains this owner.
    std::shared_ptr<lattice_db> owner_;
    receive_install_limits limits_;
    database& connection() const;
    receive_install_usage configuration() const;
    std::optional<receive_install_snapshot> row(const std::string&) const;
    void write_row(const receive_install_snapshot&, const receive_install_snapshot* prior);
    // Only the journal's exact frozen-attempt cancellation may consume an
    // unstarted next sequence. No installation, frontier or ACK is created.
    receive_install_snapshot retire_unstarted_for_journal(const receive_install_snapshot&,int64_t);
    struct journal_snapshot {
        receive_install_usage usage;
        std::vector<receive_install_snapshot> channels;
        bool operator==(const journal_snapshot&) const=default;
    };
    // Bounded exact preservation for cancellation, including unrelated receiver
    // channels/high waters. Private to the already-friended journal helper.
    journal_snapshot snapshot_for_journal() const;
public:
    receive_install_store(std::shared_ptr<lattice_db>, receive_install_limits);
    // Every method, including reads, requires this thread's actual owned main
    // WRITE transaction (explicit Core transaction or trusted install frame).
    // initialize audits existing data; no migration, legacy adoption, reset,
    // automatic evidence pruning or channel retirement is provided.
    void initialize();
    void audit() const;
    receive_install_usage usage() const;
    std::optional<receive_install_snapshot> read(const std::string& channel) const;
    void bind(const receive_install_binding&); // new bindings are uninitialized
    // Exact retained retry is recognized before revision/B checks, without
    // effects. Same attempt/different bytes, older retired attempts, wrong
    // epoch/binding and implicit replacement of an active attempt are refused.
    receive_install_receipt begin(const receive_install_binding&, const receive_install_identity&);
    // The exact old last-installed identity explicitly authorizes supersession;
    // omission or a stale identity refuses replacement. Old evidence then ceases
    // to support an exact ACK; high-water sequence still refuses stale attempts.
    receive_install_receipt complete(const receive_install_binding&, const receive_install_identity&,
        const std::optional<receive_install_identity>& supersede = std::nullopt);
    // Explicit controller-fenced abandonment of this exact active identity.
    // Clears active only; preserves frontier/revision/last result and committed
    // sequence high water. Missing/installed/different identities refuse. This
    // does not abandon model/outbox work or authorize a stale callback.
    void abandon_active(const receive_install_binding&, const receive_install_identity&);
    // Convenience storage boundary: trusted SQL effects and state settle in one
    // savepoint. Exact last retry bypasses effects. Effects must not settle the
    // outer transaction, replace hooks/writer, reenter this binding's storage or
    // leave active statements. This grants no model/receipt policy authority.
    receive_install_receipt apply_if_new(const receive_install_binding&, const receive_install_identity&,
        const std::optional<receive_install_identity>& supersede,
        const std::function<void(database&)>& effects);
    // ALL returned state/receipts and new sequences are provisional until the
    // caller verifies its owned COMMIT. Discard rolled-back/unsettled outputs;
    // never ACK or activate upload from savepoint success alone. The wrapper
    // does not extend the transaction or authorize use of a physical writer
    // after it ends. cleanup_failed requires outer rollback/refusal.
};
} // namespace detail
} // namespace lattice
