#pragma once
#include "receive_install_state.hpp"
#include "recovery_writer_access.hpp"
#include "sync_recovery_outbox.hpp"
#include "sync_recovery_values.hpp"
#include <vector>

namespace lattice::detail {
struct scoped_recovery_limits {
    receive_install_limits installations;
    recovery_outbox_limits capture;
    // Finite caller policy, not defaults or an RSS claim. Metadata counts/bytes
    // include all channels. Work limits apply to this single immutable request.
    int64_t channels, members, metadata_bytes;
    uint64_t targets, receipts, fields, field_bytes, logical_bytes;
};
struct recovery_relation {
    std::string table, lhs_model, rhs_model;
    bool operator==(const recovery_relation&) const = default;
};
struct recovery_full_row {
    recovery_row_key key;
    sync_recovery::row_values values; // complete actual columns except local id
};
enum class recovery_pending_outcome {
    committed_effect, committed_noop, not_committed, unknown, policy_only
};
struct recovery_pending_grant {
    std::string audit_global_id;
    recovery_row_key target;
    recovery_pending_outcome outcome;
};
struct scoped_recovery_request {
    receive_install_binding binding;
    receive_install_identity identity;
    std::optional<receive_install_identity> supersede;
    std::vector<std::string> model_tables;
    // Complete relation catalog affecting these models, authenticated/generated
    // from actual registration by the future controller. Outside-scope links
    // are checked but never acquired or deleted implicitly.
    std::vector<recovery_relation> relations;
    std::vector<std::string> scoped_link_tables;
    std::vector<recovery_full_row> full_rows;
    // Explicit scope ownership, never inferred from an unowned row collision.
    std::vector<recovery_row_key> initial_row_grants;
    // Exact original identity + target + H-bound outcome. Also establishes
    // scope for absent-from-full lost-ACK inserts and never-dispatched inserts.
    std::vector<recovery_pending_grant> pending;
};
struct scoped_recovery_result {
    recovery_install_result transaction;
    // Present after the SQL body, but usable only if transaction is committed.
    std::optional<receive_install_receipt> installation;
};
// PRIVATE/INACTIVE. Inputs already authenticated and coverage/receipt/rebase
// digests already verified by the future controller. This method does not
// authenticate hashes, prove source receipt, classify possibly-sent intent,
// activate a network route, change epoch/schema, or migrate legacy ownership.
// Requires actual owning shared_ptr. Refuses joining an existing transaction.
// All scoped effects, channel-only receipt settlement, current membership, and
// installation revision share the retained owner's one owned transaction.
scoped_recovery_result install_scoped_recovery(std::shared_ptr<lattice_db>,
    const scoped_recovery_request&, const scoped_recovery_limits&);
} // namespace lattice::detail
