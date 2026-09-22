#pragma once
#include "receive_install_state.hpp"
#include <vector>

namespace lattice::detail {
// Private inactive storage. These limits are supplied by the caller, not
// production defaults. Counts include retained settled tombstones.
struct recovery_obligation_limits {
    int64_t scopes, records, field_bytes, encoded_bytes;
    bool operator==(const recovery_obligation_limits&) const = default;
};
enum class recovery_obligation_error_code {
    transaction_required, invalid_argument, corrupt_state, limits_mismatch,
    capacity, binding_mismatch, alias, stale, wrong_mode, conflict,
    audit_mismatch, exhausted, cleanup_failed
};
class recovery_obligation_error : public std::runtime_error {
public:
    recovery_obligation_error_code code;
    std::exception_ptr primary_error, cleanup_error;
    recovery_obligation_error(recovery_obligation_error_code c,const char* message,
        std::exception_ptr primary={},std::exception_ptr cleanup={})
        :std::runtime_error(message),code(c),primary_error(primary),cleanup_error(cleanup) {}
};
enum class recovery_obligation_mode : int64_t { recording=0, frozen=1, installed=2 };
// A caller claim is deliberately NOT a generated-producer or all-route proof.
// This primitive cannot produce the future coverage/admission capability.
enum class recovery_obligation_origin : int64_t { local_candidate=0, imported=1, legacy_unknown=2 };
enum class recovery_obligation_stage : int64_t { open=0, acknowledged_awaiting_install=1, settled=2 };
enum class recovery_obligation_outcome : int64_t { applied=0, no_op=1 };
struct recovery_obligation_profile {
    receive_install_binding binding;
    std::string profile_digest, receipt_namespace;
    // This first storage profile uses strict UUID comparison only. The grant
    // digest is opaque storage, not proof of an authenticated scope selection.
    bool operator==(const recovery_obligation_profile&) const = default;
};
struct recovery_obligation_address {
    std::string channel;
    int64_t incarnation=0, generation=0;
    bool operator==(const recovery_obligation_address&) const = default;
};
struct recovery_obligation_record {
    int64_t audit_id=0;
    std::string original_id, table, target_id;
    recovery_obligation_origin origin=recovery_obligation_origin::legacy_unknown;
    bool operator==(const recovery_obligation_record&) const = default;
};
struct recovery_obligation_receipt_claim {
    std::string original_id, receipt_namespace;
    int64_t position=0;
    recovery_obligation_outcome outcome=recovery_obligation_outcome::applied;
    // Caller must independently verify authority, namespace, target and H.
    // No API in this storage primitive authenticates this positive claim.
    bool operator==(const recovery_obligation_receipt_claim&) const = default;
};
struct recovery_obligation_entry {
    recovery_obligation_record record; // actual persisted audit spellings
    std::string canonical_original_id, canonical_target_id;
    int64_t sequence=0;
    std::optional<int64_t> first_export_claim;
    recovery_obligation_stage stage=recovery_obligation_stage::open;
    std::optional<recovery_obligation_receipt_claim> acknowledged;
    int64_t settled_install_sequence=0;
    bool operator==(const recovery_obligation_entry&) const = default;
};
struct recovery_obligation_scope {
    recovery_obligation_profile profile;
    recovery_obligation_address address;
    int64_t revision=0, last_attempt=0, freeze_revision=0;
    int64_t freeze_record_high_water=0, freeze_export_high_water=0;
    recovery_obligation_mode mode=recovery_obligation_mode::recording;
    // One exact last installation claim; actual receiver state is reread at
    // settlement/resume. This is not a second canonical frontier.
    int64_t installed_sequence=0, installed_revision=0, installed_head=0;
    std::string installed_manifest;
    bool operator==(const recovery_obligation_scope&) const = default;
};
struct recovery_obligation_snapshot {
    recovery_obligation_scope scope;
    std::vector<recovery_obligation_entry> entries; // non-settled, audit order
    // No completeness/authentication/never-dispatched assertion is exposed.
};
struct recovery_obligation_export_ticket {
    recovery_obligation_address address;
    int64_t sequence=0, journal_revision=0;
    std::vector<std::string> canonical_original_ids;
    // Provisional until the caller verifies outer owned COMMIT; no transport
    // or object-lifetime authorization is conferred by this stored claim.
};
struct recovery_obligation_usage {
    int64_t scopes=0, records=0, encoded_bytes=0;
    bool operator==(const recovery_obligation_usage&) const = default;
};

class recovery_obligation_store {
    std::shared_ptr<lattice_db> owner_;
    recovery_obligation_limits limits_;
    receive_install_limits install_limits_;
    database& writer() const;
public:
    // Actual owning pointer only, not a no-op-deleter alias for a borrowed DB.
    recovery_obligation_store(std::shared_ptr<lattice_db>,recovery_obligation_limits,receive_install_limits);
    // Every call requires this thread's actual owned main WRITE transaction.
    // No schema migration, producer/route activation or legacy adoption occurs.
    // Initialize/audit at each store opening before using indexed hot methods;
    // subsequent internal metadata/DDL access is exclusive to this helper.
    void initialize();
    void audit() const;
    recovery_obligation_usage usage() const;
    std::optional<recovery_obligation_scope> read(const std::string& channel) const;
    // Receiver storage must already be initialized and exactly bound. A new
    // journal binding inherits its actual committed baseline (no active I),
    // so retirement/recreation cannot reuse an older installed I to settle
    // new obligations. This is not legacy origin/coverage adoption.
    recovery_obligation_scope bind(const recovery_obligation_profile&);
    recovery_obligation_entry record(const recovery_obligation_address&,const recovery_obligation_record&);
    std::optional<recovery_obligation_entry> find(const recovery_obligation_address&,const std::string& original_id) const;
    recovery_obligation_export_ticket claim_export(const recovery_obligation_address&,const std::vector<std::string>& original_ids);
    recovery_obligation_scope freeze(const recovery_obligation_address&,int64_t logical_attempt);
    // Cancel only this exact, known-uninstalled frozen journal attempt in the
    // actual owned writer transaction. Refuses active/newer receiver state;
    // preserves originals and claims, and retires an unstarted exact next
    // receiver sequence without fabricating a manifest/install. The generation and
    // revision advance fences a previously issued installer. This is not a
    // transport/producer-barrier cancellation or proof that callbacks stopped.
    // Exact bounded pre/post snapshots preserve all journal entries (including
    // settled), other scopes, allocator high waters and receiver channels. Only
    // this scope's mode/generation/revision and its unstarted receiver sequence
    // may change; structurally valid trigger rewrites still roll back.
    // Result is provisional until the outer owned COMMIT is known successful.
    recovery_obligation_scope cancel_frozen_for_retry(const recovery_obligation_address&,
        int64_t logical_attempt,int64_t expected_journal_revision);
    recovery_obligation_snapshot snapshot_for_install(const recovery_obligation_address&,int64_t logical_attempt) const;
    recovery_obligation_scope acknowledge(const recovery_obligation_address&,const recovery_obligation_receipt_claim&);
    // AFTER actual same-owner apply_if_new returned installed, BEFORE its outer
    // COMMIT. Failure MUST escape to the retained outer installer and roll back
    // models/receiver/journal together. Exact installed retry skips this method.
    recovery_obligation_scope settle_install(const recovery_obligation_address&,int64_t expected_journal_revision,
        const receive_install_identity&,const std::vector<recovery_obligation_receipt_claim>& positives);
    recovery_obligation_scope resume(const recovery_obligation_address&,const receive_install_identity&);
    // Refuses non-settled obligations. Removes settled records and binding;
    // committed allocator high waters remain, so reused channel text is fenced.
    void retire(const recovery_obligation_address&);
    // Retention contract: true for ANY scope retaining this original/body.
    // Corruption/mismatched identity throws; never interpret a read error as
    // unpinned. Caller must evaluate this in its same owned prune transaction.
    bool pins_audit(int64_t audit_id,const std::string& original_id) const;
    // All results are transaction-local/provisional until successful owned
    // outer COMMIT. Rolled-back tickets are discarded. Settled tombstones remain
    // charged until explicit scope retirement; per-record pruning is not added.
};
} // namespace lattice::detail
