#pragma once
#include "recovery_obligation_store.hpp"
#include <functional>

namespace lattice::detail {
class recovery_local_producer_adapter;
struct recovery_obligation_producer_test_access;

// Private storage budgets, supplied explicitly. Retained settled stamps remain
// charged until whole-contribution retirement; no automatic evidence pruning.
struct recovery_obligation_producer_limits {
    int64_t profiles, stamps, field_bytes, manifest_bytes, encoded_bytes;
    bool operator==(const recovery_obligation_producer_limits&) const = default;
};
struct recovery_obligation_producer_discovery_limits {
    recovery_obligation_limits obligations;
    receive_install_limits installations;
    recovery_obligation_producer_limits producers;
};
struct recovery_obligation_producer_profile {
    recovery_obligation_profile contribution;
    int64_t contribution_incarnation=0;
    int64_t program_revision=0;
    // Canonical template/schema/version digest, not a self-referential hash of
    // final SQL that embeds this token. Adapter verifies exact final SQL too.
    std::string program_digest;
    std::vector<uint8_t> grant_manifest;
    bool operator==(const recovery_obligation_producer_profile&) const = default;
};
struct recovery_obligation_producer_stamp {
    int64_t contribution_incarnation=0, program_revision=0;
    int64_t audit_id=0, record_sequence=0;
    std::string program_digest;
    // These are reread durable facts, not authentication/all-route coverage.
    // No existing record() argument can supply or upgrade this stamp.
    bool operator==(const recovery_obligation_producer_stamp&) const = default;
};
struct recovery_obligation_producer_inventory {
    bool initialized=false;
    recovery_obligation_limits stored_obligation_limits{};
    receive_install_limits stored_installation_limits{};
    recovery_obligation_producer_limits stored_producer_limits{};
    std::vector<recovery_obligation_producer_profile> profiles;
    // Stored limits are metadata values checked against independent discovery
    // caps; they are never adopted as allocation policy from an unknown file.
};

class recovery_obligation_producer_program {
    friend class recovery_obligation_producer_store;
    recovery_obligation_producer_profile profile_;
    recovery_obligation_limits obligations_;
    recovery_obligation_producer_limits producers_;
    recovery_obligation_producer_program(recovery_obligation_producer_profile,
        recovery_obligation_limits,recovery_obligation_producer_limits);
public:
    const recovery_obligation_producer_profile& profile() const noexcept { return profile_; }
    // Append immediately after the genuine generated AuditLog INSERT. The
    // common model tail accepts INSERT/UPDATE/DELETE; regular links only I/D.
    // No SQL/UDF calls into C++ mutation. Guard always requests local phase 1:
    // lattice_recovery_producer_guard_v1(channel BLOB, incarnation INTEGER,
    //   program_revision INTEGER, program_digest BLOB, relation BLOB, phase INTEGER)
    // The adapter owns nonwriting guard/authorizer/runtime lifetime admission.
    // Immutable profile custody is the adapter's responsibility: enrollment
    // and bootstrap validate full manifest/digest/program equality. The hot
    // tail binds digest/incarnation/revision and exact manifest length/charge.
    // Negative default uses the explicit producer encoded-byte policy. Adapters
    // pass their independent generated-program cap. A fixed fragment preflight
    // bounds temporary literals; exact output counting precedes each append.
    std::string emit_tail(const std::string& relation,bool regular_link=false,
                          int64_t maximum_sql_bytes=-1) const;
};

class recovery_obligation_producer_store {
    friend class recovery_local_producer_adapter;
    friend class recovery_continuous_producer;
    friend struct recovery_obligation_producer_test_access;
    std::shared_ptr<lattice_db> owner_;
    recovery_obligation_limits obligations_;
    receive_install_limits installations_;
    recovery_obligation_producer_limits limits_;
    recovery_obligation_producer_store(std::shared_ptr<lattice_db>,
        recovery_obligation_limits,receive_install_limits,recovery_obligation_producer_limits);
    // Adapter-only enrollment/retirement. These retain the ordinary actual
    // owned-WRITE contract and return provisional results until outer COMMIT.
    void initialize();
    recovery_obligation_producer_program enroll(const recovery_obligation_producer_profile&);
    std::vector<recovery_obligation_producer_profile> profiles() const;
    // Atomically remove this producer's charged stamps/profile AND its settled
    // ordinary contribution. Adapter must retire runtime/route admission first.
    // Ordinary obligation retire refuses an enrolled producer contribution.
    void retire_contribution(const recovery_obligation_address&,
        const recovery_obligation_producer_profile& expected);
    // Only the adapter may compile from bootstrap facts. This grants no runtime
    // authority; exact owner/handle/hooks/program admission remains required.
    static recovery_obligation_producer_program compile(
        const recovery_obligation_producer_profile&,recovery_obligation_limits,
        recovery_obligation_producer_limits);
    // Read-only constructor/reopen exception: retain the unpublished physical
    // connection, require idle/nonwaiting admission, own one read snapshot,
    // enforce independent caps, and clean up exactly. No CREATE, migration,
    // writes, owner-WRITE inference or activation capability is permitted.
    static recovery_obligation_producer_inventory bootstrap_profiles(
        std::shared_ptr<database>,const recovery_obligation_producer_discovery_limits&,
        const std::function<void(database&,const recovery_obligation_producer_inventory&)>& validate = {});
public:
    // Final assembler reads actual provenance in its retained owned transaction.
    // A returned value cannot be supplied back to record() to mint provenance.
    static std::optional<recovery_obligation_producer_stamp> read_stamp(
        std::shared_ptr<lattice_db>,recovery_obligation_limits,receive_install_limits,
        recovery_obligation_producer_limits,const recovery_obligation_address&,
        const std::string& original_id);
};
} // namespace lattice::detail
