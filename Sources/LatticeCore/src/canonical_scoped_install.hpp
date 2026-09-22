#pragma once
#include "canonical_range_staging.hpp"
#include "recovery_obligation_store.hpp"
#include "scoped_recovery_install.hpp"
#include "receive_delivery_guard.hpp"

namespace lattice::detail {
struct canonical_scoped_contract {
    std::vector<std::string> model_tables;
    std::vector<recovery_relation> relations;
    std::vector<std::string> scoped_link_tables;
    std::vector<recovery_row_key> initial_row_grants;
};
struct canonical_scoped_limits {
    scoped_recovery_limits install;
    canonical_range::limits codec;
    canonical_staging_limits staging;
    recovery_obligation_limits obligations;
};
struct canonical_scoped_install_test_access;
// PRIVATE/INACTIVE. There is deliberately NO production issuer. The named test
// friend mints mechanical qualification inputs only, never source authority.
// A future verifier must prove authenticated grant/source/receipt coverage,
// complete generated producer and all-route custody, and current route fencing
// before it can be reviewed as an issuer. Hashes, an origin enum, a missing export
// claim, or an application assertion cannot mint this admission.
// All arguments are bound here; this cannot authorize replacement rows, scope,
// source, journal, attempt, owner or limits supplied to a later call.
class canonical_install_admission {
    std::shared_ptr<lattice_db> owner_;
    canonical_range::attempt attempt_;
    uint64_t route_=0;
    std::string request_digest_,manifest_digest_,coverage_id_;
    recovery_obligation_profile profile_;
    recovery_obligation_address journal_;
    int64_t journal_revision_=0;
    canonical_scoped_contract contract_;
    canonical_scoped_limits limits_{};
    std::optional<receive_install_identity> supersede_;
    // Inactive optional transition for existing mechanical fixture compatibility.
    // A production issuer must bind the actual live receive guard as well as all
    // source/route/producer obligations before activating canonical recovery.
    std::optional<receive_guard_snapshot> receive_guard_;
    canonical_install_admission()=default;
    friend struct canonical_scoped_install_test_access;
    friend scoped_recovery_result install_staged_canonical_range(const canonical_install_admission&);
    friend scoped_recovery_result inspect_committed_canonical_range(const canonical_install_admission&, const receive_install_identity&, const canonical_range::request&, const canonical_range::manifest&);
public:
    canonical_install_admission(const canonical_install_admission&)=default;
    canonical_install_admission(canonical_install_admission&&)=default;
    canonical_install_admission& operator=(const canonical_install_admission&)=delete;
    canonical_install_admission& operator=(canonical_install_admission&&)=delete;
};
// Actual retained stage + journal, one retained-owner transaction. Derives I
// exclusively from verified retained framing, rechecks C/E and journal after
// effects, then commits models/membership/receipts/receiver/witness together.
// An explicitly bound receive guard completes in that same COMMIT; canonical
// channels refuse admission that omits it, including retained-stage retries.
// Refuses every final non-Q obligation, including fresh local writes. No
// never-dispatched inference, automatic progress, route activation or cleanup.
// Retained-stage exact retries bypass effects and journal settlement. Released
// stages are outside this entry; no digest/head-only retry path is added.
scoped_recovery_result install_staged_canonical_range(const canonical_install_admission&);
// Inspect one full, exact retained receiver result, including after stage release
// or reopen. This reads existing receiver/journal/guard metadata in one owned
// transaction and does not initialize storage, apply models, settle originals,
// release pages, resume a journal or activate transport. The guard admission may
// be the original preinstall snapshot or a fresh exact canonical snapshot.
// Complete frozen request/manifest framing is revalidated against every logical
// attempt field and the private grant Q/M. The shared staging conversion derives
// binding and I from those exact bytes; route validation is framing, not liveness.
// The full supplied identity must equal both that derived I and actual durable last-installed evidence;
// a sequence/head/digest or journal claim alone cannot produce a receipt. The
// private admission binds the owner, source/profile and current journal address.
// This is committed-state observation, not source authentication or permission
// to resume a current/newer route. Its receipt is usable only after owned COMMIT.
scoped_recovery_result inspect_committed_canonical_range(const canonical_install_admission&,
    const receive_install_identity&,const canonical_range::request&,const canonical_range::manifest&);
} // namespace lattice::detail
