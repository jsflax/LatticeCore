#pragma once
#include "canonical_scoped_install.hpp"
#include "recovery_request_store.hpp"
#include "recovery_producer_continuity.hpp"
#include "recovery_receiver_source.hpp"

namespace lattice::detail {
class recovery_unknown_reconciliation;
class recovery_receiver_controller;
class recovery_receiver_route;
struct recovery_reconciliation_reservation;

// Only the real controller issues this after verified receipt pages expose an
// unresolved, possibly-exported original. It is not a negative source receipt
// and never grants ordinary DML or authority to reconstruct an UNSENT proof.
class recovery_reconciliation_descriptor final {
    friend class recovery_receiver_controller;
    friend class recovery_continuous_producer;
    friend class recovery_receiver_route;
    friend class recovery_unknown_reconciliation;
    // Issued only by the actual controller after correlated source inspection
    // and known owned local consumption. No raw reply/Q payload is retained.
    struct predecessor {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        canonical_range::attempt logical;
        recovery_obligation_address journal;
        std::string request_digest,old_context,current_context,domain;
        std::string transition_id,transition_digest,source_identity_digest;
        int64_t row_barrier=0,row_sequence=0,row_revision=0;
        int64_t physical=0,phase=0,barrier=0,attempt=0;
        uint64_t controller_revision=0;
    };
    struct contribution {
        recovery_request_row framing;
        recovery_obligation_snapshot journal;
        std::vector<std::string> unknown_originals;
        std::weak_ptr<recovery_receiver_route> route;
        std::shared_ptr<receiver_source_binding> source;
        receiver_source_binding::recovery_view view;
        std::shared_ptr<const predecessor> compatibility;
    };
    // First data member, destroyed LAST: physical capacity is released only
    // after all Q/journal/source/worker payloads below have been destroyed.
    std::shared_ptr<const recovery_reconciliation_cohort> cohort_;
    // One passive progress cell per immutable descriptor; its back-reference is weak.
    mutable std::mutex worker_mutex_;
    mutable std::shared_ptr<recovery_unknown_reconciliation> worker_;
    std::weak_ptr<lattice_db> owner_;
    std::weak_ptr<recovery_receiver_controller> controller_;
    // Present only for the initial frozen phase; restart in restricted phase
    // reconstructs current recording snapshots without minting this proof.
    std::shared_ptr<const verified_unsent_set> frozen_;
    std::vector<contribution> contributions_;
    canonical_scoped_limits limits_{};
    bool restart_revalidation_=false;
    uint64_t controller_revision_=0;
    // Fixed at issuance. Every actual send in this cohort shares one retry
    // revision, so sibling delivery deadlines cannot each mint another pass.
    uint64_t external_revision_=0,delivery_retry_revision_=0;
    int64_t physical_incarnation_=0,barrier_=0,attempt_=0,phase_=0;
    recovery_reconciliation_descriptor()=default;
public:
    recovery_reconciliation_descriptor(const recovery_reconciliation_descriptor&)=delete;
    std::shared_ptr<lattice_db> owner()const noexcept{return owner_.lock();}
    int64_t physical_incarnation()const noexcept{return physical_incarnation_;}
    int64_t barrier()const noexcept{return barrier_;}
    int64_t attempt()const noexcept{return attempt_;}
    int64_t phase()const noexcept{return phase_;}
    uint64_t controller_revision()const noexcept{return controller_revision_;}
};
enum class recovery_reconciliation_step { cancelled, ready_for_refreeze };
// Bound to one exact actual transaction and its expected durable transition.
// Even a known COMMIT cannot open the physical producer gate through this type.
class recovery_reconciliation_result final {
    friend class recovery_receiver_controller;
    friend class recovery_unknown_reconciliation;
    std::shared_ptr<const recovery_reconciliation_descriptor> descriptor_;
    recovery_install_result settlement_;
    std::shared_ptr<recovery_reconciliation_reservation> reservation_;
    bool coordinator_busy_=false;
    recovery_reconciliation_step step_=recovery_reconciliation_step::cancelled;
    int64_t next_barrier_=0,next_attempt_=0;
    recovery_reconciliation_result()=default;
public:
    recovery_reconciliation_result(recovery_reconciliation_result&&)=default;
    recovery_reconciliation_result& operator=(recovery_reconciliation_result&&)=default;
    recovery_reconciliation_result(const recovery_reconciliation_result&)=delete;
    const recovery_install_result& settlement()const noexcept{return settlement_;}
    bool coordinator_busy()const noexcept{return coordinator_busy_;}
};
} // namespace lattice::detail
