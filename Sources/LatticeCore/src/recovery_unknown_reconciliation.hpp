#pragma once
#include "recovery_reconciliation_descriptor.hpp"
#include "recovery_export_adapter.hpp"
#include <exception>
#include <mutex>

namespace lattice::detail {
class recovery_unknown_reconciliation;

// Issued only from the controller's current phase-4 descriptor. It names an
// ordered window of retained originals, not pending/ACK bookkeeping or a caller
// assertion of source absence. The frame retains the separately counted work.
class recovery_reconciliation_export final {
    friend class recovery_unknown_reconciliation;
    friend class recovery_export_adapter;
    friend class recovery_export_route;
    friend class committed_export_frame;
    std::shared_ptr<const recovery_reconciliation_descriptor> descriptor_;
    std::weak_ptr<recovery_unknown_reconciliation> worker_;
    recovery_obligation_address address_;
    std::vector<recovery_obligation_entry> requested_;
    std::vector<std::string> originals_, selected_;
    size_t contribution_=0, begin_=0;
    uint64_t reservation_=0;
    bool released_=false;
    recovery_reconciliation_export()=default;
    void release_for_retry()noexcept;
    void did_handoff()noexcept;
    size_t retained_bytes(size_t)const noexcept;
public:
    ~recovery_reconciliation_export();
    recovery_reconciliation_export(const recovery_reconciliation_export&)=delete;
};

// The descriptor owns this passive progress cell; it retains that descriptor
// and the actual database only weakly. No worker, transport, database or callback
// is owned by a cursor. One private export grant at a time owns each window.
class recovery_unknown_reconciliation final : public std::enable_shared_from_this<recovery_unknown_reconciliation> {
    friend class ::lattice::synchronizer_base;
    friend class recovery_reconciliation_export;
    struct progress {size_t next=0;uint64_t active=0;};
    std::weak_ptr<const recovery_reconciliation_descriptor> descriptor_;
    std::mutex mutex_;
    std::vector<progress> contributions_;
    uint64_t next_reservation_=0;
    bool settling_=false, completed_=false, abandoned_=false;
    std::exception_ptr failure_;
    explicit recovery_unknown_reconciliation(const std::shared_ptr<const recovery_reconciliation_descriptor>&);
    static std::shared_ptr<recovery_unknown_reconciliation> acquire(
        const std::shared_ptr<const recovery_reconciliation_descriptor>&);
    void require_live_locked()const;
    void fail(std::exception_ptr)noexcept;
    void release(size_t,uint64_t,size_t,size_t,bool,bool)noexcept;
    std::shared_ptr<recovery_reconciliation_export> reserve(
        const std::shared_ptr<const recovery_reconciliation_descriptor>&,
        const std::shared_ptr<recovery_receiver_route>&,size_t,const std::vector<int64_t>&);
    void cancel(const std::shared_ptr<const recovery_reconciliation_descriptor>&);
    bool refreeze_if_complete(const std::shared_ptr<const recovery_reconciliation_descriptor>&);
    // One phase transition or one finite page. Nullopt means only typed
    // no-effect admission busy; a blocked original remains an explicit error.
    static std::optional<recovery_export_preparation> prepare(
        const std::shared_ptr<recovery_receiver_route>&,
        const std::shared_ptr<recovery_continuous_route>&,
        const std::shared_ptr<receiver_source_binding>&,
        std::shared_ptr<lattice_db>,uint64_t,size_t,const std::vector<int64_t>&,
        std::shared_ptr<const receiver_upload_view>,bool*);
};
} // namespace lattice::detail
