#pragma once
#include "canonical_scoped_install.hpp"
#include "recovery_producer_continuity.hpp"
#include "recovery_receiver_source.hpp"
#include "recovery_reconciliation_descriptor.hpp"
#include <lattice/scheduler.hpp>

namespace lattice::detail {
class recovery_receiver_route;
class recovery_controller_executor;
class committed_export_frame;
class controller_worker_original;

// One instance per actual continuous physical session. Construction and route
// registration belong to retained producer admission; neither an application
// label nor a qualification friend can manufacture this coordinator.
class recovery_receiver_controller final : public std::enable_shared_from_this<recovery_receiver_controller> {
    friend class recovery_continuous_producer;
    friend class recovery_receiver_route;
    friend class recovery_unknown_reconciliation;
    friend struct recovery_reconciliation_reservation;
    friend struct recovery_receiver_controller_test_access;
    friend struct controller_fixture_worker_selection;
    // Restriction/observation only; captured before a real route is published.
    // The fixture peer cannot construct controllers, grants or source views.
    struct test_probe {
        const lattice_db* owner=nullptr;
        std::function<std::shared_ptr<void>(const char*)> scope;
        std::function<void(const char*)> observed;
        // Optional passive DATA only; production and old fault probes leave null.
        std::function<void(const char*)> admission_observed;
        // Passive completion of the existing physical-phase owned unit only.
        // This never requests a yield, grants admission, or changes a deadline.
        std::function<void()> phase_committed;
        // Passive DATA capture after validated phase4 owned COMMIT, before the
        // candidate descriptor is visible to any actual restricted route.
        std::function<void()> reconciliation_prepublication;
        // Passive copied DATA only: actual next request deferred until all
        // retained request/reply bytes have ended. No admission or wake grant.
        std::function<void()> response_capacity_wait;
    };
    static std::mutex test_mutex_;
    static std::shared_ptr<const test_probe> test_probe_;
    // Separate fixture ending observation; fault-probe replacement cannot erase it.
    using worker_observer=std::function<void(const recovery_controller_executor&)>;
    static std::shared_ptr<const worker_observer> test_worker_observer_;
    struct state;
    std::unique_ptr<state> state_;
    explicit recovery_receiver_controller(const recovery_continuous_policy&,
        const std::shared_ptr<controller_worker_original>&);
    void retire_for_deletion();
    static canonical_scoped_limits limits(const recovery_continuous_policy&);
    static void initialize_owned(std::shared_ptr<lattice_db>,const recovery_continuous_policy&);
    static void validate_reopen_owned(std::shared_ptr<lattice_db>,const recovery_continuous_policy&,int64_t phase,int64_t barrier,int64_t attempt);
    std::shared_ptr<recovery_receiver_route> attach(std::shared_ptr<lattice_db>,const std::shared_ptr<recovery_continuous_route>&,
        const std::shared_ptr<receiver_source_binding>&,const std::shared_ptr<owned_platform_sync_transport>&,
        const std::shared_ptr<scheduler>&,const std::shared_ptr<sync_callback_lifetime>&);
    void wake(const std::shared_ptr<recovery_receiver_route>&);
    void turn();
    void dropped_turn(const void* scheduled_token)noexcept;
    void arm_admission_retry(int64_t);
    void scheduling_failed(std::exception_ptr)noexcept;
    void retire(recovery_receiver_route*)noexcept;
    static bool reconciliation_route_current(const std::shared_ptr<const recovery_reconciliation_descriptor>&,
        const std::shared_ptr<recovery_continuous_route>&,const std::shared_ptr<lattice_db>&,uint64_t);
    static void verify_reconciliation_route(const std::shared_ptr<const recovery_reconciliation_descriptor>&,
        const std::shared_ptr<recovery_continuous_route>&,const std::shared_ptr<lattice_db>&,uint64_t,
        const std::vector<std::string>* ordered_originals=nullptr);
    static recovery_reconciliation_result controller_reconcile_owned(
        const std::shared_ptr<const recovery_reconciliation_descriptor>&,
        recovery_reconciliation_step,const std::function<void(database&)>&,bool* coordinator_busy=nullptr);
    static void controller_reconcile_publish(recovery_reconciliation_result&&);
public:
    ~recovery_receiver_controller();
    recovery_receiver_controller(const recovery_receiver_controller&)=delete;
    recovery_receiver_controller& operator=(const recovery_receiver_controller&)=delete;
};

// Actual synchronizer-held registration. It retains no borrowed database and
// accepts control bytes only from its current verified physical source record.
// Destruction and queued work follow the existing owner/lifetime scheduler.
class recovery_receiver_route final : public std::enable_shared_from_this<recovery_receiver_route> {
    friend struct recovery_receiver_cohort_test_access;
    friend class recovery_receiver_controller;
    friend class recovery_continuous_producer;
    friend class ::lattice::synchronizer_base;
    friend class recovery_unknown_reconciliation;
    std::shared_ptr<const recovery_reconciliation_descriptor> pending_reconciliation()const;
    std::shared_ptr<recovery_continuous_route> reconciliation_work_route()const;
    // Created from a real committed restricted frame before physical handoff.
    // Only the synchronizer's matching delivery timeout may invoke it.
    std::function<void()> delivery_timeout_retry(const committed_export_frame&,uint64_t);
    struct state;
    std::shared_ptr<state> state_;
    std::shared_ptr<recovery_receiver_controller> controller_;
    recovery_receiver_route(std::shared_ptr<recovery_receiver_controller>,std::shared_ptr<state>);
    enum class notification : unsigned { resumed=1,renew=2,reconcile=4,error=8 };
    void post(notification,std::exception_ptr = {});
    void drain_notifications();
    void wake();
    void request();
    void notifications(std::function<void()>,std::function<void()>,std::function<void(const std::exception_ptr&)>,std::function<void()> reconciliation={},std::function<void()> wake_notifications={});
    bool receive(const platform_transport_callbacks&,uint64_t,const transport_message&);
    bool blocks_ordinary()const noexcept;
    bool creator_current()const noexcept;
    std::exception_ptr checked_disposal_failure(uint64_t lifecycle)const;
public:
    ~recovery_receiver_route();
    recovery_receiver_route(const recovery_receiver_route&)=delete;
    recovery_receiver_route& operator=(const recovery_receiver_route&)=delete;
};
} // namespace lattice::detail
