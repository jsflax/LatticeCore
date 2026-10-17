#pragma once
#ifndef __EMSCRIPTEN__
#include <lattice/sync.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <chrono>
#include <atomic>
#include <cstddef>
#include <exception>
#include <cstdint>
#include <utility>
#include <memory>
#include <string>

namespace lattice { struct physical_store_identity; class lattice_db; class swift_managed_product_route; struct lattice_close_result; }
namespace lattice::detail {
class configured_recovery_connection;
class configured_control_service;
class ordinary_managed_lock_record;
class ordinary_managed_lock_custody;
class ordinary_managed_ipc_access;
class ordinary_receiver_wss_access;
class ordinary_receiver_wss_request;
class ordinary_managed_profile;
struct ordinary_managed_product_state;
struct sync_policy_reservation;
std::shared_ptr<const sync_policy_reservation> reserve_managed_wss_policy(const lattice_db&,const std::string&,const std::string&);
std::shared_ptr<const sync_policy_reservation> reserve_history_receiver_policy(const lattice_db&,const std::string&,const std::string&);

// Native destructor facts only. The actual start scope and actual admission
// are the only publishers; neither fact issues a source, receipt or policy.
class ordinary_managed_owner_release final {
    friend class configured_recovery_connection;
    friend class ::lattice::lattice_db;
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    friend class ordinary_managed_start_capture_tail;
    friend class ordinary_managed_wss_admission;
    friend class configured_control_service;
    friend class ordinary_managed_lock_record;
    const std::uint64_t creator_;
    std::atomic<bool> start_destroyed_{false},admission_destroyed_{false};
    std::atomic<int> lock_error_{0};
    // Sole actual start-tail writer, published by start_destroyed release.
    // Readers copy only after start_fields_destroyed's acquire observation.
    std::exception_ptr start_cleanup_error_;
    void record_start_cleanup_error(std::exception_ptr)noexcept;
    std::exception_ptr start_cleanup_error()const noexcept;
    std::atomic<bool> native_lock_complete_{false};
    std::size_t native_slot_=64;
    std::uint64_t native_serial_=0;
    std::size_t service_slot_=64;
    const configured_recovery_connection* owner_=nullptr; // comparison only
    ordinary_managed_owner_release()noexcept;
    bool fields_destroyed()const noexcept;
    bool start_fields_destroyed()const noexcept;
    int lock_error()const noexcept;
    void record_lock_error(int)noexcept;
    bool native_lock_complete()const noexcept;
    void bind_native(std::size_t,std::uint64_t)noexcept;
    void finish_native(int)noexcept;
    void bind_wakeup(std::size_t,const configured_recovery_connection*)noexcept;
    void finish_start()noexcept;
    void finish_admission()noexcept;
    void wake()const noexcept;
public:
    ~ordinary_managed_owner_release()=default;
};
// FIRST owning local in each authentic start, after its original deadline
// scalar. Destruction follows every later callback/owner/admission local,
// including exception unwinding. The returned public handle is external.
class ordinary_managed_start_capture_tail final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    native_retirement_storage<std::shared_ptr<ordinary_managed_owner_release>> completion_storage_;
    const std::shared_ptr<ordinary_managed_owner_release>& completion_;
    struct retained_application {std::shared_ptr<lattice_db> owner;};
    struct native_application_deleter {
        const std::uint64_t creator;
        void operator()(retained_application*)const noexcept;
    };
    std::unique_ptr<retained_application,native_application_deleter> application_;
    void retain_application(const std::shared_ptr<lattice_db>&);
    ordinary_managed_start_capture_tail();
    ~ordinary_managed_start_capture_tail()noexcept;
    ordinary_managed_start_capture_tail(const ordinary_managed_start_capture_tail&)=delete;
    ordinary_managed_start_capture_tail& operator=(const ordinary_managed_start_capture_tail&)=delete;
};

// Private mechanical descriptor custody, never source/admission authority.
// Only genuine authenticated start owners may reserve it through the actual
// configured service. A successful open is adopted without allocation; one
// existing collector owns its sole unlock/close attempts and sticky results.
class ordinary_managed_lock_custody final {
    friend class configured_control_service;
    friend class configured_recovery_connection;
    friend class ordinary_managed_wss_admission;
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    std::shared_ptr<ordinary_managed_lock_record> record_;
    explicit ordinary_managed_lock_custody(std::shared_ptr<ordinary_managed_lock_record>)noexcept;
    void adopt(int,int)noexcept;
    void observe(std::uint64_t,std::uint64_t);
    void issued_admission()const noexcept;
public:
    ordinary_managed_lock_custody(ordinary_managed_lock_custody&&)noexcept;
    ~ordinary_managed_lock_custody()noexcept;
    ordinary_managed_lock_custody(const ordinary_managed_lock_custody&)=delete;
    ordinary_managed_lock_custody& operator=(const ordinary_managed_lock_custody&)=delete;
    ordinary_managed_lock_custody& operator=(ordinary_managed_lock_custody&&)=delete;
};

// Only an issued, authenticated product route can create this admission.
// A configuration, token string, copied profile or ordinary_context cannot.
// It grants the fixed spoke's ordinary-v2 WSS path, never continuous-recovery
// protocol authority, a projected-value receipt, or permission to clear debt.
class ordinary_managed_wss_admission final {
    friend class configured_recovery_connection;
    // FIRST data member: runs after every original field and the mechanical
    // lock lease. Queued FD cleanup is not native completion; that is separate.
    struct admission_capture_tail {
        const std::shared_ptr<ordinary_managed_owner_release> completion;
        explicit admission_capture_tail(std::shared_ptr<ordinary_managed_owner_release> value)noexcept
            :completion(std::move(value)){}
        ~admission_capture_tail()noexcept;
    } released_;
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    const std::shared_ptr<const ordinary_receiver_wss_request> receiver_;
    ordinary_managed_wss_admission(std::shared_ptr<const ordinary_receiver_wss_request>,
        std::shared_ptr<lattice_db>,std::string,std::shared_ptr<const sync_policy_reservation>,ordinary_managed_lock_custody&&,std::chrono::steady_clock::time_point,
        const std::shared_ptr<ordinary_managed_owner_release>&)noexcept;
    const std::uint64_t creator_;
    const std::shared_ptr<const ordinary_managed_product_state> product_;
    const std::shared_ptr<lattice_db> application_;
    const std::shared_ptr<const ordinary_managed_profile> profile_;
    const std::string endpoint_;
    const std::shared_ptr<const sync_policy_reservation> policy_;
    const ordinary_managed_lock_custody lock_;
    const std::chrono::steady_clock::time_point initial_deadline_;
    ordinary_managed_wss_admission(std::shared_ptr<const ordinary_managed_product_state>,
        std::shared_ptr<lattice_db>,std::shared_ptr<const ordinary_managed_profile>,std::string,
        std::shared_ptr<const sync_policy_reservation>,ordinary_managed_lock_custody&&,std::chrono::steady_clock::time_point,
        const std::shared_ptr<ordinary_managed_owner_release>&)noexcept;
public:
    ~ordinary_managed_wss_admission();
    bool current() const noexcept; // no SQL or foreign callbacks
    std::chrono::steady_clock::time_point expires() const noexcept;
    std::chrono::steady_clock::time_point initial_deadline()const noexcept{return initial_deadline_;}
    // Fresh sequential actual owned turns. Called off the controller's leaves
    // before each physical factory entry, with one original absolute deadline.
    void validate(const std::shared_ptr<lattice_db>& application,
        const std::shared_ptr<lattice_db>& child,const sync_config&,
        std::chrono::steady_clock::time_point) const;
    // The later bind boundary cannot repeat SQL under constructor callbacks.
    // It checks the exact admitted child/profile/config and current session.
    bool matches(const std::shared_ptr<lattice_db>& child,const sync_config&) const noexcept;
};

// Copies of the Swift result share one lifetime. Dropping the last handle
// requests real asynchronous close; the configured service retains all debt
// until actual platform/native/worker/callback/child/scheduler settlement.
class ordinary_group_target_receiving_owner;
class ordinary_receiver_wss_request;
class ordinary_managed_wss_handle final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    const std::uint64_t creator_;
    const std::shared_ptr<configured_recovery_connection> owner_;
    const std::shared_ptr<ordinary_group_target_receiving_owner> group_owner_;
    const std::shared_ptr<const ordinary_receiver_wss_request> group_request_;
    explicit ordinary_managed_wss_handle(std::shared_ptr<configured_recovery_connection>);
    ordinary_managed_wss_handle(std::shared_ptr<ordinary_group_target_receiving_owner>,
        std::shared_ptr<const ordinary_receiver_wss_request>);
public:
    ~ordinary_managed_wss_handle();
    void stop() const noexcept;
    bool settled() const noexcept;
    lattice_close_result close(std::chrono::steady_clock::time_point) const noexcept;
};
}
#endif
