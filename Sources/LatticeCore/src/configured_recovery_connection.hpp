#pragma once
#ifndef __EMSCRIPTEN__
#include <lattice/sync.hpp>
#include <lattice/configured_platform.hpp>
#include "configured_retirement.hpp"
#include "configured_attempt_custody.hpp"
#include "sync_callback_lifetime.hpp"
#include "configured_recovery_observation.hpp"
#include <condition_variable>
#include <optional>
#include <lattice/native_retirement_storage.hpp>
#include <lattice/ordinary_pointer_publication.hpp>

namespace lattice {struct lattice_close_result;struct physical_store_identity;}
namespace lattice::detail {
namespace configured_recovery_test_hooks {
// Scoped source-test permission copied at actual owner creation. It changes no
// source/TLS/storage admission and cannot complete or bypass cleanup facts.
class successor_permission {
    bool prior_;
public:
    successor_permission() noexcept;
    ~successor_permission();
    successor_permission(const successor_permission&)=delete;
    successor_permission& operator=(const successor_permission&)=delete;
};
// Source-test failure at the actual boundary after both capacity reservations
// and before service registration or any child/factory allocation.
extern thread_local std::function<void()> before_service_registration;
// Null by default. Actual publication rendezvous only; no authority/cleanup fact.
extern thread_local std::function<void()> before_listener_publication;
// Default null. Failure injection at the actual final-app holder preparation;
// it cannot publish completion, change admission, or alter a native deadline.
extern thread_local std::function<void()> before_listener_retirement_preparation;
// Failure injection after actual transport construction but before a route or
// native retirement reservation is installed. It supplies no cleanup fact.
extern thread_local std::function<void()> after_transport_creation;
extern thread_local std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> before_backoff_publication;
}
class configured_recovery_connection;
class configured_managed_capture_release;
class recovery_continuous_route;
class ordinary_managed_wss_admission;
class ordinary_managed_owner_release;
class ordinary_managed_lock_custody;
struct sync_policy_reservation;
class ordinary_managed_ipc_access;
// One application listener identity across separately retired managed owners.
// No profile, credential, protocol or completion authority lives in this cell.
struct configured_managed_handlers final {
private:
    struct owned_fields {
        ordinary_shared_publication<const synchronizer::on_state_change_handler> state;
        ordinary_shared_publication<const synchronizer::on_error_handler> error;
        ordinary_shared_publication<const synchronizer::on_progress_handler> progress;
        mutable std::mutex publication_mutex;
        bool terminal=false,disposing=false,disposed=false;
        std::weak_ptr<configured_recovery_connection> retirement_owner;
    };
    struct native_fields_deleter {
        pid_t creator;
        void operator()(owned_fields* value)const noexcept {
            if(!value||creator!=::getpid())return;
            auto state=value->state.exchange({});state.reset();if(creator!=::getpid())return;
            auto error=value->error.exchange({});error.reset();if(creator!=::getpid())return;
            auto progress=value->progress.exchange({});progress.reset();if(creator!=::getpid())return;
            value->retirement_owner.reset();if(creator!=::getpid())return;
            delete value; // All actual target ownership is already empty.
        }
    };
    std::unique_ptr<owned_fields,native_fields_deleter> fields_;
    static owned_fields& allocate_fields(std::unique_ptr<owned_fields,native_fields_deleter>& holder) {holder.reset(new owned_fields{});return *holder;}
public:
    configured_managed_handlers():fields_(nullptr,native_fields_deleter{::getpid()}),
        state(allocate_fields(fields_).state),error(fields_->error),progress(fields_->progress),
        publication_mutex_(fields_->publication_mutex),terminal_(fields_->terminal),
        disposing_(fields_->disposing),disposed_(fields_->disposed),retirement_owner_(fields_->retirement_owner){}
    ordinary_shared_publication<const synchronizer::on_state_change_handler>& state;
    ordinary_shared_publication<const synchronizer::on_error_handler>& error;
    ordinary_shared_publication<const synchronizer::on_progress_handler>& progress;
private:
    friend struct configured_listener_retirement_test_access;
    friend struct configured_listener_retirement_fork_test_access;
    friend class configured_recovery_connection;
    std::mutex& publication_mutex_;
    bool& terminal_;bool& disposing_;bool& disposed_;
    std::weak_ptr<configured_recovery_connection>& retirement_owner_;
    bool owns_creator_process()const noexcept{return fields_.get_deleter().creator==::getpid();}
    void wake_retirement()noexcept;
    template<typename Handler> bool publish(
        ordinary_shared_publication<const Handler>& slot,std::shared_ptr<const Handler> replacement) {
        if(!owns_creator_process())return false;
        native_retirement_storage<std::shared_ptr<const Handler>> targets;
        targets.template get<0>()=std::move(replacement);
        if(configured_recovery_test_hooks::before_listener_publication)
            configured_recovery_test_hooks::before_listener_publication();
        if(!owns_creator_process())return false;
        bool accepted=false;
        {std::lock_guard<std::mutex> lock(publication_mutex_);
            if(!terminal_){targets.template get<0>()=slot.exchange(std::move(targets.template get<0>()),std::memory_order_acq_rel);accepted=true;}
        }
        // Refused/displaced actual targets die off leaf BEFORE any result or
        // subsequent owning access; remaining storage is already quarantined.
        if(!targets.template release<0>())return false;
        return accepted;
    }
public:
    bool publish_state(std::shared_ptr<const synchronizer::on_state_change_handler> value) {return publish(state,std::move(value));}
    bool publish_error(std::shared_ptr<const synchronizer::on_error_handler> value) {return publish(error,std::move(value));}
    bool publish_progress(std::shared_ptr<const synchronizer::on_progress_handler> value) {return publish(progress,std::move(value));}
    void retire_publication() {if(!owns_creator_process())return;std::lock_guard<std::mutex> lock(publication_mutex_);terminal_=true;}
    bool application_retiring()const {if(!owns_creator_process())return false;std::lock_guard<std::mutex> lock(publication_mutex_);return terminal_;}
    void bind_retirement_owner(std::weak_ptr<configured_recovery_connection> owner) {
        if(!owns_creator_process())return;std::lock_guard<std::mutex> lock(publication_mutex_);retirement_owner_=std::move(owner);
    }
    bool retirement_disposed()const {if(!owns_creator_process())return false;std::lock_guard<std::mutex> lock(publication_mutex_);return disposed_;}
    bool dispose_retired() {
        if(!owns_creator_process())return false;
        native_retirement_storage<std::shared_ptr<const synchronizer::on_state_change_handler>,
            std::shared_ptr<const synchronizer::on_error_handler>,std::shared_ptr<const synchronizer::on_progress_handler>> targets;
        {std::lock_guard<std::mutex> lock(publication_mutex_);
            if(!terminal_)return false;if(disposed_)return true;if(disposing_)return false;
            disposing_=true;
            targets.get<0>()=state.exchange({},std::memory_order_acq_rel);
            targets.get<1>()=error.exchange({},std::memory_order_acq_rel);
            targets.get<2>()=progress.exchange({},std::memory_order_acq_rel);
        }
        if(!targets.release<0>()||!targets.release<1>()||!targets.release<2>())return false;
        {std::lock_guard<std::mutex> lock(publication_mutex_);disposed_=true;disposing_=false;}
        wake_retirement();return owns_creator_process();
    }
};
struct configured_attempt : public std::enable_shared_from_this<configured_attempt> {
private:
    friend class configured_recovery_connection;
    // FIRST data member: its destructor runs after every admission, factory,
    // lifetime, observer/callback and custody field has actually been destroyed.
    // The enable_shared_from_this base later releases only its weak self link.
    struct managed_capture_tail {
        std::shared_ptr<configured_managed_capture_release> completion;
        explicit managed_capture_tail(std::shared_ptr<configured_managed_capture_release> value={})noexcept
            :completion(std::move(value)){}
        ~managed_capture_tail()noexcept;
        managed_capture_tail(const managed_capture_tail&)=delete;
        managed_capture_tail& operator=(const managed_capture_tail&)=delete;
    } managed_capture_;
public:
    const platform_retirement_receipt receipt;
    const std::shared_ptr<configured_retirement_registry> registry;
    const std::shared_ptr<configured_attempt_custody> custody;
    const std::shared_ptr<network_factory> factory;
    configured_platform_factory* const typed_factory;
    std::weak_ptr<configured_recovery_connection> control;
    std::shared_ptr<configured_recovery_observer> observer;
    std::shared_ptr<const ordinary_managed_wss_admission> managed_wss;
    std::chrono::steady_clock::time_point managed_deadline{};
    std::optional<sync_retirement_lane::reservation> native_reservation;
private:
    std::shared_ptr<sync_callback_lifetime> lifetime_;
public:
    std::unique_ptr<synchronizer> physical;
    mutable std::mutex facts_mutex;
    sync_retirement_result lane_result;
    bool lane_complete=false,route_registered=false,route_unregistered=false;
    std::atomic<bool> construction_finished{false},wrapper_destroyed{false};
    std::atomic<bool> factory_entered{false},retirement_started{false};
    bool native_asserted=false;
    // The base destructor writes this exact cell, including when its derived
    // constructor unwinds without ever returning a physical pointer. Install
    // before fallible setup; read only after actual destruction has returned.
    std::shared_ptr<std::exception_ptr> destructor_error;
    std::exception_ptr primary_error,cleanup_error;
    configured_attempt(platform_retirement_receipt,std::shared_ptr<configured_retirement_registry>,
        std::shared_ptr<network_factory>,configured_platform_factory*,
        std::shared_ptr<configured_managed_capture_release> = {});
    void wake() noexcept;
    void lane_settled(sync_retirement_result) noexcept;
    void route_settled() noexcept;
    void request_renewal() noexcept;
    void transport_created() noexcept;
    void install_lifetime(std::shared_ptr<sync_callback_lifetime>);
    std::shared_ptr<sync_callback_lifetime> lifetime()const noexcept;
    // Diagnostic-only, one try-lock; failure leaves output unobserved.
    bool try_lifetime(std::shared_ptr<sync_callback_lifetime>&)const noexcept;
    void bind(std::shared_ptr<sync_transport>,std::shared_ptr<sync_callback_lifetime>,
        const std::shared_ptr<recovery_continuous_route>&);
    void bind_managed(std::shared_ptr<sync_transport>,std::shared_ptr<sync_callback_lifetime>,
        const std::shared_ptr<lattice_db>&,const sync_config&);
};

// Private stable owner. No legacy facade/synchronizer object layout changes.
// Every physical pointer is either on its serialized construction/finalization
// path or protected by an exact bounded command borrow from this owner.
class configured_recovery_connection : public std::enable_shared_from_this<configured_recovery_connection> {
    friend struct configured_attempt;
    friend class configured_managed_capture_release;
    friend class ordinary_managed_owner_release;
    friend class ordinary_managed_lock_custody;
    friend class ordinary_managed_ipc_access;
    friend class ordinary_receiver_wss_access;
    friend class configured_control_service;
    friend struct configured_managed_handlers;
    struct state;
    const pid_t creator_pid_=::getpid();
    std::unique_ptr<state> state_;
    configured_recovery_connection(std::weak_ptr<lattice_db>,sync_config,
        std::shared_ptr<network_factory>,configured_platform_factory*);
    static std::shared_ptr<configured_recovery_connection> create_impl(
        const std::shared_ptr<lattice_db>&,const sync_config&,const std::shared_ptr<network_factory>&,
        configured_platform_factory*,const std::function<std::shared_ptr<lattice_db>()>&,
        std::shared_ptr<const ordinary_managed_wss_admission>,std::shared_ptr<configured_managed_handlers>);
    static void wake_managed_owner_release(std::size_t,const configured_recovery_connection*)noexcept;
    static void wake_managed_lock(std::size_t,std::uint64_t)noexcept;
    static ordinary_managed_lock_custody reserve_managed_lock(
        std::shared_ptr<const physical_store_identity>,std::string,
        std::shared_ptr<const sync_policy_reservation>,std::chrono::steady_clock::time_point,
        const std::shared_ptr<ordinary_managed_owner_release>&);
    void dispatch() noexcept;
    void advance() noexcept;
    void finalize() noexcept;
    void cleanup_returned() noexcept;
    void control_payload_destroyed(bool) noexcept;
    void construct_attempt();
    void observe(configured_recovery_stage,const std::shared_ptr<configured_attempt>&,
        std::optional<configured_retirement_snapshot> = {}) noexcept;
    void notify() noexcept;
    void state_event(const std::shared_ptr<configured_attempt>&,bool);
    void error_event(const std::shared_ptr<configured_attempt>&,const std::string&);
    void progress_event(const std::shared_ptr<configured_attempt>&,const synchronizer::sync_progress&);
    struct borrow {
        // Physical captures are gone before this borrow's charge is released.
        configured_attempt_custody::lease charge;
        std::shared_ptr<configured_attempt> attempt;
        synchronizer* pointer=nullptr;
        borrow(configured_attempt_custody::lease c,std::shared_ptr<configured_attempt> a,synchronizer* p)
            :charge(std::move(c)),attempt(std::move(a)),pointer(p){}
    };
    borrow command() const;
public:
    ~configured_recovery_connection();
    static std::shared_ptr<configured_recovery_connection> create(
        const std::shared_ptr<lattice_db>&,const sync_config&,
        const std::shared_ptr<network_factory>&,configured_platform_factory*,
        const std::function<std::shared_ptr<lattice_db>()>& make_child);
    // Publication in the real parent's admission precedes this first dial.
    void start();
    // Called only after setup/start has unwound; settles a not-yet-started
    // owner if copied callback installation or publication failed.
    void abandon_start(std::exception_ptr) noexcept;
    bool connected() const;
    void sync_now();
    void trigger_upload();
    void connect();
    void disconnect();
    // No SQL, join, foreign callback or synchronous drain. Actual cleanup is
    // retained by the service; this call never reports completed retirement.
    void request_close() noexcept;
    bool cleanup_complete() const noexcept;
    void request_renewal(const std::shared_ptr<configured_attempt>&) noexcept;
    void set_state(synchronizer::on_state_change_handler);
    void set_error(synchronizer::on_error_handler);
    void set_progress(synchronizer::on_progress_handler);
    synchronizer::sync_progress progress()const;
    lattice_close_result close(std::chrono::steady_clock::time_point)noexcept;
};
}
#endif
