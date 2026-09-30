#pragma once
#ifndef __EMSCRIPTEN__
#include <lattice/sync.hpp>
#include <lattice/configured_platform.hpp>
#include "configured_retirement.hpp"
#include "configured_attempt_custody.hpp"
#include "sync_callback_lifetime.hpp"
#include <condition_variable>
#include <optional>

namespace lattice {struct lattice_close_result;}
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
// Failure injection after actual transport construction but before a route or
// native retirement reservation is installed. It supplies no cleanup fact.
extern thread_local std::function<void()> after_transport_creation;
extern thread_local std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> before_backoff_publication;
}
class configured_recovery_connection;
class recovery_continuous_route;
struct configured_attempt : public std::enable_shared_from_this<configured_attempt> {
    const platform_retirement_receipt receipt;
    const std::shared_ptr<configured_retirement_registry> registry;
    const std::shared_ptr<configured_attempt_custody> custody;
    const std::shared_ptr<network_factory> factory;
    configured_platform_factory* const typed_factory;
    std::weak_ptr<configured_recovery_connection> control;
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
        std::shared_ptr<network_factory>,configured_platform_factory*);
    void wake() noexcept;
    void lane_settled(sync_retirement_result) noexcept;
    void route_settled() noexcept;
    void request_renewal() noexcept;
    void install_lifetime(std::shared_ptr<sync_callback_lifetime>);
    std::shared_ptr<sync_callback_lifetime> lifetime()const noexcept;
    void bind(std::shared_ptr<sync_transport>,std::shared_ptr<sync_callback_lifetime>,
        const std::shared_ptr<recovery_continuous_route>&);
};

// Private stable owner. No legacy facade/synchronizer object layout changes.
// Every physical pointer is either on its serialized construction/finalization
// path or protected by an exact bounded command borrow from this owner.
class configured_recovery_connection : public std::enable_shared_from_this<configured_recovery_connection> {
    friend struct configured_attempt;
    friend class configured_control_service;
    struct state;
    std::unique_ptr<state> state_;
    configured_recovery_connection(std::weak_ptr<lattice_db>,sync_config,
        std::shared_ptr<network_factory>,configured_platform_factory*);
    void dispatch() noexcept;
    void advance() noexcept;
    void finalize() noexcept;
    void cleanup_returned() noexcept;
    void control_payload_destroyed(bool) noexcept;
    void construct_attempt();
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
    void request_renewal(const std::shared_ptr<configured_attempt>&) noexcept;
    void set_state(synchronizer::on_state_change_handler);
    void set_error(synchronizer::on_error_handler);
    void set_progress(synchronizer::on_progress_handler);
    synchronizer::sync_progress progress()const;
    lattice_close_result close(std::chrono::steady_clock::time_point)noexcept;
};
}
#endif
