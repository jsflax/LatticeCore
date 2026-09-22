#pragma once
#include <lattice/scheduler.hpp>
#include <lattice/network.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>

namespace lattice {class lattice_db;class synchronizer_base;}
namespace lattice::detail {
namespace sync_background_test_hooks {
// Private bounded rendezvous copied at worker creation. Production is null.
// Callbacks may only coordinate a test; completion must not throw.
struct ack_schedule {
    std::function<void()> before_expiry,completed;
    // After timeout bookkeeping, before scheduling retry; no owner/ACK,
    // in-flight, receiver, endpoint or SQL lock is held during this hook.
    std::function<void()> after_timeout_transition;
};
extern thread_local std::shared_ptr<const ack_schedule> ack;
// Source fixture rendezvous immediately before the late no-effect probe.
extern thread_local std::function<void()> before_late_discovery;
// Copied at pacer creation. Tests may hold initial startup, then pause its
// final false predicate under the wait mutex. Production is null; no throwing.
struct pacer_wait_schedule {std::function<void()> starting,before_wait;};
extern thread_local std::shared_ptr<const pacer_wait_schedule> pacer_wait;
}

// The pacer may wake after its synchronizer has retired. Waiting, coalescing
// and the post-dispatch continuation therefore use only this retained state;
// access to the synchronizer itself requires the independent callback cell.
struct sync_pacer_state {
    std::mutex mutex;
    std::condition_variable ready;
    bool stop=false;
    std::atomic<bool> requested{false};
    std::atomic<int> coalesce_milliseconds{0};
    std::chrono::steady_clock::time_point next_allowed_tick{};
    // Queue revisions have their own mutex. Join the condition-variable
    // handoff after changing them so a notification cannot pass between the
    // waiter's final predicate check and its atomic unlock-and-wait. Call only
    // after releasing the queue mutex and without already holding this mutex.
    void wake_changed() noexcept {
        {std::lock_guard<std::mutex> lock(mutex);}
        ready.notify_one();
    }
};
// Admission fences physical C++ owner teardown. A shared cell alone is not an
// owner: destructor retirement waits for foreign executions before members die.
class sync_callback_lifetime {
    struct execution;
    static thread_local execution* current_;
    std::mutex mutex_;
    std::condition_variable settled_;
    synchronizer_base* owner_;
    std::weak_ptr<lattice_db> database_;
    uint64_t generation_=1, active_=0, protected_generation_=0;
    bool retired_=false, ever_connected_=false, protected_=false, attempt_live_=false;
    bool run(uint64_t,const std::function<void()>&,bool require_live=true,const platform_transport_callbacks* attempt=nullptr,bool terminal=false);
public:
    sync_callback_lifetime(synchronizer_base*,const std::shared_ptr<lattice_db>&);
    uint64_t dispatch_generation();
    void publish_generation(uint64_t);
    bool can_begin_protected(uint64_t);
    // Protected transports have exactly one physical attempt. The existing
    // transport API cannot attribute callbacks after same-endpoint reconnect.
    void begin_connect(uint64_t,bool protected_route);
    bool protected_current(uint64_t);
    void end_protected_attempt();
    void retire() noexcept;
    void wait_for_foreign();
    void retire_and_wait();
    bool current(uint64_t);
    bool protected_route();
    bool executing_here()const noexcept;
    void transport(const std::function<void()>&);
    void queued(uint64_t,const std::function<void()>&);
    // Per-dial endpoint identity and owner generation are checked in ONE owner
    // admission. Replaced/retired endpoints cannot borrow an automatic retry's
    // unchanged lifecycle. Already owner-admitted executions still settle.
    void platform_callback(uint64_t,const platform_transport_callbacks&,const std::function<void()>&);
    void platform_terminal_callback(uint64_t,const platform_transport_callbacks&,const std::function<void()>&);
    void terminal_notification(uint64_t,const std::function<void()>&);
};
void report_sync_background_error(std::shared_ptr<scheduler>,std::shared_ptr<sync_callback_lifetime>,uint64_t,
    std::function<void(const std::string&)>,std::exception_ptr,const char*) noexcept;
void schedule_sync_terminal_notification(std::shared_ptr<scheduler>,std::shared_ptr<sync_callback_lifetime>,uint64_t,std::function<void()>);
std::shared_ptr<scheduler> make_sync_lifetime_scheduler(std::shared_ptr<scheduler>,std::shared_ptr<sync_callback_lifetime>);

#ifndef __EMSCRIPTEN__
struct sync_retirement_test_access;
class sync_retirement_lane : public std::enable_shared_from_this<sync_retirement_lane> {
    enum class phase { free,reserved,queued,active,quarantined };
    struct slot {phase state=phase::free;uint64_t serial=0,queued_order=0;std::shared_ptr<sync_transport> transport;std::shared_ptr<sync_callback_lifetime> lifetime;std::thread pacer;};
    std::array<slot,64> slots_{};
    std::mutex mutex_;
    std::condition_variable ready_,settled_;
    std::thread worker_;
    bool stopping_=false;
    size_t capacity_=64;
    uint64_t next_order_=0;
    using launch_function=std::thread(*)(std::function<void()>);
    explicit sync_retirement_lane(size_t,launch_function);
    static std::thread launch(std::function<void()>);
    void loop();
    void request(size_t,uint64_t,std::thread = {}) noexcept;
    void cancel(size_t,uint64_t) noexcept;
    friend struct sync_retirement_test_access;
public:
    class reservation {
        friend class sync_retirement_lane;
        std::shared_ptr<sync_retirement_lane> lane_;
        size_t slot_=0;uint64_t serial_=0;bool published_=false;
        reservation(std::shared_ptr<sync_retirement_lane>,size_t,uint64_t);
    public:
        reservation(reservation&&) noexcept;
        reservation& operator=(reservation&&)=delete;
        reservation(const reservation&)=delete;
        ~reservation();
        void publish()noexcept{published_=true;}
        void retire(std::thread = {})noexcept;
    };
    ~sync_retirement_lane();
    static std::shared_ptr<sync_retirement_lane> instance();
    // All 64 states share the ceiling. Reserve before protected publication;
    // a slow/failed retirement keeps its slot and refuses excess new opens.
    reservation reserve(std::shared_ptr<sync_transport>,std::shared_ptr<sync_callback_lifetime> = {});
};
#endif
} // namespace lattice::detail
