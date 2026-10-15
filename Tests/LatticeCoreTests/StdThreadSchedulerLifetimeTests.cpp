#include <lattice/scheduler.hpp>

#ifndef __EMSCRIPTEN__
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

// Private access only; no production testing switch or process-global hooks.
namespace lattice::detail {
struct std_thread_scheduler_test_access {
    using queue = std_thread_scheduler;
    struct witness {
        std::shared_ptr<queue::state> shared;
        bool wait_joining() const {
            std::unique_lock<std::mutex> lock(shared->mutex);
            return shared->settlement.wait_for(lock, std::chrono::seconds(5),
                                              [&] { return shared->joining; });
        }
        bool wait_loop_settled() const {
            std::unique_lock<std::mutex> lock(shared->mutex);
            return shared->settlement.wait_for(lock, std::chrono::seconds(5),
                                              [&] { return shared->loop_settled; });
        }
        bool wait_detached_waiter() const {
            std::unique_lock<std::mutex> lock(shared->mutex);
            return shared->settlement.wait_for(lock, std::chrono::seconds(5),
                                              [&] { return shared->detached_waiters != 0; });
        }
        bool joined() const {
            std::lock_guard<std::mutex> lock(shared->mutex);
            return shared->joined;
        }
        bool detached() const {
            std::lock_guard<std::mutex> lock(shared->mutex);
            return shared->detached;
        }
        bool owns_joinable_handle() const {
            std::lock_guard<std::mutex> lock(shared->mutex);
            return shared->worker.joinable() && !shared->joining;
        }
        bool has_join_error() const {
            std::lock_guard<std::mutex> lock(shared->mutex);
            return bool(shared->first_join_error);
        }
    };
    static witness watch(const queue& value) { return {value.state_}; }
    static void shutdown_after_copy(queue& value, std::function<void()> after_copy) {
        // Same local ownership boundary as public shutdown, paused before
        // shutdown_state entry. Never access value after this copy.
        const auto shared = value.state_;
        after_copy();
        queue::shutdown_state(shared, false);
    }
    static std::thread reject_launch(std::function<void()> work) {
        // work owns loop state; throwing must destroy it outside every lock.
        (void)work;
        throw std::runtime_error("injected launch failure");
    }
    static bool rejected_launch_releases_state() {
        auto shared = std::make_shared<queue::state>();
        std::weak_ptr<queue::state> weak = shared;
        bool caught = false;
        try {
            queue value(std::move(shared), reject_launch);
        } catch (const std::runtime_error& error) {
            caught = std::string(error.what()) == "injected launch failure";
        }
        return caught && weak.expired();
    }
    static void reject_join(std::thread&) {
        throw std::runtime_error("first injected join failure");
    }
    static void reject_join_again(std::thread&) {
        throw std::runtime_error("later injected join failure");
    }
    static void failed_join(queue& value, bool later = false, bool destroying = false) {
        const auto shared = value.state_;
        queue::shutdown_state(shared, destroying,
                              later ? reject_join_again : reject_join);
    }
    static inline thread_local std::function<void()> before_join_failure;
    static void delayed_reject_join(std::thread&) {
        before_join_failure();
        throw std::runtime_error("first injected join failure");
    }
    static void failed_join_after(queue& value, std::function<void()> before) {
        const auto shared = value.state_;
        before_join_failure = std::move(before);
        try { queue::shutdown_state(shared, false, delayed_reject_join); }
        catch (...) { before_join_failure = {}; throw; }
        before_join_failure = {};
    }
};
} // namespace lattice::detail

namespace {
using queue = lattice::std_thread_scheduler;
using access = lattice::detail::std_thread_scheduler_test_access;
static_assert(!std::is_copy_constructible_v<queue> && !std::is_move_constructible_v<queue>);
using namespace std::chrono_literals;
void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
struct gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool open = false;
    void signal() {
        { std::lock_guard<std::mutex> lock(mutex); open = true; }
        cv.notify_all();
    }
    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        require(cv.wait_for(lock, 5s, [&] { return open; }), "gate timeout");
    }
};
struct capture_marker {
    std::atomic<bool>& destroyed;
    explicit capture_marker(std::atomic<bool>& value) : destroyed(value) {}
    ~capture_marker() { destroyed.store(true); }
};

void self_destruction_drains_owned_state() {
    const auto before = queue::alive_count().load();
    auto owner = std::make_shared<queue>();
    const auto probe = access::watch(*owner);
    const std::weak_ptr<queue> weak = owner;
    gate active, proceed;
    std::vector<int> order;
    std::atomic<bool> capture_destroyed{false};
    auto marker = std::make_shared<capture_marker>(capture_destroyed);
    owner->invoke([keep = owner, marker = std::move(marker), &active,
                   &proceed, &order, weak]() mutable {
        active.signal();
        proceed.wait();
        order.push_back(1);
        keep.reset();
        require(weak.expired(), "wrapper dies on its own worker");
        // No scheduler pointer is used after reset. Only independently owned
        // test captures survive, just as the production loop must use state.
        order.push_back(2);
    });
    active.wait();
    owner->invoke([&] { order.push_back(3); });
    owner->invoke([&] { order.push_back(4); });
    owner.reset();
    proceed.signal();
    require(probe.wait_loop_settled(), "independent loop reaches settlement");
    require(order == std::vector<int>({1, 2, 3, 4}), "active tail and FIFO siblings finish");
    require(capture_destroyed.load(), "active capture released before settlement");
    require(queue::alive_count().load() == before, "wrapper count balances");
    require(probe.detached() && !probe.joined(), "state settlement is not a join");
}

void external_shutdown_joins_and_drains() {
    queue value;
    const auto probe = access::watch(value);
    gate active, proceed;
    std::vector<int> order;
    std::atomic<bool> returned{false};
    value.invoke([&] { order.push_back(1); active.signal(); proceed.wait(); });
    active.wait();
    value.invoke([&] { order.push_back(2); });
    value.invoke([&] { order.push_back(3); });
    std::thread stopper([&] { value.shutdown(); returned.store(true); });
    require(probe.wait_joining(), "external caller claims join before release");
    require(!value.can_invoke(), "shutdown closes admission");
    require(!returned.load(), "join waits for held callback");
    value.invoke([&] { order.push_back(99); });
    proceed.signal();
    stopper.join();
    require(order == std::vector<int>({1, 2, 3}), "admitted siblings drain; new work rejected");
    require(probe.joined() && !probe.detached(), "external completion actually joined");
    value.shutdown();
    require(probe.joined(), "repeat shutdown is idempotent");
}

void external_join_and_self_shutdown_do_not_deadlock() {
    queue value;
    const auto probe = access::watch(value);
    gate active, proceed, second_started;
    std::atomic<int> returned{0};
    std::atomic<bool> self_returned{false};
    value.invoke([&] {
        active.signal(); proceed.wait();
        require(value.is_on_thread(), "callback owns worker");
        value.shutdown();
        self_returned.store(true);
    });
    active.wait();
    std::thread first([&] { value.shutdown(); returned.fetch_add(1); });
    require(probe.wait_joining(), "first external caller already joining");
    std::thread second([&] {
        second_started.signal(); value.shutdown(); returned.fetch_add(1);
    });
    second_started.wait();
    require(returned.load() == 0, "both external returns follow active callback");
    proceed.signal();
    first.join(); second.join();
    require(self_returned.load() && returned.load() == 2, "self request and both joins settle");
    require(probe.joined() && !probe.detached(), "one join, no competing detach");
}

void self_shutdown_preserves_later_external_join() {
    queue value;
    const auto probe = access::watch(value);
    gate self_stopped, proceed;
    value.invoke([&] {
        value.shutdown(); self_stopped.signal(); proceed.wait();
    });
    self_stopped.wait();
    require(!value.can_invoke(), "self shutdown closes admission");
    require(probe.owns_joinable_handle(), "request-only call preserves external join handle");
    require(!probe.detached(), "self request does not detach a live wrapper");
    std::thread stopper([&] { value.shutdown(); });
    require(probe.wait_joining(), "external join starts after self request");
    proceed.signal(); stopper.join();
    require(probe.joined() && !probe.detached(), "later external shutdown joins");
}

void self_destruction_during_external_join_keeps_claim() {
    auto owner = std::make_shared<queue>();
    auto* raw = owner.get();
    const auto probe = access::watch(*owner);
    const std::weak_ptr<queue> weak = owner;
    gate active, proceed;
    std::atomic<bool> tail{false};
    owner->invoke([keep = owner, weak, &active, &proceed, &tail]() mutable {
        active.signal(); proceed.wait(); keep.reset();
        require(weak.expired(), "last wrapper owner released on callback");
        tail.store(true);
    });
    active.wait(); owner.reset();
    // The callback keeps raw valid until the join claimant has copied state.
    // Once joining is observed, shutdown uses state only and may outlive raw.
    std::thread stopper([raw] { raw->shutdown(); });
    require(probe.wait_joining(), "external call admitted while wrapper alive");
    proceed.signal(); stopper.join();
    require(tail.load(), "callback tail returns after wrapper destruction");
    require(probe.joined() && !probe.detached(), "self destructor respects moved join handle");
}

void destruction_before_external_claim_still_waits_for_drain() {
    auto owner = std::make_shared<queue>();
    auto* raw = owner.get();
    const auto probe = access::watch(*owner);
    const std::weak_ptr<queue> weak = owner;
    gate active, copied, destroy_wrapper, destroyed, enter_shutdown, finish_active;
    std::atomic<bool> returned{false};
    std::atomic<bool> capture_destroyed{false};
    std::vector<int> order;
    auto marker = std::make_shared<capture_marker>(capture_destroyed);
    owner->invoke([keep = owner, marker = std::move(marker), weak, &active,
                   &destroy_wrapper, &destroyed, &finish_active, &order]() mutable {
        active.signal(); destroy_wrapper.wait(); keep.reset();
        require(weak.expired(), "wrapper destroyed before external handle claim");
        destroyed.signal(); finish_active.wait(); order.push_back(1);
    });
    active.wait();
    owner->invoke([&] { order.push_back(2); });
    owner->invoke([&] { order.push_back(3); });
    std::thread stopper([&] {
        access::shutdown_after_copy(*raw, [&] { copied.signal(); enter_shutdown.wait(); });
        require(capture_destroyed.load(), "external return follows active capture destruction");
        returned.store(true);
    });
    copied.wait(); owner.reset(); destroy_wrapper.signal(); destroyed.wait();
    require(probe.detached() && !probe.joined(), "self destructor wins handle custody");
    enter_shutdown.signal();
    require(probe.wait_detached_waiter(), "external caller reaches detached settlement wait");
    require(!returned.load(), "external call cannot return while active work remains");
    finish_active.signal(); stopper.join();
    require(order == std::vector<int>({1, 2, 3}), "active and FIFO siblings drain before return");
    require(returned.load() && probe.detached() && !probe.joined(),
            "settlement wait does not claim an actual thread join");
}

struct reenter_on_destroy {
    queue* target;
    std::atomic<bool>* destroyed;
    ~reenter_on_destroy() {
        (void)target->can_invoke();
        target->shutdown();
        destroyed->store(true);
    }
};
void capture_destruction_and_exceptions_preserve_drain() {
    queue value;
    const auto probe = access::watch(value);
    gate active, proceed;
    std::atomic<bool> destroyed{false};
    std::atomic<bool> sibling{false};
    auto marker = std::make_shared<reenter_on_destroy>();
    marker->target = &value; marker->destroyed = &destroyed;
    value.invoke([keep = std::move(marker), &active, &proceed] {
        active.signal(); proceed.wait();
        throw std::runtime_error("expected task error");
    });
    active.wait();
    value.invoke([&] { sibling.store(true); throw 7; });
    std::thread stopper([&] { value.shutdown(); });
    require(probe.wait_joining(), "join precedes capture destructor reentry");
    proceed.signal(); stopper.join();
    require(destroyed.load() && sibling.load(), "off-lock destruction and both exception kinds drain");
    require(probe.joined(), "task exceptions preserve external join");
}

void launch_failure_balances_state_and_wrapper_count() {
    const auto before = queue::alive_count().load();
    require(access::rejected_launch_releases_state(), "launch exception releases every state holder");
    require(queue::alive_count().load() == before, "failed launch does not count an unconstructed wrapper");
}

void rejected_capture_destructs_after_admission_lock() {
    queue value;
    value.shutdown();
    std::atomic<bool> destroyed{false};
    std::atomic<bool> invoked{false};
    auto marker = std::make_shared<reenter_on_destroy>();
    marker->target = &value; marker->destroyed = &destroyed;
    value.invoke([keep = std::move(marker), &invoked] { invoked.store(true); });
    require(destroyed.load() && !invoked.load(), "rejected capture can reenter shutdown off-lock");
}

void join_failure_retains_handle_and_first_error() {
    queue value;
    const auto probe = access::watch(value);
    gate active, proceed;
    value.invoke([&] { active.signal(); proceed.wait(); });
    active.wait();
    for (const bool later : {false, true}) {
        bool first = false;
        try { access::failed_join(value, later); }
        catch (const std::runtime_error& error) {
            first = std::string(error.what()) == "first injected join failure";
        }
        require(first, "first join error retained across retries");
        require(probe.owns_joinable_handle(), "failed join restores handle before throw");
        require(!probe.joined() && !probe.detached(), "failure is not reported as completion");
    }
    proceed.signal(); value.shutdown();
    require(probe.joined() && probe.has_join_error(), "real retry joins without erasing first error");
}

void last_owner_join_failure_detaches_independent_state() {
    queue value;
    const auto probe = access::watch(value);
    gate active, proceed;
    value.invoke([&] { active.signal(); proceed.wait(); });
    active.wait();
    // Exercise the destructor's failure branch without invoking a platform
    // failure or destroying a raw callback target. Its state remains owned.
    access::failed_join(value, false, true);
    require(probe.detached() && !probe.joined() && probe.has_join_error(),
            "fallback retains error and explicitly does not claim join");
    proceed.signal();
    require(probe.wait_loop_settled(), "detached independent state finishes");
}

void join_failure_after_self_destruction_retains_custody() {
    auto owner = std::make_shared<queue>();
    auto* raw = owner.get();
    const auto probe = access::watch(*owner);
    const std::weak_ptr<queue> weak = owner;
    gate active, proceed, destroyed, finish;
    std::atomic<bool> first_error{false};
    owner->invoke([keep = owner, weak, &active, &proceed, &destroyed, &finish]() mutable {
        active.signal(); proceed.wait(); keep.reset();
        require(weak.expired(), "wrapper destroyed while external join is claimed");
        destroyed.signal(); finish.wait();
    });
    active.wait(); owner.reset();
    std::thread stopper([&] {
        try { access::failed_join_after(*raw, [&] { destroyed.wait(); }); }
        catch (const std::runtime_error& error) {
            first_error.store(std::string(error.what()) == "first injected join failure");
        }
    });
    require(probe.wait_joining(), "join claimant admitted before wrapper destruction");
    proceed.signal(); stopper.join();
    require(first_error.load(), "external caller receives original join failure");
    require(probe.detached() && !probe.joined() && !probe.owns_joinable_handle(),
            "failure after destructor uses state-only fallback, not an abandoned handle");
    finish.signal();
    require(probe.wait_loop_settled(), "state drains after last wrapper and failed claimant");
}

using case_fn = void (*)();
void bounded(case_fn body) {
#if defined(__APPLE__) || defined(__linux__)
    struct restore_style {
        std::string prior = GTEST_FLAG_GET(death_test_style);
        ~restore_style() { GTEST_FLAG_SET(death_test_style, prior); }
    } restore;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        try {
            sigset_t unblocked;
            sigemptyset(&unblocked); sigaddset(&unblocked, SIGALRM);
            require(std::signal(SIGALRM, SIG_DFL) != SIG_ERR &&
                    sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) == 0, "watchdog installation");
            alarm(15);
            body();
            std::fputs("std_scheduler_lifetime_complete\n", stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "std_scheduler_lifetime_failure: %s\n", error.what());
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "std_scheduler_lifetime_complete");
#else
    GTEST_SKIP() << "Bounded worker-lifetime qualification uses the supported POSIX child watchdog";
#endif
}
} // namespace

TEST(StdThreadSchedulerLifetime, SelfDestructionDrainsOwnedState) { bounded(self_destruction_drains_owned_state); }
TEST(StdThreadSchedulerLifetime, ExternalShutdownJoinsAndDrains) { bounded(external_shutdown_joins_and_drains); }
TEST(StdThreadSchedulerLifetime, ConcurrentExternalJoinAndSelfShutdown) { bounded(external_join_and_self_shutdown_do_not_deadlock); }
TEST(StdThreadSchedulerLifetime, SelfShutdownPreservesLaterExternalJoin) { bounded(self_shutdown_preserves_later_external_join); }
TEST(StdThreadSchedulerLifetime, SelfDestructionDuringExternalJoin) { bounded(self_destruction_during_external_join_keeps_claim); }
TEST(StdThreadSchedulerLifetime, DestructionBeforeExternalClaimWaitsForDrain) { bounded(destruction_before_external_claim_still_waits_for_drain); }
TEST(StdThreadSchedulerLifetime, CaptureDestructionAndExceptionsPreserveDrain) { bounded(capture_destruction_and_exceptions_preserve_drain); }
TEST(StdThreadSchedulerLifetime, LaunchFailureBalancesStateAndCount) { bounded(launch_failure_balances_state_and_wrapper_count); }
TEST(StdThreadSchedulerLifetime, RejectedCaptureDestructsOffLock) { bounded(rejected_capture_destructs_after_admission_lock); }
TEST(StdThreadSchedulerLifetime, JoinFailureRetainsHandleAndFirstError) { bounded(join_failure_retains_handle_and_first_error); }
TEST(StdThreadSchedulerLifetime, LastOwnerJoinFailureDetachesStateOnly) { bounded(last_owner_join_failure_detaches_independent_state); }
TEST(StdThreadSchedulerLifetime, JoinFailureAfterSelfDestructionRetainsCustody) { bounded(join_failure_after_self_destruction_retains_custody); }
#endif // !__EMSCRIPTEN__
