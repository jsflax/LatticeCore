#include <lattice/scheduler.hpp>
#include <gtest/gtest.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>
#if defined(__APPLE__) || defined(__linux__)
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
// Exercise the real void* C-function adapter, not a substitute work wrapper.
struct c_adapter {
    void* pending = nullptr;
    unsigned submitted = 0;
    static void submit(void* work, void* context) {
        auto& self = *static_cast<c_adapter*>(context);
        if (self.pending) std::terminate(); // This fixture admits one item.
        self.pending = work;
        ++self.submitted;
    }
    static bool on_thread(void*) { return true; }
    static bool same(const lattice::scheduler*, void*) { return false; }
    static bool available(void*) { return true; }
    lattice::generic_scheduler scheduler() {
        return {this, submit, on_thread, same, available};
    }
    void execute() {
        auto* work = std::exchange(pending, nullptr);
        if (!work) throw std::logic_error("fixture has no actual adapter work");
        lattice::generic_scheduler::execute_work(work);
        // No fixture owner cleans up a taken item, including on exceptions.
    }
    ~c_adapter() {
        // Only unstarted work remains fixture-owned after an early assertion.
        delete static_cast<std::function<void()>*>(pending);
    }
};
struct capture_marker {
    std::atomic<unsigned>& destroyed;
    explicit capture_marker(std::atomic<unsigned>& count) : destroyed(count) {}
    ~capture_marker() { ++destroyed; }
};
}

TEST(GenericSchedulerLifetime, ActualCAdapterReturnReleasesFinalCaptureBeforeReturning) {
    std::atomic<unsigned> destroyed{0};
    c_adapter adapter;
    auto scheduler = adapter.scheduler();
    unsigned called = 0;
    auto marker = std::make_shared<capture_marker>(destroyed);
    const std::weak_ptr<capture_marker> weak = marker;
    scheduler.invoke([held = std::move(marker), &called] { ++called; });
    ASSERT_EQ(adapter.submitted, 1u);
    ASSERT_NE(adapter.pending, nullptr);
    ASSERT_FALSE(weak.expired());
    EXPECT_EQ(destroyed.load(), 0u);
    adapter.execute();
    EXPECT_EQ(called, 1u);
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(destroyed.load(), 1u);
    EXPECT_EQ(adapter.pending, nullptr);
}

TEST(GenericSchedulerLifetime, ActualCAdapterThrowReleasesCaptureAndPropagatesExactException) {
    std::atomic<unsigned> destroyed{0};
    c_adapter adapter;
    auto scheduler = adapter.scheduler();
    auto marker = std::make_shared<capture_marker>(destroyed);
    const std::weak_ptr<capture_marker> weak = marker;
    const auto expected = std::make_exception_ptr(std::runtime_error("actual callback failure"));
    scheduler.invoke([held = std::move(marker), expected] { std::rethrow_exception(expected); });
    ASSERT_FALSE(weak.expired());
    std::exception_ptr actual;
    try { adapter.execute(); } catch (...) { actual = std::current_exception(); }
    EXPECT_EQ(actual, expected);
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(destroyed.load(), 1u);
    EXPECT_EQ(adapter.pending, nullptr);
}

TEST(GenericSchedulerLifetime, NonStandardCallbackThrowStillReleasesActualAdapterCapture) {
    std::atomic<unsigned> destroyed{0};
    c_adapter adapter;
    auto scheduler = adapter.scheduler();
    auto marker = std::make_shared<capture_marker>(destroyed);
    const std::weak_ptr<capture_marker> weak = marker;
    scheduler.invoke([held = std::move(marker)] { throw 73; });
    bool actual = false;
    try { adapter.execute(); } catch (int value) { actual = value == 73; }
    EXPECT_TRUE(actual);
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(destroyed.load(), 1u);
}

TEST(GenericSchedulerLifetime, NullAndActualEmptyAdapterWorkKeepTheirExistingNoOpSemantics) {
    EXPECT_NO_THROW(lattice::generic_scheduler::execute_work(nullptr));
    c_adapter adapter;
    auto scheduler = adapter.scheduler();
    scheduler.invoke(std::function<void()>{});
    ASSERT_EQ(adapter.submitted, 1u);
    ASSERT_NE(adapter.pending, nullptr);
    EXPECT_NO_THROW(adapter.execute());
    EXPECT_EQ(adapter.pending, nullptr);
}

#if defined(__APPLE__) || defined(__linux__)
namespace {
int callback_fork_control(bool throwing) {
    using clock = std::chrono::steady_clock;
    c_adapter adapter;
    auto scheduler = adapter.scheduler();
    std::atomic<unsigned> destroyed{0};
    auto marker = std::make_shared<capture_marker>(destroyed);
    const std::weak_ptr<capture_marker> weak = marker;
    unsigned called = 0;
    pid_t child = -1;
    const auto work_end = clock::now() + std::chrono::seconds(2);
    const auto ending_end = work_end + std::chrono::seconds(1);
    scheduler.invoke([held = std::move(marker), &called, &child, throwing] {
        ++called;
        child = ::fork(); // Fork only after this actual parent work was entered.
        if (throwing) throw 73;
    });
    bool caught = false;
    try { adapter.execute(); }
    catch (int value) { caught = value == 73; }
    if (child == 0) {
        const bool unchanged = !weak.expired() && destroyed.load() == 0 &&
            called == 1 && caught == throwing && adapter.pending == nullptr;
        std::fprintf(stderr, "generic_scheduler_fork_child pid=%ld parent=%ld pgid=%ld mode=%d capturesUnreleased=%d\n",
            long(::getpid()), long(::getppid()), long(::getpgrp()), int(throwing), int(unchanged));
        // The inherited raw payload is intentionally abandoned. Do not unwind
        // inherited fixture owners or claim child reuse of a parent payload.
        ::_exit(unchanged ? 0 : 81);
    }
    if (child < 0) return 82;
    const bool parent_ended = weak.expired() && destroyed.load() == 1 &&
        called == 1 && caught == throwing && adapter.pending == nullptr;
    int status = 0, wait_error = 0, kill_result = 0, kill_error = 0;
    unsigned wait_attempts = 0;
    bool reaped = false, timed_out = false, kill_attempted = false;
    const auto poll = [&](clock::time_point end) {
        while (!reaped && !wait_error && clock::now() < end) {
            ++wait_attempts;
            const auto actual = ::waitpid(child, &status, WNOHANG);
            if (actual == child) { reaped = true; break; }
            if (actual < 0 && errno != EINTR) { wait_error = errno; break; }
            ::usleep(1000);
        }
    };
    poll(work_end);
    if (!reaped && !wait_error) {
        timed_out = true; // Sticky even if a late child exits zero before kill.
        kill_attempted = true;
        kill_result = ::kill(child, SIGKILL);
        if (kill_result < 0) kill_error = errno;
        poll(ending_end); // Same first-issued ending; never renew either clock.
    }
    std::fprintf(stderr, "generic_scheduler_fork_ending pid=%ld child=%ld mode=%d reaped=%d status=%d timeout=%d waitAttempts=%u waitErrno=%d killAttempt=%d killResult=%d killErrno=%d nativeNamespace=none\n",
        long(::getpid()), long(child), int(throwing), int(reaped), status, int(timed_out),
        wait_attempts, wait_error, int(kill_attempted), kill_result, kill_error);
    if (!reaped) {
        // Exact child and first ending debt stay in the test's retained stderr.
        // Do not unwind an unproved process ending or relabel it as settled.
        ::_exit(95);
    }
    return parent_ended && !timed_out && !wait_error && !kill_attempted &&
        WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 83;
}
void bounded_callback_fork(bool throwing) {
    const auto prior = GTEST_FLAG_GET(death_test_style);
    struct restore_style {
        std::string value;
        ~restore_style() { GTEST_FLAG_SET(death_test_style, value); }
    } restore{prior};
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        sigset_t unblocked;
        sigemptyset(&unblocked);
        sigaddset(&unblocked, SIGALRM);
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR ||
            ::sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) ::_exit(84);
        ::alarm(10);
        const int result = callback_fork_control(throwing);
        std::fprintf(stderr, "generic_scheduler_fork_complete mode=%d result=%d\n", int(throwing), result);
        ::_exit(result);
    }, ::testing::ExitedWithCode(0), "generic_scheduler_fork_complete");
}
}

TEST(GenericSchedulerLifetime, CallbackForkReturningDoesNotDestroyInheritedAdapterCaptures) {
    bounded_callback_fork(false);
}
TEST(GenericSchedulerLifetime, CallbackForkThrowingDoesNotDestroyInheritedAdapterCaptures) {
    bounded_callback_fork(true);
}
#endif
