#include "../../Sources/LatticeCore/src/sync_immediate_scheduler.hpp"

#ifndef __EMSCRIPTEN__
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif
#ifndef LATTICE_SYNC_IMMEDIATE_STANDALONE
#include <gtest/gtest.h>
#else
namespace lattice { std::atomic<log_level> g_log_level{log_level::off}; }
#endif

namespace {
using inline_queue = lattice::detail::sync_immediate_scheduler;
using namespace std::chrono_literals;
void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
struct gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool set = false;
    void signal() { std::lock_guard<std::mutex> lock(mutex); set = true; cv.notify_all(); }
    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        require(cv.wait_for(lock, 5s, [&] { return set; }), "gate timed out");
    }
};

void exact_type_preserves_custom_policy() {
    struct custom_inline : lattice::immediate_scheduler {
        int calls = 0;
        void invoke(std::function<void()>&& fn) override {
            ++calls;
            if (fn) fn();
        }
    };
    auto custom = std::make_shared<custom_inline>();
    auto selected = lattice::detail::make_synchronizer_scheduler(custom);
    require(selected.get() == custom.get(), "custom immediate subclass retains identity and policy");
    bool ran = false;
    selected->invoke([&] { ran = true; });
    require(ran && custom->calls == 1, "custom invoke override is called");
    auto plain = std::make_shared<lattice::immediate_scheduler>();
    auto serialized = lattice::detail::make_synchronizer_scheduler(plain);
    require(serialized.get() != plain.get(), "exact immediate gets private adapter");
    require(dynamic_cast<inline_queue*>(serialized.get()) != nullptr, "correct private adapter selected");
}

void fifo_reentry() {
    inline_queue queue;
    std::vector<int> order;
    require(!queue.is_on_thread(), "idle caller is not drain owner");
    queue.invoke([&] {
        require(queue.is_on_thread(), "active caller owns drain");
        order.push_back(1);
        queue.invoke([&] { order.push_back(3); });
        queue.invoke([&] { order.push_back(4); });
        require(order.size() == 1, "reentry queues instead of nesting");
        order.push_back(2);
    });
    require(order == std::vector<int>({1,2,3,4}), "FIFO after outer item");
    require(!queue.is_on_thread(), "owner cleared after drain");
}

void overlapping_sql_lock() {
    inline_queue queue;
    std::mutex sql;
    gate active;
    std::atomic<int> order{0};
    std::unique_lock<std::mutex> writer(sql);
    std::thread owner([&] {
        queue.invoke([&] {
            active.signal();
            std::lock_guard<std::mutex> take_sql(sql);
            order.store(1);
        });
    });
    active.wait();
    require(!queue.is_on_thread(), "other caller is not owner");
    // Waiting in invoke while another operation is blocked on sql would
    // deadlock here. The fresh child watchdog bounds that regression.
    queue.invoke([&] { require(order.load() == 1, "first operation settled"); order.store(2); });
    require(order.load() == 0, "queued call returns without running inline");
    writer.unlock();
    owner.join();
    require(order.load() == 2, "queued work runs after SQL lock release");
}

struct lifetime_marker {
    std::atomic<bool>& destroyed;
    explicit lifetime_marker(std::atomic<bool>& value) : destroyed(value) {}
    ~lifetime_marker() { destroyed.store(true); }
};

void shutdown_waits_for_active() {
    inline_queue queue;
    gate active, finish, stopping;
    std::atomic<bool> destroyed{false};
    std::atomic<bool> returned{false};
    std::atomic<bool> completed{false};
    std::atomic<bool> queued_ran{false};
    std::atomic<bool> shutdown_observed_finished{false};
    auto marker = std::make_shared<lifetime_marker>(destroyed);
    std::thread owner([&, marker = std::move(marker)]() mutable {
        queue.invoke([&, keep = std::move(marker)] {
            active.signal();
            finish.wait();
            require(!destroyed.load(), "in-flight capture remains owned");
            completed.store(true);
        });
    });
    active.wait();
    queue.invoke([&] { queued_ran.store(true); });
    std::thread stopper([&] {
        stopping.signal();
        queue.shutdown();
        shutdown_observed_finished.store(completed.load() && destroyed.load());
        returned.store(true);
    });
    stopping.wait();
    // Admission becomes false only after shutdown closes the queue. This
    // handshake proves it reached shutdown before the active item is released.
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (queue.can_invoke() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    require(!queue.can_invoke(), "shutdown closed admission");
    require(!returned.load(), "shutdown waits while active item is held");
    require(!destroyed.load(), "shutdown cannot destroy active capture");
    finish.signal();
    owner.join();
    stopper.join();
    require(shutdown_observed_finished.load(), "return follows active work and capture release");
    require(!queued_ran.load(), "shutdown cancels queued work");
    queue.invoke([&] { queued_ran.store(true); });
    require(!queued_ran.load(), "closed queue rejects subsequent work");
}

void exception_restores_drain() {
    inline_queue queue;
    std::vector<int> order;
    bool caught = false;
    try {
        queue.invoke([&] {
            order.push_back(1);
            queue.invoke([&] { order.push_back(2); });
            throw std::runtime_error("expected inline failure");
        });
    } catch (const std::runtime_error& error) {
        caught = std::string(error.what()) == "expected inline failure";
    }
    require(caught, "inline drain owner receives original exception");
    require(order == std::vector<int>({1,2}), "already-admitted work is not stranded");
    require(!queue.is_on_thread(), "exception releases drain ownership");
    queue.invoke([&] { order.push_back(3); });
    require(order == std::vector<int>({1,2,3}), "later invocation is admitted");
}

void same_thread_adapter_destruction() {
    auto queue = std::make_unique<inline_queue>();
    std::vector<int> order;
    queue->invoke([&] {
        order.push_back(1);
        queue->invoke([&] { order.push_back(99); });
        queue.reset();
        // Only the adapter was destroyed; this callback's captures remain
        // independently owned by this test. No synchronizer self-delete claim.
        order.push_back(2);
    });
    require(!queue, "adapter destroyed on its drain");
    require(order == std::vector<int>({1,2}), "active finishes, pending cancels");
}

struct reenter_on_destroy {
    inline_queue* queue;
    std::atomic<bool>* destroyed;
    ~reenter_on_destroy() {
        (void)queue->can_invoke();
        destroyed->store(true);
    }
};
void cancelled_capture_destroyed_off_lock() {
    inline_queue queue;
    gate active, finish;
    std::atomic<bool> capture_destroyed{false};
    std::thread owner([&] { queue.invoke([&] { active.signal(); finish.wait(); }); });
    active.wait();
    auto capture = std::make_shared<reenter_on_destroy>();
    capture->queue = &queue;
    capture->destroyed = &capture_destroyed;
    queue.invoke([capture = std::move(capture)] {});
    std::thread stopper([&] { queue.shutdown(); });
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!capture_destroyed.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    require(capture_destroyed.load(), "queued capture destructor reenters without lock deadlock");
    finish.signal();
    owner.join();
    stopper.join();
}

using case_fn = void (*)();
struct test_case { const char* name; case_fn body; };
const test_case cases[] = {
    {"exact-type", exact_type_preserves_custom_policy},
    {"fifo", fifo_reentry}, {"overlap", overlapping_sql_lock},
    {"shutdown", shutdown_waits_for_active}, {"exception", exception_restores_drain},
    {"self-destroy", same_thread_adapter_destruction},
    {"cancel-destroy", cancelled_capture_destroyed_off_lock}
};
void install_watchdog() {
#if defined(__APPLE__) || defined(__linux__)
    sigset_t unblocked;
    sigemptyset(&unblocked);
    sigaddset(&unblocked, SIGALRM);
    require(std::signal(SIGALRM, SIG_DFL) != SIG_ERR &&
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) == 0, "watchdog installation");
    alarm(10);
#endif
}
#ifndef LATTICE_SYNC_IMMEDIATE_STANDALONE
void bounded(case_fn body) {
    struct Style {
        std::string old = GTEST_FLAG_GET(death_test_style);
        ~Style() { GTEST_FLAG_SET(death_test_style, old); }
    } style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        try {
            install_watchdog(); body();
            std::fputs("sync_inline_complete\n", stderr); _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr,"sync_inline_failure: %s\n",error.what()); _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "sync_inline_complete");
}
#endif
} // namespace

#ifndef LATTICE_SYNC_IMMEDIATE_STANDALONE
TEST(SyncImmediateScheduler, ExactTypePreservesCustomPolicy) { bounded(exact_type_preserves_custom_policy); }
TEST(SyncImmediateScheduler, ReentrantFIFO) { bounded(fifo_reentry); }
TEST(SyncImmediateScheduler, OverlapDoesNotWaitUnderSQLLock) { bounded(overlapping_sql_lock); }
TEST(SyncImmediateScheduler, ShutdownWaitsForActiveState) { bounded(shutdown_waits_for_active); }
TEST(SyncImmediateScheduler, ExceptionRestoresDrain) { bounded(exception_restores_drain); }
TEST(SyncImmediateScheduler, SameThreadAdapterDestruction) { bounded(same_thread_adapter_destruction); }
TEST(SyncImmediateScheduler, CancelledCaptureDestroyedOffLock) { bounded(cancelled_capture_destroyed_off_lock); }
#else
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    for (const auto& test : cases) if (std::string(argv[1]) == test.name) {
        try {
            install_watchdog(); test.body();
            std::printf("sync_inline_complete:%s\n", test.name); return 0;
        } catch (const std::exception& error) {
            std::fprintf(stderr,"sync_inline_failure:%s:%s\n",test.name,error.what()); return 1;
        }
    }
    return 2;
}
#endif
#endif // !__EMSCRIPTEN__
