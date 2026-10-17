#pragma once

#ifdef __cplusplus

#include "log.hpp"
#include <functional>
#include <memory>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <exception>

namespace lattice {

// ============================================================================
// Scheduler interface - abstract base for notification dispatch
// ============================================================================
//
// Similar to realm-cpp's scheduler, this allows platform-specific
// implementations:
// - Apple: CFRunLoop / dispatch_async to main queue
// - Swift: Bridge to actor isolation
// - Android: ALooper
// - Linux/Windows: libuv or generic thread pool
// - Generic: Simple worker thread with queue
//
// The key insight from Lattice.swift is that we capture the "isolation context"
// when setting up observers, then dispatch callbacks to that context.

struct scheduler {
    virtual ~scheduler() = default;

    // Invoke the given function on this scheduler's execution context.
    // Can be called from any thread.
    virtual void invoke(std::function<void()>&& fn) = 0;

    // Check if the caller is currently on this scheduler's thread/context.
    // Can be called from any thread.
    [[nodiscard]] virtual bool is_on_thread() const noexcept = 0;

    // Check if this scheduler wraps the same underlying context as another.
    // Used for caching and deduplication.
    [[nodiscard]] virtual bool is_same_as(const scheduler* other) const noexcept = 0;

    // Check if invoke() is currently possible.
    // May return false if the event loop isn't running.
    [[nodiscard]] virtual bool can_invoke() const noexcept = 0;

    // Stop the scheduler and wait for in-flight work to complete.
    // After shutdown(), invoke() becomes a no-op and can_invoke() returns false.
    // Idempotent — safe to call multiple times or from the destructor.
    // Default implementation is a no-op (for schedulers without a dedicated thread).
    virtual void shutdown() {}
};

class generic_scheduler : public scheduler {
public:
    // C function pointer constructor (portable, Swift-safe on all platforms).
    // invoke_fn receives a void* work item and scheduler context.
    // Call execute_work(work) to run and free the work item.
    generic_scheduler(void* context,
                      void (*invoke_fn)(void* work, void* ctx),
                      bool (*is_on_thread_fn)(void*),
                      bool (*is_same_as_fn)(const scheduler*, void*),
                      bool (*can_invoke_fn)(void*),
                      void (*destroy_fn)(void*) = nullptr);

    // Run and free a work item received by invoke_fn.
    static void execute_work(void* work);

#if defined(__BLOCKS__) && !defined(__swift__)
    // Block-based constructor (C++ callers on platforms with blocks)
    generic_scheduler(void* context,
                      void (^invoke_fn)(std::function<void()>&&, void*),
                      bool (*is_on_thread_fn)(void*),
                      bool (*is_same_as_fn)(const scheduler*, void*),
                      bool (*can_invoke_fn)(void*),
                      void (*destroy_fn)(void*) = nullptr)
        : context_(context)
        , invoke_fn_([invoke_fn = std::move(invoke_fn)](std::function<void()>&& fn, void* ctx) {
            invoke_fn(std::move(fn), ctx);
        })
        , is_on_thread_fn_(is_on_thread_fn)
        , is_same_as_fn_(is_same_as_fn)
        , can_invoke_fn_(can_invoke_fn)
        , destroy_fn_(destroy_fn)
    {}
#endif

#ifndef __swift__
    // std::function-based constructor (for C++ callers and internal make_shared)
    generic_scheduler(void* context,
                      std::function<void(std::function<void()>&&, void*)> invoke_fn,
                      bool (*is_on_thread_fn)(void*),
                      bool (*is_same_as_fn)(const scheduler*, void*),
                      bool (*can_invoke_fn)(void*),
                      void (*destroy_fn)(void*) = nullptr)
        : context_(context)
        , invoke_fn_(invoke_fn)
        , is_on_thread_fn_(is_on_thread_fn)
        , is_same_as_fn_(is_same_as_fn)
        , can_invoke_fn_(can_invoke_fn)
        , destroy_fn_(destroy_fn)
    {}
#endif

    ~generic_scheduler() override {
        if (destroy_fn_ && context_) {
            destroy_fn_(context_);
        }
    }

    void invoke(std::function<void()>&& fn) override {
        invoke_fn_(std::move(fn), context_);
    }

    [[nodiscard]] bool is_on_thread() const noexcept override {
        return is_on_thread_fn_(context_);
    }

    [[nodiscard]] bool is_same_as(const scheduler* other) const noexcept override {
        return is_same_as_fn_(other, context_);
    }

    [[nodiscard]] bool can_invoke() const noexcept override {
        return can_invoke_fn_(context_);
    }

    std::shared_ptr<scheduler> make_shared() const;

    void* context_;
private:
    std::function<void(std::function<void()>&&, void*)> invoke_fn_;
    bool (*is_on_thread_fn_)(void*);
    bool (*is_same_as_fn_)(const scheduler*, void*);
    bool (*can_invoke_fn_)(void*);
    void (*destroy_fn_)(void*);
};

using SharedScheduler = std::shared_ptr<scheduler>;
// ============================================================================
// Generic scheduler - runs callbacks on a dedicated worker thread
// ============================================================================
//
// This is a simple default implementation that works everywhere.
// For production use, you'd want platform-specific schedulers that
// integrate with the UI thread / main run loop.
//
// Not available on Emscripten/WASM — use immediate_scheduler instead.

#ifndef __EMSCRIPTEN__
namespace detail { struct std_thread_scheduler_test_access; }

class std_thread_scheduler : public scheduler {
    // The worker owns this state independently of the scheduler wrapper. A
    // callback may destroy the wrapper before already-admitted siblings drain.
    // Those callbacks' captures still need valid targets of their own.
    struct state {
        std::mutex mutex;
        std::condition_variable work_ready;
        std::condition_variable settlement;
        std::queue<std::function<void()>> queue;
        std::atomic<bool> running{true};
        std::thread worker;
        std::thread::id thread_id;
        bool joining = false;
        bool joined = false;
        bool detached = false;
        bool loop_settled = false;
        bool wrapper_destroying = false;
        size_t detached_waiters = 0;
        std::exception_ptr first_join_error;
    };
    std::shared_ptr<state> state_;
    friend struct detail::std_thread_scheduler_test_access;
    using launch_function = std::thread (*)(std::function<void()>);
    using join_function = void (*)(std::thread&);

    static std::thread launch_worker(std::function<void()> work) {
        return std::thread(std::move(work));
    }
    static void join_worker(std::thread& worker) { worker.join(); }

    // Private injection point: test launch unwinding without changing the
    // public constructor or introducing a process-wide thread factory.
    std_thread_scheduler(std::shared_ptr<state> shared, launch_function launch)
        : state_(std::move(shared)) {
        const auto keep = state_;
        keep->worker = launch([keep] { run_loop(keep); });
        keep->thread_id = keep->worker.get_id();
        // An incomplete construction has no destructor to balance this count.
        auto n = alive_count().fetch_add(1, std::memory_order_relaxed) + 1;
        try {
            LOG_INFO("scheduler", "std_thread_scheduler CREATED (this=%p, alive=%lld)", (void*)this, (long long)n);
        } catch (...) {
            // Logging can allocate its initial lock. An exception after the
            // thread launch must still stop and settle that launched worker.
            const auto first = std::current_exception();
            shutdown_state(keep, true);
            alive_count().fetch_sub(1, std::memory_order_relaxed);
            std::rethrow_exception(first);
        }
    }

    static void request_stop(const std::shared_ptr<state>& shared) {
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->running = false;
        }
        shared->work_ready.notify_all();
    }

    // Destructor-only: an external claimant owns its moved handle and joins
    // it. Otherwise only independent state/captures survive the wrapper.
    static void detach_unclaimed(const std::shared_ptr<state>& shared) {
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (shared->joining || !shared->worker.joinable()) return;
            try {
                shared->worker.detach();
                shared->detached = true;
            } catch (...) {
                if (!shared->first_join_error)
                    shared->first_join_error = std::current_exception();
                // Preserve custody; never pretend a joinable handle retired.
                throw;
            }
        }
        shared->settlement.notify_all();
    }

    static void shutdown_state(const std::shared_ptr<state>& shared,
                               bool destroying, join_function join = join_worker) {
        if (destroying) {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->wrapper_destroying = true;
        }
        request_stop(shared);
        if (std::this_thread::get_id() == shared->thread_id) {
            // Self-shutdown is request-only, including while another caller
            // is joining us. Waiting on that caller would deadlock.
            if (destroying) detach_unclaimed(shared);
            return;
        }
        std::thread claimed;
        {
            std::unique_lock<std::mutex> lock(shared->mutex);
            shared->settlement.wait(lock, [&] { return !shared->joining; });
            if (shared->joined) return;
            if (shared->detached) {
                // The wrapper can self-destruct after an external caller
                // copied state but before it claimed the handle. That caller
                // still owes callback/FIFO settlement, even though an actual
                // join is no longer available. Self callers returned above.
                ++shared->detached_waiters;
                shared->settlement.notify_all();
                shared->settlement.wait(lock, [&] { return shared->loop_settled; });
                --shared->detached_waiters;
                return;
            }
            shared->joining = true;
            claimed = std::move(shared->worker);
        }
        shared->settlement.notify_all();
        try {
            // The callback may call shutdown/admission or release the wrapper.
            // It needs no lock held by this join claimant.
            join(claimed);
        } catch (...) {
            std::exception_ptr first;
            bool detach_without_owner;
            {
                std::lock_guard<std::mutex> lock(shared->mutex);
                // Restore custody BEFORE throwing: a local joinable thread's
                // destructor would otherwise terminate the process.
                shared->worker = std::move(claimed);
                shared->joining = false;
                if (!shared->first_join_error)
                    shared->first_join_error = std::current_exception();
                first = shared->first_join_error;
                detach_without_owner = shared->wrapper_destroying;
            }
            shared->settlement.notify_all();
            if (detach_without_owner) {
                // State-only detach is a last-owner fallback, not a join.
                // If the platform also rejects detach, propagation through
                // the noexcept destructor fails fast rather than claiming
                // success, leaking, or freeing a joinable handle.
                detach_unclaimed(shared);
            }
            if (destroying) return;
            std::rethrow_exception(first);
        }
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->joining = false;
            shared->joined = true;
        }
        shared->settlement.notify_all();
    }

    static void run_loop(const std::shared_ptr<state>& shared) {
        for (;;) {
            std::function<void()> fn;
            {
                std::unique_lock<std::mutex> lock(shared->mutex);
                shared->work_ready.wait(lock, [&] {
                    return !shared->queue.empty() || !shared->running;
                });
                if (!shared->running && shared->queue.empty()) {
                    shared->loop_settled = true;
                    lock.unlock();
                    shared->settlement.notify_all();
                    return;
                }
                // The popped function is empty; captured destructors run only
                // after the work item, outside this lock.
                fn.swap(shared->queue.front());
                shared->queue.pop();
            }
            if (fn) {
                try {
                    fn();
                } catch (const std::exception& e) {
                    LOG_ERROR("scheduler", "Work item threw exception: %s", e.what());
                } catch (...) {
                    LOG_ERROR("scheduler", "Work item threw unknown exception");
                }
            }
            // fn and its captures die off-lock, before loop settlement.
        }
    }

public:
    static std::atomic<int64_t>& alive_count() {
        static std::atomic<int64_t> count{0};
        return count;
    }

    std_thread_scheduler()
        : std_thread_scheduler(std::make_shared<state>(), launch_worker) {}

    std_thread_scheduler(const std_thread_scheduler&) = delete;
    std_thread_scheduler& operator=(const std_thread_scheduler&) = delete;
    std_thread_scheduler(std_thread_scheduler&&) = delete;
    std_thread_scheduler& operator=(std_thread_scheduler&&) = delete;

    ~std_thread_scheduler() override {
        const auto shared = state_;
        shutdown_state(shared, true);
        auto n = alive_count().fetch_sub(1, std::memory_order_relaxed) - 1;
        LOG_INFO("scheduler", "std_thread_scheduler DESTROYED (this=%p, alive=%lld)", (void*)this, (long long)n);
    }

    // External callers join. Calls on the worker request stop and return so
    // the active callback can finish. Admitted work drains FIFO; new work is
    // rejected. This does not retain the work's own raw callback targets.
    void shutdown() override {
        const auto shared = state_;
        shutdown_state(shared, false);
    }

    void invoke(std::function<void()>&& fn) override {
        const auto shared = state_;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (!shared->running) return;
            shared->queue.push(std::move(fn));
        }
        shared->work_ready.notify_one();
    }

    [[nodiscard]] bool is_on_thread() const noexcept override {
        const auto shared = state_;
        return std::this_thread::get_id() == shared->thread_id;
    }

    [[nodiscard]] bool is_same_as(const scheduler* other) const noexcept override {
        const auto shared = state_;
        auto* g = dynamic_cast<const std_thread_scheduler*>(other);
        return g && g->state_->thread_id == shared->thread_id;
    }

    [[nodiscard]] bool can_invoke() const noexcept override {
        const auto shared = state_;
        return shared->running;
    }
};
#endif // !__EMSCRIPTEN__

// ============================================================================
// Immediate scheduler - runs callbacks synchronously on calling thread
// ============================================================================
//
// Useful for testing or single-threaded applications.

class immediate_scheduler : public scheduler {
public:
    void invoke(std::function<void()>&& fn) override {
        if (fn) fn();
    }

    [[nodiscard]] bool is_on_thread() const noexcept override {
        return true;  // Always "on thread" since we execute immediately
    }

    [[nodiscard]] bool is_same_as(const scheduler* other) const noexcept override {
        return dynamic_cast<const immediate_scheduler*>(other) != nullptr;
    }

    [[nodiscard]] bool can_invoke() const noexcept override {
        return true;
    }
};

// ============================================================================
// Main thread scheduler - for platforms with a main/UI thread concept
// ============================================================================
//
// This captures the thread ID at construction (assumed to be main thread)
// and queues work to be processed later. You must call process_pending()
// from your main run loop.

class main_thread_scheduler : public scheduler {
public:
    main_thread_scheduler() : main_thread_id_(std::this_thread::get_id()) {}

    void invoke(std::function<void()>&& fn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(fn));
    }

    // Call this from your main thread's run loop to process pending work
    void process_pending() {
        std::vector<std::function<void()>> pending;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            while (!queue_.empty()) {
                pending.push_back(std::move(queue_.front()));
                queue_.pop();
            }
        }
        for (auto& fn : pending) {
            if (fn) fn();
        }
    }

    [[nodiscard]] bool is_on_thread() const noexcept override {
        return std::this_thread::get_id() == main_thread_id_;
    }

    [[nodiscard]] bool is_same_as(const scheduler* other) const noexcept override {
        auto* m = dynamic_cast<const main_thread_scheduler*>(other);
        return m && m->main_thread_id_ == main_thread_id_;
    }

    [[nodiscard]] bool can_invoke() const noexcept override {
        return true;
    }

private:
    std::thread::id main_thread_id_;
    std::mutex mutex_;
    std::queue<std::function<void()>> queue_;
};

// ============================================================================
// Scheduler factory - allows platform-specific default registration
// ============================================================================

namespace default_scheduler {

    // Get the default scheduler for this platform
    inline std::shared_ptr<scheduler> make_default() {
        static std::function<std::shared_ptr<scheduler>()> factory = [] {
            return std::make_shared<immediate_scheduler>();
        };
        return factory();
    }

    // Register a custom factory function (call before creating any databases)
    inline void set_default_factory(std::function<std::shared_ptr<scheduler>()> factory) {
        // Note: In a real implementation, this would use a static variable
        // For now, users should create schedulers explicitly
    }

} // namespace default_scheduler

} // namespace lattice

#endif // __cplusplus
