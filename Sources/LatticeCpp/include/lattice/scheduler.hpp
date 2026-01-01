#pragma once

#include <functional>
#include <memory>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>

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
};

class generic_scheduler : public scheduler {
public:
#ifdef __BLOCKS__
    // Block-based constructor for Apple platforms (Swift interop)
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

    // std::function-based constructor (portable)
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

    std::shared_ptr<scheduler> make_shared() const {
        return std::make_shared<generic_scheduler>(context_, invoke_fn_, is_on_thread_fn_, is_same_as_fn_,
                                                   can_invoke_fn_, destroy_fn_);
    }
    
    void* context_;
private:
    std::function<void(std::function<void()>&&, void*)> invoke_fn_;
//    void (*invoke_fn_)(std::function<void()>&&, void*);
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

class std_thread_scheduler : public scheduler {
public:
    std_thread_scheduler() : running_(true) {
        worker_ = std::thread([this] { run_loop(); });
        thread_id_ = worker_.get_id();
    }

    ~std_thread_scheduler() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            running_ = false;
        }
        cv_.notify_one();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void invoke(std::function<void()>&& fn) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) return;
            queue_.push(std::move(fn));
        }
        cv_.notify_one();
    }

    [[nodiscard]] bool is_on_thread() const noexcept override {
        return std::this_thread::get_id() == thread_id_;
    }

    [[nodiscard]] bool is_same_as(const scheduler* other) const noexcept override {
        auto* g = dynamic_cast<const std_thread_scheduler*>(other);
        return g && g->thread_id_ == thread_id_;
    }

    [[nodiscard]] bool can_invoke() const noexcept override {
        return running_;
    }

private:
    void run_loop() {
        while (true) {
            std::function<void()> fn;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !queue_.empty() || !running_; });

                if (!running_ && queue_.empty()) {
                    return;
                }

                fn = std::move(queue_.front());
                queue_.pop();
            }

            if (fn) {
                fn();
            }
        }
    }

    std::thread worker_;
    std::thread::id thread_id_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> queue_;
    std::atomic<bool> running_;
};

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
