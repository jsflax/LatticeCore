#pragma once

#ifndef __EMSCRIPTEN__
#include <lattice/scheduler.hpp>
#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <thread>
#include <typeinfo>

namespace lattice::detail {

// Synchronizer-local adapter for the native immediate scheduler. Idle work
// stays inline. Overlapping/reentrant invoke calls enqueue and return without
// waiting for the active operation, including calls made under SQLite locks.
// No queue mutex is held while invoking or destroying a work item.
class sync_immediate_scheduler final : public scheduler {
    struct state {
        std::mutex mutex;
        std::condition_variable settled;
        std::deque<std::function<void()>> pending;
        bool closed = false;
        bool active = false;
        std::thread::id owner;
    };
    std::shared_ptr<state> state_ = std::make_shared<state>();

    static void drain(const std::shared_ptr<state>& shared) {
        std::exception_ptr first_error;
        for (;;) {
            std::function<void()> work;
            {
                std::lock_guard<std::mutex> lock(shared->mutex);
                if (shared->closed || shared->pending.empty()) {
                    shared->active = false;
                    shared->owner = {};
                    shared->settled.notify_all();
                    break;
                }
                work.swap(shared->pending.front());
                shared->pending.pop_front();
            }
            try {
                if (work) work();
            } catch (...) {
                // Restore admission and finish already-admitted work even if
                // a callback throws. The inline drain owner receives the first
                // exception after the queue settles, as with inline dispatch.
                if (!first_error) first_error = std::current_exception();
            }
        }
        if (first_error) std::rethrow_exception(first_error);
    }

public:
    ~sync_immediate_scheduler() override { shutdown(); }

    void invoke(std::function<void()>&& fn) override {
        const auto shared = state_;
        bool own_drain = false;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            if (shared->closed) return;
            shared->pending.push_back(std::move(fn));
            if (!shared->active) {
                shared->active = true;
                shared->owner = std::this_thread::get_id();
                own_drain = true;
            }
        }
        if (own_drain) drain(shared);
    }

    bool is_on_thread() const noexcept override {
        const auto shared = state_;
        std::lock_guard<std::mutex> lock(shared->mutex);
        return shared->active && shared->owner == std::this_thread::get_id();
    }

    bool is_same_as(const scheduler* other) const noexcept override {
        return this == other;
    }

    bool can_invoke() const noexcept override {
        const auto shared = state_;
        std::lock_guard<std::mutex> lock(shared->mutex);
        return !shared->closed;
    }

    void shutdown() override {
        const auto shared = state_;
        std::deque<std::function<void()>> cancelled;
        bool on_drain;
        {
            std::lock_guard<std::mutex> lock(shared->mutex);
            shared->closed = true;
            shared->pending.swap(cancelled);
            on_drain = shared->active && shared->owner == std::this_thread::get_id();
        }
        // Captured destructors may reenter. Run them before waiting, off lock.
        cancelled.clear();
        if (!on_drain) {
            std::unique_lock<std::mutex> lock(shared->mutex);
            shared->settled.wait(lock, [&] { return !shared->active; });
        }
        // An active callback can destroy this adapter. drain() holds only the
        // shared state after entry, so its current item completes safely.
        // This does NOT extend the lifetime of the callback's own raw captures.
    }
};

// A derived immediate scheduler may override dispatch policy; preserve it.
inline std::shared_ptr<scheduler> make_synchronizer_scheduler(std::shared_ptr<scheduler> original) {
    if (original && typeid(*original) == typeid(immediate_scheduler))
        return std::make_shared<sync_immediate_scheduler>();
    return original;
}

} // namespace lattice::detail
#endif
