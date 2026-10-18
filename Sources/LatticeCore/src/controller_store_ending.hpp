#pragma once
#ifndef __EMSCRIPTEN__
#include "recovery_controller_executor.hpp"
#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>

namespace lattice::detail {
class recovery_continuous_producer;
class recovery_receiver_controller;
class store_deletion_operation;
struct recovery_continuous_state;
bool controller_publications_ended(const std::shared_ptr<recovery_continuous_state>&,
    std::chrono::steady_clock::time_point) noexcept;
struct controller_store_ending_test_access;

// One actual candidate construction, reserved before a worker can launch.
// It is retained even if the candidate loses publication or its constructor
// later throws. This is ending DATA, not a controller or Native SQL grant.
class controller_worker_original final {
    friend class recovery_continuous_producer;
    friend class recovery_receiver_controller;
    friend struct controller_store_ending_test_access;
    using clock = std::chrono::steady_clock;
    struct payload {
        std::mutex mutex;
        std::condition_variable bound;
        std::optional<recovery_controller_executor::ending> actual;
        std::atomic<bool> constructor_failed{false}, constructor_complete{false};
        std::exception_ptr first_construction_failure;
    };
    const uint64_t creator_ = database_native_process();
    payload* value_ = new payload;
    void bind(recovery_controller_executor::ending&& actual) {
        if (creator_ != database_native_process()) throw db_error("controller ending belongs to another process");
        {
            std::lock_guard lock(value_->mutex);
            if (value_->actual) throw db_error("controller ending original already bound");
            value_->actual.emplace(std::move(actual));
        }
        value_->bound.notify_all();
    }
    void failed_constructor(std::exception_ptr first) noexcept {
        if (creator_ != database_native_process()) return;
        try {
            std::lock_guard lock(value_->mutex);
            if (!value_->first_construction_failure) value_->first_construction_failure = std::move(first);
            value_->constructor_failed.store(true, std::memory_order_release);
        } catch (...) {
            value_->constructor_failed.store(true, std::memory_order_release);
        }
        value_->constructor_complete.store(true,std::memory_order_release);
        value_->bound.notify_all();
    }
    void succeeded_constructor() noexcept {
        if(creator_!=database_native_process())return;
        value_->constructor_complete.store(true,std::memory_order_release);
        value_->bound.notify_all();
    }
public:
    struct observation {
        recovery_controller_executor::ending::disposition actual = recovery_controller_executor::ending::disposition::unavailable;
        bool constructor_failed = false, constructor_complete = false;
    };
    controller_worker_original() = default;
    controller_worker_original(const controller_worker_original&) = delete;
    controller_worker_original& operator=(const controller_worker_original&) = delete;
    ~controller_worker_original() noexcept {
        if (creator_ != database_native_process()) { value_ = nullptr; return; }
        delete value_; // Every owner releases this independent DATA off leaves.
    }
    observation wait_until(clock::time_point end) const noexcept {
        using disposition = recovery_controller_executor::ending::disposition;
        if (creator_ != database_native_process()) return {disposition::foreign, false};
        try {
            std::unique_lock lock(value_->mutex);
            if (!value_->bound.wait_until(lock, end, [&] {
                return value_->actual.has_value() || value_->constructor_failed.load(std::memory_order_acquire);
            })) return {disposition::pending, false};
            // Acquire the terminal publication before its immutable failure
            // outcome, including the conservative metadata-lock-error path.
            const bool complete=value_->constructor_complete.load(std::memory_order_acquire);
            const bool failed = value_->constructor_failed.load(std::memory_order_acquire);
            if (!value_->actual) return {disposition::unavailable, failed, complete};
            const auto* original = &*value_->actual;
            lock.unlock(); // Never nest the candidate leaf with the executor leaf/wait.
            return {original->wait_until(end), failed, complete};
        } catch (...) { return {disposition::failed, true}; }
    }
};

// Only the producer can freeze a complete, admission-fenced original census.
// A zero count is meaningful only from that private successful freeze, never
// from a weak controller lookup or a replaceable test observation callback.
class controller_store_ending final {
    friend class recovery_continuous_producer;
    friend class store_deletion_operation;
    friend struct controller_store_ending_test_access;
    // Same actual global controller retirement-lane capacity; not a new queue.
    static constexpr std::size_t capacity = 64;
    using originals = std::array<std::shared_ptr<controller_worker_original>, capacity>;
    struct payload {
        originals rows;
        std::shared_ptr<recovery_continuous_state> publication;
        std::shared_ptr<controller_worker_original> first_failed_original;
    };
    struct payload_delete {
        uint64_t creator=database_native_process();
        void operator()(payload* value)const noexcept {if(creator==database_native_process())delete value;}
    };
    using owned_payload=std::unique_ptr<payload,payload_delete>;
    const uint64_t creator_ = database_native_process();
    payload* originals_ = nullptr;
    explicit controller_store_ending(payload* original) noexcept : originals_(original) {}
public:
    struct result {
        bool census_complete = false, all_joined = false, constructor_failed = false;
        std::size_t originals = 0, joined = 0;
        recovery_controller_executor::ending::disposition first = recovery_controller_executor::ending::disposition::unavailable;
    };
    controller_store_ending(controller_store_ending&& other) noexcept
        : creator_(other.creator_), originals_(std::exchange(other.originals_, nullptr)) {}
    controller_store_ending(const controller_store_ending&) = delete;
    controller_store_ending& operator=(const controller_store_ending&) = delete;
    ~controller_store_ending() noexcept {
        if (creator_ != database_native_process()) { originals_ = nullptr; return; }
        delete originals_; // Last original/state releases are off producer/Native leaves.
    }
    result wait_until(std::chrono::steady_clock::time_point end) const noexcept {
        using disposition = recovery_controller_executor::ending::disposition;
        result output;
        if (creator_ != database_native_process()) { output.first = disposition::foreign; return output; }
        if (!originals_) return output;
        if (originals_->publication && !controller_publications_ended(originals_->publication, end)) {
            output.first = disposition::pending; return output;
        }
        output.census_complete = true; output.all_joined = true; output.first = disposition::joined;
        output.constructor_failed = bool(originals_->first_failed_original);
        for (const auto& original : originals_->rows) {
            if (!original) continue;
            ++output.originals;
            const auto observed = original->wait_until(end);
            output.constructor_failed = output.constructor_failed || observed.constructor_failed;
            if (observed.actual == disposition::joined && observed.constructor_complete) ++output.joined;
            else {
                if (output.all_joined) output.first = observed.actual==disposition::joined?disposition::pending:observed.actual;
                output.all_joined = false;
            }
        }
        return output;
    }
};
} // namespace lattice::detail
#endif
