#pragma once
#ifndef __EMSCRIPTEN__
#include "sync_callback_lifetime.hpp"
#include <lattice/db.hpp>
#include <condition_variable>
#include <cstdint>
#include <utility>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

namespace lattice::detail {

// Fixed pre-publication refusal, with the original resource/launch exception
// retained independently. No diagnostic allocation is needed to retain it.
class controller_executor_error final : public std::exception {
public:
    enum class reason { retirement_admission, launch, foreign_process, stopped, occupied };
    const reason kind;
    const std::exception_ptr first;
    explicit controller_executor_error(reason value, std::exception_ptr original = {}) noexcept
        : kind(value), first(std::move(original)) {}
    const char* what() const noexcept override {
        switch (kind) {
        case reason::retirement_admission: return "controller retirement admission refused before publication";
        case reason::launch: return "controller worker launch refused before publication";
        case reason::foreign_process: return "controller executor belongs to another process";
        case reason::stopped: return "controller executor has retired";
        case reason::occupied: return "controller executor turn slot already occupied";
        }
        return "controller executor refused";
    }
};

// A private controller lane, never a general application scheduler. There is
// exactly one pending turn, one running turn and one coalesced timed wake.
// The timed cell runs only the controller's weak wake operation; the caller
// supplies its already-established episode's next_due, never a new budget.
class recovery_controller_executor final {
    friend struct recovery_controller_executor_test_access;
    using clock = std::chrono::steady_clock;
    using launch_function = std::thread (*)(std::function<void()>);
    struct state {
        const uint64_t creator = database_native_process();
        std::mutex mutex;
        std::condition_variable changed, settled;
        std::function<void()> pending, timed, executing;
        std::weak_ptr<state> observation;
        std::exception_ptr iteration_error;
        clock::time_point due = clock::time_point::max();
        bool stopping = false, active = false, loop_ended = false;
        bool transferred = false, joined = false, retirement_tail_ended = false;
        int32_t retirement_error = 0;
        std::exception_ptr worker_error;
        std::thread::id worker_id;
        uint64_t turns = 0, timed_wakes = 0;
    };
    // Pre-admitted before worker launch. The first retire caller moves this
    // entire capsule; no allocation or automatic capture ownership is added
    // to its noexcept path. A child abandons the remaining cancelled graph.
    struct retirement_tail {
        const uint64_t creator = database_native_process();
        std::shared_ptr<state> original;
        std::function<void()> pending, timer;
        std::thread thread;
        std::optional<sync_retirement_lane::reservation> ending;
        bool transferred = false;
        explicit retirement_tail(const std::shared_ptr<state>& actual):original(actual){}
    };
    struct retirement_tail_delete {
        void operator()(retirement_tail* tail)const noexcept {
            if(!tail||tail->creator!=database_native_process())return;
            tail->pending={}; // arbitrary final capture release is OFF all leaves
            if(tail->creator!=database_native_process())return;
            tail->timer={};
            if(tail->creator!=database_native_process())return;
            tail->ending.reset(); // actual transferred reservation has ended
            if(tail->creator!=database_native_process())return;
            // A live handle can only mean retirement transfer was not proved.
            // Do not destroy that std::thread or publish a false tail ending.
            if(tail->thread.joinable())return;
            if(tail->transferred){
                try {
                    {std::lock_guard lock(tail->original->mutex);
                     tail->original->retirement_tail_ended=true;}
                    tail->original->settled.notify_all();
                }catch(...){/* bounded ending remains unproved */}
            }
            delete tail; // independent state final release remains off leaf
        }
    };
    using owned_retirement_tail=std::unique_ptr<retirement_tail,retirement_tail_delete>;
    // A child abandons this entire inherited capsule before touching any
    // shared_ptr, reservation, std::thread, mutex or retained capture.
    struct resources {
        std::shared_ptr<state> shared;
        std::optional<sync_retirement_lane::reservation> ending;
        std::thread worker;
        owned_retirement_tail retirement;
    };
    const uint64_t creator_ = database_native_process();
    resources* resources_ = nullptr;
    static thread_local state* current_;
    static std::thread launch(std::function<void()> work) { return std::thread(std::move(work)); }
    static std::shared_ptr<sync_retirement_lane> admitted_reaper() {
        // Acquisition itself may creator-refuse before the inherited singleton
        // guard. It belongs to the same typed prepublication admission boundary
        // as reserve_empty(), and retains that actual first exception.
        try { return sync_retirement_lane::instance(); }
        catch (...) { throw controller_executor_error(controller_executor_error::reason::retirement_admission, std::current_exception()); }
    }
    bool current_process() const noexcept { return creator_ == database_native_process(); }
    void require_current() const {
        if (!current_process()) throw controller_executor_error(controller_executor_error::reason::foreign_process);
    }
    static void loop(state* shared) noexcept {
        // The reserved reaper slot already retains this exact state until its
        // thread joins. The thread entry/call frame owns only a raw DATA link.
        // In a child, returning abandons all inherited heap captures rather
        // than unwinding shared/function custody or touching a parent mutex.
        if(shared->creator!=database_native_process())return;
        {
            std::lock_guard lock(shared->mutex);
            shared->worker_id = std::this_thread::get_id();
        }
        for (;;) {
            bool timer = false;
            {
                std::unique_lock lock(shared->mutex);
                for (;;) {
                    if (shared->stopping && !shared->pending) {
                        shared->loop_ended = true;
                        lock.unlock(); shared->settled.notify_all(); return;
                    }
                    if (shared->pending) { shared->executing.swap(shared->pending); break; }
                    if (shared->timed && clock::now() >= shared->due) {
                        shared->executing.swap(shared->timed); shared->due = clock::time_point::max(); timer = true; break;
                    }
                    if (shared->timed) shared->changed.wait_until(lock, shared->due);
                    else shared->changed.wait(lock);
                }
                shared->active = true;
                if (timer) { if(shared->timed_wakes!=UINT64_MAX)++shared->timed_wakes; }
                else if(shared->turns!=UINT64_MAX)++shared->turns;
            }
            auto* prior = current_; current_ = shared;
            try { if (shared->executing) shared->executing(); }
            catch (...) { shared->iteration_error = std::current_exception(); }
            if(shared->creator!=database_native_process())return;
            current_ = prior;
            // This final release can destroy the executor/controller on its
            // own worker. Only reaper-retained independent state follows it.
            shared->executing = {};
            if(shared->creator!=database_native_process())return;
            {
                std::lock_guard lock(shared->mutex);
                shared->active = false;
                if (shared->iteration_error && !shared->worker_error)
                    shared->worker_error = std::move(shared->iteration_error);
            }
            shared->iteration_error = {}; // later error final-release is off leaf
            if(shared->creator!=database_native_process())return;
            shared->settled.notify_all();
        }
    }
    recovery_controller_executor(const std::shared_ptr<sync_retirement_lane>& reaper,
                                 launch_function start) {
        auto staged = std::make_unique<resources>();
        staged->shared = std::make_shared<state>();
        staged->retirement.reset(new retirement_tail(staged->shared)); // before launch/publication
        try { staged->ending.emplace(reaper->reserve_empty()); }
        catch (...) { throw controller_executor_error(controller_executor_error::reason::retirement_admission, std::current_exception()); }
        const auto shared = staged->shared;
        staged->shared->observation=shared;
        // Bind independent state/observation before launch. The real reaper
        // retains it even if its actual join is quarantined after an error.
        staged->ending->bind_controller_thread(shared, [shared](sync_retirement_result result) {
            {
                std::lock_guard lock(shared->mutex);
                shared->retirement_error = result.first_error;
                shared->joined = result.first_error == 0 && result.pacer_present && result.pacer_joined;
            }
            shared->settled.notify_all();
        });
        try {
            staged->worker = start([raw=shared.get()] { loop(raw); });
            if (!staged->worker.joinable())
                throw controller_executor_error(controller_executor_error::reason::launch);
        } catch (...) { throw controller_executor_error(controller_executor_error::reason::launch, std::current_exception()); }
        resources_ = staged.release(); // all resource admission precedes publication
    }
public:
    // Internal deletion ending DATA only. Retains the exact original state,
    // never the controller, a Native writer or permission to run another turn.
    class ending final {
        friend class recovery_controller_executor;
        const uint64_t creator_;
        std::shared_ptr<state>* original_=nullptr;
        explicit ending(const std::shared_ptr<state>& actual)
            :creator_(database_native_process()),original_(new std::shared_ptr<state>(actual)){}
        static bool complete(const state& s)noexcept {
            return s.transferred&&s.loop_ended&&s.joined&&s.retirement_tail_ended&&!s.active&&!s.pending&&!s.timed&&
                s.retirement_error==0&&!s.worker_error;
        }
    public:
        enum class disposition { joined, pending, reentrant, failed, foreign, unavailable };
        ending()noexcept:creator_(database_native_process()){}
        ending(ending&& other)noexcept:creator_(other.creator_),original_(std::exchange(other.original_,nullptr)){}
        ending& operator=(ending&&)=delete;
        ending(const ending&)=delete;
        ending& operator=(const ending&)=delete;
        ~ending()noexcept {
            if(creator_!=database_native_process()){original_=nullptr;return;}
            delete original_; // Caller must retain this DATA beyond all Native/store leaves.
        }
        disposition wait_until(clock::time_point end)const noexcept {
            if(creator_!=database_native_process())return disposition::foreign;
            if(!original_||!*original_)return disposition::unavailable;
            auto& original=**original_;
            if(original.creator!=database_native_process())return disposition::foreign;
            try {
                std::unique_lock lock(original.mutex);
                if(complete(original))return disposition::joined;
                if(original.retirement_error||original.worker_error)return disposition::failed;
                // Never wait for the actual calling worker's own retirement.
                if(original.worker_id==std::this_thread::get_id())return disposition::reentrant;
                if(!original.settled.wait_until(lock,end,[&]{
                    return complete(original)||original.retirement_error||original.worker_error;
                }))return disposition::pending;
                return complete(original)?disposition::joined:disposition::failed;
            }catch(...){return disposition::failed;}
        }
    };
    ending retain_ending()const {
        require_current();
        if(!resources_)return {};
        return ending(resources_->shared);
    }
    recovery_controller_executor()
        : recovery_controller_executor(admitted_reaper(), launch) {}
    recovery_controller_executor(const recovery_controller_executor&) = delete;
    recovery_controller_executor& operator=(const recovery_controller_executor&) = delete;
    ~recovery_controller_executor() noexcept {
        if (!current_process()) { resources_ = nullptr; return; }
        retire();
        if (!current_process()) { resources_ = nullptr; return; }
        delete resources_; resources_ = nullptr; // wrapper methods have ended
    }
    void invoke(std::function<void()> work) {
        require_current();
        if (!resources_) throw controller_executor_error(controller_executor_error::reason::stopped);
        const auto shared = resources_->shared;
        {
            std::lock_guard lock(shared->mutex);
            if (shared->stopping) throw controller_executor_error(controller_executor_error::reason::stopped);
            if (shared->pending) throw controller_executor_error(controller_executor_error::reason::occupied);
            shared->pending.swap(work);
        }
        shared->changed.notify_one();
    }
    void retry_at(clock::time_point due, std::function<void()> wake) {
        require_current();
        if (!resources_) throw controller_executor_error(controller_executor_error::reason::stopped);
        const auto shared = resources_->shared;
        std::function<void()> displaced;
        {
            std::lock_guard lock(shared->mutex);
            if (shared->stopping) throw controller_executor_error(controller_executor_error::reason::stopped);
            displaced.swap(shared->timed);
            shared->timed.swap(wake); shared->due = due;
        }
        shared->changed.notify_one(); // displaced/wake final release is off-leaf
    }
    void cancel_retry() noexcept {
        if (!current_process() || !resources_) return;
        const auto shared = resources_->shared;
        std::function<void()> displaced;
        {
            std::lock_guard lock(shared->mutex);
            displaced.swap(shared->timed); shared->due = clock::time_point::max();
        }
        shared->changed.notify_one();
    }
    void retire() noexcept {
        if (!current_process()) return;
        if (!resources_) return;
        auto* shared = resources_->shared.get(); // actual capsule below retains it
        owned_retirement_tail tail;
        {
            std::lock_guard lock(shared->mutex);
            if (shared->transferred) return;
            tail=std::move(resources_->retirement); // only the first transfer owns completion
            shared->stopping = true;
            tail->pending.swap(shared->pending);tail->timer.swap(shared->timed);
            shared->due = clock::time_point::max(); shared->transferred = true;
            tail->transferred=true;
            tail->thread = std::move(resources_->worker);
            tail->ending.emplace(std::move(*resources_->ending));
        }
        shared->changed.notify_one();
        // No join/detach or wait under controller/Native leaves, including
        // self final release. The reserved real slot owns this exact handle
        // through actual join or quarantine; transfer is not completion.
        tail->ending->retire(std::move(tail->thread));
        // The creator-fenced tail ends actual cancelled captures/reservation
        // OFF leaves before publishing retirement_tail_ended. A raw reaper
        // join alone cannot overtake that final arbitrary capture destruction.
    }
};
}
#endif
