#pragma once
#ifndef __EMSCRIPTEN__
#include "../../Sources/LatticeCore/src/recovery_controller_executor.hpp"
namespace lattice::detail {
// Test-only DATA/ending access. No controller construction, source authority,
// admission bypass, SQL permit or deadline mutation can be obtained here.
struct recovery_controller_executor_test_access {
    using executor = recovery_controller_executor;
    using ticket = std::shared_ptr<executor::state>;
    struct copy {
        bool available=false,active=false,pending=false,timed=false,stopping=false,transferred=false,loop_ended=false,joined=false;
        int32_t retirement_error=0;uint64_t turns=0,timed_wakes=0;std::thread::id worker;
        std::exception_ptr worker_error;
    };
    static std::thread launch(std::function<void()> work){return std::thread(std::move(work));}
    static std::shared_ptr<sync_retirement_lane> lane(size_t capacity=64){return std::shared_ptr<sync_retirement_lane>(new sync_retirement_lane(capacity,launch));}
    static std::shared_ptr<executor> create(const std::shared_ptr<sync_retirement_lane>& lane,executor::launch_function start=launch){return std::shared_ptr<executor>(new executor(lane,start));}
    static ticket observe(const executor& lane){lane.require_current();return lane.resources_->shared;}
    static ticket current(){const auto* state=executor::current_;return state&&state->creator==database_native_process()?state->observation.lock():ticket{};}
    static copy snapshot(const ticket& state){
        copy result;if(!state||state->creator!=database_native_process())return result;
        std::lock_guard lock(state->mutex);result.available=true;result.active=state->active;result.pending=bool(state->pending);result.timed=bool(state->timed);
        result.stopping=state->stopping;result.transferred=state->transferred;result.loop_ended=state->loop_ended;result.joined=state->joined;
        result.retirement_error=state->retirement_error;result.turns=state->turns;result.timed_wakes=state->timed_wakes;result.worker=state->worker_id;result.worker_error=state->worker_error;return result;
    }
    static bool joined(const ticket& state,std::chrono::milliseconds bound){
        if(!state||state->creator!=database_native_process())return false;
        std::unique_lock lock(state->mutex);return state->settled.wait_for(lock,bound,[&]{return state->joined;});
    }
    static uint64_t order(const std::shared_ptr<sync_retirement_lane>& lane){std::lock_guard lock(lane->mutex_);return lane->next_order_;}
    static ticket controller_at(const std::shared_ptr<sync_retirement_lane>& lane,uint64_t order){
        std::lock_guard lock(lane->mutex_);for(size_t i=0;i<lane->capacity_;++i)if(lane->slots_[i].queued_order==order&&lane->slots_[i].controller_thread_custody)
            return std::static_pointer_cast<executor::state>(lane->slots_[i].controller_thread_custody);return {};
    }
    static size_t used(const std::shared_ptr<sync_retirement_lane>& lane){std::lock_guard lock(lane->mutex_);size_t n=0;for(size_t i=0;i<lane->capacity_;++i)if(lane->slots_[i].state!=sync_retirement_lane::phase::free)++n;return n;}
    static bool free(const std::shared_ptr<sync_retirement_lane>& lane,std::chrono::milliseconds bound){
        std::unique_lock lock(lane->mutex_);return lane->settled_.wait_for(lock,bound,[&]{for(size_t i=0;i<lane->capacity_;++i)if(lane->slots_[i].state!=sync_retirement_lane::phase::free)return false;return true;});
    }
};
}
#endif
