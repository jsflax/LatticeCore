#include "../include/configured_recovery_qualification.hpp"
#include "../../LatticeCore/src/configured_recovery_connection.hpp"
#include "../../LatticeCore/src/configured_recovery_observation.hpp"
#include <array>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <utility>

namespace lattice {
struct configured_recovery_qualification::implementation final:detail::configured_recovery_observer {
    mutable std::mutex mutex;
    std::array<configured_recovery_qualification_record,64> events{};
    std::array<std::uintptr_t,8> owners{};
    std::array<bool,8> owner_closed{};
    struct attempt_identity {std::uintptr_t owner=0,pointer=0;size_t serial=0;bool live=false;};
    std::array<attempt_identity,8> current{};
    size_t used=0,owner_count=0,next_attempt=0;
    bool overflow=false,bad=false,entered=false;
    static_assert(std::atomic<uint32_t>::is_always_lock_free&&std::atomic<bool>::is_always_lock_free);
    std::array<configured_close_qualification_record,24> close_events{};
    std::array<uint8_t,8> close_points{};
    struct close_identity {std::uintptr_t owner=0;size_t serial=0;detail::configured_retirement_identity interval;};
    std::array<close_identity,64> close_identities{}; // Authentic created intervals only; never pointer aliases.
    std::atomic<uint32_t> close_published{0};
    std::atomic<bool> close_overflow{false},close_bad{false},close_missing{false};
    void record_close(const detail::configured_close_observation& value)noexcept override {
        std::unique_lock<std::mutex> lock(mutex,std::try_to_lock);
        if(!lock.owns_lock()){close_missing.store(true,std::memory_order_relaxed);return;}
        size_t owner=0;
        for(;owner<owner_count&&owners[owner]!=value.owner;++owner){}
        if(owner==owner_count){close_missing.store(true,std::memory_order_relaxed);return;}
        if(value.point<1||value.point>3){close_bad.store(true,std::memory_order_relaxed);return;}
        auto& points=close_points[owner];const auto bit=uint8_t(1u<<(value.point-1));
        if(points&bit)return;
        if(points&uint8_t(~((1u<<value.point)-1u))){close_bad.store(true,std::memory_order_relaxed);return;}
        const auto used=close_published.load(std::memory_order_relaxed);
        if(used==close_events.size()){close_overflow.store(true,std::memory_order_relaxed);return;}
        points|=bit;auto& out=close_events[used];
        out.point=value.point;out.owner=owner+1;out.sampled_leaves=value.sampled_leaves;
        if(value.attempt){
            for(size_t i=std::min(next_attempt,close_identities.size());i>0;--i){
                const auto& identity=close_identities[i-1];
                if(identity.owner==value.owner&&identity.interval.matches(value.interval)){out.attempt=identity.serial;out.sampled_leaves|=1u<<6;break;}
            }
        }else out.sampled_leaves|=1u<<6; // Actual no-attempt sample, not an inferred ordinal.
        out.sync=value.sync;out.report_flags=value.report_flags;out.state_flags=value.state_flags;
        out.attempt_flags=value.attempt_flags;out.registry_flags=value.registry_flags;out.service_flags=value.service_flags;
        out.commands=value.commands;out.payloads=value.payloads;out.workers=value.workers;out.finished_workers=value.finished_workers;
        out.active_callbacks=value.active_callbacks;out.collector_busy=value.collector_busy;out.dispatcher_busy=value.dispatcher_busy;
        out.custody_error=value.custody_error;out.registry_error=value.registry_error;
        out.elapsed_ms=value.elapsed_ms;out.remaining_ms=value.remaining_ms;out.time_clamped=value.time_clamped;
        // This slot is never overwritten. Reader acquire publishes the entire
        // immutable plain record without a recorder lock or retry loop.
        close_published.store(used+1,std::memory_order_release);
    }
    void record(const detail::configured_recovery_observation& value)noexcept override {
        std::lock_guard<std::mutex> lock(mutex);
        if(used==events.size()){overflow=true;return;}
        size_t owner=0;
        for(;owner<owner_count&&owners[owner]!=value.owner;++owner){}
        if(owner==owner_count){
            if(value.stage!=detail::configured_recovery_stage::transport_created||owner_count==owners.size()){bad=true;return;}
            owners[owner_count++]=value.owner;
        }
        if(owner_closed[owner]){bad=true;return;}
        auto& attempt=current[owner];
        if(value.stage==detail::configured_recovery_stage::transport_created){
            // Address reuse cannot make a new attempt equal an earlier one:
            // only actual collection closes the old interval before increment.
            if(attempt.live){bad=true;return;}
            attempt={value.owner,value.attempt,++next_attempt,true};
            if(next_attempt<=close_identities.size())close_identities[next_attempt-1]={value.owner,attempt.serial,value.interval};
            else close_missing.store(true,std::memory_order_relaxed);
        }else if(value.stage!=detail::configured_recovery_stage::logical_closed&&
                 (!attempt.live||attempt.pointer!=value.attempt)){bad=true;return;}
        auto& out=events[used++];
        out.stage=static_cast<uint32_t>(value.stage)+1;out.owner=owner+1;out.attempt=attempt.serial;
        out.commands=value.commands;out.payloads=value.payloads;out.workers=value.workers;
        out.adapter_complete=value.adapter_complete;out.native_complete=value.native_complete;out.quarantined=value.quarantined;
        out.lane_complete=value.lane_complete;out.disconnect_returned=value.disconnect_returned;
        out.pacer_present=value.pacer_present;out.pacer_joined=value.pacer_joined;out.callbacks_settled=value.callbacks_settled;
        out.wrapper_destroyed=value.wrapper_destroyed;out.route_unregistered=value.route_unregistered;out.receipt_invalid=value.receipt_invalid;
        out.child_closed=value.child_closed;out.scheduler_joined=value.scheduler_joined;out.first_error=value.first_error;
        if(value.stage==detail::configured_recovery_stage::collected)attempt.live=false;
        if(value.stage==detail::configured_recovery_stage::logical_closed){owner_closed[owner]=true;if(attempt.live)bad=true;}
    }
};
size_t configured_recovery_qualification::close_count()const noexcept{return impl_->close_published.load(std::memory_order_acquire);}
bool configured_recovery_qualification::close_overflowed()const noexcept{return impl_->close_overflow.load(std::memory_order_relaxed);}
bool configured_recovery_qualification::close_invalid()const noexcept{return impl_->close_bad.load(std::memory_order_relaxed);}
bool configured_recovery_qualification::close_unavailable()const noexcept{return impl_->close_missing.load(std::memory_order_relaxed);}
configured_close_qualification_record configured_recovery_qualification::close_record(size_t index)const noexcept{
    if(index>=impl_->close_published.load(std::memory_order_acquire))return {};return impl_->close_events[index];
}
configured_recovery_qualification::configured_recovery_qualification():impl_(std::make_shared<implementation>()){}
void configured_recovery_qualification::run(void* context,void (*body)(void*))const {
    {std::lock_guard<std::mutex> lock(impl_->mutex);if(impl_->entered||!body){impl_->bad=true;return;}impl_->entered=true;}
    struct restore {
        std::shared_ptr<implementation> value;
        std::shared_ptr<detail::configured_recovery_observer> prior;
        ~restore(){
            detail::configured_recovery_test_hooks::observation=std::move(prior);
        }
    } scope{impl_,std::move(detail::configured_recovery_test_hooks::observation)};
    detail::configured_recovery_test_hooks::observation=impl_;
    detail::configured_recovery_test_hooks::successor_permission permission;
    body(context); // Synchronous; the caller catches/retains its own Swift error.
}
size_t configured_recovery_qualification::count()const noexcept{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->used;}
bool configured_recovery_qualification::overflowed()const noexcept{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->overflow;}
bool configured_recovery_qualification::invalid()const noexcept{std::lock_guard<std::mutex> lock(impl_->mutex);return impl_->bad;}
configured_recovery_qualification_record configured_recovery_qualification::record(size_t index)const noexcept{
    std::lock_guard<std::mutex> lock(impl_->mutex);if(index>=impl_->used)return {};return impl_->events[index];
}
}
