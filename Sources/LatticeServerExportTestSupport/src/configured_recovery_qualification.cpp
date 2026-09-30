#include "../include/configured_recovery_qualification.hpp"
#include "../../LatticeCore/src/configured_recovery_connection.hpp"
#include "../../LatticeCore/src/configured_recovery_observation.hpp"
#include <array>
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
