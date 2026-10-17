#include "recovery_server_export.hpp"
#include <limits>
#include <utility>

namespace lattice::detail {
// Deliberately ONLY scalar leaf state. No native-bearing type belongs here.
struct recovery_server_control {
    std::mutex mutex;
    bool stopped=false,busy=false,custody_released=false;
    uint64_t serial=0;
};
struct recovery_server_delivery {
    std::shared_ptr<recovery_server_control> control;
    const uint64_t serial;
    std::mutex mutex;
    bool admitted=false,accepted=false,native_done=false,completed=false,success=false,released=false;
    recovery_server_delivery(std::shared_ptr<recovery_server_control> c,uint64_t s):control(std::move(c)),serial(s){}
    void release_if_settled()noexcept {
        // Caller holds delivery mutex. Nothing locks control then delivery.
        if(released||!native_done||(admitted&&!completed))return;
        released=true;
        std::lock_guard<std::mutex> lock(control->mutex);
        if(control->busy&&control->serial==serial)control->busy=false;
    }
    void finish_native()noexcept {
        std::lock_guard<std::mutex> lock(mutex);native_done=true;release_if_settled();
    }
};
namespace {
// Never retain an arbitrary foreign exception object: it could itself own a
// page/context. Copy only bounded diagnostic text into our own standard error.
// Called from a catch; allocation failure is itself a payload-free diagnostic.
std::exception_ptr copied_failure(const char* stage)noexcept {
    try {
        try { throw; }
        catch(const std::exception& error){
            const char* text=error.what();size_t count=0;if(text)while(count<512&&text[count])++count;
            throw db_error(std::string(stage)+": "+(text?std::string(text,count):std::string("exception")));
        }catch(...){throw db_error(std::string(stage)+": unknown exception");}
    }catch(...){return std::current_exception();}
}
struct context_custody {
    std::shared_ptr<lattice_db> owner;
    std::shared_ptr<void> context;
    recovery_server_export_endpoint::enqueue_fn enqueue;
    std::shared_ptr<recovery_server_control> control;
    ~context_custody(){
        // Actual foreign/native release is before publication and off locks.
        context.reset();owner.reset();
        std::lock_guard<std::mutex> lock(control->mutex);control->custody_released=true;
    }
};
bool stopped(const std::shared_ptr<recovery_server_control>& control)noexcept {
    std::lock_guard<std::mutex> lock(control->mutex);return control->stopped;
}
}
struct recovery_server_endpoint_state {
    std::mutex mutex;
    std::shared_ptr<recovery_server_control> control;
    std::shared_ptr<context_custody> custody;
    recovery_export_limits limits;
    ~recovery_server_endpoint_state(){
        {std::lock_guard<std::mutex> lock(control->mutex);control->stopped=true;}
        custody.reset(); // final caller must obey IO destruction contract
    }
};
struct recovery_server_page_state {
    std::mutex mutex;
    server_export_status status=server_export_status::ready;
    const int64_t count,last;
    std::optional<committed_export_frame> frame;
    std::shared_ptr<recovery_server_endpoint_state> endpoint;
    std::shared_ptr<recovery_server_delivery> delivery;
    std::exception_ptr error;
    recovery_server_page_state(committed_export_frame f,std::shared_ptr<recovery_server_endpoint_state> e,
        std::shared_ptr<recovery_server_delivery> d):count(static_cast<int64_t>(f.entries().size())),last(*f.last_audit_id()),
        frame(std::move(f)),endpoint(std::move(e)),delivery(std::move(d)){}
    ~recovery_server_page_state(){
        frame.reset();endpoint.reset();error=nullptr;delivery->finish_native();
    }
};
recovery_server_export_stop::recovery_server_export_stop(std::shared_ptr<recovery_server_control> c):control_(std::move(c)){}
void recovery_server_export_stop::request_stop()const noexcept {
    if(control_){std::lock_guard<std::mutex> lock(control_->mutex);control_->stopped=true;}
}
bool recovery_server_export_stop::stopped()const noexcept {
    return !control_||detail::stopped(control_);
}
bool recovery_server_export_stop::resources_released()const noexcept {
    if(!control_)return true;
    std::lock_guard<std::mutex> lock(control_->mutex);return control_->custody_released&&!control_->busy;
}
recovery_server_export_completion::recovery_server_export_completion(std::shared_ptr<recovery_server_delivery> d):delivery_(std::move(d)){}
bool recovery_server_export_completion::record_result(bool success)const noexcept {
    if(!delivery_)return false;
    std::lock_guard<std::mutex> lock(delivery_->mutex);
    if(!delivery_->admitted||delivery_->completed)return false;
    delivery_->completed=true;delivery_->success=success;delivery_->release_if_settled();return true;
}
bool recovery_server_export_completion::completed()const noexcept {
    if(!delivery_)return false;std::lock_guard<std::mutex> lock(delivery_->mutex);return delivery_->completed;
}
bool recovery_server_export_completion::permits_advance()const noexcept {
    if(!delivery_)return false;
    std::lock_guard<std::mutex> lock(delivery_->mutex);
    if(!delivery_->native_done||!delivery_->accepted||!delivery_->completed||!delivery_->success)return false;
    std::lock_guard<std::mutex> gate(delivery_->control->mutex);
    return !delivery_->control->stopped&&delivery_->control->serial==delivery_->serial;
}
recovery_server_export_endpoint::recovery_server_export_endpoint(std::shared_ptr<recovery_server_endpoint_state> state):state_(std::move(state)){}
recovery_server_export_endpoint recovery_server_export_endpoint::create_for_qualification(
    std::shared_ptr<lattice_db> owner,void* context,enqueue_fn enqueue,destroy_fn destroy,const recovery_export_limits& limits){
    // No ownership transfer is possible without a valid release operation.
    // This one precondition refusal leaves context with the caller.
    if(!destroy)throw db_error("server export destroy callback required; context not transferred");
    // shared_ptr invokes its deleter even if control-block allocation fails.
    std::shared_ptr<void> owned(context,[destroy](void* p)noexcept{if(destroy){try{destroy(p);}catch(...){}}});
    if(!owner||owner->is_closed()||!enqueue)throw db_error("server export requires retained owner and owned immutable sink");
    recovery_continuous_producer::require_no_continuous_route(*owner);
    recovery_export_adapter::validate_server_limits(limits);
    auto control=std::make_shared<recovery_server_control>();
    auto custody=std::make_shared<context_custody>();custody->owner=std::move(owner);custody->context=std::move(owned);custody->enqueue=enqueue;custody->control=control;
    auto state=std::make_shared<recovery_server_endpoint_state>();state->control=std::move(control);state->custody=std::move(custody);state->limits=limits;
    return recovery_server_export_endpoint(std::move(state));
}
bool recovery_server_export_endpoint::valid()const noexcept{return bool(state_);}
recovery_server_export_stop recovery_server_export_endpoint::stop_token()const noexcept{return state_?recovery_server_export_stop(state_->control):recovery_server_export_stop();}
void recovery_server_export_endpoint::close_on_io()const noexcept {
    const auto state=state_;if(!state)return;stop_token().request_stop();std::shared_ptr<context_custody> retired;
    {std::lock_guard<std::mutex> lock(state->mutex);retired=std::move(state->custody);}
    retired.reset();
}
recovery_server_export_page::recovery_server_export_page(server_export_status status,std::exception_ptr error):fallback_(status),fallback_error_(std::move(error)){}
recovery_server_export_page::recovery_server_export_page(std::shared_ptr<recovery_server_page_state> state):state_(std::move(state)){}
recovery_server_export_page recovery_server_export_endpoint::prepare_history(int64_t after,size_t count)const noexcept {
    const auto state=state_;if(!state)return recovery_server_export_page(server_export_status::invalid);
    std::shared_ptr<recovery_server_delivery> delivery;
    bool reserved=false;
    try {
        uint64_t serial;
        {std::lock_guard<std::mutex> lock(state->control->mutex);
         if(state->control->stopped)return recovery_server_export_page(server_export_status::stopped);
         if(state->control->busy)return recovery_server_export_page(server_export_status::busy);
         if(state->control->serial==std::numeric_limits<uint64_t>::max())return recovery_server_export_page(server_export_status::refused);
         serial=++state->control->serial;state->control->busy=true;reserved=true;}
        delivery=std::make_shared<recovery_server_delivery>(state->control,serial);
        std::shared_ptr<context_custody> custody;
        {std::lock_guard<std::mutex> lock(state->mutex);custody=state->custody;}
        if(!custody||stopped(state->control)){delivery->finish_native();return recovery_server_export_page(server_export_status::stopped);}
        // Zero is intentionally unused native-route metadata. Endpoint object
        // identity, not an integer generation, encloses this server page.
        auto prepared=recovery_export_adapter::prepare_history_page(custody->owner,0,after,count,state->limits);
        if(stopped(state->control)){
            prepared.frame.reset();custody.reset();delivery->finish_native();return recovery_server_export_page(server_export_status::stopped);
        }
        if(!prepared.frame){
            const auto code=prepared.protected_store?server_export_status::empty:server_export_status::unprotected;
            custody.reset();delivery->finish_native();return recovery_server_export_page(code);
        }
        auto page=std::make_shared<recovery_server_page_state>(std::move(*prepared.frame),state,delivery);
        return recovery_server_export_page(std::move(page));
    }catch(...){
        if(delivery)delivery->finish_native();
        else if(reserved){std::lock_guard<std::mutex> lock(state->control->mutex);state->control->busy=false;}
        // Preparation has no native-bearing result on failure; use the bridge
        // caller's explicit status rather than misreporting an empty page.
        return recovery_server_export_page(server_export_status::failed,copied_failure("server export preparation"));
    }
}
server_export_status recovery_server_export_page::status()const noexcept {
    if(!state_)return fallback_;std::lock_guard<std::mutex> lock(state_->mutex);return state_->status;
}
uint64_t recovery_server_export_page::serial()const noexcept{return state_?state_->delivery->serial:0;}
int64_t recovery_server_export_page::count()const noexcept{return state_?state_->count:0;}
std::optional<int64_t> recovery_server_export_page::last_audit_id()const noexcept{return state_?std::optional<int64_t>(state_->last):std::nullopt;}
recovery_server_export_completion recovery_server_export_page::completion()const noexcept{return state_?recovery_server_export_completion(state_->delivery):recovery_server_export_completion();}
std::exception_ptr recovery_server_export_page::failure()const noexcept {
    if(!state_)return fallback_error_;std::lock_guard<std::mutex> lock(state_->mutex);return state_->error;
}
void recovery_server_export_page::close_on_io()const noexcept {
    const auto state=state_;if(!state)return;
    std::optional<committed_export_frame> retired;std::shared_ptr<recovery_server_endpoint_state> endpoint;std::exception_ptr error;
    {std::lock_guard<std::mutex> lock(state->mutex);
     if(state->frame){retired=std::move(state->frame);state->frame.reset();endpoint=std::move(state->endpoint);state->status=server_export_status::consumed;}
     error=std::move(state->error);}
    if(retired){retired.reset();endpoint.reset();error=nullptr;state->delivery->finish_native();}
}
server_export_status recovery_server_export_page::consume()const noexcept {
    const auto state=state_;if(!state)return fallback_;
    struct turn {
        std::optional<committed_export_frame> frame;
        std::shared_ptr<recovery_server_endpoint_state> endpoint;
        std::shared_ptr<context_custody> custody;
        std::shared_ptr<recovery_server_delivery> delivery;
        ~turn(){frame.reset();custody.reset();endpoint.reset();if(delivery)delivery->finish_native();}
    } active;
    {std::lock_guard<std::mutex> lock(state->mutex);
     if(!state->frame)return server_export_status::consumed;
     active.frame=std::move(state->frame);state->frame.reset();active.endpoint=std::move(state->endpoint);
     active.delivery=state->delivery;state->status=server_export_status::consumed;}
    const auto set_status=[&](server_export_status value){std::lock_guard<std::mutex> lock(state->mutex);state->status=value;return value;};
    try {
        {std::lock_guard<std::mutex> lock(active.endpoint->mutex);active.custody=active.endpoint->custody;}
        if(!active.custody||stopped(active.endpoint->control)||active.custody->owner->is_closed())return set_status(server_export_status::stopped);
        recovery_export_adapter::revalidate_claimed_frame(*active.frame);
        if(active.custody->owner->is_closed())return set_status(server_export_status::stopped);
        // This scalar stop check is the final enqueue admission point. A
        // subsequent stop may coexist with this already admitted callback.
        if(stopped(active.endpoint->control))return set_status(server_export_status::stopped);
        {std::lock_guard<std::mutex> lock(active.delivery->mutex);active.delivery->admitted=true;}
        const auto& bytes=active.frame->message_.data;
        const int32_t result=active.custody->enqueue(active.custody->context.get(),bytes.data(),bytes.size(),active.delivery->serial);
        {std::lock_guard<std::mutex> lock(active.delivery->mutex);
         active.delivery->accepted=result==1;
         if(result==0){active.delivery->completed=true;active.delivery->success=false;}}
        return set_status(result==1?server_export_status::enqueued:server_export_status::failed);
    }catch(...){
        auto error=copied_failure("server export consumption");
        {std::lock_guard<std::mutex> lock(state->mutex);state->error=std::move(error);state->status=server_export_status::failed;}
        return server_export_status::failed;
    }
}
} // namespace lattice::detail
