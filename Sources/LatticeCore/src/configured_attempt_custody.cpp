#include "configured_attempt_custody.hpp"
#include <stdexcept>

namespace lattice::detail {
std::function<void()> retain_configured_payload(configured_attempt_custody::lease charge,std::function<void()> work){
    struct payload {
        configured_attempt_custody::lease charge;
        std::function<void()> work;
        payload(configured_attempt_custody::lease c,std::function<void()> w):charge(std::move(c)),work(std::move(w)){}
        payload(payload&&) noexcept=default;
    };
    if(!charge)throw std::runtime_error("configured payload requires reserved custody");
    // Keep destruction order defined even if allocation throws before the
    // retained object is constructed; function parameter order cannot do it.
    payload pending(std::move(charge),std::move(work));
    auto retained=std::make_shared<payload>(std::move(pending));
    return [retained]{retained->work();};
}
void configured_attempt_custody::bind_wakeup(std::function<void()> callback){
    auto value=std::make_shared<const std::function<void()>>(std::move(callback));
    std::lock_guard<std::mutex> lock(mutex_);
    if(wakeup_)throw std::logic_error("configured attempt wakeup already bound");
    wakeup_=std::move(value);
}
void configured_attempt_custody::bind_foreign_probe(std::function<void(configured_bridge_operation)> callback){
    auto value=std::make_shared<const std::function<void(configured_bridge_operation)>>(std::move(callback));
    std::lock_guard<std::mutex> lock(mutex_);
    if(foreign_probe_||commands_||payloads_)throw std::logic_error("configured foreign probe must precede construction");
    foreign_probe_=std::move(value);
}
void configured_attempt_custody::before_foreign(configured_bridge_operation operation)const{
    std::shared_ptr<const std::function<void(configured_bridge_operation)>> probe;
    {std::lock_guard<std::mutex> lock(mutex_);probe=foreign_probe_;}
    if(probe&&*probe)(*probe)(operation);
}
void configured_attempt_custody::launch_worker(lease charge,std::function<void()> work){
#ifdef __EMSCRIPTEN__
    throw std::logic_error("configured native workers unavailable in browser graph");
#else
    if(!charge||charge.owner_.get()!=this||charge.kind_!=kind::payload)
        throw std::logic_error("configured worker requires its actual reserved payload");
    std::function<void()> retained;
    try{retained=retain_configured_payload(std::move(charge),std::move(work));}
    catch(...){fail(-5);throw;}
    auto keep=shared_from_this();size_t index=workers_.size();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(closed_)throw std::runtime_error("configured worker admission closed");
        for(size_t i=0;i<workers_.size();++i)if(!workers_[i].reserved){index=i;workers_[i].reserved=true;++worker_count_;break;}
        if(index==workers_.size()){if(!first_error_)first_error_=-2;closed_=true;}
    }
    if(index==workers_.size()){wake();throw std::runtime_error("configured retained worker capacity exhausted");}
    try {
        std::thread thread([keep,index,retained=std::move(retained)]()mutable{
            try{retained();}catch(...){keep->fail(-5);}
            // Work/captures and their charge settle before readiness is
            // published. Only a later actual join proves OS-thread exit.
            retained=nullptr;
            {std::lock_guard<std::mutex> lock(keep->mutex_);auto& slot=keep->workers_[index];slot.finished=true;if(slot.published)++keep->finished_worker_count_;}
            keep->wake();
        });
        {std::lock_guard<std::mutex> lock(mutex_);auto& slot=workers_[index];slot.thread=std::move(thread);slot.published=true;if(slot.finished)++finished_worker_count_;}
        wake();
    }catch(...){
        {std::lock_guard<std::mutex> lock(mutex_);workers_[index].reserved=false;--worker_count_;}
        fail(-5);throw;
    }
#endif
}
bool configured_attempt_custody::join_finished_workers()noexcept{
#ifndef __EMSCRIPTEN__
    for(size_t i=0;i<workers_.size();++i){
        std::thread thread;
        {std::lock_guard<std::mutex> lock(mutex_);auto& slot=workers_[i];
            if(!slot.reserved||!slot.published||!slot.finished||slot.joining)continue;
            slot.joining=true;--finished_worker_count_;thread=std::move(slot.thread);
        }
        try{
            if(!thread.joinable()||thread.get_id()==std::this_thread::get_id())throw std::logic_error("configured worker join custody invalid");
            thread.join();
        }catch(...){
            {std::lock_guard<std::mutex> lock(mutex_);auto& slot=workers_[i];slot.thread=std::move(thread);slot.joining=false;++finished_worker_count_;}
            fail(-5);return false; // Preserve the actual failed handle and charge.
        }
        {std::lock_guard<std::mutex> lock(mutex_);auto& slot=workers_[i];slot.reserved=false;slot.published=false;slot.finished=false;slot.joining=false;--worker_count_;}
    }
    wake();
#endif
    return true;
}
configured_attempt_custody::lease configured_attempt_custody::admit(kind value,bool terminal){
    // shared_from_this only retains an existing control block; no allocation.
    auto keep=shared_from_this();
    bool overflow=false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if((value==kind::payload&&payloads_sealed_)||(closed_&&!(terminal&&value==kind::payload)))return {};
        auto& count=value==kind::command?commands_:payloads_;
        const auto limit=value==kind::command?maximum_commands:maximum_payloads;
        if(count==limit){if(!first_error_)first_error_=-2;closed_=true;overflow=true;}
        else {++count;return {std::move(keep),value};}
    }
    if(overflow){wake();throw std::runtime_error("configured attempt admission capacity exhausted");}
    return {};
}
void configured_attempt_custody::close() noexcept {
    {std::lock_guard<std::mutex> lock(mutex_);closed_=true;}
    wake();
}
void configured_attempt_custody::seal_payloads() noexcept {
    {std::lock_guard<std::mutex> lock(mutex_);closed_=true;payloads_sealed_=true;}
    wake();
}
void configured_attempt_custody::fail(int32_t error) noexcept {
    {std::lock_guard<std::mutex> lock(mutex_);if(error&&!first_error_)first_error_=error;}
    wake();
}
configured_attempt_custody::observation configured_attempt_custody::snapshot()const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);return {commands_,payloads_,closed_,first_error_,worker_count_,finished_worker_count_};
}
void configured_attempt_custody::wake()const noexcept {
    std::shared_ptr<const std::function<void()>> callback;
    {std::lock_guard<std::mutex> lock(mutex_);callback=wakeup_;}
    if(callback&&*callback)try{(*callback)();}catch(...){}
}
void configured_attempt_custody::release(kind value) noexcept {
    {std::lock_guard<std::mutex> lock(mutex_);auto& count=value==kind::command?commands_:payloads_;--count;}
    wake();
}
}
