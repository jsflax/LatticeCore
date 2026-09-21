#include "sync_callback_lifetime.hpp"
#include <lattice/lattice.hpp>
#include <limits>

namespace lattice::detail {
namespace sync_background_test_hooks {thread_local std::shared_ptr<const ack_schedule> ack;}

struct sync_callback_lifetime::execution {
    sync_callback_lifetime& cell;uint64_t generation;execution* prior;
    std::shared_ptr<lattice_db> database;
    execution(sync_callback_lifetime& c,uint64_t g,std::shared_ptr<lattice_db> db):cell(c),generation(g),prior(current_),database(std::move(db)){current_=this;}
    ~execution(){current_=prior;{std::lock_guard<std::mutex> lock(cell.mutex_);--cell.active_;}cell.settled_.notify_all();}
};
thread_local sync_callback_lifetime::execution* sync_callback_lifetime::current_=nullptr;
sync_callback_lifetime::sync_callback_lifetime(synchronizer_base* owner,const std::shared_ptr<lattice_db>& db):owner_(owner),database_(db){}
uint64_t sync_callback_lifetime::dispatch_generation(){
    for(auto* e=current_;e;e=e->prior)if(&e->cell==this)return e->generation;
    std::lock_guard<std::mutex> lock(mutex_);return generation_;
}
void sync_callback_lifetime::publish_generation(uint64_t generation){std::lock_guard<std::mutex> lock(mutex_);generation_=generation;}
bool sync_callback_lifetime::can_begin_protected(uint64_t generation){std::lock_guard<std::mutex> lock(mutex_);return !retired_&&owner_&&generation_==generation&&!ever_connected_;}
void sync_callback_lifetime::begin_connect(uint64_t generation,bool protected_route){
    std::lock_guard<std::mutex> lock(mutex_);
    if(retired_||!owner_||generation_!=generation||(protected_route&&ever_connected_)||protected_)
        throw db_error("protected export requires a fresh physical transport endpoint");
    ever_connected_=true;
    if(protected_route){protected_=true;protected_generation_=generation;attempt_live_=true;}
}
bool sync_callback_lifetime::protected_current(uint64_t generation){std::lock_guard<std::mutex> lock(mutex_);return !retired_&&owner_&&protected_&&attempt_live_&&protected_generation_==generation&&generation_==generation;}
void sync_callback_lifetime::end_protected_attempt(){std::lock_guard<std::mutex> lock(mutex_);if(protected_)attempt_live_=false;}
bool sync_callback_lifetime::executing_here()const noexcept{for(auto* e=current_;e;e=e->prior)if(&e->cell==this)return true;return false;}
void sync_callback_lifetime::retire()noexcept{std::lock_guard<std::mutex> lock(mutex_);retired_=true;owner_=nullptr;attempt_live_=false;}
bool sync_callback_lifetime::current(uint64_t generation){std::lock_guard<std::mutex> lock(mutex_);return !retired_&&owner_&&generation_==generation;}
bool sync_callback_lifetime::protected_route(){std::lock_guard<std::mutex> lock(mutex_);return protected_;}
void sync_callback_lifetime::retire_and_wait(){retire();wait_for_foreign();}
void sync_callback_lifetime::wait_for_foreign(){
    uint64_t own=0;for(auto* e=current_;e;e=e->prior)if(&e->cell==this)++own;
    std::unique_lock<std::mutex> lock(mutex_);
    settled_.wait(lock,[&]{return active_<=own;}); // wait releases leaf lock
}
bool sync_callback_lifetime::run(uint64_t generation,const std::function<void()>& work,bool require_live){
    std::shared_ptr<lattice_db> db;
    {std::lock_guard<std::mutex> lock(mutex_);
        if(retired_||!owner_||generation_!=generation||(protected_&&(protected_generation_!=generation||(require_live&&!attempt_live_))))return false;
        if(active_==std::numeric_limits<uint64_t>::max())return false;
        db=database_.lock();++active_;
    }
    execution turn(*this,generation,std::move(db));work();return true;
}
void sync_callback_lifetime::transport(const std::function<void()>& work){
    uint64_t generation;{std::lock_guard<std::mutex> lock(mutex_);if(protected_&&!attempt_live_)return;generation=protected_?protected_generation_:generation_;}
    run(generation,work);
}
void sync_callback_lifetime::queued(uint64_t generation,const std::function<void()>& work){run(generation,work);}
void sync_callback_lifetime::terminal_notification(uint64_t generation,const std::function<void()>& work){run(generation,work,false);}
namespace {
class lifetime_scheduler final : public scheduler {
    std::shared_ptr<scheduler> target_;std::shared_ptr<sync_callback_lifetime> lifetime_;
public:
    lifetime_scheduler(std::shared_ptr<scheduler> target,std::shared_ptr<sync_callback_lifetime> lifetime):target_(std::move(target)),lifetime_(std::move(lifetime)){}
    void invoke(std::function<void()>&& fn)override{
        auto lifetime=lifetime_;const auto generation=lifetime->dispatch_generation();
        // Construct/copy all user-owned captures before any leaf admission.
        auto work=std::make_shared<std::function<void()>>(std::move(fn));
        const auto target=target_;
        target->invoke([lifetime,generation,work]{lifetime->queued(generation,*work);});
    }
    std::shared_ptr<scheduler> target()const{return target_;}
    bool is_on_thread()const noexcept override{return target_->is_on_thread();}
    bool is_same_as(const scheduler* other)const noexcept override{
        const auto* wrapper=dynamic_cast<const lifetime_scheduler*>(other);return wrapper&&target_->is_same_as(wrapper->target_.get());
    }
    bool can_invoke()const noexcept override{return target_->can_invoke();}
    void shutdown()override{target_->shutdown();}
};
}
void schedule_sync_terminal_notification(std::shared_ptr<scheduler> target,std::shared_ptr<sync_callback_lifetime> lifetime,uint64_t generation,std::function<void()> work){
    // Only copied terminal notification payloads use this seam. It bypasses
    // ended-attempt admission, never owner retirement or generation fencing.
    if(const auto wrapper=std::dynamic_pointer_cast<lifetime_scheduler>(target))target=wrapper->target();
    target->invoke([lifetime,generation,work=std::move(work)]{lifetime->terminal_notification(generation,work);});
}
void report_sync_background_error(std::shared_ptr<scheduler> scheduled,std::shared_ptr<sync_callback_lifetime> lifetime,uint64_t generation,
    std::function<void(const std::string&)> error,std::exception_ptr failure,const char* stage) noexcept {
    // This epilogue owns no synchronizer pointer. Even an error callback may
    // destroy the owner or throw; neither escapes a background thread entry.
    try {
        std::string message(stage);message+=" failed: ";
        try {if(failure)std::rethrow_exception(failure);message+="unknown error";}
        catch(const std::exception& e){const char* what=e.what();size_t n=0;if(what)while(n<2048&&what[n])++n;message.append(what?what:"",n);}
        catch(...){message+="unknown exception";}
        LOG_ERROR("synchronizer","%s",message.c_str());
        if(error)schedule_sync_terminal_notification(std::move(scheduled),std::move(lifetime),generation,
            [error=std::move(error),message=std::move(message)] {
                try {error(message);}catch(...) {LOG_ERROR("synchronizer","background failure callback threw");}
            });
    } catch(...) {LOG_ERROR("synchronizer","background failure could not be delivered");}
}
std::shared_ptr<scheduler> make_sync_lifetime_scheduler(std::shared_ptr<scheduler> target,std::shared_ptr<sync_callback_lifetime> lifetime){return std::make_shared<lifetime_scheduler>(std::move(target),std::move(lifetime));}

#ifndef __EMSCRIPTEN__
std::thread sync_retirement_lane::launch(std::function<void()> work){return std::thread(std::move(work));}
sync_retirement_lane::sync_retirement_lane(size_t capacity,launch_function start):capacity_(capacity){
    if(!capacity||capacity>slots_.size())throw db_error("invalid protected retirement capacity");
    worker_=start([this]{loop();}); // Failure precedes any published reservation.
    if(!worker_.joinable())throw db_error("protected retirement worker did not launch");
}
sync_retirement_lane::~sync_retirement_lane(){
    {std::lock_guard<std::mutex> lock(mutex_);stopping_=true;for(auto& s:slots_)if(s.state==phase::reserved)s.state=phase::queued;}
    ready_.notify_all();if(worker_.joinable())worker_.join();
}
std::shared_ptr<sync_retirement_lane> sync_retirement_lane::instance(){
    // Production lifetime is process-wide. Individual callback retirement can
    // never destroy/join this lane on one of the transport callback threads.
    // Intentionally process-lived: a quarantined transport must not be finally
    // released by static destruction on an arbitrary callback/shutdown thread.
    // This is one fixed lane and exactly 64 slots, not one leak per retirement.
    static const auto* lane=new std::shared_ptr<sync_retirement_lane>(new sync_retirement_lane(64,launch));return *lane;
}
sync_retirement_lane::reservation::reservation(std::shared_ptr<sync_retirement_lane> lane,size_t slot,uint64_t serial):lane_(std::move(lane)),slot_(slot),serial_(serial){}
sync_retirement_lane::reservation::reservation(reservation&& other)noexcept:lane_(std::move(other.lane_)),slot_(other.slot_),serial_(other.serial_),published_(other.published_){}
sync_retirement_lane::reservation::~reservation(){if(lane_){if(published_)lane_->request(slot_,serial_);else lane_->cancel(slot_,serial_);}}
void sync_retirement_lane::reservation::retire(std::thread pacer)noexcept{if(lane_){lane_->request(slot_,serial_,std::move(pacer));lane_.reset();}else if(pacer.joinable())std::terminate();}
sync_retirement_lane::reservation sync_retirement_lane::reserve(std::shared_ptr<sync_transport> transport,std::shared_ptr<sync_callback_lifetime> lifetime){
    if(!transport)throw db_error("protected retirement requires actual transport");
    auto keep=shared_from_this();
    std::lock_guard<std::mutex> lock(mutex_);if(stopping_||next_order_==std::numeric_limits<uint64_t>::max())throw db_error("protected retirement lane stopped or serial exhausted");
    for(size_t i=0;i<capacity_;++i){auto& slot=slots_[i];if(slot.state!=phase::free)continue;
        if(slot.serial==std::numeric_limits<uint64_t>::max())continue;
        ++slot.serial;slot.queued_order=++next_order_;slot.transport=std::move(transport);slot.lifetime=std::move(lifetime);slot.state=phase::reserved;return {std::move(keep),i,slot.serial};
    }
    throw db_error("protected retirement capacity exhausted before publication");
}
void sync_retirement_lane::request(size_t i,uint64_t serial,std::thread pacer)noexcept{
    {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[i];
     if(s.serial==serial&&s.state==phase::reserved){s.pacer=std::move(pacer);s.state=phase::queued;}
     else if(pacer.joinable())std::terminate();} // Lost thread custody is a bug, never detach.
    ready_.notify_one();
}
void sync_retirement_lane::cancel(size_t i,uint64_t serial)noexcept{
    std::shared_ptr<sync_transport> discarded;std::shared_ptr<sync_callback_lifetime> discarded_lifetime;
    {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[i];if(s.serial==serial&&s.state==phase::reserved){discarded=std::move(s.transport);discarded_lifetime=std::move(s.lifetime);s.state=phase::free;}}
    settled_.notify_all(); // discarded captures are released outside leaf lock
}
void sync_retirement_lane::loop(){
    for(;;){size_t index=slots_.size();std::shared_ptr<sync_transport> held;
        {std::unique_lock<std::mutex> lock(mutex_);ready_.wait(lock,[&]{if(stopping_)return true;for(size_t i=0;i<capacity_;++i)if(slots_[i].state==phase::queued)return true;return false;});
            for(size_t i=0;i<capacity_;++i)if(slots_[i].state==phase::queued &&
                (index==slots_.size()||slots_[i].queued_order<slots_[index].queued_order))index=i;
            if(index==slots_.size()){if(stopping_)return;continue;}
            auto& s=slots_[index];s.state=phase::active;held=s.transport;
        }
        bool complete=false;
        try{
            held->disconnect();
            auto& slot=slots_[index]; // Active slot is exclusively worker-owned.
            if(slot.pacer.joinable()){
                if(slot.pacer.get_id()==std::this_thread::get_id())throw db_error("protected retirement self join refused");
                slot.pacer.join();
            }
            // disconnect() is not a quiescence contract. Wait the independent
            // admitted turns too, retaining actual transport through the wait.
            if(slot.lifetime)slot.lifetime->wait_for_foreign();
            complete=true;
        }catch(...){}
        std::shared_ptr<sync_transport> discarded;std::shared_ptr<sync_callback_lifetime> discarded_lifetime;
        {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[index];if(complete){discarded=std::move(s.transport);discarded_lifetime=std::move(s.lifetime);}else s.state=phase::quarantined;}
        held.reset();discarded.reset();
        if(complete){std::lock_guard<std::mutex> lock(mutex_);slots_[index].state=phase::free;}
        settled_.notify_all();
    }
}
#endif
} // namespace lattice::detail
