#include "configured_recovery_connection.hpp"
#ifndef __EMSCRIPTEN__
#include "configured_platform.hpp"
#include "recovery_producer_continuity.hpp"
#include "recovery_export_adapter.hpp"
#include <lattice/lattice.hpp>
#include <array>
#include <cmath>
#include <limits>

namespace lattice::detail {
namespace {
using clock=std::chrono::steady_clock;
// Private scoped qualification permission is deliberately absent from public
// configuration/ABI. Default production intent is retained but successor
// dialing is refused until exact production-path qualification is accepted.
thread_local bool qualifying_configured_successors=false;
std::exception_ptr configured_error(const char* text)noexcept{
    try{throw db_error(text);}catch(...){return std::current_exception();}
}
}

configured_recovery_test_hooks::successor_permission::successor_permission()noexcept
    :prior_(qualifying_configured_successors){qualifying_configured_successors=true;}
configured_recovery_test_hooks::successor_permission::~successor_permission(){qualifying_configured_successors=prior_;}
thread_local std::function<void()> configured_recovery_test_hooks::before_service_registration;
thread_local std::function<void()> configured_recovery_test_hooks::after_transport_creation;
thread_local std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> configured_recovery_test_hooks::before_backoff_publication;

// Fixed records, one event/timer dispatcher and one bounded cleanup lane. The
// dispatcher never performs SQL, joins, or waits for a platform future. The
// cleanup lane only receives bundles already fenced against new work.
class configured_control_service {
    struct slot {
        std::shared_ptr<configured_recovery_connection> owner;
        bool dirty=false,cleanup=false,busy=false,dispatching=false,release_pending=false;
        clock::time_point due=clock::time_point::max();
    };
    std::array<slot,64> slots_;
    std::mutex mutex_;
    std::condition_variable changed_,cleanup_ready_;
    std::thread dispatcher_,collector_;
    bool stopping_=false;
    static thread_local bool cleanup_thread_;
    configured_control_service(){
        dispatcher_=std::thread([this]{dispatch_loop();});
        try{collector_=std::thread([this]{cleanup_loop();});}
        catch(...){
            {std::lock_guard<std::mutex> lock(mutex_);stopping_=true;}
            changed_.notify_all();dispatcher_.join();throw;
        }
    }
    void dispatch_loop(){
        for(;;){std::shared_ptr<configured_recovery_connection> owner;size_t index=slots_.size();
            {std::unique_lock<std::mutex> lock(mutex_);
                for(;;){
                    if(stopping_)return;
                    const auto now=clock::now();auto due=clock::time_point::max();
                    for(size_t i=0;i<slots_.size();++i){auto& s=slots_[i];if(!s.owner)continue;
                        if(s.dirty||s.due<=now){owner=s.owner;index=i;s.dispatching=true;s.dirty=false;s.due=clock::time_point::max();break;}
                        due=std::min(due,s.due);
                    }
                    if(owner)break;
                    if(due==clock::time_point::max())changed_.wait(lock);else changed_.wait_until(lock,due);
                }
            }
            owner->dispatch();
            // Keep the slot's strong owner until this transient dispatcher
            // copy has actually died. Final arbitrary captures must never be
            // released by the event dispatcher as its last owner.
            owner.reset();
            {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[index];s.dispatching=false;if(s.release_pending)s.cleanup=true;}
            cleanup_ready_.notify_one();
        }
    }
    void cleanup_loop(){
        for(;;){std::shared_ptr<configured_recovery_connection> owner;size_t index=slots_.size();
            {std::unique_lock<std::mutex> lock(mutex_);
                cleanup_ready_.wait(lock,[&]{for(const auto& s:slots_)if(s.owner&&s.cleanup&&!s.busy)return true;return stopping_;});
                if(stopping_)return;
                for(size_t i=0;i<slots_.size();++i)if(slots_[i].owner&&slots_[i].cleanup&&!slots_[i].busy){
                    auto& s=slots_[i];owner=s.owner;s.cleanup=false;s.busy=true;index=i;break;
                }
            }
            cleanup_thread_=true;owner->finalize();
            {std::lock_guard<std::mutex> lock(mutex_);slots_[index].busy=false;}
            owner->cleanup_returned();
            cleanup_ready_.notify_one();
            owner.reset();cleanup_thread_=false;
        }
    }
public:
    static configured_control_service& instance(){static auto* service=new configured_control_service;return *service;}
    static bool on_cleanup_thread()noexcept{return cleanup_thread_;}
    size_t retain(std::shared_ptr<configured_recovery_connection> owner){
        std::lock_guard<std::mutex> lock(mutex_);
        for(size_t i=0;i<slots_.size();++i)if(!slots_[i].owner&&!slots_[i].busy){slots_[i].owner=std::move(owner);return i;}
        throw db_error("configured control capacity exhausted before child allocation");
    }
    void wake(size_t i,const configured_recovery_connection* owner)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].dirty=true;}
        changed_.notify_one();
    }
    void timer(size_t i,const configured_recovery_connection* owner,clock::time_point due)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].due=due;}
        changed_.notify_one();
    }
    void cleanup(size_t i,const configured_recovery_connection* owner)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].cleanup=true;}
        cleanup_ready_.notify_one();
    }
    bool release(size_t i,const configured_recovery_connection* owner)noexcept{
        std::shared_ptr<configured_recovery_connection> displaced;
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return false;
            auto& s=slots_[i];if(s.dispatching){s.release_pending=true;return false;}
            displaced=std::move(s.owner);s.dirty=false;s.cleanup=false;s.release_pending=false;s.due=clock::time_point::max();
        }
        displaced.reset();return true;
    }
};
thread_local bool configured_control_service::cleanup_thread_=false;

struct configured_recovery_connection::state {
    mutable std::mutex mutex;
    std::condition_variable settled;
    const std::weak_ptr<lattice_db> parent;
    const sync_config config;
    const std::shared_ptr<network_factory> factory;
    configured_platform_factory* const typed;
    const std::shared_ptr<configured_retirement_registry> registry=configured_retirement_registry::instance();
    std::optional<configured_retirement_registry::reservation> reservation;
    std::shared_ptr<lattice_db> child;
    std::shared_ptr<scheduler> scheduled;
    std::shared_ptr<configured_attempt> attempt;
    size_t service_slot=64;
    bool initializing=true,desired=true,closing=false,closed=false,quarantined=false;
    bool needs_action=false,control_queued=false,finalizer_queued=false,worker_join_queued=false,cleanup_running=false;
    bool child_closed=false,scheduler_joined=false,close_release_ready=false;
    const bool successors_permitted=qualifying_configured_successors;
    const std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> backoff_probe=configured_recovery_test_hooks::before_backoff_publication;
    bool successor_refusal_reported=false;
    uint64_t intent_epoch=1,retries=0;
    clock::time_point opened{},due{};
    synchronizer::sync_progress observed;
    std::shared_ptr<const synchronizer::on_state_change_handler> state_handler;
    std::shared_ptr<const synchronizer::on_error_handler> error_handler;
    std::shared_ptr<const synchronizer::on_progress_handler> progress_handler;
    lattice_close_result close_result;
    state(std::weak_ptr<lattice_db> p,sync_config c,std::shared_ptr<network_factory> f,configured_platform_factory* t)
        :parent(std::move(p)),config(std::move(c)),factory(std::move(f)),typed(t),reservation(registry->reserve()){}
};
configured_attempt::configured_attempt(platform_retirement_receipt r,std::shared_ptr<configured_retirement_registry> registry_value,
    std::shared_ptr<network_factory> f,configured_platform_factory* typed)
    :receipt(std::move(r)),registry(std::move(registry_value)),custody(registry->attempt_custody(receipt)),factory(std::move(f)),typed_factory(typed){
    native_reservation.emplace(sync_retirement_lane::instance()->reserve_empty());
}
void configured_attempt::wake()noexcept{if(auto owner=control.lock())owner->notify();}
void configured_attempt::lane_settled(sync_retirement_result result)noexcept{
    {std::lock_guard<std::mutex> lock(facts_mutex);if(lane_complete)return;lane_result=result;lane_complete=true;}
    wake();
}
void configured_attempt::route_settled()noexcept{
    {std::lock_guard<std::mutex> lock(facts_mutex);route_unregistered=true;}
    wake();
}
void configured_attempt::request_renewal()noexcept{if(auto owner=control.lock())owner->request_renewal(shared_from_this());}
void configured_attempt::install_lifetime(std::shared_ptr<sync_callback_lifetime> value){
    std::lock_guard<std::mutex> lock(facts_mutex);
    if(lifetime_&&lifetime_!=value)throw db_error("configured attempt lifetime already installed");
    if(!lifetime_)lifetime_=std::move(value);
}
std::shared_ptr<sync_callback_lifetime> configured_attempt::lifetime()const noexcept{
    std::lock_guard<std::mutex> lock(facts_mutex);return lifetime_;
}
void configured_attempt::bind(std::shared_ptr<sync_transport> transport,std::shared_ptr<sync_callback_lifetime> life,
    const std::shared_ptr<recovery_continuous_route>& route){
    if(!route||!native_reservation||!registry->retain_transport(receipt,transport))throw db_error("configured attempt lost native construction custody");
    install_lifetime(life);
    const std::weak_ptr<configured_attempt> weak=shared_from_this();
    native_reservation->bind(std::move(transport),std::move(life),[weak](sync_retirement_result result){if(auto held=weak.lock())held->lane_settled(result);});
    route->configured_unregistered_=[weak]{if(auto held=weak.lock())held->route_settled();};
    {std::lock_guard<std::mutex> lock(facts_mutex);route_registered=true;}
}
configured_recovery_connection::configured_recovery_connection(std::weak_ptr<lattice_db> parent,sync_config config,
    std::shared_ptr<network_factory> factory,configured_platform_factory* typed)
    :state_(std::make_unique<state>(std::move(parent),std::move(config),std::move(factory),typed)){}
configured_recovery_connection::~configured_recovery_connection()=default;
std::shared_ptr<configured_recovery_connection> configured_recovery_connection::create(
    const std::shared_ptr<lattice_db>& parent,const sync_config& config,const std::shared_ptr<network_factory>& factory,
    configured_platform_factory* typed,const std::function<std::shared_ptr<lattice_db>()>& make_child){
    if(!parent||parent->is_closed()||!factory||!typed)throw db_error("configured renewal requires actual retained parent and typed factory");
    auto owner=std::shared_ptr<configured_recovery_connection>(new configured_recovery_connection(parent,config,factory,typed));
    auto& s=*owner->state_;
    try {
        const auto receipt=s.reservation->begin_attempt();
        try{s.attempt=std::make_shared<configured_attempt>(receipt,s.registry,factory,typed);}
        catch(...){
            // Neither child nor foreign factory has been entered. Failed
            // empty native reservation/allocation is a proved empty attempt.
            s.registry->request_retirement(receipt);receipt.complete_adapter_cleanup(0);
            s.registry->complete_native_cleanup(receipt,0);s.registry->collect_completed(receipt);throw;
        }
        s.attempt->control=owner;
        const std::weak_ptr<configured_recovery_connection> weak=owner;
        s.attempt->custody->bind_wakeup([weak]{if(auto held=weak.lock())held->notify();});
        if(configured_recovery_test_hooks::before_service_registration)configured_recovery_test_hooks::before_service_registration();
        s.service_slot=configured_control_service::instance().retain(owner);
        // Both native capacity charges are now held, before child resources.
        s.child=make_child();
        if(!s.child||!s.child->get_scheduler())throw db_error("configured renewal child scheduler missing");
        s.scheduled=s.child->get_scheduler();
        return owner;
    }catch(...){
        const auto error=std::current_exception();
        {std::lock_guard<std::mutex> lock(s.mutex);s.close_result.remember(error,false);s.closing=true;s.desired=false;}
        if(s.attempt){
            s.attempt->primary_error=error;s.attempt->construction_finished=true;s.attempt->wrapper_destroyed=true;
            s.attempt->native_reservation.reset();s.attempt->lane_settled({0,false,false,false,false,true});
            s.registry->request_retirement(s.attempt->receipt);
            // Foreign factory was NEVER entered. This is actual empty proof.
            s.attempt->receipt.complete_adapter_cleanup(0);
            s.registry->complete_native_cleanup(s.attempt->receipt,0);
            // Registration can fail before a service slot exists. No child or
            // foreign factory has then been entered; collect that actual empty
            // attempt here rather than relying on an impossible wakeup.
            if(s.service_slot==64)s.registry->collect_completed(s.attempt->receipt);
        }
        {std::lock_guard<std::mutex> lock(s.mutex);s.initializing=false;}
        owner->notify();throw;
    }
}
void configured_recovery_connection::notify()noexcept{
    size_t index;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->needs_action=true;index=state_->service_slot;}
    if(index<64)configured_control_service::instance().wake(index,this);
}
void configured_recovery_connection::start(){
    try{construct_attempt();}
    catch(...){
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->initializing=false;}
        notify();throw;
    }
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->initializing=false;}
    notify();
}
void configured_recovery_connection::abandon_start(std::exception_ptr error)noexcept{
    std::shared_ptr<configured_attempt> attempt;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->closing=true;state_->desired=false;
        state_->close_result.remember(error,false);attempt=state_->attempt;
    }
    if(attempt&&!attempt->construction_finished){
        // Setup has unwound before entering start: no physical wrapper or
        // foreign factory exists, but the real child still owes checked close.
        if(!attempt->factory_entered){
            attempt->native_reservation.reset();attempt->lane_settled({0,false,false,false,false,true});
            attempt->wrapper_destroyed=true;attempt->construction_finished=true;
            attempt->registry->request_retirement(attempt->receipt);
            attempt->receipt.complete_adapter_cleanup(0);
        }
    }
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->initializing=false;}
    if(attempt)request_renewal(attempt);else notify();
}
void configured_recovery_connection::construct_attempt(){
    std::shared_ptr<configured_attempt> attempt;std::shared_ptr<lattice_db> child;
    {std::lock_guard<std::mutex> lock(state_->mutex);attempt=state_->attempt;child=state_->child;}
    try {
        const auto parent=state_->parent.lock();
        if(!parent||parent->is_closed())throw db_error("configured parent closed before physical construction");
        auto physical=std::unique_ptr<synchronizer>(new synchronizer(child,state_->config,attempt));
        const std::weak_ptr<configured_recovery_connection> weak=shared_from_this();
        const std::weak_ptr<configured_attempt> weak_attempt=attempt;
        physical->set_on_state_change([weak,weak_attempt](bool connected){if(auto owner=weak.lock())if(auto held=weak_attempt.lock())owner->state_event(held,connected);});
        physical->set_on_error([weak,weak_attempt](const std::string& error){if(auto owner=weak.lock())if(auto held=weak_attempt.lock())owner->error_event(held,error);});
        physical->set_on_progress([weak,weak_attempt](const synchronizer::sync_progress& progress){if(auto owner=weak.lock())if(auto held=weak_attempt.lock())owner->progress_event(held,progress);});
        bool dial=false;
        {std::lock_guard<std::mutex> lock(state_->mutex);attempt->physical=std::move(physical);dial=state_->desired&&!state_->closing;}
        if(dial){auto held=command();if(held.pointer)held.pointer->connect();}
        else request_renewal(attempt);
        attempt->construction_finished=true;
    }catch(...){
        {std::lock_guard<std::mutex> lock(state_->mutex);
            attempt->primary_error=std::current_exception();
            if(!attempt->physical){
                // The failed constructor (or local unpublished unique_ptr)
                // has already run the real base destructor before this catch.
                if(attempt->destructor_error)attempt->cleanup_error=*attempt->destructor_error;
                attempt->wrapper_destroyed=true;
            }
            state_->close_result.remember(attempt->primary_error,false);
            state_->close_result.remember(attempt->cleanup_error);
            attempt->construction_finished=true;
        }
        request_renewal(attempt);throw;
    }
}
configured_recovery_connection::borrow configured_recovery_connection::command()const{
    std::shared_ptr<configured_attempt> attempt;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        attempt=state_->attempt;
        if(state_->closing||!attempt||attempt->retirement_started||!attempt->physical)return {{},{},nullptr};
    }
    auto charge=attempt->custody->admit(configured_attempt_custody::kind::command);
    if(!charge)return {{},{},nullptr};
    std::lock_guard<std::mutex> lock(state_->mutex);
    if(state_->closing||state_->attempt!=attempt||attempt->retirement_started||!attempt->physical)return {{},{},nullptr};
    auto* pointer=attempt->physical.get();return {std::move(charge),attempt,pointer};
}
bool configured_recovery_connection::connected()const{auto held=command();return held.pointer&&held.pointer->is_connected();}
void configured_recovery_connection::sync_now(){auto held=command();if(held.pointer)held.pointer->sync_now();}
void configured_recovery_connection::trigger_upload(){
    auto held=command();if(!held.pointer||!held.pointer->is_connected())return;
    // The queued operation captures N, never a future lookup selecting N+1.
    auto attempt=held.attempt;auto* pointer=held.pointer;
    pointer->scheduler_->invoke([attempt,pointer]{if(pointer->is_connected())pointer->sync_now();});
}
void configured_recovery_connection::connect(){
    {std::lock_guard<std::mutex> lock(state_->mutex);
        if(state_->closing)throw db_error("configured owner is closed");
        if(state_->quarantined)throw db_error("configured owner cleanup remains quarantined");
        if(state_->desired)return;
        if(state_->intent_epoch==std::numeric_limits<uint64_t>::max())throw db_error("configured intent serial exhausted");
        state_->desired=true;++state_->intent_epoch;
    }
    notify();
}
void configured_recovery_connection::disconnect(){
    std::shared_ptr<configured_attempt> attempt;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->desired=false;state_->due={};state_->retries=0;state_->opened={};attempt=state_->attempt;}
    if(attempt)request_renewal(attempt);else notify();
}
void configured_recovery_connection::request_renewal(const std::shared_ptr<configured_attempt>& attempt)noexcept{
    bool first=false;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        if(state_->attempt!=attempt||state_->closed)return;
        if(!attempt->retirement_started){attempt->retirement_started=true;first=true;
            if(state_->opened!=clock::time_point{}&&clock::now()-state_->opened>=std::chrono::milliseconds(state_->config.stable_connection_ms))state_->retries=0;
            state_->opened={};
        }
    }
    if(first){
        attempt->custody->close();
        if(auto lifetime=attempt->lifetime())lifetime->end_protected_attempt();
        attempt->registry->request_retirement(attempt->receipt);
    }
    notify();
}
void configured_recovery_connection::state_event(const std::shared_ptr<configured_attempt>& attempt,bool connected){
    std::shared_ptr<const synchronizer::on_state_change_handler> callback;
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt||(connected&&attempt->retirement_started))return;
        if(connected)state_->opened=clock::now();callback=state_->state_handler;
    }
    if(callback&&*callback)(*callback)(connected);
}
void configured_recovery_connection::error_event(const std::shared_ptr<configured_attempt>& attempt,const std::string& error){
    std::shared_ptr<const synchronizer::on_error_handler> callback;
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt)return;callback=state_->error_handler;}
    if(callback&&*callback)(*callback)(error);
}
void configured_recovery_connection::progress_event(const std::shared_ptr<configured_attempt>& attempt,const synchronizer::sync_progress& progress){
    std::shared_ptr<const synchronizer::on_progress_handler> callback;
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt)return;state_->observed=progress;callback=state_->progress_handler;}
    if(callback&&*callback)(*callback)(progress);
}
void configured_recovery_connection::set_state(synchronizer::on_state_change_handler handler){
    auto replacement=std::make_shared<const synchronizer::on_state_change_handler>(std::move(handler));
    const auto installed=replacement;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->state_handler.swap(replacement);}
    if(!*installed)return;
    // Preserve the public connected-state replay. Publish the listener BEFORE
    // checking connection state, so an intervening real open either reaches
    // this listener or is observed below. Retain N through actual submission;
    // its lifetime scheduler separately counts the queued payload and execution.
    auto held=command();if(!held.pointer||!held.pointer->is_connected())return;
    const std::weak_ptr<configured_recovery_connection> weak=shared_from_this();
    auto attempt=held.attempt;auto* physical=held.pointer;
    // This capture envelope and the scheduler's own envelope are separately
    // charged. Reserve before either retained callable allocation/copy.
    auto capture_charge=attempt->custody->admit(configured_attempt_custody::kind::payload);
    if(!capture_charge)return;
    auto replay=retain_configured_payload(std::move(capture_charge),[weak,attempt,physical,installed]{
        if(!physical->is_connected())return;
        auto owner=weak.lock();if(!owner)return;
        {
            std::lock_guard<std::mutex> lock(owner->state_->mutex);
            if(owner->state_->attempt!=attempt||owner->state_->closing||
               attempt->retirement_started||owner->state_->state_handler!=installed)return;
        }
        // Already admitted callback; stop/close may now race and must wait for
        // this exact execution/capture rather than making a successor visible.
        (*installed)(true);
    });
    physical->scheduler_->invoke(std::move(replay));
}
void configured_recovery_connection::set_error(synchronizer::on_error_handler handler){
    auto replacement=std::make_shared<const synchronizer::on_error_handler>(std::move(handler));
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->error_handler.swap(replacement);}
}
void configured_recovery_connection::set_progress(synchronizer::on_progress_handler handler){
    auto replacement=std::make_shared<const synchronizer::on_progress_handler>(std::move(handler));
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->progress_handler.swap(replacement);}
}
synchronizer::sync_progress configured_recovery_connection::progress()const{
    auto held=command();if(held.pointer)return held.pointer->get_progress();
    std::lock_guard<std::mutex> lock(state_->mutex);return state_->observed;
}
void configured_recovery_connection::dispatch()noexcept{
    std::shared_ptr<scheduler> scheduled;bool cleanup=false;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        const bool timer_due=state_->due!=clock::time_point{}&&state_->due<=clock::now();
        if(state_->initializing||state_->closed||state_->quarantined||state_->control_queued||state_->cleanup_running||(!state_->needs_action&&!timer_due))return;
        state_->needs_action=false;
        if(state_->finalizer_queued){cleanup=true;state_->cleanup_running=true;}
        else if(!state_->scheduled){state_->finalizer_queued=true;state_->cleanup_running=true;cleanup=true;}
        else {state_->control_queued=true;scheduled=state_->scheduled;}
    }
    if(cleanup){configured_control_service::instance().cleanup(state_->service_slot,this);return;}
    struct payload {
        std::shared_ptr<configured_recovery_connection> owner;
        bool entered=false;
        explicit payload(std::shared_ptr<configured_recovery_connection> value):owner(std::move(value)){}
        ~payload(){owner->control_payload_destroyed(entered);}
    };
    bool handed_to_payload=false;
    try{
        auto work=std::make_shared<payload>(shared_from_this());
        handed_to_payload=true;
        if(!scheduled->can_invoke())throw db_error("configured control scheduler refused submission");
        scheduled->invoke([work]{work->entered=true;work->owner->advance();});
    }catch(...){
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.remember(std::current_exception());}
        if(!handed_to_payload)control_payload_destroyed(false);
    }
}
void configured_recovery_connection::control_payload_destroyed(bool entered)noexcept{
    bool wake=false;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        state_->control_queued=false;
        if(!entered){state_->close_result.remember(configured_error("configured control payload was rejected or dropped"));state_->quarantined=true;}
        wake=state_->needs_action||state_->finalizer_queued;
    }
    state_->settled.notify_all();
    if(wake)configured_control_service::instance().wake(state_->service_slot,this);
}
void configured_recovery_connection::advance()noexcept{
    try{
        std::shared_ptr<configured_attempt> attempt;
        {std::lock_guard<std::mutex> lock(state_->mutex);attempt=state_->attempt;}
        if(attempt){
            if(!attempt->construction_finished)return;
            const auto workers=attempt->custody->snapshot();
            if(workers.finished_workers){
                std::lock_guard<std::mutex> lock(state_->mutex);state_->worker_join_queued=true;state_->finalizer_queued=true;state_->needs_action=true;return;
            }
            if(!attempt->retirement_started){
                if(attempt->custody->snapshot().first_error)request_renewal(attempt);
                else return;
            }
            if(attempt->physical)attempt->physical->begin_configured_retirement();
            else if(attempt->native_reservation){
                attempt->native_reservation.reset();attempt->lane_settled({0,false,false,false,false,true});
                if(!attempt->factory_entered)attempt->receipt.complete_adapter_cleanup(0);
            }
            bool lane_complete=false;sync_retirement_result lane;
            {std::lock_guard<std::mutex> lock(attempt->facts_mutex);lane_complete=attempt->lane_complete;lane=attempt->lane_result;}
            if(!lane_complete)return;
            if(lane.first_error){
                state_->registry->complete_native_cleanup(attempt->receipt,lane.first_error);
                {std::lock_guard<std::mutex> lock(state_->mutex);state_->quarantined=true;state_->close_result.remember(configured_error("configured native lane cleanup failed"));}
                state_->settled.notify_all();return;
            }
            const auto lifetime=attempt->lifetime();
            if(lifetime&&lifetime->active_callbacks())return;
            const auto counts=attempt->custody->snapshot();
            if(counts.commands||counts.payloads||counts.workers)return;
            if(!attempt->wrapper_destroyed){
                // Fence ALL possible late queued work before the final zero
                // observation. Destructors/unregister cannot admit a payload.
                if(lifetime)lifetime->retire();
                attempt->custody->seal_payloads();
                if(lifetime&&lifetime->active_callbacks())return;
                if(attempt->custody->snapshot().payloads)return;
                {std::lock_guard<std::mutex> lock(state_->mutex);state_->finalizer_queued=true;state_->needs_action=true;}
                return;
            }
            bool unregistered=false;
            {std::lock_guard<std::mutex> lock(attempt->facts_mutex);unregistered=!attempt->route_registered||attempt->route_unregistered;}
            if(!unregistered)return;
            if(!attempt->native_asserted){
                const auto error=attempt->cleanup_error?-4:counts.first_error;
                state_->registry->complete_native_cleanup(attempt->receipt,error);attempt->native_asserted=true;
            }
            const auto status=state_->registry->snapshot(attempt->receipt);
            if(status.quarantined){
                std::lock_guard<std::mutex> lock(state_->mutex);state_->quarantined=true;state_->close_result.remember(configured_error("configured cleanup remains quarantined"));state_->settled.notify_all();return;
            }
            if(!status.adapter_complete||!status.native_complete)return;
            {std::lock_guard<std::mutex> lock(state_->mutex);state_->finalizer_queued=true;state_->needs_action=true;}
            return; // Actual registry/context collection runs off this scheduler.
        }
        bool closing=false,desired=false,permitted=false;clock::time_point due;uint64_t epoch=0;
        {std::lock_guard<std::mutex> lock(state_->mutex);closing=state_->closing;desired=state_->desired;permitted=state_->successors_permitted;due=state_->due;epoch=state_->intent_epoch;}
        if(closing){std::lock_guard<std::mutex> lock(state_->mutex);state_->finalizer_queued=true;state_->needs_action=true;return;}
        if(!desired)return;
        if(!permitted){
            std::shared_ptr<const synchronizer::on_error_handler> callback;
            {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->successor_refusal_reported)return;state_->successor_refusal_reported=true;callback=state_->error_handler;}
            if(callback&&*callback)(*callback)("Configured protected renewal is awaiting production-path qualification");
            return;
        }
        if(due==clock::time_point{}){
            uint64_t retry;
            {std::lock_guard<std::mutex> lock(state_->mutex);
                if(state_->closing||!state_->desired||state_->intent_epoch!=epoch||state_->due!=clock::time_point{})return;
                if(state_->config.max_reconnect_attempts!=0&&(state_->config.max_reconnect_attempts<0||state_->retries>=static_cast<uint64_t>(state_->config.max_reconnect_attempts)))return;
                retry=state_->retries;
            }
            const auto base=state_->config.base_delay_seconds,maximum=state_->config.max_delay_seconds;
            if(!std::isfinite(base)||!std::isfinite(maximum)||base<0||maximum<0)throw db_error("configured reconnect delay invalid");
            const auto exponent=static_cast<int>(std::min<uint64_t>(retry,std::numeric_limits<int>::max()));
            const double delay=base==0?0:std::min(std::ldexp(base,exponent),maximum);
            if(!std::isfinite(delay)||delay>std::chrono::duration<double>(clock::duration::max()/2).count())throw db_error("configured reconnect delay overflow");
            const auto elapsed=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(delay));
            const auto now=clock::now();if(now>clock::time_point::max()-elapsed)throw db_error("configured reconnect deadline overflow");
            due=now+elapsed;
            if(state_->backoff_probe&&*state_->backoff_probe)(*state_->backoff_probe)(epoch,retry);
            {std::lock_guard<std::mutex> lock(state_->mutex);
                if(state_->closing||!state_->desired||state_->intent_epoch!=epoch||state_->due!=clock::time_point{}||state_->retries!=retry)return;
                if(state_->retries<std::numeric_limits<uint64_t>::max())++state_->retries;
                state_->due=due;
            }
        }
        if(clock::now()<due){configured_control_service::instance().timer(state_->service_slot,this,due);return;}
        const auto parent=state_->parent.lock();if(!parent||parent->is_closed()){disconnect();return;}
        const auto receipt=state_->reservation->begin_attempt();
        std::shared_ptr<configured_attempt> replacement;
        try{
            replacement=std::make_shared<configured_attempt>(receipt,state_->registry,state_->factory,state_->typed);
            replacement->control=shared_from_this();const std::weak_ptr<configured_recovery_connection> weak=shared_from_this();
            replacement->custody->bind_wakeup([weak]{if(auto owner=weak.lock())owner->notify();});
        }catch(...){
            replacement.reset(); // Cancel only the still-empty native slot.
            state_->registry->request_retirement(receipt);receipt.complete_adapter_cleanup(0);
            state_->registry->complete_native_cleanup(receipt,0);state_->registry->collect_completed(receipt);throw;
        }
        bool canceled=false;
        {std::lock_guard<std::mutex> lock(state_->mutex);
            canceled=state_->closing||!state_->desired||state_->intent_epoch!=epoch;
            state_->attempt=replacement;state_->due={};
        }
        if(canceled){
            // A stop/new intent raced only empty native reservation work. No
            // foreign factory may be entered for that obsolete intent.
            replacement->native_reservation.reset();replacement->lane_settled({0,false,false,false,false,true});
            replacement->wrapper_destroyed=true;replacement->construction_finished=true;
            request_renewal(replacement);replacement->receipt.complete_adapter_cleanup(0);return;
        }
        construct_attempt();
    }catch(...){
        std::shared_ptr<configured_attempt> attempt;
        std::shared_ptr<const synchronizer::on_error_handler> callback;
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.remember(std::current_exception(),false);attempt=state_->attempt;callback=state_->error_handler;
            if(!attempt){state_->desired=false;state_->due={};}
        }
        if(attempt)request_renewal(attempt);
        if(callback&&*callback)try{(*callback)("Configured connection construction or retirement failed");}catch(...){}
    }
}
void configured_recovery_connection::finalize()noexcept{
    std::shared_ptr<configured_attempt> attempt;std::unique_ptr<synchronizer> physical;bool join_workers=false;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        if(state_->close_release_ready)return;
        if(state_->control_queued){state_->cleanup_running=false;return;}
        attempt=state_->attempt;join_workers=state_->worker_join_queued;state_->worker_join_queued=false;
        if(attempt&&!join_workers)physical=std::move(attempt->physical);
        state_->finalizer_queued=false;
    }
    if(join_workers&&attempt){
        const bool joined=attempt->custody->join_finished_workers();
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->cleanup_running=false;
            if(!joined){state_->quarantined=true;state_->close_result.remember(configured_error("configured ACK worker join failed"));}
        }
        if(!joined){request_renewal(attempt);attempt->registry->complete_native_cleanup(attempt->receipt,-5);state_->settled.notify_all();}
        notify();return;
    }
    if(attempt){
        if(attempt->wrapper_destroyed){
            const bool collected=state_->registry->collect_completed(attempt->receipt);
            bool pending=false;
            {std::lock_guard<std::mutex> lock(state_->mutex);state_->cleanup_running=false;if(collected)state_->attempt.reset();pending=state_->needs_action;}
            if(collected){attempt.reset();notify();}
            else if(pending)configured_control_service::instance().wake(state_->service_slot,this);
            return;
        }
        physical.reset();
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->cleanup_running=false;
            if(attempt->destructor_error)attempt->cleanup_error=*attempt->destructor_error;
            state_->close_result.remember(attempt->cleanup_error);attempt->wrapper_destroyed=true;
        }
        notify();return;
    }
    std::shared_ptr<lattice_db> child;std::shared_ptr<scheduler> scheduled;
    {std::lock_guard<std::mutex> lock(state_->mutex);if(!state_->closing)return;child=state_->child;scheduled=state_->scheduled;}
    lattice_close_result result;
    if(child)result=child->close_checked();
    bool joined=false;
    try{
        if(scheduled){if(scheduled->is_on_thread())throw db_error("configured scheduler self join refused");scheduled->shutdown();}
        joined=true;
    }catch(...){result.remember(std::current_exception());}
    const bool complete=result.cleanup_complete&&joined;
    std::optional<configured_retirement_registry::reservation> released_reservation;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        state_->child_closed=result.cleanup_complete;state_->scheduler_joined=joined;
        state_->close_result.merge(result);
        if(complete){
            child=std::move(state_->child);scheduled=std::move(state_->scheduled);
            if(state_->reservation){released_reservation.emplace(std::move(*state_->reservation));state_->reservation.reset();}
        }
    }
    child.reset();scheduled.reset();released_reservation.reset();
    // The service epilogue also owes its real fixed cleanup-slot release.
    // Keep dispatch fenced until that epilogue publishes completed close.
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_release_ready=complete;state_->cleanup_running=complete;state_->quarantined=!complete;}
    if(!complete)state_->settled.notify_all();
}
void configured_recovery_connection::cleanup_returned()noexcept{
    {std::lock_guard<std::mutex> lock(state_->mutex);if(!state_->close_release_ready)return;}
    if(!configured_control_service::instance().release(state_->service_slot,this))return;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_release_ready=false;state_->closed=true;state_->cleanup_running=false;}
    state_->settled.notify_all();
}
lattice_close_result configured_recovery_connection::close(clock::time_point deadline)noexcept{
    lattice_close_result result;
    std::optional<borrow> held;
    // Try to retain the exact existing attempt for drain. Even admission
    // failure must not skip the logical close fence below.
    try{held.emplace(command());}catch(...){result.remember(std::current_exception(),false);}
    std::shared_ptr<configured_attempt> attempt;std::shared_ptr<scheduler> scheduled;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        state_->closing=true;state_->desired=false;attempt=state_->attempt;scheduled=state_->scheduled;
    }
    bool reentrant=false;
    try{
        reentrant=configured_control_service::on_cleanup_thread()||(scheduled&&scheduled->is_on_thread());
        if(attempt)if(auto lifetime=attempt->lifetime())if(lifetime->executing_here())reentrant=true;
        if(held&&held->pointer){
            const auto drained=held->pointer->drain_checked(deadline);result.sync=drained.state;result.remember(drained.error,false);
        }
    }catch(...){result.remember(std::current_exception(),false);}
    held.reset(); // Actual drain captures/borrow settle before retirement.
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.merge(result);}
    if(attempt)request_renewal(attempt);else notify();
    std::unique_lock<std::mutex> lock(state_->mutex);
    if(!reentrant)state_->settled.wait_until(lock,deadline,[&]{return state_->closed||state_->quarantined;});
    result=state_->close_result;
    result.cleanup_complete=state_->closed&&state_->child_closed&&state_->scheduler_joined;
    if(!result.cleanup_complete&&result.sync!=sync_drain_state::failed)
        result.sync=reentrant?sync_drain_state::reentrant_pending:sync_drain_state::deadline_pending;
    return result;
}
}
#endif
