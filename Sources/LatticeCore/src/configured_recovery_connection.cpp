#include "configured_recovery_connection.hpp"
#ifndef __EMSCRIPTEN__
#include "configured_platform.hpp"
#include "ordinary_managed_wss_owner.hpp"
#include "recovery_producer_continuity.hpp"
#include "recovery_export_adapter.hpp"
#include <lattice/lattice.hpp>
#include <array>
#include <cmath>
#include <limits>
#include <unistd.h>
#include <sys/file.h>
#include <cerrno>

namespace lattice::detail {
namespace {
using clock=std::chrono::steady_clock;
const auto configured_service_creator=static_cast<std::uint64_t>(::getpid());
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
thread_local std::function<void()> configured_recovery_test_hooks::before_listener_publication;
thread_local std::function<void()> configured_recovery_test_hooks::before_listener_retirement_preparation;
thread_local std::function<void()> configured_recovery_test_hooks::after_transport_creation;
thread_local std::shared_ptr<configured_recovery_observer> configured_recovery_test_hooks::observation;
thread_local std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> configured_recovery_test_hooks::before_backoff_publication;

// Passive fixed-slot native custody. No controller/application/function owner
// is captured here; quarantine retains facts and policy agreement only. Record
// construction/all argument allocation completes before the actual open.
class ordinary_managed_lock_record final {
    friend class configured_control_service;
    friend class ordinary_managed_lock_custody;
    const std::uint64_t creator_=static_cast<std::uint64_t>(::getpid());
    const std::shared_ptr<const physical_store_identity> main_;
    const std::string lock_path_;
    const std::shared_ptr<const sync_policy_reservation> policy_;
    const clock::time_point deadline_;
    const std::shared_ptr<ordinary_managed_owner_release> completion_;
    std::size_t slot_=64;
    std::uint64_t serial_=0;
    std::atomic<int> fd_{-1};
    std::atomic<bool> cleanup_requested_{false},complete_{false},admission_issued_{false};
    bool observed_=false;std::uint64_t lock_device_=0,lock_inode_=0;
    int open_result_=-1,open_errno_=0;
    bool unlock_attempted_=false,close_attempted_=false;
    int unlock_result_=0,unlock_errno_=0,close_result_=0,close_errno_=0,first_error_=0;
    ordinary_managed_lock_record(std::shared_ptr<const physical_store_identity> main,std::string path,
        std::shared_ptr<const sync_policy_reservation> policy,clock::time_point end,
        const std::shared_ptr<ordinary_managed_owner_release>& completion)
        :main_(std::move(main)),lock_path_(std::move(path)),policy_(std::move(policy)),deadline_(end),completion_(completion){}
    bool tails_complete()const noexcept {
        return creator_==static_cast<std::uint64_t>(::getpid())&&completion_->start_destroyed_.load(std::memory_order_acquire)&&
            (!admission_issued_.load(std::memory_order_acquire)||completion_->admission_destroyed_.load(std::memory_order_acquire));
    }
    void finish_native()noexcept {
        if(creator_!=static_cast<std::uint64_t>(::getpid()))return;
        const auto fd=fd_.exchange(-1,std::memory_order_relaxed);
        // Exactly one attempt of each operation, including EINTR. The integer
        // may have been consumed on close failure; it is never retried.
        if(fd>=0){
            unlock_attempted_=true;unlock_result_=::flock(fd,LOCK_UN);
            unlock_errno_=unlock_result_!=0?(errno?errno:EIO):0;
            close_attempted_=true;close_result_=::close(fd);
            close_errno_=close_result_!=0?(errno?errno:EIO):0;
            first_error_=unlock_errno_?unlock_errno_:close_errno_;
        }
        complete_.store(true,std::memory_order_release);
        completion_->finish_native(first_error_);
    }
public:
    ~ordinary_managed_lock_record()=default; // Never closes/retries a descriptor.
};

// Fixed records, one event/timer dispatcher and one bounded cleanup lane. The
// dispatcher never performs SQL, joins, or waits for a platform future. The
// cleanup lane only receives bundles already fenced against new work.
class configured_control_service {
    struct slot {
        std::shared_ptr<configured_recovery_connection> owner;
        std::shared_ptr<ordinary_managed_lock_record> native_lock;
        std::uint64_t serial=0;
        bool native_dirty=false;
        bool dirty=false,cleanup=false,busy=false,dispatching=false,release_pending=false;
        clock::time_point due=clock::time_point::max(),admission_due=clock::time_point::max();
    };
    std::array<slot,64> slots_;
    std::mutex mutex_;
    std::condition_variable changed_,cleanup_ready_;
    std::thread dispatcher_,collector_;
    bool stopping_=false;
    const pid_t creator_pid_=::getpid();
    native_retirement_storage<std::shared_ptr<configured_recovery_connection>> collector_owner_;
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
        for(;;){std::shared_ptr<configured_recovery_connection> owner;size_t index=slots_.size();bool admission_expired=false;
            {std::unique_lock<std::mutex> lock(mutex_);
                for(;;){
                    if(stopping_)return;
                    const auto now=clock::now();auto due=clock::time_point::max();
                    for(size_t i=0;i<slots_.size();++i){auto& s=slots_[i];if(!s.owner)continue;
                        if(s.dirty||s.due<=now||s.admission_due<=now){owner=s.owner;index=i;s.dispatching=true;s.dirty=false;s.due=clock::time_point::max();if(s.admission_due<=now){s.admission_due=clock::time_point::max();admission_expired=true;}break;}
                        due=std::min(due,std::min(s.due,s.admission_due));
                    }
                    if(owner)break;
                    if(due==clock::time_point::max())changed_.wait(lock);else changed_.wait_until(lock,due);
                }
            }
            if(admission_expired)owner->notify(); // Advisory work only, off the service leaf.
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
        for(;;){
            auto& held=collector_owner_;
            if(!held.in_creator_process())return;
            auto& owner=held.get<0>();size_t index=slots_.size();
            std::shared_ptr<ordinary_managed_lock_record> native;
            {std::unique_lock<std::mutex> lock(mutex_);
                const auto native_work=[](const slot& s){return s.native_lock&&!s.busy&&
                    ((s.native_lock->cleanup_requested_.load(std::memory_order_acquire)&&!s.native_lock->complete_.load(std::memory_order_acquire))||
                     (s.native_dirty&&!s.owner));};
                cleanup_ready_.wait(lock,[&]{for(const auto& s:slots_)if(native_work(s)||(s.owner&&s.cleanup&&!s.busy))return true;return stopping_;});
                if(stopping_)return;
                for(size_t i=0;i<slots_.size();++i)if(native_work(slots_[i])){
                    auto& s=slots_[i];native=s.native_lock;s.native_dirty=false;s.busy=true;index=i;break;
                }
                if(!native)for(size_t i=0;i<slots_.size();++i)if(slots_[i].owner&&slots_[i].cleanup&&!slots_[i].busy){
                    auto& s=slots_[i];owner=s.owner;s.cleanup=false;s.busy=true;index=i;break;
                }
            }
            if(native){
                cleanup_thread_=true;
                if(native->cleanup_requested_.load(std::memory_order_acquire)&&!native->complete_.load(std::memory_order_acquire))native->finish_native();
                std::shared_ptr<ordinary_managed_lock_record> displaced;
                {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[index];s.busy=false;
                    if(s.native_lock==native&&!s.owner&&native->complete_.load(std::memory_order_acquire)&&
                       !native->first_error_&&native->tails_complete())displaced=std::move(s.native_lock);
                }
                displaced.reset();native.reset();cleanup_thread_=false;
                cleanup_ready_.notify_one();continue;
            }
            cleanup_thread_=true;owner->finalize();
            if(creator_pid_!=::getpid())return;
            {std::lock_guard<std::mutex> lock(mutex_);slots_[index].busy=false;}
            owner->cleanup_returned();
            if(creator_pid_!=::getpid())return;
            cleanup_ready_.notify_one();
            if(!held.release<0>())return;
            cleanup_thread_=false;
        }
    }
public:
    static configured_control_service& instance(){static auto* service=new configured_control_service;return *service;}
    static bool on_cleanup_thread()noexcept{return cleanup_thread_;}
    size_t retain(std::shared_ptr<configured_recovery_connection> owner){
        std::lock_guard<std::mutex> lock(mutex_);
        for(size_t i=0;i<slots_.size();++i)if(!slots_[i].owner&&!slots_[i].native_lock&&!slots_[i].busy){slots_[i].owner=std::move(owner);return i;}
        throw db_error("configured control capacity exhausted before child allocation");
    }
    ordinary_managed_lock_custody reserve_native(std::shared_ptr<const physical_store_identity> main,std::string path,
        std::shared_ptr<const sync_policy_reservation> policy,clock::time_point end,
        const std::shared_ptr<ordinary_managed_owner_release>& completion){
        if(!main||path.empty()||!policy||!completion||clock::now()>=end)
            throw db_error("managed lock reservation lacks actual current physical owner");
        auto record=std::shared_ptr<ordinary_managed_lock_record>(new ordinary_managed_lock_record(std::move(main),std::move(path),std::move(policy),end,completion),
            [](ordinary_managed_lock_record* p){if(p->creator_==static_cast<std::uint64_t>(::getpid()))delete p;});
        {std::lock_guard<std::mutex> lock(mutex_);
            for(const auto& s:slots_)if(s.native_lock&&
                (*s.native_lock->main_==*record->main_||s.native_lock->lock_path_==record->lock_path_))
                throw db_error("managed physical lock custody already retained or quarantined");
            for(size_t i=0;i<slots_.size();++i){auto& s=slots_[i];
                if(s.owner||s.native_lock||s.busy)continue;
                if(s.serial==std::numeric_limits<std::uint64_t>::max())continue;
                record->slot_=i;record->serial_=++s.serial;
                completion->bind_native(i,record->serial_);s.native_lock=record;
                return ordinary_managed_lock_custody(std::move(record));
            }
        }
        throw db_error("configured control capacity exhausted before lock open");
    }
    void observe_native(const std::shared_ptr<ordinary_managed_lock_record>& record,std::uint64_t device,std::uint64_t inode){
        std::lock_guard<std::mutex> lock(mutex_);
        if(record->slot_>=slots_.size()||slots_[record->slot_].native_lock!=record||slots_[record->slot_].serial!=record->serial_)
            throw db_error("managed lock observation lost its native slot");
        record->observed_=true;record->lock_device_=device;record->lock_inode_=inode;
        for(const auto& s:slots_)if(s.native_lock&&s.native_lock!=record&&s.native_lock->observed_&&
            s.native_lock->lock_device_==device&&s.native_lock->lock_inode_==inode)
            throw db_error("managed observed lock identity already retained or quarantined");
    }
    size_t transfer_native(const ordinary_managed_lock_custody& custody,std::shared_ptr<configured_recovery_connection> owner){
        const auto& record=custody.record_;
        std::lock_guard<std::mutex> lock(mutex_);
        if(!record||record->slot_>=slots_.size()||slots_[record->slot_].native_lock!=record||
           slots_[record->slot_].serial!=record->serial_||slots_[record->slot_].owner||
           record->cleanup_requested_.load(std::memory_order_acquire)||clock::now()>=record->deadline_)
            throw db_error("managed controller lost actual pre-open lock custody");
        slots_[record->slot_].owner=std::move(owner);return record->slot_;
    }
    void wake_native(size_t i,std::uint64_t serial)noexcept {
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||!slots_[i].native_lock||slots_[i].serial!=serial)return;
            slots_[i].native_dirty=true;if(slots_[i].owner)slots_[i].dirty=true;
        }
        changed_.notify_one();cleanup_ready_.notify_one();
    }
    void wake(size_t i,const configured_recovery_connection* owner)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].dirty=true;}
        changed_.notify_one();
    }
    void timer(size_t i,const configured_recovery_connection* owner,clock::time_point due)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].due=due;}
        changed_.notify_one();
    }
    void admission_deadline(size_t i,const configured_recovery_connection* owner,clock::time_point due)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].admission_due=due;}
        changed_.notify_one();
    }
    void cleanup(size_t i,const configured_recovery_connection* owner)noexcept{
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return;slots_[i].cleanup=true;}
        cleanup_ready_.notify_one();
    }
    struct diagnostic_sample {uint32_t flags=0;size_t collector_busy=0,dispatcher_busy=0;bool available=false;};
    diagnostic_sample sample(size_t i,const configured_recovery_connection* owner)noexcept{
        diagnostic_sample out;
        std::unique_lock<std::mutex> lock(mutex_,std::try_to_lock);if(!lock.owns_lock())return out;
        out.available=true;
        for(const auto& s:slots_){out.collector_busy+=s.busy;out.dispatcher_busy+=s.dispatching;}
        if(i<slots_.size()&&slots_[i].owner.get()==owner){
            const auto& s=slots_[i];out.flags=1u|(uint32_t(s.dirty)<<1)|(uint32_t(s.cleanup)<<2)|
                (uint32_t(s.busy)<<3)|(uint32_t(s.dispatching)<<4)|(uint32_t(s.release_pending)<<5);
        }
        return out; // Plain copies only; no owner/callback destruction here.
    }
    bool release(size_t i,const configured_recovery_connection* owner)noexcept{
        std::shared_ptr<configured_recovery_connection> displaced;
        std::shared_ptr<ordinary_managed_lock_record> retired_native;
        {std::lock_guard<std::mutex> lock(mutex_);if(i>=slots_.size()||slots_[i].owner.get()!=owner)return false;
            auto& s=slots_[i];
            if(s.native_lock&&(!s.native_lock->complete_.load(std::memory_order_acquire)||s.native_lock->first_error_))return false;
            if(s.dispatching){s.release_pending=true;return false;}
            retired_native=std::move(s.native_lock);s.native_dirty=false;
            displaced=std::move(s.owner);s.dirty=false;s.cleanup=false;s.release_pending=false;s.due=clock::time_point::max();s.admission_due=clock::time_point::max();
        }
        displaced.reset();retired_native.reset();return true;
    }
};
thread_local bool configured_control_service::cleanup_thread_=false;
void configured_recovery_connection::wake_managed_owner_release(std::size_t slot,
    const configured_recovery_connection* owner)noexcept {
    // Native comparison/advisory work only. No borrowed controller access or
    // strong promotion can move final controller destruction onto this caller.
    configured_control_service::instance().wake(slot,owner);
}

ordinary_managed_lock_custody::ordinary_managed_lock_custody(std::shared_ptr<ordinary_managed_lock_record> record)noexcept
    :record_(std::move(record)){}
ordinary_managed_lock_custody::ordinary_managed_lock_custody(ordinary_managed_lock_custody&& other)noexcept
    :record_(std::move(other.record_)){}
ordinary_managed_lock_custody::~ordinary_managed_lock_custody()noexcept {
    if(!record_||record_->creator_!=static_cast<std::uint64_t>(::getpid()))return;
    record_->cleanup_requested_.store(true,std::memory_order_release);
    configured_recovery_connection::wake_managed_lock(record_->slot_,record_->serial_);
}
void ordinary_managed_lock_custody::adopt(int fd,int error)noexcept {
    // First operation after open: no allocation, mutex, callback or syscall.
    record_->fd_.store(fd,std::memory_order_relaxed);
    record_->open_result_=fd;record_->open_errno_=fd<0?error:0;
}
void ordinary_managed_lock_custody::observe(std::uint64_t device,std::uint64_t inode){
    if(record_->creator_!=static_cast<std::uint64_t>(::getpid()))throw db_error("managed lock lease used by inherited process");
    configured_control_service::instance().observe_native(record_,device,inode);
}
void ordinary_managed_lock_custody::issued_admission()const noexcept {
    record_->admission_issued_.store(true,std::memory_order_release);
}
ordinary_managed_lock_custody configured_recovery_connection::reserve_managed_lock(
    std::shared_ptr<const physical_store_identity> main,std::string path,
    std::shared_ptr<const sync_policy_reservation> policy,clock::time_point end,
    const std::shared_ptr<ordinary_managed_owner_release>& completion){
    if(configured_service_creator!=static_cast<std::uint64_t>(::getpid()))throw db_error("inherited managed lock reservation refused");
    return configured_control_service::instance().reserve_native(std::move(main),std::move(path),std::move(policy),end,completion);
}
void configured_recovery_connection::wake_managed_lock(std::size_t slot,std::uint64_t serial)noexcept {
    if(configured_service_creator!=static_cast<std::uint64_t>(::getpid()))return;
    configured_control_service::instance().wake_native(slot,serial);
}

// Native destruction observation only, never source/policy/receipt authority.
// Only the actual attempt's last data-member destructor can publish release.
// A shared_ptr count/weak.expired() would be too early: count zero can precede
// arbitrarily long admission/factory/callback member destruction.
class configured_managed_capture_release final {
    friend struct configured_attempt;
    friend class configured_recovery_connection;
    const std::uint64_t creator_=static_cast<std::uint64_t>(::getpid());
    std::atomic<bool> fields_destroyed_{false};
    const std::weak_ptr<configured_recovery_connection> owner_;
    bool fields_destroyed()const noexcept {
        return creator_==static_cast<std::uint64_t>(::getpid())&&fields_destroyed_.load(std::memory_order_acquire);
    }
    void finish()noexcept {
        if(creator_!=static_cast<std::uint64_t>(::getpid()))return;
        fields_destroyed_.store(true,std::memory_order_release);
        if(auto owner=owner_.lock())owner->notify(); // Advisory work only, off all attempt leaves.
    }
public:
    explicit configured_managed_capture_release(std::weak_ptr<configured_recovery_connection> owner)noexcept
        :owner_(std::move(owner)){}
};
configured_attempt::managed_capture_tail::~managed_capture_tail()noexcept {
    // All owning attempt fields have already died. This record itself owns
    // only native scalar state and a weak controller, never an admission.
    if(completion)completion->finish();
}

struct configured_recovery_connection::state {
    mutable std::mutex mutex;
    std::condition_variable settled;
    const std::weak_ptr<lattice_db> parent;
    const sync_config config;
    std::shared_ptr<configured_managed_handlers> managed_handlers;
    const std::shared_ptr<network_factory> factory;
    configured_platform_factory* const typed;
    const std::shared_ptr<configured_retirement_registry> registry=configured_retirement_registry::instance();
    std::optional<configured_retirement_registry::reservation> reservation;
    std::shared_ptr<lattice_db> child;
    std::shared_ptr<const ordinary_managed_wss_admission> managed_wss;
    std::shared_ptr<const ordinary_managed_owner_release> managed_owner_release;
    std::shared_ptr<scheduler> scheduled;
    std::shared_ptr<configured_attempt> attempt;
    std::shared_ptr<configured_managed_capture_release> retired_managed_capture;
    size_t service_slot=64;
    bool initializing=true,desired=true,closing=false,closed=false,quarantined=false;
    bool needs_action=false,control_queued=false,finalizer_queued=false,worker_join_queued=false,cleanup_running=false;
    bool child_closed=false,scheduler_joined=false,close_release_ready=false;
    const bool successors_permitted=qualifying_configured_successors;
    const std::shared_ptr<configured_recovery_observer> observer=configured_recovery_test_hooks::observation;
    std::atomic<bool> first_close_observed{false}; // Diagnostic invocation fence only.
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
    std::shared_ptr<network_factory> f,configured_platform_factory* typed,
    std::shared_ptr<configured_managed_capture_release> completion)
    :managed_capture_(std::move(completion)),receipt(std::move(r)),registry(std::move(registry_value)),custody(registry->attempt_custody(receipt)),factory(std::move(f)),typed_factory(typed){
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
void configured_attempt::transport_created()noexcept{if(observer)if(auto owner=control.lock())owner->observe(configured_recovery_stage::transport_created,shared_from_this());}
void configured_attempt::install_lifetime(std::shared_ptr<sync_callback_lifetime> value){
    std::lock_guard<std::mutex> lock(facts_mutex);
    if(lifetime_&&lifetime_!=value)throw db_error("configured attempt lifetime already installed");
    if(!lifetime_)lifetime_=std::move(value);
}
bool configured_attempt::try_lifetime(std::shared_ptr<sync_callback_lifetime>& out)const noexcept{
    std::unique_lock<std::mutex> lock(facts_mutex,std::try_to_lock);if(!lock.owns_lock())return false;
    out=lifetime_;return true;
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
void configured_attempt::bind_managed(std::shared_ptr<sync_transport> transport,
    std::shared_ptr<sync_callback_lifetime> life,const std::shared_ptr<lattice_db>& child,const sync_config& config){
    if(!managed_wss||clock::now()>=managed_deadline||!managed_wss->matches(child,config)||!native_reservation||
       !registry->retain_transport(receipt,transport))throw db_error("managed configured attempt lost actual admission");
    install_lifetime(life);
    const std::weak_ptr<configured_attempt> weak=shared_from_this();
    native_reservation->bind(std::move(transport),std::move(life),[weak](sync_retirement_result result){if(auto held=weak.lock())held->lane_settled(result);});
    // No continuous route was registered. The actual ordinary receiver is
    // destroyed with the physical wrapper; native/adapter/lifetime zero and
    // actual ACK worker joins still gate registry collection unchanged.
}
configured_recovery_connection::configured_recovery_connection(std::weak_ptr<lattice_db> parent,sync_config config,
    std::shared_ptr<network_factory> factory,configured_platform_factory* typed)
    :state_(std::make_unique<state>(std::move(parent),std::move(config),std::move(factory),typed)){}
configured_recovery_connection::~configured_recovery_connection()=default;
void configured_managed_handlers::wake_retirement()noexcept {
    if(!owns_creator_process())return;
    std::weak_ptr<configured_recovery_connection> weak;
    {std::lock_guard<std::mutex> lock(publication_mutex_);weak=retirement_owner_;}
    if(auto owner=weak.lock())owner->notify(); // Work hint only; no completion from promotion.
}
void configured_recovery_connection::observe(configured_recovery_stage stage,const std::shared_ptr<configured_attempt>& attempt,
    std::optional<configured_retirement_snapshot> before_collection)noexcept {
    if(!state_->observer)return;
    configured_recovery_observation fact;fact.stage=stage;fact.owner=reinterpret_cast<std::uintptr_t>(this);
    {std::lock_guard<std::mutex> lock(state_->mutex);fact.child_closed=state_->child_closed;fact.scheduler_joined=state_->scheduler_joined;}
    if(attempt){
        fact.attempt=reinterpret_cast<std::uintptr_t>(attempt.get());
        fact.interval=attempt->registry->comparison_identity(attempt->receipt);
        const auto counts=attempt->custody->snapshot();fact.commands=counts.commands;fact.payloads=counts.payloads;fact.workers=counts.workers;
        const auto status=before_collection?*before_collection:attempt->registry->snapshot(attempt->receipt);
        fact.adapter_complete=status.adapter_complete;fact.native_complete=status.native_complete;fact.quarantined=status.quarantined;
        fact.first_error=status.first_error;fact.receipt_invalid=!attempt->receipt.valid();fact.wrapper_destroyed=attempt->wrapper_destroyed;
        {std::lock_guard<std::mutex> lock(attempt->facts_mutex);
            fact.lane_complete=attempt->lane_complete;fact.disconnect_returned=attempt->lane_result.disconnect_returned;
            fact.pacer_present=attempt->lane_result.pacer_present;fact.pacer_joined=attempt->lane_result.pacer_joined;
            fact.callbacks_settled=attempt->lane_result.callbacks_settled;
            fact.route_unregistered=!attempt->route_registered||attempt->route_unregistered;
        }
    }
    state_->observer->record(fact); // Passive, fixed-capacity, off every leaf.
}
std::shared_ptr<configured_recovery_connection> configured_recovery_connection::create(
    const std::shared_ptr<lattice_db>& parent,const sync_config& config,const std::shared_ptr<network_factory>& factory,
    configured_platform_factory* typed,const std::function<std::shared_ptr<lattice_db>()>& make_child){
    return create_impl(parent,config,factory,typed,make_child,{},{});
}
std::shared_ptr<configured_recovery_connection> configured_recovery_connection::create_impl(
    const std::shared_ptr<lattice_db>& parent,const sync_config& config,const std::shared_ptr<network_factory>& factory,
    configured_platform_factory* typed,const std::function<std::shared_ptr<lattice_db>()>& make_child,
    std::shared_ptr<const ordinary_managed_wss_admission> managed,std::shared_ptr<configured_managed_handlers> handlers){
    if(!parent||parent->is_closed()||!factory||!typed)throw db_error("configured renewal requires actual retained parent and typed factory");
    auto owner=std::shared_ptr<configured_recovery_connection>(new configured_recovery_connection(parent,config,factory,typed));
    auto& s=*owner->state_;
    s.managed_wss=std::move(managed);s.managed_handlers=std::move(handlers);
    if(s.managed_handlers)s.managed_handlers->bind_retirement_owner(owner);
    if(s.managed_wss)s.managed_owner_release=s.managed_wss->released_.completion;
    try {
        const auto receipt=s.reservation->begin_attempt();
        try{
            auto captures=s.managed_wss?std::make_shared<configured_managed_capture_release>(owner):nullptr;
            s.attempt=std::make_shared<configured_attempt>(receipt,s.registry,factory,typed,std::move(captures));
        }
        catch(...){
            // Neither child nor foreign factory has been entered. Failed
            // empty native reservation/allocation is a proved empty attempt.
            s.registry->request_retirement(receipt);receipt.complete_adapter_cleanup(0);
            s.registry->complete_native_cleanup(receipt,0);s.registry->collect_completed(receipt);throw;
        }
        s.attempt->control=owner;s.attempt->managed_wss=s.managed_wss;
        if(s.managed_wss)s.attempt->managed_deadline=s.managed_wss->initial_deadline();
        s.attempt->observer=s.observer;
        const std::weak_ptr<configured_recovery_connection> weak=owner;
        s.attempt->custody->bind_wakeup([weak]{if(auto held=weak.lock())held->notify();});
        if(configured_recovery_test_hooks::before_service_registration)configured_recovery_test_hooks::before_service_registration();
        s.service_slot=s.managed_wss?
            configured_control_service::instance().transfer_native(s.managed_wss->lock_,owner):
            configured_control_service::instance().retain(owner);
        if(s.managed_wss)s.managed_wss->released_.completion->bind_wakeup(s.service_slot,owner.get());
        if(s.managed_wss)configured_control_service::instance().admission_deadline(s.service_slot,owner.get(),s.managed_wss->expires());
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
        if(attempt->managed_wss)attempt->managed_wss->validate(parent,child,state_->config,attempt->managed_deadline);
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
    if(attempt->managed_wss&&!attempt->managed_wss->current())return {{},{},nullptr};
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
void configured_recovery_connection::request_close()noexcept{
    std::shared_ptr<configured_attempt> attempt;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->closing=true;state_->desired=false;attempt=state_->attempt;}
    if(attempt)request_renewal(attempt);else notify();
}
bool configured_recovery_connection::cleanup_complete()const noexcept{
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->closed&&state_->child_closed&&state_->scheduler_joined;
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
    if(state_->managed_handlers)callback=state_->managed_handlers->state.load(std::memory_order_acquire);
    if(connected&&attempt->managed_wss&&!attempt->managed_wss->current()){request_close();return;}
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt||(connected&&attempt->retirement_started))return;
        if(connected)state_->opened=clock::now();if(!state_->managed_handlers)callback=state_->state_handler;
    }
    if(callback&&*callback)(*callback)(connected);
}
void configured_recovery_connection::error_event(const std::shared_ptr<configured_attempt>& attempt,const std::string& error){
    std::shared_ptr<const synchronizer::on_error_handler> callback;
    if(state_->managed_handlers)callback=state_->managed_handlers->error.load(std::memory_order_acquire);
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt)return;if(!state_->managed_handlers)callback=state_->error_handler;}
    observe(configured_recovery_stage::transport_error,attempt);
    if(callback&&*callback)(*callback)(error);
}
void configured_recovery_connection::progress_event(const std::shared_ptr<configured_attempt>& attempt,const synchronizer::sync_progress& progress){
    std::shared_ptr<const synchronizer::on_progress_handler> callback;
    if(state_->managed_handlers)callback=state_->managed_handlers->progress.load(std::memory_order_acquire);
    {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->attempt!=attempt)return;state_->observed=progress;if(!state_->managed_handlers)callback=state_->progress_handler;}
    if(callback&&*callback)(*callback)(progress);
}
void configured_recovery_connection::set_state(synchronizer::on_state_change_handler handler){
    if(creator_pid_!=::getpid())return;
    native_retirement_storage<synchronizer::on_state_change_handler,std::shared_ptr<const synchronizer::on_state_change_handler>,std::shared_ptr<const synchronizer::on_state_change_handler>> retained;
    retained.get<0>().swap(handler);
    auto& replacement=retained.get<1>();replacement=std::make_shared<const synchronizer::on_state_change_handler>(std::move(retained.get<0>()));
    auto& installed=retained.get<2>();installed=replacement;
    if(state_->managed_handlers){
        const bool published=state_->managed_handlers->publish_state(installed);
        if(creator_pid_!=::getpid())return;
        if(!published)throw db_error("application listener registration is retired");
    }
    else {std::lock_guard<std::mutex> lock(state_->mutex);state_->state_handler.swap(replacement);}
    if(!retained.release<1>())return;
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
        if(attempt->managed_wss&&!attempt->managed_wss->current())return;
        if(!physical->is_connected())return;
        auto owner=weak.lock();if(!owner)return;
        const auto current=owner->state_->managed_handlers?owner->state_->managed_handlers->state.load(std::memory_order_acquire):nullptr;
        {
            std::lock_guard<std::mutex> lock(owner->state_->mutex);
            if(owner->state_->attempt!=attempt||owner->state_->closing||
               attempt->retirement_started||(owner->state_->managed_handlers?current!=installed:owner->state_->state_handler!=installed))return;
        }
        // Already admitted callback; stop/close may now race and must wait for
        // this exact execution/capture rather than making a successor visible.
        (*installed)(true);
    });
    physical->scheduler_->invoke(std::move(replay));
}
void configured_recovery_connection::set_error(synchronizer::on_error_handler handler){
    if(creator_pid_!=::getpid())return;
    native_retirement_storage<synchronizer::on_error_handler,std::shared_ptr<const synchronizer::on_error_handler>,std::shared_ptr<const synchronizer::on_error_handler>> retained;
    retained.get<0>().swap(handler);
    auto& replacement=retained.get<1>();replacement=std::make_shared<const synchronizer::on_error_handler>(std::move(retained.get<0>()));
    if(state_->managed_handlers){
        const bool published=state_->managed_handlers->publish_error(std::move(replacement));
        if(creator_pid_!=::getpid())return;
        if(!published)throw db_error("application listener registration is retired");
    }
    else {std::lock_guard<std::mutex> lock(state_->mutex);state_->error_handler.swap(replacement);}
    (void)retained.release<1>();
}
void configured_recovery_connection::set_progress(synchronizer::on_progress_handler handler){
    if(creator_pid_!=::getpid())return;
    native_retirement_storage<synchronizer::on_progress_handler,std::shared_ptr<const synchronizer::on_progress_handler>,std::shared_ptr<const synchronizer::on_progress_handler>> retained;
    retained.get<0>().swap(handler);
    auto& replacement=retained.get<1>();replacement=std::make_shared<const synchronizer::on_progress_handler>(std::move(retained.get<0>()));
    if(state_->managed_handlers){
        const bool published=state_->managed_handlers->publish_progress(std::move(replacement));
        if(creator_pid_!=::getpid())return;
        if(!published)throw db_error("application listener registration is retired");
    }
    else {std::lock_guard<std::mutex> lock(state_->mutex);state_->progress_handler.swap(replacement);}
    (void)retained.release<1>();
}
synchronizer::sync_progress configured_recovery_connection::progress()const{
    auto held=command();if(held.pointer)return held.pointer->get_progress();
    std::lock_guard<std::mutex> lock(state_->mutex);return state_->observed;
}
void configured_recovery_connection::dispatch()noexcept{
    std::shared_ptr<const ordinary_managed_wss_admission> managed;
    std::shared_ptr<scheduler> scheduled;bool cleanup=false;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        const bool timer_due=state_->due!=clock::time_point{}&&state_->due<=clock::now();
        const bool managed_release_due=state_->closing&&state_->managed_owner_release&&state_->managed_owner_release->fields_destroyed()&&state_->managed_owner_release->native_lock_complete();
        if(state_->initializing||state_->closed||state_->quarantined||state_->control_queued||state_->cleanup_running||(!state_->needs_action&&!timer_due&&!managed_release_due))return;
        state_->needs_action=false;
        if(state_->finalizer_queued){cleanup=true;state_->cleanup_running=true;}
        else if(!state_->scheduled){state_->finalizer_queued=true;state_->cleanup_running=true;cleanup=true;}
        else {state_->control_queued=true;scheduled=state_->scheduled;}
        managed=state_->managed_wss;
    }
    if(managed&&!managed->current())request_close();
    // The existing control/cleanup fence above prevents final owner release
    // while this actual currentness sample holds arbitrary application state.
    // Drop it before handing the cleanup branch to the collector.
    managed.reset();
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
        {std::lock_guard<std::mutex> lock(state_->mutex);
            if(state_->retired_managed_capture){
                if(!state_->retired_managed_capture->fields_destroyed())return;
                state_->retired_managed_capture.reset();
            }
            attempt=state_->attempt;
        }
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
                observe(configured_recovery_stage::native_completed,attempt);
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
                observe(configured_recovery_stage::native_completed,attempt);
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
            if(state_->managed_handlers)callback=state_->managed_handlers->error.load(std::memory_order_acquire);
            {std::lock_guard<std::mutex> lock(state_->mutex);if(state_->successor_refusal_reported)return;state_->successor_refusal_reported=true;if(!state_->managed_handlers)callback=state_->error_handler;}
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
        clock::time_point managed_deadline{};
        if(state_->managed_wss){
            // A new physical attempt consumes only the retained factory's
            // original admission. It never renews that grant. An already
            // admitted history episode still uses current()/expires(), with
            // its original transfer deadline and retained settlement debt.
            managed_deadline=std::min(clock::now()+std::chrono::seconds(30),state_->managed_wss->initial_deadline());
            if(!state_->managed_wss->current()||clock::now()>=managed_deadline){request_close();return;}
        }
        const auto receipt=state_->reservation->begin_attempt();
        std::shared_ptr<configured_attempt> replacement;
        try{
            auto captures=state_->managed_wss?std::make_shared<configured_managed_capture_release>(weak_from_this()):nullptr;
            replacement=std::make_shared<configured_attempt>(receipt,state_->registry,state_->factory,state_->typed,std::move(captures));
            replacement->control=shared_from_this();replacement->observer=state_->observer;replacement->managed_wss=state_->managed_wss;replacement->managed_deadline=managed_deadline;
            const std::weak_ptr<configured_recovery_connection> weak=shared_from_this();
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
        if(state_->managed_handlers)callback=state_->managed_handlers->error.load(std::memory_order_acquire);
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.remember(std::current_exception(),false);attempt=state_->attempt;if(!state_->managed_handlers)callback=state_->error_handler;
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
        if(state_->retired_managed_capture){
            if(!state_->retired_managed_capture->fields_destroyed()){state_->cleanup_running=false;return;}
            state_->retired_managed_capture.reset();
        }
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
            std::optional<configured_retirement_snapshot> before_collection;
            if(state_->observer)before_collection=state_->registry->snapshot(attempt->receipt);
            const bool collected=state_->registry->collect_completed(attempt->receipt);
            if(collected)observe(configured_recovery_stage::collected,attempt,before_collection);
            bool pending=false;
            {std::lock_guard<std::mutex> lock(state_->mutex);state_->cleanup_running=false;
                if(collected){
                    // Existing real native/adapter/lifetime/custody collection
                    // has succeeded. Retain observation before removing the
                    // controller copy; collector's local still owns attempt.
                    state_->retired_managed_capture=attempt->managed_capture_.completion;
                    state_->attempt.reset();
                }
                pending=state_->needs_action;
            }
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
    using terminal_storage=native_retirement_storage<std::shared_ptr<lattice_db>,std::shared_ptr<scheduler>,
        std::optional<configured_retirement_registry::reservation>,std::shared_ptr<configured_managed_handlers>,
        std::shared_ptr<const ordinary_managed_wss_admission>,std::shared_ptr<const ordinary_managed_owner_release>>;
    std::unique_ptr<terminal_storage> retained;
    try{retained=std::make_unique<terminal_storage>();}
    catch(...){std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.remember(std::current_exception());state_->cleanup_running=false;state_->quarantined=true;state_->settled.notify_all();return;}
    auto& child=retained->get<0>();auto& scheduled=retained->get<1>();
    {std::lock_guard<std::mutex> lock(state_->mutex);if(!state_->closing)return;child=state_->child;scheduled=state_->scheduled;}
    lattice_close_result result;
    if(child)result=child->close_checked();
    if(!retained->in_creator_process())return;
    bool joined=false;
    try{
        if(scheduled){if(scheduled->is_on_thread())throw db_error("configured scheduler self join refused");scheduled->shutdown();}
        joined=true;
    }catch(...){result.remember(std::current_exception());}
    if(!retained->in_creator_process())return;
    const bool complete=result.cleanup_complete&&joined;
    auto& released_reservation=retained->get<2>();
    {std::lock_guard<std::mutex> lock(state_->mutex);
        state_->child_closed=result.cleanup_complete;state_->scheduler_joined=joined;
        state_->close_result.merge(result);
        if(complete){
            child=std::move(state_->child);scheduled=std::move(state_->scheduled);
            if(state_->reservation){released_reservation.emplace(std::move(*state_->reservation));state_->reservation.reset();}
        }
    }
    if(!retained->release<0>()||!retained->release<1>()||!retained->release<2>())return;
    // Only APPLICATION retirement clears its registration. A normal managed
    // stop/reconnect keeps the exact same cell and all three listener identities.
    // Real native/attempt collection, child close and scheduler join above have
    // already returned. The still-issued admission below retains app payload
    // throughout actual attached target disposal, off all controller/cell leaves.
    auto& handlers=retained->get<3>();handlers=state_->managed_handlers;
    const bool disposal_pending=complete&&handlers&&handlers->application_retiring()&&!handlers->dispose_retired();
    if(!retained->in_creator_process())return;
    if(disposal_pending){
        {std::lock_guard<std::mutex> lock(state_->mutex);state_->cleanup_running=false;}
        // A completed-disposal wake may have reached a fenced control turn.
        // Recheck only after clearing that fence; no poll/spin or fake close.
        if(handlers->retirement_disposed())notify();
        return; // Concurrent/reentrant actual disposal remains pending.
    }
    auto& retired_admission=retained->get<4>();
    if(complete){std::lock_guard<std::mutex> lock(state_->mutex);retired_admission=std::move(state_->managed_wss);}
    if(!retained->release<4>())return; // Real admission destruction remains off every controller leaf.
    // The service epilogue also owes its real fixed cleanup-slot release.
    // Keep dispatch fenced until that epilogue publishes completed close.
    auto& released=retained->get<5>();
    {std::lock_guard<std::mutex> lock(state_->mutex);released=state_->managed_owner_release;}
    const bool captures_released=!released||(released->fields_destroyed()&&released->native_lock_complete());
    const bool lock_failed=captures_released&&released&&released->lock_error()!=0;
    const auto start_error=released?released->start_cleanup_error():std::exception_ptr{};
    const auto lock_error=lock_failed?configured_error("managed WSS ownership lock release failed"):std::exception_ptr{};
    {std::lock_guard<std::mutex> lock(state_->mutex);
        if(start_error)state_->close_result.remember(start_error);
        if(lock_error)state_->close_result.remember(lock_error);
        state_->close_release_ready=complete&&captures_released&&!lock_failed&&!start_error;
        state_->cleanup_running=state_->close_release_ready;state_->quarantined=!complete||lock_failed||bool(start_error);
    }
    // A destructor wake can be consumed while cleanup_running still fences
    // dispatch, after the earlier false captures_released sample. Recheck only
    // after publishing that fence clear. Completion before this recheck earns
    // another real dispatch; later completion supplies its existing raw wake.
    // No timer/poll or closed fact is minted here: the next collector turn must
    // still check the actual lock result and release its real service slot.
    if(complete&&!captures_released&&released&&released->fields_destroyed()&&released->native_lock_complete())notify();
    if(lock_failed||start_error)state_->settled.notify_all();
    if(!complete)state_->settled.notify_all();
}
void configured_recovery_connection::cleanup_returned()noexcept{
    {std::lock_guard<std::mutex> lock(state_->mutex);if(!state_->close_release_ready)return;}
    if(!configured_control_service::instance().release(state_->service_slot,this))return;
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_release_ready=false;state_->closed=true;state_->cleanup_running=false;}
    observe(configured_recovery_stage::logical_closed,{});
    state_->settled.notify_all();
}
lattice_close_result configured_recovery_connection::close(clock::time_point deadline)noexcept{
    // Only the first actual invocation is sampled. Missing observations stay
    // missing; later explicit/destructor closes cannot replace the first call.
    const bool diagnostic=state_->observer&&!state_->first_close_observed.exchange(true,std::memory_order_relaxed);
    const auto diagnostic_started=diagnostic?clock::now():clock::time_point{};
    bool diagnostic_drain_called=false;
    const auto sample_close=[&](uint32_t point,const std::shared_ptr<configured_attempt>& sampled,
                               const lattice_close_result& report,bool reentrant)noexcept{
        if(!diagnostic)return;
        configured_close_observation out;out.point=point;out.owner=reinterpret_cast<std::uintptr_t>(this);
        out.sync=static_cast<uint32_t>(report.sync);
        out.report_flags=uint32_t(report.cleanup_complete)|(uint32_t(bool(report.error))<<1)|
            (uint32_t(bool(report.cleanup_error))<<2)|(uint32_t(reentrant)<<3)|
            (uint32_t(diagnostic_drain_called)<<4)|(uint32_t(bool(sampled))<<5);
        size_t service_slot=64;
        {std::unique_lock<std::mutex> lock(state_->mutex,std::try_to_lock);
            if(lock.owns_lock()){
                out.sampled_leaves|=1u;
                const bool bits[]={state_->initializing,state_->desired,state_->closing,state_->closed,state_->quarantined,
                    state_->needs_action,state_->control_queued,state_->finalizer_queued,state_->worker_join_queued,
                    state_->cleanup_running,state_->child_closed,state_->scheduler_joined,state_->close_release_ready,
                    bool(sampled)&&state_->attempt==sampled,bool(state_->close_result.error),bool(state_->close_result.cleanup_error)};
                for(size_t i=0;i<16;++i)out.state_flags|=uint32_t(bits[i])<<i;
                service_slot=state_->service_slot;
            }
        }
        if(sampled){
            out.attempt=reinterpret_cast<std::uintptr_t>(sampled.get());
            out.interval=sampled->registry->comparison_identity(sampled->receipt);
            configured_attempt_custody::observation counts;
            if(sampled->custody->try_snapshot(counts)){
                out.sampled_leaves|=1u<<1;out.commands=counts.commands;out.payloads=counts.payloads;
                out.workers=counts.workers;out.finished_workers=counts.finished_workers;out.custody_error=counts.first_error;
            }
            configured_retirement_snapshot status;
            if(sampled->registry->try_snapshot(sampled->receipt,status)){
                out.sampled_leaves|=1u<<2;
                const bool bits[]={status.valid,status.requested,status.adapter_complete,status.native_complete,
                    status.owner_live,status.transport_retained,status.quarantined};
                for(size_t i=0;i<7;++i)out.registry_flags|=uint32_t(bits[i])<<i;
                out.registry_error=status.first_error;
            }
            {std::unique_lock<std::mutex> lock(sampled->facts_mutex,std::try_to_lock);
                if(lock.owns_lock()){
                    out.sampled_leaves|=1u<<3;
                    const bool bits[]={sampled->construction_finished.load(),sampled->wrapper_destroyed.load(),
                        sampled->factory_entered.load(),sampled->retirement_started.load(),sampled->lane_complete,
                        sampled->route_registered,sampled->route_unregistered,sampled->lane_result.disconnect_returned,
                        sampled->lane_result.pacer_present,sampled->lane_result.pacer_joined,sampled->lane_result.callbacks_settled};
                    for(size_t i=0;i<11;++i)out.attempt_flags|=uint32_t(bits[i])<<i;
                }
            }
            std::shared_ptr<sync_callback_lifetime> lifetime;
            if(sampled->try_lifetime(lifetime)){
                if(lifetime){out.attempt_flags|=1u<<11;if(lifetime->try_active_callbacks(out.active_callbacks))out.sampled_leaves|=1u<<4;}
                else out.sampled_leaves|=1u<<4; // Actual no-lifetime observation.
            }
        }
        if(service_slot<64){
            const auto service=configured_control_service::instance().sample(service_slot,this);
            if(service.available){out.sampled_leaves|=1u<<5;out.service_flags=service.flags;
                out.collector_busy=service.collector_busy;out.dispatcher_busy=service.dispatcher_busy;}
        }
        const auto now=clock::now();
        const auto milliseconds=[&](clock::duration value)noexcept{
            if(value<=clock::duration::zero())return uint32_t(0);
            const auto count=std::chrono::duration_cast<std::chrono::milliseconds>(value).count();
            if(count>65535){out.time_clamped=true;return uint32_t(65535);}
            return static_cast<uint32_t>(count);
        };
        out.elapsed_ms=milliseconds(now-diagnostic_started);out.remaining_ms=milliseconds(deadline-now);
        state_->observer->record_close(out); // One try-lock; never retries/waits.
    };
    lattice_close_result result;
    std::optional<borrow> held;
    // Try to retain the exact existing attempt for drain. Even admission
    // failure must not skip the logical close fence below.
    try{held.emplace(command());}catch(...){result.remember(std::current_exception(),false);}
    std::shared_ptr<configured_attempt> attempt;std::shared_ptr<scheduler> scheduled;
    {std::lock_guard<std::mutex> lock(state_->mutex);
        state_->closing=true;state_->desired=false;attempt=state_->attempt;scheduled=state_->scheduled;
    }
    sample_close(1,attempt,result,false);
    bool reentrant=false;
    try{
        reentrant=configured_control_service::on_cleanup_thread()||(scheduled&&scheduled->is_on_thread());
        if(attempt)if(auto lifetime=attempt->lifetime())if(lifetime->executing_here())reentrant=true;
        if(held&&held->pointer){
            diagnostic_drain_called=true;
            const auto drained=held->pointer->drain_checked(deadline);result.sync=drained.state;result.remember(drained.error,false);
        }
    }catch(...){result.remember(std::current_exception(),false);}
    sample_close(2,held&&held->attempt?held->attempt:attempt,result,reentrant);
    held.reset(); // Actual drain captures/borrow settle before retirement.
    {std::lock_guard<std::mutex> lock(state_->mutex);state_->close_result.merge(result);}
    // A managed diagnostic strong copy cannot remain through this wait:
    // it would itself prevent the actual last-field destruction being awaited.
    // Keep only its weak identity after sample1/sample2 and the real drain.
    const bool managed_sample=attempt&&bool(attempt->managed_wss);
    const std::weak_ptr<configured_attempt> retired_sample=managed_sample?attempt:std::shared_ptr<configured_attempt>{};
    if(attempt)request_renewal(attempt);else notify();
    if(managed_sample)attempt.reset();
    std::unique_lock<std::mutex> lock(state_->mutex);
    if(!reentrant)state_->settled.wait_until(lock,deadline,[&]{return state_->closed||state_->quarantined;});
    result=state_->close_result;
    result.cleanup_complete=state_->closed&&state_->child_closed&&state_->scheduler_joined;
    if(!result.cleanup_complete&&result.sync!=sync_drain_state::failed)
        result.sync=reentrant?sync_drain_state::reentrant_pending:sync_drain_state::deadline_pending;
    lock.unlock(); // Sample off the original wait/result leaf.
    // A destroyed attempt cannot be resurrected for diagnostics. Missing
    // post-wait leaves remain missing; never replay a stale pre-close sample.
    sample_close(3,managed_sample?retired_sample.lock():attempt,result,reentrant);
    return result;
}
}
#endif
