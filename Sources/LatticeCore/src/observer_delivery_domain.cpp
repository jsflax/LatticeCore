#include "observer_delivery_domain.hpp"
#include "observer_delivery_domain_test_access.hpp"
#include <algorithm>
#include <array>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>

namespace lattice::detail {
uint64_t database_native_process() noexcept; // existing portable Core helper
namespace {
using domain = observer_delivery_domain;
using units = domain::units;
thread_local bool fail_client_allocation=false,fail_lease_allocation=false;
uint64_t extra(uint64_t value, uint64_t guaranteed) noexcept {
    return value > guaranteed ? value - guaranteed : 0;
}
bool valid(units value) noexcept { return value.count != 0 && value.bytes != 0; }
}
struct observer_delivery_domain::state {
    const uint64_t creator;
    const limits caps;
    const units capacity;
    std::mutex mutex;
    std::condition_variable changed;
    bool closed = false, poisoned = false;
    struct slot {
        bool occupied = false, closed = false, building = false;
        uint64_t serial = 0;
        units used;
        size_t waiters = 0;
    };
    struct waiter {
        bool active = false;
        size_t slot = 0;
        uint64_t serial = 0, order = 0;
        units wanted;
        clock::time_point deadline;
        const cancellation* stop = nullptr; // live acquire frame owns lifetime
    };
    std::array<slot,64> slots{};
    std::array<waiter,256> queue{};
    units used, burst_used;
    size_t clients = 0, waiters = 0;
    uint64_t leases = 0, next_order = 0;
    state(limits value, units maximum, uint64_t process) : creator(process), caps(value), capacity(maximum) {}
    units excess(units value) const noexcept {
        return {extra(value.count,caps.guaranteed.count),extra(value.bytes,caps.guaranteed.bytes)};
    }
    bool obsolete(const waiter& value, clock::time_point stamp) const noexcept {
        const auto& s=slots[value.slot];
        return stamp>=value.deadline || (value.stop&&value.stop->stop_requested()) ||
            !s.occupied || s.closed || s.serial!=value.serial;
    }
    bool needs_burst(const slot& s, units wanted) const noexcept {
        // Request maxima and the subtraction checks in fits() bound overflow.
        return wanted.count>caps.guaranteed.count-std::min(s.used.count,caps.guaranteed.count) ||
            wanted.bytes>caps.guaranteed.bytes-std::min(s.used.bytes,caps.guaranteed.bytes);
    }
    bool fits(const slot& s,units wanted) const noexcept {
        const units maximum{caps.guaranteed.count+caps.burst.count,caps.guaranteed.bytes+caps.burst.bytes};
        if(wanted.count>maximum.count-s.used.count || wanted.bytes>maximum.bytes-s.used.bytes)return false;
        const auto before=excess(s.used);
        const auto after=excess({s.used.count+wanted.count,s.used.bytes+wanted.bytes});
        return after.count-before.count<=caps.burst.count-burst_used.count &&
               after.bytes-before.bytes<=caps.burst.bytes-burst_used.bytes;
    }
    bool eligible(size_t index,units wanted,uint64_t order,clock::time_point stamp) const noexcept {
        const auto& s=slots[index];
        if(!fits(s,wanted))return false;
        const bool burst=needs_burst(s,wanted);
        for(size_t i=0;i<caps.waiters;++i){const auto& prior=queue[i];
            if(!prior.active||prior.order>=order||obsolete(prior,stamp))continue;
            // Never overtake an older request from this client. Other clients'
            // guaranteed shares bypass a blocked burst head by design.
            if(prior.slot==index || (burst&&needs_burst(slots[prior.slot],prior.wanted)))return false;
        }
        return true;
    }
    void remove(waiter& value) noexcept {
        if(!value.active)return;
        --slots[value.slot].waiters;--waiters;value.active=false;
    }
    void charge(slot& s,units value) noexcept {
        const auto before=excess(s.used);
        s.used.count+=value.count;s.used.bytes+=value.bytes;
        const auto after=excess(s.used);
        burst_used.count+=after.count-before.count;burst_used.bytes+=after.bytes-before.bytes;
        used.count+=value.count;used.bytes+=value.bytes;++leases;
    }
};
struct observer_delivery_domain::client_state {
    std::shared_ptr<state> domain;
    const uint64_t creator = database_native_process();
    size_t index = 0;
    uint64_t serial = 0;
    bool registered = false;
};
struct observer_delivery_domain::lease_state {
    std::shared_ptr<client_state> client;
    const uint64_t creator = database_native_process();
    units amount;
    bool granted = false;
};

observer_delivery_domain::cancellation::cancellation(const observer_delivery_domain& domain)
    : creator_(database_native_process()),domain_(domain.value_) {
    if(!domain_||domain_->creator!=creator_)throw std::invalid_argument("cancellation requires its current domain");
}
void observer_delivery_domain::cancellation::request_stop() noexcept {
    if(creator_!=database_native_process())return;
    // The same leaf orders the stop publication with check -> CV wait. A
    // notify without this leaf could otherwise be lost before wait begins.
    {std::lock_guard lock(domain_->mutex);stopped_.store(true,std::memory_order_release);}
    domain_->changed.notify_all();
}
bool observer_delivery_domain::cancellation::stop_requested() const noexcept {
    return creator_!=database_native_process() || stopped_.load(std::memory_order_acquire);
}
observer_delivery_domain observer_delivery_domain::create(limits caps) {
    constexpr auto maximum=std::numeric_limits<uint64_t>::max();
    if(!caps.clients||caps.clients>64||!caps.waiters||caps.waiters>256||!valid(caps.guaranteed) ||
       caps.guaranteed.count>(maximum-caps.burst.count)/caps.clients ||
       caps.guaranteed.bytes>(maximum-caps.burst.bytes)/caps.clients)
        throw std::invalid_argument("invalid observer delivery domain limits");
    const units capacity{caps.guaranteed.count*caps.clients+caps.burst.count,
                         caps.guaranteed.bytes*caps.clients+caps.burst.bytes};
    const auto creator=database_native_process();
    auto value=std::shared_ptr<state>(new state(caps,capacity,creator),[creator](state* p)noexcept{
        if(creator==database_native_process())delete p;
    });
    if(creator!=database_native_process())throw std::runtime_error("delivery domain allocation crossed creator process");
    return observer_delivery_domain(std::move(value));
}
observer_delivery_domain::registration observer_delivery_domain::register_client() const {
    if(!value_)return {};
    if(value_->creator!=database_native_process())return {status::wrong_process,{}};
    const auto value=value_; // actual owner pin BEFORE allocator activity
    size_t index=0;uint64_t serial=0;
    {
        std::lock_guard lock(value->mutex);
        if(value->closed)return {status::closed,{}};
        if(value->poisoned)return {status::invariant_failure,{}};
        while(index<value->caps.clients&&(value->slots[index].occupied||
            value->slots[index].serial==std::numeric_limits<uint64_t>::max()))++index;
        if(index==value->caps.clients)return {status::client_capacity,{}};
        auto& s=value->slots[index];serial=++s.serial;s.occupied=true;s.closed=false;++value->clients;
    }
    struct reserved_slot {
        state& d;size_t index;uint64_t serial,creator;bool active=true;
        ~reserved_slot()noexcept {if(!active||creator!=database_native_process())return;
            {std::lock_guard lock(d.mutex);auto& s=d.slots[index];
                if(s.serial!=serial||!s.occupied||s.used!=units{}||s.waiters||s.building)d.poisoned=true;
                else{s.occupied=false;s.closed=false;--d.clients;}}
            d.changed.notify_all();
        }
    } reservation{*value,index,serial,value->creator};
    if(fail_client_allocation){fail_client_allocation=false;throw std::bad_alloc();}
    auto candidate=std::shared_ptr<client_state>(new client_state,[creator=value->creator](client_state* p)noexcept{
        if(creator!=database_native_process())return;
        if(p->registered){auto& d=*p->domain;{
            std::lock_guard lock(d.mutex);auto& s=d.slots[p->index];
            if(s.serial!=p->serial||!s.occupied||s.used!=units{}||s.waiters||s.building)d.poisoned=true;
            else{s.occupied=false;s.closed=false;--d.clients;}
        }d.changed.notify_all();}
        delete p; // domain's final ownership release is OFF its metadata leaf
    });
    candidate->domain=value;candidate->index=index;candidate->serial=serial;
    if(value->creator!=database_native_process())return {status::wrong_process,{}};
    {
        std::lock_guard lock(value->mutex);
        if(value->closed)return {status::closed,{}};
        if(value->poisoned)return {status::invariant_failure,{}};
        if(!value->slots[index].occupied||value->slots[index].serial!=serial)return {status::invariant_failure,{}};
        candidate->registered=true;reservation.active=false;
    }
    return {status::admitted,client(std::move(candidate))};
}
observer_delivery_domain::admission observer_delivery_domain::acquire_(const std::shared_ptr<client_state>& original,
    units wanted,clock::time_point deadline,const cancellation* stop,bool wait) {
    if(!original)return {};
    if(original->creator!=database_native_process())return {status::wrong_process,{}};
    const auto owner=original; // no caller field/reference across allocation
    const auto& d=owner->domain;
    if(stop&&stop->domain_.get()!=d.get())return {status::invalid,{}};
    if(!valid(wanted))return {status::invalid,{}};
    if(wanted.count>d->caps.guaranteed.count+d->caps.burst.count ||
       wanted.bytes>d->caps.guaranteed.bytes+d->caps.burst.bytes)return {status::oversized,{}};
    {
        std::lock_guard lock(d->mutex);auto& s=d->slots[owner->index];
        if(d->poisoned)return {status::invariant_failure,{}};
        if(d->closed||s.closed)return {status::closed,{}};
        if(!s.occupied||s.serial!=owner->serial)return {status::invariant_failure,{}};
        if(s.building)return {status::admission_capacity,{}};
        s.building=true; // one fixed prospective-allocation cell PER client
    }
    struct construction {
        state& d;size_t index;uint64_t serial,creator;bool active=true;
        void published_locked()noexcept {d.slots[index].building=false;active=false;}
        ~construction()noexcept {if(!active||creator!=database_native_process())return;
            {std::lock_guard lock(d.mutex);auto& s=d.slots[index];
                if(s.serial!=serial||!s.building)d.poisoned=true;else s.building=false;}
            d.changed.notify_all();
        }
    } building{*d,owner->index,owner->serial,owner->creator};
    state::waiter* queued=nullptr;
    struct waiter_ending {
        state& d;state::waiter*& value;uint64_t creator;
        ~waiter_ending()noexcept {if(!value||creator!=database_native_process())return;
            {std::lock_guard lock(d.mutex);d.remove(*value);}d.changed.notify_all();
        }
    } cleanup{*d,queued,owner->creator};
    // Both guards precede pending: an unpublished record must really be
    // destroyed OFF leaf before its construction/waiter cell is reusable.
    // Prospective allocation is bounded even under concurrent failed offers.
    if(fail_lease_allocation){fail_lease_allocation=false;throw std::bad_alloc();}
    auto pending=std::shared_ptr<lease_state>(new lease_state,[creator=owner->creator](lease_state* p)noexcept{
        if(creator!=database_native_process())return; // abandon inherited custody
        if(p->granted){auto& d=*p->client->domain;{
            std::lock_guard lock(d.mutex);auto& s=d.slots[p->client->index];
            if(!s.occupied||s.serial!=p->client->serial||p->amount.count>s.used.count||p->amount.bytes>s.used.bytes||!d.leases)
                d.poisoned=true; // no fabricated refund on invariant failure
            else{const auto before=d.excess(s.used);s.used.count-=p->amount.count;s.used.bytes-=p->amount.bytes;
                const auto after=d.excess(s.used);d.burst_used.count-=before.count-after.count;d.burst_used.bytes-=before.bytes-after.bytes;
                d.used.count-=p->amount.count;d.used.bytes-=p->amount.bytes;--d.leases;}
        }d.changed.notify_all();}
        delete p; // client/slot/domain retirement occurs only AFTER unlock
    });
    pending->client=owner;pending->amount=wanted;
    if(owner->creator!=database_native_process())return {status::wrong_process,{}};
    const bool reentrant=delivery_scope::active_for(d.get());
    std::unique_lock lock(d->mutex);
    auto& s=d->slots[owner->index];
    for(;;){
        const auto stamp=clock::now();
        if(d->poisoned)return {status::invariant_failure,{}};
        if(d->closed||s.closed)return {status::closed,{}};
        if(!s.occupied||s.serial!=owner->serial)return {status::invariant_failure,{}};
        if(stop&&stop->stop_requested())return {status::cancelled,{}};
        if(wait&&stamp>=deadline)return {status::deadline,{}};
        const auto order=queued?queued->order:std::numeric_limits<uint64_t>::max();
        if(d->eligible(owner->index,wanted,order,stamp)){
            d->charge(s,wanted);pending->granted=true;
            if(queued){d->remove(*queued);queued=nullptr;}
            if(building.active)building.published_locked();
            lock.unlock();d->changed.notify_all();
            return {status::admitted,lease(std::move(pending))};
        }
        if(reentrant)return {status::reentrant_would_block,{}};
        if(!wait)return {status::unavailable,{}};
        if(!queued){
            if(d->waiters==d->caps.waiters)return {status::waiter_capacity,{}};
            if(d->next_order==std::numeric_limits<uint64_t>::max())return {status::exhausted,{}};
            for(size_t i=0;i<d->caps.waiters;++i)if(!d->queue[i].active){queued=&d->queue[i];break;}
            if(!queued){d->poisoned=true;return {status::invariant_failure,{}};}
            *queued={true,owner->index,owner->serial,++d->next_order,wanted,deadline,stop};++d->waiters;++s.waiters;
            building.published_locked();
            d->changed.notify_all();
        }
        // Cancellation and retirement notify this actual CV. Spurious wakes
        // recheck the SAME absolute deadline; there is no polling/retry clock.
        d->changed.wait_until(lock,deadline);
    }
}
observer_delivery_domain::admission observer_delivery_domain::client::try_acquire(units wanted) const {
    return acquire_(value_,wanted,clock::time_point{},nullptr,false);
}
observer_delivery_domain::admission observer_delivery_domain::client::acquire(units wanted,clock::time_point deadline,
    const cancellation* stop) const {return acquire_(value_,wanted,deadline,stop,true);}
void observer_delivery_domain::client::close() const noexcept {
    const auto& p=value_;if(!p||p->creator!=database_native_process())return;
    auto& d=*p->domain;{std::lock_guard lock(d.mutex);auto& s=d.slots[p->index];if(s.serial==p->serial)s.closed=true;}
    d.changed.notify_all(); // active leases remain charged through true ending
}
bool observer_delivery_domain::lease::valid() const noexcept {
    return value_&&value_->creator==database_native_process()&&value_->granted;
}
observer_delivery_domain::units observer_delivery_domain::lease::charge() const noexcept {
    return valid()?value_->amount:units{};
}
observer_delivery_domain::snapshot observer_delivery_domain::inspect() const noexcept {
    const auto& p=value_;if(!p)return {};
    if(p->creator!=database_native_process())return {status::wrong_process};
    std::lock_guard lock(p->mutex);
    return {p->poisoned?status::invariant_failure:p->closed?status::closed:status::admitted,
        p->used,p->burst_used,p->capacity,p->clients,p->waiters,p->leases};
}
void observer_delivery_domain::close() const noexcept {
    const auto& p=value_;if(!p||p->creator!=database_native_process())return;
    {std::lock_guard lock(p->mutex);p->closed=true;}p->changed.notify_all();
}
thread_local observer_delivery_domain::delivery_scope* observer_delivery_domain::delivery_scope::current_=nullptr;
bool observer_delivery_domain::delivery_scope::active_for(const state* p) noexcept {
    const auto creator=database_native_process();
    for(auto* frame=current_;frame;frame=frame->previous_)
        if(frame->creator_==creator&&frame->domain_==p)return true;
    return false;
}
observer_delivery_domain::delivery_scope::delivery_scope(const lease& value) {
    if(!value.valid())throw std::invalid_argument("delivery scope requires a current owning lease");
    held_=value;domain_=held_.value_->client->domain.get();creator_=database_native_process();previous_=current_;current_=this;
}
observer_delivery_domain::delivery_scope::~delivery_scope() noexcept {
    current_=previous_; // only TLS shell; held_'s actual deleter fences PID
}
void observer_delivery_domain_test_access::lock_metadata(observer_delivery_domain& value) {value.value_->mutex.lock();}
void observer_delivery_domain_test_access::unlock_metadata(observer_delivery_domain& value) noexcept {value.value_->mutex.unlock();}
void observer_delivery_domain_test_access::fail_next_client_allocation() noexcept {fail_client_allocation=true;}
void observer_delivery_domain_test_access::fail_next_lease_allocation() noexcept {fail_lease_allocation=true;}
}
