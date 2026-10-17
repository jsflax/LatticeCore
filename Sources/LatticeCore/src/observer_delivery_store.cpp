#include "observer_delivery_store.hpp"
#include <lattice/db.hpp>
#include <array>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace lattice::detail {
struct observer_delivery_store::state {
    const uint64_t creator=database_native_process();
    const uint64_t device,inode;
    const domain::limits limits;
    const domain budget;
    state(uint64_t dev,uint64_t ino,domain::limits value)
        :device(dev),inode(ino),limits(value),budget(domain::create(value)){}
    static void destroy(state* value)noexcept {
        if(value&&value->creator==database_native_process())delete value;
    }
};
namespace {
const uint64_t directory_creator=database_native_process();
bool same_limits(const observer_delivery_domain::limits& a,const observer_delivery_domain::limits& b)noexcept {
    return a.clients==b.clients&&a.waiters==b.waiters&&a.guaranteed==b.guaranteed&&a.burst==b.burst;
}
}

observer_delivery_store observer_delivery_store::bind(database& writer,domain::limits limits) {
    const auto creator=database_native_process();
    // Before both Native and the function-local singleton initialization guard.
    if(creator!=directory_creator)throw std::runtime_error("inherited physical delivery domain directory");
    // Validation may enter SQLite, but it finishes before creating/acquiring
    // any domain or directory leaf. No descriptor is opened/closed here.
    const auto physical=writer.physical_identity("main",{},true);
    if(!physical||!physical->inode)throw std::runtime_error("delivery domain requires a live physical MAIN store");
    struct directory {
        const uint64_t creator=database_native_process();
        std::mutex mutex;
        struct slot {
            std::weak_ptr<state> value;
            uint64_t device=0,inode=0,serial=0;
            bool building=false;
        };
        std::array<slot,64> stores;
        uint64_t next=0;
    };
    // An inherited singleton is denial-only. Never enter its copied mutex.
    static directory* const index=new directory;
    if(creator!=database_native_process()||index->creator!=creator)
        throw std::runtime_error("inherited physical delivery domain directory");
    std::shared_ptr<state> selected;
    // Even a weak lock can become the last strong custody after a concurrent
    // owner drop. Hold ALL lookup pins until outside directory metadata.
    std::array<std::shared_ptr<state>,64> observed;
    std::weak_ptr<state> displaced;
    size_t reserved=index->stores.size();uint64_t serial=0;
    {
        std::lock_guard lock(index->mutex);
        size_t available=index->stores.size();
        for(size_t i=0;i<index->stores.size();++i) {
            auto& slot=index->stores[i];
            if(slot.building){
                if(slot.device==physical->device&&slot.inode==physical->inode)
                    throw std::runtime_error("physical delivery domain construction pending");
                continue;
            }
            observed[i]=slot.value.lock();const auto& actual=observed[i];
            if(!actual){if(available==index->stores.size())available=i;continue;}
            if(actual->device==physical->device&&actual->inode==physical->inode) {
                if(!same_limits(actual->limits,limits))
                    throw std::runtime_error("physical delivery domain configuration differs");
                selected=actual;break;
            }
        }
        if(!selected) {
            if(available==index->stores.size())throw std::runtime_error("physical delivery domain directory capacity");
            if(index->next==UINT64_MAX)throw std::runtime_error("physical delivery domain identity exhausted");
            auto& slot=index->stores[available];displaced=std::move(slot.value);
            slot.device=physical->device;slot.inode=physical->inode;slot.serial=++index->next;slot.building=true;
            reserved=available;serial=slot.serial;
        }
    }
    if(selected)return observer_delivery_store(std::move(selected));
    struct construction {
        directory* index;size_t slot;uint64_t serial,creator;bool active=true;
        ~construction()noexcept {
            if(!active||creator!=database_native_process())return;
            std::lock_guard lock(index->mutex);auto& value=index->stores[slot];
            if(value.building&&value.serial==serial)value.building=false;
        }
    } building{index,reserved,serial,creator};
    // Fixed metadata custody exists BEFORE either domain/state allocation.
    // No unbounded contenders may construct full candidate domains. Declared
    // before candidate, the guard releases only AFTER failed candidate tail.
    auto candidate=std::shared_ptr<state>(new state(physical->device,physical->inode,limits),state::destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited physical delivery domain construction");
    {
        std::lock_guard lock(index->mutex);auto& slot=index->stores[reserved];
        if(!slot.building||slot.serial!=serial||slot.device!=physical->device||slot.inode!=physical->inode)
            throw std::runtime_error("physical delivery domain construction custody differs");
        slot.value=candidate;slot.building=false;building.active=false;selected=candidate;
    }
    return observer_delivery_store(std::move(selected));
}
observer_delivery_store::lease::lease(std::shared_ptr<state> store,domain::lease held)
    :store_(std::move(store)),held_(std::move(held)){}
observer_delivery_store::lease& observer_delivery_store::lease::operator=(lease other)noexcept {
    // Incoming and displaced binding pins both survive the complete exchange.
    // The parameter retires its displaced domain lease before its store pin.
    std::swap(held_,other.held_);std::swap(store_,other.store_);return *this;
}
bool observer_delivery_store::lease::valid()const noexcept {
    return store_&&store_->creator==database_native_process()&&held_.valid();
}
bool observer_delivery_store::lease::current_for(database& writer)const {
    if(!valid())return false;
    const auto physical=writer.physical_identity("main",{},true);
    return physical&&physical->device==store_->device&&physical->inode==store_->inode;
}
observer_delivery_store::units observer_delivery_store::lease::charge()const noexcept {
    return valid()?held_.charge():units{};
}
void observer_delivery_store::lease::reset()noexcept {held_.reset();store_.reset();}
observer_delivery_store::client::client(std::shared_ptr<state> store,domain::client value)
    :store_(std::move(store)),client_(std::move(value)){}
observer_delivery_store::client& observer_delivery_store::client::operator=(client other)noexcept {
    std::swap(client_,other.client_);std::swap(store_,other.store_);return *this;
}
observer_delivery_store::admission observer_delivery_store::client::try_acquire(units wanted)const {
    const auto actual=store_;
    if(!actual)return {};
    if(actual->creator!=database_native_process())return {status::wrong_process,{}};
    auto result=client_.try_acquire(wanted);
    return {result.code,result.code==status::admitted?lease(actual,std::move(result.held)):lease{}};
}
observer_delivery_store::admission observer_delivery_store::client::acquire(
    units wanted,clock::time_point end,const cancellation* cancel)const {
    const auto actual=store_;
    if(!actual)return {};
    if(actual->creator!=database_native_process())return {status::wrong_process,{}};
    if(cancel&&cancel->store_!=actual)return {status::invalid,{}};
    auto result=client_.acquire(wanted,end,cancel?&cancel->stop_:nullptr);
    return {result.code,result.code==status::admitted?lease(actual,std::move(result.held)):lease{}};
}
void observer_delivery_store::client::close()const noexcept {
    if(store_&&store_->creator==database_native_process())client_.close();
}
void observer_delivery_store::client::reset()noexcept {client_.reset();store_.reset();}
observer_delivery_store::cancellation::cancellation(const observer_delivery_store& value)
    :store_(value.state_),stop_(store_?store_->budget:domain{}){}
void observer_delivery_store::cancellation::request_stop()noexcept {
    if(store_&&store_->creator==database_native_process())stop_.request_stop();
}
bool observer_delivery_store::cancellation::stop_requested()const noexcept {
    return !store_||store_->creator!=database_native_process()||stop_.stop_requested();
}
observer_delivery_store::registration observer_delivery_store::register_client()const {
    const auto actual=state_;
    if(!actual)return {};
    if(actual->creator!=database_native_process())return {status::wrong_process,{}};
    auto result=actual->budget.register_client();
    return {result.code,result.code==status::admitted?client(actual,std::move(result.value)):client{}};
}
observer_delivery_store::domain::snapshot observer_delivery_store::inspect()const noexcept {
    if(!state_)return {};
    if(state_->creator!=database_native_process()){domain::snapshot result;result.code=status::wrong_process;return result;}
    return state_->budget.inspect();
}
bool observer_delivery_store::same_domain(const observer_delivery_store& other)const noexcept {
    return state_&&state_->creator==database_native_process()&&state_==other.state_;
}
bool observer_delivery_store::current_for(database& writer)const {
    if(!state_||state_->creator!=database_native_process())return false;
    const auto actual=writer.physical_identity("main",{},true);
    return actual&&actual->device==state_->device&&actual->inode==state_->inode;
}
}
