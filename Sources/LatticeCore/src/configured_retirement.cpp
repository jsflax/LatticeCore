#include "configured_retirement.hpp"
#include <lattice/network.hpp>
#include <limits>
#include <stdexcept>
#include <utility>

namespace lattice {
platform_retirement_receipt::platform_retirement_receipt(std::shared_ptr<detail::configured_retirement_registry> registry,size_t slot,uint64_t owner,uint64_t attempt) noexcept
    :registry_(std::move(registry)),slot_(slot),owner_(owner),attempt_(attempt){}
bool platform_retirement_receipt::valid() const noexcept {return registry_&&registry_->snapshot(*this).valid;}
bool platform_retirement_receipt::matches(const platform_retirement_receipt& other) const noexcept {
    return registry_&&registry_==other.registry_&&owner_&&attempt_&&slot_==other.slot_&&owner_==other.owner_&&attempt_==other.attempt_;
}
bool platform_retirement_receipt::retirement_requested() const noexcept {return registry_&&registry_->snapshot(*this).requested;}
bool platform_retirement_receipt::complete_adapter_cleanup(int32_t error) const noexcept {return registry_&&registry_->complete(*this,error,true);}
}
namespace lattice::detail {
configured_retirement_registry::configured_retirement_registry(size_t capacity):capacity_(capacity){
    if(!capacity||capacity>maximum_owners)throw std::invalid_argument("invalid configured retirement capacity");
}
std::shared_ptr<configured_retirement_registry> configured_retirement_registry::instance(){
    static const auto* registry=new std::shared_ptr<configured_retirement_registry>(new configured_retirement_registry(maximum_owners));
    return *registry;
}
configured_retirement_registry::reservation::reservation(std::shared_ptr<configured_retirement_registry> registry,size_t slot,uint64_t owner) noexcept
    :registry_(std::move(registry)),slot_(slot),owner_(owner){}
configured_retirement_registry::reservation::reservation(reservation&& other) noexcept
    :registry_(std::move(other.registry_)),slot_(other.slot_),owner_(other.owner_){}
configured_retirement_registry::reservation::~reservation(){if(registry_)registry_->abandon(slot_,owner_);}
platform_retirement_receipt configured_retirement_registry::reservation::begin_attempt(){
    if(!registry_)throw std::logic_error("invalid configured retirement reservation");
    return registry_->begin(slot_,owner_);
}
configured_retirement_registry::reservation configured_retirement_registry::reserve(){
    auto keep=shared_from_this();
    std::lock_guard<std::mutex> lock(mutex_);
    if(next_owner_==std::numeric_limits<uint64_t>::max())throw std::overflow_error("configured retirement owner serial exhausted");
    for(size_t i=0;i<capacity_;++i){auto& s=slots_[i];if(s.occupied)continue;
        s.occupied=true;s.owner_live=true;s.owner=++next_owner_;
        return {std::move(keep),i,s.owner};
    }
    throw std::runtime_error("configured retirement capacity exhausted before factory allocation");
}
platform_retirement_receipt configured_retirement_registry::begin(size_t index,uint64_t owner){
    auto keep=shared_from_this();
    std::lock_guard<std::mutex> lock(mutex_);
    auto& s=slots_[index];
    if(!s.occupied||!s.owner_live||s.owner!=owner||s.attempt)throw std::logic_error("configured retirement attempt already charged or owner closed");
    if(next_attempt_==std::numeric_limits<uint64_t>::max())throw std::overflow_error("configured retirement attempt serial exhausted");
    s.attempt=++next_attempt_;
    return {std::move(keep),index,owner,s.attempt};
}
bool configured_retirement_registry::current_locked(const platform_retirement_receipt& receipt) const noexcept {
    if(receipt.registry_.get()!=this||receipt.slot_>=capacity_||!receipt.owner_||!receipt.attempt_)return false;
    const auto& s=slots_[receipt.slot_];
    return s.occupied&&s.owner==receipt.owner_&&s.attempt==receipt.attempt_;
}
bool configured_retirement_registry::bind_request(const platform_retirement_receipt& receipt,request_handler callback){
    if(!callback)return false;
    auto retained=std::make_shared<const request_handler>(std::move(callback));
    callback=nullptr;
    bool deliver=false;
    {std::lock_guard<std::mutex> lock(mutex_);
        if(!current_locked(receipt))return false;
        auto& s=slots_[receipt.slot_];if(s.request||s.collecting)return false;
        s.request=std::move(retained);
        if(s.requested){s.notified=true;s.notifying=true;retained=s.request;deliver=true;}
    }
    if(deliver)notify(receipt,std::move(retained));
    return true;
}
bool configured_retirement_registry::retain_transport(const platform_retirement_receipt& receipt,std::shared_ptr<sync_transport> transport){
    if(!transport)return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if(!current_locked(receipt))return false;
    auto& s=slots_[receipt.slot_];
    // Late construction may finish after request, but never after its native
    // cleanup assertion; request delivery must settle that late resource too.
    if(s.transport||s.native_complete||s.adapter_complete||s.collecting)return false;
    s.transport=std::move(transport);return true;
}
void configured_retirement_registry::notify(const platform_retirement_receipt& receipt,std::shared_ptr<const request_handler> callback) noexcept {
    try {(*callback)(receipt);}
    catch(...){
        std::lock_guard<std::mutex> lock(mutex_);
        if(current_locked(receipt)){auto& s=slots_[receipt.slot_];if(!s.first_error)s.first_error=request_delivery_failed;}
    }
    callback.reset();
    {std::lock_guard<std::mutex> lock(mutex_);if(current_locked(receipt))slots_[receipt.slot_].notifying=false;}
}
bool configured_retirement_registry::request_retirement(const platform_retirement_receipt& receipt) noexcept {
    std::shared_ptr<const request_handler> callback;
    {std::lock_guard<std::mutex> lock(mutex_);
        if(!current_locked(receipt))return false;
        auto& s=slots_[receipt.slot_];if(s.requested)return false;
        s.requested=true;
        if(s.request&&!s.notified){s.notified=true;s.notifying=true;callback=s.request;}
    }
    if(callback)notify(receipt,std::move(callback));
    return true;
}
void configured_retirement_registry::abandon(size_t index,uint64_t owner) noexcept {
    platform_retirement_receipt receipt;
    {std::lock_guard<std::mutex> lock(mutex_);
        auto& s=slots_[index];if(!s.occupied||s.owner!=owner)return;
        s.owner_live=false;
        if(!s.attempt){s.occupied=false;return;}
        receipt=platform_retirement_receipt(shared_from_this(),index,owner,s.attempt);
    }
    // Pending/failed custody remains charged even when the logical owner dies.
    request_retirement(receipt);
}
bool configured_retirement_registry::complete(const platform_retirement_receipt& receipt,int32_t error,bool adapter) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!current_locked(receipt))return false;
    auto& s=slots_[receipt.slot_];if(!s.requested)return false;
    auto& done=adapter?s.adapter_complete:s.native_complete;
    if(done)return false;
    done=true;if(error&&!s.first_error)s.first_error=error;
    return true;
}
bool configured_retirement_registry::complete_native_cleanup(const platform_retirement_receipt& receipt,int32_t error) noexcept {return complete(receipt,error,false);}
configured_retirement_snapshot configured_retirement_registry::snapshot(const platform_retirement_receipt& receipt) const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!current_locked(receipt))return {};
    const auto& s=slots_[receipt.slot_];
    return {true,s.requested,s.adapter_complete,s.native_complete,s.owner_live,bool(s.transport),bool(s.first_error),s.first_error};
}
bool configured_retirement_registry::collect_completed(const platform_retirement_receipt& receipt) noexcept {
    std::shared_ptr<sync_transport> transport;
    std::shared_ptr<const request_handler> request;
    {std::lock_guard<std::mutex> lock(mutex_);
        if(!current_locked(receipt))return false;
        auto& s=slots_[receipt.slot_];
        if(!s.requested||!s.adapter_complete||!s.native_complete||s.first_error||s.notifying||s.collecting)return false;
        s.collecting=true;
        transport=std::move(s.transport);request=std::move(s.request);
    }
    // Actual destruction is deliberately confined to the collector caller and
    // outside the registry leaf, never the adapter's completion callback.
    transport.reset();request.reset();
    {std::lock_guard<std::mutex> lock(mutex_);auto& s=slots_[receipt.slot_];
        s.attempt=0;s.requested=false;s.notified=false;s.adapter_complete=false;s.native_complete=false;s.collecting=false;
        if(!s.owner_live)s.occupied=false;
    }
    return true;
}
size_t configured_retirement_registry::charged_owners() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);size_t count=0;
    for(size_t i=0;i<capacity_;++i)if(slots_[i].occupied)++count;
    return count;
}
}
