#include "observer_delivery_original.hpp"
#include "observer_delivery_plan.hpp"
#include <stdexcept>
#include <sqlite3.h>
#include <utility>

namespace lattice::detail {
uint64_t database_native_process()noexcept;
thread_local observer_delivery_original::begin_offer* observer_delivery_original::begin_offer::current_=nullptr;
namespace {
struct delivery_frame {
    const uint64_t creator=database_native_process();
    std::shared_ptr<const observer_delivery_original> original;
    delivery_frame* previous=nullptr;
    explicit delivery_frame(std::shared_ptr<const observer_delivery_original> value);
    ~delivery_frame()noexcept;
};
thread_local delivery_frame* current_delivery=nullptr;
delivery_frame::delivery_frame(std::shared_ptr<const observer_delivery_original> value)
    :original(std::move(value)),previous(current_delivery){current_delivery=this;}
delivery_frame::~delivery_frame()noexcept {
    // This stack frame has DATA-only owning custody. The original's custom
    // deleter abandons all fields in a child; no inherited charge is returned.
    current_delivery=previous; // POD TLS shell only, never inherited payload cleanup.
}
}
struct observer_delivery_original::carried_work {
    const uint64_t creator;
    // Callable dies before credit, even on exception/cancellation/no invoke.
    std::shared_ptr<const observer_delivery_original> original;
    std::function<void()> work;
    std::vector<std::function<void()>> callbacks;
    std::atomic<bool> batch_adopted{false};
    explicit carried_work(uint64_t value):creator(value){}
    static void destroy(carried_work* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        if(!value->original){if(!value->work&&value->callbacks.empty())delete value;return;}
        // A capture destructor may reenter writes. Keep a real same-domain
        // no-self-wait scope through that tail as well as callback invocation.
        // Its original remains heap-owned until after arbitrary target cleanup.
        if(!observer_delivery_original::retire_target(value->original,value->work))return;
        if(value->creator!=database_native_process())return;
        // Captures may reenter/fork on destruction. Retire each target while
        // this same original remains live, and stop before any inherited next
        // target if the previous destructor returned in a child.
        try {
            delivery_frame frame(value->original);
            observer_delivery_store::delivery_scope no_self_wait(value->original->credit_);
            for(auto& callback:value->callbacks){callback={};if(value->creator!=database_native_process())return;}
        }catch(...){return;}
        value->callbacks.clear();
        delete value;
    }
};
struct observer_delivery_original::batch_target {
    std::shared_ptr<carried_work> owned;
    explicit batch_target(std::shared_ptr<carried_work> value):owned(std::move(value)){}
    batch_target(batch_target&&)noexcept=default;
    batch_target(const batch_target& other) {
        if(!other.owned||other.owned->creator!=database_native_process())
            throw std::runtime_error("inherited delivery batch copy");
        const auto creator=database_native_process();
        auto copy=std::shared_ptr<carried_work>(new carried_work(creator),carried_work::destroy);
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery batch allocation");
        copy->original=other.owned->original;
        copy->callbacks.reserve(other.owned->callbacks.size());
        if(creator!=database_native_process())throw std::runtime_error("inherited delivery batch reserve");
        // Preserve the old vector<function> target's opaque copy/value/order
        // behavior when a supported scheduler copies its queued std::function.
        // Never invoke a shared mutable user callback as an allocation shortcut.
        for(const auto& callback:other.owned->callbacks) {
            copy->callbacks.push_back(callback);
            if(creator!=database_native_process())throw std::runtime_error("inherited delivery callback copy");
        }
        copy->batch_adopted.store(other.owned->batch_adopted.load(std::memory_order_acquire),std::memory_order_release);
        owned=std::move(copy);
    }
    void operator()() {
        if(!owned||owned->creator!=database_native_process())return;
        delivery_frame frame(owned->original);
        observer_delivery_store::delivery_scope no_self_wait(owned->original->credit_);
        for(auto& callback:owned->callbacks){
            callback();
            if(owned->creator!=database_native_process())return;
        }
    }
};
struct observer_delivery_original::prepared_batch {
    const uint64_t creator;
    std::shared_ptr<carried_work> owned;
    std::function<void()> ready;
    explicit prepared_batch(uint64_t value):creator(value){}
    static void destroy(prepared_batch* value)noexcept {
        if(value&&value->creator==database_native_process())delete value;
    }
};
observer_delivery_original::observer_delivery_original(uint64_t creator,observer_delivery_store::lease credit,observer_delivery_meter_budget budget,std::weak_ptr<const void> input,bool explicit_envelope,
    std::shared_ptr<observer_delivery_workspace> workspace)
    :creator_(creator),credit_(std::move(credit)),budget_(budget),token_([] {
        // Identity DATA only, never SQL permission. Complete before real BEGIN;
        // a later original never intentionally resets/reuses a durable ledger
        // token. The runtime also proves the exact current Core original.
        std::array<unsigned char,32> token{};sqlite3_randomness(int(token.size()),token.data());return token;
     }()),input_identity_(std::move(input)),explicit_envelope_(explicit_envelope),workspace_(std::move(workspace)){}
observer_delivery_original::begin_offer::begin_offer(database& writer,observer_delivery_store::lease credit,observer_delivery_meter_budget budget)
    :begin_offer(writer,std::move(credit),budget,{}){}
observer_delivery_original::begin_offer::begin_offer(database& writer,observer_delivery_store::lease credit,observer_delivery_meter_budget budget,std::weak_ptr<const void> input,bool explicit_envelope,
    std::shared_ptr<observer_delivery_workspace> workspace)
    :creator_(database_native_process()),database_(&writer),original_([&]{
        if(!credit.current_for(writer))throw std::invalid_argument("delivery offer belongs to another physical store");
        return observer_delivery_original::create(std::move(credit),budget,std::move(input),explicit_envelope,std::move(workspace));
    }()),previous_(current_) {
    if(creator_!=database_native_process())throw std::runtime_error("inherited delivery BEGIN offer");
    for(auto* prior=current_;prior;prior=prior->previous_)
        if(prior->creator_!=creator_||prior->database_==database_)
            throw std::runtime_error("delivery BEGIN offer is inherited or already active for this writer");
    current_=this;
}
observer_delivery_original::begin_offer::begin_offer(database& writer,const observer_delivery_plan& plan)
    :begin_offer(writer,plan.take_credit(),plan.declared_budget(),plan.input_identity(),false,plan.workspace()){}
observer_delivery_original::begin_offer::~begin_offer()noexcept {
    // Only the POD TLS shell changes in a child. Its DATA original uses the
    // creator-fenced deleter; no inherited credit is returned there.
    current_=previous_;
}
std::shared_ptr<const observer_delivery_original> observer_delivery_original::observe_offer(database& writer) {
    for(auto* active=begin_offer::current_;active;active=active->previous_) {
        if(active->creator_!=database_native_process())throw std::runtime_error("inherited delivery BEGIN identity observation");
        if(active->database_==&writer)return active->original_; // immutable DATA; no claimed_ CAS
    }
    return {};
}
std::shared_ptr<const observer_delivery_original> observer_delivery_original::offered(database& writer) {
    for(auto* active=begin_offer::current_;active;active=active->previous_) {
        if(active->creator_!=database_native_process())throw std::runtime_error("inherited delivery BEGIN offer");
        if(active->database_!=&writer)continue;
        bool unused=false;
        if(!active->original_->claimed_.compare_exchange_strong(unused,true,std::memory_order_acq_rel))
            throw std::runtime_error("delivery offer already belongs to its original BEGIN");
        return active->original_;
    }
    return {};
}
std::shared_ptr<const observer_delivery_original> observer_delivery_original::create(observer_delivery_store::lease credit,observer_delivery_meter_budget budget,std::weak_ptr<const void> input,bool explicit_envelope,
    std::shared_ptr<observer_delivery_workspace> workspace) {
    if(!credit.valid()||credit.charge()!=observer_delivery_meter::admission_charge(budget))
        throw std::invalid_argument("delivery original requires exact admitted declaration credit");
    const auto creator=database_native_process();
    if(!workspace)workspace=observer_delivery_workspace::create(creator,budget.workspace_bytes);
    if(!workspace->current(budget.workspace_bytes))throw std::invalid_argument("delivery original workspace differs from its actual plan");
    auto original=std::shared_ptr<const observer_delivery_original>(new observer_delivery_original(creator,std::move(credit),budget,std::move(input),explicit_envelope,std::move(workspace)),destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery original allocation");
    return original;
}
void observer_delivery_original::claim_workspace(uint64_t bytes)const {
    if(creator_!=database_native_process()||!explicit_envelope_||!credit_.valid()||
       !began_.load(std::memory_order_acquire)||ended_.load(std::memory_order_acquire))
        throw std::runtime_error("delivery subplan requires its live admitted BEGIN envelope");
    if(!workspace_||!workspace_->current(budget_.workspace_bytes))throw std::runtime_error("delivery original workspace unavailable");
    if(bytes>workspace_->remaining())throw std::length_error("delivery subplans exceed the original workspace envelope");
    workspace_->claim(bytes);
}
uint64_t observer_delivery_original::remaining_workspace()const {
    if(creator_!=database_native_process()||!explicit_envelope_||!credit_.valid())
        throw std::runtime_error("delivery original workspace unavailable");
    if(!workspace_||!workspace_->current(budget_.workspace_bytes))throw std::runtime_error("delivery original workspace unavailable");
    return workspace_->remaining();
}
void observer_delivery_original::claim_begin_workspace(uint64_t bytes)const {
    if(creator_!=database_native_process()||!credit_.valid()||attempted_.load(std::memory_order_acquire)||
       began_.load(std::memory_order_acquire)||!workspace_||!workspace_->current(budget_.workspace_bytes))
        throw std::runtime_error("BEGIN DATA requires its actual unissued original workspace");
    workspace_->claim(bytes);
}
void observer_delivery_original::claim_read_workspace(uint64_t bytes)const {
    if(creator_!=database_native_process()||!credit_.valid()||!began_.load(std::memory_order_acquire)||
       ended_.load(std::memory_order_acquire)||!workspace_||!workspace_->current(budget_.workspace_bytes))
        throw std::runtime_error("read debit requires its actual live original workspace");
    // Implicit originals share their actual plan's existing tally; explicit
    // originals share it with all earlier/later subplans. No second allowance.
    workspace_->claim(bytes);
}
void observer_delivery_original::destroy(const observer_delivery_original* value)noexcept {
    if(!value||value->creator_!=database_native_process())return;
    // A lost original/capture cannot turn last shared ownership into success.
    // Keep the bounded charged DATA intact; no Native original is retained.
    if(!value->owner_storage_ended_.load(std::memory_order_acquire)&&value->attempted_.load(std::memory_order_acquire)&&
       !value->began_.load(std::memory_order_acquire)&&!value->unissued_.load(std::memory_order_acquire))return;
    if(!value->owner_storage_ended_.load(std::memory_order_acquire)&&value->began_.load(std::memory_order_acquire)&&
       (!value->ended_.load(std::memory_order_acquire)||!value->captured_.load(std::memory_order_acquire)))return;
    delete value;
}
void observer_delivery_original::attempting()const noexcept {
    if(creator_==database_native_process()&&!began_.load(std::memory_order_acquire)){
        unissued_.store(false,std::memory_order_release);attempted_.store(true,std::memory_order_release);
    }
}
void observer_delivery_original::unissued()const noexcept {
    if(creator_==database_native_process()&&attempted_.load(std::memory_order_acquire)&&!began_.load(std::memory_order_acquire))
        unissued_.store(true,std::memory_order_release); // Only actual failed BEGIN + Native autocommit proof.
}
void observer_delivery_original::began()const noexcept {
    if(creator_==database_native_process())began_.store(true,std::memory_order_release);
}
void observer_delivery_original::ended()const noexcept {
    if(creator_==database_native_process()&&began_.load(std::memory_order_acquire))
        ended_.store(true,std::memory_order_release);
}
void observer_delivery_original::captured()const noexcept {
    if(creator_==database_native_process()&&ended_.load(std::memory_order_acquire))
        captured_.store(true,std::memory_order_release);
}
void observer_delivery_original::owner_storage_ended()const noexcept {
    // Only the actual checked-close/final-database-member producer calls this.
    // It is resource ending, never a SQL COMMIT/capture/refund declaration.
    // Any surviving real buffer/checkpoint/batch holder still keeps credit.
    if(creator_==database_native_process())owner_storage_ended_.store(true,std::memory_order_release);
}
std::shared_ptr<const observer_delivery_original> observer_delivery_original::current()noexcept {
    auto* const actual=current_delivery;
    return actual&&actual->creator==database_native_process()?actual->original:nullptr;
}
observer_delivery_original::batch_preparation observer_delivery_original::prepare_current_batch() {
    auto original=current();if(!original)return {};
    const auto creator=database_native_process();
    auto prepared=std::shared_ptr<prepared_batch>(new prepared_batch(creator),prepared_batch::destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery batch preparation");
    prepared->owned=std::shared_ptr<carried_work>(new carried_work(creator),carried_work::destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery batch custody");
    prepared->owned->original=std::move(original);
    prepared->ready=batch_target(prepared->owned);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery batch callable");
    // Both type-erased wrappers allocate before the registry copies any user
    // callback. Every owning target will subsequently move directly into the
    // preallocated creator-fenced heap. Copies of this DATA adapter cannot
    // adopt a second transaction batch.
    return [prepared=std::move(prepared)]
        (std::vector<std::function<void()>>&& callbacks)mutable -> std::function<void()> {
        if(!prepared||prepared->creator!=database_native_process()||!prepared->ready||callbacks.empty())
            throw std::runtime_error("delivery batch preparation unavailable");
        const auto& owned=prepared->owned;
        bool unused=false;
        if(!owned->batch_adopted.compare_exchange_strong(unused,true,std::memory_order_acq_rel))
            throw std::runtime_error("delivery batch preparation already consumed");
        owned->callbacks=std::move(callbacks); // vector move, no allocation/callback copy
        if(owned->creator!=database_native_process())throw std::runtime_error("inherited delivery batch move");
        return std::move(prepared->ready);
    };
}
std::function<void()> observer_delivery_original::carry(
    std::shared_ptr<const observer_delivery_original> original,std::function<void()>&& work) {
    if(!original)return std::move(work);
    auto prepared=prepare(std::move(original));prepared.adopt(std::move(work));return prepared.finish();
}
observer_delivery_original::carrier::carrier(std::shared_ptr<carried_work> owned,std::function<void()>&& ready)
    :owned_(std::move(owned)),ready_(std::move(ready)){}
observer_delivery_original::carrier observer_delivery_original::prepare(std::shared_ptr<const observer_delivery_original> original) {
    if(!original)throw std::invalid_argument("delivery carrier requires actual original");
    const auto creator=database_native_process();
    if(original->creator_!=creator)throw std::runtime_error("inherited delivery original");
    auto owned=std::shared_ptr<carried_work>(new carried_work(creator),carried_work::destroy);
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery carry allocation");
    // Complete EVERY wrapper allocation while the actual Native capture is
    // still in its caller's existing creator-fenced custody. Allocation
    // failure under Native must not destroy an already-moved capture here.
    std::function<void()> result=[owned]{
        if(owned->creator!=database_native_process())return;
        delivery_frame frame(owned->original);
        observer_delivery_store::delivery_scope no_self_wait(owned->original->credit_);
        if(owned->batch_adopted.load(std::memory_order_acquire)) {
            for(auto& callback:owned->callbacks){
                callback();
                if(owned->creator!=database_native_process())return;
            }
        } else owned->work();
    };
    if(creator!=database_native_process())throw std::runtime_error("inherited delivery carry callable allocation");
    owned->original=std::move(original);
    return carrier(std::move(owned),std::move(result));
}
void observer_delivery_original::carrier::adopt(std::function<void()>&& work) {
    if(!owned_||owned_->creator!=database_native_process()||owned_->work||!ready_||!work)
        throw std::runtime_error("delivery carrier original unavailable or consumed");
    owned_->work=std::move(work);
    if(owned_->creator!=database_native_process())throw std::runtime_error("inherited delivery capture move");
}
std::function<void()> observer_delivery_original::carrier::finish() {
    if(!owned_||owned_->creator!=database_native_process()||!owned_->work||!ready_)
        throw std::runtime_error("delivery carrier lacks actual capture");
    std::function<void()> result=std::move(ready_);
    owned_.reset(); // result already holds the actual heap original.
    return result;
}
bool observer_delivery_original::retire_target(
    const std::shared_ptr<const observer_delivery_original>& original,std::function<void()>& target)noexcept {
    if(!original||original->creator_!=database_native_process())return false;
    try {
        delivery_frame frame(original);
        observer_delivery_store::delivery_scope no_self_wait(original->credit_);
        target={};
        return original->creator_==database_native_process();
    }catch(...){return false;} // Retain actual target/credit; never terminate or refund.
}
std::function<std::function<void()>(std::vector<std::function<void()>>&&)>
prepare_observer_delivery_batch() {
    return observer_delivery_original::prepare_current_batch();
}
}
