#include "ordinary_managed_wss_binding.hpp"
#include "ordinary_managed_receiver_wss.hpp"
#include "ordinary_managed_product_route.hpp"
#include "ordinary_export.hpp"
#include "sync_callback_lifetime.hpp"
#include <lattice/lattice.hpp>
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <optional>
#include <type_traits>
#include <stdexcept>
#include <unistd.h>

namespace lattice::detail {
namespace {
const auto creator=static_cast<std::uint64_t>(::getpid());
std::atomic<std::size_t> count{0};
constexpr std::size_t scope_bound=256*260;
void require(bool b,const char* why){if(!b)throw db_error(why);}
void creator_check(){require(creator==static_cast<std::uint64_t>(::getpid()),"managed WSS inherited process");}
bool bounded(const std::string& s,std::size_t n){return !s.empty()&&s.size()<=n&&s.find('\0')==std::string::npos;}
}
struct ordinary_managed_wss_binding::reservation {
    reservation(){creator_check();auto used=count.load(std::memory_order_relaxed);
        do{require(used<64,"managed WSS binding capacity exhausted");}while(!count.compare_exchange_weak(used,used+1,std::memory_order_acq_rel));}
    ~reservation(){if(creator==static_cast<std::uint64_t>(::getpid()))count.fetch_sub(1,std::memory_order_acq_rel);}
};
struct ordinary_managed_wss_epoch final {
    struct retained {
        const std::uint64_t process=static_cast<std::uint64_t>(::getpid());
        std::shared_ptr<void> reservation;
        std::shared_ptr<std::atomic<bool>> binding_live;
        std::shared_ptr<ordinary_managed_product_credentials> credentials;
        std::shared_ptr<lattice_db> owner;
        std::shared_ptr<sync_callback_lifetime> lifetime;
        std::optional<platform_transport_callbacks> endpoint;
    };
    struct dispose_retained {
        void operator()(retained* capsule)const noexcept {
            if(!capsule||capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->endpoint.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->lifetime.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->owner.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->credentials.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->binding_live.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            capsule->reservation.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
            delete capsule;
        }
    };
    using custody=std::unique_ptr<retained,dispose_retained>;
    static custody retain(const std::shared_ptr<void>& charge,const std::shared_ptr<std::atomic<bool>>& active,
        const std::shared_ptr<ordinary_managed_product_credentials>& auth,const std::shared_ptr<lattice_db>& db,
        const std::shared_ptr<sync_callback_lifetime>& life,const platform_transport_callbacks& ep) {
        custody result(new retained);
        result->reservation=charge;result->binding_live=active;result->credentials=auth;
        result->owner=db;result->lifetime=life;
        // The actual callback value currently copies just its shared cell and
        // generation. Future throwing member copies must not silently expose
        // partially constructed callback contexts to default unwind.
        static_assert(std::is_nothrow_copy_constructible_v<platform_transport_callbacks>);
        result->endpoint.emplace(ep);
        return result;
    }
    custody held;
    const std::shared_ptr<void>& reservation;
    const std::shared_ptr<std::atomic<bool>>& binding_live;
    const std::shared_ptr<ordinary_managed_product_credentials>& credentials;
    const std::shared_ptr<lattice_db>& owner;
    const std::shared_ptr<sync_callback_lifetime>& lifetime;
    const platform_transport_callbacks& endpoint;
    const std::uint64_t lifecycle;
    std::atomic<bool> live{true};
    ordinary_managed_wss_epoch(const std::shared_ptr<void>& charge,const std::shared_ptr<std::atomic<bool>>& active,
        const std::shared_ptr<ordinary_managed_product_credentials>& auth,const std::shared_ptr<lattice_db>& db,
        const std::shared_ptr<sync_callback_lifetime>& life,const platform_transport_callbacks& ep,std::uint64_t n)
        :held(retain(charge,active,auth,db,life,ep)),
         reservation(held->reservation),binding_live(held->binding_live),credentials(held->credentials),owner(held->owner),lifetime(held->lifetime),endpoint(*held->endpoint),lifecycle(n){}
    ~ordinary_managed_wss_epoch()=default;
    bool current()const noexcept;
};
struct ordinary_managed_wss_binding::retained {
    const std::uint64_t process=static_cast<std::uint64_t>(::getpid());
    std::shared_ptr<reservation> charge;
    std::shared_ptr<ordinary_managed_product_credentials> credentials;
    std::shared_ptr<const ordinary_managed_profile> profile;
    std::shared_ptr<const ordinary_receiver_wss_request> receiver;
    std::shared_ptr<lattice_db> owner;
    std::shared_ptr<sync_callback_lifetime> lifetime;
    std::shared_ptr<std::atomic<bool>> live;
    std::mutex leaf;
    std::shared_ptr<ordinary_managed_wss_epoch> epoch;
    std::array<std::shared_ptr<ordinary_managed_wss_receipt>,16> receipts{};
};
void ordinary_managed_wss_binding::dispose_retained::operator()(retained* capsule)const noexcept {
    if(!capsule||capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    for(auto& receipt:capsule->receipts){receipt.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;}
    capsule->epoch.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->lifetime.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->owner.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->receiver.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->profile.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->credentials.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->live.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    capsule->charge.reset();if(capsule->process!=static_cast<std::uint64_t>(::getpid()))return;
    delete capsule;
}
bool ordinary_managed_wss_epoch::current()const noexcept {
    if(creator!=static_cast<std::uint64_t>(::getpid())||!live.load(std::memory_order_acquire)||
        !binding_live->load(std::memory_order_acquire)||!credentials->current())return false;
    try {return owner&&!owner->is_closed()&&lifetime&&lifetime->current(lifecycle)&&endpoint.current_system_tls_for_owner();
    }catch(...){return false;}
}
ordinary_managed_wss_receipt::ordinary_managed_wss_receipt(std::shared_ptr<ordinary_managed_wss_epoch> e,
    std::string ch,std::string sc,std::string src,std::string schema,std::string receipt,std::int64_t ep,std::int64_t c)
    :epoch_(std::move(e)),channel(std::move(ch)),scope(std::move(sc)),source(std::move(src)),schema_digest(std::move(schema)),receipt_digest(std::move(receipt)),source_epoch(ep),cut(c){}
bool ordinary_managed_wss_receipt::current()const noexcept {
    return creator==static_cast<std::uint64_t>(::getpid())&&!retired_.load(std::memory_order_acquire)&&epoch_&&epoch_->current();
}
ordinary_managed_wss_binding::ordinary_managed_wss_binding(const std::shared_ptr<reservation>& charge,const std::shared_ptr<ordinary_managed_product_credentials>& auth,
    const std::shared_ptr<const ordinary_managed_profile>& profile,const std::shared_ptr<lattice_db>& db,
    const std::shared_ptr<sync_callback_lifetime>& life,std::string url,std::string channel,std::shared_ptr<const ordinary_receiver_wss_request> receiver)
    :held_(new retained),reservation_(held_->charge),
     creator_(creator),credentials_(held_->credentials),profile_(held_->profile),receiver_(held_->receiver),owner_(held_->owner),lifetime_(held_->lifetime),
     url_(std::move(url)),channel_(std::move(channel)),live_(held_->live),leaf_(held_->leaf),epoch_(held_->epoch),receipts_(held_->receipts){
    // The full guard exists before retaining these owners or allocating live.
    held_->charge=charge;held_->credentials=auth;held_->profile=profile;held_->receiver=std::move(receiver);
    held_->owner=db;held_->lifetime=life;
    held_->live=std::make_shared<std::atomic<bool>>(true);
}
std::shared_ptr<ordinary_managed_wss_binding> ordinary_managed_wss_binding::attach(
    const std::shared_ptr<ordinary_managed_product_credentials>& credentials,const std::shared_ptr<lattice_db>& owner,
    const std::shared_ptr<sync_callback_lifetime>& life,const std::string& url,const std::string& token,const std::string& channel) {
    if(!credentials)return {}; // Exact default path: no registry/token/parser work.
    creator_check();require(credentials->current()&&owner&&life&&bounded(url,4096)&&bounded(channel,4096),"managed WSS configured owner unavailable");
    require(token.size()<=16384&&picosha2::hash256_hex_string(token)==credentials->token_digest_,"managed WSS credential differs from directory session");
    require(url.starts_with("wss://")&&url.find('#')==std::string::npos&&url.find('@')==std::string::npos,"managed WSS endpoint refused");
    auto charge=std::make_shared<reservation>();
    const auto receiver=ordinary_export_access::receiver_wss_request(*owner);
    if(receiver)require(ordinary_receiver_wss_access::binding_matches(receiver,credentials,owner,url,token,channel),"receiver actual fixed directory/child association differs");
    const auto profile=receiver?std::shared_ptr<const ordinary_managed_profile>{}:ordinary_export_access::managed_wss_profile(*owner);
    auto value=std::shared_ptr<ordinary_managed_wss_binding>(new ordinary_managed_wss_binding(std::move(charge),credentials,profile,owner,life,url,channel,receiver),
        [](ordinary_managed_wss_binding* p){if(p->creator_==static_cast<std::uint64_t>(::getpid()))delete p;});
    std::array<std::shared_ptr<ordinary_managed_wss_binding>,64> retired{};
    {std::unique_lock lock(credentials->wss_leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS registration contention");
        std::size_t free=64;
        for(std::size_t i=0;i<64;++i){retired[i]=credentials->wss_[i].lock();const auto& prior=retired[i];
            if(!prior){if(free==64)free=i;continue;}
            if(receiver?prior->receiver_==receiver:(!prior->receiver_&&prior->profile_==profile)){prior->retire();credentials->wss_[i].reset();if(free==64)free=i;}}
        require(free<64,"managed WSS credential binding capacity exhausted");credentials->wss_[free]=value;
    } // Former bindings/callback captures destroyed outside this leaf.
    return value;
}
ordinary_managed_wss_binding::~ordinary_managed_wss_binding(){
    retire(); // held_'s custom deleter also handles constructor failure.
}
void ordinary_managed_wss_binding::retire()noexcept {if(creator_==static_cast<std::uint64_t>(::getpid()))live_->store(false,std::memory_order_release);}
bool ordinary_managed_wss_binding::configuration_current()const noexcept {
    return creator_==static_cast<std::uint64_t>(::getpid())&&live_->load(std::memory_order_acquire)&&credentials_->current()&&(!receiver_||receiver_->current());
}
bool ordinary_managed_wss_binding::has_receiver_history_expectation()const noexcept {
#ifndef __EMSCRIPTEN__
    return configuration_current()&&ordinary_receiver_wss_access::has_current_history_expectation(receiver_);
#else
    return false;
#endif
}
const ordinary_receiver_history_expectation& ordinary_managed_wss_binding::expected_history_relation()const {
    require(configuration_current()&&receiver_,"managed history relation needs the actual current receive-only issuer");
#ifndef __EMSCRIPTEN__
    return ordinary_receiver_wss_access::expected_history_relation(receiver_);
#else
    throw db_error("managed history receiver unsupported on this platform");
#endif
}
void ordinary_managed_wss_binding::opened(const platform_transport_callbacks& endpoint,std::uint64_t lifecycle) {
    creator_check();require(live_->load()&&credentials_->current(),"managed WSS binding retired");
    auto next=std::shared_ptr<ordinary_managed_wss_epoch>(new ordinary_managed_wss_epoch(reservation_,live_,credentials_,owner_,lifetime_,endpoint,lifecycle),
        [](ordinary_managed_wss_epoch* p){if(creator==static_cast<std::uint64_t>(::getpid()))delete p;});
    require(next->current(),"managed WSS actual authenticated physical attempt absent");
    std::array<std::shared_ptr<ordinary_managed_wss_receipt>,16> retired;std::shared_ptr<ordinary_managed_wss_epoch> prior;
    {std::unique_lock lock(leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS opening contention");
        prior=std::move(epoch_);if(prior)prior->live=false;epoch_=std::move(next);
        retired.swap(receipts_);for(const auto& x:retired)if(x)x->retired_=true;
    }
}
void ordinary_managed_wss_binding::closed(const platform_transport_callbacks& endpoint,std::uint64_t lifecycle)noexcept {
    if(creator_!=static_cast<std::uint64_t>(::getpid()))return;
    std::unique_lock lock(leaf_,std::try_to_lock);
    if(!lock.owns_lock()){retire();return;} // Never leave old authority live on uncertain closure.
    if(epoch_&&epoch_->lifecycle==lifecycle&&epoch_->endpoint.matches(endpoint))epoch_->live=false;
}
void ordinary_managed_wss_binding::accepted(const platform_transport_callbacks& endpoint,std::uint64_t lifecycle,
    ordinary_managed_wss_acceptance&& facts) {
    creator_check();require(!receiver_,"receive-only history role cannot publish scalar receipt authority");require(bounded(facts.scope,scope_bound)&&bounded(facts.source,36)&&facts.schema_digest.size()==64&&
        facts.receipt_digest.size()==64&&facts.epoch>0&&facts.cut>=0,"managed WSS accepted metadata bound");
    std::shared_ptr<ordinary_managed_wss_epoch> actual;
    {std::unique_lock lock(leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS acceptance contention");actual=epoch_;}
    require(actual&&actual->current()&&actual->lifecycle==lifecycle&&actual->endpoint.matches(endpoint),"managed WSS accepted physical attempt retired");
    auto next=std::shared_ptr<ordinary_managed_wss_receipt>(new ordinary_managed_wss_receipt(actual,channel_,
        std::move(facts.scope),std::move(facts.source),std::move(facts.schema_digest),std::move(facts.receipt_digest),facts.epoch,facts.cut),
        [](ordinary_managed_wss_receipt* p){if(creator==static_cast<std::uint64_t>(::getpid()))delete p;});
    std::shared_ptr<ordinary_managed_wss_receipt> prior;
    {std::unique_lock lock(leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS acceptance publication contention");
        require(epoch_==actual&&actual->live.load(std::memory_order_acquire),"managed WSS accepted attempt replaced");
        std::size_t slot=16;for(std::size_t i=0;i<16;++i){if(receipts_[i]&&receipts_[i]->scope==next->scope){slot=i;break;}if(!receipts_[i]&&slot==16)slot=i;}
        require(slot<16,"managed WSS accepted scope bound");
        // At most16 scope receipts, each scope<=256*260 and fixed64-byte
        // digests. No row/envelope/payload is copied or required to be nonempty.
        prior=std::move(receipts_[slot]);if(prior)prior->retired_=true;receipts_[slot]=std::move(next);
    }
}
std::shared_ptr<const ordinary_managed_wss_receipt> ordinary_managed_wss_binding::receipt(const std::string& scope) {
    auto value=try_receipt(scope);require(bool(value),"managed WSS current accepted cut unavailable");return value;
}
std::shared_ptr<const ordinary_managed_wss_receipt> ordinary_managed_wss_binding::try_receipt(const std::string& scope) {
    creator_check();std::shared_ptr<const ordinary_managed_wss_receipt> value;
    {std::unique_lock lock(leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS receipt contention");
        for(const auto& x:receipts_)if(x&&x->scope==scope){value=x;break;}}
    if(value&&!value->current())value.reset();return value;
}
std::shared_ptr<const ordinary_managed_wss_receipt> ordinary_managed_product_credentials::receipt(
    const ordinary_managed_profile& profile,const std::string& url,const std::string& scope)const {
    auto result=try_receipt(profile,url,scope);require(bool(result),"managed WSS actual configured owner differs");return result;
}
std::shared_ptr<const ordinary_managed_wss_receipt> ordinary_managed_product_credentials::try_receipt(
    const ordinary_managed_profile& profile,const std::string& url,const std::string& scope)const {
    creator_check();require(current(),"managed WSS credential/profile unavailable");
    std::array<std::shared_ptr<ordinary_managed_wss_binding>,64> candidates{};
    {std::unique_lock lock(wss_leaf_,std::try_to_lock);require(lock.owns_lock(),"managed WSS lookup contention");
        for(std::size_t i=0;i<64;++i)candidates[i]=wss_[i].lock();}
    std::shared_ptr<const ordinary_managed_wss_receipt> result;bool matched=false;
    for(const auto& x:candidates)if(x&&x->profile_.get()==&profile&&x->url_==url&&x->live_->load(std::memory_order_acquire)){
        require(!matched,"managed WSS owner association ambiguous");matched=true;result=x->try_receipt(scope);}
    return result;
}
}
