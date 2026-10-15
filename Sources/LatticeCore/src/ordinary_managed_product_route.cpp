#include "ordinary_managed_product_route.hpp"
#include "ordinary_managed_receiver_wss.hpp"
#include "ordinary_target_staging_io.hpp"
#include "ordinary_export.hpp"
#include "ordinary_projection_recipe.hpp"
#include "ordinary_row_recipe.hpp"
#include "ordinary_internal_schema.hpp"
#include "ordinary_managed_wss_binding.hpp"
#include "ordinary_managed_wss_owner.hpp"
#include "configured_recovery_connection.hpp"
#include "configured_platform.hpp"
#include "ordinary_group_target_receiving_owner.hpp"
#include "ordinary_group_history_codec.hpp"
#include "configured_retirement.hpp"
#include "configured_attempt_custody.hpp"
#include "sync_callback_lifetime.hpp"
#include <condition_variable>
#include <lattice/lattice.hpp>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <nlohmann/json.hpp>
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <type_traits>
#include <unistd.h>

namespace lattice::detail {
namespace {
using clock=std::chrono::steady_clock;
constexpr std::size_t role_cap=64, group_body_cap=1048576, small_body_cap=16384;
const auto registry_creator=static_cast<std::uint64_t>(::getpid());
std::mutex registry_leaf;
struct slot {std::uint64_t cookie=0;std::weak_ptr<ordinary_managed_product_state> owner;};
std::array<slot,role_cap> registry;
std::atomic<std::uint64_t> next_cookie{1};
std::atomic<std::size_t> occupied{0};
std::uint64_t creator() noexcept{return static_cast<std::uint64_t>(::getpid());}
void require(bool value,const char* message){if(!value)throw std::runtime_error(message);}
void creator_check(){require(creator()==registry_creator,"managed product issuer inherited process");}
std::uint64_t reserve_cookie() {
    auto value=next_cookie.load(std::memory_order_relaxed);
    do{require(value>0&&value<INT64_MAX,"managed request identity exhausted");}
    while(!next_cookie.compare_exchange_weak(value,value+1,std::memory_order_relaxed));
    // Saturates at the refused sentinel instead of eventually wrapping and
    // reusing a completed request's private completion identity.
    return value;
}
struct role_reservation {
    bool held=false;
    role_reservation(){
        require(creator()==registry_creator,"managed product issuer inherited process");
        auto n=occupied.load(std::memory_order_relaxed);
        do{require(n<role_cap,"managed product role capacity exhausted");}
        while(!occupied.compare_exchange_weak(n,n+1,std::memory_order_acq_rel));
        held=true;
    }
    role_reservation(role_reservation&& x)noexcept:held(std::exchange(x.held,false)){}
    ~role_reservation(){if(held&&creator()==registry_creator)occupied.fetch_sub(1,std::memory_order_acq_rel);}
};
std::string uuid(const nlohmann::json& x){
    require(x.is_string(),"managed directory UUID type");auto s=x.get<std::string>();
    require(s.size()==36,"managed directory UUID length");
    for(std::size_t i=0;i<s.size();++i){auto& c=s[i];
        if(i==8||i==13||i==18||i==23){require(c=='-',"managed directory UUID delimiter");continue;}
        if(c>='A'&&c<='F')c=static_cast<char>(c+('a'-'A'));
        require((c>='0'&&c<='9')||(c>='a'&&c<='f'),"managed directory UUID digit");
    }
    return s;
}
// Fixed body, nesting and scalar limits precede DOM allocation. The callback
// additionally rejects duplicate object keys; a later duplicate cannot replace
// a principal or permission already observed by another decoder.
nlohmann::json parse(const void* data,std::size_t n,std::size_t cap){
    require(data&&n&&n<=cap,"managed directory body limit");
    const auto* p=static_cast<const char*>(data);
    std::size_t depth=0,nodes=0,scalar=0;bool quoted=false,escape=false;
    for(std::size_t i=0;i<n;++i){const auto c=p[i];
        if(quoted){require(++scalar<=65536,"managed directory scalar limit");
            if(escape)escape=false;else if(c=='\\')escape=true;else if(c=='"')quoted=false;continue;}
        if(c=='"'){quoted=true;scalar=0;require(++nodes<=16384,"managed directory node limit");}
        else if(c=='{'||c=='['){require(++depth<=16,"managed directory depth limit");require(++nodes<=16384,"managed directory node limit");}
        else if(c=='}'||c==']'){require(depth>0,"managed directory unmatched close");--depth;}
        else if(c==','||c==':')require(++nodes<=16384,"managed directory value bound");
    }
    require(!quoted&&depth==0,"managed directory truncated JSON");
    std::vector<std::unordered_set<std::string>> keys;
    return nlohmann::json::parse(p,p+n,[&](int,nlohmann::json::parse_event_t event,nlohmann::json& value){
        using event_t=nlohmann::json::parse_event_t;
        if(event==event_t::object_start)keys.emplace_back();
        else if(event==event_t::key){require(!keys.empty()&&keys.back().insert(value.get<std::string>()).second,"managed directory duplicate key");}
        else if(event==event_t::object_end){require(!keys.empty(),"managed directory key stack");keys.pop_back();}
        return true;
    });
}
}

struct ordinary_managed_product_state final {
    // First member is destroyed last, including all owners/response/policy
    // captures. A cancelled or failed wrapper cannot prematurely recycle it.
    struct retained {
        const std::uint64_t process=creator();
        std::optional<role_reservation> reservation;
        std::shared_ptr<ordinary_managed_product_credentials> credentials;
        std::optional<ordinary_managed_profile_pair> pair;
        std::mutex leaf;
    };
    struct dispose_retained {
        void operator()(retained* capsule)const noexcept {
            if(!capsule||capsule->process!=creator())return;
            // Mandatory pair003 handles each nested actual owner boundary.
            capsule->pair.reset();if(capsule->process!=creator())return;
            capsule->credentials.reset();if(capsule->process!=creator())return;
            capsule->reservation.reset();if(capsule->process!=creator())return;
            delete capsule;
        }
    };
    using custody=std::unique_ptr<retained,dispose_retained>;
    static custody retain(role_reservation&& reservation,
        std::shared_ptr<ordinary_managed_product_credentials>&& credentials,ordinary_managed_profile_pair&& pair) {
        custody result(new retained); // empty leaf/owners before taking custody
        static_assert(std::is_nothrow_move_constructible_v<role_reservation>);
        static_assert(std::is_nothrow_move_constructible_v<ordinary_managed_profile_pair>);
        result->reservation.emplace(std::move(reservation));
        result->credentials=std::move(credentials);
        result->pair.emplace(std::move(pair));
        return result;
    }
    custody held;
    const std::uint64_t process=creator();
    const std::uint64_t cookie;
    const clock::time_point deadline;
    const std::shared_ptr<ordinary_managed_product_credentials>& credentials;
    ordinary_managed_profile_pair& pair;
    const managed_engram_direction direction;
    const std::string group;
    std::atomic<bool> cancelled{false};
    std::mutex& leaf;
    bool received=false,publishing=false;
    mutable std::weak_ptr<configured_recovery_connection> wss_owner;
    std::string principal;
    clock::time_point received_at{};
    std::shared_ptr<const managed_engram_policy> policy;
    ordinary_managed_product_state(role_reservation&& r,std::uint64_t c,clock::time_point end,
        std::shared_ptr<ordinary_managed_product_credentials>&& auth,ordinary_managed_profile_pair&& p,
        managed_engram_direction d,std::string g)
        :held(retain(std::move(r),std::move(auth),std::move(p))),cookie(c),deadline(end),
         credentials(held->credentials),pair(*held->pair),direction(d),group(std::move(g)),leaf(held->leaf){}
    ~ordinary_managed_product_state()=default;
    std::shared_ptr<const ordinary_managed_wss_receipt> wss_receipt(
        const ordinary_managed_profile& profile,const std::string& scope)const {
        std::string url="wss://"+credentials->endpoint_.substr(8)+"/sync";
        if(!group.empty()){auto spelling=group;for(auto& c:spelling)if(c>='a'&&c<='f')c=static_cast<char>(c-('a'-'A'));url+="/group/"+spelling;}
        return credentials->receipt(profile,url,scope);
    }
    std::shared_ptr<const ordinary_managed_wss_receipt> try_wss_receipt(
        const ordinary_managed_profile& profile,const std::string& scope)const {
        std::string url="wss://"+credentials->endpoint_.substr(8)+"/sync";
        if(!group.empty()){auto spelling=group;for(auto& c:spelling)if(c>='a'&&c<='f')c=static_cast<char>(c-('a'-'A'));url+="/group/"+spelling;}
        return credentials->try_receipt(profile,url,scope);
    }
    bool live()const noexcept {
        return process==creator()&&credentials&&credentials->current()&&!cancelled.load(std::memory_order_acquire);
    }
};
ordinary_managed_product_credentials::ordinary_managed_product_credentials(std::string endpoint,std::string token)
    :creator_(creator()),endpoint_(std::move(endpoint)),token_digest_(picosha2::hash256_hex_string(token)) {
    require(!token.empty()&&token.size()<=16384&&token.find_first_of("\r\n") == std::string::npos&&token.find('\0')==std::string::npos,"managed credential token bound");
    require(creator_==registry_creator&&endpoint_.starts_with("https://")&&endpoint_.size()<=4096&&
        endpoint_.find('\0')==std::string::npos,"managed product endpoint unavailable");
}
bool ordinary_managed_product_credentials::current()const noexcept{return creator_==creator()&&live_.load(std::memory_order_acquire);}
void ordinary_managed_product_credentials::revoke()noexcept{
    if(creator_!=creator())return;
    live_.store(false,std::memory_order_release);
    #ifndef __EMSCRIPTEN__
    std::array<std::shared_ptr<configured_recovery_connection>,64> owners;
    std::array<std::shared_ptr<ordinary_group_target_receiving_owner>,64> group_owners;
    {std::lock_guard lock(wss_leaf_);for(std::size_t i=0;i<owners.size();++i){owners[i]=wss_owners_[i].lock();group_owners[i]=group_receiving_owners_[i].lock();}}
    for(const auto& owner:owners)if(owner)owner->request_close();
    for(const auto& owner:group_owners)if(owner)owner->request_close();
    #endif
}
bool ordinary_managed_product_credentials::bind_principal(const std::string& principal) {
    if(!current())return false;
    bool changed=false;
    {std::lock_guard lock(principal_leaf_);
        if(!current())return false;
        if(principal_.empty())principal_=principal;
        changed=principal_!=principal;
    }
    if(changed){revoke();return false;}
    return true;
}

std::shared_ptr<ordinary_managed_product_state> ordinary_managed_ipc_access::begin(
    std::shared_ptr<lattice_db> hub,std::shared_ptr<lattice_db> spoke,
    std::shared_ptr<ordinary_managed_product_credentials> credentials,std::uint8_t raw,const std::string& requested_group) {
    const auto end=clock::now()+std::chrono::seconds(30);
    role_reservation reservation; // BEFORE actual owner/profile retention.
    require(credentials&&credentials->current(),"managed credential session closed");
    require(raw>=1&&raw<=4,"managed product direction unavailable");
    const auto direction=static_cast<managed_engram_direction>(raw);
    const bool group_route=raw>=3;
    std::string group;
    if(group_route){group=uuid(nlohmann::json(requested_group));require(group==requested_group,"managed group selector noncanonical");}
    else require(requested_group.empty(),"managed personal route has group selector");
    auto pair=ordinary_export_access::retain_managed_pair(std::move(hub),std::move(spoke),end);
    require(pair.source_member()=="memory.sqlite","managed product hub is not actual primary member");
    const auto expected=group_route?"sync/group-"+group+".sqlite":"sync/memory-synced.sqlite";
    // The inventory preserves UUID spelling from the real installed product.
    // Normalize only the UUID segment, never a path or decoded peer identity.
    auto member=pair.recipient_member();
    if(group_route&&member.starts_with("sync/group-")&&member.ends_with(".sqlite"))
        member="sync/group-"+uuid(nlohmann::json(member.substr(11,member.size()-18)))+".sqlite";
    require(member==expected,"managed product recipient is not selected actual member");
    const auto id=reserve_cookie();
    auto state=std::shared_ptr<ordinary_managed_product_state>(new ordinary_managed_product_state(
        std::move(reservation),id,end,std::move(credentials),std::move(pair),direction,std::move(group)),
        [](ordinary_managed_product_state* p){if(p->process==creator())delete p;});
    {
        std::lock_guard lock(registry_leaf);
        auto at=std::find_if(registry.begin(),registry.end(),[](const auto& x){return x.owner.expired();});
        require(at!=registry.end(),"managed request registry exhausted");at->cookie=id;at->owner=state;
    }
    return state;
}

// Definition deliberately confined to this translation unit. The only caller
// of receive is the hidden C completion used by the fixed SDK HTTPS transport.
class managed_directory_completion final {
public:
    static std::shared_ptr<ordinary_managed_product_state> find(std::uint64_t cookie){
        if(creator()!=registry_creator||!cookie)return {};
        std::shared_ptr<ordinary_managed_product_state> out;
        {std::lock_guard lock(registry_leaf);for(const auto& entry:registry)if(entry.cookie==cookie){out=entry.owner.lock();break;}}
        return out;
    }
    static bool receive(std::uint64_t cookie,const void* me,std::size_t me_size,
        const void* groups,std::size_t groups_size,const void* status,std::size_t status_size)noexcept {
        try{
            auto state=find(cookie);require(state&&state->live()&&clock::now()<state->deadline,"managed directory request closed");
            const auto who=parse(me,me_size,small_body_cap), memberships=parse(groups,groups_size,group_body_cap),
                       entitlement=parse(status,status_size,small_body_cap);
            require(who.is_object()&&who.contains("id"),"managed directory principal missing");
            const auto principal=uuid(who.at("id"));
            require(memberships.is_array()&&memberships.size()<=256,"managed directory membership bound");
            require(entitlement.is_object()&&entitlement.contains("status")&&entitlement.at("status").is_string(),"managed directory entitlement unavailable");
            const auto billing=entitlement.at("status").get<std::string>();
            require(billing=="active"||billing=="trialing"||(!state->group.empty()&&billing=="past_due"),"managed product sync entitlement refused");
            bool selected=state->group.empty();std::unordered_set<std::string> seen;
            for(const auto& entry:memberships){require(entry.is_object()&&entry.contains("id"),"managed directory membership shape");
                const auto id=uuid(entry.at("id"));require(seen.insert(id).second,"managed directory duplicate membership");
                if(id!=state->group)continue;
                require(entry.contains("myRole")&&entry.at("myRole").is_string(),"managed group role unavailable");
                const auto role=entry.at("myRole").get<std::string>();
                require(role=="owner"||role=="admin"||role=="member","managed group role unsupported");
                // A linked read grant must not silently become contribution
                // or author-retraction authority. Its separate route is debt.
                require(!entry.contains("viaLink")||entry.at("viaLink").is_null()||entry.at("viaLink")==false,"managed linked-only group needs its distinct read role");
                selected=true;
            }
            require(selected,"managed selected membership absent");
            require(state->credentials->bind_principal(principal),"managed credential principal changed");
            std::lock_guard lock(state->leaf);
            require(state->live()&&!state->received&&!state->publishing,"managed directory receipt duplicate or revoked");
            state->principal=principal;state->received_at=clock::now();state->received=true;return true;
        }catch(...){if(auto state=find(cookie))state->cancelled.store(true,std::memory_order_release);return false;}
    }
};

std::uint64_t ordinary_managed_ipc_access::cookie(const std::shared_ptr<ordinary_managed_product_state>& x)noexcept{return x&&x->process==creator()?x->cookie:0;}
void ordinary_managed_ipc_access::cancel(const std::shared_ptr<ordinary_managed_product_state>& x)noexcept{
    if(!x||x->process!=creator())return;x->cancelled.store(true,std::memory_order_release);
    #ifndef __EMSCRIPTEN__
    std::shared_ptr<configured_recovery_connection> owner;
    {std::lock_guard lock(x->leaf);owner=x->wss_owner.lock();}
    if(owner)owner->request_close();
    #endif
}
bool ordinary_managed_ipc_access::current(const std::shared_ptr<ordinary_managed_product_state>& x)noexcept{return x&&x->live();}
void require_managed_product_live(const std::shared_ptr<const ordinary_managed_product_state>& x){
    require(x&&x->live(),"managed product route revoked or foreign process");
    // Matches the actual daemon's five-minute directory refresh interval.
    // Missing refresh refuses; this is not instantaneous server revocation.
    require(x->received&&clock::now()-x->received_at<std::chrono::seconds(300),"managed product directory refresh required");
}
const managed_engram_policy& managed_product_policy(const std::shared_ptr<const ordinary_managed_product_state>& x){
    require_managed_product_live(x);require(bool(x->policy),"managed product policy unavailable");return *x->policy;
}
std::shared_ptr<const ordinary_managed_ipc_local_route> ordinary_managed_ipc_access::publish(
    const std::shared_ptr<ordinary_managed_product_state>& state) {
    require(state&&state->live()&&clock::now()<state->deadline,"managed route request expired");
    {std::lock_guard lock(state->leaf);require(state->received&&!state->publishing,"managed route has no actual response or was consumed");state->publishing=true;}
    try{
        state->pair.validate(state->deadline);
        auto policy=ordinary_export_access::capture_managed_engram_policy(state->pair,state->direction,state->principal,state->group,state->deadline);
        const auto identity=nlohmann::json::array({"managed-engram-route-v1",state->credentials->endpoint_,
            state->pair.generation(),state->pair.source_member(),state->pair.recipient_member(),
            state->principal,state->group,static_cast<int>(state->direction)}).dump();
        policy.route.route_key=picosha2::hash256_hex_string(identity);
        (void)policy.route.copy_charge();
        const bool reverse=state->direction==managed_engram_direction::personal_to_hub||state->direction==managed_engram_direction::group_to_hub;
        auto source=reverse?state->pair.recipient_owner():state->pair.source_owner();
        auto recipient=reverse?state->pair.source_owner():state->pair.recipient_owner();
        auto sp=reverse?state->pair.recipient_profile():state->pair.source_profile();
        auto rp=reverse?state->pair.source_profile():state->pair.recipient_profile();
        state->policy=std::make_shared<const managed_engram_policy>(std::move(policy));
        require_managed_product_live(state);state->pair.validate(state->deadline);
        return std::shared_ptr<const ordinary_managed_ipc_local_route>(new ordinary_managed_ipc_local_route(
            std::move(source),std::move(sp),std::move(recipient),std::move(rp),
            ordinary_managed_ipc_route_description(state->policy->route),state));
    }catch(...){state->cancelled.store(true,std::memory_order_release);throw;}
}
void ordinary_managed_ipc_access::validate_local_route(const ordinary_managed_ipc_local_route& route,
    clock::time_point deadline) {
    require(deadline!=clock::time_point::max()&&clock::now()<deadline,"managed IPC local admission deadline");
    route.validate_lifetime();
    route.product_authority()->pair.validate(deadline);
    route.validate_lifetime();
    require(clock::now()<deadline,"managed IPC local admission expired");
}
std::string ordinary_managed_ipc_access::principal(const ordinary_managed_ipc_local_route& route){route.validate_lifetime();return route.product_authority()->principal;}
bool ordinary_managed_ipc_access::route_current(const ordinary_managed_ipc_local_route& route)noexcept{try{route.validate_lifetime();return true;}catch(...){return false;}}
}

// Not declared in the Swift-visible C++ headers. Fixed private implementation
// linkage, not a general SDK response/verified-facts API.
extern "C" bool lattice_managed_directory_received_v1(std::uint64_t cookie,
    const void* me,std::size_t me_count,const void* groups,std::size_t groups_count,
    const void* status,std::size_t status_count) noexcept {
    return lattice::detail::managed_directory_completion::receive(cookie,me,me_count,groups,groups_count,status,status_count);
}
extern "C" void lattice_managed_directory_failed_v1(std::uint64_t cookie) noexcept {
    if(auto state=lattice::detail::managed_directory_completion::find(cookie))state->cancelled.store(true,std::memory_order_release);
}

namespace lattice::detail {
std::shared_ptr<const ordinary_managed_wss_receipt> managed_product_wss_receipt(
    const std::shared_ptr<const ordinary_managed_product_state>& state,
    const ordinary_managed_profile& profile,const std::string& scope) {
    require_managed_product_live(state);return state->wss_receipt(profile,scope);
}
}

namespace lattice::detail {
bool ordinary_managed_product_credentials::matches_configuration(const std::string& url,const std::string& token)const noexcept {
    try{return current()&&token.size()<=16384&&url.size()<=4096&&
        picosha2::hash256_hex_string(token)==token_digest_&&
        (url=="wss://"+endpoint_.substr(8)+"/sync"||url.starts_with("wss://"+endpoint_.substr(8)+"/sync/group/"));}
    catch(...){return false;}
}
std::shared_ptr<const ordinary_managed_wss_receipt> try_managed_product_wss_receipt(
    const std::shared_ptr<const ordinary_managed_product_state>& state,
    const ordinary_managed_profile& profile,const std::string& scope) {
    require_managed_product_live(state);return state->try_wss_receipt(profile,scope);
}
bool managed_wss_configuration_matches(const std::shared_ptr<ordinary_managed_product_credentials>& value,
    const std::string& url,const std::string& token)noexcept {
    return !value||value->matches_configuration(url,token);
}
}

#ifndef __EMSCRIPTEN__
namespace lattice::detail {
ordinary_managed_owner_release::ordinary_managed_owner_release()noexcept:creator_(creator()){}
bool ordinary_managed_owner_release::fields_destroyed()const noexcept {
    return creator_==creator()&&start_destroyed_.load(std::memory_order_acquire)&&
        admission_destroyed_.load(std::memory_order_acquire);
}
bool ordinary_managed_owner_release::start_fields_destroyed()const noexcept {
    return creator_==creator()&&start_destroyed_.load(std::memory_order_acquire);
}
void ordinary_managed_owner_release::record_start_cleanup_error(std::exception_ptr error)noexcept {
    if(creator_==creator()&&!start_cleanup_error_)start_cleanup_error_=std::move(error);
}
std::exception_ptr ordinary_managed_owner_release::start_cleanup_error()const noexcept {
    return start_fields_destroyed()?start_cleanup_error_:std::exception_ptr{};
}
int ordinary_managed_owner_release::lock_error()const noexcept {
    return creator_==creator()?lock_error_.load(std::memory_order_acquire):0;
}
void ordinary_managed_owner_release::record_lock_error(int value)noexcept {
    if(creator_==creator()&&value){int empty=0;lock_error_.compare_exchange_strong(empty,value,std::memory_order_release,std::memory_order_relaxed);}
}
bool ordinary_managed_owner_release::native_lock_complete()const noexcept {
    return creator_==creator()&&native_lock_complete_.load(std::memory_order_acquire);
}
void ordinary_managed_owner_release::bind_native(std::size_t slot,std::uint64_t serial)noexcept {
    if(creator_!=creator())return;
    native_slot_=slot;native_serial_=serial;
}
void ordinary_managed_owner_release::finish_native(int error)noexcept {
    if(creator_!=creator())return;
    record_lock_error(error);native_lock_complete_.store(true,std::memory_order_release);wake();
}
void ordinary_managed_owner_release::bind_wakeup(std::size_t slot,const configured_recovery_connection* owner)noexcept {
    // Actual create_impl retains both the caller admission and controller
    // before this one association, while initialization blocks dispatch.
    if(creator_!=creator())return;
    service_slot_=slot;owner_=owner;
}
void ordinary_managed_owner_release::wake()const noexcept {
    // Each real tail wakes after its own release-store. Requiring both flags
    // here can suppress both wakes when separate tails observe the other's
    // earlier value. Completion authority remains the collector's real AND
    // check, lock result, fence handoff and actual service-slot release.
    if(creator_==creator()&&native_slot_<64&&native_serial_)
        configured_recovery_connection::wake_managed_lock(native_slot_,native_serial_);
    if(creator_==creator()&&service_slot_<64&&owner_)
        configured_recovery_connection::wake_managed_owner_release(service_slot_,owner_);
    // No weak promotion: service wake only compares its retained native slot.
    // It never dereferences owner_, even if release raced this advisory wake.
}
void ordinary_managed_owner_release::finish_start()noexcept {
    if(creator_!=creator())return;
    start_destroyed_.store(true,std::memory_order_release);wake();
}
void ordinary_managed_owner_release::finish_admission()noexcept {
    if(creator_!=creator())return;
    admission_destroyed_.store(true,std::memory_order_release);wake();
}
void ordinary_managed_start_capture_tail::native_application_deleter::operator()(retained_application* value)const noexcept {
    if(!value||creator!=::lattice::detail::creator())return;
    value->owner.reset(); // Actual app destruction can fork; no later inherited delete.
    if(creator!=::lattice::detail::creator())return;
    delete value;
    // Foreign PID abandons the entire actual app reference graph untouched.
}
ordinary_managed_start_capture_tail::ordinary_managed_start_capture_tail()
    :completion_(completion_storage_.get<0>()),application_(nullptr,native_application_deleter{creator()}) {
        completion_storage_.get<0>().reset(new ordinary_managed_owner_release);
    }
void ordinary_managed_start_capture_tail::retain_application(const std::shared_ptr<lattice_db>& application){
    creator_check();
    require(application&&application->owns_creator_process(),"managed start app belongs to another process");
    require(!application_,"managed start already retained its actual app");
    application_.reset(new retained_application{application});
}
ordinary_managed_start_capture_tail::~ordinary_managed_start_capture_tail()noexcept {
    if(application_.get_deleter().creator!=creator())return;
    if(application_&&application_->owner->is_closed()){
        try{(void)application_->owner->retire_configured_listener_targets();}
        catch(...){if(application_.get_deleter().creator!=creator())return;completion_->record_start_cleanup_error(std::current_exception());}
        if(application_.get_deleter().creator!=creator())return;
    }
    // The genuine retained owner dies before the original actual start flag.
    // A published active controller still retains app via its real admission
    // and owns collector disposal; no-service terminal starts dispose above.
    application_.reset();
    if(!completion_storage_.in_creator_process())return;
    completion_->finish_start();
}
ordinary_managed_wss_admission::admission_capture_tail::~admission_capture_tail()noexcept {
    if(completion)completion->finish_admission();
}

ordinary_managed_wss_admission::ordinary_managed_wss_admission(
    std::shared_ptr<const ordinary_managed_product_state> product,std::shared_ptr<lattice_db> application,
    std::shared_ptr<const ordinary_managed_profile> profile,std::string endpoint,
    std::shared_ptr<const sync_policy_reservation> policy,ordinary_managed_lock_custody&& lock,clock::time_point deadline,
    const std::shared_ptr<ordinary_managed_owner_release>& completion)noexcept
    :released_(completion),creator_(creator()),product_(std::move(product)),application_(std::move(application)),
     profile_(std::move(profile)),endpoint_(std::move(endpoint)),policy_(std::move(policy)),lock_(std::move(lock)),initial_deadline_(deadline){lock_.issued_admission();}
ordinary_managed_wss_admission::~ordinary_managed_wss_admission()=default;
// The mechanical lease queues the real collector. The first member's tail
// witnesses fields only; native unlock/close completion is an independent gate.
bool ordinary_managed_wss_admission::current()const noexcept{
    if(creator_!=creator())return false;
    if(receiver_)return receiver_->current();
    try{require_managed_product_live(product_);return application_&&!application_->is_closed();}catch(...){return false;}
}
clock::time_point ordinary_managed_wss_admission::expires()const noexcept{
    if(creator_!=creator())return {};
    if(receiver_)return ordinary_receiver_wss_access::expires(receiver_);
    return product_->received_at+std::chrono::seconds(300);
}
bool ordinary_managed_wss_admission::matches(const std::shared_ptr<lattice_db>& child,const sync_config& config)const noexcept{
    if(receiver_)return ordinary_receiver_wss_access::matches_admission(receiver_,application_,child,config);
    try{return current()&&child&&child!=application_&&!child->is_closed()&&
        ordinary_export_access::managed_wss_profile(*child)==profile_&&
        config.ordinary_policy==ordinary_sync_policy::ordered_required&&config.recovery_source_expectation.empty()&&
        config.ordinary_model_profile==(product_->group.empty()?"engram-personal-v1":"engram-group-v1")&&
        !config.sync_filter&&config.websocket_url==endpoint_&&config.sync_id=="wss:"+endpoint_&&
        config.managed_product_credentials==product_->credentials&&
        product_->credentials->matches_configuration(config.websocket_url,config.authorization_token);}
    catch(...){return false;}
}
void ordinary_managed_ipc_access::validate_wss(const ordinary_managed_product_state& product,
    const std::shared_ptr<lattice_db>& application,clock::time_point deadline){
    require(product.live()&&clock::now()<deadline,"managed WSS operation expired");
    require(application==product.pair.recipient_owner(),"managed WSS application is not actual selected spoke");
    product.pair.validate(deadline);
    require(product.live()&&clock::now()<deadline,"managed WSS pair changed during validation");
}
void ordinary_managed_wss_admission::validate(const std::shared_ptr<lattice_db>& application,
    const std::shared_ptr<lattice_db>& child,const sync_config& config,clock::time_point deadline)const{
    require(creator_==creator()&&application==application_,"managed WSS parent identity differs");
    require(matches(child,config),"managed WSS child/session admission expired");
    if(receiver_)ordinary_receiver_wss_access::validate_admission(receiver_,application,child,config,deadline);
    else ordinary_managed_ipc_access::validate_wss(*product_,application,deadline);
    require(matches(child,config)&&clock::now()<deadline,"managed WSS validation lost current custody");
}
ordinary_managed_wss_handle::ordinary_managed_wss_handle(std::shared_ptr<configured_recovery_connection> owner)
    :creator_(creator()),owner_(std::move(owner)){}
ordinary_managed_wss_handle::~ordinary_managed_wss_handle(){stop();}
ordinary_managed_wss_handle::ordinary_managed_wss_handle(std::shared_ptr<ordinary_group_target_receiving_owner> owner,
    std::shared_ptr<const ordinary_receiver_wss_request> request)
    :creator_(creator()),group_owner_(std::move(owner)),group_request_(std::move(request)){}
void ordinary_managed_wss_handle::stop()const noexcept{
    if(creator_!=creator())return;
    if(group_owner_)group_owner_->request_close();
    else if(owner_)owner_->request_close();
}
bool ordinary_managed_wss_handle::settled()const noexcept{
    return creator_==creator()&&(group_owner_?group_owner_->cleanup_complete():owner_&&owner_->cleanup_complete());
}
lattice_close_result ordinary_managed_wss_handle::close(clock::time_point deadline)const noexcept{
    if(creator_==creator()&&group_owner_)return group_owner_->close(deadline);
    if(creator_==creator()&&owner_)return owner_->close(deadline);
    lattice_close_result refused;refused.cleanup_complete=false;refused.sync=sync_drain_state::failed;
    try{throw db_error("managed WSS close has no actual creating owner");}catch(...){refused.remember(std::current_exception(),false);}
    return refused;
}
std::shared_ptr<ordinary_managed_wss_handle> ordinary_managed_ipc_access::start_wss(
    const ordinary_managed_ipc_local_route& route,const std::shared_ptr<ordinary_managed_product_credentials>& credentials,const std::string& token){
    creator_check();
    const auto deadline=clock::now()+std::chrono::seconds(30);
    route.validate_lifetime();
    ordinary_managed_start_capture_tail start_captures;
    const auto state=route.product_authority();
    require_managed_product_live(state);
    require(credentials==state->credentials,"managed WSS credential capability differs");
    // Both directional route wrappers select the same actual spoke. Socket
    // direction, a caller's Lattice pointer or a URL never chooses authority.
    const auto application=state->pair.recipient_owner();
    application->require_creator_process();
    start_captures.retain_application(application);
    std::shared_ptr<const ordinary_managed_owner_release> previous_start;
    {std::lock_guard lock(application->managed_wss_mutex_);
        require(!application->is_closed(),"managed application is retired before start association");
        require(!application->managed_wss_start_release_||application->managed_wss_start_release_->start_fields_destroyed(),"managed prior start still owns native captures");
        previous_start=std::move(application->managed_wss_start_release_);
        application->managed_wss_start_release_=start_captures.completion_;
    }
    validate_wss(*state,application,deadline);
    require(!application->config_.is_sync_enabled()&&application->config_.ipc_targets.empty()&&
        !application->recovery_continuous_,"managed WSS requires separately enrolled dormant application");
    std::string endpoint="wss://"+state->credentials->endpoint_.substr(8)+"/sync";
    if(!state->group.empty()){auto group=state->group;for(auto& c:group)if(c>='a'&&c<='f')c-=('a'-'A');endpoint+="/group/"+group;}
    require(state->credentials->matches_configuration(endpoint,token),"managed WSS credential differs from actual directory session");
    // The ordinary callback facade records its immutable listener before any
    // legacy/controller delivery. This load never copies mutable std::function
    // fields and cannot lose a listener racing initial managed publication.
    auto handlers=application->configured_listener_cell();
    std::shared_ptr<configured_recovery_connection> previous;
    {std::lock_guard lock(application->managed_wss_mutex_);
        require(!application->managed_wss_starting_,"managed WSS construction already admitted");
        previous=application->managed_wss_connection_;
        require(!previous||previous->cleanup_complete(),"managed WSS predecessor retirement is unsettled");
        application->managed_wss_starting_=true;
    }
    std::shared_ptr<configured_recovery_connection> control;
    std::shared_ptr<ordinary_managed_wss_admission> admission;
    try{
        const auto factory=get_network_factory();auto* typed=dynamic_cast<configured_platform_factory*>(factory.get());
        require(factory&&typed,"managed WSS requires actual typed platform factory");
        // The actual retained enrolled product route selects this fixed ID.
        // No caller profile string can substitute for principal/group custody.
        const std::string model_profile=state->group.empty()?"engram-personal-v1":"engram-group-v1";
        auto policy=reserve_managed_wss_policy(*application,endpoint,model_profile);
        // Same real WSS ownership lock used by legacy setup. O_NOFOLLOW and
        // actual owner/type/link checks are additional refusal checks. It is
        // retained separately from legacy teardown until actual managed close.
        const auto lock_path=application->config_.path+".sync.lock";
        const auto main=application->db_->physical_identity("main",{},true);
        validate_wss(*state,application,deadline);
        auto retained_product=state;auto retained_application=application;
        auto retained_profile=state->pair.recipient_profile();auto retained_endpoint=endpoint;
        auto lock_custody=configured_recovery_connection::reserve_managed_lock(main,lock_path,policy,deadline,start_captures.completion_);
        const auto lock_fd=::open(lock_path.c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK,0600);
        lock_custody.adopt(lock_fd,lock_fd<0?errno:0);
        require(lock_fd>=0,"managed WSS ownership lock unavailable");
        struct stat st{};const bool observed=::fstat(lock_fd,&st)==0;
        if(observed)lock_custody.observe(static_cast<std::uint64_t>(st.st_dev),static_cast<std::uint64_t>(st.st_ino));
        require(observed&&S_ISREG(st.st_mode)&&st.st_uid==::geteuid()&&
            st.st_nlink==1&&(st.st_mode&077)==0&&::flock(lock_fd,LOCK_EX|LOCK_NB)==0,"managed WSS ownership lock refused");
        validate_wss(*state,application,deadline);
        // Every fallible argument copy preceded open; allocation failure leaves
        // the local lease intact. All constructor arguments below only move.
        auto* raw_admission=new ordinary_managed_wss_admission(
            std::move(retained_product),std::move(retained_application),std::move(retained_profile),std::move(retained_endpoint),
            std::move(policy),std::move(lock_custody),deadline,start_captures.completion_);
        admission=std::shared_ptr<ordinary_managed_wss_admission>(raw_admission,
            [](ordinary_managed_wss_admission* p){if(p->creator_==creator())delete p;});
        sync_config config;config.ordinary_policy=ordinary_sync_policy::ordered_required;
        config.ordinary_model_profile=model_profile;
        config.managed_product_credentials=state->credentials;config.websocket_url=endpoint;
        config.authorization_token=token;config.sync_id="wss:"+endpoint;
        config.all_active_sync_ids={config.sync_id};config.is_observer=application->config_.sync_is_observer;
        application->config_.tuning.apply(config);
        control=configured_recovery_connection::create_impl(application,config,factory,typed,[application,deadline]{
            require(clock::now()<deadline&&!application->is_closed(),"managed WSS child admission expired");
            configuration child(application->config_.path,std::make_shared<std_thread_scheduler>());
            child.ordinary_context=application->config_.ordinary_context;
            child.wal_extent_limit_bytes=application->config_.wal_extent_limit_bytes;
            child.busy_timeout_ms=application->config_.busy_timeout_ms;
            child.target_schema_version=application->config_.target_schema_version;
            child.migration_block=application->config_.migration_block;
            return std::shared_ptr<lattice_db>(ordinary_export_access::child(*application,child));
        },admission,std::move(handlers));
        // Registration of the exact retained controller precedes first dial.
        // Revocation closes admission before promoting weak owners off-leaf.
        {std::lock_guard lock(state->credentials->wss_leaf_);
            require(state->credentials->current(),"managed WSS credentials revoked before publication");
            auto& owners=state->credentials->wss_owners_;
            const auto at=std::find_if(owners.begin(),owners.end(),[](const auto& x){return x.expired();});
            require(at!=owners.end(),"managed credential WSS owner capacity exhausted");*at=control;
        }
        {std::lock_guard lock(state->leaf);require(state->live(),"managed route revoked before WSS publication");state->wss_owner=control;}
        auto handle=std::shared_ptr<ordinary_managed_wss_handle>(new ordinary_managed_wss_handle(control),
            [](ordinary_managed_wss_handle* p){if(p->creator_==creator())delete p;});
        {std::lock_guard lock(application->managed_wss_mutex_);
            require(!application->is_closed()&&admission->current(),"managed application closed before publication");
            application->managed_wss_connection_=control;application->managed_wss_starting_=false;
        }
        control->start();return handle;
    }catch(...){
        if(control)control->abandon_start(std::current_exception());
        {std::lock_guard lock(application->managed_wss_mutex_);application->managed_wss_starting_=false;}
        throw;
    }
}
}
#endif

#include "ordinary_managed_receiver_wss.inc"
