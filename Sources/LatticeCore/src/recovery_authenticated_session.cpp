#include "recovery_authenticated_session.hpp"
#include "lattice/lattice.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <charconv>

namespace lattice::detail {
namespace {
using json=nlohmann::json;
constexpr size_t policy_bytes=32768,connection_bytes=8192,frame_bytes=1048576,frame_entries=256;
[[noreturn]] void reject(const char* text){throw db_error(text);}
json bounded(const std::string& raw,size_t cap,size_t scalar=65536) {
    if(raw.empty() || raw.size()>cap)reject("relay JSON byte bound");
    size_t nodes=0;std::vector<std::set<std::string>> keys;
    const auto callback=[&](int depth,json::parse_event_t event,json& value) {
        if(depth<0 || depth>16 || ++nodes>32768)reject("relay JSON structural bound");
        if(value.is_string() && value.get_ref<const std::string&>().size()>scalar)reject("relay JSON scalar bound");
        if(event==json::parse_event_t::object_start)keys.emplace_back();
        if(event==json::parse_event_t::key) {
            if(keys.empty() || !keys.back().insert(value.get<std::string>()).second)reject("relay duplicate JSON key");
        }
        if(event==json::parse_event_t::object_end)keys.pop_back();
        return true;
    };
    auto result=json::parse(raw,callback);if(!result.is_object())reject("relay JSON object required");return result;
}
void shape(const json& j,std::initializer_list<const char*> names) {
    if(!j.is_object() || j.size()!=names.size())reject("relay unexpected object shape");
    for(const auto* name:names)if(!j.contains(name))reject("relay missing object member");
}
std::string text(const json& j,const char* key,size_t cap=256) {
    const auto& v=j.at(key);if(!v.is_string())reject("relay text required");
    const auto& s=v.get_ref<const std::string&>();if(s.empty() || s.size()>cap || s.find('\0')!=std::string::npos)reject("relay text bound");return s;
}
int64_t number(const json& j,const char* key,int64_t lo,int64_t hi) {
    const auto& v=j.at(key);if(!v.is_number_integer() || v.is_number_unsigned() && v.get<uint64_t>()>uint64_t(hi))reject("relay integer required");
    const auto n=v.get<int64_t>();if(n<lo || n>hi)reject("relay integer bound");return n;
}
std::string uuid(const json& j,const char* key){const auto s=text(j,key,36);return canonical_writer_adapter::uuid_key(s);}
std::string lower(std::string s){for(auto& c:s)if(c>='A'&&c<='Z')c=char(c+32);return s;}
bool identifier(const std::string& s) {
    if(s.empty()||s.size()>64||s[0]>='0'&&s[0]<='9')return false;
    for(unsigned char c:s)if(!(c>='A'&&c<='Z'||c>='a'&&c<='z'||c>='0'&&c<='9'||c=='_'))return false;return true;
}
uint8_t operations(const json& a) {
    if(!a.is_array() || a.size()>3)reject("relay operation mask bound");uint8_t mask=0;
    for(const auto& op:a){if(!op.is_string())reject("relay operation must be text");
        const auto s=op.get<std::string>();const uint8_t bit=s=="INSERT"?1:s=="UPDATE"?2:s=="DELETE"?4:0;
        if(!bit || mask&bit)reject("relay unknown or duplicate operation");mask|=bit;}
    return mask;
}
struct source_recipe {
    canonical_namespaced_writer_profile profile;
    canonical_ready_profile ready;
    json incoming;
    std::string key,selected_namespace;
    int64_t maximum_duration;
    std::map<std::string,uint8_t> upload;
    uint8_t unlisted=7;
    size_t maximum_deletes=frame_entries;
    std::string ready_name="boundedV1";
};
source_recipe recipe(const recovery_owner_schema& catalog,const json& j) {
    auto base_shape=j;base_shape.erase("readyProfile");
    shape(base_shape,{"version","authority","sourceID","epoch","localNamespace","namespaces","receiptNamespace","models","walFull","maximumAuthorizationMilliseconds","upload"});
    if(number(j,"version",1,1)!=1 || j.at("walFull")!=true)reject("relay explicit durability opt-in required");
    source_recipe r;r.maximum_duration=number(j,"maximumAuthorizationMilliseconds",1,3600000);
    auto& p=r.profile;
    if(!catalog.valid()||catalog.swift_digest.size()!=64)reject("relay actual Swift declaration catalog unavailable");
    p.writer.binding.source=uuid(j,"sourceID");p.writer.binding.epoch=uuid(j,"epoch");p.writer.binding.schema=catalog.swift_digest;
    const auto& models=j.at("models");if(!models.is_array()||models.empty()||models.size()>16)reject("relay model scope bound");
    std::set<std::string> unique,folded;
    for(const auto& entry:models) {
        if(!entry.is_string())reject("relay model name required");const auto name=entry.get<std::string>();
        if(!identifier(name)||!unique.insert(name).second||!folded.insert(lower(name)).second||!catalog.swift_models.count(name)||!catalog.find(name))
            reject("relay scope differs from actual Swift catalog");p.writer.models.push_back(name);
    }
    std::sort(p.writer.models.begin(),p.writer.models.end());
    const json all=json::array({"INSERT","UPDATE","DELETE"});
    r.incoming={{"models",json::array()},{"relations",json::array()},{"scopedLinkTables",json::array()},{"catalogDigest",catalog.swift_digest}};
    for(const auto& name:p.writer.models)r.incoming["models"].push_back({{"table",name},{"incomingOperations",all}});
    std::set<std::string> tables=unique;
    for(const auto& [name,model]:catalog.models)for(const auto& prop:model.properties) {
        if(prop.kind!=property_kind::link&&prop.kind!=property_kind::list)continue;
        const bool lhs=unique.count(name),rhs=unique.count(prop.target_table);if(!lhs&&!rhs)continue;
        if(!lhs||!rhs)reject("relay scope must include the actual connected relation closure");
        const auto table="_"+model.table_name+"_"+prop.target_table+"_"+prop.name;
        if(!identifier(table)||!tables.insert(table).second||tables.size()>16||!folded.insert(lower(table)).second)reject("relay relation catalog bound");
        r.incoming["relations"].push_back({{"table",table},{"lhsModel",model.table_name},{"rhsModel",prop.target_table},{"incomingOperations",all}});
        r.incoming["scopedLinkTables"].push_back(table);
    }
    p.writer.binding.scope=picosha2::hash256_hex_string(r.incoming.dump());
    // The named bounded-v1 profile is immutable across reopen, including all
    // future READY budgets. No per-socket policy can rewrite it.
    p.writer.limits={65536,16777216,65536,16777216,256,128,64};p.writer.upstream_requested=true;
    p.namespaces.local_namespace=text(j,"localNamespace");r.selected_namespace=text(j,"receiptNamespace");
    const auto& ns=j.at("namespaces");if(!ns.is_array()||ns.empty()||ns.size()>64)reject("relay namespace catalog bound");
    for(const auto& n:ns){shape(n,{"namespaceID","coverageID","revision"});p.namespaces.entries.push_back({text(n,"namespaceID"),text(n,"coverageID"),number(n,"revision",1,INT64_MAX)});}
    std::sort(p.namespaces.entries.begin(),p.namespaces.entries.end(),[](const auto& a,const auto& b){return a.namespace_id<b.namespace_id;});
    p.namespaces.validate();bool selected=false;
    for(const auto& n:p.namespaces.entries)if(n.namespace_id==r.selected_namespace)selected=true;
    if(!selected||r.selected_namespace==p.namespaces.local_namespace)reject("relay peer namespace must be enrolled and distinct from local");
    auto& ready=r.ready;ready.authority=text(j,"authority");ready.transfers=16;ready.bindings=1024;ready.charged_bytes=67108864;ready.transfer_bytes=2097152;
    ready.package={{{16384,4096,2,256,4096,1048576,256,256,262144},16,4096,4096,256,256,65536,131072,3600000,{4096,32,256,2048,4096}},1572864,514};
    ready.capture={{{65536,16,4096,8192,2,2048,4096,1048576},16,32,32},p.writer.limits,256,256,32};
    if(j.contains("readyProfile")) {
        r.ready_name=text(j,"readyProfile",32);
        if(r.ready_name!="bounded48MiBV1")reject("relay explicit READY profile unknown");
        ready.transfers=8;ready.charged_bytes=536870912;ready.transfer_bytes=50331648;
        ready.package={{{4194304,16384,64,512,16384,33554432,256,8192,8388608},
            16,262144,32768,8192,8192,2097152,4194304,3600000,{16384,64,256,4096,16384}},41943040,770};
        ready.capture={{{262144,16,16384,16384,64,256,16384,33554432},16,32,32},p.writer.limits,8192,8192,256};
    }
    const auto& upload=j.at("upload");shape(upload,{"tables","unlisted","maximumDeletes"});
    r.unlisted=upload.at("unlisted")=="allow"?7:upload.at("unlisted")=="deny"?0:255;if(r.unlisted==255)reject("relay unlisted policy required");
    r.maximum_deletes=static_cast<size_t>(number(upload,"maximumDeletes",0,frame_entries));
    if(!upload.at("tables").is_array()||upload.at("tables").size()>16)reject("relay upload table bound");
    for(const auto& t:upload.at("tables")){shape(t,{"table","operations"});const auto name=text(t,"table",64),key=lower(name);
        if(!identifier(name)||!folded.count(key)||!r.upload.emplace(key,operations(t.at("operations"))).second)reject("relay ambiguous or unregistered upload table");}
    auto identity=j;identity.erase("receiptNamespace");identity.erase("maximumAuthorizationMilliseconds");identity.erase("upload");
    identity["models"]=p.writer.models;identity["namespaces"]=json::array();for(const auto& n:p.namespaces.entries)identity["namespaces"].push_back({{"namespaceID",n.namespace_id},{"coverageID",n.coverage_id},{"revision",n.revision}});
    identity["actualSchema"]=catalog.swift_digest;identity["actualScope"]=p.writer.binding.scope;r.key=identity.dump();return r;
}
struct route_lifetime {
    std::shared_ptr<void> context;
    int32_t(*current)(void*)=nullptr;
    bool live()const {return current&&current(context.get())==1;}
};
}
struct authenticated_mounted_source {
    std::shared_ptr<lattice_db> owner;
    std::shared_ptr<canonical_writer_adapter> adapter;
    source_recipe recipe;
    std::atomic<size_t> sessions{0};
    std::shared_ptr<authenticated_ready_budget> ready_budget;
    std::mutex ready_mutex;
    std::map<std::string,std::weak_ptr<authenticated_ready_fence>> ready_fences;
};
struct authenticated_ready_budget {
    // Keep only the payload-free physical identity and bounded immutable recipe.
    // A queued result may outlive the last mounted source without retaining its
    // owner/adapter or running their destructors on a socket callback.
    const std::shared_ptr<instance_guard> owner_guard;
    const std::string recipe_key;
    std::mutex mutex;
    uint64_t requests=0,bytes=0,workspace=0;
    static constexpr uint64_t max_requests=64,max_bytes=67108864,max_workspace=268435456,input_limit=8388608,reply_limit=4194304;
    authenticated_ready_budget(std::shared_ptr<instance_guard> guard,std::string key):owner_guard(std::move(guard)),recipe_key(std::move(key)){}
};
struct authenticated_ready_fence {
    std::atomic<bool> current{false};
    const std::shared_ptr<std::atomic<bool>> admitted=std::make_shared<std::atomic<bool>>(true);
    const int64_t deadline;
    explicit authenticated_ready_fence(int64_t value):deadline(value){}
    bool live()const noexcept{return admitted->load(std::memory_order_acquire)&&current.load(std::memory_order_acquire)&&authenticated_session_fence::now()<deadline;}
};
authenticated_ready_charge::~authenticated_ready_charge(){if(budget_){std::lock_guard lock(budget_->mutex);--budget_->requests;budget_->bytes-=charged_;budget_->workspace-=workspace_;}}
namespace {
struct registry_slot {
    std::weak_ptr<authenticated_mounted_source> value;
    std::weak_ptr<authenticated_ready_budget> budget;
    bool building=false;
};
std::mutex registry_mutex;
std::map<instance_guard*,registry_slot> registry;
std::atomic<uint64_t> turns{0};
}
struct authenticated_relay_setup::state {
    std::shared_ptr<authenticated_mounted_source> source;
    std::shared_ptr<authenticated_session_fence> fence;
    std::shared_ptr<route_lifetime> route;
    source_recipe recipe;
    json context;
    std::optional<canonical_namespace_admission> admission;
    std::string authorization_revision;
    uint64_t route_generation=0,lease_sequence=0;
    struct ready_slot {
        canonical_ready_lease lease;
        canonical_range::attempt attempt;
        std::string request_digest,id;
        std::shared_ptr<authenticated_ready_fence> fence;
    };
    std::optional<ready_slot> ready;
    bool consumed=false,charged=false;
    ~state(){if(charged)source->sessions.fetch_sub(1,std::memory_order_relaxed);}
};
int64_t authenticated_session_fence::now()noexcept{return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
bool authenticated_session_fence::live()const noexcept{return !stopped_.load(std::memory_order_acquire)&&authorized_.load(std::memory_order_acquire)&&
    owner_guard_&&owner_guard_->alive.load(std::memory_order_seq_cst)&&active_guard_&&active_guard_->load(std::memory_order_acquire)&&
    now()<deadline_.load(std::memory_order_acquire);}
bool authenticated_session_fence::stopped()const noexcept{return stopped_.load(std::memory_order_acquire);}
bool authenticated_session_fence::drained()const noexcept{std::lock_guard lock(mutex_);return active_==0;}
void authenticated_session_fence::stop()noexcept{std::lock_guard lock(mutex_);stopped_.store(true,std::memory_order_release);}
std::shared_ptr<authenticated_ready_charge> authenticated_session_fence::reserve_ready(uint64_t input)const {
    if(!live()||!ready_budget_||!input||input>authenticated_ready_budget::input_limit)return {};
    auto charge=std::shared_ptr<authenticated_ready_charge>(new authenticated_ready_charge);
    const auto bytes=input+authenticated_ready_budget::reply_limit;
    {std::lock_guard lock(ready_budget_->mutex);
        if(!live()||ready_budget_->requests>=authenticated_ready_budget::max_requests||bytes>authenticated_ready_budget::max_bytes-ready_budget_->bytes)return {};
        charge->input_=input;charge->charged_=bytes;charge->budget_=ready_budget_;++ready_budget_->requests;ready_budget_->bytes+=bytes;}
    return charge;
}
authenticated_relay_operation::authenticated_relay_operation(std::shared_ptr<authenticated_session_fence> f):fence_(std::move(f)){}
authenticated_relay_operation::~authenticated_relay_operation(){if(fence_){std::lock_guard lock(fence_->mutex_);--fence_->active_;}}
bool authenticated_relay_operation::publishable()const noexcept{return fence_&&fence_->live()&&(!ready_||ready_->live());}
authenticated_relay_setup::authenticated_relay_setup(std::shared_ptr<state> s):state_(std::move(s)){}
authenticated_relay_setup::~authenticated_relay_setup(){close();}
std::shared_ptr<authenticated_session_fence> authenticated_relay_setup::stop_token()const noexcept{return state_?state_->fence:nullptr;}
void authenticated_relay_setup::close()noexcept{if(state_)state_->fence->stop();}
std::shared_ptr<authenticated_relay_setup> authenticated_relay_setup::open(std::shared_ptr<lattice_db> owner,
    const std::string& policy,const std::string& connection,void* context,int32_t(*current)(void*),void(*destroy)(void*)) {
    // A supplied nonthrowing destroy transfers route custody on EVERY outcome.
    if(!destroy)reject("relay route destroy required before ownership transfer");
    std::shared_ptr<void> retained(context,destroy);
    if(!owner||!context||!current||current(context)!=1)reject("relay actual live route required");
    auto r=recipe(canonical_writer_adapter::authenticated_catalog(*owner),bounded(policy,policy_bytes));auto c=bounded(connection,connection_bytes);
    shape(c,{"mount","connection","channel","authenticatedUserID","peer"});
    c["mount"]=uuid(c,"mount");c["connection"]=uuid(c,"connection");(void)text(c,"channel",64);c["authenticatedUserID"]=uuid(c,"authenticatedUserID");
    shape(c.at("peer"),{"replicaID","receiverIncarnation","channelIncarnation"});
    (void)text(c.at("peer"),"replicaID");c["peer"]["receiverIncarnation"]=uuid(c.at("peer"),"receiverIncarnation");c["peer"]["channelIncarnation"]=uuid(c.at("peer"),"channelIncarnation");
    const auto owner_guard=canonical_writer_adapter::authenticated_owner_guard(*owner);
    if(!owner_guard)reject("relay actual owner identity unavailable");
    std::shared_ptr<authenticated_mounted_source> source;
    std::shared_ptr<authenticated_ready_budget> budget;
    bool busy=false,profile_differs=false;
    {
        std::lock_guard lock(registry_mutex);
        for(auto i=registry.begin();i!=registry.end();) {
            if(!i->second.building&&i->second.value.expired()&&i->second.budget.expired())i=registry.erase(i);else ++i;
        }
        auto i=registry.find(owner_guard.get());
        if(i!=registry.end()) {
            source=i->second.value.lock();budget=i->second.budget.lock();busy=i->second.building;
            profile_differs=budget&&budget->recipe_key!=r.key;
        }
        if(!source&&!busy&&!profile_differs) {
            if(i==registry.end()&&registry.size()>=128)busy=true;
            else {
                if(!budget)budget=std::make_shared<authenticated_ready_budget>(owner_guard,r.key);
                auto& slot=registry[owner_guard.get()];slot.budget=budget;slot.building=true;
            }
        }
    }
    if(profile_differs)reject("relay source profile differs on actual owner");
    if(busy)reject("relay bounded source enrollment busy");
    if(source&&source->recipe.key!=r.key)reject("relay source profile differs on actual owner");
    if(!source) {
        try {
            source=std::make_shared<authenticated_mounted_source>();source->owner=owner;source->recipe=r;
            source->ready_budget=budget;
            source->adapter=canonical_writer_adapter::open_authenticated_source(owner,r.profile,{frame_entries,65536,frame_bytes},{64,3600000},r.ready,true);
            std::lock_guard lock(registry_mutex);auto& slot=registry.at(owner_guard.get());slot.value=source;slot.building=false;
        }catch(...) {
            // Failed re-enrollment must not erase charges retained by an old
            // result. Only an expired source AND budget can release the slot.
            std::lock_guard lock(registry_mutex);registry.at(owner_guard.get()).building=false;throw;
        }
    }
    auto s=std::make_shared<state>();s->source=std::move(source);
    auto count=s->source->sessions.load(std::memory_order_relaxed);
    do{if(count>=1024)reject("relay native source session capacity");}
    while(!s->source->sessions.compare_exchange_weak(count,count+1,std::memory_order_relaxed));
    s->charged=true;s->recipe=std::move(r);
    s->fence=std::shared_ptr<authenticated_session_fence>(new authenticated_session_fence());
    s->fence->ready_budget_=s->source->ready_budget;
    s->fence->owner_guard_=owner_guard;
    s->fence->active_guard_=s->source->adapter->authenticated_active_guard();
    s->route=std::make_shared<route_lifetime>(route_lifetime{std::move(retained),current});
    if(!s->route->live())reject("relay route closed during source enrollment");
    auto serial=turns.load(std::memory_order_relaxed);
    do{if(serial>=INT64_MAX)reject("relay authorization turn exhausted");}
    while(!turns.compare_exchange_weak(serial,serial+1,std::memory_order_relaxed));
    s->route_generation=serial+1;
    const auto& p=s->recipe.profile;const canonical_namespace_entry* ns=nullptr;
    for(const auto& n:p.namespaces.entries)if(n.namespace_id==s->recipe.selected_namespace)ns=&n;
    s->context={{"route",c},{"authorizationTurn",std::to_string(serial)},
        {"source",{{"authority",s->recipe.ready.authority},{"sourceID",p.writer.binding.source},{"epoch",p.writer.binding.epoch},
        {"scopeDigest",p.writer.binding.scope},{"schemaDigest",p.writer.binding.schema},{"receiptNamespace",ns->namespace_id},
        {"coverageID",ns->coverage_id},{"coverageRevision",ns->revision},{"descriptorDigest",s->source->adapter->authenticated_descriptor_digest()}}},
        {"incomingScope",s->recipe.incoming}};
    return std::shared_ptr<authenticated_relay_setup>(new authenticated_relay_setup(std::move(s)));
}
std::string authenticated_relay_setup::descriptor()const{if(!state_||state_->fence->stopped()||!state_->route->live())reject("relay setup retired");return state_->context.dump();}
bool authenticated_relay_setup::finish_authorization(const std::string& raw) {
    auto s=state_;if(!s||s->consumed||s->fence->stopped()||!s->route->live())return false;
    s->consumed=true; // A rejected/throwing outcome cannot be edited and retried.
    try {
        auto value=bounded(raw,policy_bytes);shape(value,{"context","authenticatedUserID","peer","source","incomingScope","authorizationRevision","validForMilliseconds"});
        value["authenticatedUserID"]=uuid(value,"authenticatedUserID");
        value["peer"]["receiverIncarnation"]=uuid(value.at("peer"),"receiverIncarnation");
        value["peer"]["channelIncarnation"]=uuid(value.at("peer"),"channelIncarnation");
        value["source"]["sourceID"]=uuid(value.at("source"),"sourceID");value["source"]["epoch"]=uuid(value.at("source"),"epoch");
        if(value.at("authenticatedUserID")!=s->context.at("route").at("authenticatedUserID") ||
           value.at("peer")!=s->context.at("route").at("peer") || value.at("source")!=s->context.at("source") ||
           value.at("incomingScope")!=s->context.at("incomingScope") || value.at("context")!=s->context)reject("relay authorization differs from actual setup context");
        s->authorization_revision=text(value,"authorizationRevision");const auto ms=number(value,"validForMilliseconds",1,s->recipe.maximum_duration);
        const auto time=authenticated_session_fence::now();if(time>INT64_MAX-ms)reject("relay authorization deadline exhausted");
        s->fence->deadline_.store(time+ms,std::memory_order_release);s->fence->authorized_.store(true,std::memory_order_release);
        s->admission=s->source->adapter->admit_authenticated_session(s->source->owner,s->recipe.selected_namespace,
            text(s->context.at("route").at("peer"),"replicaID"),s->fence);
        if(!s->route->live()||!s->fence->live())reject("relay authorization completed after route retirement");return true;
    }catch(...){s->fence->stop();throw;}
}
authenticated_relay_result authenticated_relay_setup::receive(const std::string& raw) {
    auto s=state_;if(!s||!s->admission||!s->route->live())return {2,{},{}};
    // Allocate before charging. Failed admission destroys no charged token.
    auto operation=std::shared_ptr<authenticated_relay_operation>(new authenticated_relay_operation(nullptr));
    {std::lock_guard lock(s->fence->mutex_);if(!s->fence->live()||s->fence->active_>=16)return {2,{},{}};
        ++s->fence->active_;operation->fence_=s->fence;}
    const auto frame=bounded(raw,frame_bytes);
    if(frame.contains("auditLog")==frame.contains("ack"))reject("relay ambiguous frame");
    const bool upload=frame.contains("auditLog");
    if(frame.size()!=(frame.contains("kind")?2u:1u) ||
       (frame.contains("kind")&&frame.at("kind")!=(upload?"auditLog":"ack")))reject("relay unsupported frame");const auto& entries=frame.at(upload?"auditLog":"ack");
    if(!entries.is_array()||entries.size()>frame_entries)reject("relay frame entry bound");
    auto event=server_sent_event::from_json(raw);if(!event)reject("relay native frame decode refused");
    if(upload) {
        if(event->event_type!=server_sent_event::type::audit_log||event->audit_logs.size()!=entries.size())reject("relay malformed upload entry");
        size_t deletes=0;
        for(const auto& entry:event->audit_logs) {
            const auto key=lower(entry.table_name);const uint8_t op=entry.operation=="INSERT"?1:entry.operation=="UPDATE"?2:entry.operation=="DELETE"?4:0;
            const auto rule=s->recipe.upload.find(key);const auto mask=rule==s->recipe.upload.end()?s->recipe.unlisted:rule->second;
            if(!op||!(mask&op)||key=="auditlog")reject("relay native upload policy refused");
            if(op==4&&++deletes>s->recipe.maximum_deletes)reject("relay delete cap refused");
        }
        auto ids=s->source->adapter->apply_upstream_namespaced_owned(s->source->owner,*s->admission,event->audit_logs,text(s->context.at("route"),"channel",64));
        // Revocation after pre-effect admission never changes committed IDs
        // into absence. The result is retained but no late ACK is permitted.
        return {1,std::move(ids),std::move(operation)};
    }
    if(event->event_type!=server_sent_event::type::ack||event->acked_ids.size()!=entries.size())reject("relay malformed ACK entry");
    for(const auto& id:event->acked_ids)(void)canonical_writer_adapter::uuid_key(id);
    if(!s->fence->live()||!s->route->live())return {2,{},std::move(operation)};
    // Legacy server bookkeeping is independent of upload rights. This does
    // not mint a canonical receipt, a receiver install, or negative evidence.
    mark_audit_entries_synced(*s->source->owner,event->acked_ids);
    return {1,{},std::move(operation)};
}

namespace {
namespace ready_cr=canonical_range;
uint64_t ready_decimal(const json& value,const char* key,uint64_t low=1) {
    const auto raw=text(value,key,20);uint64_t result=0;
    const auto parsed=std::from_chars(raw.data(),raw.data()+raw.size(),result);
    if(parsed.ec!=std::errc{}||parsed.ptr!=raw.data()+raw.size()||result<low||result>INT64_MAX||std::to_string(result)!=raw)
        reject("READY unsigned decimal spelling outside bounds");
    return result;
}
json ready_settlement(const recovery_install_result& result) {
    const char* state="unknown";
    switch(result.state) {
        case recovery_install_state::refused:state="refused";break;
        case recovery_install_state::rolled_back:state="rolledBack";break;
        case recovery_install_state::committed:state="committed";break;
        case recovery_install_state::unsettled:state="unsettled";break;
        case recovery_install_state::ownership_lost:state="ownershipLost";break;
    }
    return {{"state",state},{"unexpectedCommitObserved",result.unexpected_commit_observed},
        {"primaryError",bool(result.primary_error)},{"cleanupError",bool(result.cleanup_error)},
        {"postcommitError",bool(result.postcommit_error)},{"notificationError",bool(result.notification_error)}};
}
json ready_wire_limits(const ready_cr::wire_limits& v) {
    return {{"frame_bytes",std::to_string(v.frame_bytes)},{"payload_bytes",std::to_string(v.payload_bytes)},
        {"items_per_page",std::to_string(v.items_per_page)},{"content_pages",std::to_string(v.content_pages)},
        {"content_identities",std::to_string(v.content_identities)},{"content_bytes",std::to_string(v.content_bytes)},
        {"receipt_pages",std::to_string(v.receipt_pages)},{"receipts",std::to_string(v.receipts)},{"receipt_bytes",std::to_string(v.receipt_bytes)}};
}
json ready_profile_description(const source_recipe& r) {
    const auto& p=r.ready;const auto& c=p.package.codec;
    return {{"name",r.ready_name},{"wire",ready_wire_limits(c.maximum)},
        {"requestEntries",c.request_entries},{"requestTargets",c.request_targets},{"requestTargetBytes",c.request_target_bytes},
        {"parserDepth",c.depth},{"parserNodes",c.nodes},{"scalarBytes",c.string_bytes},{"restartBytes",c.restart_bytes},
        {"valueLimits",{{"rawBytes",c.values.raw_bytes},{"fields",c.values.fields},{"nameBytes",c.values.name_bytes},
            {"valueBytes",c.values.value_bytes},{"decodedBytes",c.values.decoded_bytes}}},
        {"leaseMilliseconds",c.lease_ms},{"packageBytes",p.package.retained_wire_bytes},{"frames",p.package.frames},
        {"transfers",p.transfers},{"bindings",p.bindings},{"durableBytes",p.charged_bytes},{"transferBytes",p.transfer_bytes},
        {"captureRows",p.capture.rows.wire.total_rows},{"captureBytes",p.capture.rows.wire.total_bytes},
        {"requestBytes",authenticated_ready_budget::input_limit},{"pendingRequests",authenticated_ready_budget::max_requests},
        {"pendingInputAndReplyBytes",authenticated_ready_budget::max_bytes},{"pendingWorkspaceBytes",authenticated_ready_budget::max_workspace}};
}
std::string ready_live_binding(const json& context) {
    // The exact actual namespace/registered replica/logical receiver binding;
    // attempt sequence is deliberately excluded so successors revoke old sends.
    const auto& route=context.at("route");const auto& peer=route.at("peer");
    return json::array({context.at("source").at("receiptNamespace"),peer.at("replicaID"),
        peer.at("receiverIncarnation"),peer.at("channelIncarnation"),route.at("channel")}).dump();
}
}
thread_local const std::function<void()>* authenticated_relay_setup::ready_before_owned_test_hook_=nullptr;
authenticated_ready_result authenticated_relay_setup::ready(const std::string& raw,const std::shared_ptr<authenticated_ready_charge>& charge) {
    auto s=state_;if(!s||!s->admission||!s->route->live())return {2,{},{}};
    if(!charge||charge->budget_!=s->source->ready_budget||charge->input_!=raw.size()||charge->consumed_.exchange(true,std::memory_order_acq_rel))
        reject("READY actual one-shot source input reservation required");
    auto operation=std::shared_ptr<authenticated_relay_operation>(new authenticated_relay_operation(nullptr));
    {std::lock_guard lock(s->fence->mutex_);if(!s->fence->live()||s->fence->active_>=16)return {2,{},{}};
        ++s->fence->active_;operation->fence_=s->fence;operation->charge_=charge;}
    const auto control=bounded(raw,authenticated_ready_budget::input_limit,authenticated_ready_budget::reply_limit);
    if(!control.contains("kind")||control.at("kind")!="recoveryReady") {
        if(raw.size()>frame_bytes)reject("relay ordinary frame bound");return {};
    }
    if(number(control,"version",1,1)!=1)reject("READY control version");
    const auto op=text(control,"operation",16),request_id=uuid(control,"requestID");
    json response={{"kind","recoveryReady"},{"version",1},{"operation",op},{"requestID",request_id},
        {"routeGeneration",std::to_string(s->route_generation)}};
    const auto output=[&]() {
        auto wire=response.dump();if(wire.size()>authenticated_ready_budget::reply_limit)reject("READY response bound");
        return authenticated_ready_result{1,std::move(wire),operation,request_id};
    };
    if(op=="describe") {
        shape(control,{"kind","version","operation","requestID"});
        response["source"]=s->context.at("source");response["incomingScope"]=s->context.at("incomingScope");
        response["peer"]=s->context.at("route").at("peer");response["channel"]=s->context.at("route").at("channel");
        response["profile"]=ready_profile_description(s->recipe);
        response["upload"]={{"maximumEntries",frame_entries},{"maximumWireBytes",frame_bytes},{"maximumScalarBytes",65536},{"parserNodes",32768},{"parserDepth",16},{"maximumDeletes",s->recipe.maximum_deletes}};return output();
    }
    if(ready_decimal(control,"routeGeneration")!=s->route_generation)reject("READY physical setup generation differs");
    if(op=="read") {
        shape(control,{"kind","version","operation","requestID","routeGeneration","leaseID","requestDigest","attemptID","sequence","index"});
        if(!s->ready||text(control,"leaseID",128)!=s->ready->id||text(control,"requestDigest",64)!=s->ready->request_digest||
            uuid(control,"attemptID")!=s->ready->attempt.attempt_id||ready_decimal(control,"sequence")!=s->ready->attempt.sequence||!s->ready->fence->live())
            reject("READY current setup lease or exact attempt/Q differs");
        const auto held=*s->ready; // Capture this exact fence BEFORE the transaction.
        const auto read=s->source->adapter->read_ready_frame_owned(s->source->owner,*s->admission,held.lease,ready_decimal(control,"index",0));
        if(read.settlement.state==recovery_install_state::committed&&read.frame) {
            operation->ready_=held.fence;
            if(read.frame->size()>authenticated_ready_budget::reply_limit)reject("READY returned frame bound");
            return {1,*read.frame,std::move(operation),request_id};
        }
        response["settlement"]=ready_settlement(read.settlement);response["frameAvailable"]=false;return output();
    }
    if(op!="prepare"&&op!="resume"&&op!="discard")reject("READY control operation unknown");
    if(op=="prepare") {
        // Finite source-wide charge for retained capture, canonical vectors,
        // capsule strings and request copies. This is explicit logical storage
        // accounting, not allocator/container/SQLite/NIO RSS measurement.
        const auto& p=s->recipe.ready;
        const uint64_t workspace=3*p.capture.rows.wire.total_bytes+2*p.package.retained_wire_bytes+8*p.package.codec.maximum.frame_bytes;
        std::lock_guard lock(charge->budget_->mutex);
        if(workspace>authenticated_ready_budget::max_workspace-charge->budget_->workspace)reject("READY source capture workspace unavailable");
        charge->workspace_=workspace;charge->budget_->workspace+=workspace;
    }
    if(op=="discard")shape(control,{"kind","version","operation","requestID","routeGeneration","request"});
    else shape(control,{"kind","version","operation","requestID","routeGeneration","request","durationMilliseconds"});
    const auto& codec=s->recipe.ready.package.codec;
    const auto request_bytes=text(control,"request",codec.maximum.frame_bytes);
    const auto decoded=ready_cr::decode(request_bytes,codec);const auto* request=std::get_if<ready_cr::request>(&decoded.body);
    const auto& peer=s->context.at("route").at("peer");const auto& logical=decoded.logical;
    const auto& profile=s->recipe.profile.writer.binding;
    if(!request||decoded.route_generation!=s->route_generation||logical.receiver_incarnation!=text(peer,"receiverIncarnation",36)||
        logical.channel_incarnation!=text(peer,"channelIncarnation",36)||logical.channel!=text(s->context.at("route"),"channel",64)||
        request->source!=ready_cr::source_binding{s->recipe.ready.authority,profile.source,profile.epoch,profile.scope,profile.schema})
        reject("READY request differs from actual authenticated source/receiver/channel");
    for(const auto& item:request->receipts) {
        if(item.namespace_id!=std::optional<std::string>{s->recipe.selected_namespace})reject("READY requested namespace differs from actual peer admission");
        for(const auto& target:item.targets) {
            bool allowed=false;
            for(const auto& table:s->recipe.incoming.at("models"))if(table.at("table")==target.table)allowed=true;
            for(const auto& table:s->recipe.incoming.at("relations"))if(table.at("table")==target.table)allowed=true;
            if(!allowed)reject("READY requested target differs from actual incoming scope");
        }
    }
    const auto now=authenticated_session_fence::now();
    const auto duration=op=="discard"?int64_t(1):number(control,"durationMilliseconds",1,static_cast<int64_t>(codec.lease_ms));
    if(now>INT64_MAX-duration||now+duration>s->fence->deadline_.load(std::memory_order_acquire)||!s->fence->live()||!s->route->live())
        reject("READY finite lease exceeds actual authorization or route");
    if(s->lease_sequence==INT64_MAX)reject("READY setup lease sequence exhausted");
    const auto lease_id=std::to_string(s->route_generation)+":"+std::to_string(++s->lease_sequence);
    const auto key=ready_live_binding(s->context);
    auto fence=std::make_shared<authenticated_ready_fence>(now+duration);
    {
        std::lock_guard lock(s->source->ready_mutex);
        for(auto i=s->source->ready_fences.begin();i!=s->source->ready_fences.end();)if(i->second.expired())i=s->source->ready_fences.erase(i);else ++i;
        auto i=s->source->ready_fences.find(key);
        if(i==s->source->ready_fences.end()&&s->source->ready_fences.size()>=1024)reject("READY live binding capacity");
        if(i!=s->source->ready_fences.end())if(auto prior=i->second.lock()) {
            prior->admitted->store(false,std::memory_order_release);prior->current.store(false,std::memory_order_release);
        }
        s->source->ready_fences[key]=fence;
    }
    // An error may follow a durable effect. Superseded old bytes stay fenced,
    // even if this operation cannot return a replacement lease.
    if(s->ready)s->ready->fence->current.store(false,std::memory_order_release);
    s->ready.reset();
    // Recheck this exact reservation inside every owned phase. A later session
    // may reserve while this worker is waiting for SQL; it must not let stale
    // work overwrite the later committed lease and relabel its result.
    const auto ready_admission=canonical_writer_adapter::ready_operation_admission(*s->admission,fence->admitted);
    if(ready_before_owned_test_hook_)(*ready_before_owned_test_hook_)();
    if(op=="discard") {
        const auto result=s->source->adapter->discard_authenticated_ready(s->source->owner,ready_admission,logical,*request);
        response["settlement"]=ready_settlement(result);response["leaseAvailable"]=false;return output();
    }
    std::optional<canonical_ready_info> info;std::optional<canonical_ready_lease> lease;
    if(op=="prepare") {
        const auto expiration=s->source->adapter->expire_authenticated_ready(s->source->owner,ready_admission);
        response["expiration"]=ready_settlement(expiration);
        if(expiration.state!=recovery_install_state::committed){response["leaseAvailable"]=false;return output();}
        const auto result=s->source->adapter->prepare_ready_owned(s->source->owner,ready_admission,logical,*request,duration,s->route_generation);
        response["preparation"]=ready_settlement(result.preparation);response["publication"]=ready_settlement(result.publication);
        response["captureError"]=bool(result.capture_error);response["requiresFullRequest"]=result.requires_full_request;
        if(result.publication.state==recovery_install_state::committed){info=result.transfer;lease=result.lease;}
    } else {
        const auto result=s->source->adapter->resume_ready_owned(s->source->owner,ready_admission,logical,*request,duration,s->route_generation);
        response["settlement"]=ready_settlement(result.settlement);
        if(result.settlement.state==recovery_install_state::committed){info=result.transfer;lease=result.lease;}
    }
    response["leaseAvailable"]=bool(info&&lease);
    if(info&&lease) {
        if(!info->ready||info->logical!=logical||info->namespace_id!=s->recipe.selected_namespace||info->replica_id!=text(peer,"replicaID"))
            reject("READY committed result identity differs");
        {
            std::lock_guard lock(s->source->ready_mutex);
            const auto i=s->source->ready_fences.find(key);
            if(i!=s->source->ready_fences.end()&&i->second.lock()==fence)fence->current.store(true,std::memory_order_release);
        }
        s->ready=state::ready_slot{*lease,logical,request->request_digest,lease_id,fence};operation->ready_=fence;
        response["leaseID"]=lease_id;response["requestDigest"]=request->request_digest;response["attemptID"]=logical.attempt_id;
        response["sequence"]=std::to_string(logical.sequence);response["frames"]=std::to_string(info->frames);
        response["wireBytes"]=std::to_string(info->wire_bytes);response["durationMilliseconds"]=duration;
    }
    return output();
}
} // namespace lattice::detail
