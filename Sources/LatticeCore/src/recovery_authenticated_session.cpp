#include "recovery_authenticated_session.hpp"
#include "canonical_ready_named_profile.hpp"
#include "recovery_predecessor_wire.hpp"
#include "recovery_receipt_json.hpp"
#include "lattice/lattice.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <charconv>
#include <condition_variable>
#include <filesystem>
#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

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
    auto base_shape=j;base_shape.erase("readyProfile");base_shape.erase("receiptCoverage");base_shape.erase("orphanResumeGraceMilliseconds");
    shape(base_shape,{"version","authority","sourceID","epoch","localNamespace","namespaces","receiptNamespace","models","walFull","maximumAuthorizationMilliseconds","upload"});
    const auto version=number(j,"version",1,2);
    if((version==2)!=j.contains("receiptCoverage") || j.at("walFull")!=true)reject("relay explicit durability and receipt profile required");
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
    if(version==2)p.namespaces.coverage=receipt_json::profile(j.at("receiptCoverage"));
    p.namespaces.validate();bool selected=false;
    for(const auto& n:p.namespaces.entries)if(n.namespace_id==r.selected_namespace)selected=true;
    if(!selected||r.selected_namespace==p.namespaces.local_namespace)reject("relay peer namespace must be enrolled and distinct from local");
    if(p.namespaces.coverage && std::find(p.namespaces.coverage->namespaces.begin(),p.namespaces.coverage->namespaces.end(),r.selected_namespace)==p.namespaces.coverage->namespaces.end())reject("relay selected namespace is outside receipt cohort");
    if(j.contains("readyProfile")) {
        r.ready_name=text(j,"readyProfile",32);
        // The historical default remains implicit only. All old accepted
        // explicit names keep their exact bytes and numeric envelopes.
        if(r.ready_name!="bounded48MiBV1"&&r.ready_name!="bounded48MiBOrphanV1"&&r.ready_name!="boundedV1OrphanV1")
            reject("relay explicit READY profile unknown");
    }
    std::optional<int64_t> grace;
    if(r.ready_name=="bounded48MiBOrphanV1"||r.ready_name=="boundedV1OrphanV1")
        grace=number(j,"orphanResumeGraceMilliseconds",1,3600000);
    else if(j.contains("orphanResumeGraceMilliseconds"))reject("relay orphan grace requires explicit lifecycle profile");
    r.ready=canonical_named_ready_profile(text(j,"authority"),p.writer.limits,bool(p.namespaces.coverage),r.ready_name,grace);
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
    std::shared_ptr<instance_guard> owner_guard;
    std::shared_ptr<canonical_writer_adapter> adapter;
    source_recipe recipe;
    std::atomic<size_t> sessions{0};
    std::shared_ptr<authenticated_ready_budget> ready_budget;
    std::mutex ready_mutex;
    std::map<std::string,std::weak_ptr<authenticated_ready_fence>> ready_fences;
    std::shared_ptr<const authenticated_ready_maintenance_test_observation::probe> maintenance_probe;
};
namespace {
std::mutex maintenance_probe_mutex;
std::shared_ptr<const authenticated_ready_maintenance_test_observation::probe> maintenance_probe;
void observe_maintenance(const std::shared_ptr<authenticated_mounted_source>& source,const char* point) {
    const auto& p=source->maintenance_probe;if(p&&p->owner==source->owner.get()&&p->observed)p->observed(point);
}
}
std::shared_ptr<const authenticated_ready_maintenance_test_observation::probe> authenticated_ready_maintenance_test_observation::exchange(std::shared_ptr<const probe> next) {
    std::shared_ptr<const probe> prior;{std::lock_guard lock(maintenance_probe_mutex);prior=std::move(maintenance_probe);maintenance_probe=std::move(next);}return prior;
}
// One bounded registration per existing physical registry slot. No per-tick
// task queue and no SQL, callback or source destruction under the leaf mutex.
struct authenticated_ready_maintenance {
#if defined(__APPLE__) || defined(__linux__)
    struct entry {
        std::shared_ptr<authenticated_mounted_source> source;
        bool armed=false,dirty=true,observed_empty=false;
        std::optional<std::chrono::steady_clock::time_point> due;
    };
    struct shared_state {
        std::mutex mutex;std::condition_variable changed;bool stopped=false;
        std::map<authenticated_mounted_source*,std::shared_ptr<entry>> entries;
    };
    std::shared_ptr<shared_state> shared=std::make_shared<shared_state>();
    std_thread_scheduler executor;
    authenticated_ready_maintenance() {
        // The scheduler owns launch-failure cleanup/join/self-shutdown. If
        // enqueue allocation throws its destructor settles the launched worker.
        executor.invoke([keep=shared]{loop(keep);});
    }
    ~authenticated_ready_maintenance() {
        {std::lock_guard lock(shared->mutex);shared->stopped=true;}
        shared->changed.notify_all();
        // A join failure restores the scheduler's thread custody. Its own
        // destructor retries and provides the state-only detach fallback; do
        // not let the first shutdown error escape this noexcept destructor.
        try{executor.shutdown();}catch(...){}
    }
    static authenticated_ready_maintenance& instance(){static authenticated_ready_maintenance value;return value;}
    static bool live(const std::shared_ptr<authenticated_mounted_source>& source) {
        return source->owner_guard&&source->owner_guard->alive.load(std::memory_order_seq_cst)&&
            source->adapter&&source->adapter->authenticated_active_guard()->load(std::memory_order_acquire);
    }
    static void loop(const std::shared_ptr<shared_state>& state) {
        for(;;) {
            try {
                std::vector<std::shared_ptr<entry>> work,retired;
                {
                    std::unique_lock lock(state->mutex);
                    auto wake=std::chrono::steady_clock::now()+std::chrono::seconds(1);
                    for(const auto& [key,item]:state->entries)if(item->armed){
                        if(item->dirty){wake=std::chrono::steady_clock::now();break;}
                        if(item->due&&*item->due<wake)wake=*item->due;}
                    state->changed.wait_until(lock,wake);
                    const auto now=std::chrono::steady_clock::now();
                    for(auto it=state->entries.begin();it!=state->entries.end();) {
                        const auto& item=it->second;
                        if(state->stopped||!item->source->owner_guard->alive.load(std::memory_order_seq_cst)) {
                            retired.push_back(item);it=state->entries.erase(it);continue;
                        }
                        if(item->armed&&(item->dirty||(item->due&&now>=*item->due))) {
                            // Do not consume the kick while building a batch:
                            // allocation failure must leave every unrun item
                            // eligible for the next bounded retry.
                            work.push_back(item);
                        }
                        ++it;
                    }
                    if(state->stopped){lock.unlock();return;}
                }
                retired.clear(); // owner/adapter/callback destruction off leaf
                for(const auto& item:work) {
                    const auto source=item->source;canonical_ready_maintenance_result result;
                    {std::lock_guard lock(state->mutex);const auto found=state->entries.find(source.get());
                        if(state->stopped||found==state->entries.end()||found->second!=item)continue;
                        item->dirty=false;}
                    try {
                        observe_maintenance(source,"before-maintenance");
                        if(live(source))result=source->adapter->maintain_authenticated_ready(source->owner);
                        observe_maintenance(source,"maintenance-settled");
                        if(result.observed&&!result.next_delay_ms)observe_maintenance(source,"empty-before-retire");
                    } catch(...) {result.observed=false;}
                    const bool still_live=live(source);
                    std::shared_ptr<entry> dropped;
                    {
                        std::lock_guard lock(state->mutex);const auto found=state->entries.find(source.get());
                        if(found==state->entries.end()||found->second!=item)continue;
                        // A concurrent mutation/last-setup release marks dirty
                        // before this decision. It can never be overwritten by
                        // an earlier empty observation; its post-operation kick
                        // also covers a worker that ran before the owned write.
                        if(state->stopped||!still_live) {dropped=std::move(found->second);state->entries.erase(found);}
                        else if(item->dirty)item->due=std::chrono::steady_clock::now();
                        else if(result.settlement.state==recovery_install_state::committed&&result.observed&&
                                !result.settlement.primary_error&&!result.settlement.cleanup_error&&!result.settlement.unexpected_commit_observed) {
                            item->observed_empty=!result.next_delay_ms;
                            item->due=result.next_delay_ms?std::optional{std::chrono::steady_clock::now()+std::chrono::milliseconds(*result.next_delay_ms)}:std::nullopt;
                            if(item->observed_empty&&source->sessions.load(std::memory_order_acquire)==0){dropped=std::move(found->second);state->entries.erase(found);}
                        } else {
                            // A refusal/error is not an empty source. Retain
                            // custody and retry, without a hard wall-time claim
                            // under indefinite contention or corruption.
                            item->observed_empty=false;item->due=std::chrono::steady_clock::now()+std::chrono::seconds(1);
                        }
                    }
                    dropped.reset();
                    observe_maintenance(source,"maintenance-published");
                }
            }catch(...) {
                std::unique_lock lock(state->mutex);if(state->stopped)return;
                state->changed.wait_for(lock,std::chrono::seconds(1));
            }
        }
    }
    static void add(const std::shared_ptr<authenticated_mounted_source>& source,bool armed) {
        if(!source->recipe.ready.orphan_resume_grace_ms)return;
        observe_maintenance(source,"before-maintenance-register");
        auto& worker=instance();
        // Allocate before the leaf. The existing physical registry already
        // charged this source slot; this is not another independent owner cap.
        auto candidate=std::make_shared<entry>();candidate->source=source;candidate->armed=armed;
        {std::lock_guard lock(worker.shared->mutex);
            if(worker.shared->stopped)reject("READY maintenance service stopped");
            const auto [at,added]=worker.shared->entries.emplace(source.get(),candidate);
            if(!added){at->second->armed|=armed;at->second->dirty=true;}}
        worker.shared->changed.notify_one();
    }
    static void kick(const std::shared_ptr<authenticated_mounted_source>& source)noexcept {
        if(!source->recipe.ready.orphan_resume_grace_ms)return;
        try{auto& worker=instance();{std::lock_guard lock(worker.shared->mutex);const auto at=worker.shared->entries.find(source.get());
            if(at!=worker.shared->entries.end())at->second->dirty=true;}worker.shared->changed.notify_one();}catch(...){}
    }
    static void arm(const std::shared_ptr<authenticated_mounted_source>& source)noexcept {
        if(!source->recipe.ready.orphan_resume_grace_ms)return;
        // Enrollment already owns an allocated registration. No callback or
        // allocation may fail between its known COMMIT and arming the keeper.
        try{auto& worker=instance();{std::lock_guard lock(worker.shared->mutex);const auto at=worker.shared->entries.find(source.get());
            if(at!=worker.shared->entries.end()){at->second->armed=true;at->second->dirty=true;}}worker.shared->changed.notify_one();}catch(...){}
    }
    static void remove(const std::shared_ptr<authenticated_mounted_source>& source)noexcept {
        if(!source->recipe.ready.orphan_resume_grace_ms)return;
        try{auto& worker=instance();std::shared_ptr<entry> retired;
            {std::lock_guard lock(worker.shared->mutex);const auto at=worker.shared->entries.find(source.get());if(at!=worker.shared->entries.end()){retired=std::move(at->second);worker.shared->entries.erase(at);}}
            worker.shared->changed.notify_one();retired.reset();}catch(...){}
    }
#else
    static void add(const std::shared_ptr<authenticated_mounted_source>& source,bool){if(source->recipe.ready.orphan_resume_grace_ms)reject("READY lifecycle platform unavailable");}
    static void kick(const std::shared_ptr<authenticated_mounted_source>&)noexcept{}
    static void arm(const std::shared_ptr<authenticated_mounted_source>&)noexcept{}
    static void remove(const std::shared_ptr<authenticated_mounted_source>&)noexcept{}
#endif
};
struct authenticated_ready_budget : canonical_ready_transport_limits {
    // Keep only the payload-free physical identity and bounded immutable recipe.
    // A queued result may outlive the last mounted source without retaining its
    // owner/adapter or running their destructors on a socket callback.
    const std::shared_ptr<const physical_store_identity> physical;
    const std::string recipe_key;
    std::mutex mutex;
    uint64_t requests=0,bytes=0,workspace=0;
    authenticated_ready_budget(std::shared_ptr<const physical_store_identity> identity,std::string key):physical(std::move(identity)),recipe_key(std::move(key)){}
};
struct authenticated_ready_fence {
    std::atomic<bool> current{false};
    const std::shared_ptr<std::atomic<bool>> admitted=std::make_shared<std::atomic<bool>>(true);
    const int64_t deadline;
    explicit authenticated_ready_fence(int64_t value):deadline(value){}
    bool live()const noexcept{return admitted->load(std::memory_order_acquire)&&current.load(std::memory_order_acquire)&&authenticated_session_fence::now()<deadline;}
};
authenticated_ready_charge::~authenticated_ready_charge(){if(budget_){std::lock_guard lock(budget_->mutex);--budget_->requests;budget_->bytes-=charged_;}}
namespace {
struct registry_slot {
    std::weak_ptr<authenticated_mounted_source> value;
    std::weak_ptr<authenticated_ready_budget> budget;
    bool building=false;
};
std::mutex registry_mutex;
using physical_key=std::pair<uint64_t,uint64_t>;
std::map<physical_key,registry_slot> registry;
std::atomic<uint64_t> turns{0};
}
struct authenticated_relay_setup::state {
    std::shared_ptr<authenticated_mounted_source> source;
    std::shared_ptr<authenticated_session_fence> fence;
    std::shared_ptr<route_lifetime> route;
    source_recipe recipe;
    json context;
    std::optional<canonical_namespace_admission> admission;
    std::optional<recovery_receipt_binding> receipt_binding;
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
    ~state(){if(charged){source->sessions.fetch_sub(1,std::memory_order_acq_rel);authenticated_ready_maintenance::kick(source);}}
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
thread_local const std::function<void()>* authenticated_relay_setup::admin_before_open_test_hook_=nullptr;
// Only these two closed source operations can hold this unpublished owner.
// Destruction releases the SQLite owner before opening the physical registry
// slot, and closes directory descriptors last. No main-file descriptor exists.
struct authenticated_relay_setup::source_file_administration {
#if defined(__APPLE__) || defined(__linux__)
    int parent_fd=-1;
    std::filesystem::path requested;
    std::string name;
    struct stat parent_stat{},main_stat{};
    std::shared_ptr<const physical_store_identity> expected;
    physical_key key{};
    bool reserved=false;
    std::shared_ptr<database> writer;
    std::shared_ptr<lattice_db> owner;
    ~source_file_administration() {
        owner.reset();writer.reset();
        if(reserved){std::lock_guard lock(registry_mutex);auto i=registry.find(key);if(i!=registry.end())i->second.building=false;}
        if(parent_fd>=0)::close(parent_fd);
    }
    void verify()const {
        struct stat current_parent{},current_main{};
        if(::stat(requested.parent_path().c_str(),&current_parent)!=0||!S_ISDIR(current_parent.st_mode)||
           current_parent.st_dev!=parent_stat.st_dev||current_parent.st_ino!=parent_stat.st_ino||
           ::fstatat(parent_fd,name.c_str(),&current_main,AT_SYMLINK_NOFOLLOW)!=0||!S_ISREG(current_main.st_mode)||current_main.st_nlink!=1||
           current_main.st_dev!=main_stat.st_dev||current_main.st_ino!=main_stat.st_ino)
            reject("receipt administration intended file/parent changed");
    }
    void verify_writer()const {
        verify();const auto actual=writer->physical_identity("main",{},true);
        if(!actual||*actual!=*expected||actual->filename!=expected->filename)
            reject("receipt administration actual SQLite identity changed");
    }
#endif
};
std::unique_ptr<authenticated_relay_setup::source_file_administration>
authenticated_relay_setup::open_administrative_file(const std::string& path,recovery_owner_schema catalog,
    int64_t schema_version,int busy_timeout_ms) {
#if defined(__APPLE__) || defined(__linux__)
    if(path.empty()||path.size()>4096||path.find('\0')!=std::string::npos||path.starts_with("file:")||
       !std::filesystem::path(path).is_absolute()||schema_version<1||schema_version>INT32_MAX||
       busy_timeout_ms<0||busy_timeout_ms>30000||!catalog.valid())reject("receipt administration bounded intended-file contract required");
    auto session=std::make_unique<source_file_administration>();auto& s=*session;
    s.requested=std::filesystem::path(path);s.name=s.requested.filename().string();
    if(s.name.empty()||s.name=="."||s.name=="..")reject("receipt administration regular file required");
    std::error_code error;const auto parent=std::filesystem::canonical(s.requested.parent_path(),error);
    if(error)reject("receipt administration existing parent unavailable");
    s.parent_fd=::open(parent.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
    if(s.parent_fd<0||::fstat(s.parent_fd,&s.parent_stat)!=0||!S_ISDIR(s.parent_stat.st_mode)||
       ::fstatat(s.parent_fd,s.name.c_str(),&s.main_stat,AT_SYMLINK_NOFOLLOW)!=0||!S_ISREG(s.main_stat.st_mode)||s.main_stat.st_nlink!=1)
        reject("receipt administration existing regular main/parent required");
    auto expected=std::make_shared<physical_store_identity>();expected->device=s.main_stat.st_dev;expected->inode=s.main_stat.st_ino;
    expected->filename=(parent/s.name).string();s.expected=std::move(expected);
    s.verify();s.key={s.expected->device,s.expected->inode};
    {
        std::lock_guard lock(registry_mutex);
        for(auto i=registry.begin();i!=registry.end();) {
            if(!i->second.building&&i->second.value.expired()&&i->second.budget.expired())i=registry.erase(i);else ++i;
        }
        auto i=registry.find(s.key);
        if(i!=registry.end()&&(i->second.building||!i->second.value.expired()||!i->second.budget.expired()))return {};
        if(i==registry.end()&&registry.size()>=128)return {};
        registry[s.key].building=true;s.reserved=true;
    }
    const auto observation=admin_before_open_test_hook_?*admin_before_open_test_hook_:std::function<void()>{};
    if(observation)observation();
    s.writer=std::make_shared<database>(s.expected->filename,database::open_mode::read_write,busy_timeout_ms,
        std::shared_ptr<database_read_control>{},database::initialization_key(s.expected));
    s.verify_writer();
    const auto& writer=s.writer;
    // Lattice's declared version is the existing metadata row. SQLite's
    // user_version is an independent application header, never that authority.
    // Refuse absent/malformed metadata instead of the ordinary owner's fallback
    // or ensure path. Bound every returned spelling before allocating it.
    const auto meta=writer->query("SELECT type,CASE WHEN length(CAST(tbl_name AS BLOB))<=64 THEN tbl_name END AS name,rootpage "
        "FROM main.sqlite_schema WHERE name='_lattice_meta' COLLATE BINARY LIMIT 2");
    if(meta.size()!=1||!std::holds_alternative<std::string>(meta[0].at("type"))||std::get<std::string>(meta[0].at("type"))!="table"||
       !std::holds_alternative<std::string>(meta[0].at("name"))||std::get<std::string>(meta[0].at("name"))!="_lattice_meta"||
       !std::holds_alternative<int64_t>(meta[0].at("rootpage"))||std::get<int64_t>(meta[0].at("rootpage"))<=0)
        reject("receipt administration existing metadata table required; no repair");
    const auto columns=writer->query("SELECT cid,CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END AS name,"
        "CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END AS type,[notnull] AS required,"
        "dflt_value IS NULL AS no_default,pk,hidden FROM pragma_table_xinfo('_lattice_meta','main') ORDER BY cid LIMIT 3");
    if(columns.size()!=2)reject("receipt administration metadata shape differs; no repair");
    for(size_t i=0;i<2;++i) {
        const auto& c=columns[i];const auto integer=[&](const char* key,int64_t value){const auto& v=c.at(key);return std::holds_alternative<int64_t>(v)&&std::get<int64_t>(v)==value;};
        const auto spelling=[&](const char* key,const char* value){const auto& v=c.at(key);return std::holds_alternative<std::string>(v)&&std::get<std::string>(v)==value;};
        if(!integer("cid",i)||!spelling("name",i==0?"key":"value")||!spelling("type","TEXT")||
           !integer("required",i==0?0:1)||!integer("no_default",1)||!integer("pk",i==0?1:0)||!integer("hidden",0))
            reject("receipt administration metadata shape differs; no repair");
    }
    const auto version=writer->query("SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS version "
        "FROM main._lattice_meta WHERE key='schema_version' COLLATE BINARY LIMIT 2");
    if(version.size()!=1||!std::holds_alternative<std::string>(version[0].at("version"))||
       std::get<std::string>(version[0].at("version"))!=std::to_string(schema_version))
        reject("receipt administration declared schema version differs; no migration");
    configuration config(s.expected->filename);config.busy_timeout_ms=busy_timeout_ms;config.target_schema_version=static_cast<int32_t>(schema_version);
    s.owner=std::shared_ptr<lattice_db>(new lattice_db(config,std::move(catalog),s.writer));
    s.verify();return session;
#else
    (void)path;(void)catalog;(void)schema_version;(void)busy_timeout_ms;
    reject("receipt administration platform unqualified");
#endif
}
bool authenticated_relay_setup::migrate_receipt_coverage_file(const std::string& path,recovery_owner_schema catalog,
    int64_t schema_version,int busy_timeout_ms,const std::string& before_bytes,const std::string& after_bytes) {
#if defined(__APPLE__) || defined(__linux__)
    // Parse the exact declared catalog/policies before opening any file.
    const auto before_json=bounded(before_bytes,policy_bytes),after_json=bounded(after_bytes,policy_bytes);
    const auto before=recipe(catalog,before_json),after=recipe(catalog,after_json);
    if(before.profile.namespaces.coverage||!after.profile.namespaces.coverage)
        reject("receipt migration requires exact v2 to registered v3 transition");
    auto common=after_json;common.erase("receiptCoverage");common["version"]=1;
    if(before_json.contains("readyProfile"))common["readyProfile"]=before_json.at("readyProfile");else common.erase("readyProfile");
    if(common!=before_json)reject("receipt migration cannot change source, namespace catalog, models or permissions");
    auto session=open_administrative_file(path,std::move(catalog),schema_version,busy_timeout_ms);
    if(!session)return false;
    canonical_writer_adapter::migrate_authenticated_source(session->owner,after.profile,
        {frame_entries,65536,frame_bytes},{64,3600000},before.ready,after.ready);
    session->verify_writer();return true;
#else
    (void)path;(void)catalog;(void)schema_version;(void)busy_timeout_ms;(void)before_bytes;(void)after_bytes;
    reject("receipt administration platform unqualified");
#endif
}
authenticated_lifecycle_adoption_result authenticated_relay_setup::adopt_lifecycle_file(const std::string& path,
    recovery_owner_schema catalog,int64_t schema_version,int busy_timeout_ms,
    const std::string& before_bytes,const std::string& after_bytes) {
    authenticated_lifecycle_adoption_result out;
#if defined(__APPLE__) || defined(__linux__)
    const auto before_json=bounded(before_bytes,policy_bytes),after_json=bounded(after_bytes,policy_bytes);
    const auto before=recipe(catalog,before_json),after=recipe(catalog,after_json);
    const auto expected=before.ready_name=="boundedV1"?"boundedV1OrphanV1":"bounded48MiBOrphanV1";
    if((before.ready_name!="boundedV1"&&before.ready_name!="bounded48MiBV1")||
       after.ready_name!=expected||!after.ready.orphan_resume_grace_ms)
        reject("lifecycle adoption requires an exact old named profile and explicit finite grace");
    auto common=after_json;common.erase("orphanResumeGraceMilliseconds");
    if(before_json.contains("readyProfile"))common["readyProfile"]=before_json.at("readyProfile");else common.erase("readyProfile");
    if(common!=before_json)reject("lifecycle adoption cannot change source, receipt identity, permissions or capacity");
    auto session=open_administrative_file(path,std::move(catalog),schema_version,busy_timeout_ms);
    if(!session){out.pending_quiescence=true;return out;}
    out.adoption=canonical_writer_adapter::adopt_authenticated_lifecycle(session->owner,before.profile,
        {frame_entries,65536,frame_bytes},{64,3600000},before.ready,before.ready_name,*after.ready.orphan_resume_grace_ms);
    try {session->verify_writer();}
    catch(...) {
        auto& error=out.adoption.settlement.state==recovery_install_state::committed?
            out.adoption.settlement.postcommit_error:out.adoption.settlement.primary_error;
        if(!error)error=std::current_exception();out.adoption.record.reset();out.adoption.disposition.reset();
    }
    return out;
#else
    (void)path;(void)catalog;(void)schema_version;(void)busy_timeout_ms;(void)before_bytes;(void)after_bytes;
    reject("lifecycle administration platform unqualified");
#endif
}
bool authenticated_relay_setup::migrate_receipt_coverage(std::shared_ptr<lattice_db> owner,
    const std::string& before_bytes,const std::string& after_bytes) {
    if(!owner)reject("receipt migration requires the resolved mount owner");
    const auto before_json=bounded(before_bytes,policy_bytes),after_json=bounded(after_bytes,policy_bytes);
    const auto& catalog=canonical_writer_adapter::authenticated_catalog(*owner);
    const auto before=recipe(catalog,before_json),after=recipe(catalog,after_json);
    if(before.profile.namespaces.coverage||!after.profile.namespaces.coverage)
        reject("receipt migration requires exact v2 to registered v3 transition");
    auto common=after_json;common.erase("receiptCoverage");common["version"]=1;
    if(before_json.contains("readyProfile"))common["readyProfile"]=before_json.at("readyProfile");else common.erase("readyProfile");
    if(common!=before_json)reject("receipt migration cannot change source, namespace catalog, models or permissions");
    const auto identity=canonical_writer_adapter::authenticated_owner_guard(*owner);
    if(!identity||!identity->alive.load(std::memory_order_seq_cst))reject("receipt migration owner retired");
    const auto physical=canonical_writer_adapter::authenticated_physical_identity(*owner);
    const physical_key key{physical->device,physical->inode};
    {
        std::lock_guard lock(registry_mutex);
        for(auto i=registry.begin();i!=registry.end();) {
            if(!i->second.building&&i->second.value.expired()&&i->second.budget.expired())i=registry.erase(i);else ++i;
        }
        auto i=registry.find(key);
        if(i!=registry.end()&&(i->second.building||!i->second.value.expired()||!i->second.budget.expired()))return false;
        if(i==registry.end()&&registry.size()>=128)return false;
        registry[key].building=true;
    }
    const auto release=[&] {
        std::lock_guard lock(registry_mutex);
        auto i=registry.find(key);if(i!=registry.end())i->second.building=false;
    };
    try {
        canonical_writer_adapter::migrate_authenticated_source(owner,after.profile,
            {frame_entries,65536,frame_bytes},{64,3600000},before.ready,after.ready);
        release();return true;
    } catch(...) {release();throw;}
}
std::shared_ptr<authenticated_relay_setup> authenticated_relay_setup::open(std::shared_ptr<lattice_db> owner,
    const std::string& policy,const std::string& connection,void* context,int32_t(*current)(void*),void(*destroy)(void*)) {
    return open_impl(std::move(owner),policy,connection,context,current,destroy,nullptr,nullptr);
}
thread_local uint64_t* authenticated_relay_setup::setup_registry_entries_test_counter_=nullptr;
std::shared_ptr<authenticated_relay_setup> authenticated_relay_setup::open_automatic(std::shared_ptr<lattice_db> owner,
    const std::string& policy,const std::string& connection,void* context,int32_t(*current)(void*),
    int32_t(*admissible)(void*),void(*destroy)(void*),bool& pre_effect_busy) {
    pre_effect_busy=false;
    return open_impl(std::move(owner),policy,connection,context,current,destroy,admissible,&pre_effect_busy);
}
std::shared_ptr<authenticated_relay_setup> authenticated_relay_setup::open_impl(std::shared_ptr<lattice_db> owner,
    const std::string& policy,const std::string& connection,void* context,int32_t(*current)(void*),void(*destroy)(void*),
    int32_t(*admissible)(void*),bool* pre_effect_busy) {
    // A supplied nonthrowing destroy transfers route custody on EVERY outcome.
    if(!destroy)reject("relay route destroy required before ownership transfer");
    std::shared_ptr<void> retained(context,destroy);
    if(pre_effect_busy&&!admissible)reject("automatic relay setup admission callback required");
    if(!owner||!context||!current||current(context)!=1)reject("relay actual live route required");
    auto r=recipe(canonical_writer_adapter::authenticated_catalog(*owner),bounded(policy,policy_bytes));auto c=bounded(connection,connection_bytes);
    shape(c,{"mount","connection","channel","authenticatedUserID","peer"});
    c["mount"]=uuid(c,"mount");c["connection"]=uuid(c,"connection");(void)text(c,"channel",64);c["authenticatedUserID"]=uuid(c,"authenticatedUserID");
    shape(c.at("peer"),{"replicaID","receiverIncarnation","channelIncarnation"});
    (void)text(c.at("peer"),"replicaID");c["peer"]["receiverIncarnation"]=uuid(c.at("peer"),"receiverIncarnation");c["peer"]["channelIncarnation"]=uuid(c.at("peer"),"channelIncarnation");
    const auto owner_guard=canonical_writer_adapter::authenticated_owner_guard(*owner);
    if(!owner_guard)reject("relay actual owner identity unavailable");
    bool busy_before_registry=false;
    const auto physical=pre_effect_busy?
        canonical_writer_adapter::try_authenticated_physical_identity(*owner,context,current,admissible,busy_before_registry):
        canonical_writer_adapter::authenticated_physical_identity(*owner);
    if(busy_before_registry){*pre_effect_busy=true;return {};}
    if(setup_registry_entries_test_counter_)++*setup_registry_entries_test_counter_;
    const physical_key key{physical->device,physical->inode};
    std::shared_ptr<authenticated_mounted_source> source;
    std::shared_ptr<authenticated_ready_budget> budget;
    bool busy=false,profile_differs=false;
    {
        std::lock_guard lock(registry_mutex);
        for(auto i=registry.begin();i!=registry.end();) {
            if(!i->second.building&&i->second.value.expired()&&i->second.budget.expired())i=registry.erase(i);else ++i;
        }
        auto i=registry.find(key);
        if(i!=registry.end()) {
            source=i->second.value.lock();budget=i->second.budget.lock();busy=i->second.building;
            profile_differs=budget&&budget->recipe_key!=r.key;
            // Physical capacity spans owner turnover, while a live source may
            // be reused only by its exact instance. Never attach a new owner
            // to a retired owner's context or release that owner under lock.
            busy=busy||(source&&source->owner_guard!=owner_guard);
        }
        if(!source&&!busy&&!profile_differs) {
            if(i==registry.end()&&registry.size()>=128)busy=true;
            else {
                if(!budget)budget=std::make_shared<authenticated_ready_budget>(physical,r.key);
                auto& slot=registry[key];slot.budget=budget;slot.building=true;
            }
        }
    }
    if(profile_differs)reject("relay source profile differs on actual owner");
    if(busy)reject("relay bounded source enrollment busy");
    if(source&&source->recipe.key!=r.key)reject("relay source profile differs on actual owner");
    if(!source) {
        try {
            source=std::make_shared<authenticated_mounted_source>();source->owner=owner;source->owner_guard=owner_guard;source->recipe=r;
            source->ready_budget=budget;
            {std::lock_guard lock(maintenance_probe_mutex);source->maintenance_probe=maintenance_probe;}
            // Register/start before enrollment can publish durable state. The
            // worker cannot enter an unarmed source under construction.
            authenticated_ready_maintenance::add(source,false);
            source->adapter=canonical_writer_adapter::open_authenticated_source(owner,r.profile,{frame_entries,65536,frame_bytes},{64,3600000},r.ready,true);
            authenticated_ready_maintenance::arm(source);
            std::lock_guard lock(registry_mutex);auto& slot=registry.at(key);slot.value=source;slot.building=false;
        }catch(...) {
            // Failed re-enrollment must not erase charges retained by an old
            // result. Only an expired source AND budget can release the slot.
            if(source)authenticated_ready_maintenance::remove(source);
            std::lock_guard lock(registry_mutex);registry.at(key).building=false;throw;
        }
    }
    auto s=std::make_shared<state>();s->source=std::move(source);
    auto count=s->source->sessions.load(std::memory_order_relaxed);
    do{if(count>=1024)reject("relay native source session capacity");}
    while(!s->source->sessions.compare_exchange_weak(count,count+1,std::memory_order_relaxed));
    s->charged=true;s->recipe=std::move(r);
    // Also re-register an existing empty source retained by a concurrent open
    // after the worker removed its last empty/unused registration.
    authenticated_ready_maintenance::add(s->source,true);
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
    if(p.namespaces.coverage)s->context["source"]["receiptCoverage"]=receipt_json::encode(*p.namespaces.coverage);
    return std::shared_ptr<authenticated_relay_setup>(new authenticated_relay_setup(std::move(s)));
}
std::string authenticated_relay_setup::descriptor()const{if(!state_||state_->fence->stopped()||!state_->route->live())reject("relay setup retired");return state_->context.dump();}
bool authenticated_relay_setup::finish_authorization(const std::string& raw) {
    auto s=state_;if(!s||s->consumed||s->fence->stopped()||!s->route->live())return false;
    s->consumed=true; // A rejected/throwing outcome cannot be edited and retried.
    try {
        auto value=bounded(raw,policy_bytes);auto base=value;base.erase("receiptCoverage");
        shape(base,{"context","authenticatedUserID","peer","source","incomingScope","authorizationRevision","validForMilliseconds"});
        if(bool(s->recipe.profile.namespaces.coverage)!=value.contains("receiptCoverage"))reject("relay authorization receipt coverage profile differs");
        if(s->recipe.profile.namespaces.coverage)s->receipt_binding=receipt_json::authorization(value.at("receiptCoverage"),*s->recipe.profile.namespaces.coverage);
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
            text(s->context.at("route").at("peer"),"replicaID"),s->fence,s->receipt_binding);
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
json ready_profile_description(const source_recipe& r) {
    return canonical_ready_profile_description(r.ready,r.ready_name);
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
    // Capture/package workspace belongs to this synchronous call. Declare its
    // reservation before every parsed/request/result local so it retires last.
    // The separate input/reply charge and counted operation still follow the
    // returned response through its final publication or disposal.
    struct workspace_reservation {
        std::shared_ptr<authenticated_ready_budget> budget;
        uint64_t bytes=0;
        ~workspace_reservation(){if(bytes){std::lock_guard lock(budget->mutex);budget->workspace-=bytes;}}
    } workspace{s->source->ready_budget};
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
        if(s->receipt_binding)response["receiptBinding"]=receipt_json::encode(*s->receipt_binding);
        response["upload"]={{"maximumEntries",frame_entries},{"maximumWireBytes",frame_bytes},{"maximumScalarBytes",65536},{"parserNodes",32768},{"parserDepth",16},{"maximumDeletes",s->recipe.maximum_deletes}};return output();
    }
    if(ready_decimal(control,"routeGeneration")!=s->route_generation)reject("READY physical setup generation differs");
    if(op=="read") {
        shape(control,{"kind","version","operation","requestID","routeGeneration","leaseID","requestDigest","attemptID","sequence","index"});
        if(!s->ready||text(control,"leaseID",128)!=s->ready->id||text(control,"requestDigest",64)!=s->ready->request_digest||
            uuid(control,"attemptID")!=s->ready->attempt.attempt_id||ready_decimal(control,"sequence")!=s->ready->attempt.sequence||!s->ready->fence->live())
            reject("READY current setup lease or exact attempt/Q differs");
        const auto held=*s->ready; // Capture this exact fence BEFORE the transaction.
        const auto read_admission=canonical_writer_adapter::ready_operation_admission(*s->admission,held.fence->admitted);
        const auto read=s->source->adapter->read_authenticated_ready_frame_owned(s->source->owner,read_admission,held.lease,ready_decimal(control,"index",0));
        if(read.settlement.state==recovery_install_state::committed&&read.frame) {
            operation->ready_=held.fence;
            if(read.frame->size()>authenticated_ready_budget::reply_limit)reject("READY returned frame bound");
            return {1,*read.frame,std::move(operation),request_id};
        }
        response["settlement"]=ready_settlement(read.settlement);response["frameAvailable"]=false;return output();
    }
    const bool lifecycle=s->recipe.ready.orphan_resume_grace_ms.has_value();
    if(op!="prepare"&&op!="resume"&&op!="discard"&&!(lifecycle&&(op=="inspect"||op=="predecessor")))reject("READY control operation unknown");
    // A post-operation kick is essential: an earlier empty scan may have run
    // after the pre-operation kick but before this call acquired its WRITE.
    struct maintenance_kick {std::shared_ptr<authenticated_mounted_source> source;~maintenance_kick(){if(source)authenticated_ready_maintenance::kick(source);}} maintenance{op=="inspect"||op=="predecessor"?nullptr:s->source};
    if(op!="inspect"&&op!="predecessor")authenticated_ready_maintenance::kick(s->source);
    if(op=="prepare") {
        // Finite source-wide charge while capture, canonical vectors, capsule
        // strings and request copies are live in this call. This is logical
        // storage accounting, not allocator/container/SQLite/NIO RSS measurement.
        const auto& p=s->recipe.ready;
        const uint64_t bytes=3*p.capture.rows.wire.total_bytes+2*p.package.retained_wire_bytes+8*p.package.codec.maximum.frame_bytes;
        std::lock_guard lock(workspace.budget->mutex);
        if(bytes>authenticated_ready_budget::max_workspace-workspace.budget->workspace)reject("READY source capture workspace unavailable");
        workspace.bytes=bytes;workspace.budget->workspace+=bytes;
    }
    if(op=="predecessor")shape(control,{"kind","version","operation","requestID","routeGeneration","request","priorProfile"});
    else if(op=="discard"||op=="inspect")shape(control,{"kind","version","operation","requestID","routeGeneration","request"});
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
    const auto duration=op=="discard"||op=="inspect"||op=="predecessor"?int64_t(1):number(control,"durationMilliseconds",1,static_cast<int64_t>(codec.lease_ms));
    if(now>INT64_MAX-duration||now+duration>s->fence->deadline_.load(std::memory_order_acquire)||!s->fence->live()||!s->route->live())
        reject("READY finite lease exceeds actual authorization or route");
    const auto lifecycle_output=[&](const canonical_ready_lifecycle_result& result) {
        response["settlement"]=ready_settlement(result.settlement);response["leaseAvailable"]=false;
        if(result.settlement.state==recovery_install_state::committed&&result.disposition) {
            const char* state=*result.disposition==canonical_ready_lifecycle_state::available?"available":
                *result.disposition==canonical_ready_lifecycle_state::terminal?"terminal":"unstarted";
            response["lifecycle"]={{"state",state},{"requestDigest",request->request_digest},{"attemptID",logical.attempt_id},
                {"sequence",std::to_string(logical.sequence)},{"bindingHighWater",std::to_string(result.binding_high_water)},
                {"namespaceID",s->recipe.selected_namespace},{"replicaID",peer.at("replicaID")},
                {"receiverIncarnation",logical.receiver_incarnation},{"channelIncarnation",logical.channel_incarnation},{"channel",logical.channel}};
        }
        return output();
    };
    if(op=="predecessor") {
        const auto prior=predecessor_wire::canonical_profile(control.at("priorProfile"),bool(s->recipe.profile.namespaces.coverage));
        predecessor_wire::pair(control.at("priorProfile"),ready_profile_description(s->recipe),bool(s->recipe.profile.namespaces.coverage));
        if(ready_before_owned_test_hook_)(*ready_before_owned_test_hook_)();
        const auto result=s->source->adapter->inspect_authenticated_predecessor(s->source->owner,*s->admission,logical,*request,prior);
        response["settlement"]=ready_settlement(result.settlement);response["leaseAvailable"]=false;
        if(result.settlement.state==recovery_install_state::committed&&result.facts){const auto& f=*result.facts;
            response["predecessor"]={{"version",1},{"transitionID",f.transition_id},{"transitionDigest",f.transition_digest},
                {"beforeProfileDigest",f.before_profile_digest},{"afterProfileDigest",f.after_profile_digest},{"sourceIdentityDigest",f.source_identity_digest},
                {"disposition","preserveCompleted"},{"requestDigest",request->request_digest},{"attemptID",logical.attempt_id},{"sequence",std::to_string(logical.sequence)},
                {"namespaceID",s->recipe.selected_namespace},{"replicaID",peer.at("replicaID")},{"receiverIncarnation",logical.receiver_incarnation},
                {"channelIncarnation",logical.channel_incarnation},{"channel",logical.channel}};
            if(response["predecessor"].dump().size()>predecessor_wire::body_bytes)reject("READY predecessor body bound");
        }
        return output();
    }
    if(op=="inspect") {
        // Pure inspection does not revoke a physical lease or infer expiry.
        if(ready_before_owned_test_hook_)(*ready_before_owned_test_hook_)();
        return lifecycle_output(s->source->adapter->inspect_authenticated_ready(s->source->owner,*s->admission,logical,*request,false));
    }
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
        if(lifecycle)return lifecycle_output(s->source->adapter->inspect_authenticated_ready(s->source->owner,ready_admission,logical,*request,true));
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
