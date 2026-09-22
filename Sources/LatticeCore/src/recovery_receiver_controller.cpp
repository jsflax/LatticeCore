#include "recovery_receiver_controller.hpp"
#include "recovery_request_store.hpp"
#include "canonical_writer_adapter.hpp"
#include "recovery_witness.hpp"
#include "sync_callback_lifetime.hpp"
#include "vendor/picosha2/picosha2.h"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <set>

namespace lattice::detail {
namespace {
namespace cr=canonical_range;
using json=nlohmann::json;
constexpr size_t pending_bytes=16777216;
[[noreturn]] void refuse(const char* reason){throw db_error(reason);}
void require(bool value,const char* reason){if(!value)refuse(reason);}
int64_t now(){return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
json parse(const std::string& raw,size_t cap) {
    require(!raw.empty()&&raw.size()<=cap,"controller frame byte capacity");
    size_t nodes=0;std::vector<std::set<std::string>> keys;
    return json::parse(raw,[&](int depth,json::parse_event_t event,json& value){
        require(depth>=0&&depth<=20&&++nodes<=262144,"controller frame structure capacity");
        if(value.is_string())require(value.get_ref<const std::string&>().size()<=4194304,"controller scalar capacity");
        if(event==json::parse_event_t::object_start)keys.emplace_back();
        if(event==json::parse_event_t::key)require(!keys.empty()&&keys.back().insert(value.get<std::string>()).second,"controller duplicate member");
        if(event==json::parse_event_t::object_end)keys.pop_back();return true;
    });
}
void members(const json& value,std::initializer_list<const char*> required,std::initializer_list<const char*> optional={}) {
    require(value.is_object(),"controller control object required");std::set<std::string> allowed;
    for(const auto* key:required){require(value.contains(key),"controller required control member missing");allowed.insert(key);}
    for(const auto* key:optional)allowed.insert(key);
    for(const auto& item:value.items())require(allowed.count(item.key()),"controller unknown control member");
}
void settlement_shape(const json& value) {
    members(value,{"state","unexpectedCommitObserved","primaryError","cleanupError","postcommitError","notificationError"});
    require(value.at("state").is_string(),"controller settlement state type");const auto state=value.at("state").get<std::string>();
    require(state=="refused"||state=="rolledBack"||state=="committed"||state=="unsettled"||state=="ownershipLost"||state=="unknown","controller settlement state unknown");
    for(const auto* key:{"unexpectedCommitObserved","primaryError","cleanupError","postcommitError","notificationError"})require(value.at(key).is_boolean(),"controller settlement flag type");
}
int64_t integer(const database::row_t& row,const char* key) {
    const auto it=row.find(key);require(it!=row.end()&&std::holds_alternative<int64_t>(it->second),"controller durable integer unavailable");return std::get<int64_t>(it->second);
}
uint64_t decimal(const json& row,const char* key,uint64_t minimum=1) {
    const auto& value=row.at(key);require(value.is_string(),"controller normalized decimal required");
    const auto& text=value.get_ref<const std::string&>();require(!text.empty()&&text.size()<=19,"controller decimal capacity");
    uint64_t n=0;const auto parsed=std::from_chars(text.data(),text.data()+text.size(),n);
    require(parsed.ec==std::errc{}&&parsed.ptr==text.data()+text.size()&&n>=minimum&&n<=uint64_t(INT64_MAX)&&std::to_string(n)==text,"controller invalid decimal");return n;
}
std::string source_context(const json& description) {
    json value;for(const auto* key:{"source","incomingScope","peer","channel","profile","upload"})value[key]=description.at(key);
    auto raw=value.dump();require(raw.size()<=recovery_request_store::context_bytes,"controller source context capacity");return raw;
}
std::string domain(const json& description) {
    auto value=json{{"source",description.at("source")},{"incomingScope",description.at("incomingScope")}};
    auto& source=value["source"];for(const auto* key:{"receiptNamespace","coverageID","coverageRevision","descriptorDigest"})source.erase(key);
    return picosha2::hash256_hex_string(value.dump());
}
cr::source_binding source_binding(const recovery_obligation_profile& profile) {
    const auto& b=profile.binding;return {b.authority,b.source,b.epoch,b.scope,b.schema};
}
void known(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed){if(result.primary_error)std::rethrow_exception(result.primary_error);refuse("controller transaction outcome unavailable; gate remains closed");}
    // Postcommit notification failure cannot roll back or justify model replay.
    // The next actual owned inspection observes the committed durable phase.
}
canonical_scoped_contract contract(const json& description,const recovery_continuous_contribution& contribution,const recovery_owner_schema& catalog) {
    require(catalog.valid()&&catalog.swift_digest.size()==64,"controller actual Swift catalog unavailable");
    const auto& source=description.at("source");const auto& binding=contribution.profile.binding;
    require(description.at("channel")==binding.channel&&source.at("authority")==binding.authority&&source.at("sourceID")==binding.source&&
        source.at("epoch")==binding.epoch&&source.at("scopeDigest")==binding.scope&&source.at("schemaDigest")==binding.schema&&
        source.at("receiptNamespace")==contribution.profile.receipt_namespace&&source.at("schemaDigest")==catalog.swift_digest,
        "controller source differs from enrolled contribution or actual catalog");
    const auto& incoming=description.at("incomingScope");
    const std::string claimed(contribution.incoming_grant_claim.begin(),contribution.incoming_grant_claim.end());
    require(parse(claimed,65536)==incoming,"controller actual incoming authorization differs from enrolled claim");
    require(incoming.at("catalogDigest")==catalog.swift_digest,"controller incoming catalog differs");
    canonical_scoped_contract result;const std::set<std::string> all{"INSERT","UPDATE","DELETE"};
    const auto operations=[&](const json& value){std::set<std::string> mask;for(const auto& op:value.at("incomingOperations"))mask.insert(op.get<std::string>());require(mask==all,"controller full replacement operation scope incomplete");};
    for(const auto& model:incoming.at("models")){operations(model);const auto name=model.at("table").get<std::string>();require(catalog.find(name)&&catalog.swift_models.count(name),"controller incoming model not actual Swift declaration");result.model_tables.push_back(name);}
    auto actual=result.model_tables,expected=contribution.models;std::sort(actual.begin(),actual.end());std::sort(expected.begin(),expected.end());require(actual==expected,"controller model contribution coverage differs");
    for(const auto& relation:incoming.at("relations")){operations(relation);result.relations.push_back({relation.at("table"),relation.at("lhsModel"),relation.at("rhsModel")});}
    for(const auto& link:incoming.at("scopedLinkTables"))result.scoped_link_tables.push_back(link.get<std::string>());
    // Relation spelling/column closure is independently checked against the
    // same actual SQLite catalog by the installer before model effects.
    return result;
}
cr::wire_limits negotiate(const json& description,const cr::limits& local) {
    const auto& remote=description.at("profile").at("wire");auto value=local.maximum;
    value.frame_bytes=std::min(value.frame_bytes,decimal(remote,"frame_bytes"));value.payload_bytes=std::min(value.payload_bytes,decimal(remote,"payload_bytes"));
    value.items_per_page=std::min(value.items_per_page,decimal(remote,"items_per_page"));value.content_pages=std::min(value.content_pages,decimal(remote,"content_pages"));
    value.content_identities=std::min(value.content_identities,decimal(remote,"content_identities"));value.content_bytes=std::min(value.content_bytes,decimal(remote,"content_bytes"));
    value.receipt_pages=std::min(value.receipt_pages,decimal(remote,"receipt_pages"));value.receipts=std::min(value.receipts,decimal(remote,"receipts"));value.receipt_bytes=std::min(value.receipt_bytes,decimal(remote,"receipt_bytes"));return value;
}
}
struct recovery_receiver_route::state {
    std::weak_ptr<lattice_db> owner;
    std::shared_ptr<recovery_continuous_route> continuous;
    std::shared_ptr<receiver_source_binding> source;
    std::shared_ptr<owned_platform_sync_transport> transport;
    std::shared_ptr<scheduler> dispatch;
    std::shared_ptr<sync_callback_lifetime> lifetime;
    std::atomic<bool> retired{false},blocked{true};
    std::function<void()> resumed,renew,reconcile;
    std::atomic<bool> renewal_requested{false};
    std::function<void(std::exception_ptr)> error;
};
struct recovery_receiver_controller::state {
    const recovery_continuous_policy policy;
    const canonical_scoped_limits caps;
    std::shared_ptr<const test_probe> probe;
    std::mutex mutex;
    std::vector<std::weak_ptr<recovery_receiver_route>> routes;
    bool scheduled=false,running=false,demand=true,idle=false,deferred_external_request=false;
    const recovery_reconciliation_reservation* reconciliation_reservation=nullptr;
    uint64_t revision=1,external_revision=1,reconciled_external_revision=0;
    std::exception_ptr failure;
    std::shared_ptr<const verified_unsent_set> frozen;
    std::shared_ptr<const recovery_reconciliation_descriptor> reconciliation;
    int64_t frozen_attempt=0,frozen_barrier=0;
    bool framing_committed=false;
    struct pending {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        std::string request_id,operation,request_bytes,response;
        int64_t deadline=0;
        uint64_t index=0;
    };
    std::shared_ptr<pending> outstanding;
    struct lease {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        std::string id,request_digest,attempt_id;
        uint64_t sequence=0,frames=0;int64_t deadline=0;
    };
    std::map<std::string,lease> leases;
    std::map<std::string,receiver_source_binding::recovery_view> observed;
    explicit state(const recovery_continuous_policy& p):policy(p),caps(recovery_receiver_controller::limits(p)){}
};
// One result owns the successful admission until known-COMMIT publication or
// abandonment. SQL bodies and owner destruction never run under the leaf.
struct recovery_reconciliation_reservation {
    std::shared_ptr<recovery_receiver_controller> controller;
    std::shared_ptr<lattice_db> owner;
    bool active=false;
    void release()noexcept {
        std::shared_ptr<const recovery_reconciliation_descriptor> retired;
        if(active){auto& runtime=*controller->state_;{
            std::lock_guard lock(runtime.mutex);
            if(runtime.reconciliation_reservation==this){runtime.reconciliation_reservation=nullptr;runtime.running=false;
                // Admission reserves room for publication plus this one
                // coalesced actual external event. Internal wakes never set it.
                if(runtime.deferred_external_request){runtime.deferred_external_request=false;++runtime.revision;++runtime.external_revision;
                    runtime.demand=true;runtime.failure={};retired=std::move(runtime.reconciliation);}}
            active=false;}}
    }
    ~recovery_reconciliation_reservation(){release();}
};
canonical_scoped_limits recovery_receiver_controller::limits(const recovery_continuous_policy& policy) {
    canonical_scoped_limits result;
    result.obligations=policy.limits.obligations;result.install.installations=policy.limits.installations;
    result.install.capture={8192,32768,16,256,4096,4194304,65536,536870912};
    result.install.channels=16;result.install.members=262144;result.install.metadata_bytes=33554432;
    result.install.targets=32768;result.install.receipts=8192;result.install.fields=4194304;result.install.field_bytes=65536;result.install.logical_bytes=536870912;
    result.codec={{4194304,16384,64,512,16384,33554432,256,8192,8388608},16,262144,32768,8192,8192,2097152,4194304,3600000,{16384,64,256,4096,16384}};
    result.staging={16,8192,262144,536870912,4096,131072,134217728,805306368};return result;
}
std::mutex recovery_receiver_controller::test_mutex_;
std::shared_ptr<const recovery_receiver_controller::test_probe> recovery_receiver_controller::test_probe_;
recovery_receiver_controller::recovery_receiver_controller(const recovery_continuous_policy& policy):state_(std::make_unique<state>(policy)){
    std::lock_guard lock(test_mutex_);state_->probe=test_probe_;
}
recovery_receiver_controller::~recovery_receiver_controller()=default;
void recovery_receiver_controller::initialize_owned(std::shared_ptr<lattice_db> owner,const recovery_continuous_policy& policy) {
    const auto caps=limits(policy);recovery_request_store requests(owner);requests.initialize();
    canonical_range_staging stage(owner,caps.install.installations,caps.codec,caps.staging);stage.initialize();
    initialize_canonical_domains_owned(owner,caps.install,true);
    (void)bump_recovery_witness(*owner);
}
void recovery_receiver_controller::validate_reopen_owned(std::shared_ptr<lattice_db> owner,const recovery_continuous_policy& policy,int64_t phase,int64_t attempt) {
    const auto caps=limits(policy);recovery_request_store requests(owner);requests.audit();
    initialize_canonical_domains_owned(owner,caps.install,false);
    require(read_recovery_witness(*recovery_writer_access::active_writer(*owner)).has_value(),"controller witness missing on reopen");
    canonical_range_staging stages(owner,caps.install.installations,caps.codec,caps.staging);stages.audit();
    recovery_obligation_store journals(owner,caps.obligations,caps.install.installations);journals.audit();
    receive_install_store receiver(owner,caps.install.installations);receiver.audit();
    const auto count=recovery_writer_access::active_writer(*owner)->query("SELECT COUNT(*) AS n FROM (SELECT DISTINCT original FROM main._lattice_obligation_entry WHERE stage<>2 ORDER BY original LIMIT 8193)");
    require(count.size()==1&&integer(count[0],"n")<=8192,"controller reopened unresolved-original capacity differs");
    std::set<std::string> allowed;
    for(const auto& c:policy.contributions){allowed.insert(c.profile.binding.channel);const auto scope=journals.read(c.profile.binding.channel);const auto actual=receiver.read(c.profile.binding.channel);
        require(scope&&scope->profile==c.profile&&actual&&actual->binding==c.profile.binding,"controller reopened contribution binding differs");
        require(phase==0||phase==1||phase==4?scope->mode==recovery_obligation_mode::recording:
            phase==2?scope->mode==recovery_obligation_mode::frozen:phase==3&&scope->mode==recovery_obligation_mode::installed,
            "controller reopened journal phase differs");
        const auto framing=requests.read(c.profile.binding.channel);
        if(framing){
            const auto decoded=cr::decode(framing->request_frame,caps.codec);const auto* q=std::get_if<cr::request>(&decoded.body);
            require(q&&decoded.logical.channel==c.profile.binding.channel&&decoded.logical.sequence==uint64_t(framing->sequence)&&q->source==source_binding(c.profile),"controller reopened Q differs");
            if(!framing->manifest_frame.empty()){
                const auto m=cr::decode(framing->manifest_frame,caps.codec);require(m.logical==decoded.logical&&std::holds_alternative<cr::manifest>(m.body),"controller reopened manifest framing differs");
                const auto described=describe_canonical_range(decoded.logical,*q,std::get<cr::manifest>(m.body),caps.codec,framing->route);
                require(described.installation_binding==c.profile.binding,"controller reopened manifest binding differs");
                if(phase==0||phase==3||(phase==1&&actual->last_installed==std::optional<receive_install_identity>{described.installation_identity})){require(actual->last_installed==std::optional<receive_install_identity>{described.installation_identity}&&scope->installed_manifest==described.installation_identity.manifest_digest&&
                    scope->installed_revision==actual->revision,"controller reopened installed framing differs");
                    require(!actual->active&&actual->last_sequence==framing->sequence&&
                        (phase!=1||(framing->sequence==attempt-1&&scope->last_attempt==framing->sequence)),"controller prior installed request is not the exact barrier predecessor");}
            }else require(phase==2&&framing->sequence==attempt,"controller manifestless Q outside frozen attempt");
            if(phase==1)require(framing->sequence==attempt-1&&actual->last_sequence==framing->sequence&&!actual->active,
                "controller preparing phase lacks its exact prior receiver sequence");
            if(phase==4||(phase==1&&framing->sequence<attempt&&(!actual->last_installed||actual->last_installed->sequence<framing->sequence)))require(framing->sequence==attempt-1&&framing->barrier+1>framing->barrier&&
                scope->last_attempt==framing->sequence&&scope->address.incarnation==framing->journal.incarnation&&scope->address.generation==framing->journal.generation+1&&
                scope->revision>=framing->journal_revision+1&&!actual->active&&actual->last_sequence==framing->sequence&&
                (!actual->last_installed||actual->last_installed->sequence<framing->sequence),"controller reopened reconciliation differs from exact canceled attempt");
        }else require(!actual->last_installed&&phase!=3,"controller missing installed framing");
        if(phase==2||phase==3)require(scope->last_attempt==attempt,"controller reopened attempt differs");
        if(phase==3)require(framing&&framing->sequence==attempt&&!framing->manifest_frame.empty(),"controller installed phase lacks complete framing");
    }
    for(const auto& channel:requests.channels())require(allowed.count(channel),"controller unknown durable request channel");
}
std::shared_ptr<recovery_receiver_route> recovery_receiver_controller::attach(std::shared_ptr<lattice_db> owner,const std::shared_ptr<recovery_continuous_route>& continuous,
    const std::shared_ptr<receiver_source_binding>& source,const std::shared_ptr<owned_platform_sync_transport>& transport,
    const std::shared_ptr<scheduler>& scheduler,const std::shared_ptr<sync_callback_lifetime>& lifetime) {
    auto route_state=std::make_shared<recovery_receiver_route::state>();route_state->owner=owner;route_state->continuous=continuous;route_state->source=source;route_state->transport=transport;route_state->dispatch=scheduler;route_state->lifetime=lifetime;
    auto route=std::shared_ptr<recovery_receiver_route>(new recovery_receiver_route(shared_from_this(),std::move(route_state)));
    {std::lock_guard lock(state_->mutex);auto& all=state_->routes;all.erase(std::remove_if(all.begin(),all.end(),[](const auto& value){return value.expired();}),all.end());
        require(all.size()<state_->policy.physical_routes,"controller physical route capacity");all.push_back(route);}
    return route;
}
recovery_receiver_route::recovery_receiver_route(std::shared_ptr<recovery_receiver_controller> controller,std::shared_ptr<state> state):state_(std::move(state)),controller_(std::move(controller)){}
recovery_receiver_route::~recovery_receiver_route(){state_->retired.store(true,std::memory_order_release);controller_->retire(this);}
void recovery_receiver_controller::retire(recovery_receiver_route*)noexcept {
    // Weak registrations expire without destroying an owner/transport under a
    // leaf. Durable barrier/Q survives; a later actual route can continue it.
}
void recovery_receiver_route::wake(){if(!state_->retired.load(std::memory_order_acquire))controller_->wake(shared_from_this());}
void recovery_receiver_route::notifications(std::function<void()> resumed,std::function<void()> renew,std::function<void(std::exception_ptr)> error,std::function<void()> reconcile){state_->resumed=std::move(resumed);state_->renew=std::move(renew);state_->error=std::move(error);state_->reconcile=std::move(reconcile);}
void recovery_receiver_route::request(){
    std::shared_ptr<const recovery_reconciliation_descriptor> retired;
    {std::lock_guard lock(controller_->state_->mutex);auto& state=*controller_->state_;
        require(state.revision!=UINT64_MAX&&state.external_revision!=UINT64_MAX,"controller demand revision exhausted");
        if(state.reconciliation_reservation)state.deferred_external_request=true;
        else {++state.revision;++state.external_revision;state.demand=true;state.failure={};retired=std::move(state.reconciliation);}}
    retired.reset();wake();
}
bool recovery_receiver_route::blocks_ordinary()const noexcept{return state_->blocked.load(std::memory_order_acquire);}
void recovery_receiver_controller::dropped_turn()noexcept {std::lock_guard lock(state_->mutex);state_->scheduled=false;}
void recovery_receiver_controller::wake(const std::shared_ptr<recovery_receiver_route>& route) {
    if(route->state_->retired.load())return;
    {std::lock_guard lock(state_->mutex);if(state_->scheduled||state_->running)return;state_->scheduled=true;}
    struct scheduled {std::shared_ptr<recovery_receiver_controller> owner;std::atomic<bool> invoked{false};~scheduled(){if(!invoked.load())owner->dropped_turn();}};
    auto held=std::make_shared<scheduled>();held->owner=shared_from_this();
    try {route->state_->dispatch->invoke([held]{if(held->invoked.exchange(true))return;held->owner->turn();});}
    catch(...){dropped_turn();throw;}
}
bool recovery_receiver_route::receive(const platform_transport_callbacks& endpoint,uint64_t lifecycle,const transport_message& message) {
    const std::string_view raw(reinterpret_cast<const char*>(message.data.data()),message.data.size());
    if(!reserved_recovery_source_frame(raw))return false;
    auto& coordinator=*controller_->state_;
    std::shared_ptr<recovery_receiver_controller::state::pending> pending;
    {std::lock_guard lock(coordinator.mutex);pending=coordinator.outstanding;}
    if(!pending)return false; // actual describe continues through its own verifier
    if(pending->route.lock().get()!=this||!state_->source->recovery_matches(pending->view,endpoint,lifecycle))return true;
    require(message.data.size()<=recovery_request_store::frame_bytes&&pending->request_bytes.size()<=pending_bytes-message.data.size(),"controller pending response byte capacity");
    std::string response(raw); // bounded before copy; parsing belongs to worker
    {std::lock_guard lock(coordinator.mutex);if(coordinator.outstanding!=pending)return true;
        require(pending->response.empty(),"controller duplicate outstanding response");pending->response=std::move(response);}
    wake();return true;
}
void recovery_receiver_controller::turn() {
    auto& runtime=*state_;
    {std::lock_guard lock(runtime.mutex);runtime.scheduled=false;if(runtime.running)return;runtime.running=true;}
    struct settlement {recovery_receiver_controller::state& value;std::function<void()> after;
        ~settlement(){{std::lock_guard lock(value.mutex);value.running=false;}if(after)try{after();}catch(...) {}}} settle{runtime,{}};
    try {
        struct connected {
            std::shared_ptr<recovery_receiver_route> route;
            receiver_source_binding::recovery_view view;
            json description;
            canonical_scoped_contract scope;
        };
        std::vector<std::shared_ptr<recovery_receiver_route>> routes;
        {std::lock_guard lock(runtime.mutex);for(const auto& weak:runtime.routes)if(auto route=weak.lock())routes.push_back(std::move(route));}
        std::map<std::string,connected> connected_routes;
        for(const auto& route:routes) {
            if(route->state_->retired.load())continue;
            auto owner=route->state_->owner.lock();if(!owner||owner->is_closed())continue;
            const auto view=route->state_->source->recovery_current();if(!view) {
                if(route->state_->source->recovery_expired()&&!route->state_->renewal_requested.exchange(true)&&route->state_->renew)route->state_->renew();
                continue;
            }
            route->state_->renewal_requested.store(false);
            auto description=parse(route->state_->source->recovery_description(*view),65536);
            const auto channel=description.at("channel").get<std::string>();
            auto contribution=std::find_if(runtime.policy.contributions.begin(),runtime.policy.contributions.end(),[&](const auto& c){return c.profile.binding.channel==channel;});
            require(contribution!=runtime.policy.contributions.end(),"controller live channel outside enrollment");
            auto scope=contract(description,*contribution,recovery_continuous_producer::controller_catalog(*owner));
            const auto prior=connected_routes.find(channel);
            if(prior==connected_routes.end()||decimal(description,"routeGeneration")>decimal(prior->second.description,"routeGeneration"))
                connected_routes.insert_or_assign(channel,connected{route,*view,std::move(description),std::move(scope)});
        }
        if(connected_routes.size()!=runtime.policy.contributions.size())return;
        auto owner=connected_routes.begin()->second.route->state_->owner.lock();if(!owner||owner->is_closed())return;
        std::string common_domain;
        for(const auto& [channel,c]:connected_routes) {
            const auto key=domain(c.description);if(common_domain.empty())common_domain=key;
            require(key==common_domain,"controller overlapping replacement authority requires explicit configuration");
            if(!runtime.observed.count(channel)||runtime.observed.at(channel).value!=c.view.value){
                {std::lock_guard lock(runtime.mutex);require(runtime.revision!=UINT64_MAX&&runtime.external_revision!=UINT64_MAX,"controller demand revision exhausted");++runtime.revision;++runtime.external_revision;runtime.demand=true;runtime.failure={};}
                std::shared_ptr<const recovery_reconciliation_descriptor> retired;
                {std::lock_guard lock(runtime.mutex);retired=std::move(runtime.reconciliation);}
                runtime.observed[channel]=c.view;runtime.frozen.reset();runtime.framing_committed=false;
            }
        }
        bool reconciliation_waiting=false;
        {std::lock_guard lock(runtime.mutex);reconciliation_waiting=static_cast<bool>(runtime.reconciliation);
            if(!reconciliation_waiting&&(runtime.failure||(runtime.idle&&!runtime.demand&&!runtime.outstanding)))return;}
        if(reconciliation_waiting){settle.after=[routes]{for(const auto& route:routes)if(!route->state_->retired.load()&&route->state_->reconcile)route->state_->reconcile();};return;}
        const auto observe=[&](const char* stage){if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->observed)runtime.probe->observed(stage);};
        // No new Q/frozen/journal graph may coexist with a retired cohort.
        // The actual running/current-controller reservation spans this whole
        // turn; current descriptors took their notification branch above.
        if(recovery_continuous_producer::controller_cohort(*this,owner)==recovery_continuous_producer::cohort_admission::retained){observe("cohort-retained");return;}
        const auto reserve_cohort=[&](const std::shared_ptr<recovery_reconciliation_descriptor>& descriptor){
            const auto admitted=recovery_continuous_producer::controller_cohort(*this,owner,descriptor);
            if(admitted==recovery_continuous_producer::cohort_admission::retained){observe("cohort-retained");return false;}
            observe("cohort-reserved");return true;
        };
        const auto probe_scope=[&](const char* stage)->std::shared_ptr<void>{return runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->scope?runtime.probe->scope(stage):nullptr;};
        const auto live=[&]{for(const auto& [_,c]:connected_routes)require(!c.route->state_->retired.load()&&c.route->state_->source->recovery_live(c.view),"controller authenticated source retired during owned operation");};
        const auto owned=[&](const std::function<void(database&)>& body){const auto result=recovery_continuous_producer::controller_owned(*this,owner,[&](database& db){live();body(db);live();});known(result);};
        // Socket callbacks enqueue at most one bounded reply. Only this worker
        // parses it or touches SQL. Claiming the reply keeps its full byte
        // charge until the exact transactional consumer finishes.
        std::shared_ptr<state::pending> pending;
        {std::lock_guard lock(runtime.mutex);pending=runtime.outstanding;}
        if(pending) {
            auto route=pending->route.lock();
            if(!route||!route->state_->source->recovery_live(pending->view)||now()>=pending->deadline){
                std::lock_guard lock(runtime.mutex);if(runtime.outstanding==pending)runtime.outstanding.reset();return;
            }
            std::string response;
            {std::lock_guard lock(runtime.mutex);response.swap(pending->response);}
            if(response.empty())return;
            const auto description=parse(route->state_->source->recovery_description(pending->view),65536);
            const auto channel=description.at("channel").get<std::string>();
            if(pending->operation=="read") {
                // A failed control response is not a range, absence proof or
                // durable progress. Retain Q/stage and renew the real lease.
                const auto envelope=parse(response,recovery_request_store::frame_bytes);
                if(envelope.contains("kind")) {
                    members(envelope,{"kind","version","operation","requestID","routeGeneration","settlement","frameAvailable"});settlement_shape(envelope.at("settlement"));
                    require(envelope.at("version")==1&&envelope.at("kind")=="recoveryReady"&&envelope.at("operation")=="read"&&envelope.at("requestID")==pending->request_id&&
                        envelope.at("routeGeneration")==description.at("routeGeneration")&&envelope.at("frameAvailable")==false,"controller read refusal correlation differs");
                    runtime.leases.erase(channel);
                } else {
                    const auto frame=cr::decode(response,runtime.caps.codec);
                    owned([&](database&){
                        recovery_request_store requests(owner);const auto row=requests.read(channel);require(row.has_value(),"controller range lacks durable Q");
                        const auto q=cr::decode(row->request_frame,runtime.caps.codec);
                        require(frame.logical==q.logical&&frame.route_generation==uint64_t(row->route),"controller range physical or logical attempt differs");
                        canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                        if(const auto* manifest=std::get_if<cr::manifest>(&frame.body)) {
                            require(pending->index==0,"controller unsolicited manifest index");
                            if(row->manifest_frame.empty()){stages.begin(q.logical,std::get<cr::request>(q.body),*manifest,row->route);requests.add_manifest(*row,response);}
                            else {const auto original=cr::decode(row->manifest_frame,runtime.caps.codec);require(original.logical==frame.logical&&original.body==frame.body,"controller manifest changed on resume");}
                        } else {
                            require(!row->manifest_frame.empty(),"controller page before committed manifest");
                            auto current=stages.resume(q.logical,std::get<cr::manifest>(cr::decode(row->manifest_frame,runtime.caps.codec).body).manifest_digest,row->route);
                            require(pending->index==1+current.state.next_content_page+current.state.next_receipt_page,"controller range index differs from durable stage");
                            if(std::holds_alternative<cr::end>(frame.body))stages.verify_end(frame);else stages.append(frame);
                        }
                    });
                }
            } else {
                const auto value=parse(response,recovery_request_store::frame_bytes);
                members(value,{"kind","version","operation","requestID","routeGeneration","leaseAvailable"},
                    {"settlement","expiration","preparation","publication","captureError","requiresFullRequest","leaseID","requestDigest","attemptID","sequence","frames","wireBytes","durationMilliseconds"});
                require(value.at("leaseAvailable").is_boolean(),"controller lease status type");
                for(const auto* key:{"settlement","expiration","preparation","publication"})if(value.contains(key))settlement_shape(value.at(key));
                if(pending->operation=="resume")require(value.contains("settlement")&&!value.contains("expiration")&&!value.contains("preparation")&&!value.contains("publication")&&!value.contains("captureError")&&!value.contains("requiresFullRequest"),"controller mixed resume result");
                else require(value.contains("expiration")&&!value.contains("settlement"),"controller mixed prepare result");
                require(value.at("kind")=="recoveryReady"&&value.at("version")==1&&value.at("operation")==pending->operation&&
                    value.at("requestID")==pending->request_id&&value.at("routeGeneration")==description.at("routeGeneration"),"controller lease correlation differs");
                if(value.at("leaseAvailable")==true) {
                    if(pending->operation=="resume")require(value.at("settlement").at("state")=="committed","controller resume lease without known source commit");
                    else require(value.at("expiration").at("state")=="committed"&&value.at("publication").at("state")=="committed"&&
                        value.at("captureError")==false&&value.at("requiresFullRequest")==false,"controller preparation lease without complete source publication");
                    const auto q=cr::decode(parse(pending->request_bytes,8388608).at("request").get<std::string>(),runtime.caps.codec);
                    require(value.at("requestDigest")==std::get<cr::request>(q.body).request_digest&&value.at("attemptID")==q.logical.attempt_id&&decimal(value,"sequence")==q.logical.sequence,
                        "controller returned lease differs from frozen Q");
                    const auto duration=value.at("durationMilliseconds").get<int64_t>();require(duration>0&&duration<=3600000&&now()<=INT64_MAX-duration,"controller returned lease duration invalid");
                    const auto id=value.at("leaseID").get<std::string>();require(!id.empty()&&id.size()<=128,"controller lease identity capacity");
                    const auto frames=decimal(value,"frames");require(frames<=770&&frames<=description.at("profile").at("frames").get<uint64_t>(),"controller lease frame inventory capacity");
                    require(decimal(value,"wireBytes")<=description.at("profile").at("transferBytes").get<uint64_t>(),"controller lease wire inventory capacity");
                    runtime.leases.insert_or_assign(channel,state::lease{route,pending->view,id,std::get<cr::request>(q.body).request_digest,q.logical.attempt_id,q.logical.sequence,frames,now()+duration});
                } else {
                    require(value.at("leaseAvailable")==false,"controller invalid lease status");
                    for(const auto* key:{"leaseID","requestDigest","attemptID","sequence","frames","wireBytes","durationMilliseconds"})require(!value.contains(key),"controller unavailable lease carries positive identity");
                    // No source error is converted to absence. A later prepare
                    // can only succeed under the source's own exact sequence,
                    // active-binding and high-water checks; it cannot overwrite
                    // a retained transfer. Mark this as a bounded retry choice.
                    runtime.leases.erase(channel);
                    if(pending->operation=="resume") {
                        // Empty lease identity denotes one allowed prepare
                        // attempt, never an authenticated negative outcome.
                        runtime.leases.emplace(channel,state::lease{route,pending->view,{},{},{},0,0,now()+1000});
                    } else refuse("controller source preparation refused; exact Q retained");
                }
            }
            {std::lock_guard lock(runtime.mutex);if(runtime.outstanding==pending)runtime.outstanding.reset();}
        }
        for(unsigned quantum=0;quantum<4;++quantum) {
            int64_t phase=0,barrier=0,attempt=0,physical_incarnation=0;uint64_t demand_revision;
            {std::lock_guard lock(runtime.mutex);demand_revision=runtime.revision;}
            owned([&](database& db){const auto rows=db.query("SELECT CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(phase)='integer' THEN phase END AS phase,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt FROM main._lattice_producer_continuity WHERE id=1 LIMIT 2");require(rows.size()==1,"controller physical phase missing");phase=integer(rows[0],"phase");barrier=integer(rows[0],"barrier");attempt=integer(rows[0],"attempt");physical_incarnation=integer(rows[0],"incarnation");});
            if(phase==0) {
                bool demand;{std::lock_guard lock(runtime.mutex);demand=runtime.demand;}
                if(!demand){runtime.idle=true;for(const auto& route:routes)route->state_->blocked.store(false,std::memory_order_release);return;}
                runtime.idle=false;
                for(const auto& route:routes)route->state_->blocked.store(true,std::memory_order_release);
                int64_t next=0;owned([&](database&){next=recovery_continuous_producer::controller_next_attempt_owned(owner);});
                auto begun=recovery_continuous_producer::begin(owner,next);known(begun.settlement);observe("barrier-committed");if(begun.waiting)return;continue;
            }
            for(const auto& route:routes)route->state_->blocked.store(true,std::memory_order_release);
            if(phase==4) {
                auto descriptor=std::shared_ptr<recovery_reconciliation_descriptor>(new recovery_reconciliation_descriptor);
                if(!reserve_cohort(descriptor))return;
                descriptor->owner_=owner;descriptor->limits_=runtime.caps;descriptor->controller_=shared_from_this();descriptor->controller_revision_=demand_revision;
                descriptor->physical_incarnation_=physical_incarnation;descriptor->barrier_=barrier;descriptor->attempt_=attempt;descriptor->phase_=4;descriptor->restart_revalidation_=true;
                owned([&](database&){
                    recovery_request_store requests(owner);recovery_obligation_store journal(owner,runtime.caps.obligations,runtime.caps.install.installations);
                    receive_install_store receiver(owner,runtime.caps.install.installations);receiver.audit();journal.audit();
                    for(const auto& c:runtime.policy.contributions){const auto row=requests.read(c.profile.binding.channel);const auto scope=journal.read(c.profile.binding.channel);const auto current=receiver.read(c.profile.binding.channel);
                        require(row&&scope&&current&&row->sequence==attempt-1&&row->barrier==barrier-1&&scope->mode==recovery_obligation_mode::recording&&scope->last_attempt==row->sequence&&
                            scope->address.incarnation==row->journal.incarnation&&scope->address.generation==row->journal.generation+1&&scope->revision>=row->journal_revision+1&&
                            current->binding==c.profile.binding&&!current->active&&current->last_sequence==row->sequence&&(!current->last_installed||current->last_installed->sequence<row->sequence),
                            "controller restricted restart lacks exact canceled receiver/journal");
                        const auto& actual=connected_routes.at(c.profile.binding.channel);require(row->domain==common_domain&&row->source_context==source_context(actual.description),"controller restricted restart source changed");
                        auto snapshot=journal.snapshot_for_reconciliation(scope->address);const auto q=cr::decode(row->request_frame,runtime.caps.codec);
                        std::set<std::string> requested;for(const auto& item:std::get<cr::request>(q.body).receipts)requested.insert(canonical_writer_adapter::uuid_key(item.original_id));
                        recovery_reconciliation_descriptor::contribution contribution{*row,std::move(snapshot),{},actual.route,actual.route->state_->source,actual.view};
                        for(const auto& entry:contribution.journal.entries)if(entry.stage==recovery_obligation_stage::open&&!entry.acknowledged&&requested.count(entry.canonical_original_id))contribution.unknown_originals.push_back(entry.canonical_original_id);
                        descriptor->contributions_.push_back(std::move(contribution));
                    }
                });
                {std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision,"controller restricted restart generation changed");runtime.reconciled_external_revision=runtime.external_revision;runtime.reconciliation=std::move(descriptor);}observe("reconciliation-pending");settle.after=[routes]{for(const auto& route:routes)if(!route->state_->retired.load()&&route->state_->reconcile)route->state_->reconcile();};return;
            }
            if(phase==1) {auto status=recovery_continuous_producer::inspect(owner);known(status.settlement);require(status.barrier.has_value(),"controller closed phase lacks actual barrier");
                auto frozen=recovery_continuous_producer::finish(*status.barrier);if(frozen.waiting)return;known(frozen.settlement);continue;}
            if(phase==3) {
                auto scope_probe=probe_scope("resume");
                owned([&](database& db){
                    recovery_request_store requests(owner);receive_install_store receiver(owner,runtime.caps.install.installations);
                    recovery_obligation_store journals(owner,runtime.caps.obligations,runtime.caps.install.installations);
                    canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                    for(const auto& c:runtime.policy.contributions) {
                        const auto row=requests.read(c.profile.binding.channel);require(row&&row->sequence==attempt&&!row->manifest_frame.empty(),"controller resume framing missing");
                        const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                        const auto actual=journals.read(c.profile.binding.channel);require(actual&&actual->mode==recovery_obligation_mode::installed,"controller resume journal not installed");
                        canonical_install_admission grant;grant.owner_=owner;grant.attempt_=q.logical;grant.route_=row->route;grant.request_digest_=std::get<cr::request>(q.body).request_digest;
                        grant.manifest_digest_=std::get<cr::manifest>(m.body).manifest_digest;grant.profile_=c.profile;grant.journal_=actual->address;grant.journal_revision_=row->journal_revision;
                        grant.coverage_id_=connected_routes.at(c.profile.binding.channel).description.at("source").at("coverageID");grant.limits_=runtime.caps;
                        grant.receive_guard_=receive_delivery_guard_access::read_owned(*owner,db,c.profile.binding.channel);
                        const auto identity=describe_canonical_range(q.logical,std::get<cr::request>(q.body),std::get<cr::manifest>(m.body),runtime.caps.codec,row->route).installation_identity;
                        (void)inspect_committed_canonical_owned(grant,identity,std::get<cr::request>(q.body),std::get<cr::manifest>(m.body));
                        stages.release_installed(q.logical,grant.manifest_digest_,row->route);
                        journals.resume(actual->address,identity);
                    }
                    recovery_continuous_producer::controller_resume_owned(*this,owner,barrier,attempt);
                });
                scope_probe.reset();observe("resume-committed");
                recovery_continuous_producer::controller_publish_resume(*this,owner,barrier,attempt);
                runtime.frozen.reset();runtime.framing_committed=false;runtime.idle=true;
                {std::lock_guard lock(runtime.mutex);if(runtime.revision==demand_revision)runtime.demand=false;runtime.failure={};}
                for(const auto& route:routes){route->state_->blocked.store(false,std::memory_order_release);if(route->state_->resumed)route->state_->resumed();}return;
            }
            require(phase==2,"controller unsupported durable phase");
            if(!runtime.frozen||runtime.frozen_attempt!=attempt||runtime.frozen_barrier!=barrier) {
                auto status=recovery_continuous_producer::inspect(owner);known(status.settlement);require(status.barrier.has_value(),"controller frozen barrier missing");
                auto frozen=recovery_continuous_producer::finish(*status.barrier);if(frozen.waiting)return;known(frozen.settlement);require(frozen.unsent.has_value(),"controller frozen local custody missing");
                auto parked=std::make_shared<verified_unsent_set>(std::move(*frozen.unsent));recovery_continuous_producer::controller_park_proof(*parked);runtime.frozen=std::move(parked);runtime.frozen_attempt=attempt;runtime.frozen_barrier=barrier;runtime.framing_committed=false;
            }
            const auto proof=runtime.frozen;
            bool created=false;
            if(!runtime.framing_committed)owned([&](database& db){
                recovery_continuous_producer::verify_for_owned_write(*proof);recovery_request_store requests(owner);
                receive_install_store receiver(owner,runtime.caps.install.installations);
                std::map<std::string,recovery_obligation_record> originals;
                for(const auto& scope:proof->frozen_journals())for(const auto& entry:scope.entries){
                    require(originals.count(entry.canonical_original_id)||originals.size()<8192,"controller complete union exceeds finite request capacity");
                    const auto [at,added]=originals.emplace(entry.canonical_original_id,entry.record);require(added||at->second==entry.record,"controller shared original differs");}
                for(const auto& scope:proof->frozen_journals()) {
                    const auto& connected=connected_routes.at(scope.scope.address.channel);const auto& d=connected.description;
                    const auto existing=requests.read(scope.scope.address.channel);
                    if(existing&&existing->sequence==attempt){require(existing->journal==scope.scope.address&&existing->journal_revision==scope.scope.revision&&existing->barrier==barrier&&
                        existing->source_context==source_context(d)&&existing->domain==common_domain,"controller frozen request binding changed");continue;}
                    if(existing){require(!existing->manifest_frame.empty(),"controller prior incomplete Q cannot be replaced");const auto oldq=cr::decode(existing->request_frame,runtime.caps.codec),oldm=cr::decode(existing->manifest_frame,runtime.caps.codec);
                        const auto old=describe_canonical_range(oldq.logical,std::get<cr::request>(oldq.body),std::get<cr::manifest>(oldm.body),runtime.caps.codec,existing->route);
                        const auto current=receiver.read(scope.scope.address.channel);const bool installed=current&&current->last_installed==std::optional<receive_install_identity>{old.installation_identity}&&!current->active;
                        const bool canceled=current&&!current->active&&existing->sequence==attempt-1&&existing->barrier==barrier-1&&current->last_sequence==existing->sequence&&
                            (!current->last_installed||current->last_installed->sequence<existing->sequence)&&scope.scope.address.incarnation==existing->journal.incarnation&&
                            scope.scope.address.generation==existing->journal.generation+2&&scope.scope.last_attempt==attempt;
                        require(installed||canceled,"controller prior Q lacks exact installed or canceled receiver evidence");requests.erase(*existing);}
                    const auto prior=receiver.read(scope.scope.address.channel);require(prior&&prior->binding==scope.scope.profile.binding&&!prior->active,"controller Q actual receiver unavailable");
                    cr::attempt logical{d.at("peer").at("receiverIncarnation"),d.at("peer").at("channelIncarnation"),scope.scope.address.channel,static_cast<uint64_t>(attempt),uuid_t::generate().to_string()};
                    cr::request q;q.source=source_binding(scope.scope.profile);q.expected.binding=q.source;q.expected.revision=prior->revision;
                    q.expected.base={prior->frontier.kind==receive_frontier_kind::position?cr::frontier_kind::position:prior->frontier.kind==receive_frontier_kind::beginning_null?cr::frontier_kind::beginning_null:cr::frontier_kind::uninitialized,
                        prior->frontier.position?std::optional<uint64_t>{static_cast<uint64_t>(*prior->frontier.position)}:std::nullopt};q.budget=negotiate(d,runtime.caps.codec);
                    require(originals.size()<=d.at("profile").at("requestEntries").get<size_t>()&&originals.size()<=d.at("profile").at("requestTargets").get<size_t>(),"controller actual source request count capacity");
                    for(const auto& [id,entry]:originals)q.receipts.push_back({id,scope.scope.profile.receipt_namespace,{{entry.table,canonical_writer_adapter::uuid_key(entry.target_id)}}});
                    q.request_digest=cr::request_sha256(logical,q,runtime.caps.codec);const auto route=decimal(d,"routeGeneration");
                    requests.insert({scope.scope.address,barrier,attempt,scope.scope.revision,static_cast<int64_t>(route),common_domain,source_context(d),cr::encode({logical,route,q},runtime.caps.codec),{}});created=true;
                }
            });
            runtime.framing_committed=true;
            // Known Q COMMIT precedes every handoff. A restart always attempts
            // exact resume first; a newly committed Q can start preparation.
            bool all_complete=true;std::optional<recovery_request_row> selected;uint64_t index=0;
            owned([&](database&){recovery_request_store requests(owner);canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                for(const auto& c:runtime.policy.contributions){auto row=requests.read(c.profile.binding.channel);require(row.has_value(),"controller contribution Q missing");
                    const auto& d=connected_routes.at(c.profile.binding.channel).description;require(row->source_context==source_context(d)&&row->domain==common_domain,"controller retained source context changed");const auto route=decimal(d,"routeGeneration");
                    const auto q=cr::decode(row->request_frame,runtime.caps.codec);
                    if(row->route!=static_cast<int64_t>(route)){
                        if(!row->manifest_frame.empty()){const auto m=cr::decode(row->manifest_frame,runtime.caps.codec);stages.rebind(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route,route);}
                        requests.rebind(*row,route);row->route=route;
                    }
                    if(row->manifest_frame.empty()){all_complete=false;if(!selected){selected=std::move(row);index=0;}continue;}
                    const auto m=cr::decode(row->manifest_frame,runtime.caps.codec);const auto progress=stages.resume(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route);
                    if(!progress.content_verified){all_complete=false;if(!selected){selected=std::move(row);index=1+progress.state.next_content_page+progress.state.next_receipt_page;}}
                }});
            if(all_complete) {
                auto descriptor=std::shared_ptr<recovery_reconciliation_descriptor>(new recovery_reconciliation_descriptor);
                if(!reserve_cohort(descriptor))return;
                descriptor->owner_=owner;descriptor->limits_=runtime.caps;descriptor->controller_=shared_from_this();descriptor->frozen_=proof;descriptor->controller_revision_=demand_revision;
                descriptor->physical_incarnation_=physical_incarnation;descriptor->barrier_=barrier;descriptor->attempt_=attempt;descriptor->phase_=2;
                bool unknown=false;const std::set<std::string> unsent(proof->canonical_originals().begin(),proof->canonical_originals().end());
                owned([&](database&){recovery_continuous_producer::verify_for_owned_write(*proof);recovery_request_store requests(owner);
                    canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                    for(const auto& snapshot:proof->frozen_journals()){
                        const auto row=requests.read(snapshot.scope.address.channel);require(row.has_value(),"controller pending framing missing");
                        const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                        const auto& actual=connected_routes.at(snapshot.scope.address.channel);
                        recovery_reconciliation_descriptor::contribution contribution{*row,snapshot,{},actual.route,actual.route->state_->source,actual.view};
                        const auto verified=stages.resume(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route);require(verified.content_verified,"controller pending stage unverified");
                        std::set<std::string> unknown_ids;
                        for(uint64_t n=0;n<verified.state.offer.counts.receipt_pages;++n){const auto page=std::get<cr::receipt_page>(stages.read_verified_page(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route,cr::stream_kind::receipts,n));
                            for(const auto& item:page.items)if(!std::holds_alternative<cr::committed>(item.value)&&!std::holds_alternative<cr::not_committed>(item.value)){
                                const auto id=canonical_writer_adapter::uuid_key(item.original_id);if(!unsent.count(id))unknown_ids.insert(id);}}
                        for(const auto& entry:snapshot.entries)if(unknown_ids.count(entry.canonical_original_id))contribution.unknown_originals.push_back(entry.canonical_original_id);
                        // Contextual union requests on other channels may also
                        // be UNKNOWN; keep the entire cohort closed even when
                        // this channel has no own unresolved entry for that ID.
                        unknown|=!unknown_ids.empty();descriptor->contributions_.push_back(std::move(contribution));
                    }
                });
                if(unknown){{std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision,"controller pending generation changed");
                    require(runtime.reconciled_external_revision!=runtime.external_revision,"controller UNKNOWN persisted after one restricted pass; new external source/request generation required");
                    runtime.reconciled_external_revision=runtime.external_revision;runtime.reconciliation=std::move(descriptor);}observe("reconciliation-pending");settle.after=[routes]{for(const auto& route:routes)if(!route->state_->retired.load()&&route->state_->reconcile)route->state_->reconcile();};return;}
                auto scope_probe=probe_scope("install");
                owned([&](database& db){
                    recovery_continuous_producer::verify_for_owned_write(*proof);recovery_request_store requests(owner);
                    canonical_cohort_admission cohort;cohort.owner_=owner;cohort.local_=proof;cohort.verify_current_sources_=live;
                    cohort.finalize_owned_=[&]{recovery_continuous_producer::controller_installed_owned(*this,*proof);};
                    for(const auto& c:runtime.policy.contributions){auto guard=receive_delivery_guard_access::read_owned(*owner,db,c.profile.binding.channel);
                        if(!guard.present){const auto token=receive_delivery_guard_access::begin(*owner,db,c.profile.binding.channel);require(token.may_advance&&!token.capacity_refused,"controller modern receive guard allocation refused");}}
                    // Capture only after all global guard counters have settled.
                    for(const auto& scope:proof->frozen_journals()) {
                        const auto row=requests.read(scope.scope.address.channel);const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                        const auto& connected=connected_routes.at(scope.scope.address.channel);
                        canonical_install_admission grant;grant.owner_=owner;grant.attempt_=q.logical;grant.route_=row->route;grant.request_digest_=std::get<cr::request>(q.body).request_digest;
                        grant.manifest_digest_=std::get<cr::manifest>(m.body).manifest_digest;grant.profile_=scope.scope.profile;grant.journal_=scope.scope.address;grant.journal_revision_=scope.scope.revision;
                        grant.coverage_id_=connected.description.at("source").at("coverageID");grant.contract_=connected.scope;grant.limits_=runtime.caps;
                        receive_install_store receiver(owner,runtime.caps.install.installations);grant.supersede_=receiver.read(scope.scope.address.channel)->last_installed;
                        grant.receive_guard_=receive_delivery_guard_access::read_owned(*owner,db,scope.scope.address.channel);
                        cohort.channels_.push_back(std::move(grant));cohort.domains_.push_back(common_domain);
                    }
                    (void)install_canonical_cohort_owned(cohort);
                });
                scope_probe.reset();observe("install-committed");continue;
            }
            require(selected.has_value(),"controller incomplete stage selection missing");
            auto& c=connected_routes.at(selected->journal.channel);auto q=cr::decode(selected->request_frame,runtime.caps.codec);q.route_generation=selected->route;
            auto lease=runtime.leases.find(selected->journal.channel);
            const bool usable=lease!=runtime.leases.end()&&!lease->second.id.empty()&&lease->second.view.value==c.view.value&&
                lease->second.request_digest==std::get<cr::request>(q.body).request_digest&&now()<lease->second.deadline;
            const std::string op=usable?"read":created?"prepare":lease!=runtime.leases.end()&&lease->second.id.empty()&&selected->manifest_frame.empty()?"prepare":"resume";
            auto outgoing=std::make_shared<state::pending>();outgoing->route=c.route;outgoing->view=c.view;outgoing->request_id=uuid_t::generate().to_string();outgoing->operation=op;outgoing->index=index;
            json command={{"kind","recoveryReady"},{"version",1},{"operation",op},{"requestID",outgoing->request_id},{"routeGeneration",c.description.at("routeGeneration")}};
            const auto remaining=c.route->state_->source->recovery_remaining(c.view);require(remaining>100,"controller source authorization renewal required");
            if(usable){require(index<lease->second.frames,"controller frame index exceeds actual lease inventory");command["leaseID"]=lease->second.id;command["requestDigest"]=lease->second.request_digest;
                command["attemptID"]=lease->second.attempt_id;command["sequence"]=std::to_string(lease->second.sequence);command["index"]=std::to_string(index);}
            else {command["request"]=cr::encode(q,runtime.caps.codec);command["durationMilliseconds"]=std::min<int64_t>({remaining-100,3600000,c.description.at("profile").at("leaseMilliseconds").get<int64_t>()});}
            outgoing->request_bytes=command.dump();require(outgoing->request_bytes.size()<=8388608&&outgoing->request_bytes.size()<=pending_bytes-4194304,"controller outgoing aggregate capacity");
            outgoing->deadline=now()+std::min<int64_t>(remaining,30000);
            {std::lock_guard lock(runtime.mutex);require(!runtime.outstanding,"controller overlapping request admission");runtime.outstanding=outgoing;}
            require(c.route->state_->source->recovery_send(c.view,*c.route->state_->transport,transport_message::from_string(outgoing->request_bytes)),"controller final physical handoff refused");
            return;
        }
    }catch(...){
        const auto error=std::current_exception();std::shared_ptr<state::pending> released;
        {std::lock_guard lock(runtime.mutex);runtime.failure=error;released=std::move(runtime.outstanding);}
        released.reset(); // source/endpoint ownership is released off the leaf
        std::vector<std::shared_ptr<recovery_receiver_route>> report;
        {std::lock_guard lock(runtime.mutex);for(const auto& weak:runtime.routes)if(auto route=weak.lock())report.push_back(std::move(route));}
        for(const auto& route:report)if(route->state_->error)try{route->state_->error(error);}catch(...){}
        try{std::rethrow_exception(error);}catch(const std::exception& e){LOG_ERROR("recovery-controller","receiver remains closed: %s",e.what());}catch(...){LOG_ERROR("recovery-controller","receiver remains closed after unknown failure");}
    }
}
std::shared_ptr<recovery_continuous_route> recovery_receiver_route::reconciliation_work_route()const {return state_->continuous;}
std::shared_ptr<const recovery_reconciliation_descriptor> recovery_receiver_route::pending_reconciliation()const {
    if(state_->retired.load())return {};
    std::lock_guard lock(controller_->state_->mutex);const auto& current=controller_->state_->reconciliation;
    if(!current)return {};
    for(const auto& contribution:current->contributions_)if(contribution.route.lock().get()==this)return current;
    return {};
}
bool recovery_receiver_controller::reconciliation_route_current(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor,const std::shared_ptr<recovery_continuous_route>& continuous,
    const std::shared_ptr<lattice_db>& owner,uint64_t physical) {
    require(descriptor&&descriptor->phase_==4&&descriptor->owner()==owner&&owner&&continuous&&physical,
        "restricted export lacks exact phase-4 owner/descriptor");
    if(owner->is_closed())return false;
    auto controller=descriptor->controller_.lock();if(!controller)return false;
    {std::lock_guard lock(controller->state_->mutex);if(controller->state_->reconciliation!=descriptor||controller->state_->revision!=descriptor->controller_revision_)return false;}
    bool matched=false;
    for(const auto& c:descriptor->contributions_)if(auto route=c.route.lock())if(route->state_->continuous==continuous&&route->state_->owner.lock()==owner&&
        !route->state_->retired.load()&&c.source==route->state_->source&&c.source->recovery_live(c.view)&&c.source->recovery_lifecycle(c.view)==physical){require(!matched,"restricted export ambiguous actual route");matched=true;}
    return matched;
}
void recovery_receiver_controller::verify_reconciliation_route(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor,const std::shared_ptr<recovery_continuous_route>& continuous,
    const std::shared_ptr<lattice_db>& owner,uint64_t physical,const std::vector<std::string>* originals) {
    require(descriptor&&descriptor->phase_==4&&descriptor->owner()==owner&&owner&&!owner->is_closed()&&continuous&&physical,
        "restricted export lacks exact phase-4 owner/descriptor");
    auto controller=descriptor->controller_.lock();require(controller!=nullptr,"restricted export controller retired");
    {std::lock_guard lock(controller->state_->mutex);require(controller->state_->reconciliation==descriptor&&controller->state_->revision==descriptor->controller_revision_,"restricted export descriptor replaced");}
    const recovery_reconciliation_descriptor::contribution* matched=nullptr;
    for(const auto& c:descriptor->contributions_)if(auto route=c.route.lock())if(route->state_->continuous==continuous&&route->state_->owner.lock()==owner&&
        !route->state_->retired.load()&&c.source==route->state_->source&&c.source->recovery_live(c.view)&&c.source->recovery_lifecycle(c.view)==physical){require(!matched,"restricted export ambiguous actual route");matched=&c;}
    require(matched!=nullptr,"restricted export physical authenticated source differs");
    if(originals){require(!originals->empty()&&originals->size()<=8192,"restricted export original window capacity");
        size_t next=0;for(const auto& allowed:matched->unknown_originals)if(next<originals->size()&&allowed==(*originals)[next])++next;
        require(next==originals->size(),"restricted export window outside exact ordered retained-Q candidates");}
}
recovery_reconciliation_result recovery_receiver_controller::controller_reconcile_owned(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor,recovery_reconciliation_step step,
    const std::function<void(database&)>& body,bool* coordinator_busy) {
    if(coordinator_busy)*coordinator_busy=false;
    require(descriptor&&body,"controller reconciliation input missing");auto controller=descriptor->controller_.lock();
    const auto owner=descriptor->owner();require(controller&&owner&&!owner->is_closed(),"controller reconciliation actual owner retired");
    auto& runtime=*controller->state_;
    recovery_reconciliation_result result;
    auto reservation=std::make_shared<recovery_reconciliation_reservation>();reservation->controller=controller;reservation->owner=owner;
    {std::lock_guard lock(runtime.mutex);
        require(runtime.reconciliation==descriptor&&runtime.revision==descriptor->controller_revision_,"controller reconciliation descriptor replaced");
        if(runtime.running){result.coordinator_busy_=true;if(coordinator_busy)*coordinator_busy=true;return result;}
        require(!runtime.reconciliation_reservation&&runtime.revision<UINT64_MAX-1&&runtime.external_revision<UINT64_MAX,"controller reconciliation reservation exhausted");
        runtime.running=true;runtime.reconciliation_reservation=reservation.get();reservation->active=true;}
    result.descriptor_=descriptor;result.step_=step;result.reservation_=reservation;
    const bool cancel=step==recovery_reconciliation_step::cancelled;
    require(descriptor->phase_==(cancel?2:4),"controller reconciliation step/phase differs");
    require(!cancel||(descriptor->barrier_<INT64_MAX&&descriptor->attempt_<INT64_MAX),"controller reconciliation sequence exhausted");
    result.next_barrier_=descriptor->barrier_+(cancel?1:0);result.next_attempt_=descriptor->attempt_+(cancel?1:0);
    std::shared_ptr<void> scope_probe;
    if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->scope)
        scope_probe=runtime.probe->scope(cancel?"reconcile-cancel":"reconcile-refreeze");
    result.settlement_=recovery_continuous_producer::controller_owned(*controller,owner,[&](database& db){
        const auto current_sources=[&]{for(const auto& c:descriptor->contributions_)require(!c.route.expired()&&c.source&&c.source->recovery_live(c.view),"controller reconciliation source retired");};
        current_sources();
        const auto rows=db.query("SELECT CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(phase)='integer' THEN phase END AS phase,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt FROM main._lattice_producer_continuity WHERE id=1 LIMIT 2");
        require(rows.size()==1&&integer(rows[0],"incarnation")==descriptor->physical_incarnation_&&integer(rows[0],"phase")==descriptor->phase_&&
            integer(rows[0],"barrier")==descriptor->barrier_&&integer(rows[0],"attempt")==descriptor->attempt_,"controller reconciliation physical generation differs");
        if(cancel){require(descriptor->frozen_!=nullptr,"controller frozen reconciliation lacks actual provenance");recovery_continuous_producer::verify_for_owned_write(*descriptor->frozen_);}
        recovery_request_store requests(owner);const auto framing=requests.fingerprints();
        recovery_obligation_store journal(owner,runtime.caps.obligations,runtime.caps.install.installations);
        receive_install_store receiver(owner,runtime.caps.install.installations);receiver.audit();journal.audit();
        std::vector<recovery_obligation_snapshot> journals;std::vector<receive_install_snapshot> receivers;
        for(const auto& c:descriptor->contributions_) {
            require(requests.read(c.framing.journal.channel)==std::optional<recovery_request_row>{c.framing},"controller reconciliation frozen Q/M changed");
            const auto scope=journal.read(c.framing.journal.channel);const auto installed=receiver.read(c.framing.journal.channel);
            require(scope&&installed&&scope->profile==c.journal.scope.profile&&scope->address==c.journal.scope.address,"controller reconciliation journal address changed");
            auto snapshot=cancel?journal.snapshot_for_install(scope->address,descriptor->attempt_):journal.snapshot_for_reconciliation(scope->address);
            require(snapshot.entries.size()==c.journal.entries.size(),"controller reconciliation pending inventory changed");
            for(size_t n=0;n<snapshot.entries.size();++n){auto current=snapshot.entries[n],original=c.journal.entries[n];
                if(!cancel&&!original.first_export_claim)original.first_export_claim=current.first_export_claim;
                require(current==original,"controller reconciliation original, order, receipt or first claim changed");}
            if(cancel)require(snapshot.scope==c.journal.scope,"controller frozen reconciliation scope changed");
            else require(scope->mode==recovery_obligation_mode::recording&&scope->last_attempt==c.framing.sequence&&scope->revision>=c.journal.scope.revision,
                "controller restricted reconciliation scope changed");
            require(installed->binding==scope->profile.binding&&installed->last_sequence==c.framing.sequence&&
                (!installed->last_installed||installed->last_installed->sequence<c.framing.sequence),"controller reconciliation attempt already installed or superseded");
            journals.push_back(std::move(snapshot));receivers.push_back(*installed);
        }
        body(db); // closed internal helper only; no public callback/issuer
        require(requests.fingerprints()==framing,"controller reconciliation altered retained framing");
        for(size_t n=0;n<journals.size();++n){auto expected=journals[n];auto expected_receiver=receivers[n];
            if(cancel){expected.scope.mode=recovery_obligation_mode::recording;++expected.scope.address.generation;++expected.scope.revision;expected_receiver.active.reset();}
            const auto current=journal.snapshot_for_reconciliation(expected.scope.address);
            require(current.scope==expected.scope&&current.entries==expected.entries&&receiver.read(expected.scope.address.channel)==std::optional<receive_install_snapshot>{expected_receiver},
                "controller reconciliation authorized postimage differs");}
        receiver.audit();journal.audit();current_sources();
        recovery_continuous_producer::controller_transition_reconcile_owned(*controller,owner,descriptor->phase_,descriptor->barrier_,descriptor->attempt_,
            cancel?4:1,result.next_barrier_,result.next_attempt_);
    });
    scope_probe.reset();
    if(result.settlement_.state!=recovery_install_state::committed){reservation->release();result.reservation_.reset();}
    else if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->observed)
        runtime.probe->observed(cancel?"reconcile-cancel-committed":"reconcile-refreeze-committed");
    return result;
}
void recovery_receiver_controller::controller_reconcile_publish(recovery_reconciliation_result&& result) {
    require(result.descriptor_&&result.reservation_&&result.settlement_.state==recovery_install_state::committed,
        "controller reconciliation publication needs reserved known COMMIT");
    const auto descriptor=std::move(result.descriptor_);auto reservation=std::move(result.reservation_);
    const auto controller=reservation->controller;const auto owner=reservation->owner;
    require(controller&&owner&&owner==descriptor->owner()&&!owner->is_closed(),"controller reconciliation publication retired");
    auto& runtime=*controller->state_;
    {std::lock_guard lock(runtime.mutex);require(reservation->active&&runtime.running&&runtime.reconciliation_reservation==reservation.get()&&
        runtime.reconciliation==descriptor&&runtime.revision==descriptor->controller_revision_&&runtime.revision<UINT64_MAX,
        "controller reconciliation publication reservation changed");}
    // All coordinator preconditions are checked before runtime publication.
    // The reserved result excludes worker turns; real external requests are
    // coalesced until release, and cannot relabel this known COMMIT.
    recovery_continuous_producer::controller_publish_reconcile(*controller,owner,result.next_barrier_,result.next_attempt_);
    std::shared_ptr<const recovery_reconciliation_descriptor> retired;
    {std::lock_guard lock(runtime.mutex);retired=std::move(runtime.reconciliation);++runtime.revision;runtime.failure={};runtime.demand=true;}
    runtime.frozen.reset();runtime.framing_committed=false;
    reservation->release(); // payload/owner release is outside the leaf
    if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->observed)
        runtime.probe->observed(result.step_==recovery_reconciliation_step::cancelled?"reconcile-cancel-published":"reconcile-refreeze-published");
    std::shared_ptr<recovery_receiver_route> next;
    {std::lock_guard lock(runtime.mutex);for(const auto& weak:runtime.routes)if(auto route=weak.lock())if(!route->state_->retired.load()){next=std::move(route);break;}}
    if(next)controller->wake(next);
}
} // namespace lattice::detail
