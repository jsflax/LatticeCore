#include <cstring>
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#include "ordinary_receiver_reclamation_fixture.hpp"
#endif
#ifndef __EMSCRIPTEN__
// Included in the actual fixed directory transport/issuer translation unit.
namespace lattice::detail {
namespace {
struct receiver_slot {std::uint64_t cookie=0;std::weak_ptr<ordinary_receiver_wss_request> owner;};
std::array<receiver_slot,role_cap> receiver_registry;
}
struct ordinary_receiver_wss_state {
    const std::uint64_t process=creator();
    std::optional<role_reservation> charge;
    std::shared_ptr<const ordinary_managed_receiver_preservation> archive;
    std::shared_ptr<ordinary_managed_product_credentials> credentials;
    std::shared_ptr<lattice_db> application;
    std::uint64_t cookie=0;
    clock::time_point deadline{},received_at{};
    std::string member,group,principal,endpoint,model_profile,admission_nonce;
    std::atomic<bool> cancelled{false},received{false},starting{false};
    std::shared_ptr<const ordinary_receiver_history_expectation> expectation;
    std::shared_ptr<const ordinary_unpublished_group_target> unpublished_target;
    std::shared_ptr<ordinary_target_staging_io> staging_io;
    std::shared_ptr<ordinary_target_staging_connection> staging_connection;
    std::shared_ptr<ordinary_group_target_schema_original> target_schema;
    std::weak_ptr<configured_recovery_connection> owner;
    std::shared_ptr<ordinary_group_target_receiving_owner> group_receiver;
    std::mutex leaf;
    static void dispose(ordinary_receiver_wss_state* value)noexcept{
        if(!value||value->process!=creator())return;
        value->unpublished_target.reset();if(value->process!=creator())return;
        value->archive.reset();if(value->process!=creator())return;
        value->application.reset();if(value->process!=creator())return;
        value->credentials.reset();if(value->process!=creator())return;
        value->charge.reset();if(value->process!=creator())return;
        delete value;
    }
    bool live()const noexcept{return process==creator()&&credentials&&credentials->current()&&application&&!application->is_closed()&&!cancelled.load(std::memory_order_acquire);}
};

// The new receiving owner uses this real private request state.
#include "ordinary_group_target_receiving_owner.inc"
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
struct ordinary_receiver_reclamation_fixture::state {
    const std::uint64_t process=creator();
    std::mutex leaf;
    bool capture_returned=false,close_entered=false,close_returned=false;
    std::uint64_t observation=0;
    std::shared_ptr<const ordinary_receiver_wss_request> request;
    std::shared_ptr<ordinary_group_target_receiving_owner> owner;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
    std::condition_variable history_changed;
    const ordinary_group_target_receiving_owner* history_owner_original=nullptr;
    const void* history_state_original=nullptr;
    ordinary_target_lifetime_observation history_latest{},history_ready_original{};
    ordinary_target_raw_object_observation history_raw_latest{},history_raw_ready_original{};
    ordinary_target_programme_observation history_programme_latest{},history_programme_ready_original{};
    ordinary_target_schema_root_observation history_schema_latest{},history_schema_ready_original{};
    ordinary_target_scope_root_observation history_scope_latest{},history_scope_ready_original{};
    bool history_observed=false,history_ready=false,history_stopped=false,history_owner_failure=false;
    bool history_close_entered=false,history_close_returned=false;
    std::exception_ptr history_bridge_first_failure;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
    // One genuine extra shared owner, never a manufactured count or Native.
    std::shared_ptr<ordinary_target_staging_connection> history_extra_alias;
    std::array<std::uint64_t,28> history_alias_before{},history_alias_after{};
    std::array<std::uint64_t,6> history_alias_turn{};
    const void* history_alias_original=nullptr;
    bool history_alias_armed=false,history_alias_control_known=false;
    bool history_alias_call_returned=false,history_alias_call_ended=false,history_alias_turn_returned=false;
    bool history_alias_release_entered=false,history_alias_release_returned=false,history_alias_empty_after=false;
#endif
#endif
};
ordinary_receiver_reclamation_fixture::state& ordinary_receiver_reclamation_fixture::original() {
    // Exactly one actual authenticated owner per qualification process. Debt
    // and first diagnostics remain held through exit, without a slot refund.
    static auto* retained=new state;
    return *retained;
}
void ordinary_receiver_reclamation_fixture::checkpoint(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,ordinary_group_target_receiving_owner& owner) {
    ordinary_receiver_wss_access::require_group_receiving_current(request);
    owner.require_worker_current();
    const auto data=owner.state_;
    require(data&&data->process==creator()&&data->request.lock()==request&&request->state_&&
        std::atomic_load(&request->state_->group_receiver).get()==&owner&&request->state_->staging_connection&&
        request->state_->staging_io&&request->state_->unpublished_target&&!request->state_->target_schema&&
        data->application==request->state_->application&&data->archive==request->state_->archive&&
        data->credentials==request->state_->credentials&&data->end==request->state_->deadline,
        "reclamation checkpoint lacks same actual authenticated constructor request");
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_EXTRA_ENTRY)
    require(request->state_->staging_connection->qualification_create_extra_entry(),
        "reclamation extra-entry fixture lacks actual first native creation/close");
#endif
    {std::lock_guard lock(data->leaf);
        require(!data->qualification_checkpoint&&!data->cleanup_entered&&data->target_preparation_entered&&
            data->worker_entered&&data->creator_thread==std::this_thread::get_id()&&!data->factory_entered,
            "reclamation checkpoint is not actual first receiving-worker preparation");
        data->qualification_checkpoint=true;}
    data->changed.notify_all(); // Actual returned constructor/fault ending, never an input grant.
}
void ordinary_receiver_reclamation_fixture::capture(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& owner)noexcept {
    try {
        if(!request||!request->state_||request->state_->process!=creator()||!owner||!owner->state_||
           owner->state_->process!=creator()||owner->state_->request.lock()!=request||
           std::atomic_load(&request->state_->group_receiver)!=owner)return;
        auto& retained=original();if(retained.process!=creator())return;
        std::lock_guard lock(retained.leaf);
        if(retained.capture_returned||retained.close_entered)return;
        retained.request=request;retained.owner=owner;retained.capture_returned=true;
        // Called only after the actual authenticated native start returned.
        // A terminal request may already be cancelled; capture grants only
        // observation/close of that same owner, never a replacement live start.
    }catch(...){}
}
ordinary_receiver_reclamation_fixture::disposition ordinary_receiver_reclamation_fixture::observe_disposition(
    ordinary_group_target_receiving_owner& owner,disposition result)noexcept {
    const auto data=owner.state_;if(!data||data->process!=creator())return result;
    const auto bits=data->connection?data->connection->qualification_disposition_observation():0;
    {std::lock_guard lock(data->leaf);
        if(data->worker_entered&&data->creator_thread==std::this_thread::get_id()&&!data->qualification_disposition_returned) {
            data->qualification_disposition=result;data->qualification_native_observation=bits;
            data->qualification_disposition_returned=true;
        }}
    return result; // Actual first result unchanged; observation never supplies cleanup authority.
}
void ordinary_receiver_reclamation_fixture::observe_ordinary_fallback(ordinary_group_target_receiving_owner& owner)noexcept {
    const auto data=owner.state_;if(!data||data->process!=creator())return;
    std::lock_guard lock(data->leaf);data->qualification_fallback_entered=true;
}
std::uint64_t ordinary_receiver_reclamation_fixture::close_and_observe()noexcept {
    try {
        auto& retained=original();if(retained.process!=creator())return 0;
        std::shared_ptr<const ordinary_receiver_wss_request> request;
        std::shared_ptr<ordinary_group_target_receiving_owner> owner;
        {std::lock_guard lock(retained.leaf);
            if(retained.close_entered)return retained.close_returned?retained.observation:0;
            if(!retained.capture_returned||!retained.request||!retained.owner)return 0;
            retained.close_entered=true;request=retained.request;owner=retained.owner;}
        const auto data=owner->state_;if(!data||data->process!=creator())return 0;
        const auto until=std::min(request->state_->deadline,clock::now()+std::chrono::seconds(2));
        {std::unique_lock lock(data->leaf);
            if(data->worker_entered&&data->creator_thread==std::this_thread::get_id())return 0;
            data->changed.wait_until(lock,until,[&]{return data->qualification_checkpoint||data->cleanup_entered||data->first_failure;});}
        const auto closed=owner->close(until); // SAME real off-worker scheduler/service ending.
        if(data->process!=creator())return 0;
        std::uint64_t bits=captured;
        {std::lock_guard lock(data->leaf);
            if(data->qualification_checkpoint)bits|=checkpoint_reached;
            if(data->qualification_disposition_returned) {
                bits|=data->qualification_native_observation;
                if(!data->qualification_fallback_entered)bits|=no_fallback;
                if(data->qualification_disposition==disposition::joined)bits|=disposition_joined;
                if(data->qualification_disposition==disposition::retained_debt)bits|=disposition_debt;
            }
            if(data->collected)bits|=collected;else bits|=still_uncollected;
            if(data->worker_joined)bits|=worker_joined;
            if(data->service_released)bits|=service_released;
            if(!data->factory_entered&&!data->native_lane_bound&&data->native_lane_returned&&
               data->lane_result.unstarted_reservation_canceled)bits|=actual_unstarted_lane_cancelled;
        }
        if(closed.cleanup_complete)bits|=actual_close_cleanup_complete;
        if(!closed.error)bits|=actual_close_no_error;
        {std::lock_guard lock(retained.leaf);retained.observation=bits;retained.close_returned=true;}
        return bits;
    }catch(...){return 0;}
}
extern "C" std::uint64_t lattice_receiver_reclamation_fixture_close_v1()noexcept {
    return ordinary_receiver_reclamation_fixture::close_and_observe();
}
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
#include "ordinary_receiver_history_lifecycle_fixture.inc"
#endif
#endif

// No catalog copy or target SQL grant is produced here. Strong actual owner
// custody keeps the immutable full declarations alive; request custody is
// weak so request -> schema original -> request cannot form a cycle.
struct ordinary_group_target_schema_original::state {
    ordinary_target_schema_root_tail terminal_schema_root_{this,1}; // First: all actual payload members end before this tail.
    const std::uint64_t process=creator();
    const clock::time_point end;
    const std::weak_ptr<const ordinary_receiver_wss_request> request;
    const std::shared_ptr<lattice_db> application;
    const std::shared_ptr<const ordinary_managed_receiver_preservation> archive;
    const std::shared_ptr<const ordinary_unpublished_group_target> target;
    const std::shared_ptr<ordinary_target_staging_io> io;
    const std::shared_ptr<ordinary_target_staging_connection> connection;
    const std::shared_ptr<const ordinary_receiver_history_expectation> expectation;
    const recovery_owner_schema* const declarations;
    const std::int32_t version;
    std::exception_ptr first_failure;
    struct terminal_programmes_ending;
    bool programmes_attempted=false,programmes_allocation_returned=false,programmes_allocation_exception_returned=false;
    std::shared_ptr<terminal_programmes_ending> programmes_original;
    std::exception_ptr programmes_first_failure;
    bool catalog_preparation_attempted=false;
    std::shared_ptr<catalog_program> catalog;
    bool row_import_attempted=false;
    std::shared_ptr<ordinary_group_target_schema_original::row_import_program> row_import;
    bool import_suspension_attempted=false;
    std::shared_ptr<ordinary_group_target_schema_original::import_suspension_program> import_suspension;
    bool projection_preparation_attempted=false;
    std::shared_ptr<projection_program> projection;
    state(const std::shared_ptr<const ordinary_receiver_wss_request>& original,
        const std::shared_ptr<ordinary_receiver_wss_state>& value,const recovery_owner_schema* catalog,
        std::int32_t schema_version)
        :end(value->deadline),request(original),application(value->application),archive(value->archive),
         target(value->unpublished_target),io(value->staging_io),connection(value->staging_connection),
         expectation(value->expectation),declarations(catalog),version(schema_version) {terminal_schema_root_.construction_returned_=true;}
    static void dispose(state* value)noexcept {
        if(value&&value->process==creator())delete value;
        // Foreign implicit teardown has no original retirement/cleanup grant.
    }
};
ordinary_group_target_schema_original::ordinary_group_target_schema_original(std::shared_ptr<state> value)
    :terminal_schema_root_(this,0),state_(std::move(value)) {terminal_schema_root_.construction_returned_=true;}
ordinary_group_target_schema_original::~ordinary_group_target_schema_original()=default;
void ordinary_export_access::prepare_receiver_group_target_schema(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,
    std::shared_ptr<ordinary_group_target_schema_original>& retained,ordinary_launch::deadline end) {
    creator_check();
    require(request&&request->state_&&request->state_->process==creator(),"GROUP schema actual request unavailable");
    const auto& value=request->state_;
    require(!retained&&value->application&&value->archive&&value->unpublished_target&&value->staging_io&&
        value->staging_connection&&value->expectation&&!value->group.empty(),"GROUP schema original custody unavailable or already held");
    // Derive every field from the actual completed original. Publish output
    // directly into its request BEFORE current/catalog checks and diagnostics.
    const auto version=value->application->config_.target_schema_version;
    const auto* declarations=&value->application->recovery_declarations();
    auto state=std::shared_ptr<ordinary_group_target_schema_original::state>(
        new ordinary_group_target_schema_original::state(request,value,declarations,version),
        &ordinary_group_target_schema_original::state::dispose);
    auto original=std::shared_ptr<ordinary_group_target_schema_original>(
        new ordinary_group_target_schema_original(std::move(state)),[](auto* owner)noexcept {
            if(owner->state_->process==creator())delete owner;
        });
    retained=std::move(original);
    const auto& held=retained->state_;
    try {
        validate_receiver_group_target_schema(*retained,*held->connection,end);
        // This held typed original is the concrete input for the next private
        // target schema generator/SQL channel. No ready bit, table/model effect,
        // cut/checkpoint/import, source promotion or row disposition is issued.
    }catch(...) {
        if(!held->first_failure)held->first_failure=std::current_exception();
        throw; // SAME request retains original and first failure; no replacement.
    }
}

void ordinary_group_target_schema_original::require_connection(
    const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    if(!state_||state_->process!=creator())throw std::runtime_error("GROUP schema original creator unavailable");
    if(state_->programmes_attempted)throw std::runtime_error("GROUP schema programme ending already selected");
    ordinary_export_access::validate_receiver_group_target_schema(*this,connection,end);
}
void ordinary_export_access::validate_receiver_group_target_schema(
    const ordinary_group_target_schema_original& original,const ordinary_target_staging_connection& connection,
    ordinary_launch::deadline end) {
    creator_check();const auto& held=original.state_;
    require(held&&held->process==creator(),"GROUP schema original creator differs");
    require(!held->programmes_attempted,"GROUP schema programme ending already selected");
    const auto request=held->request.lock();
    require(request&&request->state_&&request->state_->process==creator(),"GROUP schema original request unavailable");
    const auto& value=request->state_;
        // This is a private original/current join, not a validation receipt
        // used as permission. The later native channel must repeat it before
        // each actual generated operation and retain actual native endings.
        require(end==held->end&&end==value->deadline&&clock::now()<end&&request->current()&&
            value->received.load(std::memory_order_acquire)&&value->starting.load(std::memory_order_acquire),
            "GROUP schema original request is not its active start");
        require(&connection==held->connection.get()&&held->request.lock()==request&&held->application==value->application&&held->archive==value->archive&&
            held->target==value->unpublished_target&&held->io==value->staging_io&&held->connection==value->staging_connection&&
            held->expectation==value->expectation,"GROUP schema actual originals changed");
        held->application->require_creator_process();
        require(receiver_restart_parent(*held->archive)==held->application&&receiver_restart_deadline(*held->archive)==end,
            "GROUP schema archived original parent/deadline differs");
        validate_receiver_restart_until(*held->archive,end);
        held->connection->require_original(end);
        held->connection->require_target_identity(held->target.get(),end);
        require(held->connection->io_original(end)==held->io,"GROUP schema actual connection I/O differs");
        held->io->require_same_target(held->target,end);
        require(held->declarations==&held->application->recovery_declarations()&&held->declarations->valid()&&
            held->version>0&&held->version==held->application->config_.target_schema_version,
            "GROUP schema full engine-owned catalog/version unavailable");
        // Fixed GROUP roots are selectors inside the retained full catalog.
        // Generated declarations/constraints, not merely these names, remain
        // in the SAME engine-owned catalog for the later actual generator.
        for(const auto* name:{"Edge","GroupProjectMap","Memory"})
            require(held->declarations->swift_models.contains(name)&&held->declarations->find(name),
                "GROUP schema full catalog lacks a fixed actual root");
        require(!held->declarations->swift_digest.empty()&&!held->declarations->swift_fingerprint.empty(),
            "GROUP schema actual Swift declarations/fingerprint unavailable");
        held->connection->require_original(end);
        require(request->current()&&clock::now()<end,"GROUP schema original retired during preparation");
    require(value->target_schema.get()==&original,"GROUP schema is not the SAME request-retained original");
}
void ordinary_export_access::bootstrap_receiver_group_target_metadata(
    const std::shared_ptr<ordinary_group_target_schema_original>& original,ordinary_launch::deadline end) {
    creator_check();require(original&&original->state_&&original->state_->process==creator(),"GROUP schema original unavailable");
    const auto& held=original->state_;
    try {
        original->require_connection(*held->connection,end);
        held->connection->bootstrap_metadata(original,end);
        original->require_connection(*held->connection,end);
    }catch(...) {
        if(!held->first_failure)held->first_failure=std::current_exception();
        throw;
    }
}

#include "ordinary_group_target_catalog_program.inc"
#include "ordinary_group_target_projection_program.inc"
#include "ordinary_group_target_import_suspension_program.inc"
#include "ordinary_group_target_row_import_program.inc"
#include "ordinary_target_terminal_programmes.inc"
#include "ordinary_target_terminal_schema_roots.inc"

ordinary_receiver_wss_request::ordinary_receiver_wss_request(std::shared_ptr<ordinary_receiver_wss_state> state):state_(std::move(state)){}
ordinary_receiver_wss_request::~ordinary_receiver_wss_request()=default;
bool ordinary_receiver_wss_request::current()const noexcept{
    const auto& state=state_;return state&&state->live()&&
        (state->received.load(std::memory_order_acquire)?clock::now()<state->received_at+std::chrono::seconds(300):clock::now()<state->deadline);
}
bool ordinary_receiver_wss_access::creator_current()noexcept{return creator()==registry_creator;}
std::shared_ptr<ordinary_receiver_wss_request> ordinary_receiver_wss_access::begin(
    std::shared_ptr<const ordinary_managed_receiver_preservation> archive,std::shared_ptr<ordinary_managed_product_credentials> auth){
    role_reservation charge; // Same total64 limit BEFORE owner/cap retention.
    require(archive&&auth&&auth->current(),"receiver requires actual archive and directory credential session");
    const auto end=ordinary_export_access::receiver_restart_deadline(*archive);
    require(clock::now()<end,"receiver original restart admission expired");
    const auto member=ordinary_export_access::receiver_restart_member(*archive);
    std::string group;
    if(member=="sync/memory-synced.sqlite"){}
    else if(member.starts_with("sync/group-")&&member.ends_with(".sqlite"))group=uuid(nlohmann::json(member.substr(11,member.size()-18)));
    else require(false,"receiver archive is not an actual personal/group spoke");
    auto state=std::shared_ptr<ordinary_receiver_wss_state>(new ordinary_receiver_wss_state,&ordinary_receiver_wss_state::dispose);
    state->charge.emplace(std::move(charge));state->deadline=end;state->member=member;state->group=std::move(group);
    state->application=ordinary_export_access::receiver_restart_parent(*archive);state->archive=std::move(archive);state->credentials=std::move(auth);
    state->admission_nonce=uuid_t::generate().to_string();
    state->cookie=reserve_cookie();state->model_profile=state->group.empty()?"engram-personal-v1":"engram-group-v1";
    state->endpoint="wss://"+state->credentials->endpoint_.substr(8)+"/sync";
    // The genuine GROUP request selects the dedicated typed history route.
    // Archived old channel/original observations remain unchanged custody.
    if(!state->group.empty()){auto spelling=state->group;for(auto& c:spelling)if(c>='a'&&c<='f')c-=('a'-'A');state->endpoint+="/group/"+spelling+"/history-v1";}
    require(state->endpoint.size()<=4096,"receiver fixed endpoint exceeds original native bound");
    // Native immutable declarations must contain the fixed selected closure.
    const auto& roots=state->application->recovery_declarations().swift_models;
    const std::vector<std::string> selected=state->group.empty()?std::vector<std::string>{"Edge","Memory","SyncConfig"}:std::vector<std::string>{"Edge","GroupProjectMap","Memory"};
    for(const auto& model:selected)require(roots.contains(model),"receiver actual catalog lacks role-specific fixed selected root");
    auto request=std::shared_ptr<ordinary_receiver_wss_request>(new ordinary_receiver_wss_request(std::move(state)),[](ordinary_receiver_wss_request* value){if(value->state_->process==creator())delete value;});
    {std::lock_guard lock(registry_leaf);auto at=std::find_if(receiver_registry.begin(),receiver_registry.end(),[](const auto& x){return x.owner.expired();});require(at!=receiver_registry.end(),"receiver directory registry exhausted");at->cookie=request->state_->cookie;at->owner=request;}
    return request;
}
class receiver_directory_completion final {
public:
    static std::shared_ptr<ordinary_receiver_wss_request> find(std::uint64_t id){
        if(creator()!=registry_creator||!id)return {};std::shared_ptr<ordinary_receiver_wss_request> out;
        {std::lock_guard lock(registry_leaf);for(const auto& x:receiver_registry)if(x.cookie==id){out=x.owner.lock();break;}}return out;
    }
    static void fail(std::uint64_t id)noexcept{if(auto request=find(id))ordinary_receiver_wss_access::cancel(request);}
    static bool receive(std::uint64_t id,const void* me,size_t me_size,const void* groups,size_t groups_size,const void* status,size_t status_size)noexcept{
        try{auto request=find(id);require(request&&request->current(),"receiver directory request expired");const auto& state=request->state_;
            require(clock::now()<state->deadline&&!state->received.load(std::memory_order_acquire),"receiver directory reply duplicate or expired");
            const auto who=parse(me,me_size,small_body_cap),members=parse(groups,groups_size,group_body_cap),entitlement=parse(status,status_size,small_body_cap);
            require(who.is_object()&&who.contains("id"),"receiver principal absent");const auto principal=uuid(who.at("id"));
            require(members.is_array()&&members.size()<=256,"receiver membership bounds");
            // This third fixed HTTPS body is ONLY the real guard-backed mount
            // admission response. Old status/none/unknown/grace/mode flags are
            // not an alternate proof. Native never relaxes billing statuses.
            require(entitlement.is_object()&&entitlement.size()==7&&entitlement.contains("version")&&entitlement.at("version").is_number_unsigned()&&entitlement.at("version")==1&&
                entitlement.contains("kind")&&entitlement.at("kind")=="receiverAdmission"&&entitlement.contains("request")&&
                uuid(entitlement.at("request"))==state->admission_nonce&&entitlement.contains("principal")&&uuid(entitlement.at("principal"))==principal&&
                entitlement.contains("modelProfile")&&entitlement.at("modelProfile")==state->model_profile&&
                entitlement.contains("channel")&&entitlement.at("channel").is_string()&&entitlement.contains("group"),"receiver authentic mount admission shape/correlation differs");
            const auto sdk_channel=entitlement.at("channel").get<std::string>();
            if(state->group.empty())require(entitlement.at("group").is_null()&&uuid(nlohmann::json(sdk_channel))==principal,"receiver personal SDK channel differs");
            else require(uuid(entitlement.at("group"))==state->group&&sdk_channel.starts_with("group-")&&uuid(nlohmann::json(sdk_channel.substr(6)))==state->group,"receiver actual group SDK channel differs");
            // SDK channel is validated in ITS namespace; expected native channel
            // remains exactly wss:<state->endpoint>, never a relabelled SDK ID.
            require(request->current()&&clock::now()<state->deadline,"receiver mount admission expired or retired");
            bool selected=state->group.empty();std::unordered_set<std::string> seen;
            for(const auto& entry:members){require(entry.is_object()&&entry.contains("id"),"receiver directory member shape");const auto group=uuid(entry.at("id"));require(seen.insert(group).second,"receiver duplicate membership");if(group!=state->group)continue;
                require(entry.contains("myRole")&&entry.at("myRole").is_string(),"receiver selected member role absent");const auto role=entry.at("myRole").get<std::string>();require(role=="owner"||role=="admin"||role=="member","receiver selected role unsupported");
                require(!entry.contains("viaLink")||entry.at("viaLink").is_null()||entry.at("viaLink").is_boolean(),"receiver linked read role type");
                // Actual viaLink membership grants this RECEIVE-ONLY history
                // path; it never becomes producer/contribution/retraction.
                selected=true;
            }
            require(selected&&state->credentials->bind_principal(principal),"receiver membership/principal changed");
            auto expected=std::shared_ptr<const ordinary_receiver_history_expectation>(new ordinary_receiver_history_expectation(
                principal,state->group.empty()?std::optional<std::string>{}:std::optional<std::string>{state->group},
                state->model_profile,state->endpoint,state->member));
            std::lock_guard lock(state->leaf);require(request->current()&&!state->received.load()&&!state->starting.load(),"receiver directory publication retired");state->principal=principal;state->expectation=std::move(expected);state->received_at=clock::now();state->received.store(true,std::memory_order_release);return true;
        }catch(...){if(auto request=find(id))request->state_->cancelled.store(true,std::memory_order_release);return false;}
    }
};
std::string ordinary_receiver_wss_access::requested_group(const std::shared_ptr<const ordinary_receiver_wss_request>& request){
    require(request&&request->current(),"receiver requested member expired");return request->state_->group;
}
std::string ordinary_receiver_wss_access::admission_nonce(const std::shared_ptr<const ordinary_receiver_wss_request>& request){
    require(request&&request->current(),"receiver admission nonce request expired");return request->state_->admission_nonce;
}
std::uint64_t ordinary_receiver_wss_access::cookie(const std::shared_ptr<const ordinary_receiver_wss_request>& request)noexcept{return request&&request->state_->process==creator()?request->state_->cookie:0;}
void ordinary_receiver_wss_access::cancel(const std::shared_ptr<const ordinary_receiver_wss_request>& request)noexcept{
    if(!request||request->state_->process!=creator())return;const auto& state=request->state_;state->cancelled.store(true,std::memory_order_release);std::shared_ptr<configured_recovery_connection> owner;
    {std::lock_guard lock(state->leaf);owner=state->owner.lock();}if(owner)owner->request_close();
    if(auto group=std::atomic_load(&state->group_receiver))group->request_close();
}
bool ordinary_receiver_wss_access::has_current_history_expectation(const std::shared_ptr<const ordinary_receiver_wss_request>& request)noexcept{
    return request&&request->current()&&request->state_->received.load(std::memory_order_acquire)&&bool(request->state_->expectation);
}
const ordinary_receiver_history_expectation& ordinary_receiver_wss_access::expected_history_relation(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request){
    require(request&&request->current()&&request->state_->received.load(std::memory_order_acquire)&&request->state_->expectation,
        "receiver current retained history relation expectation unavailable");
    return *request->state_->expectation; // Request/binding retain it; never replace after publication.
}
bool ordinary_receiver_wss_access::binding_matches(const std::shared_ptr<const ordinary_receiver_wss_request>& request,
    const std::shared_ptr<ordinary_managed_product_credentials>& credentials,const std::shared_ptr<lattice_db>& child,
    const std::string& url,const std::string& token,const std::string& channel)noexcept{
    try{return request&&request->current()&&request->state_->received.load(std::memory_order_acquire)&&credentials==request->state_->credentials&&
        child&&child!=request->state_->application&&!child->is_closed()&&ordinary_export_access::receiver_wss_request(*child)==request&&
        url==request->state_->endpoint&&channel=="wss:"+url&&credentials->matches_configuration(url,token);}catch(...){return false;}
}
std::vector<std::string> ordinary_receiver_wss_access::models_for_actual_child(lattice_db& child,const std::string& selector){
    const auto request=ordinary_export_access::receiver_wss_request(child);if(!request)return {};
    require(request->current()&&request->state_->received.load(std::memory_order_acquire)&&selector==request->state_->model_profile,"receiver fixed native model profile differs");
    const auto selected=request->state_->group.empty()?std::vector<std::string>{"Edge","Memory","SyncConfig"}:std::vector<std::string>{"Edge","GroupProjectMap","Memory"};
    const auto& roots=child.recovery_declarations().swift_models;for(const auto& model:selected)require(roots.contains(model),"receiver actual child lacks role-specific fixed model root");
    return selected;
}

ordinary_managed_wss_admission::ordinary_managed_wss_admission(
    std::shared_ptr<const ordinary_receiver_wss_request> request,std::shared_ptr<lattice_db> application,
    std::string endpoint,std::shared_ptr<const sync_policy_reservation> policy,ordinary_managed_lock_custody&& lock,clock::time_point deadline,
    const std::shared_ptr<ordinary_managed_owner_release>& completion)noexcept
    :released_(completion),receiver_(std::move(request)),creator_(creator()),product_(),application_(std::move(application)),profile_(),
     endpoint_(std::move(endpoint)),policy_(std::move(policy)),lock_(std::move(lock)),initial_deadline_(deadline){lock_.issued_admission();}
clock::time_point ordinary_receiver_wss_access::expires(const std::shared_ptr<const ordinary_receiver_wss_request>& request)noexcept{
    return request&&request->state_->process==creator()&&request->state_->received.load(std::memory_order_acquire)?request->state_->received_at+std::chrono::seconds(300):clock::time_point{};
}
bool ordinary_receiver_wss_access::matches_admission(const std::shared_ptr<const ordinary_receiver_wss_request>& request,
    const std::shared_ptr<lattice_db>& application,const std::shared_ptr<lattice_db>& child,const sync_config& config)noexcept{
    try{return request&&clock::now()<request->state_->deadline&&application==request->state_->application&&
        config.ordinary_policy==ordinary_sync_policy::ordered_history_required&&config.recovery_source_expectation.empty()&&
        config.ordinary_model_profile==request->state_->model_profile&&!config.sync_filter&&config.is_observer&&
        binding_matches(request,config.managed_product_credentials,child,config.websocket_url,config.authorization_token,config.sync_id);}catch(...){return false;}
}
void ordinary_receiver_wss_access::validate_admission(const std::shared_ptr<const ordinary_receiver_wss_request>& request,
    const std::shared_ptr<lattice_db>& application,const std::shared_ptr<lattice_db>& child,const sync_config& config,clock::time_point until){
    require(request&&until!=clock::time_point::max(),"receiver actual new-attempt deadline unavailable");
    // A controller retry supplies its own original30s attempt. This retained
    // factory clamps that attempt to the original archive admission; it does
    // not renew the grant. The bind boundary repeats the archive deadline.
    until=std::min(until,request->state_->deadline);
    require(matches_admission(request,application,child,config)&&clock::now()<until,"receiver actual new-attempt admission expired");
    ordinary_export_access::validate_receiver_restart_until(*request->state_->archive,until);
    require(matches_admission(request,application,child,config)&&clock::now()<until,"receiver currentness changed during owned validation");
}
void ordinary_receiver_wss_access::require_group_receiving_current(const std::shared_ptr<const ordinary_receiver_wss_request>& request) {
    creator_check();require(request&&request->state_&&request->state_->process==creator(),"GROUP receiving original unavailable");
    const auto& value=request->state_;
    require(request->current()&&value->received.load(std::memory_order_acquire)&&value->starting.load(std::memory_order_acquire)&&
        !value->group.empty()&&value->expectation&&value->application&&value->archive&&clock::now()<value->deadline,
        "GROUP receiving current original admission expired or incomplete");
    value->application->require_creator_process();
    require(!value->application->config_.is_sync_enabled()&&!value->application->config_.is_ipc_enabled()&&
        !value->application->recovery_continuous_&&value->application->config_.audit_retention_seconds==0,
        "GROUP receiving archive is not actual dormant application");
    ordinary_export_access::validate_receiver_restart_until(*value->archive,value->deadline);
    require(request->current()&&clock::now()<value->deadline,"GROUP receiving original changed during current join");
}
void ordinary_receiver_wss_access::require_group_history_ingress(const std::shared_ptr<const ordinary_receiver_wss_request>& request) {
    require_group_receiving_current(request);const auto& value=request->state_;
    const auto owner=std::atomic_load(&value->group_receiver);
    require(owner&&value->target_schema&&value->staging_connection&&value->staging_io&&value->unpublished_target,
        "GROUP history ingress lacks its actual retained target/schema chain");
    owner->require_worker_current();
    ordinary_export_access::validate_receiver_group_target_schema(*value->target_schema,*value->staging_connection,value->deadline);
    owner->require_worker_current();
}
void ordinary_receiver_wss_access::prepare_group_on_receiving_worker(const std::shared_ptr<const ordinary_receiver_wss_request>& request) {
    require_group_receiving_current(request);const auto& state=request->state_;const auto end=state->deadline;
    const auto receiver=std::atomic_load(&state->group_receiver);
    require(bool(receiver),"GROUP receiving actual owner unavailable");receiver->require_worker_current();
    receiver->require_worker_current();
    ordinary_export_access::retain_receiver_group_target(*state->archive,state->unpublished_target,end);
    receiver->require_worker_current();
    ordinary_export_access::prepare_receiver_group_target_io(state->unpublished_target,state->staging_io,end);
    receiver->require_worker_current();
    ordinary_export_access::prepare_receiver_group_target_connection(state->unpublished_target,state->staging_io,state->staging_connection,end);
    receiver->require_worker_current();
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE) && !defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
    // The same real authenticated request has returned its actual fixed
    // constructor. Qualification closes this owner before ANY schema work.
    ordinary_receiver_reclamation_fixture::checkpoint(request,*receiver);
    return;
#endif
    ordinary_export_access::prepare_receiver_group_target_schema(request,state->target_schema,end);
    receiver->require_worker_current();
    ordinary_export_access::bootstrap_receiver_group_target_metadata(state->target_schema,end);
    receiver->require_worker_current();
    ordinary_export_access::install_receiver_group_target_catalog(state->target_schema,end);
    receiver->require_worker_current();
}
std::shared_ptr<ordinary_managed_wss_handle> ordinary_receiver_wss_access::start(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,const std::shared_ptr<ordinary_managed_product_credentials>& credentials,const std::string& token){
    creator_check();
    require(request&&request->current()&&request->state_->received.load(std::memory_order_acquire),"receiver start requires actual directory completion");
    ordinary_managed_start_capture_tail start_captures;
    const auto& state=request->state_;const auto end=state->deadline;
    require(clock::now()<end&&credentials==state->credentials&&credentials->matches_configuration(state->endpoint,token),"receiver original admission or credential changed");
    bool was=false;require(state->starting.compare_exchange_strong(was,true,std::memory_order_acq_rel),"receiver request already consumed by actual start");
    const auto application=state->application;
    application->require_creator_process();
    start_captures.retain_application(application);
    std::shared_ptr<const ordinary_managed_owner_release> previous_start;
    {std::lock_guard lock(application->managed_wss_mutex_);
        require(!application->is_closed(),"receiver application is retired before start association");
        require(!application->managed_wss_start_release_||application->managed_wss_start_release_->start_fields_destroyed(),"receiver prior start still owns native captures");
        previous_start=std::move(application->managed_wss_start_release_);
        application->managed_wss_start_release_=start_captures.completion_;
    }
    require(!application->config_.is_sync_enabled()&&!application->config_.is_ipc_enabled()&&!application->recovery_continuous_&&application->config_.audit_retention_seconds==0,"receiver requires actual dormant archived application");
    ordinary_export_access::validate_receiver_restart_until(*state->archive,end);
    if(!state->group.empty()) {
        // New target birth is mounted on the actual dedicated receiving worker,
        // not transferred from this caller's creator-bound target/SQLite owner.
        ordinary_group_target_receiving_owner::prepare(request,state->group_receiver);
        const auto receiver=std::atomic_load(&state->group_receiver);
        try {
            {std::lock_guard lock(credentials->wss_leaf_);
                require(credentials->current(),"GROUP receiving credential revoked before native start");
                auto& owners=credentials->group_receiving_owners_;
                const auto at=std::find_if(owners.begin(),owners.end(),[](const auto& x){return x.expired();});
                require(at!=owners.end(),"GROUP receiving credential owner capacity exhausted");*at=receiver;}
            auto handle=std::shared_ptr<ordinary_managed_wss_handle>(new ordinary_managed_wss_handle(receiver,request),
                [](ordinary_managed_wss_handle* value){if(value->creator_==creator())delete value;});
            receiver->start(token);
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
            ordinary_receiver_reclamation_fixture::capture(request,receiver);
#endif
            return handle; // Actual receiving lifetime; no schema/import/activation result.
        }catch(...) {receiver->remember_failure(std::current_exception());receiver->request_close();throw;}
        require(false,"unpublished GROUP target opening remains closed"); // Never fall through into the old-file child.
    }
    auto handlers=application->configured_listener_cell();std::shared_ptr<configured_recovery_connection> previous;
    {std::lock_guard lock(application->managed_wss_mutex_);require(!application->managed_wss_starting_,"receiver WSS construction already admitted");previous=application->managed_wss_connection_;require(!previous||previous->cleanup_complete(),"receiver WSS predecessor still owns cleanup debt");application->managed_wss_starting_=true;}
    std::shared_ptr<configured_recovery_connection> control;std::shared_ptr<ordinary_managed_wss_admission> admission;
    try{
        const auto factory=get_network_factory();auto* typed=dynamic_cast<configured_platform_factory*>(factory.get());require(factory&&typed,"receiver requires actual typed configured transport factory");
        auto policy=reserve_history_receiver_policy(*application,state->endpoint,state->model_profile);
        const auto lock_path=application->config_.path+".sync.lock";
        const auto main=application->db_->physical_identity("main",{},true);
        ordinary_export_access::validate_receiver_restart_until(*state->archive,end);
        auto retained_request=request;auto retained_application=application;auto retained_endpoint=state->endpoint;
        auto lock_custody=configured_recovery_connection::reserve_managed_lock(main,lock_path,policy,end,start_captures.completion_);
        const auto fd=::open(lock_path.c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK,0600);
        lock_custody.adopt(fd,fd<0?errno:0);require(fd>=0,"receiver ownership lock unavailable");
        struct stat st{};const bool observed=::fstat(fd,&st)==0;
        if(observed)lock_custody.observe(static_cast<std::uint64_t>(st.st_dev),static_cast<std::uint64_t>(st.st_ino));
        require(observed&&S_ISREG(st.st_mode)&&st.st_uid==::geteuid()&&st.st_nlink==1&&(st.st_mode&077)==0&&::flock(fd,LOCK_EX|LOCK_NB)==0,"receiver actual ownership lock refused");
        ordinary_export_access::validate_receiver_restart_until(*state->archive,end);
        auto* raw=new ordinary_managed_wss_admission(std::move(retained_request),std::move(retained_application),std::move(retained_endpoint),std::move(policy),std::move(lock_custody),end,start_captures.completion_);
        admission=std::shared_ptr<ordinary_managed_wss_admission>(raw,[](ordinary_managed_wss_admission* value){if(value->creator_==creator())delete value;});
        sync_config config;config.ordinary_policy=ordinary_sync_policy::ordered_history_required;config.ordinary_model_profile=state->model_profile;
        config.managed_product_credentials=credentials;config.websocket_url=state->endpoint;config.authorization_token=token;config.sync_id="wss:"+state->endpoint;config.all_active_sync_ids={config.sync_id};config.is_observer=true;
        application->config_.tuning.apply(config);
        control=configured_recovery_connection::create_impl(application,config,factory,typed,[application,request,end]{
            require(request->current()&&clock::now()<end&&!application->is_closed(),"receiver same-file child admission expired");
            configuration child_config(application->config_.path,std::make_shared<std_thread_scheduler>());child_config.ordinary_context=application->config_.ordinary_context;
            child_config.wal_extent_limit_bytes=application->config_.wal_extent_limit_bytes;
            child_config.busy_timeout_ms=application->config_.busy_timeout_ms;child_config.audit_retention_seconds=0;
            child_config.target_schema_version=application->config_.target_schema_version;child_config.migration_block=application->config_.migration_block;
            auto child=std::shared_ptr<lattice_db>(ordinary_export_access::child(*application,child_config));
            ordinary_export_access::publish_receiver_restart_child(request->state_->archive,request,*application,*child);return child;
        },admission,std::move(handlers));
        {std::lock_guard lock(credentials->wss_leaf_);require(credentials->current(),"receiver credential revoked before publication");auto& owners=credentials->wss_owners_;const auto at=std::find_if(owners.begin(),owners.end(),[](const auto& x){return x.expired();});require(at!=owners.end(),"receiver credential owner capacity exhausted");*at=control;}
        {std::lock_guard lock(state->leaf);require(request->current(),"receiver directory role revoked before publication");state->owner=control;}
        auto handle=std::shared_ptr<ordinary_managed_wss_handle>(new ordinary_managed_wss_handle(control),[](ordinary_managed_wss_handle* value){if(value->creator_==creator())delete value;});
        {std::lock_guard lock(application->managed_wss_mutex_);require(!application->is_closed()&&admission->current(),"receiver application closed before publication");application->managed_wss_connection_=control;application->managed_wss_starting_=false;}
        control->start();return handle;
    }catch(...){
        state->cancelled.store(true,std::memory_order_release);if(control)control->abandon_start(std::current_exception());
        {std::lock_guard lock(application->managed_wss_mutex_);application->managed_wss_starting_=false;}throw;
    }
}

}
extern "C" bool lattice_managed_receiver_directory_received_v1(std::uint64_t id,const void* me,size_t me_size,const void* groups,size_t groups_size,const void* status,size_t status_size)noexcept{
    return lattice::detail::receiver_directory_completion::receive(id,me,me_size,groups,groups_size,status,status_size);
}
extern "C" void lattice_managed_receiver_directory_failed_v1(std::uint64_t id)noexcept{
    lattice::detail::receiver_directory_completion::fail(id);
}
#else
namespace lattice::detail {
bool ordinary_receiver_wss_access::creator_current()noexcept{return false;}
ordinary_receiver_wss_request::ordinary_receiver_wss_request(std::shared_ptr<ordinary_receiver_wss_state> state):state_(std::move(state)){}
ordinary_receiver_wss_request::~ordinary_receiver_wss_request()=default;
bool ordinary_receiver_wss_request::current()const noexcept{return false;}
bool ordinary_receiver_wss_access::binding_matches(const std::shared_ptr<const ordinary_receiver_wss_request>&,
    const std::shared_ptr<ordinary_managed_product_credentials>&,const std::shared_ptr<lattice_db>&,
    const std::string&,const std::string&,const std::string&)noexcept{return false;}
std::vector<std::string> ordinary_receiver_wss_access::models_for_actual_child(lattice_db&,const std::string&){return {};}
}
extern "C" bool lattice_managed_receiver_directory_received_v1(std::uint64_t,const void*,size_t,const void*,size_t,const void*,size_t)noexcept{return false;}
extern "C" void lattice_managed_receiver_directory_failed_v1(std::uint64_t)noexcept{}
#endif
