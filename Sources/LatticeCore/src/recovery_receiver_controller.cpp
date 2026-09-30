#include "recovery_receiver_controller.hpp"
#include "recovery_unknown_reconciliation.hpp"
#include "recovery_export_adapter.hpp"
#include "recovery_receipt_json.hpp"
#include "recovery_request_store.hpp"
#include "recovery_writer_access.hpp"
#include "recovery_predecessor_wire.hpp"
#include "canonical_writer_adapter.hpp"
#include "recovery_witness.hpp"
#include "sync_callback_lifetime.hpp"
#include "vendor/picosha2/picosha2.h"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <set>

namespace lattice::detail {
namespace {
namespace cr=canonical_range;
using json=nlohmann::json;
constexpr size_t pending_bytes=16777216;
constexpr unsigned admission_retry_attempts=32;
constexpr int64_t admission_retry_window_ms=5000,admission_retry_tick_ms=100;
[[noreturn]] void refuse(const char* reason){throw db_error(reason);}
class controller_admission_wait final {
public:
    const recovery_install_deferred reason;
    explicit controller_admission_wait(recovery_install_deferred value):reason(value){}
};
class delivery_retry_wait final : public db_error {
public:
    delivery_retry_wait():db_error("controller UNKNOWN persisted after one restricted pass; new external source/request generation or actual delivery timeout required"){}
};
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
    if(description.contains("receiptBinding"))value["receiptBinding"]=description.at("receiptBinding");
    auto raw=value.dump();require(raw.size()<=recovery_request_store::context_bytes,"controller source context capacity");return raw;
}
std::string domain(const json& description) {
    auto value=json{{"source",description.at("source")},{"incomingScope",description.at("incomingScope")}};
    // Receipt enrollment changes how an original is proved, not which rows
    // this exact canonical authority/catalog replaces. Keep that enrollment
    // in source_context and separately require one binding across the cohort.
    auto& source=value["source"];for(const auto* key:{"receiptNamespace","coverageID","coverageRevision","descriptorDigest","receiptCoverage"})source.erase(key);
    return picosha2::hash256_hex_string(value.dump());
}
cr::source_binding source_binding(const recovery_obligation_profile& profile) {
    const auto& b=profile.binding;return {b.authority,b.source,b.epoch,b.scope,b.schema};
}
bool terminal_profile(const json& description) {
    const auto& p=description.at("profile");
    if(!predecessor_wire::lifecycle_name(p))return false;
    require(p.contains("orphanResumeGraceMilliseconds")&&p.at("orphanResumeGraceMilliseconds").is_number_integer(),"controller terminal profile grace missing");
    const auto grace=p.at("orphanResumeGraceMilliseconds").get<int64_t>();
    require(grace>0&&grace<=3600000,"controller terminal profile grace invalid");return true;
}
json predecessor_profile(const recovery_request_row& row,const json& description) {
    auto old=parse(row.source_context,recovery_request_store::context_bytes);
    require(old.dump()==row.source_context,"controller prior context encoding differs");
    const auto prior=old.at("profile");const auto current=source_context(description);
    old["profile"]=description.at("profile");require(old.dump()==current,"controller predecessor non-profile context changed");
    predecessor_wire::pair(prior,description.at("profile"),description.contains("receiptBinding"));return prior;
}
template<class Proof,class Route,class View> bool compatible_context(const recovery_request_row& row,const json& description,
    const Proof& proof,const Route& route,const View& view,uint64_t revision,int64_t physical,int64_t phase,int64_t barrier,int64_t attempt,
    const cr::limits& caps) {
    const auto current=source_context(description);if(row.source_context==current)return true;
    (void)predecessor_profile(row,description);
    if(!proof||proof->route.lock()!=route||proof->view.value!=view.value||proof->controller_revision!=revision||
        proof->physical!=physical||proof->phase!=phase||proof->barrier!=barrier||proof->attempt!=attempt||
        proof->journal!=row.journal||proof->row_barrier!=row.barrier||proof->row_sequence!=row.sequence||proof->row_revision!=row.journal_revision||
        proof->domain!=row.domain||proof->old_context!=picosha2::hash256_hex_string(row.source_context)||proof->current_context!=picosha2::hash256_hex_string(current))return false;
    const auto q=cr::decode(row.request_frame,caps);
    return q.logical==proof->logical&&std::get<cr::request>(q.body).request_digest==proof->request_digest;
}
void predecessor_reply_shape(const json& value) {
    members(value,{"kind","version","operation","requestID","routeGeneration","settlement","leaseAvailable"},{"predecessor"});
    settlement_shape(value.at("settlement"));
    require(value.at("kind")=="recoveryReady"&&value.at("version").is_number_integer()&&value.at("version")==1&&value.at("operation")=="predecessor"&&
        value.at("requestID").is_string()&&value.at("requestID").get_ref<const std::string&>().size()==36&&canonical_writer_adapter::uuid_key(value.at("requestID").get<std::string>())==value.at("requestID").get_ref<const std::string&>()&&
        value.at("leaseAvailable").is_boolean()&&value.at("leaseAvailable")==false,"controller predecessor envelope differs");
    (void)decimal(value,"routeGeneration");if(!value.contains("predecessor"))return;
    require(value.at("settlement").at("state")=="committed","controller predecessor without known source COMMIT");
    const auto& body=value.at("predecessor");
    members(body,{"version","transitionID","transitionDigest","beforeProfileDigest","afterProfileDigest","sourceIdentityDigest","disposition",
        "requestDigest","attemptID","sequence","namespaceID","replicaID","receiverIncarnation","channelIncarnation","channel"});
    require(body.dump().size()<=predecessor_wire::body_bytes&&body.at("version").is_number_integer()&&body.at("version")==1&&
        body.at("disposition")=="preserveCompleted","controller predecessor body/version differs");
    for(const auto* key:{"transitionDigest","beforeProfileDigest","afterProfileDigest","sourceIdentityDigest","requestDigest"}){
        require(body.at(key).is_string(),"controller predecessor digest type");const auto& s=body.at(key).get_ref<const std::string&>();
        require(s.size()==64&&std::all_of(s.begin(),s.end(),[](char c){return (c>='0'&&c<='9')||(c>='a'&&c<='f');}),"controller predecessor digest bound");}
    for(const auto* key:{"transitionID","attemptID","receiverIncarnation","channelIncarnation"}){
        require(body.at(key).is_string(),"controller predecessor UUID type");const auto s=body.at(key).get<std::string>();
        require(s.size()==36&&canonical_writer_adapter::uuid_key(s)==s,"controller predecessor UUID differs");}
    for(const auto* key:{"namespaceID","replicaID","channel"})require(body.at(key).is_string()&&!body.at(key).get_ref<const std::string&>().empty()&&
        body.at(key).get_ref<const std::string&>().size()<=256&&body.at(key).get_ref<const std::string&>().find('\0')==std::string::npos,"controller predecessor binding text differs");
    (void)decimal(body,"sequence");
}
void late_predecessor_shape(const json& value,const json& description) {
    predecessor_reply_shape(value);require(terminal_profile(description)&&value.at("routeGeneration")==description.at("routeGeneration"),"controller late predecessor route differs");
    if(!value.contains("predecessor"))return;
    const auto& body=value.at("predecessor");const auto& peer=description.at("peer");
    require(body.at("afterProfileDigest")==predecessor_wire::profile_digest(description.at("profile"))&&
        body.at("namespaceID")==description.at("source").at("receiptNamespace")&&body.at("replicaID")==peer.at("replicaID")&&
        body.at("receiverIncarnation")==peer.at("receiverIncarnation")&&body.at("channelIncarnation")==peer.at("channelIncarnation")&&
        body.at("channel")==description.at("channel"),"controller late predecessor authenticated binding differs");
}
void lifecycle_reply_shape(const json& value) {
    members(value,{"kind","version","operation","requestID","routeGeneration","settlement","leaseAvailable"},{"lifecycle"});
    settlement_shape(value.at("settlement"));
    require(value.at("kind")=="recoveryReady"&&value.at("version").is_number_integer()&&value.at("version")==1&&
        (value.at("operation")=="inspect"||value.at("operation")=="discard")&&value.at("requestID").is_string()&&
        !value.at("requestID").get_ref<const std::string&>().empty()&&value.at("requestID").get_ref<const std::string&>().size()<=128&&value.at("requestID").get_ref<const std::string&>().find('\0')==std::string::npos&&
        value.at("leaseAvailable").is_boolean()&&value.at("leaseAvailable")==false,"controller lifecycle envelope shape differs");
    (void)decimal(value,"routeGeneration");
    if(!value.contains("lifecycle"))return;
    require(value.at("settlement").at("state")=="committed","controller lifecycle body without known source COMMIT");
    const auto& body=value.at("lifecycle");
    members(body,{"state","requestDigest","attemptID","sequence","bindingHighWater","namespaceID","replicaID","receiverIncarnation","channelIncarnation","channel"});
    for(const auto* key:{"state","requestDigest","attemptID","namespaceID","replicaID","receiverIncarnation","channelIncarnation","channel"})
        require(body.at(key).is_string()&&!body.at(key).get_ref<const std::string&>().empty()&&body.at(key).get_ref<const std::string&>().size()<=256&&body.at(key).get_ref<const std::string&>().find('\0')==std::string::npos,
            "controller lifecycle bounded text differs");
    const auto& digest=body.at("requestDigest").get_ref<const std::string&>();
    require(digest.size()==64&&std::all_of(digest.begin(),digest.end(),[](char c){return (c>='0'&&c<='9')||(c>='a'&&c<='f');}),"controller lifecycle digest shape differs");
    for(const auto* key:{"attemptID","receiverIncarnation","channelIncarnation"})
        require(canonical_writer_adapter::uuid_key(body.at(key).get<std::string>())==body.at(key).get_ref<const std::string&>(),"controller lifecycle canonical UUID differs");
    const auto seq=decimal(body,"sequence"),high=decimal(body,"bindingHighWater",0);const auto status=body.at("state").get<std::string>();
    require((status=="available"||status=="terminal"||status=="unstarted")&&(status=="unstarted"?high<seq:high==seq)&&
        (value.at("operation")!="discard"||status=="terminal"),"controller lifecycle status/high-water shape differs");
}
void original_discard_reply_shape(const json& value,const json& description) {
    members(value,{"kind","version","operation","requestID","routeGeneration","settlement","leaseAvailable"});
    settlement_shape(value.at("settlement"));
    require(value.at("kind")=="recoveryReady"&&value.at("version").is_number_integer()&&value.at("version")==1&&
        value.at("operation")=="discard"&&value.at("requestID").is_string()&&value.at("requestID").get_ref<const std::string&>().size()==36&&
        canonical_writer_adapter::uuid_key(value.at("requestID").get<std::string>())==value.at("requestID").get_ref<const std::string&>()&&
        value.at("routeGeneration")==description.at("routeGeneration")&&value.at("leaseAvailable").is_boolean()&&value.at("leaseAvailable")==false,
        "controller original discard envelope differs");
    (void)decimal(value,"routeGeneration");
}
// Late lifecycle traffic can only be disposed, never published as proof.
void late_lifecycle_shape(const json& value,const json& description) {
    lifecycle_reply_shape(value);
    require(terminal_profile(description)&&value.at("routeGeneration")==description.at("routeGeneration"),
        "controller late lifecycle physical route differs");
    if(!value.contains("lifecycle"))return; // legal body-absent late failure
    const auto& body=value.at("lifecycle");const auto& peer=description.at("peer");
    require(body.at("namespaceID")==description.at("source").at("receiptNamespace")&&
        body.at("replicaID")==peer.at("replicaID")&&body.at("receiverIncarnation")==peer.at("receiverIncarnation")&&
        body.at("channelIncarnation")==peer.at("channelIncarnation")&&body.at("channel")==description.at("channel"),
        "controller late lifecycle authenticated binding differs");
}
bool canceled_predecessor(const recovery_request_row& row,const recovery_obligation_scope& scope,
    const receive_install_snapshot& receiver,const cr::request& q,int64_t phase,int64_t barrier,int64_t attempt) {
    if((phase!=1&&phase!=2)||barrier<=1||attempt<=1||row.barrier!=barrier-1||row.sequence!=attempt-1||
       row.journal.generation>INT64_MAX-phase||row.journal_revision>INT64_MAX-phase||
       scope.address.incarnation!=row.journal.incarnation||scope.address.generation!=row.journal.generation+phase||
       scope.revision!=row.journal_revision+phase||scope.last_attempt!=(phase==1?row.sequence:attempt)||
       scope.mode!=(phase==1?recovery_obligation_mode::recording:recovery_obligation_mode::frozen)||
       scope.freeze_revision!=(phase==1?row.journal_revision:scope.revision)||receiver.active||
       receiver.binding!=scope.profile.binding||receiver.last_sequence!=row.sequence||
       (receiver.last_installed&&receiver.last_installed->sequence>=row.sequence)||
       receiver.revision!=scope.installed_revision||uint64_t(receiver.revision)!=q.expected.revision)return false;
    const cr::frontier baseline{receiver.frontier.kind==receive_frontier_kind::position?cr::frontier_kind::position:
        receiver.frontier.kind==receive_frontier_kind::beginning_null?cr::frontier_kind::beginning_null:cr::frontier_kind::uninitialized,
        receiver.frontier.position?std::optional<uint64_t>{static_cast<uint64_t>(*receiver.frontier.position)}:std::nullopt};
    if(baseline!=q.expected.base)return false;
    return scope.installed_sequence==0?!receiver.last_installed&&receiver.frontier==receive_install_frontier{}:
        receiver.last_installed&&receiver.last_installed->sequence==scope.installed_sequence&&
        receiver.last_installed->head==scope.installed_head&&receiver.last_installed->manifest_digest==scope.installed_manifest&&
        receiver.frontier==receive_install_frontier{receive_frontier_kind::position,scope.installed_head};
}
void known(const recovery_install_result& result) {
    if(result.deferred!=recovery_install_deferred::none) {
        require(result.state==recovery_install_state::refused&&!result.primary_error&&!result.cleanup_error&&
            !result.postcommit_error&&!result.notification_error&&!result.unexpected_commit_observed,
            "controller invalid no-effect admission outcome");
        throw controller_admission_wait{result.deferred};
    }
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
    // Set only by worker validation of the actual current orphan profile.
    // Coordinator mutex guards this weak record; it cannot retain old sources.
    std::weak_ptr<const receiver_source_binding::record> late_control_view;
    // Original-profile late discard is admitted only after this worker issued
    // a completed-predecessor disposal on the exact current physical view.
    std::weak_ptr<const receiver_source_binding::record> issued_discard_view;
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
    uint64_t delivery_retry_revision=0,reconciled_delivery_retry_revision=0;
    bool awaiting_delivery_retry=false;
    std::exception_ptr failure;
    // One episode, driven by the existing native 100ms receiver tick. A wake
    // does not extend either the original deadline or the attempt allowance.
    struct admission_episode {
        uint64_t revision=0;
        unsigned attempts=0;
        int64_t deadline=0,next_due=0;
    } admission_wait;
    std::shared_ptr<const verified_unsent_set> frozen;
    std::shared_ptr<const recovery_reconciliation_descriptor> reconciliation;
    int64_t frozen_attempt=0,frozen_barrier=0;
    bool framing_committed=false;
    // Reservations outlive the outstanding pointer when a callback or worker
    // still owns bytes. Only reservations run under mutex; atomic releases
    // happen after payload destruction and never need the coordinator leaf.
    struct inbox_budget { std::atomic<size_t> bytes{0},slots{0}; };
    struct charge {
        std::shared_ptr<inbox_budget> budget;
        size_t bytes=0;bool slot=false;
        explicit charge(std::shared_ptr<inbox_budget> value):budget(std::move(value)){}
        ~charge(){budget->bytes.fetch_sub(bytes);if(slot)budget->slots.fetch_sub(1);}
        void shrink(size_t actual){require(actual<=bytes,"controller reservation exceeded");budget->bytes.fetch_sub(bytes-actual);bytes=actual;}
    };
    std::shared_ptr<inbox_budget> budget=std::make_shared<inbox_budget>();
    std::shared_ptr<charge> reserve_locked(size_t bytes,bool slot) {
        if(bytes>pending_bytes-budget->bytes.load()||(slot&&budget->slots.load()>=2))return {};
        auto result=std::make_shared<charge>(budget);result->bytes=bytes;result->slot=slot;
        budget->bytes.fetch_add(bytes);if(slot)budget->slots.fetch_add(1);return result;
    }
    struct reply {
        std::shared_ptr<charge> reservation; // destroyed after bytes
        std::string bytes;
        uint64_t order=0;
        std::weak_ptr<recovery_receiver_route> late_route;
        receiver_source_binding::recovery_view late_view;
        bool ready=false; // published under mutex, bytes immutable thereafter
    };
    struct disposal_key {
        int64_t physical=0,phase=2,barrier=0,attempt=0;uint64_t revision=0;
        recovery_obligation_address journal,frozen_journal;
        int64_t row_barrier=0,row_sequence=0,row_revision=0,row_route=0,frozen_revision=0;
        cr::attempt logical;
        std::string request_digest,request_hash,manifest_hash,old_context,old_domain,current_context,cohort;
        receive_install_snapshot receiver;
        bool installed=false;
        bool operator==(const disposal_key&)const=default;
    };
    struct disposal {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        disposal_key key;
    };
    // Worker-only compact receipts, at most one per configured contribution.
    // Callbacks revoke route/view/revision authority without touching this map.
    std::map<std::string,disposal> disposals;
    struct pending {
        std::shared_ptr<charge> reservation; // destroyed after request/inbox
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        // Identity, request bytes and deadline are immutable after publication.
        std::string request_id,operation,request_bytes;
        int64_t deadline=0;
        uint64_t index=0,next_order=0;
        bool terminal_inbox=false;
        std::optional<disposal> completed_disposal;
        int64_t proof_physical=0,proof_phase=0,proof_barrier=0,proof_attempt=0;
        uint64_t proof_revision=0;
        std::array<std::shared_ptr<reply>,2> inbox;
    };
    std::shared_ptr<pending> outstanding;
    // Separate pointer inventory, SAME two-slot/16MiB global reservations.
    std::array<std::shared_ptr<reply>,2> late_inbox;
    uint64_t late_order=0;
    struct lease {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        std::string id,request_digest,attempt_id;
        uint64_t sequence=0,frames=0;int64_t deadline=0;
    };
    // Ephemeral authority only: each result belongs to the current actual
    // physical source view and full Q. Reopen reacquires it from the source.
    struct lifecycle {
        std::weak_ptr<recovery_receiver_route> route;
        receiver_source_binding::recovery_view view;
        canonical_range::attempt attempt;
        std::string request_digest,state;
    };
    std::map<std::string,lifecycle> lifecycles;
    std::map<std::string,std::shared_ptr<const recovery_reconciliation_descriptor::predecessor>> predecessors;
    std::set<std::string> inspect_required;
    bool terminal_rearm=false;
    int64_t terminal_successor_attempt=0; // observation only; never authority
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
                    runtime.demand=true;runtime.failure={};runtime.awaiting_delivery_retry=false;retired=std::move(runtime.reconciliation);}}
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
void recovery_receiver_controller::validate_reopen_owned(std::shared_ptr<lattice_db> owner,const recovery_continuous_policy& policy,int64_t phase,int64_t barrier,int64_t attempt) {
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
            }else require((phase==2&&framing->sequence==attempt&&framing->barrier==barrier&&framing->journal==scope->address&&framing->journal_revision==scope->revision)||
                canceled_predecessor(*framing,*scope,*actual,*q,phase,barrier,attempt),"controller manifestless Q lacks exact current or canceled predecessor");
            if(framing->manifest_frame.empty()&&(phase==1||phase==2)&&framing->sequence==attempt-1)
                require(canceled_predecessor(*framing,*scope,*actual,*q,phase,barrier,attempt),"controller reopened canceled predecessor differs");
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
    std::shared_ptr<state::pending> released;
    std::array<std::shared_ptr<state::reply>,2> retired;
    {std::lock_guard lock(state_->mutex);
        if(state_->outstanding&&state_->outstanding->route.expired())released=std::move(state_->outstanding);
        for(size_t i=0;i<retired.size();++i)if(state_->late_inbox[i]&&state_->late_inbox[i]->late_route.expired())
            retired[i]=std::move(state_->late_inbox[i]);}
    // In-flight callbacks/worker retain their charges until actual disposal.
    // Durable barrier/Q survives; a later actual route can continue it.
}
void recovery_receiver_route::wake(){if(!state_->retired.load(std::memory_order_acquire))controller_->wake(shared_from_this());}
void recovery_receiver_route::notifications(std::function<void()> resumed,std::function<void()> renew,std::function<void(std::exception_ptr)> error,std::function<void()> reconcile){state_->resumed=std::move(resumed);state_->renew=std::move(renew);state_->error=std::move(error);state_->reconcile=std::move(reconcile);}
void recovery_receiver_route::request(){
    std::shared_ptr<const recovery_reconciliation_descriptor> retired;
    {std::lock_guard lock(controller_->state_->mutex);auto& state=*controller_->state_;
        require(state.revision!=UINT64_MAX&&state.external_revision!=UINT64_MAX,"controller demand revision exhausted");
        if(state.reconciliation_reservation)state.deferred_external_request=true;
        else {++state.revision;++state.external_revision;state.demand=true;state.failure={};state.awaiting_delivery_retry=false;retired=std::move(state.reconciliation);}}
    retired.reset();wake();
}
std::function<void()> recovery_receiver_route::delivery_timeout_retry(const committed_export_frame& frame,uint64_t physical) {
    if(!frame.reconciliation_)return {};
    const auto& grant=*frame.reconciliation_;const auto& descriptor=grant.descriptor_;
    require(descriptor&&descriptor->phase_==4&&!frame.consumed_&&!frame.entries_.empty()&&frame.physical_generation_==physical&&
        frame.owner_==state_->owner.lock(),"delivery retry lacks actual restricted frame");
    require(grant.contribution_<descriptor->contributions_.size(),"delivery retry contribution missing");
    const auto& part=descriptor->contributions_[grant.contribution_];
    require(part.route.lock().get()==this&&part.source==state_->source,"delivery retry frame route differs");
    // An obsolete parked frame must reach the actual handoff's stale disposal
    // path. It cannot issue a retry event, but staleness is not a new error.
    if(state_->retired.load()||!state_->lifetime->current(physical)||!part.source->recovery_live(part.view))return {};
    {
        std::lock_guard lock(controller_->state_->mutex);const auto& runtime=*controller_->state_;
        if(runtime.reconciliation!=descriptor||runtime.revision!=descriptor->controller_revision_||
            runtime.external_revision!=descriptor->external_revision_)return {};
    }
    // No descriptor/cohort/database/transport retention in the ACK worker.
    // The actual send registration separately fences which IDs may expire.
    return [weak=weak_from_this(),record=std::weak_ptr<const receiver_source_binding::record>(part.view.value),
        physical,external=descriptor->external_revision_,retry=descriptor->delivery_retry_revision_] {
        const auto route=weak.lock();const auto current=record.lock();
        if(!route||!current||route->state_->retired.load()||!route->state_->lifetime->current(physical))return;
        const receiver_source_binding::recovery_view view{current};
        if(!route->state_->source->recovery_live(view))return;
        {
            std::lock_guard lock(route->controller_->state_->mutex);auto& runtime=*route->controller_->state_;
            if(runtime.external_revision!=external||runtime.delivery_retry_revision!=retry)return;
            require(runtime.delivery_retry_revision!=UINT64_MAX,"controller delivery retry revision exhausted");
            ++runtime.delivery_retry_revision;runtime.demand=true;
            // This event never replaces a live descriptor or changes its
            // ordinary revision, including during known-COMMIT publication.
            // A timeout preceding fresh UNKNOWN remains pending in the counter.
            if(runtime.awaiting_delivery_retry){runtime.failure={};runtime.awaiting_delivery_retry=false;}
        }
        const auto probe=route->controller_->state_->probe;
        if(probe&&probe->owner==route->state_->owner.lock().get()&&probe->observed)
            probe->observed("delivery-retry-admitted");
        route->wake();
    };
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
    if(!pending) {
        receiver_source_binding::recovery_view view;
        {std::lock_guard lock(coordinator.mutex);view.value=state_->late_control_view.lock();}
        // Initial/renewed describe still belongs to the original verifier.
        // This lane admits only an already worker-verified physical record.
        if(!view.value||!state_->source->recovery_matches(view,endpoint,lifecycle))return false;
        try {
            require(message.data.size()<=recovery_request_store::frame_bytes,"controller late response byte capacity");
            auto reply=std::make_shared<recovery_receiver_controller::state::reply>();
            reply->late_route=shared_from_this();reply->late_view=view;
            size_t slot=0;
            {std::lock_guard lock(coordinator.mutex);
                // Publication and late admission share this leaf. If a new
                // request won the race, retain its ordinary correlation path.
                pending=coordinator.outstanding;
                if(!pending){
                    while(slot<coordinator.late_inbox.size()&&coordinator.late_inbox[slot])++slot;
                    require(slot<coordinator.late_inbox.size(),"controller bounded late response inbox full");
                    require(coordinator.late_order!=UINT64_MAX,"controller late response order exhausted");
                    reply->reservation=coordinator.reserve_locked(message.data.size(),true);
                    require(reply->reservation!=nullptr,"controller retained late response capacity");
                    reply->order=++coordinator.late_order;coordinator.late_inbox[slot]=reply;}}
            if(!pending){
                try {reply->bytes.assign(raw);}
                catch(...){
                    {std::lock_guard lock(coordinator.mutex);if(coordinator.late_inbox[slot]==reply)coordinator.late_inbox[slot].reset();}
                    throw;
                }
                const bool live=state_->source->recovery_live(view);
                {std::lock_guard lock(coordinator.mutex);
                    if(coordinator.late_inbox[slot]!=reply)return true;
                    if(!live||state_->retired.load()||state_->late_control_view.lock()!=view.value)coordinator.late_inbox[slot].reset();
                    else reply->ready=true;}
                wake();return true;
            }
        }catch(...){
            const auto failure=std::current_exception();
            // Preserve source.receive's exact-view revocation on newly
            // intercepted late admission failures, after every leaf unwinds.
            bool invalidated=false;
            try {invalidated=state_->source->recovery_live(view)&&state_->source->invalidate(view.value);}
            catch(...){std::rethrow_exception(failure);}
            if(invalidated)std::rethrow_exception(failure);
            return true; // a retired/replaced view cannot revoke its successor
        }
    }
    if(pending->route.lock().get()!=this||!state_->source->recovery_matches(pending->view,endpoint,lifecycle))return true;
    require(message.data.size()<=recovery_request_store::frame_bytes,"controller pending response byte capacity");
    auto reply=std::make_shared<recovery_receiver_controller::state::reply>();
    size_t slot=0;
    {std::lock_guard lock(coordinator.mutex);if(coordinator.outstanding!=pending)return true;
        const size_t limit=pending->terminal_inbox?2:1;
        while(slot<limit&&pending->inbox[slot])++slot;
        require(slot<limit,"controller bounded response inbox full");
        require(pending->next_order!=UINT64_MAX,"controller response order exhausted");
        reply->reservation=coordinator.reserve_locked(message.data.size(),true);
        require(reply->reservation!=nullptr,"controller retained response capacity");
        reply->order=++pending->next_order;pending->inbox[slot]=reply;}
    try {reply->bytes.assign(raw);}
    catch(...){
        // The local reference retains its charge beyond the leaf on failure.
        {std::lock_guard lock(coordinator.mutex);if(pending->inbox[slot]==reply)pending->inbox[slot].reset();}
        throw;
    }
    {std::lock_guard lock(coordinator.mutex);if(coordinator.outstanding!=pending)return true;
        reply->ready=true;}
    wake();return true;
}
void recovery_receiver_controller::turn() {
    auto& runtime=*state_;
    {std::lock_guard lock(runtime.mutex);runtime.scheduled=false;if(runtime.running)return;runtime.running=true;}
    struct settlement {recovery_receiver_controller::state& value;std::function<void()> after;
        // Admission-retry composition consumes this flag. Merely disposing a
        // late frame (or waiting for its copy) is not recovery progress and
        // must not reset an already-running admission deadline/allowance.
        bool keep_admission_wait=false;
        ~settlement(){{std::lock_guard lock(value.mutex);value.running=false;if(!keep_admission_wait)value.admission_wait={};}if(after)try{after();}catch(...) {}}} settle{runtime,{}};
    std::weak_ptr<lattice_db> observed_owner;
    int64_t admission_deadline=now()+admission_retry_window_ms;
    uint64_t admission_revision=0;
    try {
        // Drain before idle/failure/cohort/source-count returns: even a dead
        // route's payload must leave the shared byte/slot budget. No SQL or
        // lifecycle proof publication is permitted in this bounded lane.
        for(unsigned quantum=0;quantum<2;++quantum) {
            std::shared_ptr<state::reply> front;
            {std::lock_guard lock(runtime.mutex);
                for(const auto& reply:runtime.late_inbox)if(reply&&(!front||reply->order<front->order))front=reply;
                if(!front)break;
                settle.keep_admission_wait=true;
                if(!front->ready)return;}
            auto route=front->late_route.lock();
            if(route&&!route->state_->retired.load()&&route->state_->source->recovery_live(front->late_view)) {
                observed_owner=route->state_->owner;
                try {
                    const auto description=parse(route->state_->source->recovery_description(front->late_view),65536);
                    const auto late=parse(front->bytes,recovery_request_store::frame_bytes);
                    if(terminal_profile(description)) {
                        if(late.value("operation",std::string{})=="predecessor")late_predecessor_shape(late,description);
                        else late_lifecycle_shape(late,description);
                    } else {
                        {std::lock_guard lock(runtime.mutex);require(route->state_->issued_discard_view.lock()==front->late_view.value,"controller unsolicited original late discard");}
                        original_discard_reply_shape(late,description);
                    }
                }catch(...){
                    const auto failure=std::current_exception();
                    if(runtime.probe&&runtime.probe->owner==route->state_->owner.lock().get()&&runtime.probe->observed)
                        runtime.probe->observed("late-lifecycle-validation-rejected");
                    // Idle routes may already permit ordinary export. Revoke
                    // exactly this current source before reporting its error;
                    // replacement/retirement must not poison another view.
                    bool invalidated=false;
                    try {invalidated=route->state_->source->recovery_live(front->late_view)&&
                        route->state_->source->invalidate(front->late_view.value);}
                    catch(...){std::rethrow_exception(failure);}
                    if(invalidated)std::rethrow_exception(failure);
                }
                // Shape and actual binding permit disposal only. The route
                // may retire during parsing; neither case creates authority.
                if(route->state_->source->recovery_live(front->late_view)&&runtime.probe&&
                    runtime.probe->owner==route->state_->owner.lock().get()&&runtime.probe->observed)
                    runtime.probe->observed("late-lifecycle-discarded");
            }
            if(route&&!route->state_->source->recovery_live(front->late_view)&&runtime.probe&&
                runtime.probe->owner==route->state_->owner.lock().get()&&runtime.probe->observed)
                runtime.probe->observed("late-lifecycle-retired-disposed");
            {std::lock_guard lock(runtime.mutex);for(auto& reply:runtime.late_inbox)if(reply==front)reply.reset();}
        }
        {std::lock_guard lock(runtime.mutex);
            if(std::any_of(runtime.late_inbox.begin(),runtime.late_inbox.end(),[](const auto& reply){return bool(reply);})) {
                settle.keep_admission_wait=true;return;
            }}
        uint64_t current_revision;{std::lock_guard lock(runtime.mutex);current_revision=runtime.revision;}
        for(auto at=runtime.disposals.begin();at!=runtime.disposals.end();) {
            auto route=at->second.route.lock();
            if(at->second.key.revision!=current_revision||!route||route->state_->retired.load()||!route->state_->source->recovery_live(at->second.view))at=runtime.disposals.erase(at);
            else ++at;
        }
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
            const bool late_control=terminal_profile(description);
            {std::lock_guard lock(runtime.mutex);route->state_->late_control_view=
                (late_control||route->state_->issued_discard_view.lock()==view->value)?std::weak_ptr<const receiver_source_binding::record>(view->value):std::weak_ptr<const receiver_source_binding::record>{};}
            const auto prior=connected_routes.find(channel);
            if(prior==connected_routes.end()||decimal(description,"routeGeneration")>decimal(prior->second.description,"routeGeneration"))
                connected_routes.insert_or_assign(channel,connected{route,*view,std::move(description),std::move(scope)});
        }
        if(connected_routes.size()!=runtime.policy.contributions.size())return;
        auto owner=connected_routes.begin()->second.route->state_->owner.lock();if(!owner||owner->is_closed())return;
        observed_owner=owner;
        std::string common_domain;
        const auto common_receipt_binding=connected_routes.begin()->second.description.value("receiptBinding",json{});
        for(const auto& [channel,c]:connected_routes) {
            const auto key=domain(c.description);if(common_domain.empty())common_domain=key;
            require(key==common_domain,"controller overlapping replacement authority requires explicit configuration");
            require(c.description.value("receiptBinding",json{})==common_receipt_binding,"controller receipt producer or cohort differs across contributions");
        }
        for(const auto& [channel,c]:connected_routes) {
            if(!runtime.observed.count(channel)||runtime.observed.at(channel).value!=c.view.value){
                {std::lock_guard lock(runtime.mutex);require(runtime.revision!=UINT64_MAX&&runtime.external_revision!=UINT64_MAX,"controller demand revision exhausted");++runtime.revision;++runtime.external_revision;runtime.demand=true;runtime.failure={};runtime.awaiting_delivery_retry=false;}
                std::shared_ptr<const recovery_reconciliation_descriptor> retired;
                {std::lock_guard lock(runtime.mutex);retired=std::move(runtime.reconciliation);}
                runtime.observed[channel]=c.view;runtime.frozen.reset();runtime.framing_committed=false;
                runtime.lifecycles.clear();runtime.inspect_required.clear();runtime.terminal_rearm=false;runtime.leases.clear();runtime.predecessors.clear();runtime.disposals.clear();
            }
        }
        for(const auto& [_,c]:connected_routes)
            admission_deadline=std::min(admission_deadline,now()+c.route->state_->source->recovery_remaining(c.view));
        bool reconciliation_waiting=false;
        {std::lock_guard lock(runtime.mutex);reconciliation_waiting=static_cast<bool>(runtime.reconciliation);
            if(!reconciliation_waiting&&(runtime.failure||(runtime.idle&&!runtime.demand&&!runtime.outstanding)))return;
            admission_revision=runtime.revision;
            if(runtime.outstanding)admission_deadline=std::min(admission_deadline,runtime.outstanding->deadline);
            auto& wait=runtime.admission_wait;
            if(wait.attempts&&wait.revision!=runtime.revision)wait={};
            if(wait.attempts){
                wait.deadline=std::min(wait.deadline,admission_deadline);
                require(now()<wait.deadline&&wait.attempts<admission_retry_attempts,"controller admission retry budget exhausted; gate remains closed");
                if(now()<wait.next_due){settle.keep_admission_wait=true;return;}
            }}
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
        const auto owned=[&](const std::function<void(database&)>& body){const auto result=recovery_continuous_producer::controller_owned(*this,owner,[&](database& db){live();body(db);live();},true);known(result);settle.keep_admission_wait=false;};
        // Read and validate the COMPLETE local precursor before any remote
        // disposal. The installed branch deliberately precedes old context
        // parsing; only canceled predecessors need compatibility authority.
        const auto disposal_snapshot=[&](int64_t physical,int64_t barrier,int64_t attempt,uint64_t revision) {
            require(runtime.frozen&&runtime.frozen_attempt==attempt&&runtime.frozen_barrier==barrier,"controller disposal lacks frozen cohort");
            recovery_continuous_producer::verify_for_owned_write(*runtime.frozen);
            auto* writer=recovery_writer_access::active_writer(*owner);require(writer!=nullptr,"controller disposal requires owned writer");
            const auto phase=writer->query("SELECT incarnation,phase,barrier,attempt FROM main._lattice_producer_continuity WHERE id=1");
            require(phase.size()==1&&integer(phase[0],"incarnation")==physical&&integer(phase[0],"phase")==2&&
                integer(phase[0],"barrier")==barrier&&integer(phase[0],"attempt")==attempt,"controller disposal durable phase changed");
            {std::lock_guard lock(runtime.mutex);require(runtime.revision==revision,"controller disposal generation changed");}
            recovery_request_store requests(owner);receive_install_store receiver(owner,runtime.caps.install.installations);receiver.audit();
            const auto fingerprints=requests.fingerprints();
            for(const auto& [channel,_]:fingerprints)require(connected_routes.count(channel),"controller disposal unknown request channel");
            const auto cohort=picosha2::hash256_hex_string(json(fingerprints).dump());
            std::map<std::string,state::disposal> result;
            for(const auto& frozen:runtime.frozen->frozen_journals()) {
                const auto& channel=frozen.scope.address.channel;const auto& c=connected_routes.at(channel);
                const auto current=receiver.read(channel);require(current&&current->binding==frozen.scope.profile.binding,
                    "controller disposal actual receiver unavailable");
                const auto row=requests.read(channel);if(!row){require(!current->active&&!current->last_installed,"controller initial Q receiver has prior state");continue;}
                const auto oldq=cr::decode(row->request_frame,runtime.caps.codec);const auto* old_request=std::get_if<cr::request>(&oldq.body);
                require(old_request&&oldq.logical.channel==channel&&oldq.logical.sequence==uint64_t(row->sequence)&&
                    old_request->source==source_binding(frozen.scope.profile),"controller disposal durable Q identity differs");
                const auto compatibility=[&]{const auto at=runtime.predecessors.find(channel);
                    const auto proof=at==runtime.predecessors.end()?std::shared_ptr<const recovery_reconciliation_descriptor::predecessor>{}:at->second;
                    return compatible_context(*row,c.description,proof,c.route,c.view,revision,physical,2,barrier,attempt,runtime.caps.codec);};
                if(row->sequence==attempt){require(row->journal==frozen.scope.address&&row->journal_revision==frozen.scope.revision&&row->barrier==barrier&&
                    compatibility()&&row->domain==common_domain,"controller disposal current Q binding changed");continue;}
                require(!current->active,"controller disposal predecessor receiver is active");
                bool installed=false;
                if(!row->manifest_frame.empty()) {
                    const auto oldm=cr::decode(row->manifest_frame,runtime.caps.codec);
                    require(oldm.logical==oldq.logical&&std::holds_alternative<cr::manifest>(oldm.body),"controller disposal durable M identity differs");
                    const auto identity=describe_canonical_range(oldq.logical,std::get<cr::request>(oldq.body),std::get<cr::manifest>(oldm.body),runtime.caps.codec,row->route).installation_identity;
                    installed=current->last_installed==std::optional<receive_install_identity>{identity};
                }
                const bool canceled=!installed&&compatibility()&&row->domain==common_domain&&
                    (row->manifest_frame.empty()?canceled_predecessor(*row,frozen.scope,*current,std::get<cr::request>(oldq.body),2,barrier,attempt):
                    row->sequence==attempt-1&&row->barrier==barrier-1&&current->last_sequence==row->sequence&&
                    (!current->last_installed||current->last_installed->sequence<row->sequence)&&frozen.scope.address.incarnation==row->journal.incarnation&&
                    row->journal.generation<=INT64_MAX-2&&frozen.scope.address.generation==row->journal.generation+2&&frozen.scope.last_attempt==attempt);
                require(installed||canceled,"controller disposal predecessor lacks installed or canceled evidence");
                state::disposal receipt;receipt.route=c.route;receipt.view=c.view;auto& key=receipt.key;
                key.physical=physical;key.barrier=barrier;key.attempt=attempt;key.revision=revision;
                key.journal=row->journal;key.frozen_journal=frozen.scope.address;key.frozen_revision=frozen.scope.revision;
                key.row_barrier=row->barrier;key.row_sequence=row->sequence;key.row_revision=row->journal_revision;key.row_route=row->route;
                key.logical=oldq.logical;key.request_digest=std::get<cr::request>(oldq.body).request_digest;
                key.request_hash=picosha2::hash256_hex_string(row->request_frame);key.manifest_hash=picosha2::hash256_hex_string(row->manifest_frame);
                key.old_context=picosha2::hash256_hex_string(row->source_context);key.old_domain=picosha2::hash256_hex_string(row->domain);
                key.current_context=picosha2::hash256_hex_string(source_context(c.description));key.cohort=cohort;key.receiver=*current;key.installed=installed;
                require(result.size()<runtime.policy.contributions.size(),"controller disposal receipt capacity");result.emplace(channel,std::move(receipt));
            }
            live();return result;
        };
        const auto same_disposal=[](const state::disposal& a,const state::disposal& b){
            return a.route.lock()==b.route.lock()&&a.view.value==b.view.value&&a.key==b.key;
        };
        const auto send_control=[&](const connected& c,const std::string& op,uint64_t index,const auto& fill,
            int64_t physical=0,int64_t phase=0,int64_t barrier=0,int64_t attempt=0,uint64_t revision=0,
            std::optional<state::disposal> completed={}) {
            auto outgoing=std::make_shared<state::pending>();
            {std::lock_guard lock(runtime.mutex);if(runtime.budget->bytes.load()==0&&runtime.budget->slots.load()==0)outgoing->reservation=runtime.reserve_locked(8388608,false);}
            if(!outgoing->reservation){settle.keep_admission_wait=true;return;}
            outgoing->route=c.route;outgoing->view=c.view;outgoing->request_id=uuid_t::generate().to_string();outgoing->operation=op;outgoing->index=index;outgoing->completed_disposal=std::move(completed);
            {std::lock_guard lock(runtime.mutex);outgoing->terminal_inbox=terminal_profile(c.description)||outgoing->completed_disposal.has_value()||c.route->state_->issued_discard_view.lock()==c.view.value;}
            outgoing->proof_physical=physical;outgoing->proof_phase=phase;outgoing->proof_barrier=barrier;outgoing->proof_attempt=attempt;outgoing->proof_revision=revision;
            json command={{"kind","recoveryReady"},{"version",1},{"operation",op},{"requestID",outgoing->request_id},{"routeGeneration",c.description.at("routeGeneration")}};
            const auto remaining=c.route->state_->source->recovery_remaining(c.view);require(remaining>100,"controller source authorization renewal required");
            fill(command,remaining);
            outgoing->request_bytes=command.dump();require(outgoing->request_bytes.size()<=8388608&&outgoing->request_bytes.size()<=pending_bytes-4194304,"controller outgoing aggregate capacity");
            outgoing->reservation->shrink(outgoing->request_bytes.size());outgoing->deadline=now()+std::min<int64_t>(remaining,30000);
            observe("outgoing-built-before-publication");bool late_pending=false;
            {std::lock_guard lock(runtime.mutex);require(!runtime.outstanding,"controller overlapping request admission");
                if(outgoing->completed_disposal)require(runtime.revision==outgoing->completed_disposal->key.revision,"controller disposal handoff generation changed");
                late_pending=runtime.budget->slots.load()!=0;if(!late_pending)runtime.outstanding=outgoing;}
            if(late_pending){settle.keep_admission_wait=true;observe("late-control-handoff-deferred");return;}
            auto message=transport_message::from_string(outgoing->request_bytes);message.msg_type=transport_message::type::binary;
            require(c.route->state_->source->recovery_send(c.view,*c.route->state_->transport,message),"controller final physical handoff refused");
            if(outgoing->completed_disposal){std::lock_guard lock(runtime.mutex);c.route->state_->issued_discard_view=c.view.value;c.route->state_->late_control_view=c.view.value;}
        };
        // Socket callbacks reserve at most two bounded replies. Only this worker
        // parses it or touches SQL. Claiming the reply keeps its full byte
        // charge until the exact transactional consumer finishes.
        std::shared_ptr<state::pending> pending;
        {std::lock_guard lock(runtime.mutex);pending=runtime.outstanding;}
        for(unsigned reply_quantum=0;pending&&reply_quantum<2;++reply_quantum) {
            auto route=pending->route.lock();
            bool disposal_retired=false;
            {std::lock_guard lock(runtime.mutex);disposal_retired=pending->completed_disposal&&pending->completed_disposal->key.revision!=runtime.revision;}
            if(!route||route->state_->retired.load()||!route->state_->source->recovery_live(pending->view)||disposal_retired||now()>=pending->deadline){
                const bool expired=now()>=pending->deadline;
                {std::lock_guard lock(runtime.mutex);if(runtime.outstanding==pending)runtime.outstanding.reset();}
                settle.keep_admission_wait=true;
                observe(expired?"pending-expired-before-successor":"pending-retired-before-successor");return;
            }
            std::shared_ptr<state::reply> front;
            {std::lock_guard lock(runtime.mutex);
                for(const auto& reply:pending->inbox)if(reply&&(!front||reply->order<front->order))front=reply;
                if(!front||!front->ready)return;}
            // Stable immutable bytes survive parsing, SQL, and a no-effect
            // admission deferral. Do not dequeue before known consumption.
            const auto& response=front->bytes;
            observe(pending->operation=="read"?"range-response-ready":"control-response-ready");
            const auto description=parse(route->state_->source->recovery_description(pending->view),65536);
            const auto channel=description.at("channel").get<std::string>();
            const auto response_value=parse(response,recovery_request_store::frame_bytes);
            bool original_discard_admitted;
            {std::lock_guard lock(runtime.mutex);original_discard_admitted=route->state_->issued_discard_view.lock()==pending->view.value;}
            if((terminal_profile(description)||(original_discard_admitted&&response_value.value("operation",std::string{})=="discard"))&&
                response_value.is_object()&&response_value.contains("kind")&&response_value.at("kind")=="recoveryReady"&&
                response_value.contains("version")&&response_value.at("version")==1&&response_value.contains("operation")&&
                (response_value.at("operation")=="inspect"||response_value.at("operation")=="discard"||response_value.at("operation")=="predecessor")&&
                response_value.contains("requestID")&&response_value.at("requestID").is_string()&&response_value.at("requestID")!=pending->request_id&&
                response_value.contains("routeGeneration")&&response_value.at("routeGeneration")==description.at("routeGeneration")){
                if(response_value.at("operation")=="predecessor")late_predecessor_shape(response_value,description);
                else if(terminal_profile(description))lifecycle_reply_shape(response_value);
                else original_discard_reply_shape(response_value,description);
                // A delayed lifecycle reply cannot settle a different request.
                // Keep its actual outstanding request, deadline and Q unchanged.
                {std::lock_guard lock(runtime.mutex);
                    require(runtime.outstanding==pending,"controller stale reply request retired");
                    for(auto& reply:pending->inbox)if(reply==front)reply.reset();}
                observe("terminal-stale-control-ignored");continue;
            }
            if(pending->completed_disposal) {
                const auto& receipt=*pending->completed_disposal;
                const auto submitted=cr::decode(parse(pending->request_bytes,8388608).at("request").get<std::string>(),runtime.caps.codec);
                require(submitted.logical==receipt.key.logical&&std::get<cr::request>(submitted.body).request_digest==receipt.key.request_digest&&
                    submitted.route_generation==decimal(description,"routeGeneration"),"controller completed disposal submitted Q changed");
                require(pending->operation=="discard"&&response_value.at("requestID")==pending->request_id,
                    "controller completed disposal correlation differs");
                if(terminal_profile(description)) {
                    late_lifecycle_shape(response_value,description);
                    require(response_value.contains("lifecycle"),"controller completed disposal lacks terminal source facts");
                    const auto& body=response_value.at("lifecycle");
                    require(body.at("state")=="terminal"&&decimal(body,"bindingHighWater")==receipt.key.logical.sequence&&
                        decimal(body,"sequence")==receipt.key.logical.sequence&&body.at("requestDigest")==receipt.key.request_digest&&
                        body.at("attemptID")==receipt.key.logical.attempt_id&&body.at("receiverIncarnation")==receipt.key.logical.receiver_incarnation&&
                        body.at("channelIncarnation")==receipt.key.logical.channel_incarnation&&body.at("channel")==receipt.key.logical.channel,
                        "controller completed disposal exact Q differs");
                } else original_discard_reply_shape(response_value,description);
                require(response_value.at("operation")=="discard"&&response_value.at("settlement").at("state")=="committed",
                    "controller completed disposal lacks current known COMMIT");
                owned([&](database&){
                    {std::lock_guard lock(runtime.mutex);require(runtime.outstanding==pending,"controller completed disposal request retired");}
                    const auto current=disposal_snapshot(receipt.key.physical,receipt.key.barrier,receipt.key.attempt,receipt.key.revision);
                    const auto at=current.find(channel);require(at!=current.end()&&same_disposal(at->second,receipt),"controller completed disposal local/source custody changed");
                });
                require(runtime.disposals.count(channel)||runtime.disposals.size()<runtime.policy.contributions.size(),"controller completed disposal receipt capacity");
                runtime.disposals.insert_or_assign(channel,receipt);observe("completed-disposal-consumed");
            } else if(pending->operation=="predecessor") {
                late_predecessor_shape(response_value,description);
                require(response_value.at("requestID")==pending->request_id&&response_value.contains("predecessor"),"controller predecessor correlation or committed facts missing");
                const auto submitted=parse(pending->request_bytes,8388608);const auto q=cr::decode(submitted.at("request").get<std::string>(),runtime.caps.codec);
                const auto& request=std::get<cr::request>(q.body);const auto& body=response_value.at("predecessor");
                predecessor_wire::pair(submitted.at("priorProfile"),description.at("profile"),description.contains("receiptBinding"));
                require(body.at("beforeProfileDigest")==predecessor_wire::profile_digest(submitted.at("priorProfile"))&&
                    body.at("requestDigest")==request.request_digest&&body.at("attemptID")==q.logical.attempt_id&&decimal(body,"sequence")==q.logical.sequence&&
                    body.at("receiverIncarnation")==q.logical.receiver_incarnation&&body.at("channelIncarnation")==q.logical.channel_incarnation&&body.at("channel")==q.logical.channel&&
                    q.route_generation==decimal(description,"routeGeneration"),"controller predecessor exact submitted Q differs");
                std::shared_ptr<recovery_reconciliation_descriptor::predecessor> issued;
                owned([&](database& db){
                    {std::lock_guard lock(runtime.mutex);require(runtime.revision==pending->proof_revision&&runtime.outstanding==pending,"controller predecessor generation changed");}
                    const auto phase=db.query("SELECT incarnation,phase,barrier,attempt FROM main._lattice_producer_continuity WHERE id=1 LIMIT 2");
                    require(phase.size()==1&&integer(phase[0],"incarnation")==pending->proof_physical&&integer(phase[0],"phase")==pending->proof_phase&&
                        integer(phase[0],"barrier")==pending->proof_barrier&&integer(phase[0],"attempt")==pending->proof_attempt&&
                        (pending->proof_phase==2||pending->proof_phase==4),"controller predecessor durable phase changed");
                    recovery_request_store requests(owner);const auto row=requests.read(channel);require(row&&row->domain==common_domain,"controller predecessor retained Q missing");
                    const auto retained=cr::decode(row->request_frame,runtime.caps.codec);
                    require(retained.logical==q.logical&&retained.body==q.body&&predecessor_profile(*row,description).dump()==submitted.at("priorProfile").dump(),"controller predecessor retained Q/context changed");
                    issued=std::make_shared<recovery_reconciliation_descriptor::predecessor>();
                    issued->route=route;issued->view=pending->view;issued->logical=q.logical;issued->request_digest=request.request_digest;
                    issued->journal=row->journal;issued->row_barrier=row->barrier;issued->row_sequence=row->sequence;issued->row_revision=row->journal_revision;issued->domain=row->domain;
                    issued->old_context=picosha2::hash256_hex_string(row->source_context);issued->current_context=picosha2::hash256_hex_string(source_context(description));
                    issued->transition_id=body.at("transitionID");issued->transition_digest=body.at("transitionDigest");issued->source_identity_digest=body.at("sourceIdentityDigest");
                    issued->physical=pending->proof_physical;issued->phase=pending->proof_phase;issued->barrier=pending->proof_barrier;issued->attempt=pending->proof_attempt;issued->controller_revision=pending->proof_revision;
                });
                live();require(issued!=nullptr,"controller predecessor inspected facts unavailable");
                runtime.predecessors.insert_or_assign(channel,std::move(issued));observe("predecessor-consumed");
            } else if(pending->operation=="read") {
                // A failed control response is not a range, absence proof or
                // durable progress. Retain Q/stage and renew the real lease.
                const auto& envelope=response_value;
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
            } else if(pending->operation=="inspect"||pending->operation=="discard") {
                require(terminal_profile(description),"controller lifecycle reply outside negotiated profile");
                const auto& value=response_value;
                lifecycle_reply_shape(value);
                require(value.at("kind")=="recoveryReady"&&value.at("version")==1&&value.at("operation")==pending->operation&&
                    value.at("requestID")==pending->request_id&&value.at("routeGeneration")==description.at("routeGeneration")&&
                    value.at("leaseAvailable").is_boolean()&&value.at("leaseAvailable")==false,"controller lifecycle correlation differs");
                require(value.at("settlement").at("state")=="committed"&&value.contains("lifecycle"),"controller lifecycle lacks known source COMMIT");
                const auto q=cr::decode(parse(pending->request_bytes,8388608).at("request").get<std::string>(),runtime.caps.codec);
                const auto& request=std::get<cr::request>(q.body);const auto& lifecycle=value.at("lifecycle");
                members(lifecycle,{"state","requestDigest","attemptID","sequence","bindingHighWater","namespaceID","replicaID","receiverIncarnation","channelIncarnation","channel"});
                require(lifecycle.at("state").is_string(),"controller lifecycle state type");const auto status=lifecycle.at("state").get<std::string>();
                const auto high=decimal(lifecycle,"bindingHighWater",0);
                require((status=="available"||status=="terminal"||status=="unstarted")&&
                    (status=="unstarted"?high<q.logical.sequence:high==q.logical.sequence)&&
                    lifecycle.at("requestDigest")==request.request_digest&&lifecycle.at("attemptID")==q.logical.attempt_id&&
                    decimal(lifecycle,"sequence")==q.logical.sequence&&lifecycle.at("namespaceID")==description.at("source").at("receiptNamespace")&&
                    lifecycle.at("replicaID")==description.at("peer").at("replicaID")&&lifecycle.at("receiverIncarnation")==q.logical.receiver_incarnation&&
                    lifecycle.at("channelIncarnation")==q.logical.channel_incarnation&&lifecycle.at("channel")==q.logical.channel&&
                    lifecycle.at("receiverIncarnation")==description.at("peer").at("receiverIncarnation")&&
                    lifecycle.at("channelIncarnation")==description.at("peer").at("channelIncarnation")&&q.logical.channel==channel,
                    "controller lifecycle differs from exact authenticated frozen Q");
                require(pending->operation!="discard"||status=="terminal","controller discard did not terminal-fence Q");
                owned([&](database& db){recovery_request_store requests(owner);const auto row=requests.read(channel);
                    require(row&&row->domain==common_domain,"controller lifecycle source context changed");
                    const auto retained=cr::decode(row->request_frame,runtime.caps.codec);
                    require(retained.logical==q.logical&&retained.body==q.body&&row->route==static_cast<int64_t>(q.route_generation),"controller lifecycle durable Q changed");
                    const auto phase=db.query("SELECT incarnation,phase,barrier,attempt FROM main._lattice_producer_continuity WHERE id=1");
                    require(phase.size()==1&&integer(phase[0],"phase")==2&&integer(phase[0],"barrier")==row->barrier&&integer(phase[0],"attempt")==row->sequence,
                        "controller lifecycle frozen attempt changed");
                    const auto at=runtime.predecessors.find(channel);
                    const auto compatibility=at==runtime.predecessors.end()?std::shared_ptr<const recovery_reconciliation_descriptor::predecessor>{}:at->second;
                    uint64_t revision;{std::lock_guard lock(runtime.mutex);revision=runtime.revision;}
                    require(compatible_context(*row,description,compatibility,route,pending->view,revision,integer(phase[0],"incarnation"),2,
                        integer(phase[0],"barrier"),integer(phase[0],"attempt"),runtime.caps.codec),"controller lifecycle source context changed");
                    require(status!="unstarted"||row->manifest_frame.empty(),"controller previously manifested Q became unstarted");
                });
                runtime.inspect_required.erase(channel);runtime.leases.erase(channel);
                runtime.lifecycles.insert_or_assign(channel,state::lifecycle{route,pending->view,q.logical,request.request_digest,status});
                if(status=="terminal"){
                    for(const auto& [_,part]:connected_routes)require(terminal_profile(part.description),"controller terminal rearm needs every negotiated source");
                    runtime.terminal_rearm=true;runtime.leases.clear();
                }else if(status=="unstarted")runtime.leases.emplace(channel,state::lease{route,pending->view,{},{},{},0,0,now()+1000});
            } else {
                const auto& value=response_value;
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
                    runtime.lifecycles.erase(channel);runtime.inspect_required.erase(channel);
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
                        if(terminal_profile(description))runtime.inspect_required.insert(channel);
                        else runtime.leases.emplace(channel,state::lease{route,pending->view,{},{},{},0,0,now()+1000});
                    } else refuse("controller source preparation refused; exact Q retained");
                }
            }
            {std::lock_guard lock(runtime.mutex);if(runtime.outstanding==pending)runtime.outstanding.reset();}
            pending.reset(); // worker/request payloads retire outside the leaf
            settle.keep_admission_wait=false; // actual correlated consumption
            observe("response-consumed");
            observe("pending-consumed-before-successor");
        }
        if(pending){settle.after=[route=pending->route.lock()]{if(route)route->wake();};return;}
        for(unsigned quantum=0;quantum<4;++quantum) {
            int64_t phase=0,barrier=0,attempt=0,physical_incarnation=0;uint64_t demand_revision;
            {std::lock_guard lock(runtime.mutex);demand_revision=runtime.revision;}
            owned([&](database& db){const auto rows=db.query("SELECT CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(phase)='integer' THEN phase END AS phase,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt FROM main._lattice_producer_continuity WHERE id=1 LIMIT 2");require(rows.size()==1,"controller physical phase missing");phase=integer(rows[0],"phase");barrier=integer(rows[0],"barrier");attempt=integer(rows[0],"attempt");physical_incarnation=integer(rows[0],"incarnation");});
            for(auto at=runtime.disposals.begin();at!=runtime.disposals.end();) {
                const auto& key=at->second.key;
                if(key.physical!=physical_incarnation||key.phase!=phase||key.barrier!=barrier||key.attempt!=attempt||key.revision!=demand_revision)at=runtime.disposals.erase(at);
                else ++at;
            }
            if(runtime.terminal_rearm&&phase==1){
                // An unknown/secondary local result can have committed the
                // cancellation. Inspect that exact successor before publishing
                // runtime state; never carry old terminal authority into new Q.
                require(runtime.frozen&&runtime.frozen_barrier<INT64_MAX&&runtime.frozen_attempt<INT64_MAX&&
                    barrier==runtime.frozen_barrier+1&&attempt==runtime.frozen_attempt+1,"controller terminal successor differs from frozen predecessor");
                owned([&](database&){validate_reopen_owned(owner,runtime.policy,phase,barrier,attempt);});
                recovery_continuous_producer::controller_publish_reconcile(*this,owner,barrier,attempt);
                runtime.frozen.reset();runtime.framing_committed=false;runtime.leases.clear();runtime.lifecycles.clear();runtime.inspect_required.clear();runtime.terminal_rearm=false;runtime.predecessors.clear();
                runtime.terminal_successor_attempt=attempt;observe("terminal-cancel-inspected");
            }
            if(phase==0) {
                bool demand;{std::lock_guard lock(runtime.mutex);demand=runtime.demand;}
                if(!demand){runtime.idle=true;for(const auto& route:routes)route->state_->blocked.store(false,std::memory_order_release);return;}
                runtime.idle=false;
                for(const auto& route:routes)route->state_->blocked.store(true,std::memory_order_release);
                int64_t next=0;owned([&](database&){next=recovery_continuous_producer::controller_next_attempt_owned(owner);});
                auto begun=recovery_continuous_producer::begin_impl(owner,next,true);known(begun.settlement);observe("barrier-committed");if(begun.waiting)return;continue;
            }
            for(const auto& route:routes)route->state_->blocked.store(true,std::memory_order_release);
            if(phase==2||phase==4) {
                std::optional<recovery_request_row> needs_predecessor;
                owned([&](database&){
                    recovery_request_store requests(owner);receive_install_store receiver(owner,runtime.caps.install.installations);bool audited=false;
                    for(const auto& contribution:runtime.policy.contributions){const auto row=requests.read(contribution.profile.binding.channel);
                        if(!row){require(phase==2,"controller restricted predecessor framing missing");continue;}
                        const auto& c=connected_routes.at(row->journal.channel);
                        if(row->source_context==source_context(c.description))continue;
                        if(!audited){validate_reopen_owned(owner,runtime.policy,phase,barrier,attempt);audited=true;}
                        // Classify actual installed evidence BEFORE examining an
                        // old context or issuing any remote proof request.
                        if(phase==2&&row->sequence!=attempt&&!row->manifest_frame.empty()){
                            const auto actual=receiver.read(row->journal.channel);const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                            const auto identity=describe_canonical_range(q.logical,std::get<cr::request>(q.body),std::get<cr::manifest>(m.body),runtime.caps.codec,row->route).installation_identity;
                            if(actual&&!actual->active&&actual->last_installed==std::optional<receive_install_identity>{identity})continue;
                        }
                        const auto at=runtime.predecessors.find(row->journal.channel);
                        const auto proof=at==runtime.predecessors.end()?std::shared_ptr<const recovery_reconciliation_descriptor::predecessor>{}:at->second;
                        if(!compatible_context(*row,c.description,proof,c.route,c.view,demand_revision,physical_incarnation,phase,barrier,attempt,runtime.caps.codec)&&!needs_predecessor)
                            needs_predecessor=*row;
                    }
                });
                if(needs_predecessor){const auto& row=*needs_predecessor;const auto& c=connected_routes.at(row.journal.channel);
                    auto q=cr::decode(row.request_frame,runtime.caps.codec);q.route_generation=decimal(c.description,"routeGeneration");
                    const auto prior=predecessor_profile(row,c.description);
                    send_control(c,"predecessor",0,[&](json& command,int64_t){command["request"]=cr::encode(q,runtime.caps.codec);command["priorProfile"]=prior;},
                        physical_incarnation,phase,barrier,attempt,demand_revision);return;
                }
            }
            const auto compatible=[&](const recovery_request_row& row,const connected& c){
                {std::lock_guard lock(runtime.mutex);if(runtime.revision!=demand_revision)return false;}
                const auto at=runtime.predecessors.find(row.journal.channel);
                const auto proof=at==runtime.predecessors.end()?std::shared_ptr<const recovery_reconciliation_descriptor::predecessor>{}:at->second;
                return compatible_context(row,c.description,proof,c.route,c.view,demand_revision,physical_incarnation,phase,barrier,attempt,runtime.caps.codec);
            };
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
                        const auto& actual=connected_routes.at(c.profile.binding.channel);require(row->domain==common_domain&&compatible(*row,actual),"controller restricted restart source changed");
                        auto snapshot=journal.snapshot_for_reconciliation(scope->address);const auto q=cr::decode(row->request_frame,runtime.caps.codec);
                        std::set<std::string> requested;for(const auto& item:std::get<cr::request>(q.body).receipts)requested.insert(canonical_writer_adapter::uuid_key(item.original_id));
                        recovery_reconciliation_descriptor::contribution contribution{*row,std::move(snapshot),{},actual.route,actual.route->state_->source,actual.view};
                        require(compatible(*row,actual),"controller descriptor source compatibility changed");
                        if(row->source_context!=source_context(actual.description))contribution.compatibility=runtime.predecessors.at(row->journal.channel);
                        for(const auto& entry:contribution.journal.entries)if(entry.stage==recovery_obligation_stage::open&&!entry.acknowledged&&requested.count(entry.canonical_original_id))contribution.unknown_originals.push_back(entry.canonical_original_id);
                        descriptor->contributions_.push_back(std::move(contribution));
                    }
                });
                {std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision,"controller restricted restart generation changed");
                    runtime.reconciled_external_revision=runtime.external_revision;
                    descriptor->external_revision_=runtime.external_revision;descriptor->delivery_retry_revision_=runtime.delivery_retry_revision;
                    // Only the fresh phase-2 UNKNOWN decision consumes retry
                    // demand. Reconstruction cannot discard an earlier expiry.
                    runtime.reconciliation=std::move(descriptor);}observe("reconciliation-pending");settle.after=[routes]{for(const auto& route:routes)if(!route->state_->retired.load()&&route->state_->reconcile)route->state_->reconcile();};return;
            }
            if(phase==1) {auto status=recovery_continuous_producer::inspect_impl(owner,true);known(status.settlement);require(status.barrier.has_value(),"controller closed phase lacks actual barrier");
                auto frozen=recovery_continuous_producer::finish_impl(*status.barrier,true);if(frozen.waiting)return;known(frozen.settlement);
                if(runtime.terminal_successor_attempt==attempt)observe("terminal-refreeze-committed");continue;}
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
                runtime.frozen.reset();runtime.framing_committed=false;runtime.predecessors.clear();runtime.idle=true;
                {std::lock_guard lock(runtime.mutex);if(runtime.revision==demand_revision)runtime.demand=false;runtime.failure={};runtime.awaiting_delivery_retry=false;}
                for(const auto& route:routes){route->state_->blocked.store(false,std::memory_order_release);if(route->state_->resumed)route->state_->resumed();}return;
            }
            require(phase==2,"controller unsupported durable phase");
            if(!runtime.frozen||runtime.frozen_attempt!=attempt||runtime.frozen_barrier!=barrier) {
                auto status=recovery_continuous_producer::inspect_impl(owner,true);known(status.settlement);require(status.barrier.has_value(),"controller frozen barrier missing");
                auto frozen=recovery_continuous_producer::finish_impl(*status.barrier,true);if(frozen.waiting)return;known(frozen.settlement);require(frozen.unsent.has_value(),"controller frozen local custody missing");
                auto parked=std::make_shared<verified_unsent_set>(std::move(*frozen.unsent));recovery_continuous_producer::controller_park_proof(*parked);runtime.frozen=std::move(parked);runtime.frozen_attempt=attempt;runtime.frozen_barrier=barrier;runtime.framing_committed=false;
            }
            const auto proof=runtime.frozen;
            bool created=false;
            if(!runtime.framing_committed) {
                std::optional<recovery_request_row> selected_disposal;std::optional<state::disposal> selected_receipt;
                owned([&](database&){
                    const auto candidates=disposal_snapshot(physical_incarnation,barrier,attempt,demand_revision);
                    recovery_request_store requests(owner);
                    for(const auto& [channel,receipt]:candidates) {
                        const auto at=runtime.disposals.find(channel);
                        if((at==runtime.disposals.end()||!same_disposal(at->second,receipt))&&!selected_disposal){
                            selected_disposal=requests.read(channel);selected_receipt=receipt;
                        }
                    }
                });
                if(selected_disposal) {
                    const auto& c=connected_routes.at(selected_disposal->journal.channel);auto q=cr::decode(selected_disposal->request_frame,runtime.caps.codec);
                    q.route_generation=decimal(c.description,"routeGeneration");observe("completed-disposal-cohort-validated");
                    send_control(c,"discard",0,[&](json& command,int64_t){command["request"]=cr::encode(q,runtime.caps.codec);},
                        physical_incarnation,2,barrier,attempt,demand_revision,std::move(selected_receipt));return;
                }
            }
            if(!runtime.framing_committed) {
                auto framing_probe=probe_scope("completed-disposal-framing");
                owned([&](database& db){
                const auto candidates=disposal_snapshot(physical_incarnation,barrier,attempt,demand_revision);
                for(const auto& [channel,receipt]:candidates){const auto at=runtime.disposals.find(channel);
                    require(at!=runtime.disposals.end()&&same_disposal(at->second,receipt),"controller framing replacement lacks whole-cohort disposal");}
                observe("completed-disposal-before-framing-replacement");
                recovery_continuous_producer::verify_for_owned_write(*proof);recovery_request_store requests(owner);
                receive_install_store receiver(owner,runtime.caps.install.installations);
                std::map<std::string,recovery_obligation_record> originals;
                for(const auto& scope:proof->frozen_journals())for(const auto& entry:scope.entries){
                    require(originals.count(entry.canonical_original_id)||originals.size()<8192,"controller complete union exceeds finite request capacity");
                    const auto [at,added]=originals.emplace(entry.canonical_original_id,entry.record);require(added||at->second==entry.record,"controller shared original differs");}
                std::map<std::string,std::string> immutable_originals;
                const auto& first_description=connected_routes.at(proof->frozen_journals().front().scope.address.channel).description;
                if(first_description.contains("receiptBinding"))immutable_originals=recovery_export_adapter::frozen_original_identities(owner,*proof,
                    receipt_json::binding(first_description.at("receiptBinding")),first_description.at("source").at("schemaDigest").get<std::string>());
                for(const auto& scope:proof->frozen_journals()) {
                    const auto& connected=connected_routes.at(scope.scope.address.channel);const auto& d=connected.description;
                    const auto existing=requests.read(scope.scope.address.channel);
                    if(existing&&existing->sequence==attempt){require(existing->journal==scope.scope.address&&existing->journal_revision==scope.scope.revision&&existing->barrier==barrier&&
                        compatible(*existing,connected)&&existing->domain==common_domain,"controller frozen request binding changed");continue;}
                    if(existing){const auto oldq=cr::decode(existing->request_frame,runtime.caps.codec);const auto current=receiver.read(scope.scope.address.channel);
                        bool installed=false;
                        if(current&&!existing->manifest_frame.empty()){const auto oldm=cr::decode(existing->manifest_frame,runtime.caps.codec);
                            const auto old=describe_canonical_range(oldq.logical,std::get<cr::request>(oldq.body),std::get<cr::manifest>(oldm.body),runtime.caps.codec,existing->route);
                            installed=current->last_installed==std::optional<receive_install_identity>{old.installation_identity}&&!current->active;}
                        // Existing manifested UNKNOWN reconciliation may have
                        // advanced claims/revision during its restricted pass.
                        // The new manifestless path has no such authority.
                        const bool canceled=!installed&&current&&compatible(*existing,connected)&&existing->domain==common_domain&&
                            (existing->manifest_frame.empty()?canceled_predecessor(*existing,scope.scope,*current,std::get<cr::request>(oldq.body),2,barrier,attempt):
                            !current->active&&existing->sequence==attempt-1&&existing->barrier==barrier-1&&current->last_sequence==existing->sequence&&
                            (!current->last_installed||current->last_installed->sequence<existing->sequence)&&scope.scope.address.incarnation==existing->journal.incarnation&&
                            existing->journal.generation<=INT64_MAX-2&&scope.scope.address.generation==existing->journal.generation+2&&scope.scope.last_attempt==attempt);
                        require(installed||canceled,"controller prior Q lacks exact installed or canceled receiver evidence");requests.erase(*existing);}
                    const auto prior=receiver.read(scope.scope.address.channel);require(prior&&prior->binding==scope.scope.profile.binding&&!prior->active,"controller Q actual receiver unavailable");
                    cr::attempt logical{d.at("peer").at("receiverIncarnation"),d.at("peer").at("channelIncarnation"),scope.scope.address.channel,static_cast<uint64_t>(attempt),uuid_t::generate().to_string()};
                    cr::request q;q.source=source_binding(scope.scope.profile);q.expected.binding=q.source;q.expected.revision=prior->revision;
                    if(d.contains("receiptBinding")){q.registered_producer=receipt_json::binding(d.at("receiptBinding"));q.receipt_namespace=scope.scope.profile.receipt_namespace;}
                    q.expected.base={prior->frontier.kind==receive_frontier_kind::position?cr::frontier_kind::position:prior->frontier.kind==receive_frontier_kind::beginning_null?cr::frontier_kind::beginning_null:cr::frontier_kind::uninitialized,
                        prior->frontier.position?std::optional<uint64_t>{static_cast<uint64_t>(*prior->frontier.position)}:std::nullopt};q.budget=negotiate(d,runtime.caps.codec);
                    require(originals.size()<=d.at("profile").at("requestEntries").get<size_t>()&&originals.size()<=d.at("profile").at("requestTargets").get<size_t>(),"controller actual source request count capacity");
                    for(const auto& [id,entry]:originals)q.receipts.push_back({id,scope.scope.profile.receipt_namespace,{{entry.table,canonical_writer_adapter::uuid_key(entry.target_id)}},
                        q.registered_producer?std::optional<std::string>{immutable_originals.at(id)}:std::nullopt});
                    q.request_digest=cr::request_sha256(logical,q,runtime.caps.codec);const auto route=decimal(d,"routeGeneration");
                    requests.insert({scope.scope.address,barrier,attempt,scope.scope.revision,static_cast<int64_t>(route),common_domain,source_context(d),cr::encode({logical,route,q},runtime.caps.codec),{}});created=true;
                }
                live();{std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision,"controller framing replacement generation changed");}
                });
            }
            runtime.framing_committed=true;runtime.disposals.clear();
            if(created)observe("completed-disposal-framing-committed");
            if(created&&runtime.terminal_successor_attempt==attempt){observe("terminal-request-committed");runtime.terminal_successor_attempt=0;}
            // Known Q COMMIT precedes every handoff. A restart always attempts
            // exact resume first; a newly committed Q can start preparation.
            bool all_complete=true;std::optional<recovery_request_row> selected;uint64_t index=0;
            owned([&](database&){recovery_request_store requests(owner);canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                for(const auto& c:runtime.policy.contributions){auto row=requests.read(c.profile.binding.channel);require(row.has_value(),"controller contribution Q missing");
                    const auto& part=connected_routes.at(c.profile.binding.channel);const auto& d=part.description;require(compatible(*row,part)&&row->domain==common_domain,"controller retained source context changed");const auto route=decimal(d,"routeGeneration");
                    const auto q=cr::decode(row->request_frame,runtime.caps.codec);
                    if(row->route!=static_cast<int64_t>(route)){
                        if(!row->manifest_frame.empty()){const auto m=cr::decode(row->manifest_frame,runtime.caps.codec);stages.rebind(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route,route);}
                        requests.rebind(*row,route);row->route=route;
                    }
                    if(row->manifest_frame.empty()){all_complete=false;if(!selected){selected=std::move(row);index=0;}continue;}
                    const auto m=cr::decode(row->manifest_frame,runtime.caps.codec);const auto progress=stages.resume(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route);
                    if(!progress.content_verified){all_complete=false;if(!selected){selected=std::move(row);index=1+progress.state.next_content_page+progress.state.next_receipt_page;}}
                }});
            if(runtime.terminal_rearm) {
                // One terminal contribution closes the entire frozen cohort.
                // No more reads/install selection until every exact Q is fenced.
                selected.reset();all_complete=false;index=0;
                const auto terminal_current=[&](const recovery_request_row& row,const connected& part){
                    const auto at=runtime.lifecycles.find(row.journal.channel);
                    if(at==runtime.lifecycles.end())return false;
                    const auto q=cr::decode(row.request_frame,runtime.caps.codec);const auto& retained=at->second;
                    return retained.state=="terminal"&&retained.route.lock()==part.route&&retained.view.value==part.view.value&&
                        retained.attempt==q.logical&&retained.request_digest==std::get<cr::request>(q.body).request_digest;
                };
                owned([&](database&){recovery_request_store requests(owner);
                    for(const auto& c:runtime.policy.contributions){const auto row=requests.read(c.profile.binding.channel);const auto& part=connected_routes.at(c.profile.binding.channel);
                        require(row&&terminal_profile(part.description)&&row->sequence==attempt&&row->barrier==barrier&&
                            compatible(*row,part)&&row->domain==common_domain,"controller terminal cohort framing/source changed");
                        if(!terminal_current(*row,part)&&!selected)selected=*row;
                    }
                });
                if(!selected){
                    require(barrier<INT64_MAX&&attempt<INT64_MAX,"controller terminal restart sequence exhausted");
                    auto scope_probe=probe_scope("terminal-cancel");
                    owned([&](database& db){
                        recovery_continuous_producer::verify_for_owned_write(*proof);
                        require(recovery_continuous_producer::controller_cohort(*this,owner)==recovery_continuous_producer::cohort_admission::available,
                            "controller terminal restart retained cohort changed");
                        {std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision&&!runtime.outstanding&&!runtime.reconciliation,
                            "controller terminal restart generation or work changed");}
                        recovery_request_store requests(owner);const auto framing=requests.fingerprints();
                        recovery_obligation_store journal(owner,runtime.caps.obligations,runtime.caps.install.installations);
                        receive_install_store receiver(owner,runtime.caps.install.installations);
                        canonical_range_staging stages(owner,runtime.caps.install.installations,runtime.caps.codec,runtime.caps.staging);
                        journal.audit();receiver.audit();stages.audit();
                        std::vector<recovery_obligation_snapshot> before;std::vector<receive_install_snapshot> receivers;
                        for(const auto& frozen:proof->frozen_journals()){
                            const auto& channel=frozen.scope.address.channel;const auto row=requests.read(channel);const auto& part=connected_routes.at(channel);
                            require(row&&terminal_current(*row,part)&&compatible(*row,part)&&row->domain==common_domain&&
                                row->sequence==attempt&&row->barrier==barrier&&row->journal==frozen.scope.address&&row->journal_revision==frozen.scope.revision&&
                                row->route==static_cast<int64_t>(decimal(part.description,"routeGeneration")),"controller terminal cancellation lost exact Q/source custody");
                            const auto snapshot=journal.snapshot_for_install(frozen.scope.address,attempt);const auto actual=receiver.read(channel);
                            require(snapshot.scope==frozen.scope&&snapshot.entries==frozen.entries&&actual&&actual->binding==frozen.scope.profile.binding&&
                                (!actual->last_installed||actual->last_installed->sequence<attempt),"controller terminal cancellation changed original inventory or installed proof");
                            require(frozen.scope.address.generation<INT64_MAX&&frozen.scope.revision<INT64_MAX,"controller terminal journal generation exhausted");
                            if(row->manifest_frame.empty()){
                                require(!actual->active&&actual->last_sequence==attempt-1&&
                                    db.query("SELECT 1 FROM main._lattice_range_attempt WHERE channel=? LIMIT 1",{std::vector<uint8_t>(channel.begin(),channel.end())}).empty(),
                                    "controller manifestless cancellation has active or retired receiver/stage");
                            }else{
                                const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                                const auto described=describe_canonical_range(q.logical,std::get<cr::request>(q.body),std::get<cr::manifest>(m.body),runtime.caps.codec,row->route);
                                require(actual->last_sequence==attempt&&actual->active==std::optional<receive_install_identity>{described.installation_identity},
                                    "controller terminal cancellation lacks exact uninstalled active identity");
                            }
                            before.push_back(snapshot);receivers.push_back(*actual);
                        }
                        // Validate every member before mutating any member.
                        for(const auto& frozen:before){const auto row=requests.read(frozen.scope.address.channel);
                            if(!row->manifest_frame.empty()){const auto q=cr::decode(row->request_frame,runtime.caps.codec),m=cr::decode(row->manifest_frame,runtime.caps.codec);
                                stages.abandon_active(q.logical,std::get<cr::manifest>(m.body).manifest_digest,row->route);}
                            journal.cancel_frozen_for_retry(frozen.scope.address,attempt,frozen.scope.revision);
                        }
                        require(requests.fingerprints()==framing,"controller terminal cancellation altered retained Q/M");
                        for(size_t n=0;n<before.size();++n){auto expected=before[n];auto expected_receiver=receivers[n];
                            expected.scope.mode=recovery_obligation_mode::recording;++expected.scope.address.generation;++expected.scope.revision;
                            expected_receiver.active.reset();expected_receiver.last_sequence=attempt;
                            const auto actual=journal.snapshot_for_reconciliation(expected.scope.address);
                            require(actual.scope==expected.scope&&actual.entries==expected.entries&&
                                receiver.read(expected.scope.address.channel)==std::optional<receive_install_snapshot>{expected_receiver},
                                "controller terminal cancellation original/ACK/claim or receiver postimage differs");
                        }
                        journal.audit();receiver.audit();stages.audit();live();
                        {std::lock_guard lock(runtime.mutex);require(runtime.revision==demand_revision,"controller terminal cancellation demand changed");}
                        recovery_continuous_producer::controller_restart_terminal_owned(*this,owner,physical_incarnation,barrier,attempt);
                    });
                    scope_probe.reset();observe("terminal-cancel-committed");
                    recovery_continuous_producer::controller_publish_reconcile(*this,owner,barrier+1,attempt+1);
                    runtime.terminal_successor_attempt=attempt+1;
                    runtime.frozen.reset();runtime.framing_committed=false;runtime.leases.clear();runtime.lifecycles.clear();runtime.inspect_required.clear();runtime.terminal_rearm=false;runtime.predecessors.clear();
                    observe("terminal-cancel-published");continue;
                }
            }
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
                        require(compatible(*row,actual),"controller descriptor source compatibility changed");
                        if(row->source_context!=source_context(actual.description))contribution.compatibility=runtime.predecessors.at(row->journal.channel);
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
                    if(runtime.reconciled_external_revision==runtime.external_revision&&runtime.reconciled_delivery_retry_revision==runtime.delivery_retry_revision)
                        throw delivery_retry_wait();
                    runtime.reconciled_external_revision=runtime.external_revision;runtime.reconciled_delivery_retry_revision=runtime.delivery_retry_revision;
                    descriptor->external_revision_=runtime.external_revision;descriptor->delivery_retry_revision_=runtime.delivery_retry_revision;
                    runtime.awaiting_delivery_retry=false;runtime.reconciliation=std::move(descriptor);}observe("reconciliation-pending");settle.after=[routes]{for(const auto& route:routes)if(!route->state_->retired.load()&&route->state_->reconcile)route->state_->reconcile();};return;}
                auto scope_probe=probe_scope("install");
                owned([&](database& db){
                    recovery_continuous_producer::verify_for_owned_write(*proof);recovery_request_store requests(owner);
                    canonical_cohort_admission cohort;cohort.owner_=owner;cohort.local_=proof;cohort.verify_current_sources_=[&]{live();
                        for(const auto& contribution:runtime.policy.contributions){const auto row=requests.read(contribution.profile.binding.channel);const auto& c=connected_routes.at(contribution.profile.binding.channel);
                            require(row&&compatible(*row,c)&&row->domain==common_domain,"controller final install source compatibility changed");}
                    };
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
            const auto lifecycle=runtime.lifecycles.find(selected->journal.channel);
            const bool discard=runtime.terminal_rearm||(lifecycle!=runtime.lifecycles.end()&&lifecycle->second.state=="available"&&
                lifecycle->second.view.value==c.view.value&&lifecycle->second.attempt==q.logical);
            const std::string op=discard?"discard":runtime.inspect_required.count(selected->journal.channel)?"inspect":
                usable?"read":created?"prepare":lease!=runtime.leases.end()&&lease->second.id.empty()&&selected->manifest_frame.empty()?"prepare":"resume";
            send_control(c,op,index,[&](json& command,int64_t remaining){
                if(op=="read"){require(index<lease->second.frames,"controller frame index exceeds actual lease inventory");command["leaseID"]=lease->second.id;command["requestDigest"]=lease->second.request_digest;
                    command["attemptID"]=lease->second.attempt_id;command["sequence"]=std::to_string(lease->second.sequence);command["index"]=std::to_string(index);}
                else {command["request"]=cr::encode(q,runtime.caps.codec);
                    if(op=="prepare"||op=="resume")command["durationMilliseconds"]=std::min<int64_t>({remaining-100,3600000,c.description.at("profile").at("leaseMilliseconds").get<int64_t>()});}
            });
            return;
        }
    }catch(...){
        auto error=std::current_exception();std::shared_ptr<state::pending> released;
        bool admission_busy=false;
        recovery_install_deferred admission_reason=recovery_install_deferred::none;
        try{std::rethrow_exception(error);}catch(const controller_admission_wait& wait){admission_busy=true;admission_reason=wait.reason;}catch(...){}
        if(admission_busy) {
            bool deferred=false;
            {std::lock_guard lock(runtime.mutex);auto& wait=runtime.admission_wait;
                // An external generation is observed by the next ordinary
                // turn. This stale turn cannot spend or reset its allowance.
                if(runtime.revision!=admission_revision)deferred=true;
                else {
                    if(!wait.attempts){wait.revision=admission_revision;wait.deadline=admission_deadline;}
                    wait.deadline=std::min(wait.deadline,admission_deadline);
                    if(now()<wait.deadline&&wait.attempts<admission_retry_attempts){++wait.attempts;wait.next_due=now()+admission_retry_tick_ms;deferred=true;}
                }
                if(deferred)settle.keep_admission_wait=true;
            }
            if(deferred){
                try{if(auto owner=observed_owner.lock();owner&&runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->observed)
                    runtime.probe->observed("admission-deferred");}catch(...){}
                // The owned admission has already unwound and the original
                // observation retains its order and behavior. No SQL or lock
                // re-probe is used to explain this actual deferred result.
                try{if(auto owner=observed_owner.lock();owner&&runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->admission_deferred)
                    runtime.probe->admission_deferred(admission_reason);}catch(...){}
                return;
            }
            error=std::make_exception_ptr(db_error("controller admission retry budget exhausted; gate remains closed"));
        }
        std::array<std::shared_ptr<state::reply>,2> retired_late;
        bool awaiting_delivery=false,retry_arrived=false;
        try{std::rethrow_exception(error);}catch(const delivery_retry_wait&){awaiting_delivery=true;}catch(...){}
        // Observation only: the typed UNKNOWN wait has left its SQL and leaf
        // scopes. A real timer can now race the catch's counter recheck.
        if(awaiting_delivery)try{if(auto owner=observed_owner.lock();owner&&runtime.probe&&
            runtime.probe->owner==owner.get()&&runtime.probe->observed)
                runtime.probe->observed("delivery-retry-wait-before-latch");}catch(...){}
        {std::lock_guard lock(runtime.mutex);
            // The actual timer may arrive after the UNKNOWN decision unlocks
            // but before this catch stores its wait. Preserve that admitted
            // demand instead of latching over it until an unrelated event.
            retry_arrived=awaiting_delivery&&(runtime.reconciled_external_revision!=runtime.external_revision||
                runtime.reconciled_delivery_retry_revision!=runtime.delivery_retry_revision);
            if(retry_arrived){runtime.demand=true;runtime.awaiting_delivery_retry=false;}
            else {runtime.failure=error;runtime.awaiting_delivery_retry=awaiting_delivery;}
            released=std::move(runtime.outstanding);
            retired_late=std::move(runtime.late_inbox);
        }
        released.reset(); // source/endpoint ownership is released off the leaf
        retired_late={};
        std::vector<std::shared_ptr<recovery_receiver_route>> report;
        {std::lock_guard lock(runtime.mutex);for(const auto& weak:runtime.routes)if(auto route=weak.lock())report.push_back(std::move(route));}
        if(retry_arrived){settle.after=[report]{for(const auto& route:report)if(!route->state_->retired.load()){route->wake();break;}};return;}
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
        !route->state_->retired.load()&&c.source==route->state_->source&&c.source->recovery_live(c.view)&&c.source->recovery_lifecycle(c.view)==physical){
        require(!matched,"restricted export ambiguous actual route");
        if(!compatible_context(c.framing,parse(c.source->recovery_description(c.view),65536),c.compatibility,route,c.view,
            descriptor->controller_revision_,descriptor->physical_incarnation_,descriptor->phase_,descriptor->barrier_,descriptor->attempt_,descriptor->limits_.codec))return false;
        matched=true;}
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
        !route->state_->retired.load()&&c.source==route->state_->source&&c.source->recovery_live(c.view)&&c.source->recovery_lifecycle(c.view)==physical){
        require(!matched,"restricted export ambiguous actual route");
        require(compatible_context(c.framing,parse(c.source->recovery_description(c.view),65536),c.compatibility,route,c.view,
            descriptor->controller_revision_,descriptor->physical_incarnation_,descriptor->phase_,descriptor->barrier_,descriptor->attempt_,descriptor->limits_.codec),"restricted export source compatibility changed");matched=&c;}
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
    const auto continuity=cancel?std::shared_ptr<recovery_unknown_reconciliation>{}:recovery_unknown_reconciliation::acquire(descriptor);
    require(cancel||continuity,"controller reconciliation first-claim worker unavailable");
    const auto first_claims=continuity?continuity->refreeze_claims():std::shared_ptr<const std::vector<int64_t>>{};
    require(descriptor->phase_==(cancel?2:4),"controller reconciliation step/phase differs");
    require(!cancel||(descriptor->barrier_<INT64_MAX&&descriptor->attempt_<INT64_MAX),"controller reconciliation sequence exhausted");
    result.next_barrier_=descriptor->barrier_+(cancel?1:0);result.next_attempt_=descriptor->attempt_+(cancel?1:0);
    std::shared_ptr<void> scope_probe;
    if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->scope)
        scope_probe=runtime.probe->scope(cancel?"reconcile-cancel":"reconcile-refreeze");
    result.settlement_=recovery_continuous_producer::controller_owned(*controller,owner,[&](database& db){
        const auto current_sources=[&]{
            {std::lock_guard lock(runtime.mutex);require(runtime.reconciliation==descriptor&&runtime.revision==descriptor->controller_revision_,"controller reconciliation compatibility generation changed");}
            for(const auto& c:descriptor->contributions_){const auto route=c.route.lock();
                require(route&&!route->state_->retired.load()&&c.source==route->state_->source&&c.source&&c.source->recovery_live(c.view),"controller reconciliation source retired");
                require(compatible_context(c.framing,parse(c.source->recovery_description(c.view),65536),c.compatibility,route,c.view,
                    descriptor->controller_revision_,descriptor->physical_incarnation_,descriptor->phase_,descriptor->barrier_,descriptor->attempt_,descriptor->limits_.codec),"controller reconciliation source compatibility changed");}
        };
        current_sources();
        const auto rows=db.query("SELECT CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(phase)='integer' THEN phase END AS phase,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt FROM main._lattice_producer_continuity WHERE id=1 LIMIT 2");
        require(rows.size()==1&&integer(rows[0],"incarnation")==descriptor->physical_incarnation_&&integer(rows[0],"phase")==descriptor->phase_&&
            integer(rows[0],"barrier")==descriptor->barrier_&&integer(rows[0],"attempt")==descriptor->attempt_,"controller reconciliation physical generation differs");
        if(cancel){require(descriptor->frozen_!=nullptr,"controller frozen reconciliation lacks actual provenance");recovery_continuous_producer::verify_for_owned_write(*descriptor->frozen_);}
        recovery_request_store requests(owner);const auto framing=requests.fingerprints();
        recovery_obligation_store journal(owner,runtime.caps.obligations,runtime.caps.install.installations);
        receive_install_store receiver(owner,runtime.caps.install.installations);receiver.audit();journal.audit();
        std::vector<recovery_obligation_snapshot> journals;std::vector<receive_install_snapshot> receivers;
        for(size_t contribution=0;contribution<descriptor->contributions_.size();++contribution) {
            const auto& c=descriptor->contributions_[contribution];
            require(requests.read(c.framing.journal.channel)==std::optional<recovery_request_row>{c.framing},"controller reconciliation frozen Q/M changed");
            const auto scope=journal.read(c.framing.journal.channel);const auto installed=receiver.read(c.framing.journal.channel);
            require(scope&&installed&&scope->profile==c.journal.scope.profile&&scope->address==c.journal.scope.address,"controller reconciliation journal address changed");
            auto snapshot=cancel?journal.snapshot_for_install(scope->address,descriptor->attempt_):journal.snapshot_for_reconciliation(scope->address);
            require(snapshot.entries.size()==c.journal.entries.size(),"controller reconciliation pending inventory changed");
            for(size_t n=0;n<snapshot.entries.size();++n){auto current=snapshot.entries[n],original=c.journal.entries[n];
                if(!cancel){const auto first=first_claims->at(continuity->claim_offsets_[contribution]+n);
                    original.first_export_claim=first?std::optional<int64_t>{first}:std::nullopt;}
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
    {std::lock_guard lock(runtime.mutex);retired=std::move(runtime.reconciliation);++runtime.revision;runtime.failure={};runtime.awaiting_delivery_retry=false;runtime.demand=true;}
    runtime.frozen.reset();runtime.framing_committed=false;runtime.predecessors.clear();
    reservation->release(); // payload/owner release is outside the leaf
    if(runtime.probe&&runtime.probe->owner==owner.get()&&runtime.probe->observed)
        runtime.probe->observed(result.step_==recovery_reconciliation_step::cancelled?"reconcile-cancel-published":"reconcile-refreeze-published");
    std::shared_ptr<recovery_receiver_route> next;
    {std::lock_guard lock(runtime.mutex);for(const auto& weak:runtime.routes)if(auto route=weak.lock())if(!route->state_->retired.load()){next=std::move(route);break;}}
    if(next)controller->wake(next);
}
} // namespace lattice::detail
