#include "TestHelpers.hpp"
#include "FixtureCheckedOwnerEnding.hpp"
#include "StartupPredicateObservation.hpp"
#include "ControllerFixtureWorkerEnding.hpp"
#include "../../Sources/LatticeCore/src/recovery_cohort_progress_observation.hpp"
#include "../../Sources/LatticeCore/src/recovery_topology_wait_observation.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include "../../Sources/LatticeCore/src/sync_discovery_deferral.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include "../../Sources/LatticeCore/src/sync_upload_exclusion.hpp"
#include <future>
#include <array>
#include <set>
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_receiver_controller.hpp"
#include "../../Sources/LatticeCore/src/recovery_authenticated_session.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include <nlohmann/json.hpp>
#include <deque>
#include <chrono>
#include <cstdio>
#include <condition_variable>
#include <cstring>
#include <sqlite3.h>

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace lattice::detail {
struct recovery_receiver_controller_test_access {
    using probe=recovery_receiver_controller::test_probe;
    std::shared_ptr<const probe> prior;
    recovery_receiver_controller_test_access(const lattice_db* owner,std::function<void(const char*)> observed,
        std::function<std::shared_ptr<void>(const char*)> scope={},std::function<void(const char*)> admission_observed={},
        std::function<void()> phase_committed={},std::function<void()> reconciliation_prepublication={},
        std::function<void()> response_capacity_wait={}) {
        auto value=std::make_shared<probe>();value->owner=owner;value->observed=std::move(observed);value->scope=std::move(scope);value->admission_observed=std::move(admission_observed);value->phase_committed=std::move(phase_committed);value->reconciliation_prepublication=std::move(reconciliation_prepublication);value->response_capacity_wait=std::move(response_capacity_wait);
        std::lock_guard lock(recovery_receiver_controller::test_mutex_);prior=std::move(recovery_receiver_controller::test_probe_);recovery_receiver_controller::test_probe_=std::move(value);
    }
    ~recovery_receiver_controller_test_access(){std::shared_ptr<const probe> old;{std::lock_guard lock(recovery_receiver_controller::test_mutex_);old=std::move(recovery_receiver_controller::test_probe_);recovery_receiver_controller::test_probe_=std::move(prior);}}
};
}
namespace {
using namespace lattice;
struct ControllerPause {
    std::mutex mutex;std::condition_variable changed;bool arrived=false,released=false,timed_out=false;
    void wait(){std::unique_lock lock(mutex);arrived=true;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
    bool ready(){std::lock_guard lock(mutex);return arrived;}
    bool timedOut(){std::lock_guard lock(mutex);return timed_out;}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
};
struct ControllerCommitFault {
    static thread_local ControllerCommitFault* active;
    std::atomic<unsigned>& hits;
    detail::recovery_local_producer_test_hooks::authorizer_fault fault;
    const detail::recovery_local_producer_test_hooks::authorizer_fault* prior;
    ControllerCommitFault* prior_active;
    ControllerCommitFault(const lattice_db* owner,std::atomic<unsigned>& count):hits(count),fault{owner,restrict_action},prior(detail::recovery_local_producer_test_hooks::fault),prior_active(active){active=this;detail::recovery_local_producer_test_hooks::fault=&fault;}
    ~ControllerCommitFault(){detail::recovery_local_producer_test_hooks::fault=prior;active=prior_active;}
    static int restrict_action(int action,const char* one,const char*,const char*)noexcept{
        if(active&&action==SQLITE_TRANSACTION&&one&&std::strcmp(one,"COMMIT")==0){++active->hits;return SQLITE_DENY;}return SQLITE_OK;
    }
};
thread_local ControllerCommitFault* ControllerCommitFault::active=nullptr;
using json=nlohmann::json;
// One immutable ACK/one-shot claim selection, published off producer leaves.
// Read on actual callback entry AND actual known after-claim COMMIT. Selection
// exists before the relevant action becomes possible, never by observer arrival.
// No selected payload is destroyed while the DATA mutex is held.
class ControllerRouteHookCell {
public:
    struct Selection {
        std::shared_ptr<const detail::sync_background_test_hooks::ack_schedule> ack;
        std::shared_ptr<const std::function<void()>> after_claim;
    };
private:
    const uint64_t creator_=detail::database_native_process();
    std::mutex mutex_;std::shared_ptr<const Selection> selected_;
public:
    void publish(std::shared_ptr<const detail::sync_background_test_hooks::ack_schedule> ack,
                 std::function<void()> after_claim={}) {
        if(creator_!=detail::database_native_process())throw db_error("inherited route hook publication");
        auto next=std::make_shared<Selection>();next->ack=std::move(ack);
        if(after_claim)next->after_claim=std::make_shared<const std::function<void()>>(std::move(after_claim));
        std::shared_ptr<const Selection> retired;
        {std::lock_guard lock(mutex_);retired=std::move(selected_);selected_=std::move(next);}
    }
    std::shared_ptr<const Selection> read(){
        if(creator_!=detail::database_native_process())throw db_error("inherited route hook read");
        std::lock_guard lock(mutex_);return selected_;
    }
};
class ControllerRouteCallbackHookScope {
    const uint64_t creator_=detail::database_native_process();
    struct Fields {
        const uint64_t creator=detail::database_native_process();
        std::shared_ptr<ControllerRouteHookCell> cell;
        std::shared_ptr<const ControllerRouteHookCell::Selection> selected;
        std::shared_ptr<const detail::sync_background_test_hooks::ack_schedule> previous_ack;
        void (*previous_claim)()=nullptr;
        void (*chained_claim)()=nullptr;
        Fields* previous_scope=nullptr;
    };
    static thread_local Fields* current_;
    // All selected payload stays heap-owned across the actual callback. A
    // child abandons this capsule before inherited TLS/control-block cleanup.
    Fields* original_=nullptr;
    static void claimed() {
        auto* const actual=current_;
        if(!actual||actual->creator!=detail::database_native_process())return;
        // A job may have entered before phase4 publication. Refresh at the
        // real known after-COMMIT hook, before actual ACK launcher capture.
        actual->selected=actual->cell->read();
        if(actual->selected){
            detail::sync_background_test_hooks::ack=actual->selected->ack;
            if(actual->selected->after_claim)(*actual->selected->after_claim)();
        }
        if(actual->creator!=detail::database_native_process())return;
        // Preserve the real outer first-claim hook. Nested route scopes use
        // the innermost actual policy and chain the non-route hook once.
        if(actual->chained_claim)actual->chained_claim();
    }
public:
    explicit ControllerRouteCallbackHookScope(const std::shared_ptr<ControllerRouteHookCell>& cell) {
        if(!cell)return;
        auto staged=std::make_unique<Fields>();
        staged->selected=cell->read();staged->cell=cell;
        staged->previous_claim=detail::recovery_export_test_hooks::after_claim_commit;
        staged->previous_scope=current_;
        staged->chained_claim=staged->previous_claim;
        if(staged->previous_claim==claimed){
            if(!current_||current_->creator!=creator_)throw db_error("route callback hook scope mismatch");
            staged->chained_claim=current_->chained_claim;
        }
        // Required custody exists before any TLS publication. Even an empty
        // cell gets the dispatcher: publication arrival never grants eligibility.
        staged->previous_ack=std::move(detail::sync_background_test_hooks::ack);
        if(staged->selected)detail::sync_background_test_hooks::ack=staged->selected->ack;
        else detail::sync_background_test_hooks::ack=staged->previous_ack;
        original_=staged.release();current_=original_;
        detail::recovery_export_test_hooks::after_claim_commit=claimed;
    }
    ~ControllerRouteCallbackHookScope()noexcept {
        if(creator_!=detail::database_native_process()){original_=nullptr;return;}
        if(!original_)return;
        detail::recovery_export_test_hooks::after_claim_commit=original_->previous_claim;
        current_=original_->previous_scope;
        detail::sync_background_test_hooks::ack=std::move(original_->previous_ack);
        delete original_;original_=nullptr; // Off callback SQL/selection leaves.
    }
    ControllerRouteCallbackHookScope(const ControllerRouteCallbackHookScope&)=delete;
    ControllerRouteCallbackHookScope& operator=(const ControllerRouteCallbackHookScope&)=delete;
};
thread_local ControllerRouteCallbackHookScope::Fields* ControllerRouteCallbackHookScope::current_=nullptr;
// Same actual std_thread_scheduler worker, FIFO, thread identity and shutdown.
// Only the callback body gets a scoped hook selection; no extra job/worker.
class ControllerRouteThreadScheduler final : public std_thread_scheduler {
    const std::shared_ptr<ControllerRouteHookCell> hooks_;
public:
    explicit ControllerRouteThreadScheduler(std::shared_ptr<ControllerRouteHookCell> hooks):hooks_(std::move(hooks)){}
    void invoke(std::function<void()>&& job)override {
        const auto hooks=hooks_;
        std_thread_scheduler::invoke([hooks,job=std::move(job)]()mutable{
            ControllerRouteCallbackHookScope scope(hooks);job();
        });
    }
};
// Pump-thread observations only. No live endpoint, source result, owner, or
// callback is retained. Diagnostic overflow never changes fixture admission.
struct ControllerBoundaryTrace {
    enum class Event { open_receiver, close_receiver, dial, retired_frame, ready, discard, wait_begin, wait_end };
    enum class Flag { unobserved, no, yes };
    enum class Operation { unknown, describe, prepare, resume, read, inspect, discard, predecessor };
    struct Copy { size_t offset=0,bytes=0;bool observed=false,stored=false; };
    struct Record {
        uint64_t ordinal=0,parent=0,connection=0,wait=0;size_t peer=0,request_bytes=0,used=0;
        Event event=Event::ready;Operation operation=Operation::unknown;
        Flag publishable=Flag::unobserved,opened=Flag::unobserved,delivered=Flag::unobserved,completed=Flag::unobserved;
        bool status_observed=false,before_suppressed=false,prepare_dropped=false,terminal_empty=false;
        bool capture_failure_observed=false;
        detail::authenticated_ready_control_test_observation::capture_failure capture_failure_copy;
        int32_t status=0;
        Copy request_id,route,request_index,source,after;
        std::array<char,8192> bytes{};
    };
    std::array<Record,64> records{};
    size_t retained=0;uint64_t total=0,overflow=0,connection=0,wait=0,next_wait=0;
    Record* current=nullptr;
    static void increment(uint64_t& value)noexcept{if(value!=UINT64_MAX)++value;}
    static Flag flag(bool value)noexcept{return value?Flag::yes:Flag::no;}
    Record* add(Event event,size_t peer=0)noexcept{
        increment(total);if(retained==records.size()){increment(overflow);return nullptr;}
        auto& out=records[retained++];out.ordinal=total;out.parent=current?current->ordinal:0;
        out.event=event;out.peer=peer;out.connection=connection;out.wait=wait;return &out;
    }
    struct Scope {
        ControllerBoundaryTrace& trace;Record* prior;
        Scope(ControllerBoundaryTrace& value,Record* record)noexcept:trace(value),prior(value.current){trace.current=record;}
        ~Scope(){trace.current=prior;}
    };
    struct WaitScope {
        ControllerBoundaryTrace& trace;uint64_t prior;
        explicit WaitScope(ControllerBoundaryTrace& value)noexcept:trace(value),prior(value.wait){
            increment(trace.next_wait);trace.wait=trace.next_wait;trace.add(Event::wait_begin);
        }
        ~WaitScope(){trace.wait=prior;}
        void finish(bool result)noexcept{if(auto* out=trace.add(Event::wait_end))out->completed=flag(result);}
    };
    static void copy(Record* out,Copy Record::* member,const std::string& value)noexcept{
        if(!out)return;auto& part=out->*member;part.observed=true;part.bytes=value.size();
        if(value.size()>out->bytes.size()-out->used)return;
        part.offset=out->used;part.stored=true;
        if(!value.empty())std::memcpy(out->bytes.data()+out->used,value.data(),value.size());out->used+=value.size();
    }
    static Operation operation(const json& value)noexcept{
        try{if(!value.is_string())return Operation::unknown;const auto& text=value.get_ref<const std::string&>();
            if(text=="describe")return Operation::describe;if(text=="prepare")return Operation::prepare;
            if(text=="resume")return Operation::resume;if(text=="read")return Operation::read;
            if(text=="inspect")return Operation::inspect;if(text=="discard")return Operation::discard;
            if(text=="predecessor")return Operation::predecessor;
        }catch(...){}return Operation::unknown;
    }
    static void request(Record* out,const json& control,size_t bytes)noexcept{
        if(!out)return;out->request_bytes=bytes;
        try{auto op=control.find("operation");if(op!=control.end())out->operation=operation(*op);
            auto id=control.find("requestID");if(id!=control.end()&&id->is_string())copy(out,&Record::request_id,id->get_ref<const std::string&>());
            auto route=control.find("routeGeneration");if(route!=control.end()&&route->is_string())copy(out,&Record::route,route->get_ref<const std::string&>());
            auto index=control.find("index");if(index!=control.end()&&index->is_string())copy(out,&Record::request_index,index->get_ref<const std::string&>());
        }catch(...){}
    }
    static void result(Record* out,int32_t status,bool publishable)noexcept{
        if(!out)return;out->status_observed=true;out->status=status;
        if(status==1)out->publishable=flag(publishable);
    }
    void delivery_bytes(const std::string& raw)noexcept{
        if(!current)return;
        // Identical source/delivery bytes share storage; a changed hook output
        // uses only the remaining part of the same aggregate 8 KiB allowance.
        const auto& source=current->source;
        if(source.stored&&source.bytes==raw.size()&&
            (raw.empty()||std::memcmp(current->bytes.data()+source.offset,raw.data(),raw.size())==0))current->after=source;
        else copy(current,&Record::after,raw);
    }
    static const char* label(Flag value)noexcept{return value==Flag::yes?"yes":value==Flag::no?"no":"unobserved";}
    static const char* label(Operation value)noexcept{
        switch(value){case Operation::describe:return "describe";case Operation::prepare:return "prepare";
            case Operation::resume:return "resume";case Operation::read:return "read";case Operation::inspect:return "inspect";
            case Operation::discard:return "discard";case Operation::predecessor:return "predecessor";default:return "unknown";}
    }
    static const char* label(Event value)noexcept{
        switch(value){case Event::open_receiver:return "openReceiver";case Event::close_receiver:return "closeReceiver";
            case Event::dial:return "dial";case Event::retired_frame:return "retiredFrame";case Event::ready:return "ready";
            case Event::discard:return "discard";case Event::wait_begin:return "waitBegin";case Event::wait_end:return "waitEnd";}
        return "unknown";
    }
    // Only used after the original predicate failed. Reject excessive depth,
    // events and duplicate keys before a bounded DOM parse; never parse a prefix.
    struct MetadataGuard final:nlohmann::json_sax<json> {
        size_t events=0,depth=0;std::array<std::set<std::string>,8> keys;std::array<bool,8> object{};
        bool tick()noexcept{return ++events<=256;}
        bool null()override{return tick();}bool boolean(bool)override{return tick();}
        bool number_integer(number_integer_t)override{return tick();}bool number_unsigned(number_unsigned_t)override{return tick();}
        bool number_float(number_float_t,const string_t&)override{return tick();}
        bool string(string_t&)override{return tick();}bool binary(binary_t&)override{return false;}
        bool start(bool is_object){if(!tick()||depth==keys.size())return false;keys[depth].clear();object[depth]=is_object;++depth;return true;}
        bool start_object(std::size_t)override{return start(true);}bool start_array(std::size_t)override{return start(false);}
        bool key(string_t& value)override{return tick()&&depth&&object[depth-1]&&keys[depth-1].insert(value).second;}
        bool end_object()override{if(!tick()||!depth)return false;--depth;return true;}
        bool end_array()override{if(!tick()||!depth)return false;--depth;return true;}
        bool parse_error(std::size_t,const std::string&,const nlohmann::detail::exception&)override{return false;}
    };
    static const char* state_label(const json& value)noexcept{
        try{if(!value.is_string())return "unknown";const auto& text=value.get_ref<const std::string&>();
            for(const auto* allowed:{"committed","refused","rolledBack","unsettled","ownershipLost","available","terminal","unstarted"})
                if(text==allowed)return allowed;
        }catch(...){}return "unknown";
    }
    static std::optional<uint64_t> quantity(const json& value)noexcept{
        try{if(value.is_number_unsigned())return value.get<uint64_t>();
            if(value.is_number_integer()){const auto n=value.get<int64_t>();if(n>=0)return static_cast<uint64_t>(n);return {};}
            if(!value.is_string())return {};const auto& text=value.get_ref<const std::string&>();if(text.empty()||text.size()>20)return {};
            uint64_t n=0;for(const auto c:text){if(c<'0'||c>'9'||n>(UINT64_MAX-static_cast<unsigned>(c-'0'))/10)return {};n=n*10+static_cast<unsigned>(c-'0');}return n;
        }catch(...){return {};}
    }
    static Flag equal(const Record& record,const Copy& part,const json& value)noexcept{
        try{if(!part.stored||!value.is_string())return Flag::unobserved;const auto& text=value.get_ref<const std::string&>();
            return flag(text.size()==part.bytes&&(!part.bytes||std::memcmp(text.data(),record.bytes.data()+part.offset,part.bytes)==0));
        }catch(...){return Flag::unobserved;}
    }
    static json metadata(const Record& record,const Copy& part){
        json out={{"observed",part.observed},{"bytes",part.bytes},{"stored",part.stored}};
        if(!part.stored)return out;const auto* begin=record.bytes.data()+part.offset;const auto* end=begin+part.bytes;
        try{MetadataGuard guard;if(!json::sax_parse(begin,end,&guard)){out["parse"]="unknown";return out;}
            const auto value=json::parse(begin,end);if(!value.is_object()){out["parse"]="nonObject";return out;}out["parse"]="boundedObject";
            auto field=value.find("kind");out["readyControl"]=field!=value.end()&&field->is_string()&&field->get_ref<const std::string&>()=="recoveryReady";
            field=value.find("operation");if(field!=value.end())out["operation"]=label(operation(*field));
            field=value.find("requestID");out["requestIDMatches"]=field==value.end()?"unobserved":label(equal(record,record.request_id,*field));
            field=value.find("routeGeneration");out["routeMatches"]=field==value.end()?"unobserved":label(equal(record,record.route,*field));
            for(const auto* key:{"leaseAvailable","frameAvailable","captureError","requiresFullRequest"}){
                field=value.find(key);if(field!=value.end()&&field->is_boolean())out[key]=field->get<bool>();}
            for(const auto* key:{"settlement","expiration","preparation","publication","lifecycle"}){
                field=value.find(key);if(field!=value.end()&&field->is_object()){
                    const auto state=field->find("state");out[key]=state==field->end()?"unknown":state_label(*state);}}
            for(const auto* key:{"frames","wireBytes","index","sequence","durationMilliseconds"}){
                field=value.find(key);if(field!=value.end())if(const auto n=quantity(*field))out[key]=*n;}
        }catch(...){out["parse"]="unknown";}return out;
    }
    static std::array<char,1023> capture_message_hex(
        const detail::authenticated_ready_control_test_observation::capture_failure& failure) noexcept {
        std::array<char,1023> out{};constexpr char digits[]="0123456789abcdef";
        const auto count=std::min<size_t>(failure.bytes,failure.message.size()-1);
        for(size_t i=0;i<count;++i){const auto byte=static_cast<unsigned char>(failure.message[i]);
            out[2*i]=digits[byte>>4];out[2*i+1]=digits[byte&15];}
        return out;
    }
    void report()const noexcept{
        try{json rows=json::array();size_t copied=0;
            for(size_t i=0;i<retained;++i){const auto& row=records[i];copied+=row.used;
                json item={{"ordinal",row.ordinal},{"parent",row.parent},{"connection",row.connection},{"wait",row.wait},
                    {"peer",row.peer},{"event",label(row.event)},{"operation",label(row.operation)},
                    {"requestBytes",row.request_bytes},{"copiedBytes",row.used},{"publishable",label(row.publishable)},
                    {"opened",label(row.opened)},{"delivered",label(row.delivered)},{"completed",label(row.completed)},
                    {"beforeSuppressed",row.before_suppressed},{"prepareDropped",row.prepare_dropped},{"terminalEmpty",row.terminal_empty},
                    {"source",metadata(row,row.source)},{"after",metadata(row,row.after)}};
                if(row.status_observed)item["status"]=row.status;
                if(row.capture_failure_observed){const auto& failure=row.capture_failure_copy;
                    const auto hex=capture_message_hex(failure);
                    item["actualCaptureFailure"]={{"kind",static_cast<unsigned>(failure.kind)},
                        {"messageHex",std::string(hex.data())},{"messageEncoding","hex"},
                        {"bytes",failure.bytes},{"truncated",failure.truncated},{"nullMessage",failure.null_message}};}
                item["requestIndexObserved"]=row.request_index.observed;item["requestIndexStored"]=row.request_index.stored;
                if(row.request_index.stored){const json text=std::string(row.bytes.data()+row.request_index.offset,row.request_index.bytes);
                    if(const auto index=quantity(text))item["requestIndex"]=*index;}
                rows.push_back(std::move(item));}
            const json report={{"total",total},{"retained",retained},{"overflow",overflow},{"copiedBytes",copied},{"records",std::move(rows)}};
            std::cerr<<"controller boundary observation: "<<report.dump()<<std::endl;
        }catch(...){/* Original count/error report and failure are independent. */}
    }
};
// Passive copies from the existing private worker probe. Fixed storage, no
// owner/route/SQL access; a later fixture phase cannot turn these into grants.
struct ControllerWorkerTrace {
    struct Stage {uint64_t count=0,first_ns=0,last_ns=0;};
    inline static constexpr std::array<const char*,7> labels{{"control-response-ready","range-response-ready",
        "response-consumed","cohort-retained","admission-deferred","install-committed","resume-committed"}};
    inline static constexpr std::array<const char*,4> predicate_labels{{"revision1Count","phase","installed2","settledCount"}};
    std::mutex mutex;
    const std::chrono::steady_clock::time_point origin=std::chrono::steady_clock::now();
    const bool idle;
    std::array<Stage,7> stages{};
    std::array<int64_t,4> predicates{};
    std::array<bool,4> evaluated{};
    uint64_t other=0,evaluations=0;
    std::atomic<bool> observation_failed{false};
    explicit ControllerWorkerTrace(bool idle_start):idle(idle_start){}
    void observe(const char* label) noexcept {
        try {size_t index=labels.size();
            for(size_t n=0;n<labels.size();++n)if(std::strcmp(label,labels[n])==0){index=n;break;}
            const auto elapsed=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-origin).count();
            const auto now=static_cast<uint64_t>(std::max<int64_t>(0,elapsed));
            std::lock_guard lock(mutex);
            if(index==labels.size()){ControllerBoundaryTrace::increment(other);return;}
            auto& stage=stages[index];if(!stage.count)stage.first_ns=now;
            else stage.first_ns=std::min(stage.first_ns,now);
            stage.last_ns=std::max(stage.last_ns,now);ControllerBoundaryTrace::increment(stage.count);
        }catch(...){observation_failed.store(true);}
    }
    void begin_predicate() noexcept {
        try {std::lock_guard lock(mutex);evaluated.fill(false);ControllerBoundaryTrace::increment(evaluations);}
        catch(...){observation_failed.store(true);}
    }
    template<class T>T value(size_t index,T copied) noexcept {
        try {std::lock_guard lock(mutex);if(index<predicates.size()){predicates[index]=static_cast<int64_t>(copied);evaluated[index]=true;}}
        catch(...){observation_failed.store(true);}return copied;
    }
    void report() noexcept {
        try {std::array<Stage,7> copied_stages;std::array<int64_t,4> copied_predicates;std::array<bool,4> copied_evaluated;
            uint64_t copied_other,copied_evaluations;
            {std::lock_guard lock(mutex);copied_stages=stages;copied_predicates=predicates;copied_evaluated=evaluated;
                copied_other=other;copied_evaluations=evaluations;}
            json rows=json::array(),values=json::object();
            for(size_t n=0;n<labels.size();++n){const auto& stage=copied_stages[n];
                json row={{"stage",labels[n]},{"count",stage.count}};
                if(stage.count){row["firstNanoseconds"]=stage.first_ns;row["lastNanoseconds"]=stage.last_ns;}rows.push_back(std::move(row));}
            for(size_t n=0;n<predicate_labels.size();++n){json value={{"evaluated",copied_evaluated[n]}};
                if(copied_evaluated[n])value["value"]=copied_predicates[n];values[predicate_labels[n]]=std::move(value);}
            const json report={{"scope",idle?"startIdle":"firstInstall"},{"stages",std::move(rows)},
                {"otherStageCount",copied_other},{"predicateEvaluations",copied_evaluations},{"lastPredicate",std::move(values)},
                {"observationFailed",observation_failed.load()}};
            std::cerr<<"controller worker observation: "<<report.dump()<<std::endl;
        }catch(...){/* The original predicate failure remains authoritative. */}
    }
};
std::string controller_uuid(unsigned n){char out[37];std::snprintf(out,sizeof(out),"80000000-0000-4000-8000-%012u",n);return out;}
swift_schema_entry controller_schema(){swift_schema_entry out;out.table_name="ControllerRow";property_descriptor p{};
    p.name="value";p.type=column_type::text;p.kind=property_kind::primitive;out.properties[p.name]=p;
    p.name="note";p.no_history=true;out.properties[p.name]=p;return out;}
struct ControllerServerRoute {std::shared_ptr<std::atomic<bool>> live;static int32_t current(void* p){return static_cast<ControllerServerRoute*>(p)->live->load()?1:0;}static void destroy(void* p){delete static_cast<ControllerServerRoute*>(p);}};
struct ControllerWire {
    struct Dial {std::string url;platform_transport_callbacks endpoint;};
    struct Frame {platform_transport_callbacks endpoint;std::string raw;};
    std::mutex mutex;std::deque<Dial> dials;std::deque<Frame> frames;std::vector<platform_transport_callbacks> endpoints;
    bool record_message_types=false;std::vector<transport_message::type> message_types;
    struct Pipe {std::weak_ptr<ControllerWire> wire;};
    static std::unique_ptr<sync_transport> transport(const std::shared_ptr<ControllerWire>& wire) {
        // Actual owned SDK platform boundary with a mechanical verifier. This
        // tests native controller wiring; hosted stock-adapter TLS is separate.
        return std::unique_ptr<sync_transport>(make_system_tls_platform_sync_transport(new Pipe{wire},
            [](void* p,const void* url,const void*,const void* endpoint){if(auto wire=static_cast<Pipe*>(p)->wire.lock()){std::lock_guard lock(wire->mutex);wire->dials.push_back({*static_cast<const std::string*>(url),*static_cast<const platform_transport_callbacks*>(endpoint)});}},
            [](void*){},
            [](void* p,const void* message,const void* endpoint){if(auto wire=static_cast<Pipe*>(p)->wire.lock()){const auto* frame=static_cast<const transport_message*>(message);
                if(frame->data.size()>8388608)throw db_error("fixture wire bound");std::lock_guard lock(wire->mutex);if(wire->frames.size()>=32)throw db_error("fixture queue bound");if(wire->record_message_types){if(wire->message_types.size()>=256)throw db_error("fixture message type observation bound");wire->message_types.push_back(frame->msg_type);}wire->frames.push_back({*static_cast<const platform_transport_callbacks*>(endpoint),frame->as_string()});}},
            [](void* p){delete static_cast<Pipe*>(p);},nullptr,[](void*,const void*,const void*)->int32_t{return 1;},[](void*){}));
    }
};
class ControllerNetwork final:public network_factory {
    std::shared_ptr<ControllerWire> wire_;
public:
    explicit ControllerNetwork(std::shared_ptr<ControllerWire> wire):wire_(std::move(wire)){}
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{return ControllerWire::transport(wire_);}
};
class RecoveryReceiverController:public ::testing::Test {
protected:
    TempDB source_file{"controller_source"},receiver_file{"controller_receiver"};
    std::filesystem::path container=receiver_file.str()+".lattice-continuous";
    std::unique_ptr<swift_lattice_ref> source_ref,receiver_ref;
    std::shared_ptr<::lattice::swift_lattice> source,receiver;
    std::shared_ptr<ControllerWire> wire=std::make_shared<ControllerWire>();
    std::shared_ptr<network_factory> previous;
    std::vector<std::unique_ptr<synchronizer>> synchronizers;
    struct Peer {std::string channel,endpoint,ns;json expectation;relay_recovery_setup setup;std::shared_ptr<std::atomic<bool>> live=std::make_shared<std::atomic<bool>>(true);platform_transport_callbacks physical;uint64_t trace_connection=0;};
    std::vector<Peer> peers;
    ControllerBoundaryTrace boundary_trace;
    std::shared_ptr<ControllerWorkerTrace> worker_trace;
    continuous_policy policy;
    std::vector<std::string> requests,errors;std::mutex errors_mutex;
    std::deque<ControllerWire::Frame> held_uploads;
    std::vector<std::pair<size_t,std::vector<std::string>>> observed_uploads;
    std::vector<std::shared_ptr<ControllerPause>> pauses;
    const std::shared_ptr<ControllerRouteHookCell> callback_hooks=std::make_shared<ControllerRouteHookCell>();
    std::unique_ptr<detail::recovery_receiver_controller_test_access> probe;
    const std::shared_ptr<detail::controller_fixture_worker_endings> worker_endings=std::make_shared<detail::controller_fixture_worker_endings>();
    detail::controller_fixture_worker_selection worker_selection{worker_endings};
    bool hold_second=false,drop_prepare=false,hold_uploads=true;size_t dropped=0,handled=0;
    size_t upload_chunk=1000;
    virtual json source_policy(const std::string& ns) {
        return {{"version",1},{"authority","controller-service"},{"sourceID",controller_uuid(1)},{"epoch",controller_uuid(2)},
            {"localNamespace","local"},{"namespaces",json::array({{{"namespaceID","local"},{"coverageID","local-v1"},{"revision",1}},
                {{"namespaceID","a"},{"coverageID","a-v1"},{"revision",1}},{{"namespaceID","b"},{"coverageID","b-v1"},{"revision",1}}})},
            {"receiptNamespace",ns},{"models",json::array({"ControllerRow"})},{"walFull",true},{"maximumAuthorizationMilliseconds",600000},
            {"readyProfile","bounded48MiBV1"},{"upload",{{"tables",json::array()},{"unlisted","allow"},{"maximumDeletes",256}}}};
    }
    virtual relay_recovery_setup serve(size_t index) {
        auto& peer=peers.at(index);auto config=json{{"mount",controller_uuid(3)},{"connection",::lattice::uuid_t::generate().to_string()},
            {"channel",peer.channel},{"authenticatedUserID",controller_uuid(4)},
            {"peer",{{"replicaID","registered-controller"},{"receiverIncarnation",controller_uuid(20)},{"channelIncarnation",controller_uuid(30+index)}}}};
        auto setup=source_ref->open_relay_recovery_setup(source_policy(peer.ns).dump(),config.dump(),new ControllerServerRoute{peer.live},ControllerServerRoute::current,ControllerServerRoute::destroy);
        if(!setup.valid())throw db_error("actual source setup failed: "+std::string(last_bridge_error()));
        const auto context=json::parse(setup.descriptor());const auto answer=json{{"context",context},{"authenticatedUserID",context["route"]["authenticatedUserID"]},
            {"peer",context["route"]["peer"]},{"source",context["source"]},{"incomingScope",context["incomingScope"]},
            {"authorizationRevision","registered-controller-v1"},{"validForMilliseconds",600000}};
        if(!setup.finish_authorization(answer.dump()))throw db_error("actual source app authorization failed");return setup;
    }
    void SetUp()override {
        previous=get_network_factory();set_network_factory(std::make_shared<ControllerNetwork>(wire));
        swift_configuration config(source_file.str(),std::make_shared<immediate_scheduler>());config.audit_retention_seconds=0;
#if LATTICE_HAS_FRT
        source_ref.reset(swift_lattice_ref::create(config,{controller_schema()}));
#else
        source_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config,{controller_schema()}));
#endif
        source=swift_lattice_ref::shared_for_lattice(source_ref->get());ASSERT_TRUE(source);
        if(auto* notifier=instance_registry::instance().get_or_create_notifier(source_file.str()))notifier->stop_listening();
    }
    void configure(size_t count=1,bool publish=true) {
        policy.scopes=4;policy.records=128;policy.field_bytes=256;policy.journal_bytes=2097152;
        policy.channels=4;policy.binding_field_bytes=256;policy.binding_bytes=32768;
        policy.profiles=4;policy.stamps=128;policy.producer_field_bytes=256;policy.manifest_bytes=1048576;policy.producer_bytes=8388608;
        policy.owners=8;policy.physical_routes=8;policy.operations=8;policy.frozen_entries=128;policy.frozen_bytes=2097152;policy.canonical_recovery_profile=2;
        for(size_t i=0;i<count;++i){Peer peer;peer.channel="controller-"+std::to_string(i);peer.endpoint="wss://registered.example/controller/"+std::to_string(i);peer.ns=i?"b":"a";peers.push_back(std::move(peer));
            peers.back().setup=serve(i);const auto d=json::parse(peers.back().setup.descriptor());
            peers.back().expectation={{"endpoint",peers.back().endpoint},{"source",d.at("source")},{"incomingScope",d.at("incomingScope")},
                {"peer",d.at("route").at("peer")},{"channel",peers.back().channel},{"validForMilliseconds",600000}};
            continuous_contribution c;c.channel=peers.back().channel;c.authority=d["source"]["authority"];c.source=d["source"]["sourceID"];c.epoch=d["source"]["epoch"];
            c.scope=d["source"]["scopeDigest"];c.schema=d["source"]["schemaDigest"];c.profile_digest="controller-profile-v1";c.receipt_namespace=peers.back().ns;
            c.models={"ControllerRow"};const auto claim=d["incomingScope"].dump();c.incoming_grant_claim={claim.begin(),claim.end()};policy.contributions.push_back(c);policy.routes.push_back({c.channel,peers.back().endpoint});}
        if(publish)open_receiver();
    }
    std::shared_ptr<ControllerPause> pause_install(){auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[pause](const char* stage){if(std::strcmp(stage,"install-committed")==0)pause->wait();});return pause;}
    void open_receiver() {
        boundary_trace.add(ControllerBoundaryTrace::Event::open_receiver);
        swift_configuration config((container/"store.sqlite").string(),std::make_shared<ControllerRouteThreadScheduler>(callback_hooks));config.audit_retention_seconds=0;config.busy_timeout_ms=100;
        continuous_result result;
#if LATTICE_HAS_FRT
        receiver_ref.reset(swift_lattice_ref::create_continuous(config,{controller_schema()},policy,result));
#else
        receiver_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create_continuous(config,{controller_schema()},policy,result));
#endif
        if(result.phase()!=2||result.has_error())throw db_error("actual receiver public factory: "+result.primary_error()+result.postcommit_error());
        receiver=swift_lattice_ref::shared_for_lattice(receiver_ref->get());ASSERT_TRUE(receiver);
        if(auto* notifier=instance_registry::instance().get_or_create_notifier(receiver->config().path))notifier->stop_listening();
    }
    struct ConnectFailureObservation {
        detail::recovery_admission_test_hooks::route_admission_observation admission;
        unsigned char boundary=0, peer=0, wrappers=0;
        bool observed=false;
    } connect_failure;
    static unsigned char finite_connect_count(size_t count)noexcept {
        return static_cast<unsigned char>(std::min<size_t>(count,255));
    }
    void report_connect_failure(const detail::recovery_admission_test_hooks::route_admission_observation& admission,
        unsigned char boundary,size_t peer)noexcept {
        // Called only after the exact throwing operation unwinds. Copy only
        // this thread's passive scalar record and fixture-owned vector size.
        // No owner/SQLite/controller/transport/scheduler inspection, exception
        // retention, mutex, callback, timestamp, identifier or error text.
        connect_failure={admission,boundary,finite_connect_count(peer),finite_connect_count(synchronizers.size()),true};
        std::fprintf(stderr,"controller connect exception observation: {\"boundary\":%u,\"peer\":%u,\"wrappers\":%u,\"routeCalls\":%u,\"point\":%u,\"maintenanceCalls\":%u,\"maintenance\":%u,\"installState\":%u,\"primary\":%u,\"cleanup\":%u,\"postcommit\":%u,\"notification\":%u,\"unexpectedCommit\":%u}\n",
            unsigned(connect_failure.boundary),unsigned(connect_failure.peer),unsigned(connect_failure.wrappers),
            unsigned(admission.route_calls),unsigned(admission.point),unsigned(admission.maintenance_calls),unsigned(admission.maintenance),
            unsigned(admission.install_state),unsigned(admission.primary),unsigned(admission.cleanup),unsigned(admission.postcommit),
            unsigned(admission.notification),unsigned(admission.unexpected_commit));
    }
    void connect() {
        size_t peer_index=0;
        for(auto& peer:peers){sync_config config;config.websocket_url=peer.endpoint;config.authorization_token="registered-token";config.sync_id=peer.channel;for(const auto& active:peers)config.all_active_sync_ids.push_back(active.channel);
            config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;config.chunk_size=upload_chunk;
            detail::recovery_admission_test_hooks::route_admission_observation admission;
            detail::recovery_admission_test_hooks::route_observation_scope observation(admission);
            unsigned char boundary=1; // exact synchronous constructor
            try {
                auto sync=std::make_unique<synchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
                boundary=2; // existing callback registration/vector publication
                sync->set_on_error([this](const std::string& e){std::lock_guard lock(errors_mutex);errors.push_back(e);});synchronizers.push_back(std::move(sync));
                boundary=3; // exact synchronous public connect
                synchronizers.back()->connect();
            }catch(...){report_connect_failure(admission,boundary,peer_index);throw;}
            ++peer_index;
        }
    }
    std::vector<std::string> originals() {
        std::vector<std::string> result;for(const auto& row:receiver->db().query("SELECT globalId FROM AuditLog ORDER BY id"))result.push_back(std::get<std::string>(row.at("globalId")));return result;
    }
    std::vector<std::string> held_originals(size_t peer=0) {
        std::vector<std::string> result;for(const auto& frame:held_uploads)if(frame.endpoint.matches(peers.at(peer).physical)) {
            const auto event=server_sent_event::from_json(frame.raw);if(!event||event->event_type!=server_sent_event::type::audit_log)throw db_error("fixture invalid held upload");
            for(const auto& entry:event->audit_logs)result.push_back(entry.global_id);
        }return result;
    }
    std::vector<std::string> observed_originals(size_t peer=0,size_t from=0) {
        std::vector<std::string> result;for(size_t n=from;n<observed_uploads.size();++n)if(observed_uploads[n].first==peer)
            result.insert(result.end(),observed_uploads[n].second.begin(),observed_uploads[n].second.end());return result;
    }
    void seed_local(unsigned count,unsigned first=200) {
        receiver->begin_transaction();try {for(unsigned n=0;n<count;++n){
            swift_dynamic_object row;row.table_name="ControllerRow";row.properties=controller_schema().properties;
            row.values["globalId"]=controller_uuid(first+n);row.values["value"]=std::to_string(n);row.values["note"]=std::string("private");
            dynamic_object object(row);receiver->add(object);
        }receiver->commit();}catch(...){if(receiver->db().is_in_transaction())receiver->rollback();throw;}
    }
    void legacy_ack(size_t peer,const std::vector<std::string>& ids) {
        if(!peers.at(peer).physical.trigger_on_message(transport_message::from_string(server_sent_event::make_ack(ids).to_json())))
            throw db_error("fixture current legacy ACK endpoint retired");
    }
    void request_recovery(size_t peer=0) {
        if(!peers.at(peer).physical.trigger_on_message(transport_message::from_string(server_sent_event::make_audit_log({}).to_json())))
            throw db_error("fixture current recovery request endpoint retired");
    }
    virtual void release_controller_worker_holds() {for(const auto& pause:pauses)pause->release();}
    void close_receiver()noexcept {
        try {
        boundary_trace.add(ControllerBoundaryTrace::Event::close_receiver);
        release_controller_worker_holds();
        synchronizers.clear(); // Retires routes/callback admission; does NOT prove controller join.
        worker_endings->join_originals(); // Actual reaper join, while Native owner/source/namespace still live.
        {std::lock_guard lock(errors_mutex);errors.clear();}
        held_uploads.clear();detail::end_fixture_owner_checked(receiver.get(),"controller-receiver");receiver.reset();receiver_ref.reset();
        }catch(...){detail::controller_fixture_worker_endings::fail_ending("fixture-release-join-or-close-threw");}
    }
    struct ControllerLocalWorkerEnding {
        RecoveryReceiverController* fixture;
        ~ControllerLocalWorkerEnding()noexcept {fixture->close_receiver();}
    };
    [[nodiscard]] ControllerLocalWorkerEnding scoped_controller_worker_ending()noexcept{return {this};}
    void TearDown()override {
        for(const auto& pause:pauses)pause->release();
        close_receiver();for(const auto& pause:pauses)EXPECT_FALSE(pause->timedOut());probe.reset();for(auto& peer:peers){peer.live->store(false);peer.setup.close_on_io();peer.setup={};}peers.clear();
        {std::lock_guard lock(wire->mutex);wire->frames.clear();wire->dials.clear();wire->endpoints.clear();}
        detail::end_fixture_owner_checked(source.get(),"controller-source");source.reset();source_ref.reset();set_network_factory(previous);std::error_code error;std::filesystem::remove_all(container,error);
    }
    // Default hooks preserve the original authenticated fixture workload.
    // Successor fixtures may only delay/corrupt actual boundary traffic.
    virtual bool before_ready(size_t,const json&){return false;}
    virtual void after_ready(size_t index,const json&,const std::string& raw){
        boundary_trace.delivery_bytes(raw);
        const bool delivered=peers.at(index).physical.trigger_on_message(transport_message::from_string(raw));
        if(boundary_trace.current)boundary_trace.current->delivered=ControllerBoundaryTrace::flag(delivered);
    }
    bool pump() {
        std::optional<ControllerWire::Dial> dial;std::optional<ControllerWire::Frame> frame;
        {std::lock_guard lock(wire->mutex);if(!wire->dials.empty()){dial=std::move(wire->dials.front());wire->dials.pop_front();}else if(!wire->frames.empty()){frame=std::move(wire->frames.front());wire->frames.pop_front();}}
        if(dial){size_t index=0;while(index<peers.size()&&dial->url.rfind(peers[index].endpoint,0)!=0)++index;if(index==peers.size())throw db_error("fixture unexpected actual dial");
            ControllerBoundaryTrace::increment(boundary_trace.connection);
            auto* observation=boundary_trace.add(ControllerBoundaryTrace::Event::dial,index);
            auto& peer=peers[index];peer.setup.close_on_io();peer.setup=serve(index);peer.physical=dial->endpoint;peer.trace_connection=boundary_trace.connection;
            {std::lock_guard lock(wire->mutex);wire->endpoints.push_back(dial->endpoint);}if(!(hold_second&&index==1)){
                const bool opened=dial->endpoint.trigger_on_open();if(observation)observation->opened=ControllerBoundaryTrace::flag(opened);
            }return true;}
        if(!frame)return false;
        size_t index=0;while(index<peers.size()&&!peers[index].physical.matches(frame->endpoint))++index;if(index==peers.size()){
            if(auto* observation=boundary_trace.add(ControllerBoundaryTrace::Event::retired_frame,index))observation->request_bytes=frame->raw.size();return true;}
        auto& peer=peers[index];const auto control=json::parse(frame->raw);
        if(control.contains("kind")&&control["kind"]=="recoveryReady") {
            if(control["operation"]=="prepare"||control["operation"]=="resume")requests.push_back(control.at("request"));
            auto* observation=boundary_trace.add(ControllerBoundaryTrace::Event::ready,index);
            if(observation)observation->connection=peer.trace_connection;
            ControllerBoundaryTrace::request(observation,control,frame->raw.size());
            ControllerBoundaryTrace::Scope trace_scope(boundary_trace,observation);
            if(before_ready(index,control)){if(observation)observation->before_suppressed=true;return true;}
            auto charge=peer.setup.stop_token().reserve_ready(frame->raw.size());if(!charge.valid())throw db_error("actual source finite request reservation failed");
            detail::authenticated_ready_control_test_observation::observation actual_ready;
            auto result=[&]{
                namespace observed=detail::authenticated_ready_control_test_observation;
                struct reset {observed::observation* previous;~reset(){observed::current=previous;}} restore{observed::current};
                observed::current=&actual_ready;
                return peer.setup.ready(frame->raw,charge); // same sole native call
            }();
            if(observation&&actual_ready.visits[static_cast<size_t>(detail::authenticated_ready_control_test_observation::point::prepare_returned)]){
                observation->capture_failure_observed=true;observation->capture_failure_copy=actual_ready.capture_failure_copy;
            }
            const auto status=result.status_code();bool publishable=false;
            if(status==1)publishable=result.publishable();ControllerBoundaryTrace::result(observation,status,publishable);
            if(status!=1||!publishable)throw db_error("actual source READY result unavailable");++handled;
            if(drop_prepare&&control["operation"]=="prepare"&&dropped++==0){if(observation)observation->prepare_dropped=true;return true;}
            const auto& raw=result.wire();ControllerBoundaryTrace::copy(observation,&ControllerBoundaryTrace::Record::source,raw);
            after_ready(index,control,raw);
        } else if(control.contains("auditLog")) {
            if(observed_uploads.size()>=64)throw db_error("fixture observed upload bound");
            const auto event=server_sent_event::from_json(frame->raw);if(!event||event->event_type!=server_sent_event::type::audit_log)throw db_error("fixture invalid actual upload");
            std::vector<std::string> ids;for(const auto& entry:event->audit_logs)ids.push_back(entry.global_id);
            observed_uploads.emplace_back(index,std::move(ids));
            if(hold_uploads){if(held_uploads.size()>=32)throw db_error("fixture held upload bound");held_uploads.push_back(std::move(*frame));return true;}
            auto result=peer.setup.receive(frame->raw);if(result.status_code()!=1)throw db_error("actual source upload refused");
            auto accepted_ids=result.take_ids();peer.physical.trigger_on_message(transport_message::from_string(server_sent_event::make_ack(accepted_ids).to_json()));
        }
        return true;
    }
    std::function<void(const char*)> worker_observer(bool idle,std::function<void(const char*)> prior={}) {
        worker_trace=std::make_shared<ControllerWorkerTrace>(idle);const std::weak_ptr<ControllerWorkerTrace> trace=worker_trace;
        return [trace,prior=std::move(prior)](const char* stage){
            // Preserve any original restriction/side effect before observing.
            // If it throws, its original error wins and this event is unobserved.
            if(prior)prior(stage);if(const auto active=trace.lock())active->observe(stage);
        };
    }
    void report_until_failure() noexcept {
        // Failure-only copies of already observed fixture facts. No owner,
        // SQLite, transport, controller inspection or extra predicate call.
        try {
            json observation={{"handled",handled},{"dropped",dropped},{"requestCount",requests.size()},
                {"heldUploadCount",held_uploads.size()},{"observedUploadCount",observed_uploads.size()}};
            json messages=json::array();size_t total=0;
            {
                std::lock_guard lock(errors_mutex);total=errors.size();
                for(size_t i=0;i<std::min<size_t>(total,16);++i)messages.push_back(errors[i].substr(0,512));
            }
            observation["errorCount"]=total;observation["errors"]=std::move(messages);
            observation["errorsTruncated"]=total>16;
            std::cerr<<"controller timeout observation: "<<observation.dump()<<std::endl;
        }catch(...){/* Observation cannot alter the original failure result. */}
        boundary_trace.report();
        if(worker_trace)worker_trace->report();
    }
    template<class F> bool until(F predicate,int milliseconds=5000) {const auto end=std::chrono::steady_clock::now()+std::chrono::milliseconds(milliseconds);
        ControllerBoundaryTrace::WaitScope observation(boundary_trace);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate()){observation.finish(true);return true;}std::this_thread::sleep_for(std::chrono::milliseconds(2));}
        const bool result=predicate();observation.finish(result);if(!result)report_until_failure();return result;}
    int64_t scalar(lattice_db& owner,const std::string& sql){return std::get<int64_t>(owner.db().query(sql).at(0).at("n"));}
    int64_t phase(){return scalar(*receiver,"SELECT phase AS n FROM _lattice_producer_continuity");}
    bool has_error(){std::lock_guard lock(errors_mutex);return !errors.empty();}
    bool error_contains(const std::string& text){std::lock_guard lock(errors_mutex);return std::any_of(errors.begin(),errors.end(),[&](const auto& e){return e.find(text)!=std::string::npos;});}
    using Snapshot=std::map<std::string,std::vector<database::row_t>>;
    Snapshot snapshot(){Snapshot value;
        for(const auto* table:{"ControllerRow","AuditLog","_lattice_obligation_entry","_lattice_obligation_scope","_lattice_obligation_store","_lattice_install_channel","_lattice_install_store","_lattice_producer_continuity","_lattice_recovery_request"})
            value[table]=receiver->db().query(std::string("SELECT * FROM ")+table);return value;}
    void refused_fresh_binding(const char* field){configure(2,false);
        auto& c=policy.contributions[1];if(std::strcmp(field,"source")==0)c.source=controller_uuid(901);else if(std::strcmp(field,"epoch")==0)c.epoch=controller_uuid(902);else c.schema=std::string(64,'a');
        EXPECT_THROW(open_receiver(),db_error);
        // A real failed factory owns rollback. Observation is fresh, read-only
        // and WAL-aware, with no immutable=1 or alternate writable owner.
        database observer((container/"store.sqlite").string(),database::open_mode::read_only,100);
        EXPECT_EQ(scalar_from(observer,"SELECT COUNT(*) AS n FROM sqlite_schema WHERE name IN ('_lattice_install_store','_lattice_install_channel','_lattice_obligation_store','_lattice_producer_continuity')"),0);
        EXPECT_EQ(scalar_from(observer,"SELECT COUNT(*) AS n FROM AuditLog"),0);
    }
    static int64_t scalar_from(database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).at("n"));}
    void insert(::lattice::swift_lattice& owner,const std::string& id,const std::string& value){owner.begin_transaction();try{
        swift_dynamic_object row;row.table_name="ControllerRow";row.properties=controller_schema().properties;row.values["globalId"]=id;row.values["value"]=value;row.values["note"]=std::string("private");dynamic_object object(row);owner.add_preserving_global_id(object,id);
        const auto stored=owner.db().query("SELECT globalId FROM ControllerRow WHERE id=?",{object.managed_primary_key()});
        if(stored.size()!=1||std::get<std::string>(stored[0].at("globalId"))!=id)throw db_error("controller fixture did not preserve requested row identity");
        owner.commit();
    }catch(...){if(owner.db().is_in_transaction())owner.rollback();throw;}}
};
TEST_F(RecoveryReceiverController, PublicRetainedFactoryConsumesRealAuthenticatedReadyAndPreservesUnsentOverlay) {
    configure();insert(*source,controller_uuid(100),"source");insert(*receiver,controller_uuid(101),"local");
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");const auto pause=pause_install();connect();
    ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE last_sequence=1 AND active IS NULL"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)>0"),1);
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));
}
TEST_F(RecoveryReceiverController, TwoActualNamespacesHoldOnePhysicalGateUntilEveryCapsuleInstalls) {
    configure(2);insert(*source,controller_uuid(110),"shared");insert(*receiver,controller_uuid(111),"local");const auto pause=pause_install();hold_second=true;connect();
    ASSERT_TRUE(until([&]{return handled>0;}));EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision>0"),0);
    hold_second=false;peers.at(1).physical.trigger_on_open();
    ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_domain"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT version AS n FROM _lattice_install_store"),2);
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));
}
TEST_F(RecoveryReceiverController, InstalledReopenUsesExactFramingAndKeepsOriginalsBeforeNextCycle) {
    configure();insert(*source,controller_uuid(120),"source");connect();
    ASSERT_TRUE(until([&]{return scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1&&phase()==0;}));
    const auto rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");const auto q=receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");
    close_receiver();open_receiver();EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel"),q);
    connect();ASSERT_TRUE(until([&]{return scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2")==1&&phase()==0;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);
}
TEST_F(RecoveryReceiverController, NextBarrierReopensWithPriorInstalledFraming) {
    configure();insert(*source,controller_uuid(125),"source");
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),worker_observer(false));connect();
    ASSERT_TRUE(until([&]{worker_trace->begin_predicate();return worker_trace->value(0,scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"))==1&&worker_trace->value(1,phase())==0;}));
    worker_trace.reset(); // This diagnostic scope ends with the first installation.
    close_receiver();probe.reset();open_receiver();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"barrier-committed")==0)throw db_error("fixture process stop after next barrier commit");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),1);
    const auto prior=receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request");
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(phase(),1);
    EXPECT_EQ(receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request"),prior);
    connect();ASSERT_TRUE(until([&]{return scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2")==1&&phase()==0;}));
}
TEST_F(RecoveryReceiverController, LostPrepareReopenResumesExactQAndFencesTheOldPhysicalEndpoint) {
    configure();insert(*source,controller_uuid(130),"source");drop_prepare=true;connect();
    ASSERT_TRUE(until([&]{return dropped==1;}));ASSERT_FALSE(requests.empty());const auto q=requests.front();const auto old=peers[0].physical;
    const auto durable=receiver->db().query("SELECT request_frame FROM _lattice_recovery_request");close_receiver();drop_prepare=false;open_receiver();
    EXPECT_EQ(receiver->db().query("SELECT request_frame FROM _lattice_recovery_request"),durable);connect();
    EXPECT_FALSE(old.trigger_on_message(transport_message::from_string("{}")));
    ASSERT_TRUE(until([&]{return scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1&&phase()==0;}));
    ASSERT_GE(requests.size(),2u);auto before=json::parse(q),after=json::parse(requests.back());before["latticeCanonicalRange"]["route_generation"]=after["latticeCanonicalRange"]["route_generation"];
    EXPECT_EQ(before,after);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
}
TEST_F(RecoveryReceiverController, ChangedPersistedProfileCannotAdoptTheEnabledStore) {
    configure();insert(*receiver,controller_uuid(140),"retained");const auto schema=receiver->db().query("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name");
    const auto original=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");close_receiver();policy.canonical_recovery_profile=0;
    EXPECT_THROW(open_receiver(),db_error);
    policy.canonical_recovery_profile=2;open_receiver();EXPECT_EQ(receiver->db().query("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name"),schema);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),original);
}
TEST_F(RecoveryReceiverController, SameDomainDifferentSourceRefusesAndRollsBackEnrollment) {refused_fresh_binding("source");}
TEST_F(RecoveryReceiverController, SameDomainDifferentEpochRefusesAndRollsBackEnrollment) {refused_fresh_binding("epoch");}
TEST_F(RecoveryReceiverController, SameDomainDifferentSchemaRefusesAndRollsBackEnrollment) {refused_fresh_binding("schema");}
TEST_F(RecoveryReceiverController, ActualInstallCommitDenialRollsBackEveryCohortAndReopenRetriesExactFraming) {
    configure(2);insert(*source,controller_uuid(150),"canonical");insert(*receiver,controller_uuid(151),"pending");
    std::atomic<unsigned> hits{0};std::mutex before_mutex;Snapshot before;
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[&](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"install")!=0)return {};{std::lock_guard lock(before_mutex);before=snapshot();}
        return std::make_shared<ControllerCommitFault>(receiver.get(),hits);
    });
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(hits.load(),1u);
    {std::lock_guard lock(before_mutex);ASSERT_FALSE(before.empty());EXPECT_EQ(snapshot(),before);}
    EXPECT_EQ(phase(),2);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision>0"),0);
    const auto framing=receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request ORDER BY channel");
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request ORDER BY channel"),framing);
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
}
TEST_F(RecoveryReceiverController, LostKnownInstallResultReopensInstalledPhaseAndResumesWithoutReplayingModels) {
    configure();insert(*source,controller_uuid(160),"canonical");
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"install-committed")==0)throw db_error("fixture lost known install result");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),3);
    const auto rows=receiver->db().query("SELECT * FROM ControllerRow");const auto original=receiver->db().query("SELECT * FROM AuditLog");
    const auto installed=receiver->db().query("SELECT * FROM _lattice_install_channel");
    close_receiver();probe.reset();open_receiver();ASSERT_EQ(phase(),3);EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_install_channel"),installed);
    connect();ASSERT_TRUE(until([&]{return phase()==0;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow"),rows);EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_install_channel"),installed);
}

TEST_F(RecoveryReceiverController, PositiveOneNamespaceAndClaimedUnknownOtherRemainClosedWithOriginalsIntact) {
    // This fixture serves version1 without registered producer coverage. A
    // receipt owned by A cannot become an UNKNOWN receipt for B. The actual
    // source refusal must retain the frozen Q and all original local intent.
    // The registered v3 reconciliation path has its own coverage fixture.
    configure(2);
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    insert(*receiver,controller_uuid(170),"local-original");
    ASSERT_TRUE(until([&]{return held_uploads.size()>=2;}));
    auto frame=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& value){return value.endpoint.matches(peers[0].physical);});ASSERT_NE(frame,held_uploads.end());
    auto imported=peers[0].setup.receive(frame->raw);ASSERT_EQ(imported.status_code(),1);ASSERT_EQ(imported.take_ids().size(),1u);
    const auto before=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");const auto entries=receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL AND stage=0"),2);
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(server_sent_event::make_audit_log({}).to_json())));
    ASSERT_TRUE(until([&]{return error_contains("controller source preparation refused; exact Q retained");}));EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),entries);
    // Only A published and verified the second attempt; B refused before a
    // manifest existed. Initial installed staging was already released.
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)>0"),1);
    const auto framing=receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");
    EXPECT_THROW(insert(*receiver,controller_uuid(171),"gate must stay closed"),db_error);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    close_receiver();open_receiver();EXPECT_EQ(phase(),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel"),framing);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),entries);
    EXPECT_THROW(insert(*receiver,controller_uuid(171),"reopened gate must stay closed"),db_error);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
}

TEST_F(RecoveryReceiverController, OneGlobalNoHistoryUpdateDeleteOverlayPreservesBothChannelOriginals) {
    configure(2);const auto id=controller_uuid(180);insert(*source,id,"canonical");insert(*receiver,id,"local");
    receiver->begin_transaction();try {
        receiver->db().execute("UPDATE ControllerRow SET value=?,note=? WHERE globalId=?",{std::string("ordinary-change"),std::string("latest-private"),id});
        receiver->db().execute("DELETE FROM ControllerRow WHERE globalId=?",{id});receiver->commit();
    }catch(...){if(receiver->db().is_in_transaction())receiver->rollback();throw;}
    const auto before=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");ASSERT_EQ(before.size(),3u);
    const auto entries=receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");ASSERT_EQ(entries.size(),6u);
    const auto pause=pause_install();connect();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),entries);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),2);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));
}

TEST_F(RecoveryReceiverController, EnabledPublicCancellationCannotReleaseAControllerFrozenRequest) {
    configure();insert(*source,controller_uuid(185),"canonical");drop_prepare=true;connect();
    ASSERT_TRUE(until([&]{return dropped==1;}));ASSERT_EQ(phase(),2);const auto before=snapshot();
    auto status=receiver_ref->inspect_continuous();ASSERT_EQ(status.phase(),2);auto barrier=status.barrier();ASSERT_TRUE(barrier.valid());
    auto refused=barrier.cancel();EXPECT_NE(refused.phase(),2);EXPECT_TRUE(refused.has_error());EXPECT_EQ(snapshot(),before);
}

TEST_F(RecoveryReceiverController, EnabledUniqueOriginalCapacityCountsSharedRowsAndRollsBackTheNextWrite) {
    configure(2,false);policy.records=20000;policy.stamps=20000;policy.journal_bytes=16777216;policy.producer_bytes=16777216;
    policy.frozen_entries=20000;policy.frozen_bytes=16777216;open_receiver();const auto id=controller_uuid(190);
    insert(*receiver,id,"0");receiver->begin_transaction();try {
        for(unsigned i=1;i<8192;++i)receiver->db().execute("UPDATE ControllerRow SET value=? WHERE globalId=?",{std::to_string(i),id});
        receiver->commit();
    }catch(...){if(receiver->db().is_in_transaction())receiver->rollback();throw;}
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM AuditLog"),8192);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry"),16384);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(DISTINCT original) AS n FROM _lattice_obligation_entry WHERE stage<>2"),8192);
    const auto before=snapshot();
    EXPECT_THROW(insert(*receiver,controller_uuid(191),"one beyond the explicit profile"),db_error);
    EXPECT_EQ(snapshot(),before);
    EXPECT_EQ(scalar(*receiver,"SELECT max_records AS n FROM _lattice_obligation_store"),20000);
}

TEST_F(RecoveryReceiverController, LegacyAckedUnknownResendsEveryOriginalAcrossNegotiatedPagesAndResumes) {
    configure();upload_chunk=2;connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(5);
    const auto ids=originals();ASSERT_EQ(ids.size(),5u);ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    const auto first=observed_uploads.size();ASSERT_EQ(first,3u);held_uploads.clear();hold_uploads=false;
    // A current legacy delivery ACK carries no canonical receipt. Even with
    // every marker=1, the actual authoritative source still reports UNKNOWN.
    legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==5;}));
    EXPECT_EQ(observed_originals(0,first),ids);EXPECT_EQ(observed_uploads.size()-first,3u);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),5);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),5);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),5);
    EXPECT_FALSE(has_error());EXPECT_NO_THROW(insert(*receiver,controller_uuid(299),"writes resumed"));
}
TEST_F(RecoveryReceiverController, UnchangedUnknownRefreezesOnceWithoutAckAndDrainRemainsPending) {
    configure();upload_chunk=2;connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(5);
    const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    const auto first=observed_uploads.size();held_uploads.clear();legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);
    {std::lock_guard lock(errors_mutex);ASSERT_EQ(errors.size(),1u);EXPECT_NE(errors.front().find("UNKNOWN persisted after one restricted pass"),std::string::npos);}
    EXPECT_EQ(held_originals(),ids);EXPECT_EQ(observed_originals(0,first),ids);EXPECT_EQ(observed_uploads.size()-first,3u);
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),3);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    const auto closed=snapshot();const auto frames=observed_uploads;
    for(unsigned n=0;n<3;++n){EXPECT_NO_THROW(synchronizers[0]->sync_now());
        const auto result=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::milliseconds(100));
        EXPECT_EQ(result.state,sync_drain_state::deadline_pending);EXPECT_FALSE(result.error);EXPECT_TRUE(result.discovery_pending);
        for(unsigned step=0;step<128&&pump();++step){}EXPECT_EQ(observed_uploads,frames);EXPECT_EQ(snapshot(),closed);
    }
    EXPECT_THROW(insert(*receiver,controller_uuid(299),"must remain closed"),db_error);
}
TEST_F(RecoveryReceiverController, LostKnownReconciliationCancelCommitReopensRestrictedPhaseWithOriginalClaims) {
    configure();std::atomic<bool> stop{false};
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[&](const char* stage){
        if(stop&&std::strcmp(stage,"reconcile-cancel-committed")==0)throw db_error("fixture lost known cancellation result");});
    connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(3);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    stop=true;held_uploads.clear();legacy_ack(0,ids);ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),4);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL"),3);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    const auto framing=receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request");
    close_receiver();probe.reset();open_receiver();ASSERT_EQ(phase(),4);
    EXPECT_EQ(receiver->db().query("SELECT request_frame,manifest_frame FROM _lattice_recovery_request"),framing);
    hold_uploads=false;connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==3;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),3);EXPECT_FALSE(has_error());
}
TEST_F(RecoveryReceiverController, ReconciliationCancelCommitDenialPreservesWholeFrozenCohortAndReopenRecovers) {
    configure();std::atomic<bool> deny{false};std::atomic<unsigned> hits{0};std::mutex before_mutex;Snapshot before;
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[&](const char* stage)->std::shared_ptr<void>{
        if(!deny||std::strcmp(stage,"reconcile-cancel")!=0)return {};{std::lock_guard lock(before_mutex);before=snapshot();}
        return std::make_shared<ControllerCommitFault>(receiver.get(),hits);});
    connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(2);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");deny=true;held_uploads.clear();legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);EXPECT_EQ(hits.load(),1u);
    {std::lock_guard lock(before_mutex);ASSERT_FALSE(before.empty());EXPECT_EQ(snapshot(),before);}
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    close_receiver();probe.reset();open_receiver();hold_uploads=false;connect();
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==2;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_FALSE(has_error());
}
TEST_F(RecoveryReceiverController, RefreezeCommitDenialPreservesRestrictedPhaseAndReopenReplaysExactOriginals) {
    configure();std::atomic<bool> deny{false};std::atomic<unsigned> hits{0};std::mutex before_mutex;Snapshot before;
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[&](const char* stage)->std::shared_ptr<void>{
        if(!deny||std::strcmp(stage,"reconcile-refreeze")!=0)return {};{std::lock_guard lock(before_mutex);before=snapshot();}
        return std::make_shared<ControllerCommitFault>(receiver.get(),hits);});
    connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(2);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");deny=true;held_uploads.clear();legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),4);EXPECT_EQ(hits.load(),1u);
    {std::lock_guard lock(before_mutex);ASSERT_FALSE(before.empty());EXPECT_EQ(snapshot(),before);}
    EXPECT_EQ(held_originals(),ids);EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    close_receiver();probe.reset();open_receiver();hold_uploads=false;connect();
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==2;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_FALSE(has_error());
}

TEST_F(RecoveryReceiverController, FullTwoThousandSendWindowRefreezesWithoutWaitingForLegacyAck) {
    configure(1,false);policy.records=2048;policy.stamps=2048;policy.frozen_entries=2048;
    policy.journal_bytes=8388608;policy.frozen_bytes=8388608;policy.producer_bytes=16777216;open_receiver();
    connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(2000,1000);
    const auto ids=originals();ASSERT_EQ(ids.size(),2000u);ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");const auto first=observed_uploads.size();
    held_uploads.clear();legacy_ack(0,ids);
    // The original fixture's five-second observation bound is shorter than
    // the ordinary ten-second ACK timeout. Only transition-only count0 work
    // can reach this next verified Q while every restricted send stays live.
    ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);
    EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,2000);
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),3);
    EXPECT_EQ(observed_originals(0,first),ids);ASSERT_EQ(observed_uploads.size()-first,8u);
    for(size_t n=0;n<8;++n){EXPECT_EQ(observed_uploads[first+n].first,0u);EXPECT_EQ(observed_uploads[first+n].second.size(),n<7?256u:208u);}
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    {std::lock_guard lock(errors_mutex);ASSERT_EQ(errors.size(),1u);EXPECT_NE(errors.front().find("UNKNOWN persisted after one restricted pass"),std::string::npos);}
}

// These cases drive the real owner scheduler. No descriptor, export grant or
// claim ticket is constructed by the fixture; callbacks only observe or pause.
class FirstClaimScheduler final : public scheduler {
    mutable std::mutex mutex_;std::deque<std::function<void()>> jobs_;bool stopped_=false;
    static thread_local const FirstClaimScheduler* current_;
    const std::shared_ptr<ControllerRouteHookCell> hooks_;
public:
    explicit FirstClaimScheduler(std::shared_ptr<ControllerRouteHookCell> hooks={}):hooks_(std::move(hooks)){}
    void invoke(std::function<void()>&& job)override {std::lock_guard lock(mutex_);if(stopped_)return;
        if(jobs_.size()>=256)throw db_error("first-claim fixture scheduler capacity");jobs_.push_back(std::move(job));}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{std::lock_guard lock(mutex_);return !stopped_;}
    size_t queued_snapshot()const {std::lock_guard lock(mutex_);return jobs_.size();}
    bool run_one(){std::function<void()> job;{std::lock_guard lock(mutex_);if(jobs_.empty())return false;job=std::move(jobs_.front());jobs_.pop_front();}
        struct Restore{const FirstClaimScheduler* old;~Restore(){current_=old;}} restore{current_};current_=this;
        ControllerRouteCallbackHookScope hook_scope(hooks_);job();return true;}
    void discard(){std::deque<std::function<void()>> old;{std::lock_guard lock(mutex_);old.swap(jobs_);}}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard lock(mutex_);stopped_=true;old.swap(jobs_);}}
};
thread_local const FirstClaimScheduler* FirstClaimScheduler::current_=nullptr;
struct FirstClaimAckGate {
    std::mutex mutex;std::condition_variable changed;bool released=false,timed_out=false;
    std::atomic<unsigned> started{0},finished{0};
    void wait(){++started;std::unique_lock lock(mutex);changed.notify_all();
        if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
    bool timedOut(){std::lock_guard lock(mutex);return timed_out;}
};
struct FirstClaimWriterHold {
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false,timed_out=false,admission_failed=false;std::thread worker;
    explicit FirstClaimWriterHold(lattice_db& owner){auto* db=detail::canonical_writer_custody_test_access::fault_handle(owner.db());
        worker=std::thread([this,db]{auto* m=sqlite3_db_mutex(db);const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
            while(m&&sqlite3_mutex_try(m)!=SQLITE_OK){if(std::chrono::steady_clock::now()>=deadline){m=nullptr;break;}std::this_thread::sleep_for(std::chrono::milliseconds(1));}
            if(!m){std::lock_guard lock(mutex);admission_failed=true;changed.notify_all();return;}
            {std::unique_lock lock(mutex);entered=true;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
            sqlite3_mutex_leave(m);});
        std::unique_lock lock(mutex);if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return entered||admission_failed;})||!entered){
            released=true;changed.notify_all();lock.unlock();worker.join();throw db_error("first-claim fixture writer admission timeout");}}
    void release(){{std::lock_guard lock(mutex);released=true;changed.notify_all();}if(worker.joinable())worker.join();}
    ~FirstClaimWriterHold(){release();}
};
thread_local std::function<void()> first_claim_committed_action;
void first_claim_committed(){if(first_claim_committed_action)first_claim_committed_action();}
// Install only around the real scheduler callback that executes the claim.
// Construct replacement captures before touching the old TLS, and restore on
// both normal return and exception after that callback's SQL has unwound.
struct FirstClaimRouteHookScope {
    void (*&slot)(); std::function<void()>& action;
    void (*previous)(); std::function<void()> previous_action;
    FirstClaimRouteHookScope(void (*&selected)(),std::function<void()>& selected_action,
        void (*callback)(),std::function<void()> replacement)
        :slot(selected),action(selected_action),previous(selected),previous_action(std::move(selected_action)) {
        action=std::move(replacement);slot=callback;
    }
    ~FirstClaimRouteHookScope(){slot=previous;action=std::move(previous_action);}
    FirstClaimRouteHookScope(const FirstClaimRouteHookScope&)=delete;
    FirstClaimRouteHookScope& operator=(const FirstClaimRouteHookScope&)=delete;
};
class RecoveryRestrictedFirstClaim : public RecoveryReceiverController {
protected:
    std::shared_ptr<FirstClaimScheduler> manual=std::make_shared<FirstClaimScheduler>(callback_hooks);
    std::vector<std::shared_ptr<FirstClaimAckGate>> ack_gates;
    std::shared_ptr<FirstClaimAckGate> restricted_ack;
    std::shared_ptr<const detail::sync_background_test_hooks::ack_schedule> previous_ack;
    void(*previous_claim)()=nullptr;
    std::function<void()> previous_claim_action;
    std::unique_ptr<FirstClaimWriterHold> writer_hold;
    std::vector<std::string> mixed_ids;
    std::vector<std::vector<database::row_t>> committed_claims;
    std::vector<int64_t> committed_sequences;
    Snapshot phase4_snapshot,committed_snapshot;
    size_t restricted_begin=0;std::atomic<size_t> phase4_publications{0};bool park_first=false;
    const std::thread::id claim_route_thread=std::this_thread::get_id();
    // Capture before descriptor visibility, never after a route can claim.
    // The separate actual after-publication observer counts real notifications.
    // One pending DATA snapshot is moved to main; it grants no SQL authority.
    std::mutex phase4_observation_mutex;
    std::unique_ptr<Snapshot> pending_phase4_snapshot;
    bool phase4_snapshot_prepared=false;
    bool first_claim_capture_enabled=false; // Main-only mode chosen before any actual route.
    void publish_phase4_observation() {
        auto actual=std::make_unique<Snapshot>(snapshot());
        std::unique_ptr<Snapshot> displaced;
        {std::lock_guard lock(phase4_observation_mutex);
            displaced=std::move(pending_phase4_snapshot);pending_phase4_snapshot=std::move(actual);}
        // Displaced DATA destruction never runs beneath the observation leaf.
    }
    void consume_phase4_observation() {
        if(std::this_thread::get_id()!=claim_route_thread)
            throw db_error("first-claim fixture route executor changed thread");
        std::unique_ptr<Snapshot> actual;
        {std::lock_guard lock(phase4_observation_mutex);actual=std::move(pending_phase4_snapshot);}
        if(actual){phase4_snapshot.swap(*actual);phase4_snapshot_prepared=true;}
    }
    virtual void claim_committed_on_route() {
        committed_claims.push_back(claims());committed_sequences.push_back(scalar(*receiver,"SELECT export_sequence AS n FROM _lattice_obligation_store"));
        if(committed_claims.size()==1){committed_snapshot=snapshot();if(park_first)writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);}
    }
    virtual bool run_claim_queue_body(FirstClaimScheduler& queue){return queue.run_one();}
    bool run_claim_queue(FirstClaimScheduler& queue) {
        consume_phase4_observation();
        // Install on EVERY real job, including one already queued before the
        // descriptor was published. Eligibility is never notification arrival.
        FirstClaimRouteHookScope hook(detail::recovery_export_test_hooks::after_claim_commit,
            first_claim_committed_action,first_claim_committed,[this]{
                if(!first_claim_capture_enabled)return;
                consume_phase4_observation();
                if(phase()==4){
                    if(!phase4_snapshot_prepared)throw db_error("restricted claim lacks actual prepublication snapshot");
                    claim_committed_on_route();
                }
            });
        return run_claim_queue_body(queue);
    }
    void SetUp()override{RecoveryReceiverController::SetUp();previous_ack=detail::sync_background_test_hooks::ack;
        previous_claim=detail::recovery_export_test_hooks::after_claim_commit;previous_claim_action=std::move(first_claim_committed_action);}
    std::shared_ptr<FirstClaimAckGate> install_ack_gate(){auto gate=std::make_shared<FirstClaimAckGate>();ack_gates.push_back(gate);
        auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();hook->before_expiry=[gate]{gate->wait();};
        hook->completed=[gate]{++gate->finished;};detail::sync_background_test_hooks::ack=std::move(hook);return gate;}
    // A real claim may beat the later after-publication DATA observer. Keep
    // that observer/count real, and finish the SAME original drive only when
    // both its original predicate and this required observation are present.
    bool claim_publication_observed()const noexcept {
        return !first_claim_capture_enabled||committed_claims.empty()||
            phase4_publications.load(std::memory_order_acquire)!=0;
    }
    template<class F> bool drive(F predicate){const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate()&&claim_publication_observed())return true;run_claim_queue(*manual);if(predicate()&&claim_publication_observed())return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}return predicate()&&claim_publication_observed();}
    template<class F> bool observe(F predicate){const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){if(predicate())return true;std::this_thread::sleep_for(std::chrono::milliseconds(1));}return predicate();}
    void open_manual_receiver(){swift_configuration config((container/"store.sqlite").string(),manual);config.audit_retention_seconds=0;config.busy_timeout_ms=100;
        continuous_result result;
#if LATTICE_HAS_FRT
        receiver_ref.reset(swift_lattice_ref::create_continuous(config,{controller_schema()},policy,result));
#else
        receiver_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create_continuous(config,{controller_schema()},policy,result));
#endif
        if(result.phase()!=2||result.has_error())throw db_error("first-claim actual continuous factory failed");
        receiver=swift_lattice_ref::shared_for_lattice(receiver_ref->get());ASSERT_TRUE(receiver);
        if(auto* notifier=instance_registry::instance().get_or_create_notifier(receiver->config().path))notifier->stop_listening();}
    std::vector<database::row_t> claims(){return receiver->db().query("SELECT channel,original,first_export FROM _lattice_obligation_entry ORDER BY channel,original");}
    int64_t first_claim(const std::string& channel,const std::string& original){const auto rows=receiver->db().query(
        "SELECT first_export AS n FROM _lattice_obligation_entry WHERE channel=CAST(? AS BLOB) AND actual_original=CAST(? AS BLOB)",{channel,original});
        if(rows.size()!=1)throw db_error("first-claim fixture original missing");return std::get<int64_t>(rows.front().at("n"));}
    Snapshot snapshot(){auto result=RecoveryReceiverController::snapshot();
        const auto tables=receiver->db().query("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_lattice_*' ORDER BY name LIMIT 129");
        if(tables.size()>128)throw db_error("first-claim fixture state-table bound");
        for(const auto& row:tables){const auto& name=std::get<std::string>(row.at("name"));
            if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](char c){return (c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_';}))
                throw db_error("first-claim fixture state-table identity");
            result[name]=receiver->db().query("SELECT * FROM \""+name+"\"");}
        result["sqlite_schema"]=receiver->db().query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");return result;}
    void prepare_mixed(){
        first_claim_capture_enabled=true;
        configure(2,false);open_manual_receiver();const auto initial_ack=install_ack_gate();connect();
        ASSERT_TRUE(drive([&]{return phase()==0;}));seed_local(1,700);const auto claimed=originals();ASSERT_EQ(claimed.size(),1u);
        ASSERT_TRUE(drive([&]{return held_originals(0)==claimed&&held_originals(1)==claimed;}));
        ASSERT_TRUE(observe([&]{return initial_ack->started.load()==2;}));
        // Both real sends are UNKNOWN at the source. Retire volatile route
        // ownership, preserve the same actual database and its durable claims.
        // The real destructor shuts down a borrowed scheduler off-thread.
        // Retire on this actual scheduler; never clear/rearm its stopped bit.
        const auto retired=std::make_shared<std::atomic<bool>>(false);
        manual->invoke([this,retired]{synchronizers.clear();retired->store(true);});
        ASSERT_TRUE(drive([&]{return retired->load();}));ASSERT_TRUE(manual->can_invoke());
        manual->discard();initial_ack->release();
        ASSERT_TRUE(observe([&]{return initial_ack->finished.load()==2;}));
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL"),2);
        seed_local(1,701);mixed_ids=originals();ASSERT_EQ(mixed_ids.size(),2u);
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NULL"),2);
        ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
        held_uploads.clear();restricted_begin=observed_uploads.size();restricted_ack=install_ack_gate();
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this](const char* stage){
            if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4)++phase4_publications;
        },nullptr, nullptr, nullptr,[this]{publish_phase4_observation();});
        connect();
    }
    void release_controller_worker_holds() override {
        if(writer_hold)writer_hold->release();
        for(const auto& gate:ack_gates)gate->release();
        if(manual)manual->shutdown();
        RecoveryReceiverController::release_controller_worker_holds();
    }
    void TearDown()override{
        if(writer_hold){writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);writer_hold.reset();}
        synchronizers.clear();manual->shutdown();for(const auto& gate:ack_gates)gate->release();
        for(const auto& gate:ack_gates){EXPECT_TRUE(observe([&]{return gate->finished.load()==gate->started.load();}));EXPECT_FALSE(gate->timedOut());}
        detail::recovery_export_test_hooks::after_claim_commit=previous_claim;first_claim_committed_action=std::move(previous_claim_action);
        detail::sync_background_test_hooks::ack=previous_ack;probe.reset();RecoveryReceiverController::TearDown();
    }
};
TEST_F(RecoveryRestrictedFirstClaim, EarlierActualRouteClaimSurvivesLaterSameDescriptorSharedOriginalHandoff) {
    ASSERT_NO_FATAL_FAILURE(prepare_mixed());
    ASSERT_TRUE(drive([&]{return observed_originals(0,restricted_begin)==mixed_ids&&observed_originals(1,restricted_begin)==mixed_ids;}));
    ASSERT_EQ(phase4_publications.load(std::memory_order_acquire),1u);ASSERT_EQ(committed_claims.size(),2u);ASSERT_EQ(committed_sequences.size(),2u);
    ASSERT_EQ(observed_uploads.size()-restricted_begin,2u);
    EXPECT_NE(observed_uploads[restricted_begin].first,observed_uploads[restricted_begin+1].first);
    // Whichever actual route ran first is A. Its claim covers both matching
    // contributions; B allocates later tickets while preserving those claims.
    EXPECT_EQ(committed_claims[0],committed_claims[1]);EXPECT_GT(committed_sequences[1],committed_sequences[0]);
    for(const auto& peer:peers)EXPECT_LT(first_claim(peer.channel,mixed_ids[1]),committed_sequences[1]);
    size_t absent=0;for(const auto& row:phase4_snapshot.at("_lattice_obligation_entry"))if(std::holds_alternative<std::nullptr_t>(row.at("first_export")))++absent;
    EXPECT_EQ(absent,2u);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog"),phase4_snapshot.at("AuditLog"));
    EXPECT_FALSE(has_error());
}
TEST_F(RecoveryRestrictedFirstClaim, AuditValidMatchingContributionFirstClaimRewriteAfterCommitRefusesBusyHandoff) {
    park_first=true;ASSERT_NO_FATAL_FAILURE(prepare_mixed());
    ASSERT_TRUE(drive([&]{return bool(writer_hold);}));ASSERT_EQ(phase4_publications.load(std::memory_order_acquire),1u);ASSERT_EQ(committed_claims.size(),1u);
    EXPECT_EQ(observed_uploads.size(),restricted_begin);EXPECT_EQ(restricted_ack->started.load(),0u);
    EXPECT_EQ(synchronizers[0]->get_progress().pending_upload+synchronizers[1]->get_progress().pending_upload,2);
    // The run_one that returned has actually parked the committed frame on
    // initial BUSY. No scheduler callback can run between release and fault.
    writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);
    const auto target_channel=peers[1].channel;const auto original=mixed_ids[1];
    const auto before_claim=first_claim(target_channel,original);const auto older_claim=first_claim(target_channel,mixed_ids[0]);
    ASSERT_GT(older_claim,0);ASSERT_LT(older_claim,before_claim);
    unsigned audited=0;
    const auto audit=[&]{return detail::recovery_writer_access::install(receiver,[&](database&){
        const auto inventory=detail::recovery_local_producer_adapter::export_inventory_for_owned_write(receiver);
        detail::recovery_obligation_store journal(receiver,inventory.limits.obligations,inventory.limits.installations);journal.audit();++audited;
    });};
    const auto before_audit=audit();ASSERT_EQ(before_audit.state,detail::recovery_install_state::committed);ASSERT_FALSE(before_audit.primary_error);
    ASSERT_EQ(snapshot(),committed_snapshot);
    // Explicit external corruption fixture. It neither replaces the actual
    // owner's authorizer nor fabricates a continuous claim/controller token.
    // The private scheduler keeps every actual handoff stopped until closure.
    sqlite3* raw_fault=nullptr;
    const auto opened=sqlite3_open_v2(receiver->config().path.c_str(),&raw_fault,SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOMUTEX,nullptr);
    std::unique_ptr<sqlite3,decltype(&sqlite3_close)> fault_db(raw_fault,sqlite3_close);ASSERT_EQ(opened,SQLITE_OK);
    ASSERT_EQ(sqlite3_busy_timeout(fault_db.get(),100),SQLITE_OK);
    sqlite3_stmt* raw_update=nullptr;
    const auto prepared=sqlite3_prepare_v2(fault_db.get(),"UPDATE _lattice_obligation_entry SET first_export=? WHERE channel=? AND actual_original=?",-1,&raw_update,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> update(raw_update,sqlite3_finalize);ASSERT_EQ(prepared,SQLITE_OK);
    ASSERT_EQ(sqlite3_bind_int64(update.get(),1,older_claim),SQLITE_OK);
    ASSERT_EQ(sqlite3_bind_blob(update.get(),2,target_channel.data(),static_cast<int>(target_channel.size()),SQLITE_TRANSIENT),SQLITE_OK);
    ASSERT_EQ(sqlite3_bind_blob(update.get(),3,original.data(),static_cast<int>(original.size()),SQLITE_TRANSIENT),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(update.get()),SQLITE_DONE);ASSERT_EQ(sqlite3_changes(fault_db.get()),1);
    ASSERT_EQ(sqlite3_finalize(update.release()),SQLITE_OK);ASSERT_EQ(sqlite3_close(fault_db.release()),SQLITE_OK);
    const auto after_audit=audit();ASSERT_EQ(after_audit.state,detail::recovery_install_state::committed);ASSERT_FALSE(after_audit.primary_error);ASSERT_EQ(audited,2u);
    auto expected=committed_snapshot;size_t changed=0;
    for(auto& row:expected.at("_lattice_obligation_entry")){
        const auto& channel=std::get<std::vector<uint8_t>>(row.at("channel"));const auto& id=std::get<std::vector<uint8_t>>(row.at("actual_original"));
        if(std::string(channel.begin(),channel.end())==target_channel&&std::string(id.begin(),id.end())==original){row["first_export"]=older_claim;++changed;}}
    ASSERT_EQ(changed,1u);ASSERT_EQ(snapshot(),expected);
    ASSERT_TRUE(drive([&]{return has_error();}));
    {std::lock_guard lock(errors_mutex);EXPECT_TRUE(std::any_of(errors.begin(),errors.end(),[](const auto& error){return error.find("restricted handoff validated first claim changed")!=std::string::npos;}));}
    for(size_t n=0;n<64&&pump();++n){}
    EXPECT_EQ(observed_uploads.size(),restricted_begin);EXPECT_EQ(restricted_ack->started.load(),0u);
    EXPECT_EQ(snapshot(),expected);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
}

// Separate real callback queues let a sibling run before an already parked
// route. The normal factory, controller, claims and queue tickets stay real.
class FirstClaimScheduledSynchronizer final : public synchronizer_base {
public:
    FirstClaimScheduledSynchronizer(std::shared_ptr<lattice_db> owner,const sync_config& config,
        std::shared_ptr<scheduler> scheduled){owned_db_=std::move(owner);db_ptr_=owned_db_.get();init_sync(config,std::move(scheduled));}
};
thread_local std::function<void()> continuity_before_claim_action;
void continuity_before_claim(){if(continuity_before_claim_action)continuity_before_claim_action();}
class RecoveryFirstClaimContinuity : public RecoveryRestrictedFirstClaim {
protected:
    std::array<std::shared_ptr<FirstClaimScheduler>,2> route_queues{std::make_shared<FirstClaimScheduler>(),std::make_shared<FirstClaimScheduler>()};
    std::vector<std::unique_ptr<FirstClaimScheduledSynchronizer>> routes;
    std::array<std::vector<std::string>,2> route_errors;
    std::array<size_t,2> executed{};
    int running_route=-1,first_route=-1;
    size_t park_commits=1;
    std::function<void()> before_claim,before_refreeze;
    void(*prior_before)()=nullptr;
    std::function<void()> prior_before_action;
    void SetUp()override{RecoveryRestrictedFirstClaim::SetUp();prior_before=detail::recovery_export_test_hooks::before_claim_commit;
        prior_before_action=std::move(continuity_before_claim_action);}
    bool route_error(size_t n){std::lock_guard lock(errors_mutex);return !route_errors[n].empty();}
    void claim_committed_on_route() override {
        if(running_route<0)throw db_error("continuity fixture claim did not run on actual route queue");
        if(first_route<0)first_route=running_route;
        committed_claims.push_back(claims());committed_sequences.push_back(scalar(*receiver,"SELECT export_sequence AS n FROM _lattice_obligation_store"));
        committed_snapshot=snapshot();
        if(committed_claims.size()<=park_commits)writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);
    }
    bool run_claim_queue_body(FirstClaimScheduler& queue) override {
        FirstClaimRouteHookScope hook(detail::recovery_export_test_hooks::before_claim_commit,
            continuity_before_claim_action,continuity_before_claim,[this]{
                if(!first_claim_capture_enabled)return;
                consume_phase4_observation();
                if(phase()==4){
                    if(!phase4_snapshot_prepared)throw db_error("restricted pre-COMMIT lacks actual prepublication snapshot");
                    if(before_claim)before_claim();
                }
            });
        return queue.run_one();
    }
    bool selected(const std::function<bool()>& predicate,std::optional<size_t> only={}){
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate()&&claim_publication_observed())return true;run_claim_queue(*manual);if(predicate()&&claim_publication_observed())return true;
            for(size_t n=0;n<2;++n)if(!only||*only==n){
                struct Restore{int& slot;int prior;~Restore(){slot=prior;}} restore{running_route,running_route};running_route=static_cast<int>(n);
                if(run_claim_queue(*route_queues[n]))++executed[n];if(predicate()&&claim_publication_observed())return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}
        return predicate()&&claim_publication_observed();
    }
    void start_mixed(){
        first_claim_capture_enabled=true;
        configure(2,false);open_manual_receiver();const auto initial_ack=install_ack_gate();
        const auto startup=std::make_shared<test_observation::StartupPredicateObservation>();
        auto startup_probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
            [startup](const char* stage){startup->observe(stage);});
        connect();
        ASSERT_TRUE(startup->result(drive([&]{return phase()==0;}),"continuity-initial-phase0",*receiver));seed_local(1,700);const auto claimed=originals();ASSERT_EQ(claimed.size(),1u);
        ASSERT_TRUE(startup->result(drive([&]{return held_originals(0)==claimed&&held_originals(1)==claimed;}),"continuity-initial-upload",*receiver));
        startup_probe.reset();
        ASSERT_TRUE(observe([&]{return initial_ack->started.load()==2;}));
        auto retired=std::make_shared<std::atomic<bool>>(false);manual->invoke([this,retired]{synchronizers.clear();retired->store(true);});
        ASSERT_TRUE(drive([&]{return retired->load();}));ASSERT_TRUE(manual->can_invoke());manual->discard();
        initial_ack->release();ASSERT_TRUE(observe([&]{return initial_ack->finished.load()==2;}));
        seed_local(1,701);mixed_ids=originals();ASSERT_EQ(mixed_ids.size(),2u);
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NULL"),2);
        held_uploads.clear();restricted_begin=observed_uploads.size();restricted_ack=install_ack_gate();
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this](const char* stage){
            if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4)++phase4_publications;
        },[this](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"reconcile-refreeze")==0){
                // This scope is reached by actual route work->step, not by the
                // private controller turn. Refuse any different executor before
                // reading a body-local callback or its plain DATA captures.
                if(std::this_thread::get_id()!=claim_route_thread||running_route<0||running_route>=2||
                    !route_queues[static_cast<size_t>(running_route)]->is_on_thread())
                    throw db_error("continuity refreeze did not run on its actual selected route");
                if(before_refreeze)before_refreeze();
            }
            return {};
        },nullptr, nullptr,[this]{publish_phase4_observation();});
        for(size_t n=0;n<2;++n){const auto& peer=peers[n];sync_config config;config.websocket_url=peer.endpoint;
            config.authorization_token="registered-token";config.sync_id=peer.channel;
            for(const auto& active:peers)config.all_active_sync_ids.push_back(active.channel);
            config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;config.chunk_size=upload_chunk;
            auto route=std::make_unique<FirstClaimScheduledSynchronizer>(receiver,config,route_queues[n]);
            route->set_on_error([this,n](const std::string& error){std::lock_guard lock(errors_mutex);errors.push_back(error);route_errors[n].push_back(error);});
            routes.push_back(std::move(route));routes.back()->connect();
        }
    }
    void release_writer(){ASSERT_TRUE(writer_hold);writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);writer_hold.reset();}
    void rewrite(size_t peer,int64_t value){
        sqlite3* raw=nullptr;const auto opened=sqlite3_open_v2(receiver->config().path.c_str(),&raw,SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOMUTEX,nullptr);
        std::unique_ptr<sqlite3,decltype(&sqlite3_close)> db(raw,sqlite3_close);ASSERT_EQ(opened,SQLITE_OK);ASSERT_EQ(sqlite3_busy_timeout(db.get(),100),SQLITE_OK);
        sqlite3_stmt* raw_statement=nullptr;const auto prepared=sqlite3_prepare_v2(db.get(),"UPDATE _lattice_obligation_entry SET first_export=? WHERE channel=? AND actual_original=?",-1,&raw_statement,nullptr);
        std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw_statement,sqlite3_finalize);ASSERT_EQ(prepared,SQLITE_OK);
        const auto& channel=peers[peer].channel;const auto& original=mixed_ids[1];
        ASSERT_EQ(sqlite3_bind_int64(statement.get(),1,value),SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_blob(statement.get(),2,channel.data(),static_cast<int>(channel.size()),SQLITE_TRANSIENT),SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_blob(statement.get(),3,original.data(),static_cast<int>(original.size()),SQLITE_TRANSIENT),SQLITE_OK);
        ASSERT_EQ(sqlite3_step(statement.get()),SQLITE_DONE);ASSERT_EQ(sqlite3_changes(db.get()),1);
        ASSERT_EQ(sqlite3_finalize(statement.release()),SQLITE_OK);ASSERT_EQ(sqlite3_close(db.release()),SQLITE_OK);
        const auto audited=detail::recovery_writer_access::install(receiver,[&](database&){
            const auto inventory=detail::recovery_local_producer_adapter::export_inventory_for_owned_write(receiver);
            detail::recovery_obligation_store journal(receiver,inventory.limits.obligations,inventory.limits.installations);journal.audit();});
        ASSERT_EQ(audited.state,detail::recovery_install_state::committed);ASSERT_FALSE(audited.primary_error);
    }
    void no_new_send(const Snapshot& expected){
        EXPECT_EQ(observed_uploads.size(),restricted_begin);EXPECT_EQ(restricted_ack->started.load(),0u);EXPECT_EQ(snapshot(),expected);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
    }
    void release_controller_worker_holds() override {
        if(writer_hold)writer_hold->release();
        routes.clear();for(const auto& queue:route_queues)queue->shutdown();
        RecoveryRestrictedFirstClaim::release_controller_worker_holds();
    }
    void TearDown()override{
        before_claim={};before_refreeze={};
        if(writer_hold)writer_hold->release();routes.clear();for(const auto& queue:route_queues)queue->shutdown();
        detail::recovery_export_test_hooks::before_claim_commit=prior_before;continuity_before_claim_action=std::move(prior_before_action);
        RecoveryRestrictedFirstClaim::TearDown();
    }
};
TEST_F(RecoveryFirstClaimContinuity, QueuedSiblingPreparesFirstAndRefusesAuditValidRewriteBeforeNewClaims) {
    ASSERT_NO_FATAL_FAILURE(start_mixed());ASSERT_TRUE(selected([&]{return bool(writer_hold);}));
    ASSERT_GE(first_route,0);const size_t a=static_cast<size_t>(first_route),b=1-a;const auto a_executed=executed[a];
    ASSERT_EQ(committed_claims.size(),1u);ASSERT_NO_FATAL_FAILURE(release_writer());
    const auto older=first_claim(peers[b].channel,mixed_ids[0]);ASSERT_LT(older,first_claim(peers[b].channel,mixed_ids[1]));
    ASSERT_NO_FATAL_FAILURE(rewrite(b,older));const auto expected=snapshot();hold_uploads=false;
    ASSERT_TRUE(selected([&]{return route_error(b);},b));EXPECT_EQ(executed[a],a_executed);EXPECT_EQ(committed_claims.size(),1u);
    no_new_send(expected);ASSERT_TRUE(selected([&]{return route_error(a);},a));no_new_send(expected);
}
TEST_F(RecoveryFirstClaimContinuity, QueuedSiblingPreservesEarlierActualClaimsAndBothRoutesSend) {
    ASSERT_NO_FATAL_FAILURE(start_mixed());ASSERT_TRUE(selected([&]{return bool(writer_hold);}));
    ASSERT_GE(first_route,0);const size_t a=static_cast<size_t>(first_route),b=1-a;const auto a_executed=executed[a];
    const auto earlier=committed_claims.at(0);ASSERT_NO_FATAL_FAILURE(release_writer());
    ASSERT_TRUE(selected([&]{return observed_originals(b,restricted_begin)==mixed_ids;},b));EXPECT_EQ(executed[a],a_executed);
    ASSERT_EQ(committed_claims.size(),2u);EXPECT_EQ(committed_claims[1],earlier);EXPECT_GT(committed_sequences[1],committed_sequences[0]);
    ASSERT_TRUE(selected([&]{return observed_originals(a,restricted_begin)==mixed_ids;},a));
    ASSERT_EQ(observed_uploads.size(),restricted_begin+2);EXPECT_EQ(claims(),earlier);
    EXPECT_FALSE(has_error());EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
}
TEST_F(RecoveryFirstClaimContinuity, FailedRouteFencesAlreadyClaimedSiblingEvenAfterExternalValueRestored) {
    park_commits=2;ASSERT_NO_FATAL_FAILURE(start_mixed());ASSERT_TRUE(selected([&]{return bool(writer_hold);}));
    ASSERT_GE(first_route,0);const size_t a=static_cast<size_t>(first_route),b=1-a;ASSERT_NO_FATAL_FAILURE(release_writer());
    ASSERT_TRUE(selected([&]{return committed_claims.size()==2&&bool(writer_hold);},b));ASSERT_NO_FATAL_FAILURE(release_writer());
    const auto unchanged=committed_snapshot;const auto exact=first_claim(peers[b].channel,mixed_ids[1]);
    const auto older=first_claim(peers[b].channel,mixed_ids[0]);ASSERT_LT(older,exact);ASSERT_NO_FATAL_FAILURE(rewrite(b,older));
    hold_uploads=false;ASSERT_TRUE(selected([&]{return route_error(a);},a));
    ASSERT_NO_FATAL_FAILURE(rewrite(b,exact));ASSERT_EQ(snapshot(),unchanged);
    ASSERT_TRUE(selected([&]{return route_error(b);},b));no_new_send(unchanged);EXPECT_EQ(committed_claims.size(),2u);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(route_errors[a].empty());ASSERT_FALSE(route_errors[b].empty());EXPECT_EQ(route_errors[a].front(),route_errors[b].front());}
}
TEST_F(RecoveryFirstClaimContinuity, DeniedOwnedClaimCommitNeverPublishesCandidateToQueuedSibling) {
    park_commits=0;std::atomic<unsigned> denied{0};std::unique_ptr<ControllerCommitFault> fault;
    auto controller_local_worker_ending=scoped_controller_worker_ending();before_claim=[&]{if(!fault)fault=std::make_unique<ControllerCommitFault>(receiver.get(),denied);};
    ASSERT_NO_FATAL_FAILURE(start_mixed());hold_uploads=false;ASSERT_TRUE(selected([&]{return has_error();}));
    fault.reset();ASSERT_GT(denied.load(),0u);EXPECT_TRUE(committed_claims.empty());no_new_send(phase4_snapshot);
    ASSERT_TRUE(selected([&]{return route_error(0)&&route_error(1);}));no_new_send(phase4_snapshot);
}
TEST_F(RecoveryFirstClaimContinuity, ReentrantBodyCommitCannotPublishOrPermitSiblingSourceMutation) {
    park_commits=0;unsigned attempted=0;auto controller_local_worker_ending=scoped_controller_worker_ending();before_claim=[&]{++attempted;receiver->db().execute("COMMIT");};
    ASSERT_NO_FATAL_FAILURE(start_mixed());hold_uploads=false;ASSERT_TRUE(selected([&]{return has_error();}));
    EXPECT_EQ(attempted,1u);EXPECT_TRUE(committed_claims.empty());EXPECT_FALSE(receiver->db().is_in_transaction());no_new_send(phase4_snapshot);
    ASSERT_TRUE(selected([&]{return route_error(0)&&route_error(1);}));no_new_send(phase4_snapshot);
}
TEST_F(RecoveryFirstClaimContinuity, RefreezeRejectsAuditValidRewriteAfterBothActualHandoffs) {
    park_commits=0;bool corrupted=false;Snapshot expected;
    uint64_t before_refreeze_calls=0;bool startup_completed=false,selected_completed=false;
    const auto report=[&]()noexcept {
        if(selected_completed)return;
        // This local guard runs only after an original fatal assertion or
        // exception has finalized. It never evaluates the predicate again.
        std::array<unsigned char,512> first_error{};size_t copied=0,total_bytes=0,error_count=0;
        size_t errors_a=0,errors_b=0;bool error_copy_known=false;
        try {std::lock_guard lock(errors_mutex);error_count=errors.size();errors_a=route_errors[0].size();errors_b=route_errors[1].size();
            if(!errors.empty()){total_bytes=errors.front().size();copied=std::min(total_bytes,first_error.size());
                if(copied)std::memcpy(first_error.data(),errors.front().data(),copied);}error_copy_known=true;
        }catch(...){}
        std::fprintf(stderr,"refreeze-predicate POST_ORIGINAL_FAILURE startupCompleted=%d selectedCompleted=%d exceptionUnwinding=%d corrupted=%d actualBeforeRefreezeCalls=%llu claimsCommitted=%zu sendsObserved=%zu restrictedBegin=%zu route0Executed=%zu route1Executed=%zu firstRoute=%d errorsKnown=%d errors=%zu route0Errors=%zu route1Errors=%zu firstErrorBytes=%zu firstErrorCopied=%zu firstErrorHex=",
            int(startup_completed),int(selected_completed),int(std::uncaught_exceptions()!=0),int(corrupted),static_cast<unsigned long long>(before_refreeze_calls),
            committed_claims.size(),observed_uploads.size(),restricted_begin,executed[0],executed[1],first_route,
            int(error_copy_known),error_count,errors_a,errors_b,total_bytes,copied);
        for(size_t i=0;i<copied;++i)std::fprintf(stderr,"%02x",static_cast<unsigned>(first_error[i]));std::fputc('\n',stderr);
        try {const auto fresh_phase=phase();
            const auto unclaimed=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL");
            const auto claimed=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL");
            const auto retained=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request");
            std::fprintf(stderr,"refreeze-predicate FRESH_POST_FAILURE phase=%lld openUnclaimed=%lld openClaimed=%lld retainedRequests=%lld\n",
                static_cast<long long>(fresh_phase),static_cast<long long>(unclaimed),static_cast<long long>(claimed),static_cast<long long>(retained));
        }catch(const std::exception& error){const char* text=error.what();std::fputs("refreeze-predicate post-read-errorHex=",stderr);
            if(!text)std::fputs("[null-what]",stderr);else for(size_t i=0;i<512&&text[i];++i)std::fprintf(stderr,"%02x",static_cast<unsigned>(static_cast<unsigned char>(text[i])));std::fputc('\n',stderr);
        }catch(...){std::fputs("refreeze-predicate post-read-error=[nonstd]\n",stderr);}
    };
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    struct ReportAfterOriginalFailure {const decltype(report)& action;~ReportAfterOriginalFailure()noexcept{action();}} observed{report};
    before_refreeze=[&]{if(before_refreeze_calls!=UINT64_MAX)++before_refreeze_calls;if(corrupted)return;corrupted=true;
        for(size_t n=0;n<64&&observed_uploads.size()<restricted_begin+2&&pump();++n){}
        EXPECT_EQ(observed_uploads.size(),restricted_begin+2);
        const auto older=first_claim(peers[1].channel,mixed_ids[0]);EXPECT_LT(older,first_claim(peers[1].channel,mixed_ids[1]));
        rewrite(1,older);expected=snapshot();};
    ASSERT_NO_FATAL_FAILURE(start_mixed());startup_completed=true;ASSERT_TRUE(selected([&]{return corrupted&&has_error();}));selected_completed=true;
    EXPECT_EQ(committed_claims.size(),2u);EXPECT_EQ(observed_uploads.size(),restricted_begin+2);EXPECT_EQ(phase(),4);
    EXPECT_EQ(snapshot(),expected);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    {std::lock_guard lock(errors_mutex);EXPECT_TRUE(std::any_of(errors.begin(),errors.end(),[](const auto& error){return error.find("first claim changed")!=std::string::npos;}));}
}

TEST_F(RecoveryReceiverController, LateClaimedAckWhileFrozenPreservesBothCanonicalNamespaces) {
    configure(2);auto armed=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,
        [armed](const char* stage)->std::shared_ptr<void>{if(armed->load()&&std::strcmp(stage,"reconcile-cancel")==0)throw db_error("fixture late ACK frozen checkpoint");return {};});
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    seed_local(1,500);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(until([&]{return held_originals(0)==ids&&held_originals(1)==ids;}));
    armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1"),2);
    // Both retained snapshots truthfully predate source acceptance. This tests
    // delivery ACK orthogonality, not a new B snapshot after A's receipt exists.
    auto frame=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& f){return f.endpoint.matches(peers[0].physical);});ASSERT_NE(frame,held_uploads.end());
    auto accepted=peers[0].setup.receive(frame->raw);ASSERT_EQ(accepted.status_code(),1);EXPECT_EQ(accepted.take_ids(),ids);
    const auto canonical=[this]{auto value=snapshot();for(const auto* table:{"_lattice_receive_guard","_lattice_receive_guard_store","_lattice_replication_slots"})value[table]=receiver->db().query(std::string("SELECT * FROM ")+table);return value;};
    const auto before=canonical();
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL"),2);
    ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,1);
    struct Observed {Snapshot state; synchronizer::sync_progress progress;std::atomic<bool> done{false};};auto observed=std::make_shared<Observed>();
    synchronizers[0]->set_on_sync_complete([this,ids,canonical,observed](const auto& actual){if(actual!=ids||observed->done.load())return;observed->state=canonical();observed->progress=synchronizers[0]->get_progress();observed->done.store(true);});
    legacy_ack(0,ids);ASSERT_TRUE(until([&]{return observed->done.load();}));EXPECT_EQ(observed->state,before);EXPECT_EQ(observed->progress.pending_upload,0);EXPECT_EQ(observed->progress.acked,1);
    const auto state=receiver->db().query("SELECT sync_id,is_synchronized FROM _lattice_sync_state WHERE audit_entry_id=(SELECT id FROM AuditLog LIMIT 1) ORDER BY sync_id");
    ASSERT_EQ(state.size(),1u);EXPECT_EQ(std::get<std::string>(state[0].at("sync_id")),peers[0].channel);EXPECT_EQ(std::get<int64_t>(state[0].at("is_synchronized")),1);
    EXPECT_EQ(synchronizers[1]->get_progress().pending_upload,1);EXPECT_EQ(phase(),2);
    EXPECT_THROW(insert(*receiver,controller_uuid(501),"closed after delivery only"),db_error);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());for(const auto& error:errors)EXPECT_NE(error.find("fixture late ACK frozen checkpoint"),std::string::npos);}
}

TEST_F(RecoveryReceiverController, LateClaimedAckWhileInstalledPreservesExactPositiveReceiptAndGuard) {
    configure();auto armed=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [armed](const char* stage){if(armed->load()&&std::strcmp(stage,"install-committed")==0)throw db_error("fixture late ACK installed checkpoint");},
        [armed](const char* stage)->std::shared_ptr<void>{if(armed->load()&&std::strcmp(stage,"resume")==0)throw db_error("fixture late ACK installed checkpoint");return {};});
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    seed_local(1,510);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));ASSERT_EQ(held_uploads.size(),1u);
    auto accepted=peers[0].setup.receive(held_uploads[0].raw);ASSERT_EQ(accepted.status_code(),1);EXPECT_EQ(accepted.take_ids(),ids);
    armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),3);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2 AND first_export IS NOT NULL AND ack_position IS NOT NULL"),1);
    const auto canonical=[this]{auto value=snapshot();for(const auto* table:{"_lattice_receive_guard","_lattice_receive_guard_store","_lattice_replication_slots"})value[table]=receiver->db().query(std::string("SELECT * FROM ")+table);return value;};
    const auto before=canonical();const auto delivery=receiver->db().query("SELECT * FROM _lattice_sync_state");ASSERT_EQ(delivery.size(),1u);
    struct Observed {Snapshot state;synchronizer::sync_progress progress;std::atomic<bool> done{false};};auto observed=std::make_shared<Observed>();
    synchronizers[0]->set_on_sync_complete([this,ids,canonical,observed](const auto& actual){if(actual!=ids||observed->done.load())return;observed->state=canonical();observed->progress=synchronizers[0]->get_progress();observed->done.store(true);});
    legacy_ack(0,ids);ASSERT_TRUE(until([&]{return observed->done.load();}));EXPECT_EQ(observed->state,before);EXPECT_EQ(observed->progress.pending_upload,0);EXPECT_EQ(observed->progress.acked,1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_sync_state"),delivery);EXPECT_EQ(phase(),3);
    EXPECT_THROW(insert(*receiver,controller_uuid(511),"installed still closed"),db_error);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());for(const auto& error:errors)EXPECT_NE(error.find("fixture late ACK installed checkpoint"),std::string::npos);}
}

TEST_F(RecoveryReceiverController, LateClaimedAckAfterResumeDoesNotDowngradeSettledOriginal) {
    configure();connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    seed_local(1,520);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));ASSERT_EQ(held_uploads.size(),1u);
    auto accepted=peers[0].setup.receive(held_uploads[0].raw);ASSERT_EQ(accepted.status_code(),1);EXPECT_EQ(accepted.take_ids(),ids);
    request_recovery();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2")==1;}));
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2 AND first_export IS NOT NULL AND ack_position IS NOT NULL"),1);
    const auto canonical=[this]{auto value=snapshot();for(const auto* table:{"_lattice_receive_guard","_lattice_receive_guard_store","_lattice_replication_slots"})value[table]=receiver->db().query(std::string("SELECT * FROM ")+table);return value;};
    const auto before=canonical();ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,1);
    struct Observed {Snapshot state;synchronizer::sync_progress progress;std::atomic<bool> done{false};};auto observed=std::make_shared<Observed>();
    synchronizers[0]->set_on_sync_complete([this,ids,canonical,observed](const auto& actual){if(actual!=ids||observed->done.load())return;observed->state=canonical();observed->progress=synchronizers[0]->get_progress();observed->done.store(true);});
    legacy_ack(0,ids);ASSERT_TRUE(until([&]{return observed->done.load();}));EXPECT_EQ(observed->state,before);EXPECT_EQ(observed->progress.pending_upload,0);EXPECT_EQ(observed->progress.acked,1);
    EXPECT_EQ(held_originals(),ids);EXPECT_FALSE(has_error());
}

TEST_F(RecoveryReceiverController, RestrictedResendAckAfterRefreezePreservesExistingDeliveryMarkerAndUnknownHistory) {
    configure();auto armed=std::make_shared<std::atomic<bool>>(false);auto saw_unknown=std::make_shared<std::atomic<bool>>(false);
    const auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [armed,saw_unknown,pause](const char* stage){if(armed->load()&&std::strcmp(stage,"reconciliation-pending")==0&&!saw_unknown->exchange(true))pause->wait();},
        [armed,saw_unknown](const char* stage)->std::shared_ptr<void>{if(armed->load()&&saw_unknown->load()&&std::strcmp(stage,"install")==0)throw db_error("fixture late restricted ACK refrozen checkpoint");return {};});
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    seed_local(1,530);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));ASSERT_EQ(held_uploads.size(),1u);
    armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),2);
    const auto frozen_attempt=scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity");
    // The source really accepts only after the retained receipt pages said
    // UNKNOWN. Queue its actual ACK, then release the worker; never wait for
    // an ACK effect while this scheduler is held by the bounded test pause.
    auto accepted=peers[0].setup.receive(held_uploads[0].raw);ASSERT_EQ(accepted.status_code(),1);EXPECT_EQ(accepted.take_ids(),ids);
    legacy_ack(0,ids);pause->release();ASSERT_TRUE(until([&]{return held_originals().size()==2&&has_error();}));ASSERT_EQ(phase(),2);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{ids[0],ids[0]}));
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),frozen_attempt+1);
    ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,1);ASSERT_EQ(synchronizers[0]->get_progress().acked,1);
    const auto delivery=receiver->db().query("SELECT * FROM _lattice_sync_state");ASSERT_EQ(delivery.size(),1u);ASSERT_EQ(std::get<int64_t>(delivery[0].at("is_synchronized")),1);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL AND ack_position IS NULL"),1);
    auto repeated=peers[0].setup.receive(held_uploads[1].raw);ASSERT_EQ(repeated.status_code(),1);EXPECT_EQ(repeated.take_ids(),ids);
    const auto canonical=[this]{auto value=snapshot();for(const auto* table:{"_lattice_receive_guard","_lattice_receive_guard_store","_lattice_replication_slots"})value[table]=receiver->db().query(std::string("SELECT * FROM ")+table);return value;};
    const auto before=canonical();
    struct Observed {Snapshot state;synchronizer::sync_progress progress;std::atomic<bool> done{false};};auto observed=std::make_shared<Observed>();
    synchronizers[0]->set_on_sync_complete([this,ids,canonical,observed](const auto& actual){if(actual!=ids||observed->done.load())return;observed->state=canonical();observed->progress=synchronizers[0]->get_progress();observed->done.store(true);});
    legacy_ack(0,ids);ASSERT_TRUE(until([&]{return observed->done.load();}));EXPECT_EQ(observed->state,before);EXPECT_EQ(observed->progress.pending_upload,0);EXPECT_EQ(observed->progress.acked,2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_sync_state"),delivery);
    EXPECT_THROW(insert(*receiver,controller_uuid(531),"refrozen remains closed"),db_error);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());for(const auto& error:errors)EXPECT_NE(error.find("fixture late restricted ACK refrozen checkpoint"),std::string::npos);}
}

}

namespace lattice::detail {
struct recovery_receiver_cohort_test_access {
    // DATA-only mailbox stimulus/dispatch on this genuinely constructed route.
    static void post_notification_error(synchronizer_base& sync,std::exception_ptr actual){sync.receiver_controller_->post(recovery_receiver_route::notification::error,std::move(actual));}
    static void drain_notification_data(synchronizer_base& sync){sync.receiver_controller_->drain_notifications();}
    static void next_ack_timeout(synchronizer_base& sync){sync.config_.ack_timeout_base_ms=0;}
    static std::weak_ptr<recovery_receiver_controller> controller(synchronizer_base& sync){return sync.receiver_controller_->controller_;}
    static std::shared_ptr<sync_discovery_operation> retained_busy(synchronizer_base& sync) {
        const auto queue=sync.discovery_deferral_;std::lock_guard lock(queue->mutex_);
        if(!queue->count_||queue->active_)return {};
        const auto& head=queue->slots_[queue->head_];
        if(!head->attempts||head->coalescible.load(std::memory_order_acquire))return {};
        return head;
    }
};
}
namespace {
struct CohortWriterHold {
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false,timed_out=false,admission_failed=false;std::thread worker;
    explicit CohortWriterHold(lattice_db& owner){
        auto* handle=detail::canonical_writer_custody_test_access::fault_handle(owner.db());
        const auto acquire_deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        worker=std::thread([this,handle,acquire_deadline]{auto* sql_mutex=sqlite3_db_mutex(handle);
            while(sql_mutex&&sqlite3_mutex_try(sql_mutex)!=SQLITE_OK){
                if(std::chrono::steady_clock::now()>=acquire_deadline){sql_mutex=nullptr;break;}
                std::this_thread::sleep_for(std::chrono::milliseconds(1));}
            if(!sql_mutex){std::lock_guard lock(mutex);admission_failed=true;changed.notify_all();return;}
            {std::unique_lock lock(mutex);entered=true;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
            sqlite3_mutex_leave(sql_mutex);});
        std::unique_lock lock(mutex);if(!changed.wait_until(lock,acquire_deadline,[&]{return entered||admission_failed;})||!entered){
            released=true;changed.notify_all();lock.unlock();worker.join();throw db_error("cohort writer fixture admission timeout");}
    }
    void release(){ {std::lock_guard lock(mutex);released=true;changed.notify_all();}if(worker.joinable())worker.join();}
    ~CohortWriterHold(){release();}
};
class RecoveryReceiverCohort : public RecoveryReceiverController {
protected:
    void retained_replacement(bool recreate) {
        configure();
        struct Observation {std::mutex mutex;std::shared_ptr<CohortWriterHold> writer;Snapshot claimed;bool closing=false;std::atomic<unsigned> captures{0},waiting{0},restricted{0};std::atomic<bool> armed{false},hooked{false};};
        auto observation=std::make_shared<Observation>();
        struct Release {std::shared_ptr<Observation> value;~Release(){std::shared_ptr<CohortWriterHold> held;{std::lock_guard lock(value->mutex);value->closing=true;held=value->writer;}if(held)held->release();}} release{observation};
        auto timeout_pause=std::make_shared<ControllerPause>();pauses.push_back(timeout_pause);
        auto timeout_seen=std::make_shared<std::atomic<bool>>(false);
        const auto timeout_hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();
        const auto following_timeout=std::make_shared<ControllerPause>();pauses.push_back(following_timeout);
        const auto timeout_workers=std::make_shared<std::atomic<unsigned>>(0);
        // Admit the original first real timeout. Later actual workers stop
        // before clearing their registration; permanent zero timeout must not
        // turn this one-expiry custody control into an unbounded resend loop.
        timeout_hook->before_expiry=[following_timeout,timeout_workers]{if(timeout_workers->fetch_add(1)!=0)following_timeout->wait();};
        timeout_hook->after_timeout_transition=[timeout_pause,timeout_seen]{if(!timeout_seen->exchange(true))timeout_pause->wait();};
        const auto claimed_once=std::make_shared<std::atomic<bool>>(false);
        callback_hooks->publish(timeout_hook);
        // Installed before actual route publication; this is a restriction on
        // the real worker, not a factory for recovery authority.
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this,observation](const char* stage){
            if(std::strcmp(stage,"cohort-reserved")==0)++observation->captures;
            if(std::strcmp(stage,"cohort-retained")==0)++observation->waiting;
            if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4)++observation->restricted;
        },nullptr, nullptr, nullptr,[this,observation,timeout_hook,claimed_once]{
            // Re-publication must not overwrite a still-unconsumed action.
            // Every armed descriptor keeps this SAME one-shot until a real
            // after-COMMIT hook consumes it; later calls cannot renew it.
            if(observation->armed.load()){
                observation->hooked.store(true);
                callback_hooks->publish(timeout_hook,[this,observation,claimed_once]{
                    if(claimed_once->exchange(true))return;
                    std::lock_guard lock(observation->mutex);if(observation->closing)return;
                    observation->claimed=snapshot();observation->writer=std::make_shared<CohortWriterHold>(*receiver);
                });
            }else callback_hooks->publish(timeout_hook);
        });
        connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
        detail::recovery_receiver_cohort_test_access::next_ack_timeout(*synchronizers[0]);
        observation->armed.store(true);seed_local(1,600);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
        ASSERT_TRUE(until([&]{return !held_originals().empty()&&timeout_pause->ready();}));
        ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,0);
        request_recovery();timeout_pause->release();
        std::shared_ptr<detail::sync_discovery_operation> retained;
        ASSERT_TRUE(until([&]{retained=detail::recovery_receiver_cohort_test_access::retained_busy(*synchronizers[0]);return bool(retained)&&observation->restricted.load()!=0;}));
        std::shared_ptr<CohortWriterHold> writer;Snapshot before;
        {std::lock_guard lock(observation->mutex);writer=observation->writer;before=observation->claimed;}
        ASSERT_TRUE(writer);ASSERT_FALSE(before.empty());ASSERT_EQ(observation->restricted.load(),1u);
        const auto captures=observation->captures.load();
        const auto old_controller=detail::recovery_receiver_cohort_test_access::controller(*synchronizers[0]);
        // Real inbound refresh retires the descriptor without needing SQL.
        // Keep the mutex held to prove pending is decided before any new read.
        request_recovery();ASSERT_TRUE(until([&]{return observation->waiting.load()>0;}));
        EXPECT_EQ(observation->captures.load(),captures);EXPECT_FALSE(has_error());
        writer->release();EXPECT_FALSE(writer->timed_out);
        if(recreate){
            // Retire on the real owner scheduler so destruction cannot stop the
            // shared scheduler before the replacement controller is admitted.
            const auto scheduled=receiver->get_scheduler();ASSERT_TRUE(scheduled);
            auto retired=std::make_shared<std::atomic<bool>>(false);
            scheduled->invoke([this,retired]{synchronizers.clear();retired->store(true);});
            ASSERT_TRUE(until([&]{return retired->load()&&old_controller.expired();}));
            ASSERT_TRUE(scheduled->can_invoke());
            const auto waits=observation->waiting.load();connect();
            ASSERT_TRUE(until([&]{return observation->waiting.load()>waits;}));
            const auto current=detail::recovery_receiver_cohort_test_access::controller(*synchronizers[0]);
            EXPECT_FALSE(current.expired());EXPECT_TRUE(old_controller.expired());
        }
        EXPECT_EQ(snapshot(),before);EXPECT_EQ(observation->captures.load(),captures);
        // Queue disposal/cancellation has no right to clear physical custody
        // while this real operation still retains the original committed frame.
        EXPECT_FALSE(has_error());retained.reset();
        ASSERT_TRUE(until([&]{return observation->restricted.load()>=2;}));
        EXPECT_GT(observation->captures.load(),captures);
        EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog"),before.at("AuditLog"));
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL"),1);
    }
};
TEST_F(RecoveryReceiverCohort, ReplacedRequestWaitsForActualRetainedBusyFrameBeforeRecapture) {retained_replacement(false);}
TEST_F(RecoveryReceiverCohort, RecreatedLastControllerSharesActualRetainedBusyFrameCustody) {retained_replacement(true);}
TEST_F(RecoveryReceiverCohort, FailedActualCaptureReleasesPhysicalReservationForFreshRequest) {
    configure();insert(*source,controller_uuid(610),"canonical");auto first=std::make_shared<std::atomic<bool>>(true);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[first](const char* stage){
        if(std::strcmp(stage,"cohort-reserved")==0&&first->exchange(false))throw db_error("fixture cohort capture interrupted");
    });
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=0"),1);
    request_recovery();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());for(const auto& error:errors)EXPECT_NE(error.find("fixture cohort capture interrupted"),std::string::npos);}
}
}

namespace lattice::detail {
struct recovery_delivery_registration_test_access {
    static bool timeout(synchronizer_base& sync,int milliseconds){
        auto done=std::make_shared<std::promise<void>>();auto result=done->get_future();
        sync.schedule_background("delivery identity fixture",[&sync,milliseconds,done]{sync.config_.ack_timeout_base_ms=milliseconds;done->set_value();});
        return result.wait_for(std::chrono::seconds(5))==std::future_status::ready;
    }
    static uint64_t token(synchronizer_base& sync,const std::string& id){
        std::lock_guard lock(sync.in_flight_mutex_);return sync.upload_tracking_->registration_locked(id);
    }
};
}
namespace {
TEST_F(RecoveryReceiverController, AckedOldWorkerCannotReleaseNewRestrictedHandoffOfSameOriginal) {
    configure();auto first_pause=std::make_shared<ControllerPause>();pauses.push_back(first_pause);
    auto started=std::make_shared<std::atomic<unsigned>>(0),completed=std::make_shared<std::atomic<unsigned>>(0);
    auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();
    hook->before_expiry=[first_pause,started]{if(started->fetch_add(1)==0)first_pause->wait();};
    hook->completed=[completed]{++*completed;};callback_hooks->publish(std::move(hook));
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    ASSERT_TRUE(detail::recovery_delivery_registration_test_access::timeout(*synchronizers[0],0));
    seed_local(1,700);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(until([&]{return held_originals()==ids&&first_pause->ready();}));
    const auto first=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]);ASSERT_NE(first,0u);
    ASSERT_TRUE(detail::recovery_delivery_registration_test_access::timeout(*synchronizers[0],10000));
    // A legacy delivery ACK is deliberately not a canonical receipt. The
    // source has not imported this held frame; its real Q reports UNKNOWN.
    legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return held_originals().size()==2&&started->load()==2;}));
    const auto replacement=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]);
    ASSERT_NE(replacement,0u);ASSERT_NE(replacement,first);ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,1);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL AND ack_position IS NULL"),1);
    first_pause->release();ASSERT_TRUE(until([&]{return completed->load()>=1;}));
    EXPECT_EQ(detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]),replacement);
    EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,1);EXPECT_EQ(held_originals().size(),2u);
    // The source UNKNOWN latch remains closed. This correction adds delivery
    // ownership only and does not yet authorize another controller retry.
    EXPECT_THROW(insert(*receiver,controller_uuid(701),"closed pending source outcome"),db_error);
}
TEST(RecoveryDeliveryRegistration, PresendCancellationAndLegacyTimerCannotEraseReplacement) {
    auto state=std::make_shared<detail::sync_upload_tracking>();
    auto old=detail::sync_upload_exclusion::create(state,1,{{"original",7}});const auto first=old->delivery_token();
    {std::lock_guard lock(state->mutex);EXPECT_FALSE(state->matches_locked("original",1,first,true));}
    old->release();
    {std::lock_guard lock(state->mutex);EXPECT_TRUE(state->ids.empty());EXPECT_TRUE(state->delivery_tokens.empty());EXPECT_TRUE(state->pre_handoff.empty());}
    auto current=detail::sync_upload_exclusion::create(state,1,{{"original",7}});const auto second=current->delivery_token();current->handed_off();
    {std::lock_guard lock(state->mutex);EXPECT_NE(first,second);EXPECT_FALSE(state->erase_locked("original",1,first,true));EXPECT_FALSE(state->erase_locked("original",1,0,true));EXPECT_TRUE(state->matches_locked("original",1,second,true));}
    old.reset();current->release();
    {std::lock_guard lock(state->mutex);EXPECT_TRUE(state->matches_locked("original",1,second,true));EXPECT_TRUE(state->erase_locked("original",1,second,false));EXPECT_TRUE(state->delivery_tokens.empty());}
}
TEST(RecoveryDeliveryRegistration, LifecycleClearRetiresTransferredTokensWithoutAffectingNewSend) {
    auto state=std::make_shared<detail::sync_upload_tracking>();auto old=detail::sync_upload_exclusion::create(state,1,{{"original",7}});
    const auto first=old->delivery_token();old->handed_off();
    {std::lock_guard lock(state->mutex);state->clear_ids_locked();state->generation=3;EXPECT_TRUE(state->delivery_tokens.empty());EXPECT_TRUE(state->pre_handoff.empty());}
    auto current=detail::sync_upload_exclusion::create(state,3,{{"original",7}});const auto second=current->delivery_token();current->handed_off();old.reset();
    {std::lock_guard lock(state->mutex);EXPECT_FALSE(state->erase_locked("original",1,first,true));EXPECT_FALSE(state->erase_locked("original",1,0,true));EXPECT_TRUE(state->matches_locked("original",3,second,true));state->clear_ids_locked();EXPECT_TRUE(state->ids.empty());EXPECT_TRUE(state->delivery_tokens.empty());EXPECT_TRUE(state->pre_handoff.empty());}
}
}

namespace {
// Every gate holds only copied test state on a real ACK worker or an existing
// off-lock controller probe. Teardown releases gates before retiring the owner.
struct DeliveryTimeoutGate {
    std::mutex mutex;std::condition_variable changed;bool released=false,timed_out=false;
    std::atomic<unsigned> arrived{0};
    void wait(){std::unique_lock lock(mutex);++arrived;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
    bool timedOut(){std::lock_guard lock(mutex);return timed_out;}
};
struct DeliveryTimeoutReadFault {
    static thread_local DeliveryTimeoutReadFault* active;
    std::atomic<unsigned>& hits;detail::recovery_local_producer_test_hooks::authorizer_fault fault;
    const detail::recovery_local_producer_test_hooks::authorizer_fault* prior;DeliveryTimeoutReadFault* prior_active;
    DeliveryTimeoutReadFault(const lattice_db* owner,std::atomic<unsigned>& count):hits(count),fault{owner,restrict_action},prior(detail::recovery_local_producer_test_hooks::fault),prior_active(active){active=this;detail::recovery_local_producer_test_hooks::fault=&fault;}
    ~DeliveryTimeoutReadFault(){detail::recovery_local_producer_test_hooks::fault=prior;active=prior_active;}
    static int restrict_action(int action,const char* table,const char*,const char*)noexcept{
        if(active&&action==SQLITE_READ&&table&&std::strcmp(table,"_lattice_producer_continuity")==0){++active->hits;return SQLITE_DENY;}return SQLITE_OK;
    }
};
thread_local DeliveryTimeoutReadFault* DeliveryTimeoutReadFault::active=nullptr;
struct DeliveryTimeoutWorkers:std::enable_shared_from_this<DeliveryTimeoutWorkers> {
    std::atomic<unsigned> started{0},completed{0},transitioned{0};
    unsigned first_windows=1;
    std::shared_ptr<DeliveryTimeoutGate> first,following,after;
    std::shared_ptr<const detail::sync_background_test_hooks::ack_schedule> hooks(){
        auto self=shared_from_this();auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();
        hook->before_expiry=[self]{const auto index=self->started.fetch_add(1);const auto gate=index<self->first_windows?self->first:self->following;if(gate)gate->wait();};
        hook->after_timeout_transition=[self]{++self->transitioned;if(self->after)self->after->wait();};
        hook->completed=[self]{++self->completed;};return hook;
    }
};
struct DeliveryTimeoutEvents {
    std::shared_ptr<DeliveryTimeoutWorkers> ordinary=std::make_shared<DeliveryTimeoutWorkers>();
    std::shared_ptr<DeliveryTimeoutWorkers> restricted=std::make_shared<DeliveryTimeoutWorkers>();
    std::atomic<unsigned> admitted{0},pending{0},refrozen{0};
    std::shared_ptr<DeliveryTimeoutGate> admitted_gate;
    std::function<void(const char*)> observed;
    std::function<std::shared_ptr<void>(const char*)> scope;
};
struct DeliveryTimeoutThrowState {std::atomic<bool> enabled{false};std::atomic<unsigned> rejected{0};};
class DeliveryTimeoutThrowNetwork final:public network_factory {
    struct Pipe {std::weak_ptr<ControllerWire> wire;std::shared_ptr<DeliveryTimeoutThrowState> fault;};
    std::shared_ptr<ControllerWire> wire_;std::shared_ptr<DeliveryTimeoutThrowState> fault_;
public:
    DeliveryTimeoutThrowNetwork(std::shared_ptr<ControllerWire> wire,std::shared_ptr<DeliveryTimeoutThrowState> fault):wire_(std::move(wire)),fault_(std::move(fault)){}
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{
        return std::unique_ptr<sync_transport>(make_system_tls_platform_sync_transport(new Pipe{wire_,fault_},
            [](void* p,const void* url,const void*,const void* endpoint){if(auto wire=static_cast<Pipe*>(p)->wire.lock()){std::lock_guard lock(wire->mutex);wire->dials.push_back({*static_cast<const std::string*>(url),*static_cast<const platform_transport_callbacks*>(endpoint)});}},
            [](void*){},
            [](void* p,const void* message,const void* endpoint){auto& pipe=*static_cast<Pipe*>(p);const auto raw=static_cast<const transport_message*>(message)->as_string();
                if(raw.size()>8388608)throw db_error("timeout fixture wire bound");
                const auto value=json::parse(raw);
                if(value.contains("auditLog")&&pipe.fault->enabled.load()){++pipe.fault->rejected;throw db_error("timeout fixture rejects actual restricted send");}
                if(auto wire=pipe.wire.lock()){std::lock_guard lock(wire->mutex);if(wire->frames.size()>=32)throw db_error("timeout fixture queue bound");wire->frames.push_back({*static_cast<const platform_transport_callbacks*>(endpoint),raw});}},
            [](void* p){delete static_cast<Pipe*>(p);},nullptr,[](void*,const void*,const void*)->int32_t{return 1;},[](void*){}));
    }
};
class RecoveryDeliveryTimeout:public RecoveryReceiverController {
protected:
    std::shared_ptr<DeliveryTimeoutEvents> events=std::make_shared<DeliveryTimeoutEvents>();
    std::vector<std::shared_ptr<DeliveryTimeoutGate>> gates;
    std::vector<std::string> sent_ids;
    size_t ordinary_windows=0;
    std::shared_ptr<DeliveryTimeoutGate> gate(){auto value=std::make_shared<DeliveryTimeoutGate>();gates.push_back(value);return value;}
    void start(unsigned count=1,size_t chunk=1000,bool default_timeout=false){
        upload_chunk=chunk;configure();ordinary_windows=(count+chunk-1)/chunk;
        events->ordinary->first_windows=static_cast<unsigned>(ordinary_windows);events->ordinary->first=gate();
        events->restricted->first_windows=static_cast<unsigned>(ordinary_windows);
        if(!default_timeout){events->restricted->first=gate();events->restricted->following=gate();}
        const auto captured=events;
        callback_hooks->publish(captured->ordinary->hooks());
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[captured](const char* stage){
            if(std::strcmp(stage,"reconciliation-pending")==0)++captured->pending;
            if(std::strcmp(stage,"reconcile-refreeze-committed")==0)++captured->refrozen;
            if(std::strcmp(stage,"delivery-retry-admitted")==0){++captured->admitted;if(captured->admitted_gate)captured->admitted_gate->wait();}
            if(captured->observed)captured->observed(stage);
        },[captured](const char* stage)->std::shared_ptr<void>{return captured->scope?captured->scope(stage):nullptr;}, nullptr, nullptr,
        [captured,hooks=callback_hooks]{hooks->publish(captured->restricted->hooks());});
        connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
        if(!default_timeout)ASSERT_TRUE(detail::recovery_delivery_registration_test_access::timeout(*synchronizers[0],0));
        seed_local(count,800);sent_ids=originals();ASSERT_EQ(sent_ids.size(),count);
        ASSERT_TRUE(until([&]{return held_originals()==sent_ids&&events->ordinary->started.load()==ordinary_windows;}));
    }
    void restricted(){
        // The actual ordinary delivery ACK creates no canonical acceptance:
        // these first bytes are deliberately held before source.receive.
        legacy_ack(0,sent_ids);
        ASSERT_TRUE(until([&]{return held_uploads.size()==2*ordinary_windows&&events->restricted->started.load()==ordinary_windows;}));
        events->ordinary->first->release();
        ASSERT_TRUE(until([&]{return events->ordinary->completed.load()==ordinary_windows;}));
        ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
    }
    size_t dial_count(){std::lock_guard lock(wire->mutex);return wire->endpoints.size();}
    void expect_failed_drain(){const auto result=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(5));EXPECT_EQ(result.state,sync_drain_state::failed);EXPECT_TRUE(result.error);}
    void controller_failure_survives_timeout(bool commit_fault){
        struct fault_state {std::atomic<unsigned> hits{0},installs{0};std::atomic<bool> first_refreeze{true},armed{false};std::mutex mutex;Snapshot before;};
        const auto state=std::make_shared<fault_state>();const auto refreeze=gate();
        events->scope=[this,state,refreeze,commit_fault](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"reconcile-refreeze")==0&&state->first_refreeze.exchange(false))refreeze->wait();
            if(std::strcmp(stage,"install")!=0||!state->armed.load())return {};
            // Only the first real positive install is faulted. An incorrect
            // timeout reset would allow a second install, and is observable.
            if(state->installs.fetch_add(1)!=0)return {};
            {std::lock_guard lock(state->mutex);state->before=snapshot();}
            if(commit_fault)return std::make_shared<ControllerCommitFault>(receiver.get(),state->hits);
            return std::make_shared<DeliveryTimeoutReadFault>(receiver.get(),state->hits);
        };
        start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
        ASSERT_TRUE(until([&]{return refreeze->arrived.load()==1;}));ASSERT_EQ(phase(),4);
        auto applied=peers[0].setup.receive(held_uploads[1].raw);ASSERT_EQ(applied.status_code(),1);ASSERT_EQ(applied.take_ids(),sent_ids);
        // The real positive source receipt precedes the fresh Q. No legacy ACK
        // is delivered, so the first restricted timer still owns its ID.
        state->armed.store(true);refreeze->release();
        ASSERT_TRUE(until([&]{return state->hits.load()==1&&has_error();}));ASSERT_EQ(phase(),2);
        Snapshot before;{std::lock_guard lock(state->mutex);before=state->before;}
        ASSERT_FALSE(before.empty());EXPECT_EQ(snapshot(),before);
        events->restricted->first->release();
        ASSERT_TRUE(until([&]{return events->admitted.load()==1&&events->restricted->completed.load()==1;}));
        // This is an actual checked drain, not a queue sentinel or a sleep:
        // with the one-shot fault gone, a wrongly cleared controller failure
        // could install the genuine positive receipt and settle this drain.
        const auto drained=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(5));
        EXPECT_EQ(drained.state,sync_drain_state::deadline_pending);EXPECT_TRUE(drained.discovery_pending);EXPECT_FALSE(drained.error);
        EXPECT_EQ(state->installs.load(),1u);EXPECT_EQ(state->hits.load(),1u);EXPECT_EQ(phase(),2);EXPECT_EQ(snapshot(),before);
        EXPECT_EQ(held_uploads.size(),2u);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),1);
        EXPECT_EQ(originals(),sent_ids);
    }
    void release_controller_worker_holds() override {
        for(const auto& value:gates)value->release();
        RecoveryReceiverController::release_controller_worker_holds();
    }
    void TearDown()override{
        for(const auto& value:gates)value->release();
        RecoveryReceiverController::TearDown();
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<deadline&&(events->ordinary->started.load()!=events->ordinary->completed.load()||events->restricted->started.load()!=events->restricted->completed.load()))std::this_thread::sleep_for(std::chrono::milliseconds(2));
        EXPECT_EQ(events->ordinary->started.load(),events->ordinary->completed.load());
        EXPECT_EQ(events->restricted->started.load(),events->restricted->completed.load());
        for(const auto& value:gates)EXPECT_FALSE(value->timedOut());
    }
};
TEST_F(RecoveryDeliveryTimeout, DefaultTimeoutRecoversDroppedRestrictedDeliveryWithoutReconnectOrAnotherRequest) {
    ASSERT_EQ(sync_config{}.ack_timeout_base_ms,10000);
    start(1,1000,true);ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    const auto initial_endpoint=peers[0].physical;const auto initial_dials=dial_count();
    const auto raw_originals=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto first_claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    ASSERT_TRUE(until([&]{return phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    ASSERT_EQ(events->admitted.load(),0u);
    // The first restricted true handoff is dropped. No further user request,
    // reconnect, timeout override, or legacy delivery ACK occurs in this test.
    bool accepted=false;size_t consumed=2;
    ASSERT_TRUE(until([&]{
        while(consumed<held_uploads.size()){
            const auto& frame=held_uploads[consumed++];const auto event=server_sent_event::from_json(frame.raw);
            if(!event||event->audit_logs.size()!=1||event->audit_logs[0].global_id!=sent_ids[0])throw db_error("timeout retransmission changed original");
            auto result=peers[0].setup.receive(frame.raw);if(result.status_code()!=1||result.take_ids()!=sent_ids)throw db_error("actual retransmission was not accepted");
            accepted=true; // Retain the real canonical receipt; withhold ACK.
        }
        return accepted&&events->restricted->started.load()+1==held_uploads.size()&&phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==1;
    },35000));
    EXPECT_GE(events->admitted.load(),1u);EXPECT_EQ(dial_count(),initial_dials);EXPECT_TRUE(peers[0].physical.matches(initial_endpoint));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),1);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM AuditLog"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),raw_originals);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),first_claims);
    EXPECT_EQ(originals(),sent_ids);
}
TEST_F(RecoveryDeliveryTimeout, TwoActualSiblingWindowsAdmitOnlyOneCohortRetry) {
    events->restricted->after=gate();events->admitted_gate=gate();
    start(2,1);ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    const auto before=snapshot();events->restricted->first->release();
    ASSERT_TRUE(until([&]{return events->restricted->transitioned.load()==2;}));
    EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,0);EXPECT_EQ(events->admitted.load(),0u);
    events->restricted->after->release();
    ASSERT_TRUE(until([&]{return events->admitted_gate->arrived.load()==1&&events->restricted->completed.load()==1;}));
    EXPECT_EQ(events->admitted.load(),1u);EXPECT_EQ(snapshot(),before);
    events->admitted_gate->release();
    ASSERT_TRUE(until([&]{return events->restricted->completed.load()>=2&&held_uploads.size()==6&&events->restricted->started.load()==4;}));
    EXPECT_EQ(events->admitted.load(),1u);EXPECT_EQ(events->restricted->started.load(),4u);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{sent_ids[0],sent_ids[1],sent_ids[0],sent_ids[1],sent_ids[0],sent_ids[1]}));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(originals(),sent_ids);
}
TEST_F(RecoveryDeliveryTimeout, ExpiryBeforeRefreezeRetainsDemandForTheNextActualUnknownDecision) {
    const auto refreeze=gate();auto first=std::make_shared<std::atomic<bool>>(true);
    events->scope=[refreeze,first](const char* stage)->std::shared_ptr<void>{if(std::strcmp(stage,"reconcile-refreeze")==0&&first->exchange(false))refreeze->wait();return {};};
    start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return refreeze->arrived.load()==1;}));ASSERT_EQ(phase(),4);
    const auto before=snapshot();events->restricted->first->release();
    ASSERT_TRUE(until([&]{return events->admitted.load()==1&&events->restricted->completed.load()==1;}));
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(held_uploads.size(),2u);
    refreeze->release();
    ASSERT_TRUE(until([&]{return held_uploads.size()==3&&events->restricted->started.load()==2;}));
    EXPECT_EQ(events->admitted.load(),1u);EXPECT_EQ(held_originals(),(std::vector<std::string>{sent_ids[0],sent_ids[0],sent_ids[0]}));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
}
TEST_F(RecoveryDeliveryTimeout, ActualExternalRequestInvalidatesAnAlreadyExpiredOldRetryToken) {
    events->restricted->after=gate();const auto replacement=gate();auto replacing=std::make_shared<std::atomic<bool>>(false);
    events->observed=[replacement,replacing](const char* stage){if(replacing->load()&&std::strcmp(stage,"reconciliation-pending")==0)replacement->wait();};
    start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    events->restricted->first->release();ASSERT_TRUE(until([&]{return events->restricted->transitioned.load()==1;}));
    replacing->store(true);request_recovery();ASSERT_TRUE(until([&]{return replacement->arrived.load()==1;}));
    const auto before=snapshot();events->restricted->after->release();
    ASSERT_TRUE(until([&]{return events->restricted->completed.load()==1;}));
    EXPECT_EQ(events->admitted.load(),0u);EXPECT_EQ(snapshot(),before);EXPECT_EQ(held_uploads.size(),2u);
    replacement->release();
}
TEST_F(RecoveryDeliveryTimeout, RetiredPhysicalAttemptRejectsAnAlreadyExpiredOldRetryToken) {
    events->restricted->after=gate();start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    events->restricted->first->release();ASSERT_TRUE(until([&]{return events->restricted->transitioned.load()==1;}));
    const auto old=peers[0].physical;synchronizers[0]->disconnect();const auto before=snapshot();
    events->restricted->after->release();ASSERT_TRUE(until([&]{return events->restricted->completed.load()==1;}));
    EXPECT_EQ(events->admitted.load(),0u);EXPECT_EQ(snapshot(),before);
    ASSERT_TRUE(until([&]{return !old.is_current();}));
    EXPECT_EQ(held_uploads.size(),2u);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
}
TEST_F(RecoveryDeliveryTimeout, RevokedSourceOnTheSamePhysicalAttemptRejectsAnExpiredOldRetryToken) {
    events->restricted->after=gate();start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    events->restricted->first->release();ASSERT_TRUE(until([&]{return events->restricted->transitioned.load()==1;}));
    const auto endpoint=peers[0].physical;
    // An unsolicited second describe is a real native-source response. Feed
    // it through the same physical endpoint to exercise source revocation,
    // without changing the physical lifecycle or creating a source record.
    const auto command=json{{"kind","recoveryReady"},{"version",1},{"operation","describe"},{"requestID",::lattice::uuid_t::generate().to_string()}}.dump();
    auto charge=peers[0].setup.stop_token().reserve_ready(command.size());ASSERT_TRUE(charge.valid());
    auto response=peers[0].setup.ready(command,charge);ASSERT_EQ(response.status_code(),1);ASSERT_TRUE(response.publishable());
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(response.wire())));
    ASSERT_TRUE(until([&]{return error_contains("unsolicited, repeated or expired recovery frame");}));
    const auto before=snapshot();events->restricted->after->release();
    ASSERT_TRUE(until([&]{return events->restricted->completed.load()==1;}));
    EXPECT_EQ(events->admitted.load(),0u);EXPECT_TRUE(endpoint.is_current());
    EXPECT_TRUE(peers[0].physical.matches(endpoint));EXPECT_EQ(snapshot(),before);EXPECT_EQ(held_uploads.size(),2u);
}
TEST_F(RecoveryDeliveryTimeout, PositiveInstallCommitFailureRemainsClosedAfterARealDeliveryTimeout) {
    controller_failure_survives_timeout(true);
}
TEST_F(RecoveryDeliveryTimeout, PositiveInstallSqlReadFailureRemainsClosedAfterARealDeliveryTimeout) {
    controller_failure_survives_timeout(false);
}
TEST_F(RecoveryDeliveryTimeout, ActualOrdinaryAckAndReplacementRegistrationGiveTheOldWorkerNoRetryEffect) {
    start();ASSERT_FALSE(HasFatalFailure());
    const auto old=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],sent_ids[0]);ASSERT_NE(old,0u);
    restricted();ASSERT_FALSE(HasFatalFailure());
    const auto current=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],sent_ids[0]);
    EXPECT_NE(current,0u);EXPECT_NE(current,old);EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,1);
    EXPECT_EQ(events->ordinary->completed.load(),1u);EXPECT_EQ(events->ordinary->transitioned.load(),0u);
    EXPECT_EQ(events->admitted.load(),0u);EXPECT_EQ(events->restricted->completed.load(),0u);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
    // A late restricted ACK while frozen belongs to the separately composed
    // late-ACK correction. This case does not manufacture its tracking effect.
}
TEST_F(RecoveryDeliveryTimeout, ThrowingActualRestrictedSendLaunchesNoWorkerAndCannotRearm) {
    const auto fault=std::make_shared<DeliveryTimeoutThrowState>();set_network_factory(std::make_shared<DeliveryTimeoutThrowNetwork>(wire,fault));
    start();ASSERT_FALSE(HasFatalFailure());fault->enabled.store(true);legacy_ack(0,sent_ids);
    ASSERT_TRUE(until([&]{return fault->rejected.load()==1&&error_contains("timeout fixture rejects actual restricted send");}));
    events->ordinary->first->release();ASSERT_TRUE(until([&]{return events->ordinary->completed.load()==1;}));
    EXPECT_EQ(events->restricted->started.load(),0u);EXPECT_EQ(events->restricted->transitioned.load(),0u);EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(held_uploads.size(),1u);EXPECT_EQ(originals(),sent_ids);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL AND stage=0"),1);expect_failed_drain();
}


// Composed delivery regressions use the actual admitted owner, mounted source,
// controller descriptor and physical route. The manual lane places the COMMIT
// restriction on the same thread that executes the real ACK FIFO unit.
class RecoveryComposedDelivery : public RecoveryRestrictedFirstClaim {
protected:
    std::shared_ptr<DeliveryTimeoutEvents> events=std::make_shared<DeliveryTimeoutEvents>();
    std::vector<std::shared_ptr<DeliveryTimeoutGate>> gates;
    std::vector<std::string> ids;
    std::shared_ptr<DeliveryTimeoutGate> gate(){auto value=std::make_shared<DeliveryTimeoutGate>();gates.push_back(value);return value;}
    Snapshot source_snapshot(){Snapshot value;
        const auto tables=source->db().query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name LIMIT 129");
        if(tables.size()>128)throw db_error("composed fixture source table bound");
        for(const auto& row:tables){const auto& name=std::get<std::string>(row.at("name"));
            if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](char c){return (c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_';}))
                throw db_error("composed fixture source table identity");
            value[name]=source->db().query("SELECT * FROM \""+name+"\"");}
        value["sqlite_schema"]=source->db().query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");return value;
    }
    void start_frozen(){
        configure(1,false);open_manual_receiver();
        events->ordinary->first=gate();events->restricted->first=gate();events->restricted->following=gate();
        const auto captured=events;
        callback_hooks->publish(captured->ordinary->hooks());
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[captured](const char* stage){
            if(std::strcmp(stage,"reconciliation-pending")==0)++captured->pending;
            if(std::strcmp(stage,"delivery-retry-admitted")==0)++captured->admitted;
        },nullptr, nullptr, nullptr,[captured,hooks=callback_hooks]{hooks->publish(captured->restricted->hooks());});
        connect();ASSERT_TRUE(drive([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
        auto configured=std::make_shared<std::atomic<bool>>(false);
        manual->invoke([this,configured]{detail::recovery_receiver_cohort_test_access::next_ack_timeout(*synchronizers[0]);configured->store(true);});
        ASSERT_TRUE(drive([&]{return configured->load();}));
        seed_local(1,820);ids=originals();ASSERT_EQ(ids.size(),1u);
        ASSERT_TRUE(drive([&]{return held_originals()==ids&&events->ordinary->started.load()==1;}));
        legacy_ack(0,ids);
        ASSERT_TRUE(drive([&]{return held_uploads.size()==2&&events->restricted->started.load()==1;}));
        events->ordinary->first->release();ASSERT_TRUE(observe([&]{return events->ordinary->completed.load()==1;}));
        ASSERT_TRUE(drive([&]{return phase()==2&&has_error();}));
        {std::lock_guard lock(errors_mutex);ASSERT_EQ(errors.size(),1u);ASSERT_NE(errors[0].find("UNKNOWN persisted after one restricted pass"),std::string::npos);}
        ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL AND ack_position IS NULL"),1);
        ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,1);ASSERT_EQ(synchronizers[0]->get_progress().acked,1);
    }
    void TearDown()override{
        for(const auto& value:gates)value->release();
        RecoveryRestrictedFirstClaim::TearDown();
        EXPECT_TRUE(observe([&]{return events->ordinary->started.load()==events->ordinary->completed.load()&&events->restricted->started.load()==events->restricted->completed.load();}));
        for(const auto& value:gates)EXPECT_FALSE(value->timedOut());
    }
};
TEST_F(RecoveryComposedDelivery, RestrictedLateAckWhileFrozenRetiresExactTimerWithoutCanonicalSettlement) {
    start_frozen();ASSERT_FALSE(HasFatalFailure());
    auto accepted=peers[0].setup.receive(held_uploads[1].raw);ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.take_ids(),ids);
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),1);
    const auto before=snapshot(),canonical=source_snapshot();const auto original_claims=claims();
    const auto delivery=receiver->db().query("SELECT * FROM _lattice_sync_state");
    const auto token=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]);ASSERT_NE(token,0u);
    const auto progress=synchronizers[0]->get_progress();
    auto completed=std::make_shared<std::atomic<unsigned>>(0);
    synchronizers[0]->set_on_sync_complete([completed,expected=ids](const auto& actual){EXPECT_EQ(actual,expected);++*completed;});
    legacy_ack(0,ids);ASSERT_TRUE(drive([&]{return completed->load()==1;}));
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(source_snapshot(),canonical);EXPECT_EQ(claims(),original_claims);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_sync_state"),delivery);
    EXPECT_EQ(detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]),0u);
    const auto after=synchronizers[0]->get_progress();EXPECT_EQ(after.pending_upload,0);EXPECT_EQ(after.acked,progress.acked+1);
    EXPECT_EQ(after.total_upload,progress.total_upload);EXPECT_EQ(after.received,progress.received);EXPECT_EQ(after.sync_id,progress.sync_id);
    // No queue drive or external request can create a substitute retry. Release
    // the old real worker only after its actual ACK removed this registration.
    events->restricted->first->release();ASSERT_TRUE(observe([&]{return events->restricted->completed.load()==1;}));
    EXPECT_EQ(events->restricted->transitioned.load(),0u);EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(source_snapshot(),canonical);EXPECT_EQ(held_uploads.size(),2u);EXPECT_EQ(phase(),2);
    EXPECT_EQ(completed->load(),1u);EXPECT_EQ(originals(),ids);
}
TEST_F(RecoveryComposedDelivery, RestrictedLateAckCommitDenialPreservesActualTokenProgressAndAllDurableState) {
    start_frozen();ASSERT_FALSE(HasFatalFailure());
    auto accepted=peers[0].setup.receive(held_uploads[1].raw);ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.take_ids(),ids);
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),1);
    const auto before=snapshot(),canonical=source_snapshot();const auto original_claims=claims();
    const auto delivery=receiver->db().query("SELECT * FROM _lattice_sync_state");
    const auto token=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]);ASSERT_NE(token,0u);
    const auto progress=synchronizers[0]->get_progress();size_t prior_errors;{std::lock_guard lock(errors_mutex);prior_errors=errors.size();}
    auto completed=std::make_shared<std::atomic<unsigned>>(0);synchronizers[0]->set_on_sync_complete([completed](const auto&){++*completed;});
    std::atomic<unsigned> denied{0};legacy_ack(0,ids);
    {
        ControllerCommitFault fault(receiver.get(),denied);
        ASSERT_TRUE(drive([&]{std::lock_guard lock(errors_mutex);return denied.load()==1&&errors.size()>prior_errors;}));
    }
    EXPECT_EQ(denied.load(),1u);EXPECT_EQ(completed->load(),0u);EXPECT_EQ(snapshot(),before);EXPECT_EQ(source_snapshot(),canonical);
    EXPECT_EQ(claims(),original_claims);EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_sync_state"),delivery);
    EXPECT_EQ(detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]),token);
    const auto after=synchronizers[0]->get_progress();EXPECT_EQ(after.pending_upload,progress.pending_upload);EXPECT_EQ(after.total_upload,progress.total_upload);
    EXPECT_EQ(after.acked,progress.acked);EXPECT_EQ(after.received,progress.received);EXPECT_EQ(after.sync_id,progress.sync_id);
    EXPECT_EQ(events->restricted->completed.load(),0u);EXPECT_EQ(events->restricted->transitioned.load(),0u);EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(held_uploads.size(),2u);EXPECT_EQ(originals(),ids);EXPECT_EQ(phase(),2);
}
TEST_F(RecoveryDeliveryTimeout, ActualExpiryBetweenUnknownThrowAndCatchPreservesDemandAndOriginalFailure) {
    const auto caught=gate(),next_caught=gate();auto waits=std::make_shared<std::atomic<unsigned>>(0);
    events->observed=[caught,next_caught,waits](const char* stage){if(std::strcmp(stage,"delivery-retry-wait-before-latch")!=0)return;
        const auto n=waits->fetch_add(1);if(n==0){caught->wait();throw db_error("observation must not replace typed UNKNOWN wait");}if(n==1)next_caught->wait();};
    start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return caught->arrived.load()==1;}));ASSERT_EQ(phase(),2);ASSERT_FALSE(has_error());
    const auto before=snapshot();const auto raw=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    events->restricted->first->release();ASSERT_TRUE(until([&]{return events->admitted.load()==1&&events->restricted->completed.load()==1;}));
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(held_uploads.size(),2u);EXPECT_FALSE(has_error());
    caught->release();
    ASSERT_TRUE(until([&]{return held_uploads.size()==3&&events->restricted->started.load()==2&&next_caught->arrived.load()==1;}));
    EXPECT_EQ(events->admitted.load(),1u);EXPECT_FALSE(has_error());EXPECT_EQ(waits->load(),2u);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),raw);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{sent_ids[0],sent_ids[0],sent_ids[0]}));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
    next_caught->release();ASSERT_TRUE(until([&]{return error_contains("UNKNOWN persisted after one restricted pass");}));
    EXPECT_FALSE(error_contains("observation must not replace"));
}
TEST_F(RecoveryDeliveryTimeout, PhaseFourReconstructionPreservesPreviouslyAdmittedRetryForFreshUnknown) {
    struct Observed {std::atomic<unsigned> denied{0},refreezes{0},phase4{0};};auto state=std::make_shared<Observed>();
    events->scope=[this,state](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"reconcile-refreeze")==0&&state->refreezes.fetch_add(1)==0)
            return std::make_shared<ControllerCommitFault>(receiver.get(),state->denied);
        return {};
    };
    events->observed=[this,state](const char* stage){if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4)++state->phase4;};
    start();ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return state->denied.load()==1&&has_error();}));ASSERT_EQ(phase(),4);
    const auto raw=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    events->restricted->first->release();ASSERT_TRUE(until([&]{return events->admitted.load()==1&&events->restricted->completed.load()==1;}));
    ASSERT_EQ(phase(),4);ASSERT_EQ(held_uploads.size(),2u);
    const auto attempt=scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity");
    // This one real request replaces the failed descriptor. Reconstruction
    // consumes external demand and sends once. The next fresh UNKNOWN can
    // cancel back to phase4 only using the already-admitted timer revision.
    // Its ID is still in flight, so no third restricted handoff is permitted.
    request_recovery();
    ASSERT_TRUE(until([&]{return state->phase4.load()==3&&phase()==4&&held_uploads.size()==3&&events->restricted->started.load()==2;}));
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),attempt+1);
    EXPECT_EQ(state->denied.load(),1u);EXPECT_EQ(events->admitted.load(),1u);
    EXPECT_EQ(events->restricted->completed.load(),1u);EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,1);
    EXPECT_NE(detail::recovery_delivery_registration_test_access::token(*synchronizers[0],sent_ids[0]),0u);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{sent_ids[0],sent_ids[0],sent_ids[0]}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),raw);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
}

TEST_F(RecoveryComposedDelivery, StaleParkedRestrictedFrameKeepsCohortUntilActualDisposalWithoutTimerOrError) {
    configure(1,false);open_manual_receiver();events->ordinary->first=gate();events->restricted->first=gate();
    auto captures=std::make_shared<std::atomic<unsigned>>(0),waiting=std::make_shared<std::atomic<unsigned>>(0),phase4=std::make_shared<std::atomic<unsigned>>(0);
    auto park=std::make_shared<std::atomic<bool>>(true);const auto captured=events;
    callback_hooks->publish(captured->ordinary->hooks());
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this,captured,captures,waiting,phase4](const char* stage){
        if(std::strcmp(stage,"cohort-reserved")==0)++*captures;
        if(std::strcmp(stage,"cohort-retained")==0)++*waiting;
        if(std::strcmp(stage,"delivery-retry-admitted")==0)++captured->admitted;
        if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4)++*phase4;
    },nullptr, nullptr, nullptr,[this,captured,park]{
        callback_hooks->publish(captured->restricted->hooks(),[this,park]{
            if(park->exchange(false)){committed_snapshot=snapshot();writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);}
        });
    });
    connect();ASSERT_TRUE(drive([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    auto configured=std::make_shared<std::atomic<bool>>(false);
    manual->invoke([this,configured]{detail::recovery_receiver_cohort_test_access::next_ack_timeout(*synchronizers[0]);configured->store(true);});
    ASSERT_TRUE(drive([&]{return configured->load();}));seed_local(1,830);ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(drive([&]{return held_originals()==ids&&events->ordinary->started.load()==1;}));legacy_ack(0,ids);
    std::shared_ptr<detail::sync_discovery_operation> retained;
    ASSERT_TRUE(drive([&]{retained=detail::recovery_receiver_cohort_test_access::retained_busy(*synchronizers[0]);return bool(retained)&&phase4->load()!=0;}));
    ASSERT_TRUE(writer_hold);ASSERT_FALSE(committed_snapshot.empty());ASSERT_EQ(phase4->load(),1u);
    const auto count=captures->load();const auto token=detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0]);ASSERT_NE(token,0u);
    events->ordinary->first->release();ASSERT_TRUE(observe([&]{return events->ordinary->completed.load()==1;}));
    // Retire this real descriptor while its committed frame is parked. No
    // factory creates a replacement frame, grant or timeout callback here.
    request_recovery();ASSERT_TRUE(drive([&]{return waiting->load()>0;}));EXPECT_EQ(captures->load(),count);
    writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);writer_hold.reset();
    ASSERT_TRUE(drive([&]{return !detail::recovery_receiver_cohort_test_access::retained_busy(*synchronizers[0])&&
        detail::recovery_delivery_registration_test_access::token(*synchronizers[0],ids[0])==0;}));
    EXPECT_FALSE(has_error());EXPECT_EQ(events->restricted->started.load(),0u);EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(synchronizers[0]->get_progress().pending_upload,0);EXPECT_EQ(held_uploads.size(),1u);
    EXPECT_EQ(snapshot(),committed_snapshot);EXPECT_EQ(captures->load(),count);EXPECT_EQ(phase4->load(),1u);
    const auto original_claims=claims();const auto raw=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    // The queue released its ownership. This actual retained operation is the
    // final frame custodian: only dropping it permits a replacement cohort.
    retained.reset();
    ASSERT_TRUE(drive([&]{return phase4->load()==2&&held_uploads.size()==2&&events->restricted->started.load()==1;}));
    EXPECT_GT(captures->load(),count);EXPECT_FALSE(has_error());EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(claims(),original_claims);EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),raw);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{ids[0],ids[0]}));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);
}
}
#endif


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(RecoveryReceiverController, EqualSourceFrontierInstallsDistinctActualRequestAndLeaseCommitments) {
    configure(2);insert(*source,controller_uuid(9100),"shared source row");
    insert(*receiver,controller_uuid(9101),"retained local intent");const auto pause=pause_install();connect();
    ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    const auto stored=receiver->db().query("SELECT manifest_frame FROM _lattice_recovery_request ORDER BY channel");ASSERT_EQ(stored.size(),2u);
    std::vector<json> manifests;
    for(const auto& row:stored){const auto& bytes=std::get<std::vector<uint8_t>>(row.at("manifest_frame"));
        manifests.push_back(json::parse(std::string(bytes.begin(),bytes.end())).at("latticeCanonicalRange").at("body"));}
    EXPECT_EQ(manifests[0].at("source"),manifests[1].at("source"));EXPECT_EQ(manifests[0].at("head"),manifests[1].at("head"));
    EXPECT_NE(manifests[0].at("request_digest"),manifests[1].at("request_digest"));
    EXPECT_NE(manifests[0].at("lease"),manifests[1].at("lease"));
    EXPECT_NE(manifests[0].at("content_digest"),manifests[1].at("content_digest"));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1 AND active IS NULL"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);EXPECT_FALSE(has_error());
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class TerminalReceiverController : public RecoveryReceiverController {
protected:
    std::function<bool(size_t,const json&)> before_control;
    std::function<void(size_t,const json&,std::string&)> after_control;
    std::vector<std::pair<size_t,json>> controls;
    std::map<size_t,json> full_requests;
    json source_policy(const std::string& ns)override {
        auto p=RecoveryReceiverController::source_policy(ns);p["readyProfile"]="bounded48MiBOrphanV1";
        p["orphanResumeGraceMilliseconds"]=60000;return p;
    }
    bool before_ready(size_t index,const json& control)override {
        if(controls.size()>=256)throw db_error("terminal fixture control bound");controls.emplace_back(index,control);
        if(control.contains("request"))full_requests[index]=control;
        return before_control&&before_control(index,control);
    }
    void after_ready(size_t index,const json& control,const std::string& raw)override {
        std::string outgoing=raw;if(after_control)after_control(index,control,outgoing);
        if(!outgoing.empty())RecoveryReceiverController::after_ready(index,control,outgoing);
        else if(boundary_trace.current)boundary_trace.current->terminal_empty=true;
    }
    json discard(size_t index) {
        const auto& q=full_requests.at(index);json command={{"kind","recoveryReady"},{"version",1},{"operation","discard"},
            {"requestID",::lattice::uuid_t::generate().to_string()},{"routeGeneration",q.at("routeGeneration")},{"request",q.at("request")}};
        const auto raw=command.dump();
        auto* observation=boundary_trace.add(ControllerBoundaryTrace::Event::discard,index);
        if(observation)observation->connection=peers.at(index).trace_connection;
        ControllerBoundaryTrace::request(observation,command,raw.size());
        ControllerBoundaryTrace::Scope trace_scope(boundary_trace,observation);
        auto charge=peers.at(index).setup.stop_token().reserve_ready(raw.size());
        if(!charge.valid())throw db_error("terminal fixture discard admission refused");
        const auto result=peers.at(index).setup.ready(raw,charge);
        const auto status=result.status_code();bool publishable=false;if(status==1)publishable=result.publishable();
        ControllerBoundaryTrace::result(observation,status,publishable);
        if(status!=1||!publishable)throw db_error("terminal fixture actual discard unavailable");
        const auto& wire=result.wire();ControllerBoundaryTrace::copy(observation,&ControllerBoundaryTrace::Record::source,wire);
        const auto reply=json::parse(wire);
        if(reply.at("settlement").at("state")!="committed"||reply.at("lifecycle").at("state")!="terminal")
            throw db_error("terminal fixture actual discard did not commit");return reply;
    }
    std::shared_ptr<ControllerPause> pause_terminal(){auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[pause](const char* stage){if(std::strcmp(stage,"terminal-cancel-committed")==0)pause->wait();});return pause;}
    // Same-setup discard revokes its ready fence. Drop the obsolete positive
    // reply, then explicitly reopen; never pretend the subsequent status-4
    // bridge refusal is a successful negative READY control.
    void reopen_after_terminal(const std::function<bool(size_t,const json&)>& target) {
        bool discarded=false;
        after_control=[&](size_t index,const json& control,std::string& outgoing){
            if(!discarded&&target(index,control)){(void)discard(index);discarded=true;outgoing.clear();}};
        connect();ASSERT_TRUE(until([&]{return discarded;}));ASSERT_FALSE(has_error());ASSERT_EQ(phase(),2);
        const auto old=framing(),intent=entries(),allocation=allocators();
        const auto attempts=receiver->db().query("SELECT * FROM _lattice_range_attempt ORDER BY channel");
        const auto pages=receiver->db().query("SELECT * FROM _lattice_range_page ORDER BY channel,stream,page_index");
        close_receiver();after_control={};open_receiver();
        EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
        EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_range_attempt ORDER BY channel"),attempts);
        EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_range_page ORDER BY channel,stream,page_index"),pages);
    }
    void reopen_after_terminal_prepare(size_t selected=0) {
        reopen_after_terminal([selected](size_t index,const json& control){return index==selected&&control.at("operation")=="prepare";});
    }

    std::vector<database::row_t> entries(){return receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");}
    std::vector<database::row_t> allocators(){return receiver->db().query("SELECT * FROM _lattice_obligation_store");}
    std::vector<database::row_t> framing(){return receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");}
    bool installed(int64_t revision=1){return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision="+std::to_string(revision))==static_cast<int64_t>(peers.size());}
};
TEST_F(TerminalReceiverController, PreManifestTerminalRearmsExactOriginalsAndAllocatorState) {
    configure();insert(*source,controller_uuid(9300),"canonical");seed_local(2,9301);
    const auto audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before_entries=entries(),before_allocators=allocators();
    reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());const auto pause=pause_terminal();connect();ASSERT_TRUE(until([&]{return pause->ready();}));
    EXPECT_EQ(phase(),1);EXPECT_EQ(entries(),before_entries);EXPECT_EQ(allocators(),before_allocators);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1 AND length(manifest_frame)=0"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE last_sequence=1 AND active IS NULL AND revision=0"),1);
    EXPECT_THROW(insert(*receiver,controller_uuid(9399),"closed"),db_error);
    pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),3);
}
TEST_F(TerminalReceiverController, PartialManifestTerminalRetiresOnlyExactActiveStageAndPreservesIntent) {
    configure();insert(*source,controller_uuid(9310),"canonical");seed_local(1,9311);const auto audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    reopen_after_terminal([&](size_t,const json& control){return control.at("operation")=="read"&&control.at("index")=="2";});ASSERT_FALSE(HasFatalFailure());
    EXPECT_GT(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_page"),0);
    const auto pause=pause_terminal();connect();ASSERT_TRUE(until([&]{return pause->ready();}));EXPECT_EQ(phase(),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt"),0);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_page"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1 AND length(manifest_frame)>0"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
    pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, MixedVerifiedAndManifestlessCohortDisposesEverySourceBeforeOneRestart) {
    configure(2);insert(*source,controller_uuid(9320),"canonical");seed_local(1,9321);const auto before_entries=entries(),before_allocators=allocators();
    bool verified_first=false;
    reopen_after_terminal([&](size_t index,const json& control){if(index!=1||control.at("operation")!="prepare")return false;
        verified_first=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1")==1;return true;});ASSERT_FALSE(HasFatalFailure());
    const auto pause=pause_terminal();connect();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_TRUE(verified_first);EXPECT_EQ(phase(),1);
    EXPECT_EQ(entries(),before_entries);EXPECT_EQ(allocators(),before_allocators);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE last_sequence=1 AND active IS NULL AND revision=0"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)>0"),1);
    bool discarded_live=false;for(const auto& [index,control]:controls)if(index==0&&control.at("operation")=="discard")discarded_live=true;
    EXPECT_TRUE(discarded_live);pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, NeverStartedQReopensAndPreparesExactSequenceWithoutTerminalInference) {
    configure();insert(*source,controller_uuid(9330),"canonical");bool dropped_before_source=false;
    before_control=[&](size_t,const json& control){if(!dropped_before_source&&control.at("operation")=="prepare"){dropped_before_source=true;return true;}return false;};
    connect();ASSERT_TRUE(until([&]{return dropped_before_source;}));const auto original=framing();
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding"),0);
    close_receiver();before_control={};open_receiver();EXPECT_EQ(framing(),original);connect();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1"),1);
    bool inspected=false,discarded=false;for(const auto& [_,control]:controls){inspected|=control.at("operation")=="inspect";discarded|=control.at("operation")=="discard";}
    EXPECT_TRUE(inspected);EXPECT_FALSE(discarded);EXPECT_FALSE(has_error());
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(TerminalReceiverController, CancellationCommitDenialRetainsWholeFrozenCohortForReopen) {
    configure(2);insert(*source,controller_uuid(9340),"canonical");seed_local(1,9341);reopen_after_terminal_prepare(1);ASSERT_FALSE(HasFatalFailure());
    std::atomic<unsigned> denied{0};Snapshot before;
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[&](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"terminal-cancel")!=0)return {};before=snapshot();return std::make_shared<ControllerCommitFault>(receiver.get(),denied);});
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_GT(denied.load(),0u);ASSERT_FALSE(before.empty());EXPECT_EQ(phase(),2);EXPECT_EQ(snapshot(),before);
    const auto old=framing();close_receiver();probe.reset();after_control={};open_receiver();EXPECT_EQ(framing(),old);connect();
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, CanceledManifestlessPredecessorReopensBeforeRefreeze) {
    configure();seed_local(1,9350);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){
        if(std::strcmp(stage,"terminal-cancel-committed")==0)throw db_error("fixture crash after terminal cancel COMMIT");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),1);const auto old=framing(),intent=entries(),alloc=allocators();
    close_receiver();probe.reset();after_control={};open_receiver();EXPECT_EQ(phase(),1);EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),alloc);
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, CanceledManifestlessPredecessorReopensAfterRefreezeBeforeNewQ) {
    configure();seed_local(1,9360);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){
        if(std::strcmp(stage,"terminal-refreeze-committed")==0)throw db_error("fixture crash after terminal refreeze COMMIT");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);const auto old=framing();
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1 AND length(manifest_frame)=0"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),2);
    close_receiver();probe.reset();after_control={};open_receiver();EXPECT_EQ(framing(),old);connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, FreshQCommitReopenResumesItsExactNewIdentity) {
    configure();seed_local(1,9370);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){
        if(std::strcmp(stage,"terminal-request-committed")==0)throw db_error("fixture crash after terminal successor Q COMMIT");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);const auto fresh=framing();
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    close_receiver();probe.reset();after_control={};open_receiver();EXPECT_EQ(framing(),fresh);connect();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, LostCommittedDiscardReplyReopensAndRefencesTheWholeCohort) {
    configure(2);seed_local(1,9380);reopen_after_terminal_prepare(1);ASSERT_FALSE(HasFatalFailure());bool lost=false;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(!lost&&index==0&&control.at("operation")=="discard"){lost=true;outgoing.clear();}};
    connect();ASSERT_TRUE(until([&]{return lost;}));ASSERT_EQ(phase(),2);const auto old=framing(),intent=entries();
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    const auto retired_endpoint=peers[0].physical;close_receiver();after_control={};open_receiver();EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);connect();
    EXPECT_FALSE(retired_endpoint.trigger_on_message(transport_message::from_string("{}")));
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, UnknownSettlementCannotTurnTerminalBodyIntoRestartAuthority) {
    configure();seed_local(1,9390);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());bool corrupted=false;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect"){auto reply=json::parse(outgoing);reply["settlement"]["state"]="unknown";outgoing=reply.dump();corrupted=true;}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_TRUE(corrupted);EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL"),0);
    const auto old=framing();close_receiver();after_control={};open_receiver();EXPECT_EQ(framing(),old);connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, FutureHighWaterCannotReplaceFrozenOriginals) {
    configure();seed_local(1,9400);const auto original=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect"){auto reply=json::parse(outgoing);reply["lifecycle"]["bindingHighWater"]="2";outgoing=reply.dump();}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),original);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=0 AND active IS NULL"),1);
}
TEST_F(TerminalReceiverController, KnownInstalledReopenIgnoresGoneSourceCapsule) {
    configure();insert(*source,controller_uuid(9410),"canonical");seed_local(1,9411);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){
        if(std::strcmp(stage,"install-committed")==0)throw db_error("fixture stop after known install");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),3);(void)discard(0);
    const auto original=receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
    const auto control_count=controls.size();close_receiver();probe.reset();open_receiver();EXPECT_EQ(phase(),3);connect();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),original);EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1"),1);
    for(size_t n=control_count;n<controls.size();++n)EXPECT_NE(controls[n].second.at("operation"),"inspect");EXPECT_FALSE(has_error());
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(TerminalReceiverController, DelayedOldTerminalReplyOnCurrentRouteCannotSettleFreshQ) {
    configure();seed_local(1,9420);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());auto ignored=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[ignored](const char* stage){
        if(std::strcmp(stage,"terminal-stale-control-ignored")==0)ignored->store(true);});
    bool injected=false;std::string old_reply,actual_reply;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect")old_reply=outgoing;
        if(!old_reply.empty()&&!injected&&control.at("operation")=="prepare"){
            injected=true;actual_reply=outgoing;outgoing=old_reply;}};
    connect();ASSERT_TRUE(until([&]{return ignored->load();}));ASSERT_TRUE(injected);EXPECT_FALSE(has_error());EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    ASSERT_FALSE(actual_reply.empty());ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(actual_reply)));
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
}
TEST_F(TerminalReceiverController, PriorAcceptedOriginalClaimAckTombstoneAndNewUnsentSurviveTerminalRestart) {
    configure();connect();ASSERT_TRUE(until([&]{return installed();}));seed_local(1,9430);
    ASSERT_TRUE(until([&]{return !held_uploads.empty();}));
    auto accepted=peers[0].setup.receive(held_uploads.front().raw);ASSERT_EQ(accepted.status_code(),1);
    const auto ids=accepted.take_ids();ASSERT_EQ(ids.size(),1u);legacy_ack(0,ids);accepted={};request_recovery();
    ASSERT_TRUE(until([&]{return installed(2);}));
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2 AND first_export IS NOT NULL AND ack_position IS NOT NULL"),1);
    const auto source_rows=source->db().query("SELECT * FROM ControllerRow ORDER BY id"),receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt");
    close_receiver();open_receiver();seed_local(1,9431);
    const auto before_entries=entries(),before_allocators=allocators(),audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());const auto pause=pause_terminal();connect();ASSERT_TRUE(until([&]{return pause->ready();}));
    EXPECT_EQ(phase(),1);EXPECT_EQ(entries(),before_entries);EXPECT_EQ(allocators(),before_allocators);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
    EXPECT_EQ(source->db().query("SELECT * FROM ControllerRow ORDER BY id"),source_rows);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt"),receipts);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2 AND first_export IS NOT NULL AND ack_position IS NOT NULL"),1);
    pause->release();ASSERT_TRUE(until([&]{return installed(3);}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=4"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(TerminalReceiverController, LostCancelPublicationInspectsKnownSuccessorInSameController) {
    configure();seed_local(1,9440);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());auto stopped=std::make_shared<std::atomic<bool>>(false),inspected=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[stopped,inspected](const char* stage){
        if(std::strcmp(stage,"terminal-cancel-committed")==0&&!stopped->exchange(true))throw db_error("fixture lost cancel publication");
        if(std::strcmp(stage,"terminal-cancel-inspected")==0)inspected->store(true);});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),1);
    {std::lock_guard lock(errors_mutex);errors.clear();}request_recovery();
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_TRUE(inspected->load());EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
    EXPECT_NO_THROW(insert(*receiver,controller_uuid(9441),"admission reopened after exact successor"));
}
TEST_F(TerminalReceiverController, CurrentRequestWithMismatchedTerminalIdentityRemainsFrozen) {
    configure();seed_local(1,9450);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect"){auto reply=json::parse(outgoing);reply["lifecycle"]["requestDigest"]=std::string(64,'0');outgoing=reply.dump();}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1"),1);
}
TEST_F(TerminalReceiverController, KnownCommittedLifecycleSecondaryErrorRetainsTerminalFact) {
    configure();seed_local(1,9460);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());bool reported=false;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect"){auto reply=json::parse(outgoing);
            if(reply.contains("lifecycle")&&reply["lifecycle"]["state"]=="terminal"){
                reply["settlement"]["postcommitError"]=true;reply["settlement"]["notificationError"]=true;outgoing=reply.dump();reported=true;}}};
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_TRUE(reported);EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(TerminalReceiverController, QuietLostCommittedDiscardRetriesOnNativePacerAtOriginalThirtySecondDeadline) {
    configure(2);seed_local(1,9470);reopen_after_terminal_prepare(1);ASSERT_FALSE(HasFatalFailure());
    const auto pause=pause_terminal();bool lost=false;
    std::chrono::steady_clock::time_point dropped_at;std::string dropped_id,route_generation,dropped_request;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(!lost&&index==0&&control.at("operation")=="discard"){
            const auto actual=json::parse(outgoing);ASSERT_EQ(actual.at("settlement").at("state"),"committed");
            ASSERT_EQ(actual.at("lifecycle").at("state"),"terminal");
            lost=true;dropped_at=std::chrono::steady_clock::now();dropped_id=control.at("requestID");
            route_generation=control.at("routeGeneration");dropped_request=control.at("request");outgoing.clear();}};
    connect();ASSERT_TRUE(until([&]{return lost;}));ASSERT_EQ(phase(),2);
    const auto intent=entries(),allocation=allocators(),audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto old=framing();const auto endpoint=peers[0].physical;const auto source_view=peers[0].setup.descriptor();const auto sent_before=controls.size();
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    // From the lost reply onward there is no manual request, close/reopen,
    // synthetic response, or scheduler invocation. until only pumps the wire.
    ASSERT_TRUE(until([&]{return pause->ready();},45000));
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-dropped_at).count(),29000);
    EXPECT_EQ(phase(),1);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);EXPECT_EQ(framing(),old);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);EXPECT_TRUE(endpoint.matches(peers[0].physical));
    unsigned retried=0;for(size_t i=sent_before;i<controls.size();++i){const auto& [index,control]=controls[i];
        if(index==0&&control.at("operation")=="discard"){++retried;EXPECT_NE(control.at("requestID"),dropped_id);
            EXPECT_EQ(control.at("routeGeneration"),route_generation);EXPECT_EQ(control.at("request"),dropped_request);}}
    EXPECT_EQ(retried,1u);EXPECT_LT(controls.size()-sent_before,8u);EXPECT_EQ(peers[0].setup.descriptor(),source_view);EXPECT_FALSE(has_error());
    pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
}
TEST_F(TerminalReceiverController, SchedulerHeldStaleAndCurrentRepliesRetainBothUntilActualWorkerConsumption) {
    configure();seed_local(1,9480);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    auto ignored=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[ignored](const char* stage){
        if(std::strcmp(stage,"terminal-stale-control-ignored")==0)++*ignored;});
    auto held=std::make_shared<ControllerPause>();pauses.push_back(held);
    bool injected=false;std::string stale;platform_transport_callbacks endpoint;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect")stale=outgoing;
        if(injected||stale.empty()||control.at("operation")!="prepare")return;
        injected=true;endpoint=peers[index].physical;
        receiver->get_scheduler()->invoke([held]{held->wait();});
        const auto limit=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(!held->ready()&&std::chrono::steady_clock::now()<limit)std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_TRUE(held->ready());ASSERT_EQ(ignored->load(),0u);
        // Both actual envelopes arrive before the scheduler may drain either.
        EXPECT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));
        EXPECT_TRUE(endpoint.trigger_on_message(transport_message::from_string(outgoing)));
        EXPECT_EQ(ignored->load(),0u);outgoing.clear();held->release();
    };
    connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_TRUE(injected);EXPECT_FALSE(has_error());
    EXPECT_EQ(ignored->load(),1u);EXPECT_TRUE(endpoint.matches(peers[0].physical));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
    unsigned prepares=0;for(const auto& [_,control]:controls)if(control.at("operation")=="prepare")++prepares;
    EXPECT_EQ(prepares,2u);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
}
TEST_F(TerminalReceiverController, MalformedStaleLifecycleCannotBeDiscardedAheadOfCurrentReply) {
    configure();seed_local(1,9490);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    auto ignored=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[ignored](const char* stage){
        if(std::strcmp(stage,"terminal-stale-control-ignored")==0)++*ignored;});
    auto held=std::make_shared<ControllerPause>();pauses.push_back(held);
    bool injected=false;std::string stale;std::vector<database::row_t> fresh,intent;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect")stale=outgoing;
        if(injected||stale.empty()||control.at("operation")!="prepare")return;
        injected=true;receiver->get_scheduler()->invoke([held]{held->wait();});
        const auto limit=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(!held->ready()&&std::chrono::steady_clock::now()<limit)std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_TRUE(held->ready());fresh=framing();intent=entries();
        auto malformed=json::parse(stale);malformed["lifecycle"]["bindingHighWater"]="01";
        EXPECT_TRUE(peers[index].physical.trigger_on_message(transport_message::from_string(malformed.dump())));
        EXPECT_TRUE(peers[index].physical.trigger_on_message(transport_message::from_string(outgoing)));
        outgoing.clear();held->release();
    };
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_TRUE(injected);
    EXPECT_EQ(ignored->load(),0u);EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),fresh);EXPECT_EQ(entries(),intent);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=0 AND active IS NULL"),1);
}

}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class LateLifecycleReceiverController : public TerminalReceiverController {
protected:
    struct Gap {
        std::shared_ptr<ControllerPause> pause=std::make_shared<ControllerPause>();
        std::atomic<bool> armed{false},paused{false};
        std::atomic<unsigned> discarded{0},retired{0};
        std::string stale,fresh,describe;
        platform_transport_callbacks endpoint;
        std::string source_view;
    };
    std::shared_ptr<Gap> consumed_gap() {
        configure();seed_local(1,9500);reopen_after_terminal_prepare();if(HasFatalFailure())return {};
        auto gap=std::make_shared<Gap>();pauses.push_back(gap->pause);
        after_control=[gap](size_t,const json& control,std::string& outgoing){
            if(control.at("operation")=="describe")gap->describe=outgoing;
            if(control.at("operation")=="inspect")gap->stale=outgoing;
            if(!gap->stale.empty()&&control.at("operation")=="prepare"){
                gap->fresh=outgoing;gap->armed.store(true);}};
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[gap](const char* stage){
            if(std::strcmp(stage,"pending-consumed-before-successor")==0&&gap->armed.load()&&!gap->paused.exchange(true))gap->pause->wait();
            if(std::strcmp(stage,"late-lifecycle-discarded")==0)++gap->discarded;
            if(std::strcmp(stage,"late-lifecycle-retired-disposed")==0)++gap->retired;});
        connect();if(!until([&]{return gap->pause->ready();})){ADD_FAILURE()<<"actual current reply did not reach consumed gap";return {};}
        gap->endpoint=peers[0].physical;gap->source_view=peers[0].setup.descriptor();return gap;
    }
    void refuses_gap_frame(const std::shared_ptr<Gap>& gap,const std::string& raw) {
        const auto before=framing(),intent=entries(),allocation=allocators(),audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
        ASSERT_TRUE(gap->endpoint.trigger_on_message(transport_message::from_string(raw)));gap->pause->release();
        ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(gap->discarded.load(),0u);EXPECT_EQ(phase(),2);
        EXPECT_EQ(framing(),before);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
        EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=0 AND active IS NULL"),1);
    }
};
TEST_F(LateLifecycleReceiverController, CurrentConsumedThenActualLateLifecycleKeepsSamePhysicalViewAndFreshQ) {
    const auto gap=consumed_gap();ASSERT_TRUE(gap);ASSERT_FALSE(gap->stale.empty());ASSERT_FALSE(gap->fresh.empty());
    const auto q=receiver->db().query("SELECT request_frame FROM _lattice_recovery_request ORDER BY channel");
    const auto audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    ASSERT_TRUE(gap->endpoint.trigger_on_message(transport_message::from_string(gap->stale)));gap->pause->release();
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(gap->discarded.load(),1u);EXPECT_FALSE(has_error());
    EXPECT_TRUE(gap->endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),gap->source_view);
    EXPECT_EQ(receiver->db().query("SELECT request_frame FROM _lattice_recovery_request ORDER BY channel"),q);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),1);
    unsigned prepares=0;for(const auto& [_,control]:controls)if(control.at("operation")=="prepare")++prepares;EXPECT_EQ(prepares,2u);
}
TEST_F(LateLifecycleReceiverController, MalformedLifecycleInConsumedGapFailsClosedWithoutSettlingFreshQ) {
    const auto gap=consumed_gap();ASSERT_TRUE(gap);auto invalid=json::parse(gap->stale);invalid["lifecycle"]["bindingHighWater"]="01";
    refuses_gap_frame(gap,invalid.dump());
}
TEST_F(LateLifecycleReceiverController, WrongAuthenticatedBindingInConsumedGapFailsClosed) {
    const auto gap=consumed_gap();ASSERT_TRUE(gap);auto invalid=json::parse(gap->stale);invalid["lifecycle"]["namespaceID"]="b";
    refuses_gap_frame(gap,invalid.dump());
}
TEST_F(LateLifecycleReceiverController, LateNonLifecycleLeaseReplyRetainsOriginalRefusal) {
    const auto gap=consumed_gap();ASSERT_TRUE(gap);ASSERT_FALSE(gap->fresh.empty());refuses_gap_frame(gap,gap->fresh);
}
TEST_F(LateLifecycleReceiverController, RepeatedDescribeInConsumedGapRetainsOriginalRefusal) {
    const auto gap=consumed_gap();ASSERT_TRUE(gap);ASSERT_FALSE(gap->describe.empty());refuses_gap_frame(gap,gap->describe);
}
class PendingRepeatedDescribeController : public TerminalReceiverController {
protected:
    void refuses_actual_description(const std::string& operation) {
        configure();seed_local(1,9570);
        std::string description;bool injected=false;Snapshot before;
        struct ClearHook {decltype(after_control)& hook;~ClearHook(){hook={};}} clear{after_control};
        after_control=[&](size_t,const json& control,std::string& outgoing){
            if(control.at("operation")=="describe")description=outgoing;
            if(!injected&&control.at("operation")==operation){
                if(description.empty())throw db_error("pending describe fixture has no actual source description");
                before=snapshot();injected=true;outgoing=description;
            }
        };
        connect();ASSERT_TRUE(until([&]{return error_contains("receiver source unsolicited, repeated or expired recovery frame");}));
        ASSERT_TRUE(injected);ASSERT_FALSE(before.empty());EXPECT_EQ(snapshot(),before);EXPECT_EQ(phase(),2);
        // The physical connection remains present, but its source authority
        // must be gone: a new upload cannot use the earlier accepted describe.
        EXPECT_TRUE(peers[0].physical.is_current());EXPECT_THROW(synchronizers[0]->sync_now(),db_error);
        const auto drained=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
        EXPECT_EQ(drained.state,sync_drain_state::failed);EXPECT_TRUE(drained.error);
        EXPECT_EQ(snapshot(),before);EXPECT_TRUE(observed_uploads.empty());
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    }
};
TEST_F(PendingRepeatedDescribeController, ActualDescriptionDuringPrepareRevokesCurrentSourceWithoutDurableChange) {
    refuses_actual_description("prepare");
}
TEST_F(PendingRepeatedDescribeController, ActualDescriptionDuringReadRevokesCurrentSourceWithoutDurableChange) {
    refuses_actual_description("read");
}
TEST_F(LateLifecycleReceiverController, ActualThirtySecondTimeoutGapDisposesLateDiscardBeforeFreshCorrelatedRetry) {
    configure(2);seed_local(1,9510);reopen_after_terminal_prepare(1);ASSERT_FALSE(HasFatalFailure());
    auto gap=std::make_shared<ControllerPause>(),canceled=std::make_shared<ControllerPause>();pauses.push_back(gap);pauses.push_back(canceled);
    auto timed_out=std::make_shared<std::atomic<bool>>(false);auto discarded=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[=](const char* stage){
        if(std::strcmp(stage,"pending-expired-before-successor")==0&&!timed_out->exchange(true))gap->wait();
        if(std::strcmp(stage,"late-lifecycle-discarded")==0)++*discarded;
        if(std::strcmp(stage,"terminal-cancel-committed")==0)canceled->wait();});
    std::string late;json original;std::chrono::steady_clock::time_point dropped_at;
    after_control=[&](size_t index,const json& control,std::string& outgoing){
        if(late.empty()&&index==0&&control.at("operation")=="discard"){
            const auto actual=json::parse(outgoing);ASSERT_EQ(actual.at("settlement").at("state"),"committed");
            ASSERT_EQ(actual.at("lifecycle").at("state"),"terminal");
            late=outgoing;original=control;dropped_at=std::chrono::steady_clock::now();outgoing.clear();}};
    connect();ASSERT_TRUE(until([&]{return !late.empty();}));const auto sent=controls.size();
    const auto intent=entries(),allocation=allocators(),old=framing(),audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto endpoint=peers[0].physical;const auto source_view=peers[0].setup.descriptor();
    // Unchanged production deadline and native pacer: no close, request,
    // synthetic scheduler wake, shortened timeout, or altered authorization.
    ASSERT_TRUE(until([&]{return gap->ready();},45000));
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-dropped_at).count(),29000);
    EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),old);EXPECT_EQ(discarded->load(),0u);
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(late)));gap->release();
    ASSERT_TRUE(until([&]{return canceled->ready();}));EXPECT_EQ(discarded->load(),1u);EXPECT_FALSE(has_error());
    EXPECT_EQ(phase(),1);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);EXPECT_EQ(framing(),old);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
    unsigned retries=0;for(size_t i=sent;i<controls.size();++i){const auto& [index,control]=controls[i];
        if(index==0&&control.at("operation")=="discard"){++retries;EXPECT_NE(control.at("requestID"),original.at("requestID"));
            EXPECT_EQ(control.at("routeGeneration"),original.at("routeGeneration"));EXPECT_EQ(control.at("request"),original.at("request"));}}
    EXPECT_EQ(retries,1u);EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),source_view);
    canceled->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);
}
TEST_F(LateLifecycleReceiverController, LateReservationRacingOutgoingBuildDefersUnsentHandoffUnderSharedBudget) {
    configure();seed_local(1,9520);reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    auto gap=std::make_shared<ControllerPause>();pauses.push_back(gap);
    auto armed=std::make_shared<std::atomic<bool>>(false),paused=std::make_shared<std::atomic<bool>>(false);
    auto deferred=std::make_shared<std::atomic<unsigned>>(0),discarded=std::make_shared<std::atomic<unsigned>>(0);
    std::string late;
    after_control=[&](size_t,const json& control,std::string& outgoing){if(control.at("operation")=="inspect"){late=outgoing;armed->store(true);}};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[=](const char* stage){
        if(std::strcmp(stage,"outgoing-built-before-publication")==0&&armed->load()&&!paused->exchange(true))gap->wait();
        if(std::strcmp(stage,"late-control-handoff-deferred")==0)++*deferred;
        if(std::strcmp(stage,"late-lifecycle-discarded")==0)++*discarded;});
    connect();ASSERT_TRUE(until([&]{return gap->ready();}));ASSERT_FALSE(late.empty());
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    const auto q=receiver->db().query("SELECT request_frame FROM _lattice_recovery_request ORDER BY channel");
    const auto endpoint=peers[0].physical;const auto source_view=peers[0].setup.descriptor();
    unsigned before=0;for(const auto& [_,control]:controls)if(control.at("operation")=="prepare")++before;ASSERT_EQ(before,1u);
    // Fill both late slots while the unsent outgoing request owns its byte
    // reservation. Both copies must drain before that request can be sent.
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(late)));
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(late)));gap->release();
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(deferred->load(),1u);EXPECT_EQ(discarded->load(),2u);EXPECT_FALSE(has_error());
    EXPECT_EQ(receiver->db().query("SELECT request_frame FROM _lattice_recovery_request ORDER BY channel"),q);
    EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),source_view);
    unsigned after=0;for(const auto& [_,control]:controls)if(control.at("operation")=="prepare")++after;EXPECT_EQ(after,2u);
}
TEST_F(LateLifecycleReceiverController, QueuedLatePayloadFromActuallyRetiredPhysicalViewIsInert) {
    configure();seed_local(1,9530);
    auto held=std::make_shared<ControllerPause>();pauses.push_back(held);auto retired=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[retired](const char* stage){
        if(std::strcmp(stage,"late-lifecycle-retired-disposed")==0)++*retired;});
    connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(has_error());
    receiver->get_scheduler()->invoke([held]{held->wait();});ASSERT_TRUE(until([&]{return held->ready();}));
    const auto before=snapshot();const auto endpoint=peers[0].physical;
    // This shape-invalid control would fail a current view; it must never be
    // parsed after this real endpoint's close revokes the queued view.
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string("{\"kind\":\"recoveryReady\",\"operation\":\"inspect\"}")));
    ASSERT_TRUE(endpoint.trigger_on_close(1000,"retire actual late-control view"));held->release();
    ASSERT_TRUE(until([&]{return retired->load()==1;}));EXPECT_FALSE(has_error());EXPECT_EQ(snapshot(),before);
    EXPECT_FALSE(endpoint.trigger_on_message(transport_message::from_string("{\"kind\":\"recoveryReady\"}")));
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class IdleLateLifecycleReceiverController : public TerminalReceiverController {
protected:
    std::string actual_describe;
    std::shared_ptr<std::atomic<unsigned>> rejected=std::make_shared<std::atomic<unsigned>>(0),retired=std::make_shared<std::atomic<unsigned>>(0);
    void start_idle(const std::shared_ptr<ControllerPause>& rejection_pause={}) {
        configure();seed_local(1,9540);hold_uploads=false;
        after_control=[this](size_t,const json& control,std::string& outgoing){if(control.at("operation")=="describe")actual_describe=outgoing;};
        const auto rejected_count=rejected,retired_count=retired;
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),worker_observer(true,[=](const char* stage){
            if(std::strcmp(stage,"late-lifecycle-validation-rejected")==0){++*rejected_count;if(rejection_pause)rejection_pause->wait();}
            if(std::strcmp(stage,"late-lifecycle-retired-disposed")==0)++*retired_count;}));
        // The first install releases the UNSENT original. Its real upload
        // and legacy ACK request the second, receipt-bearing installation.
        connect();ASSERT_TRUE(until([&]{worker_trace->begin_predicate();return worker_trace->value(2,installed(2))&&worker_trace->value(3,scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2"))==1;}));
        worker_trace.reset(); // Later lifecycle callbacks keep their original restrictions only.
        ASSERT_FALSE(has_error());ASSERT_FALSE(actual_describe.empty());ASSERT_EQ(observed_uploads.size(),1u);
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)>0"),1);
        ASSERT_EQ(json::parse(full_requests.at(0).at("request").get<std::string>()).at("latticeCanonicalRange").at("attempt").at("sequence"),"2");
    }
    std::string actual_inspect() {
        const auto& q=full_requests.at(0);const auto command=json{{"kind","recoveryReady"},{"version",1},{"operation","inspect"},
            {"requestID",::lattice::uuid_t::generate().to_string()},{"routeGeneration",q.at("routeGeneration")},{"request",q.at("request")}}.dump();
        auto charge=peers[0].setup.stop_token().reserve_ready(command.size());
        if(!charge.valid())throw db_error("idle lifecycle fixture inspect reservation refused");
        const auto response=peers[0].setup.ready(command,charge);
        if(response.status_code()!=1||!response.publishable())throw db_error("idle lifecycle fixture inspect unavailable");
        const auto request=json::parse(q.at("request").get<std::string>()).at("latticeCanonicalRange");const auto actual=json::parse(response.wire());
        if(actual.at("settlement").at("state")!="committed"||actual.at("lifecycle").at("sequence")!="2"||
           actual.at("lifecycle").at("bindingHighWater")!="2"||actual.at("lifecycle").at("attemptID")!=request.at("attempt").at("attempt_id")||
           actual.at("lifecycle").at("requestDigest")!=request.at("body").at("request_digest"))
            throw db_error("idle lifecycle fixture actual source did not inspect settled Q2");return response.wire();
    }
    void verifies_upload_fence(const Snapshot& before,size_t sent) {
        ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),0);EXPECT_EQ(snapshot(),before);
        // This allowed local write creates fresh ordinary demand after the
        // malformed source was revoked. It may not claim or send an original.
        seed_local(1,9541);const auto after_write=snapshot();
        EXPECT_THROW(synchronizers[0]->sync_now(),db_error);
        const auto drain=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
        EXPECT_EQ(drain.state,sync_drain_state::failed);EXPECT_TRUE(drain.error);
        auto barrier=std::make_shared<std::promise<void>>();auto completed=barrier->get_future();
        receiver->get_scheduler()->invoke([barrier]{barrier->set_value();});
        ASSERT_EQ(completed.wait_for(std::chrono::seconds(2)),std::future_status::ready);
        for(unsigned n=0;n<64&&pump();++n){}
        EXPECT_EQ(snapshot(),after_write);EXPECT_EQ(observed_uploads.size(),sent);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2"),1);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
        EXPECT_TRUE(peers[0].physical.is_current());
    }
};
TEST_F(IdleLateLifecycleReceiverController, InstalledMalformedLateLifecycleRevokesActualOrdinaryExportView) {
    start_idle();ASSERT_FALSE(HasFatalFailure());auto invalid=json::parse(actual_inspect());invalid["lifecycle"]["bindingHighWater"]="01";
    const auto before=snapshot();const auto sent=observed_uploads.size();
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(invalid.dump())));
    verifies_upload_fence(before,sent);EXPECT_EQ(rejected->load(),1u);
}
TEST_F(IdleLateLifecycleReceiverController, InstalledRepeatedDescribeRevokesActualOrdinaryExportView) {
    start_idle();ASSERT_FALSE(HasFatalFailure());const auto before=snapshot();const auto sent=observed_uploads.size();
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(actual_describe)));
    verifies_upload_fence(before,sent);EXPECT_EQ(rejected->load(),1u);
}
TEST_F(IdleLateLifecycleReceiverController, InstalledWrongLateBindingRevokesActualOrdinaryExportView) {
    start_idle();ASSERT_FALSE(HasFatalFailure());auto invalid=json::parse(actual_inspect());invalid["lifecycle"]["namespaceID"]="b";
    const auto before=snapshot();const auto sent=observed_uploads.size();
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(invalid.dump())));
    verifies_upload_fence(before,sent);EXPECT_EQ(rejected->load(),1u);
}
TEST_F(IdleLateLifecycleReceiverController, InstalledOversizedLateAdmissionRevokesActualOrdinaryExportView) {
    start_idle();ASSERT_FALSE(HasFatalFailure());const auto before=snapshot();const auto sent=observed_uploads.size();
    std::string oversized="{\"kind\":\"recoveryReady\",\"padding\":\"";oversized.append(4194304,'x');oversized+="\"}";
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(oversized)));
    verifies_upload_fence(before,sent);EXPECT_EQ(rejected->load(),0u); // rejected before worker parsing/copy
}
TEST_F(IdleLateLifecycleReceiverController, InstalledThirdLateSlotAdmissionRevokesActualOrdinaryExportView) {
    start_idle();ASSERT_FALSE(HasFatalFailure());const auto actual=actual_inspect();const auto before=snapshot();const auto sent=observed_uploads.size();
    auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);
    receiver->get_scheduler()->invoke([pause]{pause->wait();});ASSERT_TRUE(until([&]{return pause->ready();}));
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(actual)));
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(actual)));
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(actual)));pause->release();
    verifies_upload_fence(before,sent);EXPECT_EQ(rejected->load(),0u); // full global inbox refused on callback
}
TEST_F(IdleLateLifecycleReceiverController, ActualPhysicalRetirementAfterLateParseFailureDoesNotPoisonSuccessor) {
    auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);start_idle(pause);ASSERT_FALSE(HasFatalFailure());
    const auto before=snapshot();const auto old=peers[0].physical;
    ASSERT_TRUE(old.trigger_on_message(transport_message::from_string(actual_describe)));
    ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(rejected->load(),1u);
    ASSERT_TRUE(old.trigger_on_close(1000,"retire after actual late parse rejection"));pause->release();
    ASSERT_TRUE(until([&]{return retired->load()==1;}));EXPECT_FALSE(has_error());EXPECT_EQ(snapshot(),before);
    EXPECT_FALSE(old.trigger_on_message(transport_message::from_string(actual_describe)));
    // This case isolates physical retirement. Retain the disconnected old
    // synchronizer: its destructor shuts down this shared receiver scheduler.
    for(auto& sync:synchronizers)sync->disconnect();ASSERT_EQ(synchronizers.size(),1u);
    connect();ASSERT_EQ(synchronizers.size(),2u);ASSERT_TRUE(until([&]{return installed(3);}));EXPECT_FALSE(has_error());
    EXPECT_FALSE(old.matches(peers[0].physical));EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=3"),1);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include "RecoveryAdmissionTestRead.hpp"
namespace {
struct ControllerAdmissionRead {
    std::mutex mutex;
    std::shared_ptr<recovery_admission_test::held_read> read;
    bool released=false;
    std::atomic<bool> armed{false};
    std::atomic<unsigned> deferrals{0},consumed{0};
    bool observe_after_deferrals=false; // Set before the controller is created.
    std::atomic<bool> sample_ready{false},sample_ok{false};
    int64_t unconsumed_requests=-1,model_rows=-1; // Published by sample_ready.
    void hold(database& db) {
        if(armed.exchange(true))return;
        std::function<void(database&)> sample;
        if(observe_after_deferrals)sample=[this](database& actual){
            try {
                const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
                while(deferrals.load(std::memory_order_acquire)<3&&std::chrono::steady_clock::now()<end)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                if(deferrals.load(std::memory_order_acquire)<3)throw db_error("fixture held-read deferrals not observed");
                // Same original state oracles, on the genuinely held reader's
                // existing engine frame. A fresh main-thread read must wait.
                unconsumed_requests=std::get<int64_t>(actual.query("SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)=0").at(0).at("n"));
                model_rows=std::get<int64_t>(actual.query("SELECT COUNT(*) AS n FROM ControllerRow").at(0).at("n"));
                sample_ok.store(true,std::memory_order_release);
                sample_ready.store(true,std::memory_order_release);
            }catch(...){sample_ready.store(true,std::memory_order_release);throw;}
        };
        auto value=std::make_shared<recovery_admission_test::held_read>(db,false,"SELECT name FROM sqlite_schema",false,std::move(sample));
        bool finish=false;{std::lock_guard lock(mutex);read=value;finish=released;}
        if(finish)value->finish();
    }
    void finish() {
        std::shared_ptr<recovery_admission_test::held_read> value;
        {std::lock_guard lock(mutex);released=true;value=read;}
        if(value)value->finish();
    }
};
struct ControllerAdmissionReadRelease {
    std::shared_ptr<ControllerAdmissionRead> value;
    ~ControllerAdmissionReadRelease(){value->finish();}
};
TEST_F(RecoveryReceiverController, RealReadDeferralRetainsActualRangeFrameUntilOwnedConsumption) {
    configure();insert(*source,controller_uuid(9200),"source after read");
    auto held=std::make_shared<ControllerAdmissionRead>();held->observe_after_deferrals=true;ControllerAdmissionReadRelease cleanup{held};
    auto installed=std::make_shared<ControllerPause>();pauses.push_back(installed);
    const auto owner=receiver;
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[held,owner,installed](const char* stage){
        if(std::strcmp(stage,"range-response-ready")==0)held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0)++held->deferrals;
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
        if(std::strcmp(stage,"install-committed")==0)installed->wait();
    });
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=3&&held->sample_ready.load(std::memory_order_acquire);}));
    ASSERT_TRUE(held->sample_ok.load(std::memory_order_acquire));
    EXPECT_FALSE(has_error());EXPECT_EQ(held->consumed.load(),0u);
    EXPECT_EQ(held->unconsumed_requests,1);
    EXPECT_EQ(held->model_rows,0);
    const auto requests_before=handled;EXPECT_EQ(requests.size(),1u);
    held->finish();ASSERT_TRUE(until([&]{return installed->ready();}));
    EXPECT_GT(held->consumed.load(),0u);EXPECT_GT(handled,requests_before);EXPECT_EQ(requests.size(),1u);EXPECT_FALSE(has_error());
    EXPECT_EQ(phase(),3);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow WHERE value='source after read'"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1 AND active IS NULL"),1);
    installed->release();ASSERT_TRUE(until([&]{return phase()==0;}));
}
TEST_F(RecoveryReceiverController, QuietReadAdmissionExhaustionIsBoundedAndDoesNotLoseFrozenRequest) {
    configure();insert(*source,controller_uuid(9201),"source remains pending");
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};const auto owner=receiver;
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[held,owner](const char* stage){
        if(std::strcmp(stage,"range-response-ready")==0)held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0)++held->deferrals;
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
    });
    connect();ASSERT_TRUE(until([&]{return has_error();},7000));
    const auto attempts=held->deferrals.load();EXPECT_GT(attempts,1u);EXPECT_LE(attempts,32u);
    EXPECT_EQ(held->consumed.load(),0u);EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)=0"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),0);EXPECT_EQ(requests.size(),1u);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());EXPECT_NE(errors.front().find("admission retry budget exhausted"),std::string::npos);}
    const auto before=handled;held->finish();EXPECT_FALSE(until([&]{return handled!=before;},350));
    EXPECT_EQ(held->deferrals.load(),attempts);EXPECT_EQ(phase(),2);EXPECT_EQ(held->consumed.load(),0u);
}
TEST_F(RecoveryReceiverController, CloseWhileReadAdmissionDeferredCannotConsumeQueuedRange) {
    configure();insert(*source,controller_uuid(9202),"closed receiver source");
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};const auto owner=receiver;
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[held,owner](const char* stage){
        if(std::strcmp(stage,"range-response-ready")==0)held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0)++held->deferrals;
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
    });
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=2;}));
    close_receiver();held->finish();const auto deferrals=held->deferrals.load();
    EXPECT_FALSE(until([&]{return held->consumed.load()!=0||held->deferrals.load()!=deferrals;},350));
    database observer((container/"store.sqlite").string(),database::open_mode::read_only,100);
    EXPECT_EQ(scalar_from(observer,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(scalar_from(observer,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)=0"),1);
    EXPECT_EQ(scalar_from(observer,"SELECT phase AS n FROM _lattice_producer_continuity"),2);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(TerminalReceiverController, RealReadDeferralRetainsFirstRangeWithSecondStaleLifecycleInSharedInbox) {
    configure();insert(*source,controller_uuid(9550),"combined source row");seed_local(1,9551);
    reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};
    auto installed_pause=std::make_shared<ControllerPause>();pauses.push_back(installed_pause);const auto owner=receiver;
    std::string stale,first_range;
    after_control=[&](size_t,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect")stale=outgoing;
        if(control.at("operation")=="read"&&first_range.empty())first_range=outgoing;};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[held,owner,installed_pause](const char* stage){
        if(std::strcmp(stage,"range-response-ready")==0)held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0&&held->armed.load())++held->deferrals;
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
        if(std::strcmp(stage,"install-committed")==0)installed_pause->wait();});
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=1;}));ASSERT_FALSE(has_error());
    ASSERT_FALSE(stale.empty());ASSERT_FALSE(first_range.empty());
    ASSERT_EQ(json::parse(first_range).at("latticeCanonicalRange").at("kind"),"manifest");
    const auto before=snapshot();const auto before_framing=framing();const auto endpoint=peers[0].physical;
    const auto before_q=receiver->db().query("SELECT request_frame FROM _lattice_recovery_request");
    const auto source_view=peers[0].setup.descriptor();const auto handled_before=handled,requests_before=requests.size();
    const auto source_receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt");
    const auto deferrals_before=held->deferrals.load();
    // The exact range already occupies the first slot and remains unconsumed
    // while its real engine read blocks pre-BEGIN installation admission.
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));
    ASSERT_TRUE(until([&]{return held->deferrals.load()>=deferrals_before+3;}));
    EXPECT_FALSE(has_error());EXPECT_EQ(held->consumed.load(),0u);EXPECT_EQ(snapshot(),before);EXPECT_EQ(framing(),before_framing);
    EXPECT_EQ(handled,handled_before);EXPECT_EQ(requests.size(),requests_before);EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt"),source_receipts);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    held->finish();ASSERT_TRUE(until([&]{return installed_pause->ready();}));EXPECT_FALSE(has_error());EXPECT_GT(held->consumed.load(),0u);
    const auto stored=receiver->db().query("SELECT manifest_frame FROM _lattice_recovery_request WHERE sequence=2");ASSERT_EQ(stored.size(),1u);
    const auto& bytes=std::get<std::vector<uint8_t>>(stored[0].at("manifest_frame"));EXPECT_EQ(std::string(bytes.begin(),bytes.end()),first_range);
    EXPECT_EQ(receiver->db().query("SELECT request_frame FROM _lattice_recovery_request"),before_q);
    EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),source_view);EXPECT_EQ(requests.size(),requests_before);
    EXPECT_EQ(phase(),3);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1 AND active IS NULL"),1);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow WHERE value='combined source row'"),1);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before.at("AuditLog"));
    installed_pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(TerminalReceiverController, LateOnlyDrainKeepsOriginalReadAdmissionDeadlineAfterPendingNullGap) {
    configure();insert(*source,controller_uuid(9560),"unchanged behind exhausted admission");seed_local(1,9561);
    reopen_after_terminal_prepare();ASSERT_FALSE(HasFatalFailure());
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};const auto owner=receiver;
    auto arm=std::make_shared<std::atomic<bool>>(false);auto first_deferral_ms=std::make_shared<std::atomic<int64_t>>(0);
    auto late_count=std::make_shared<std::atomic<unsigned>>(0);
    auto first_late=std::make_shared<ControllerPause>(),second_late=std::make_shared<ControllerPause>();pauses.push_back(first_late);pauses.push_back(second_late);
    std::string stale;
    after_control=[&](size_t,const json& control,std::string& outgoing){
        if(control.at("operation")=="inspect")stale=outgoing;
        if(!stale.empty()&&control.at("operation")=="prepare")arm->store(true);};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[=](const char* stage){
        // Actual correlated prepare has been consumed and outstanding is null.
        // The next owned phase inspection must encounter this real engine read.
        if(std::strcmp(stage,"pending-consumed-before-successor")==0&&arm->load())held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0&&held->armed.load()&&held->deferrals.fetch_add(1)==0)
            first_deferral_ms->store(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
        if(std::strcmp(stage,"late-lifecycle-discarded")==0){const auto n=late_count->fetch_add(1)+1;
            if(n==1)first_late->wait();if(n==2)second_late->wait();}});
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=12;}));ASSERT_FALSE(has_error());ASSERT_FALSE(stale.empty());
    ASSERT_GT(first_deferral_ms->load(),0);ASSERT_EQ(held->consumed.load(),0u);
    const auto before=snapshot();const auto old=framing();const auto handled_before=handled,requests_before=requests.size();
    const auto source_receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt");
    const auto endpoint=peers[0].physical;const auto source_view=peers[0].setup.descriptor();
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));ASSERT_TRUE(until([&]{return first_late->ready();}));
    const auto spent=held->deferrals.load();EXPECT_GE(spent,12u);EXPECT_LE(spent,32u);EXPECT_FALSE(has_error());
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));first_late->release();
    ASSERT_TRUE(until([&]{return second_late->ready();}));EXPECT_EQ(held->deferrals.load(),spent);
    // Twelve real >=100ms-spaced deferrals have already spent >1s of the
    // episode, leaving <4.1s here (within the unchanged five-second pause).
    // Keep the real read held until the ORIGINAL five-second window expires.
    const auto after_original_deadline=std::chrono::steady_clock::time_point(std::chrono::milliseconds(first_deferral_ms->load()+5100));
    const auto pause_remaining=std::chrono::duration_cast<std::chrono::milliseconds>(after_original_deadline-std::chrono::steady_clock::now()).count();
    ASSERT_LT(pause_remaining,4500);if(pause_remaining>0)std::this_thread::sleep_until(after_original_deadline);
    // With the second front still retained, only one other slot is available.
    // The third reply forces this two-frame worker quantum to return before
    // admission checks. Its next turn must retain the original expired episode.
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));second_late->release();
    ASSERT_TRUE(until([&]{return has_error()||held->deferrals.load()!=spent;},2000));
    ASSERT_TRUE(has_error());EXPECT_EQ(held->deferrals.load(),spent);EXPECT_LE(held->deferrals.load(),32u);EXPECT_EQ(late_count->load(),3u);
    const auto elapsed=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count()-first_deferral_ms->load();
    EXPECT_GE(elapsed,5100);EXPECT_LE(elapsed,7500);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());EXPECT_NE(errors.front().find("admission retry budget exhausted"),std::string::npos);}
    EXPECT_EQ(phase(),2);EXPECT_EQ(snapshot(),before);EXPECT_EQ(framing(),old);EXPECT_EQ(held->consumed.load(),0u);
    EXPECT_EQ(handled,handled_before);EXPECT_EQ(requests.size(),requests_before);EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt"),source_receipts);
    EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),source_view);
    held->finish();EXPECT_FALSE(until([&]{return handled!=handled_before||held->deferrals.load()!=spent||held->consumed.load()!=0;},350));
    EXPECT_TRUE(has_error());EXPECT_EQ(snapshot(),before);EXPECT_EQ(phase(),2);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class PacerCallbackSynchronizer final : public synchronizer {
public:
    using synchronizer::synchronizer;
    auto discovery_observation()const{return std::make_pair(discovery_deferral_,reconnect_lifecycle_.load());}
};
struct PacerCallbackOwner {
    std::mutex mutex;std::unique_ptr<PacerCallbackSynchronizer> sync;
    void retire(){std::unique_ptr<PacerCallbackSynchronizer> old;{std::lock_guard lock(mutex);old=std::move(sync);}old.reset();}
    ~PacerCallbackOwner(){retire();}
};
struct PacerCallbackObservation {
    std::shared_ptr<ControllerPause> pause=std::make_shared<ControllerPause>();
    std::atomic<bool> armed{false},cancelled{false};std::atomic<unsigned> ticks{0};
    std::thread::id tick_thread;std::promise<void> completed,pacer_exited;
    std::mutex mutex;std::vector<std::string> errors,received;
    bool same_tick=false,capture_alive=false;unsigned capture_uses=0;
};
struct PacerCallbackCapture {unsigned uses=0;};
struct ObservedReceiverPacerSchedule : detail::sync_background_test_hooks::pacer_wait_schedule {
    std::shared_ptr<PacerCallbackObservation> observation;
    ~ObservedReceiverPacerSchedule(){observation->pacer_exited.set_value();}
};
struct PacerCallbackCleanup {
    std::shared_ptr<PacerCallbackOwner> owner;std::shared_ptr<PacerCallbackObservation> observation;
    ~PacerCallbackCleanup(){observation->cancelled.store(true);observation->pause->release();owner->retire();}
};
TEST_F(RecoveryReceiverController, ActualDueReceiverPacerRetiresDuringDeferredPhysicalAckCompletion) {
    configure(1,false);
    // Select the real public inline scheduler at factory creation. The native
    // synchronizer adapter keeps idle invokes on the actual pacer thread.
    swift_configuration configuration((container/"store.sqlite").string(),std::make_shared<immediate_scheduler>());
    configuration.audit_retention_seconds=0;configuration.busy_timeout_ms=100;continuous_result result;
#if LATTICE_HAS_FRT
    receiver_ref.reset(swift_lattice_ref::create_continuous(configuration,{controller_schema()},policy,result));
#else
    receiver_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create_continuous(configuration,{controller_schema()},policy,result));
#endif
    ASSERT_EQ(result.phase(),2);ASSERT_FALSE(result.has_error());
    receiver=swift_lattice_ref::shared_for_lattice(receiver_ref->get());ASSERT_TRUE(receiver);
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(receiver->config().path))notifier->stop_listening();
    auto observation=std::make_shared<PacerCallbackObservation>();auto done=observation->completed.get_future();
    auto exited=observation->pacer_exited.get_future();auto owner=std::make_shared<PacerCallbackOwner>();
    PacerCallbackCleanup cleanup{owner,observation};
    auto schedule=std::make_shared<ObservedReceiverPacerSchedule>();schedule->observation=observation;
    schedule->before_receiver_discovery=[observation]{if(!observation->armed.exchange(false))return;
        observation->tick_thread=std::this_thread::get_id();++observation->ticks;observation->pause->wait();};
    sync_config config;config.websocket_url=peers[0].endpoint;config.authorization_token="registered-token";
    config.sync_id=peers[0].channel;config.all_active_sync_ids={config.sync_id};
    config.recovery_source_expectation=peers[0].expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;
    {
        struct Restore {std::shared_ptr<const detail::sync_background_test_hooks::pacer_wait_schedule> prior;
            ~Restore(){detail::sync_background_test_hooks::pacer_wait=std::move(prior);}} restore{detail::sync_background_test_hooks::pacer_wait};
        detail::sync_background_test_hooks::pacer_wait=schedule;
        owner->sync=std::make_unique<PacerCallbackSynchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
    }
    schedule.reset(); // Only the actual native pacer now owns this exit witness.
    auto* const actual=owner->sync.get();
    actual->set_on_error([observation](const auto& error){std::lock_guard lock(observation->mutex);observation->errors.push_back(error);});
    actual->connect();const auto [queue,generation]=actual->discovery_observation();
    ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(1,9801);
    const auto ids=originals();ASSERT_EQ(ids.size(),1u);ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    ASSERT_EQ(held_uploads.size(),1u);const auto endpoint=peers[0].physical;
    auto accepted=peers[0].setup.receive(held_uploads[0].raw);ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.take_ids(),ids);
    const auto before=snapshot();const auto source_rows=source->db().query("SELECT * FROM ControllerRow ORDER BY id");
    const auto receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt");
    ASSERT_FALSE(queue->pending(generation));ASSERT_EQ(actual->get_progress().pending_upload,1);
    auto capture=std::make_shared<PacerCallbackCapture>();const std::weak_ptr<PacerCallbackCapture> weak_capture=capture;
    const std::weak_ptr<PacerCallbackOwner> weak_owner=owner;
    actual->set_on_sync_complete([observation,weak_owner,weak_capture,capture=std::move(capture)](const auto& received) {
        // These independent locals survive destruction of the synchronizer and
        // its member handler. A missing invocation copy expires weak_capture.
        const auto report=observation;const auto watched=weak_capture;const auto retiring=weak_owner.lock();
        if(report->cancelled.load())return;
        report->same_tick=std::this_thread::get_id()==report->tick_thread;report->received=received;
        if(retiring)retiring->retire();
        report->capture_alive=!watched.expired();
        if(report->capture_alive)report->capture_uses=++capture->uses;
        report->completed.set_value();
    });
    observation->armed.store(true);
    ASSERT_TRUE(until([&]{return observation->pause->ready();}));ASSERT_EQ(observation->ticks.load(),1u);
    {
        FirstClaimWriterHold held(*receiver);legacy_ack(0,ids);
        // The real physical ACK's initial writer probe is deferred, not a
        // fixture-created operation or synthetic discovery completion.
        ASSERT_TRUE(queue->pending(generation));EXPECT_EQ(done.wait_for(std::chrono::milliseconds(0)),std::future_status::timeout);
        const auto due=queue->wake_at();ASSERT_LE(due,std::chrono::steady_clock::now()+std::chrono::milliseconds(100));
        held.release();EXPECT_FALSE(held.timed_out);
        std::this_thread::sleep_until(due); // Preserve the actual five-ms first retry.
    }
    observation->pause->release();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(5)),std::future_status::ready);
    ASSERT_EQ(exited.wait_for(std::chrono::seconds(5)),std::future_status::ready);
    EXPECT_TRUE(observation->same_tick);EXPECT_TRUE(observation->capture_alive);EXPECT_EQ(observation->capture_uses,1u);
    EXPECT_EQ(observation->received,ids);EXPECT_EQ(observation->ticks.load(),1u);EXPECT_FALSE(observation->pause->timedOut());
    EXPECT_TRUE(weak_capture.expired());{std::lock_guard lock(owner->mutex);EXPECT_FALSE(owner->sync);}
    EXPECT_FALSE(endpoint.is_current());EXPECT_FALSE(endpoint.trigger_on_message(transport_message::from_string(server_sent_event::make_ack(ids).to_json())));
    EXPECT_FALSE(queue->pending(generation));EXPECT_EQ(snapshot(),before);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),1);
    EXPECT_EQ(source->db().query("SELECT * FROM ControllerRow ORDER BY id"),source_rows);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt"),receipts);
    {std::lock_guard lock(observation->mutex);EXPECT_TRUE(observation->errors.empty());}
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include "CanonicalReadyAdoptionTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_ready_named_profile.hpp"
namespace {
class PredecessorReceiverController : public TerminalReceiverController {
protected:
    bool small=false,adopted=false;
    json source_policy(const std::string& ns)override {
        auto p=RecoveryReceiverController::source_policy(ns);
        p["readyProfile"]=small?(adopted?"boundedV1OrphanV1":"boundedV1"):(adopted?"bounded48MiBOrphanV1":"bounded48MiBV1");
        // The historical small profile is selected only by omission.
        if(small&&!adopted)p.erase("readyProfile");
        if(adopted)p["orphanResumeGraceMilliseconds"]=60000;return p;
    }
    void adopt_source() {
        const auto s=peers.at(0).expectation.at("source");
        close_receiver();probe.reset();
        for(auto& peer:peers){peer.live->store(false);peer.setup.close_on_io();peer.setup={};peer.physical={};}
        {std::lock_guard lock(wire->mutex);wire->dials.clear();wire->frames.clear();wire->endpoints.clear();}
        detail::canonical_namespaced_writer_profile p;
        p.writer.binding={s.at("sourceID"),s.at("epoch"),s.at("scopeDigest"),s.at("schemaDigest")};
        p.writer.limits={65536,16777216,65536,16777216,256,128,64};p.writer.models={"ControllerRow"};p.writer.upstream_requested=true;
        p.namespaces.local_namespace="local";p.namespaces.entries={{"a","a-v1",1},{"b","b-v1",1},{"local","local-v1",1}};
        const std::string name=small?"boundedV1":"bounded48MiBV1";
        const auto result=detail::adopt_ready_lifecycle_for_test(source,p,{256,65536,1048576},{64,3600000},
            detail::canonical_named_ready_profile(s.at("authority"),p.writer.limits,false,name),name,60000);
        ASSERT_EQ(result.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(result.record);
        adopted=true;for(auto& peer:peers)peer.live=std::make_shared<std::atomic<bool>>(true);
        before_control={};after_control={};drop_prepare=false;open_receiver();
    }
    void retain_pre_manifest(bool unstarted=false) {
        if(unstarted)before_control=[](size_t,const json& c){return c.at("operation")=="prepare";};else drop_prepare=true;
        connect();ASSERT_TRUE(until([&]{return unstarted?!full_requests.empty():dropped==1;}));ASSERT_EQ(phase(),2);
        ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)=0"),static_cast<int64_t>(peers.size()));
    }
    std::vector<database::row_t> immutable_q(){return receiver->db().query("SELECT channel,sequence,request_frame,source_context FROM _lattice_recovery_request ORDER BY channel");}
    unsigned proof_controls()const{unsigned n=0;for(const auto& [_,c]:controls)if(c.at("operation")=="predecessor")++n;return n;}
    void terminal_adopted() {
        configure();seed_local(1,9700);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
        reopen_after_terminal([](size_t,const json& c){return c.at("operation")=="predecessor";});
    }
};
TEST_F(PredecessorReceiverController, ActualSmallCompletedPreManifestReopenPreservesQAndInstallsWithProof) {
    small=true;configure();insert(*source,controller_uuid(9701),"canonical");seed_local(1,9702);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());
    const auto q=immutable_q(),audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto frames=source->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");ASSERT_FALSE(frames.empty());
    adopt_source();ASSERT_FALSE(HasFatalFailure());EXPECT_EQ(immutable_q(),q);EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),frames);
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());EXPECT_EQ(proof_controls(),1u);EXPECT_EQ(immutable_q(),q);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
}
TEST_F(PredecessorReceiverController, ActualLargeUnstartedQRequiresProofThenPreparesSameIdentityWithoutHistoricalClaim) {
    configure();seed_local(1,9710);retain_pre_manifest(true);ASSERT_FALSE(HasFatalFailure());const auto q=immutable_q();
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding"),0);
    adopt_source();ASSERT_FALSE(HasFatalFailure());connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(proof_controls(),1u);EXPECT_EQ(immutable_q(),q);EXPECT_FALSE(has_error());
    unsigned inspected=0,discarded=0;for(const auto& [_,c]:controls){inspected+=c.at("operation")=="inspect";discarded+=c.at("operation")=="discard";}
    EXPECT_EQ(inspected,1u);EXPECT_EQ(discarded,0u);EXPECT_EQ(scalar(*receiver,"SELECT sequence AS n FROM _lattice_recovery_request"),1);
}
TEST_F(PredecessorReceiverController, ActualLargeMixedVerifiedAndPartialCohortRequiresBothCurrentProofs) {
    configure(2);insert(*source,controller_uuid(9720),"canonical");seed_local(1,9721);bool held=false;
    before_control=[&](size_t i,const json& c){if(i==1&&c.at("operation")=="read"&&c.at("index")=="2"){held=true;return true;}return false;};
    connect();ASSERT_TRUE(until([&]{return held;}));ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1"),1);
    const auto q=immutable_q(),manifests=receiver->db().query("SELECT manifest_frame FROM _lattice_recovery_request ORDER BY channel");
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");adopt_source();ASSERT_FALSE(HasFatalFailure());connect();
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());EXPECT_EQ(proof_controls(),2u);EXPECT_EQ(immutable_q(),q);
    EXPECT_EQ(receiver->db().query("SELECT manifest_frame FROM _lattice_recovery_request ORDER BY channel"),manifests);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);
}
TEST_F(PredecessorReceiverController, KnownInstalledPhaseThreeResumesWithoutFreshProofOrCapsuleDependency) {
    configure();insert(*source,controller_uuid(9730),"canonical");seed_local(1,9731);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"install-committed")==0)throw db_error("stop after installed COMMIT");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),3);
    const auto q=immutable_q(),rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),installed_rows=receiver->db().query("SELECT * FROM _lattice_install_channel");
    adopt_source();ASSERT_FALSE(HasFatalFailure());ASSERT_EQ(phase(),3);
    before_control=[](size_t,const json& c){EXPECT_NE(c.at("operation"),"predecessor");return c.at("operation")=="predecessor";};
    bool disposed=false;after_control=[&](size_t index,const json& c,std::string& raw){if(!disposed&&c.at("operation")=="describe"){
        const auto d=json::parse(raw);auto& submitted=full_requests.at(index);auto frame=json::parse(submitted.at("request").get<std::string>());
        frame["latticeCanonicalRange"]["route_generation"]=d.at("routeGeneration");submitted["request"]=frame.dump();submitted["routeGeneration"]=d.at("routeGeneration");
        (void)discard(index);disposed=true;}};
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());EXPECT_EQ(proof_controls(),0u);EXPECT_TRUE(disposed);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);EXPECT_EQ(immutable_q(),q);
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_install_channel"),installed_rows);
}
TEST_F(PredecessorReceiverController, ExactInstalledPredecessorAtNextBarrierSkipsProofAndCreatesFreshTargetQ) {
    configure();insert(*source,controller_uuid(9740),"canonical");connect();ASSERT_TRUE(until([&]{return installed();}));
    close_receiver();open_receiver();probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"barrier-committed")==0)throw db_error("stop at next barrier");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),1);adopt_source();ASSERT_FALSE(HasFatalFailure());
    before_control=[](size_t,const json& c){EXPECT_NE(c.at("operation"),"predecessor");return c.at("operation")=="predecessor";};
    connect();ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_EQ(proof_controls(),0u);EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT sequence AS n FROM _lattice_recovery_request"),2);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
}
TEST_F(PredecessorReceiverController, ActualPhaseFourReopenReacquiresProofBeforeRestrictedExportAndPreservesClaims) {
    configure();auto stop=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[stop](const char* stage){if(stop->load()&&std::strcmp(stage,"reconcile-cancel-committed")==0)throw db_error("stop after UNKNOWN cancellation");});
    connect();ASSERT_TRUE(until([&]{return installed();}));seed_local(3,9750);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    stop->store(true);held_uploads.clear();legacy_ack(0,ids);ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),4);
    const auto q=immutable_q();adopt_source();ASSERT_FALSE(HasFatalFailure());EXPECT_EQ(phase(),4);EXPECT_EQ(immutable_q(),q);
    hold_uploads=false;connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==3;}));
    EXPECT_GT(proof_controls(),0u);EXPECT_FALSE(has_error());EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),3);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),3);
}
TEST_F(PredecessorReceiverController, GoneAdoptedCapsuleUsesSeparateTerminalFenceBeforeRefreeze) {
    terminal_adopted();ASSERT_FALSE(HasFatalFailure());const auto q=immutable_q(),intent=entries(),allocation=allocators();const auto pause=pause_terminal();
    connect();ASSERT_TRUE(until([&]{return pause->ready();}));EXPECT_EQ(phase(),1);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);pause->release();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_GE(proof_controls(),3u);EXPECT_EQ(scalar(*receiver,"SELECT sequence AS n FROM _lattice_recovery_request"),2);EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, LostTerminalCancelCommitReopensAndReacquiresProofForCanceledOldQ) {
    terminal_adopted();ASSERT_FALSE(HasFatalFailure());probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"terminal-cancel-committed")==0)throw db_error("stop after terminal cancel");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),1);const auto intent=entries(),q=immutable_q();const auto prior=proof_controls();
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);connect();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_GT(proof_controls(),prior);EXPECT_EQ(scalar(*receiver,"SELECT sequence AS n FROM _lattice_recovery_request"),2);EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, LostRefreezeCommitReopensWithCanceledOldQAndExactOriginals) {
    terminal_adopted();ASSERT_FALSE(HasFatalFailure());probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char* stage){if(std::strcmp(stage,"terminal-refreeze-committed")==0)throw db_error("stop after refreeze");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_EQ(phase(),2);const auto intent=entries(),q=immutable_q();const auto prior=proof_controls();
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);connect();ASSERT_TRUE(until([&]{return installed();}));
    EXPECT_GT(proof_controls(),prior);EXPECT_EQ(scalar(*receiver,"SELECT sequence AS n FROM _lattice_recovery_request"),2);EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, QuietLostProofRetriesSameQAtUnchangedThirtySecondDeadline) {
    configure();retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());bool lost=false;
    std::chrono::steady_clock::time_point dropped_at;std::string id,request;
    after_control=[&](size_t,const json& c,std::string& raw){if(!lost&&c.at("operation")=="predecessor"){lost=true;dropped_at=std::chrono::steady_clock::now();id=c.at("requestID");request=c.at("request");raw.clear();}};
    connect();ASSERT_TRUE(until([&]{return lost;}));const auto q=immutable_q();const auto endpoint=peers[0].physical;
    ASSERT_TRUE(until([&]{return installed();},45000));EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-dropped_at).count(),29000);
    EXPECT_EQ(proof_controls(),2u);EXPECT_EQ(immutable_q(),q);EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_FALSE(has_error());
    for(const auto& [_,c]:controls)if(c.at("operation")=="predecessor"&&c.at("requestID")!=id)EXPECT_EQ(c.at("request"),request);
}
TEST_F(PredecessorReceiverController, WrongProofQCannotAuthorizeAnyInstallOrRestrictedExport) {
    configure();seed_local(1,9760);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());const auto q=immutable_q(),intent=entries();
    after_control=[](size_t,const json& c,std::string& raw){if(c.at("operation")=="predecessor"){auto reply=json::parse(raw);reply["predecessor"]["requestDigest"]=std::string(64,'0');raw=reply.dump();}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);EXPECT_TRUE(observed_uploads.empty());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);EXPECT_THROW(insert(*receiver,controller_uuid(9761),"closed"),db_error);
}
TEST_F(PredecessorReceiverController, RealReadDeferralRetainsExactProofAndOnePendingControlUntilOwnedConsume) {
    configure();seed_local(1,9770);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};const auto owner=receiver;auto installed_pause=std::make_shared<ControllerPause>();pauses.push_back(installed_pause);
    auto consumed=std::make_shared<std::atomic<unsigned>>(0);probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[held,owner,consumed,installed_pause](const char* stage){
        if(std::strcmp(stage,"control-response-ready")==0)held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0&&held->armed.load())++held->deferrals;
        if(std::strcmp(stage,"predecessor-consumed")==0)++*consumed;
        if(std::strcmp(stage,"install-committed")==0)installed_pause->wait();});
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=3;}));EXPECT_EQ(consumed->load(),0u);EXPECT_EQ(proof_controls(),1u);EXPECT_FALSE(has_error());
    const auto q=immutable_q(),intent=entries();held->finish();ASSERT_TRUE(until([&]{return installed_pause->ready();}));EXPECT_EQ(consumed->load(),1u);EXPECT_EQ(proof_controls(),1u);
    EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);EXPECT_FALSE(has_error());installed_pause->release();ASSERT_TRUE(until([&]{return installed();}));
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(PredecessorReceiverController, EveryTypedProofBindingRefusesWithoutRewritingTheRetainedRequest) {
    configure();seed_local(1,9820);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
    const std::vector<std::pair<std::string,json>> changes={{"version",1.0},{"version",true},{"namespaceID","b"},{"replicaID","other"},{"receiverIncarnation",controller_uuid(99)},
        {"channelIncarnation",controller_uuid(98)},{"channel","other"},{"attemptID",controller_uuid(97)},{"sequence","2"},{"sequence","01"},{"afterProfileDigest",std::string(64,'0')},
        {"beforeProfileDigest",std::string(64,'0')},{"disposition","discardCompleted"},{"unknown",false}};
    for(const auto& change:changes){SCOPED_TRACE(change.first+":"+change.second.dump());const auto q=immutable_q(),intent=entries();
        after_control=[change](size_t,const json& c,std::string& raw){if(c.at("operation")=="predecessor"){auto reply=json::parse(raw);reply["predecessor"][change.first]=change.second;raw=reply.dump();}};
        connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);EXPECT_TRUE(observed_uploads.empty());
        close_receiver();after_control={};open_receiver();
    }
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, WrongPriorNonProfileContextCannotRequestAnAdoptionException) {
    configure();seed_local(1,9830);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
    const auto before=immutable_q();after_control=[](size_t,const json& c,std::string& raw){if(c.at("operation")=="describe"){auto reply=json::parse(raw);reply["upload"]["maximumDeletes"]=255;raw=reply.dump();}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(proof_controls(),0u);EXPECT_EQ(immutable_q(),before);EXPECT_TRUE(observed_uploads.empty());EXPECT_EQ(phase(),2);
}
class PredecessorInboxController : public PredecessorReceiverController {
protected:
    void both_orders(bool current_first) {
        configure();seed_local(1,9840);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
        const auto q=immutable_q();auto held=std::make_shared<ControllerPause>();pauses.push_back(held);bool sent=false;std::string proof;
        after_control=[&](size_t index,const json& c,std::string& raw){
            if(c.at("operation")=="predecessor")proof=raw;
            if(sent||proof.empty()||c.at("operation")!="resume")return;sent=true;
            receiver->get_scheduler()->invoke([held]{held->wait();});const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
            while(!held->ready()&&std::chrono::steady_clock::now()<end)std::this_thread::sleep_for(std::chrono::milliseconds(1));ASSERT_TRUE(held->ready());
            const auto& first=current_first?raw:proof;const auto& second=current_first?proof:raw;
            EXPECT_TRUE(peers[index].physical.trigger_on_message(transport_message::from_string(first)));
            EXPECT_TRUE(peers[index].physical.trigger_on_message(transport_message::from_string(second)));raw.clear();held->release();};
        connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_TRUE(sent);EXPECT_FALSE(has_error());EXPECT_EQ(immutable_q(),q);EXPECT_EQ(proof_controls(),1u);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),1);
        after_control={};
    }
};
TEST_F(PredecessorInboxController, StaleProofAndCurrentResumeShareTwoSlotsInThatOrder){both_orders(false);}
TEST_F(PredecessorInboxController, CurrentResumeAndStaleProofShareTwoSlotsInThatOrder){both_orders(true);}
TEST_F(PredecessorReceiverController, RetiredPhysicalProofCannotAuthorizeReplacementView) {
    configure();seed_local(1,9850);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());std::string proof;
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="predecessor"){proof=raw;raw.clear();}};
    connect();ASSERT_TRUE(until([&]{return !proof.empty();}));const auto endpoint=peers[0].physical;const auto q=immutable_q();
    close_receiver();after_control={};open_receiver();connect();EXPECT_FALSE(endpoint.trigger_on_message(transport_message::from_string(proof)));
    ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(proof_controls(),2u);EXPECT_EQ(immutable_q(),q);EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, ProofAcquiredBeforeRouteRetirementIsReacquiredBeforeAnyInstall) {
    configure();seed_local(1,9860);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
    auto stopped=std::make_shared<std::atomic<bool>>(false);probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[stopped](const char* stage){
        if(std::strcmp(stage,"predecessor-consumed")==0&&!stopped->exchange(true))throw db_error("stop after proof consumption");});
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(proof_controls(),1u);const auto q=immutable_q();EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);
    close_receiver();probe.reset();open_receiver();connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_EQ(proof_controls(),2u);EXPECT_EQ(immutable_q(),q);EXPECT_FALSE(has_error());
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(PredecessorReceiverController, LateProofOnlyDrainCannotExtendOriginalFiveSecondReadAdmissionEpisode) {
    configure();seed_local(1,9870);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());
    auto held=std::make_shared<ControllerAdmissionRead>();ControllerAdmissionReadRelease cleanup{held};const auto owner=receiver;
    auto arm=std::make_shared<std::atomic<bool>>(false);auto first_deferral_ms=std::make_shared<std::atomic<int64_t>>(0);
    auto late_count=std::make_shared<std::atomic<unsigned>>(0);
    auto first_late=std::make_shared<ControllerPause>(),second_late=std::make_shared<ControllerPause>();pauses.push_back(first_late);pauses.push_back(second_late);
    std::string stale;
    after_control=[&](size_t,const json& control,std::string& outgoing){
        if(control.at("operation")=="predecessor"){stale=outgoing;arm->store(true);}};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[=](const char* stage){
        // Actual correlated proof has been consumed and outstanding is null.
        // The next owned phase inspection must encounter this real engine read.
        if(std::strcmp(stage,"pending-consumed-before-successor")==0&&arm->load())held->hold(owner->db());
        if(std::strcmp(stage,"admission-deferred")==0&&held->armed.load()&&held->deferrals.fetch_add(1)==0)
            first_deferral_ms->store(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
        if(std::strcmp(stage,"response-consumed")==0&&held->armed.load())++held->consumed;
        if(std::strcmp(stage,"late-lifecycle-discarded")==0){const auto n=late_count->fetch_add(1)+1;
            if(n==1)first_late->wait();if(n==2)second_late->wait();}});
    connect();ASSERT_TRUE(until([&]{return held->deferrals.load()>=12;}));ASSERT_FALSE(has_error());ASSERT_FALSE(stale.empty());
    ASSERT_GT(first_deferral_ms->load(),0);ASSERT_EQ(held->consumed.load(),0u);
    const auto before=snapshot();const auto old=framing();const auto handled_before=handled,requests_before=requests.size();
    const auto source_receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt");
    const auto endpoint=peers[0].physical;const auto source_view=peers[0].setup.descriptor();
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=1 AND length(manifest_frame)=0"),1);
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));ASSERT_TRUE(until([&]{return first_late->ready();}));
    const auto spent=held->deferrals.load();EXPECT_GE(spent,12u);EXPECT_LE(spent,32u);EXPECT_FALSE(has_error());
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));first_late->release();
    ASSERT_TRUE(until([&]{return second_late->ready();}));EXPECT_EQ(held->deferrals.load(),spent);
    // Twelve real >=100ms-spaced deferrals have already spent >1s of the
    // episode, leaving <4.1s here (within the unchanged five-second pause).
    // Keep the real read held until the ORIGINAL five-second window expires.
    const auto after_original_deadline=std::chrono::steady_clock::time_point(std::chrono::milliseconds(first_deferral_ms->load()+5100));
    const auto pause_remaining=std::chrono::duration_cast<std::chrono::milliseconds>(after_original_deadline-std::chrono::steady_clock::now()).count();
    ASSERT_LT(pause_remaining,4500);if(pause_remaining>0)std::this_thread::sleep_until(after_original_deadline);
    // With the second front still retained, only one other slot is available.
    // The third reply forces this two-frame worker quantum to return before
    // admission checks. Its next turn must retain the original expired episode.
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(stale)));second_late->release();
    ASSERT_TRUE(until([&]{return has_error()||held->deferrals.load()!=spent;},2000));
    ASSERT_TRUE(has_error());EXPECT_EQ(held->deferrals.load(),spent);EXPECT_LE(held->deferrals.load(),32u);EXPECT_EQ(late_count->load(),3u);
    const auto elapsed=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count()-first_deferral_ms->load();
    EXPECT_GE(elapsed,5100);EXPECT_LE(elapsed,7500);
    {std::lock_guard lock(errors_mutex);ASSERT_FALSE(errors.empty());EXPECT_NE(errors.front().find("admission retry budget exhausted"),std::string::npos);}
    EXPECT_EQ(phase(),2);EXPECT_EQ(snapshot(),before);EXPECT_EQ(framing(),old);EXPECT_EQ(held->consumed.load(),0u);
    EXPECT_EQ(handled,handled_before);EXPECT_EQ(requests.size(),requests_before);EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt"),source_receipts);
    EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),source_view);
    held->finish();EXPECT_FALSE(until([&]{return handled!=handled_before||held->deferrals.load()!=spent||held->consumed.load()!=0;},350));
    EXPECT_TRUE(has_error());EXPECT_EQ(snapshot(),before);EXPECT_EQ(phase(),2);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class PredecessorSeparateSources : public PredecessorReceiverController {
protected:
    TempDB second_file{"predecessor_second_source"};std::unique_ptr<swift_lattice_ref> second_ref;std::shared_ptr<::lattice::swift_lattice> second;
    bool second_orphan=false;
    void SetUp()override {
        PredecessorReceiverController::SetUp();if(HasFatalFailure())return;
        swift_configuration c(second_file.str(),std::make_shared<immediate_scheduler>());c.audit_retention_seconds=0;
#if LATTICE_HAS_FRT
        second_ref.reset(swift_lattice_ref::create(c,{controller_schema()}));
#else
        second_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(c,{controller_schema()}));
#endif
        second=swift_lattice_ref::shared_for_lattice(second_ref->get());ASSERT_TRUE(second);
        if(auto* n=instance_registry::instance().get_or_create_notifier(second_file.str()))n->stop_listening();
    }
    void TearDown()override {PredecessorReceiverController::TearDown();detail::end_fixture_owner_checked(second.get(),"controller-second-source");second.reset();second_ref.reset();}
    relay_recovery_setup serve(size_t index)override {
        if(index==0)return PredecessorReceiverController::serve(index);
        auto& peer=peers.at(index);auto p=RecoveryReceiverController::source_policy(peer.ns);
        if(second_orphan){p["readyProfile"]="bounded48MiBOrphanV1";p["orphanResumeGraceMilliseconds"]=60000;}
        const auto config=json{{"mount",controller_uuid(3)},{"connection",::lattice::uuid_t::generate().to_string()},
            {"channel",peer.channel},{"authenticatedUserID",controller_uuid(4)},
            {"peer",{{"replicaID","registered-controller"},{"receiverIncarnation",controller_uuid(20)},{"channelIncarnation",controller_uuid(30+index)}}}};
        auto actual=second_ref->open_relay_recovery_setup(p.dump(),config.dump(),new ControllerServerRoute{peer.live},ControllerServerRoute::current,ControllerServerRoute::destroy);
        if(!actual.valid())throw db_error("actual second source setup failed: "+last_bridge_error());const auto context=json::parse(actual.descriptor());
        const auto answer=json{{"context",context},{"authenticatedUserID",context["route"]["authenticatedUserID"]},{"peer",context["route"]["peer"]},
            {"source",context["source"]},{"incomingScope",context["incomingScope"]},{"authorizationRevision","registered-controller-v1"},{"validForMilliseconds",600000}};
        if(!actual.finish_authorization(answer.dump()))throw db_error("actual second source authorization failed");return actual;
    }
    void equal_actual_data() {
        insert(*source,controller_uuid(9880),"same canonical state");insert(*second,controller_uuid(9880),"same canonical state");
        EXPECT_NE(source->config().path,second->config().path);
        EXPECT_EQ(source->db().query("SELECT globalId,value,note FROM ControllerRow ORDER BY globalId"),second->db().query("SELECT globalId,value,note FROM ControllerRow ORDER BY globalId"));
        EXPECT_EQ(source->db().query("SELECT head FROM _lattice_canonical_store"),second->db().query("SELECT head FROM _lattice_canonical_store"));
        auto a=peers.at(0).expectation.at("source"),b=peers.at(1).expectation.at("source");
        for(const auto* key:{"receiptNamespace","coverageID","coverageRevision","descriptorDigest"}){a.erase(key);b.erase(key);}EXPECT_EQ(a,b);
    }
};
TEST_F(PredecessorSeparateSources, MixedAdoptedTerminalCurrentAvailableAndUnstartedRetireOneWholeCohort) {
    second_orphan=true;configure(3);equal_actual_data();ASSERT_FALSE(HasFailure());seed_local(1,9881);bool unstarted=false;
    before_control=[&](size_t i,const json& c){if(i==2&&c.at("operation")=="prepare"){unstarted=true;return true;}return false;};
    connect();ASSERT_TRUE(until([&]{return unstarted;}));ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1"),2);
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),1);ASSERT_EQ(scalar(*second,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),1);
    const auto source_rows=source->db().query("SELECT * FROM ControllerRow"),second_rows=second->db().query("SELECT * FROM ControllerRow");
    adopt_source();ASSERT_FALSE(HasFatalFailure());reopen_after_terminal([](size_t i,const json& c){return i==0&&c.at("operation")=="predecessor";});ASSERT_FALSE(HasFatalFailure());
    const auto intent=entries(),q=immutable_q(),allocation=allocators();const auto pause=pause_terminal();connect();ASSERT_TRUE(until([&]{return pause->ready();}));
    EXPECT_EQ(phase(),1);EXPECT_EQ(entries(),intent);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);EXPECT_EQ(scalar(*second,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(scalar(*second,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding WHERE sequence=1"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=0 AND last_sequence=1 AND active IS NULL"),3);
    EXPECT_EQ(source->db().query("SELECT * FROM ControllerRow"),source_rows);EXPECT_EQ(second->db().query("SELECT * FROM ControllerRow"),second_rows);
    bool available_discarded=false,unstarted_discarded=false;for(const auto& [i,c]:controls)if(c.at("operation")=="discard"){available_discarded|=i==1;unstarted_discarded|=i==2;}
    EXPECT_TRUE(available_discarded);EXPECT_TRUE(unstarted_discarded);pause->release();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),2);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),3);
}
TEST_F(PredecessorSeparateSources, UnchangedOldPeerBlocksTerminalRestartWithoutPartialLocalCancellation) {
    configure(2);equal_actual_data();ASSERT_FALSE(HasFailure());seed_local(1,9890);bool held=false;
    before_control=[&](size_t i,const json& c){if(i==1&&c.at("operation")=="read"&&c.at("index")=="2"){held=true;return true;}return false;};
    connect();ASSERT_TRUE(until([&]{return held;}));adopt_source();ASSERT_FALSE(HasFatalFailure());
    reopen_after_terminal([](size_t i,const json& c){return i==0&&c.at("operation")=="predecessor";});ASSERT_FALSE(HasFatalFailure());
    const auto q=immutable_q(),intent=entries(),allocation=allocators();const auto old_frames=second->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    const auto old_binding=second->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");
    const auto start=controls.size();connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),1);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);
    EXPECT_EQ(second->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),old_frames);
    EXPECT_EQ(second->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),old_binding);
    for(size_t n=start;n<controls.size();++n)if(controls[n].first==1)EXPECT_NE(controls[n].second.at("operation"),"discard");EXPECT_TRUE(observed_uploads.empty());
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(PredecessorReceiverController, InstalledMalformedLateProofRevokesCurrentOrdinaryExportView) {
    configure();seed_local(1,9900);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());std::string proof;
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="predecessor")proof=raw;};
    connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(proof.empty());ASSERT_TRUE(until([&]{return !held_uploads.empty();}));
    auto bad=json::parse(proof);bad["predecessor"]["namespaceID"]="b";const auto before=snapshot();const auto sent=observed_uploads.size();
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(bad.dump())));ASSERT_TRUE(until([&]{return has_error();}));
    EXPECT_EQ(phase(),0);EXPECT_EQ(snapshot(),before);seed_local(1,9901);EXPECT_THROW(synchronizers[0]->sync_now(),db_error);
    const auto drain=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));EXPECT_EQ(drain.state,sync_drain_state::failed);EXPECT_TRUE(drain.error);
    auto barrier=std::make_shared<std::promise<void>>();auto done=barrier->get_future();receiver->get_scheduler()->invoke([barrier]{barrier->set_value();});ASSERT_EQ(done.wait_for(std::chrono::seconds(2)),std::future_status::ready);
    for(unsigned n=0;n<64&&pump();++n){}EXPECT_EQ(observed_uploads.size(),sent);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
}
TEST_F(PredecessorReceiverController, UnsettledProofResponseCannotAuthorizeTerminalCancellationOrInstall) {
    configure();seed_local(1,9910);retain_pre_manifest();ASSERT_FALSE(HasFatalFailure());adopt_source();ASSERT_FALSE(HasFatalFailure());const auto q=immutable_q(),intent=entries();
    after_control=[](size_t,const json& c,std::string& raw){if(c.at("operation")=="predecessor"){auto reply=json::parse(raw);reply["settlement"]["state"]="unsettled";reply.erase("predecessor");raw=reply.dump();}};
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);EXPECT_EQ(immutable_q(),q);EXPECT_EQ(entries(),intent);EXPECT_TRUE(observed_uploads.empty());
    for(const auto& [_,c]:controls)EXPECT_NE(c.at("operation"),"discard");EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class CompletedPredecessorController : public TerminalReceiverController {
protected:
    bool orphan=false;
    json source_policy(const std::string& ns)override {
        return orphan?TerminalReceiverController::source_policy(ns):RecoveryReceiverController::source_policy(ns);
    }
    static std::string stored_text(const database::row_t& row,const char* key) {
        const auto& bytes=std::get<std::vector<uint8_t>>(row.at(key));return {bytes.begin(),bytes.end()};
    }
    Snapshot source_inventory() {
        Snapshot out;
        const auto tables=source->db().query("SELECT name FROM sqlite_schema WHERE type='table' AND (name LIKE '_lattice_canonical_%' OR name IN ('ControllerRow','AuditLog')) ORDER BY name LIMIT 97");
        if(tables.size()>96)throw db_error("completed fixture source inventory bound");
        for(const auto& row:tables){const auto name=std::get<std::string>(row.at("name"));out[name]=source->db().query("SELECT * FROM \""+name+"\"");}
        return out;
    }
    void start_completed(size_t count=1) {
        configure(count);insert(*source,controller_uuid(9920),"canonical");connect();
        ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(has_error());
        ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer WHERE sequence=1"),static_cast<int64_t>(count));
    }
    void actual_ack_successor() {
        configure();seed_local(1,9921);const auto audits=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");const auto ids=originals();hold_uploads=false;
        std::vector<json> discarded;
        after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard")discarded.push_back(json::parse(raw));};
        connect();ASSERT_TRUE(until([&]{return installed(2)&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==1;}));
        ASSERT_EQ(discarded.size(),1u);EXPECT_EQ(discarded[0].at("settlement").at("state"),"committed");
        if(orphan)EXPECT_EQ(discarded[0].at("lifecycle").at("state"),"terminal");else EXPECT_FALSE(discarded[0].contains("lifecycle"));
        EXPECT_EQ(observed_uploads.size(),1u);EXPECT_EQ(observed_originals(),ids);EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audits);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)>0"),1);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer WHERE sequence=2"),1);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding WHERE sequence=2"),1);EXPECT_FALSE(has_error());
    }
    void held_original_discard_burst(bool stale_first) {
        start_completed();ASSERT_FALSE(HasFatalFailure());std::string stale,current;bool capture_current=false;
        after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard"&&stale.empty())stale=raw;
            if(capture_current&&c.at("operation")=="prepare"){current=raw;raw.clear();}};
        request_recovery();ASSERT_TRUE(until([&]{return installed(2);}));ASSERT_FALSE(stale.empty());
        capture_current=true;request_recovery();ASSERT_TRUE(until([&]{return !current.empty();}));
        // The saved Q1 disposal reply predates the live Q3 prepare request.
        // Hold the actual worker before both socket callbacks reserve slots.
        auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);
        receiver->get_scheduler()->invoke([pause]{pause->wait();});ASSERT_TRUE(until([&]{return pause->ready();}));
        const auto q=framing(),intent=entries();const auto endpoint=peers[0].physical;const auto view=peers[0].setup.descriptor();
        const auto first=stale_first?stale:current,second=stale_first?current:stale;
        ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(first)));
        ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(second)));
        EXPECT_EQ(framing(),q);EXPECT_EQ(entries(),intent);pause->release();capture_current=false;
        ASSERT_TRUE(until([&]{return installed(3);}));EXPECT_FALSE(has_error());EXPECT_FALSE(pause->timedOut());
        EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),view);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=3"),1);
    }
};
TEST_F(CompletedPredecessorController, OriginalProfileDisposesInstalledQBeforeRealAckSuccessor) {actual_ack_successor();}
TEST_F(CompletedPredecessorController, OrphanProfileDisposesInstalledQBeforeRealAckSuccessor) {orphan=true;actual_ack_successor();}
TEST_F(CompletedPredecessorController, WholeCohortKeepsEveryOldQMUntilLastActualDisposalReply) {
    start_completed(2);ASSERT_FALSE(HasFatalFailure());const auto old=framing(),intent=entries(),allocation=allocators();std::string held;size_t held_peer=0;unsigned disposals=0;
    after_control=[&](size_t index,const json& c,std::string& raw){if(c.at("operation")=="discard"){
        EXPECT_EQ(json::parse(raw).at("settlement").at("state"),"committed");if(++disposals==2){held=raw;held_peer=index;raw.clear();}}};
    request_recovery();ASSERT_TRUE(until([&]{return !held.empty();}));EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding WHERE sequence=1"),2);
    ASSERT_TRUE(peers[held_peer].physical.trigger_on_message(transport_message::from_string(held)));after_control={};
    ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2"),2);
}
TEST_F(CompletedPredecessorController, LostActualDiscardReplyRetriesOnQuietLiveRouteAtOriginalThirtySecondDeadline) {
    start_completed();ASSERT_FALSE(HasFatalFailure());const auto old=framing(),intent=entries();std::vector<json> commands;std::string lost;
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard"){
        commands.push_back(c);EXPECT_EQ(json::parse(raw).at("settlement").at("state"),"committed");if(lost.empty()){lost=raw;raw.clear();}}};
    const auto endpoint=peers[0].physical;const auto view=peers[0].setup.descriptor();request_recovery();ASSERT_TRUE(until([&]{return !lost.empty();}));
    const auto started=std::chrono::steady_clock::now();EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    // No close, reconnect, sync_now or request_recovery after the loss. The
    // existing native 100ms pacer owns progress across the unchanged 30s wait.
    ASSERT_TRUE(until([&]{return installed(2);},45000));EXPECT_GE(std::chrono::steady_clock::now()-started,std::chrono::seconds(29));
    ASSERT_EQ(commands.size(),2u);EXPECT_EQ(commands[0].at("request"),commands[1].at("request"));EXPECT_NE(commands[0].at("requestID"),commands[1].at("requestID"));
    EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),view);EXPECT_FALSE(has_error());
}
TEST_F(CompletedPredecessorController, PartialCohortDisposalReopensWithEveryOldQMAndRepeatsExactAbsentRetry) {
    start_completed(2);ASSERT_FALSE(HasFatalFailure());const auto old=framing(),intent=entries(),allocation=allocators();std::string held_request;bool lost=false;
    after_control=[&](size_t,const json& c,std::string& raw){if(!lost&&c.at("operation")=="discard"){held_request=c.at("request");lost=true;raw.clear();}};
    request_recovery();ASSERT_TRUE(until([&]{return lost;}));ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),1);
    const auto source_before=source_inventory();EXPECT_EQ(framing(),old);close_receiver();after_control={};open_receiver();
    EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);EXPECT_EQ(source_inventory(),source_before);
    std::string retry;after_control=[&](size_t,const json& c,std::string&){if(retry.empty()&&c.at("operation")=="discard")retry=c.at("request");};
    connect();ASSERT_TRUE(until([&]{return installed(2);}));ASSERT_FALSE(retry.empty());auto before=json::parse(held_request),after=json::parse(retry);
    before["latticeCanonicalRange"]["route_generation"]=after["latticeCanonicalRange"]["route_generation"];EXPECT_EQ(before,after);EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer WHERE sequence=2"),2);
}
TEST_F(CompletedPredecessorController, DeniedAtomicFramingCommitKeepsOldQMThenReopensAfterRealSourceCleanup) {
    configure();insert(*source,controller_uuid(9922),"canonical");auto armed=std::make_shared<std::atomic<bool>>(false);auto hits=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[this,armed,hits](const char* stage)->std::shared_ptr<void>{
        if(armed->load()&&std::strcmp(stage,"completed-disposal-framing")==0)return std::make_shared<ControllerCommitFault>(receiver.get(),*hits);return {};});
    connect();ASSERT_TRUE(until([&]{return installed();}));const auto old=framing(),intent=entries(),allocation=allocators();armed->store(true);request_recovery();
    ASSERT_TRUE(until([&]{return has_error();}));EXPECT_GT(hits->load(),0u);EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(framing(),old);connect();ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());
}
TEST_F(CompletedPredecessorController, CrashAfterNewQCommitReopensWithoutRequiringAnotherOldDisposal) {
    configure();insert(*source,controller_uuid(9923),"canonical");auto armed=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[armed](const char* stage){
        if(armed->load()&&std::strcmp(stage,"completed-disposal-framing-committed")==0)throw db_error("fixture crash after replacement Q COMMIT");});
    connect();ASSERT_TRUE(until([&]{return installed();}));armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return has_error();}));
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);const auto q=framing(),intent=entries();
    close_receiver();probe.reset();open_receiver();EXPECT_EQ(framing(),q);EXPECT_EQ(entries(),intent);const auto start=controls.size();connect();
    ASSERT_TRUE(until([&]{return installed(2);}));for(size_t n=start;n<controls.size();++n)EXPECT_NE(controls[n].second.at("operation"),"discard");EXPECT_FALSE(has_error());
}
TEST_F(CompletedPredecessorController, OriginalLateDiscardThenCurrentResponseSharesFixedTwoSlotInbox) {held_original_discard_burst(true);}
TEST_F(CompletedPredecessorController, OriginalCurrentThenLateDiscardSharesFixedTwoSlotInbox) {held_original_discard_burst(false);}
TEST_F(CompletedPredecessorController, OriginalLateDiscardAtConsumedGapAndIdleNeverSettlesSuccessor) {
    configure();insert(*source,controller_uuid(9924),"canonical");auto armed=std::make_shared<std::atomic<bool>>(false),paused=std::make_shared<std::atomic<bool>>(false);
    auto ignored=std::make_shared<std::atomic<unsigned>>(0);auto pause=std::make_shared<ControllerPause>();pauses.push_back(pause);std::string actual;
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[=](const char* stage){
        if(std::strcmp(stage,"pending-consumed-before-successor")==0&&armed->load()&&!paused->exchange(true))pause->wait();
        if(std::strcmp(stage,"late-lifecycle-discarded")==0)++*ignored;});
    connect();ASSERT_TRUE(until([&]{return installed();}));
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard"){actual=raw;armed->store(true);}};
    request_recovery();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_FALSE(actual.empty());const auto old=framing(),intent=entries();const auto endpoint=peers[0].physical;
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(actual)));EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);pause->release();
    ASSERT_TRUE(until([&]{return installed(2);}));ASSERT_TRUE(until([&]{return ignored->load()==1;}));const auto settled=snapshot();
    ASSERT_TRUE(endpoint.trigger_on_message(transport_message::from_string(actual)));ASSERT_TRUE(until([&]{return ignored->load()==2;}));
    EXPECT_EQ(snapshot(),settled);EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_FALSE(has_error());EXPECT_FALSE(pause->timedOut());
}
TEST_F(CompletedPredecessorController, RetiredDisposalResponseCannotAuthorizeNewPhysicalView) {
    start_completed();ASSERT_FALSE(HasFatalFailure());std::string lost;after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard"){lost=raw;raw.clear();}};
    request_recovery();ASSERT_TRUE(until([&]{return !lost.empty();}));const auto q=framing(),intent=entries();const auto old_endpoint=peers[0].physical;
    close_receiver();after_control={};open_receiver();EXPECT_EQ(framing(),q);EXPECT_EQ(entries(),intent);connect();
    EXPECT_FALSE(old_endpoint.trigger_on_message(transport_message::from_string(lost)));ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());
}
TEST_F(CompletedPredecessorController, UnknownReconciliationDisposesCanceledCapsuleWithoutChangingOriginalClaims) {
    start_completed();ASSERT_FALSE(HasFatalFailure());seed_local(2,9925);const auto ids=originals();ASSERT_TRUE(until([&]{return held_originals()==ids;}));
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    held_uploads.clear();hold_uploads=false;legacy_ack(0,ids);
    ASSERT_TRUE(until([&]{return installed(2)&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==2;}));
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),3);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
    unsigned count=0;for(const auto& [_,c]:controls)if(c.at("operation")=="discard")++count;EXPECT_EQ(count,2u);EXPECT_FALSE(has_error());
}
TEST_F(PredecessorReceiverController, ExactInstalledPredecessorAfterAdoptionDoesNotAcquireOldProfileProofPrerequisite) {
    configure();insert(*source,controller_uuid(9927),"canonical");connect();ASSERT_TRUE(until([&]{return installed();}));const auto rows=receiver->db().query("SELECT * FROM ControllerRow");
    adopt_source();ASSERT_FALSE(HasFatalFailure());const auto before=proof_controls();connect();ASSERT_TRUE(until([&]{return installed(2);}));
    EXPECT_EQ(proof_controls(),before);EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow"),rows);EXPECT_FALSE(has_error());
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer WHERE sequence=2"),1);
}
TEST_F(CompletedPredecessorController, SimulatedPreFixMissingQEvidenceFailsClosedWithActualOldSourceCapsule) {
    start_completed();ASSERT_FALSE(HasFatalFailure());seed_local(1,9928);const auto old=framing();const auto source_before=source_inventory();
    ASSERT_EQ(old.size(),1u);const auto old_q=json::parse(stored_text(old[0],"request_frame")).at("latticeCanonicalRange");
    const auto actual_transfer=source->db().query("SELECT * FROM _lattice_canonical_ready_transfer");ASSERT_EQ(actual_transfer.size(),1u);
    const auto source_q=json::parse(stored_text(actual_transfer[0],"request")).at("latticeCanonicalRange");EXPECT_EQ(source_q.at("attempt"),old_q.at("attempt"));EXPECT_EQ(source_q.at("body"),old_q.at("body"));
    bool forged=false,held_q2=false;json real_q2;
    before_control=[&](size_t index,const json& c){
        if(!forged&&c.at("operation")=="discard"){
            // Deliberately UNTRUTHFUL authenticated response simulates an
            // unshipped pre-fix receiver losing Q1. This is no evidence of
            // source COMMIT and is never a positive cleanup assertion.
            forged=true;const auto reply=json{{"kind","recoveryReady"},{"version",1},{"operation","discard"},{"requestID",c.at("requestID")},
                {"routeGeneration",c.at("routeGeneration")},{"leaseAvailable",false},{"settlement",{{"state","committed"},{"unexpectedCommitObserved",false},
                    {"primaryError",false},{"cleanupError",false},{"postcommitError",false},{"notificationError",false}}}};
            peers[index].physical.trigger_on_message(transport_message::from_string(reply.dump()));return true;
        }
        if(forged&&c.at("operation")=="prepare"){real_q2=c;held_q2=true;return true;}return false;};
    request_recovery();ASSERT_TRUE(until([&]{return held_q2;}));ASSERT_TRUE(forged);
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)=0"),1);
    const auto new_q=json::parse(stored_text(framing().at(0),"request_frame")).at("latticeCanonicalRange");
    EXPECT_EQ(new_q.at("attempt").at("sequence"),"2");EXPECT_EQ(old_q.at("attempt").at("sequence"),"1");EXPECT_NE(new_q.at("body").at("request_digest"),old_q.at("body").at("request_digest"));
    EXPECT_EQ(json::parse(real_q2.at("request").get<std::string>()).at("latticeCanonicalRange"),new_q);
    ASSERT_EQ(source_inventory(),source_before);ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_binding WHERE sequence=1"),1);
    const auto before=snapshot(),source_frozen=source_inventory();before_control={};const auto raw=real_q2.dump();auto charge=peers[0].setup.stop_token().reserve_ready(raw.size());ASSERT_TRUE(charge.valid());
    const auto result=peers[0].setup.ready(raw,charge);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable());auto refusal=json::parse(result.wire());EXPECT_FALSE(refusal.at("leaseAvailable").get<bool>());
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(result.wire())));ASSERT_TRUE(until([&]{return has_error();}));
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(source_inventory(),source_frozen);
    close_receiver();open_receiver();Snapshot retried;
    after_control=[&](size_t,const json& c,std::string&){if(c.at("operation")=="resume")retried=snapshot();};
    connect();ASSERT_TRUE(until([&]{return has_error();}));ASSERT_FALSE(retried.empty());
    EXPECT_EQ(snapshot(),retried);EXPECT_EQ(source_inventory(),source_frozen);
}
TEST_F(CompletedPredecessorController, UnsettledLastDisposalKeepsWholeLocalCohortUntilReopen) {
    start_completed(2);ASSERT_FALSE(HasFatalFailure());const auto old=framing(),intent=entries(),allocation=allocators();unsigned replies=0;
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard"&&++replies==2){
        auto value=json::parse(raw);ASSERT_EQ(value.at("settlement").at("state"),"committed");
        // Withhold current settlement truth at the authenticated boundary;
        // the first peer's real receipt cannot authorize partial replacement.
        value["settlement"]["state"]="unsettled";raw=value.dump();}};
    request_recovery();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(replies,2u);EXPECT_EQ(framing(),old);EXPECT_EQ(entries(),intent);EXPECT_EQ(allocators(),allocation);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),0);
    close_receiver();after_control={};open_receiver();EXPECT_EQ(framing(),old);connect();ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());
}
TEST_F(CompletedPredecessorController, MalformedOriginalLateDiscardCannotKeepOrdinaryExportAdmission) {
    start_completed();ASSERT_FALSE(HasFatalFailure());std::string actual;
    after_control=[&](size_t,const json& c,std::string& raw){if(c.at("operation")=="discard")actual=raw;};
    request_recovery();ASSERT_TRUE(until([&]{return installed(2);}));ASSERT_FALSE(actual.empty());
    auto bad=json::parse(actual);bad["leaseAvailable"]=true;const auto before=snapshot();
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(bad.dump())));ASSERT_TRUE(until([&]{return has_error();}));
    EXPECT_EQ(snapshot(),before);seed_local(1,9929);EXPECT_THROW(synchronizers[0]->sync_now(),db_error);
    const auto drained=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));EXPECT_EQ(drained.state,sync_drain_state::failed);EXPECT_TRUE(drained.error);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL"),1);
}

}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
// Record only the actual outbound platform message discriminator. The existing
// bounded wire still delivers the unchanged bytes to the real authenticated
// source; this does not substitute a control or claim stock-adapter TLS proof.
class BinaryRecoveryReceiverController:public RecoveryReceiverController {
protected:
    std::map<std::string,size_t> controls;
    void capture_message_types(){std::lock_guard lock(wire->mutex);wire->record_message_types=true;}
    bool before_ready(size_t,const json& control)override {
        const auto operation=control.at("operation").get<std::string>();
        if(controls.size()>=16&&!controls.count(operation))throw db_error("fixture control observation bound");
        ++controls[operation];return false;
    }
    bool installed(int64_t revision){return phase()==0&&scalar(*receiver,
        "SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision="+std::to_string(revision)+" AND active IS NULL")==1;}
    void expect_binary_messages(){
        std::vector<transport_message::type> types;
        {std::lock_guard lock(wire->mutex);types=wire->message_types;}
        size_t consumed=0;for(const auto& [_,count]:controls)consumed+=count;
        ASSERT_GT(consumed,0u);ASSERT_GE(types.size(),consumed);
        for(const auto type:types)EXPECT_EQ(type,transport_message::type::binary);
    }
};
TEST_F(BinaryRecoveryReceiverController, ActualDescribePrepareReadAndCompletedDiscardUseBinaryAcrossPhysicalReconnect) {
    capture_message_types();configure();insert(*source,controller_uuid(9940),"source");connect();
    ASSERT_TRUE(until([&]{return installed(1);}));ASSERT_FALSE(has_error());
    EXPECT_EQ(controls["describe"],1u);EXPECT_EQ(controls["prepare"],1u);EXPECT_GT(controls["read"],0u);
    expect_binary_messages();
    const auto rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
    ASSERT_EQ(rows.size(),1u);const auto old=peers[0].physical;
    const auto initial_dials=boundary_trace.connection;ASSERT_EQ(initial_dials,1u);ASSERT_EQ(synchronizers.size(),1u);
    // A protected wrapper owns one physical attempt. Retain the retired
    // wrapper while a fresh wrapper uses the same live receiver scheduler.
    synchronizers[0]->disconnect();connect();ASSERT_EQ(synchronizers.size(),2u);
    ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());
    EXPECT_EQ(boundary_trace.connection,initial_dials+1);EXPECT_FALSE(old.matches(peers[0].physical));EXPECT_TRUE(peers[0].physical.is_current());
    EXPECT_FALSE(old.trigger_on_message(transport_message::from_string("{}")));
    EXPECT_EQ(controls["describe"],2u);EXPECT_EQ(controls["prepare"],2u);EXPECT_GT(controls["discard"],0u);
    EXPECT_GT(controls["read"],1u);expect_binary_messages();
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer"),1);
}
TEST_F(BinaryRecoveryReceiverController, ActualLostPrepareReopenResumesIdenticalRequestUsingBinaryControls) {
    capture_message_types();configure();insert(*source,controller_uuid(9941),"source");drop_prepare=true;connect();
    ASSERT_TRUE(until([&]{return dropped==1;}));ASSERT_FALSE(requests.empty());
    const auto original=requests.front();const auto old_request=receiver->db().query("SELECT request_frame FROM _lattice_recovery_request");
    const auto old=peers[0].physical;expect_binary_messages();
    close_receiver();drop_prepare=false;open_receiver();
    EXPECT_EQ(receiver->db().query("SELECT request_frame FROM _lattice_recovery_request"),old_request);connect();
    ASSERT_TRUE(until([&]{return installed(1);}));EXPECT_FALSE(has_error());
    EXPECT_FALSE(old.trigger_on_message(transport_message::from_string("{}")));
    EXPECT_EQ(controls["describe"],2u);EXPECT_EQ(controls["prepare"],1u);EXPECT_GT(controls["resume"],0u);EXPECT_GT(controls["read"],0u);
    ASSERT_GE(requests.size(),2u);auto before=json::parse(original),after=json::parse(requests.back());
    before["latticeCanonicalRange"]["route_generation"]=after["latticeCanonicalRange"]["route_generation"];
    EXPECT_EQ(before,after);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);expect_binary_messages();
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST_F(RecoveryDeliveryTimeout, CompletedRestrictedPrefixesReleaseCustodyBeforeActualRefreezeCommit) {
    const auto refreeze=gate();auto first=std::make_shared<std::atomic<bool>>(true);
    const auto frozen_attempt=std::make_shared<std::atomic<int64_t>>(0);
    auto controller_local_worker_ending=scoped_controller_worker_ending();
    // The private controller still owns this actual phase2 turn. Cancellation
    // cannot take its reservation until the off-leaf observation returns.
    events->observed=[this,frozen_attempt](const char* stage){
        if(std::strcmp(stage,"reconciliation-pending")!=0||frozen_attempt->load()!=0)return;
        const auto row=receiver->db().query("SELECT phase,attempt FROM _lattice_producer_continuity").at(0);
        if(std::get<int64_t>(row.at("phase"))==2)frozen_attempt->store(std::get<int64_t>(row.at("attempt")),std::memory_order_release);
    };
    events->scope=[refreeze,first](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"reconcile-refreeze")==0&&first->exchange(false))refreeze->wait();return {};};
    start(2,1);ASSERT_FALSE(HasFatalFailure());restricted();ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(until([&]{return refreeze->arrived.load()==1;}));ASSERT_EQ(phase(),4);
    ASSERT_EQ(events->restricted->started.load(),2u);EXPECT_EQ(events->restricted->completed.load(),0u);EXPECT_EQ(events->admitted.load(),0u);
    EXPECT_EQ(held_originals(),(std::vector<std::string>{sent_ids[0],sent_ids[1],sent_ids[0],sent_ids[1]}));
    const auto raw=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto claims=receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original");
    const auto prior_attempt=frozen_attempt->load(std::memory_order_acquire);ASSERT_GT(prior_attempt,0);
    // Phase2->4 cancellation already created this exact successor. Refreeze
    // 4->1 and the next freeze retain it; neither issues another attempt.
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),prior_attempt+1);
    refreeze->release(); // No ACK, timer release, public request or reconnect.
    ASSERT_TRUE(until([&]{return events->refrozen.load()==1&&phase()==2&&error_contains("UNKNOWN persisted after one restricted pass");}));
    EXPECT_EQ(scalar(*receiver,"SELECT attempt AS n FROM _lattice_producer_continuity"),prior_attempt+1);
    EXPECT_EQ(events->restricted->completed.load(),0u);EXPECT_EQ(events->admitted.load(),0u);EXPECT_EQ(held_uploads.size(),4u);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),0);EXPECT_EQ(originals(),sent_ids);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),raw);
    EXPECT_EQ(receiver->db().query("SELECT original,first_export FROM _lattice_obligation_entry ORDER BY original"),claims);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct ControlledGenerationRollbackFault {
    static thread_local ControlledGenerationRollbackFault* active;
    std::atomic<unsigned>& hits;
    detail::recovery_local_producer_test_hooks::authorizer_fault fault;
    const detail::recovery_local_producer_test_hooks::authorizer_fault* prior;
    ControlledGenerationRollbackFault* prior_active;
    ControlledGenerationRollbackFault(const lattice_db* owner,std::atomic<unsigned>& count)
        :hits(count),fault{owner,restrict_action},prior(detail::recovery_local_producer_test_hooks::fault),prior_active(active){
        active=this;detail::recovery_local_producer_test_hooks::fault=&fault;
    }
    ~ControlledGenerationRollbackFault(){detail::recovery_local_producer_test_hooks::fault=prior;active=prior_active;}
    static int restrict_action(int action,const char* one,const char*,const char*)noexcept{
        if(active&&action==SQLITE_TRANSACTION&&one&&std::strcmp(one,"ROLLBACK")==0){++active->hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local ControlledGenerationRollbackFault* ControlledGenerationRollbackFault::active=nullptr;
enum class ControlledGenerationLocation {sql_validation,unpublished_handoff};
enum class ControlledGenerationFault {none,rollback_denied,semantic};
class ControlledGenerationReceiverController:public CompletedPredecessorController {
protected:
    void actual_generation_race(ControlledGenerationLocation location,ControlledGenerationFault fault,bool orphan_profile){
        orphan=orphan_profile;configure(2);insert(*source,controller_uuid(9960),"unchanged across admitted generations");
        auto armed=std::make_shared<std::atomic<bool>>(false),paused=std::make_shared<std::atomic<bool>>(false);
        auto cancelled=std::make_shared<std::atomic<unsigned>>(0),rollbacks=std::make_shared<std::atomic<unsigned>>(0);
        auto old_turn=std::make_shared<ControllerPause>(),cancelled_turn=std::make_shared<ControllerPause>();
        pauses.push_back(old_turn);pauses.push_back(cancelled_turn);
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
            [=](const char* stage){
                const auto trigger=location==ControlledGenerationLocation::sql_validation?
                    "disposal-before-generation-validation":"outgoing-built-before-publication";
                if(armed->load()&&std::strcmp(stage,trigger)==0&&!paused->exchange(true)){
                    old_turn->wait();
                    if(fault==ControlledGenerationFault::semantic)throw db_error("actual controlled-generation semantic refusal");
                }
                if(std::strcmp(stage,"stale-generation-cancelled-before-current-turn")==0){++*cancelled;cancelled_turn->wait();}
            },
            [this,armed,rollbacks,fault](const char* stage)->std::shared_ptr<void>{
                if(armed->load()&&fault==ControlledGenerationFault::rollback_denied&&std::strcmp(stage,"completed-disposal-validation")==0)
                    return std::make_shared<ControlledGenerationRollbackFault>(receiver.get(),*rollbacks);
                return {};
            });
        connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(has_error());
        const auto old_q=framing(),old_entries=entries(),old_allocators=allocators();
        const auto old_rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
        const auto old_source=source_inventory();const auto old_controls=controls.size();
        const auto endpoint=peers[0].physical;const auto descriptor=peers[0].setup.descriptor();
        armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return old_turn->ready();}));
        // A real current endpoint admits a second demand while the actual
        // controller is running. No test peer mutates controller generation.
        request_recovery();old_turn->release();
        if(fault!=ControlledGenerationFault::none){
            ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(cancelled->load(),0u);
            if(fault==ControlledGenerationFault::rollback_denied){
                EXPECT_GT(rollbacks->load(),0u);
                {std::lock_guard lock(errors_mutex);EXPECT_TRUE(std::any_of(errors.begin(),errors.end(),[](const auto& error){
                    return error.find("controller disposal generation changed")!=std::string::npos;}));}
            }else{
                EXPECT_EQ(rollbacks->load(),0u);
                {std::lock_guard lock(errors_mutex);EXPECT_TRUE(std::any_of(errors.begin(),errors.end(),[](const auto& error){
                    return error.find("actual controlled-generation semantic refusal")!=std::string::npos;}));}
                EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),old_q);EXPECT_EQ(entries(),old_entries);
            }
            // Actual queued worker completion, not a scalar wake oracle.
            auto barrier=std::make_shared<std::promise<void>>();auto done=barrier->get_future();
            receiver->get_scheduler()->invoke([barrier]{barrier->set_value();});
            ASSERT_EQ(done.wait_for(std::chrono::seconds(2)),std::future_status::ready);
            for(unsigned n=0;n<64&&pump();++n){}
            EXPECT_EQ(source_inventory(),old_source);
            for(size_t n=old_controls;n<controls.size();++n)EXPECT_NE(controls[n].second.at("operation"),"discard");
            const auto drain=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
            EXPECT_EQ(drain.state,sync_drain_state::failed);EXPECT_TRUE(drain.error);
            EXPECT_EQ(cancelled->load(),0u);return;
        }
        ASSERT_TRUE(until([&]{return cancelled_turn->ready();}));EXPECT_EQ(cancelled->load(),1u);EXPECT_EQ(rollbacks->load(),0u);
        // Cancellation is observed only after actual SQL settlement/off-leaf
        // payload destruction. It cannot publish success, new Q or a discard.
        EXPECT_EQ(phase(),2);EXPECT_EQ(framing(),old_q);EXPECT_EQ(entries(),old_entries);EXPECT_EQ(allocators(),old_allocators);
        EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),old_rows);EXPECT_EQ(source_inventory(),old_source);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1 AND active IS NULL"),2);
        for(size_t n=old_controls;n<controls.size();++n)EXPECT_NE(controls[n].second.at("operation"),"discard");
        EXPECT_FALSE(has_error());EXPECT_TRUE(endpoint.matches(peers[0].physical));EXPECT_EQ(peers[0].setup.descriptor(),descriptor);
        cancelled_turn->release();
        // No third request, reconnect, ACK, timeout or manual revision change:
        // the controlled catch must arrange the current actual worker turn.
        ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(has_error());
        EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),old_rows);
        EXPECT_EQ(entries(),old_entries);EXPECT_EQ(allocators(),old_allocators);EXPECT_EQ(cancelled->load(),1u);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE sequence=2 AND length(manifest_frame)>0"),2);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_ready_transfer WHERE sequence=2"),2);
        size_t actual_discards=0;for(size_t n=old_controls;n<controls.size();++n)if(controls[n].second.at("operation")=="discard")++actual_discards;
        EXPECT_EQ(actual_discards,2u);EXPECT_FALSE(old_turn->timedOut());EXPECT_FALSE(cancelled_turn->timedOut());
    }
};
TEST_F(ControlledGenerationReceiverController, ActualSqlRollbackCancelsOldGenerationAndRevalidatesWholeOriginalCohort){
    actual_generation_race(ControlledGenerationLocation::sql_validation,ControlledGenerationFault::none,false);
}
TEST_F(ControlledGenerationReceiverController, ActualUnpublishedHandoffCancelsOldGenerationAndRevalidatesWholeOrphanCohort){
    actual_generation_race(ControlledGenerationLocation::unpublished_handoff,ControlledGenerationFault::none,true);
}
TEST_F(ControlledGenerationReceiverController, ActualRollbackDenialKeepsGenerationPrimaryFatalWithoutRetry){
    actual_generation_race(ControlledGenerationLocation::sql_validation,ControlledGenerationFault::rollback_denied,false);
}
TEST_F(ControlledGenerationReceiverController, ActualSemanticRefusalWithNewDemandNeverBecomesGenerationRetry){
    actual_generation_race(ControlledGenerationLocation::sql_validation,ControlledGenerationFault::semantic,false);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(ControlledGenerationReceiverController, ActualDisposalFailureAfterDrainAdmissionDoesNotLeakIntoFreshRoute) {
    configure();insert(*source,controller_uuid(9961),"retained across checked failure");
    auto armed=std::make_shared<std::atomic<bool>>(false);
    auto paused=std::make_shared<ControllerPause>();pauses.push_back(paused);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[armed,paused](const char* stage){
        if(std::strcmp(stage,"disposal-before-generation-validation")==0&&armed->exchange(false)){
            paused->wait();throw db_error("actual disposal refusal after checked drain admission");
        }
    });
    connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(has_error());
    const auto original_rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
    armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return paused->ready();}));ASSERT_FALSE(has_error());
    sync_drain_result drain;
    {
        struct restore_hook {
            std::function<void()> previous=std::move(detail::sync_background_test_hooks::before_drain_admission);
            ~restore_hook(){detail::sync_background_test_hooks::before_drain_admission=std::move(previous);}
        } restore;
        // The checked drain's initial live/fatal observation has already
        // completed. Release the real SQL turn before its upload barrier.
        detail::sync_background_test_hooks::before_drain_admission=[paused]{paused->release();};
        drain=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
    }
    EXPECT_EQ(drain.state,sync_drain_state::failed);ASSERT_TRUE(drain.error);
    try{std::rethrow_exception(drain.error);}catch(const std::exception& error){
        EXPECT_NE(std::string(error.what()).find("actual disposal refusal after checked drain admission"),std::string::npos);
    }catch(...){FAIL()<<"actual primary exception type was lost";}
    ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(phase(),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),original_rows);
    EXPECT_FALSE(paused->timedOut());
    const auto old=peers[0].physical;synchronizers[0]->disconnect();connect();
    ASSERT_TRUE(until([&]{return installed(2);}));EXPECT_FALSE(old.matches(peers[0].physical));
    const auto fresh=synchronizers.back()->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
    EXPECT_NE(fresh.state,sync_drain_state::failed);EXPECT_FALSE(fresh.error);
    const auto retired=synchronizers[0]->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
    EXPECT_NE(retired.state,sync_drain_state::failed);EXPECT_FALSE(retired.error);
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),original_rows);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(ControlledGenerationReceiverController, LiveDeselectedWrapperDoesNotInheritSelectedCohortDisposalFailure) {
    configure();insert(*source,controller_uuid(9962),"two live physical wrappers");
    auto armed=std::make_shared<std::atomic<bool>>(false);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[armed](const char* stage){
        if(std::strcmp(stage,"disposal-before-generation-validation")==0&&armed->exchange(false))
            throw db_error("actual selected-cohort disposal refusal");
    });
    connect();ASSERT_TRUE(until([&]{return installed();}));ASSERT_FALSE(has_error());
    ASSERT_EQ(synchronizers.size(),1u);const auto old_endpoint=peers[0].physical;
    const auto original_rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
    // The ordinary pump closes its replaced server setup on redial. Retain
    // this actual setup separately so BOTH authenticated sources stay live.
    struct retained_source {
        relay_recovery_setup setup;
        ~retained_source(){setup.close_on_io();}
    } old_source{std::move(peers[0].setup)};
    peers[0].setup={};
    const auto actual_description=[&](const relay_recovery_setup& setup){
        const auto raw=json{{"kind","recoveryReady"},{"version",1},{"operation","describe"},
            {"requestID",controller_uuid(9963)}}.dump();
        auto charge=setup.stop_token().reserve_ready(raw.size());
        if(!charge.valid())throw db_error("live source describe reservation refused");
        const auto result=setup.ready(raw,charge);
        if(result.status_code()!=1||!result.publishable())throw db_error("actual retained source is not live");
        return json::parse(result.wire());
    };
    const auto old_description=actual_description(old_source.setup);
    ASSERT_TRUE(old_endpoint.is_current());ASSERT_TRUE(old_source.setup.stop_token().live());
    // No disconnect/close of the old wrapper: physical_routes permits both.
    connect();ASSERT_EQ(synchronizers.size(),2u);
    ASSERT_TRUE(until([&]{return installed(2);}));ASSERT_FALSE(has_error());
    const auto selected_endpoint=peers[0].physical;
    const auto selected_description=actual_description(peers[0].setup);
    ASSERT_FALSE(old_endpoint.matches(selected_endpoint));
    ASSERT_GT(std::stoull(selected_description.at("routeGeneration").get<std::string>()),
              std::stoull(old_description.at("routeGeneration").get<std::string>()));
    ASSERT_TRUE(old_endpoint.is_current());ASSERT_TRUE(selected_endpoint.is_current());
    ASSERT_TRUE(old_source.setup.stop_token().live());ASSERT_TRUE(peers[0].setup.stop_token().live());
    EXPECT_EQ(actual_description(old_source.setup),old_description);
    armed->store(true);request_recovery();ASSERT_TRUE(until([&]{return has_error();}));
    const auto selected=synchronizers.back()->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
    EXPECT_EQ(selected.state,sync_drain_state::failed);ASSERT_TRUE(selected.error);
    try{std::rethrow_exception(selected.error);}catch(const std::exception& error){
        EXPECT_NE(std::string(error.what()).find("actual selected-cohort disposal refusal"),std::string::npos);
    }catch(...){FAIL()<<"actual selected primary exception was lost";}
    const auto old=synchronizers.front()->drain_checked(std::chrono::steady_clock::now()+std::chrono::seconds(2));
    EXPECT_EQ(old.state,sync_drain_state::deadline_pending);EXPECT_FALSE(old.error);
    EXPECT_TRUE(old_endpoint.is_current());EXPECT_TRUE(selected_endpoint.is_current());
    EXPECT_TRUE(old_source.setup.stop_token().live());EXPECT_TRUE(peers[0].setup.stop_token().live());
    EXPECT_EQ(actual_description(old_source.setup),old_description);
    EXPECT_EQ(actual_description(peers[0].setup),selected_description);
    EXPECT_EQ(phase(),2);EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),original_rows);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(ControlledGenerationReceiverController, ConstructorMutexRefusalObservationPreservesExactFailureAndStore) {
    configure();insert(*source,controller_uuid(9964),"actual constructor admission");
    const auto original=snapshot();
    auto* mutex=sqlite3_db_mutex(detail::canonical_writer_custody_test_access::fault_handle(receiver->db()));
    ASSERT_NE(mutex,nullptr);
    struct HeldMutex {
        sqlite3_mutex* mutex;
        std::promise<void> entered, release;
        std::future<void> ready=entered.get_future(), resume=release.get_future();
        std::atomic<bool> timed_out{false};
        std::thread worker;
        explicit HeldMutex(sqlite3_mutex* value):mutex(value),worker([this]{
            sqlite3_mutex_enter(mutex);entered.set_value();
            timed_out.store(resume.wait_for(std::chrono::seconds(5))!=std::future_status::ready);
            sqlite3_mutex_leave(mutex);
        }){}
        void finish(){if(worker.joinable()){release.set_value();worker.join();}}
        ~HeldMutex(){finish();}
    } held(mutex);
    ASSERT_EQ(held.ready.wait_for(std::chrono::seconds(2)),std::future_status::ready);
    try {connect();FAIL()<<"actual serialized writer mutex was not refused";}
    catch(const db_error& error){EXPECT_EQ(std::string(error.what()),"audit maintenance connection is busy");}
    ASSERT_TRUE(connect_failure.observed);
    const auto& observed=connect_failure.admission;
    EXPECT_EQ(connect_failure.boundary,1u);EXPECT_EQ(connect_failure.peer,0u);EXPECT_EQ(connect_failure.wrappers,0u);
    EXPECT_EQ(observed.route_calls,1u);EXPECT_EQ(observed.maintenance_calls,1u);
    EXPECT_EQ(observed.point,detail::recovery_admission_test_hooks::route_admission_point::primary_rethrow);
    EXPECT_EQ(observed.maintenance,detail::recovery_admission_test_hooks::route_maintenance_point::mutex_busy);
    EXPECT_EQ(observed.install_state,static_cast<unsigned char>(detail::recovery_install_state::refused));
    EXPECT_TRUE(observed.primary);EXPECT_FALSE(observed.cleanup);EXPECT_FALSE(observed.postcommit);
    EXPECT_FALSE(observed.notification);EXPECT_FALSE(observed.unexpected_commit);EXPECT_FALSE(observed.inside_install);
    EXPECT_TRUE(synchronizers.empty());EXPECT_FALSE(has_error());
    held.finish();EXPECT_FALSE(held.timed_out.load());EXPECT_EQ(snapshot(),original);
    // A separately requested construction after the held physical mutex ends
    // follows the unchanged admission path. The refusal creates no retry.
    connect();ASSERT_TRUE(until([&]{return installed();}));EXPECT_FALSE(has_error());
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(AuthenticatedReadyCaptureFailureObservation, CopiesActualFirstDbExceptionWithoutReplacingIt) {
    namespace observed=lattice::detail::authenticated_ready_control_test_observation;
    const auto original=std::make_exception_ptr(lattice::db_error("actual static capture refusal"));
    const auto copy=observed::copy_capture_failure(original);
    EXPECT_EQ(copy.kind,observed::capture_exception_kind::db_error);
    EXPECT_EQ(std::string(copy.message.data(),copy.bytes),"actual static capture refusal");
    EXPECT_FALSE(copy.truncated);EXPECT_FALSE(copy.null_message);
    try{std::rethrow_exception(original);}catch(const lattice::db_error& error){EXPECT_STREQ(error.what(),"actual static capture refusal");}
    EXPECT_EQ(observed::copy_capture_failure({}).kind,observed::capture_exception_kind::none);
}
TEST(AuthenticatedReadyCaptureFailureObservation, BoundedKnownStdAndNonStdCausesRemainDistinct) {
    namespace observed=lattice::detail::authenticated_ready_control_test_observation;
    const auto large=std::make_exception_ptr(std::runtime_error(std::string(4096,'x')));
    const auto copy=observed::copy_capture_failure(large);
    EXPECT_EQ(copy.kind,observed::capture_exception_kind::std_exception);
    EXPECT_EQ(copy.bytes,511u);EXPECT_TRUE(copy.truncated);EXPECT_EQ(copy.message.back(),'\0');
    const auto other=observed::copy_capture_failure(std::make_exception_ptr(7));
    EXPECT_EQ(other.kind,observed::capture_exception_kind::non_std_exception);EXPECT_EQ(other.bytes,0u);
    const auto empty=observed::copy_capture_failure(std::make_exception_ptr(std::runtime_error("")));
    EXPECT_EQ(empty.kind,observed::capture_exception_kind::std_exception);EXPECT_EQ(empty.bytes,0u);EXPECT_FALSE(empty.null_message);
    const auto binary=observed::copy_capture_failure(std::make_exception_ptr(std::runtime_error(std::string(600,static_cast<char>(0xff)))));
    const auto hex=ControllerBoundaryTrace::capture_message_hex(binary);
    EXPECT_EQ(std::string(hex.data()),std::string(1022,'f'));EXPECT_TRUE(binary.truncated);
    const nlohmann::json wire={{"messageHex",std::string(hex.data())},{"messageEncoding","hex"}};
    EXPECT_NO_THROW((void)wire.dump());
    struct NullWhat final:std::exception {const char* what()const noexcept override{return nullptr;}};
    const auto missing=observed::copy_capture_failure(std::make_exception_ptr(NullWhat{}));
    EXPECT_EQ(missing.kind,observed::capture_exception_kind::std_exception);EXPECT_TRUE(missing.null_message);EXPECT_EQ(missing.bytes,0u);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(AuthenticatedReadyCaptureFailureObservation, DiscardCopiesRetainedPrimaryCleanupAndFlagsWithoutReplacingThem) {
    namespace observed=lattice::detail::authenticated_ready_control_test_observation;
    observed::observation out;EXPECT_FALSE(out.discard_observed);EXPECT_EQ(out.logical_sequence,-1);
    lattice::detail::recovery_install_result actual;actual.state=lattice::detail::recovery_install_state::unsettled;
    const auto primary=std::make_exception_ptr(lattice::db_error("actual retained discard primary"));
    const auto cleanup=std::make_exception_ptr(std::runtime_error(std::string(600,static_cast<char>(0xff))));
    actual.primary_error=primary;actual.cleanup_error=cleanup;actual.unexpected_commit_observed=true;
    observed::copy_discard(out,actual);
    EXPECT_TRUE(out.discard_observed);EXPECT_EQ(out.discard.state,static_cast<int32_t>(lattice::detail::recovery_install_state::unsettled));
    EXPECT_EQ(out.discard.errors,3u);EXPECT_TRUE(out.discard.unexpected_commit);
    EXPECT_EQ(out.discard_primary.kind,observed::capture_exception_kind::db_error);
    EXPECT_EQ(std::string(out.discard_primary.message.data(),out.discard_primary.bytes),"actual retained discard primary");
    EXPECT_EQ(out.discard_cleanup.bytes,511u);EXPECT_TRUE(out.discard_cleanup.truncated);
    EXPECT_EQ(actual.primary_error,primary);EXPECT_EQ(actual.cleanup_error,cleanup);
    observed::observation known;const lattice::detail::recovery_install_result no_errors;observed::copy_discard(known,no_errors);
    EXPECT_EQ(known.discard_primary.kind,observed::capture_exception_kind::none);
    EXPECT_EQ(known.discard_cleanup.kind,observed::capture_exception_kind::none);EXPECT_EQ(known.discard.errors,0u);
}
#endif


// TEST-only causal control: real queued controller admission vs a fresh read.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct ControllerTurnReadTrace {
    sqlite3* native=nullptr;
    std::atomic<bool> watch{false},real_commit{false},reader_entered{false},reader_before_commit{false};
    std::mutex mutex;std::condition_variable changed;
    static int observe(unsigned event,void* context,void* statement,void*)noexcept {
        auto& self=*static_cast<ControllerTurnReadTrace*>(context);
        if(!self.watch.load())return 0;
        const char* sql=sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
        if(!sql)return 0;
        if(event==SQLITE_TRACE_PROFILE&&std::strcmp(sql,"COMMIT")==0&&sqlite3_get_autocommit(self.native)==1)
            self.real_commit.store(true);
        if(event==SQLITE_TRACE_STMT&&std::strcmp(sql,"SELECT 731 AS n")==0){
            self.reader_before_commit.store(!self.real_commit.load());self.reader_entered.store(true);self.changed.notify_all();
        }
        return 0;
    }
};
struct ControllerTurnProbeState {
    std::shared_ptr<ControllerPause> paused=std::make_shared<ControllerPause>();
    std::atomic<unsigned> install_calls{0},actual_mutex_deferrals{0};
    std::atomic<bool> hold_published{false},pause_selected{false};
    std::shared_ptr<detail::recovery_cohort_progress_observation::record> progress=
        std::make_shared<detail::recovery_cohort_progress_observation::record>();
    std::atomic<uint64_t> selected_scopes{0};std::array<std::atomic<uint64_t>,6> admissions{};
};
struct IntentProgressScope {
    std::shared_ptr<ControllerTurnProbeState> custody;
    detail::recovery_cohort_progress_observation::scope scoped;
    explicit IntentProgressScope(std::shared_ptr<ControllerTurnProbeState> state)
        :custody(std::move(state)),scoped(*custody->progress){++custody->selected_scopes;}
};
void report_intent_progress(const ControllerTurnProbeState& state,size_t queued,uint64_t entries,
    uint64_t phase_returns,uint64_t revision_returns,int64_t phase,int64_t revision,
    size_t error_count,const std::array<unsigned char,512>& error,size_t error_bytes)noexcept {
    struct Copy {uint64_t calls,bodies,completed,returns;uint32_t outcome;};std::array<Copy,3> copy{};
    const auto& record=*state.progress;const auto scopes=state.selected_scopes.load();const auto reservations=record.reservations.load();
    for(size_t i=0;i<copy.size();++i){copy[i]={record.calls[i].load(),record.bodies[i].load(),record.completed[i].load(),record.results[i].returns.load(),record.results[i].outcome.load(std::memory_order_acquire)};}
    std::array<uint64_t,6> admissions{};for(size_t i=0;i<admissions.size();++i)admissions[i]=state.admissions[i].load();
    constexpr char digits[]="0123456789abcdef";std::array<char,1025> hex{};
    for(size_t i=0;i<error_bytes;++i){hex[2*i]=digits[error[i]>>4];hex[2*i+1]=digits[error[i]&15];}
    // Actual independent detached copies, after the original predicate ended.
    // No SQL, Native query, additional predicate, clock or callback is invoked.
    std::fprintf(stderr,"intent-final-drive afterPredicateFinalized=1 scopes=%llu reservations=%llu queuedJobs=%zu predicateEntries=%llu phaseReturns=%llu revisionReturns=%llu lastPhase=%lld lastRevision=%lld errorCount=%zu firstErrorBytes=%zu firstErrorHex=%s independentCounters=1\n",
        static_cast<unsigned long long>(scopes),static_cast<unsigned long long>(reservations),queued,
        static_cast<unsigned long long>(entries),static_cast<unsigned long long>(phase_returns),static_cast<unsigned long long>(revision_returns),static_cast<long long>(phase),static_cast<long long>(revision),error_count,error_bytes,hex.data());
    const char* labels[]={"classification","install","resume"};
    for(size_t i=0;i<copy.size();++i){const auto& c=copy[i];std::fprintf(stderr,"intent-final-drive stage=%s calls=%llu bodies=%llu completed=%llu returns=%llu resultObserved=%d state=%u deferred=%u flags=%u\n",labels[i],static_cast<unsigned long long>(c.calls),static_cast<unsigned long long>(c.bodies),static_cast<unsigned long long>(c.completed),static_cast<unsigned long long>(c.returns),int((c.outcome&0x80000000u)!=0),c.outcome&255u,(c.outcome>>8)&255u,(c.outcome>>16)&31u);}
    const char* reasons[]={"connection-mutex","engine-read","store-gate","core-write","none-unexpected","unknown"};
    for(size_t i=0;i<admissions.size();++i)std::fprintf(stderr,"intent-final-drive admission=%s count=%llu\n",reasons[i],static_cast<unsigned long long>(admissions[i]));
}
TEST_F(RecoveryRestrictedFirstClaim, PendingControllerOwnedTurnPrecedesFreshOrdinaryReadAfterActualDeferral) {
    configure(1,false);open_manual_receiver();
    // The real controller retains its probe beyond this TestBody. Callback
    // captures own passive state until actual route/controller retirement;
    // they never borrow a destroyed stack counter or pause shared_ptr.
    const auto state=std::make_shared<ControllerTurnProbeState>();const auto paused=state->paused;
    auto& install_calls=state->install_calls;auto& actual_mutex_deferrals=state->actual_mutex_deferrals;
    auto& hold_published=state->hold_published;
    std::atomic<bool> stop_pump{false},reader_started{false};
    std::exception_ptr reader_error,pump_error;int64_t reader_value=0;
    std::thread fresh_reader,pump_worker;
    ControllerTurnReadTrace trace;
    trace.native=detail::canonical_writer_custody_test_access::fault_handle(receiver->db());
    int trace_removal=SQLITE_ERROR;bool cleanup_done=false;
    const auto cleanup=[&]()noexcept {
        if(cleanup_done)return;
        paused->release();if(writer_hold)writer_hold->release();stop_pump.store(true);
        if(pump_worker.joinable())pump_worker.join();if(fresh_reader.joinable())fresh_reader.join();
        trace.watch.store(false);trace_removal=sqlite3_trace_v2(trace.native,0,nullptr,nullptr);cleanup_done=true;
    };
    struct AlwaysReleaseAndJoin {const decltype(cleanup)& action;~AlwaysReleaseAndJoin()noexcept{action();}} ending{cleanup};
    ASSERT_EQ(sqlite3_trace_v2(trace.native,SQLITE_TRACE_STMT|SQLITE_TRACE_PROFILE,ControllerTurnReadTrace::observe,&trace),SQLITE_OK);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,
        [this,state](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"install")==0&&state->install_calls.fetch_add(1)==0){
                writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);state->hold_published.store(true);
            }
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){
                auto retained=std::make_shared<IntentProgressScope>(state);
                if(state->actual_mutex_deferrals.load()!=0&&!state->pause_selected.exchange(true))state->paused->wait();
                return retained;
            }
            return {};
        },[state](const char* actual){
            if(state->hold_published.load()&&std::strcmp(actual,"admission-deferred-connection-mutex")==0)++state->actual_mutex_deferrals;
            const char* labels[]={"admission-deferred-connection-mutex","admission-deferred-engine-read","admission-deferred-store-gate","admission-deferred-core-write","admission-deferred-none-unexpected","admission-deferred-unknown"};
            for(size_t i=0;i<6;++i)if(std::strcmp(actual,labels[i])==0){++state->admissions[i];break;}
        });
    connect();ASSERT_TRUE(drive([&]{return hold_published.load()&&actual_mutex_deferrals.load()!=0;}));
    ASSERT_TRUE(writer_hold);EXPECT_FALSE(writer_hold->timed_out);
    // The real first install was refused before an owned transaction. No
    // new callback SQL, transaction flag or permission is manufactured here.
    trace.watch.store(true);
    fresh_reader=std::thread([&]{reader_started.store(true);try{
        const auto rows=receiver->db().query("SELECT 731 AS n");reader_value=std::get<int64_t>(rows.at(0).at("n"));
    }catch(...){reader_error=std::current_exception();}});
    ASSERT_TRUE(observe([&]{return reader_started.load();}));
    writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);
    // Run the real original scheduler queue on one owned thread. Pausing the
    // next selected turn stops it before any next-turn owned SQL can COMMIT.
    pump_worker=std::thread([&]{try{while(!stop_pump.load()){
        if(!manual->run_one())std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }}catch(...){pump_error=std::current_exception();}});
    ASSERT_TRUE(observe([&]{return paused->ready();}));EXPECT_FALSE(paused->timedOut());
    bool entered_while_pending=false;
    {std::unique_lock lock(trace.mutex);entered_while_pending=trace.changed.wait_for(lock,std::chrono::milliseconds(100),[&]{return trace.reader_entered.load();});}
    // The baseline has no intent and permits the fresh read before the held
    // controller turn. This assertion is intended to fail on that real product.
    EXPECT_FALSE(entered_while_pending);
    paused->release();ASSERT_TRUE(observe([&]{return trace.reader_entered.load();}));
    // Join the completed reader and stop recording BEFORE testing the actual
    // initial COMMIT/read ordering. Keep the one existing manual executor alive
    // through final synchronous SQL; removing its trace waits until that worker
    // is joined. No new executor or query-side scheduler reentry is introduced.
    if(fresh_reader.joinable())fresh_reader.join();trace.watch.store(false);
    EXPECT_FALSE(paused->timedOut());EXPECT_FALSE(writer_hold->timed_out);
    EXPECT_FALSE(reader_error);EXPECT_EQ(reader_value,731);
    EXPECT_TRUE(trace.real_commit.load());EXPECT_FALSE(trace.reader_before_commit.load());
    const auto drive_with_owned_executor=[&](auto predicate){
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate())return true;
            // The already-running worker alone calls manual->run_one(). Keep
            // both original predicate boundaries; never stop it before SQL.
            if(predicate())return true;std::this_thread::sleep_for(std::chrono::milliseconds(1));}
        return predicate();
    };
    uint64_t predicate_entries=0,phase_returns=0,revision_returns=0;int64_t last_phase=INT64_MIN,last_revision=INT64_MIN;
    const bool finished=drive_with_owned_executor([&]{++predicate_entries;const auto actual_phase=phase();++phase_returns;last_phase=actual_phase;
        if(actual_phase!=0)return false;
        const auto actual_revision=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1");++revision_returns;last_revision=actual_revision;return actual_revision==1;});
    // This same unwind guard also handles exceptions/fatal setup assertions.
    // Only after the final predicate ends stop/join its sole executor, then
    // remove the actual native trace and inspect its worker exception safely.
    cleanup();EXPECT_EQ(trace_removal,SQLITE_OK);EXPECT_FALSE(pump_error);
    if(!finished){std::array<unsigned char,512> first{};size_t count=0,bytes=0;
        {std::lock_guard lock(errors_mutex);count=errors.size();if(count){bytes=std::min(first.size(),errors.front().size());if(bytes)std::memcpy(first.data(),errors.front().data(),bytes);}}
        const auto queued=manual->queued_snapshot(); // Only after the old predicate finalized; no work is run.
        report_intent_progress(*state,queued,predicate_entries,phase_returns,revision_returns,last_phase,last_revision,count,first,bytes);
    }
    ASSERT_TRUE(finished);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),0);EXPECT_FALSE(has_error());
}
} // namespace
#endif

// DATA discriminator, not a scheduler-independent controller progress claim.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(RecoveryRestrictedFirstClaim, StoppedSoleManualPumpLeavesFreshPhaseReadBehindActualControllerIntent) {
    configure(1,false);open_manual_receiver();
    struct State {
        std::atomic<unsigned> installs{0},deferrals{0},selected_turns{0};
        std::atomic<bool> hold_published{false},private_turn_held{false};
    };
    const auto state=std::make_shared<State>();
    const auto private_turn=std::make_shared<ControllerPause>();pauses.push_back(private_turn);
    const auto release=[&]()noexcept{private_turn->release();if(writer_hold)writer_hold->release();};
    struct AlwaysRelease {const decltype(release)& action;~AlwaysRelease()noexcept{action();}} ending{release};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,
        [this,state,private_turn](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){
                ++state->selected_turns;
                if(state->deferrals.load()!=0&&!state->private_turn_held.exchange(true))private_turn->wait();
            }
            if(std::strcmp(stage,"install")==0&&state->installs.fetch_add(1)==0){
                writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);state->hold_published.store(true);
            }
            return {};
        },[state](const char* actual){
            if(state->hold_published.load()&&std::strcmp(actual,"admission-deferred-connection-mutex")==0)++state->deferrals;
        });
    connect();ASSERT_TRUE(drive([&]{return state->hold_published.load()&&state->deferrals.load()!=0;}));
    ASSERT_TRUE(writer_hold);writer_hold->release();EXPECT_FALSE(writer_hold->timed_out);
    // The manual route deque is no longer the controller executor. Hold the
    // next genuine private turn at its existing off-leaf boundary, after its
    // exact intent capture and before any intermediate SQL/install can run.
    // Until query ending, no fixture scheduler executor or Native bypass runs.
    ASSERT_TRUE(observe([&]{return private_turn->ready()&&manual->queued_snapshot()!=0;}));ASSERT_FALSE(has_error());
    const auto selected_before=state->selected_turns.load();const auto queued_before=manual->queued_snapshot();
    auto* const actual_writer=&receiver->db();
    namespace observed=detail::recovery_topology_wait_observation;
    observed::record actual{};std::exception_ptr query_error;int64_t actual_phase=INT64_MIN;
    try {
        observed::scope observation(actual);
        actual_phase=scalar(*receiver,"SELECT phase AS n FROM _lattice_producer_continuity");
    }catch(...){query_error=std::current_exception();}
    const auto queued_after=manual->queued_snapshot();const auto selected_after=state->selected_turns.load();
    // All copies are inspected only after the query returned/threw and its
    // same-thread scope ended. No observer runs under Native/metadata leaves.
    std::fprintf(stderr,"stopped-pump-intent queryEnded=1 queryError=%d calls=%llu selectedWaits=%llu priorSelectedOmitted=%llu selected=%d wrapper=%llu original=%llu slot=%zu token=%llu excluded=%d selectionNow=%lld deadline=%lld deadlineObserved=%d deadlineExit=%lld returned=%d sleeps=%llu queuedBefore=%zu queuedAfter=%zu selectedTurnsBefore=%u selectedTurnsAfter=%u actualPhase=%lld queueMembershipTyped=0\n",
        int(bool(query_error)),static_cast<unsigned long long>(actual.calls),static_cast<unsigned long long>(actual.selected_waits),
        static_cast<unsigned long long>(actual.selected_waits?actual.selected_waits-1:0),int(actual.selected),
        static_cast<unsigned long long>(actual.wrapper),static_cast<unsigned long long>(actual.original),actual.slot,
        static_cast<unsigned long long>(actual.token),int(actual.excluded),static_cast<long long>(actual.selection_now),
        static_cast<long long>(actual.deadline),int(actual.deadline_observed),static_cast<long long>(actual.deadline_exit),
        int(actual.returned),static_cast<unsigned long long>(actual.sleeps),queued_before,queued_after,selected_before,selected_after,
        static_cast<long long>(actual_phase));
    if(query_error)std::rethrow_exception(query_error); // Preserve the actual first query exception.
    ASSERT_TRUE(actual.selected);ASSERT_GT(actual.selected_waits,0u);
    EXPECT_EQ(actual.wrapper,reinterpret_cast<std::uintptr_t>(actual_writer));EXPECT_NE(actual.original,0u);
    EXPECT_NE(actual.token,0u);EXPECT_NE(actual.token,UINT64_MAX);EXPECT_FALSE(actual.excluded);
    EXPECT_GT(actual.deadline,actual.selection_now);EXPECT_TRUE(actual.deadline_observed);
    EXPECT_GE(actual.deadline_exit,actual.deadline);EXPECT_TRUE(actual.returned);
    EXPECT_GT(queued_before,0u);EXPECT_GT(queued_after,0u);EXPECT_EQ(selected_after,selected_before);
    EXPECT_NE(actual_phase,0);EXPECT_FALSE(has_error());
    // Only now release that genuine private turn and resume route pumping.
    // Its original episode deadline actually elapsed; no clock is renewed.
    private_turn->release();
    ASSERT_TRUE(drive([&]{return has_error();}));
    EXPECT_TRUE(error_contains("controller admission retry budget exhausted; gate remains closed"));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
}
}
#endif


#include "RecoveryControllerExecutorTestAccess.hpp"

// TEST-only real-worker control. No manual executor or setup SQL polling.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct SoleFifoAdmissionState {
    std::weak_ptr<lattice_db> owner;
    std::unique_ptr<FirstClaimWriterHold> holder; // published once before hold_published
    std::atomic<bool> holder_release_started{false};
    std::atomic<bool> query_selected{false},callback_on_private{false},lane_identity_changed{false};
    std::mutex lane_mutex;detail::recovery_controller_executor_test_access::ticket lane;
    std::shared_ptr<scheduler> queue;
    std::atomic<unsigned> installs{0},deferrals{0},selected_turns{0};
    std::atomic<bool> hold_published{false},enqueue_once{false},enqueued{false};
    std::atomic<bool> callback_on_worker{false},query_on_worker{false},query_started{false},query_finished{false};
    std::atomic<bool> install_active{false},install_commit{false},install_returned{false},read_entered{false};
    std::atomic<bool> read_before_install_commit{false},read_before_install_returned{false};
    sqlite3* native=nullptr;
    // The worker alone writes these until it is joined. Test reads occur only
    // after actual shutdown/drain, never concurrently with the passive POD.
    detail::recovery_topology_wait_observation::record query_wait{};
    std::exception_ptr enqueue_error,query_error;
    int64_t query_value=INT64_MIN;
    unsigned turns_before=0,turns_after=0;
    static int trace(unsigned event,void* context,void* statement,void*)noexcept {
        auto& state=*static_cast<SoleFifoAdmissionState*>(context);
        const char* sql=sqlite3_sql(static_cast<sqlite3_stmt*>(statement));if(!sql)return 0;
        if(event==SQLITE_TRACE_PROFILE&&state.install_active.load()&&std::strcmp(sql,"COMMIT")==0&&
           sqlite3_get_autocommit(state.native)==1)state.install_commit.store(true);
        if(event==SQLITE_TRACE_STMT&&std::strcmp(sql,"SELECT 731 AS n")==0){
            state.read_before_install_commit.store(!state.install_commit.load());
            state.read_before_install_returned.store(!state.install_returned.load());state.read_entered.store(true);
        }
        return 0;
    }
};
struct SoleFifoInstallScope {
    std::shared_ptr<SoleFifoAdmissionState> state;
    explicit SoleFifoInstallScope(std::shared_ptr<SoleFifoAdmissionState> actual):state(std::move(actual)){
        state->install_active.store(true);
    }
    ~SoleFifoInstallScope(){state->install_active.store(false);}
};
TEST_F(RecoveryRestrictedFirstClaim, PendingControllerIntentCannotBlockItsSoleRealFifoWorkerBeforeInstall) {
    constexpr unsigned required_deferrals=1;
    configure(); // The unchanged real factory supplies one std_thread_scheduler.
    const auto state=std::make_shared<SoleFifoAdmissionState>();state->owner=receiver;
    state->queue=receiver->get_scheduler();ASSERT_TRUE(state->queue);
    ASSERT_NE(std::dynamic_pointer_cast<std_thread_scheduler>(state->queue),nullptr);
    state->native=detail::canonical_writer_custody_test_access::fault_handle(receiver->db());
    // Allocate failure-diagnostic custody before any controlled callback exists.
    const std::string retained_store=receiver->config().path;
    const auto ending_debt=[&](const char* stage)noexcept{
        std::fprintf(stderr,"sole-fifo-intent ending-debt stage=%s process=%ld group=%ld retained_store=%s\n",
            stage,static_cast<long>(::getpid()),static_cast<long>(::getpgrp()),retained_store.c_str());
        std::fflush(stderr);::_exit(93); // no Native/fixture cleanup on unknown ending
    };
    std::exception_ptr ending_error;int trace_removal=SQLITE_ERROR;bool ending_done=false;
    const auto ending=[&]()noexcept{
        if(ending_done)return;
        // Release the actual SQLite holder first. Then drain/join the SAME
        // worker; no replacement executor runs its callbacks. The constant
        // query's real intent wait retains its original fixed episode end.
        try{if(state->hold_published.load())state->holder->release();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("holder-join");}
        // Real route retirement precedes original-worker drain. No private
        // controller capture may remain live when the trace context is removed.
        synchronizers.clear();
        try{state->queue->shutdown();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("worker-drain-join");}
        // Worker ending also settles any first-scope construction that had
        // not yet published a holder when cleanup started. Main alone joins it.
        try{if(state->holder)state->holder->release();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("late-holder-join");}
        detail::recovery_controller_executor_test_access::ticket lane;
        {std::lock_guard lock(state->lane_mutex);lane=state->lane;}
        if(!detail::recovery_controller_executor_test_access::joined(lane,std::chrono::seconds(3)))ending_debt("private-controller-actual-join");
        trace_removal=sqlite3_trace_v2(state->native,0,nullptr,nullptr);
        if(trace_removal!=SQLITE_OK)ending_debt("trace-removal");
        ending_done=true;
    };
    struct AlwaysEnd {const decltype(ending)& action;~AlwaysEnd()noexcept{action();}} release{ending};
    ASSERT_EQ(sqlite3_trace_v2(state->native,SQLITE_TRACE_STMT|SQLITE_TRACE_PROFILE,SoleFifoAdmissionState::trace,state.get()),SQLITE_OK);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [state](const char* stage){
            // Existing controller1745 observation follows the successfully
            // returned owned install; PROFILE/autocommit alone is not proof.
            if(std::strcmp(stage,"install-committed")==0)state->install_returned.store(true);
        },
        [this,state](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){
                ++state->selected_turns;auto actual=detail::recovery_controller_executor_test_access::current();
                {std::lock_guard lock(state->lane_mutex);if(!state->lane)state->lane=std::move(actual);else if(state->lane!=actual)state->lane_identity_changed.store(true);}
            }
            if(std::strcmp(stage,"install")!=0)return {};
            if(state->installs.fetch_add(1)==0){
                state->holder=std::make_unique<FirstClaimWriterHold>(*receiver);state->hold_published.store(true);
            }
            return std::make_shared<SoleFifoInstallScope>(state);
        },[this,state](const char* actual){
            if(!state->hold_published.load()||std::strcmp(actual,"admission-deferred-connection-mutex")!=0)return;
            ++state->deferrals;
            if(state->enqueue_once.exchange(true))return;
            state->callback_on_worker.store(state->queue->is_on_thread());
            state->callback_on_private.store(bool(detail::recovery_controller_executor_test_access::current()));
            try{
                state->queue->invoke([state]{
                    state->query_on_worker.store(state->queue->is_on_thread());state->query_started.store(true);
                    state->turns_before=state->selected_turns.load();
                    try{
                        detail::recovery_topology_wait_observation::scope observed(state->query_wait,&state->query_selected);
                        auto owner=state->owner.lock();if(!owner)throw db_error("sole FIFO control owner retired");
                        const auto rows=owner->db().query("SELECT 731 AS n");
                        state->query_value=std::get<int64_t>(rows.at(0).at("n"));
                    }catch(...){state->query_error=std::current_exception();}
                    state->turns_after=state->selected_turns.load();state->query_finished.store(true);
                });
                state->enqueued.store(true);
            }catch(...){state->enqueue_error=std::current_exception();}
            // The main test thread alone releases/joins the published holder.
            // This callback performs no release/join and cannot race cleanup.
        });
    connect();
    // Mechanical transport stays live; predicate has no SQL and never pumps
    // another scheduler. This is the original five-second observation bound.
    const bool finished_in_original_bound=until([&]{
        if(state->enqueued.load()&&state->query_selected.load()&&state->deferrals.load()>=required_deferrals&&!state->holder_release_started.exchange(true))state->holder->release();
        return state->query_finished.load();
    });
    ending(); // All worker-owned data and trace context are settled before reads.
    const auto& actual=state->query_wait;
    size_t controller_error_count=0,controller_first_bytes=0;bool controller_first_truncated=false;
    std::array<unsigned char,512> controller_first{};
    {std::lock_guard lock(errors_mutex);controller_error_count=errors.size();
        if(controller_error_count){controller_first_bytes=std::min(controller_first.size(),errors.front().size());
            controller_first_truncated=controller_first_bytes!=errors.front().size();
            if(controller_first_bytes)std::memcpy(controller_first.data(),errors.front().data(),controller_first_bytes);}}
    // Detached fixed byte copy only: no formatting/callback under the mutex,
    // no SQL, extra predicate, Native lookup or delivery proof is synthesized.
    constexpr char hex_digits[]="0123456789abcdef";std::array<char,1025> controller_first_hex{};
    for(size_t n=0;n<controller_first_bytes;++n){controller_first_hex[2*n]=hex_digits[controller_first[n]>>4];controller_first_hex[2*n+1]=hex_digits[controller_first[n]&15];}
    std::fprintf(stderr,"sole-fifo-intent originalPredicateFinalized=1 finishedInOriginalBound=%d enqueued=%d callbackOnWorker=%d queryOnWorker=%d queryStarted=%d queryFinished=%d installs=%u deferrals=%u selectedTurnsBefore=%u selectedTurnsAfter=%u selected=%d selectedWaits=%llu priorSelectedOmitted=%llu slot=%zu token=%llu excluded=%d selectionNow=%lld deadline=%lld deadlineObserved=%d deadlineExit=%lld returned=%d installCommit=%d installReturned=%d readEntered=%d readBeforeInstallCommit=%d readBeforeInstallReturned=%d enqueueError=%d queryError=%d endingError=%d traceRemoval=%d controllerErrorCount=%zu firstControllerErrorBytes=%zu firstControllerErrorTruncated=%d firstControllerErrorHex=%s independentCopies=1\n",
        int(finished_in_original_bound),int(state->enqueued.load()),int(state->callback_on_worker.load()),int(state->query_on_worker.load()),
        int(state->query_started.load()),int(state->query_finished.load()),state->installs.load(),state->deferrals.load(),
        state->turns_before,state->turns_after,int(actual.selected),static_cast<unsigned long long>(actual.selected_waits),
        static_cast<unsigned long long>(actual.selected_waits?actual.selected_waits-1:0),actual.slot,static_cast<unsigned long long>(actual.token),
        int(actual.excluded),static_cast<long long>(actual.selection_now),static_cast<long long>(actual.deadline),int(actual.deadline_observed),
        static_cast<long long>(actual.deadline_exit),int(actual.returned),int(state->install_commit.load()),int(state->install_returned.load()),int(state->read_entered.load()),
        int(state->read_before_install_commit.load()),int(state->read_before_install_returned.load()),int(bool(state->enqueue_error)),int(bool(state->query_error)),int(bool(ending_error)),trace_removal,
        controller_error_count,controller_first_bytes,int(controller_first_truncated),controller_first_hex.data());
    EXPECT_EQ(trace_removal,SQLITE_OK);EXPECT_FALSE(ending_error);
    if(state->enqueue_error)std::rethrow_exception(state->enqueue_error);
    if(state->query_error)std::rethrow_exception(state->query_error);
    ASSERT_TRUE(state->holder);EXPECT_FALSE(state->holder->timed_out);
    ASSERT_GT(state->deferrals.load(),0u);ASSERT_TRUE(state->enqueued.load());
    EXPECT_FALSE(state->callback_on_worker.load());EXPECT_TRUE(state->callback_on_private.load());EXPECT_TRUE(state->query_on_worker.load());
    EXPECT_FALSE(state->lane_identity_changed.load());EXPECT_TRUE(state->query_selected.load());
    EXPECT_GE(state->deferrals.load(),required_deferrals);EXPECT_GT(state->turns_after,state->turns_before);
    EXPECT_TRUE(actual.selected);EXPECT_FALSE(actual.excluded);EXPECT_FALSE(actual.deadline_observed);
    ASSERT_TRUE(state->query_started.load());ASSERT_TRUE(state->query_finished.load());EXPECT_EQ(state->query_value,731);
    // This is a correctness oracle, not permission to skip the real intent.
    // On a self-blocking baseline the selected query waits until that episode
    // expires with no intervening controller turn, then enters before install.
    EXPECT_TRUE(finished_in_original_bound);
    EXPECT_TRUE(state->install_commit.load());EXPECT_TRUE(state->install_returned.load());EXPECT_TRUE(state->read_entered.load());
    EXPECT_FALSE(state->read_before_install_commit.load());
    // Post-return observer timing is printed independently. It is not an
    // admission fence after the real Native COMMIT/mutex have been released.
    EXPECT_EQ(controller_error_count,0u);
}

TEST_F(RecoveryRestrictedFirstClaim, PrivateTimedRetriesProgressWhileOriginalFifoWorkerWaitsForInstall) {
    constexpr unsigned required_deferrals=3;
    configure(); // The unchanged real factory supplies one std_thread_scheduler.
    const auto state=std::make_shared<SoleFifoAdmissionState>();state->owner=receiver;
    const auto pacer_pause=std::make_shared<ControllerPause>();
    const auto prior_pacer=detail::sync_background_test_hooks::pacer_wait;
    struct RestorePacer{std::shared_ptr<const detail::sync_background_test_hooks::pacer_wait_schedule> prior;
        ~RestorePacer(){detail::sync_background_test_hooks::pacer_wait=std::move(prior);}} restore_pacer{prior_pacer};
    auto pacer_hook=std::make_shared<detail::sync_background_test_hooks::pacer_wait_schedule>();
    pacer_hook->before_receiver_discovery=[pacer_pause]{pacer_pause->wait();};detail::sync_background_test_hooks::pacer_wait=std::move(pacer_hook);
    state->queue=receiver->get_scheduler();ASSERT_TRUE(state->queue);
    ASSERT_NE(std::dynamic_pointer_cast<std_thread_scheduler>(state->queue),nullptr);
    state->native=detail::canonical_writer_custody_test_access::fault_handle(receiver->db());
    // Allocate failure-diagnostic custody before any controlled callback exists.
    const std::string retained_store=receiver->config().path;
    const auto ending_debt=[&](const char* stage)noexcept{
        std::fprintf(stderr,"sole-fifo-intent ending-debt stage=%s process=%ld group=%ld retained_store=%s\n",
            stage,static_cast<long>(::getpid()),static_cast<long>(::getpgrp()),retained_store.c_str());
        std::fflush(stderr);::_exit(93); // no Native/fixture cleanup on unknown ending
    };
    std::exception_ptr ending_error;int trace_removal=SQLITE_ERROR;bool ending_done=false;
    const auto ending=[&]()noexcept{
        if(ending_done)return;
        // Release the actual SQLite holder first. Then drain/join the SAME
        // worker; no replacement executor runs its callbacks. The constant
        // query's real intent wait retains its original fixed episode end.
        try{if(state->hold_published.load())state->holder->release();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("holder-join");}
        // Real route retirement precedes original-worker drain. No private
        // controller capture may remain live when the trace context is removed.
        pacer_pause->release();
        synchronizers.clear();
        try{state->queue->shutdown();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("worker-drain-join");}
        // Worker ending also settles any first-scope construction that had
        // not yet published a holder when cleanup started. Main alone joins it.
        try{if(state->holder)state->holder->release();}catch(...){if(!ending_error)ending_error=std::current_exception();ending_debt("late-holder-join");}
        detail::recovery_controller_executor_test_access::ticket lane;
        {std::lock_guard lock(state->lane_mutex);lane=state->lane;}
        if(!detail::recovery_controller_executor_test_access::joined(lane,std::chrono::seconds(3)))ending_debt("private-controller-actual-join");
        trace_removal=sqlite3_trace_v2(state->native,0,nullptr,nullptr);
        if(trace_removal!=SQLITE_OK)ending_debt("trace-removal");
        ending_done=true;
    };
    struct AlwaysEnd {const decltype(ending)& action;~AlwaysEnd()noexcept{action();}} release{ending};
    ASSERT_EQ(sqlite3_trace_v2(state->native,SQLITE_TRACE_STMT|SQLITE_TRACE_PROFILE,SoleFifoAdmissionState::trace,state.get()),SQLITE_OK);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [state](const char* stage){
            // Existing controller1745 observation follows the successfully
            // returned owned install; PROFILE/autocommit alone is not proof.
            if(std::strcmp(stage,"install-committed")==0)state->install_returned.store(true);
        },
        [this,state](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){
                ++state->selected_turns;auto actual=detail::recovery_controller_executor_test_access::current();
                {std::lock_guard lock(state->lane_mutex);if(!state->lane)state->lane=std::move(actual);else if(state->lane!=actual)state->lane_identity_changed.store(true);}
            }
            if(std::strcmp(stage,"install")!=0)return {};
            if(state->installs.fetch_add(1)==0){
                state->holder=std::make_unique<FirstClaimWriterHold>(*receiver);state->hold_published.store(true);
            }
            return std::make_shared<SoleFifoInstallScope>(state);
        },[this,state](const char* actual){
            if(!state->hold_published.load()||std::strcmp(actual,"admission-deferred-connection-mutex")!=0)return;
            ++state->deferrals;
            if(state->enqueue_once.exchange(true))return;
            state->callback_on_worker.store(state->queue->is_on_thread());
            state->callback_on_private.store(bool(detail::recovery_controller_executor_test_access::current()));
            try{
                state->queue->invoke([state]{
                    state->query_on_worker.store(state->queue->is_on_thread());state->query_started.store(true);
                    state->turns_before=state->selected_turns.load();
                    try{
                        detail::recovery_topology_wait_observation::scope observed(state->query_wait,&state->query_selected);
                        auto owner=state->owner.lock();if(!owner)throw db_error("sole FIFO control owner retired");
                        const auto rows=owner->db().query("SELECT 731 AS n");
                        state->query_value=std::get<int64_t>(rows.at(0).at("n"));
                    }catch(...){state->query_error=std::current_exception();}
                    state->turns_after=state->selected_turns.load();state->query_finished.store(true);
                });
                state->enqueued.store(true);
            }catch(...){state->enqueue_error=std::current_exception();}
            // The main test thread alone releases/joins the published holder.
            // This callback performs no release/join and cannot race cleanup.
        });
    connect();
    // Mechanical transport stays live; predicate has no SQL and never pumps
    // another scheduler. This is the original five-second observation bound.
    const bool finished_in_original_bound=until([&]{
        if(state->enqueued.load()&&state->query_selected.load()&&state->deferrals.load()>=required_deferrals&&pacer_pause->ready()&&!pacer_pause->timedOut()&&!state->holder_release_started.exchange(true))state->holder->release();
        return state->query_finished.load();
    });
    ending(); // All worker-owned data and trace context are settled before reads.
    const auto& actual=state->query_wait;
    size_t controller_error_count=0,controller_first_bytes=0;bool controller_first_truncated=false;
    std::array<unsigned char,512> controller_first{};
    {std::lock_guard lock(errors_mutex);controller_error_count=errors.size();
        if(controller_error_count){controller_first_bytes=std::min(controller_first.size(),errors.front().size());
            controller_first_truncated=controller_first_bytes!=errors.front().size();
            if(controller_first_bytes)std::memcpy(controller_first.data(),errors.front().data(),controller_first_bytes);}}
    // Detached fixed byte copy only: no formatting/callback under the mutex,
    // no SQL, extra predicate, Native lookup or delivery proof is synthesized.
    constexpr char hex_digits[]="0123456789abcdef";std::array<char,1025> controller_first_hex{};
    for(size_t n=0;n<controller_first_bytes;++n){controller_first_hex[2*n]=hex_digits[controller_first[n]>>4];controller_first_hex[2*n+1]=hex_digits[controller_first[n]&15];}
    std::fprintf(stderr,"sole-fifo-intent originalPredicateFinalized=1 finishedInOriginalBound=%d enqueued=%d callbackOnWorker=%d queryOnWorker=%d queryStarted=%d queryFinished=%d installs=%u deferrals=%u selectedTurnsBefore=%u selectedTurnsAfter=%u selected=%d selectedWaits=%llu priorSelectedOmitted=%llu slot=%zu token=%llu excluded=%d selectionNow=%lld deadline=%lld deadlineObserved=%d deadlineExit=%lld returned=%d installCommit=%d installReturned=%d readEntered=%d readBeforeInstallCommit=%d readBeforeInstallReturned=%d enqueueError=%d queryError=%d endingError=%d traceRemoval=%d controllerErrorCount=%zu firstControllerErrorBytes=%zu firstControllerErrorTruncated=%d firstControllerErrorHex=%s independentCopies=1\n",
        int(finished_in_original_bound),int(state->enqueued.load()),int(state->callback_on_worker.load()),int(state->query_on_worker.load()),
        int(state->query_started.load()),int(state->query_finished.load()),state->installs.load(),state->deferrals.load(),
        state->turns_before,state->turns_after,int(actual.selected),static_cast<unsigned long long>(actual.selected_waits),
        static_cast<unsigned long long>(actual.selected_waits?actual.selected_waits-1:0),actual.slot,static_cast<unsigned long long>(actual.token),
        int(actual.excluded),static_cast<long long>(actual.selection_now),static_cast<long long>(actual.deadline),int(actual.deadline_observed),
        static_cast<long long>(actual.deadline_exit),int(actual.returned),int(state->install_commit.load()),int(state->install_returned.load()),int(state->read_entered.load()),
        int(state->read_before_install_commit.load()),int(state->read_before_install_returned.load()),int(bool(state->enqueue_error)),int(bool(state->query_error)),int(bool(ending_error)),trace_removal,
        controller_error_count,controller_first_bytes,int(controller_first_truncated),controller_first_hex.data());
    EXPECT_EQ(trace_removal,SQLITE_OK);EXPECT_FALSE(ending_error);
    if(state->enqueue_error)std::rethrow_exception(state->enqueue_error);
    if(state->query_error)std::rethrow_exception(state->query_error);
    ASSERT_TRUE(state->holder);EXPECT_FALSE(state->holder->timed_out);
    ASSERT_GT(state->deferrals.load(),0u);ASSERT_TRUE(state->enqueued.load());
    EXPECT_FALSE(state->callback_on_worker.load());EXPECT_TRUE(state->callback_on_private.load());EXPECT_TRUE(state->query_on_worker.load());
    EXPECT_FALSE(state->lane_identity_changed.load());EXPECT_TRUE(state->query_selected.load());
    EXPECT_GE(state->deferrals.load(),required_deferrals);EXPECT_GT(state->turns_after,state->turns_before);
    EXPECT_TRUE(actual.selected);EXPECT_FALSE(actual.excluded);EXPECT_FALSE(actual.deadline_observed);
    ASSERT_TRUE(state->query_started.load());ASSERT_TRUE(state->query_finished.load());EXPECT_EQ(state->query_value,731);
    // This is a correctness oracle, not permission to skip the real intent.
    // On a self-blocking baseline the selected query waits until that episode
    // expires with no intervening controller turn, then enters before install.
    EXPECT_TRUE(finished_in_original_bound);
    EXPECT_TRUE(state->install_commit.load());EXPECT_TRUE(state->install_returned.load());EXPECT_TRUE(state->read_entered.load());
    EXPECT_FALSE(state->read_before_install_commit.load());
    // Post-return observer timing is printed independently. It is not an
    // admission fence after the real Native COMMIT/mutex have been released.
    detail::recovery_controller_executor_test_access::ticket lane;
    {std::lock_guard lock(state->lane_mutex);lane=state->lane;}
    EXPECT_GE(detail::recovery_controller_executor_test_access::snapshot(lane).timed_wakes,2u);
    EXPECT_TRUE(pacer_pause->ready());EXPECT_FALSE(pacer_pause->timedOut());
    EXPECT_EQ(controller_error_count,0u);
}

TEST_F(RecoveryReceiverController, SecondActualRouteReusesControllerWithoutAnotherEndingReservation) {
    configure(2);using Access=detail::recovery_controller_executor_test_access;
    const auto reaper=detail::sync_retirement_lane::instance();const auto before=Access::order(reaper);
    Access::ticket controller;bool ending_done=false;const std::string retained_store=receiver->config().path;
    const auto ending=[&]()noexcept{
        if(ending_done)return;synchronizers.clear();
        if(controller&&!Access::joined(controller,std::chrono::seconds(3))){
            std::fprintf(stderr,"second-route ending-debt process=%ld group=%ld retained_store=%s\n",long(::getpid()),long(::getpgrp()),retained_store.c_str());std::fflush(stderr);::_exit(93);}
        ending_done=true;
    };
    struct AlwaysEnd{const decltype(ending)& work;~AlwaysEnd()noexcept{work();}} cleanup{ending};
    for(size_t i=0;i<peers.size();++i){auto& peer=peers[i];sync_config config;config.websocket_url=peer.endpoint;config.authorization_token="registered-token";config.sync_id=peer.channel;
        for(const auto& active:peers)config.all_active_sync_ids.push_back(active.channel);
        config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;config.chunk_size=upload_chunk;
        auto route=std::make_unique<synchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
        route->set_on_error([this](const std::string& e){std::lock_guard lock(errors_mutex);errors.push_back(e);});synchronizers.push_back(std::move(route));
        if(i==0)controller=Access::controller_at(reaper,before+1);
        ASSERT_TRUE(controller);
        // Neither route has called public connect yet: the only reservation
        // in this interval is the genuine controller's prelaunch ending slot.
        EXPECT_EQ(Access::order(reaper),before+1);
        EXPECT_EQ(Access::controller_at(reaper,before+1),controller);
    }
    for(const auto& route:synchronizers)route->connect();
    ASSERT_TRUE(until([&]{return phase()==0;}));
    const auto actual=Access::snapshot(controller);EXPECT_TRUE(actual.available);EXPECT_NE(actual.worker,std::thread::id{});
    size_t errors_before_ending;{std::lock_guard lock(errors_mutex);errors_before_ending=errors.size();}
    ending();EXPECT_EQ(errors_before_ending,0u);EXPECT_TRUE(Access::snapshot(controller).joined);
}


TEST_F(RecoveryReceiverController, PrivateControllerFailureNotifiesOnOriginalWorkerAndRetiresItsActualLane) {
    configure();using Access=detail::recovery_controller_executor_test_access;
    struct State {
        std::mutex mutex;Access::ticket lane;std::array<unsigned char,512> first{};size_t first_bytes=0;
        std::atomic<unsigned> faults{0},callbacks{0};std::atomic<bool> done{false},on_private{false},on_original{false};
        std::exception_ptr callback_failure;int64_t value=INT64_MIN;
    };const auto state=std::make_shared<State>();const auto queue=receiver->get_scheduler();
    const std::string retained_store=receiver->config().path;bool ended=false;
    const auto ending=[&]()noexcept{
        if(ended)return;synchronizers.clear();
        try{queue->shutdown();}catch(...){std::fprintf(stderr,"private-error caller-ending-debt group=%ld store=%s\n",long(::getpgrp()),retained_store.c_str());std::fflush(stderr);::_exit(93);}
        Access::ticket actual;{std::lock_guard lock(state->mutex);actual=state->lane;}
        if(!Access::joined(actual,std::chrono::seconds(3))){std::fprintf(stderr,"private-error controller-ending-debt group=%ld store=%s\n",long(::getpgrp()),retained_store.c_str());std::fflush(stderr);::_exit(93);}ended=true;
    };
    struct AlwaysEnd{const decltype(ending)& work;~AlwaysEnd()noexcept{work();}} cleanup{ending};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,
        [state](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){auto actual=Access::current();std::lock_guard lock(state->mutex);if(!state->lane)state->lane=std::move(actual);}
            if(std::strcmp(stage,"install")==0){++state->faults;throw db_error("private-controller-real-install-scope-refusal");}return {};
        });
    // The actual public error callback is registered before connect, so the
    // first copied error cannot race a later replacement handler installation.
    auto& peer=peers.at(0);sync_config config;config.websocket_url=peer.endpoint;config.authorization_token="registered-token";config.sync_id=peer.channel;
    config.all_active_sync_ids.push_back(peer.channel);config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;config.chunk_size=upload_chunk;
    auto route=std::make_unique<synchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
    route->set_on_error([this,state,queue](const std::string& actual){
        ++state->callbacks;state->on_private.store(bool(Access::current()));state->on_original.store(queue->is_on_thread());
        try{
            // Real reentrant public Native read. It is never executed by the
            // private lane and receives no controller exact-cell bypass.
            const auto rows=receiver->db().query("SELECT 733 AS n");state->value=std::get<int64_t>(rows.at(0).at("n"));
            const auto count=std::min(state->first.size(),actual.size());
            {std::lock_guard lock(state->mutex);if(!state->first_bytes){state->first_bytes=count;if(count)std::memcpy(state->first.data(),actual.data(),count);}}
        }catch(...){state->callback_failure=std::current_exception();}
        state->done.store(true);
    });synchronizers.push_back(std::move(route));synchronizers.back()->connect();
    const bool completed=until([&]{return state->done.load();});ending();
    EXPECT_TRUE(completed);EXPECT_EQ(state->faults.load(),1u);EXPECT_EQ(state->callbacks.load(),1u);
    EXPECT_FALSE(state->on_private.load());EXPECT_TRUE(state->on_original.load());EXPECT_EQ(state->value,733);EXPECT_FALSE(state->callback_failure);
    std::array<char,513> copied{};size_t bytes;{std::lock_guard lock(state->mutex);bytes=state->first_bytes;if(bytes)std::memcpy(copied.data(),state->first.data(),bytes);}
    EXPECT_NE(std::string(copied.data(),bytes).find("private-controller-real-install-scope-refusal"),std::string::npos);
}

}
#endif


// TEST-only supported concurrent scheduler. Exactly two real workers and32
// queue cells make the independent worker capacity observable while one real
// public handler is held. This does not serialize any production scheduler.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class ControllerNotificationConcurrentScheduler final : public scheduler {
    struct State {
        std::mutex mutex;std::condition_variable ready,ended;
        std::array<std::function<void()>,32> jobs{};size_t head=0,count=0,active=0,workers_ended=0;
        bool stopping=false;uint64_t completed=0;std::exception_ptr first_error;
    };
    std::shared_ptr<State> state_=std::make_shared<State>();
    std::array<std::thread,2> workers_;size_t started_=0;bool joined_=false;
    inline static thread_local const State* current_=nullptr;
    [[noreturn]] static void ending_debt(const char* stage)noexcept{
        std::fprintf(stderr,"controller-notification scheduler-ending-debt stage=%s process=%ld group=%ld retained=1\n",stage,long(::getpid()),long(::getpgrp()));
        std::fflush(stderr);::_exit(95);
    }
    static void loop(State* state)noexcept {
        current_=state;
        for(;;){std::function<void()> job;
            {std::unique_lock lock(state->mutex);state->ready.wait(lock,[&]{return state->stopping||state->count;});
                if(!state->count){current_=nullptr;++state->workers_ended;lock.unlock();state->ended.notify_all();return;}
                job.swap(state->jobs[state->head]);state->head=(state->head+1)%state->jobs.size();--state->count;++state->active;}
            try{job();}catch(...){std::lock_guard lock(state->mutex);if(!state->first_error)state->first_error=std::current_exception();}
            job={}; // all actual work/captures end before completion DATA
            {std::lock_guard lock(state->mutex);--state->active;++state->completed;}
            state->ended.notify_all();
        }
    }
public:
    ControllerNotificationConcurrentScheduler(){
        try{for(auto& worker:workers_){worker=std::thread([raw=state_.get()]{loop(raw);});++started_;}}
        catch(...){const auto first=std::current_exception();shutdown();std::rethrow_exception(first);}
    }
    ~ControllerNotificationConcurrentScheduler()override{shutdown();}
    void invoke(std::function<void()>&& job)override{
        {std::lock_guard lock(state_->mutex);if(state_->stopping)return;
            if(state_->count==state_->jobs.size())throw db_error("controller notification fixture queue capacity");
            const auto tail=(state_->head+state_->count)%state_->jobs.size();state_->jobs[tail].swap(job);++state_->count;}
        state_->ready.notify_one();
    }
    bool is_on_thread()const noexcept override{return current_==state_.get();}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{std::lock_guard lock(state_->mutex);return !state_->stopping;}
    std::future<void> checkpoint(std::function<void()> observe={}){
        auto done=std::make_shared<std::promise<void>>();auto future=done->get_future();
        invoke([done,observe=std::move(observe)]{if(observe)observe();done->set_value();});return future;
    }
    std::exception_ptr first_error()const{std::lock_guard lock(state_->mutex);return state_->first_error;}
    void shutdown()override{
        if(joined_)return;if(is_on_thread())ending_debt("self-shutdown");
        {std::lock_guard lock(state_->mutex);state_->stopping=true;}state_->ready.notify_all();
        {std::unique_lock lock(state_->mutex);if(!state_->ended.wait_for(lock,std::chrono::seconds(3),[&]{return state_->workers_ended==started_;}))ending_debt("worker-ending-unobserved");}
        for(auto& worker:workers_)if(worker.joinable())try{worker.join();}catch(...){ending_debt("actual-worker-join");}
        joined_=true;
    }
};
struct ControllerHeldNotificationHandler {
    std::mutex mutex;std::condition_variable changed;
    unsigned callbacks=0;bool first_arrived=false,released=false,timed_out=false,free_worker_while_held=false;
    bool wrong_execution_context=false,message_overflow=false;
    std::array<std::array<char,256>,2> messages{};std::array<size_t,2> message_bytes{};
    void release()noexcept{std::lock_guard lock(mutex);released=true;changed.notify_all();}
};
TEST_F(RecoveryReceiverController, ConcurrentRouteHandoffRemainsOutstandingThroughActualPublicErrorHandler) {
    configure(1,false);using Access=detail::recovery_controller_executor_test_access;
    const auto queue=std::make_shared<ControllerNotificationConcurrentScheduler>();
    const auto held=std::make_shared<ControllerHeldNotificationHandler>();
    const auto reaper=detail::sync_retirement_lane::instance();Access::ticket actual_lane;
    const std::string retained_store=(container/"store.sqlite").string();bool route_born=false,ended=false;
    const auto ending_debt=[&](const char* stage)noexcept{
        std::fprintf(stderr,"controller-notification ending-debt stage=%s process=%ld group=%ld retained_store=%s\n",stage,long(::getpid()),long(::getpgrp()),retained_store.c_str());
        std::fflush(stderr);::_exit(95);
    };
    const auto ending=[&]()noexcept{
        if(ended)return;held->release(); // release actual handler before owner retirement
        synchronizers.clear(); // original synchronizer/lifetime/pacer ending
        try{queue->shutdown();}catch(...){ending_debt("original-concurrent-scheduler");}
        if(route_born&&!Access::joined(actual_lane,std::chrono::seconds(3)))ending_debt("private-controller-actual-join");
        ended=true;
    };
    struct AlwaysEnd{const decltype(ending)& action;~AlwaysEnd()noexcept{action();}} cleanup{ending};
    swift_configuration owner_config(retained_store,queue);owner_config.audit_retention_seconds=0;owner_config.busy_timeout_ms=100;
    continuous_result owner_result;
#if LATTICE_HAS_FRT
    receiver_ref.reset(swift_lattice_ref::create_continuous(owner_config,{controller_schema()},policy,owner_result));
#else
    receiver_ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create_continuous(owner_config,{controller_schema()},policy,owner_result));
#endif
    ASSERT_EQ(owner_result.phase(),2);ASSERT_FALSE(owner_result.has_error());
    receiver=swift_lattice_ref::shared_for_lattice(receiver_ref->get());ASSERT_TRUE(receiver);
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(receiver->config().path))notifier->stop_listening();
    const auto& peer=peers.at(0);sync_config config;config.websocket_url=peer.endpoint;config.authorization_token="registered-token";config.sync_id=peer.channel;
    config.all_active_sync_ids.push_back(peer.channel);config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;
    const auto reservation_before=Access::order(reaper);
    auto route=std::make_unique<synchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
    route_born=true;actual_lane=Access::controller_at(reaper,reservation_before+1);ASSERT_TRUE(actual_lane);
    route->set_on_error([held,queue](const std::string& message){
        const bool on_private=bool(Access::current());const bool on_original=queue->is_on_thread();
        std::unique_lock lock(held->mutex);const auto ordinal=++held->callbacks;
        if(on_private||!on_original)held->wrong_execution_context=true;
        if(ordinal<=held->messages.size()){
            const auto count=std::min(held->messages[ordinal-1].size(),message.size());held->message_bytes[ordinal-1]=count;
            if(count)std::memcpy(held->messages[ordinal-1].data(),message.data(),count);
            if(count!=message.size())held->message_overflow=true;
        }else held->message_overflow=true;
        if(ordinal==1){held->first_arrived=true;held->changed.notify_all();
            if(!held->changed.wait_for(lock,std::chrono::seconds(5),[&]{return held->released;}))held->timed_out=true;}
        held->changed.notify_all();
    });synchronizers.push_back(std::move(route));
    // The public constructor enrolled the real route/controller/owner. No
    // connect or fabricated authenticated source/outcome is needed: the two
    // existing-friend forwards inject only fixed test notification DATA.
    auto& actual=*synchronizers.front();
    detail::recovery_receiver_cohort_test_access::post_notification_error(actual,std::make_exception_ptr(db_error("first-injected-route-mailbox-error")));
    detail::recovery_receiver_cohort_test_access::drain_notification_data(actual);
    {std::unique_lock lock(held->mutex);ASSERT_TRUE(held->changed.wait_for(lock,std::chrono::seconds(2),[&]{return held->first_arrived;}));}
    for(unsigned n=0;n<8;++n){
        detail::recovery_receiver_cohort_test_access::post_notification_error(actual,std::make_exception_ptr(db_error(n?"later-coalesced-route-mailbox-error":"first-pending-route-mailbox-error")));
        detail::recovery_receiver_cohort_test_access::drain_notification_data(actual);
    }
    // One worker is actually held in the public handler. The other worker
    // must execute both fixed FIFO markers. Two markers also drain the old
    // helper's one extra scheduled-error hop; neither is a sleep/yield guess.
    auto first_checkpoint=queue->checkpoint([held]{std::lock_guard lock(held->mutex);held->free_worker_while_held=held->first_arrived&&!held->released&&!held->timed_out;});
    ASSERT_EQ(first_checkpoint.wait_for(std::chrono::seconds(1)),std::future_status::ready);first_checkpoint.get();
    auto second_checkpoint=queue->checkpoint();ASSERT_EQ(second_checkpoint.wait_for(std::chrono::seconds(1)),std::future_status::ready);second_checkpoint.get();
    {std::lock_guard lock(held->mutex);EXPECT_TRUE(held->free_worker_while_held);EXPECT_FALSE(held->released);EXPECT_FALSE(held->timed_out);EXPECT_EQ(held->callbacks,1u);}
    held->release();
    // Demand retained during the held callback remains actual pending DATA.
    // A later real pacer tick or this explicit ordinary drain can dispatch it.
    detail::recovery_receiver_cohort_test_access::drain_notification_data(actual);
    {std::unique_lock lock(held->mutex);EXPECT_TRUE(held->changed.wait_for(lock,std::chrono::seconds(2),[&]{return held->callbacks>=2;}));}
    ending(); // all actual original/private jobs and retained captures ended
    EXPECT_FALSE(queue->first_error());
    {std::lock_guard lock(held->mutex);
        EXPECT_EQ(held->callbacks,2u);EXPECT_FALSE(held->timed_out);EXPECT_FALSE(held->wrong_execution_context);EXPECT_FALSE(held->message_overflow);
        const std::string first(held->messages[0].data(),held->message_bytes[0]);const std::string pending(held->messages[1].data(),held->message_bytes[1]);
        EXPECT_NE(first.find("first-injected-route-mailbox-error"),std::string::npos);
        EXPECT_NE(pending.find("first-pending-route-mailbox-error"),std::string::npos);
    }
    EXPECT_TRUE(Access::snapshot(actual_lane).joined);
}
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
TEST_F(RecoveryRestrictedFirstClaim, DeferredInstallTargetSurvivesIntermediateOwnedCommitAndAnotherActualTurn) {
    configure();
    struct State : SoleFifoAdmissionState {
        std::mutex holds_mutex;std::condition_variable holds_changed;
        std::array<std::shared_ptr<FirstClaimWriterHold>,2> holds;
        unsigned creating=0;bool ending=false;
        std::atomic<bool> first_release_started{false},second_published{false},second_release_started{false};
        std::atomic<bool> still_denied_after_intermediate_turn{false};
        std::atomic<unsigned> intermediate_commits{0},intermediate_turn{0},second_deferrals{0},second_first_turn{0};
    };
    const auto state=std::make_shared<State>();state->owner=receiver;state->queue=receiver->get_scheduler();
    ASSERT_NE(std::dynamic_pointer_cast<std_thread_scheduler>(state->queue),nullptr);
    state->native=detail::canonical_writer_custody_test_access::fault_handle(receiver->db());
    const std::string retained_store=receiver->config().path;
    const auto debt=[&](const char* stage)noexcept{
        std::fprintf(stderr,"install-target ending-debt stage=%s process=%ld group=%ld retained_store=%s\n",
            stage,static_cast<long>(::getpid()),static_cast<long>(::getpgrp()),retained_store.c_str());
        std::fflush(stderr);::_exit(93);
    };
    const auto release_hold=[state](size_t index){
        std::shared_ptr<FirstClaimWriterHold> held;
        {std::lock_guard lock(state->holds_mutex);held=state->holds[index];}
        if(held)held->release(); // main alone joins, never under the fixture leaf
    };
    std::exception_ptr ending_error;bool ended=false;int trace_removal=SQLITE_ERROR;
    const auto ending=[&]()noexcept{
        if(ended)return;
        {std::lock_guard lock(state->holds_mutex);state->ending=true;}
        try{release_hold(0);release_hold(1);}catch(...){ending_error=std::current_exception();debt("holder-release");}
        // A constructor already admitted before ending may still publish.
        // Release known holders first so that constructor cannot wait on one.
        {std::unique_lock lock(state->holds_mutex);
            if(!state->holds_changed.wait_for(lock,std::chrono::seconds(5),[&]{return state->creating==0;}))debt("holder-publication");}
        try{release_hold(0);release_hold(1);}catch(...){ending_error=std::current_exception();debt("late-holder-release");}
        synchronizers.clear();
        try{state->queue->shutdown();}catch(...){ending_error=std::current_exception();debt("original-worker-join");}
        detail::recovery_controller_executor_test_access::ticket lane;
        {std::lock_guard lock(state->lane_mutex);lane=state->lane;}
        if(!detail::recovery_controller_executor_test_access::joined(lane,std::chrono::seconds(3)))debt("private-lane-join");
        trace_removal=sqlite3_trace_v2(state->native,0,nullptr,nullptr);
        if(trace_removal!=SQLITE_OK)debt("trace-removal");
        ended=true;
    };
    struct AlwaysEnd{const decltype(ending)& action;~AlwaysEnd()noexcept{action();}} always_end{ending};
    ASSERT_EQ(sqlite3_trace_v2(state->native,SQLITE_TRACE_STMT|SQLITE_TRACE_PROFILE,
        SoleFifoAdmissionState::trace,static_cast<SoleFifoAdmissionState*>(state.get())),SQLITE_OK);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [state](const char* stage){if(std::strcmp(stage,"install-committed")==0)state->install_returned.store(true);},
        [this,state](const char* stage)->std::shared_ptr<void>{
            if(std::strcmp(stage,"selected-controller-turn-through-settlement")==0){
                ++state->selected_turns;auto lane=detail::recovery_controller_executor_test_access::current();
                std::lock_guard lock(state->lane_mutex);
                if(!state->lane)state->lane=std::move(lane);else if(state->lane!=lane)state->lane_identity_changed.store(true);
            }
            if(std::strcmp(stage,"install")!=0)return {};
            const auto install=state->installs.fetch_add(1);
            if(install<2){
                {std::lock_guard lock(state->holds_mutex);if(state->ending)return {};++state->creating;}
                std::shared_ptr<FirstClaimWriterHold> actual;
                try{actual=std::make_shared<FirstClaimWriterHold>(*receiver);}
                catch(...){std::lock_guard lock(state->holds_mutex);--state->creating;state->holds_changed.notify_all();throw;}
                {std::lock_guard lock(state->holds_mutex);state->holds[install]=std::move(actual);--state->creating;
                    if(install==0)state->hold_published.store(true);else state->second_published.store(true);}
                state->holds_changed.notify_all();
            }
            return std::make_shared<SoleFifoInstallScope>(state);
        },
        [state](const char* reason){
            if(!state->hold_published.load()||std::strcmp(reason,"admission-deferred-connection-mutex")!=0)return;
            ++state->deferrals;
            if(state->second_published.load()){
                if(state->second_deferrals.fetch_add(1)==0)state->second_first_turn.store(state->selected_turns.load());
            }
            if(state->enqueue_once.exchange(true))return;
            state->callback_on_private.store(bool(detail::recovery_controller_executor_test_access::current()));
            state->callback_on_worker.store(state->queue->is_on_thread());
            try{state->queue->invoke([state]{
                state->query_on_worker.store(state->queue->is_on_thread());state->query_started.store(true);
                state->turns_before=state->selected_turns.load();
                try{detail::recovery_topology_wait_observation::scope selected(state->query_wait,&state->query_selected);
                    const auto owner=state->owner.lock();if(!owner)throw db_error("install target control owner retired");
                    const auto rows=owner->db().query("SELECT 731 AS n");state->query_value=std::get<int64_t>(rows.at(0).at("n"));
                }catch(...){state->query_error=std::current_exception();}
                state->turns_after=state->selected_turns.load();state->query_finished.store(true);
            });state->enqueued.store(true);}catch(...){state->enqueue_error=std::current_exception();}
        },
        [state]{
            if(state->first_release_started.load()&&!state->install_returned.load()){
                ++state->intermediate_commits;state->intermediate_turn.store(state->selected_turns.load());
            }
        });
    connect();
    const bool finished=until([&]{
        if(state->enqueued.load()&&state->query_selected.load()&&!state->first_release_started.exchange(true))release_hold(0);
        if(state->second_published.load()&&state->second_deferrals.load()>=2&&state->intermediate_commits.load()!=0&&
           state->selected_turns.load()>state->second_first_turn.load()&&!state->second_release_started.exchange(true)){
            state->still_denied_after_intermediate_turn.store(!state->query_finished.load());release_hold(1);
        }
        return state->query_finished.load();
    }); // unchanged five-second predicate, no SQL or alternate executor
    ending();
    std::fprintf(stderr,"install-target finished=%d installs=%u deferrals=%u intermediateCommits=%u intermediateTurn=%u secondDeferrals=%u secondFirstTurn=%u turnsBefore=%u turnsAfter=%u deniedAcrossTurn=%d selectedWaits=%llu token=%llu deadlineObserved=%d actualCommit=%d installReturned=%d readBeforeCommit=%d\n",
        int(finished),state->installs.load(),state->deferrals.load(),state->intermediate_commits.load(),state->intermediate_turn.load(),
        state->second_deferrals.load(),state->second_first_turn.load(),state->turns_before,state->turns_after,
        int(state->still_denied_after_intermediate_turn.load()),static_cast<unsigned long long>(state->query_wait.selected_waits),
        static_cast<unsigned long long>(state->query_wait.token),int(state->query_wait.deadline_observed),
        int(state->install_commit.load()),int(state->install_returned.load()),int(state->read_before_install_commit.load()));
    EXPECT_EQ(trace_removal,SQLITE_OK);EXPECT_FALSE(ending_error);
    if(state->enqueue_error)std::rethrow_exception(state->enqueue_error);
    if(state->query_error)std::rethrow_exception(state->query_error);
    for(const auto& held:state->holds){ASSERT_TRUE(held);EXPECT_FALSE(held->timed_out);}
    EXPECT_TRUE(finished);EXPECT_TRUE(state->second_release_started.load());
    EXPECT_GT(state->intermediate_commits.load(),0u);EXPECT_GE(state->second_deferrals.load(),2u);
    EXPECT_TRUE(state->still_denied_after_intermediate_turn.load());EXPECT_GT(state->turns_after,state->second_first_turn.load());
    EXPECT_TRUE(state->callback_on_private.load());EXPECT_FALSE(state->callback_on_worker.load());EXPECT_TRUE(state->query_on_worker.load());
    EXPECT_FALSE(state->lane_identity_changed.load());EXPECT_TRUE(state->query_wait.selected);EXPECT_FALSE(state->query_wait.excluded);
    EXPECT_EQ(state->query_wait.selected_waits,1u);EXPECT_FALSE(state->query_wait.deadline_observed);
    EXPECT_NE(state->query_wait.token,0u);EXPECT_TRUE(state->query_wait.returned);EXPECT_EQ(state->query_value,731);
    EXPECT_TRUE(state->install_commit.load());EXPECT_TRUE(state->install_returned.load());EXPECT_TRUE(state->read_entered.load());
    EXPECT_FALSE(state->read_before_install_commit.load());EXPECT_FALSE(has_error());
}
}
#endif


// Real charge-ending continuation. The supported original manual route queue
// runs on ONE owned worker only until the held callback returns. After its join,
// no original route job executes. The actual pacer is independently held at
// its existing starting observation until after the next-request witness.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class RecoveryResponseCapacity : public RecoveryRestrictedFirstClaim {
protected:
    size_t actual_reads=0,ready_offered=0;
    std::string first_read_index;
    struct DeliveryOutcome {std::atomic<unsigned> delivered{0},refused{0};};
    const std::shared_ptr<DeliveryOutcome> delivery_outcome=std::make_shared<DeliveryOutcome>();
    struct QueuedReady {
        const uint64_t creator;
        platform_transport_callbacks endpoint;
        transport_message message;
        std::shared_ptr<DeliveryOutcome> outcome;
        explicit QueuedReady(uint64_t issuer):creator(issuer){}
        static void destroy(QueuedReady* value)noexcept {
            if(!value||value->creator!=detail::database_native_process())return;
            value->message={};if(value->creator!=detail::database_native_process())return;
            value->endpoint={};if(value->creator!=detail::database_native_process())return;
            value->outcome.reset();if(value->creator!=detail::database_native_process())return;
            delete value;
        }
    };
    void after_ready(size_t index,const json& control,const std::string& raw) override {
        // trigger_on_message is synchronous on its caller. Queue that SAME
        // actual endpoint/message onto the SAME supported manual scheduler;
        // no base trace/member access occurs from the worker callback.
        if(raw.size()>8388608||ready_offered==32)throw db_error("response-credit fixture READY envelope bound");
        boundary_trace.delivery_bytes(raw);++ready_offered; // Main-only source pump DATA.
        const auto issuer=detail::database_native_process(); // Before either allocation.
        auto queued=std::shared_ptr<QueuedReady>(new QueuedReady(issuer),QueuedReady::destroy);
        if(queued->creator!=detail::database_native_process())throw db_error("response-credit fixture changed process");
        queued->endpoint=peers.at(index).physical;queued->outcome=delivery_outcome;
        queued->message=transport_message::from_string(raw);
        if(queued->creator!=detail::database_native_process())throw db_error("response-credit fixture changed process");
        manual->invoke([queued]{
            if(queued->creator!=detail::database_native_process())return;
            const bool delivered=queued->endpoint.trigger_on_message(queued->message);
            if(queued->creator!=detail::database_native_process())return;
            if(delivered)++queued->outcome->delivered;
            else {++queued->outcome->refused;throw db_error("response-credit actual READY endpoint refused");}
        });
        if(control.at("operation")=="read"){
            if(!actual_reads)first_read_index=control.at("index").get<std::string>();
            ++actual_reads;
        }
    }
};
TEST_F(RecoveryResponseCapacity, ActualCallbackCaptureEndingResumesPrivateControlWithoutOriginalSchedulerTick) {
    configure(1,false);insert(*source,controller_uuid(966),"canonical");open_manual_receiver();
    struct State {
        std::atomic<bool> callback_selected{false},stop{false},worker_ended{false};
        std::atomic<unsigned> capacity_waits{0},jobs{0};
        std::exception_ptr worker_error;
        std::mutex lane_mutex;detail::recovery_controller_executor_test_access::ticket lane;
        std::shared_ptr<ControllerPause> callback=std::make_shared<ControllerPause>();
        std::shared_ptr<ControllerPause> pacer=std::make_shared<ControllerPause>();
    };
    const auto state=std::make_shared<State>();pauses.push_back(state->callback);pauses.push_back(state->pacer);
    std::thread route_worker;bool cleaned=false;
    const auto prior_pacer=detail::sync_background_test_hooks::pacer_wait;
    struct RestorePacer {std::shared_ptr<const detail::sync_background_test_hooks::pacer_wait_schedule> prior;
        ~RestorePacer(){detail::sync_background_test_hooks::pacer_wait=std::move(prior);}} restore_pacer{prior_pacer};
    auto pacer_hook=std::make_shared<detail::sync_background_test_hooks::pacer_wait_schedule>();
    pacer_hook->starting=[state]{state->pacer->wait();};
    detail::sync_background_test_hooks::pacer_wait=std::move(pacer_hook);
    const auto cleanup=[&]()noexcept{
        if(cleaned)return;
        state->stop.store(true);state->callback->release();state->pacer->release();
        if(route_worker.joinable()){
            const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
            while(!state->worker_ended.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if(!state->worker_ended.load(std::memory_order_acquire))
                detail::controller_fixture_worker_endings::fail_ending("response-credit-route-callback-not-ended");
            route_worker.join();
        }
        close_receiver();cleaned=true; // Actual private worker joins before callback/local destruction.
    };
    struct Ending {const decltype(cleanup)& run;~Ending()noexcept{run();}} ending{cleanup};
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),
        [state](const char* stage){
            if(std::strcmp(stage,"physical-reply-wake-returned")==0&&!state->callback_selected.exchange(true))
                state->callback->wait(); // Actual receive locals STILL retain pending/reply originals.
        },nullptr,nullptr,nullptr,nullptr,[state]{
            auto actual=detail::recovery_controller_executor_test_access::current();
            {std::lock_guard lock(state->lane_mutex);if(!state->lane)state->lane=std::move(actual);}
            ++state->capacity_waits;
        });
    connect();
    route_worker=std::thread([this,state]{
        try{while(!state->stop.load()){
            if(manual->run_one())++state->jobs;
            else std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }}catch(...){state->worker_error=std::current_exception();}
        state->worker_ended.store(true,std::memory_order_release);
    });
    // Original fixed5s helper pumps only real peer traffic, never manual jobs.
    ASSERT_TRUE(until([&]{
        if(!state->pacer->ready()||!state->callback->ready()||!state->capacity_waits.load())return false;
        detail::recovery_controller_executor_test_access::ticket actual;
        {std::lock_guard lock(state->lane_mutex);actual=state->lane;}
        const auto copied=detail::recovery_controller_executor_test_access::snapshot(actual);
        return copied.available&&!copied.active&&!copied.pending&&!copied.timed&&!copied.worker_error;
    }));
    // Genuine empty private pending/timer slots exclude a previously queued
    // private turn providing an incidental retry after callback release.
    EXPECT_EQ(actual_reads,0u);EXPECT_FALSE(state->callback->timedOut());EXPECT_FALSE(state->pacer->timedOut());
    // Stop is published while that actual callback is held. The sole worker
    // returns from it, destroys its real callback tail/charge, then exits; it
    // cannot execute the already queued periodic/discovery jobs after release.
    state->stop.store(true);state->callback->release();
    ASSERT_TRUE(observe([&]{return state->worker_ended.load(std::memory_order_acquire);}));
    route_worker.join();ASSERT_FALSE(state->worker_error);const auto jobs=state->jobs.load();
    ASSERT_TRUE(until([&]{return actual_reads!=0;}));
    ASSERT_GT(manual->queued_snapshot(),0u); // Real next read response remains queued, unexecuted.
    EXPECT_EQ(state->jobs.load(),jobs);EXPECT_EQ(actual_reads,1u);EXPECT_EQ(first_read_index,"0");
    EXPECT_FALSE(state->callback->timedOut());EXPECT_FALSE(state->pacer->timedOut());EXPECT_FALSE(has_error());
    // The actual captured pacer starting hook still holds that independent
    // fallback thread; stopping manual jobs alone would NOT exclude it.
    state->pacer->release();
    // Resume the SAME actual original queue only after the independent private
    // next-request witness. All canonical rows/install/COMMIT semantics remain.
    ASSERT_TRUE(drive([&]{return phase()==0&&scalar(*receiver,
        "SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    EXPECT_EQ(delivery_outcome->refused.load(),0u);EXPECT_EQ(delivery_outcome->delivered.load(),ready_offered);
    EXPECT_FALSE(has_error());cleanup();
}
} // namespace
#endif
