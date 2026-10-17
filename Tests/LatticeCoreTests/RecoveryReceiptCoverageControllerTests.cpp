// Real v3 source/controller path. The mechanical platform verifier is not hosted TLS qualification.
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_receiver_controller.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_request_store.hpp"
#include "../../Sources/LatticeCore/src/recovery_authenticated_session.hpp"
#include <array>
#include <nlohmann/json.hpp>
#include <deque>
#include <chrono>
#include <cstdio>
#include <condition_variable>
#include <cstring>
#include <sqlite3.h>

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <csignal>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
extern char** environ;
namespace lattice::detail {
struct recovery_receiver_controller_test_access {
    using probe=recovery_receiver_controller::test_probe;
    std::shared_ptr<const probe> prior;
    recovery_receiver_controller_test_access(const lattice_db* owner,std::function<void(const char*)> observed,
        std::function<std::shared_ptr<void>(const char*)> scope={}) {
        auto value=std::make_shared<probe>();value->owner=owner;value->observed=std::move(observed);value->scope=std::move(scope);
        std::lock_guard lock(recovery_receiver_controller::test_mutex_);prior=std::move(recovery_receiver_controller::test_probe_);recovery_receiver_controller::test_probe_=std::move(value);
    }
    ~recovery_receiver_controller_test_access(){std::shared_ptr<const probe> old;{std::lock_guard lock(recovery_receiver_controller::test_mutex_);old=std::move(recovery_receiver_controller::test_probe_);recovery_receiver_controller::test_probe_=std::move(prior);}}
};
#if !defined(LATTICE_RECEIPT_RETENTION_LEGACY_WRITER)
// Storage-only test adapter. Every operation still requires the actual live
// Core WRITE; this adapter issues no receipt proof, route or recovery permit.
struct recovery_request_store_test_access {
    static void initialize(const std::shared_ptr<lattice_db>& o){recovery_request_store(o).initialize();}
    static void insert(const std::shared_ptr<lattice_db>& o,const recovery_request_row& r){recovery_request_store(o).insert(r);}
    static void add_manifest(const std::shared_ptr<lattice_db>& o,const recovery_request_row& r,const std::string& m){recovery_request_store(o).add_manifest(r,m);}
    static std::optional<recovery_request_row> read(const std::shared_ptr<lattice_db>& o,const std::string& c){return recovery_request_store(o).read(c);}
    static std::string fingerprint(const recovery_request_row& r){return recovery_request_store::fingerprint(r);}
    static std::map<std::string,std::string> fingerprints(const std::shared_ptr<lattice_db>& o){return recovery_request_store(o).fingerprints();}
    static size_t charged_bytes(const std::shared_ptr<lattice_db>& o){return recovery_request_store(o).charged_bytes();}
    static void retain(const std::shared_ptr<lattice_db>& o,const recovery_request_row& r,const std::string& d){recovery_request_store(o).retain_receipt_decisions(r,d.empty()?d:recovery_request_store::seal_receipt_decisions(r,d.substr(1)));}
};
#endif
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
std::string controller_uuid(unsigned n){char out[37];std::snprintf(out,sizeof(out),"80000000-0000-4000-8000-%012u",n);return out;}
swift_schema_entry controller_schema(){swift_schema_entry out;out.table_name="ControllerRow";property_descriptor p{};
    p.name="value";p.type=column_type::text;p.kind=property_kind::primitive;out.properties[p.name]=p;
    p.name="note";p.no_history=true;out.properties[p.name]=p;return out;}
struct ControllerServerRoute {std::shared_ptr<std::atomic<bool>> live;static int32_t current(void* p){return static_cast<ControllerServerRoute*>(p)->live->load()?1:0;}static void destroy(void* p){delete static_cast<ControllerServerRoute*>(p);}};
struct ControllerWire {
    struct Dial {std::string url;platform_transport_callbacks endpoint;};
    struct Frame {platform_transport_callbacks endpoint;std::string raw;};
    std::mutex mutex;std::deque<Dial> dials;std::deque<Frame> frames;std::vector<platform_transport_callbacks> endpoints;
    struct Pipe {std::weak_ptr<ControllerWire> wire;};
    static std::unique_ptr<sync_transport> transport(const std::shared_ptr<ControllerWire>& wire) {
        // Actual owned SDK platform boundary with a mechanical verifier. This
        // tests native controller wiring; hosted stock-adapter TLS is separate.
        return std::unique_ptr<sync_transport>(make_system_tls_platform_sync_transport(new Pipe{wire},
            [](void* p,const void* url,const void*,const void* endpoint){if(auto wire=static_cast<Pipe*>(p)->wire.lock()){std::lock_guard lock(wire->mutex);wire->dials.push_back({*static_cast<const std::string*>(url),*static_cast<const platform_transport_callbacks*>(endpoint)});}},
            [](void*){},
            [](void* p,const void* message,const void* endpoint){if(auto wire=static_cast<Pipe*>(p)->wire.lock()){const auto* frame=static_cast<const transport_message*>(message);
                if(frame->data.size()>8388608)throw db_error("fixture wire bound");std::lock_guard lock(wire->mutex);if(wire->frames.size()>=32)throw db_error("fixture queue bound");wire->frames.push_back({*static_cast<const platform_transport_callbacks*>(endpoint),frame->as_string()});}},
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
class RecoveryReceiptCoverageController:public ::testing::Test {
protected:
    TempDB source_file{"controller_source"},receiver_file{"controller_receiver"};
    std::filesystem::path container=receiver_file.str()+".lattice-continuous";
    std::unique_ptr<swift_lattice_ref> source_ref,receiver_ref;
    std::shared_ptr<::lattice::swift_lattice> source,receiver;
    std::shared_ptr<ControllerWire> wire=std::make_shared<ControllerWire>();
    std::shared_ptr<network_factory> previous;
    std::vector<std::unique_ptr<synchronizer>> synchronizers;
    struct Peer {std::string channel,endpoint,ns;json expectation;relay_recovery_setup setup;std::shared_ptr<std::atomic<bool>> live=std::make_shared<std::atomic<bool>>(true);platform_transport_callbacks physical;};
    std::vector<Peer> peers;
    continuous_policy policy;
    std::vector<std::string> requests,errors;std::mutex errors_mutex;
    std::deque<ControllerWire::Frame> held_uploads;
    std::vector<std::pair<size_t,std::vector<std::string>>> observed_uploads;
    std::vector<std::shared_ptr<ControllerPause>> pauses;
    std::unique_ptr<detail::recovery_receiver_controller_test_access> probe;
    bool hold_second=false,drop_prepare=false,hold_uploads=true;size_t dropped=0,handled=0;
    size_t upload_chunk=1000;
    bool suppress_upload_ack=true;
    bool registered_source=true;
    std::map<size_t,std::pair<std::string,std::string>> registration_overrides;
    std::vector<std::pair<size_t,json>> range_responses;
    struct DiscardCopy {
        size_t peer=0;int64_t sequence=-1;
        detail::canonical_ready_control_observation::settlement settlement;
        detail::authenticated_ready_control_test_observation::capture_failure primary,cleanup;
    };
    std::array<DiscardCopy,16> discard_copies{};size_t discard_retained=0;uint64_t discard_total=0;
    int64_t last_phase=-1,last_revision1=-1,last_revision2=-1;
    bool phase_observed=false,revision1_observed=false,revision2_observed=false;
    void observe_discard(size_t peer,const detail::authenticated_ready_control_test_observation::observation& actual)noexcept {
        if(!actual.discard_observed)return;if(discard_total!=UINT64_MAX)++discard_total;
        if(discard_retained==discard_copies.size())return;
        auto& out=discard_copies[discard_retained++];out.peer=peer;out.sequence=actual.logical_sequence;
        out.settlement=actual.discard;out.primary=actual.discard_primary;out.cleanup=actual.discard_cleanup;
    }
    static void report_failure_copy(const char* stage,const detail::authenticated_ready_control_test_observation::capture_failure& f)noexcept {
        std::array<char,1023> hex{};constexpr char digits[]="0123456789abcdef";
        const size_t count=std::min<size_t>(f.bytes,f.message.size()-1);
        for(size_t n=0;n<count;++n){const auto b=static_cast<unsigned char>(f.message[n]);hex[2*n]=digits[b>>4];hex[2*n+1]=digits[b&15];}
        std::fprintf(stderr," %s kind=%u bytes=%u truncated=%d null=%d messageHex=%s",stage,unsigned(f.kind),unsigned(f.bytes),int(f.truncated),int(f.null_message),hex.data());
    }
    void report_copied_failure()noexcept {
        // Only previously returned values: no extra SQL, Native admission,
        // predicate invocation, live endpoint or exception traversal here.
        std::fprintf(stderr,"receipt-controller observed phase=%lld phaseObserved=%d revision1=%lld revision1Observed=%d revision2=%lld revision2Observed=%d requests=%zu handled=%zu discarded=%llu retained=%zu\n",
            static_cast<long long>(last_phase),int(phase_observed),static_cast<long long>(last_revision1),int(revision1_observed),static_cast<long long>(last_revision2),int(revision2_observed),requests.size(),handled,static_cast<unsigned long long>(discard_total),discard_retained);
        for(size_t n=0;n<discard_retained;++n){const auto& row=discard_copies[n];
            std::fprintf(stderr,"receipt-controller discard peer=%zu sequence=%lld state=%d errors=%u unexpectedCommit=%d",row.peer,static_cast<long long>(row.sequence),row.settlement.state,unsigned(row.settlement.errors),int(row.settlement.unexpected_commit));
            report_failure_copy("primary",row.primary);report_failure_copy("cleanup",row.cleanup);std::fputc('\n',stderr);}
    }

    virtual json source_policy(const std::string& ns) {
        auto value=json{{"version",2},{"receiptCoverage",{{"kind","registeredProducerV3"},{"cohortID",controller_uuid(90)},{"cohortRevision",1},{"operationCodec",1},{"namespaces",json::array({"a","b"})}}},{"authority","controller-service"},{"sourceID",controller_uuid(1)},{"epoch",controller_uuid(2)},
            {"localNamespace","local"},{"namespaces",json::array({{{"namespaceID","local"},{"coverageID","local-v1"},{"revision",1}},
                {{"namespaceID","a"},{"coverageID","a-v1"},{"revision",1}},{{"namespaceID","b"},{"coverageID","b-v1"},{"revision",1}}})},
            {"receiptNamespace",ns},{"models",json::array({"ControllerRow"})},{"walFull",true},{"maximumAuthorizationMilliseconds",600000},
            {"readyProfile","bounded48MiBV1"},{"upload",{{"tables",json::array()},{"unlisted","allow"},{"maximumDeletes",256}}}};
        if(!registered_source){value["version"]=1;value.erase("receiptCoverage");}return value;
    }
    relay_recovery_setup serve(size_t index) {
        auto& peer=peers.at(index);auto config=json{{"mount",controller_uuid(3)},{"connection",::lattice::uuid_t::generate().to_string()},
            {"channel",peer.channel},{"authenticatedUserID",controller_uuid(4)},
            {"peer",{{"replicaID","registered-controller-"+std::to_string(index)},{"receiverIncarnation",controller_uuid(20)},{"channelIncarnation",controller_uuid(30+index)}}}};
        auto setup=source_ref->open_relay_recovery_setup(source_policy(peer.ns).dump(),config.dump(),new ControllerServerRoute{peer.live},ControllerServerRoute::current,ControllerServerRoute::destroy);
        if(!setup.valid())throw db_error("actual source setup failed: "+std::string(last_bridge_error()));
        const auto context=json::parse(setup.descriptor());auto answer=json{{"context",context},{"authenticatedUserID",context["route"]["authenticatedUserID"]},
            {"peer",context["route"]["peer"]},{"source",context["source"]},{"incomingScope",context["incomingScope"]},
            {"authorizationRevision","registered-controller-v1"},{"validForMilliseconds",600000},
            {"receiptCoverage",{{"kind","registeredProducer"},{"registrationID","one-durable-controller"},{"incarnation",controller_uuid(91)},{"cohortID",controller_uuid(90)},{"cohortRevision",1}}}};
        if(!registered_source)answer.erase("receiptCoverage");
        else if(const auto override=registration_overrides.find(index);override!=registration_overrides.end()) {
            answer["receiptCoverage"]["registrationID"]=override->second.first;answer["receiptCoverage"]["incarnation"]=override->second.second;
        }
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
        swift_configuration config((container/"store.sqlite").string(),std::make_shared<std_thread_scheduler>());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
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
    void connect() {
        for(auto& peer:peers){sync_config config;config.websocket_url=peer.endpoint;config.authorization_token="registered-token";config.sync_id=peer.channel;for(const auto& active:peers)config.all_active_sync_ids.push_back(active.channel);
            config.recovery_source_expectation=peer.expectation.dump();config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;config.chunk_size=upload_chunk;
            auto sync=std::make_unique<synchronizer>(std::static_pointer_cast<lattice_db>(receiver),config);
            sync->set_on_error([this](const std::string& e){std::lock_guard lock(errors_mutex);errors.push_back(e);});synchronizers.push_back(std::move(sync));synchronizers.back()->connect();}
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
    void close_receiver(){{std::lock_guard lock(errors_mutex);errors.clear();}held_uploads.clear();synchronizers.clear();if(receiver)receiver->close();receiver.reset();receiver_ref.reset();}
    void TearDown()override {
        for(const auto& pause:pauses)pause->release();
        close_receiver();for(const auto& pause:pauses)EXPECT_FALSE(pause->timedOut());probe.reset();for(auto& peer:peers){peer.live->store(false);peer.setup.close_on_io();peer.setup={};}peers.clear();
        {std::lock_guard lock(wire->mutex);wire->frames.clear();wire->dials.clear();wire->endpoints.clear();}
        if(source)source->close();source.reset();source_ref.reset();set_network_factory(previous);std::error_code error;std::filesystem::remove_all(container,error);
    }
    bool pump() {
        std::optional<ControllerWire::Dial> dial;std::optional<ControllerWire::Frame> frame;
        {std::lock_guard lock(wire->mutex);if(!wire->dials.empty()){dial=std::move(wire->dials.front());wire->dials.pop_front();}else if(!wire->frames.empty()){frame=std::move(wire->frames.front());wire->frames.pop_front();}}
        if(dial){size_t index=0;while(index<peers.size()&&dial->url.rfind(peers[index].endpoint,0)!=0)++index;if(index==peers.size())throw db_error("fixture unexpected actual dial");
            auto& peer=peers[index];peer.setup.close_on_io();peer.setup=serve(index);peer.physical=dial->endpoint;
            {std::lock_guard lock(wire->mutex);wire->endpoints.push_back(dial->endpoint);}if(!(hold_second&&index==1))dial->endpoint.trigger_on_open();return true;}
        if(!frame)return false;
        size_t index=0;while(index<peers.size()&&!peers[index].physical.matches(frame->endpoint))++index;if(index==peers.size())return true;
        auto& peer=peers[index];const auto control=json::parse(frame->raw);
        if(control.contains("kind")&&control["kind"]=="recoveryReady") {
            if(control["operation"]=="prepare"||control["operation"]=="resume")requests.push_back(control.at("request"));
            auto charge=peer.setup.stop_token().reserve_ready(frame->raw.size());if(!charge.valid())throw db_error("actual source finite request reservation failed");
            detail::authenticated_ready_control_test_observation::observation actual_ready;
            auto result=[&]{namespace observed=detail::authenticated_ready_control_test_observation;
                struct reset{observed::observation* prior;~reset(){observed::current=prior;}} restore{observed::current};
                observed::current=&actual_ready;return peer.setup.ready(frame->raw,charge); // unchanged sole source call
            }();
            observe_discard(index,actual_ready);
            if(result.status_code()!=1||!result.publishable())throw db_error("actual source READY result unavailable");++handled;
            if(drop_prepare&&control["operation"]=="prepare"&&dropped++==0)return true;
            const auto wire=json::parse(result.wire());
            if(wire.contains("latticeCanonicalRange")) {
                if(range_responses.size()>=256)throw db_error("receipt coverage fixture response bound");
                range_responses.emplace_back(index,wire.at("latticeCanonicalRange"));
            }
            peer.physical.trigger_on_message(transport_message::from_string(result.wire()));
        } else if(control.contains("auditLog")) {
            if(observed_uploads.size()>=64)throw db_error("fixture observed upload bound");
            const auto event=server_sent_event::from_json(frame->raw);if(!event||event->event_type!=server_sent_event::type::audit_log)throw db_error("fixture invalid actual upload");
            std::vector<std::string> ids;for(const auto& entry:event->audit_logs)ids.push_back(entry.global_id);
            observed_uploads.emplace_back(index,std::move(ids));
            if(hold_uploads){if(held_uploads.size()>=32)throw db_error("fixture held upload bound");held_uploads.push_back(std::move(*frame));return true;}
            auto result=peer.setup.receive(frame->raw);if(result.status_code()!=1)throw db_error("actual source upload refused");
            auto accepted_ids=result.take_ids();if(!suppress_upload_ack)peer.physical.trigger_on_message(transport_message::from_string(server_sent_event::make_ack(accepted_ids).to_json()));
        }
        return true;
    }
    template<class F> bool until(F predicate,int milliseconds=5000) {const auto end=std::chrono::steady_clock::now()+std::chrono::milliseconds(milliseconds);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate())return true;std::this_thread::sleep_for(std::chrono::milliseconds(2));}const bool result=predicate();if(!result)report_copied_failure();return result;}
    int64_t scalar(lattice_db& owner,const std::string& sql){const auto value=std::get<int64_t>(owner.db().query(sql).at(0).at("n"));
        if(&owner==receiver.get()){
            if(sql=="SELECT phase AS n FROM _lattice_producer_continuity"){last_phase=value;phase_observed=true;}
            if(sql=="SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"){last_revision1=value;revision1_observed=true;}
            if(sql=="SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2"){last_revision2=value;revision2_observed=true;}}
        return value;}
    int64_t phase(){return scalar(*receiver,"SELECT phase AS n FROM _lattice_producer_continuity");}
    bool has_error(){std::lock_guard lock(errors_mutex);return !errors.empty();}
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

TEST_F(RecoveryReceiptCoverageController, AcceptedAThenUnknownBResendsCoverageOnlyAndInstallsWithoutLegacyAck) {
    configure(2);connect();
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    seed_local(1,810);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(until([&]{return held_originals(0)==ids&&held_originals(1)==ids;}));
    const auto first=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& f){return f.endpoint.matches(peers[0].physical);});
    ASSERT_NE(first,held_uploads.end());const auto a_wire=first->raw;
    auto accepted=peers[0].setup.receive(a_wire);ASSERT_EQ(accepted.status_code(),1);EXPECT_EQ(accepted.take_ids(),ids);
    const auto source_before=source->db().query("SELECT * FROM ControllerRow ORDER BY id");
    const auto source_audit=source->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto source_receipts=source->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id");
    const auto source_store=source->db().query("SELECT * FROM _lattice_canonical_store");
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage WHERE namespace_id=CAST('a' AS BLOB)"),1);
    ASSERT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage WHERE namespace_id=CAST('b' AS BLOB)"),0);
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL"),2);
    // Actual process/route retirement releases old in-flight exclusions. Both
    // durable claims remain; A's genuine source receipt predates the next Q.
    accepted={};close_receiver();open_receiver();range_responses.clear();
    auto cancel=std::make_shared<ControllerPause>(),installed=std::make_shared<ControllerPause>();pauses.push_back(cancel);pauses.push_back(installed);
    auto pending=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[pending,installed](const char* stage){
        if(std::strcmp(stage,"reconciliation-pending")==0)++*pending;
        if(std::strcmp(stage,"install-committed")==0)installed->wait();
    },[cancel](const char* stage)->std::shared_ptr<void>{if(std::strcmp(stage,"reconcile-cancel")==0)cancel->wait();return {};});
    connect();ASSERT_TRUE(until([&]{return cancel->ready();}));ASSERT_EQ(phase(),2);ASSERT_GT(pending->load(),0u);
    bool a_positive=false,b_unknown=false;
    for(const auto& [peer,frame]:range_responses)if(frame.at("kind")=="receipt_page")for(const auto& item:frame.at("body").at("items")) {
        EXPECT_EQ(frame.at("version"),3);EXPECT_EQ(item.at("original_id"),detail::canonical_writer_adapter::uuid_key(ids[0]));EXPECT_TRUE(item.contains("operation_digest"));
        if(peer==0&&item.at("status")=="committed")a_positive=true;
        if(peer==1&&item.at("status")=="unknown")b_unknown=true;
    }
    EXPECT_TRUE(a_positive);EXPECT_TRUE(b_unknown);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_THROW(insert(*receiver,controller_uuid(811),"closed while B is unknown"),db_error);
    const auto start=observed_uploads.size();hold_uploads=false;cancel->release();
    ASSERT_TRUE(until([&]{return installed->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(observed_originals(1,start),ids);EXPECT_TRUE(observed_originals(0,start).empty());
    EXPECT_EQ(source->db().query("SELECT * FROM ControllerRow ORDER BY id"),source_before);
    EXPECT_EQ(source->db().query("SELECT * FROM AuditLog ORDER BY id"),source_audit);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id"),source_receipts);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_store"),source_store);
    EXPECT_EQ(scalar(*source,"SELECT origins AS n FROM _lattice_canonical_receipt_profile"),1);
    EXPECT_EQ(scalar(*source,"SELECT cells AS n FROM _lattice_canonical_receipt_profile"),2);
    EXPECT_EQ(scalar(*source,"SELECT mutation AS n FROM _lattice_canonical_receipt_profile"),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2"),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    installed->release();ASSERT_TRUE(until([&]{return phase()==0;}));EXPECT_FALSE(has_error());
    // No ACK was fabricated for either physical upload; canonical positives
    // alone settled the two obligations. A duplicate upload after lost ACK has
    // neither another global effect nor a third coverage cell.
    auto duplicate=peers[1].setup.receive(a_wire);ASSERT_EQ(duplicate.status_code(),1);EXPECT_EQ(duplicate.take_ids(),ids);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_store"),source_store);
    EXPECT_EQ(scalar(*source,"SELECT mutation AS n FROM _lattice_canonical_receipt_profile"),2);
}

TEST_F(RecoveryReceiptCoverageController, RegisteredUnsentUpdateDeleteKeepsBothOriginalJournalsAndNoHistoryOverlay) {
    configure(2);const auto id=controller_uuid(820);insert(*source,id,"canonical");insert(*receiver,id,"local");
    receiver->begin_transaction();try {
        receiver->db().execute("UPDATE ControllerRow SET value=?,note=? WHERE globalId=?",{std::string("ordinary-change"),std::string("latest-private"),id});
        receiver->db().execute("DELETE FROM ControllerRow WHERE globalId=?",{id});receiver->commit();
    }catch(...){if(receiver->db().is_in_transaction())receiver->rollback();throw;}
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");ASSERT_EQ(audit.size(),3u);
    const auto journal=receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");ASSERT_EQ(journal.size(),6u);
    const auto pause=pause_install();connect();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),journal);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM ControllerRow"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),2);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    EXPECT_EQ(scalar(*source,"SELECT origins AS n FROM _lattice_canonical_receipt_profile"),0);
    std::set<std::string> requested_channels;
    ASSERT_FALSE(requests.empty());
    for(const auto& raw:requests) {
        const auto q=json::parse(raw).at("latticeCanonicalRange");EXPECT_EQ(q.at("version"),3);
        requested_channels.insert(q.at("attempt").at("channel").get<std::string>());
        EXPECT_TRUE(q.at("body").contains("registered_producer"));
        EXPECT_EQ(q.at("body").at("receipt_requests").size(),3u);
        for(const auto& item:q.at("body").at("receipt_requests"))EXPECT_EQ(item.at("operation_digest").get<std::string>().size(),64u);
    }
    EXPECT_EQ(requested_channels,(std::set<std::string>{peers[0].channel,peers[1].channel}));
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));EXPECT_FALSE(has_error());
}

TEST_F(RecoveryReceiptCoverageController, SurvivingUnsentUpdateRestoresExactCurrentNoHistoryAndOrdinaryValues) {
    configure(2);const auto id=controller_uuid(825);insert(*source,id,"canonical");insert(*receiver,id,"local");
    receiver->begin_transaction();try {
        receiver->db().execute("UPDATE ControllerRow SET value=?,note=? WHERE globalId=?",{std::string("ordinary-current"),std::string("no-history-current"),id});receiver->commit();
    }catch(...){if(receiver->db().is_in_transaction())receiver->rollback();throw;}
    const auto before=receiver->db().query("SELECT globalId,value,note FROM ControllerRow ORDER BY globalId");
    const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");ASSERT_EQ(audit.size(),2u);
    const auto journal=receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");ASSERT_EQ(journal.size(),4u);
    const auto pause=pause_install();connect();ASSERT_TRUE(until([&]{return pause->ready();}));ASSERT_EQ(phase(),3);
    EXPECT_EQ(receiver->db().query("SELECT globalId,value,note FROM ControllerRow ORDER BY globalId"),before);
    ASSERT_EQ(before.size(),1u);EXPECT_EQ(std::get<std::string>(before[0].at("note")),"no-history-current");
    EXPECT_EQ(std::get<std::string>(before[0].at("value")),"ordinary-current");
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),journal);
    EXPECT_EQ(scalar(*source,"SELECT origins AS n FROM _lattice_canonical_receipt_profile"),0);
    EXPECT_TRUE(observed_uploads.empty());
    pause->release();ASSERT_TRUE(until([&]{return phase()==0;}));EXPECT_FALSE(has_error());
}

TEST_F(RecoveryReceiptCoverageController, RegisteredInstalledReopenUsesDurableV3FramingBeforeNewAttempt) {
    configure(2);insert(*source,controller_uuid(830),"retained");connect();
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    const auto rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");
    const auto prior=receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");
    close_receiver();open_receiver();EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel"),prior);
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2")==2;}));
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);EXPECT_FALSE(has_error());
}


class RecoveryReceiptCoverageMigrationController:public RecoveryReceiptCoverageController {
protected:
    Snapshot all_receiver_state() {
        Snapshot value;const auto tables=receiver->db().query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name LIMIT 129");
        if(tables.size()>128)throw db_error("migration controller fixture table bound");
        for(const auto& row:tables) {
            const auto& name=std::get<std::string>(row.at("name"));
            if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](char c){return (c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_';}))
                throw db_error("migration controller fixture table name refused");
            value[name]=receiver->db().query("SELECT * FROM \""+name+"\" ORDER BY 1");
        }
        value["sqlite_schema"]=receiver->db().query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");return value;
    }
    Snapshot source_global_state() {
        Snapshot value;for(const auto* table:{"ControllerRow","AuditLog","_lattice_canonical_store","_lattice_canonical_receipt","_lattice_canonical_touch","_lattice_canonical_namespace"})
            value[table]=source->db().query("SELECT * FROM "+std::string(table)+" ORDER BY 1");return value;
    }
    Snapshot domain_state() {
        Snapshot value;for(const auto* table:{"_lattice_recovery_domain_config","_lattice_recovery_domain","_lattice_recovery_domain_member"})
            value[table]=receiver->db().query("SELECT * FROM "+std::string(table)+" ORDER BY 1");return value;
    }
    bool error_contains(const std::string& expected) {
        std::lock_guard lock(errors_mutex);return std::any_of(errors.begin(),errors.end(),[&](const auto& error){return error.find(expected)!=std::string::npos;});
    }
    void retire_for_source_migration() {
        // The old receiver and its scheduler are fully retired. No source
        // setup, result, or stop token is carried across the owned migration.
        close_receiver();probe.reset();
        for(auto& peer:peers){peer.live->store(false);peer.setup.close_on_io();peer.setup={};peer.physical={};}
        std::lock_guard lock(wire->mutex);wire->dials.clear();wire->frames.clear();wire->endpoints.clear();
    }
    int migrate_actual_source() {
        if(registered_source)throw db_error("fixture migration requires an actual old v2 source");
        const auto prior=source_policy(peers.at(0).ns);retire_for_source_migration();registered_source=true;
        return source_ref->migrate_relay_receipt_coverage(prior.dump(),source_policy(peers.at(0).ns).dump());
    }
    void refresh_actual_expectations() {
        for(size_t index=0;index<peers.size();++index) {
            auto& peer=peers[index];peer.live=std::make_shared<std::atomic<bool>>(true);peer.setup=serve(index);
            const auto d=json::parse(peer.setup.descriptor());
            peer.expectation={{"endpoint",peer.endpoint},{"source",d.at("source")},{"incomingScope",d.at("incomingScope")},
                {"peer",d.at("route").at("peer")},{"channel",peer.channel},{"validForMilliseconds",600000}};
        }
        // The durable receiver enrollment is unchanged. Only its actual new
        // authenticated source expectation names the newly enrolled profile.
    }
    static std::string stored_text(const database::row_t& row,const char* key) {
        const auto& bytes=std::get<std::vector<uint8_t>>(row.at(key));return {bytes.begin(),bytes.end()};
    }
    void refuse_mixed_producer(bool incarnation) {
        registration_overrides[1]={incarnation?"one-durable-controller":"another-durable-controller",controller_uuid(incarnation?92:91)};
        configure(2);insert(*source,controller_uuid(861),"must not install");
        const auto before=all_receiver_state(),global=source_global_state();
        auto barriers=std::make_shared<std::atomic<unsigned>>(0);
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[barriers](const char* stage){if(std::strcmp(stage,"barrier-committed")==0)++*barriers;});
        connect();ASSERT_TRUE(until([&]{return error_contains("controller receipt producer or cohort differs across contributions");}));
        EXPECT_EQ(barriers->load(),0u);EXPECT_EQ(phase(),0);
        EXPECT_EQ(all_receiver_state(),before);EXPECT_EQ(source_global_state(),global);
        EXPECT_TRUE(requests.empty());EXPECT_TRUE(observed_uploads.empty());
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request"),0);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);
        EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_origin"),0);
    }
};

TEST_F(RecoveryReceiptCoverageMigrationController, ActualV2SourceMigrationKeepsInstalledReceiverDomainAndCreatesFreshV3Attempt) {
    registered_source=false;configure(2);insert(*source,controller_uuid(850),"same canonical authority");connect();
    ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    ASSERT_FALSE(has_error());const auto domains=domain_state(),prior=all_receiver_state();
    ASSERT_EQ(domains.at("_lattice_recovery_domain").size(),1u);ASSERT_EQ(domains.at("_lattice_recovery_domain_member").size(),1u);
    const auto rows=receiver->db().query("SELECT * FROM ControllerRow ORDER BY id");ASSERT_EQ(rows.size(),1u);
    const auto old_q=receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");ASSERT_EQ(old_q.size(),2u);
    for(const auto& row:old_q)EXPECT_EQ(json::parse(stored_text(row,"request_frame")).at("latticeCanonicalRange").at("version"),2);
    auto global=source_global_state();ASSERT_FALSE(global.at("_lattice_canonical_receipt").empty());
    ASSERT_EQ(migrate_actual_source(),1)<<last_bridge_error();
    global.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};EXPECT_EQ(source_global_state(),global);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_origin"),0);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage"),0);
    refresh_actual_expectations();open_receiver();
    EXPECT_EQ(domain_state(),domains);EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel"),old_q);
    EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);
    auto reopened=prior;auto& incarnation=reopened.at("_lattice_producer_continuity").at(0).at("incarnation");
    incarnation=std::get<int64_t>(incarnation)+1;EXPECT_EQ(all_receiver_state(),reopened);
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2")==2;}));
    EXPECT_FALSE(has_error());EXPECT_EQ(domain_state(),domains);EXPECT_EQ(receiver->db().query("SELECT * FROM ControllerRow ORDER BY id"),rows);
    EXPECT_EQ(source_global_state(),global);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_origin"),0);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage"),0);
    const auto fresh_q=receiver->db().query("SELECT * FROM _lattice_recovery_request ORDER BY channel");ASSERT_EQ(fresh_q.size(),2u);
    for(size_t index=0;index<fresh_q.size();++index) {
        const auto& before=old_q[index];const auto& after=fresh_q[index];
        EXPECT_EQ(after.at("domain"),before.at("domain"));EXPECT_EQ(std::get<int64_t>(after.at("sequence")),std::get<int64_t>(before.at("sequence"))+1);
        EXPECT_NE(after.at("source_context"),before.at("source_context"));
        const auto frame=json::parse(stored_text(after,"request_frame")).at("latticeCanonicalRange");EXPECT_EQ(frame.at("version"),3);
        EXPECT_TRUE(frame.at("body").contains("registered_producer"));
    }
}

TEST_F(RecoveryReceiptCoverageMigrationController, ActualSourceMigrationRefusesPendingOldQWithoutChangingFrozenReceiverHistory) {
    registered_source=false;configure(2);insert(*source,controller_uuid(851),"canonical pending");seed_local(1,852);
    auto attempts=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),nullptr,[attempts](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"install")==0){++*attempts;throw db_error("fixture preserve pending v2 install");}return {};
    });
    connect();ASSERT_TRUE(until([&]{return error_contains("fixture preserve pending v2 install");}));
    ASSERT_EQ(attempts->load(),1u);ASSERT_EQ(phase(),2);
    const auto before=all_receiver_state();ASSERT_EQ(before.at("_lattice_recovery_request").size(),2u);
    ASSERT_EQ(before.at("AuditLog").size(),1u);ASSERT_EQ(before.at("_lattice_obligation_entry").size(),2u);
    for(const auto& row:before.at("_lattice_recovery_request")) {
        EXPECT_EQ(json::parse(stored_text(row,"request_frame")).at("latticeCanonicalRange").at("version"),2);
        EXPECT_FALSE(stored_text(row,"manifest_frame").empty());
    }
    ASSERT_EQ(migrate_actual_source(),1)<<last_bridge_error();refresh_actual_expectations();open_receiver();
    auto expected=before;auto& incarnation=expected.at("_lattice_producer_continuity").at(0).at("incarnation");incarnation=std::get<int64_t>(incarnation)+1;
    ASSERT_EQ(all_receiver_state(),expected);const auto request_count=requests.size(),upload_count=observed_uploads.size();
    connect();ASSERT_TRUE(until([&]{return error_contains("controller predecessor non-profile context changed");}));
    EXPECT_EQ(phase(),2);EXPECT_EQ(all_receiver_state(),expected);
    EXPECT_EQ(requests.size(),request_count);EXPECT_EQ(observed_uploads.size(),upload_count);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision<>0"),0);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_origin"),0);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage"),0);
}

TEST_F(RecoveryReceiptCoverageMigrationController, IndividuallyAuthorizedRegistrationMismatchRefusesBeforeControllerEffects) {
    refuse_mixed_producer(false);
}

TEST_F(RecoveryReceiptCoverageMigrationController, IndividuallyAuthorizedProducerIncarnationMismatchRefusesBeforeControllerEffects) {
    refuse_mixed_producer(true);
}

}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include "CanonicalReadyAdoptionTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_ready_named_profile.hpp"
namespace {
class PredecessorReceiptCoverageController : public RecoveryReceiptCoverageMigrationController {
protected:
    bool adopted=false;
    json source_policy(const std::string& ns)override {
        auto p=RecoveryReceiptCoverageController::source_policy(ns);
        if(adopted){p["readyProfile"]="bounded48MiBOrphanV1";p["orphanResumeGraceMilliseconds"]=60000;}return p;
    }
    void adopt_actual_v3() {
        const auto s=peers.at(0).expectation.at("source");retire_for_source_migration();
        detail::canonical_namespaced_writer_profile p;
        p.writer.binding={s.at("sourceID"),s.at("epoch"),s.at("scopeDigest"),s.at("schemaDigest")};
        p.writer.limits={65536,16777216,65536,16777216,256,128,64};p.writer.models={"ControllerRow"};p.writer.upstream_requested=true;
        p.namespaces.local_namespace="local";p.namespaces.entries={{"a","a-v1",1},{"b","b-v1",1},{"local","local-v1",1}};
        p.namespaces.coverage=detail::canonical_coverage_profile{controller_uuid(90),1,{"a","b"}};
        const auto result=detail::adopt_ready_lifecycle_for_test(source,p,{256,65536,1048576},{64,3600000},
            detail::canonical_named_ready_profile(s.at("authority"),p.writer.limits,true,"bounded48MiBV1"),"bounded48MiBV1",60000);
        ASSERT_EQ(result.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(result.record);
        adopted=true;for(auto& peer:peers)peer.live=std::make_shared<std::atomic<bool>>(true);drop_prepare=false;open_receiver();
    }
};
TEST_F(PredecessorReceiptCoverageController, ActualRegisteredCohortAdoptionKeepsOneReceiptTwoCellsAndBothExactOldQs) {
    configure(2);connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    seed_local(1,9800);hold_uploads=false;
    ASSERT_TRUE(until([&]{return scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_coverage")==2;}));
    const auto global=source_global_state();const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto coverage=source->db().query("SELECT * FROM _lattice_canonical_receipt_coverage ORDER BY namespace_id,original_id");
    const auto origins=source->db().query("SELECT * FROM _lattice_canonical_receipt_origin ORDER BY original_id");
    drop_prepare=true;request_recovery();ASSERT_TRUE(until([&]{return dropped==1;}));ASSERT_EQ(phase(),2);
    const auto framing=receiver->db().query("SELECT channel,request_frame,source_context FROM _lattice_recovery_request ORDER BY channel");
    adopt_actual_v3();ASSERT_FALSE(HasFatalFailure());auto proved=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[proved](const char* stage){if(std::strcmp(stage,"predecessor-consumed")==0)++*proved;});
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2")==2;}));
    EXPECT_FALSE(has_error());EXPECT_EQ(proved->load(),2u);EXPECT_EQ(source_global_state(),global);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt_coverage ORDER BY namespace_id,original_id"),coverage);
    EXPECT_EQ(source->db().query("SELECT * FROM _lattice_canonical_receipt_origin ORDER BY original_id"),origins);
    EXPECT_EQ(receiver->db().query("SELECT channel,request_frame,source_context FROM _lattice_recovery_request ORDER BY channel"),framing);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM ControllerRow"),1);
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt"),1);EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=2"),2);
}
TEST_F(PredecessorReceiptCoverageController, ChangedRegisteredProducerCannotUseAdoptionToRelaxRetainedReceiptBinding) {
    configure(2);seed_local(1,9810);drop_prepare=true;connect();ASSERT_TRUE(until([&]{return dropped==1;}));
    const auto prior=all_receiver_state();adopt_actual_v3();ASSERT_FALSE(HasFatalFailure());
    registration_overrides[0]={"different-controller",controller_uuid(99)};registration_overrides[1]=registration_overrides[0];
    auto proved=std::make_shared<std::atomic<unsigned>>(0);probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[proved](const char* stage){if(std::strcmp(stage,"predecessor-consumed")==0)++*proved;});
    connect();ASSERT_TRUE(until([&]{return has_error();}));EXPECT_EQ(proved->load(),0u);EXPECT_EQ(phase(),2);
    const auto after=all_receiver_state();for(const auto* table:{"ControllerRow","AuditLog","_lattice_obligation_entry","_lattice_recovery_request","_lattice_install_channel"})EXPECT_EQ(after.at(table),prior.at(table));
    EXPECT_EQ(scalar(*source,"SELECT COUNT(*) AS n FROM _lattice_canonical_receipt_origin"),0);EXPECT_TRUE(observed_uploads.empty());
}

// This helper is also compiled against the genuine previous product/headers
// for the migration case. That old executable creates actual format-v1 rows;
// no v2 test flag downgrades or fabricates persisted receipt knowledge.
class ReceiptDispositionProcess : public RecoveryReceiptCoverageController {
protected:
    void SetUp() override {
        const char* area=std::getenv("LATTICE_RECEIPT_RETENTION_AREA");
        if(!area||!*area)return;
        const std::filesystem::path root(area);
        source_file.path=root/"source.sqlite";receiver_file.path=root/"receiver.sqlite";
        container=receiver_file.str()+".lattice-continuous";
        RecoveryReceiptCoverageController::SetUp();
    }
    void TearDown() override {
        if(std::getenv("LATTICE_RECEIPT_RETENTION_AREA"))RecoveryReceiptCoverageController::TearDown();
    }
};
TEST_F(ReceiptDispositionProcess, ProcessHelper) {
    const char* mode=std::getenv("LATTICE_RECEIPT_RETENTION_MODE");
    if(!mode||!*mode)GTEST_SKIP()<<"parent-only receipt crash/reopen helper";
    if(std::strcmp(mode,"crash")==0||std::strcmp(mode,"frozen")==0){
        const bool frozen_mode=std::strcmp(mode,"frozen")==0;
        configure(2);connect();ASSERT_TRUE(until([&]{return phase()==0;}));
        seed_local(1,9930);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
        ASSERT_TRUE(until([&]{return held_originals(0)==ids&&held_originals(1)==ids;}));
        const auto at=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& f){return f.endpoint.matches(peers[0].physical);});ASSERT_NE(at,held_uploads.end());
        auto accepted=peers[0].setup.receive(at->raw);ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.take_ids(),ids);accepted={};
        close_receiver();open_receiver();
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[frozen_mode](const char* stage){
            if(frozen_mode&&std::strcmp(stage,"reconciliation-pending")==0)::_exit(0);
            // Original real cancellation COMMIT has returned. Stop the process
            // without publishing or running any C++/SQLite cleanup destructor.
            if(!frozen_mode&&std::strcmp(stage,"reconcile-cancel-committed")==0)::_exit(0);
        });
        connect();ASSERT_TRUE(until([&]{return has_error();}));FAIL()<<"helper never reached known cancellation COMMIT";
    }else if(std::strcmp(mode,"verify")==0||std::strcmp(mode,"verify-frozen")==0||std::strcmp(mode,"verify-partial")==0){
        configure(2);const bool frozen_mode=std::strcmp(mode,"verify")==0?false:true;
        ASSERT_EQ(phase(),frozen_mode?2:4);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
        const auto legacy=scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE receipt_format=1 AND receipt_decisions IS NULL");
        ASSERT_TRUE(legacy==0||legacy==2);
        const auto old_sequence=scalar(*receiver,"SELECT MIN(sequence) AS n FROM _lattice_recovery_request");
        const auto first=observed_uploads.size();auto installed=std::make_shared<ControllerPause>();pauses.push_back(installed);
        auto local_cancels=std::make_shared<std::atomic<unsigned>>(0);
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[installed,local_cancels](const char* stage){
            if(std::strcmp(stage,"migrated-staged-local-cancel")==0)++*local_cancels;
            if(std::strcmp(stage,"install-committed")==0)installed->wait();
        });
        hold_uploads=false;connect();
        ASSERT_TRUE(until([&]{return installed->ready();}));EXPECT_EQ(phase(),3);
        // These actual old-product children keep the original C/A and U/B
        // no-resend/selective-send assertions below. Observe a fresh reserved
        // successor rather than manufacturing classification for legacy Q.
        if(legacy){
            EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE receipt_format=2 AND typeof(receipt_decisions)='blob'"),2);
            EXPECT_GT(scalar(*receiver,"SELECT MIN(sequence) AS n FROM _lattice_recovery_request"),old_sequence);
            if(frozen_mode)EXPECT_EQ(local_cancels->load(),2u);
        }
        EXPECT_TRUE(observed_originals(0,first).empty());EXPECT_EQ(observed_originals(1,first),ids);
        EXPECT_EQ(scalar(*source,"SELECT origins AS n FROM _lattice_canonical_receipt_profile"),1);
        EXPECT_EQ(scalar(*source,"SELECT cells AS n FROM _lattice_canonical_receipt_profile"),2);
        EXPECT_EQ(scalar(*source,"SELECT mutation AS n FROM _lattice_canonical_receipt_profile"),2);
        EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=2"),2);
        installed->release();ASSERT_TRUE(until([&]{return phase()==0;}));EXPECT_FALSE(has_error());
    }else FAIL()<<"unsupported receipt helper mode";
}

int run_receipt_child(const std::filesystem::path& area,const std::string& mode,const std::string& executable) {
    std::vector<std::string> environment;
    for(char** item=::environ;*item;++item){std::string value(*item);if(!value.starts_with("LATTICE_RECEIPT_RETENTION_")&&!value.starts_with("LATTICE_TEST_LOG_PATH="))environment.push_back(std::move(value));}
    environment.push_back("LATTICE_RECEIPT_RETENTION_AREA="+area.string());environment.push_back("LATTICE_RECEIPT_RETENTION_MODE="+mode);
    const char* parent_log=std::getenv("LATTICE_TEST_LOG_PATH");
    const std::string native=parent_log&&*parent_log?std::string(parent_log)+"."+area.filename().string()+"."+mode+".native.log":(area.string()+"."+mode+".native.log");
    environment.push_back("LATTICE_TEST_LOG_PATH="+native);
    std::vector<char*> env;for(auto& value:environment)env.push_back(value.data());env.push_back(nullptr);
    std::string filter="--gtest_filter=ReceiptDispositionProcess.ProcessHelper",repeat="--gtest_repeat=1",xml="--gtest_output=",color="--gtest_color=no";
    char* argv[]={const_cast<char*>(executable.c_str()),filter.data(),repeat.data(),xml.data(),color.data(),nullptr};
    posix_spawn_file_actions_t actions;if(posix_spawn_file_actions_init(&actions))throw db_error("receipt helper actions");
    struct actions_end{posix_spawn_file_actions_t& a;~actions_end(){posix_spawn_file_actions_destroy(&a);}} ae{actions};
    const std::filesystem::path log=native+".terminal.log";
    if(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)||posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO))throw db_error("receipt helper log");
    posix_spawnattr_t attr;if(posix_spawnattr_init(&attr))throw db_error("receipt helper attributes");
    struct attr_end{posix_spawnattr_t& a;~attr_end(){posix_spawnattr_destroy(&a);}} at{attr};
    if(posix_spawnattr_setpgroup(&attr,0)||posix_spawnattr_setflags(&attr,POSIX_SPAWN_SETPGROUP))throw db_error("receipt helper group");
    const auto pending=area/"child-ending.pending";
    std::FILE* mark=std::fopen(pending.c_str(),"wx");if(!mark)throw db_error("receipt helper ending marker");
    const int wrote=std::fputs("owned child ending is not yet proved; preserve this store namespace\n",mark);
    const int closed=std::fclose(mark);if(wrote<0||closed)throw db_error("receipt helper ending marker write");
    const auto issued=std::chrono::steady_clock::now();
    const auto work_end=issued+std::chrono::seconds(15),ending_end=issued+std::chrono::seconds(17);
    pid_t child=-1;const int spawned=posix_spawn(&child,executable.c_str(),&actions,&attr,argv,env.data());
    if(spawned){std::error_code error;std::filesystem::remove(pending,error);throw db_error("receipt helper spawn");}
    int status=0,forced=0;bool reaped=false,wait_known=true,signalled=false;
    while(std::chrono::steady_clock::now()<ending_end){
        if(!reaped&&wait_known){const auto rc=waitpid(child,&status,WNOHANG);
            if(rc==child)reaped=true;
            else if(rc<0&&errno!=EINTR){wait_known=false;forced=126;}}
        errno=0;const int group=::kill(-child,0);const bool gone=group<0&&errno==ESRCH;
        if(reaped&&gone){
            if(!forced&&std::chrono::steady_clock::now()>=work_end)forced=124;
            std::error_code error;if(!std::filesystem::remove(pending,error)||error)return 126;
            return forced?forced:WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);}
        if(!signalled&&((reaped&&!gone)||!wait_known||std::chrono::steady_clock::now()>=work_end)){
            if(!forced)forced=reaped?125:124;
            // The original15s work watchdog is unchanged. Ending observation
            // has its own fixed2s allowance, fixed at original issue time;
            // no blocking wait and no renewed application deadline.
            ::kill(-child,SIGKILL);signalled=true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if(!signalled){::kill(-child,SIGKILL);signalled=true;}
    std::fprintf(stderr,"receipt helper ending UNKNOWN pid=%ld reaped=%d waitKnown=%d signalIssued=%d; namespace retained\n",static_cast<long>(child),int(reaped),int(wait_known),int(signalled));
    return 126; // marker deliberately remains: do not delete a possibly live store

}
struct StockReceiptFault {
    bool identity=false,marker_removed=false;
    int open=SQLITE_MISUSE,prepare=SQLITE_MISUSE,step=SQLITE_MISUSE,finalize=SQLITE_MISUSE,close=SQLITE_MISUSE;
    sqlite3_int64 changes=-1;
};
StockReceiptFault corrupt_owned_receipt_file(const std::filesystem::path& area,const char* sql) {
    StockReceiptFault result;const auto file=area/"receiver.sqlite.lattice-continuous/store.sqlite";
    const auto pending=area/"sqlite-fault-ending.pending";
    // Four faults in this finite parent process. Any unknown first close keeps
    // its original pointer and namespace charged; there is no close retry.
    static std::array<sqlite3*,4> debts{};sqlite3** cell=nullptr;
    for(auto& slot:debts)if(!slot){cell=&slot;break;}if(!cell)return result;
    struct stat before{},after{};if(::lstat(file.c_str(),&before)||!S_ISREG(before.st_mode))return result;
    std::FILE* mark=std::fopen(pending.c_str(),"wx");if(!mark)return result;
    const int wrote=std::fputs("first SQLite ending unproved: preserve exact owned store namespace\n",mark);
    const int marker_close=std::fclose(mark);if(wrote<0||marker_close)return result;
    result.open=sqlite3_open_v2(file.c_str(),cell,SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOFOLLOW,nullptr); // no CREATE/URI
    sqlite3_stmt* statement=nullptr;
    if(result.open==SQLITE_OK&&*cell){const char* actual=sqlite3_db_filename(*cell,"main");int moved=-1;
        result.identity=actual&&file.string()==actual&&!::lstat(actual,&after)&&S_ISREG(after.st_mode)&&
            before.st_dev==after.st_dev&&before.st_ino==after.st_ino&&
            sqlite3_file_control(*cell,"main",SQLITE_FCNTL_HAS_MOVED,&moved)==SQLITE_OK&&!moved;
        if(result.identity){const char* tail=nullptr;result.prepare=sqlite3_prepare_v2(*cell,sql,-1,&statement,&tail);
            if(result.prepare==SQLITE_OK&&statement&&tail&&!*tail){result.step=sqlite3_step(statement);if(result.step==SQLITE_DONE)result.changes=sqlite3_changes64(*cell);}}
    }
    if(statement)result.finalize=sqlite3_finalize(statement); // exact original, first result
    if(*cell){result.close=sqlite3_close(*cell);if(result.close==SQLITE_OK)*cell=nullptr;}
    if(result.identity&&result.open==SQLITE_OK&&result.prepare==SQLITE_OK&&result.step==SQLITE_DONE&&result.finalize==SQLITE_OK&&result.close==SQLITE_OK){
        std::error_code error;result.marker_removed=std::filesystem::remove(pending,error)&&!error;}
    return result;
}
void expect_stock_receipt_fault(const StockReceiptFault& result,sqlite3_int64 changes) {
    EXPECT_TRUE(result.identity);EXPECT_EQ(result.open,SQLITE_OK);EXPECT_EQ(result.prepare,SQLITE_OK);
    EXPECT_EQ(result.step,SQLITE_DONE);EXPECT_EQ(result.finalize,SQLITE_OK);EXPECT_EQ(result.close,SQLITE_OK);
    EXPECT_EQ(result.changes,changes);EXPECT_TRUE(result.marker_removed);
}
struct ReceiptProcessArea {
    TempDB name{"receipt_retention_process"};std::filesystem::path path=name.str()+".owned";
    ReceiptProcessArea(){if(!std::filesystem::create_directory(path))throw db_error("receipt helper own namespace");}
    ~ReceiptProcessArea(){std::error_code error;if(std::filesystem::exists(path/"child-ending.pending",error)||error||std::filesystem::exists(path/"sqlite-fault-ending.pending",error)||error)return;std::filesystem::remove_all(path,error);}
};
#if !defined(LATTICE_RECEIPT_RETENTION_LEGACY_WRITER)
TEST(ReceiptDispositionCrash, KnownCancelCommitSurvivesAbruptExitAndReopensOnlyUnknownNamespace) {
    ReceiptProcessArea area;const auto executable=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();
    ASSERT_EQ(run_receipt_child(area.path,"crash",executable),0);
    // Read the real committed cohort after actual kernel process ending.
    {database observer((area.path/"receiver.sqlite.lattice-continuous/store.sqlite").string(),database::open_mode::read_only,100);
    const auto rows=observer.query("SELECT CAST(substr(receipt_decisions,66) AS TEXT) AS d FROM _lattice_recovery_request ORDER BY channel");ASSERT_EQ(rows.size(),2u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("d")),"C0");EXPECT_EQ(std::get<std::string>(rows[1].at("d")),"U1");}
    ASSERT_EQ(run_receipt_child(area.path,"verify",executable),0);
}
TEST(ReceiptDispositionCrash, ActualPreviousWriterV1RefreezesWithoutSendAndReacquiresSourceKnowledge) {
    const char* previous=std::getenv("LATTICE_RECEIPT_V1_HELPER_EXECUTABLE");
    if(!previous||!*previous)GTEST_SKIP()<<"requires separately compiled actual previous-product crash producer";
    ReceiptProcessArea area;ASSERT_EQ(run_receipt_child(area.path,"crash",previous),0);
    {database observer((area.path/"receiver.sqlite.lattice-continuous/store.sqlite").string(),database::open_mode::read_only,100);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT version AS n FROM _lattice_recovery_request_config").at(0).at("n")),1);}
    const auto current=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"verify",current),0);
}

using DecisionStoreAccess=detail::recovery_request_store_test_access;
struct DecisionWrite {
    std::shared_ptr<lattice_db> owner;bool ended=false;
    explicit DecisionWrite(std::shared_ptr<lattice_db> value):owner(std::move(value)){owner->begin_transaction();}
    ~DecisionWrite(){if(!ended)try{owner->rollback();}catch(...){}}
    void commit(){owner->commit();ended=true;}
};
std::shared_ptr<lattice_db> decision_owner(const std::string& path){configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;auto o=std::make_shared<lattice_db>(c);if(auto* n=instance_registry::instance().get_or_create_notifier(path))n->stop_listening();return o;}
detail::recovery_request_row decision_row(){return {{"channel",1,1},1,1,1,1,std::string(64,'d'),"source-epoch-namespace-operation-context","bounded-exact-Q","bounded-exact-M"};}

TEST_F(RecoveryReceiptCoverageController, DeniedCancelCommitRestoresVerifiedPagesAndEveryDecisionPrefix) {
    configure(2);connect();ASSERT_TRUE(until([&]{return phase()==0;}));seed_local(1,9940);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(until([&]{return held_originals(0)==ids&&held_originals(1)==ids;}));
    const auto at=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& f){return f.endpoint.matches(peers[0].physical);});ASSERT_NE(at,held_uploads.end());
    auto accepted=peers[0].setup.receive(at->raw);ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.take_ids(),ids);accepted={};
    close_receiver();open_receiver();const auto audit=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");auto hits=std::make_shared<std::atomic<unsigned>>(0);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[](const char*){},[this,hits](const char* stage)->std::shared_ptr<void>{
        if(std::strcmp(stage,"reconcile-cancel")==0)return std::make_shared<ControllerCommitFault>(receiver.get(),*hits);return {};});
    connect();ASSERT_TRUE(until([&]{return hits->load()>0&&has_error();}));EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(receipt_decisions)<>0"),0);
    EXPECT_GT(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_page"),0);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL"),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
}
TEST(ReceiptDispositionCrash, ActualPreviousFrozenV1RevalidatesWithoutRestrictedSendBeforeReservedQ) {
    const char* previous=std::getenv("LATTICE_RECEIPT_V1_HELPER_EXECUTABLE");
    if(!previous||!*previous)GTEST_SKIP()<<"requires separately compiled actual previous-product frozen producer";
    ReceiptProcessArea area;ASSERT_EQ(run_receipt_child(area.path,"frozen",previous),0);
    {database observer((area.path/"receiver.sqlite.lattice-continuous/store.sqlite").string(),database::open_mode::read_only,100);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT version AS n FROM _lattice_recovery_request_config").at(0).at("n")),1);}
    const auto current=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"verify-frozen",current),0);
}
TEST(ReceiptDispositionCrash, ActualPreviousPartialV1CancelsLocallyBeforeFreshReservedRevalidation) {
    const char* previous=std::getenv("LATTICE_RECEIPT_V1_PARTIAL_HELPER_EXECUTABLE");
    if(!previous||!*previous)GTEST_SKIP()<<"requires separately compiled actual previous-product partial-stage producer";
    ReceiptProcessArea area;ASSERT_EQ(run_receipt_child(area.path,"partial",previous),0);
    {database observer((area.path/"receiver.sqlite.lattice-continuous/store.sqlite").string(),database::open_mode::read_only,100);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT version AS n FROM _lattice_recovery_request_config").at(0).at("n")),1);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT COUNT(*) AS n FROM _lattice_recovery_request WHERE length(manifest_frame)>0").at(0).at("n")),2);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=0").at(0).at("n")),1);
     EXPECT_EQ(std::get<int64_t>(observer.query("SELECT phase AS n FROM _lattice_producer_continuity").at(0).at("n")),2);}
    const auto current=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();
    ASSERT_EQ(run_receipt_child(area.path,"verify-partial",current),0);
}
TEST(ReceiptDispositionCrash, FreshFormatTwoNullProjectionCannotClaimLegacyMigrationOrigin) {
    ReceiptProcessArea area;const auto exe=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"crash",exe),0);
    {database observer((area.path/"receiver.sqlite.lattice-continuous/store.sqlite").string(),database::open_mode::read_only,100);
     EXPECT_EQ(observer.query("SELECT receipt_format AS n FROM _lattice_recovery_request WHERE receipt_format IS NOT 2").size(),0u);}
    const auto fault=corrupt_owned_receipt_file(area.path,"UPDATE _lattice_recovery_request SET receipt_decisions=NULL");
    expect_stock_receipt_fault(fault,2);ASSERT_TRUE(fault.marker_removed);
    EXPECT_EQ(run_receipt_child(area.path,"verify",exe),1);
}
TEST(ReceiptDispositionCrash, MissingV2DecisionsCannotMasqueradeAsUnavailableLegacyKnowledge) {
    ReceiptProcessArea area;const auto exe=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"crash",exe),0);
    // Deliberate on-disk corruption after the real crash. This native fault
    // creates no Core-origin stamp or recovery permission.
    const auto fault=corrupt_owned_receipt_file(area.path,"UPDATE _lattice_recovery_request SET receipt_decisions=X''");
    expect_stock_receipt_fault(fault,2);ASSERT_TRUE(fault.marker_removed);
    EXPECT_EQ(run_receipt_child(area.path,"verify",exe),1);
}
TEST(ReceiptDispositionCrash, WellTypedDispositionBitChangeStillRefusesItsRecordedBindingDigest) {
    ReceiptProcessArea area;const auto exe=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"crash",exe),0);
    const auto fault=corrupt_owned_receipt_file(area.path,"UPDATE _lattice_recovery_request SET receipt_decisions=CAST(substr(receipt_decisions,1,65)||'U1' AS BLOB) WHERE channel=(SELECT channel FROM _lattice_recovery_request ORDER BY channel LIMIT 1)");
    expect_stock_receipt_fault(fault,1);ASSERT_TRUE(fault.marker_removed);
    EXPECT_EQ(run_receipt_child(area.path,"verify",exe),1);
}
TEST(ReceiptDispositionCrash, ForeignChannelRequestCannotBorrowAnotherRetainedDecisionOrdinal) {
    ReceiptProcessArea area;const auto exe=std::filesystem::absolute(::testing::internal::GetArgvs().at(0)).string();ASSERT_EQ(run_receipt_child(area.path,"crash",exe),0);
    const auto fault=corrupt_owned_receipt_file(area.path,"UPDATE _lattice_recovery_request SET request_frame=(SELECT request_frame FROM _lattice_recovery_request ORDER BY channel DESC LIMIT 1) WHERE channel=(SELECT channel FROM _lattice_recovery_request ORDER BY channel LIMIT 1)");
    expect_stock_receipt_fault(fault,1);ASSERT_TRUE(fault.marker_removed);
    EXPECT_EQ(run_receipt_child(area.path,"verify",exe),1);
}

TEST(ReceiptDecisionStorage, CAndNRemainDistinctAndNeitherUnknownNorLegacyAckAcrossReopen) {
    TempDB file{"receipt_decisions"};const auto original=decision_row();
    {auto o=decision_owner(file.str());DecisionWrite tx(o);DecisionStoreAccess::initialize(o);DecisionStoreAccess::insert(o,original);DecisionStoreAccess::retain(o,original,"1C0N0U1U0");tx.commit();}
    auto o=decision_owner(file.str());DecisionWrite tx(o);DecisionStoreAccess::initialize(o);const auto actual=DecisionStoreAccess::read(o,"channel");ASSERT_TRUE(actual);auto expected=original;expected.receipt_decisions=actual->receipt_decisions;EXPECT_EQ(expected.receipt_decisions.substr(65),"C0N0U1U0");EXPECT_EQ(*actual,expected);tx.commit();
}
TEST(ReceiptDecisionStorage, ForeignFramingAndInvalidDecisionOrCapacityRefuseWithoutChangingFirstRecord) {
    auto o=decision_owner(":memory:");DecisionWrite tx(o);DecisionStoreAccess::initialize(o);const auto original=decision_row();DecisionStoreAccess::insert(o,original);
    auto foreign=original;foreign.source_context+="foreign-epoch";EXPECT_THROW(DecisionStoreAccess::retain(o,foreign,"1U1"),db_error);
    for(const auto& invalid:{std::string("1C1"),std::string("1N1"),std::string("1X0"),std::string("1U"),std::string("1")+std::string(2*(detail::recovery_request_store::decision_entries+1),'U')})EXPECT_THROW(DecisionStoreAccess::retain(o,original,invalid),db_error);
    EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),std::optional{original});tx.commit();
}
TEST(ReceiptDecisionStorage, RealRollbackRestoresBothRowsAfterARecordedFirstContribution) {
    auto o=decision_owner(":memory:");auto a=decision_row(),b=a;b.journal.channel="other";
    {DecisionWrite tx(o);DecisionStoreAccess::initialize(o);DecisionStoreAccess::insert(o,a);DecisionStoreAccess::insert(o,b);tx.commit();}
    {DecisionWrite tx(o);DecisionStoreAccess::retain(o,a,"1C0");auto foreign=b;foreign.request_frame+="changed-operation";EXPECT_THROW(DecisionStoreAccess::retain(o,foreign,"1U1"),db_error);}
    DecisionWrite tx(o);EXPECT_EQ(DecisionStoreAccess::read(o,a.journal.channel),std::optional{a});EXPECT_EQ(DecisionStoreAccess::read(o,b.journal.channel),std::optional{b});tx.commit();
}
TEST(ReceiptDecisionStorage, ValidProjectionCopiedFromDifferentOperationRefusesItsParentBinding) {
    auto o=decision_owner(":memory:");auto a=decision_row(),b=a;b.journal.channel="other";b.request_frame="different-operation-Q";
    {DecisionWrite tx(o);DecisionStoreAccess::initialize(o);DecisionStoreAccess::insert(o,a);DecisionStoreAccess::insert(o,b);DecisionStoreAccess::retain(o,a,"1C0");DecisionStoreAccess::retain(o,b,"1U1");tx.commit();}
    {DecisionWrite tx(o);o->db().execute("UPDATE _lattice_recovery_request SET receipt_decisions=(SELECT receipt_decisions FROM _lattice_recovery_request WHERE channel=CAST('other' AS BLOB)) WHERE channel=CAST('channel' AS BLOB)");EXPECT_THROW(DecisionStoreAccess::read(o,"channel"),db_error);}
    DecisionWrite tx(o);ASSERT_TRUE(DecisionStoreAccess::read(o,"channel"));EXPECT_EQ(DecisionStoreAccess::read(o,"channel")->receipt_decisions.substr(65),"C0");tx.commit();
}
void initialize_genuine_v1_schema(lattice_db& owner) {
    // Fixed actual previous schema, only for storage/migration fault tests.
    // The end-to-end migration case uses a separately built real v1 writer.
    owner.db().execute("CREATE TABLE _lattice_recovery_request_config(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,max_rows INTEGER NOT NULL,max_frame INTEGER NOT NULL,max_context INTEGER NOT NULL,max_bytes INTEGER NOT NULL) WITHOUT ROWID");
    owner.db().execute("CREATE TABLE _lattice_recovery_request(channel BLOB PRIMARY KEY,incarnation INTEGER NOT NULL,generation INTEGER NOT NULL,barrier INTEGER NOT NULL,sequence INTEGER NOT NULL,journal_revision INTEGER NOT NULL,route INTEGER NOT NULL,domain BLOB NOT NULL,source_context BLOB NOT NULL,request_frame BLOB NOT NULL,manifest_frame BLOB NOT NULL) WITHOUT ROWID");
    owner.db().execute("INSERT INTO _lattice_recovery_request_config VALUES(1,1,16,4194304,65536,134217728)");
}
void insert_genuine_v1_storage(lattice_db& owner,const detail::recovery_request_row& r) {
    const auto blob=[](const std::string& x){return std::vector<uint8_t>(x.begin(),x.end());};
    owner.db().execute("INSERT INTO _lattice_recovery_request VALUES(?,?,?,?,?,?,?,?,?,?,?)",{blob(r.journal.channel),r.journal.incarnation,r.journal.generation,r.barrier,r.sequence,r.journal_revision,r.route,blob(r.domain),blob(r.source_context),blob(r.request_frame),blob(r.manifest_frame)});
}
void initialize_genuine_v1_storage(lattice_db& owner,const detail::recovery_request_row& r) {
    initialize_genuine_v1_schema(owner);insert_genuine_v1_storage(owner,r);
}
TEST(ReceiptDecisionStorage, V1MigrationPreservesExactParentAndMarksUnavailableRatherThanUnknown) {
    auto o=decision_owner(":memory:");const auto original=decision_row();DecisionWrite tx(o);initialize_genuine_v1_storage(*o,original);DecisionStoreAccess::initialize(o);
    auto expected=original;expected.receipt_decisions="0";EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),std::optional{expected});
    EXPECT_EQ(std::get<int64_t>(o->db().query("SELECT version AS n FROM _lattice_recovery_request_config").at(0).at("n")),2);tx.commit();
}
TEST(ReceiptDecisionStorage, SixteenNearFullNewRowsReserveMaximumDecisionsBeforeManifestWrite) {
    auto o=decision_owner(":memory:");DecisionWrite tx(o);DecisionStoreAccess::initialize(o);
    const auto make=[](unsigned n){auto r=decision_row();r.journal.channel="capacity-"+std::to_string(n);r.request_frame=std::string(detail::recovery_request_store::frame_bytes,'q');r.manifest_frame.clear();return r;};
    std::size_t charged=0;
    for(unsigned n=0;n<16;++n){const auto r=make(n);DecisionStoreAccess::insert(o,r);
        charged+=56+r.journal.channel.size()+r.domain.size()+r.source_context.size()+r.request_frame.size()+detail::recovery_request_store::decision_bytes;}
    for(unsigned n=0;n<16;++n){const auto r=make(n);const auto room=detail::recovery_request_store::stored_bytes-charged;
        const auto bytes=std::min(room,detail::recovery_request_store::frame_bytes);ASSERT_GT(bytes,0u);
        if(n==15){ASSERT_LT(bytes,detail::recovery_request_store::frame_bytes);
            EXPECT_THROW(DecisionStoreAccess::add_manifest(o,r,std::string(bytes+1,'m')),db_error);EXPECT_EQ(DecisionStoreAccess::read(o,r.journal.channel),std::optional{r});}
        DecisionStoreAccess::add_manifest(o,r,std::string(bytes,'m'));charged+=bytes;
    }
    ASSERT_EQ(charged,detail::recovery_request_store::stored_bytes);
    std::string pairs="1";for(std::size_t n=0;n<detail::recovery_request_store::decision_entries;++n)pairs+="U1";
    for(unsigned n=0;n<16;++n){const auto row=DecisionStoreAccess::read(o,make(n).journal.channel);ASSERT_TRUE(row);DecisionStoreAccess::retain(o,*row,pairs);}
    const auto actual=o->db().query("SELECT SUM(56+length(channel)+length(domain)+length(source_context)+length(request_frame)+length(manifest_frame)+length(receipt_decisions)) AS n FROM _lattice_recovery_request");
    EXPECT_EQ(std::get<int64_t>(actual.at(0).at("n")),static_cast<int64_t>(detail::recovery_request_store::stored_bytes));tx.commit();
}
TEST(ReceiptDecisionStorage, ExactFullSixteenRowV1MigratesWithinSameChargeWithoutLateDecisionAllocation) {
    auto o=decision_owner(":memory:");DecisionWrite tx(o);initialize_genuine_v1_schema(*o);std::size_t charged=0;
    for(unsigned n=0;n<16;++n){auto r=decision_row();r.journal.channel="legacy-capacity-"+std::to_string(n);r.request_frame=std::string(detail::recovery_request_store::frame_bytes,'q');
        const auto base=56+r.journal.channel.size()+r.domain.size()+r.source_context.size()+r.request_frame.size();
        const auto bytes=std::min(detail::recovery_request_store::frame_bytes,detail::recovery_request_store::stored_bytes-charged-base);
        r.manifest_frame=std::string(bytes,'m');charged+=base+bytes;insert_genuine_v1_storage(*o,r);}
    ASSERT_EQ(charged,detail::recovery_request_store::stored_bytes);DecisionStoreAccess::initialize(o);
    EXPECT_EQ(o->db().query("SELECT 1 FROM _lattice_recovery_request WHERE receipt_format IS NOT 1 OR receipt_decisions IS NOT NULL").size(),0u);
    const auto actual=o->db().query("SELECT SUM(56+length(channel)+length(domain)+length(source_context)+length(request_frame)+length(manifest_frame)) AS n FROM _lattice_recovery_request");
    EXPECT_EQ(std::get<int64_t>(actual.at(0).at("n")),static_cast<int64_t>(detail::recovery_request_store::stored_bytes));
    const auto first=DecisionStoreAccess::read(o,"legacy-capacity-0");ASSERT_TRUE(first);ASSERT_EQ(first->receipt_decisions,"0");
    EXPECT_THROW(DecisionStoreAccess::retain(o,*first,"1C0"),db_error);EXPECT_EQ(DecisionStoreAccess::read(o,first->journal.channel),first);tx.commit();
}
TEST(ReceiptDecisionStorage, ActualMigrationConfigWriteFailureRollsBackSchemaRowsAndConfiguration) {
    auto o=decision_owner(":memory:");const auto original=decision_row();{DecisionWrite tx(o);initialize_genuine_v1_storage(*o,original);tx.commit();}
    unsigned hits=0;int restored=SQLITE_MISUSE;
    {DecisionWrite tx(o);auto* handle=detail::canonical_writer_custody_test_access::fault_handle(o->db());
     struct restore{sqlite3* db;int& result;~restore(){result=sqlite3_set_authorizer(db,nullptr,nullptr);}} restore_original{handle,restored};
     ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* one,const char* two,const char*,const char*)noexcept {
         if(action==SQLITE_UPDATE&&one&&two&&std::strcmp(one,"_lattice_recovery_request_config")==0&&std::strcmp(two,"version")==0){++*static_cast<unsigned*>(raw);return SQLITE_DENY;}return SQLITE_OK;
     },&hits),SQLITE_OK);
     EXPECT_THROW(DecisionStoreAccess::initialize(o),db_error);EXPECT_EQ(hits,1u);}
    ASSERT_EQ(restored,SQLITE_OK);DecisionWrite tx(o);EXPECT_EQ(std::get<int64_t>(o->db().query("SELECT version AS n FROM _lattice_recovery_request_config").at(0).at("n")),1);
    EXPECT_EQ(o->db().query("PRAGMA main.table_info(_lattice_recovery_request)").size(),11u);
    EXPECT_TRUE(o->db().query("SELECT 1 FROM sqlite_schema WHERE name='_lattice_recovery_request_v1_migration'").empty());
    DecisionStoreAccess::initialize(o);auto expected=original;expected.receipt_decisions="0";EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),std::optional{expected});tx.commit();
}

#endif
}
#endif


// Appended only: the preceding receipt fixtures and all their original
// transaction, process, source-currentness and installation oracles remain.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__) && !defined(LATTICE_RECEIPT_RETENTION_LEGACY_WRITER)
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <array>
#include <limits>
namespace {
std::string admission_parent_reference(const lattice::detail::recovery_request_row& r) {
    // An independent explicit preimage, rather than the product's incremental
    // helper. Exact eight-byte big-endian lengths and values remain the format.
    std::string bytes;
    const auto number=[&](uint64_t n){for(unsigned i=0;i<8;++i)bytes.push_back(static_cast<char>(n>>(56-8*i)));};
    const auto text=[&](const std::string& value){number(value.size());bytes.append(value);};
    text(r.journal.channel);number(r.journal.incarnation);number(r.journal.generation);
    number(r.barrier);number(r.sequence);number(r.journal_revision);number(r.route);
    text(r.domain);text(r.source_context);text(r.request_frame);text(r.manifest_frame);
    return picosha2::hash256_hex_string(bytes);
}
std::string admission_projection_reference(const lattice::detail::recovery_request_row& r,const std::string& pairs) {
    return "1"+picosha2::hash256_hex_string(admission_parent_reference(r)+pairs)+pairs;
}
std::string admission_fingerprint_reference(const lattice::detail::recovery_request_row& r) {
    return picosha2::hash256_hex_string(r.receipt_decisions.empty()||r.receipt_decisions=="0"?
        "framing:"+admission_parent_reference(r)+r.receipt_decisions:"decisions:"+r.receipt_decisions);
}
size_t admission_charge_reference(const lattice::detail::recovery_request_row& r) {
    return 56+r.journal.channel.size()+r.domain.size()+r.source_context.size()+r.request_frame.size()+r.manifest_frame.size()+
        (r.receipt_decisions=="0"?0:lattice::detail::recovery_request_store::decision_bytes);
}
TEST(AdmittedRequestRow, IndependentBinaryParentAndFingerprintStreamsRemainExact) {
    for(size_t length:{size_t(1),size_t(55),size_t(56),size_t(63),size_t(64),size_t(65),size_t(127),size_t(128),size_t(129)}) {
        SCOPED_TRACE(length);auto r=decision_row();r.source_context.clear();r.request_frame.clear();r.manifest_frame.clear();
        for(size_t i=0;i<length;++i){r.source_context.push_back(static_cast<char>(i%256));r.request_frame.push_back(static_cast<char>((255-i)%256));}
        r.manifest_frame=std::string("\0manifest\xff",10);r.route=std::numeric_limits<int64_t>::max();
        for(const auto& disposition:{std::string{},std::string("0"),std::string("C0"),std::string("N0"),std::string("U0"),std::string("U1")}) {
            auto value=r;value.receipt_decisions=disposition.size()==2?admission_projection_reference(r,disposition):disposition;
            EXPECT_EQ(DecisionStoreAccess::fingerprint(value),admission_fingerprint_reference(value));
        }
    }
    auto maximum=decision_row();maximum.source_context=std::string(lattice::detail::recovery_request_store::context_bytes,'c');
    maximum.request_frame=std::string(lattice::detail::recovery_request_store::frame_bytes,'q');
    maximum.manifest_frame=std::string(lattice::detail::recovery_request_store::frame_bytes,'m');
    maximum.request_frame[63]='\0';maximum.manifest_frame.back()=static_cast<char>(255);
    const auto parent=admission_parent_reference(maximum);
    EXPECT_EQ(DecisionStoreAccess::fingerprint(maximum),picosha2::hash256_hex_string("framing:"+parent));
    maximum.receipt_decisions="1"+picosha2::hash256_hex_string(parent+"C0N0U0U1")+"C0N0U0U1";
    EXPECT_EQ(DecisionStoreAccess::fingerprint(maximum),picosha2::hash256_hex_string("decisions:"+maximum.receipt_decisions));
}
TEST(AdmittedRequestRow, RealOwnedScansMatchStrictReferenceAndTypedLegacyCharge) {
    auto o=decision_owner(":memory:");DecisionWrite tx(o);auto legacy=decision_row();legacy.journal.channel="legacy";
    initialize_genuine_v1_storage(*o,legacy);DecisionStoreAccess::initialize(o);
    auto empty=decision_row();empty.journal.channel="empty";DecisionStoreAccess::insert(o,empty);
    auto decided=decision_row();decided.journal.channel="decided";DecisionStoreAccess::insert(o,decided);
    DecisionStoreAccess::retain(o,decided,"1C0N0U0U1");
    std::map<std::string,std::string> expected;size_t charge=0;
    for(const auto& channel:{std::string("legacy"),std::string("empty"),std::string("decided")}) {
        const auto actual=DecisionStoreAccess::read(o,channel);ASSERT_TRUE(actual);
        if(channel=="legacy")EXPECT_EQ(actual->receipt_decisions,"0");
        else if(channel=="decided")EXPECT_EQ(actual->receipt_decisions,admission_projection_reference(decided,"C0N0U0U1"));
        expected.emplace(channel,admission_fingerprint_reference(*actual));charge+=admission_charge_reference(*actual);
        EXPECT_EQ(DecisionStoreAccess::fingerprint(*actual),expected.at(channel));
    }
    EXPECT_EQ(DecisionStoreAccess::fingerprints(o),expected);EXPECT_EQ(DecisionStoreAccess::charged_bytes(o),charge);
    auto mutable_empty=empty;mutable_empty.request_frame+="changed after successful scan";
    EXPECT_EQ(DecisionStoreAccess::fingerprint(mutable_empty),admission_fingerprint_reference(mutable_empty));
    EXPECT_NE(DecisionStoreAccess::fingerprint(mutable_empty),expected.at("empty"));
    EXPECT_EQ(DecisionStoreAccess::fingerprints(o),expected);tx.commit();
}
TEST(AdmittedRequestRow, CallerMutationAfterSuccessNeverReusesParentAdmission) {
    auto o=decision_owner(":memory:");DecisionWrite tx(o);DecisionStoreAccess::initialize(o);const auto initial=decision_row();
    DecisionStoreAccess::insert(o,initial);DecisionStoreAccess::retain(o,initial,"1U0");const auto original=DecisionStoreAccess::read(o,"channel");ASSERT_TRUE(original);
    const auto before=DecisionStoreAccess::fingerprints(o);EXPECT_EQ(DecisionStoreAccess::fingerprint(*original),before.at("channel"));
    for(unsigned field=0;field<12;++field){SCOPED_TRACE(field);auto mutated=*original;
        switch(field){case 0:mutated.journal.channel+="other";break;case 1:++mutated.journal.incarnation;break;case 2:++mutated.journal.generation;break;
        case 3:++mutated.barrier;break;case 4:++mutated.sequence;break;case 5:++mutated.journal_revision;break;case 6:++mutated.route;break;
        case 7:mutated.domain[0]='e';break;case 8:mutated.source_context+="other";break;case 9:mutated.request_frame+="other";break;
        case 10:mutated.manifest_frame+="other";break;case 11:mutated.receipt_decisions.back()='1';break;}
        EXPECT_THROW(DecisionStoreAccess::fingerprint(mutated),lattice::db_error);
        EXPECT_EQ(DecisionStoreAccess::fingerprints(o),before);EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),original);
    }tx.commit();
}
TEST(AdmittedRequestRow, FreshScansRefuseTypedOrProjectionCorruptionBetweenCalls) {
    const std::vector<std::string> faults{
        "receipt_decisions=NULL","receipt_decisions=CAST('0' AS BLOB)","receipt_format=2.5",
        "request_frame=3","receipt_decisions=CAST('' AS TEXT)","source_context=CAST('changed' AS BLOB)"};
    for(const auto& fault:faults){SCOPED_TRACE(fault);auto o=decision_owner(":memory:");const auto r=decision_row();
        {DecisionWrite tx(o);DecisionStoreAccess::initialize(o);DecisionStoreAccess::insert(o,r);DecisionStoreAccess::retain(o,r,"1C0");tx.commit();}
        std::map<std::string,std::string> before;std::optional<lattice::detail::recovery_request_row> row;
        {DecisionWrite tx(o);before=DecisionStoreAccess::fingerprints(o);row=DecisionStoreAccess::read(o,"channel");
            o->db().execute("UPDATE _lattice_recovery_request SET "+fault);
            EXPECT_THROW(DecisionStoreAccess::read(o,"channel"),lattice::db_error);
            EXPECT_THROW(DecisionStoreAccess::fingerprints(o),lattice::db_error);
            EXPECT_THROW(DecisionStoreAccess::charged_bytes(o),lattice::db_error);}
        DecisionWrite tx(o);EXPECT_EQ(DecisionStoreAccess::fingerprints(o),before);EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),row);
        ASSERT_TRUE(row);EXPECT_EQ(DecisionStoreAccess::charged_bytes(o),admission_charge_reference(*row));tx.commit();
    }
}
TEST(AdmittedRequestRow, IgnoredWriteStillRequiresFreshExactPostimage) {
    auto o=decision_owner(":memory:");const auto r=decision_row();
    {DecisionWrite tx(o);DecisionStoreAccess::initialize(o);DecisionStoreAccess::insert(o,r);tx.commit();}
    unsigned ignored=0;int restored=SQLITE_MISUSE;
    {DecisionWrite tx(o);const auto before=DecisionStoreAccess::fingerprints(o);
        auto* handle=lattice::detail::canonical_writer_custody_test_access::fault_handle(o->db());
        struct restore_authorizer{sqlite3* handle;int& restored;~restore_authorizer(){restored=sqlite3_set_authorizer(handle,nullptr,nullptr);}} restore{handle,restored};
        ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* table,const char* column,const char*,const char*)noexcept {
            if(action==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_recovery_request")==0&&std::strcmp(column,"receipt_decisions")==0){++*static_cast<unsigned*>(raw);return SQLITE_IGNORE;}
            return SQLITE_OK;},&ignored),SQLITE_OK);
        EXPECT_THROW(DecisionStoreAccess::retain(o,r,"1U1"),lattice::db_error);EXPECT_EQ(ignored,1u);
        EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),std::optional{r});EXPECT_EQ(DecisionStoreAccess::fingerprints(o),before);
    }
    EXPECT_EQ(restored,SQLITE_OK);DecisionWrite tx(o);EXPECT_EQ(DecisionStoreAccess::read(o,"channel"),std::optional{r});tx.commit();
}
} // namespace
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST(ReceiptCanonicalIdentity, WireKeyUsesValidatedUuidDomainAndLeavesActualAuditSpellingUntouched) {
    const std::string actual="5f82827d-762b-4e71-A788-648902367e32";
    const std::string key="5f82827d-762b-4e71-a788-648902367e32";
    EXPECT_EQ(lattice::detail::canonical_writer_adapter::uuid_key(actual),key);
    EXPECT_EQ(actual,"5f82827d-762b-4e71-A788-648902367e32");
    EXPECT_EQ(lattice::detail::canonical_writer_adapter::uuid_key(key),key);
    EXPECT_NE(lattice::detail::canonical_writer_adapter::uuid_key("5f82827d-762b-4e71-b788-648902367e32"),key);
    for(const auto* invalid:{"not-a-UUID-identifier","5f82827d-762b-4e71-g788-648902367e32","5f82827dX762b-4e71-A788-648902367e32"})
        EXPECT_THROW(lattice::detail::canonical_writer_adapter::uuid_key(invalid),lattice::db_error);
}
#endif
