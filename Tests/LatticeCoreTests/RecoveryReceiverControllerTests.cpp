#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/sync_discovery_deferral.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include "../../Sources/LatticeCore/src/sync_upload_exclusion.hpp"
#include <future>
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_receiver_controller.hpp"
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
        std::function<std::shared_ptr<void>(const char*)> scope={}) {
        auto value=std::make_shared<probe>();value->owner=owner;value->observed=std::move(observed);value->scope=std::move(scope);
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
class RecoveryReceiverController:public ::testing::Test {
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
    json source_policy(const std::string& ns) {
        return {{"version",1},{"authority","controller-service"},{"sourceID",controller_uuid(1)},{"epoch",controller_uuid(2)},
            {"localNamespace","local"},{"namespaces",json::array({{{"namespaceID","local"},{"coverageID","local-v1"},{"revision",1}},
                {{"namespaceID","a"},{"coverageID","a-v1"},{"revision",1}},{{"namespaceID","b"},{"coverageID","b-v1"},{"revision",1}}})},
            {"receiptNamespace",ns},{"models",json::array({"ControllerRow"})},{"walFull",true},{"maximumAuthorizationMilliseconds",600000},
            {"readyProfile","bounded48MiBV1"},{"upload",{{"tables",json::array()},{"unlisted","allow"},{"maximumDeletes",256}}}};
    }
    relay_recovery_setup serve(size_t index) {
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
            auto result=peer.setup.ready(frame->raw,charge);if(result.status_code()!=1||!result.publishable())throw db_error("actual source READY result unavailable");++handled;
            if(drop_prepare&&control["operation"]=="prepare"&&dropped++==0)return true;
            peer.physical.trigger_on_message(transport_message::from_string(result.wire()));
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
    template<class F> bool until(F predicate,int milliseconds=5000) {const auto end=std::chrono::steady_clock::now()+std::chrono::milliseconds(milliseconds);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate())return true;std::this_thread::sleep_for(std::chrono::milliseconds(2));}return predicate();}
    int64_t scalar(lattice_db& owner,const std::string& sql){return std::get<int64_t>(owner.db().query(sql).at(0).at("n"));}
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
        swift_dynamic_object row;row.table_name="ControllerRow";row.properties=controller_schema().properties;row.values["globalId"]=id;row.values["value"]=value;row.values["note"]=std::string("private");dynamic_object object(row);owner.add(object);owner.commit();
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
    configure();insert(*source,controller_uuid(125),"source");connect();
    ASSERT_TRUE(until([&]{return scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1&&phase()==0;}));
    close_receiver();open_receiver();
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
    configure(2);std::atomic<bool> pending{false};const auto cancel_pause=std::make_shared<ControllerPause>();pauses.push_back(cancel_pause);
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[&](const char* stage){if(std::strcmp(stage,"reconciliation-pending")==0)pending=true;},
        [cancel_pause](const char* stage)->std::shared_ptr<void>{if(std::strcmp(stage,"reconcile-cancel")==0)cancel_pause->wait();return {};});
    connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==2;}));
    insert(*receiver,controller_uuid(170),"local-original");
    ASSERT_TRUE(until([&]{return held_uploads.size()>=2;}));
    auto frame=std::find_if(held_uploads.begin(),held_uploads.end(),[&](const auto& value){return value.endpoint.matches(peers[0].physical);});ASSERT_NE(frame,held_uploads.end());
    auto imported=peers[0].setup.receive(frame->raw);ASSERT_EQ(imported.status_code(),1);ASSERT_EQ(imported.take_ids().size(),1u);
    const auto before=receiver->db().query("SELECT * FROM AuditLog ORDER BY id");const auto entries=receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original");
    ASSERT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL AND stage=0"),2);
    ASSERT_TRUE(peers[0].physical.trigger_on_message(transport_message::from_string(server_sent_event::make_audit_log({}).to_json())));
    ASSERT_TRUE(until([&]{return pending.load()&&cancel_pause->ready();}));EXPECT_EQ(phase(),2);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1"),2);
    EXPECT_EQ(receiver->db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_EQ(receiver->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY channel,original"),entries);
    EXPECT_EQ(scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_range_attempt WHERE verified=1"),2);
    EXPECT_THROW(insert(*receiver,controller_uuid(171),"gate must stay closed"),db_error);
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
public:
    void invoke(std::function<void()>&& job)override {std::lock_guard lock(mutex_);if(stopped_)return;
        if(jobs_.size()>=256)throw db_error("first-claim fixture scheduler capacity");jobs_.push_back(std::move(job));}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{std::lock_guard lock(mutex_);return !stopped_;}
    bool run_one(){std::function<void()> job;{std::lock_guard lock(mutex_);if(jobs_.empty())return false;job=std::move(jobs_.front());jobs_.pop_front();}
        struct Restore{const FirstClaimScheduler* old;~Restore(){current_=old;}} restore{current_};current_=this;job();return true;}
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
class RecoveryRestrictedFirstClaim : public RecoveryReceiverController {
protected:
    std::shared_ptr<FirstClaimScheduler> manual=std::make_shared<FirstClaimScheduler>();
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
    size_t restricted_begin=0,phase4_publications=0;bool park_first=false;
    void SetUp()override{RecoveryReceiverController::SetUp();previous_ack=detail::sync_background_test_hooks::ack;
        previous_claim=detail::recovery_export_test_hooks::after_claim_commit;previous_claim_action=std::move(first_claim_committed_action);}
    std::shared_ptr<FirstClaimAckGate> install_ack_gate(){auto gate=std::make_shared<FirstClaimAckGate>();ack_gates.push_back(gate);
        auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();hook->before_expiry=[gate]{gate->wait();};
        hook->completed=[gate]{++gate->finished;};detail::sync_background_test_hooks::ack=std::move(hook);return gate;}
    template<class F> bool drive(F predicate){const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){pump();if(predicate())return true;manual->run_one();if(predicate())return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}return predicate();}
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
            if(std::strcmp(stage,"reconciliation-pending")!=0||phase()!=4)return;
            ++phase4_publications;phase4_snapshot=snapshot();
            first_claim_committed_action=[this]{
                committed_claims.push_back(claims());committed_sequences.push_back(scalar(*receiver,"SELECT export_sequence AS n FROM _lattice_obligation_store"));
                if(committed_claims.size()==1){committed_snapshot=snapshot();if(park_first)writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);}
            };
            detail::recovery_export_test_hooks::after_claim_commit=first_claim_committed;
        });
        connect();
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
    ASSERT_EQ(phase4_publications,1u);ASSERT_EQ(committed_claims.size(),2u);ASSERT_EQ(committed_sequences.size(),2u);
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
    ASSERT_TRUE(drive([&]{return bool(writer_hold);}));ASSERT_EQ(phase4_publications,1u);ASSERT_EQ(committed_claims.size(),1u);
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
thread_local std::function<void()> cohort_after_claim;
void cohort_claimed(){auto action=std::move(cohort_after_claim);detail::recovery_export_test_hooks::after_claim_commit=nullptr;if(action)action();}
class RecoveryReceiverCohort : public RecoveryReceiverController {
protected:
    void retained_replacement(bool recreate) {
        configure();
        struct Observation {std::mutex mutex;std::shared_ptr<CohortWriterHold> writer;Snapshot claimed;bool closing=false;std::atomic<unsigned> captures{0},waiting{0},restricted{0};std::atomic<bool> armed{false},hooked{false};};
        auto observation=std::make_shared<Observation>();
        struct Release {std::shared_ptr<Observation> value;~Release(){std::shared_ptr<CohortWriterHold> held;{std::lock_guard lock(value->mutex);value->closing=true;held=value->writer;}if(held)held->release();}} release{observation};
        auto timeout_pause=std::make_shared<ControllerPause>();pauses.push_back(timeout_pause);
        auto timeout_seen=std::make_shared<std::atomic<bool>>(false);
        // Installed before actual route publication; this is a restriction on
        // the real worker, not a factory for recovery authority.
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this,observation,timeout_pause,timeout_seen](const char* stage){
            if(std::strcmp(stage,"install-committed")==0){
                auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();
                hook->after_timeout_transition=[timeout_pause,timeout_seen]{if(!timeout_seen->exchange(true))timeout_pause->wait();};
                detail::sync_background_test_hooks::ack=std::move(hook);
            }
            if(std::strcmp(stage,"cohort-reserved")==0)++observation->captures;
            if(std::strcmp(stage,"cohort-retained")==0)++observation->waiting;
            if(std::strcmp(stage,"reconciliation-pending")==0&&phase()==4){
                ++observation->restricted;
                if(observation->armed.load()&&!observation->hooked.exchange(true)){
                    cohort_after_claim=[this,observation]{
                        std::lock_guard lock(observation->mutex);if(observation->closing)return;
                        observation->claimed=snapshot();observation->writer=std::make_shared<CohortWriterHold>(*receiver);
                    };
                    detail::recovery_export_test_hooks::after_claim_commit=cohort_claimed;
                }
            }
        });
        connect();ASSERT_TRUE(until([&]{return phase()==0&&scalar(*receiver,"SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1")==1;}));
        detail::recovery_receiver_cohort_test_access::next_ack_timeout(*synchronizers[0]);
        observation->armed.store(true);seed_local(1,600);const auto ids=originals();ASSERT_EQ(ids.size(),1u);
        ASSERT_TRUE(until([&]{return !held_originals().empty()&&timeout_pause->ready();}));
        ASSERT_EQ(synchronizers[0]->get_progress().pending_upload,0);
        request_recovery();timeout_pause->release();
        std::shared_ptr<detail::sync_discovery_operation> retained;
        ASSERT_TRUE(until([&]{retained=detail::recovery_receiver_cohort_test_access::retained_busy(*synchronizers[0]);return bool(retained);}));
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
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[first_pause,started,completed](const char* stage){
        if(std::strcmp(stage,"install-committed")!=0)return;
        auto hook=std::make_shared<detail::sync_background_test_hooks::ack_schedule>();
        hook->before_expiry=[first_pause,started]{if(started->fetch_add(1)==0)first_pause->wait();};
        hook->completed=[completed]{++*completed;};detail::sync_background_test_hooks::ack=std::move(hook);
    });
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
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[captured](const char* stage){
            if(std::strcmp(stage,"install-committed")==0)detail::sync_background_test_hooks::ack=captured->ordinary->hooks();
            if(std::strcmp(stage,"reconciliation-pending")==0){++captured->pending;detail::sync_background_test_hooks::ack=captured->restricted->hooks();}
            if(std::strcmp(stage,"reconcile-refreeze-committed")==0)++captured->refrozen;
            if(std::strcmp(stage,"delivery-retry-admitted")==0){++captured->admitted;if(captured->admitted_gate)captured->admitted_gate->wait();}
            if(captured->observed)captured->observed(stage);
        },[captured](const char* stage)->std::shared_ptr<void>{return captured->scope?captured->scope(stage):nullptr;});
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
    bool error_contains(const std::string& text){std::lock_guard lock(errors_mutex);return std::any_of(errors.begin(),errors.end(),[&](const auto& e){return e.find(text)!=std::string::npos;});}
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
        probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[captured](const char* stage){
            if(std::strcmp(stage,"install-committed")==0)detail::sync_background_test_hooks::ack=captured->ordinary->hooks();
            if(std::strcmp(stage,"reconciliation-pending")==0){++captured->pending;detail::sync_background_test_hooks::ack=captured->restricted->hooks();}
            if(std::strcmp(stage,"delivery-retry-admitted")==0)++captured->admitted;
        });
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
    probe=std::make_unique<detail::recovery_receiver_controller_test_access>(receiver.get(),[this,captured,captures,waiting,phase4,park](const char* stage){
        if(std::strcmp(stage,"install-committed")==0)detail::sync_background_test_hooks::ack=captured->ordinary->hooks();
        if(std::strcmp(stage,"cohort-reserved")==0)++*captures;
        if(std::strcmp(stage,"cohort-retained")==0)++*waiting;
        if(std::strcmp(stage,"delivery-retry-admitted")==0)++captured->admitted;
        if(std::strcmp(stage,"reconciliation-pending")==0){
            detail::sync_background_test_hooks::ack=captured->restricted->hooks();
            if(phase()==4){++*phase4;
                first_claim_committed_action=[this,park]{if(park->exchange(false)){committed_snapshot=snapshot();writer_hold=std::make_unique<FirstClaimWriterHold>(*receiver);}};
                detail::recovery_export_test_hooks::after_claim_commit=first_claim_committed;
            }
        }
    });
    connect();ASSERT_TRUE(drive([&]{return phase()==0;}));
    auto configured=std::make_shared<std::atomic<bool>>(false);
    manual->invoke([this,configured]{detail::recovery_receiver_cohort_test_access::next_ack_timeout(*synchronizers[0]);configured->store(true);});
    ASSERT_TRUE(drive([&]{return configured->load();}));seed_local(1,830);ids=originals();ASSERT_EQ(ids.size(),1u);
    ASSERT_TRUE(drive([&]{return held_originals()==ids&&events->ordinary->started.load()==1;}));legacy_ack(0,ids);
    std::shared_ptr<detail::sync_discovery_operation> retained;
    ASSERT_TRUE(drive([&]{retained=detail::recovery_receiver_cohort_test_access::retained_busy(*synchronizers[0]);return bool(retained);}));
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
