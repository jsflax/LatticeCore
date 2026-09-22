#include "TestHelpers.hpp"
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
        auto& peer=peers.at(index);auto config=json{{"mount",controller_uuid(3)},{"connection",uuid_t::generate().to_string()},
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
            if(hold_uploads){if(held_uploads.size()>=32)throw db_error("fixture held upload bound");held_uploads.push_back(std::move(*frame));return true;}
            auto result=peer.setup.receive(frame->raw);if(result.status_code()!=1)throw db_error("actual source upload refused");
            auto ids=result.take_ids();peer.physical.trigger_on_message(transport_message::from_string(server_sent_event::make_ack(ids).to_json()));
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
#endif
