// Real v3 source/controller path. The mechanical platform verifier is not hosted TLS qualification.
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_receiver_controller.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "CanonicalWriterTestAccess.hpp"
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
            auto result=peer.setup.ready(frame->raw,charge);if(result.status_code()!=1||!result.publishable())throw db_error("actual source READY result unavailable");++handled;
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
        EXPECT_EQ(frame.at("version"),3);EXPECT_EQ(item.at("original_id"),ids[0]);EXPECT_TRUE(item.contains("operation_digest"));
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
    connect();ASSERT_TRUE(until([&]{return error_contains("controller frozen request binding changed");}));
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
}
#endif
