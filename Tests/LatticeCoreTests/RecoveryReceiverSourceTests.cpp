#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_receiver_source.hpp"
#include <nlohmann/json.hpp>

#ifndef __EMSCRIPTEN__ // actual retained native owner; WASM borrows an owner

namespace lattice::detail {
// Observation only: no constructor, TLS setter, source grant or install admission.
struct receiver_source_test_access {
    static bool described(const std::shared_ptr<receiver_source_binding>& source){return source&&source->described();}
};
}
namespace {
using json=nlohmann::json;
constexpr auto endpoint="wss://registered.example/sync?tenant=shared";
constexpr auto uuid="10000000-0000-4000-8000-000000000001";
json expectation(int64_t duration=3600000) {
    const std::string hash(64,'a');
    return {{"endpoint",endpoint},{"source",{{"authority","registered-source"},{"sourceID",uuid},{"epoch",uuid},
        {"scopeDigest",hash},{"schemaDigest",hash},{"receiptNamespace","shared-peers"},{"coverageID","coverage"},
        {"coverageRevision",1},{"descriptorDigest",hash}}},
        {"incomingScope",{{"models",json::array({{{"table","TestPerson"},{"incomingOperations",json::array({"INSERT","UPDATE","DELETE"})}}})},
            {"relations",json::array()},{"scopedLinkTables",json::array()},{"catalogDigest",hash}}},
        {"peer",{{"replicaID","registered/replica"},{"receiverIncarnation",uuid},{"channelIncarnation",uuid}}},
        {"channel","shared"},{"validForMilliseconds",duration}};
}
json described_profile() {
    json p={{"name","bounded-v1"},{"wire",json::object()},{"valueLimits",json::object()}};
    for(const auto* key:{"frame_bytes","payload_bytes","items_per_page","content_pages","content_identities","content_bytes","receipt_pages","receipts","receipt_bytes"})p["wire"][key]="1";
    for(const auto* key:{"requestEntries","requestTargets","requestTargetBytes","parserDepth","parserNodes","scalarBytes","restartBytes","leaseMilliseconds","packageBytes","frames","transfers","bindings","durableBytes","transferBytes","captureRows","captureBytes","requestBytes","pendingRequests","pendingInputAndReplyBytes","pendingWorkspaceBytes"})p[key]=1;
    for(const auto* key:{"rawBytes","fields","nameBytes","valueBytes","decodedBytes"})p["valueLimits"][key]=1;
    return p; // Structural test facts, deliberately NOT a serving profile proof.
}
class ReceiverSync final : public lattice::synchronizer {
public:
    using synchronizer::synchronizer;
    auto observed_source()const{return receiver_source_;}
    void close_actual_owner(){db().close();}
    lattice::lattice_db& actual_owner(){return db();}
};
struct Probe {
    mutable std::mutex mutex;std::vector<lattice::platform_transport_callbacks> endpoints;std::vector<std::string> urls,describes;
    bool verified=true;int destroyed=0,released=0;
    ~Probe(){endpoints.clear();}
    std::unique_ptr<lattice::sync_transport> transport(bool sdk_boundary) {
        const auto connect=[](void* p,const void* url,const void*,const void* callbacks){auto& s=*static_cast<Probe*>(p);std::lock_guard lock(s.mutex);s.urls.push_back(*static_cast<const std::string*>(url));s.endpoints.push_back(*static_cast<const lattice::platform_transport_callbacks*>(callbacks));};
        const auto disconnect=[](void*){};
        const auto send=[](void* p,const void* message,const void*){const auto value=json::parse(static_cast<const lattice::transport_message*>(message)->as_string());if(value.contains("kind")&&value.at("kind")=="recoveryReady"){auto& s=*static_cast<Probe*>(p);std::lock_guard lock(s.mutex);s.describes.push_back(value.dump());}};
        const auto destroy=[](void* p){++static_cast<Probe*>(p)->destroyed;};
        if(!sdk_boundary)return std::unique_ptr<lattice::sync_transport>(lattice::make_owned_platform_sync_transport(this,connect,disconnect,send,destroy));
        // Mechanical native integration only. This is not real TLS evidence;
        // actual URLSession/NIOSSL qualification is an independent hosted gate.
        return std::unique_ptr<lattice::sync_transport>(lattice::make_system_tls_platform_sync_transport(this,connect,disconnect,send,destroy,this,
            [](void* p,const void*,const void*)->int32_t{return static_cast<Probe*>(p)->verified?1:0;},
            [](void* p){++static_cast<Probe*>(p)->released;}));
    }
    auto attempt(size_t i){std::lock_guard lock(mutex);return endpoints.at(i);}
    std::string request(size_t i){std::lock_guard lock(mutex);return describes.at(i);}
    size_t requests()const{std::lock_guard lock(mutex);return describes.size();}
};
struct Fixture {
    TempDB file{"receiver_source"};Probe probe;json policy;std::unique_ptr<ReceiverSync> sync;std::atomic<int> errors{0};
    explicit Fixture(bool sdk=true,bool verified=true,int64_t duration=3600000):policy(expectation(duration)) {
        probe.verified=verified;auto owner=std::make_unique<lattice::lattice_db>(lattice::configuration(file.str()));
        lattice::sync_config config;config.websocket_url=endpoint;config.sync_id="receiver-source";config.all_active_sync_ids={config.sync_id};
        config.recovery_source_expectation=policy.dump();config.max_reconnect_attempts=1;config.base_delay_seconds=0;config.max_delay_seconds=0;
        config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
        sync=std::make_unique<ReceiverSync>(std::move(owner),config,probe.transport(sdk));
        sync->set_on_error([this](const std::string&){++errors;});sync->connect();
    }
    ~Fixture(){sync.reset();}
    void open(size_t i=0){ASSERT_TRUE(probe.attempt(i).trigger_on_open());}
    json response(size_t i=0){auto result=json::parse(probe.request(i));result["routeGeneration"]="1";
        for(const auto* key:{"source","incomingScope","peer","channel"})result[key]=policy.at(key);result["profile"]=described_profile();result["upload"]={{"maximumEntries",256},{"maximumWireBytes",1048576},{"maximumScalarBytes",65536},{"parserNodes",32768},{"parserDepth",16},{"maximumDeletes",0}};return result;}
    bool described(){return lattice::detail::receiver_source_test_access::described(sync->observed_source());}
    void receive(const json& value,size_t i=0){probe.attempt(i).trigger_on_message(lattice::transport_message::from_string(value.dump()));}
};
}
TEST(RecoveryReceiverSource, ActualOwnerDescribeBindsOnlySourceFacts) {
    Fixture f;f.open();ASSERT_EQ(f.probe.requests(),1u);
    {std::lock_guard lock(f.probe.mutex);EXPECT_NE(f.probe.urls.front().find("recovery-replica=registered%2Freplica"),std::string::npos);}
    EXPECT_FALSE(f.described());f.receive(f.response());EXPECT_TRUE(f.described());
    EXPECT_FALSE(lattice::detail::receiver_source_binding::ready_authority);
    EXPECT_FALSE(lattice::detail::receiver_source_binding::install_authority);
    EXPECT_FALSE(lattice::detail::receiver_source_binding::automatic_recovery);
}
TEST(RecoveryReceiverSource, GenericOwnedPlatformCannotMintVerifiedSource) {
    Fixture f(false);f.open();EXPECT_EQ(f.probe.requests(),0u);EXPECT_FALSE(f.described());
}
TEST(RecoveryReceiverSource, FailedTrustedAdapterVerificationCannotDescribe) {
    Fixture f(true,false);f.open();EXPECT_EQ(f.probe.requests(),0u);EXPECT_FALSE(f.described());
}
TEST(RecoveryReceiverSource, ChangedRegistrationSourceAndCatalogRefuse) {
    for(const auto* field:{"peer","source","incomingScope","channel"}) {
        Fixture f;f.open();auto reply=f.response();reply[field]=json::object();f.receive(reply);EXPECT_FALSE(f.described());
        f.receive(f.response());EXPECT_FALSE(f.described()); // failed attempt is not revived by later correct bytes
    }
}
TEST(RecoveryReceiverSource, CorrelationAndPhysicalGenerationAreStrict) {
    for(int mode=0;mode<4;++mode){Fixture f;f.open();auto reply=f.response();
        if(mode==0)reply["requestID"]="20000000-0000-4000-8000-000000000002";
        if(mode==1)reply["routeGeneration"]="01";if(mode==2)reply["routeGeneration"]="0";if(mode==3)reply["version"]=2;
        f.receive(reply);EXPECT_FALSE(f.described());}
}
TEST(RecoveryReceiverSource, RepeatedDescribeAndUnsolicitedReadyRevokeFacts) {
    Fixture f;f.open();auto reply=f.response();f.receive(reply);ASSERT_TRUE(f.described());f.receive(reply);EXPECT_FALSE(f.described());
    Fixture other;other.open();reply=other.response();reply["operation"]="prepare";other.receive(reply);EXPECT_FALSE(other.described());
}
TEST(RecoveryReceiverSource, ReplacementAndStopFenceRetainedBinding) {
    Fixture f;f.open();const auto old=f.probe.attempt(0);const auto old_reply=f.response();f.receive(old_reply);ASSERT_TRUE(f.described());
    auto held=f.sync->observed_source();f.sync->connect();EXPECT_FALSE(lattice::detail::receiver_source_test_access::described(held));f.open(1);
    EXPECT_FALSE(old.trigger_on_message(lattice::transport_message::from_string(old_reply.dump())));
    f.receive(f.response(1),1);ASSERT_TRUE(f.described());f.sync->disconnect();EXPECT_FALSE(f.described());
    f.sync.reset();EXPECT_FALSE(lattice::detail::receiver_source_test_access::described(held));
}
TEST(RecoveryReceiverSource, PhysicalCloseImmediatelyFencesDescribe) {
    Fixture f;f.open();f.receive(f.response());ASSERT_TRUE(f.described());
    const auto old=f.probe.attempt(0);ASSERT_TRUE(old.trigger_on_close(1000,"closed"));EXPECT_FALSE(f.described());
    EXPECT_FALSE(old.trigger_on_close(1000,"duplicate"));
}
TEST(RecoveryReceiverSource, FiniteDeadlineDoesNotRenewFromFrames) {
    Fixture f(true,true,1);f.open();std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if(f.probe.requests())f.receive(f.response());EXPECT_FALSE(f.described());
}
TEST(RecoveryReceiverSource, ReservedEnvelopesCannotEnterLegacyApplyOrAck) {
    for(const auto& marker:{json{{"kind","recoveryReady"}},json{{"latticeCanonicalRange",json::object()}}}) {
        auto value=marker;value["auditLog"]=json::array();value["ack"]=json::array({uuid});
        EXPECT_FALSE(lattice::server_sent_event::from_json(value.dump()).has_value());
    }
    Fixture f;f.open();auto reply=f.response();const auto raw=reply.dump();
    const auto duplicate="{\"kind\":\"recoveryReady\","+raw.substr(1);
    f.probe.attempt(0).trigger_on_message(lattice::transport_message::from_string(duplicate));EXPECT_FALSE(f.described());
    f.receive(reply);EXPECT_FALSE(f.described());
}
TEST(RecoveryReceiverSource, OversizedControlAndProfileBoundsCannotAllocateAuthority) {
    Fixture f;f.open();auto value=f.response();value["padding"]=std::string(65536,'x');f.receive(value);EXPECT_FALSE(f.described());
    Fixture g;g.open();value=g.response();value["profile"]["requestBytes"]=0;g.receive(value);EXPECT_FALSE(g.described());
}
TEST(RecoveryReceiverSource, NativeProviderReleasedOnlyAfterLastEndpoint) {
    Probe probe;auto transport=probe.transport(true);transport->connect(endpoint);auto held=probe.attempt(0);transport.reset();
    EXPECT_EQ(probe.destroyed,1);EXPECT_EQ(probe.released,0);EXPECT_FALSE(held.trigger_on_open());
    {std::lock_guard lock(probe.mutex);probe.endpoints.clear();}held={};EXPECT_EQ(probe.released,1);
}

TEST(RecoveryReceiverSource, InvalidAppExpectationRefusesWithCompleteOwnerTeardown) {
    for(int mode=0;mode<3;++mode) {
        TempDB file{"invalid_receiver_source"};Probe probe;
        auto owner=std::make_unique<lattice::lattice_db>(lattice::configuration(file.str()));
        auto value=expectation();if(mode==0)value["validForMilliseconds"]=0;
        if(mode==1)value["endpoint"]="wss://other.example/sync";
        if(mode==2)value["source"]["descriptorDigest"]="invalid";
        lattice::sync_config config;config.websocket_url=endpoint;config.sync_id="invalid-receiver";
        config.recovery_source_expectation=value.dump();
        EXPECT_THROW((void)std::make_unique<ReceiverSync>(std::move(owner),config,probe.transport(true)),std::exception);
        EXPECT_EQ(probe.destroyed,1);
        EXPECT_EQ(probe.released,1);
        EXPECT_EQ(probe.requests(),0u);
    }
}
TEST(RecoveryReceiverSource, NativeRetirementDuringVerificationRetainsContextAndRejectsOpen) {
    struct Context {
        std::mutex mutex;std::condition_variable ready;bool entered=false,release=false,timed_out=false;
        int destroyed=0,released=0;lattice::platform_transport_callbacks endpoint;
    } context;
    auto transport=std::unique_ptr<lattice::sync_transport>(lattice::make_system_tls_platform_sync_transport(&context,
        [](void* p,const void*,const void*,const void* endpoint){static_cast<Context*>(p)->endpoint=*static_cast<const lattice::platform_transport_callbacks*>(endpoint);},
        [](void*){},[](void*,const void*,const void*){},[](void* p){++static_cast<Context*>(p)->destroyed;},&context,
        [](void* p,const void*,const void*)->int32_t {auto& c=*static_cast<Context*>(p);std::unique_lock lock(c.mutex);c.entered=true;c.ready.notify_all();
            if(!c.ready.wait_for(lock,std::chrono::seconds(5),[&]{return c.release;}))c.timed_out=true;return 1;},
        [](void* p){++static_cast<Context*>(p)->released;}));
    ASSERT_NE(transport,nullptr);
    transport->connect(endpoint);
    bool admitted=true;std::thread callback([&]{admitted=context.endpoint.trigger_on_open();});
    bool entered=false;
    {std::unique_lock lock(context.mutex);entered=context.ready.wait_for(lock,std::chrono::seconds(5),[&]{return context.entered;});}
    transport.reset();
    EXPECT_EQ(context.destroyed,1);
    EXPECT_EQ(context.released,0);
    {std::lock_guard lock(context.mutex);context.release=true;}context.ready.notify_all();callback.join();
    EXPECT_TRUE(entered);
    EXPECT_FALSE(context.timed_out);
    EXPECT_FALSE(admitted);
    context.endpoint={};
    EXPECT_EQ(context.released,1);
}

TEST(RecoveryReceiverSource, ClosedActualOwnerCannotRetainSourceFacts) {
    Fixture f;f.open();f.receive(f.response());ASSERT_TRUE(f.described());
    f.sync->close_actual_owner();EXPECT_FALSE(f.described());
}

TEST(RecoveryReceiverSource, OrdinaryOptedRouteAuditAndAckStillReachActualOwner) {
    Fixture f;f.open();f.receive(f.response());ASSERT_TRUE(f.described());
    lattice::lattice_db source{lattice::configuration(":memory:")};
    source.add(TestPerson{"remote-through-described-source",42,std::nullopt});
    const auto originals=lattice::query_audit_log(source.db());
    ASSERT_EQ(originals.size(),1u);
    const auto frame=lattice::server_sent_event::make_audit_log(originals).to_json();
    ASSERT_EQ(json::parse(frame).at("kind"),"auditLog");
    ASSERT_TRUE(f.probe.attempt(0).trigger_on_message(lattice::transport_message::from_string(frame)));
    const auto remote=f.sync->actual_owner().find_by_global_id<TestPerson>(originals.front().global_row_id);
    ASSERT_TRUE(remote.has_value());
    EXPECT_EQ(std::string(remote->name),"remote-through-described-source");
    EXPECT_TRUE(f.described());
    // The positive per-channel marker remains while another actual live slot
    // has not acknowledged this original. A lone channel instead collapses it.
    lattice::register_replication_slot(f.sync->actual_owner().db(),"receiver-other-unacked");
    const auto slots=f.sync->actual_owner().db().query("SELECT sync_id FROM _lattice_replication_slots ORDER BY sync_id");
    ASSERT_EQ(slots.size(),2u);
    EXPECT_EQ(std::get<std::string>(slots[0].at("sync_id")),"receiver-other-unacked");
    EXPECT_EQ(std::get<std::string>(slots[1].at("sync_id")),"receiver-source");
    f.sync->actual_owner().add(TestPerson{"local-to-ack",43,std::nullopt});
    const auto pending=lattice::query_audit_log(f.sync->actual_owner().db());
    const auto local=std::find_if(pending.begin(),pending.end(),[](const auto& entry){return !entry.is_from_remote;});
    ASSERT_NE(local,pending.end());
    const auto ack=lattice::server_sent_event::make_ack({local->global_id}).to_json();
    ASSERT_EQ(json::parse(ack).at("kind"),"ack");
    ASSERT_TRUE(f.probe.attempt(0).trigger_on_message(lattice::transport_message::from_string(ack)));
    const auto state=f.sync->actual_owner().db().query("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id=? AND audit_entry_id=?",{std::string("receiver-source"),local->id});
    ASSERT_EQ(state.size(),1u);
    EXPECT_EQ(std::get<int64_t>(state.front().at("is_synchronized")),1);
    EXPECT_TRUE(f.sync->actual_owner().db().query("SELECT 1 FROM _lattice_sync_state WHERE sync_id=? AND audit_entry_id=? AND is_synchronized=1",{std::string("receiver-other-unacked"),local->id}).empty());
    const auto original=f.sync->actual_owner().db().query("SELECT isSynchronized FROM AuditLog WHERE id=?",{local->id});
    ASSERT_EQ(original.size(),1u);EXPECT_EQ(std::get<int64_t>(original[0].at("isSynchronized")),0);
    EXPECT_TRUE(f.described());
    EXPECT_EQ(f.errors.load(),0);
}
TEST(RecoveryReceiverSource, OrdinaryKindsAndMixedDuplicateControlAreDiscriminatedByValue) {
    for(const auto& event:{lattice::server_sent_event::make_audit_log({}),lattice::server_sent_event::make_ack({uuid}),lattice::server_sent_event::make_replay_request()}) {
        const auto raw=event.to_json();EXPECT_FALSE(lattice::detail::reserved_recovery_source_frame(raw));
        EXPECT_TRUE(lattice::server_sent_event::from_json(raw).has_value());
    }
    for(const auto& raw:{std::string(R"({"kind":"recoveryReady","kind":"ack","ack":[]})"),
                        std::string(R"({"kind":"ack","ack":[],"kind":"recoveryReady"})"),
                        std::string(R"({"kind":"auditLog","auditLog":[],"latticeCanonicalRange":{}})"),
                        std::string(R"({"k\u0069nd":"recovery\u0052eady","ack":[]})")}) {
        EXPECT_TRUE(lattice::detail::reserved_recovery_source_frame(raw));
        EXPECT_FALSE(lattice::server_sent_event::from_json(raw).has_value());
    }
}

namespace {
// Exercise actual owner construction/publication, without opening a socket or
// granting TLS/source authority. The global factory is restored on every exit.
class PolicyQuietTransport final:public lattice::mock_sync_transport {
public:
    void connect(const std::string&,const std::map<std::string,std::string>& = {})override{}
};
class PolicyConstructionFactory final:public lattice::network_factory {
    std::mutex mutex_;std::condition_variable changed_;
    bool hold_=false,entered_=false,released_=false,fail_=false,timed_out_=false;
public:
    std::atomic<unsigned> calls{0};
    void arm(bool fail=false){std::lock_guard lock(mutex_);hold_=true;entered_=released_=timed_out_=false;fail_=fail;}
    bool wait(){std::unique_lock lock(mutex_);return changed_.wait_for(lock,std::chrono::seconds(5),[&]{return entered_;});}
    void release(){std::lock_guard lock(mutex_);released_=true;changed_.notify_all();}
    bool timed_out(){std::lock_guard lock(mutex_);return timed_out_;}
    std::unique_ptr<lattice::http_client> create_http_client()override{return std::make_unique<lattice::null_http_client>();}
    std::unique_ptr<lattice::sync_transport> create_sync_transport()override {
        ++calls;bool fail=false;
        {
            std::unique_lock lock(mutex_);
            if(hold_) {
                hold_=false;entered_=true;changed_.notify_all();
                if(!changed_.wait_for(lock,std::chrono::seconds(5),[&]{return released_;}))timed_out_=true;
                fail=fail_||timed_out_;
            }
        }
        if(fail)throw std::runtime_error("bounded injected transport construction failure");
        return std::make_unique<PolicyQuietTransport>();
    }
};
struct PolicyFactoryScope {
    std::shared_ptr<lattice::network_factory> previous=lattice::get_network_factory();
    std::shared_ptr<PolicyConstructionFactory> factory=std::make_shared<PolicyConstructionFactory>();
    PolicyFactoryScope(){lattice::set_network_factory(factory);}
    ~PolicyFactoryScope(){lattice::set_network_factory(std::move(previous));}
};
lattice::configuration owner_policy(const std::string& path,bool different=false) {
    lattice::configuration config(path);config.websocket_url=endpoint;config.authorization_token="actual-owner-fixture";
    auto policy=expectation();if(different)policy["source"]["authority"]="different-registration";
    config.recovery_source_expectation=policy.dump();return config;
}
}

TEST(RecoveryReceiverSource, PendingActualConstructorRefusesDifferentPolicyBeforeFactory) {
    TempDB file{"receiver_pending_policy"};PolicyFactoryScope installed;installed.factory->arm();
    std::unique_ptr<lattice::lattice_db> first;std::exception_ptr error;
    std::thread constructor([&]{try{first=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));}catch(...){error=std::current_exception();}});
    const bool entered=installed.factory->wait();bool refused=false;std::exception_ptr unexpected;
    if(entered)try{auto wrong=std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true));}
        catch(const lattice::db_error& e){refused=std::string(e.what()).find("different recovery source expectations")!=std::string::npos;}
        catch(...){unexpected=std::current_exception();}
    installed.factory->release();constructor.join();
    ASSERT_TRUE(entered);ASSERT_FALSE(error);ASSERT_FALSE(unexpected);ASSERT_NE(first,nullptr);EXPECT_FALSE(installed.factory->timed_out());
    EXPECT_TRUE(refused);EXPECT_EQ(installed.factory->calls.load(),1u);
    first->close();
    auto replacement=std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true));
    EXPECT_TRUE(replacement->is_sync_agent());EXPECT_EQ(installed.factory->calls.load(),2u);replacement->close();
}

TEST(RecoveryReceiverSource, IdenticalPendingAndDormantOwnersRetainPolicyThroughHandoff) {
    TempDB file{"receiver_identical_policy"};PolicyFactoryScope installed;installed.factory->arm();
    std::unique_ptr<lattice::lattice_db> first,second;std::exception_ptr error,second_error;
    std::thread constructor([&]{try{first=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));}catch(...){error=std::current_exception();}});
    const bool entered=installed.factory->wait();
    if(entered)try{second=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));}catch(...){second_error=std::current_exception();}
    installed.factory->release();constructor.join();
    ASSERT_TRUE(entered);ASSERT_FALSE(error);ASSERT_FALSE(second_error);ASSERT_NE(first,nullptr);ASSERT_NE(second,nullptr);
    EXPECT_FALSE(installed.factory->timed_out());EXPECT_EQ(installed.factory->calls.load(),1u);EXPECT_FALSE(second->is_sync_agent());
    first->close();EXPECT_TRUE(second->is_sync_agent());EXPECT_EQ(installed.factory->calls.load(),2u);
    EXPECT_THROW((void)std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true)),lattice::db_error);
    second->disconnect_sync();
    EXPECT_THROW((void)std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true)),lattice::db_error);
    second->close();auto replacement=std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true));
    EXPECT_TRUE(replacement->is_sync_agent());replacement->close();
}

TEST(RecoveryReceiverSource, DormantReservationSurvivesFailedFirstConstructionAndRetry) {
    // The known base C7 partial-synchronizer destructor defect must be fixed by
    // the separately reviewed composition before this authored oracle can run.
    TempDB file{"receiver_failed_policy"};PolicyFactoryScope installed;installed.factory->arm(true);
    std::unique_ptr<lattice::lattice_db> first,second;std::exception_ptr error,second_error;
    std::thread constructor([&]{try{first=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));}catch(...){error=std::current_exception();}});
    const bool entered=installed.factory->wait();
    if(entered)try{second=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));}catch(...){second_error=std::current_exception();}
    installed.factory->release();constructor.join();
    ASSERT_TRUE(entered);ASSERT_TRUE(error);ASSERT_FALSE(second_error);ASSERT_NE(second,nullptr);EXPECT_EQ(first,nullptr);
    EXPECT_FALSE(installed.factory->timed_out());EXPECT_FALSE(second->is_sync_agent());
    EXPECT_THROW((void)std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true)),lattice::db_error);
    second->connect_sync();EXPECT_TRUE(second->is_sync_agent());EXPECT_EQ(installed.factory->calls.load(),2u);
    second->close();auto replacement=std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true));
    EXPECT_TRUE(replacement->is_sync_agent());replacement->close();
}

TEST(RecoveryReceiverSource, DestroyedDormantOwnerDoesNotLeakPolicyReservation) {
    TempDB file{"receiver_destroyed_policy"};PolicyFactoryScope installed;
    auto first=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));
    {
        auto dormant=std::make_unique<lattice::lattice_db>(owner_policy(file.str()));EXPECT_FALSE(dormant->is_sync_agent());
        EXPECT_THROW((void)std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true)),lattice::db_error);
    }
    first.reset();auto replacement=std::make_unique<lattice::lattice_db>(owner_policy(file.str(),true));
    EXPECT_TRUE(replacement->is_sync_agent());EXPECT_EQ(installed.factory->calls.load(),2u);replacement->close();
}
#endif

#ifndef __EMSCRIPTEN__
TEST(RecoveryReceiverSource, OrdinaryOptedSingleChannelAckCollapsesExactAuditPostimage) {
    Fixture f;f.open();f.receive(f.response());ASSERT_TRUE(f.described());
    auto& owner=f.sync->actual_owner();
    const auto slots=owner.db().query("SELECT sync_id FROM _lattice_replication_slots ORDER BY sync_id");
    ASSERT_EQ(slots.size(),1u);EXPECT_EQ(std::get<std::string>(slots[0].at("sync_id")),"receiver-source");
    owner.add(TestPerson{"single-channel-to-ack",44,std::nullopt});
    const auto audit=owner.db().query("SELECT * FROM AuditLog ORDER BY id");ASSERT_EQ(audit.size(),1u);
    ASSERT_EQ(std::get<int64_t>(audit[0].at("isSynchronized")),0);
    ASSERT_EQ(std::get<int64_t>(audit[0].at("isFromRemote")),0);
    const auto models=owner.db().query("SELECT * FROM TestPerson ORDER BY id");
    const auto id=std::get<int64_t>(audit[0].at("id"));const auto original=std::get<std::string>(audit[0].at("globalId"));
    const auto ack=lattice::server_sent_event::make_ack({original}).to_json();ASSERT_EQ(json::parse(ack).at("kind"),"ack");
    ASSERT_TRUE(f.probe.attempt(0).trigger_on_message(lattice::transport_message::from_string(ack)));
    auto expected=audit;expected[0]["isSynchronized"]=int64_t{1};
    EXPECT_EQ(owner.db().query("SELECT * FROM AuditLog ORDER BY id"),expected);
    EXPECT_TRUE(owner.db().query("SELECT * FROM _lattice_sync_state WHERE audit_entry_id=?",{id}).empty());
    EXPECT_EQ(owner.db().query("SELECT * FROM TestPerson ORDER BY id"),models);
    EXPECT_EQ(owner.db().query("SELECT sync_id FROM _lattice_replication_slots ORDER BY sync_id"),slots);
    EXPECT_TRUE(f.described());EXPECT_EQ(f.errors.load(),0);
}
#endif
