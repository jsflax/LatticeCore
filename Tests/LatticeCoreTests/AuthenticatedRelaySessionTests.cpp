#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <nlohmann/json.hpp>
#include <atomic>
#include <cstdio>
#include <chrono>
#include <thread>

#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using json=nlohmann::json;
std::string relay_uuid(unsigned n){char out[37];std::snprintf(out,sizeof(out),"00000000-0000-4000-8000-%012u",n);return out;}
struct RelayRouteState {std::atomic<bool> current{true};std::atomic<int> destroyed{0};};
struct RelayRoute {
    std::shared_ptr<RelayRouteState> state;
    static int32_t current(void* raw){return static_cast<RelayRoute*>(raw)->state->current.load()?1:0;}
    static void destroy(void* raw){auto* route=static_cast<RelayRoute*>(raw);++route->state->destroyed;delete route;}
};
swift_schema_entry relay_schema() {
    swift_schema_entry schema;schema.table_name="AuthenticatedRelayRow";
    property_descriptor text{};text.name="text";text.type=column_type::text;schema.properties[text.name]=text;return schema;
}
class AuthenticatedRelaySession:public ::testing::Test {
protected:
    TempDB file{"authenticated-relay"};
    std::unique_ptr<swift_lattice_ref> ref;
    std::shared_ptr<lattice::swift_lattice> owner;
    std::shared_ptr<RelayRouteState> route=std::make_shared<RelayRouteState>();
    relay_recovery_setup setup;
    json policy() {
        return {{"version",1},{"authority","registered-test-service"},{"sourceID",relay_uuid(1)},{"epoch",relay_uuid(2)},
            {"localNamespace","local"},{"namespaces",json::array({{{"namespaceID","local"},{"coverageID","local-v1"},{"revision",1}},
                {{"namespaceID","app"},{"coverageID","app-v1"},{"revision",1}},{{"namespaceID","other"},{"coverageID","other-v1"},{"revision",1}}})},
            {"receiptNamespace","app"},{"models",json::array({"AuthenticatedRelayRow"})},{"walFull",true},
            {"maximumAuthorizationMilliseconds",10000},{"upload",{{"tables",json::array()},{"unlisted","allow"},{"maximumDeletes",256}}}};
    }
    json connection(unsigned replica=1) {
        return {{"mount",relay_uuid(3)},{"connection",relay_uuid(100+replica)},{"channel","shared-channel"},{"authenticatedUserID",relay_uuid(4)},
            {"peer",{{"replicaID","registered-replica-"+std::to_string(replica)},{"receiverIncarnation",relay_uuid(200+replica)},
                {"channelIncarnation",relay_uuid(300+replica)}}}};
    }
    void SetUp()override {
        swift_configuration c(file.str(),std::make_shared<immediate_scheduler>());c.audit_retention_seconds=0;c.busy_timeout_ms=100;
#if LATTICE_HAS_FRT
        ref.reset(swift_lattice_ref::create(c,{relay_schema()}));
#else
        ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(c,{relay_schema()}));
#endif
        owner=swift_lattice_ref::shared_for_lattice(ref->get());ASSERT_TRUE(owner);
        if(auto* n=instance_registry::instance().get_or_create_notifier(file.str()))n->stop_listening();
    }
    void TearDown()override {setup.close_on_io();setup={};if(owner)owner->close();owner.reset();ref.reset();}
    relay_recovery_setup open(const json& p,const json& c,std::shared_ptr<RelayRouteState> state={}) {
        return ref->open_relay_recovery_setup(p.dump(),c.dump(),new RelayRoute{state?state:route},RelayRoute::current,RelayRoute::destroy);
    }
    void open(){setup=open(policy(),connection());ASSERT_TRUE(setup.valid())<<last_bridge_error();}
    json outcome(const relay_recovery_setup& s) {
        const auto context=json::parse(s.descriptor());return {{"context",context},{"authenticatedUserID",context["route"]["authenticatedUserID"]},
            {"peer",context["route"]["peer"]},{"source",context["source"]},{"incomingScope",context["incomingScope"]},
            {"authorizationRevision","actual-registration-7"},{"validForMilliseconds",10000}};
    }
    void authorize(){ASSERT_TRUE(setup.finish_authorization(outcome(setup).dump()))<<last_bridge_error();}
    audit_log_entry entry(unsigned n=1,std::string value="accepted") {
        audit_log_entry e;e.global_id=relay_uuid(1000+n);e.global_row_id=relay_uuid(2000+n);e.table_name="AuthenticatedRelayRow";
        e.operation="INSERT";e.changed_fields_names={"text"};e.changed_fields={{"text",any_property(std::move(value))}};e.timestamp="1789819200.0";return e;
    }
    std::string frame(const audit_log_entry& e){return server_sent_event::make_audit_log({e}).to_json();}
    int64_t count(const char* table){return std::get<int64_t>(owner->db().query("SELECT COUNT(*) AS n FROM "+std::string(table)).at(0).at("n"));}
    std::vector<database::row_t> receipts(){return owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id");}
};
TEST_F(AuthenticatedRelaySession, ActualSwiftCatalogPrecedesAuthorizationAndUnauthenticatedReceiveRefuses) {
    open();const auto d=json::parse(setup.descriptor());EXPECT_EQ(d["source"]["schemaDigest"],owner->recovery_declarations().swift_digest);
    EXPECT_EQ(d["incomingScope"]["models"].size(),1u);EXPECT_EQ(setup.receive(frame(entry())).status_code(),2);
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, AcceptedOriginalAndLostAckReplayHaveOnePositiveReceiptAndUnchangedRow) {
    open();authorize();const auto e=entry();auto accepted=setup.receive(frame(e));ASSERT_EQ(accepted.status_code(),1);
    EXPECT_EQ(accepted.ids(),std::vector<std::string>{e.global_id});const auto first=receipts();ASSERT_EQ(first.size(),1u);
    auto altered=e;altered.changed_fields["text"]=any_property(std::string("must not replace"));
    auto replay=setup.receive(frame(altered));EXPECT_EQ(replay.ids(),accepted.ids());EXPECT_EQ(receipts(),first);
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT text FROM AuthenticatedRelayRow").at(0).at("text")),"accepted");
}
TEST_F(AuthenticatedRelaySession, TwoRegisteredReplicasForOneUserSharePhysicalSourceWithDistinctSetups) {
    open();authorize();auto other=open(policy(),connection(2));ASSERT_TRUE(other.valid());ASSERT_TRUE(other.finish_authorization(outcome(other).dump()));
    auto a=setup.receive(frame(entry(1))),b=other.receive(frame(entry(2)));EXPECT_EQ(a.ids().size(),1u);EXPECT_EQ(b.ids().size(),1u);
    EXPECT_EQ(count("AuthenticatedRelayRow"),2);EXPECT_EQ(count("_lattice_canonical_receipt"),2);
    setup.close_on_io();EXPECT_FALSE(setup.stop_token().live());EXPECT_TRUE(other.stop_token().live());
}
TEST_F(AuthenticatedRelaySession, CrossConnectionApplicationOutcomeIsNotTransferable) {
    open();auto other=open(policy(),connection(2));ASSERT_TRUE(other.valid());
    EXPECT_FALSE(other.finish_authorization(outcome(setup).dump()));EXPECT_FALSE(other.stop_token().live());
    EXPECT_EQ(other.receive(frame(entry())).status_code(),2);EXPECT_EQ(count("AuthenticatedRelayRow"),0);
}
TEST_F(AuthenticatedRelaySession, EveryIdentityAndScopeMismatchConsumesAuthorizationWithoutEffects) {
    for(const char* field:{"authenticatedUserID","peer","source","incomingScope"}) {
        auto actual=open(policy(),connection());ASSERT_TRUE(actual.valid());const auto valid=outcome(actual);auto answer=valid;
        if(std::string(field)=="authenticatedUserID")answer[field]=relay_uuid(90);
        if(std::string(field)=="peer")answer[field]["receiverIncarnation"]=relay_uuid(90);
        if(std::string(field)=="source")answer[field]["coverageRevision"]=2;
        if(std::string(field)=="incomingScope")answer[field]["models"][0]["incomingOperations"]=json::array({"INSERT"});
        EXPECT_FALSE(actual.finish_authorization(answer.dump()));EXPECT_FALSE(actual.finish_authorization(valid.dump()));
        EXPECT_EQ(actual.receive(frame(entry())).status_code(),2);actual={};
    }
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, ActualRouteCloseBeforeCallbackConsumptionRejectsLateOutcome) {
    open();const auto answer=outcome(setup);route->current=false;
    EXPECT_FALSE(setup.finish_authorization(answer.dump()));EXPECT_EQ(setup.receive(frame(entry())).status_code(),2);
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);
}
TEST_F(AuthenticatedRelaySession, StopPreventsLatePublicationAndCountSettlesAfterLastResultCopy) {
    open();authorize();auto stop=setup.stop_token();auto result=setup.receive(frame(entry()));auto copy=result;
    ASSERT_EQ(result.ids().size(),1u);EXPECT_FALSE(stop.drained());stop.stop();EXPECT_FALSE(result.publishable());
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("_lattice_canonical_receipt"),1);
    result={};EXPECT_FALSE(stop.drained());copy={};EXPECT_TRUE(stop.drained());
    EXPECT_EQ(setup.receive(frame(entry(2))).status_code(),2);
}
TEST_F(AuthenticatedRelaySession, FiniteAuthorizationExpiryStopsNewEffectsWithoutClockOverride) {
    open();auto answer=outcome(setup);answer["validForMilliseconds"]=100;
    ASSERT_TRUE(setup.finish_authorization(answer.dump()));auto stop=setup.stop_token();
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    while(stop.live()&&std::chrono::steady_clock::now()<deadline)std::this_thread::yield();
    ASSERT_FALSE(stop.live());EXPECT_EQ(setup.receive(frame(entry())).status_code(),2);
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, OwnerCloseInvalidatesAlreadyAuthorizedPhysicalAdmission) {
    open();authorize();owner->close();const auto result=setup.receive(frame(entry()));EXPECT_NE(result.status_code(),1);EXPECT_TRUE(result.ids().empty());
}
TEST_F(AuthenticatedRelaySession, DenyAllObserverAllowsAckWithoutGrantingAnyUpload) {
    auto p=policy();p["upload"]["unlisted"]="deny";setup=open(p,connection());ASSERT_TRUE(setup.valid());authorize();
    auto denied=setup.receive(frame(entry()));EXPECT_EQ(denied.status_code(),4);EXPECT_TRUE(denied.ids().empty());
    auto ack=setup.receive(server_sent_event::make_ack({relay_uuid(6000)}).to_json());EXPECT_EQ(ack.status_code(),1);EXPECT_TRUE(ack.ids().empty());
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, NativePolicyChecksMixedFrameBeforeAnyEntryEffects) {
    auto p=policy();p["upload"]["maximumDeletes"]=0;setup=open(p,connection());ASSERT_TRUE(setup.valid());authorize();
    auto deletion=entry(2);deletion.operation="DELETE";deletion.changed_fields.clear();deletion.changed_fields_names.clear();
    auto result=setup.receive(server_sent_event::make_audit_log({entry(),deletion}).to_json());EXPECT_EQ(result.status_code(),4);
    EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, DuplicateKeysOversizedFramesAndMalformedMembersNeverBecomePartialUploads) {
    open();authorize();auto valid=json::parse(frame(entry()));valid["auditLog"].push_back(17);
    for(const auto& raw:{valid.dump(),std::string("{\"auditLog\":[],\"auditLog\":[]}"),std::string(1048577,' ')}) {
        EXPECT_EQ(setup.receive(raw).status_code(),4);EXPECT_EQ(count("AuthenticatedRelayRow"),0);
    }
}
TEST_F(AuthenticatedRelaySession, SameOwnerProfileMismatchRefusesWithoutRetiringOtherSession) {
    open();authorize();auto p=policy();p["epoch"]=relay_uuid(99);auto other=open(p,connection(2));EXPECT_FALSE(other.valid());
    EXPECT_TRUE(setup.stop_token().live());EXPECT_EQ(setup.receive(frame(entry())).ids().size(),1u);
}
TEST_F(AuthenticatedRelaySession, EquivalentReopenPreservesFirstPositiveReceipt) {
    open();authorize();const auto e=entry();auto result=setup.receive(frame(e));ASSERT_EQ(result.ids().size(),1u);const auto first=receipts();
    result={};setup.close_on_io();setup={};open();authorize();EXPECT_EQ(setup.receive(frame(e)).ids().size(),1u);EXPECT_EQ(receipts(),first);
}
TEST_F(AuthenticatedRelaySession, DistinctEnrolledNamespaceCannotAdoptAnotherNamespacesOriginal) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);const auto first=receipts();
    auto p=policy();p["receiptNamespace"]="other";auto other=open(p,connection(2));ASSERT_TRUE(other.valid());ASSERT_TRUE(other.finish_authorization(outcome(other).dump()));
    EXPECT_TRUE(other.receive(frame(e)).ids().empty());EXPECT_EQ(receipts(),first);EXPECT_EQ(count("AuthenticatedRelayRow"),1);
}
TEST_F(AuthenticatedRelaySession, EmptyUploadHasNoReceiptAndLegacyReceiveCannotBypassProtectedNamespace) {
    open();authorize();auto empty=setup.receive(server_sent_event::make_audit_log({}).to_json());EXPECT_EQ(empty.status_code(),1);EXPECT_TRUE(empty.ids().empty());
    EXPECT_THROW(apply_remote_changes(*owner,{entry()}),db_error);EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedRelaySession, FactoryFailureAndFinalCloseReleaseActualRouteExactlyOnce) {
    auto p=policy();p["models"]=json::array();auto bad=open(p,connection());EXPECT_FALSE(bad.valid());EXPECT_EQ(route->destroyed.load(),1);
    open();authorize();auto stop=setup.stop_token();setup.close_on_io();setup={};EXPECT_EQ(route->destroyed.load(),2);EXPECT_FALSE(stop.live());
}
}
#endif
