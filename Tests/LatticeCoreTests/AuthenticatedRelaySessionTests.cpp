#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_authenticated_session.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <algorithm>
#include <set>
#include <cstring>
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <nlohmann/json.hpp>
#include <atomic>
#include <cstdio>
#include <chrono>
#include <thread>

#if defined(__APPLE__) || defined(__linux__)
namespace lattice::detail {
struct authenticated_ready_test_access {
    static void before_owned(const std::function<void()>* hook) {authenticated_relay_setup::ready_before_owned_test_hook_=hook;}
};
struct authenticated_relay_catalog_test_access {
    static const recovery_owner_schema& catalog(const lattice_db& owner) noexcept {
        return canonical_writer_adapter::authenticated_catalog(owner);
    }
};
}
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
    open();const auto d=json::parse(setup.descriptor());EXPECT_EQ(d["source"]["schemaDigest"],lattice::detail::authenticated_relay_catalog_test_access::catalog(*owner).swift_digest);
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
TEST_F(AuthenticatedRelaySession, OwnedIdsOutliveResultsWithoutReleasingPublicationChargeEarly) {
    open();authorize();const auto e=entry();auto result=setup.receive(frame(e));
    auto stop=setup.stop_token();ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable());
    auto ids=result.take_ids();EXPECT_EQ(ids,std::vector<std::string>{e.global_id});
    EXPECT_TRUE(result.ids().empty());EXPECT_TRUE(result.take_ids().empty());
    EXPECT_EQ(result.status_code(),1);EXPECT_TRUE(result.publishable());EXPECT_FALSE(stop.drained());
    auto copy=result;result={};EXPECT_FALSE(stop.drained());
    stop.stop();EXPECT_FALSE(copy.publishable());EXPECT_FALSE(stop.drained());
    copy={};EXPECT_TRUE(stop.drained());EXPECT_EQ(ids,std::vector<std::string>{e.global_id});
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("_lattice_canonical_receipt"),1);
}

namespace ready_wire=lattice::detail::canonical_range;
class AuthenticatedReadySession:public AuthenticatedRelaySession {
protected:
    json source_policy(bool large=false){auto p=policy();p["maximumAuthorizationMilliseconds"]=600000;if(large)p["readyProfile"]="bounded48MiBV1";return p;}
    relay_recovery_setup admitted(unsigned replica=1,bool large=false) {
        auto value=open(source_policy(large),connection(replica));if(!value.valid())throw std::runtime_error("actual READY setup failed");
        auto answer=outcome(value);answer["validForMilliseconds"]=600000;
        if(!value.finish_authorization(answer.dump()))throw std::runtime_error("actual READY authorization failed");return value;
    }
    json control(const char* op){return {{"kind","recoveryReady"},{"version",1},{"operation",op},{"requestID",relay_uuid(9000)}};}
    relay_ready_result invoke(const relay_recovery_setup& value,const json& command) {
        const auto raw=command.dump();const auto charge=value.stop_token().reserve_ready(raw.size());
        if(!charge.valid())throw std::runtime_error("actual READY input not admitted");return value.ready(raw,charge);
    }
    json description(const relay_recovery_setup& value) {
        auto result=invoke(value,control("describe"));if(result.status_code()!=1||!result.publishable())throw std::runtime_error("READY describe unavailable");
        return json::parse(result.wire());
    }
    ready_wire::limits codec(const json& d) {
        const auto& p=d.at("profile");const auto& b=p.at("wire");const auto n=[&](const char* k){return std::stoull(b.at(k).get<std::string>());};
        const auto& v=p.at("valueLimits");
        return {{n("frame_bytes"),n("payload_bytes"),n("items_per_page"),n("content_pages"),n("content_identities"),n("content_bytes"),n("receipt_pages"),n("receipts"),n("receipt_bytes")},
            p.at("parserDepth"),p.at("parserNodes"),p.at("scalarBytes"),p.at("requestEntries"),p.at("requestTargets"),p.at("requestTargetBytes"),p.at("restartBytes"),p.at("leaseMilliseconds"),
            {v.at("rawBytes"),v.at("fields"),v.at("nameBytes"),v.at("valueBytes"),v.at("decodedBytes")}};
    }
    ready_wire::frame request(const json& d,uint64_t sequence=1) {
        const auto& source=d.at("source");ready_wire::request q;
        q.source={source.at("authority"),source.at("sourceID"),source.at("epoch"),source.at("scopeDigest"),source.at("schemaDigest")};
        q.budget=codec(d).maximum;q.expected.binding=q.source;
        ready_wire::attempt a{d.at("peer").at("receiverIncarnation"),d.at("peer").at("channelIncarnation"),d.at("channel"),sequence,relay_uuid(9100+static_cast<unsigned>(sequence))};
        q.request_digest=ready_wire::request_sha256(a,q,codec(d));return {a,std::stoull(d.at("routeGeneration").get<std::string>()),q};
    }
    void seal(ready_wire::frame& f,const json& d) {auto& q=std::get<ready_wire::request>(f.body);q.request_digest=ready_wire::request_sha256(f.logical,q,codec(d));}
    json command(const char* op,const ready_wire::frame& f,const json& d,int64_t duration=10000) {
        auto value=control(op);value["routeGeneration"]=d.at("routeGeneration");value["request"]=ready_wire::encode(f,codec(d));
        if(std::string(op)!="discard")value["durationMilliseconds"]=duration;return value;
    }
    json lease(const relay_recovery_setup& value,const ready_wire::frame& f,const json& d,const char* op="prepare",int64_t duration=10000) {
        auto result=invoke(value,command(op,f,d,duration));if(result.status_code()!=1||!result.publishable())throw std::runtime_error("READY lease response unavailable");
        auto answer=json::parse(result.wire());if(answer.at("leaseAvailable")!=true)throw std::runtime_error("READY lease did not commit: "+answer.dump());return answer;
    }
    relay_ready_result read(const relay_recovery_setup& value,const json& lease,uint64_t index) {
        auto c=control("read");for(const auto* name:{"routeGeneration","leaseID","requestDigest","attemptID","sequence"})c[name]=lease.at(name);
        c["index"]=std::to_string(index);return invoke(value,c);
    }
    std::vector<std::vector<database::row_t>> exact_source() {
        std::vector<std::vector<database::row_t>> out;
        for(const auto* table:{"AuthenticatedRelayRow","AuditLog","_lattice_canonical_store","_lattice_canonical_receipt","_lattice_canonical_touch",
            "_lattice_canonical_ready_profile","_lattice_canonical_ready_binding","_lattice_canonical_ready_transfer","_lattice_canonical_ready_frame","_lattice_canonical_retention","_lattice_canonical_attempt"})
            out.push_back(owner->db().query("SELECT * FROM "+std::string(table)+" ORDER BY 1"));return out;
    }
};
TEST_F(AuthenticatedReadySession, DescribeUsesActualAuthorizationAndPreservesOriginalProfile) {
    setup=open(source_policy(),connection());ASSERT_TRUE(setup.valid());EXPECT_FALSE(setup.stop_token().reserve_ready(128).valid());
    auto answer=outcome(setup);answer["validForMilliseconds"]=600000;ASSERT_TRUE(setup.finish_authorization(answer.dump()));
    const auto before=exact_source();const auto d=description(setup);
    EXPECT_EQ(d["source"],json::parse(setup.descriptor())["source"]);EXPECT_EQ(d["peer"],connection()["peer"]);
    EXPECT_EQ(d["profile"]["name"],"boundedV1");EXPECT_EQ(d["profile"]["transferBytes"],2097152);EXPECT_EQ(d["profile"]["wire"]["content_bytes"],"1048576");
    EXPECT_EQ(d["upload"],json({{"maximumEntries",256},{"maximumWireBytes",1048576},{"maximumScalarBytes",65536},{"parserNodes",32768},{"parserDepth",16},{"maximumDeletes",256}}));
    EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedReadySession, RealAcceptedAndMissingOriginalsProduceCommittedAndUnknownReceipts) {
    setup=admitted();const auto present=entry(1),missing=entry(2);ASSERT_EQ(setup.receive(frame(present)).ids().size(),1u);
    const auto before=receipts();auto d=description(setup);auto f=request(d);auto& q=std::get<ready_wire::request>(f.body);
    q.receipts={{present.global_id,"app",{{present.table_name,present.global_row_id}}},{missing.global_id,"app",{{missing.table_name,missing.global_row_id}}}};seal(f,d);
    auto offered=lease(setup,f,d);ASSERT_EQ(offered["publication"]["state"],"committed");size_t committed=0,unknown=0,rows=0;
    for(uint64_t i=0;i<std::stoull(offered["frames"].get<std::string>());++i) {
        auto value=read(setup,offered,i);ASSERT_EQ(value.status_code(),1);ASSERT_TRUE(value.publishable());
        const auto decoded=ready_wire::decode(value.wire(),codec(d));EXPECT_EQ(decoded.logical,f.logical);EXPECT_EQ(decoded.route_generation,f.route_generation);
        if(const auto* page=std::get_if<ready_wire::content_page>(&decoded.body))rows+=page->items.size();
        if(const auto* page=std::get_if<ready_wire::receipt_page>(&decoded.body))for(const auto& receipt:page->items) {
            if(std::holds_alternative<ready_wire::committed>(receipt.value)){++committed;EXPECT_EQ(receipt.original_id,present.global_id);}
            if(std::holds_alternative<ready_wire::unknown>(receipt.value)){++unknown;EXPECT_EQ(receipt.original_id,missing.global_id);}
            EXPECT_FALSE(std::holds_alternative<ready_wire::not_committed>(receipt.value));
        }
    }
    EXPECT_EQ(committed,1u);EXPECT_EQ(unknown,1u);EXPECT_EQ(rows,2u);EXPECT_EQ(receipts(),before);EXPECT_EQ(count("AuthenticatedRelayRow"),1);
}
TEST_F(AuthenticatedReadySession, WrongSourcePeerChannelNamespaceAndTargetNeverReserveOrCapture) {
    setup=admitted();const auto d=description(setup);const auto before=exact_source();
    for(unsigned which=0;which<6;++which) {
        auto f=request(d);auto& q=std::get<ready_wire::request>(f.body);
        if(which==0)q.source.source_id=relay_uuid(99);
        if(which==1)f.logical.receiver_incarnation=relay_uuid(99);
        if(which==2)f.logical.channel_incarnation=relay_uuid(99);
        if(which==3)f.logical.channel="other-channel";
        if(which==4)q.receipts={{entry().global_id,"other",{{"AuthenticatedRelayRow",entry().global_row_id}}}};
        if(which==5)q.receipts={{entry().global_id,"app",{{"NotInActualScope",entry().global_row_id}}}};
        seal(f,d);EXPECT_EQ(invoke(setup,command("prepare",f,d)).status_code(),4);EXPECT_EQ(exact_source(),before);
    }
}
TEST_F(AuthenticatedReadySession, SameAndCrossSessionResumeFencesQueuedFramesAndKeepsCapsuleBytes) {
    setup=admitted();auto d=description(setup);auto f=request(d);const auto offered=lease(setup,f,d);
    auto queued=read(setup,offered,0);ASSERT_TRUE(queued.publishable());const auto original=queued.wire();
    auto same=lease(setup,f,d,"resume");EXPECT_FALSE(queued.publishable());EXPECT_EQ(read(setup,same,0).wire(),original);
    auto keeper=admitted(2);auto successor=admitted();auto next=description(successor);auto retry=f;retry.route_generation=std::stoull(next["routeGeneration"].get<std::string>());
    auto same_queued=read(setup,same,0);ASSERT_TRUE(same_queued.publishable());
    auto renewed=lease(successor,retry,next,"resume");EXPECT_FALSE(same_queued.publishable());EXPECT_TRUE(keeper.stop_token().live());
    auto current=read(successor,renewed,0);ASSERT_TRUE(current.publishable());auto a=json::parse(original),b=json::parse(current.wire());
    a["latticeCanonicalRange"]["route_generation"]=b["latticeCanonicalRange"]["route_generation"];EXPECT_EQ(a,b);
    EXPECT_EQ(read(successor,same,0).status_code(),4);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
TEST_F(AuthenticatedReadySession, ActualAdapterReopenOrphansAndResumesExactDurableCapsule) {
    setup=admitted();auto d=description(setup);auto f=request(d);auto offered=lease(setup,f,d);auto old=read(setup,offered,0);const auto wire=old.wire();
    const auto stored=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY frame_index");
    old={};setup.close_on_io();setup={};setup=admitted();auto after=description(setup);f.route_generation=std::stoull(after["routeGeneration"].get<std::string>());
    auto renewed=lease(setup,f,after,"resume");auto current=read(setup,renewed,0);ASSERT_TRUE(current.publishable());
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY frame_index"),stored);
    auto a=json::parse(wire),b=json::parse(current.wire());a["latticeCanonicalRange"]["route_generation"]=b["latticeCanonicalRange"]["route_generation"];EXPECT_EQ(a,b);
}
TEST_F(AuthenticatedReadySession, ExpiredSameProcessLeaseRequiresExactDiscardAndHigherSequence) {
    setup=admitted();auto keeper=admitted(2);const auto d=description(setup);auto f=request(d);const auto offered=lease(setup,f,d,"prepare",1000);
    auto queued=read(setup,offered,0);const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
    while(queued.publishable()&&std::chrono::steady_clock::now()<deadline)std::this_thread::yield();ASSERT_FALSE(queued.publishable());
    auto resume=invoke(setup,command("resume",f,d));ASSERT_EQ(resume.status_code(),1);EXPECT_FALSE(json::parse(resume.wire())["leaseAvailable"].get<bool>());
    const auto before=receipts();auto discarded=invoke(setup,command("discard",f,d));ASSERT_EQ(discarded.status_code(),1);
    EXPECT_EQ(json::parse(discarded.wire())["settlement"]["state"],"committed");EXPECT_EQ(receipts(),before);EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
    auto stale=invoke(setup,command("prepare",f,d));ASSERT_EQ(stale.status_code(),1);EXPECT_FALSE(json::parse(stale.wire())["leaseAvailable"].get<bool>());
    auto next=request(d,2);EXPECT_TRUE(lease(setup,next,d)["leaseAvailable"].get<bool>());EXPECT_TRUE(keeper.stop_token().live());
}
TEST_F(AuthenticatedReadySession, SourceWideReservationsBoundPeersAndAreOneShot) {
    setup=admitted();auto other=admitted(2);const auto c=control("describe");const auto raw=c.dump();
    std::vector<relay_ready_charge> holds;
    for(unsigned i=0;i<32;++i){auto held=(i%2?setup:other).stop_token().reserve_ready(raw.size());if(!held.valid())break;holds.push_back(std::move(held));}
    ASSERT_GT(holds.size(),1u);EXPECT_LT(holds.size(),32u);EXPECT_FALSE(other.stop_token().reserve_ready(raw.size()).valid());
    holds.pop_back();auto available=other.stop_token().reserve_ready(raw.size());ASSERT_TRUE(available.valid());
    auto first=other.ready(raw,available);ASSERT_EQ(first.status_code(),1);EXPECT_EQ(other.ready(raw,available).status_code(),4);
    available={};EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());first={};EXPECT_TRUE(setup.stop_token().reserve_ready(raw.size()).valid());
    holds.clear();EXPECT_TRUE(setup.stop_token().drained());EXPECT_TRUE(other.stop_token().drained());
}
TEST_F(AuthenticatedReadySession, OriginalAndLargerProfilesCannotSilentlyAdoptEachOthersSource) {
    setup=admitted();auto wrong=open(source_policy(true),connection(2));EXPECT_FALSE(wrong.valid());EXPECT_TRUE(setup.stop_token().live());
    setup.close_on_io();setup={};wrong=open(source_policy(true),connection());EXPECT_FALSE(wrong.valid());
    setup=admitted();EXPECT_EQ(description(setup)["profile"]["name"],"boundedV1");
}
TEST_F(AuthenticatedReadySession, DenyAllUploadsStillPermitAuthorizedCanonicalReadScope) {
    auto p=source_policy();p["upload"]["unlisted"]="deny";setup=open(p,connection());ASSERT_TRUE(setup.valid());
    auto answer=outcome(setup);answer["validForMilliseconds"]=600000;ASSERT_TRUE(setup.finish_authorization(answer.dump()));
    EXPECT_EQ(setup.receive(frame(entry())).status_code(),4);auto d=description(setup);auto f=request(d);auto offered=lease(setup,f,d);
    EXPECT_TRUE(read(setup,offered,0).publishable());EXPECT_EQ(count("AuthenticatedRelayRow"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);
}
TEST_F(AuthenticatedReadySession, MalformedMixedDuplicateAndForeignChargeRefuseWithoutEffects) {
    setup=admitted();const auto before=exact_source();auto c=control("describe");c["auditLog"]=json::array();EXPECT_EQ(invoke(setup,c).status_code(),4);
    c=control("describe");c["version"]=2;EXPECT_EQ(invoke(setup,c).status_code(),4);
    const std::string duplicate="{\"kind\":\"recoveryReady\",\"kind\":\"recoveryReady\"}";
    auto charge=setup.stop_token().reserve_ready(duplicate.size());ASSERT_TRUE(charge.valid());EXPECT_EQ(setup.ready(duplicate,charge).status_code(),4);
    EXPECT_FALSE(setup.stop_token().reserve_ready(8388609).valid());EXPECT_EQ(setup.ready(control("describe").dump(),{}).status_code(),4);EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedReadySession, OwnerCloseInProgressRejectsAlreadyQueuedResultBeforeNotificationDrain) {
    setup=admitted();auto d=description(setup);auto f=request(d);auto offered=lease(setup,f,d);auto queued=read(setup,offered,0);ASSERT_TRUE(queued.publishable());
    std::atomic<bool> entered{false},release{false},finished{false};auto retained=owner;
    std::thread notification([&]{instance_registry::instance().for_each_alive(file.str(),[&](lattice_db* value){if(value!=retained.get())return;
        entered=true;const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(!release.load()&&std::chrono::steady_clock::now()<until)std::this_thread::yield();});});
    const auto entered_deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    while(!entered.load()&&std::chrono::steady_clock::now()<entered_deadline)std::this_thread::yield();
    if(!entered.load()){release=true;notification.join();FAIL()<<"actual notifier did not enter bounded hold";return;}
    std::thread closing([&]{retained->close();finished=true;});
    const auto close_deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    while(!retained->is_closed()&&std::chrono::steady_clock::now()<close_deadline)std::this_thread::yield();
    EXPECT_TRUE(retained->is_closed());EXPECT_FALSE(finished.load());EXPECT_FALSE(queued.publishable());
    release=true;notification.join();closing.join();EXPECT_TRUE(finished.load());
}
TEST_F(AuthenticatedReadySession, LargerExplicitProfileCarriesEightThousandRowsAndRealReceiptRequests) {
    setup=admitted(1,true);const auto d=description(setup);ASSERT_EQ(d["profile"]["name"],"bounded48MiBV1");
    std::vector<audit_log_entry> entries;entries.reserve(8000);const std::string payload(2048,'x');
    for(unsigned i=0;i<8000;++i){auto e=entry(i,payload);e.global_id=relay_uuid(100000+i);e.global_row_id=relay_uuid(200000+i);entries.push_back(std::move(e));}
    for(size_t begin=0;begin<entries.size();begin+=256) {
        const auto end=std::min(entries.size(),begin+256);std::vector<audit_log_entry> page(entries.begin()+begin,entries.begin()+end);
        auto accepted=setup.receive(server_sent_event::make_audit_log(page).to_json());ASSERT_EQ(accepted.status_code(),1);ASSERT_EQ(accepted.ids().size(),page.size());
    }
    ASSERT_EQ(count("AuthenticatedRelayRow"),8000);ASSERT_EQ(count("_lattice_canonical_receipt"),8000);
    const auto before=receipts();auto f=request(d);auto& q=std::get<ready_wire::request>(f.body);
    for(const auto& e:entries)q.receipts.push_back({e.global_id,"app",{{e.table_name,e.global_row_id}}});seal(f,d);
    const auto offered=lease(setup,f,d,"prepare",300000);const auto frames=std::stoull(offered["frames"].get<std::string>());ASSERT_LE(frames,770u);
    auto first=read(setup,offered,0);ASSERT_TRUE(first.publishable());auto manifest=ready_wire::decode(first.wire(),codec(d));
    const auto& m=std::get<ready_wire::manifest>(manifest.body);auto sequence=ready_wire::begin(f.logical,q,m,codec(d));
    ready_wire::stream_hasher contents(m,ready_wire::stream_kind::content,codec(d)),receipts_hash(m,ready_wire::stream_kind::receipts,codec(d));
    std::set<std::string> row_ids,original_ids;size_t bytes=first.wire().size();
    for(uint64_t index=1;index<frames;++index) {
        auto result=read(setup,offered,index);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable());bytes+=result.wire().size();
        const auto frame=ready_wire::decode(result.wire(),codec(d));sequence=ready_wire::propose(sequence,frame,codec(d));
        if(const auto* page=std::get_if<ready_wire::content_page>(&frame.body))for(const auto& item:page->items) {
            ASSERT_TRUE(std::holds_alternative<ready_wire::present>(item.value));contents.append(item);EXPECT_TRUE(row_ids.insert(item.key.id).second);
            const auto values=lattice::detail::sync_recovery::decode_values(std::get<ready_wire::present>(item.value).payload,codec(d).values);
            EXPECT_EQ(std::get<std::string>(values.at("text")),payload);
        }
        if(const auto* page=std::get_if<ready_wire::receipt_page>(&frame.body))for(const auto& item:page->items) {
            receipts_hash.append(item);ASSERT_TRUE(std::holds_alternative<ready_wire::committed>(item.value));EXPECT_TRUE(original_ids.insert(item.original_id).second);
        }
    }
    EXPECT_EQ(sequence.status,ready_wire::phase::sequence_complete_unverified);EXPECT_EQ(contents.finish(),m.content_digest);EXPECT_EQ(receipts_hash.finish(),m.receipt_digest);
    EXPECT_EQ(row_ids.size(),8000u);EXPECT_EQ(original_ids.size(),8000u);EXPECT_EQ(receipts(),before);EXPECT_LE(bytes,41943040u);
    for(const auto& e:entries){EXPECT_EQ(row_ids.count(e.global_row_id),1u);EXPECT_EQ(original_ids.count(e.global_id),1u);}
}

TEST_F(AuthenticatedReadySession, DelayedOldReservationCannotReplaceLaterSessionsCommittedLease) {
    setup=admitted();auto later=admitted();const auto d=description(setup),other=description(later);auto f=request(d),next=f;
    next.route_generation=std::stoull(other["routeGeneration"].get<std::string>());
    std::atomic<bool> entered{false},release{false};relay_ready_result delayed;std::exception_ptr worker_error;
    std::thread worker([&]{
        const std::function<void()> hook=[&]{entered=true;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
            while(!release.load()&&std::chrono::steady_clock::now()<end)std::this_thread::yield();};
        lattice::detail::authenticated_ready_test_access::before_owned(&hook);
        try{delayed=invoke(setup,command("prepare",f,d));}catch(...){worker_error=std::current_exception();}
        lattice::detail::authenticated_ready_test_access::before_owned(nullptr);
    });
    const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    while(!entered.load()&&std::chrono::steady_clock::now()<until)std::this_thread::yield();
    json accepted;std::exception_ptr later_error;
    try{if(entered.load())accepted=lease(later,next,other);}catch(...){later_error=std::current_exception();}
    release=true;worker.join();ASSERT_TRUE(entered.load());ASSERT_FALSE(worker_error);ASSERT_FALSE(later_error);
    ASSERT_EQ(delayed.status_code(),1);EXPECT_FALSE(json::parse(delayed.wire())["leaseAvailable"].get<bool>());
    auto actual=read(later,accepted,0);EXPECT_TRUE(actual.publishable());EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
namespace {
struct AuthenticatedReadyFault {
    static thread_local AuthenticatedReadyFault* current;
    bool publication;int commits=0,hits=0;
    lattice::detail::canonical_upstream_test_hooks::authorizer_fault fault;
    const lattice::detail::canonical_upstream_test_hooks::authorizer_fault* prior;
    AuthenticatedReadyFault(database& db,bool p):publication(p),fault{lattice::detail::canonical_writer_custody_test_access::fault_handle(db),deny},
        prior(lattice::detail::canonical_retention_test_hooks::fault){current=this;lattice::detail::canonical_retention_test_hooks::fault=&fault;}
    ~AuthenticatedReadyFault(){lattice::detail::canonical_retention_test_hooks::fault=prior;current=nullptr;}
    static int deny(int action,const char* table,const char*,const char* origin)noexcept {
        auto& f=*current;if(origin)return SQLITE_OK;
        if(f.publication&&action==SQLITE_TRANSACTION&&table&&std::strcmp(table,"COMMIT")==0&&++f.commits==3){++f.hits;return SQLITE_DENY;}
        if(!f.publication&&action==SQLITE_READ&&table&&std::strcmp(table,"_lattice_canonical_ready_frame")==0){++f.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local AuthenticatedReadyFault* AuthenticatedReadyFault::current=nullptr;
}
TEST_F(AuthenticatedReadySession, FailedPublicationReportsKnownPreparationAndExactDiscardRecoversCapacity) {
    setup=admitted();const auto d=description(setup);auto f=request(d);const auto before=receipts();relay_ready_result result;
    {AuthenticatedReadyFault fault(owner->db(),true);result=invoke(setup,command("prepare",f,d));EXPECT_EQ(fault.hits,1);}
    ASSERT_EQ(result.status_code(),1);const auto answer=json::parse(result.wire());EXPECT_EQ(answer["preparation"]["state"],"committed");
    EXPECT_EQ(answer["publication"]["state"],"rolledBack");EXPECT_EQ(answer["leaseAvailable"],false);
    EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);EXPECT_EQ(count("_lattice_canonical_attempt"),1);EXPECT_EQ(receipts(),before);
    auto discard=invoke(setup,command("discard",f,d));ASSERT_EQ(discard.status_code(),1);
    EXPECT_EQ(json::parse(discard.wire())["settlement"]["state"],"committed");EXPECT_EQ(count("_lattice_canonical_attempt"),0);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);EXPECT_EQ(receipts(),before);
    EXPECT_TRUE(lease(setup,request(d,2),d)["leaseAvailable"].get<bool>());
}
TEST_F(AuthenticatedReadySession, ReadDenialReturnsNoFrameAndPreservesExactCapsule) {
    setup=admitted();const auto d=description(setup);auto offered=lease(setup,request(d),d);const auto before=exact_source();relay_ready_result result;
    {AuthenticatedReadyFault fault(owner->db(),false);result=read(setup,offered,0);EXPECT_GT(fault.hits,0);}
    ASSERT_EQ(result.status_code(),1);auto answer=json::parse(result.wire());EXPECT_EQ(answer["frameAvailable"],false);
    EXPECT_NE(answer["settlement"]["state"],"committed");EXPECT_EQ(exact_source(),before);EXPECT_TRUE(read(setup,offered,0).publishable());
}
TEST_F(AuthenticatedReadySession, NewPeerPreparationReclaimsOnlyExpiredCurrentIncarnationCapsule) {
    setup=admitted();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);const auto evidence=receipts();
    const auto d=description(setup);auto offer=lease(setup,request(d),d,"prepare",1000);auto queued=read(setup,offer,0);
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
    while(queued.publishable()&&std::chrono::steady_clock::now()<end)std::this_thread::yield();ASSERT_FALSE(queued.publishable());
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);auto other=admitted(2);const auto next=description(other);
    const auto committed=lease(other,request(next),next);EXPECT_EQ(committed["expiration"]["state"],"committed");
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);EXPECT_EQ(count("_lattice_canonical_ready_binding"),2);EXPECT_EQ(receipts(),evidence);
    EXPECT_TRUE(read(other,committed,0).publishable());
}
TEST_F(AuthenticatedReadySession, ChangedFrozenRequestCannotRecaptureOrAlterStoredCapsule) {
    setup=admitted();const auto d=description(setup);auto f=request(d);const auto offered=lease(setup,f,d);auto queued=read(setup,offered,0);
    const auto before=exact_source();auto changed=f;--std::get<ready_wire::request>(changed.body).budget.content_pages;seal(changed,d);
    auto refused=invoke(setup,command("resume",changed,d));ASSERT_EQ(refused.status_code(),1);
    EXPECT_FALSE(json::parse(refused.wire())["leaseAvailable"].get<bool>());EXPECT_FALSE(queued.publishable());EXPECT_EQ(exact_source(),before);
    auto renewed=lease(setup,f,d,"resume");EXPECT_TRUE(read(setup,renewed,0).publishable());
}

TEST_F(AuthenticatedReadySession, HeldQueuedFrameAndResultsKeepSourceBudgetAcrossLastSetupReopen) {
    setup=admitted();const auto d=description(setup);const auto offered=lease(setup,request(d),d);
    std::vector<relay_ready_result> held;
    held.push_back(read(setup,offered,0));ASSERT_EQ(held.front().status_code(),1);ASSERT_TRUE(held.front().publishable());
    // Match the SDK handoff: move bytes to the socket queue and retain only
    // the stripped result token until the send completion releases it.
    const auto queued_wire=held.front().take_wire();ASSERT_FALSE(queued_wire.empty());EXPECT_TRUE(held.front().wire().empty());
    const auto raw=control("describe").dump();
    for(unsigned i=0;i<32;++i) {
        auto charge=setup.stop_token().reserve_ready(raw.size());if(!charge.valid())break;
        auto result=setup.ready(raw,charge);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable());held.push_back(std::move(result));
    }
    ASSERT_GT(held.size(),1u);ASSERT_LT(held.size(),16u);EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());
    const auto evidence=receipts();const auto frames=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY frame_index");
    setup.close_on_io();setup={};EXPECT_EQ(route->destroyed.load(),1);
    for(const auto& result:held)EXPECT_FALSE(result.publishable());
    // Each pass tears down the actual native setup and adapter. No keeper
    // setup or qualification counter keeps the mounted source alive.
    for(unsigned pass=0;pass<3;++pass) {
        setup=admitted(2);EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());
        EXPECT_EQ(receipts(),evidence);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY frame_index"),frames);
        if(pass<2){setup.close_on_io();setup={};}
    }
    const auto before_release=exact_source();held.pop_back();
    auto available=setup.stop_token().reserve_ready(raw.size());ASSERT_TRUE(available.valid());
    auto fresh=setup.ready(raw,available);ASSERT_EQ(fresh.status_code(),1);EXPECT_TRUE(fresh.publishable());
    available={};EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());
    for(const auto& result:held)EXPECT_FALSE(result.publishable());
    held.clear();fresh={};EXPECT_TRUE(setup.stop_token().drained());EXPECT_TRUE(setup.stop_token().reserve_ready(raw.size()).valid());
    EXPECT_EQ(exact_source(),before_release);EXPECT_FALSE(queued_wire.empty());
}
TEST_F(AuthenticatedReadySession, FailedOrChangedProfileReopenCannotDiscardHeldResultCharges) {
    setup=admitted();const auto raw=control("describe").dump();std::vector<relay_ready_result> held;
    for(unsigned i=0;i<32;++i) {
        auto charge=setup.stop_token().reserve_ready(raw.size());if(!charge.valid())break;
        auto result=setup.ready(raw,charge);ASSERT_EQ(result.status_code(),1);held.push_back(std::move(result));
    }
    ASSERT_GT(held.size(),1u);ASSERT_LT(held.size(),16u);setup.close_on_io();setup={};
    const auto before=exact_source();auto changed=open(source_policy(true),connection(2));
    EXPECT_FALSE(changed.valid());EXPECT_NE(last_bridge_error().find("source profile differs on actual owner"),std::string::npos);
    EXPECT_EQ(exact_source(),before);
    // Exercise the real source factory's idle-owner refusal after registry
    // reservation; the failed constructor must preserve the old charge domain.
    owner->db().begin_transaction();auto refused=open(source_policy(),connection(2));const auto error=last_bridge_error();owner->db().rollback();
    EXPECT_FALSE(refused.valid());EXPECT_NE(error.find("explicit idle WAL/FULL owner"),std::string::npos);EXPECT_EQ(exact_source(),before);
    setup=admitted(2);EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());
    for(const auto& result:held)EXPECT_FALSE(result.publishable());
    held.pop_back();auto charge=setup.stop_token().reserve_ready(raw.size());ASSERT_TRUE(charge.valid());
    auto current=setup.ready(raw,charge);EXPECT_EQ(current.status_code(),1);EXPECT_TRUE(current.publishable());
    charge={};EXPECT_FALSE(setup.stop_token().reserve_ready(raw.size()).valid());held.clear();current={};EXPECT_TRUE(setup.stop_token().drained());
}
TEST_F(AuthenticatedReadySession, HeldReadyResultRetainsNoActualOwnerAfterSetupAndRefRelease) {
    setup=admitted();auto queued=invoke(setup,control("describe"));ASSERT_TRUE(queued.publishable());
    const std::weak_ptr<lattice::swift_lattice> observed=owner;
    setup.close_on_io();setup={};owner->close();owner.reset();ref.reset();
    EXPECT_TRUE(observed.expired());EXPECT_FALSE(queued.publishable());
    queued={};EXPECT_TRUE(observed.expired());
}
}
#endif
