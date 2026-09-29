#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "CanonicalReadyAdoptionTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_ready_named_profile.hpp"
#include "../../Sources/LatticeCore/src/recovery_authenticated_session.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <algorithm>
#include <set>
#include <cstring>
#include <cctype>
#include <filesystem>
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/canonical_validated_sequence.hpp"
#include <nlohmann/json.hpp>
#include <atomic>
#include <cstdio>
#include <chrono>
#include <thread>

#if defined(__APPLE__) || defined(__linux__)
namespace lattice::detail {
struct authenticated_ready_test_access {
    static void before_owned(const std::function<void()>* hook) {authenticated_relay_setup::ready_before_owned_test_hook_=hook;}
    static void before_admin_open(const std::function<void()>* hook) {authenticated_relay_setup::admin_before_open_test_hook_=hook;}
};
struct authenticated_relay_catalog_test_access {
    static const std::function<void(lattice_db&)>* before_write(const std::function<void(lattice_db&)>* hook) {
        const auto* prior=canonical_writer_adapter::namespace_before_write_test_hook_;
        canonical_writer_adapter::namespace_before_write_test_hook_=hook;return prior;
    }
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
    lattice::detail::canonical_ready_read_test_observation::observation last_read_trace;
    lattice::detail::canonical_range::sequence_test_observation::counters last_read_work;
    uint64_t last_read_index=0;
    std::string last_read_bridge_error;
    static json cost_diagnostic(const lattice::detail::canonical_ready_cost_observation::observation& cost) {
        return {{"inclusive",true},{"phases",{"retentionAudit","storeAudit","request","sequenceInit","rawFetchHash",
            "frameDecode","canonicalEncode","sequenceAdvance","receiptEvidence","receiptBatch","fusedFrameValidation"}},
            {"calls",cost.calls},{"microseconds",cost.microseconds},{"receiptBatches",cost.receipt_batches},{"receiptBatchIDs",cost.receipt_batch_ids}};
    }
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
        using namespace lattice::detail::canonical_ready_test_observation;
        observation trace;const auto previous=current;current=&trace;
        namespace counts=lattice::detail::canonical_range::sequence_test_observation;
        counts::counters work;const auto previous_work=counts::current;counts::current=&work;
        struct reset {observation* previous;counts::counters* work;~reset(){current=previous;counts::current=work;}} restore{previous,previous_work};
        auto result=invoke(value,command(op,f,d,duration));
        if(result.status_code()!=1||!result.publishable()) {
            // Bounded local diagnostics even when the expired response MUST NOT
            // be sent. The original failure and all workload/lease inputs stay.
            const json diagnostic={{"status",result.status_code()},{"publishable",result.publishable()},
                {"bridgeError",last_bridge_error().substr(0,2048)},{"response",result.wire().substr(0,4096)},
                {"preparation",trace.preparation},{"publication",trace.publication},
                {"preparationError",trace.preparation_error},{"publicationError",trace.publication_error},
                {"captureError",trace.capture_error},{"leaseAvailable",trace.lease_available},
                {"phases",{"entered","prepared","captured","assembled","publishRequested","publishBody","publicationSettled","auditBegin","auditEnd","finished"}},
                {"visits",trace.visits},{"firstMicroseconds",trace.first_us},{"lastMicroseconds",trace.last_us},{"auditedFrames",trace.audited_frames},
                {"requestValidations",work.request_validations},{"rebaseBuilds",work.rebase_builds},{"restartObjects",work.restart_objects},
                {"cursors",work.cursors},{"pageTransitions",work.transitions},{"subphaseCost",cost_diagnostic(trace.cost)}};
            throw std::runtime_error("READY lease response unavailable: "+diagnostic.dump());
        }
        auto answer=json::parse(result.wire());if(answer.at("leaseAvailable")!=true)throw std::runtime_error("READY lease did not commit: "+answer.dump());return answer;
    }
    std::string read_diagnostic(const relay_ready_result& result) const {
        const auto& trace=last_read_trace;const auto& work=last_read_work;
        // Format only when the actual assertion or canonical decoder fails.
        // Every read retains its fixed measurements even if its lease expires
        // after return; this later sample never substitutes for an assertion.
        return json{{"requestedIndex",last_read_index},{"observedIndex",trace.index},
            {"status",result.status_code()},{"publishable",result.publishable()},
            {"bridgeError",last_read_bridge_error},{"response",result.wire().substr(0,4096)},
            {"phases",{"entered","ownedRequested","bodyBegin","bodyEnd","settled","finished"}},
            {"visits",trace.visits},{"firstMicroseconds",trace.first_us},{"lastMicroseconds",trace.last_us},
            {"fullAudits",trace.full_audits},{"auditedFrames",trace.audited_frames},{"auditedBytes",trace.audited_bytes},
            {"positiveReceiptLookups",trace.positive_receipt_lookups},
            {"addressedFrames",trace.addressed_frames},{"addressedBytes",trace.addressed_bytes},
            {"deadlineMilliseconds",trace.deadline_ms},{"clockBeforeBodyMilliseconds",trace.clock_before_ms},
            {"clockAfterBodyMilliseconds",trace.clock_after_ms},{"clockAfterSettlementMilliseconds",trace.clock_settled_ms},
            {"settlement",trace.settlement},{"primaryError",trace.primary_error},{"cleanupError",trace.cleanup_error},
            {"postcommitError",trace.postcommit_error},{"notificationError",trace.notification_error},
            {"requestValidations",work.request_validations},{"rebaseBuilds",work.rebase_builds},
            {"restartObjects",work.restart_objects},{"cursors",work.cursors},{"pageTransitions",work.transitions},
            {"subphaseCost",cost_diagnostic(trace.cost)}}.dump();
    }
    ready_wire::frame decode_read(const relay_ready_result& result,const json& d) {
        try{return ready_wire::decode(result.wire(),codec(d));}
        catch(...) {
            // A structured frameAvailable=false response can have status 1 and
            // a live ordinary authorization fence. The original frame decoder
            // must still reject it, with the same exception and a bounded trace.
            try{const auto diagnostic=read_diagnostic(result);std::fprintf(stderr,"READY frame decode failed: %s\n",diagnostic.c_str());}catch(...){}
            throw;
        }
    }
    relay_ready_result read(const relay_recovery_setup& value,const json& lease,uint64_t index) {
        auto c=control("read");for(const auto* name:{"routeGeneration","leaseID","requestDigest","attemptID","sequence"})c[name]=lease.at(name);
        c["index"]=std::to_string(index);
        namespace reads=lattice::detail::canonical_ready_read_test_observation;
        namespace counts=lattice::detail::canonical_range::sequence_test_observation;
        last_read_trace={};last_read_work={};last_read_index=index;last_read_bridge_error.clear();
        const auto prior=reads::current;const auto prior_work=counts::current;
        reads::current=&last_read_trace;counts::current=&last_read_work;
        struct reset {reads::observation* prior;counts::counters* work;~reset(){reads::current=prior;counts::current=work;}} restore{prior,prior_work};
        auto result=invoke(value,c);last_read_bridge_error=last_bridge_error().substr(0,2048);return result;
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
    auto queued=read(setup,offered,0);const auto observedAt=std::chrono::steady_clock::now();
    const auto deadline=observedAt+std::chrono::seconds(3);const auto& trace=last_read_trace;
    ASSERT_EQ(queued.status_code(),1);ASSERT_EQ(trace.settlement,static_cast<int>(lattice::detail::recovery_install_state::committed));
    ASSERT_EQ(trace.index,0u);ASSERT_EQ(trace.addressed_frames,1u);ASSERT_GT(trace.addressed_bytes,0u);
    ASSERT_GE(trace.clock_before_ms,0);ASSERT_GE(trace.clock_after_ms,trace.clock_before_ms);
    ASSERT_GE(trace.clock_settled_ms,trace.clock_after_ms);ASSERT_GE(trace.deadline_ms,0);
    const auto remaining=std::max<int64_t>(0,trace.deadline_ms-trace.clock_settled_ms);ASSERT_LE(remaining,1000);
    // The queued publication fence starts earlier than the stored lease. This
    // post-read anchor bounds durable expiry without changing either deadline.
    const auto expiredBy=observedAt+std::chrono::milliseconds(remaining);
    while((queued.publishable()||std::chrono::steady_clock::now()<expiredBy)&&std::chrono::steady_clock::now()<deadline)std::this_thread::yield();
    ASSERT_FALSE(queued.publishable());ASSERT_TRUE(std::chrono::steady_clock::now()>=expiredBy);
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
    auto first=read(setup,offered,0);ASSERT_TRUE(first.publishable())<<read_diagnostic(first);auto manifest=decode_read(first,d);
    const auto& m=std::get<ready_wire::manifest>(manifest.body);auto sequence=ready_wire::begin(f.logical,q,m,codec(d));
    ready_wire::stream_hasher contents(m,ready_wire::stream_kind::content,codec(d)),receipts_hash(m,ready_wire::stream_kind::receipts,codec(d));
    std::set<std::string> row_ids,original_ids;size_t bytes=first.wire().size();
    for(uint64_t index=1;index<frames;++index) {
        auto result=read(setup,offered,index);ASSERT_EQ(result.status_code(),1)<<read_diagnostic(result);ASSERT_TRUE(result.publishable())<<read_diagnostic(result);bytes+=result.wire().size();
        const auto frame=decode_read(result,d);sequence=ready_wire::propose(sequence,frame,codec(d));
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

class AuthenticatedReceiptCoverageV3:public AuthenticatedReadySession {
protected:
    using Rows=std::vector<database::row_t>;
    using Snapshot=std::map<std::string,Rows>;
    detail::recovery_producer_registration producer(unsigned n=1) {
        return {"explicit-installation-"+std::to_string(n),relay_uuid(5000+n)};
    }
    json covered_policy(const std::string& selected="app") {
        auto p=policy();p["version"]=2;p["readyProfile"]="bounded48MiBV1";p["receiptNamespace"]=selected;
        p["receiptCoverage"]={{"kind","registeredProducerV3"},{"cohortID",relay_uuid(5100)},
            {"cohortRevision",7},{"operationCodec",1},{"namespaces",json::array({"app","other"})}};
        return p;
    }
    json covered_answer(const relay_recovery_setup& actual,unsigned registration=1) {
        auto answer=outcome(actual);const auto p=producer(registration);
        answer["receiptCoverage"]={{"kind","registeredProducer"},{"registrationID",p.registration_id},
            {"incarnation",p.incarnation},{"cohortID",relay_uuid(5100)},{"cohortRevision",7}};
        return answer;
    }
    relay_recovery_setup covered_setup(const std::string& selected="app",unsigned peer=1,unsigned registration=1) {
        auto actual=open(covered_policy(selected),connection(peer),std::make_shared<RelayRouteState>());
        if(!actual.valid())throw std::runtime_error("actual v3 source setup refused: "+last_bridge_error());
        if(!actual.finish_authorization(covered_answer(actual,registration).dump()))
            throw std::runtime_error("actual v3 source authorization refused: "+last_bridge_error());
        return actual;
    }
    audit_log_entry identified(audit_log_entry value,unsigned registration=1) {
        const auto& catalog=detail::authenticated_relay_catalog_test_access::catalog(*owner);
        value.original_identity=detail::make_original_identity(value,{{"text",column_type::text}},{},catalog.swift_digest,producer(registration));
        return value;
    }
    Snapshot global_state() {
        Snapshot result;
        for(const auto* table:{"AuthenticatedRelayRow","AuditLog","_lattice_canonical_store","_lattice_canonical_receipt",
                              "_lattice_canonical_touch","_lattice_canonical_namespace"})
            result[table]=owner->db().query("SELECT * FROM "+std::string(table)+" ORDER BY 1");
        return result;
    }
    Snapshot all_state() {
        Snapshot result;const auto tables=owner->db().query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name LIMIT 129");
        if(tables.size()>128)throw std::runtime_error("v3 fixture table inventory bound");
        for(const auto& row:tables) {
            const auto& name=std::get<std::string>(row.at("name"));
            if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](char c){return c>='a'&&c<='z'||c>='A'&&c<='Z'||c>='0'&&c<='9'||c=='_';}))
                throw std::runtime_error("v3 fixture table name refused");
            result[name]=owner->db().query("SELECT * FROM \""+name+"\" ORDER BY 1");
        }
        result["sqlite_schema"]=owner->db().query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");return result;
    }
    Rows coverage() {return owner->db().query("SELECT * FROM _lattice_canonical_receipt_coverage ORDER BY original_id,namespace_id");}
    static std::vector<uint8_t> bytes(const std::string& text) {return {text.begin(),text.end()};}
};

TEST_F(AuthenticatedReceiptCoverageV3, DistinctAuthenticatedPeersShareOneGlobalApplicationAndAddOnlyNamespaceCoverage) {
    setup=covered_setup();auto other=covered_setup("other",2);
    const auto a=json::parse(setup.descriptor()),b=json::parse(other.descriptor());
    EXPECT_NE(a["route"]["peer"],b["route"]["peer"]);EXPECT_EQ(a["source"]["receiptCoverage"],b["source"]["receiptCoverage"]);
    const auto e=identified(entry());auto first=setup.receive(frame(e));
    ASSERT_EQ(first.status_code(),1);ASSERT_EQ(first.take_ids(),std::vector<std::string>{e.global_id});ASSERT_TRUE(first.publishable());
    const auto global=global_state();const auto original_receipts=receipts();ASSERT_EQ(original_receipts.size(),1u);
    ASSERT_EQ(count("_lattice_canonical_receipt_origin"),1);ASSERT_EQ(coverage().size(),1u);
    const auto first_origin=owner->db().query("SELECT * FROM _lattice_canonical_receipt_origin");
    auto second=other.receive(frame(e));ASSERT_EQ(second.status_code(),1);ASSERT_EQ(second.take_ids(),std::vector<std::string>{e.global_id});ASSERT_TRUE(second.publishable());
    EXPECT_EQ(global_state(),global);EXPECT_EQ(receipts(),original_receipts);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt_origin"),first_origin);
    const auto cells=coverage();ASSERT_EQ(cells.size(),2u);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(cells[0].at("namespace_id")),bytes("app"));
    EXPECT_EQ(std::get<std::vector<uint8_t>>(cells[1].at("namespace_id")),bytes("other"));
    for(const auto& cell:cells)EXPECT_EQ(std::get<std::vector<uint8_t>>(cell.at("original_id")),bytes(e.global_id));
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("AuditLog"),1);EXPECT_EQ(count("_lattice_canonical_receipt"),1);
    const auto complete=all_state();
    EXPECT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    EXPECT_EQ(other.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    EXPECT_EQ(all_state(),complete);
}

TEST_F(AuthenticatedReceiptCoverageV3, DifferentProducerAndChangedOrdinaryContentCannotAdoptTheOriginal) {
    setup=covered_setup();const auto e=identified(entry());ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    auto different=covered_setup("other",2,2);auto same=covered_setup("other",3);
    const auto before=all_state();auto wrong_producer=identified(entry(),2);
    EXPECT_TRUE(different.receive(frame(wrong_producer)).ids().empty());EXPECT_EQ(all_state(),before);
    auto changed=e;changed.changed_fields["text"]=any_property("different content with old identity");
    EXPECT_TRUE(same.receive(frame(changed)).ids().empty());EXPECT_EQ(all_state(),before);
    changed=identified(changed); // Valid current encoding, but not the accepted immutable operation.
    EXPECT_TRUE(same.receive(frame(changed)).ids().empty());EXPECT_EQ(all_state(),before);
    EXPECT_EQ(same.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);EXPECT_EQ(coverage().size(),2u);
}

TEST_F(AuthenticatedReceiptCoverageV3, DenyAllCannotAddCoverageEvenForAnAlreadyAcceptedOriginal) {
    setup=covered_setup();const auto e=identified(entry());ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    auto p=covered_policy("other");p["upload"]["unlisted"]="deny";auto denied=open(p,connection(2));ASSERT_TRUE(denied.valid());
    ASSERT_TRUE(denied.finish_authorization(covered_answer(denied).dump()));const auto before=all_state();
    const auto result=denied.receive(frame(e));EXPECT_EQ(result.status_code(),4);EXPECT_TRUE(result.ids().empty());
    EXPECT_EQ(all_state(),before);EXPECT_EQ(coverage().size(),1u);
    EXPECT_EQ(denied.receive(server_sent_event::make_ack({relay_uuid(9999)}).to_json()).status_code(),1);
    EXPECT_EQ(coverage().size(),1u);
}

TEST_F(AuthenticatedReceiptCoverageV3, AuthorizationRequiresTheActualContextAndExactExplicitCohort) {
    setup=covered_setup();const auto baseline=all_state();
    for(const auto* mismatch:{"missing","cohort","revision","context"}) {
        SCOPED_TRACE(mismatch);auto actual=open(covered_policy("other"),connection(2));ASSERT_TRUE(actual.valid());
        auto answer=covered_answer(actual);const auto correct=answer;
        if(std::string(mismatch)=="missing")answer.erase("receiptCoverage");
        if(std::string(mismatch)=="cohort")answer["receiptCoverage"]["cohortID"]=relay_uuid(5199);
        if(std::string(mismatch)=="revision")answer["receiptCoverage"]["cohortRevision"]=8;
        if(std::string(mismatch)=="context")answer["context"]=json::parse(setup.descriptor());
        EXPECT_FALSE(actual.finish_authorization(answer.dump()));EXPECT_FALSE(actual.finish_authorization(correct.dump()));
        EXPECT_EQ(actual.receive(frame(identified(entry()))).status_code(),2);EXPECT_EQ(all_state(),baseline);
    }
    const auto missing=setup.receive(frame(entry()));EXPECT_TRUE(missing.ids().empty());EXPECT_NE(missing.status_code(),1);
    EXPECT_EQ(all_state(),baseline);
}

TEST_F(AuthenticatedReceiptCoverageV3, RegisteredSourceRequiresExplicitLargeProfileAndCannotImplicitlyUpgradeV2) {
    const auto fresh=all_state();auto invalid=covered_policy();invalid.erase("readyProfile");
    auto refused=open(invalid,connection());EXPECT_FALSE(refused.valid());EXPECT_EQ(all_state(),fresh);
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    setup.close_on_io();setup={};const auto before=all_state(),global=global_state();
    auto implicit=open(covered_policy(),connection(2));EXPECT_FALSE(implicit.valid());EXPECT_EQ(all_state(),before);
    setup=open(policy(),connection(3));ASSERT_TRUE(setup.valid());authorize();
    EXPECT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});EXPECT_EQ(global_state(),global);
}

TEST_F(AuthenticatedReceiptCoverageV3, ExplicitOwnedMigrationPreservesLegacyReceiptWithoutInventingProducerCoverage) {
    open();authorize();const auto legacy=entry();ASSERT_EQ(setup.receive(frame(legacy)).ids().size(),1u);
    const auto original_receipts=receipts();auto expected=global_state();
    setup.close_on_io();setup={};ASSERT_FALSE(owner->is_closed());
    ASSERT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),1)<<last_bridge_error();
    expected.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};
    EXPECT_EQ(global_state(),expected);EXPECT_EQ(receipts(),original_receipts);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    const auto migrated=all_state();auto old=open(policy(),connection(2));EXPECT_FALSE(old.valid());EXPECT_EQ(all_state(),migrated);
    setup=covered_setup("app",3);auto other=covered_setup("other",4);const auto retained=identified(legacy);
    EXPECT_EQ(global_state(),expected);const auto reopened=all_state();
    EXPECT_EQ(setup.receive(frame(retained)).take_ids(),std::vector<std::string>{legacy.global_id});
    EXPECT_TRUE(other.receive(frame(retained)).ids().empty());EXPECT_EQ(all_state(),reopened);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    const auto next=identified(entry(2,"new registered operation"));ASSERT_EQ(setup.receive(frame(next)).ids().size(),1u);
    const auto after_first=global_state();ASSERT_EQ(other.receive(frame(next)).ids().size(),1u);EXPECT_EQ(global_state(),after_first);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);EXPECT_EQ(coverage().size(),2u);
    EXPECT_EQ(count("AuthenticatedRelayRow"),2);EXPECT_EQ(count("_lattice_canonical_receipt"),2);
}

TEST_F(AuthenticatedReceiptCoverageV3, MigrationWaitsForActualSetupAndHeldReadyResultWithoutAnySqlEffects) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    auto held=invoke(setup,control("describe"));ASSERT_TRUE(held.publishable());const auto before=all_state();
    EXPECT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),2);EXPECT_TRUE(last_bridge_error().empty());EXPECT_EQ(all_state(),before);
    setup.close_on_io();EXPECT_FALSE(held.publishable());
    EXPECT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),2);EXPECT_EQ(all_state(),before);
    setup={};
    EXPECT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),2);EXPECT_TRUE(last_bridge_error().empty());EXPECT_EQ(all_state(),before);
    held={};
    ASSERT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),1)<<last_bridge_error();
    EXPECT_EQ(receipts(),before.at("_lattice_canonical_receipt"));EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);
    setup=covered_setup();EXPECT_TRUE(setup.stop_token().live());
}

TEST_F(AuthenticatedReceiptCoverageV3, InvalidMigrationPolicyLeavesTheExistingSourceExactlyReopenable) {
    open();authorize();ASSERT_EQ(setup.receive(frame(entry())).ids().size(),1u);setup.close_on_io();setup={};
    const auto before=all_state(),global=global_state();auto changed=covered_policy();changed["epoch"]=relay_uuid(5999);
    EXPECT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),changed.dump()),4);EXPECT_FALSE(last_bridge_error().empty());EXPECT_EQ(all_state(),before);
    auto stale=policy();stale["epoch"]=relay_uuid(5999);auto corresponding=covered_policy();corresponding["epoch"]=relay_uuid(5999);
    EXPECT_EQ(ref->migrate_relay_receipt_coverage(stale.dump(),corresponding.dump()),4);EXPECT_FALSE(last_bridge_error().empty());EXPECT_EQ(all_state(),before);
    setup=open(policy(),connection(2));ASSERT_TRUE(setup.valid())<<last_bridge_error();authorize();
    EXPECT_EQ(setup.receive(frame(entry())).ids().size(),1u);EXPECT_EQ(global_state(),global);
}

TEST_F(AuthenticatedReceiptCoverageV3, MigrationDisposesActualOldReadyCapsuleButPreservesBindingHighWaterAndGlobalData) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    const auto d=description(setup);auto request_frame=request(d);auto& q=std::get<ready_wire::request>(request_frame.body);
    q.receipts={{e.global_id,"app",{{e.table_name,e.global_row_id}}}};seal(request_frame,d);
    const auto offered=lease(setup,request_frame,d,"prepare",1000);ASSERT_GT(std::stoull(offered.at("frames").get<std::string>()),0u);
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),1);ASSERT_GT(count("_lattice_canonical_ready_frame"),0);
    const auto binding=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");ASSERT_EQ(binding.size(),1u);
    auto expected=global_state();setup.close_on_io();setup={};
    ASSERT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),1)<<last_bridge_error();
    expected.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};EXPECT_EQ(global_state(),expected);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),binding);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    setup=covered_setup();EXPECT_EQ(global_state(),expected);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),binding);
}

class AuthenticatedReceiptMigrationClosure:public AuthenticatedReceiptCoverageV3 {
protected:
    void recreate_owner() {
        if(owner||ref)throw std::runtime_error("migration fixture requires complete prior owner release");
        swift_configuration c(file.str(),std::make_shared<immediate_scheduler>());c.audit_retention_seconds=0;c.busy_timeout_ms=100;
#if LATTICE_HAS_FRT
        ref.reset(swift_lattice_ref::create(c,{relay_schema()}));
#else
        ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(c,{relay_schema()}));
#endif
        if(!ref)throw std::runtime_error("replacement migration ref unavailable");
        owner=swift_lattice_ref::shared_for_lattice(ref->get());
        if(!owner)throw std::runtime_error("replacement migration owner unavailable");
        if(auto* n=instance_registry::instance().get_or_create_notifier(file.str()))n->stop_listening();
        // Reopen starts with ordinary connection durability. Migration is
        // deliberately forbidden from changing it while settling source state.
        owner->db().execute("PRAGMA main.synchronous=FULL");
        ASSERT_EQ(std::get<int64_t>(owner->db().query("PRAGMA main.synchronous").at(0).at("synchronous")),2);
    }
    static json migration_cell_summary(const column_value_t* value) {
        if(!value)return {{"missing",true}};
        json result={{"type",value->index()}};
        if(const auto* text=std::get_if<std::string>(value)){result["bytes"]=text->size();result["prefix"]=text->substr(0,96);}
        else if(const auto* bytes=std::get_if<std::vector<uint8_t>>(value)){result["bytes"]=bytes->size();std::string hex;const char* digits="0123456789abcdef";
            for(size_t n=0;n<std::min<size_t>(16,bytes->size());++n){hex+=digits[(*bytes)[n]>>4];hex+=digits[(*bytes)[n]&15];}result["prefixHex"]=std::move(hex);}
        else if(const auto* number=std::get_if<int64_t>(value))result["value"]=*number;
        else if(const auto* number=std::get_if<double>(value))result["value"]=*number;
        else result["value"]=nullptr;
        return result;
    }
    // Evaluated only by a failed original whole-state assertion. This is an
    // explicitly labeled fresh diagnostic sample, never the comparison oracle.
    std::string migration_state_difference(const Snapshot& expected) {
        try {
            const auto actual=all_state();json report={{"diagnosticResampleEqual",actual==expected},{"tables",json::array()}};
            std::set<std::string> tables;for(const auto& [table,_]:expected)tables.insert(table);for(const auto& [table,_]:actual)tables.insert(table);
            size_t changed=0;
            for(const auto& table:tables){const auto e=expected.find(table),a=actual.find(table);
                if(e!=expected.end()&&a!=actual.end()&&e->second==a->second)continue;
                ++changed;if(report["tables"].size()>=8)continue;
                const auto en=e==expected.end()?0:e->second.size(),an=a==actual.end()?0:a->second.size();
                json difference={{"table",table},{"expectedPresent",e!=expected.end()},{"actualPresent",a!=actual.end()},
                    {"expectedRows",en},{"actualRows",an},{"cells",json::array()}};
                for(size_t row=0;row<std::max(en,an);++row){const auto* er=row<en?&e->second[row]:nullptr;const auto* ar=row<an?&a->second[row]:nullptr;
                    if(er&&ar&&*er==*ar)continue;difference["firstDifferentRow"]=row;
                    std::set<std::string> columns;if(er)for(const auto& [column,_]:*er)columns.insert(column);if(ar)for(const auto& [column,_]:*ar)columns.insert(column);
                    for(const auto& column:columns){const auto* ev=er&&er->count(column)?&er->at(column):nullptr;const auto* av=ar&&ar->count(column)?&ar->at(column):nullptr;
                        if(ev&&av&&*ev==*av)continue;
                        if(difference["cells"].size()>=4){difference["moreDifferentCells"]=true;break;}
                        difference["cells"].push_back({{"column",column},{"expected",migration_cell_summary(ev)},{"actual",migration_cell_summary(av)}});
                    }break;
                }report["tables"].push_back(std::move(difference));
            }
            report["differentTables"]=changed;return report.dump();
        }catch(const std::exception& error){return json{{"diagnosticError",std::string(error.what()).substr(0,256)}}.dump();}
    }
    void require_pending_without_sql(const Snapshot& expected) {
        const auto statements=database::thread_statement_count();
        const auto result=ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump());
        EXPECT_EQ(database::thread_statement_count(),statements);
        EXPECT_EQ(result,2)<<last_bridge_error();EXPECT_TRUE(last_bridge_error().empty());
        EXPECT_EQ(all_state(),expected)<<migration_state_difference(expected);
    }
};

struct ReceiptMigrationCommitDenial {
    static thread_local ReceiptMigrationCommitDenial* active;
    int hits=0;
    detail::canonical_upstream_test_hooks::authorizer_fault fault;
    const detail::canonical_upstream_test_hooks::authorizer_fault* previous;
    ReceiptMigrationCommitDenial* prior;
    explicit ReceiptMigrationCommitDenial(database& db):
        fault{detail::canonical_writer_custody_test_access::fault_handle(db),deny},
        previous(detail::canonical_retention_test_hooks::fault),prior(active) {
        active=this;detail::canonical_retention_test_hooks::fault=&fault;
    }
    ~ReceiptMigrationCommitDenial(){detail::canonical_retention_test_hooks::fault=previous;active=prior;}
    ReceiptMigrationCommitDenial(const ReceiptMigrationCommitDenial&)=delete;
    ReceiptMigrationCommitDenial& operator=(const ReceiptMigrationCommitDenial&)=delete;
    static int deny(int action,const char* operation,const char*,const char* origin)noexcept {
        if(active&&!origin&&action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"COMMIT")==0) {
            ++active->hits;return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
};
thread_local ReceiptMigrationCommitDenial* ReceiptMigrationCommitDenial::active=nullptr;

TEST_F(AuthenticatedReceiptMigrationClosure, ReplacementOwnerMigrationWaitsForEveryActualDescribeResultAndStopFence) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    auto held=invoke(setup,control("describe"));ASSERT_EQ(held.status_code(),1);ASSERT_TRUE(held.publishable());
    auto copied=held;auto stopped=setup.stop_token();EXPECT_FALSE(stopped.drained());
    const auto before=all_state();auto expected=global_state();
    const auto physical=owner->db().physical_identity("main",{},true);ASSERT_TRUE(physical);
    const std::weak_ptr<lattice::swift_lattice> prior_owner=owner;
    setup.close_on_io();setup={};owner->close();owner.reset();ref.reset();
    ASSERT_TRUE(prior_owner.expired());EXPECT_FALSE(held.publishable());EXPECT_FALSE(copied.publishable());EXPECT_FALSE(stopped.live());
    recreate_owner();const auto replacement=owner->db().physical_identity("main",{},true);ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->device,physical->device);EXPECT_EQ(replacement->inode,physical->inode);EXPECT_EQ(all_state(),before)<<migration_state_difference(before);
    require_pending_without_sql(before);
    held={};EXPECT_FALSE(stopped.drained());require_pending_without_sql(before);
    copied={};EXPECT_TRUE(stopped.drained());
    // A drained, stopped fence still retains this source's capacity domain.
    // Releasing the final actual fence is required before changing its recipe.
    require_pending_without_sql(before);stopped={};
    ASSERT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),1)<<last_bridge_error();
    expected.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};EXPECT_EQ(global_state(),expected);
    EXPECT_EQ(receipts(),before.at("_lattice_canonical_receipt"));EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    setup=covered_setup();EXPECT_TRUE(setup.stop_token().live());EXPECT_EQ(global_state(),expected);
}

TEST_F(AuthenticatedReceiptMigrationClosure, ActualMigrationCommitDenialRestoresWholeV2CapsuleAndRetryPreservesGlobalHistory) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    const auto d=description(setup);auto requested=request(d);auto& q=std::get<ready_wire::request>(requested.body);
    q.receipts={{e.global_id,"app",{{e.table_name,e.global_row_id}}}};seal(requested,d);
    const auto offered=lease(setup,requested,d,"prepare",1000);ASSERT_GT(std::stoull(offered.at("frames").get<std::string>()),0u);
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),1);ASSERT_GT(count("_lattice_canonical_ready_frame"),0);
    const auto binding=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");ASSERT_EQ(binding.size(),1u);
    setup.close_on_io();setup={};const auto before=all_state();auto expected=global_state();
    {
        ReceiptMigrationCommitDenial fault(owner->db());
        const auto result=ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump());
        const auto error=last_bridge_error();EXPECT_EQ(fault.hits,1);EXPECT_EQ(result,4);EXPECT_FALSE(error.empty());
    }
    EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(all_state(),before);
    EXPECT_EQ(std::get<int64_t>(before.at("_lattice_canonical_store").at(0).at("version")),2);
    ASSERT_EQ(ref->migrate_relay_receipt_coverage(policy().dump(),covered_policy().dump()),1)<<last_bridge_error();
    expected.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};EXPECT_EQ(global_state(),expected);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),binding);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    const auto migrated=all_state();auto old=open(policy(),connection(2));EXPECT_FALSE(old.valid());EXPECT_EQ(all_state(),migrated);
    setup=covered_setup("app",3);EXPECT_TRUE(setup.stop_token().live());EXPECT_EQ(global_state(),expected);
}


// The only rendezvous is before either actual API call, outside all production
// locks/callbacks. SQL admission remains serialized by the real owned writer.
struct ReceiptIngressWorkers {
    struct Outcome {int32_t status=0;bool publishable=false;std::vector<std::string> ids;std::exception_ptr error;};
    std::atomic<unsigned> arrived{0},completed{0};std::atomic<bool> released{false},cancelled{false},timed_out{false};
    Outcome results[2];std::vector<std::thread> threads;
    ReceiptIngressWorkers(){threads.reserve(2);}
    ~ReceiptIngressWorkers(){cancelled.store(true);released.store(true);for(auto& worker:threads)if(worker.joinable())worker.join();}
    void launch(size_t index,relay_recovery_setup actual,std::string raw){
        threads.emplace_back([this,index,actual=std::move(actual),raw=std::move(raw)]{
            ++arrived;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
            while(!released.load()&&std::chrono::steady_clock::now()<deadline)std::this_thread::yield();
            if(!released.load())timed_out.store(true);
            if(released.load()&&!cancelled.load())try{auto result=actual.receive(raw);results[index].status=result.status_code();
                results[index].publishable=result.publishable();results[index].ids=result.take_ids();}catch(...){results[index].error=std::current_exception();}
            ++completed;
        });
    }
    bool wait(const std::atomic<unsigned>& count,unsigned expected){const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(count.load()!=expected&&std::chrono::steady_clock::now()<deadline)std::this_thread::yield();return count.load()==expected;}
    void join(){for(auto& worker:threads)if(worker.joinable())worker.join();}
};
struct ReceiptIngressCommitDenial {
    static thread_local ReceiptIngressCommitDenial* active;
    unsigned hits=0;detail::canonical_upstream_test_hooks::authorizer_fault fault;
    const detail::canonical_upstream_test_hooks::authorizer_fault* previous;ReceiptIngressCommitDenial* prior;
    explicit ReceiptIngressCommitDenial(database& db):fault{detail::canonical_writer_custody_test_access::fault_handle(db),deny},
        previous(detail::canonical_upstream_test_hooks::fault),prior(active){active=this;detail::canonical_upstream_test_hooks::fault=&fault;}
    ~ReceiptIngressCommitDenial(){detail::canonical_upstream_test_hooks::fault=previous;active=prior;}
    static int deny(int action,const char* operation,const char*,const char* origin)noexcept{
        if(active&&!origin&&action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"COMMIT")==0){++active->hits;return SQLITE_DENY;}return SQLITE_OK;
    }
};
thread_local ReceiptIngressCommitDenial* ReceiptIngressCommitDenial::active=nullptr;

TEST_F(AuthenticatedReceiptCoverageV3, ConcurrentApiStartsAcceptOneGlobalOriginalThroughTwoAuthorizedNamespaces) {
    setup=covered_setup();auto c=connection(2);c["channel"]="other-channel";
    auto other=open(covered_policy("other"),c,std::make_shared<RelayRouteState>());ASSERT_TRUE(other.valid());
    ASSERT_TRUE(other.finish_authorization(covered_answer(other).dump()));
    const auto e=identified(entry());const auto raw=frame(e);ASSERT_EQ(count("_lattice_canonical_receipt"),0);
    ReceiptIngressWorkers workers;workers.launch(0,setup,raw);workers.launch(1,other,raw);
    ASSERT_TRUE(workers.wait(workers.arrived,2));workers.released.store(true);
    ASSERT_TRUE(workers.wait(workers.completed,2));workers.join();EXPECT_FALSE(workers.timed_out.load());
    for(const auto& result:workers.results){EXPECT_FALSE(result.error);EXPECT_EQ(result.status,1);EXPECT_TRUE(result.publishable);EXPECT_EQ(result.ids,std::vector<std::string>{e.global_id});}
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("AuditLog"),1);EXPECT_EQ(count("_lattice_canonical_receipt"),1);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);
    // The first INSERT records its model touch, then its original receipt.
    // The other namespace adds coverage without advancing either position.
    const auto head=owner->db().query("SELECT head FROM _lattice_canonical_store");ASSERT_EQ(head.size(),1u);EXPECT_EQ(std::get<int64_t>(head[0].at("head")),2);
    const auto accepted=receipts();ASSERT_EQ(accepted.size(),1u);EXPECT_EQ(std::get<int64_t>(accepted[0].at("position")),2);
    const auto touches=owner->db().query("SELECT position FROM _lattice_canonical_touch ORDER BY relation,identity");
    ASSERT_EQ(touches.size(),1u);EXPECT_EQ(std::get<int64_t>(touches[0].at("position")),1);
    const auto coverage_state=owner->db().query("SELECT mutation,origins,cells FROM _lattice_canonical_receipt_profile");
    ASSERT_EQ(coverage_state.size(),1u);EXPECT_EQ(std::get<int64_t>(coverage_state[0].at("mutation")),2);
    EXPECT_EQ(std::get<int64_t>(coverage_state[0].at("origins")),1);EXPECT_EQ(std::get<int64_t>(coverage_state[0].at("cells")),2);
    const auto cells=coverage();ASSERT_EQ(cells.size(),2u);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(cells[0].at("namespace_id")),bytes("app"));EXPECT_EQ(std::get<std::vector<uint8_t>>(cells[1].at("namespace_id")),bytes("other"));
    for(const auto& cell:cells)EXPECT_EQ(std::get<std::vector<uint8_t>>(cell.at("original_id")),bytes(e.global_id));
    EXPECT_TRUE(setup.stop_token().drained());EXPECT_TRUE(other.stop_token().drained());
    const auto complete=all_state();EXPECT_EQ(setup.receive(raw).take_ids(),std::vector<std::string>{e.global_id});
    EXPECT_EQ(other.receive(raw).take_ids(),std::vector<std::string>{e.global_id});EXPECT_EQ(all_state(),complete);
}
TEST_F(AuthenticatedReceiptCoverageV3, BothActualCommitAttemptsRollbackNewOriginAndLaterChannelCoverageBeforeRetry) {
    setup=covered_setup();auto c=connection(2);c["channel"]="other-channel";
    auto other=open(covered_policy("other"),c,std::make_shared<RelayRouteState>());ASSERT_TRUE(other.valid());
    ASSERT_TRUE(other.finish_authorization(covered_answer(other).dump()));const auto e=identified(entry());const auto raw=frame(e);
    const auto empty=all_state();
    {
        ReceiptIngressCommitDenial fault(owner->db());const auto failed=other.receive(raw);
        EXPECT_EQ(fault.hits,2u);EXPECT_EQ(failed.status_code(),1);EXPECT_TRUE(failed.ids().empty());
    }
    EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(all_state(),empty);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_EQ(count("_lattice_canonical_receipt"),0);EXPECT_TRUE(coverage().empty());
    ASSERT_EQ(other.receive(raw).take_ids(),std::vector<std::string>{e.global_id});const auto one=all_state(),global=global_state();
    ASSERT_EQ(coverage().size(),1u);ASSERT_EQ(count("_lattice_canonical_receipt_origin"),1);
    {
        ReceiptIngressCommitDenial fault(owner->db());const auto failed=setup.receive(raw);
        EXPECT_EQ(fault.hits,2u);EXPECT_EQ(failed.status_code(),1);EXPECT_TRUE(failed.ids().empty());
    }
    EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(all_state(),one);EXPECT_EQ(global_state(),global);
    ASSERT_EQ(setup.receive(raw).take_ids(),std::vector<std::string>{e.global_id});EXPECT_EQ(global_state(),global);
    EXPECT_EQ(coverage().size(),2u);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("AuditLog"),1);
    const auto complete=all_state();EXPECT_EQ(other.receive(raw).take_ids(),std::vector<std::string>{e.global_id});EXPECT_EQ(all_state(),complete);
}
TEST_F(AuthenticatedReceiptCoverageV3, OrdinaryIngressCountAndWireByteLimitsRefuseWithoutChangingDurableCapacityOrState) {
    setup=covered_setup();const auto actual=description(setup);ASSERT_EQ(actual.at("upload").at("maximumEntries"),256);
    ASSERT_EQ(actual.at("upload").at("maximumWireBytes"),1048576);ASSERT_EQ(actual.at("upload").at("maximumScalarBytes"),65536);
    ASSERT_EQ(actual.at("upload").at("maximumDeletes"),256);
    const auto profile=owner->db().query("SELECT max_origins,max_origin_bytes,max_cells,max_cell_bytes FROM _lattice_canonical_receipt_profile");ASSERT_EQ(profile.size(),1u);
    EXPECT_EQ(std::get<int64_t>(profile[0].at("max_origins")),65536);EXPECT_EQ(std::get<int64_t>(profile[0].at("max_origin_bytes")),67108864);
    EXPECT_EQ(std::get<int64_t>(profile[0].at("max_cells")),4194304);EXPECT_EQ(std::get<int64_t>(profile[0].at("max_cell_bytes")),1610612736);
    const auto before=all_state();std::vector<audit_log_entry> too_many;too_many.reserve(257);
    for(unsigned n=1;n<=257;++n)too_many.push_back(identified(entry(n)));
    const auto count_frame=server_sent_event::make_audit_log(too_many).to_json();ASSERT_LT(count_frame.size(),1048576u);
    const auto count_refusal=setup.receive(count_frame);EXPECT_EQ(count_refusal.status_code(),4);EXPECT_TRUE(count_refusal.ids().empty());EXPECT_EQ(all_state(),before);
    std::vector<audit_log_entry> too_wide;too_wide.reserve(32);
    for(unsigned n=1;n<=32;++n)too_wide.push_back(identified(entry(1000+n,std::string(40000,'x'))));
    const auto byte_frame=server_sent_event::make_audit_log(too_wide).to_json();ASSERT_GT(byte_frame.size(),1048576u);ASSERT_LE(too_wide.size(),256u);
    const auto byte_refusal=setup.receive(byte_frame);EXPECT_EQ(byte_refusal.status_code(),4);EXPECT_TRUE(byte_refusal.ids().empty());EXPECT_EQ(all_state(),before);
    EXPECT_EQ(owner->db().query("SELECT max_origins,max_origin_bytes,max_cells,max_cell_bytes FROM _lattice_canonical_receipt_profile"),profile);
    EXPECT_TRUE(setup.stop_token().live());EXPECT_TRUE(setup.stop_token().drained());
    const auto valid=identified(entry(800));ASSERT_EQ(setup.receive(frame(valid)).take_ids(),std::vector<std::string>{valid.global_id});
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);EXPECT_EQ(coverage().size(),1u);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
struct AddressedReadAuthorizerFault {
    using fault_type=detail::canonical_upstream_test_hooks::authorizer_fault;
    static thread_local AddressedReadAuthorizerFault* active;
    bool deny_commit;unsigned commits=0;
    fault_type fault;const fault_type** slot;const fault_type* previous;AddressedReadAuthorizerFault* prior;
    AddressedReadAuthorizerFault(database& db,bool upstream,bool deny=false):deny_commit(deny),
        fault{detail::canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        slot(upstream?&detail::canonical_upstream_test_hooks::fault:&detail::canonical_retention_test_hooks::fault),
        previous(*slot),prior(active){active=this;*slot=&fault;}
    ~AddressedReadAuthorizerFault(){*slot=previous;active=prior;}
    static int restrict_action(int action,const char* operation,const char*,const char* origin)noexcept {
        if(active&&!origin&&action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"COMMIT")==0){
            ++active->commits;if(active->deny_commit)return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
};
thread_local AddressedReadAuthorizerFault* AddressedReadAuthorizerFault::active=nullptr;
struct AddressedReadInvalidationHook {
    std::shared_ptr<lattice::swift_lattice> owner;uint64_t token;
    ~AddressedReadInvalidationHook(){owner->remove_invalidation_hook(token);}
};

TEST_F(AuthenticatedReadySession, EveryAuthenticatedReadAuditsAllCapsulesOnceAndReturnsExactStoredFrames) {
    setup=admitted();auto other=admitted(2);const auto a=entry(41,"first"),b=entry(42,"second");
    ASSERT_EQ(setup.receive(frame(a)).ids().size(),1u);ASSERT_EQ(other.receive(frame(b)).ids().size(),1u);
    const auto da=description(setup),db=description(other);auto qa=request(da),qb=request(db);
    for(auto* q:{&qa,&qb})std::get<ready_wire::request>(q->body).budget.items_per_page=1;
    std::get<ready_wire::request>(qa.body).receipts={{a.global_id,std::string("app"),{{a.table_name,a.global_row_id}}}};
    std::get<ready_wire::request>(qb.body).receipts={{b.global_id,std::string("app"),{{b.table_name,b.global_row_id}}}};
    seal(qa,da);seal(qb,db);const auto la=lease(setup,qa,da),lb=lease(other,qb,db);
    const auto before=exact_source();const auto stored=owner->db().query("SELECT frame_index,data FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),2);uint64_t bytes=0;
    for(const auto& row:stored)bytes+=std::get<std::vector<uint8_t>>(row.at("data")).size();
    for(const auto* selected:{&setup,&other}) {
        const auto& d=selected==&setup?da:db;const auto& offered=selected==&setup?la:lb;const auto& q=selected==&setup?qa:qb;
        const auto frames=std::stoull(offered.at("frames").get<std::string>());ASSERT_GT(frames,3u);
        for(uint64_t index=0;index<frames;++index) {
            const auto result=read(*selected,offered,index);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable())<<read_diagnostic(result);
            EXPECT_EQ(last_read_trace.full_audits,1u);EXPECT_EQ(last_read_trace.audited_frames,stored.size());EXPECT_EQ(last_read_trace.audited_bytes,bytes);
            EXPECT_EQ(last_read_trace.positive_receipt_lookups,2u);EXPECT_EQ(last_read_trace.addressed_frames,1u);
            EXPECT_FALSE(last_read_trace.primary_error||last_read_trace.cleanup_error||last_read_trace.postcommit_error||last_read_trace.notification_error);
            unsigned matches=0;
            for(const auto& row:stored)if(std::get<int64_t>(row.at("frame_index"))==static_cast<int64_t>(index)) {
                const auto& data=std::get<std::vector<uint8_t>>(row.at("data"));auto expected=ready_wire::decode(std::string(data.begin(),data.end()),codec(d));
                if(expected.logical!=q.logical)continue;++matches;expected.route_generation=q.route_generation;
                EXPECT_EQ(result.wire(),ready_wire::encode(expected,codec(d)));
            }
            EXPECT_EQ(matches,1u);EXPECT_EQ(exact_source(),before);
        }
    }
}

TEST_F(AuthenticatedReceiptCoverageV3, RegisteredNamespaceReadAuditsBothPositiveCoverageCapsulesOnce) {
    auto policy_a=covered_policy();policy_a["maximumAuthorizationMilliseconds"]=600000;
    auto policy_b=policy_a;policy_b["receiptNamespace"]="other";
    setup=open(policy_a,connection());auto other=open(policy_b,connection(2),std::make_shared<RelayRouteState>());
    for(const auto* actual:{&setup,&other}){ASSERT_TRUE(actual->valid());auto answer=covered_answer(*actual);answer["validForMilliseconds"]=600000;
        ASSERT_TRUE(actual->finish_authorization(answer.dump()));}
    const auto e=identified(entry(51));
    ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    ASSERT_EQ(other.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});ASSERT_EQ(coverage().size(),2u);
    const auto da=description(setup),db=description(other);auto qa=request(da),qb=request(db);
    for(auto* f:{&qa,&qb}) {
        const auto ns=f==&qa?std::string("app"):std::string("other");auto& q=std::get<ready_wire::request>(f->body);
        f->version=3;q.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};q.receipt_namespace=ns;
        q.receipts={{e.global_id,ns,{{e.table_name,e.global_row_id}},e.original_identity->digest}};
    }
    seal(qa,da);seal(qb,db);const auto la=lease(setup,qa,da),lb=lease(other,qb,db);const auto before=all_state();
    const auto stored=owner->db().query("SELECT data FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    uint64_t bytes=0;for(const auto& row:stored)bytes+=std::get<std::vector<uint8_t>>(row.at("data")).size();
    for(const auto* selected:{&setup,&other}) {
        const auto& d=selected==&setup?da:db;const auto& offered=selected==&setup?la:lb;const auto ns=selected==&setup?"app":"other";
        size_t positives=0;const auto frames=std::stoull(offered.at("frames").get<std::string>());
        for(uint64_t index=0;index<frames;++index) {
            const auto result=read(*selected,offered,index);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable())<<read_diagnostic(result);
            EXPECT_EQ(last_read_trace.full_audits,1u);EXPECT_EQ(last_read_trace.audited_frames,stored.size());EXPECT_EQ(last_read_trace.audited_bytes,bytes);
            EXPECT_EQ(last_read_trace.positive_receipt_lookups,2u);const auto decoded=decode_read(result,d);EXPECT_EQ(decoded.version,3u);
            if(const auto* page=std::get_if<ready_wire::receipt_page>(&decoded.body))for(const auto& item:page->items) {
                const auto* positive=std::get_if<ready_wire::committed>(&item.value);ASSERT_NE(positive,nullptr);++positives;
                EXPECT_EQ(positive->namespace_id,ns);EXPECT_EQ(item.original_id,e.global_id);EXPECT_EQ(item.operation_digest,e.original_identity->digest);EXPECT_FALSE(item.legacy_unbound);
            }
            EXPECT_EQ(all_state(),before);
        }
        EXPECT_EQ(positives,1u);
    }
}

TEST_F(AuthenticatedReadySession, OffPageCorruptionBeforeAnotherReadStillFailsItsWholePreAudit) {
    setup=admitted();const auto e=entry(61);ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    const auto d=description(setup);const auto offered=lease(setup,request(d),d);const auto good=read(setup,offered,0);
    ASSERT_TRUE(good.publishable());EXPECT_EQ(last_read_trace.full_audits,1u);const auto before=exact_source();
    const auto tail=owner->db().query("SELECT binding,frame_index,data FROM _lattice_canonical_ready_frame ORDER BY frame_index DESC LIMIT 1").at(0);
    ASSERT_GT(std::get<int64_t>(tail.at("frame_index")),0);
    const auto replace=[&](const std::vector<uint8_t>& data) {
        // Deliberate external SQL corruption before the next call. Restore the
        // exact guard so the oracle must inspect off-page content, not DDL drift.
        database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.begin_transaction();raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");
        raw.execute("UPDATE _lattice_canonical_ready_frame SET data=? WHERE binding=? AND frame_index=?",{data,tail.at("binding"),tail.at("frame_index")});
        raw.execute(guard);raw.commit();
    };
    replace(std::vector<uint8_t>{'{','}'});const auto corrupt=exact_source();const auto refused=read(setup,offered,0);
    ASSERT_EQ(refused.status_code(),1);const auto answer=json::parse(refused.wire());EXPECT_EQ(answer.at("frameAvailable"),false);
    EXPECT_NE(answer.at("settlement").at("state"),"committed");EXPECT_EQ(last_read_trace.full_audits,1u);EXPECT_EQ(last_read_trace.addressed_frames,0u);
    EXPECT_EQ(exact_source(),corrupt);replace(std::get<std::vector<uint8_t>>(tail.at("data")));EXPECT_EQ(exact_source(),before);
    const auto restored=read(setup,offered,0);EXPECT_TRUE(restored.publishable());EXPECT_EQ(restored.wire(),good.wire());EXPECT_EQ(last_read_trace.full_audits,1u);
}

TEST_F(AuthenticatedReadySession, EitherAuthorizerFaultUsesBothAuditsAndCommitRefusalReturnsNoFrame) {
    setup=admitted();const auto d=description(setup);const auto offered=lease(setup,request(d),d);const auto baseline=read(setup,offered,0);
    ASSERT_TRUE(baseline.publishable());const auto before=exact_source();const auto frames=std::stoull(offered.at("frames").get<std::string>());
    for(const bool upstream:{false,true}) {
        SCOPED_TRACE(upstream);
        {AddressedReadAuthorizerFault fault(owner->db(),upstream);const auto result=read(setup,offered,0);
            EXPECT_EQ(result.wire(),baseline.wire());EXPECT_TRUE(result.publishable());EXPECT_EQ(fault.commits,1u);
            EXPECT_EQ(last_read_trace.full_audits,2u);EXPECT_EQ(last_read_trace.audited_frames,2*frames);}
        {AddressedReadAuthorizerFault fault(owner->db(),upstream,true);const auto result=read(setup,offered,0);
            ASSERT_EQ(result.status_code(),1);const auto answer=json::parse(result.wire());EXPECT_EQ(answer.at("frameAvailable"),false);
            EXPECT_EQ(answer.at("settlement").at("state"),"rolledBack");EXPECT_EQ(answer.at("settlement").at("primaryError"),true);
            EXPECT_EQ(fault.commits,1u);EXPECT_EQ(last_read_trace.full_audits,2u);EXPECT_EQ(last_read_trace.audited_frames,2*frames);}
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(exact_source(),before);
        const auto retry=read(setup,offered,0);EXPECT_EQ(retry.wire(),baseline.wire());EXPECT_TRUE(retry.publishable());EXPECT_EQ(last_read_trace.full_audits,1u);
    }
}

TEST_F(AuthenticatedReadySession, ReadPostcommitObserverErrorKeepsTheExistingCommittedFrameContract) {
    setup=admitted();const auto d=description(setup);const auto offered=lease(setup,request(d),d);const auto baseline=read(setup,offered,0);
    ASSERT_TRUE(baseline.publishable());const auto before=exact_source();unsigned calls=0;
    AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++calls;throw std::runtime_error("addressed read observer");})};
    const auto result=read(setup,offered,0);EXPECT_EQ(calls,1u);EXPECT_EQ(result.status_code(),1);EXPECT_TRUE(result.publishable());EXPECT_EQ(result.wire(),baseline.wire());
    EXPECT_EQ(last_read_trace.full_audits,1u);EXPECT_EQ(last_read_trace.settlement,static_cast<int>(detail::recovery_install_state::committed));
    EXPECT_TRUE(last_read_trace.postcommit_error);EXPECT_FALSE(last_read_trace.primary_error||last_read_trace.cleanup_error||last_read_trace.notification_error);
    EXPECT_EQ(exact_source(),before);
}

TEST_F(AuthenticatedReadySession, ReadPostcommitRevocationStillPreventsPublicationOfCommittedBytes) {
    setup=admitted();const auto d=description(setup);const auto offered=lease(setup,request(d),d);const auto baseline=read(setup,offered,0);
    ASSERT_TRUE(baseline.publishable());const auto before=exact_source();const auto stop=setup.stop_token();unsigned calls=0;
    AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++calls;stop.stop();})};
    const auto result=read(setup,offered,0);EXPECT_EQ(calls,1u);EXPECT_EQ(result.status_code(),1);EXPECT_FALSE(result.publishable());EXPECT_FALSE(baseline.publishable());
    EXPECT_EQ(result.wire(),baseline.wire());EXPECT_EQ(last_read_trace.full_audits,1u);
    EXPECT_EQ(last_read_trace.settlement,static_cast<int>(detail::recovery_install_state::committed));EXPECT_EQ(exact_source(),before);
}

TEST_F(AuthenticatedReadySession, ReadPostcommitLeaseExpiryStillPreventsPublicationAndLaterRead) {
    setup=admitted();const auto d=description(setup);const auto offered=lease(setup,request(d),d,"prepare",1000);const auto baseline=read(setup,offered,0);
    ASSERT_TRUE(baseline.publishable());const auto before=exact_source();bool reached_expiry=false;unsigned calls=0;
    {
        AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){
            ++calls;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
            while(baseline.publishable()&&std::chrono::steady_clock::now()<end)std::this_thread::yield();reached_expiry=!baseline.publishable();
        })};
        const auto result=read(setup,offered,0);EXPECT_EQ(calls,1u);ASSERT_TRUE(reached_expiry);EXPECT_EQ(result.status_code(),1);
        EXPECT_FALSE(result.publishable());EXPECT_EQ(result.wire(),baseline.wire());EXPECT_EQ(last_read_trace.full_audits,1u);
        EXPECT_EQ(last_read_trace.settlement,static_cast<int>(detail::recovery_install_state::committed));
    }
    const auto expired=read(setup,offered,0);EXPECT_NE(expired.status_code(),1);EXPECT_EQ(last_read_trace.full_audits,0u);EXPECT_EQ(exact_source(),before);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
#include <condition_variable>
namespace {
struct ReadyLifecycleGate {
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false,timed_out=false;
    void wait(){std::unique_lock lock(mutex);entered=true;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
    bool arrived(){std::lock_guard lock(mutex);return entered;}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
    bool timedOut(){std::lock_guard lock(mutex);return timed_out;}
};
struct ReadyLifecycleEvents {
    std::mutex mutex;std::function<void(const char*)> callback;
    std::atomic<unsigned> settled{0},published{0};
    void observe(const char* point){
        if(std::strcmp(point,"maintenance-settled")==0)++settled;
        if(std::strcmp(point,"maintenance-published")==0)++published;
        std::function<void(const char*)> current;{std::lock_guard lock(mutex);current=callback;}if(current)current(point);
    }
    void set(std::function<void(const char*)> next){std::lock_guard lock(mutex);callback=std::move(next);}
};
struct ReadyLifecycleFault {
    static thread_local ReadyLifecycleFault* current;
    bool ignore_charge;unsigned hits=0;
    detail::canonical_upstream_test_hooks::authorizer_fault fault;
    const detail::canonical_upstream_test_hooks::authorizer_fault* prior;
    ReadyLifecycleFault(database& db,bool ignore=false):ignore_charge(ignore),fault{detail::canonical_writer_custody_test_access::fault_handle(db),apply},prior(detail::canonical_retention_test_hooks::fault){current=this;detail::canonical_retention_test_hooks::fault=&fault;}
    ~ReadyLifecycleFault(){detail::canonical_retention_test_hooks::fault=prior;current=nullptr;}
    static int apply(int action,const char* table,const char* column,const char* origin)noexcept {
        if(origin||!table)return SQLITE_OK;auto& value=*current;
        if(!value.ignore_charge&&action==SQLITE_TRANSACTION&&std::strcmp(table,"COMMIT")==0){++value.hits;return SQLITE_DENY;}
        if(value.ignore_charge&&action==SQLITE_UPDATE&&std::strcmp(table,"_lattice_canonical_ready_profile")==0&&column&&std::strcmp(column,"charged")==0){++value.hits;return SQLITE_IGNORE;}
        return SQLITE_OK;
    }
};
thread_local ReadyLifecycleFault* ReadyLifecycleFault::current=nullptr;
class AuthenticatedReadyLifecycle:public AuthenticatedReadySession {
protected:
    std::shared_ptr<ReadyLifecycleEvents> events=std::make_shared<ReadyLifecycleEvents>();
    std::shared_ptr<const detail::authenticated_ready_maintenance_test_observation::probe> prior_probe;
    std::vector<std::shared_ptr<ReadyLifecycleGate>> gates;
    void SetUp()override {
        AuthenticatedReadySession::SetUp();auto probe=std::make_shared<detail::authenticated_ready_maintenance_test_observation::probe>();
        probe->owner=owner.get();probe->observed=[keep=events](const char* point){keep->observe(point);};
        prior_probe=detail::authenticated_ready_maintenance_test_observation::exchange(std::move(probe));
    }
    void TearDown()override {
        events->set({});for(const auto& gate:gates)gate->release();
        for(const auto& gate:gates)EXPECT_FALSE(gate->timedOut())<<"maintenance race gate timed out before release";
        detail::authenticated_ready_maintenance_test_observation::exchange(std::move(prior_probe));
        AuthenticatedReadySession::TearDown();
    }
    template<class F> bool until(F condition){const auto limit=std::chrono::steady_clock::now()+std::chrono::seconds(5);while(!condition()&&std::chrono::steady_clock::now()<limit)std::this_thread::sleep_for(std::chrono::milliseconds(2));return condition();}
    std::shared_ptr<ReadyLifecycleGate> gate(){auto value=std::make_shared<ReadyLifecycleGate>();gates.push_back(value);return value;}
    json lifecycle_policy(int64_t grace=10000){auto p=source_policy(true);p["readyProfile"]="bounded48MiBOrphanV1";p["orphanResumeGraceMilliseconds"]=grace;return p;}
    relay_recovery_setup lifecycle_setup(unsigned replica=1,int64_t grace=10000) {
        auto value=open(lifecycle_policy(grace),connection(replica),std::make_shared<RelayRouteState>());
        if(!value.valid())throw std::runtime_error("actual lifecycle setup failed: "+last_bridge_error());
        auto answer=outcome(value);answer["validForMilliseconds"]=600000;
        if(!value.finish_authorization(answer.dump()))throw std::runtime_error("actual lifecycle authorization failed");return value;
    }
    json lifecycle_command(const char* op,const ready_wire::frame& f,const json& d){auto value=command(op,f,d);value.erase("durationMilliseconds");return value;}
    json lifecycle_result(const relay_recovery_setup& actual,const char* op,const ready_wire::frame& f,const json& d){
        const auto response=invoke(actual,lifecycle_command(op,f,d));if(response.status_code()!=1||!response.publishable())throw std::runtime_error("actual lifecycle response unavailable: "+last_bridge_error());return json::parse(response.wire());
    }
    int64_t number(const char* sql){return std::get<int64_t>(owner->db().query(sql).at(0).begin()->second);}
    void reopen_owner(){
        setup.close_on_io();setup={};const std::weak_ptr<lattice::swift_lattice> prior=owner;
        owner->close();owner.reset();ref.reset();
        if(!until([&]{return prior.expired();}))throw std::runtime_error("closed lifecycle owner retained beyond bounded worker observation");
        AuthenticatedReadySession::SetUp();
        auto probe=std::make_shared<detail::authenticated_ready_maintenance_test_observation::probe>();probe->owner=owner.get();
        probe->observed=[keep=events](const char* point){keep->observe(point);};detail::authenticated_ready_maintenance_test_observation::exchange(std::move(probe));
    }
};
TEST_F(AuthenticatedReadyLifecycle, ProfileRequiresExplicitBoundedGraceAndLeavesOldNamesUnchanged) {
    const auto schema=owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name");
    auto invalid=lifecycle_policy();invalid.erase("orphanResumeGraceMilliseconds");EXPECT_FALSE(open(invalid,connection()).valid());
    for(const auto value:{0,3600001}){invalid=lifecycle_policy(value);EXPECT_FALSE(open(invalid,connection()).valid());}
    invalid=source_policy(true);invalid["orphanResumeGraceMilliseconds"]=1000;EXPECT_FALSE(open(invalid,connection()).valid());
    EXPECT_EQ(owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"),schema);
    setup=lifecycle_setup();const auto d=description(setup);EXPECT_EQ(d["profile"]["name"],"bounded48MiBOrphanV1");
    EXPECT_EQ(d["profile"]["orphanResumeGraceMilliseconds"],10000);EXPECT_EQ(d["profile"]["transfers"],8);EXPECT_EQ(d["profile"]["bindings"],1024);
    EXPECT_FALSE(open(source_policy(true),connection(2)).valid());EXPECT_FALSE(open(lifecycle_policy(9999),connection(2)).valid());
}
TEST_F(AuthenticatedReadyLifecycle, InspectUnstartedIsReadOnlyAndDiscardFencesDelayedPrepareWithExactHighWater) {
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);const auto before=exact_source();
    const auto unseen=lifecycle_result(setup,"inspect",q,d);EXPECT_EQ(unseen["lifecycle"]["state"],"unstarted");EXPECT_EQ(unseen["lifecycle"]["bindingHighWater"],"0");EXPECT_EQ(exact_source(),before);
    const auto disposed=lifecycle_result(setup,"discard",q,d);ASSERT_EQ(disposed["settlement"]["state"],"committed");EXPECT_EQ(disposed["lifecycle"]["state"],"terminal");
    EXPECT_EQ(disposed["lifecycle"]["requestDigest"],std::get<ready_wire::request>(q.body).request_digest);EXPECT_EQ(disposed["lifecycle"]["attemptID"],q.logical.attempt_id);
    EXPECT_EQ(disposed["lifecycle"]["sequence"],"1");EXPECT_EQ(disposed["lifecycle"]["bindingHighWater"],"1");EXPECT_EQ(disposed["lifecycle"]["namespaceID"],"app");
    EXPECT_EQ(disposed["lifecycle"]["replicaID"],d["peer"]["replicaID"]);EXPECT_EQ(disposed["lifecycle"]["receiverIncarnation"],q.logical.receiver_incarnation);
    EXPECT_EQ(disposed["lifecycle"]["channelIncarnation"],q.logical.channel_incarnation);EXPECT_EQ(disposed["lifecycle"]["channel"],q.logical.channel);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);EXPECT_EQ(count("_lattice_canonical_attempt"),0);
    EXPECT_EQ(number("SELECT charged FROM _lattice_canonical_ready_profile"),number("SELECT SUM(charge) FROM _lattice_canonical_ready_binding"));
    const auto terminal=exact_source();const auto delayed=invoke(setup,command("prepare",q,d));ASSERT_EQ(delayed.status_code(),1);EXPECT_FALSE(json::parse(delayed.wire())["leaseAvailable"].get<bool>());
    EXPECT_EQ(exact_source(),terminal);EXPECT_EQ(lifecycle_result(setup,"discard",q,d)["lifecycle"]["state"],"terminal");EXPECT_EQ(exact_source(),terminal);
    EXPECT_EQ(lifecycle_result(setup,"inspect",q,d)["lifecycle"]["state"],"terminal");
    auto next=request(d,2);EXPECT_TRUE(lease(setup,next,d)["leaseAvailable"].get<bool>());
    const auto stale=lifecycle_result(setup,"inspect",q,d);EXPECT_FALSE(stale.contains("lifecycle"));EXPECT_NE(stale["settlement"]["state"],"committed");
}
TEST_F(AuthenticatedReadyLifecycle, PrepareBeforeDiscardKeepsReadOnlyInspectionAndRejectsChangedActiveQ) {
    setup=lifecycle_setup();const auto d=description(setup);auto q=request(d);const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    std::get<ready_wire::request>(q.body).receipts={{e.global_id,"app",{{e.table_name,e.global_row_id}}}};seal(q,d);
    const auto offered=lease(setup,q,d);auto queued=read(setup,offered,0);ASSERT_TRUE(queued.publishable());const auto before=exact_source();const auto accepted=receipts();
    EXPECT_EQ(lifecycle_result(setup,"inspect",q,d)["lifecycle"]["state"],"available");EXPECT_TRUE(queued.publishable());EXPECT_EQ(exact_source(),before);
    auto changed=q;--std::get<ready_wire::request>(changed.body).budget.content_pages;seal(changed,d);
    for(const auto* op:{"inspect","discard"}){const auto result=lifecycle_result(setup,op,changed,d);EXPECT_FALSE(result.contains("lifecycle"));EXPECT_NE(result["settlement"]["state"],"committed");EXPECT_EQ(exact_source(),before);}
    const auto discarded=lifecycle_result(setup,"discard",q,d);EXPECT_EQ(discarded["lifecycle"]["state"],"terminal");EXPECT_FALSE(queued.publishable());
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(receipts(),accepted);EXPECT_EQ(count("AuthenticatedRelayRow"),1);
}
TEST_F(AuthenticatedReadyLifecycle, DiscardCommitDenialAndIgnoredChargeNeverPublishTerminalOrRetireSequence) {
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);const auto before=exact_source();
    for(bool ignore:{false,true}){ReadyLifecycleFault fault(owner->db(),ignore);const auto result=lifecycle_result(setup,"discard",q,d);
        EXPECT_GT(fault.hits,0u);EXPECT_NE(result["settlement"]["state"],"committed");EXPECT_FALSE(result.contains("lifecycle"));}
    EXPECT_EQ(exact_source(),before);EXPECT_EQ(lifecycle_result(setup,"inspect",q,d)["lifecycle"]["state"],"unstarted");
    EXPECT_EQ(lifecycle_result(setup,"discard",q,d)["lifecycle"]["state"],"terminal");
}
TEST_F(AuthenticatedReadyLifecycle, KnownDiscardCommitWithSecondaryErrorRetainsTerminalForLostReplyRetry) {
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);const auto this_thread=std::this_thread::get_id();
    const auto hook=owner->lattice_db::add_invalidation_hook([this_thread](const auto&,auto){if(std::this_thread::get_id()==this_thread)throw std::runtime_error("lifecycle committed observer");});
    const auto result=lifecycle_result(setup,"discard",q,d);owner->remove_invalidation_hook(hook);
    EXPECT_EQ(result["settlement"]["state"],"committed");EXPECT_EQ(result["settlement"]["postcommitError"],true);EXPECT_EQ(result["lifecycle"]["state"],"terminal");
    const auto exact=exact_source();EXPECT_EQ(lifecycle_result(setup,"discard",q,d)["lifecycle"]["state"],"terminal");EXPECT_EQ(exact_source(),exact);
}
TEST_F(AuthenticatedReadyLifecycle, ForeignPeerAndNamespaceCannotInspectOrDiscardAnotherBinding) {
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);lease(setup,q,d);const auto before=exact_source();
    auto peer=lifecycle_setup(2);auto other_description=description(peer);auto foreign=q;foreign.route_generation=std::stoull(other_description["routeGeneration"].get<std::string>());
    EXPECT_EQ(invoke(peer,lifecycle_command("discard",foreign,other_description)).status_code(),4);EXPECT_EQ(exact_source(),before);
    auto changed=q;std::get<ready_wire::request>(changed.body).source.epoch=relay_uuid(9901);seal(changed,d);
    EXPECT_EQ(invoke(setup,lifecycle_command("inspect",changed,d)).status_code(),4);EXPECT_EQ(exact_source(),before);
    changed=q;std::get<ready_wire::request>(changed.body).receipts={{relay_uuid(9902),"other",{{"AuthenticatedRelayRow",relay_uuid(9903)}}}};seal(changed,d);
    EXPECT_EQ(invoke(setup,lifecycle_command("discard",changed,d)).status_code(),4);EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedReadyLifecycle, AutomaticMaintenanceReclaimsFullSpoolWithoutPrepareCapacityOrReturningClients) {
    std::vector<relay_recovery_setup> clients;std::vector<relay_ready_charge> held;
    for(unsigned n=1;n<=8;++n){auto client=lifecycle_setup(n);const auto d=description(client);lease(client,request(d),d,"prepare",10000);clients.push_back(std::move(client));}
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),8);const auto bindings=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");
    const auto incarnation=number("SELECT incarnation FROM _lattice_canonical_retention");
    // Shorten only after observing all eight simultaneously retained slots.
    for(auto& client:clients){const auto d=description(client);const auto response=invoke(client,command("resume",request(d),d,1000));
        ASSERT_EQ(response.status_code(),1);ASSERT_TRUE(json::parse(response.wire())["leaseAvailable"].get<bool>());}
    for(unsigned n=0;n<64;++n){auto charge=clients[0].stop_token().reserve_ready(128);if(!charge.valid())break;held.push_back(std::move(charge));}
    ASSERT_FALSE(clients[0].stop_token().reserve_ready(128).valid());ASSERT_FALSE(held.empty());
    for(auto& client:clients){client.close_on_io();client={};}clients.clear();
    ASSERT_TRUE(until([&]{return count("_lattice_canonical_ready_transfer")==0;}));EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),bindings);
    EXPECT_EQ(number("SELECT charged FROM _lattice_canonical_ready_profile"),number("SELECT SUM(charge) FROM _lattice_canonical_ready_binding"));
}
TEST_F(AuthenticatedReadyLifecycle, EmptyDecisionRacingPrepareAndLastSetupCloseKeepsCurrentIncarnationUntilExpiry) {
    const auto paused=gate();auto once=std::make_shared<std::atomic<bool>>(false);
    events->set([paused,once](const char* point){if(std::strcmp(point,"empty-before-retire")==0&&!once->exchange(true))paused->wait();});
    setup=lifecycle_setup();ASSERT_TRUE(until([&]{return paused->arrived();}));const auto d=description(setup);const auto q=request(d);
    const auto incarnation=number("SELECT incarnation FROM _lattice_canonical_retention");lease(setup,q,d,"prepare",1000);
    setup.close_on_io();setup={};paused->release();ASSERT_TRUE(until([&]{return events->published.load()>0;}));
    setup=lifecycle_setup();EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation);
    ASSERT_TRUE(until([&]{return count("_lattice_canonical_ready_transfer")==0;}));const auto next=description(setup);auto exact=q;exact.route_generation=std::stoull(next["routeGeneration"].get<std::string>());
    EXPECT_EQ(lifecycle_result(setup,"inspect",exact,next)["lifecycle"]["state"],"terminal");
}
TEST_F(AuthenticatedReadyLifecycle, RegistrationFailurePrecedesEnrollmentAndDoesNotCreatePermanentUnarmedKeeper) {
    const auto before=owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name");auto once=std::make_shared<std::atomic<bool>>(false);
    events->set([once](const char* point){if(std::strcmp(point,"before-maintenance-register")==0&&!once->exchange(true))throw std::runtime_error("fixture worker registration failure");});
    EXPECT_FALSE(open(lifecycle_policy(),connection()).valid());EXPECT_EQ(owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"),before);
    events->set({});setup=lifecycle_setup();const auto d=description(setup);EXPECT_EQ(lifecycle_result(setup,"discard",request(d),d)["lifecycle"]["state"],"terminal");
}
TEST_F(AuthenticatedReadyLifecycle, TransientMaintenanceCallbackFailureIsNotAnEmptySourceOrNewIncarnation) {
    setup=lifecycle_setup();const auto d=description(setup);lease(setup,request(d),d,"prepare",1000);
    const auto incarnation=number("SELECT incarnation FROM _lattice_canonical_retention");
    const auto paused=gate();auto once=std::make_shared<std::atomic<bool>>(false);
    events->set([paused,once](const char* point){if(std::strcmp(point,"before-maintenance")==0&&!once->exchange(true)){
        paused->wait();throw std::runtime_error("fixture transient maintenance refusal");}});
    setup.close_on_io();setup={};ASSERT_TRUE(until([&]{return paused->arrived();}));const auto published=events->published.load();paused->release();
    ASSERT_TRUE(until([&]{return events->published.load()>published;}));events->set({});
    setup=lifecycle_setup();EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation);
    ASSERT_TRUE(until([&]{return count("_lattice_canonical_ready_transfer")==0;}));EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
}
TEST_F(AuthenticatedReadyLifecycle, ActualSourceReopenReclaimsDepartedCompletedOrphanAfterExplicitNewGrace) {
    setup=lifecycle_setup(1,100);const auto d=description(setup);const auto q=request(d);lease(setup,q,d,"prepare",10000);
    const auto bindings=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");const auto incarnation=number("SELECT incarnation FROM _lattice_canonical_retention");
    reopen_owner();setup=lifecycle_setup(1,100);const auto resumed_description=description(setup);
    EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation+1);
    setup.close_on_io();setup={};ASSERT_TRUE(until([&]{return count("_lattice_canonical_ready_transfer")==0;}));
    EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),bindings);
}
TEST_F(AuthenticatedReadyLifecycle, PostEnrollmentSetupFailureKeepsOrphanMaintenanceArmedWithoutAnyReturningClient) {
    setup=lifecycle_setup(1,100);const auto d=description(setup);lease(setup,request(d),d,"prepare",10000);
    const auto incarnation=number("SELECT incarnation FROM _lattice_canonical_retention");reopen_owner();
    auto registrations=std::make_shared<std::atomic<unsigned>>(0);
    events->set([registrations](const char* point){if(std::strcmp(point,"before-maintenance-register")==0&&++*registrations==2)
        throw std::runtime_error("fixture setup allocation after enrolled worker armed");});
    EXPECT_FALSE(open(lifecycle_policy(100),connection()).valid());events->set({});
    EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation+1);
    ASSERT_TRUE(until([&]{return count("_lattice_canonical_ready_transfer")==0;}));EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
    EXPECT_EQ(number("SELECT incarnation FROM _lattice_canonical_retention"),incarnation+1);
}
TEST_F(AuthenticatedReadyLifecycle, ClosedOwnerAfterMaintenanceCommitCannotRetainOrTouchReplacementOwner) {
    const auto paused=gate();auto enabled=std::make_shared<std::atomic<bool>>(false),once=std::make_shared<std::atomic<bool>>(false);
    events->set([paused,enabled,once](const char* point){if(enabled->load()&&std::strcmp(point,"maintenance-settled")==0&&!once->exchange(true))paused->wait();});
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);lease(setup,q,d);
    const auto frames=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    enabled->store(true);auto temporary=lifecycle_setup(2);ASSERT_TRUE(until([&]{return paused->arrived();}));
    temporary.close_on_io();temporary={};setup.close_on_io();setup={};owner->close();paused->release();events->set({});reopen_owner();
    setup=lifecycle_setup();const auto next=description(setup);auto resumed=q;resumed.route_generation=std::stoull(next["routeGeneration"].get<std::string>());
    EXPECT_TRUE(lease(setup,resumed,next,"resume")["leaseAvailable"].get<bool>());
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),frames);
}
TEST_F(AuthenticatedReadyLifecycle, IncompletePublicationIsAvailableTransportStateUntilExactDiscard) {
    setup=lifecycle_setup();const auto d=description(setup);const auto q=request(d);
    {AuthenticatedReadyFault fault(owner->db(),true);const auto failed=invoke(setup,command("prepare",q,d));ASSERT_EQ(failed.status_code(),1);
        const auto body=json::parse(failed.wire());EXPECT_EQ(body["preparation"]["state"],"committed");EXPECT_EQ(body["publication"]["state"],"rolledBack");}
    EXPECT_EQ(count("_lattice_canonical_attempt"),1);EXPECT_EQ(lifecycle_result(setup,"inspect",q,d)["lifecycle"]["state"],"available");
    EXPECT_EQ(lifecycle_result(setup,"discard",q,d)["lifecycle"]["state"],"terminal");EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
}
TEST_F(AuthenticatedReadyLifecycle, PermanentBindingCapRefusesNewRetirementButExistingBindingCanAdvance) {
    setup=lifecycle_setup();
    for(unsigned n=1;n<=1024;++n){auto peer=lifecycle_setup(n);const auto d=description(peer);const auto result=lifecycle_result(peer,"discard",request(d),d);
        ASSERT_EQ(result["lifecycle"]["state"],"terminal")<<n;}
    ASSERT_EQ(count("_lattice_canonical_ready_binding"),1024);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);const auto charged=number("SELECT charged FROM _lattice_canonical_ready_profile");
    auto extra=lifecycle_setup(1025);const auto d=description(extra);const auto failed=lifecycle_result(extra,"discard",request(d),d);
    EXPECT_NE(failed["settlement"]["state"],"committed");EXPECT_FALSE(failed.contains("lifecycle"));EXPECT_EQ(number("SELECT charged FROM _lattice_canonical_ready_profile"),charged);
    const auto original=description(setup);EXPECT_EQ(lifecycle_result(setup,"discard",request(original,2),original)["lifecycle"]["state"],"terminal");
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1024);EXPECT_EQ(number("SELECT charged FROM _lattice_canonical_ready_profile"),charged);
}
TEST_F(AuthenticatedReceiptCoverageV3, LifecycleDiscardKeepsRegisteredOriginalsAndEveryNamespaceCoverageCell) {
    const auto admitted=[&](const std::string& ns,unsigned peer){auto p=covered_policy(ns);p["readyProfile"]="bounded48MiBOrphanV1";p["orphanResumeGraceMilliseconds"]=1000;
        auto value=open(p,connection(peer),std::make_shared<RelayRouteState>());if(!value.valid())throw std::runtime_error(last_bridge_error());
        if(!value.finish_authorization(covered_answer(value).dump()))throw std::runtime_error(last_bridge_error());return value;};
    setup=admitted("app",1);auto other=admitted("other",2);const auto e=identified(entry());ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);ASSERT_EQ(other.receive(frame(e)).ids().size(),1u);
    const auto globals=global_state();const auto cells=coverage();const auto origins=owner->db().query("SELECT * FROM _lattice_canonical_receipt_origin ORDER BY original_id");
    for(auto* client:{&setup,&other}){const auto d=description(*client);auto q=request(d);auto& body=std::get<ready_wire::request>(q.body);
        body.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};body.receipt_namespace=d["source"]["receiptNamespace"].get<std::string>();seal(q,d);
        auto foreign=q;std::get<ready_wire::request>(foreign.body).receipt_namespace=*body.receipt_namespace=="app"?"other":"app";seal(foreign,d);
        const auto before=all_state();const auto refused=invoke(*client,command("discard",foreign,d));ASSERT_EQ(refused.status_code(),1);
        EXPECT_FALSE(json::parse(refused.wire()).contains("lifecycle"));EXPECT_EQ(all_state(),before);
        EXPECT_TRUE(lease(*client,q,d,"prepare",1000)["leaseAvailable"].get<bool>());EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
        auto discard=command("discard",q,d);const auto result=invoke(*client,discard);ASSERT_EQ(result.status_code(),1);EXPECT_EQ(json::parse(result.wire())["lifecycle"]["state"],"terminal");}
    EXPECT_EQ(global_state(),globals);EXPECT_EQ(coverage(),cells);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt_origin ORDER BY original_id"),origins);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),2);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
}
}
#endif


#if defined(__APPLE__) || defined(__linux__)
namespace {
class AuthenticatedReceiptFileAdministration:public AuthenticatedReceiptCoverageV3 {
protected:
    int migrate_file(int64_t version=1,const SchemaVector& schema={relay_schema()}) {
        return swift_lattice_ref::migrate_relay_receipt_coverage_file(file.str(),schema,version,100,policy().dump(),covered_policy().dump());
    }
    void release_owner() {setup.close_on_io();setup={};if(owner)owner->close();owner.reset();ref.reset();}
    static Snapshot read_file(const std::string& path) {
        database reader(path,database::open_mode::read_only,100);Snapshot result;
        const auto tables=reader.query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name LIMIT 129");
        if(tables.size()>128)throw std::runtime_error("administration fixture inventory bound");
        for(const auto& row:tables){const auto& name=std::get<std::string>(row.at("name"));
            if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](unsigned char c){return std::isalnum(c)||c=='_';}))
                throw std::runtime_error("administration fixture table name bound");
            result[name]=reader.query("SELECT * FROM \""+name+"\" ORDER BY 1");}
        result["sqlite_schema"]=reader.query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");
        result["user_version"]=reader.query("PRAGMA user_version");result["journal_mode"]=reader.query("PRAGMA journal_mode");return result;
    }
    struct observation {
        std::function<void()> callback;
        explicit observation(std::function<void()> value):callback(std::move(value)){detail::authenticated_ready_test_access::before_admin_open(&callback);}
        ~observation(){detail::authenticated_ready_test_access::before_admin_open(nullptr);}
    };
};
TEST_F(AuthenticatedReceiptFileAdministration, OneShotUsesActualExistingFileAndPreservesGlobalReceipts) {
    open();authorize();const auto e=entry();ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    auto expected=global_state();const auto original_receipts=receipts();setup.close_on_io();setup={};
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
    expected.at("_lattice_canonical_store").at(0).at("version")=int64_t{3};EXPECT_EQ(global_state(),expected);EXPECT_EQ(receipts(),original_receipts);
    EXPECT_EQ(count("_lattice_canonical_receipt_origin"),0);EXPECT_TRUE(coverage().empty());
    setup=covered_setup();EXPECT_EQ(global_state(),expected);
}
TEST_F(AuthenticatedReceiptFileAdministration, LiveAndRetainedResultCustodyRefusesBeforeAnyOpenSQL) {
    open();authorize();const auto before=read_file(file.str());auto result=invoke(setup,control("describe"));ASSERT_EQ(result.status_code(),1);
    auto copy=result;auto stop=setup.stop_token();const std::weak_ptr<lattice::swift_lattice> prior=owner;
    const auto pending=[&]{const auto statements=database::thread_statement_count();EXPECT_EQ(migrate_file(),2)<<last_bridge_error();EXPECT_EQ(database::thread_statement_count(),statements);};
    pending();release_owner();EXPECT_TRUE(prior.expired());pending();result={};pending();copy={};EXPECT_TRUE(stop.drained());pending();
    EXPECT_EQ(read_file(file.str()),before);stop={};ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
}
TEST_F(AuthenticatedReceiptFileAdministration, MissingMainAfterCapturedIdentityIsNotRecreatedAndReservationRetires) {
    open();authorize();release_owner();const auto before=read_file(file.str());const auto saved=file.str()+".admin-saved";
    {observation race([&]{std::filesystem::rename(file.str(),saved);});EXPECT_EQ(migrate_file(),4);EXPECT_FALSE(std::filesystem::exists(file.str()));}
    std::filesystem::rename(saved,file.str());EXPECT_EQ(read_file(file.str()),before);
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
}
TEST_F(AuthenticatedReceiptFileAdministration, ReplacementMainCannotBeAdoptedOrRepaired) {
    open();authorize();release_owner();const auto before=read_file(file.str());const auto saved=file.str()+".admin-saved";
    TempDB replacement{"receipt-admin-replacement"};
    {database seed(replacement.str());seed.execute("CREATE TABLE KeepExact(value INTEGER)");seed.execute("INSERT INTO KeepExact VALUES(19)");seed.execute("PRAGMA user_version=1");}
    const auto other=read_file(replacement.str());
    {observation race([&]{std::filesystem::rename(file.str(),saved);std::filesystem::copy_file(replacement.str(),file.str());});EXPECT_EQ(migrate_file(),4);}
    EXPECT_EQ(read_file(file.str()),other);std::filesystem::remove(file.str());std::filesystem::rename(saved,file.str());EXPECT_EQ(read_file(file.str()),before);
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
}
TEST_F(AuthenticatedReceiptFileAdministration, NonWalSourceRefusesWithoutJournalConversionOrCleanup) {
    open();authorize();release_owner();
    {database change(file.str());const auto mode=change.query("PRAGMA journal_mode=DELETE");ASSERT_EQ(std::get<std::string>(mode.at(0).at("journal_mode")),"delete");}
    const auto before=read_file(file.str());ASSERT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    EXPECT_NE(last_bridge_error().find("existing WAL"),std::string::npos);
}
TEST_F(AuthenticatedReceiptFileAdministration, DeclaredVersionAndCatalogMismatchNeverRunSchemaRepair) {
    open();authorize();release_owner();const auto before=read_file(file.str());
    EXPECT_EQ(migrate_file(2),4);EXPECT_EQ(read_file(file.str()),before);
    auto changed=relay_schema();property_descriptor extra{};extra.name="must_not_be_created";extra.type=column_type::text;changed.properties.emplace(extra.name,extra);
    EXPECT_EQ(migrate_file(1,{changed}),4);EXPECT_EQ(read_file(file.str()),before);
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
}
TEST_F(AuthenticatedReceiptFileAdministration, PreOpenObservationThrowLeavesEveryTableAndReservationUnchanged) {
    open();authorize();release_owner();const auto before=read_file(file.str());
    {observation fail([]{throw std::runtime_error("passive administration observation");});EXPECT_EQ(migrate_file(),4);}
    EXPECT_EQ(read_file(file.str()),before);ASSERT_EQ(migrate_file(),1)<<last_bridge_error();
}
TEST_F(AuthenticatedReceiptFileAdministration, UnadmittedExistingSchemaIsNotHealedOrPublished) {
    // Ordinary fixture construction has no canonical source enrollment yet.
    release_owner();const auto before=read_file(file.str());
    EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    size_t published=0;instance_registry::instance().for_each_alive(file.str(),[&](lattice_db*){++published;});EXPECT_EQ(published,0u);
}
TEST_F(AuthenticatedReceiptFileAdministration, ActualClosedOwnerSettlesSameValueHeaderWithoutOrdinaryPublication) {
    open();authorize();setup.close_on_io();setup={};const auto before=all_state();size_t observations=0;
    const std::function<void(lattice_db&)> inspect=[&](lattice_db& actual) {
        ++observations;EXPECT_NE(&actual,owner.get());
        size_t published=0;instance_registry::instance().for_each_alive(file.str(),[&](lattice_db* value){EXPECT_NE(value,&actual);++published;});
        EXPECT_EQ(published,1u);
        const auto header=[&] {
            const auto rows=actual.db().query("PRAGMA main.user_version");
            return rows.size()==1&&rows[0].size()==1&&std::holds_alternative<int64_t>(rows[0].begin()->second)&&
                std::get<int64_t>(rows[0].begin()->second)==0;
        };
        ASSERT_TRUE(header());
        // The production factory strongly retains actual for this entire
        // synchronous callback. This alias tests settlement, not lifetime.
        auto borrowed=std::shared_ptr<lattice_db>(&actual,[](lattice_db*){});
        using state=detail::recovery_install_state;
        const auto committed=detail::recovery_writer_access::install(borrowed,[&](database& writer){
            EXPECT_EQ(detail::recovery_writer_access::active_writer(actual),&writer);
            writer.execute("PRAGMA main.user_version=0");
        });
        EXPECT_EQ(committed.state,state::committed);EXPECT_FALSE(committed.primary_error);EXPECT_FALSE(committed.postcommit_error);
        EXPECT_EQ(all_state(),before);EXPECT_TRUE(header());
        const auto rolled=detail::recovery_writer_access::install(borrowed,[](database& writer){
            writer.execute("PRAGMA main.user_version=0");throw std::runtime_error("owned administration rollback");
        });
        EXPECT_EQ(rolled.state,state::rolled_back);EXPECT_TRUE(rolled.primary_error);EXPECT_FALSE(rolled.cleanup_error);EXPECT_EQ(all_state(),before);EXPECT_TRUE(header());
        const auto premature=detail::recovery_writer_access::install(borrowed,[](database& writer){
            writer.execute("PRAGMA main.user_version=0");writer.commit();
        });
        EXPECT_EQ(premature.state,state::rolled_back);EXPECT_TRUE(premature.primary_error);EXPECT_FALSE(premature.cleanup_error);
        EXPECT_FALSE(premature.unexpected_commit_observed);EXPECT_EQ(all_state(),before);EXPECT_TRUE(header());
    };
    struct restore {const std::function<void(lattice_db&)>* prior;~restore(){detail::authenticated_relay_catalog_test_access::before_write(prior);}}
        restored{detail::authenticated_relay_catalog_test_access::before_write(&inspect)};
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();EXPECT_EQ(observations,1u);
    // The normal receipt migration adds its documented coverage tables; the
    // hook's three header transactions leave every pre-migration table unchanged.
    EXPECT_EQ(receipts(),before.at("_lattice_canonical_receipt"));
}

}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
TEST_F(AuthenticatedReadySession, SmallLifecycleNameKeepsOldSmallDescriptionExceptExplicitLifecycleFields) {
    setup=admitted();const auto old=description(setup);setup.close_on_io();setup={};
    // A fresh source can explicitly select the small lifecycle profile. An
    // existing old source still requires administration; opening cannot adopt.
    auto p=source_policy();p["readyProfile"]="boundedV1OrphanV1";p["orphanResumeGraceMilliseconds"]=10000;
    const auto unchanged=exact_source();auto implicit=open(p,connection(2));EXPECT_FALSE(implicit.valid());EXPECT_EQ(exact_source(),unchanged);
    detail::canonical_namespaced_writer_profile native;
    const auto& source=old.at("source");native.writer.binding={source.at("sourceID"),source.at("epoch"),source.at("scopeDigest"),source.at("schemaDigest")};
    native.writer.limits={65536,16777216,65536,16777216,256,128,64};native.writer.models={"AuthenticatedRelayRow"};native.writer.upstream_requested=true;
    native.namespaces.local_namespace="local";native.namespaces.entries={{"app","app-v1",1},{"local","local-v1",1},{"other","other-v1",1}};
    const auto ready=detail::canonical_named_ready_profile(source.at("authority"),native.writer.limits,false,"boundedV1");
    const auto adopted=detail::adopt_ready_lifecycle_for_test(owner,native,{256,65536,1048576},{64,3600000},ready,"boundedV1",10000);
    ASSERT_EQ(adopted.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(adopted.record);
    setup=open(p,connection(3));ASSERT_TRUE(setup.valid())<<last_bridge_error();authorize();auto current=description(setup).at("profile");
    EXPECT_EQ(current.at("name"),"boundedV1OrphanV1");EXPECT_EQ(current.at("orphanResumeGraceMilliseconds"),10000);
    current["name"]="boundedV1";current.erase("orphanResumeGraceMilliseconds");EXPECT_EQ(current,old.at("profile"));
}
TEST_F(AuthenticatedReceiptCoverageV3, ExplicitAdoptionKeepsActualTwoNamespaceCapsulesAndAllRegisteredReceiptState) {
    setup=covered_setup();auto other=covered_setup("other",2);const auto e=identified(entry(72));
    ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    ASSERT_EQ(other.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});ASSERT_EQ(coverage().size(),2u);
    const auto da=description(setup),db=description(other);auto qa=request(da),qb=request(db);
    for(auto* f:{&qa,&qb}) {
        const auto ns=f==&qa?std::string("app"):std::string("other");auto& q=std::get<ready_wire::request>(f->body);
        f->version=3;q.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};q.receipt_namespace=ns;
        q.receipts={{e.global_id,ns,{{e.table_name,e.global_row_id}},e.original_identity->digest}};
    }
    seal(qa,da);seal(qb,db);(void)lease(setup,qa,da,"prepare",1000);(void)lease(other,qb,db,"prepare",1000);
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),2);ASSERT_EQ(count("_lattice_canonical_receipt_origin"),1);
    const auto preserved=[&]{auto value=all_state();value.erase("sqlite_schema");
        for(auto& row:value.at("_lattice_canonical_ready_profile")){row.erase("policy");row.erase("predecessor");}return value;};
    const auto before=preserved();const auto exact_frames=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    other.close_on_io();other={};setup.close_on_io();setup={}; // No setup, result, charge or stop token remains.
    detail::canonical_namespaced_writer_profile native;const auto& source=da.at("source");
    native.writer.binding={source.at("sourceID"),source.at("epoch"),source.at("scopeDigest"),source.at("schemaDigest")};
    native.writer.limits={65536,16777216,65536,16777216,256,128,64};native.writer.models={"AuthenticatedRelayRow"};native.writer.upstream_requested=true;
    native.namespaces.local_namespace="local";native.namespaces.entries={{"app","app-v1",1},{"local","local-v1",1},{"other","other-v1",1}};
    native.namespaces.coverage=detail::canonical_coverage_profile{relay_uuid(5100),7,{"app","other"}};
    const auto ready=detail::canonical_named_ready_profile(source.at("authority"),native.writer.limits,true,"bounded48MiBV1");
    const auto adopted=detail::adopt_ready_lifecycle_for_test(owner,native,{256,65536,1048576},{64,3600000},ready,"bounded48MiBV1",10000);
    ASSERT_EQ(adopted.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(adopted.record);EXPECT_EQ(preserved(),before);
    const auto retry=detail::adopt_ready_lifecycle_for_test(owner,native,{256,65536,1048576},{64,3600000},ready,"bounded48MiBV1",10000);
    ASSERT_EQ(retry.settlement.state,detail::recovery_install_state::committed);EXPECT_EQ(retry.record,adopted.record);EXPECT_EQ(preserved(),before);
    auto target=covered_policy();target["readyProfile"]="bounded48MiBOrphanV1";target["orphanResumeGraceMilliseconds"]=10000;
    setup=open(target,connection(),std::make_shared<RelayRouteState>());ASSERT_TRUE(setup.valid())<<last_bridge_error();
    ASSERT_TRUE(setup.finish_authorization(covered_answer(setup).dump()));const auto current=description(setup);
    qa.route_generation=std::stoull(current.at("routeGeneration").get<std::string>());const auto resumed=lease(setup,qa,current,"resume",1000);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),exact_frames);
    size_t positives=0;for(uint64_t i=0;i<std::stoull(resumed.at("frames").get<std::string>());++i) {
        const auto actual=read(setup,resumed,i);ASSERT_EQ(actual.status_code(),1);ASSERT_TRUE(actual.publishable())<<read_diagnostic(actual);
        const auto decoded=decode_read(actual,current);if(const auto* page=std::get_if<ready_wire::receipt_page>(&decoded.body))for(const auto& item:page->items) {
            ASSERT_TRUE(std::holds_alternative<ready_wire::committed>(item.value));EXPECT_EQ(item.original_id,e.global_id);
            EXPECT_EQ(item.operation_digest,e.original_identity->digest);EXPECT_FALSE(item.legacy_unbound);++positives;
        }
    }
    EXPECT_EQ(positives,1u);EXPECT_EQ(coverage().size(),2u);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);
    EXPECT_EQ(global_state().at("_lattice_canonical_receipt"),before.at("_lattice_canonical_receipt"));
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
class AuthenticatedLifecycleFileAdministration:public AuthenticatedReceiptFileAdministration {
protected:
    static json lifecycle(json prior,int64_t grace=10000) {
        prior["readyProfile"]=prior.value("readyProfile",std::string("boundedV1"))=="boundedV1"?"boundedV1OrphanV1":"bounded48MiBOrphanV1";
        prior["orphanResumeGraceMilliseconds"]=grace;return prior;
    }
    relay_lifecycle_adoption_result adopt_file(const json& prior,int64_t grace=10000,int64_t version=1,const SchemaVector& schema={relay_schema()}) {
        return swift_lattice_ref::adopt_relay_lifecycle_file(file.str(),schema,version,100,prior.dump(),lifecycle(prior,grace).dump());
    }
    static Snapshot retained(Snapshot value) {
        value.erase("sqlite_schema");
        for(auto& row:value.at("_lattice_canonical_ready_profile")){row.erase("policy");row.erase("predecessor");}return value;
    }
    static void committed(const relay_lifecycle_adoption_result& value) {
        ASSERT_FALSE(value.pending());ASSERT_EQ(value.phase(),2)<<value.primary_error()<<value.cleanup_error()<<value.postcommit_error();
        ASSERT_FALSE(value.has_error());ASSERT_NE(value.disposition(),0);ASSERT_EQ(value.transition_id().size(),36u);ASSERT_EQ(value.record_digest().size(),64u);
    }
    detail::canonical_namespaced_writer_profile native_profile(const json& description) {
        const auto& source=description.at("source");detail::canonical_namespaced_writer_profile native;
        native.writer.binding={source.at("sourceID"),source.at("epoch"),source.at("scopeDigest"),source.at("schemaDigest")};
        native.writer.limits={65536,16777216,65536,16777216,256,128,64};native.writer.models={"AuthenticatedRelayRow"};native.writer.upstream_requested=true;
        native.namespaces.local_namespace="local";native.namespaces.entries={{"app","app-v1",1},{"local","local-v1",1},{"other","other-v1",1}};return native;
    }
};
struct LifecycleAdministrationFault {
    enum Kind {deny_frames,ignore_policy,ignore_record,deny_alter,deny_commit};
    static thread_local LifecycleAdministrationFault* current;
    Kind kind;int hits=0;
    detail::canonical_upstream_test_hooks::authorizer_fault fault;
    const detail::canonical_upstream_test_hooks::authorizer_fault* prior;LifecycleAdministrationFault* previous;
    LifecycleAdministrationFault(database& db,Kind k):kind(k),fault{detail::canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        prior(detail::canonical_retention_test_hooks::fault),previous(current){current=this;detail::canonical_retention_test_hooks::fault=&fault;}
    ~LifecycleAdministrationFault(){detail::canonical_retention_test_hooks::fault=prior;current=previous;}
    static int restrict_action(int action,const char* one,const char* two,const char* origin)noexcept {
        auto& f=*current;if(f.hits||origin)return SQLITE_OK;
        const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};
        if(f.kind==deny_frames&&action==SQLITE_INSERT&&same(one,"_lattice_canonical_ready_frame")){++f.hits;return SQLITE_DENY;}
        if(action==SQLITE_UPDATE&&same(one,"_lattice_canonical_ready_profile")&&
           (f.kind==ignore_policy&&same(two,"policy")||f.kind==ignore_record&&same(two,"predecessor"))){++f.hits;return SQLITE_IGNORE;}
        if(f.kind==deny_alter&&action==SQLITE_ALTER_TABLE){++f.hits;return SQLITE_DENY;}
        if(f.kind==deny_commit&&action==SQLITE_TRANSACTION&&same(one,"COMMIT")){++f.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local LifecycleAdministrationFault* LifecycleAdministrationFault::current=nullptr;
struct LifecycleBeforeWrite {
    std::function<void(lattice_db&)> callback;
    const std::function<void(lattice_db&)>* prior;
    explicit LifecycleBeforeWrite(std::function<void(lattice_db&)> body):callback(std::move(body)),prior(detail::authenticated_relay_catalog_test_access::before_write(&callback)){}
    ~LifecycleBeforeWrite(){detail::authenticated_relay_catalog_test_access::before_write(prior);}
};
TEST_F(AuthenticatedLifecycleFileAdministration, ActualSmallSixteenCapsulesUseClosedFactoryAndResumeOriginalRequest) {
    setup=admitted();const auto d=description(setup);auto first=request(d);(void)lease(setup,first,d);
    std::vector<relay_recovery_setup> peers;
    for(unsigned i=2;i<=16;++i){auto actual=admitted(i);const auto info=description(actual);(void)lease(actual,request(info),info);peers.push_back(std::move(actual));}
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),16);const auto before=retained(read_file(file.str()));
    for(auto& peer:peers)peer.close_on_io();peers.clear();setup.close_on_io();setup={};
    const auto first_result=adopt_file(source_policy());committed(first_result);EXPECT_EQ(first_result.disposition(),1);EXPECT_EQ(retained(read_file(file.str())),before);
    const auto retry=adopt_file(source_policy());committed(retry);EXPECT_EQ(retry.disposition(),2);
    EXPECT_EQ(retry.transition_id(),first_result.transition_id());EXPECT_EQ(retry.record_digest(),first_result.record_digest());EXPECT_EQ(retained(read_file(file.str())),before);
    setup=open(lifecycle(source_policy()),connection());ASSERT_TRUE(setup.valid());auto answer=outcome(setup);answer["validForMilliseconds"]=600000;
    ASSERT_TRUE(setup.finish_authorization(answer.dump()));const auto current=description(setup);
    EXPECT_EQ(current.at("profile").at("transfers"),16);EXPECT_EQ(current.at("profile").at("durableBytes"),67108864);
    EXPECT_EQ(current.at("profile").at("transferBytes"),2097152);
    first.route_generation=std::stoull(current.at("routeGeneration").get<std::string>());const auto resumed=lease(setup,first,current,"resume");
    const auto frame=read(setup,resumed,0);ASSERT_TRUE(frame.publishable());EXPECT_EQ(decode_read(frame,current).logical,first.logical);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),16);
}
TEST_F(AuthenticatedLifecycleFileAdministration, ActualFactoryKeepsLargePreparingOrdinaryReservationsAndNoNotificationPublication) {
    setup=admitted(1,true);const auto d=description(setup);auto q=request(d);(void)lease(setup,q,d);setup.close_on_io();setup={};
    const auto native=native_profile(d);const auto ready=detail::canonical_named_ready_profile(d.at("source").at("authority"),native.writer.limits,false,"bounded48MiBV1");
    auto adapter=detail::canonical_writer_adapter::attach_ready_for_qualification(owner,native,{256,65536,1048576},{64,3600000},ready);
    auto admission=adapter->admit_namespace_for_qualification(owner,"app","registered-replica-1");q.logical.channel="preparing-for-administration";seal(q,d);
    {
        LifecycleAdministrationFault fault(owner->db(),LifecycleAdministrationFault::deny_frames);
        auto prepared=adapter->prepare_ready_owned(owner,admission,q.logical,std::get<ready_wire::request>(q.body),10000,q.route_generation);
        EXPECT_EQ(fault.hits,1);ASSERT_EQ(prepared.preparation.state,detail::recovery_install_state::committed);
        EXPECT_NE(prepared.publication.state,detail::recovery_install_state::committed);
    }
    const auto head=std::get<int64_t>(owner->db().query("SELECT head FROM _lattice_canonical_store").at(0).at("head"));
    auto ordinary=adapter->reserve_recovery_owned(owner,head,10000);ASSERT_EQ(ordinary.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(ordinary.reservation);
    ASSERT_EQ(count("_lattice_canonical_attempt"),2);ASSERT_EQ(count("_lattice_canonical_ready_transfer"),2);
    const auto before=retained(read_file(file.str()));adapter.reset();int published=0,observed=0;
    const auto hook=owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++published;});
    {
        LifecycleBeforeWrite inspect([&](lattice_db& actual){++observed;EXPECT_NE(&actual,owner.get());
            instance_registry::instance().for_each_alive(file.str(),[&](lattice_db* value){EXPECT_NE(value,&actual);});
            actual.add_invalidation_hook([&](const auto&,auto){++published;});
        });
        const auto result=adopt_file(source_policy(true));committed(result);
    }
    owner->remove_invalidation_hook(hook);EXPECT_EQ(observed,1);EXPECT_EQ(published,0);EXPECT_EQ(retained(read_file(file.str())),before);
}
TEST_F(AuthenticatedLifecycleFileAdministration, RegisteredTwoNamespaceCapsulesAndOriginalReceiptSurviveActualFactory) {
    setup=covered_setup();auto other=covered_setup("other",2);const auto e=identified(entry(83));
    ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});ASSERT_EQ(other.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    const auto a=description(setup),b=description(other);auto qa=request(a),qb=request(b);
    for(auto* f:{&qa,&qb}){auto& request=std::get<ready_wire::request>(f->body);const std::string ns=f==&qa?"app":"other";
        f->version=3;request.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};request.receipt_namespace=ns;
        request.receipts={{e.global_id,ns,{{e.table_name,e.global_row_id}},e.original_identity->digest}};}
    seal(qa,a);seal(qb,b);(void)lease(setup,qa,a,"prepare",1000);(void)lease(other,qb,b,"prepare",1000);
    const auto before=retained(read_file(file.str()));other.close_on_io();other={};setup.close_on_io();setup={};
    const auto result=adopt_file(covered_policy());committed(result);EXPECT_EQ(retained(read_file(file.str())),before);
    setup=open(lifecycle(covered_policy()),connection());ASSERT_TRUE(setup.valid());ASSERT_TRUE(setup.finish_authorization(covered_answer(setup).dump()));
    const auto current=description(setup);qa.route_generation=std::stoull(current.at("routeGeneration").get<std::string>());const auto resumed=lease(setup,qa,current,"resume",1000);
    size_t positives=0;for(uint64_t i=0;i<std::stoull(resumed.at("frames").get<std::string>());++i){const auto actual=read(setup,resumed,i);ASSERT_TRUE(actual.publishable());
        auto f=decode_read(actual,current);if(auto* page=std::get_if<ready_wire::receipt_page>(&f.body))for(const auto& item:page->items){EXPECT_TRUE(std::holds_alternative<ready_wire::committed>(item.value));EXPECT_EQ(item.operation_digest,e.original_identity->digest);++positives;}}
    EXPECT_EQ(positives,1u);EXPECT_EQ(coverage().size(),2u);EXPECT_EQ(count("_lattice_canonical_receipt_origin"),1);
}
TEST_F(AuthenticatedLifecycleFileAdministration, RegistryBlocksActualResultCopiesStopTokenAndConcurrentAdministrationBeforeOpen) {
    setup=admitted();auto result=invoke(setup,control("describe"));auto copy=result;auto stop=setup.stop_token();
    const auto pending=[&]{auto n=database::thread_statement_count();auto value=adopt_file(source_policy());EXPECT_TRUE(value.pending());EXPECT_EQ(value.disposition(),0);EXPECT_EQ(database::thread_statement_count(),n);};
    pending();release_owner();pending();result={};pending();copy={};pending();stop={};
    bool observed=false;
    {
        observation held([&]{observed=true;auto n=database::thread_statement_count();
            auto other=adopt_file(source_policy());EXPECT_TRUE(other.pending());EXPECT_EQ(database::thread_statement_count(),n);
        });
        committed(adopt_file(source_policy()));
    }
    EXPECT_TRUE(observed);
}
TEST_F(AuthenticatedLifecycleFileAdministration, ActualClosedWriterRestrictionFaultsRollbackAndKeepExactTransitionTruth) {
    setup=admitted();const auto d=description(setup);(void)lease(setup,request(d),d);setup.close_on_io();setup={};const auto before=read_file(file.str());
    for(auto kind:{LifecycleAdministrationFault::ignore_policy,LifecycleAdministrationFault::ignore_record,LifecycleAdministrationFault::deny_alter,LifecycleAdministrationFault::deny_commit}) {
        std::unique_ptr<LifecycleAdministrationFault> fault;
        {LifecycleBeforeWrite arm([&](lattice_db& actual){fault=std::make_unique<LifecycleAdministrationFault>(actual.db(),kind);});
            auto result=adopt_file(source_policy());ASSERT_TRUE(fault);EXPECT_EQ(fault->hits,1);EXPECT_FALSE(result.pending());EXPECT_EQ(result.phase(),1);
            EXPECT_TRUE(result.has_error());EXPECT_FALSE(result.primary_error().empty());EXPECT_EQ(result.disposition(),0);EXPECT_TRUE(result.take_record().empty());}
        fault.reset();EXPECT_EQ(read_file(file.str()),before);
    }
    committed(adopt_file(source_policy()));
}
TEST_F(AuthenticatedLifecycleFileAdministration, WrongCatalogVersionNonWalAndMissingReplacementKeepIntendedFileUnrepaired) {
    setup=admitted();setup.close_on_io();setup={};const auto prior=source_policy();const auto original=read_file(file.str());
    auto wrong=relay_schema();property_descriptor extra{};extra.name="must_not_exist";extra.type=column_type::text;wrong.properties.emplace(extra.name,extra);
    EXPECT_EQ(adopt_file(prior,10000,2).phase(),0);EXPECT_NE(adopt_file(prior,10000,1,{wrong}).phase(),2);EXPECT_EQ(read_file(file.str()),original);
    release_owner();const auto saved=file.str()+".adoption-saved";
    {observation missing([&]{std::filesystem::rename(file.str(),saved);});auto result=adopt_file(prior);EXPECT_FALSE(result.pending());EXPECT_EQ(result.phase(),0);EXPECT_FALSE(std::filesystem::exists(file.str()));}
    std::filesystem::rename(saved,file.str());EXPECT_EQ(read_file(file.str()),original);
    TempDB replacement{"lifecycle-admin-replacement"};{database raw(replacement.str());raw.execute("CREATE TABLE KeepExact(value INTEGER)");raw.execute("INSERT INTO KeepExact VALUES(23)");raw.execute("PRAGMA user_version=1");}
    const auto alternate=read_file(replacement.str());
    {observation changed([&]{std::filesystem::rename(file.str(),saved);std::filesystem::copy_file(replacement.str(),file.str());});auto result=adopt_file(prior);EXPECT_EQ(result.phase(),0);EXPECT_EQ(result.disposition(),0);}
    EXPECT_EQ(read_file(file.str()),alternate);std::filesystem::remove(file.str());std::filesystem::rename(saved,file.str());EXPECT_EQ(read_file(file.str()),original);
    {database raw(file.str());ASSERT_EQ(std::get<std::string>(raw.query("PRAGMA journal_mode=DELETE").at(0).at("journal_mode")),"delete");}
    const auto before=read_file(file.str());auto nonwal=adopt_file(prior);EXPECT_EQ(nonwal.phase(),0);EXPECT_FALSE(nonwal.pending());EXPECT_EQ(read_file(file.str()),before);
}
TEST_F(AuthenticatedLifecycleFileAdministration, ReceiptConversionMustPrecedeAdoptionAndExactOldHandleRetryDoesNotChangeGrace) {
    open();authorize();const auto e=entry(88);ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});setup.close_on_io();setup={};
    ASSERT_EQ(migrate_file(),1);const auto migrated=read_file(file.str());const auto adopted=adopt_file(covered_policy());committed(adopted);
    EXPECT_EQ(retained(read_file(file.str())),retained(migrated));
    const auto exact=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),exact);
    const auto changed=adopt_file(covered_policy(),10001);EXPECT_FALSE(changed.pending());EXPECT_NE(changed.phase(),2);EXPECT_EQ(changed.disposition(),0);EXPECT_EQ(read_file(file.str()),exact);
    const auto retry=adopt_file(covered_policy());committed(retry);EXPECT_EQ(retry.disposition(),2);EXPECT_EQ(retry.transition_id(),adopted.transition_id());EXPECT_EQ(read_file(file.str()),exact);
}
TEST_F(AuthenticatedLifecycleFileAdministration, EmptyUnadmittedFileAndPreOpenThrowNeverPublishAClosedOwner) {
    release_owner();const auto before=read_file(file.str());const auto refused=adopt_file(source_policy());EXPECT_FALSE(refused.pending());EXPECT_NE(refused.phase(),2);EXPECT_EQ(refused.disposition(),0);EXPECT_EQ(read_file(file.str()),before);
    size_t published=0;instance_registry::instance().for_each_alive(file.str(),[&](lattice_db*){++published;});EXPECT_EQ(published,0u);
    {observation fail([]{throw std::runtime_error("passive adoption observation");});const auto result=adopt_file(source_policy());EXPECT_EQ(result.phase(),0);EXPECT_FALSE(result.pending());}
    EXPECT_EQ(read_file(file.str()),before);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
TEST_F(AuthenticatedLifecycleFileAdministration, ReservedAdministrationRefusesActualSourceOpenUntilItsOwnerRetires) {
    setup=admitted();setup.close_on_io();setup={};const auto before=retained(read_file(file.str()));bool attempted=false;
    {
        observation held([&]{attempted=true;const auto statements=database::thread_statement_count();
            auto competing=open(source_policy(),connection(29),std::make_shared<RelayRouteState>());
            EXPECT_FALSE(competing.valid());EXPECT_EQ(database::thread_statement_count(),statements);
        });
        committed(adopt_file(source_policy()));
    }
    EXPECT_TRUE(attempted);EXPECT_EQ(retained(read_file(file.str())),before);
    setup=open(lifecycle(source_policy()),connection(29),std::make_shared<RelayRouteState>());ASSERT_TRUE(setup.valid())<<last_bridge_error();
    auto answer=outcome(setup);answer["validForMilliseconds"]=600000;EXPECT_TRUE(setup.finish_authorization(answer.dump()));
}
TEST_F(AuthenticatedLifecycleFileAdministration, UnregisteredDirectoryCustodianIsRefusalNotFabricatedRegistryPending) {
    setup=admitted();const auto d=description(setup);setup.close_on_io();setup={};
    const auto native=native_profile(d);const auto ready=detail::canonical_named_ready_profile(d.at("source").at("authority"),native.writer.limits,false,"boundedV1");
    auto actual=detail::canonical_writer_adapter::attach_ready_for_qualification(owner,native,{256,65536,1048576},{64,3600000},ready);
    const auto before=read_file(file.str());const auto result=adopt_file(source_policy());EXPECT_FALSE(result.pending());
    EXPECT_NE(result.phase(),2);EXPECT_EQ(result.disposition(),0);EXPECT_TRUE(result.has_error());EXPECT_EQ(read_file(file.str()),before);
    actual.reset();committed(adopt_file(source_policy()));
}
TEST_F(AuthenticatedLifecycleFileAdministration, FreshLifecycleEnrollmentCannotInventAnOldProfilePredecessor) {
    setup=open(lifecycle(source_policy()),connection());ASSERT_TRUE(setup.valid());auto answer=outcome(setup);answer["validForMilliseconds"]=600000;
    ASSERT_TRUE(setup.finish_authorization(answer.dump()));setup.close_on_io();setup={};const auto before=read_file(file.str());
    const auto result=adopt_file(source_policy());EXPECT_FALSE(result.pending());EXPECT_NE(result.phase(),2);EXPECT_EQ(result.disposition(),0);EXPECT_EQ(read_file(file.str()),before);
}
class AuthenticatedLifecycleDirectoryAdministration:public AuthenticatedLifecycleFileAdministration {
protected:
    std::filesystem::path parent;
    void SetUp()override {
        parent=file.path.string()+".owned-parent";std::filesystem::create_directory(parent);file.path=parent/"source.sqlite";
        AuthenticatedLifecycleFileAdministration::SetUp();
    }
};
TEST_F(AuthenticatedLifecycleDirectoryAdministration, ReplacedIntendedParentNeverCreatesOrUsesANewMainFile) {
    setup=admitted();release_owner();const auto before=read_file(file.str());const auto saved=parent.string()+".held";
    {
        observation moved([&]{std::filesystem::rename(parent,saved);std::filesystem::create_directory(parent);});
        const auto result=adopt_file(source_policy());EXPECT_FALSE(result.pending());EXPECT_EQ(result.phase(),0);EXPECT_EQ(result.disposition(),0);
        EXPECT_FALSE(std::filesystem::exists(file.str()));
    }
    ASSERT_TRUE(std::filesystem::is_empty(parent));std::filesystem::remove(parent);std::filesystem::rename(saved,parent);
    EXPECT_EQ(read_file(file.str()),before);committed(adopt_file(source_policy()));
    // Existing custody directories stay bound to their original inode; no
    // test or administration unlink/rebind is used to make the retry succeed.
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
TEST_F(AuthenticatedReceiptFileAdministration, OrdinaryCreatedFileUsesMetadataVersionAndPreservesIndependentHeader) {
    // SetUp calls the real swift_lattice constructor; no fixture version PRAGMA.
    ASSERT_EQ(std::get<int64_t>(owner->db().query("PRAGMA main.user_version").at(0).at("user_version")),0);
    ASSERT_EQ(std::get<std::string>(owner->db().query("SELECT value FROM _lattice_meta WHERE key='schema_version'").at(0).at("value")),"1");
    open();authorize();const auto original=entry(301);ASSERT_EQ(setup.receive(frame(original)).take_ids(),std::vector<std::string>{original.global_id});
    const auto receipts_before=receipts();release_owner();const auto before=read_file(file.str());
    ASSERT_EQ(migrate_file(),1)<<last_bridge_error();const auto after=read_file(file.str());
    EXPECT_EQ(after.at("_lattice_meta"),before.at("_lattice_meta"));EXPECT_EQ(after.at("user_version"),before.at("user_version"));
    EXPECT_EQ(after.at("_lattice_canonical_receipt"),receipts_before);EXPECT_EQ(after.at("AuthenticatedRelayRow"),before.at("AuthenticatedRelayRow"));
}
TEST_F(AuthenticatedReceiptFileAdministration, MetadataVersionCannotBeAbsentMalformedOrReplacedByMatchingHeader) {
    open();authorize();release_owner();
    const std::vector<std::string> malformed={"", "0", "01", "+1", "1 ", "1tail", "-1", "2147483648", "999999999999999999999999"};
    for(const auto& spelling:malformed) {
        {database raw(file.str());raw.execute("UPDATE _lattice_meta SET value=? WHERE key='schema_version'",{spelling});raw.execute("PRAGMA user_version=1");}
        const auto before=read_file(file.str());EXPECT_EQ(migrate_file(),4)<<spelling;EXPECT_EQ(read_file(file.str()),before);
    }
    {database raw(file.str());raw.execute("UPDATE _lattice_meta SET value=CAST(X'31' AS BLOB) WHERE key='schema_version'");}
    auto before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    {database raw(file.str());raw.execute("DELETE FROM _lattice_meta WHERE key='schema_version'");}
    before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    {database raw(file.str());raw.execute("INSERT INTO _lattice_meta(key,value) VALUES('schema_version','2')");}
    before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
}
TEST_F(AuthenticatedReceiptFileAdministration, ViewAndDuplicateMetadataRefuseWithoutRepairOrFallback) {
    open();authorize();release_owner();
    {database raw(file.str());raw.execute("DROP TABLE _lattice_meta");raw.execute("CREATE VIEW _lattice_meta AS SELECT 'schema_version' AS key,'1' AS value");}
    auto before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    {database raw(file.str());raw.execute("DROP VIEW _lattice_meta");raw.execute("CREATE TABLE _lattice_meta(key TEXT,value TEXT NOT NULL)");
        raw.execute("INSERT INTO _lattice_meta VALUES('schema_version','1'),('schema_version','1')");}
    before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
    {database raw(file.str());raw.execute("DROP TABLE _lattice_meta");}
    before=read_file(file.str());EXPECT_EQ(migrate_file(),4);EXPECT_EQ(read_file(file.str()),before);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
#include "../../Sources/LatticeCore/src/recovery_predecessor_wire.hpp"
namespace {
class AuthenticatedPredecessor : public AuthenticatedReadySession {
protected:
    json prior,current;
    ready_wire::frame frozen;
    void adopt(bool large=false,bool capsule=true) {
        setup=admitted(1,large);prior=description(setup);frozen=request(prior);
        if(capsule)(void)lease(setup,frozen,prior);
        setup.close_on_io();setup={};
        detail::canonical_namespaced_writer_profile p;const auto& s=prior.at("source");
        p.writer.binding={s.at("sourceID"),s.at("epoch"),s.at("scopeDigest"),s.at("schemaDigest")};
        p.writer.limits={65536,16777216,65536,16777216,256,128,64};p.writer.models={"AuthenticatedRelayRow"};p.writer.upstream_requested=true;
        p.namespaces.local_namespace="local";p.namespaces.entries={{"app","app-v1",1},{"local","local-v1",1},{"other","other-v1",1}};
        const std::string name=large?"bounded48MiBV1":"boundedV1";
        const auto result=detail::adopt_ready_lifecycle_for_test(owner,p,{256,65536,1048576},{64,3600000},
            detail::canonical_named_ready_profile(s.at("authority"),p.writer.limits,false,name),name,60000);
        ASSERT_EQ(result.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(result.record);
        auto target=source_policy(large);target["readyProfile"]=large?"bounded48MiBOrphanV1":"boundedV1OrphanV1";target["orphanResumeGraceMilliseconds"]=60000;
        setup=open(target,connection());ASSERT_TRUE(setup.valid())<<last_bridge_error();auto authorized=outcome(setup);authorized["validForMilliseconds"]=600000;
        ASSERT_TRUE(setup.finish_authorization(authorized.dump()));current=description(setup);
        frozen.route_generation=std::stoull(current.at("routeGeneration").get<std::string>());
    }
    json proof_command() {
        auto c=command("predecessor",frozen,current);c.erase("durationMilliseconds");c["priorProfile"]=prior.at("profile");return c;
    }
    json proof() {
        const auto result=invoke(setup,proof_command());if(result.status_code()!=1||!result.publishable())throw db_error("fixture predecessor unavailable: "+last_bridge_error());
        return json::parse(result.wire());
    }
};
TEST_F(AuthenticatedPredecessor, ActualSmallAdoptionProofKeepsLiveLeaseAndEveryDurableCounter) {
    adopt();ASSERT_FALSE(HasFatalFailure());const auto resumed=lease(setup,frozen,current,"resume");const auto frame=read(setup,resumed,0);ASSERT_TRUE(frame.publishable());
    const auto before=exact_source();detail::canonical_ready_read_test_observation::observation trace;
    const auto previous=detail::canonical_ready_read_test_observation::current;detail::canonical_ready_read_test_observation::current=&trace;
    struct restore {detail::canonical_ready_read_test_observation::observation* previous;~restore(){detail::canonical_ready_read_test_observation::current=previous;}} reset{previous};
    const auto answer=proof();ASSERT_EQ(answer.at("settlement").at("state"),"committed");ASSERT_TRUE(answer.contains("predecessor"));EXPECT_EQ(answer.at("leaseAvailable"),false);
    EXPECT_EQ(answer.at("predecessor").at("requestDigest"),std::get<ready_wire::request>(frozen.body).request_digest);
    EXPECT_EQ(answer.at("predecessor").at("beforeProfileDigest"),detail::predecessor_wire::profile_digest(prior.at("profile")));
    EXPECT_EQ(answer.at("predecessor").at("afterProfileDigest"),detail::predecessor_wire::profile_digest(current.at("profile")));
    EXPECT_EQ(trace.full_audits,2u);EXPECT_EQ(exact_source(),before);EXPECT_TRUE(frame.publishable());EXPECT_EQ(read(setup,resumed,0).wire(),frame.wire());
}
TEST_F(AuthenticatedPredecessor, ActualLargeEmptyBindingProofDoesNotInventCapsuleOrHistory) {
    adopt(true,false);ASSERT_FALSE(HasFatalFailure());const auto before=exact_source();const auto answer=proof();ASSERT_TRUE(answer.contains("predecessor"));
    EXPECT_FALSE(answer.contains("lifecycle"));EXPECT_FALSE(answer.contains("bindingHighWater"));EXPECT_FALSE(answer.contains("leaseID"));
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),0);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(exact_source(),before);
    const auto repeated=proof();EXPECT_EQ(repeated.at("predecessor"),answer.at("predecessor"));EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedPredecessor, FreshOrphanCannotManufactureRecordFromMatchingConfiguration) {
    auto p=source_policy();p["readyProfile"]="boundedV1OrphanV1";p["orphanResumeGraceMilliseconds"]=60000;
    setup=open(p,connection());ASSERT_TRUE(setup.valid());authorize();current=description(setup);prior=current;
    prior["profile"]["name"]="boundedV1";prior["profile"].erase("orphanResumeGraceMilliseconds");frozen=request(current);
    const auto before=exact_source();const auto answer=proof();EXPECT_NE(answer.at("settlement").at("state"),"committed");EXPECT_FALSE(answer.contains("predecessor"));EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedPredecessor, ExactProfileTypesMembersAndEveryAuthenticatedQBindingRemainMandatory) {
    adopt();ASSERT_FALSE(HasFatalFailure());const auto before=exact_source();const auto original=proof_command();
    std::vector<json> invalid;
    for(const auto& value:std::vector<json>{16.0,true,"16"}){auto c=original;c["priorProfile"]["transfers"]=value;invalid.push_back(c);}
    {auto c=original;c["priorProfile"]["extra"]=1;invalid.push_back(c);}
    {auto c=original;c["priorProfile"]["orphanResumeGraceMilliseconds"]=60000;invalid.push_back(c);}
    {auto c=original;c["routeGeneration"]="1";if(c["routeGeneration"]==current["routeGeneration"])c["routeGeneration"]="2";invalid.push_back(c);}
    for(unsigned kind=0;kind<5;++kind){auto q=frozen;auto& body=std::get<ready_wire::request>(q.body);
        if(kind==0)q.logical.receiver_incarnation=relay_uuid(9911);if(kind==1)q.logical.channel_incarnation=relay_uuid(9912);
        if(kind==2)q.logical.channel="wrong-channel";if(kind==3){body.source.epoch=relay_uuid(9913);body.expected.binding=body.source;}
        if(kind==4)body.receipts={{relay_uuid(9914),"other",{{"AuthenticatedRelayRow",relay_uuid(9915)}}}};
        seal(q,current);auto c=original;c["request"]=ready_wire::encode(q,codec(current));invalid.push_back(c);
    }
    for(const auto& c:invalid){SCOPED_TRACE(c.dump().substr(0,256));const auto result=invoke(setup,c);EXPECT_NE(result.status_code(),1);EXPECT_EQ(exact_source(),before);}
    EXPECT_TRUE(proof().contains("predecessor"));
}
TEST_F(AuthenticatedPredecessor, CommitDenialWithholdsFactsAndSecondaryErrorKeepsKnownCommit) {
    adopt();ASSERT_FALSE(HasFatalFailure());const auto before=exact_source();
    {AddressedReadAuthorizerFault fault(owner->db(),false,true);const auto answer=proof();EXPECT_EQ(fault.commits,1u);EXPECT_EQ(answer.at("settlement").at("state"),"rolledBack");EXPECT_FALSE(answer.contains("predecessor"));}
    EXPECT_EQ(exact_source(),before);unsigned calls=0;
    {AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++calls;throw db_error("predecessor secondary observer");})};
        const auto answer=proof();EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_EQ(answer.at("settlement").at("postcommitError"),true);EXPECT_TRUE(answer.contains("predecessor"));}
    EXPECT_EQ(calls,1u);EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedPredecessor, PostcommitRevocationWithholdsFactsWithoutRelabelingCommit) {
    adopt();ASSERT_FALSE(HasFatalFailure());const auto c=proof_command();const auto before=exact_source();const auto stop=setup.stop_token();
    AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([stop](const auto&,auto){stop.stop();})};
    const auto result=invoke(setup,c);ASSERT_EQ(result.status_code(),1);EXPECT_FALSE(result.publishable());const auto answer=json::parse(result.wire());
    EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_FALSE(answer.contains("predecessor"));EXPECT_EQ(exact_source(),before);
}
TEST_F(AuthenticatedPredecessor, OffPageCorruptionPreventsProvenancePublicationBeforeAnyCleanup) {
    adopt();ASSERT_FALSE(HasFatalFailure());const auto tail=owner->db().query("SELECT binding,frame_index,data FROM _lattice_canonical_ready_frame ORDER BY frame_index DESC LIMIT 1").at(0);
    const auto replace=[&](const std::vector<uint8_t>& data){database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.begin_transaction();raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");raw.execute("UPDATE _lattice_canonical_ready_frame SET data=? WHERE binding=? AND frame_index=?",{data,tail.at("binding"),tail.at("frame_index")});raw.execute(guard);raw.commit();};
    replace({'{','}'});const auto corrupted=exact_source();const auto answer=proof();EXPECT_NE(answer.at("settlement").at("state"),"committed");EXPECT_FALSE(answer.contains("predecessor"));EXPECT_EQ(exact_source(),corrupted);
    replace(std::get<std::vector<uint8_t>>(tail.at("data")));EXPECT_TRUE(proof().contains("predecessor"));
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
TEST_F(AuthenticatedPredecessor, FullSmallTransferInventoryDoesNotChargeInspectionAsAnotherTransfer) {
    adopt();ASSERT_FALSE(HasFatalFailure());std::vector<relay_recovery_setup> active;
    for(unsigned n=2;n<=16;++n){auto p=source_policy();p["readyProfile"]="boundedV1OrphanV1";p["orphanResumeGraceMilliseconds"]=60000;
        auto next=open(p,connection(n));ASSERT_TRUE(next.valid())<<last_bridge_error();auto authorized=outcome(next);authorized["validForMilliseconds"]=600000;ASSERT_TRUE(next.finish_authorization(authorized.dump()));
        const auto d=description(next);(void)lease(next,request(d),d,"prepare",590000);active.push_back(std::move(next));}
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),16);const auto before=exact_source();
    const auto answer=proof();EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_TRUE(answer.contains("predecessor"));EXPECT_EQ(exact_source(),before);
    for(auto& next:active)next.close_on_io();
}
TEST(PredecessorWire, FixedDomainLengthFramingAndStrictNamedEnvelopeVectors) {
    EXPECT_EQ(detail::predecessor_wire::profile_digest(json::object()),"880fb25a80b731fab82d0a81fbb353e19a1b55016a74dcb9da65f2568afabd1d");
    EXPECT_EQ(detail::predecessor_wire::transition_digest("abc"),"48a8220dee0fc435f3d7dd270a8cb3025d989147d0be0e2087f1400c52182906");
    for(const auto registered:{false,true})for(const auto large:{false,true}){
        if(registered&&!large)continue;const std::string name=large?"bounded48MiBV1":"boundedV1";const auto target=large?"bounded48MiBOrphanV1":"boundedV1OrphanV1";
        const auto before=detail::canonical_ready_profile_description(detail::canonical_named_ready_profile("test",{},registered,name),name);
        const auto after=detail::canonical_ready_profile_description(detail::canonical_named_ready_profile("test",{},registered,target,60000),target);
        EXPECT_NO_THROW(detail::predecessor_wire::pair(before,after,registered));
        for(const auto& value:std::vector<json>{60000.0,true,"60000",0,3600001}){auto bad=after;bad["orphanResumeGraceMilliseconds"]=value;EXPECT_THROW(detail::predecessor_wire::pair(before,bad,registered),db_error);}
        auto bad=after;bad["valueLimits"]["rawBytes"]=bad["valueLimits"]["rawBytes"].get<double>();EXPECT_THROW(detail::predecessor_wire::pair(before,bad,registered),db_error);
        bad=after;bad["unknown"]=false;EXPECT_THROW(detail::predecessor_wire::pair(before,bad,registered),db_error);
    }
}
TEST_F(AuthenticatedReceiptCoverageV3, ActualAdoptedV3ProofBindsRegisteredProducerAndLeavesReceiptCoverageExact) {
    setup=covered_setup();auto other=covered_setup("other",2);const auto e=identified(entry(85));
    ASSERT_EQ(setup.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});ASSERT_EQ(other.receive(frame(e)).take_ids(),std::vector<std::string>{e.global_id});
    const auto prior=description(setup);auto q=request(prior);q.version=3;auto& body=std::get<ready_wire::request>(q.body);
    body.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};body.receipt_namespace="app";
    body.receipts={{e.global_id,"app",{{e.table_name,e.global_row_id}},e.original_identity->digest}};seal(q,prior);(void)lease(setup,q,prior,"prepare",1000);
    other.close_on_io();other={};setup.close_on_io();setup={};detail::canonical_namespaced_writer_profile p;const auto& s=prior.at("source");
    p.writer.binding={s.at("sourceID"),s.at("epoch"),s.at("scopeDigest"),s.at("schemaDigest")};p.writer.limits={65536,16777216,65536,16777216,256,128,64};
    p.writer.models={"AuthenticatedRelayRow"};p.writer.upstream_requested=true;p.namespaces.local_namespace="local";
    p.namespaces.entries={{"app","app-v1",1},{"local","local-v1",1},{"other","other-v1",1}};p.namespaces.coverage=detail::canonical_coverage_profile{relay_uuid(5100),7,{"app","other"}};
    const auto adopted=detail::adopt_ready_lifecycle_for_test(owner,p,{256,65536,1048576},{64,3600000},detail::canonical_named_ready_profile(s.at("authority"),p.writer.limits,true,"bounded48MiBV1"),"bounded48MiBV1",60000);
    ASSERT_EQ(adopted.settlement.state,detail::recovery_install_state::committed);ASSERT_TRUE(adopted.record);
    auto policy=covered_policy();policy["readyProfile"]="bounded48MiBOrphanV1";policy["orphanResumeGraceMilliseconds"]=60000;
    setup=open(policy,connection());ASSERT_TRUE(setup.valid());ASSERT_TRUE(setup.finish_authorization(covered_answer(setup).dump()));const auto d=description(setup);q.route_generation=std::stoull(d.at("routeGeneration").get<std::string>());
    auto control=command("predecessor",q,d);control.erase("durationMilliseconds");control["priorProfile"]=prior.at("profile");const auto before=all_state();
    const auto result=invoke(setup,control);ASSERT_EQ(result.status_code(),1);ASSERT_TRUE(result.publishable());const auto answer=json::parse(result.wire());
    EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_TRUE(answer.contains("predecessor"));EXPECT_EQ(all_state(),before);EXPECT_EQ(coverage().size(),2u);
    auto wrong=q;std::get<ready_wire::request>(wrong.body).registered_producer->producer=producer(2);seal(wrong,d);control["request"]=ready_wire::encode(wrong,codec(d));
    EXPECT_NE(invoke(setup,control).status_code(),1);EXPECT_EQ(all_state(),before);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
TEST_F(AuthenticatedReadySession, ReadCostObservationSaturatesWithoutChangingRealBytesOrSettlement) {
    setup=admitted();const auto e=entry(62001);ASSERT_EQ(setup.receive(frame(e)).ids().size(),1u);
    const auto d=description(setup);auto f=request(d);std::get<ready_wire::request>(f.body).receipts={{e.global_id,std::string("app"),{{e.table_name,e.global_row_id}}}};seal(f,d);
    const auto offered=lease(setup,f,d);const auto baseline=read(setup,offered,0);ASSERT_TRUE(baseline.publishable());
    ASSERT_TRUE(std::holds_alternative<ready_wire::manifest>(decode_read(baseline,d).body));const auto before=exact_source();
    auto command=control("read");for(const auto* name:{"routeGeneration","leaseID","requestDigest","attemptID","sequence"})command[name]=offered.at(name);command["index"]="0";
    namespace reads=lattice::detail::canonical_ready_read_test_observation;reads::observation trace;
    const auto maximum=~uint64_t(0);trace.cost.calls.fill(maximum-1);trace.cost.microseconds.fill(maximum);
    trace.cost.receipt_batches=maximum;trace.cost.receipt_batch_ids=maximum;
    const auto previous=reads::current;reads::current=&trace;
    struct reset {reads::observation* previous;~reset(){reads::current=previous;}} restore{previous};
    const auto result=invoke(setup,command);EXPECT_TRUE(result.publishable());EXPECT_EQ(result.wire(),baseline.wire());
    EXPECT_EQ(trace.settlement,static_cast<int>(detail::recovery_install_state::committed));EXPECT_EQ(trace.full_audits,1u);
    for(const auto value:trace.cost.calls)EXPECT_EQ(value,maximum);
    for(const auto value:trace.cost.microseconds)EXPECT_EQ(value,maximum);
    EXPECT_EQ(trace.cost.receipt_batches,maximum);EXPECT_EQ(trace.cost.receipt_batch_ids,maximum);EXPECT_EQ(exact_source(),before);
}
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
using CompletedDisposalSnapshot=std::map<std::string,std::vector<database::row_t>>;
CompletedDisposalSnapshot completed_disposal_state(database& db) {
    CompletedDisposalSnapshot state;
    const auto tables=db.query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name LIMIT 129");
    if(tables.size()>128)throw db_error("completed disposal fixture table inventory bound");
    for(const auto& row:tables) {
        const auto& name=std::get<std::string>(row.at("name"));
        if(name.empty()||name.size()>128||!std::all_of(name.begin(),name.end(),[](char c){return c>='a'&&c<='z'||c>='A'&&c<='Z'||c>='0'&&c<='9'||c=='_';}))
            throw db_error("completed disposal fixture table name refused");
        state[name]=db.query("SELECT * FROM \""+name+"\" ORDER BY 1");
    }
    state["sqlite_schema"]=db.query("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY type,name");
    return state;
}
struct CompletedDisposalAudit {
    detail::canonical_ready_read_test_observation::observation trace;
    detail::canonical_ready_read_test_observation::observation* previous=detail::canonical_ready_read_test_observation::current;
    CompletedDisposalAudit(){detail::canonical_ready_read_test_observation::current=&trace;}
    ~CompletedDisposalAudit(){detail::canonical_ready_read_test_observation::current=previous;}
};
json completed_disposal_answer(const relay_ready_result& result) {
    if(result.status_code()!=1||!result.publishable())throw db_error("completed disposal response unavailable: "+last_bridge_error());
    auto answer=json::parse(result.wire());
    if(answer.at("operation")!="discard"||answer.at("leaseAvailable")!=false||answer.contains("lifecycle")||answer.contains("leaseID"))
        throw db_error("original-profile disposal response shape changed");
    return answer;
}
class AuthenticatedCompletedDisposal:public AuthenticatedReadySession {
protected:
    void start_disposal() {
        setup=open(policy(),connection());
        if(!setup.valid()||!setup.finish_authorization(outcome(setup).dump()))
            throw db_error("completed disposal actual source authorization failed: "+last_bridge_error());
    }
    json discard(const ready_wire::frame& q,const json& d) {return completed_disposal_answer(invoke(setup,command("discard",q,d)));}
    CompletedDisposalSnapshot state(){return completed_disposal_state(owner->db());}
};

TEST_F(AuthenticatedCompletedDisposal, LostCompletedDiscardReplyRetriesWithOneBindingAndNoDurableChanges) {
    start_disposal();const auto original=entry(801,"retained application row");
    ASSERT_EQ(setup.receive(frame(original)).ids(),std::vector<std::string>{original.global_id});
    const auto d=description(setup);ASSERT_EQ(d.at("profile").at("name"),"boundedV1");const auto q=request(d);
    const auto offered=lease(setup,q,d,"prepare",1000);auto queued=read(setup,offered,0);ASSERT_TRUE(queued.publishable());
    const auto bindings=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");
    const auto old_receipts=receipts();ASSERT_EQ(bindings.size(),1u);ASSERT_EQ(count("_lattice_canonical_ready_transfer"),1);
    auto first=invoke(setup,command("discard",q,d));const auto answer=completed_disposal_answer(first);
    ASSERT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_FALSE(queued.publishable());
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(receipts(),old_receipts);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),bindings);
    const auto disposed=state();first={}; // The original response is lost after the real COMMIT.
    {CompletedDisposalAudit audit;AddressedReadAuthorizerFault commit(owner->db(),false);
        const auto retry=discard(q,d);EXPECT_EQ(retry.at("settlement").at("state"),"committed");
        EXPECT_EQ(commit.commits,1u);EXPECT_EQ(audit.trace.full_audits,2u);}
    EXPECT_EQ(state(),disposed);
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),disposed);
    const auto stale=invoke(setup,command("prepare",q,d,1000));ASSERT_EQ(stale.status_code(),1);
    EXPECT_FALSE(json::parse(stale.wire()).at("leaseAvailable").get<bool>());EXPECT_EQ(state(),disposed);
    EXPECT_TRUE(lease(setup,request(d,2),d,"prepare",1000).at("leaseAvailable").get<bool>());
    EXPECT_EQ(count("AuthenticatedRelayRow"),1);EXPECT_EQ(receipts(),old_receipts);
}

TEST_F(AuthenticatedCompletedDisposal, ExtantCapsuleRequiresExactAttemptRequestAndSequence) {
    start_disposal();const auto d=description(setup);const auto q=request(d,2);(void)lease(setup,q,d,"prepare",1000);
    const auto before=state();
    for(unsigned kind=0;kind<4;++kind) {
        SCOPED_TRACE(kind);auto changed=q;
        if(kind==0)changed.logical.attempt_id=relay_uuid(9810);
        if(kind==1)std::get<ready_wire::request>(changed.body).receipts={{relay_uuid(9811),"app",{{"AuthenticatedRelayRow",relay_uuid(9812)}}}};
        if(kind==2)changed.logical.sequence=1;
        if(kind==3)changed.logical.sequence=3;
        seal(changed,d);const auto refused=discard(changed,d);
        EXPECT_NE(refused.at("settlement").at("state"),"committed");EXPECT_EQ(state(),before);
    }
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
}

TEST_F(AuthenticatedCompletedDisposal, NeverStartedOrWrongHighWaterCannotRetireOrAdvanceASequence) {
    start_disposal();const auto d=description(setup);const auto q=request(d,2);const auto empty=state();
    EXPECT_NE(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),empty);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),0);
    (void)lease(setup,q,d,"prepare",1000);ASSERT_EQ(discard(q,d).at("settlement").at("state"),"committed");
    const auto terminal=state();
    for(const uint64_t sequence:{uint64_t{1},uint64_t{3}}) {
        SCOPED_TRACE(sequence);EXPECT_NE(discard(request(d,sequence),d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),terminal);
    }
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),terminal);
}

TEST_F(AuthenticatedCompletedDisposal, AbsentCapsuleProvesCurrentSequenceStateWithoutInventingHistoricalQBytes) {
    start_disposal();const auto d=description(setup);const auto q=request(d);(void)lease(setup,q,d,"prepare",1000);
    ASSERT_EQ(discard(q,d).at("settlement").at("state"),"committed");const auto terminal=state();
    // Deleted capsules do not retain a historical attempt-ID/request digest.
    // This different, valid Q names the same actual binding and exact high-water.
    auto different=q;different.logical.attempt_id=relay_uuid(9820);
    std::get<ready_wire::request>(different.body).receipts={{relay_uuid(9821),"app",{{"AuthenticatedRelayRow",relay_uuid(9822)}}}};seal(different,d);
    ASSERT_NE(std::get<ready_wire::request>(different.body).request_digest,std::get<ready_wire::request>(q.body).request_digest);
    const auto answer=discard(different,d);EXPECT_EQ(answer.at("settlement").at("state"),"committed");
    EXPECT_FALSE(answer.contains("receipts"));EXPECT_FALSE(answer.contains("predecessor"));EXPECT_EQ(state(),terminal);
}

TEST_F(AuthenticatedCompletedDisposal, AbsentRetryStillRequiresActualSourcePeerNamespaceAndExistingBinding) {
    start_disposal();const auto d=description(setup);const auto q=request(d);(void)lease(setup,q,d,"prepare",1000);
    ASSERT_EQ(discard(q,d).at("settlement").at("state"),"committed");const auto terminal=state();
    for(unsigned kind=0;kind<4;++kind) {
        SCOPED_TRACE(kind);auto foreign=q;auto& body=std::get<ready_wire::request>(foreign.body);
        if(kind==0){body.source.epoch=relay_uuid(9830);body.expected.binding=body.source;}
        if(kind==1)foreign.logical.receiver_incarnation=relay_uuid(9831);
        if(kind==2)foreign.logical.channel_incarnation=relay_uuid(9832);
        if(kind==3)foreign.logical.channel="another-channel";
        seal(foreign,d);const auto result=invoke(setup,command("discard",foreign,d));EXPECT_NE(result.status_code(),1);EXPECT_EQ(state(),terminal);
    }
    auto other_peer=open(policy(),connection(2));ASSERT_TRUE(other_peer.valid());ASSERT_TRUE(other_peer.finish_authorization(outcome(other_peer).dump()));
    const auto peer_d=description(other_peer);const auto peer_q=request(peer_d);const auto before_peer=state();
    EXPECT_NE(completed_disposal_answer(invoke(other_peer,command("discard",peer_q,peer_d))).at("settlement").at("state"),"committed");EXPECT_EQ(state(),before_peer);
    auto p=policy();p["receiptNamespace"]="other";auto other_namespace=open(p,connection());ASSERT_TRUE(other_namespace.valid());
    ASSERT_TRUE(other_namespace.finish_authorization(outcome(other_namespace).dump()));const auto ns_d=description(other_namespace);const auto ns_q=request(ns_d);const auto before_ns=state();
    EXPECT_NE(completed_disposal_answer(invoke(other_namespace,command("discard",ns_q,ns_d))).at("settlement").at("state"),"committed");EXPECT_EQ(state(),before_ns);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");
}

TEST_F(AuthenticatedCompletedDisposal, AbsentRetryStillAuditsAnotherCapsulesOffPageContentBeforeAnyCommit) {
    start_disposal();const auto d=description(setup);const auto q=request(d);(void)lease(setup,q,d,"prepare",1000);
    ASSERT_EQ(discard(q,d).at("settlement").at("state"),"committed");
    auto other=open(policy(),connection(2));ASSERT_TRUE(other.valid());ASSERT_TRUE(other.finish_authorization(outcome(other).dump()));
    const auto other_d=description(other);(void)lease(other,request(other_d),other_d,"prepare",1000);
    const auto before=state();const auto tail=owner->db().query("SELECT binding,frame_index,data FROM _lattice_canonical_ready_frame ORDER BY frame_index DESC LIMIT 1").at(0);
    ASSERT_GT(std::get<int64_t>(tail.at("frame_index")),0);
    const auto replace=[&](const std::vector<uint8_t>& data){
        database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.begin_transaction();raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");
        raw.execute("UPDATE _lattice_canonical_ready_frame SET data=? WHERE binding=? AND frame_index=?",{data,tail.at("binding"),tail.at("frame_index")});
        raw.execute(guard);raw.commit();
    };
    replace({'{','}'});const auto corrupt=state();
    {CompletedDisposalAudit audit;AddressedReadAuthorizerFault commit(owner->db(),false);const auto refused=discard(q,d);
        EXPECT_NE(refused.at("settlement").at("state"),"committed");EXPECT_EQ(audit.trace.full_audits,1u);EXPECT_EQ(commit.commits,0u);}
    EXPECT_EQ(state(),corrupt);EXPECT_FALSE(owner->db().is_in_transaction());
    replace(std::get<std::vector<uint8_t>>(tail.at("data")));EXPECT_EQ(state(),before);
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),before);
}

TEST_F(AuthenticatedCompletedDisposal, CommitDenialRestoresExtantCapsuleAndCannotPublishAnAbsentRetryCommit) {
    start_disposal();const auto d=description(setup);const auto q=request(d);const auto offered=lease(setup,q,d,"prepare",1000);
    const auto queued=read(setup,offered,0);ASSERT_TRUE(queued.publishable());const auto before=state();
    {AddressedReadAuthorizerFault fault(owner->db(),false,true);const auto answer=discard(q,d);
        EXPECT_EQ(fault.commits,1u);EXPECT_EQ(answer.at("settlement").at("state"),"rolledBack");EXPECT_EQ(answer.at("settlement").at("primaryError"),true);}
    EXPECT_FALSE(queued.publishable());EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(state(),before);
    ASSERT_EQ(discard(q,d).at("settlement").at("state"),"committed");const auto terminal=state();
    {AddressedReadAuthorizerFault fault(owner->db(),false,true);const auto answer=discard(q,d);
        EXPECT_EQ(fault.commits,1u);EXPECT_EQ(answer.at("settlement").at("state"),"rolledBack");EXPECT_EQ(answer.at("settlement").at("primaryError"),true);}
    EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(state(),terminal);
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),terminal);
}

TEST_F(AuthenticatedCompletedDisposal, ObserverErrorKeepsKnownCommittedDisposalAndNoChangeRetryTruth) {
    start_disposal();const auto d=description(setup);const auto q=request(d);(void)lease(setup,q,d,"prepare",1000);unsigned calls=0;
    {AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++calls;throw db_error("completed disposal observer");})};
        const auto answer=discard(q,d);EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_EQ(answer.at("settlement").at("postcommitError"),true);
        EXPECT_EQ(answer.at("settlement").at("primaryError"),false);EXPECT_EQ(answer.at("settlement").at("cleanupError"),false);}
    ASSERT_EQ(calls,1u);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);const auto terminal=state();
    {AddressedReadInvalidationHook hook{owner,owner->lattice_db::add_invalidation_hook([&](const auto&,auto){++calls;throw db_error("completed disposal retry observer");})};
        const auto answer=discard(q,d);EXPECT_EQ(answer.at("settlement").at("state"),"committed");EXPECT_EQ(answer.at("settlement").at("postcommitError"),true);}
    EXPECT_EQ(calls,2u);EXPECT_EQ(state(),terminal);EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST_F(AuthenticatedCompletedDisposal, LostResultStaysRouteFencedAndReopenedSameBindingCanRetryWithoutCapsule) {
    start_disposal();auto d=description(setup);auto q=request(d);(void)lease(setup,q,d,"prepare",1000);
    auto first=invoke(setup,command("discard",q,d));ASSERT_EQ(completed_disposal_answer(first).at("settlement").at("state"),"committed");
    route->current=false;EXPECT_FALSE(first.publishable());first={};setup.close_on_io();setup={};route=std::make_shared<RelayRouteState>();
    start_disposal();d=description(setup);q.route_generation=std::stoull(d.at("routeGeneration").get<std::string>());
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),0);ASSERT_EQ(count("_lattice_canonical_ready_binding"),1);const auto reopened=state();
    EXPECT_EQ(discard(q,d).at("settlement").at("state"),"committed");EXPECT_EQ(state(),reopened);
}

TEST_F(AuthenticatedReceiptCoverageV3, CompletedAbsentDiscardStillRequiresActualRegisteredProducerAndReceiptNamespace) {
    setup=covered_setup();const auto original=identified(entry(802,"registered immutable original"));
    ASSERT_EQ(setup.receive(frame(original)).ids(),std::vector<std::string>{original.global_id});const auto d=description(setup);auto q=request(d);
    q.version=3;auto& body=std::get<ready_wire::request>(q.body);
    body.registered_producer=detail::recovery_receipt_binding{producer(),relay_uuid(5100),7,1};body.receipt_namespace="app";
    body.receipts={{original.global_id,"app",{{original.table_name,original.global_row_id}},original.original_identity->digest}};seal(q,d);
    (void)lease(setup,q,d,"prepare",1000);const auto coverage_before=coverage(),receipts_before=receipts();const auto global_before=global_state();
    ASSERT_EQ(completed_disposal_answer(invoke(setup,command("discard",q,d))).at("settlement").at("state"),"committed");
    const auto terminal=all_state();EXPECT_EQ(coverage(),coverage_before);EXPECT_EQ(receipts(),receipts_before);EXPECT_EQ(global_state(),global_before);
    for(unsigned kind=0;kind<3;++kind) {
        SCOPED_TRACE(kind);auto wrong=q;auto& changed=std::get<ready_wire::request>(wrong.body);
        if(kind==0)changed.registered_producer->producer=producer(2);
        if(kind==1){changed.receipt_namespace="other";changed.receipts.clear();}
        if(kind==2){wrong.version=2;changed.registered_producer.reset();changed.receipt_namespace.reset();changed.receipts.clear();}
        seal(wrong,d);const auto refused=completed_disposal_answer(invoke(setup,command("discard",wrong,d)));
        EXPECT_NE(refused.at("settlement").at("state"),"committed");EXPECT_EQ(all_state(),terminal);
    }
    EXPECT_EQ(completed_disposal_answer(invoke(setup,command("discard",q,d))).at("settlement").at("state"),"committed");EXPECT_EQ(all_state(),terminal);
}
}
#endif
