#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <algorithm>
#include <set>

struct SourceSessionRecord { std::string body; int64_t rank; };
LATTICE_SCHEMA(SourceSessionRecord,body,rank);
struct SourceSessionRoot { std::string name; };
LATTICE_SCHEMA(SourceSessionRoot,name);
struct SourceSessionLeaf { std::string name; };
LATTICE_SCHEMA(SourceSessionLeaf,name);
struct SourceSessionLocal { std::string note; };
LATTICE_SCHEMA(SourceSessionLocal,note);
namespace lattice::detail {
struct canonical_source_session_test_access {
    static sync_recovery::owned_canonical_capture capture(canonical_writer_adapter& adapter,
        std::shared_ptr<lattice_db> owner,const canonical_store_binding& binding,std::optional<int64_t> base,
        const std::vector<sync_recovery::canonical_capture_request>& requests,
        const sync_recovery::canonical_capture_limits& limits,
        const std::function<void(size_t,uint64_t)>& batch={},const std::function<void()>& before={},
        const std::function<void()>& after={}) {
        return adapter.capture_recovery_impl(std::move(owner),binding,base,requests,limits,batch,before,after);
    }
};
}
namespace {
using namespace lattice::detail;
namespace sr=lattice::detail::sync_recovery;
const bool registered_source_session=[] {
    auto record=lattice::managed<SourceSessionRecord>::schema();record.properties[0].no_history=true;
    lattice::schema_registry::instance().register_model(typeid(SourceSessionRecord),std::move(record));
    auto root=lattice::managed<SourceSessionRoot>::schema();lattice::property_descriptor link{};
    link.name="leaf";link.kind=lattice::property_kind::link;link.type=lattice::column_type::integer;
    link.nullable=true;link.target_table="SourceSessionLeaf";root.properties.push_back(link);
    lattice::schema_registry::instance().register_model(typeid(SourceSessionRoot),std::move(root));return true;
}();
constexpr const char* relation="_SourceSessionRoot_SourceSessionLeaf_leaf";
std::string unknown_id(){return "ffff0000-0000-4000-8000-000000000001";}
lattice::configuration source_config(const std::string& path) {
    lattice::configuration config(path);config.audit_retention_seconds=0;return config;
}
class CanonicalSourceSession:public ::testing::Test {
protected:
    TempDB path{"canonical-source-session"};
    std::shared_ptr<lattice::lattice_db> owner=std::make_shared<lattice::lattice_db>(source_config(path.str()));
    // Preopened sibling is ONLY a deterministic corruption/floor-maintenance
    // fixture. It does not claim that an unadmitted writer has source authority.
    std::shared_ptr<lattice::lattice_db> sibling=std::make_shared<lattice::lattice_db>(source_config(path.str()));
    canonical_writer_profile profile{{"session-source","session-epoch","session-scope","session-schema"},
        {128,65536,128,65536,32,64,64},{"SourceSessionRecord","SourceSessionRoot","SourceSessionLeaf"},false};
    sr::canonical_capture_limits limits{{{65536,16,4096,8192,2,32,64,524288},8,16,16},profile.limits,32,64,2};
    std::unique_ptr<canonical_writer_adapter> adapter;
    void attach(){adapter=canonical_writer_adapter::attach(*owner,profile);}
    sr::owned_canonical_capture capture(std::optional<int64_t> base={},const std::vector<sr::canonical_capture_request>& requests={}) {
        return adapter->capture_recovery_owned(owner,profile.binding,base,requests,limits);
    }
    static canonical_identity key(const std::string& gid){return {"SourceSessionRecord",canonical_writer_adapter::uuid_key(gid)};}
    std::string original(const std::string& gid) {
        const auto rows=owner->db().query("SELECT globalId FROM AuditLog WHERE tableName='SourceSessionRecord' AND globalRowId=? ORDER BY id DESC LIMIT 1",{gid});
        if(rows.size()!=1)throw std::runtime_error("missing actual generated audit original");
        return canonical_writer_adapter::uuid_key(std::get<std::string>(rows[0].at("globalId")));
    }
    std::string body(const sr::canonical_source_row& row) {
        if(!row.payload)throw std::runtime_error("missing captured source row");
        return std::get<std::string>(sr::decode_values(*row.payload,{8192,16,256,8192,8192}).at("body"));
    }
    void advance_floor(int64_t floor) {
        sibling->begin_transaction();canonical_change_store store(*sibling,profile.binding,profile.limits);
        store.advance_floor(floor,floor);sibling->commit();
    }
};
}

TEST_F(CanonicalSourceSession, CompleteAdmittedModelsAndRealLinkExcludeUnrelatedRows) {
    attach();auto row=owner->add(SourceSessionRecord{"owned",1});auto root=owner->add(SourceSessionRoot{"root"});
    auto leaf=owner->add(SourceSessionLeaf{"leaf"});owner->add(SourceSessionLocal{"not this scope"});
    owner->ensure_link_table(relation,"SourceSessionRoot","SourceSessionLeaf");
    owner->db().execute("INSERT INTO "+std::string(relation)+"(lhs,rhs) VALUES(?,?)",{root.global_id(),leaf.global_id()});
    const auto got=capture();ASSERT_TRUE(got.capture);EXPECT_EQ(got.selection,sr::source_capture_selection::full);
    EXPECT_EQ(got.binding,profile.binding);EXPECT_EQ(got.descriptor_digest.size(),64u);EXPECT_FALSE(got.requested_base);
    ASSERT_EQ(got.capture->layouts.size(),4u);ASSERT_EQ(got.capture->rows.size(),4u);
    std::set<std::string> tables;for(const auto& layout:got.capture->layouts)tables.insert(layout.table);
    EXPECT_EQ(tables,(std::set<std::string>{"SourceSessionRecord","SourceSessionRoot","SourceSessionLeaf",relation}));
    const auto found=std::find_if(got.capture->rows.begin(),got.capture->rows.end(),[](const auto& item){return item.key.table==relation;});
    ASSERT_NE(found,got.capture->rows.end());ASSERT_TRUE(found->payload);
    const auto values=sr::decode_values(*found->payload,{8192,16,256,8192,8192});
    EXPECT_EQ(std::get<std::string>(values.at("lhs")),root.global_id());EXPECT_EQ(std::get<std::string>(values.at("rhs")),leaf.global_id());
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    static_assert(!canonical_writer_adapter::serving_capability);
}

TEST_F(CanonicalSourceSession, EmptyAndPopulatedZeroHeadAreNotLegacyCursorAdoption) {
    const std::string upper="AAAA0000-0000-4000-8000-000000000001";
    owner->db().execute("INSERT INTO SourceSessionRecord(globalId,body,rank) VALUES(?,'before enrollment',1)",{upper});
    attach();const auto full=capture();ASSERT_TRUE(full.capture);EXPECT_EQ(full.head,0);ASSERT_EQ(full.capture->rows.size(),1u);
    const auto values=sr::decode_values(*full.capture->rows[0].payload,{8192,16,256,8192,8192});
    EXPECT_EQ(std::get<std::string>(values.at("globalId")),upper);EXPECT_FALSE(values.count("id"));
    const auto delta=capture(0);ASSERT_TRUE(delta.capture);EXPECT_EQ(delta.selection,sr::source_capture_selection::delta);
    EXPECT_EQ(delta.requested_base,std::optional<int64_t>{0});EXPECT_TRUE(delta.capture->rows.empty());
    // The numeric base is a caller declaration, not a certificate that this
    // preexisting row was installed. Explicit full is the initial baseline.
}

TEST_F(CanonicalSourceSession, EmptySourcePublishesNumericZeroOnlyAsUnsealedFacts) {
    attach();const auto got=capture();ASSERT_TRUE(got.capture);EXPECT_EQ(got.head,0);EXPECT_EQ(got.floor,0);
    EXPECT_TRUE(got.capture->rows.empty());EXPECT_TRUE(got.capture->receipts.empty());
    EXPECT_EQ(got.selection,sr::source_capture_selection::full);
}

TEST_F(CanonicalSourceSession, RetiredBaseRequiresNewFullRequestWithNoPayloadAndEqualFloorStillDeltas) {
    attach();auto row=owner->add(SourceSessionRecord{"before floor",1});const auto before=capture();
    ASSERT_GT(before.head,0);advance_floor(before.head);
    const auto retired=capture(0);EXPECT_EQ(retired.selection,sr::source_capture_selection::requires_full_request);
    EXPECT_EQ(retired.head,before.head);EXPECT_EQ(retired.floor,before.head);EXPECT_FALSE(retired.capture);
    EXPECT_EQ(retired.requested_base,std::optional<int64_t>{0});
    const auto same=capture(before.head);ASSERT_TRUE(same.capture);EXPECT_TRUE(same.capture->rows.empty());
    EXPECT_EQ(same.selection,sr::source_capture_selection::delta);
    const auto full=capture();ASSERT_TRUE(full.capture);ASSERT_EQ(full.capture->rows.size(),1u);
    EXPECT_EQ(body(full.capture->rows[0]),"before floor");EXPECT_EQ(full.selection,sr::source_capture_selection::full);
    EXPECT_THROW(capture(before.head+1),sr::protocol_error);
    EXPECT_THROW(capture(-1),sr::protocol_error);
}

TEST_F(CanonicalSourceSession, SameHeadReceiptRefreshIncludesOriginalTargetAndMissingRemainsUnknown) {
    attach();auto row=owner->add(SourceSessionRecord{"first",1});const auto original_id=original(row.global_id());
    row.body="current NoHistory";const auto before=capture();
    std::vector<sr::canonical_capture_request> requests{{original_id,{key(row.global_id())}},{unknown_id(),{key(unknown_id())}}};
    std::sort(requests.begin(),requests.end(),[](const auto& a,const auto& b){return a.original_id<b.original_id;});
    const auto got=capture(before.head,requests);ASSERT_TRUE(got.capture);EXPECT_EQ(got.head,before.head);
    ASSERT_EQ(got.capture->rows.size(),2u);ASSERT_EQ(got.capture->receipts.size(),2u);
    const auto receipt=std::find_if(got.capture->receipts.begin(),got.capture->receipts.end(),[&](const auto& r){return r.original_id==original_id;});
    ASSERT_NE(receipt,got.capture->receipts.end());ASSERT_TRUE(receipt->stored);EXPECT_LT(receipt->stored->position,got.head);
    const auto missing=std::find_if(got.capture->receipts.begin(),got.capture->receipts.end(),[](const auto& r){return r.original_id==unknown_id();});
    ASSERT_NE(missing,got.capture->receipts.end());EXPECT_FALSE(missing->stored);
    const auto present=std::find_if(got.capture->rows.begin(),got.capture->rows.end(),[](const auto& r){return r.payload.has_value();});
    ASSERT_NE(present,got.capture->rows.end());EXPECT_EQ(body(*present),"current NoHistory");
}

TEST_F(CanonicalSourceSession, SelectionRowsReceiptsAndNoHistoryShareThePinnedView) {
    attach();auto one=owner->add(SourceSessionRecord{"one",1});auto two=owner->add(SourceSessionRecord{"two",2});
    const auto original_id=original(one.global_id());const auto before=capture();bool changed=false;
    const auto got=canonical_source_session_test_access::capture(*adapter,owner,profile.binding,0,
        {{original_id,{key(one.global_id())}}},limits,[&](size_t batch,uint64_t){
            if(batch==0&&!changed){changed=true;one.body="after pin";owner->remove(two);owner->add(SourceSessionRecord{"new",3});}
        });
    ASSERT_TRUE(changed);ASSERT_TRUE(got.capture);EXPECT_EQ(got.head,before.head);EXPECT_EQ(got.floor,before.floor);
    ASSERT_EQ(got.capture->rows.size(),2u);std::set<std::string> bodies;
    for(const auto& row:got.capture->rows)bodies.insert(body(row));EXPECT_EQ(bodies,(std::set<std::string>{"one","two"}));
    ASSERT_EQ(got.capture->receipts.size(),1u);ASSERT_TRUE(got.capture->receipts[0].stored);
    EXPECT_LE(got.capture->receipts[0].stored->position,got.head);EXPECT_GT(capture().head,got.head);
}

TEST_F(CanonicalSourceSession, WrongOwnerAndEveryChangedBindingComponentRefuse) {
    attach();EXPECT_THROW(adapter->capture_recovery_owned(sibling,profile.binding,{}, {},limits),lattice::db_error);
    for(auto member:{&canonical_store_binding::source,&canonical_store_binding::epoch,
                     &canonical_store_binding::scope,&canonical_store_binding::schema}) {
        auto wrong=profile.binding;wrong.*member+="-wrong";
        EXPECT_THROW(adapter->capture_recovery_owned(owner,wrong,{}, {},limits),lattice::db_error);
    }
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceSession, OutsideScopeRequestsAndFiniteCapsRefuseWithoutPartialCapture) {
    attach();owner->add(SourceSessionRecord{"one",1});owner->add(SourceSessionRecord{"two",2});
    const auto before=capture();ASSERT_TRUE(before.capture);
    EXPECT_THROW(capture({},{{unknown_id(),{{"SourceSessionLocal",unknown_id()}}}}),sr::protocol_error);
    const auto saved=limits;limits.rows.wire.total_rows=1;EXPECT_THROW(capture(),sr::protocol_error);limits=saved;
    limits.rows.wire.total_bytes=1;EXPECT_THROW(capture(),sr::protocol_error);limits=saved;
    limits.requests=1;EXPECT_THROW(capture({},{{"aaaa0000-0000-4000-8000-000000000001",{key(unknown_id())}},
        {unknown_id(),{key(unknown_id())}}}),sr::protocol_error);limits=saved;
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);EXPECT_EQ(capture().head,before.head);
}

TEST_F(CanonicalSourceSession, CorruptSourceDoesNotBecomeAnOrdinaryRetiredBaseRecommendation) {
    attach();owner->add(SourceSessionRecord{"body",1});const auto before=capture();advance_floor(before.head);
    sibling->db().execute("UPDATE _lattice_canonical_store SET receipts=receipts+1");
    EXPECT_THROW(capture(0),sr::protocol_error);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    sibling->db().execute("UPDATE _lattice_canonical_store SET receipts=receipts-1");
    EXPECT_EQ(capture(0).selection,sr::source_capture_selection::requires_full_request);
    EXPECT_THROW(capture(0,{{unknown_id(),{{"SourceSessionLocal",unknown_id()}}}}),sr::protocol_error);
}

TEST_F(CanonicalSourceSession, ChangedGeneratedProgramsRefuseEvenIfOpaqueStoredManifestIsUntouched) {
    attach();const auto before=capture();ASSERT_TRUE(before.capture);
    sibling->db().execute("CREATE TRIGGER source_session_extra AFTER UPDATE ON SourceSessionRecord BEGIN SELECT 1; END");
    EXPECT_THROW(capture(),lattice::db_error);
    sibling->db().execute("DROP TRIGGER source_session_extra");
    const auto after=capture();ASSERT_TRUE(after.capture);EXPECT_EQ(after.descriptor_digest,before.descriptor_digest);
}

TEST_F(CanonicalSourceSession, KeeperRetirementReturnsNoCapture) {
    attach();owner->add(SourceSessionRecord{"body",1});bool retired=false;
    EXPECT_THROW(canonical_source_session_test_access::capture(*adapter,owner,profile.binding,{}, {},limits,
        [&](size_t batch,uint64_t generation){if(batch==0&&!retired){retired=true;owner->release_read_generation(generation);}}),sr::protocol_error);
    EXPECT_TRUE(retired);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceSession, RetiringWrapperBeforeFinalDecisionRefusesWithoutUseAfterFree) {
    attach();owner->add(SourceSessionRecord{"body",1});auto* entered=adapter.get();bool retired=false;
    EXPECT_THROW(canonical_source_session_test_access::capture(*entered,owner,profile.binding,{}, {},limits,{},
        [&]{adapter.reset();retired=true;}),lattice::db_error);
    EXPECT_TRUE(retired);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceSession, CloseBeforeDecisionRefusesAndLaterAdmissionStaysClosed) {
    attach();bool closed=false;
    EXPECT_THROW(canonical_source_session_test_access::capture(*adapter,owner,profile.binding,{}, {},limits,{},
        [&]{owner->close();closed=true;}),lattice::db_error);
    EXPECT_TRUE(closed);EXPECT_THROW(capture(),lattice::db_error);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceSession, ReplacingPhysicalWriterCannotRetargetAnOldSourceSession) {
    attach();owner->add(SourceSessionRecord{"body",1});bool replaced=false;
    EXPECT_THROW(canonical_source_session_test_access::capture(*adapter,owner,profile.binding,{}, {},limits,{},
        [&]{owner->reopen_write_db();replaced=true;}),lattice::db_error);
    EXPECT_TRUE(replaced);EXPECT_THROW(capture(),lattice::db_error);
    adapter.reset();attach();const auto current=capture();ASSERT_TRUE(current.capture);ASSERT_EQ(current.capture->rows.size(),1u);
}

TEST_F(CanonicalSourceSession, CloseAfterDecisionDoesNotRetroactivelyChangeUnsealedResult) {
    attach();owner->add(SourceSessionRecord{"decided",1});bool closed=false;
    const auto got=canonical_source_session_test_access::capture(*adapter,owner,profile.binding,{}, {},limits,{}, {},
        [&]{owner->close();closed=true;});
    EXPECT_TRUE(closed);ASSERT_TRUE(got.capture);ASSERT_EQ(got.capture->rows.size(),1u);EXPECT_EQ(body(got.capture->rows[0]),"decided");
    EXPECT_THROW(capture(),lattice::db_error);
}

TEST_F(CanonicalSourceSession, ActualOwnerSurvivesBothDecisionCallbacksAndIsReleasedAfterReturn) {
    attach();std::weak_ptr<lattice::lattice_db> weak=owner;bool before=false,after=false;
    const auto got=canonical_source_session_test_access::capture(*adapter,std::move(owner),profile.binding,{}, {},limits,{},
        [&]{before=true;EXPECT_FALSE(weak.expired());},[&]{after=true;EXPECT_FALSE(weak.expired());});
    EXPECT_TRUE(before&&after);EXPECT_TRUE(weak.expired());ASSERT_TRUE(got.capture);EXPECT_EQ(got.head,0);
}

TEST_F(CanonicalSourceSession, MemoryStorageRemainsAnExplicitCaptureRefusal) {
    auto memory=std::make_shared<lattice::lattice_db>(source_config(":memory:"));
    auto source=canonical_writer_adapter::attach(*memory,profile);
    EXPECT_THROW(source->capture_recovery_owned(memory,profile.binding,{}, {},limits),sr::protocol_error);
    EXPECT_EQ(memory->local_read_generations_outstanding(),0u);
}
