#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_source_capture.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <algorithm>
#include <cctype>

struct CanonicalCaptureRecord { std::string body; int64_t rank; };
LATTICE_SCHEMA(CanonicalCaptureRecord,body,rank);
namespace {
const bool registered_capture=[] {
    auto schema=lattice::managed<CanonicalCaptureRecord>::schema();schema.properties[0].no_history=true;
    lattice::schema_registry::instance().register_model(typeid(CanonicalCaptureRecord),std::move(schema));return true;
}();
namespace sr=lattice::detail::sync_recovery;
using namespace lattice::detail;
std::string id(char last) {return std::string("aaaa0000-0000-4000-8000-00000000000")+last;}
std::string upper_id(char last) {auto s=id(last);std::transform(s.begin(),s.end(),s.begin(),[](unsigned char c){return std::toupper(c);});return s;}
lattice::configuration capture_config(const std::string& path) {lattice::configuration c(path);c.audit_retention_seconds=0;return c;}
class CanonicalSourceCapture:public ::testing::Test {
protected:
    TempDB path{"canonical-source-capture"};
    lattice::lattice_db db{capture_config(path.str())};
    canonical_store_binding binding{"source","epoch","scope","schema"};
    canonical_store_limits store_limits{128,65536,128,65536,32,64,64};
    sr::canonical_capture_limits limits{{{65536,16,4096,8192,2,32,64,524288},8,16,16},store_limits,32,64,2};
    std::vector<sr::source_relation> scope{{"CanonicalCaptureRecord",sr::relation_kind::model,true}};
    void SetUp() override {db.begin_transaction();canonical_change_store(db,binding,store_limits).initialize();db.commit();}
    canonical_identity key(char c) {return {"CanonicalCaptureRecord",id(c)};}
    int64_t head() {db.begin_transaction();const auto h=canonical_change_store(db,binding,store_limits).state().head;db.commit();return h;}
    void put(char c,const std::string& body,std::optional<char> receipt={}) {
        db.begin_transaction();db.db().execute("INSERT INTO CanonicalCaptureRecord(globalId,body,rank) VALUES(?,?,1) "
            "ON CONFLICT(globalId) DO UPDATE SET body=excluded.body",{id(c),body});
        std::optional<canonical_receipt_request> original;
        if(receipt)original=canonical_receipt_request{id(*receipt),canonical_receipt_outcome::applied,key(c)};
        canonical_change_store(db,binding,store_limits).record({key(c)},original);db.commit();
    }
    void erase(char c) {
        db.begin_transaction();db.db().execute("DELETE FROM CanonicalCaptureRecord WHERE globalId=?",{id(c)});
        canonical_change_store(db,binding,store_limits).record({key(c)});db.commit();
    }
    sr::unsealed_canonical_capture capture(std::optional<int64_t> base={},const std::vector<sr::canonical_capture_request>& requests={}) {
        return sr::capture_canonical_source(db,binding,scope,base,requests,limits);
    }
    std::string body(const sr::canonical_source_row& row) {
        if(!row.payload)throw std::runtime_error("missing source row");
        return std::get<std::string>(sr::decode_values(*row.payload,{8192,16,256,8192,8192}).at("body"));
    }
};
}

TEST_F(CanonicalSourceCapture, FullBaselineIncludesExistingRowsAtZeroHeadAndPreservesSpelling) {
    db.db().execute("INSERT INTO CanonicalCaptureRecord(globalId,body,rank) VALUES(?,'baseline',1)",{upper_id('1')});
    const auto got=capture();ASSERT_EQ(got.rows.size(),1u);EXPECT_EQ(got.head,0);EXPECT_EQ(got.rows[0].key,key('1'));
    ASSERT_TRUE(got.rows[0].payload);const auto values=sr::decode_values(*got.rows[0].payload,{8192,16,256,8192,8192});
    EXPECT_EQ(std::get<std::string>(values.at("globalId")),upper_id('1'));EXPECT_FALSE(values.count("id"));
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, DeltaCoalescesRepeatedWritesAndCarriesAnExplicitTombstone) {
    put('1',"base");const auto base=head();put('1',"middle");put('1',"final");put('2',"temporary");erase('2');
    const auto got=capture(base);ASSERT_EQ(got.rows.size(),2u);EXPECT_EQ(body(got.rows[0]),"final");
    EXPECT_EQ(got.rows[1].key,key('2'));EXPECT_FALSE(got.rows[1].payload);EXPECT_EQ(got.head,head());
}

TEST_F(CanonicalSourceCapture, ReceiptBeforeBaseStillRefreshesItsUnchangedTargetAndMissingIsUnknown) {
    put('1',"accepted",'8');const auto base=head();put('2',"later");
    const auto got=capture(base,{{id('8'),{key('1')}},{id('9'),{key('3')}}});
    ASSERT_EQ(got.rows.size(),3u);EXPECT_EQ(body(got.rows[0]),"accepted");EXPECT_FALSE(got.rows[2].payload);
    ASSERT_EQ(got.receipts.size(),2u);ASSERT_TRUE(got.receipts[0].stored);
    EXPECT_LE(got.receipts[0].stored->position,base);EXPECT_FALSE(got.receipts[1].stored);
}

TEST_F(CanonicalSourceCapture, SameHeadReceiptRefreshIsACompleteNonemptyCapture) {
    put('1',"accepted",'8');const auto base=head();
    const auto got=capture(base,{{id('8'),{key('1')}}});EXPECT_EQ(got.head,base);ASSERT_EQ(got.rows.size(),1u);
    EXPECT_EQ(body(got.rows[0]),"accepted");ASSERT_TRUE(got.receipts[0].stored);
    const auto empty=capture(base);EXPECT_TRUE(empty.rows.empty());EXPECT_TRUE(empty.receipts.empty());EXPECT_EQ(empty.head,base);
}

TEST_F(CanonicalSourceCapture, SourceMutationAfterPinCannotMixRowsHeadOrReceipts) {
    put('1',"one",'8');put('2',"two");const auto before=head();bool changed=false;
    const auto got=sr::source_test_hooks::capture_canonical(db,binding,scope,0,{{id('8'),{key('1')}}},limits,
        [&](size_t batch,uint64_t){if(batch==0&&!changed){changed=true;put('1',"new");erase('2');put('3',"after",'9');}});
    ASSERT_TRUE(changed);EXPECT_EQ(got.head,before);ASSERT_EQ(got.rows.size(),2u);
    EXPECT_EQ(body(got.rows[0]),"one");EXPECT_EQ(body(got.rows[1]),"two");ASSERT_TRUE(got.receipts[0].stored);
    EXPECT_LE(got.receipts[0].stored->position,before);EXPECT_GT(head(),before);
}

TEST_F(CanonicalSourceCapture, ActualAdmittedNoHistoryWriterAndDisabledAuditRemainVisible) {
    const canonical_writer_profile p{binding,store_limits,{"CanonicalCaptureRecord"},false};
    auto adapter=canonical_writer_adapter::attach(db,p);auto object=db.add(CanonicalCaptureRecord{"first",1});
    const auto baseline=capture();ASSERT_EQ(baseline.rows.size(),1u);
    object.body="latest";db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    db.db().execute("UPDATE CanonicalCaptureRecord SET rank=7");db.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    const auto got=capture(baseline.head);ASSERT_EQ(got.rows.size(),1u);EXPECT_EQ(body(got.rows[0]),"latest");
    const auto values=sr::decode_values(*got.rows[0].payload,{8192,16,256,8192,8192});EXPECT_EQ(std::get<int64_t>(values.at("rank")),7);
}

TEST_F(CanonicalSourceCapture, FullAddsAbsentReceiptTargetsWithoutTurningAllMissingRowsIntoDeletes) {
    put('1',"present");const auto got=capture({},{{id('8'),{key('2')}}});
    ASSERT_EQ(got.rows.size(),2u);EXPECT_EQ(got.rows[0].key,key('1'));EXPECT_TRUE(got.rows[0].payload);
    EXPECT_EQ(got.rows[1].key,key('2'));EXPECT_FALSE(got.rows[1].payload);EXPECT_FALSE(got.receipts[0].stored);
}

TEST_F(CanonicalSourceCapture, RetiredAndAheadBasesRefuseWhileFullRemainsAvailable) {
    put('1',"before");const auto floor=head();db.begin_transaction();
    canonical_change_store(db,binding,store_limits).advance_floor(floor,floor);db.commit();
    EXPECT_THROW(capture(0),sr::protocol_error);
    EXPECT_THROW(capture(floor+1),sr::protocol_error);
    const auto got=capture();EXPECT_EQ(got.floor,floor);ASSERT_EQ(got.rows.size(),1u);EXPECT_EQ(body(got.rows[0]),"before");
}

TEST_F(CanonicalSourceCapture, MissingRequestedTargetOrChangedBindingCannotReuseAReceipt) {
    put('1',"original",'8');EXPECT_THROW(capture(0,{{id('8'),{key('2')}}}),sr::protocol_error);
    auto wrong=binding;wrong.epoch="replacement";
    EXPECT_THROW(sr::capture_canonical_source(db,wrong,scope,{}, {},limits),sr::protocol_error);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, EveryExceededFiniteLimitReturnsNoUsablePrefix) {
    put('1',"one");put('2',"two");const auto original=limits;
    limits.rows.wire.total_rows=1;EXPECT_THROW(capture(),sr::protocol_error);limits=original;
    limits.rows.wire.total_bytes=100;EXPECT_THROW(capture(),sr::protocol_error);limits=original;
    limits.requested_targets=1;EXPECT_THROW(capture(0,{{id('8'),{key('1'),key('2')}}}),sr::protocol_error);limits=original;
    limits.requests=1;EXPECT_THROW(capture(0,{{id('8'),{key('1')}},{id('9'),{key('2')}}}),sr::protocol_error);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, ForcedReadGenerationRetirementCannotReturnCapturedHead) {
    put('1',"one");bool retired=false;
    EXPECT_THROW(sr::source_test_hooks::capture_canonical(db,binding,scope,{}, {},limits,
        [&](size_t batch,uint64_t generation){if(batch==0&&!retired){retired=true;db.release_read_generation(generation);}}),sr::protocol_error);
    EXPECT_TRUE(retired);EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, MalformedReceiptAndMarkerStorageRefuseBeforePayloadPublication) {
    put('1',"one",'8');db.db().execute("UPDATE _lattice_canonical_receipt SET charge=X'0001'");
    EXPECT_THROW(capture(0,{{id('8'),{key('1')}}}),sr::protocol_error);
    db.db().execute("UPDATE _lattice_canonical_touch SET identity=X'0001'");EXPECT_THROW(capture(0),sr::protocol_error);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, NonUuidRowsAndUndeclaredScopeAreNotSilentlyDropped) {
    db.db().execute("INSERT INTO CanonicalCaptureRecord(globalId,body,rank) VALUES('legacy','body',1)");
    EXPECT_THROW(capture(),sr::protocol_error);scope[0].complete_table_scope=false;
    EXPECT_THROW(capture(),sr::protocol_error);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, ExactNulTextAndCaptureDoNotMutateTheSource) {
    db.db().execute("INSERT INTO CanonicalCaptureRecord(globalId,body,rank) VALUES(?,CAST(X'610062' AS TEXT),1)",{id('1')});
    const auto before=db.db().query("SELECT total_changes() AS n");const auto got=capture();
    ASSERT_EQ(got.rows.size(),1u);EXPECT_EQ(body(got.rows[0]),std::string("a\0b",3));EXPECT_EQ(db.db().query("SELECT total_changes() AS n"),before);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, ReceiptOnlyAcceptanceAdvancesHeadWithoutInventingARow) {
    db.begin_transaction();canonical_change_store(db,binding,store_limits).record({},
        canonical_receipt_request{id('8'),canonical_receipt_outcome::no_op,key('1')});db.commit();
    const auto delta=capture(0);EXPECT_EQ(delta.head,1);EXPECT_TRUE(delta.rows.empty());
    const auto refresh=capture(1,{{id('8'),{key('1')}}});ASSERT_EQ(refresh.rows.size(),1u);
    EXPECT_FALSE(refresh.rows[0].payload);ASSERT_TRUE(refresh.receipts[0].stored);
    EXPECT_EQ(refresh.receipts[0].stored->original.outcome,canonical_receipt_outcome::no_op);
}

TEST_F(CanonicalSourceCapture, CorruptLargeMetadataTypesAreRefusedBeforeValueCopy) {
    put('1',"one");db.db().execute("UPDATE _lattice_canonical_store SET head=zeroblob(65536)");
    EXPECT_THROW(capture(),sr::protocol_error);EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}

TEST_F(CanonicalSourceCapture, ExcludedMalformedMarkerPositionsCannotPublishAFrontier) {
    put('1',"one",'8');const auto h=head();
    const std::vector<lattice::column_value_t> invalid{
        std::vector<uint8_t>(65536,1),std::string(65536,'x'),int64_t{0},h+1,-1.5};
    for(const auto& position:invalid) {
        db.db().execute("UPDATE _lattice_canonical_touch SET position=?",{position});
        EXPECT_THROW(capture(0),sr::protocol_error);
        EXPECT_THROW(capture(h),sr::protocol_error); // B=H must still validate retained state.
        EXPECT_THROW(capture(),sr::protocol_error);
        EXPECT_EQ(db.local_read_generations_outstanding(),0u);
        db.db().execute("UPDATE _lattice_canonical_touch SET position=?",{h});
    }
    const auto got=capture(0);ASSERT_EQ(got.rows.size(),1u);EXPECT_EQ(body(got.rows[0]),"one");
    EXPECT_EQ(got.head,h);EXPECT_TRUE(capture(h).rows.empty());
}

TEST_F(CanonicalSourceCapture, CounterDriftAndUnrequestedCorruptReceiptsRefuseBeforeHeadPublication) {
    put('1',"one",'8');const auto h=head();
    for(const auto* field:{"markers","marker_bytes","receipts","receipt_bytes"}) {
        const auto original=db.db().query(std::string("SELECT ")+field+" AS n FROM _lattice_canonical_store");
        db.db().execute(std::string("UPDATE _lattice_canonical_store SET ")+field+"=0");
        EXPECT_THROW(capture(h),sr::protocol_error);
        EXPECT_THROW(capture(),sr::protocol_error);
        db.db().execute(std::string("UPDATE _lattice_canonical_store SET ")+field+"=?",{original.at(0).at("n")});
    }
    db.db().execute("UPDATE _lattice_canonical_receipt SET position=zeroblob(65536)");
    EXPECT_THROW(capture(h),sr::protocol_error); // No receipt request is needed to detect bad storage.
    db.db().execute("UPDATE _lattice_canonical_receipt SET position=?",{h});
    const auto got=capture(0,{{id('8'),{key('1')}}});ASSERT_EQ(got.rows.size(),1u);
    ASSERT_TRUE(got.receipts[0].stored);EXPECT_EQ(got.head,h);EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}


#include "../../Sources/LatticeCore/src/canonical_receipt_coverage.hpp"
#include <type_traits>
#if defined(__APPLE__) || defined(__linux__)
namespace {
class CanonicalCoverageSnapshot:public CanonicalSourceCapture {
protected:
    using blob=std::vector<uint8_t>;
    canonical_namespace_profile namespaces{"app",{{"app","app-coverage",1},{"other","other-coverage",1}},
        canonical_coverage_profile{id('f'),7,{"app","other"}}};
    recovery_receipt_binding producer{{"capture-producer",id('e')},id('f'),7,1};
    const std::string digest=std::string(64,'d');
    static blob bytes(const std::string& value){return {value.begin(),value.end()};}
    void SetUp()override {
        db.begin_transaction();canonical_change_store(db,binding,store_limits,&namespaces).initialize();db.commit();
        if(auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path.str()))notifier->stop_listening();
    }
    std::vector<sr::canonical_capture_request> seed(size_t count) {
        if(count==0||count>8)throw std::runtime_error("coverage fixture seed bound");
        std::vector<sr::canonical_capture_request> requests;
        db.begin_transaction();
        try {
            db.db().execute("INSERT INTO CanonicalCaptureRecord(globalId,body,rank) VALUES(?,'stable',1)",{id('1')});
            canonical_change_store store(db,binding,store_limits,&namespaces);
            const auto origin_charge=canonical_origin_charge(producer.producer),cell_charge=canonical_coverage_charge("app");
            for(size_t i=0;i<count;++i) {
                const auto original=id(static_cast<char>('1'+i));
                store.record({key('1')},canonical_receipt_request{original,canonical_receipt_outcome::applied,key('1'),"app"});
                db.db().execute("INSERT INTO _lattice_canonical_receipt_origin VALUES(?,?,?,?,?,?)",
                    {bytes(original),bytes(producer.producer.registration_id),bytes(producer.producer.incarnation),bytes(digest),bytes("INSERT"),origin_charge});
                db.db().execute("INSERT INTO _lattice_canonical_receipt_coverage VALUES(?,?,?,?)",
                    {bytes(original),bytes("app"),static_cast<int64_t>(i+1),cell_charge});
                requests.push_back({original,{key('1')},"app",producer,digest});
            }
            db.db().execute("UPDATE _lattice_canonical_receipt_profile SET mutation=?,origins=?,origin_bytes=?,cells=?,cell_bytes=?",
                {static_cast<int64_t>(count),static_cast<int64_t>(count),origin_charge*static_cast<int64_t>(count),
                 static_cast<int64_t>(count),cell_charge*static_cast<int64_t>(count)});
            store.audit();db.commit();return requests;
        }catch(...){const auto error=std::current_exception();try{db.rollback();}catch(...){}std::rethrow_exception(error);}
    }
    sr::unsealed_canonical_capture covered(const std::vector<sr::canonical_capture_request>& requests,
        const std::function<void(size_t,uint64_t)>& after={}) {
        return sr::source_test_hooks::capture_canonical_namespaced(db,binding,scope,{},requests,limits,namespaces,after);
    }
};
}
TEST(CanonicalCoverageSnapshotAccess, ProofCannotBeConstructedFromMutableStateOrCopiedOut) {
    static_assert(!std::is_default_constructible_v<canonical_coverage_snapshot>);
    static_assert(!std::is_constructible_v<canonical_coverage_snapshot,const canonical_coverage_query&,const canonical_coverage_profile&>);
    static_assert(!std::is_constructible_v<canonical_coverage_snapshot,canonical_coverage_state>);
    static_assert(!std::is_copy_constructible_v<canonical_coverage_snapshot>);
    static_assert(!std::is_move_constructible_v<canonical_coverage_snapshot>);
    SUCCEED();
}
TEST_F(CanonicalCoverageSnapshot, CoveredReceiptLoopUsesFourActualStatementsPerOriginal) {
    const auto requests=seed(8);(void)covered({}); // Warm the same keeper construction path.
    const auto start=lattice::database::thread_statement_count();const auto empty=covered({});
    const auto fixed=lattice::database::thread_statement_count()-start;
    const auto begin=lattice::database::thread_statement_count();const auto captured=covered(requests);
    const auto all=lattice::database::thread_statement_count()-begin;
    EXPECT_EQ(all,fixed+4*requests.size());EXPECT_EQ(captured.coverage_revision,std::optional<uint64_t>{8});
    ASSERT_EQ(captured.receipts.size(),8u);ASSERT_EQ(captured.rows.size(),1u);ASSERT_EQ(empty.rows.size(),1u);
    EXPECT_EQ(captured.rows[0].payload,empty.rows[0].payload);
    for(const auto& receipt:captured.receipts){ASSERT_TRUE(receipt.stored);EXPECT_FALSE(receipt.legacy_unbound);EXPECT_EQ(receipt.operation_digest,digest);}
    // The generic writer/read utility keeps its original five addressed reads.
    size_t generic_reads=0;const auto query=[&](const std::string& sql,const std::vector<lattice::column_value_t>& values){++generic_reads;return db.db().query(sql,values);};
    EXPECT_EQ(lookup_canonical_coverage(query,*namespaces.coverage,requests[0].original_id,"app",producer,digest),canonical_coverage_lookup::covered);
    EXPECT_EQ(generic_reads,5u);EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, LaterProfileMutationCannotChangePinnedViewAndFreshCaptureRejectsIt) {
    const auto requests=seed(2);bool changed=false;
    const auto captured=covered(requests,[&](size_t batch,uint64_t){if(batch==0&&!changed){changed=true;
        db.db().execute("UPDATE _lattice_canonical_receipt_profile SET max_cells=max_cells+1");}});
    ASSERT_TRUE(changed);ASSERT_EQ(captured.receipts.size(),2u);for(const auto& r:captured.receipts)EXPECT_TRUE(r.stored);
    EXPECT_EQ(captured.coverage_revision,std::optional<uint64_t>{2});EXPECT_ANY_THROW(covered(requests));
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, LaterMemberMutationCannotChangePinnedViewAndFreshCaptureRejectsIt) {
    const auto requests=seed(1);bool changed=false;
    const auto captured=covered(requests,[&](size_t batch,uint64_t){if(batch==0&&!changed){changed=true;
        db.db().execute("DELETE FROM _lattice_canonical_receipt_member WHERE namespace_id=CAST('other' AS BLOB)");}});
    ASSERT_TRUE(changed);ASSERT_EQ(captured.receipts.size(),1u);EXPECT_TRUE(captured.receipts[0].stored);
    EXPECT_ANY_THROW(covered(requests));EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, CallerProfileMutationCannotReplaceTheHeldProofInputs) {
    const auto requests=seed(1);bool changed=false;
    const auto captured=covered(requests,[&](size_t batch,uint64_t){if(batch==0&&!changed){changed=true;
        namespaces.coverage.reset();namespaces.entries.clear();namespaces.local_namespace="changed";}});
    ASSERT_TRUE(changed);ASSERT_EQ(captured.receipts.size(),1u);ASSERT_TRUE(captured.receipts[0].stored);
    EXPECT_FALSE(captured.receipts[0].legacy_unbound);EXPECT_EQ(captured.coverage_revision,std::optional<uint64_t>{1});
    EXPECT_ANY_THROW(covered(requests));EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, AddressedProducerAndDigestDivergenceStillRefuse) {
    const auto requests=seed(1);auto wrong=requests;wrong[0].operation_digest=std::string(64,'c');
    EXPECT_ANY_THROW(covered(wrong));wrong=requests;wrong[0].registered_producer->producer.incarnation=id('d');
    EXPECT_ANY_THROW(covered(wrong));const auto captured=covered(requests);ASSERT_TRUE(captured.receipts[0].stored);
    EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, CorruptCellRevisionAndDurableCountersAreNotCachedAway) {
    const auto requests=seed(1);
    db.db().execute("UPDATE _lattice_canonical_receipt_coverage SET revision=2");EXPECT_ANY_THROW(covered(requests));
    db.db().execute("UPDATE _lattice_canonical_receipt_coverage SET revision=1");
    db.db().execute("UPDATE _lattice_canonical_receipt_profile SET mutation=2");EXPECT_ANY_THROW(covered(requests));
    db.db().execute("UPDATE _lattice_canonical_receipt_profile SET mutation=1");
    const auto captured=covered(requests);ASSERT_TRUE(captured.receipts[0].stored);EXPECT_EQ(db.local_read_generations_outstanding(),0u);
}
TEST_F(CanonicalCoverageSnapshot, MissingCoverageAndLegacyNamespaceRulesRemainDistinct) {
    auto requests=seed(1);requests[0].namespace_id="other";const auto missing=covered(requests);
    ASSERT_EQ(missing.receipts.size(),1u);EXPECT_FALSE(missing.receipts[0].stored);
    db.db().execute("DELETE FROM _lattice_canonical_receipt_coverage");db.db().execute("DELETE FROM _lattice_canonical_receipt_origin");
    db.db().execute("UPDATE _lattice_canonical_receipt_profile SET mutation=0,origins=0,origin_bytes=0,cells=0,cell_bytes=0");
    EXPECT_ANY_THROW(covered(requests));requests[0].namespace_id="app";const auto legacy=covered(requests);
    ASSERT_EQ(legacy.receipts.size(),1u);ASSERT_TRUE(legacy.receipts[0].stored);EXPECT_TRUE(legacy.receipts[0].legacy_unbound);
    requests[0].original_id=id('9');const auto unknown=covered(requests);ASSERT_EQ(unknown.receipts.size(),1u);EXPECT_FALSE(unknown.receipts[0].stored);
}
TEST_F(CanonicalCoverageSnapshot, RetiredGenerationCannotUsePreviouslyValidatedSnapshot) {
    const auto requests=seed(1);bool retired=false;
    EXPECT_THROW(covered(requests,[&](size_t batch,uint64_t generation){if(batch==0&&!retired){retired=true;db.release_read_generation(generation);}}),sr::protocol_error);
    EXPECT_TRUE(retired);EXPECT_EQ(db.local_read_generations_outstanding(),0u);const auto fresh=covered(requests);ASSERT_TRUE(fresh.receipts[0].stored);
}
#endif
