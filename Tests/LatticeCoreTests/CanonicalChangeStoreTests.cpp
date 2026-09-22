#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/canonical_change_store.hpp"
#include <limits>

namespace {
using namespace lattice::detail;
using err = canonical_store_error_code;
using outcome = canonical_receipt_outcome;
using blob = std::vector<uint8_t>;
blob bytes(const std::string& s) { return {s.begin(),s.end()}; }
struct Owned {
    lattice::lattice_db& db; bool done=false;
    explicit Owned(lattice::lattice_db& value):db(value) { db.begin_transaction(); }
    ~Owned() { if (!done) { try { db.rollback(); } catch (...) {} } }
    void commit() { db.commit(); done=true; }
    void rollback() { db.rollback(); done=true; }
};
template<class F> void refused(err expected,F&& f) {
    try { f(); FAIL()<<"expected canonical store refusal"; }
    catch (const canonical_store_error& e) { EXPECT_EQ(e.code,expected)<<e.what(); }
}
canonical_store_binding binding() { return {"source","epoch","complete-table-scope","schema"}; }
canonical_store_limits limits() { return {8,4096,8,4096,8,64,64}; }
canonical_receipt_request request(std::string id="original",outcome result=outcome::applied) {
    return {std::move(id),result,canonical_identity{"TestPerson","row"}};
}
lattice::configuration config(const std::string& path) {
    lattice::configuration c(path); c.audit_retention_seconds=0; c.busy_timeout_ms=100; return c;
}
void stop_notifier(lattice::lattice_db& owner) {
    auto* n=lattice::instance_registry::instance().get_or_create_notifier(owner.config().path);
    ASSERT_NE(n,nullptr); n->stop_listening(); ASSERT_FALSE(n->is_listening());
}
class CanonicalChangeStore : public ::testing::Test {
protected:
    lattice::lattice_db db{config(":memory:")};
    canonical_store_limits budget=limits();
    canonical_change_store store() { return {db,binding(),budget}; }
    void initialize() { Owned tx(db); store().initialize(); tx.commit(); }
};
}

TEST_F(CanonicalChangeStore, RequiresActualOwnedWriterAndChecksEveryCall) {
    auto s=store(); refused(err::transaction_required,[&]{s.initialize();});
    db.db().begin_transaction(); refused(err::transaction_required,[&]{s.initialize();}); db.db().rollback();
    Owned tx(db); s.initialize();
    std::optional<err> other_result;
    std::thread other([&] { try { s.record({{"T","id"}}); } catch(const canonical_store_error& e) { other_result=e.code; } });
    other.join(); ASSERT_TRUE(other_result); EXPECT_EQ(*other_result,err::transaction_required);
    tx.commit(); refused(err::transaction_required,[&]{s.state();});
}

TEST_F(CanonicalChangeStore, InitializationIsAtomicAndNeverMigratesPartialState) {
    { Owned tx(db); store().initialize(); tx.rollback(); }
    Owned tx(db);
    EXPECT_TRUE(db.db().query("SELECT name FROM sqlite_master WHERE name LIKE '_lattice_canonical_%'").empty());
    db.db().execute("CREATE TABLE _lattice_canonical_store(inherited INTEGER)");
    refused(err::corrupt_state,[&]{store().initialize();}); tx.rollback();
    initialize();
    Owned verify(db); auto wrong=binding(); wrong.epoch="new-epoch";
    canonical_change_store stale(db,wrong,budget);
    refused(err::binding_mismatch,[&]{stale.record({{"T","id"}});});
    auto b=budget; ++b.markers; canonical_change_store changed(db,binding(),b);
    refused(err::limits_mismatch,[&]{changed.initialize();});
    EXPECT_EQ(store().state(),canonical_store_state{}); verify.commit();
}

TEST_F(CanonicalChangeStore, RepeatedIdentitiesHaveOneMarkerAndFirstReceiptWins) {
    initialize(); Owned tx(db); auto s=store();
    const canonical_identity a{"T","a"},b{"T","b"};
    auto first=s.record({a,a,b},request());
    EXPECT_TRUE(first.newly_recorded); EXPECT_EQ(first.position,1); EXPECT_EQ(s.state().markers,2);
    EXPECT_EQ(s.touch(a),std::optional<int64_t>(1));
    const auto before=s.state();
    auto replay=request(); replay.outcome=outcome::policy; replay.target=canonical_identity{"Changed","identity"};
    auto duplicate=s.record({{"Never","recorded"}},replay);
    EXPECT_FALSE(duplicate.newly_recorded); EXPECT_EQ(duplicate.receipt,first.receipt); EXPECT_EQ(s.state(),before);
    EXPECT_FALSE(s.touch({"Never","recorded"}));
    EXPECT_EQ(s.record({a}).position,2); EXPECT_EQ(s.touch(a),std::optional<int64_t>(2));
    EXPECT_EQ(s.state().markers,2); EXPECT_EQ(s.state().marker_bytes,before.marker_bytes);
    s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, NoOpReceiptAdvancesHeadButAbsentReceiptRemainsUnknown) {
    initialize(); Owned tx(db); auto s=store();
    EXPECT_FALSE(s.receipt("possibly-sent-legacy"));
    EXPECT_FALSE(s.record({}).newly_recorded); EXPECT_EQ(s.state().head,0);
    canonical_receipt_request no_op{"original-no-op",outcome::no_op,std::nullopt};
    auto accepted=s.record({},no_op);
    ASSERT_TRUE(accepted.receipt); EXPECT_EQ(accepted.position,1); EXPECT_EQ(s.state().markers,0);
    EXPECT_EQ(accepted.receipt->original,no_op); EXPECT_EQ(s.state().receipts,1);
    s.advance_floor(1,1); EXPECT_TRUE(s.receipt("original-no-op"));
    EXPECT_FALSE(s.receipt("possibly-sent-legacy")); tx.commit();
}

TEST_F(CanonicalChangeStore, MarkerCountAndByteCapsReserveNoPartialPrefix) {
    budget.markers=2; budget.marker_bytes=52; // exactly two (24 + 1 + 1) markers
    initialize(); Owned tx(db); auto s=store();
    refused(err::capacity,[&]{s.record({{"T","a"},{"T","b"},{"T","c"}});});
    EXPECT_EQ(s.state(),canonical_store_state{});
    s.record({{"T","a"},{"T","b"}}); const auto before=s.state();
    refused(err::capacity,[&]{s.record({{"T","a"},{"T","c"}},request());});
    EXPECT_EQ(s.state(),before); EXPECT_EQ(s.touch({"T","a"}),std::optional<int64_t>(1));
    EXPECT_FALSE(s.receipt("original")); s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, ByteCapAndBatchCapAreIndependentOfIdentityCount) {
    budget.marker_bytes=26; budget.batch_identities=2; initialize(); Owned tx(db); auto s=store();
    refused(err::capacity,[&]{s.record({{"T","longer"}});});
    refused(err::capacity,[&]{s.record({{"T","a"},{"T","a"},{"T","a"}});});
    refused(err::invalid_argument,[&]{s.record({{"T",std::string(65,'x')}});});
    EXPECT_EQ(s.state().head,0); s.record({{"T","a"}}); s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, ReceiptCapsDoNotEvictAndFloorCannotReclaimReceipts) {
    budget.receipts=1; budget.receipt_bytes=33; initialize(); Owned tx(db); auto s=store();
    canonical_receipt_request too_big{"xx",outcome::no_op,std::nullopt},fits{"x",outcome::no_op,std::nullopt};
    refused(err::capacity,[&]{s.record({},too_big);}); EXPECT_EQ(s.state().head,0);
    s.record({},fits); s.advance_floor(1,1); const auto before=s.state();
    refused(err::capacity,[&]{s.record({{"T","a"}},canonical_receipt_request{"y",outcome::no_op,std::nullopt});});
    EXPECT_EQ(s.state(),before); EXPECT_TRUE(s.receipt("x")); EXPECT_FALSE(s.receipt("y"));
    EXPECT_FALSE(s.record({},fits).newly_recorded); tx.commit();
}

TEST_F(CanonicalChangeStore, FloorAdvancesWithDeletionAndHonorsProtectedBase) {
    initialize(); Owned tx(db); auto s=store();
    s.record({{"T","deleted"}},request()); s.record({{"T","live"}}); s.record({{"T","live"}});
    refused(err::protected_floor,[&]{s.advance_floor(2,1);}); EXPECT_EQ(s.state().floor,0);
    s.advance_floor(1,1); EXPECT_FALSE(s.touch({"T","deleted"})); EXPECT_TRUE(s.touch({"T","live"}));
    EXPECT_TRUE(s.receipt("original")); refused(err::base_retired,[&]{s.require_base(0);});
    EXPECT_NO_THROW(s.require_base(1)); refused(err::base_ahead,[&]{s.require_base(4);});
    refused(err::invalid_argument,[&]{s.advance_floor(0,3);});
    EXPECT_EQ(s.record({{"T","deleted"}}).position,4); EXPECT_EQ(s.state().markers,2);
    s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, IgnoredFloorDeleteRestoresFloorAndMarkers) {
    initialize(); Owned tx(db); auto s=store(); s.record({{"T","a"}}); const auto before=s.state();
    db.db().execute("CREATE TRIGGER canonical_ignore_delete BEFORE DELETE ON _lattice_canonical_touch BEGIN SELECT RAISE(IGNORE); END");
    refused(err::corrupt_state,[&]{s.advance_floor(1,1);}); EXPECT_EQ(s.state(),before); EXPECT_TRUE(s.touch({"T","a"}));
    db.db().execute("DROP TRIGGER canonical_ignore_delete"); s.advance_floor(1,1); EXPECT_EQ(s.state().markers,0); tx.commit();
}

TEST(CanonicalChangeStoreFaults, AbortedOrIgnoredWritesRestoreEveryCounterAndAllowRetry) {
    for (auto mode : {"ABORT,'injected canonical fault'","IGNORE"}) {
        for (auto stage : {"store","insert","update","receipt"}) {
            SCOPED_TRACE(std::string(stage)+"/"+mode);
            lattice::lattice_db db{config(":memory:")}; Owned tx(db); canonical_change_store s(db,binding(),limits()); s.initialize();
            s.record({{"T","existing"}}); const auto before=s.state(); int calls=0;
            ASSERT_EQ(sqlite3_create_function(db.db().handle(),"canonical_fault_seen",0,SQLITE_UTF8,&calls,
                [](sqlite3_context* c,int,sqlite3_value**) noexcept { ++*static_cast<int*>(sqlite3_user_data(c)); sqlite3_result_int(c,1); },
                nullptr,nullptr),SQLITE_OK);
            const std::string stage_name=stage;
            const std::string target=stage_name=="store" ? "UPDATE ON _lattice_canonical_store" :
                stage_name=="insert" ? "INSERT ON _lattice_canonical_touch" :
                stage_name=="update" ? "UPDATE ON _lattice_canonical_touch" : "INSERT ON _lattice_canonical_receipt";
            db.db().execute("CREATE TRIGGER canonical_fault BEFORE "+target+" BEGIN SELECT canonical_fault_seen(); SELECT RAISE("+mode+"); END");
            const std::vector<canonical_identity> ids=stage_name=="update" ? std::vector<canonical_identity>{{"T","existing"}} :
                std::vector<canonical_identity>{{"T","fresh"}};
            EXPECT_THROW(s.record(ids,request()),std::exception); EXPECT_EQ(calls,1);
            EXPECT_EQ(s.state(),before); EXPECT_FALSE(s.receipt("original")); EXPECT_FALSE(s.touch({"T","fresh"}));
            EXPECT_EQ(s.touch({"T","existing"}),std::optional<int64_t>(1)); s.audit();
            db.db().execute("DROP TRIGGER canonical_fault");
            EXPECT_TRUE(s.record(ids,request()).newly_recorded); s.audit();
            ASSERT_EQ(sqlite3_create_function(db.db().handle(),"canonical_fault_seen",0,SQLITE_UTF8,nullptr,nullptr,nullptr,nullptr),SQLITE_OK);
            tx.commit();
        }
    }
}

TEST_F(CanonicalChangeStore, ModelAndRecordShareOuterRollbackAndSuccessfulSuccessor) {
    auto person=db.add(TestPerson{"before",10,std::nullopt}); const auto id=person.global_id(); initialize();
    std::string failed_id,committed_id;
    {
        Owned tx(db); auto s=store();
        db.db().execute("CREATE TRIGGER canonical_receipt_failure BEFORE INSERT ON _lattice_canonical_receipt BEGIN SELECT RAISE(ABORT,'receipt failure'); END");
        db.db().execute("UPDATE TestPerson SET age=11 WHERE globalId=?",{id});
        const auto audit=db.db().query("SELECT globalId FROM AuditLog WHERE tableName='TestPerson' AND operation='UPDATE' "
            "AND globalRowId=? ORDER BY id DESC LIMIT 1",{id}); ASSERT_EQ(audit.size(),1u);
        failed_id=std::get<std::string>(audit.at(0).at("globalId"));
        const canonical_receipt_request original{failed_id,outcome::applied,canonical_identity{"TestPerson",id}};
        EXPECT_THROW(s.record({{"TestPerson",id}},original),lattice::db_error);
        EXPECT_EQ(s.state(),canonical_store_state{});
        tx.rollback(); // caller's model, AuditLog and helper state all revert
    }
    {
        Owned tx(db); auto s=store();
        EXPECT_EQ(std::get<int64_t>(db.db().query("SELECT age FROM TestPerson WHERE globalId=?",{id}).at(0).at("age")),10);
        EXPECT_FALSE(s.receipt(failed_id));
        EXPECT_TRUE(db.db().query("SELECT 1 FROM AuditLog WHERE globalId=?",{failed_id}).empty());
        db.db().execute("UPDATE TestPerson SET age=12 WHERE globalId=?",{id});
        const auto audit=db.db().query("SELECT globalId FROM AuditLog WHERE tableName='TestPerson' AND operation='UPDATE' "
            "AND globalRowId=? ORDER BY id DESC LIMIT 1",{id}); ASSERT_EQ(audit.size(),1u);
        committed_id=std::get<std::string>(audit.at(0).at("globalId"));
        s.record({{"TestPerson",id}},canonical_receipt_request{committed_id,outcome::applied,canonical_identity{"TestPerson",id}}); tx.commit();
    }
    Owned verify(db); auto s=store(); EXPECT_EQ(s.state().head,1); EXPECT_TRUE(s.receipt(committed_id));
    EXPECT_EQ(std::get<int64_t>(db.db().query("SELECT age FROM TestPerson WHERE globalId=?",{id}).at(0).at("age")),12); verify.commit();
}

TEST_F(CanonicalChangeStore, OuterRollbackDiscardsSuccessfulRecordAndFloorAdvance) {
    initialize(); { Owned tx(db); store().record({{"T","a"}},request()); tx.commit(); }
    { Owned tx(db); auto s=store(); s.record({{"T","b"}},request("later")); s.advance_floor(1,1); tx.rollback(); }
    Owned verify(db); auto s=store(); EXPECT_EQ(s.state().head,1); EXPECT_EQ(s.state().floor,0);
    EXPECT_TRUE(s.touch({"T","a"})); EXPECT_FALSE(s.touch({"T","b"})); EXPECT_FALSE(s.receipt("later")); s.audit(); verify.commit();
}

TEST_F(CanonicalChangeStore, FailedOuterCommitCannotPublishTentativeHeadOrReceipt) {
    initialize();
    {
        Owned tx(db); auto s=store(); s.record({{"T","a"}},request()); int attempts=0;
        ASSERT_EQ(sqlite3_set_authorizer(db.db().handle(),
            [](void* value,int action,const char* first,const char*,const char*,const char*) noexcept {
                if (action==SQLITE_TRANSACTION && first && std::strcmp(first,"COMMIT")==0) {
                    ++*static_cast<int*>(value); return SQLITE_DENY;
                }
                return SQLITE_OK;
            },&attempts),SQLITE_OK);
        EXPECT_THROW(tx.commit(),lattice::db_error); EXPECT_EQ(attempts,1);
        ASSERT_EQ(sqlite3_set_authorizer(db.db().handle(),nullptr,nullptr),SQLITE_OK); tx.rollback();
    }
    Owned verify(db); EXPECT_EQ(store().state(),canonical_store_state{}); EXPECT_FALSE(store().receipt("original")); verify.commit();
}

TEST_F(CanonicalChangeStore, CleanupFailureCarriesBothErrorsAndRequiresOuterRollback) {
    initialize();
    {
        Owned tx(db); auto s=store(); int denied=0;
        db.db().execute("CREATE TRIGGER canonical_fail BEFORE INSERT ON _lattice_canonical_receipt BEGIN SELECT RAISE(ABORT,'receipt'); END");
        ASSERT_EQ(sqlite3_set_authorizer(db.db().handle(),
            [](void* value,int action,const char* first,const char* second,const char*,const char*) noexcept {
                if (action==SQLITE_SAVEPOINT && first && second && std::strcmp(first,"ROLLBACK")==0 &&
                    std::strcmp(second,"lattice_canonical_primitive")==0) {
                    ++*static_cast<int*>(value); return SQLITE_DENY;
                }
                return SQLITE_OK;
            },&denied),SQLITE_OK);
        bool caught=false;
        try { s.record({{"T","a"}},request()); }
        catch (const canonical_store_error& e) {
            caught=true; EXPECT_EQ(e.code,err::cleanup_failed); EXPECT_TRUE(e.primary_error); EXPECT_TRUE(e.cleanup_error);
        }
        catch (...) { ADD_FAILURE()<<"expected structured cleanup failure"; }
        ASSERT_EQ(sqlite3_set_authorizer(db.db().handle(),nullptr,nullptr),SQLITE_OK);
        EXPECT_TRUE(caught); EXPECT_EQ(denied,1); tx.rollback();
    }
    Owned verify(db); EXPECT_EQ(store().state(),canonical_store_state{}); store().audit(); verify.commit();
}

TEST_F(CanonicalChangeStore, FullAuditRejectsCounterDriftAndAddressedMalformedState) {
    initialize(); Owned tx(db); auto s=store(); s.record({{"T","a"}},request());
    db.db().execute("UPDATE _lattice_canonical_store SET marker_bytes=marker_bytes+1");
    refused(err::corrupt_state,[&]{s.audit();});
    db.db().execute("UPDATE _lattice_canonical_store SET marker_bytes=marker_bytes-1");
    db.db().execute("UPDATE _lattice_canonical_receipt SET identity=?",{bytes(std::string(65,'x'))});
    refused(err::corrupt_state,[&]{s.receipt("original");}); refused(err::corrupt_state,[&]{s.initialize();});
    tx.rollback();
}

TEST_F(CanonicalChangeStore, SequenceExhaustionDoesNotWrapOrConsumeAnything) {
    initialize(); Owned tx(db); auto s=store();
    db.db().execute("UPDATE _lattice_canonical_store SET head=?",{std::numeric_limits<int64_t>::max()});
    const auto before=s.state(); refused(err::sequence_exhausted,[&]{s.record({},request());});
    EXPECT_EQ(s.state(),before); EXPECT_FALSE(s.receipt("original")); s.audit(); tx.rollback();
}

TEST_F(CanonicalChangeStore, OpaqueOriginalIdsAreExactAndNoWriterCoverageIsImplied) {
    initialize(); db.add(TestPerson{"unattached write",1,std::nullopt});
    Owned tx(db); auto s=store(); EXPECT_EQ(s.state().head,0); EXPECT_EQ(s.state().markers,0);
    const std::string id("original\0id",11); auto r=request(id,outcome::no_op); s.record({},r);
    EXPECT_TRUE(s.receipt(id)); EXPECT_FALSE(s.receipt("original")); EXPECT_FALSE(s.receipt("ORIGINAL")); s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, MetadataDoesNotPublishModelObserverEvents) {
    struct Observed {
        lattice::lattice_db& db;
        std::vector<std::pair<std::string,lattice::lattice_db::observer_id>> tokens;
        int metadata=0,model=0;
        explicit Observed(lattice::lattice_db& owner):db(owner) {
            for (const auto* name : {"_lattice_canonical_store","_lattice_canonical_touch","_lattice_canonical_receipt"})
                tokens.emplace_back(name,db.add_table_observer(name,[this](const auto&){++metadata;}));
            tokens.emplace_back("TestPerson",db.add_table_observer("TestPerson",[this](const auto&){++model;}));
        }
        ~Observed() { for (const auto& [name,id]:tokens) db.remove_table_observer(name,id); }
    } observed(db);
    initialize(); { Owned tx(db); auto s=store(); s.record({{"T","a"}},request()); s.advance_floor(1,1); tx.commit(); }
    db.add(TestPerson{"positive observer fence",1,std::nullopt});
    EXPECT_GT(observed.model,0); EXPECT_EQ(observed.metadata,0);
}

TEST(CanonicalChangeStoreDurable, ReopenAuditsHeadFloorMarkersAndRetainedReceipts) {
    TempDB file{"canonical_marker_reopen"};
    {
        lattice::lattice_db db{config(file.str())}; ASSERT_NO_FATAL_FAILURE(stop_notifier(db));
        Owned tx(db); canonical_change_store s(db,binding(),limits()); s.initialize();
        s.record({{"T","old"}},request()); s.record({{"T","new"}}); s.advance_floor(1,1); tx.commit();
    }
    {
        lattice::lattice_db db{config(file.str())}; ASSERT_NO_FATAL_FAILURE(stop_notifier(db));
        Owned tx(db); canonical_change_store s(db,binding(),limits()); s.initialize();
        EXPECT_EQ(s.state().head,2); EXPECT_EQ(s.state().floor,1); EXPECT_FALSE(s.touch({"T","old"}));
        EXPECT_EQ(s.touch({"T","new"}),std::optional<int64_t>(2)); EXPECT_TRUE(s.receipt("original"));
        EXPECT_FALSE(s.record({},request()).newly_recorded); tx.commit();
    }
}

TEST(CanonicalChangeStoreNoHistory, GrowingValuesRemainOnlyInCurrentRowsNotMarkers) {
    lattice::property_descriptor body; body.name="body"; body.type=lattice::column_type::text;
    body.kind=lattice::property_kind::primitive; body.no_history=true;
    lattice::swift_schema_entry schema; schema.table_name="CanonicalNoHistory"; schema.properties["body"]=body;
    lattice::SchemaVector schemas{schema};
    lattice::swift_lattice db{lattice::swift_configuration(":memory:"),schemas};
    canonical_change_store s(db,binding(),limits()); Owned tx(db); s.initialize();
    db.db().execute("INSERT INTO CanonicalNoHistory(globalId,body) VALUES('row','initial')");
    const canonical_identity row{"CanonicalNoHistory","row"}; s.record({row}); const auto charged=s.state().marker_bytes;
    std::string current;
    for (int n=1;n<=24;++n) {
        current=std::string(static_cast<size_t>(n)*1024,'x');
        db.db().execute("UPDATE CanonicalNoHistory SET body=? WHERE globalId='row'",{current}); s.record({row});
    }
    EXPECT_EQ(s.state().head,25); EXPECT_EQ(s.state().markers,1); EXPECT_EQ(s.state().marker_bytes,charged);
    EXPECT_EQ(db.db().query("PRAGMA table_info(_lattice_canonical_touch)").size(),4u);
    const auto audit=db.db().query("SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='CanonicalNoHistory' AND operation='UPDATE' "
        "AND json_type(changedFields,'$.body')='null'");
    EXPECT_EQ(std::get<int64_t>(audit.at(0).at("n")),24);
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT body FROM CanonicalNoHistory WHERE globalId='row'").at(0).at("body")),current);
    db.db().execute("DELETE FROM CanonicalNoHistory WHERE globalId='row'"); s.record({row});
    EXPECT_TRUE(db.db().query("SELECT 1 FROM CanonicalNoHistory WHERE globalId='row'").empty());
    EXPECT_EQ(s.touch(row),std::optional<int64_t>(26));
    db.db().execute("INSERT INTO CanonicalNoHistory(globalId,body) VALUES('row','reinserted')"); s.record({row});
    EXPECT_EQ(s.touch(row),std::optional<int64_t>(27)); EXPECT_EQ(s.state().markers,1);
    EXPECT_EQ(s.state().marker_bytes,charged); // deletion evidence is identity-only as well
    s.audit(); tx.commit();
}

TEST_F(CanonicalChangeStore, AddressedRecordDoesNotScanRetainedMarkerSet) {
    budget.markers=4096; budget.marker_bytes=1048576; initialize(); Owned tx(db); auto s=store();
    db.db().execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<3072) "
        "INSERT INTO _lattice_canonical_touch SELECT CAST('T' AS BLOB),CAST(x AS BLOB),x,25+length(CAST(x AS BLOB)) FROM n");
    db.db().execute("UPDATE _lattice_canonical_store SET head=3072,markers=3072,marker_bytes="
        "(SELECT SUM(charge) FROM _lattice_canonical_touch)"); s.audit();
    int callbacks=0; auto* handle=db.db().handle();
    sqlite3_progress_handler(handle,10,[](void* value) noexcept { return ++*static_cast<int*>(value)>2000 ? 1:0; },&callbacks);
    EXPECT_NO_THROW(s.record({{"T","3072"}},request()));
    sqlite3_progress_handler(handle,0,nullptr,nullptr);
    EXPECT_LE(callbacks,2000); EXPECT_EQ(s.state().markers,3072); EXPECT_EQ(s.state().head,3073); s.audit(); tx.commit();
}
