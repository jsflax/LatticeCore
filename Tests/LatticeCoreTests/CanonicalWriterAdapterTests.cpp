#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"

struct WriterMarkerNode { std::string name; std::string body; };
LATTICE_SCHEMA(WriterMarkerNode,name,body);
struct WriterMarkerLeaf { std::string name; };
LATTICE_SCHEMA(WriterMarkerLeaf,name);
struct WriterMarkerRoot { std::string name; };
LATTICE_SCHEMA(WriterMarkerRoot,name);
namespace {
const bool registered=[] {
    auto stream=lattice::managed<WriterMarkerNode>::schema();stream.properties[1].no_history=true;
    lattice::schema_registry::instance().register_model(typeid(WriterMarkerNode),std::move(stream));
    auto root=lattice::managed<WriterMarkerRoot>::schema();
    lattice::property_descriptor link{};link.name="leaf";link.kind=lattice::property_kind::link;
    link.type=lattice::column_type::integer;link.nullable=true;link.target_table="WriterMarkerLeaf";
    root.properties.push_back(link);
    lattice::schema_registry::instance().register_model(typeid(WriterMarkerRoot),std::move(root));return true;
}();
using namespace lattice::detail;
using blob=std::vector<uint8_t>;
constexpr const char* relation="_WriterMarkerRoot_WriterMarkerLeaf_leaf";
struct Owned {
    lattice::lattice_db& db;bool done=false;
    explicit Owned(lattice::lattice_db& d):db(d){db.begin_transaction();}
    ~Owned(){if(!done)try{db.rollback();}catch(...) {}}
    void commit(){db.commit();done=true;}
    void rollback(){db.rollback();done=true;}
};
canonical_writer_profile profile() {
    return {{"source","fixed-epoch","node-root-leaf","schema-v1"},
        {32,16384,2048,262144,32,64,64},{"WriterMarkerNode","WriterMarkerRoot","WriterMarkerLeaf"},false};
}
lattice::configuration config(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;
}
int64_t scalar(lattice::database& db,const std::string& sql) {
    const auto rows=db.query(sql);if(rows.size()!=1||rows[0].size()!=1)throw std::runtime_error("unexpected scalar shape");
    return std::get<int64_t>(rows[0].begin()->second);
}
canonical_store_state inspect(lattice::lattice_db& db,const canonical_writer_profile& p) {
    Owned tx(db);canonical_change_store s(db,p.binding,p.limits);s.audit();auto state=s.state();tx.commit();return state;
}
std::string audit_id(lattice::lattice_db& db,const std::string& gid) {
    auto rows=db.db().query("SELECT globalId FROM AuditLog WHERE tableName='WriterMarkerNode' AND globalRowId=? ORDER BY id DESC LIMIT 1",{gid});
    if(rows.size()!=1)throw std::runtime_error("missing actual local audit ID");return std::get<std::string>(rows[0].at("globalId"));
}
void stop_notifier(lattice::lattice_db& db) {
    auto* n=lattice::instance_registry::instance().get_or_create_notifier(db.config().path);
    ASSERT_NE(n,nullptr);n->stop_listening();ASSERT_FALSE(n->is_listening());
}
class CanonicalWriterAdapter : public ::testing::Test {
protected:
    lattice::lattice_db db{config(":memory:")};
    canonical_writer_profile p=profile();
    std::unique_ptr<canonical_writer_adapter> attachment;
    void attach(){attachment=canonical_writer_adapter::attach(db,p);}
};
}

TEST_F(CanonicalWriterAdapter, ImplicitOperationsProduceRealReceiptsAndOnePayloadFreeIdentity) {
    attach();auto row=db.add(WriterMarkerNode{"one","small"});const auto gid=row.global_id();
    const auto original=audit_id(db,gid);
    row.body="changed";
    db.remove(row);
    const auto s=inspect(db,p);EXPECT_EQ(s.markers,1);EXPECT_EQ(s.receipts,3);
    EXPECT_EQ(s.head,6);EXPECT_EQ(s.marker_bytes,24+std::string("WriterMarkerNode").size()+36);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);
    const auto receipt=store.receipt(canonical_writer_adapter::uuid_key(original));ASSERT_TRUE(receipt);
    EXPECT_EQ(receipt->original.target,(canonical_identity{"WriterMarkerNode",canonical_writer_adapter::uuid_key(gid)}));
    EXPECT_EQ(receipt->original.outcome,canonical_receipt_outcome::applied);tx.commit();
}

TEST_F(CanonicalWriterAdapter, LargeNoHistoryUpdatesDoNotEnterCanonicalMetadata) {
    attach();auto row=db.add(WriterMarkerNode{"stream","seed"});
    for(int i=1;i<=40;++i)row.body=std::string(i*1024,'x');
    auto s=inspect(db,p);EXPECT_EQ(s.markers,1);EXPECT_EQ(s.receipts,41);
    EXPECT_EQ(s.marker_bytes,24+std::string("WriterMarkerNode").size()+36);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM AuditLog WHERE tableName='WriterMarkerNode' AND operation='UPDATE' AND json_extract(changedFields,'$.body') IS NULL"),40);
    EXPECT_EQ(scalar(db.db(),"SELECT MAX(length(identity)) FROM _lattice_canonical_touch"),36);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM pragma_table_info('_lattice_canonical_touch') WHERE name IN ('payload','value','body')"),0);
}

TEST_F(CanonicalWriterAdapter, AuditDisabledNestedGeneratedProgramsAndLinkCascadeRemainCovered) {
    attach();auto root=db.add(WriterMarkerRoot{"root"});auto leaf=db.add(WriterMarkerLeaf{"leaf"});
    // Exercise the existing on-demand API. It must retain the already validated
    // receipt programs instead of attempting forbidden CREATE TRIGGER again.
    ASSERT_NO_THROW(db.ensure_link_table(relation,"WriterMarkerRoot","WriterMarkerLeaf"));
    db.db().execute("INSERT INTO "+std::string(relation)+"(lhs,rhs) VALUES(?,?)",{root.global_id(),leaf.global_id()});
    auto before=inspect(db,p);EXPECT_EQ(before.markers,3);EXPECT_EQ(before.receipts,3);
    db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    db.db().execute("UPDATE "+std::string(relation)+" SET rhs=rhs"); // Canonical UPDATE exists without an audit UPDATE trigger.
    db.db().execute("UPDATE WriterMarkerRoot SET name='disabled' WHERE id=?",{root.id()});
    db.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    auto middle=inspect(db,p);EXPECT_EQ(middle.head,before.head+2);EXPECT_EQ(middle.receipts,before.receipts);
    db.remove(leaf); // Actual library cascade, with nested generated AuditLog receipt programs.
    auto after=inspect(db,p);EXPECT_EQ(after.markers,3);EXPECT_EQ(after.receipts,5);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM "+std::string(relation)),0);
}

TEST_F(CanonicalWriterAdapter, ReplaceMarksConflictVictimAndReplacementAndPreservesUuidSpelling) {
    // Unique-index DDL is frozen into this source's initial scope before attachment.
    db.db().execute("CREATE UNIQUE INDEX marker_unique_name ON WriterMarkerNode(name)");
    attach();auto old=db.add(WriterMarkerNode{"unique","old"});const auto gid=old.global_id();
    const std::string replacement="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA";
    db.db().execute("INSERT OR REPLACE INTO WriterMarkerNode(globalId,name,body) VALUES(?,'unique','new')",{replacement});
    auto s=inspect(db,p);EXPECT_EQ(s.markers,2);EXPECT_EQ(s.receipts,3);
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT globalId FROM WriterMarkerNode")[0].at("globalId")),replacement);
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);
    EXPECT_TRUE(store.touch({"WriterMarkerNode",canonical_writer_adapter::uuid_key(gid)}));
    EXPECT_TRUE(store.touch({"WriterMarkerNode",canonical_writer_adapter::uuid_key(replacement)}));tx.commit();
    EXPECT_THROW(db.db().execute("PRAGMA recursive_triggers=OFF"),lattice::db_error);
}

TEST_F(CanonicalWriterAdapter, RewritesRefuseButDeleteReinsertCaseAliasKeepsOneKey) {
    attach();const std::string upper="BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB";
    const auto lower=canonical_writer_adapter::uuid_key(upper);
    db.db().execute("INSERT INTO WriterMarkerNode(globalId,name,body) VALUES(?,'case','one')",{upper});
    const auto before=inspect(db,p);
    EXPECT_THROW(db.db().execute("UPDATE WriterMarkerNode SET globalId=?",{lower}),lattice::db_error);
    EXPECT_THROW(db.db().execute("UPDATE WriterMarkerNode SET id=id+10"),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);
    db.db().execute("DELETE FROM WriterMarkerNode WHERE globalId=?",{lower});
    db.db().execute("INSERT INTO WriterMarkerNode(globalId,name,body) VALUES(?,'case','two')",{lower});
    EXPECT_EQ(inspect(db,p).markers,1);
    EXPECT_THROW(db.db().execute("INSERT INTO WriterMarkerNode(globalId,name,body) VALUES('not-a-uuid','bad','bad')"),lattice::db_error);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),1);
}

TEST_F(CanonicalWriterAdapter, CallerRollbackIncludesRowsAuditReceiptsAndHead) {
    attach();const auto before=inspect(db,p);
    {Owned tx(db);db.add(WriterMarkerNode{"rolled","body"});tx.rollback();}
    EXPECT_EQ(inspect(db,p),before);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM AuditLog WHERE tableName='WriterMarkerNode'"),0);
    db.add(WriterMarkerNode{"successor","body"});EXPECT_EQ(inspect(db,p).receipts,1);
}

TEST_F(CanonicalWriterAdapter, CapacityRefusalAbortsWholeMultirowStatementDespiteIgnore) {
    // The first new row fits; the second exceeds the cap. Both must roll back.
    p.limits.markers=2;attach();auto row=db.add(WriterMarkerNode{"kept","body"});auto before=inspect(db,p);
    EXPECT_THROW(db.db().execute("INSERT OR IGNORE INTO WriterMarkerNode(name,body) VALUES('first','body'),('second','body')"),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),1);
    row.body="retry-same-identity";EXPECT_EQ(inspect(db,p).markers,1);
}

TEST_F(CanonicalWriterAdapter, ReceiptRefusalCannotLeaveModelEffectOrTouch) {
    p.limits.receipts=0;attach();const auto before=inspect(db,p);
    EXPECT_THROW(db.add(WriterMarkerNode{"blocked","body"}),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
}

TEST_F(CanonicalWriterAdapter, ExistingOriginalKeepsFirstOutcomeAndTarget) {
    attach();auto row=db.add(WriterMarkerNode{"first","body"});
    const auto original=canonical_writer_adapter::uuid_key(audit_id(db,row.global_id()));
    const auto before=inspect(db,p);
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);const auto prior=store.receipt(original);ASSERT_TRUE(prior);
    auto result=store.record({{"invalid-replacement-table","invalid-id"}},
        canonical_receipt_request{original,canonical_receipt_outcome::policy,std::nullopt});
    EXPECT_FALSE(result.newly_recorded);EXPECT_EQ(result.receipt,prior);tx.commit();EXPECT_EQ(inspect(db,p),before);
}

TEST_F(CanonicalWriterAdapter, ManualAuditInsertionIsNotAnOriginalReceipt) {
    attach();const auto before=inspect(db,p);
    const std::string original="CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC";
    db.db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,isFromRemote,synthesized) VALUES(?,'WriterMarkerNode','INSERT',0,?,0,0)",{original,original});
    EXPECT_EQ(inspect(db,p),before);
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);
    EXPECT_FALSE(store.receipt(canonical_writer_adapter::uuid_key(original)));tx.commit();
}

TEST_F(CanonicalWriterAdapter, CompleteAdmissionRejectsUnsupportedAndPartialDescriptors) {
    p.models={"WriterMarkerRoot"};EXPECT_THROW(attach(),lattice::db_error);
    EXPECT_FALSE(db.db().table_exists("_lattice_canonical_coverage"));
    p=profile();p.upstream_requested=true;EXPECT_THROW(attach(),lattice::db_error);
    p=profile();p.models={"TestPlace"};EXPECT_THROW(attach(),lattice::db_error); // Derived geographic index is unqualified.
    EXPECT_FALSE(canonical_writer_adapter::serving_capability);
}

TEST_F(CanonicalWriterAdapter, ExtraTriggerAndActiveSchemaMutationRefuseWholeScope) {
    db.db().execute("CREATE TRIGGER private_uncovered AFTER INSERT ON WriterMarkerNode BEGIN SELECT 1; END");
    EXPECT_THROW(attach(),lattice::db_error);
    EXPECT_FALSE(db.db().table_exists("_lattice_canonical_coverage"));
}

TEST_F(CanonicalWriterAdapter, AttachedUpstreamRefusesWhileLegacyUnattachedStillApplies) {
    lattice::lattice_db sender{config(":memory:")};sender.add(WriterMarkerNode{"upload","body"});
    const auto entries=lattice::query_audit_log(sender.db(),false,std::nullopt);ASSERT_FALSE(entries.empty());
    lattice::lattice_db legacy{config(":memory:")};EXPECT_EQ(lattice::apply_remote_changes(legacy,entries).size(),entries.size());
    attach();const auto before=inspect(db,p);
    EXPECT_THROW(lattice::apply_remote_changes(db,entries),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
    EXPECT_THROW(db.db().execute("ALTER TABLE WriterMarkerNode ADD COLUMN later TEXT"),lattice::db_error);
    EXPECT_THROW(db.db().execute("DROP TRIGGER _lattice_canonical_WriterMarkerNode_INSERT"),lattice::db_error);
    EXPECT_THROW(db.db().execute("UPDATE _lattice_canonical_store SET head=head+1"),lattice::db_error);
}

TEST_F(CanonicalWriterAdapter, IgnoredCounterWriteRollsBackOriginatingStatement) {
    attach();const auto before=inspect(db,p);
    // Deliberate post-admission fault injection outside supported callback/DDL
    // custody. The SQL postcondition must still catch a silently ignored write.
    auto* raw=db.db().handle();ASSERT_EQ(sqlite3_set_authorizer(raw,nullptr,nullptr),SQLITE_OK);
    db.db().execute("CREATE TRIGGER _fault_ignore BEFORE UPDATE ON _lattice_canonical_store BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(db.add(WriterMarkerNode{"blocked","body"}),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
}

TEST_F(CanonicalWriterAdapter, RevocationFailsCachedStatementsAndExactReattachmentRestoresWrites) {
    attach();sqlite3_stmt* statement=nullptr;auto* raw=db.db().handle();
    ASSERT_EQ(sqlite3_prepare_v2(raw,"INSERT INTO WriterMarkerNode(name,body) VALUES('prepared','body')",-1,&statement,nullptr),SQLITE_OK);
    attachment.reset();EXPECT_NE(sqlite3_step(statement),SQLITE_DONE);sqlite3_finalize(statement);
    EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),0);
    attach();db.add(WriterMarkerNode{"adopted","body"});EXPECT_EQ(inspect(db,p).receipts,1);
}

TEST(CanonicalWriterAdapterReopen, RequiresExplicitExactBindingAndRetainsCommittedFacts) {
    TempDB file{"canonical_writer"};auto p=profile();canonical_store_state before;
    {lattice::lattice_db db{config(file.str())};stop_notifier(db);
     auto attachment=canonical_writer_adapter::attach(db,p);db.add(WriterMarkerNode{"durable","body"});before=inspect(db,p);}
    {lattice::lattice_db db{config(file.str())};stop_notifier(db);
     EXPECT_THROW(db.add(WriterMarkerNode{"unadopted","body"}),lattice::db_error);
     auto changed=p;changed.binding.epoch="different";
     EXPECT_THROW(canonical_writer_adapter::attach(db,changed),canonical_store_error);
     auto attachment=canonical_writer_adapter::attach(db,p);EXPECT_EQ(inspect(db,p),before);
     db.add(WriterMarkerNode{"next","body"});EXPECT_EQ(inspect(db,p).receipts,before.receipts+1);}
}

TEST_F(CanonicalWriterAdapter, EqualValueUpdateIsConservativeTouchWithoutFabricatedReceipt) {
    attach();auto row=db.add(WriterMarkerNode{"same","body"});const auto before=inspect(db,p);
    db.db().execute("UPDATE WriterMarkerNode SET name=name WHERE id=?",{row.id()});
    const auto after=inspect(db,p);EXPECT_EQ(after.markers,before.markers);
    EXPECT_EQ(after.receipts,before.receipts);EXPECT_EQ(after.head,before.head+1);
}

TEST_F(CanonicalWriterAdapter, AuditProgramAndTemporaryShadowRefuseWholeAdmission) {
    db.db().execute("CREATE TRIGGER _fault_audit BEFORE INSERT ON AuditLog BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(attach(),lattice::db_error);EXPECT_FALSE(db.db().table_exists("_lattice_canonical_coverage"));
    // A separate owner keeps the first failed writer's refused authorizer intact.
    lattice::lattice_db other{config(":memory:")};
    other.db().execute("CREATE TEMP TABLE WriterMarkerNode(globalId TEXT)");
    EXPECT_THROW(canonical_writer_adapter::attach(other,p),lattice::db_error);
    EXPECT_FALSE(other.db().table_exists("_lattice_canonical_coverage"));
}

TEST_F(CanonicalWriterAdapter, IgnoredAuditInsertCannotAttestPreviousRow) {
    attach();auto kept=db.add(WriterMarkerNode{"kept","body"});const auto before=inspect(db,p);
    auto* raw=db.db().handle();ASSERT_EQ(sqlite3_set_authorizer(raw,nullptr,nullptr),SQLITE_OK);
    db.db().execute("CREATE TRIGGER _fault_audit BEFORE INSERT ON AuditLog BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(db.add(WriterMarkerNode{"blocked","body"}),lattice::db_error);
    EXPECT_EQ(inspect(db,p),before);EXPECT_EQ(scalar(db.db(),"SELECT COUNT(*) FROM WriterMarkerNode"),1);
}

TEST_F(CanonicalWriterAdapter, LogicalCloseRevokesAlreadyPreparedRawStatement) {
    attach();auto* raw=db.db().handle();sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(raw,"INSERT INTO WriterMarkerNode(name,body) VALUES('closed','body')",-1,&statement,nullptr),SQLITE_OK);
    db.db().close();EXPECT_NE(sqlite3_step(statement),SQLITE_DONE);sqlite3_finalize(statement);
    // Query via the still-owned physical handle only to prove the refused raw
    // statement left no effect; normal database admission is logically closed.
    sqlite3_stmt* query=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(raw,"SELECT COUNT(*) FROM main.WriterMarkerNode",-1,&query,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(query),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int64(query,0),0);sqlite3_finalize(query);
}

TEST(CanonicalWriterAdapterReopen, TwoPhysicalWritersRequireTheirOwnAdmissionAndShareCounters) {
    TempDB file{"canonical_two_writers"};auto p=profile();
    lattice::lattice_db first{config(file.str())};stop_notifier(first);
    auto first_attachment=canonical_writer_adapter::attach(first,p);
    first.add(WriterMarkerNode{"one","body"});
    lattice::lattice_db second{config(file.str())};stop_notifier(second);
    EXPECT_THROW(second.add(WriterMarkerNode{"unadmitted","body"}),lattice::db_error);
    auto second_attachment=canonical_writer_adapter::attach(second,p);
    second.add(WriterMarkerNode{"two","body"});first.add(WriterMarkerNode{"three","body"});
    EXPECT_EQ(inspect(first,p),inspect(second,p));EXPECT_EQ(inspect(first,p).receipts,3);
    second_attachment.reset();EXPECT_THROW(second.add(WriterMarkerNode{"revoked","body"}),lattice::db_error);
    first.add(WriterMarkerNode{"four","body"});EXPECT_EQ(inspect(first,p).receipts,4);
}

TEST_F(CanonicalWriterAdapter, OpaquePriorKeysAreNotSilentlyReinterpretedAsUuidCoverage) {
    {Owned tx(db);canonical_change_store store(db,p.binding,p.limits);store.initialize();
     store.record({{"WriterMarkerNode","legacy-opaque-id"}});tx.commit();}
    EXPECT_THROW(attach(),lattice::db_error);
    EXPECT_FALSE(db.db().table_exists("_lattice_canonical_coverage"));
}
