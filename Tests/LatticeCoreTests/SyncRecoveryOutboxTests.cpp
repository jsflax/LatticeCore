#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_outbox.hpp"
#include <set>
#include <string_view>

namespace {
using namespace lattice::detail;
using error_code = recovery_outbox_error_code;
using blob = std::vector<uint8_t>;
lattice::configuration outbox_config() {
    lattice::configuration c(":memory:"); c.audit_retention_seconds=0; return c;
}
class OutboxOwner : public lattice::lattice_db {
public:
    OutboxOwner() : lattice_db(outbox_config()) {}
    void doc_schema() {
        lattice::model_schema s; s.table_name="OutboxDoc";
        lattice::property_descriptor body; body.name="body";
        body.type=lattice::column_type::text; body.no_history=true;
        lattice::property_descriptor bytes; bytes.name="bytes";
        bytes.type=lattice::column_type::blob;
        s.properties={body,bytes}; create_model_table_public(s);
    }
};
struct OwnedTransaction {
    lattice::lattice_db& owner; bool done=false;
    explicit OwnedTransaction(lattice::lattice_db& o) : owner(o) { owner.begin_transaction(); }
    ~OwnedTransaction() { if (!done) { try { owner.rollback(); } catch (...) {} } }
    void commit() { owner.commit(); done=true; }
    void rollback() { owner.rollback(); done=true; }
};
template<class F> void expect_error(error_code code, F&& operation) {
    try { operation(); FAIL()<<"expected bounded outbox refusal"; }
    catch (const recovery_outbox_error& e) { EXPECT_EQ(e.code,code)<<e.what(); }
}
class SyncRecoveryOutbox : public ::testing::Test {
protected:
    // Test-only budgets; there are no default production policy values.
    recovery_outbox_limits limits{64,64,8,32,128,10000,65536,1048576};
    OutboxOwner owner;
    recovery_outbox_capture capture() { return capture_pending_outbox(owner,"channel",limits); }
    void person(const std::string& id="person",const std::string& name="original") {
        owner.db().execute("INSERT INTO TestPerson(globalId,name,age,email) VALUES(?,?,9,NULL)",{id,name});
    }
    int64_t audit(const std::string& global_row_id,const std::string& event,
                  int64_t global_sync=0,int64_t remote=0,int64_t synthesized=0,
                  const std::string& table="TestPerson",const std::string& op="UPDATE",
                  const std::string& fields="{\"name\":\"preserved\"}",const std::string& names="[\"name\"]") {
        owner.db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,"
            "changedFieldsNames,isFromRemote,isSynchronized,timestamp,synthesized) VALUES(?,?,?,0,?,?,?,?,?,42.25,?)",
            {event,table,op,global_row_id,fields,names,remote,global_sync,synthesized});
        return static_cast<int64_t>(sqlite3_last_insert_rowid(owner.db().handle()));
    }
    void receipt(int64_t id,const std::string& channel,int64_t synchronized) {
        owner.db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,?)",
            {id,channel,synchronized});
    }
    const recovery_scalar& value(const recovery_outbox_capture& c,size_t row,const std::string& name) {
        const auto& r=c.current_rows.at(row); const auto& t=c.tables.at(r.table_index);
        for (size_t i=0;i<t.columns.size();++i) if(t.columns[i].name==name) return r.values.at(i);
        throw std::runtime_error("missing captured column");
    }
};
int statement_count(sqlite3* db) {
    int result=0; for(auto* s=sqlite3_next_stmt(db,nullptr);s;s=sqlite3_next_stmt(db,s)) ++result;
    return result;
}
struct Authorizer {
    sqlite3* db; bool deny_name=false; int writes=0;
    explicit Authorizer(sqlite3* handle) : db(handle) {
        sqlite3_set_authorizer(db,[](void* context,int action,const char* one,const char* two,const char*,const char*) {
            auto& self=*static_cast<Authorizer*>(context);
            if(action==SQLITE_INSERT || action==SQLITE_UPDATE || action==SQLITE_DELETE) ++self.writes;
            if(self.deny_name && action==SQLITE_READ && one && two &&
               std::string_view(one)=="TestPerson" && std::string_view(two)=="name") return SQLITE_DENY;
            return SQLITE_OK;
        },this);
    }
    ~Authorizer() { sqlite3_set_authorizer(db,nullptr,nullptr); }
};
}

TEST_F(SyncRecoveryOutbox, RequiresSameThreadsActualOwnedWriteTransaction) {
    expect_error(error_code::transaction_required,[&]{capture();});
    owner.db().begin_transaction();
    expect_error(error_code::transaction_required,[&]{capture();});
    owner.db().rollback();
    OwnedTransaction tx(owner);
    std::optional<error_code> other_error;
    std::thread other([&]{try {capture();} catch(const recovery_outbox_error& e){other_error=e.code;}});
    other.join(); ASSERT_TRUE(other_error); EXPECT_EQ(*other_error,error_code::transaction_required);
    EXPECT_TRUE(capture().audit.empty()); tx.commit();
    expect_error(error_code::transaction_required,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, CapturesExplicitAndImplicitPendingWithoutFloorFilterOrOriginLoss) {
    person(); owner.db().execute("DELETE FROM AuditLog");
    const auto implicit=audit("person","implicit",0);
    const auto explicit_pending=audit("person","remote-explicit",1,1,1);
    const auto explicit_ack=audit("person","explicit-ack",0);
    audit("person","global-ack",1);
    const auto other_channel=audit("person","other-channel-ack",0);
    receipt(explicit_pending,"channel",0); receipt(explicit_ack,"channel",1);
    receipt(other_channel,"other-channel",1);
    OwnedTransaction tx(owner); auto c=capture();
    ASSERT_EQ(c.audit.size(),3u); ASSERT_EQ(c.current_rows.size(),1u);
    EXPECT_EQ(c.audit[0].id,implicit); EXPECT_FALSE(c.audit[0].channel_synchronized);
    EXPECT_EQ(c.audit[1].id,explicit_pending); EXPECT_EQ(c.audit[1].channel_synchronized,0);
    EXPECT_TRUE(c.audit[1].from_remote); EXPECT_TRUE(c.audit[1].synthesized);
    EXPECT_TRUE(c.audit[1].globally_synchronized); EXPECT_EQ(std::get<double>(c.audit[1].timestamp),42.25);
    EXPECT_EQ(c.audit[2].id,other_channel); EXPECT_EQ(c.audit[2].global_id,"other-channel-ack");
    EXPECT_EQ(c.audit[1].changed_fields_json,"{\"name\":\"preserved\"}");
    EXPECT_TRUE(c.current_rows[0].present); EXPECT_EQ(c.tables[0].trigger_flags,std::optional<std::string>(""));
}

TEST_F(SyncRecoveryOutbox, NoHistoryPreservesRawAuditAndLatestAllColumnState) {
    owner.doc_schema();
    owner.db().execute("INSERT INTO OutboxDoc(globalId,body,bytes) VALUES('doc',?,?)",{std::string("first"),blob{0,7,0}});
    owner.db().execute("UPDATE OutboxDoc SET body='second' WHERE globalId='doc'");
    OwnedTransaction tx(owner);
    owner.db().execute("UPDATE OutboxDoc SET body='latest inside installer transaction' WHERE globalId='doc'");
    auto c=capture(); ASSERT_EQ(c.audit.size(),3u); ASSERT_EQ(c.current_rows.size(),1u);
    EXPECT_EQ(c.tables[0].trigger_flags,std::optional<std::string>("body"));
    EXPECT_NE(c.audit.back().changed_fields_json.find("\"body\":null"),std::string::npos);
    EXPECT_EQ(std::get<std::string>(value(c,0,"body")),"latest inside installer transaction");
    EXPECT_EQ(std::get<blob>(value(c,0,"bytes")),(blob{0,7,0}));
    EXPECT_EQ(c.current_rows[0].local_row_id,std::get<int64_t>(value(c,0,"id")));
}

TEST_F(SyncRecoveryOutbox, DeletesAreExplicitAbsentRowsAndKeepOriginalIdentity) {
    person(); owner.db().execute("DELETE FROM TestPerson WHERE globalId='person'");
    OwnedTransaction tx(owner); auto c=capture();
    ASSERT_EQ(c.audit.size(),2u); EXPECT_EQ(c.audit.back().operation,"DELETE");
    EXPECT_FALSE(c.audit.back().global_id.empty());
    ASSERT_EQ(c.current_rows.size(),1u); EXPECT_EQ(c.current_rows[0].lookup_global_id,"person");
    EXPECT_FALSE(c.current_rows[0].present); EXPECT_FALSE(c.current_rows[0].local_row_id);
    EXPECT_TRUE(c.current_rows[0].values.empty());
}

TEST_F(SyncRecoveryOutbox, ActualNoRelayDeletePreservesFilterRemovalMarkerAndAbsence) {
    person("filtered"); person("retained"); owner.db().execute("DELETE FROM AuditLog");
    ASSERT_EQ(owner.delete_rows_no_relay("TestPerson",{"filtered"}),1);
    const auto produced=owner.db().query("SELECT id,globalId FROM AuditLog"); ASSERT_EQ(produced.size(),1u);
    OwnedTransaction tx(owner); auto c=capture();
    ASSERT_EQ(c.audit.size(),1u); ASSERT_EQ(c.current_rows.size(),1u);
    const auto& a=c.audit[0];
    EXPECT_EQ(a.operation,"DELETE"); EXPECT_EQ(a.row_id,0); EXPECT_EQ(a.global_row_id,"filtered");
    EXPECT_EQ(a.id,std::get<int64_t>(produced[0].at("id")));
    EXPECT_EQ(a.global_id,std::get<std::string>(produced[0].at("globalId")));
    EXPECT_FALSE(a.from_remote); EXPECT_FALSE(a.globally_synchronized);
    EXPECT_EQ(a.changed_fields_json,"{}"); EXPECT_EQ(a.changed_names_json,"[\"__lattice_filter_removal\"]");
    EXPECT_FALSE(c.current_rows[0].present); EXPECT_EQ(c.current_rows[0].lookup_global_id,"filtered");
    EXPECT_EQ(std::get<int64_t>(owner.db().query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId='retained'").at(0).at("n")),1);
}

TEST_F(SyncRecoveryOutbox, MalformedFilterRemovalShapesRefuseWithoutNormalizingToDelete) {
    person(); owner.db().execute("DELETE FROM AuditLog");
    audit("person","marker",0,0,0,"TestPerson","DELETE","{}","[\"__lattice_filter_removal\"]");
    const std::vector<std::string> mutations={
        "UPDATE AuditLog SET operation='UPDATE'",
        "UPDATE AuditLog SET rowId=1",
        "UPDATE AuditLog SET changedFields='{\"name\":\"x\"}'",
        "UPDATE AuditLog SET changedFieldsNames='[\"__lattice_filter_removal\",null]'",
        "UPDATE AuditLog SET changedFieldsNames='[\"__lattice_filter_removal\",\"__lattice_filter_removal\"]'",
        "UPDATE AuditLog SET changedFields='{\"name\":\"x\"}',changedFieldsNames='[\"__lattice_filter_removal\",\"name\"]'"};
    for(const auto& sql:mutations) {
        OwnedTransaction tx(owner); owner.db().execute(sql);
        expect_error(error_code::corrupt_state,[&]{capture();}); tx.rollback();
    }
    // Narrowing may leave the row present. Preserve that state too; the
    // marker itself must never be interpreted as an ordinary deletion.
    OwnedTransaction tx(owner); auto c=capture(); ASSERT_EQ(c.current_rows.size(),1u);
    EXPECT_TRUE(c.current_rows[0].present);
    EXPECT_EQ(c.audit[0].changed_names_json,"[\"__lattice_filter_removal\"]");
}

TEST_F(SyncRecoveryOutbox, PreservesLiveDeletedAndPolymorphicLinksWithMetadata) {
    owner.ensure_link_table("_OutboxLinks","TestPerson","TestPerson");
    owner.ensure_virtual_link_table("_OutboxVirtualLinks","TestPerson");
    owner.db().execute("INSERT INTO _OutboxLinks(lhs,rhs,globalId) VALUES('left','right','link')");
    owner.db().execute("INSERT INTO _OutboxLinks(lhs,rhs,globalId) VALUES('left','deleted','gone')");
    owner.db().execute("DELETE FROM _OutboxLinks WHERE globalId='gone'");
    owner.db().execute("INSERT INTO _OutboxVirtualLinks(lhs,rhs,rhs_type,globalId) VALUES('left','right','TestPerson','virtual')");
    OwnedTransaction tx(owner); auto c=capture();
    ASSERT_EQ(c.audit.size(),4u); ASSERT_EQ(c.current_rows.size(),3u); ASSERT_EQ(c.tables.size(),2u);
    EXPECT_EQ(c.tables[0].kind,recovery_outbox_table_kind::link);
    EXPECT_EQ(c.tables[0].internal_parent,std::optional<std::string>("TestPerson"));
    EXPECT_EQ(std::get<std::string>(value(c,0,"rhs")),"right");
    EXPECT_FALSE(c.current_rows[1].present);
    EXPECT_EQ(c.tables[1].kind,recovery_outbox_table_kind::polymorphic_link);
    EXPECT_EQ(std::get<std::string>(value(c,2,"rhs_type")),"TestPerson");
}

TEST_F(SyncRecoveryOutbox, TextAndBlobIncludeEmbeddedAndTrailingNulBytes) {
    owner.doc_schema(); const std::string body("a\0b\0",4);
    // Baseline database::bind_value and query TEXT both truncate at NUL. Use
    // an exact SQL fixture, then only the new raw bounded reader as the oracle.
    owner.db().execute("INSERT INTO OutboxDoc(globalId,body,bytes) VALUES('doc',CAST(X'61006200' AS TEXT),X'0002000300')");
    OwnedTransaction tx(owner); auto c=capture(); ASSERT_EQ(c.current_rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(value(c,0,"body")),body);
    EXPECT_EQ(std::get<blob>(value(c,0,"bytes")),(blob{0,2,0,3,0}));
    EXPECT_NE(c.audit[0].changed_fields_json.find("\\u0000"),std::string::npos);
}

TEST_F(SyncRecoveryOutbox, QuotesActualSchemaNamesAndPreservesScalarTypes) {
    owner.db().execute("CREATE TABLE \"_outbox\"\"quoted\"(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE,\"v\"\"alue\" TEXT,payload BLOB)");
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",{std::string("trigger_flags:_outbox\"quoted"),std::string("")});
    owner.db().execute("INSERT INTO \"_outbox\"\"quoted\" VALUES(7,'row','value',X'00FF00')");
    audit("row","quoted",0,0,0,"_outbox\"quoted","INSERT","{\"v\\\"alue\":\"value\",\"payload\":\"00FF00\"}","[\"v\\\"alue\",\"payload\"]");
    OwnedTransaction tx(owner); auto c=capture(); ASSERT_EQ(c.current_rows.size(),1u);
    EXPECT_EQ(c.tables[0].name,"_outbox\"quoted");
    EXPECT_EQ(std::get<std::string>(value(c,0,"v\"alue")),"value");
    EXPECT_EQ(std::get<blob>(value(c,0,"payload")),(blob{0,255,0}));
    EXPECT_EQ(c.current_rows[0].local_row_id,7);
}

TEST_F(SyncRecoveryOutbox, ChannelAndAuditIdentityBytesAreNeverCStringTruncated) {
    person(); owner.db().execute("DELETE FROM AuditLog");
    const auto id=audit("person","placeholder",1,1);
    owner.db().execute("UPDATE AuditLog SET globalId=CAST(X'65007600' AS TEXT) WHERE id=?",{id});
    owner.db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,CAST(X'63006800' AS TEXT),0)",{id});
    OwnedTransaction tx(owner);
    const std::string channel("c\0h\0",4), event("e\0v\0",4);
    auto c=capture_pending_outbox(owner,channel,limits);
    ASSERT_EQ(c.audit.size(),1u); EXPECT_EQ(c.sync_id,channel); EXPECT_EQ(c.audit[0].global_id,event);
    EXPECT_EQ(c.audit[0].channel_synchronized,0);
    EXPECT_TRUE(capture_pending_outbox(owner,"c",limits).audit.empty());
}

TEST_F(SyncRecoveryOutbox, CallerRollbackDiscardsCapturedAndLateWrittenState) {
    { OwnedTransaction tx(owner); person("late","inside write");
      auto c=capture(); ASSERT_EQ(c.audit.size(),1u);
      EXPECT_EQ(std::get<std::string>(value(c,0,"name")),"inside write");
      tx.rollback(); // c must never be used as later installation evidence
    }
    OwnedTransaction tx(owner); auto c=capture(); EXPECT_TRUE(c.audit.empty());
    EXPECT_TRUE(c.current_rows.empty()); EXPECT_TRUE(c.tables.empty());
}

TEST_F(SyncRecoveryOutbox, CountColumnFieldAndLogicalBudgetsRefuseWithoutAnyWrite) {
    person(); OwnedTransaction tx(owner); const auto normal=capture();
    auto* handle=owner.db().handle(); const auto before=sqlite3_total_changes64(handle);
    Authorizer auth(handle);
    const auto original=limits;
    for(int dimension=0;dimension<8;++dimension) {
        limits=original;
        switch(dimension) {
        case 0: limits.audit_records=0; break;
        case 1: limits.current_rows=0; break;
        case 2: limits.tables=0; break;
        case 3: limits.columns_per_table=1; break;
        case 4: limits.total_columns=1; break;
        case 5: limits.fields=normal.charged_fields-1; break;
        case 6: limits.field_bytes=2; break;
        case 7: limits.logical_bytes=normal.charged_logical_bytes-1; break;
        }
        expect_error(error_code::budget_exceeded,[&]{capture();});
    }
    limits=original; EXPECT_EQ(capture().audit.size(),1u);
    EXPECT_EQ(auth.writes,0); EXPECT_EQ(sqlite3_total_changes64(handle),before);
    EXPECT_TRUE(owner.db().is_in_transaction());
}

TEST_F(SyncRecoveryOutbox, OversizedLiveNoHistoryValueCannotBeSilentlyOmitted) {
    owner.doc_schema(); owner.db().execute("INSERT INTO OutboxDoc(globalId,body,bytes) VALUES('doc','small',X'01')");
    owner.db().execute("DELETE FROM AuditLog");
    owner.db().execute("UPDATE OutboxDoc SET body=? WHERE globalId='doc'",{std::string(80000,'x')});
    OwnedTransaction tx(owner);
    // The small historical UPDATE is admissible; the required current body is not.
    expect_error(error_code::budget_exceeded,[&]{capture();});
    limits.field_bytes=100000; EXPECT_EQ(std::get<std::string>(value(capture(),0,"body")).size(),80000u);
}

TEST_F(SyncRecoveryOutbox, MissingCorruptMetadataOrUnknownSchemaRefuses) {
    person(); OwnedTransaction tx(owner);
    owner.db().execute("DELETE FROM _lattice_meta WHERE key='trigger_flags:TestPerson'");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('trigger_flags:TestPerson','missing_column')");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("UPDATE _lattice_meta SET value='' WHERE key='trigger_flags:TestPerson'");
    owner.db().execute("UPDATE AuditLog SET tableName='MissingModel'");
    expect_error(error_code::unsupported_schema,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, MissingLinkMetadataAndUnsupportedInternalModelRefuse) {
    owner.ensure_link_table("_OutboxLinks","TestPerson");
    owner.db().execute("INSERT INTO _OutboxLinks(lhs,rhs,globalId) VALUES('l','r','link')");
    OwnedTransaction tx(owner);
    owner.db().execute("DELETE FROM _lattice_meta WHERE key='internal_table:_OutboxLinks'");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("DELETE FROM AuditLog"); person();
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('internal_table:TestPerson','Parent:field')");
    expect_error(error_code::unsupported_schema,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, AbsentRowDoesNotHideNonAliasPhysicalPrimaryKey) {
    owner.db().execute("CREATE TABLE _OutboxNonAlias(id INTEGER PRIMARY KEY DESC,globalId TEXT UNIQUE,name TEXT)");
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('trigger_flags:_OutboxNonAlias','')");
    audit("absent","nonalias",0,0,0,"_OutboxNonAlias","DELETE");
    OwnedTransaction tx(owner); expect_error(error_code::unsupported_schema,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, UnreadableLiveNoHistoryTypeRefusesInsteadOfDroppingField) {
    owner.doc_schema(); owner.db().execute("INSERT INTO OutboxDoc(globalId,body,bytes) VALUES('doc','original',X'01')");
    owner.db().execute("DELETE FROM AuditLog");
    owner.db().execute("UPDATE OutboxDoc SET body='latest' WHERE globalId='doc'");
    OwnedTransaction tx(owner);
    owner.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    owner.db().execute("UPDATE OutboxDoc SET body=X'0001' WHERE globalId='doc'");
    owner.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    expect_error(error_code::corrupt_state,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, MalformedAuditFlagsPayloadAndPendingReceiptRefuse) {
    person(); OwnedTransaction tx(owner);
    owner.db().execute("UPDATE AuditLog SET isSynchronized=2");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("UPDATE AuditLog SET isSynchronized=0,changedFields='not JSON'");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("UPDATE AuditLog SET changedFields='{\"missing\":1}',changedFieldsNames='[\"missing\"]'");
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("UPDATE AuditLog SET changedFields='{\"name\":\"x\"}',changedFieldsNames='[\"name\"]'");
    owner.db().execute("INSERT INTO _lattice_sync_state SELECT id,'channel',2 FROM AuditLog");
    expect_error(error_code::corrupt_state,[&]{capture();});
}

TEST_F(SyncRecoveryOutbox, PendingReceiptWithoutRetainedAuditBodyRefuses) {
    receipt(999,"channel",0); OwnedTransaction tx(owner);
    expect_error(error_code::corrupt_state,[&]{capture();});
    owner.db().execute("UPDATE _lattice_sync_state SET is_synchronized=1 WHERE audit_entry_id=999");
    EXPECT_TRUE(capture().audit.empty()); // an ACK alone is not an unresolved obligation
}

TEST_F(SyncRecoveryOutbox, SqlFailureFinalizesStatementsAndLeavesCallerTransactionUntouched) {
    person(); OwnedTransaction tx(owner); auto* handle=owner.db().handle();
    const int initial_statements=statement_count(handle);
    { Authorizer auth(handle); auth.deny_name=true;
      expect_error(error_code::sql_error,[&]{capture();}); EXPECT_EQ(auth.writes,0);
    }
    EXPECT_EQ(statement_count(handle),initial_statements);
    EXPECT_TRUE(owner.db().is_in_transaction()); EXPECT_EQ(capture().audit.size(),1u);
    tx.rollback();
}

TEST_F(SyncRecoveryOutbox, NegativePendingAuditIdentityIsRefusedRatherThanMissedByFloor) {
    person(); owner.db().execute("DELETE FROM AuditLog");
    const auto id=audit("person","bad-id");
    owner.db().execute("UPDATE AuditLog SET id=-1 WHERE id=?",{id});
    OwnedTransaction tx(owner); expect_error(error_code::corrupt_state,[&]{capture();});
}
