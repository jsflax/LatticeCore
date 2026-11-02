#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <cstdio>
#include <lattice/detached_read.hpp>

namespace {
using namespace lattice;
std::string identity(int value) { char text[37]; std::snprintf(text,sizeof(text),"00000000-0000-0000-0000-%012d",value); return text; }
SchemaVector schemas() {
    swift_schema_entry row;row.table_name="ScopedAsset";
    for(const auto& [name,type]:std::vector<std::pair<std::string,column_type>>{{"ownerID",column_type::text},{"label",column_type::text},{"ordinal",column_type::integer},{"payload",column_type::blob},{"embedding",column_type::blob}}) {
        property_descriptor property;property.name=name;property.type=type;property.kind=property_kind::primitive;property.is_indexed=name=="ownerID";row.properties[name]=property;
    } return {row};
}
swift_configuration config(const std::string& path,bool readOnly=false) {swift_configuration c(path);c.target_schema_version=6;c.read_only=readOnly;if(readOnly)c.required_existing_schema_version=6;return c;}
std::shared_ptr<swift_lattice> opened(const std::string& path,bool readOnly=false) {return std::make_shared<swift_lattice>(config(path,readOnly),schemas());}
void insert(swift_lattice& db,int n,size_t size=3,std::string label="saved",int owner=100) {
    db.db().execute("INSERT INTO ScopedAsset(globalId,ownerID,label,ordinal,payload,embedding) VALUES(?,?,?,?,?,?)",
        {identity(n),identity(owner),label,static_cast<int64_t>(n),std::vector<uint8_t>(size,static_cast<uint8_t>(n)),std::vector<uint8_t>(1048576,99)});
}
detached_read::exact_request request(std::vector<std::string> ids,std::vector<std::string> fields={"ownerID","label","ordinal","payload"}) {
    detached_read::exact_request r;r.table="ScopedAsset";r.properties=std::move(fields);r.global_ids=std::move(ids);return r;
}
detached_read::indexed_request page(size_t limit=2) {
    detached_read::indexed_request r;r.table="ScopedAsset";r.properties={"ownerID","ordinal","label"};r.property="ownerID";r.equal_to=identity(100);r.limit=limit;return r;
}
struct Authorizer {
    std::shared_ptr<database> connection;
    int forbidden=0;
    explicit Authorizer(const std::shared_ptr<swift_lattice>& owner) : connection(owner->borrow_read_connection()) {
        sqlite3_set_authorizer(connection->handle(),[](void* context,int action,const char* table,const char* column,const char*,const char*) -> int {
            const bool badRead=action==SQLITE_READ&&table&&column&&
                ((std::string(table)=="ScopedAsset"&&std::string(column)=="embedding") ||
                 (std::string(table)=="AuditLog"&&(std::string(column)=="changedFields"||std::string(column)=="changedFieldsNames")));
            if(badRead||action==SQLITE_INSERT||action==SQLITE_UPDATE||action==SQLITE_DELETE) {++static_cast<Authorizer*>(context)->forbidden;return SQLITE_DENY;}return SQLITE_OK;
        },this);
    }
    ~Authorizer() {sqlite3_set_authorizer(connection->handle(),nullptr,nullptr);}
};
}
TEST(DetachedRead, ExactFourFullChunksIgnoreForeignPayloadAndRemainOwnedAfterClose) {
    TempDB file{"scoped_four"};auto db=opened(file.str());for(int i=1;i<=5;++i)insert(*db,i,262144);
    lattice::detached_read::result result;
    {Authorizer authorizer(db);result=lattice::detached_read::by_ids(db,request({identity(4),identity(2),identity(1),identity(3)}));
     ASSERT_EQ(result.rows.size(),4u);EXPECT_EQ(authorizer.forbidden,0);}
    for(const auto& row:result.rows) {const auto bytes=std::get<std::vector<uint8_t>>(row.values.at("payload"));EXPECT_EQ(bytes.size(),262144u);EXPECT_EQ(bytes.front(),row.id);}
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1),identity(2),identity(3),identity(4),identity(5)})),lattice::db_error);
    db->close();EXPECT_EQ(std::get<std::vector<uint8_t>>(result.rows[0].values.at("payload")).front(),1);
}
TEST(DetachedRead, EverySelectedRowIsPreflightedBeforeVariableHydration) {
    TempDB file{"scoped_preflight"};auto db=opened(file.str());insert(*db,1);insert(*db,2,262145);
    auto connection=db->borrow_read_connection();int hydration=0;
    sqlite3_trace_v2(connection->handle(),SQLITE_TRACE_STMT,[](unsigned,void* context,void* statement,void*) -> int {
        const auto* raw=sqlite3_sql(static_cast<sqlite3_stmt*>(statement));if(raw&&std::string(raw).find("SELECT globalId,")==0)++*static_cast<int*>(context);return 0;
    },&hydration);
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1),identity(2)})),lattice::db_error);EXPECT_EQ(hydration,0);
    sqlite3_trace_v2(connection->handle(),0,nullptr,nullptr);
    db->db().execute("UPDATE ScopedAsset SET payload=? WHERE globalId=?",{std::vector<uint8_t>{2},identity(2)});
    EXPECT_EQ(lattice::detached_read::by_ids(db,request({identity(1),identity(2)})).rows.size(),2u);
    db->db().execute("UPDATE ScopedAsset SET ordinal=zeroblob(1048576) WHERE globalId=?",{identity(1)});
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)})),lattice::db_error);db->close();
}
TEST(DetachedRead, MetadataPagesUseRealIndexAndAdvanceOnlyThroughIncludedRows) {
    TempDB file{"scoped_page"};auto db=opened(file.str());for(int i=1;i<=5;++i)insert(*db,i);insert(*db,6,3,"foreign",101);
    auto r=page();const auto first=lattice::detached_read::by_indexed_uuid(db,r);ASSERT_EQ(first.rows.size(),2u);EXPECT_TRUE(first.has_more);EXPECT_EQ(first.next_id,first.rows.back().id);
    r.after_id=first.next_id;const auto second=lattice::detached_read::by_indexed_uuid(db,r);ASSERT_EQ(second.rows.size(),2u);EXPECT_TRUE(second.has_more);EXPECT_GT(second.rows.front().id,first.next_id);
    r.after_id=second.next_id;const auto last=lattice::detached_read::by_indexed_uuid(db,r);ASSERT_EQ(last.rows.size(),1u);EXPECT_FALSE(last.has_more);EXPECT_EQ(last.rows.front().global_id,identity(5));
    r.after_id=last.next_id;const auto empty=lattice::detached_read::by_indexed_uuid(db,r);EXPECT_TRUE(empty.rows.empty());EXPECT_EQ(empty.next_id,0);EXPECT_FALSE(empty.has_more);
    r.property="label";r.equal_to=identity(100);EXPECT_THROW(lattice::detached_read::by_indexed_uuid(db,r),lattice::db_error);
    r=page();r.properties={"payload"};EXPECT_THROW(lattice::detached_read::by_indexed_uuid(db,r),lattice::db_error);db->close();
}
TEST(DetachedRead, UTF8TextAndAggregateMetadataCeilingsAreExact) {
    TempDB file{"scoped_text"};auto db=opened(file.str());std::string unicode;for(int i=0;i<1024;++i)unicode+="\xF0\x9F\x98\x80";insert(*db,1,1,unicode);
    EXPECT_EQ(std::get<std::string>(lattice::detached_read::by_ids(db,request({identity(1)},{"label"})).rows.front().values.at("label")),unicode);
    db->db().execute("UPDATE ScopedAsset SET label=label||'x'");EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)},{"label"})),lattice::db_error);
    db->db().execute("UPDATE ScopedAsset SET label=?",{std::string(4096,'x')});for(int i=2;i<=64;++i)insert(*db,i,1,std::string(4096,'x'));
    auto r=page(64);r.properties={"label"};EXPECT_THROW(lattice::detached_read::by_indexed_uuid(db,r),lattice::db_error);r.limit=63;EXPECT_EQ(lattice::detached_read::by_indexed_uuid(db,r).rows.size(),63u);db->close();
}
TEST(DetachedRead, MissingIsDistinctFromSQLHistoryAndClosedFailure) {
    TempDB file{"scoped_errors"};auto db=opened(file.str());insert(*db,1);
    EXPECT_TRUE(lattice::detached_read::by_ids(db,request({identity(2)})).rows.empty());
    db->db().execute("INSERT OR REPLACE INTO _lattice_meta(key,value) VALUES('history_generation_revision_v1','bad')");
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(2)})),lattice::db_error);
    db->db().execute("DELETE FROM _lattice_meta WHERE key='history_generation_revision_v1'");
    auto connection=db->borrow_read_connection();sqlite3_set_authorizer(connection->handle(),[](void*,int action,const char* table,const char*,const char*,const char*) -> int {return action==SQLITE_READ&&table&&std::string(table)=="ScopedAsset"?SQLITE_DENY:SQLITE_OK;},nullptr);
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(2)})),lattice::db_error);sqlite3_set_authorizer(connection->handle(),nullptr,nullptr);
    db->close();EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)})),lattice::db_error);
}
TEST(DetachedRead, StrictReadonlyNeverReadsAuditPayloadOrWritesMetadata) {
    TempDB file{"scoped_readonly"};auto writer=opened(file.str());insert(*writer,1);writer->close();writer.reset();auto reader=opened(file.str(),true);
    {Authorizer guard(reader);const auto result=lattice::detached_read::by_ids(reader,request({identity(1)}));ASSERT_EQ(result.rows.size(),1u);EXPECT_FALSE(result.fence.has_history_generation);EXPECT_EQ(guard.forbidden,0);}
    reader->close();
}
TEST(DetachedRead, OuterTransactionAndAttachedRoutesAreNotSilentlyChanged) {
    TempDB file{"scoped_outer"},otherFile{"scoped_attached"};auto db=opened(file.str());insert(*db,1);
    db->begin_transaction();EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)})),lattice::db_error);EXPECT_TRUE(db->db().is_in_transaction());db->rollback();
    auto other=opened(otherFile.str());insert(*other,2);static_cast<lattice::lattice_db&>(*db).attach(*other);
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)})),lattice::db_error);db->close();other->close();
}
TEST(DetachedRead, HistoryABAChangesDetachedFenceEvenWhenCursorReturnsToOriginal) {
    TempDB file{"scoped_history"};auto db=opened(file.str());insert(*db,1);auto& core=static_cast<lattice::lattice_db&>(*db);
    lattice::register_replication_slot(db->db(),"scoped-reader");lattice::ensure_cursor_column(db->db());
    db->db().execute("UPDATE _lattice_replication_slots SET last_received_event_id=NULL WHERE sync_id='scoped-reader'");db->db().execute("UPDATE AuditLog SET isFromRemote=1");
    const auto before=lattice::detached_read::by_ids(db,request({identity(1)}));db->db().execute("UPDATE ScopedAsset SET label='changed'");
    db->db().execute("UPDATE _lattice_replication_slots SET upload_floor=(SELECT MAX(id) FROM AuditLog)");ASSERT_GT(core.safe_compact_audit_log(),0);
    const auto after=lattice::detached_read::by_ids(db,request({identity(1)}));EXPECT_EQ(before.fence.cursor.head,after.fence.cursor.head);EXPECT_EQ(before.fence.cursor.floor,after.fence.cursor.floor);
    EXPECT_FALSE(lattice::detached_read::same_fence(before.fence,after.fence));EXPECT_EQ(std::get<std::string>(after.rows.front().values.at("label")),"changed");db->close();
}

TEST(DetachedRead, RegisteredTableCannotHideUnindexedOrAmbiguousSystemIdentity) {
    TempDB file{"scoped_identity_shape"};auto db=opened(file.str());insert(*db,1);
    db->db().execute("ALTER TABLE ScopedAsset RENAME TO ScopedAssetOriginal");
    db->db().execute("CREATE TABLE ScopedAsset(id INTEGER,globalId TEXT UNIQUE,ownerID TEXT,label TEXT,ordinal INTEGER,payload BLOB,embedding BLOB)");
    db->db().execute("INSERT INTO ScopedAsset(id,globalId,ownerID,label,ordinal,payload,embedding) VALUES(1,?,?,'saved',1,x'01',x'')",{identity(1),identity(100)});
    EXPECT_THROW(lattice::detached_read::by_ids(db,request({identity(1)})),lattice::db_error);
    db->close();
}
