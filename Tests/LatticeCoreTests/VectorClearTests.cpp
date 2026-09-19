#include "TestHelpers.hpp"
#include <algorithm>
#include <map>
#include <set>

namespace {
using Blob=std::vector<uint8_t>;
using Rows=std::vector<lattice::database::row_t>;
using Image=std::map<std::string,Rows>;
using Contents=std::map<std::string,float>;
const std::string model="VectorClearDocument";
const std::string index_name="_VectorClearDocument_embedding_vec";
const std::string failed="00000000-0000-4000-8000-000000000701";
const std::string successor="00000000-0000-4000-8000-000000000702";

lattice::model_schema clear_schema(const std::string& name) {
    lattice::model_schema s;s.table_name=name;
    lattice::property_descriptor label;label.name="label";label.type=lattice::column_type::text;
    lattice::property_descriptor embedding;embedding.name="embedding";embedding.type=lattice::column_type::blob;
    embedding.is_vector=true;embedding.nullable=true;s.properties={label,embedding};return s;
}
lattice::configuration clear_config(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;
}
class ClearStore:public lattice::lattice_db {
public:
    explicit ClearStore(const std::string& path=":memory:"):lattice_db(clear_config(path)) {}
    void create() {create_model_table_public(clear_schema(model));}
    void ensure(int dimensions=4) {ensure_vec0_table(model,"embedding",dimensions);}
    void stamp_epoch_six_fingerprint() {
        // Reproduce the old versioned fingerprint with the existing stable
        // property serializer. At the old epoch this marker permits fast open.
        std::ostringstream text;text<<"epoch:6\ntarget_schema_version:"<<config().target_schema_version<<'\n';
        auto schemas=lattice::schema_registry::instance().all_schemas();
        std::sort(schemas.begin(),schemas.end(),[](const auto* a,const auto* b){return a->table_name<b->table_name;});
        for(const auto* s:schemas) {
            text<<"table:"<<s->table_name<<'\n';
            for(const auto& p:s->properties)serialize_property_for_fingerprint(text,p);
        }
        std::ostringstream hex;hex<<std::hex<<std::setfill('0')<<std::setw(16)<<fnv1a_hash(text.str());
        db().execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
        store_fingerprint_marker("schema_fingerprint:"+hex.str());
    }
};
int64_t number(lattice::database& db,const std::string& sql,const std::vector<lattice::column_value_t>& args={}) {
    return std::get<int64_t>(db.query(sql,args).at(0).at("n"));
}
int64_t local_id(lattice::database& db,const std::string& id) {
    return number(db,"SELECT id AS n FROM "+model+" WHERE globalId=?",{id});
}
void insert(lattice::database& db,const std::string& id,const lattice::column_value_t& value) {
    db.execute("INSERT INTO "+model+"(globalId,label,embedding) VALUES(?,?,?)",{id,id,value});
}
void update(lattice::database& db,const std::string& id,const lattice::column_value_t& value) {
    db.execute("UPDATE "+model+" SET embedding=? WHERE globalId=?",{value,id});
}
void managed_set(ClearStore& owner,const std::string& id,const Blob& value) {
    lattice::managed<Blob> field;
    field.assign(&owner.db(),&owner,model,"embedding",local_id(owner.db(),id));field.is_vector_column=true;
    field.set_value(value);
}
Rows programs(lattice::database& db,const std::string& table=model) {
    return db.query("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=? ORDER BY name",{table});
}
Image image(lattice::database& db) {
    Image result{
        {"model",db.query("SELECT id,globalId,label,embedding FROM "+model+" ORDER BY id")},
        {"audit",db.query("SELECT * FROM AuditLog WHERE tableName=? ORDER BY id",{model})},
        {"receipts",db.query("SELECT s.* FROM _lattice_sync_state s JOIN AuditLog a ON a.id=s.audit_entry_id WHERE a.tableName=? ORDER BY s.audit_entry_id,s.sync_id",{model})}};
    if(!db.table_exists(index_name))return result;
    result.emplace("virtual",db.query("SELECT global_id,embedding FROM "+index_name+" ORDER BY global_id"));
    result.emplace("rowids",db.query("SELECT rowid,id,chunk_id,chunk_offset FROM "+index_name+"_rowids ORDER BY rowid"));
    result.emplace("chunks",db.query("SELECT chunk_id,size,validity,rowids FROM "+index_name+"_chunks ORDER BY chunk_id"));
    result.emplace("vectors",db.query("SELECT _rowid_ AS physical_rowid,rowid,vectors FROM "+index_name+"_vector_chunks00 ORDER BY _rowid_"));
    result.emplace("sequences",db.query("SELECT name,seq FROM sqlite_sequence WHERE name IN (?,?,?,?) ORDER BY name",
        {model,index_name+"_rowids",index_name+"_chunks",std::string("AuditLog")}));
    return result;
}
void same_image(lattice::database& db,const Image& expected) {
    const auto actual=image(db);ASSERT_EQ(actual.size(),expected.size());
    for(const auto& [name,rows]:expected)EXPECT_TRUE(actual.at(name)==rows)<<name;
}
void indexed(lattice::database& db,const Contents& expected) {
    const auto actual=db.query("SELECT global_id,embedding FROM "+index_name+" ORDER BY global_id");
    ASSERT_EQ(actual.size(),expected.size());size_t at=0;
    for(const auto& [id,value]:expected) {
        EXPECT_EQ(std::get<std::string>(actual[at].at("global_id")),id);
        EXPECT_EQ(std::get<Blob>(actual[at].at("embedding")),pack_floats({value,0,0,0}));++at;
    }
    const auto rowids=db.query("SELECT id FROM "+index_name+"_rowids ORDER BY id");
    ASSERT_EQ(rowids.size(),expected.size());at=0;
    for(const auto& [id,_]:expected)EXPECT_EQ(std::get<std::string>(rowids[at++].at("id")),id);
}
void public_search(ClearStore& owner,const Contents& expected) {
    indexed(owner.db(),expected); // must precede any query that could repair
    const auto before=image(owner.db());const auto changes=sqlite3_total_changes64(owner.db().handle());
    std::vector<std::pair<std::string,float>> ordered(expected.begin(),expected.end());
    std::sort(ordered.begin(),ordered.end(),[](const auto& a,const auto& b){return a.second<b.second;});
    for(const auto& filter:{std::optional<std::string>{},std::optional<std::string>{model+".label <> 'excluded'"}}) {
        const auto results=owner.knn_query(model,"embedding",pack_floats({0,0,0,0}),10,lattice::lattice_db::distance_metric::l2,filter);
        ASSERT_EQ(results.size(),ordered.size());
        for(size_t i=0;i<results.size();++i){EXPECT_EQ(results[i].global_id,ordered[i].first);EXPECT_NEAR(results[i].distance,ordered[i].second,0.00001);}
    }
    EXPECT_EQ(sqlite3_total_changes64(owner.db().handle()),changes);same_image(owner.db(),before);
}
void legacy_triple(lattice::database& db,const std::string& table=model) {
    // Removing only the new clear program leaves the byte-identical supported
    // three-program predecessor. No current generator is called after this.
    db.execute("DROP TRIGGER _"+table+"_embedding_vec_clear");
}
// Match the receiving path's post-model order: audit row, then channel state.
// This qualifies rollback of the primitive, not remote apply or ACK policy.
void bookkeeping(lattice::database& db,const std::string& op,const std::string& id,int64_t pk,const std::string& operation="UPDATE") {
    db.execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,isFromRemote,isSynchronized,timestamp) "
        "VALUES(?,?,?,?,?,'{}','[]',1,0,1789819200)",{op,model,operation,pk,id});
    db.execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) SELECT id,'vector-clear',1 FROM AuditLog WHERE globalId=?",{op});
}
struct Fault {
    sqlite3* db;int hits=0;int rc;
    explicit Fault(lattice::database& owner):db(owner.handle()),rc(sqlite3_create_function_v2(db,"clear_fault_stage",0,SQLITE_UTF8,&hits,
        [](sqlite3_context* c,int,sqlite3_value**) noexcept{++*static_cast<int*>(sqlite3_user_data(c));sqlite3_result_int(c,1);},nullptr,nullptr,nullptr)) {}
    ~Fault(){sqlite3_create_function_v2(db,"clear_fault_stage",0,SQLITE_UTF8,nullptr,nullptr,nullptr,nullptr,nullptr);}
};
struct RegisteredClearTag {};
}

TEST(VectorClear, ManagedEmptyFullEmptyFullPersistsBeforeSearchAndReopen) {
    TempDB path("vector-clear-cycle");Image final;
    {
        ClearStore owner(path.str());owner.create();insert(owner.db(),"target",Blob{});
        managed_set(owner,"target",{});EXPECT_FALSE(owner.db().table_exists(index_name));
        managed_set(owner,"target",pack_floats({1,0,0,0}));indexed(owner.db(),{{"target",1}});public_search(owner,{{"target",1}});
        const auto full=image(owner.db());managed_set(owner,"target",{});indexed(owner.db(),{});
        EXPECT_EQ(std::get<Blob>(image(owner.db()).at("model")[0].at("embedding")),Blob{});
        EXPECT_NE(image(owner.db()).at("chunks"),full.at("chunks"));public_search(owner,{});
        managed_set(owner,"target",pack_floats({2,0,0,0}));public_search(owner,{{"target",2}});final=image(owner.db());
    }
    {lattice::database raw(path.str());same_image(raw,final);indexed(raw,{{"target",2}});}
    {ClearStore reopened(path.str());same_image(reopened.db(),final);public_search(reopened,{{"target",2}});}
}
TEST(VectorClear, GeneratedNullClearAndDeleteEmptyReinsertHaveNoOldHit) {
    ClearStore owner;owner.create();owner.ensure();insert(owner.db(),"target",pack_floats({1,0,0,0}));
    lattice::managed<std::optional<Blob>> field;
    field.assign(&owner.db(),&owner,model,"embedding",local_id(owner.db(),"target"));field.is_vector_column=true;field.set_nil();
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(image(owner.db()).at("model")[0].at("embedding")));
    indexed(owner.db(),{});public_search(owner,{});
    update(owner.db(),"target",pack_floats({2,0,0,0}));public_search(owner,{{"target",2}});
    owner.db().execute("DELETE FROM "+model+" WHERE globalId='target'");indexed(owner.db(),{});
    insert(owner.db(),"target",Blob{});public_search(owner,{});
    managed_set(owner,"target",pack_floats({3,0,0,0}));public_search(owner,{{"target",3}});
}
TEST(VectorClear, OldGeneratedTripleUpgradesOnActualManagedEmptySetterWithoutChangingIt) {
    ClearStore owner;owner.create();owner.ensure();insert(owner.db(),"target",pack_floats({1,0,0,0}));legacy_triple(owner.db());
    const auto old=programs(owner.db());managed_set(owner,"target",{});indexed(owner.db(),{});
    auto after=programs(owner.db());ASSERT_EQ(after.size(),old.size()+1);
    after.erase(std::remove_if(after.begin(),after.end(),[](const auto& row){return std::get<std::string>(row.at("name"))==index_name+"_clear";}),after.end());
    EXPECT_EQ(after,old);public_search(owner,{});
}
TEST(VectorClear, ZeroDimensionAdmissionNeverCreatesMissingSidecar) {
    ClearStore owner;owner.create();insert(owner.db(),"empty",Blob{});const auto before=programs(owner.db());const auto rows=image(owner.db());
    owner.ensure(0);EXPECT_FALSE(owner.db().table_exists(index_name));EXPECT_EQ(programs(owner.db()),before);same_image(owner.db(),rows);
}
TEST(VectorClear, QualifiedMainNameUsesTheSameGeneratedPrograms) {
    ClearStore owner;owner.create();owner.ensure();const auto initial=programs(owner.db());
    owner.ensure_vec0_table("main."+model,"embedding",0);EXPECT_EQ(programs(owner.db()),initial);
    insert(owner.db(),"target",pack_floats({1,0,0,0}));legacy_triple(owner.db());
    owner.ensure_vec0_table("main."+model,"embedding",0);EXPECT_EQ(programs(owner.db()),initial);
    update(owner.db(),"target",Blob{});public_search(owner,{});
}
TEST(VectorClear, ReservedNameConflictRefusesWithoutChangingAnyMetadataOrData) {
    for(const bool present:{false,true})for(const auto& suffix:{std::string("clear"),std::string("update")}) {
        SCOPED_TRACE(present);
        SCOPED_TRACE(suffix);ClearStore owner;owner.create();
        if(present){owner.ensure();insert(owner.db(),"target",pack_floats({1,0,0,0}));legacy_triple(owner.db());}
        if(present&&suffix=="update")owner.db().execute("DROP TRIGGER "+index_name+"_update");
        owner.db().execute("CREATE TRIGGER "+index_name+"_"+suffix+" AFTER UPDATE ON "+model+" BEGIN SELECT 1; END");
        const auto before=programs(owner.db());const auto data=image(owner.db());
        EXPECT_THROW(owner.ensure(0),lattice::db_error);EXPECT_EQ(programs(owner.db()),before);same_image(owner.db(),data);
    }
}
TEST(VectorClear, UnrelatedUserTriggerSurvivesKnownMetadataUpgrade) {
    ClearStore owner;owner.create();owner.ensure();legacy_triple(owner.db());
    owner.db().execute("CREATE TRIGGER clear_user_program AFTER UPDATE ON "+model+" BEGIN SELECT 1; END");
    const auto before=owner.db().query("SELECT sql FROM sqlite_schema WHERE name='clear_user_program'");owner.ensure(0);
    EXPECT_EQ(owner.db().query("SELECT sql FROM sqlite_schema WHERE name='clear_user_program'"),before);
}
TEST(VectorClear, FailedLaterGeneratedDDLRestoresEarlierProgramsAndCanRetry) {
    ClearStore owner;owner.create();owner.ensure();legacy_triple(owner.db());
    owner.db().execute("DROP TRIGGER "+index_name+"_update");owner.db().execute("DROP TRIGGER "+index_name+"_delete");
    const auto before=programs(owner.db());int create_attempts=0;
    ASSERT_EQ(sqlite3_set_authorizer(owner.db().handle(),[](void* p,int action,const char* one,const char*,const char*,const char*) noexcept {
        if(action==SQLITE_CREATE_TRIGGER){++*static_cast<int*>(p);if(one&&std::strcmp(one,"_VectorClearDocument_embedding_vec_clear")==0)return SQLITE_DENY;}
        return SQLITE_OK;
    },&create_attempts),SQLITE_OK);
    EXPECT_THROW(owner.ensure(0),lattice::db_error);
    ASSERT_EQ(sqlite3_set_authorizer(owner.db().handle(),nullptr,nullptr),SQLITE_OK);
    EXPECT_EQ(create_attempts,3);EXPECT_EQ(programs(owner.db()),before);EXPECT_FALSE(owner.db().is_in_transaction());
    owner.ensure(0);insert(owner.db(),"target",pack_floats({1,0,0,0}));managed_set(owner,"target",{});public_search(owner,{});
}
TEST(VectorClear, UpgradeInsideOwnedTransactionRollsBackAndDoesNotCacheFalseSuccess) {
    ClearStore owner;owner.create();owner.ensure();legacy_triple(owner.db());const auto old=programs(owner.db());
    owner.begin_transaction();owner.ensure(0);EXPECT_EQ(programs(owner.db()).size(),old.size()+1);owner.rollback();
    EXPECT_EQ(programs(owner.db()),old);owner.ensure(0);EXPECT_EQ(programs(owner.db()).size(),old.size()+1);
}
TEST(VectorClear, ClearBookkeepingFailureRollsBackRawIndexThenSuccessorRetryAndReopen) {
    for(const bool null_value:{false,true}) {
        SCOPED_TRACE(null_value);TempDB path("vector-clear-rollback");Image final;
        {
            ClearStore owner(path.str());owner.create();owner.ensure();insert(owner.db(),"target",pack_floats({1,0,0,0}));insert(owner.db(),"anchor",pack_floats({9,0,0,0}));
            Fault fault(owner.db());ASSERT_EQ(fault.rc,SQLITE_OK);
            owner.db().execute("CREATE TRIGGER clear_fail_receipt BEFORE INSERT ON _lattice_sync_state WHEN NEW.sync_id='vector-clear' AND EXISTS "
                "(SELECT 1 FROM AuditLog WHERE id=NEW.audit_entry_id AND globalId='"+failed+"') BEGIN SELECT clear_fault_stage(); SELECT RAISE(ABORT,'clear bookkeeping denied'); END");
            owner.begin_transaction();owner.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");const auto before=image(owner.db());
            owner.db().execute("SAVEPOINT clear_entry");const auto pk=local_id(owner.db(),"target");
            if(null_value)update(owner.db(),"target",nullptr);else managed_set(owner,"target",{});
            indexed(owner.db(),{{"anchor",9}});EXPECT_NE(image(owner.db()).at("chunks"),before.at("chunks"));
            EXPECT_THROW(bookkeeping(owner.db(),failed,"target",pk),lattice::db_error);ASSERT_EQ(fault.hits,1);
            EXPECT_EQ(number(owner.db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?",{failed}),1);
            owner.db().execute("ROLLBACK TO clear_entry");owner.db().execute("RELEASE clear_entry");same_image(owner.db(),before);
            public_search(owner,{{"target",1},{"anchor",9}});
            insert(owner.db(),"successor",pack_floats({2,0,0,0}));bookkeeping(owner.db(),successor,"successor",local_id(owner.db(),"successor"),"INSERT");
            owner.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");owner.commit();
            EXPECT_EQ(number(owner.db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?",{failed}),0);
            public_search(owner,{{"target",1},{"successor",2},{"anchor",9}});
            owner.db().execute("DROP TRIGGER clear_fail_receipt");owner.begin_transaction();owner.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            if(null_value)update(owner.db(),"target",nullptr);else managed_set(owner,"target",{});
            bookkeeping(owner.db(),failed,"target",pk);owner.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");owner.commit();
            EXPECT_EQ(fault.hits,1);EXPECT_EQ(number(owner.db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE sync_id='vector-clear' AND is_synchronized=1"),2);
            public_search(owner,{{"successor",2},{"anchor",9}});final=image(owner.db());
        }
        {lattice::database raw(path.str());same_image(raw,final);indexed(raw,{{"successor",2},{"anchor",9}});}
        {ClearStore reopened(path.str());same_image(reopened.db(),final);public_search(reopened,{{"successor",2},{"anchor",9}});}
    }
}
TEST(VectorClear, EpochSevenOpenUpgradesLegacyMetadataButDoesNotClaimHistoricalBackfill) {
    lattice::schema_registry::instance().register_model(typeid(RegisteredClearTag),clear_schema(model));
    TempDB path("vector-clear-legacy-open");Rows old_programs;Image stale;
    {
        ClearStore owner(path.str());owner.ensure();insert(owner.db(),"historic",pack_floats({1,0,0,0}));insert(owner.db(),"target",pack_floats({2,0,0,0}));
        legacy_triple(owner.db());update(owner.db(),"historic",Blob{});indexed(owner.db(),{{"historic",1},{"target",2}});
        old_programs=programs(owner.db());stale=image(owner.db());owner.stamp_epoch_six_fingerprint();
    }
    {lattice::database raw(path.str());EXPECT_EQ(programs(raw),old_programs);same_image(raw,stale);}
    {
        ClearStore reopened(path.str());EXPECT_EQ(lattice::lattice_db::schema_format_epoch(),7);
        EXPECT_EQ(programs(reopened.db()).size(),old_programs.size()+1);same_image(reopened.db(),stale);
        // No explicit ensure or vector setter after reopen: the generated NULL
        // path must already be admitted before the first ordinary SQL UPDATE.
        update(reopened.db(),"target",nullptr);indexed(reopened.db(),{{"historic",1}});
        // An already-stale untouched value was deliberately not backfilled.
        // Re-clearing that exact identity now removes its old entry.
        update(reopened.db(),"historic",Blob{});public_search(reopened,{});
    }
}

namespace {
// Deliberately never registered globally: shuffled test order must not allow
// Core's epoch-open vector pass to admit this dynamic schema for the fixture.
const std::string optional_model="UnregisteredOptionalVectorClearDocument";
const std::string optional_index="_UnregisteredOptionalVectorClearDocument_embedding_vec";
class OptionalClearStore:public lattice::lattice_db {
public:
    explicit OptionalClearStore(const std::string& path=":memory:"):lattice_db(clear_config(path)) {}
    void admit_dynamic_schema(bool vector=true) {
        auto s=clear_schema(optional_model);s.properties[0].nullable=true;s.properties[1].is_vector=vector;
        create_model_table_public(s);
    }
};
enum class OptionalClearWay { method, nullopt, disengaged, empty_blob };
const std::array<OptionalClearWay,4> optional_clear_ways{
    OptionalClearWay::method,OptionalClearWay::nullopt,OptionalClearWay::disengaged,OptionalClearWay::empty_blob};
void optional_insert(lattice::database& db,const lattice::column_value_t& value) {
    db.execute("INSERT INTO "+optional_model+"(globalId,label,embedding) VALUES('target','label',?)",{value});
}
void bind_optional(lattice::managed<std::optional<Blob>>& field,OptionalClearStore& owner,bool vector=true) {
    const auto pk=number(owner.db(),"SELECT id AS n FROM "+optional_model+" WHERE globalId='target'");
    field.assign(&owner.db(),&owner,optional_model,"embedding",pk);field.is_vector_column=vector;
}
void clear_optional(lattice::managed<std::optional<Blob>>& field,OptionalClearWay way) {
    switch(way) {
        case OptionalClearWay::method:field.set_nil();break;
        case OptionalClearWay::nullopt:field=std::nullopt;break;
        case OptionalClearWay::disengaged:field=std::optional<Blob>{};break;
        case OptionalClearWay::empty_blob:field=std::optional<Blob>{Blob{}};break;
    }
}
Image optional_image(lattice::database& db) {
    Image result{
        {"model",db.query("SELECT id,globalId,label,embedding FROM "+optional_model+" ORDER BY id")},
        {"audit",db.query("SELECT * FROM AuditLog WHERE tableName=? ORDER BY id",{optional_model})},
        {"programs",programs(db,optional_model)}};
    if(!db.table_exists(optional_index))return result;
    result.emplace("virtual",db.query("SELECT global_id,embedding FROM "+optional_index+" ORDER BY global_id"));
    result.emplace("rowids",db.query("SELECT rowid,id,chunk_id,chunk_offset FROM "+optional_index+"_rowids ORDER BY rowid"));
    result.emplace("chunks",db.query("SELECT chunk_id,size,validity,rowids FROM "+optional_index+"_chunks ORDER BY chunk_id"));
    result.emplace("vectors",db.query("SELECT _rowid_ AS physical_rowid,rowid,vectors FROM "+optional_index+"_vector_chunks00 ORDER BY _rowid_"));
    result.emplace("sequences",db.query("SELECT name,seq FROM sqlite_sequence WHERE name IN (?,?,?,?) ORDER BY name",
        {optional_model,optional_index+"_rowids",optional_index+"_chunks",std::string("AuditLog")}));
    return result;
}
void same_optional_image(lattice::database& db,const Image& expected) {
    const auto actual=optional_image(db);ASSERT_EQ(actual.size(),expected.size());
    for(const auto& [name,rows]:expected)EXPECT_TRUE(actual.at(name)==rows)<<name;
}
void optional_search(OptionalClearStore& owner,std::optional<float> expected) {
    // Raw sidecar and physical ID checks precede public search. No query-time
    // reconciliation can silently supply the state that this oracle expects.
    const auto before=optional_image(owner.db());const auto& rows=before.at("virtual");
    ASSERT_EQ(rows.size(),expected ? 1u:0u);ASSERT_EQ(before.at("rowids").size(),rows.size());
    if(expected) {
        EXPECT_EQ(std::get<std::string>(rows[0].at("global_id")),"target");
        EXPECT_EQ(std::get<Blob>(rows[0].at("embedding")),pack_floats({*expected,0,0,0}));
        EXPECT_EQ(std::get<std::string>(before.at("rowids")[0].at("id")),"target");
    }
    const auto changes=sqlite3_total_changes64(owner.db().handle());
    for(const auto& filter:{std::optional<std::string>{},std::optional<std::string>{optional_model+".label <> 'excluded'"}}) {
        const auto hits=owner.knn_query(optional_model,"embedding",pack_floats({0,0,0,0}),10,lattice::lattice_db::distance_metric::l2,filter);
        ASSERT_EQ(hits.size(),expected ? 1u:0u);
        if(expected){EXPECT_EQ(hits[0].global_id,"target");EXPECT_NEAR(hits[0].distance,*expected,0.00001);}
    }
    EXPECT_EQ(sqlite3_total_changes64(owner.db().handle()),changes);same_optional_image(owner.db(),before);
}
void prepare_optional_legacy(OptionalClearStore& owner) {
    owner.admit_dynamic_schema();
    // Old-store construction only. After the supported legacy triple is
    // installed, no explicit ensure is permitted before the tested setter.
    owner.ensure_vec0_table(optional_model,"embedding",4);
    optional_insert(owner.db(),pack_floats({1,0,0,0}));legacy_triple(owner.db(),optional_model);
}
}

TEST(VectorClearOptional, DynamicLegacyFileAllClearEntryPointsUpgradeWithoutExplicitEnsure) {
    for(const auto way:optional_clear_ways) {
        SCOPED_TRACE(static_cast<int>(way));TempDB path("optional-vector-legacy");Rows predecessor;Image final;
        for(const auto* s:lattice::schema_registry::instance().all_schemas())ASSERT_NE(s->table_name,optional_model);
        {OptionalClearStore old(path.str());prepare_optional_legacy(old);predecessor=programs(old.db(),optional_model);}
        {
            OptionalClearStore owner(path.str());owner.admit_dynamic_schema();
            EXPECT_EQ(programs(owner.db(),optional_model),predecessor); // no epoch/schema helper pre-admitted it
            lattice::managed<std::optional<Blob>> field;bind_optional(field,owner);clear_optional(field,way);
            const auto value=owner.db().query("SELECT embedding FROM "+optional_model).at(0).at("embedding");
            if(way==OptionalClearWay::empty_blob){ASSERT_TRUE(std::holds_alternative<Blob>(value));EXPECT_TRUE(std::get<Blob>(value).empty());}
            else EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(value));
            EXPECT_EQ(programs(owner.db(),optional_model).size(),predecessor.size()+1);optional_search(owner,{});
            field=std::optional<Blob>{pack_floats({2,0,0,0})};optional_search(owner,2);final=optional_image(owner.db());
        }
        {lattice::database raw(path.str());same_optional_image(raw,final);}
        {OptionalClearStore reopened(path.str());reopened.admit_dynamic_schema();same_optional_image(reopened.db(),final);optional_search(reopened,2);}
    }
}
TEST(VectorClearOptional, FirstOptionalBytesInferDimensionsAndClearsNeverCreateMissingSidecar) {
    OptionalClearStore owner;owner.admit_dynamic_schema();optional_insert(owner.db(),nullptr);
    lattice::managed<std::optional<Blob>> field;bind_optional(field,owner);
    for(const auto way:optional_clear_ways){clear_optional(field,way);EXPECT_FALSE(owner.db().table_exists(optional_index));}
    field=std::optional<Blob>{pack_floats({1,0,0,0})};optional_search(owner,1);
    field=std::optional<Blob>{pack_floats({3,0,0,0})};optional_search(owner,3);
    field=std::optional<Blob>{Blob{}};optional_search(owner,{});
    field=std::optional<Blob>{pack_floats({2,0,0,0})};optional_search(owner,2);
}
TEST(VectorClearOptional, NonVectorBlobAndOtherOptionalScalarsKeepScalarStorage) {
    OptionalClearStore owner;owner.admit_dynamic_schema(false);optional_insert(owner.db(),nullptr);
    // A reserved-looking user program must not even be inspected for a plain
    // Blob field. The optional scalar write is independent of vec metadata.
    owner.db().execute("CREATE TRIGGER "+optional_index+"_clear AFTER UPDATE ON "+optional_model+" BEGIN SELECT 1; END");
    const auto before=programs(owner.db(),optional_model);
    lattice::managed<std::optional<Blob>> field;bind_optional(field,owner,false);
    field=std::optional<Blob>{Blob{1,0,255}};
    EXPECT_EQ(std::get<Blob>(owner.db().query("SELECT embedding FROM "+optional_model).at(0).at("embedding")),(Blob{1,0,255}));
    field=std::optional<Blob>{Blob{}};
    EXPECT_EQ(std::get<Blob>(owner.db().query("SELECT embedding FROM "+optional_model).at(0).at("embedding")),Blob{});
    field.set_nil();EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(owner.db().query("SELECT embedding FROM "+optional_model).at(0).at("embedding")));
    EXPECT_FALSE(owner.db().table_exists(optional_index));EXPECT_EQ(programs(owner.db(),optional_model),before);
    lattice::managed<std::optional<std::string>> label;
    label.assign(&owner.db(),&owner,optional_model,"label",number(owner.db(),"SELECT id AS n FROM "+optional_model));
    label=std::optional<std::string>{"changed"};EXPECT_EQ(std::get<std::string>(owner.db().query("SELECT label FROM "+optional_model).at(0).at("label")),"changed");
    label=std::nullopt;EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(owner.db().query("SELECT label FROM "+optional_model).at(0).at("label")));
}
TEST(VectorClearOptional, UnknownLegacyReservedProgramRefusesEveryClearWithoutWritingTheModel) {
    OptionalClearStore owner;prepare_optional_legacy(owner);
    owner.db().execute("CREATE TRIGGER "+optional_index+"_clear AFTER UPDATE ON "+optional_model+" BEGIN SELECT 1; END");
    const auto before=optional_image(owner.db());lattice::managed<std::optional<Blob>> field;bind_optional(field,owner);
    for(const auto way:optional_clear_ways) {
        SCOPED_TRACE(static_cast<int>(way));EXPECT_THROW(clear_optional(field,way),lattice::db_error);same_optional_image(owner.db(),before);
    }
    EXPECT_THROW((field=std::optional<Blob>{pack_floats({2,0,0,0})}),lattice::db_error);same_optional_image(owner.db(),before);
}
TEST(VectorClearOptional, OptionalUpgradeAndModelClearShareOuterRollbackThenRetry) {
    for(const auto way:optional_clear_ways) {
        SCOPED_TRACE(static_cast<int>(way));OptionalClearStore owner;prepare_optional_legacy(owner);
        const auto before=optional_image(owner.db());lattice::managed<std::optional<Blob>> field;bind_optional(field,owner);
        owner.begin_transaction();clear_optional(field,way);optional_search(owner,{});
        EXPECT_EQ(programs(owner.db(),optional_model).size(),before.at("programs").size()+1);
        owner.rollback();same_optional_image(owner.db(),before);
        // No metadata-success cache survives rollback: repeat the actual setter.
        clear_optional(field,way);optional_search(owner,{});
        field=std::optional<Blob>{pack_floats({2,0,0,0})};optional_search(owner,2);
    }
}
