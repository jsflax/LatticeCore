#include <gtest/gtest.h>
#include <lattice/ordinary_schema_recipe.hpp>
#include <lattice/sync.hpp>
#include "../../Sources/LatticeCore/src/ordinary_schema_validation.hpp"
#include <sqlite3.h>
#include <array>
#include <memory>
#include <stdexcept>
#include <utility>

namespace lattice::detail {
// Distinct from OrdinarySchemaRecipeTests' existing friend class: no different
// definitions of the same accessor are introduced into the CoreTests image.
struct ordinary_preimage_recipe_test_access final {
    static std::string shadow_column(const std::string& table,const std::string& column){return ordinary_schema_recipe::shadow_column(table,column);}
    template<class Allocator>
    static ordinary_initialization_text shadow_column(std::string_view table,const std::basic_string<char,std::char_traits<char>,Allocator>& column){return ordinary_schema_recipe::shadow_column_storage<ordinary_initialization_text>(table,std::string_view(column));}
    static ordinary_schema_preimage construction_initial(ordinary_schema_projection& p,
        const ordinary_preimage_query& q,const ordinary_metadata_limits& limits) {
        return ordinary_initialization_schema_validation::construction_initial_(p,q,limits);
    }
    static ordinary_schema_preimage construction_prefix(const ordinary_schema_projection& p,
        const ordinary_preimage_query& q,const ordinary_metadata_limits& limits) {
        return ordinary_initialization_schema_validation::construction_prefix_(p,q,limits);
    }
    static ordinary_schema_preimage required_present(const ordinary_schema_projection& p,
        const ordinary_preimage_query& q,const ordinary_metadata_limits& limits) {
        return ordinary_initialization_schema_validation::required_present_(p,q,limits);
    }

    static const char* sync_set()noexcept{return ordinary_schema_recipe::sync_set();}
    static const char* sync_state()noexcept{return ordinary_schema_recipe::sync_state();}
    static const char* sync_state_pending()noexcept{return ordinary_schema_recipe::sync_state_pending();}
    static const char* replication_slots()noexcept{return ordinary_schema_recipe::replication_slots();}
    static const char* replication_upload_floor()noexcept{return ordinary_schema_recipe::replication_upload_floor();}
    static std::string replication_slots_current(){return ordinary_schema_recipe::replication_slots_after_upload_floor();}
    static const char* observer_column()noexcept{return ordinary_schema_recipe::replication_observer_column();}
    static std::string replication_slots_observer(){return ordinary_schema_recipe::replication_slots_after_upload_floor_and_observer();}
    template<class Rows>static bool observer_columns_match(std::string_view table,const Rows& rows)noexcept{
        return ordinary_schema_recipe::static_sync_observer_columns_match(table,rows);
    }
    template<class Rows>static bool columns_match(std::string_view table,bool floor,const Rows& rows)noexcept{
        return ordinary_schema_recipe::static_sync_columns_match(table,floor,rows);
    }
    static const char* sync_control_table()noexcept{return ordinary_schema_recipe::sync_control_table();}
    static const char* sync_control_initial()noexcept{return ordinary_schema_recipe::sync_control_initial();}
    static const char* metadata_table()noexcept{return ordinary_schema_recipe::metadata_table();}
    static const char* metadata_initial_version()noexcept{return ordinary_schema_recipe::metadata_initial_version();}
    static const char* audit_table()noexcept{return ordinary_schema_recipe::audit_table();}
    static const char* audit_pending_index()noexcept{return ordinary_schema_recipe::audit_pending_index();}
    static const char* audit_lookup_index()noexcept{return ordinary_schema_recipe::audit_lookup_index();}
    template<class Rows>static bool audit_columns(const Rows& rows)noexcept{return ordinary_schema_recipe::audit_columns_match(rows);}
};
}
using namespace lattice;
using namespace lattice::detail;
namespace {
// Primitive real-SQLite recipe controls only. No Host, physical-original,
// factory activation, setup-domain lease or whole-process ending is invented.
struct PreimageDb {
    sqlite3* value=nullptr;
    PreimageDb(){
        if(sqlite3_open(":memory:",&value)!=SQLITE_OK){
            const int closed=value?sqlite3_close(std::exchange(value,nullptr)):SQLITE_OK;
            if(closed!=SQLITE_OK)throw std::runtime_error("actual failed-open cleanup retained Native debt");
            throw std::runtime_error("actual memory open failed");
        }
    }
    ~PreimageDb(){if(value&&sqlite3_close(value)!=SQLITE_OK)ADD_FAILURE()<<"preimage control retained a real Native statement";}
    void execute(const std::string& sql){if(sqlite3_exec(value,sql.c_str(),nullptr,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("actual fixture DDL failed");}
    template<class Allocator>
    void execute(const std::basic_string<char,std::char_traits<char>,Allocator>& sql){if(sqlite3_exec(value,sql.c_str(),nullptr,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("actual fixture DDL failed");}
};
struct Statement {
    sqlite3_stmt* value=nullptr;
    ~Statement(){if(value&&sqlite3_finalize(value)!=SQLITE_OK)ADD_FAILURE()<<"actual metadata unwind finalization returned nonOK";}
    void finish(){auto* actual=std::exchange(value,nullptr);if(sqlite3_finalize(actual)!=SQLITE_OK)throw std::runtime_error("actual metadata finalization failed");}
};
struct TypedRead {
    PreimageDb& db;
    size_t tvf_queries=0,metadata_rows=0;
    std::vector<database::row_t> operator()(const std::shared_ptr<const ordinary_preimage_statement>& entry){
        if(!entry||!entry->creator_current())throw std::runtime_error("actual typed metadata entry missing");
        using kind=ordinary_preimage_statement::kind;
        const auto issued=entry->kind_id();
        if(issued==kind::table_columns||issued==kind::table_foreign_keys||issued==kind::temp_table_foreign_keys||
           issued==kind::index_list||issued==kind::index_columns)++tvf_queries;
        if(entry->sql().size()>256*1024||entry->parameters().size()>1)throw std::runtime_error("actual metadata declaration exceeds control bounds");
        Statement statement;const char* tail=nullptr;
        if(sqlite3_prepare_v2(db.value,entry->sql().data(),int(entry->sql().size()),&statement.value,&tail)!=SQLITE_OK||
           !statement.value||tail!=entry->sql().data()+entry->sql().size())throw std::runtime_error("actual typed recipe prepare failed");
        int bound_index=1;
        for(const auto& parameter:entry->parameters()){
            const auto* text=std::get_if<std::string>(&parameter);
            if(!text||text->size()>256||sqlite3_bind_text(statement.value,bound_index++,text->data(),int(text->size()),SQLITE_STATIC)!=SQLITE_OK)
                throw std::runtime_error("actual typed metadata parameter failed");
        }
        const int columns=sqlite3_column_count(statement.value);
        if(columns<1||size_t(columns)>entry->column_limit())throw std::runtime_error("metadata columns exceeded BEFORE copy");
        std::array<std::string,16> names;size_t name_bytes=0;
        for(int i=0;i<columns;++i){const auto* name=sqlite3_column_name(statement.value,i);if(!name)throw std::runtime_error("actual column name missing");
            size_t count=0;while(count<=256&&name[count])++count;
            if(count>256)throw std::runtime_error("actual column name exceeded BEFORE copy");
            if(count>entry->result_byte_limit()-name_bytes)throw std::runtime_error("metadata key bytes exceeded BEFORE copy");
            name_bytes+=count;names[size_t(i)].assign(name,count);
        }
        std::vector<database::row_t> result;size_t copied=0;int rc;
        while((rc=sqlite3_step(statement.value))==SQLITE_ROW){
            if(result.size()>=entry->row_limit())throw std::runtime_error("metadata rows exceeded BEFORE copy");
            size_t charge=0;std::array<int,16> types{},sizes{};
            for(int i=0;i<columns;++i){types[size_t(i)]=sqlite3_column_type(statement.value,i);
                const int size=(types[size_t(i)]==SQLITE_TEXT||types[size_t(i)]==SQLITE_BLOB)?sqlite3_column_bytes(statement.value,i):8;
                if(size<0)throw std::runtime_error("actual metadata extent invalid");sizes[size_t(i)]=size;
                const auto amount=names[size_t(i)].size()+size_t(size);
                if(amount>entry->result_byte_limit()-charge)throw std::runtime_error("metadata row bytes exceeded BEFORE copy");charge+=amount;
            }
            if(charge>entry->result_byte_limit()-copied)throw std::runtime_error("metadata result bytes exceeded BEFORE copy");
            copied+=charge;database::row_t row;
            for(int i=0;i<columns;++i){column_value_t cell=nullptr;
                switch(types[size_t(i)]){
                  case SQLITE_INTEGER:cell=int64_t(sqlite3_column_int64(statement.value,i));break;
                  case SQLITE_FLOAT:cell=sqlite3_column_double(statement.value,i);break;
                  case SQLITE_TEXT:{const auto* value=sqlite3_column_text(statement.value,i);if(!value&&sizes[size_t(i)])throw std::runtime_error("actual text missing");cell=std::string((value?reinterpret_cast<const char*>(value):""),size_t(sizes[size_t(i)]));break;}
                  case SQLITE_BLOB:{const auto* value=static_cast<const uint8_t*>(sqlite3_column_blob(statement.value,i));if(!value&&sizes[size_t(i)])throw std::runtime_error("actual blob missing");cell=value?std::vector<uint8_t>(value,value+sizes[size_t(i)]):std::vector<uint8_t>{};break;}
                  case SQLITE_NULL:break;
                  default:throw std::runtime_error("actual column type unsupported");
                }
                row.emplace(names[size_t(i)],std::move(cell));
            }
            result.push_back(std::move(row));++metadata_rows;
        }
        if(rc!=SQLITE_DONE)throw std::runtime_error("actual metadata step failed");statement.finish();return result;
    }
};
recovery_owner_schema declaration(bool geographic=false){
    model_schema model;model.table_name="PreimageRow";
    property_descriptor label{};label.name="label";label.type=column_type::text;label.kind=property_kind::primitive;
    model.properties.push_back(label);
    if(geographic){property_descriptor bounds{};bounds.name="area";bounds.type=column_type::real;bounds.kind=property_kind::primitive;bounds.is_geo_bounds=true;model.properties.push_back(bounds);}
    recovery_owner_schema actual;if(!actual.admit_model(model))throw std::runtime_error("actual closed declaration refused");return actual;
}
void seed(PreimageDb& db,const ordinary_schema_projection& projection){
    for(const auto& [name,table]:projection.tables)db.execute(table.create_sql);
    for(const auto& derived:projection.derived)if(!derived.create_sql.empty())db.execute(derived.create_sql);
    for(const auto& [name,table]:projection.tables){
        for(const auto& sql:table.audit_programs)db.execute(sql);
        for(const auto& sql:table.other_programs)db.execute(sql);
        for(const auto& sql:table.index_programs)db.execute(sql);
    }
    for(const auto& derived:projection.derived)for(const auto& sql:derived.programs)db.execute(sql);
}
ordinary_schema_preimage inspect(PreimageDb& db,const ordinary_schema_projection& projection,TypedRead& read){
    // Known small fixture declares the WHOLE procedure resource envelope. This
    // is not a production setup-domain admission nor an allocator/RSS bound.
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    db.execute("BEGIN");
    try{auto result=validate_ordinary_preexisting_schema(projection,[&](const auto& entry){return read(entry);},limits);db.execute("ROLLBACK");return result;}
    catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, ActualEmptyStoreKeepsSelectedFamiliesAbsent){
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});TypedRead read{db};
    const auto result=inspect(db,projection,read);EXPECT_TRUE(result.main_objects.empty());
    EXPECT_EQ(result.absent_tables,std::vector<std::string>{"PreimageRow"});EXPECT_TRUE(result.verified_trigger_origins.empty());EXPECT_EQ(read.tvf_queries,0u);
}
TEST(OrdinarySchemaPreimage, ActualCompleteOrdinaryFamilyReturnsOnlyVerifiedTriggerObjects){
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);TypedRead read{db};
    const auto result=inspect(db,projection,read);EXPECT_TRUE(result.absent_tables.empty());ASSERT_EQ(result.verified_trigger_origins.size(),3u);
    for(const auto index:result.verified_trigger_origins){ASSERT_LT(index,result.main_objects.size());EXPECT_EQ(result.main_objects[index].type,"trigger");EXPECT_EQ(result.main_objects[index].owner,"PreimageRow");}
    EXPECT_GT(read.tvf_queries,0u);
}
TEST(OrdinarySchemaPreimage, ActualUnknownAndAlteredTriggerBodiesNeverBecomeOrigins){
    for(const bool altered:{false,true}){PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
        if(altered){auto sql=projection.tables.at("PreimageRow").audit_programs.at(1);ASSERT_EQ(sql.find("CREATE TRIGGER IF NOT EXISTS AuditPreimageRowInsert"),0u);const auto at=sql.rfind(" END");ASSERT_NE(at,std::string::npos);sql.insert(at," SELECT 1;");db.execute("DROP TRIGGER AuditPreimageRowInsert");db.execute(sql);}
        else db.execute("CREATE TRIGGER ExtraPreimageProgram AFTER INSERT ON PreimageRow BEGIN SELECT 1; END");
        TypedRead read{db};EXPECT_THROW((void)inspect(db,projection,read),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, ActualRTreeAndShadowsPreflightWithoutApplicationRowSelect){
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(true),{});ASSERT_EQ(projection.derived.size(),1u);seed(db,projection);TypedRead read{db};
    const auto result=inspect(db,projection,read);EXPECT_TRUE(result.absent_tables.empty());EXPECT_GT(result.main_objects.size(),projection.tables.size());
    EXPECT_GT(read.metadata_rows,0u); // Typed issuer exposes only metadata recipe kinds.
}
TEST(OrdinarySchemaPreimage, ActualOrphanSidecarRefusesEvenWhenParentWasNeverCreated){
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});db.execute("CREATE TABLE _PreimageRow_old(value TEXT)");TypedRead read{db};
    EXPECT_THROW((void)inspect(db,projection,read),ordinary_schema_validation_error);
}
TEST(OrdinarySchemaPreimage, MainAndTempProducerCollisionsRefuseBeforeAnyTableValuedPragma){
    for(const char* name:{"pragma_table_xinfo","pragma_foreign_key_list","pragma_index_list","pragma_index_xinfo","pragma_database_list"})
    for(const char* space:{"main","temp"})for(int kind=0;kind<3;++kind){
        PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
        const auto target=std::string(space)+".\""+name+"\"";
        if(kind==0)db.execute("CREATE TABLE "+target+"(id INTEGER)");
        else if(kind==1)db.execute("CREATE VIEW "+target+" AS SELECT 1 AS id");
        else db.execute("CREATE VIRTUAL TABLE "+target+" USING rtree(id,minX,maxX,minY,maxY)");
        TypedRead read{db};EXPECT_THROW((void)inspect(db,projection,read),ordinary_schema_validation_error);EXPECT_EQ(read.tvf_queries,0u);
    }
}

namespace {
ordinary_schema_projection internal_audit_projection(){
    ordinary_schema_projection scope;ordinary_projection_table audit;
    audit.name="AuditLog";audit.create_sql=ordinary_preimage_recipe_test_access::audit_table();
    audit.local_id=false;audit.role=ordinary_projection_role::link;
    audit.index_programs={ordinary_preimage_recipe_test_access::audit_pending_index(),ordinary_preimage_recipe_test_access::audit_lookup_index()};
    scope.tables.emplace(audit.name,std::move(audit));return scope;
}
}
TEST(OrdinarySchemaPreimage, GenuineEmptyCatalogInternalAuditHasExactPartialIndexPreimage){
    PreimageDb db;const auto scope=internal_audit_projection();seed(db,scope);TypedRead read{db};
    const auto proof=inspect(db,scope,read);EXPECT_TRUE(proof.absent_tables.empty());
    ASSERT_EQ(proof.plain_shapes.size(),1u);EXPECT_EQ(proof.plain_shapes[0].name,"AuditLog");
    EXPECT_EQ(proof.plain_shapes[0].columns,12u);EXPECT_FALSE(proof.plain_shapes[0].without_rowid);
    EXPECT_TRUE(proof.verified_trigger_origins.empty());EXPECT_GT(read.tvf_queries,0u);
}
TEST(OrdinarySchemaPreimage, AlteredInternalPartialPredicateAndAuditViewNeverBecomePlainTableProof){
    for(const bool view:{false,true}){
        PreimageDb db;const auto scope=internal_audit_projection();
        if(view)db.execute("CREATE VIEW AuditLog AS SELECT 1 AS id");
        else{seed(db,scope);db.execute("DROP INDEX idx_audit_log_pending_sync");
            db.execute("CREATE INDEX idx_audit_log_pending_sync ON AuditLog(isSynchronized) WHERE isSynchronized=1");}
        TypedRead read{db};EXPECT_THROW((void)inspect(db,scope,read),ordinary_schema_validation_error);
    }
}

TEST(OrdinarySchemaPreimage, ActualWithoutRowidCompositePrimaryAndUniqueMetadataSuppliesShape){
    PreimageDb db;ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="ActualWithout";table.local_id=false;table.role=ordinary_projection_role::link;
    table.create_sql="CREATE TABLE ActualWithout(a TEXT NOT NULL,b INTEGER NOT NULL UNIQUE,payload TEXT,PRIMARY KEY(a,b),UNIQUE(a)) WITHOUT ROWID";
    scope.tables.emplace(table.name,table);seed(db,scope);TypedRead read{db};
    const auto proof=inspect(db,scope,read);ASSERT_EQ(proof.plain_shapes.size(),1u);
    EXPECT_EQ(proof.plain_shapes[0].name,"ActualWithout");EXPECT_EQ(proof.plain_shapes[0].columns,3u);
    EXPECT_TRUE(proof.plain_shapes[0].without_rowid);
}

TEST(OrdinarySchemaPreimage, ActualUniqueBeforeCompositePrimaryPreservesAutoindexOrdinals){
    PreimageDb db;ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="UniqueFirst";table.local_id=false;table.role=ordinary_projection_role::link;
    table.create_sql="CREATE TABLE UniqueFirst(a TEXT NOT NULL,b INTEGER NOT NULL,payload TEXT,UNIQUE(a),PRIMARY KEY(a,b)) WITHOUT ROWID";
    scope.tables.emplace(table.name,table);seed(db,scope);TypedRead read{db};
    const auto proof=inspect(db,scope,read);ASSERT_EQ(proof.plain_shapes.size(),1u);
    EXPECT_EQ(proof.plain_shapes[0].name,"UniqueFirst");EXPECT_EQ(proof.plain_shapes[0].columns,3u);
    EXPECT_TRUE(proof.plain_shapes[0].without_rowid);
}

namespace {
ordinary_schema_preimage inspect_read_tables(PreimageDb& db,const ordinary_schema_projection& scope,TypedRead& read){
    db.execute("BEGIN");
    try{
        auto proof=inspect_ordinary_preexisting_read_tables(scope,[&](const auto& entry){return read(entry);},
            ordinary_metadata_limits{128,4096,1024*1024,256*1024});
        db.execute("ROLLBACK");return proof;
    }catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, ReadTableInspectionDefersRealTriggerBundleWithoutExecutingOrGrantingIt){
    int trigger_calls=0; // UDF DATA outlives actual SQLite close.
    PreimageDb db;ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="ReadPhaseRow";table.create_sql="CREATE TABLE ReadPhaseRow(id INTEGER PRIMARY KEY,value TEXT)";
    table.local_id=false;table.role=ordinary_projection_role::link;scope.tables.emplace(table.name,std::move(table));
    db.execute(scope.tables.at("ReadPhaseRow").create_sql);
    db.execute("INSERT INTO ReadPhaseRow VALUES(1,'retained')");
    ASSERT_EQ(sqlite3_create_function_v2(db.value,"read_phase_trip",0,SQLITE_UTF8,&trigger_calls,
        [](sqlite3_context* context,int,sqlite3_value**){++*static_cast<int*>(sqlite3_user_data(context));sqlite3_result_null(context);},
        nullptr,nullptr,nullptr),SQLITE_OK);
    db.execute("CREATE TRIGGER ActualForeignBundle BEFORE UPDATE ON ReadPhaseRow BEGIN SELECT read_phase_trip(); END");
    TypedRead read{db};const auto proof=inspect_read_tables(db,scope,read);
    EXPECT_TRUE(proof.verified_trigger_origins.empty());
    ASSERT_EQ(proof.plain_shapes.size(),1u);EXPECT_EQ(proof.plain_shapes[0].columns,2u);
    Statement actual;ASSERT_EQ(sqlite3_prepare_v2(db.value,"SELECT value FROM ReadPhaseRow WHERE id=1",-1,&actual.value,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(actual.value),SQLITE_ROW);EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(actual.value,0)),"retained");
    ASSERT_EQ(sqlite3_step(actual.value),SQLITE_DONE);actual.finish();
    EXPECT_EQ(trigger_calls,0);
    // Complete validation STILL refuses this unissued bundle. It cannot be
    // promoted to ordinary DML or publication by the read-phase result.
    TypedRead complete{db};EXPECT_THROW((void)inspect(db,scope,complete),ordinary_schema_validation_error);
    db.execute("UPDATE ReadPhaseRow SET value='actual trigger ran' WHERE id=1");
    EXPECT_EQ(trigger_calls,1); // The retained trigger was real executable SQL.
}
TEST(OrdinarySchemaPreimage, ReadTableInspectionStillRefusesViewAndExpressionIndexBeforePlainProof){
    for(const bool view:{false,true}){
        PreimageDb db;ordinary_schema_projection scope;ordinary_projection_table table;
        table.name="ReadPhaseRow";table.create_sql="CREATE TABLE ReadPhaseRow(id INTEGER PRIMARY KEY,value TEXT)";
        table.local_id=false;table.role=ordinary_projection_role::link;scope.tables.emplace(table.name,std::move(table));
        if(view)db.execute("CREATE VIEW ReadPhaseRow AS SELECT 1 AS id,'view' AS value");
        else{db.execute(scope.tables.at("ReadPhaseRow").create_sql);db.execute("CREATE INDEX ForeignExpression ON ReadPhaseRow(length(value))");}
        TypedRead read{db};EXPECT_THROW((void)inspect_read_tables(db,scope,read),ordinary_schema_validation_error);
    }
}

namespace {
ordinary_schema_projection metadata_projection(){
    ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="_lattice_meta";table.create_sql=ordinary_preimage_recipe_test_access::metadata_table();
    table.local_id=false;table.role=ordinary_projection_role::link;
    scope.tables.emplace(table.name,std::move(table));return scope;
}
std::string metadata_scalar(PreimageDb& db,const char* sql){
    Statement value;
    if(sqlite3_prepare_v2(db.value,sql,-1,&value.value,nullptr)!=SQLITE_OK||!value.value)
        throw std::runtime_error("metadata primitive scalar prepare failed");
    if(sqlite3_step(value.value)!=SQLITE_ROW||sqlite3_column_type(value.value,0)!=SQLITE_TEXT)
        throw std::runtime_error("metadata primitive scalar type/row failed");
    const auto* bytes=sqlite3_column_text(value.value,0);const int count=sqlite3_column_bytes(value.value,0);
    if(!bytes||count<0||count>64)throw std::runtime_error("metadata primitive scalar bound failed");
    std::string result(reinterpret_cast<const char*>(bytes),std::size_t(count));
    if(sqlite3_step(value.value)!=SQLITE_DONE)throw std::runtime_error("metadata primitive scalar trailing row");
    value.finish();return result;
}
std::string metadata_scalar(PreimageDb& db,const std::string& sql){
    return metadata_scalar(db,sql.c_str());
}
}
TEST(OrdinarySchemaPreimage, MetadataCreateReadPhaseProvesActualAbsentOrExactTable){
    PreimageDb db;const auto scope=metadata_projection();TypedRead absent{db};
    const auto before=inspect_read_tables(db,scope,absent);
    EXPECT_EQ(before.absent_tables,std::vector<std::string>{"_lattice_meta"});
    db.execute(ordinary_preimage_recipe_test_access::metadata_table());
    TypedRead present{db};const auto after=inspect_read_tables(db,scope,present);
    EXPECT_TRUE(after.absent_tables.empty());ASSERT_EQ(after.plain_shapes.size(),1u);
    EXPECT_EQ(after.plain_shapes[0].columns,2u);EXPECT_FALSE(after.plain_shapes[0].without_rowid);
    EXPECT_TRUE(after.verified_trigger_origins.empty());
    db.execute("CREATE INDEX ForeignMetadataIndex ON _lattice_meta(value)");
    TypedRead extra_index{db};
    EXPECT_THROW((void)inspect_read_tables(db,scope,extra_index),ordinary_schema_validation_error);
}
TEST(OrdinarySchemaPreimage, MetadataSeedStrictPhaseRejectsRealMainAndTempInsertTriggers){
    for(const bool temporary:{false,true}){
        PreimageDb db;const auto scope=metadata_projection();db.execute(ordinary_preimage_recipe_test_access::metadata_table());
        db.execute("CREATE TABLE MetadataSeedEffects(value TEXT NOT NULL)");
        db.execute(std::string("CREATE ")+(temporary?"TEMP ":"")+"TRIGGER UnexpectedMetadataSeed AFTER INSERT ON main._lattice_meta BEGIN INSERT INTO MetadataSeedEffects(value) VALUES('executed'); END");
        TypedRead strict{db};
        EXPECT_THROW((void)inspect(db,scope,strict),ordinary_schema_validation_error);
        // Actual side effect remains absent because strict proof refused
        // before the seed. The primitive then deliberately executes the old
        // SQL outside any Host permission to prove this was a real trigger.
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM MetadataSeedEffects"),"0");
        db.execute(ordinary_preimage_recipe_test_access::metadata_initial_version());
        EXPECT_EQ(metadata_scalar(db,"SELECT value FROM MetadataSeedEffects"),"executed");
        EXPECT_EQ(metadata_scalar(db,"SELECT value FROM _lattice_meta WHERE key='schema_version'"),"1");
    }
}
TEST(OrdinarySchemaPreimage, MetadataOriginalSeedIsIdempotentAndCallerRollbackRemovesFreshFamily){
    PreimageDb db;db.execute("BEGIN IMMEDIATE");
    db.execute(ordinary_preimage_recipe_test_access::metadata_table());
    db.execute(ordinary_preimage_recipe_test_access::metadata_initial_version());
    EXPECT_EQ(metadata_scalar(db,"SELECT value FROM _lattice_meta WHERE key='schema_version'"),"1");
    db.execute("UPDATE _lattice_meta SET value='7' WHERE key='schema_version'");
    db.execute(ordinary_preimage_recipe_test_access::metadata_initial_version());
    EXPECT_EQ(metadata_scalar(db,"SELECT value FROM _lattice_meta WHERE key='schema_version'"),"7");
    db.execute("ROLLBACK");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE name='_lattice_meta'"),"0");
}

namespace {
ordinary_schema_projection sync_control_projection(){
    ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="_SyncControl";table.create_sql=ordinary_preimage_recipe_test_access::sync_control_table();
    table.local_id=false;table.role=ordinary_projection_role::link;
    scope.tables.emplace(table.name,std::move(table));return scope;
}
}
TEST(OrdinarySchemaPreimage, SyncControlCreateProofChecksActualRowidShapeAndRejectsExtraIndex){
    PreimageDb db;const auto scope=sync_control_projection();TypedRead absent{db};
    EXPECT_EQ(inspect_read_tables(db,scope,absent).absent_tables,std::vector<std::string>{"_SyncControl"});
    db.execute(ordinary_preimage_recipe_test_access::sync_control_table());
    TypedRead present{db};const auto proof=inspect_read_tables(db,scope,present);
    ASSERT_EQ(proof.plain_shapes.size(),1u);EXPECT_EQ(proof.plain_shapes[0].columns,2u);
    EXPECT_FALSE(proof.plain_shapes[0].without_rowid);
    EXPECT_THROW(db.execute("INSERT INTO _SyncControl(id,disabled) VALUES(2,0)"),std::runtime_error);
    db.execute(ordinary_preimage_recipe_test_access::sync_control_initial());
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(rowid AS TEXT) FROM _SyncControl WHERE id=1"),"1");
    db.execute("CREATE INDEX ForeignSyncControlIndex ON _SyncControl(disabled)");
    TypedRead extra{db};
    EXPECT_THROW((void)inspect_read_tables(db,scope,extra),ordinary_schema_validation_error);
}
TEST(OrdinarySchemaPreimage, SyncControlSeedStrictProofRejectsActualMainAndTempTriggers){
    for(const bool temporary:{false,true}){
        PreimageDb db;const auto scope=sync_control_projection();db.execute(ordinary_preimage_recipe_test_access::sync_control_table());
        db.execute("CREATE TABLE SyncControlSeedEffects(value TEXT NOT NULL)");
        db.execute(std::string("CREATE ")+(temporary?"TEMP ":"")+"TRIGGER UnexpectedSyncControlSeed AFTER INSERT ON main._SyncControl BEGIN INSERT INTO SyncControlSeedEffects(value) VALUES('executed'); END");
        TypedRead strict{db};
        EXPECT_THROW((void)inspect(db,scope,strict),ordinary_schema_validation_error);
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM SyncControlSeedEffects"),"0");
        // Deliberate raw primitive execution after refusal proves the trigger
        // is real; this supplies no Host/private initialization authority.
        db.execute(ordinary_preimage_recipe_test_access::sync_control_initial());
        EXPECT_EQ(metadata_scalar(db,"SELECT value FROM SyncControlSeedEffects"),"executed");
    }
}
TEST(OrdinarySchemaPreimage, SyncControlOriginalSeedPreservesDisabledAndCallerRollback){
    PreimageDb db;db.execute("BEGIN IMMEDIATE");
    db.execute(ordinary_preimage_recipe_test_access::sync_control_table());
    db.execute(ordinary_preimage_recipe_test_access::sync_control_initial());
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(disabled AS TEXT) FROM _SyncControl WHERE id=1"),"0");
    db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    db.execute(ordinary_preimage_recipe_test_access::sync_control_initial());
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(disabled AS TEXT) FROM _SyncControl WHERE id=1"),"1");
    db.execute("ROLLBACK");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE name='_SyncControl'"),"0");
}

namespace {
ordinary_schema_projection static_sync_projection(bool floor,bool pending=true){
    ordinary_schema_projection scope;
    const auto add=[&](const char* name,const std::string& sql){ordinary_projection_table table;
        table.name=name;table.create_sql=sql;table.local_id=false;table.role=ordinary_projection_role::link;
        scope.tables.emplace(name,std::move(table));};
    add("_lattice_sync_set",ordinary_preimage_recipe_test_access::sync_set());
    add("_lattice_sync_state",ordinary_preimage_recipe_test_access::sync_state());
    add("_lattice_replication_slots",floor?ordinary_preimage_recipe_test_access::replication_slots_current():ordinary_preimage_recipe_test_access::replication_slots());
    if(pending)scope.tables.at("_lattice_sync_state").index_programs.emplace_back(ordinary_preimage_recipe_test_access::sync_state_pending());
    return scope;
}
ordinary_schema_preimage static_sync_view(PreimageDb& db,const ordinary_schema_projection& scope,bool floor){
    TypedRead read{db};const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    return validate_ordinary_preexisting_schema(scope,[&](const auto& entry){
        auto rows=read(entry);
        if(entry->kind_id()==ordinary_preimage_statement::kind::table_columns){
            const auto& args=entry->parameters();const auto* table=args.size()==1?std::get_if<std::string>(&args[0]):nullptr;
            if(!table||!ordinary_preimage_recipe_test_access::columns_match(*table,floor,rows))
                throw ordinary_schema_validation_error("actual static sync physical column order differs");
        }
        return rows;
    },limits);
}
ordinary_schema_preimage inspect_static_sync(PreimageDb& db,const ordinary_schema_projection& scope,bool floor){
    db.execute("BEGIN");try{auto out=static_sync_view(db,scope,floor);db.execute("ROLLBACK");return out;}
    catch(...){db.execute("ROLLBACK");throw;}
}
void seed_static_sync(PreimageDb& db,bool floor,bool pending=true){
    db.execute(ordinary_preimage_recipe_test_access::sync_set());
    db.execute(ordinary_preimage_recipe_test_access::sync_state());
    if(pending)db.execute(ordinary_preimage_recipe_test_access::sync_state_pending());
    db.execute(ordinary_preimage_recipe_test_access::replication_slots());
    if(floor)db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
}
}
TEST(OrdinarySchemaPreimage, StaticSyncActualCreateAndAddColumnProducersHaveStrictCompleteImages){
    for(const bool floor:{false,true})for(const bool pending:{false,true}){
        PreimageDb db;const auto scope=static_sync_projection(floor,pending);
        const auto absent=inspect_static_sync(db,scope,floor);EXPECT_EQ(absent.absent_tables.size(),3u);
        seed_static_sync(db,floor,pending);
        const auto proof=inspect_static_sync(db,scope,floor);EXPECT_TRUE(proof.absent_tables.empty());
        ASSERT_EQ(proof.plain_shapes.size(),3u);
        for(const auto& shape:proof.plain_shapes){EXPECT_FALSE(shape.without_rowid);
            EXPECT_EQ(shape.columns,shape.name=="_lattice_replication_slots"?(floor?5u:4u):3u);}
        db.execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id) VALUES(3,'a'),(3,'b')");
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(SUM(is_synchronized) AS TEXT) FROM _lattice_sync_state"),"0");
        EXPECT_THROW(db.execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id) VALUES(3,'a')"),std::runtime_error);
    }
}
TEST(OrdinarySchemaPreimage, StaticSyncPartialIndexRejectsAlteredPredicateExpressionAndUnexpectedIndex){
    for(const int variant:{0,1,2}){
        PreimageDb db;seed_static_sync(db,true,false);const auto scope=static_sync_projection(true);
        if(variant==0)db.execute("CREATE INDEX idx_sync_state_pending ON _lattice_sync_state(sync_id,is_synchronized) WHERE is_synchronized=1");
        else if(variant==1)db.execute("CREATE INDEX idx_sync_state_pending ON _lattice_sync_state(lower(sync_id),is_synchronized) WHERE is_synchronized=0");
        else{db.execute(ordinary_preimage_recipe_test_access::sync_state_pending());db.execute("CREATE INDEX ExtraStaticSyncIndex ON _lattice_sync_state(audit_entry_id)");}
        EXPECT_THROW((void)inspect_static_sync(db,scope,true),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, StaticSyncUploadFloorActualAlterPreservesRowsAndRealCallerRollback){
    PreimageDb db;seed_static_sync(db,false);db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,last_received_event_id) VALUES('saved',17,'event')");
    db.execute("BEGIN IMMEDIATE");
    EXPECT_NO_THROW((void)static_sync_view(db,static_sync_projection(false),false));
    db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
    EXPECT_NO_THROW((void)static_sync_view(db,static_sync_projection(true),true));
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(upload_floor AS TEXT) FROM _lattice_replication_slots WHERE sync_id='saved'"),"0");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(confirmed_audit_id AS TEXT)||':'||last_received_event_id FROM _lattice_replication_slots WHERE sync_id='saved'"),"17:event");
    db.execute("UPDATE _lattice_replication_slots SET upload_floor=9 WHERE sync_id='saved'");
    // Strict current proof is available without issuing a duplicate ALTER.
    EXPECT_NO_THROW((void)static_sync_view(db,static_sync_projection(true),true));
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(upload_floor AS TEXT) FROM _lattice_replication_slots WHERE sync_id='saved'"),"9");
    db.execute("ROLLBACK");
    EXPECT_NO_THROW((void)inspect_static_sync(db,static_sync_projection(false),false));
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(confirmed_audit_id AS TEXT)||':'||last_received_event_id FROM _lattice_replication_slots WHERE sync_id='saved'"),"17:event");
}
TEST(OrdinarySchemaPreimage, StaticSyncWrongFloorDefinitionAndPhysicalOrderRefuse){
    for(const int variant:{0,1,2}){
        PreimageDb db;
        if(variant<2){seed_static_sync(db,false);db.execute(variant==0?
            "ALTER TABLE _lattice_replication_slots ADD COLUMN upload_floor INTEGER NOT NULL DEFAULT 1":
            "ALTER TABLE _lattice_replication_slots ADD COLUMN upload_floor TEXT NOT NULL DEFAULT '0'");}
        else{
            db.execute(ordinary_preimage_recipe_test_access::sync_set());
            db.execute("CREATE TABLE _lattice_sync_state(sync_id TEXT NOT NULL,audit_entry_id INTEGER NOT NULL,is_synchronized INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(audit_entry_id,sync_id))");
            db.execute(ordinary_preimage_recipe_test_access::sync_state_pending());
            db.execute(ordinary_preimage_recipe_test_access::replication_slots());db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
        }
        EXPECT_THROW((void)inspect_static_sync(db,static_sync_projection(true),true),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, StaticSyncMainTempTriggerViewAndForeignKeyCollisionsRefuse){
    for(const int variant:{0,1,2,3}){
        PreimageDb db;
        if(variant==0){db.execute("CREATE VIEW _lattice_sync_set AS SELECT 'x' AS sync_id,'t' AS table_name,'g' AS global_row_id");}
        else{
            seed_static_sync(db,true);
            if(variant==1)db.execute("CREATE TEMP TABLE _lattice_sync_state(value INTEGER)");
            if(variant==2)db.execute("CREATE TEMP TRIGGER UnexpectedStaticSyncTrigger AFTER INSERT ON main._lattice_sync_state BEGIN SELECT 1; END");
            if(variant==3){db.execute("DROP TABLE _lattice_sync_set");db.execute("CREATE TABLE _lattice_sync_set(sync_id TEXT NOT NULL REFERENCES _lattice_replication_slots(sync_id),table_name TEXT NOT NULL,global_row_id TEXT NOT NULL,PRIMARY KEY(sync_id,table_name,global_row_id))");}
        }
        EXPECT_THROW((void)inspect_static_sync(db,static_sync_projection(true),true),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, StaticSyncUnsupportedTwoColumnSetRefusesBeforeAnyRepair){
    // Synthetic negative only: this is not a claimed genuine historical
    // producer fixture or permission to run the still-unclosed rebuild.
    PreimageDb db;db.execute("CREATE TABLE _lattice_sync_set(table_name TEXT NOT NULL,global_row_id TEXT NOT NULL,PRIMARY KEY(table_name,global_row_id))");
    db.execute("INSERT INTO _lattice_sync_set VALUES('old','retained')");
    EXPECT_THROW((void)inspect_static_sync(db,static_sync_projection(false,false),false),ordinary_schema_validation_error);
    EXPECT_EQ(metadata_scalar(db,"SELECT table_name||':'||global_row_id FROM _lattice_sync_set"),"old:retained");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE name='_lattice_replication_slots'"),"0");
}


TEST(OrdinarySchemaPreimage, StaticSyncCopiedPredicateCannotIssueRenamedReorderedOrAlteredPartialRecipe){
    for(const int variant:{0,1,2}){
        PreimageDb db;seed_static_sync(db,true,false);auto scope=static_sync_projection(true);
        ordinary_initialization_text copied=ordinary_preimage_recipe_test_access::sync_state_pending();
        const std::string from=variant==0?"idx_sync_state_pending":variant==1?"(sync_id, is_synchronized)":"is_synchronized = 0";
        const std::string to=variant==0?"idx_copied_sync_pending":variant==1?"(is_synchronized, sync_id)":"is_synchronized = 1";
        const auto at=copied.find(from);ASSERT_NE(at,std::string::npos);copied.replace(at,from.size(),to);
        db.execute(copied);
        // Both actual engine and expected metadata contain the same copied
        // program. Equality alone must not issue a new partial-index recipe.
        scope.tables.at("_lattice_sync_state").index_programs={copied};
        try{(void)inspect_static_sync(db,scope,true);FAIL()<<"copied partial recipe must refuse";}
        catch(const ordinary_schema_validation_error& error){EXPECT_STREQ(error.what(),"ordinary unissued partial index recipe");}
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM pragma_index_list('_lattice_sync_state') WHERE partial=1"),"1");
    }
}

TEST(OrdinarySchemaPreimage, StaticSyncPredicateOnAnotherActualOwnedTableRemainsUnissued){
    PreimageDb db;ordinary_schema_projection scope;ordinary_projection_table table;
    table.name="ApplicationPending";table.local_id=false;table.role=ordinary_projection_role::link;
    table.create_sql="CREATE TABLE ApplicationPending(sync_id TEXT NOT NULL,is_synchronized INTEGER NOT NULL DEFAULT 0)";
    table.index_programs={"CREATE INDEX idx_sync_state_pending ON ApplicationPending(sync_id,is_synchronized) WHERE is_synchronized=0"};
    scope.tables.emplace(table.name,table);seed(db,scope);
    db.execute("INSERT INTO ApplicationPending VALUES('saved',0)");TypedRead read{db};
    try{(void)inspect(db,scope,read);FAIL()<<"same predicate on another actual owner must refuse";}
    catch(const ordinary_schema_validation_error& error){EXPECT_STREQ(error.what(),"ordinary unissued partial index recipe");}
    EXPECT_EQ(metadata_scalar(db,"SELECT sync_id FROM ApplicationPending"),"saved");
}


namespace {
ordinary_schema_projection audit_prefix(bool pending,bool lookup){
    auto scope=internal_audit_projection();auto& indexes=scope.tables.at("AuditLog").index_programs;indexes.clear();
    if(pending)indexes.emplace_back(ordinary_preimage_recipe_test_access::audit_pending_index());
    if(lookup)indexes.emplace_back(ordinary_preimage_recipe_test_access::audit_lookup_index());
    return scope;
}
ordinary_schema_preimage audit_view(PreimageDb& db,bool pending,bool lookup){
    TypedRead read{db};const ordinary_metadata_limits caps{128,4096,1024*1024,256*1024};
    return validate_ordinary_preexisting_schema(audit_prefix(pending,lookup),[&](const auto& entry){
        auto rows=read(entry);
        if(entry->kind_id()==ordinary_preimage_statement::kind::table_columns&&
           !ordinary_preimage_recipe_test_access::audit_columns(rows))
            throw ordinary_schema_validation_error("actual AuditLog physical column order differs");
        return rows;
    },caps);
}
ordinary_schema_preimage inspect_audit_prefix(PreimageDb& db,bool pending,bool lookup){
    db.execute("BEGIN");try{auto proof=audit_view(db,pending,lookup);db.execute("ROLLBACK");return proof;}
    catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, AuditActualSharedCreateIndexPrefixesAndCallerRollbackPreserveProducerSemantics){
    PreimageDb db;db.execute("BEGIN IMMEDIATE");
    EXPECT_EQ(audit_view(db,false,false).absent_tables,std::vector<std::string>{"AuditLog"});
    db.execute(ordinary_preimage_recipe_test_access::audit_table());
    auto table=audit_view(db,false,false);ASSERT_EQ(table.plain_shapes.size(),1u);EXPECT_EQ(table.plain_shapes[0].columns,12u);
    db.execute(ordinary_preimage_recipe_test_access::audit_pending_index());EXPECT_NO_THROW((void)audit_view(db,true,false));
    db.execute(ordinary_preimage_recipe_test_access::audit_lookup_index());EXPECT_NO_THROW((void)audit_view(db,true,true));
    db.execute("INSERT INTO AuditLog(tableName,operation,rowId) VALUES('kept','INSERT',7)");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(length(globalId) AS TEXT)||':'||CAST(synthesized AS TEXT)||':'||typeof(timestamp) FROM AuditLog"),"36:0:real");
    db.execute("ROLLBACK");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE name IN ('AuditLog','idx_audit_log_pending_sync','idx_audit_log_change_lookup','sqlite_sequence')"),"0");
}
TEST(OrdinarySchemaPreimage, AuditActualCurrentMissingIndexPrefixesAreIdempotentAndKeepRows){
    for(const bool pending:{false,true})for(const bool lookup:{false,true}){
        PreimageDb db;seed(db,audit_prefix(pending,lookup));
        db.execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,synthesized) VALUES('saved','kept','UPDATE',17,1)");
        EXPECT_NO_THROW((void)inspect_audit_prefix(db,pending,lookup));
        db.execute("BEGIN IMMEDIATE");
        db.execute(ordinary_preimage_recipe_test_access::audit_table());
        db.execute(ordinary_preimage_recipe_test_access::audit_pending_index());
        db.execute(ordinary_preimage_recipe_test_access::audit_lookup_index());
        EXPECT_NO_THROW((void)audit_view(db,true,true));
        db.execute(ordinary_preimage_recipe_test_access::audit_pending_index());
        db.execute(ordinary_preimage_recipe_test_access::audit_lookup_index());
        EXPECT_NO_THROW((void)audit_view(db,true,true));db.execute("COMMIT");
        EXPECT_EQ(metadata_scalar(db,"SELECT globalId||':'||CAST(rowId AS TEXT)||':'||CAST(synthesized AS TEXT) FROM AuditLog"),"saved:17:1");
    }
}
TEST(OrdinarySchemaPreimage, AuditActualWrongLookupIndexAndMainTempNamespaceCollisionsRefuse){
    for(const int variant:{0,1,2,3}){
        PreimageDb db;seed(db,audit_prefix(true,false));
        if(variant==0)db.execute("CREATE INDEX idx_audit_log_change_lookup ON AuditLog(tableName,operation,rowId)");
        if(variant==1)db.execute("CREATE INDEX idx_audit_log_change_lookup ON AuditLog(tableName COLLATE NOCASE,rowId,operation)");
        if(variant==2)db.execute("CREATE TEMP VIEW AuditLog AS SELECT 1 AS id");
        if(variant==3)db.execute("CREATE TEMP TRIGGER UnexpectedAuditStartup AFTER INSERT ON main.AuditLog BEGIN SELECT 1; END");
        EXPECT_THROW((void)inspect_audit_prefix(db,true,variant<2),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, AuditUnsupportedPreSynthesizedShapeRefusesWithoutRepairOrDataLoss){
    // Synthetic negative only. No genuine historical producer or ALTER
    // migration authority is claimed by deleting this current declaration.
    PreimageDb db;std::string old=ordinary_preimage_recipe_test_access::audit_table();
    const std::string column=",\n                synthesized INTEGER DEFAULT 0";
    const auto at=old.find(column);ASSERT_NE(at,std::string::npos);old.erase(at,column.size());db.execute(old);
    db.execute("INSERT INTO AuditLog(globalId,tableName) VALUES('saved','retained')");
    EXPECT_THROW((void)inspect_audit_prefix(db,false,false),ordinary_schema_validation_error);
    EXPECT_EQ(metadata_scalar(db,"SELECT tableName FROM AuditLog WHERE globalId='saved'"),"retained");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE name IN ('idx_audit_log_pending_sync','idx_audit_log_change_lookup')"),"0");
}
TEST(OrdinarySchemaPreimage, AuditActualForeignKeyExtraIndexAndPhysicalOrderRefuse){
    for(const int variant:{0,1,2}){
        PreimageDb db;std::string ddl=ordinary_preimage_recipe_test_access::audit_table();
        if(variant==0){
            db.execute("CREATE TABLE AuditForeignParent(id INTEGER PRIMARY KEY)");
            const std::string column="rowId INTEGER";const auto at=ddl.find(column);ASSERT_NE(at,std::string::npos);
            ddl.replace(at,column.size(),"rowId INTEGER REFERENCES AuditForeignParent(id)");
        }
        if(variant==2){
            const std::string columns="changedFields TEXT,\n                changedFieldsNames TEXT";
            const auto at=ddl.find(columns);ASSERT_NE(at,std::string::npos);
            ddl.replace(at,columns.size(),"changedFieldsNames TEXT,\n                changedFields TEXT");
        }
        db.execute(ddl);
        if(variant==1)db.execute("CREATE INDEX UnexpectedAuditStartupIndex ON AuditLog(rowId)");
        EXPECT_THROW((void)inspect_audit_prefix(db,false,false),ordinary_schema_validation_error);
    }
}


namespace {
std::string current_sync_slot_declaration(PreimageDb& db){
    constexpr std::string_view sql="SELECT sql FROM main.sqlite_schema WHERE type='table' AND name='_lattice_replication_slots'";
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual slot declaration prepare failed");
    if(sqlite3_step(statement.value)!=SQLITE_ROW||sqlite3_column_count(statement.value)!=1||
       sqlite3_column_type(statement.value,0)!=SQLITE_TEXT)throw std::runtime_error("actual slot declaration row differs");
    const int bytes=sqlite3_column_bytes(statement.value,0);
    if(bytes<1||bytes>16384)throw std::runtime_error("actual slot declaration exceeded control bound BEFORE copy");
    const auto* text=sqlite3_column_text(statement.value,0);
    if(!text)throw std::runtime_error("actual slot declaration text missing");
    std::string image(reinterpret_cast<const char*>(text),size_t(bytes));
    if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual slot declaration ending differs");
    statement.finish();return image;
}
}
// Current producer primitives and the genuine legacy helper are separate
// controls. These do not claim private pending-Open or Host publication.
TEST(OrdinarySchemaPreimage, CurrentSyncObservationRetainsPerChannelMembershipAndCallerRollback){
    PreimageDb db;seed_static_sync(db,true);
    db.execute("INSERT INTO _lattice_sync_set VALUES('a','Item','shared'),('b','Item','shared'),('a','Item','other')");
    db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,last_received_event_id,upload_floor) VALUES('a',17,NULL,3),('b',29,'saved',7)");
    const auto declaration=current_sync_slot_declaration(db);
    db.execute("BEGIN IMMEDIATE");
    const int changes=sqlite3_total_changes(db.value);
    const auto proof=static_sync_view(db,static_sync_projection(true),true);
    EXPECT_TRUE(proof.absent_tables.empty());
    EXPECT_EQ(sqlite3_total_changes(db.value),changes);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),0);
    EXPECT_EQ(metadata_scalar(db,"SELECT group_concat(value,'|') FROM (SELECT sync_id||':'||table_name||':'||global_row_id AS value FROM _lattice_sync_set ORDER BY sync_id,global_row_id)"),"a:Item:other|a:Item:shared|b:Item:shared");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(last_received_event_id IS NULL AS TEXT)||':'||CAST(confirmed_audit_id AS TEXT)||':'||CAST(upload_floor AS TEXT) FROM _lattice_replication_slots WHERE sync_id='a'"),"1:17:3");
    EXPECT_EQ(metadata_scalar(db,"SELECT last_received_event_id||':'||CAST(confirmed_audit_id AS TEXT)||':'||CAST(upload_floor AS TEXT) FROM _lattice_replication_slots WHERE sync_id='b'"),"saved:29:7");
    EXPECT_EQ(current_sync_slot_declaration(db),declaration);
    db.execute("DELETE FROM _lattice_sync_set WHERE sync_id='a'");
    db.execute("UPDATE _lattice_replication_slots SET last_received_event_id='tx-only' WHERE sync_id='b'");
    db.execute("ROLLBACK");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM _lattice_sync_set"),"3");
    EXPECT_EQ(metadata_scalar(db,"SELECT last_received_event_id FROM _lattice_replication_slots WHERE sync_id='b'"),"saved");
}
TEST(OrdinarySchemaPreimage, GenuineCurrentCursorHelperLeavesNullAndSavedValuesWithoutSeeding){
    database db(":memory:");
    db.execute(ordinary_preimage_recipe_test_access::replication_slots());
    db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
    db.execute(ordinary_preimage_recipe_test_access::audit_table());
    db.execute(ordinary_preimage_recipe_test_access::audit_pending_index());
    db.execute(ordinary_preimage_recipe_test_access::audit_lookup_index());
    db.execute("INSERT INTO AuditLog(globalId,isFromRemote,tableName) VALUES('older-remote',1,'Item'),('newer-remote',1,'Item')");
    db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,last_received_event_id,upload_floor) VALUES('null',17,NULL,3),('saved',29,'checkpoint',7)");
    const char* rows_sql="SELECT sync_id,confirmed_audit_id,last_received_event_id,last_active_at,upload_floor FROM main._lattice_replication_slots ORDER BY sync_id";
    const char* schema_sql="SELECT type,name,tbl_name,sql FROM main.sqlite_schema ORDER BY type,name,tbl_name";
    const auto original=db.query(rows_sql);
    ASSERT_EQ(original.size(),2u);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(original[0].at("last_received_event_id")));
    EXPECT_EQ(std::get<std::string>(original[1].at("last_received_event_id")),"checkpoint");
    const auto schema=db.query(schema_sql);
    const auto audit=db.query("SELECT id,globalId,isFromRemote FROM AuditLog ORDER BY id");
    db.begin_transaction();
    db.execute("INSERT INTO _lattice_replication_slots(sync_id,last_received_event_id) VALUES('tx-only',NULL)");
    const auto during=db.query(rows_sql);
    const auto changes=db.query("SELECT total_changes() AS n");
    ensure_cursor_column(db);ensure_cursor_column(db);
    ASSERT_TRUE(db.is_in_transaction());
    EXPECT_EQ(db.query(rows_sql),during);
    EXPECT_EQ(db.query(schema_sql),schema);
    EXPECT_EQ(db.query("SELECT total_changes() AS n"),changes);
    EXPECT_EQ(db.query("SELECT id,globalId,isFromRemote FROM AuditLog ORDER BY id"),audit);
    db.rollback();
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(db.query(rows_sql),original);
    EXPECT_EQ(db.query(schema_sql),schema);
}
TEST(OrdinarySchemaPreimage, CurrentCursorObservationRefusesSyntheticMissingColumnWithoutRepair){
    // Negative shape only: no genuine historical producer or ALTER/seed
    // authority is inferred by deleting the current cursor declaration.
    PreimageDb db;
    db.execute(ordinary_preimage_recipe_test_access::sync_set());
    db.execute(ordinary_preimage_recipe_test_access::sync_state());
    db.execute(ordinary_preimage_recipe_test_access::sync_state_pending());
    db.execute("CREATE TABLE _lattice_replication_slots(sync_id TEXT PRIMARY KEY,confirmed_audit_id INTEGER NOT NULL DEFAULT 0,last_active_at TEXT NOT NULL DEFAULT (datetime('now')),upload_floor INTEGER NOT NULL DEFAULT 0)");
    db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,upload_floor) VALUES('retained',41,11)");
    const auto declaration=current_sync_slot_declaration(db);
    const int changes=sqlite3_total_changes(db.value);
    EXPECT_THROW((void)inspect_static_sync(db,static_sync_projection(true),true),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes(db.value),changes);
    EXPECT_EQ(current_sync_slot_declaration(db),declaration);
    EXPECT_EQ(metadata_scalar(db,"SELECT sync_id||':'||CAST(confirmed_audit_id AS TEXT)||':'||CAST(upload_floor AS TEXT) FROM _lattice_replication_slots"),"retained:41:11");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM pragma_table_info('_lattice_replication_slots') WHERE name='last_received_event_id'"),"0");
}


namespace {
ordinary_schema_projection current_observer_sync_projection(){
    auto scope=static_sync_projection(true);
    scope.tables.at("_lattice_replication_slots").create_sql=ordinary_preimage_recipe_test_access::replication_slots_observer();
    return scope;
}
ordinary_schema_preimage genuine_current_observer_sync_view(database& db){
    // Genuine public database fixture, outside pending startup. This proves
    // exact schema primitives, not pending-Open/Host admission or publication.
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    return validate_ordinary_preexisting_schema(current_observer_sync_projection(),[&](const auto& entry){
        auto rows=db.query(entry->sql(),entry->parameters());
        if(entry->kind_id()==ordinary_preimage_statement::kind::table_columns){
            const auto& args=entry->parameters();const auto* table=args.size()==1?std::get_if<std::string>(&args[0]):nullptr;
            if(!table||!ordinary_preimage_recipe_test_access::observer_columns_match(*table,rows))
                throw ordinary_schema_validation_error("actual observer slots physical order differs");
        }
        return rows;
    },limits);
}
ordinary_schema_preimage inspect_current_observer_sync(PreimageDb& db){
    db.execute("BEGIN");try{
        TypedRead read{db};const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
        auto proof=validate_ordinary_preexisting_schema(current_observer_sync_projection(),[&](const auto& entry){
            auto rows=read(entry);
            if(entry->kind_id()==ordinary_preimage_statement::kind::table_columns){
                const auto& args=entry->parameters();const auto* table=args.size()==1?std::get_if<std::string>(&args[0]):nullptr;
                if(!table||!ordinary_preimage_recipe_test_access::observer_columns_match(*table,rows))
                    throw ordinary_schema_validation_error("actual observer slots physical order differs");
            }
            return rows;
        },limits);
        db.execute("ROLLBACK");return proof;
    }catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, GenuineCurrentFloorThenObserverProducersHaveStrictImageAndCallerRollback){
    database db(":memory:");
    db.execute(ordinary_preimage_recipe_test_access::sync_set());
    db.execute(ordinary_preimage_recipe_test_access::sync_state());
    db.execute(ordinary_preimage_recipe_test_access::sync_state_pending());
    db.execute(ordinary_preimage_recipe_test_access::replication_slots());
    db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
    db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,last_received_event_id,upload_floor) VALUES('saved',23,NULL,7)");
    const char* schema_sql="SELECT type,name,tbl_name,sql FROM main.sqlite_schema ORDER BY type,name,tbl_name";
    const auto schema=db.query(schema_sql);
    const auto original=db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id");
    db.begin_transaction();
    ensure_observer_column(db);
    register_replication_slot(db,"registered",true);
    const auto columns=db.query("PRAGMA main.table_xinfo('_lattice_replication_slots')");
    ASSERT_EQ(columns.size(),6u);
    EXPECT_EQ(std::get<std::string>(columns[4].at("name")),"upload_floor");
    EXPECT_EQ(std::get<std::string>(columns[5].at("name")),"is_observer");
    const auto images=db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id");
    ASSERT_EQ(images.size(),2u);
    EXPECT_EQ(std::get<int64_t>(images[0].at("is_observer")),1);
    EXPECT_EQ(std::get<int64_t>(images[1].at("is_observer")),0);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(images[0].at("last_received_event_id")));
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(images[1].at("last_received_event_id")));
    EXPECT_EQ(std::get<int64_t>(images[1].at("confirmed_audit_id")),23);
    EXPECT_EQ(std::get<int64_t>(images[1].at("upload_floor")),7);
    const auto current_schema=db.query(schema_sql);
    const auto changes=db.query("SELECT total_changes() AS n");
    const auto proof=genuine_current_observer_sync_view(db);
    EXPECT_TRUE(proof.absent_tables.empty());
    const auto slot=std::find_if(proof.plain_shapes.begin(),proof.plain_shapes.end(),[](const auto& shape){return shape.name=="_lattice_replication_slots";});
    ASSERT_NE(slot,proof.plain_shapes.end());EXPECT_EQ(slot->columns,6u);
    EXPECT_TRUE(db.is_in_transaction());
    EXPECT_EQ(db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id"),images);
    EXPECT_EQ(db.query(schema_sql),current_schema);
    EXPECT_EQ(db.query("SELECT total_changes() AS n"),changes);
    db.rollback();
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id"),original);
    EXPECT_EQ(db.query(schema_sql),schema);
}
TEST(OrdinarySchemaPreimage, CurrentObserverLayoutRefusesUnselectedOrderDefinitionAndExtraColumns){
    for(const int variant:{0,1,2,3,4}){
        PreimageDb db;seed_static_sync(db,variant!=2&&variant!=3);
        if(variant==0)db.execute("ALTER TABLE _lattice_replication_slots ADD COLUMN is_observer INTEGER NOT NULL DEFAULT 1");
        else if(variant==1)db.execute("ALTER TABLE _lattice_replication_slots ADD COLUMN is_observer TEXT NOT NULL DEFAULT '0'");
        else db.execute(ordinary_preimage_recipe_test_access::observer_column());
        if(variant==2)db.execute(ordinary_preimage_recipe_test_access::replication_upload_floor());
        if(variant==4)db.execute("ALTER TABLE _lattice_replication_slots ADD COLUMN unissued_value INTEGER");
        // Reordered/observer-only sequences are synthetic negative histories,
        // never claimed as a pinned ordinary or historical migration producer.
        db.execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id) VALUES('retained',31)");
        const auto declaration=current_sync_slot_declaration(db);
        const int changes=sqlite3_total_changes(db.value);
        EXPECT_THROW((void)inspect_current_observer_sync(db),ordinary_schema_validation_error);
        EXPECT_EQ(sqlite3_total_changes(db.value),changes);
        EXPECT_EQ(current_sync_slot_declaration(db),declaration);
        EXPECT_EQ(metadata_scalar(db,"SELECT sync_id||':'||CAST(confirmed_audit_id AS TEXT) FROM _lattice_replication_slots"),"retained:31");
    }
}

namespace {
ordinary_schema_preimage inspect_initialization_image(PreimageDb& db,const ordinary_schema_projection& projection,
    TypedRead& read,bool required) {
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    db.execute("BEGIN");
    try {
        const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
        auto result=required?ordinary_preimage_recipe_test_access::required_present(projection,query,limits):
            ordinary_preimage_recipe_test_access::construction_prefix(projection,query,limits);
        db.execute("ROLLBACK");return result;
    }catch(...){db.execute("ROLLBACK");throw;}
}
ordinary_schema_projection initialization_projection() {
    auto catalog=declaration(true);catalog.models.at("PreimageRow").properties.front().is_unique=true;
    return describe_ordinary_projection(catalog,{});
}
}
TEST(OrdinarySchemaPreimage, GenuineConstructionPrefixBuildsFullAuditIndexAndRTreeBeforeRequiredPresent) {
    PreimageDb db;const auto projection=initialization_projection();TypedRead read{db};
    const auto absent=inspect_initialization_image(db,projection,read,false);
    EXPECT_EQ(absent.absent_tables.size(),2u);EXPECT_TRUE(absent.verified_trigger_origins.empty());
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
    const auto& table=projection.tables.at("PreimageRow");db.execute(table.create_sql);
    EXPECT_NO_THROW((void)inspect_initialization_image(db,projection,read,false));
    // The original strict preexisting reader stays strict for present bundles.
    EXPECT_THROW((void)inspect(db,projection,read),ordinary_schema_validation_error);
    for(size_t i=0;i<table.audit_programs.size();++i) {
        db.execute(table.audit_programs[i]);const auto prefix=inspect_initialization_image(db,projection,read,false);
        EXPECT_EQ(prefix.verified_trigger_origins.size(),i+1);
        EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
    }
    ASSERT_EQ(projection.derived.size(),1u);db.execute(projection.derived.front().create_sql);
    EXPECT_NO_THROW((void)inspect_initialization_image(db,projection,read,false));
    for(const auto& sql:table.other_programs){db.execute(sql);EXPECT_NO_THROW((void)inspect_initialization_image(db,projection,read,false));}
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
    ASSERT_EQ(table.index_programs.size(),1u);db.execute(table.index_programs.front());
    const auto complete=inspect_initialization_image(db,projection,read,true);
    EXPECT_TRUE(complete.absent_tables.empty());EXPECT_EQ(complete.verified_trigger_origins.size(),table.audit_programs.size()+table.other_programs.size());
    EXPECT_NO_THROW((void)inspect(db,projection,read));
}
TEST(OrdinarySchemaPreimage, ConstructionPrefixRejectsAlteredPresentTriggerAndUnknownIndex) {
    for(const bool unknown_index:{false,true}) {
        PreimageDb db;const auto projection=initialization_projection();const auto& table=projection.tables.at("PreimageRow");
        db.execute(table.create_sql);
        if(unknown_index)db.execute("CREATE INDEX attacker_index ON PreimageRow(label)");
        else {auto sql=table.audit_programs.front();const auto at=sql.rfind(" END");ASSERT_NE(at,std::string::npos);sql.insert(at," SELECT 1;");db.execute(sql);}
        TypedRead read{db};EXPECT_THROW((void)inspect_initialization_image(db,projection,read,false),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, ConstructionPrefixStillRequiresImplicitIdentityIndexAndActualPhysicalColumns) {
    for(const bool missing_unique:{false,true}) {
        PreimageDb db;const auto projection=initialization_projection();auto sql=projection.tables.at("PreimageRow").create_sql;
        if(missing_unique) {
            const auto at=sql.find("globalId TEXT UNIQUE");ASSERT_NE(at,std::string::npos);sql.erase(at+14,7);
        }else {
            const auto at=sql.find("label TEXT");ASSERT_NE(at,std::string::npos);sql.replace(at,10,"label BLOB");
        }
        db.execute(sql);TypedRead read{db};EXPECT_THROW((void)inspect_initialization_image(db,projection,read,false),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, ConstructionPrefixRetainsInboundFKAndTempNamespaceClosure) {
    for(const bool temp_shadow:{false,true}) {
        PreimageDb db;const auto projection=initialization_projection();db.execute(projection.tables.at("PreimageRow").create_sql);
        if(temp_shadow)db.execute("CREATE TEMP VIEW PreimageRow AS SELECT 1 AS id");
        else db.execute("CREATE TABLE outsider(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES PreimageRow(id) ON DELETE CASCADE)");
        TypedRead read{db};EXPECT_THROW((void)inspect_initialization_image(db,projection,read,false),ordinary_schema_validation_error);
    }
}
TEST(OrdinarySchemaPreimage, RequiredPresentRefusesMissingOrdinaryOrDerivedFamilyEvenWhenPreexistingAllowsAbsence) {
    for(const bool ordinary_missing:{false,true}) {
        PreimageDb db;const auto projection=initialization_projection();TypedRead read{db};
        if(!ordinary_missing) {
            const auto& table=projection.tables.at("PreimageRow");db.execute(table.create_sql);
            for(const auto& sql:table.audit_programs)db.execute(sql);
            for(const auto& sql:table.index_programs)db.execute(sql);
        }
        EXPECT_NO_THROW((void)inspect_initialization_image(db,projection,read,false));
        EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
    }
}

namespace {
ordinary_schema_projection declared_swift_shadow_projection() {
    auto catalog=declaration();
    model_schema target;target.table_name="PreimageTarget";
    property_descriptor value{};value.name="value";value.type=column_type::text;target.properties.push_back(value);
    if(!catalog.admit_model(target))throw std::runtime_error("actual target declaration refused");
    property_descriptor link{};link.name="group";link.kind=property_kind::link;link.target_table=target.table_name;
    catalog.models.at("PreimageRow").properties.push_back(link);
    catalog.swift_models.insert("PreimageRow");catalog.swift_constraints["PreimageRow"]={recovery_constraint{{"group"},false}};
    return describe_ordinary_projection(catalog,{});
}
}
TEST(OrdinarySchemaPreimage, ConstructionPrefixRequiresEveryDeclaredSwiftShadowPhysicalColumn) {
    PreimageDb db;const auto projection=declared_swift_shadow_projection();TypedRead read{db};
    const auto& row=projection.tables.at("PreimageRow");ASSERT_EQ(row.shadow_columns,std::vector<std::string>{"group__link_gid"});
    db.execute(row.create_sql);
    // Missing programs/indexes can be a construction prefix. A missing
    // declared physical column is a distinct typed pre/post-image problem.
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,false),ordinary_schema_validation_error);
    db.execute(ordinary_preimage_recipe_test_access::shadow_column("PreimageRow","group__link_gid"));
    EXPECT_NO_THROW((void)inspect_initialization_image(db,projection,read,false));
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
}
TEST(OrdinarySchemaPreimage, ConstructionPrefixRefusesWrongDeclaredSwiftShadowPhysicalType) {
    PreimageDb db;const auto projection=declared_swift_shadow_projection();TypedRead read{db};
    db.execute(projection.tables.at("PreimageRow").create_sql);
    db.execute("ALTER TABLE PreimageRow ADD COLUMN group__link_gid BLOB");
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,false),ordinary_schema_validation_error);
}

TEST(OrdinarySchemaPreimage, ConstructionProofRetainsActualValidatedPhysicalColumnData) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});TypedRead read{db};
    db.execute(projection.tables.at("PreimageRow").create_sql);
    const auto proof=inspect_initialization_image(db,projection,read,false);
    const auto at=std::find_if(proof.plain_shapes.begin(),proof.plain_shapes.end(),[](const auto& shape){return shape.name=="PreimageRow";});
    ASSERT_NE(at,proof.plain_shapes.end());ASSERT_EQ(at->columns,3u);ASSERT_EQ(at->physical_columns.size(),at->columns);
    EXPECT_FALSE(at->without_rowid);
    std::map<std::string,std::string> types;
    for(std::size_t i=0;i<at->physical_columns.size();++i) {
        const auto& column=at->physical_columns[i];EXPECT_EQ(column.cid,std::int64_t(i));EXPECT_EQ(column.hidden,0);
        EXPECT_TRUE(types.emplace(column.name,column.type).second);
    }
    EXPECT_EQ(types,(std::map<std::string,std::string>{{"id","integer"},{"globalId","text"},{"label","text"}}));
    // Full final closure remains stronger: retained DATA does not replace
    // absent audit/index/derived programs or the required-present predicate.
    EXPECT_THROW((void)inspect_initialization_image(db,projection,read,true),ordinary_schema_validation_error);
}


#include "../../Sources/LatticeCore/src/ordinary_initialization_storage.hpp"
#include <array>
namespace {
struct actual_storage_debit_refusal {};
struct actual_storage_debits {
    std::uint64_t maximum=0,used=0,calls=0,first_request=0;
    bool failed=false;std::exception_ptr actual_refusal;
    explicit actual_storage_debits(std::uint64_t cap):maximum(cap) {
        try{throw actual_storage_debit_refusal{};}catch(...){actual_refusal=std::current_exception();}
    }
    static void debit(void* account,void* owner,std::uint64_t bytes) {
        auto& actual=*static_cast<actual_storage_debits*>(account);
        if(owner!=&actual)throw std::runtime_error("actual storage debit owner differs");
        ++actual.calls;if(actual.calls==1)actual.first_request=bytes;
        if(actual.failed||actual.used>actual.maximum||bytes>actual.maximum-actual.used) {
            actual.failed=true;std::rethrow_exception(actual.actual_refusal);
        }
        actual.used+=bytes;
    }
};
using storage_scope=lattice::detail::ordinary_initialization_storage_scope;
template<class T>using storage_allocator=lattice::detail::ordinary_initialization_allocator<T>;
}
TEST(OrdinarySchemaPreimage, ActualStorageAllocatorExactFitDoesNotRefundAtFree) {
    actual_storage_debits account(8*sizeof(std::uint64_t));
    storage_scope original(&account,&account,actual_storage_debits::debit);
    storage_allocator<std::uint64_t> allocator;
    auto* actual=allocator.allocate(8);
    ASSERT_NE(actual,nullptr);EXPECT_EQ(account.used,8*sizeof(std::uint64_t));EXPECT_EQ(account.calls,1u);
    allocator.deallocate(actual,8);
    EXPECT_EQ(account.used,8*sizeof(std::uint64_t));
    std::exception_ptr first;
    try{auto* unexpected=allocator.allocate(1);allocator.deallocate(unexpected,1);}catch(...){first=std::current_exception();}
    EXPECT_EQ(first,account.actual_refusal);EXPECT_TRUE(account.failed);EXPECT_EQ(account.calls,2u);
    EXPECT_EQ(account.used,8*sizeof(std::uint64_t));
}
TEST(OrdinarySchemaPreimage, ActualStorageAllocatorRefusesBeforeVectorAllocationAndRetainsFirstError) {
    actual_storage_debits account(sizeof(std::uint64_t)-1);
    std::vector<std::uint64_t,storage_allocator<std::uint64_t>> actual;
    std::exception_ptr first;
    {storage_scope original(&account,&account,actual_storage_debits::debit);
     try{actual.reserve(1);}catch(...){first=std::current_exception();}}
    EXPECT_EQ(first,account.actual_refusal);EXPECT_EQ(account.calls,1u);EXPECT_EQ(account.used,0u);
    EXPECT_TRUE(account.failed);EXPECT_EQ(actual.data(),nullptr);EXPECT_EQ(actual.capacity(),0u);EXPECT_TRUE(actual.empty());
}
TEST(OrdinarySchemaPreimage, ActualStorageAllocatorCountsReboundMapNodesAndRetainsNoAccount) {
    actual_storage_debits account(1024*1024);
    std::map<int,int,std::less<int>,storage_allocator<std::pair<const int,int>>> actual;
    {storage_scope original(&account,&account,actual_storage_debits::debit);
     ASSERT_TRUE(actual.emplace(7,31).second);ASSERT_EQ(account.calls,1u);
     EXPECT_GE(account.first_request,sizeof(std::pair<const int,int>));
     const auto first=account.used;actual.clear();EXPECT_EQ(account.used,first);
     ASSERT_TRUE(actual.emplace(8,32).second);EXPECT_EQ(account.used,2*first);EXPECT_EQ(account.calls,2u);}
    const auto ended=account.used,calls=account.calls;
    // Real retained DATA can die or be used ordinarily after the stack debit
    // scope ended. Its allocator has no stale account callback to invoke.
    actual.clear();ASSERT_TRUE(actual.emplace(9,33).second);
    EXPECT_EQ(account.used,ended);EXPECT_EQ(account.calls,calls);EXPECT_EQ(actual.at(9),33);
}
TEST(OrdinarySchemaPreimage, ActualStorageAllocatorOverflowAndAccountReplacementRefuseBeforeDebit) {
    actual_storage_debits original(4096),replacement(4096);
    storage_scope held(&original,&original,actual_storage_debits::debit);
    storage_allocator<std::uint64_t> allocator;
    EXPECT_THROW((void)allocator.allocate(std::numeric_limits<std::size_t>::max()),
        lattice::detail::ordinary_initialization_storage_error);
    EXPECT_EQ(original.calls,0u);EXPECT_EQ(original.used,0u);
    EXPECT_THROW((storage_scope(&replacement,&replacement,actual_storage_debits::debit)),
        lattice::detail::ordinary_initialization_storage_error);
    EXPECT_EQ(replacement.calls,0u);EXPECT_EQ(replacement.used,0u);
    auto* actual=allocator.allocate(1);allocator.deallocate(actual,1);
    EXPECT_EQ(original.calls,1u);EXPECT_EQ(original.used,sizeof(std::uint64_t));
}

namespace {
ordinary_schema_projection two_declared_shadow_projection() {
    auto catalog=declaration();model_schema target;target.table_name="PreimageTarget";
    property_descriptor value{};value.name="value";value.type=column_type::text;target.properties.push_back(value);
    if(!catalog.admit_model(target))throw std::runtime_error("actual second stage model declaration refused");
    for(const auto* field:{"group","other"}) {
        property_descriptor link{};link.name=field;link.kind=property_kind::link;link.target_table=target.table_name;
        catalog.models.at("PreimageRow").properties.push_back(link);
    }
    catalog.swift_models.insert("PreimageRow");catalog.swift_constraints["PreimageRow"]={recovery_constraint{{"group"},false},recovery_constraint{{"other"},false}};
    return describe_ordinary_projection(catalog,{});
}
ordinary_schema_preimage inspect_initial_physical(PreimageDb& db,ordinary_schema_projection& selected,TypedRead& read) {
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    db.execute("BEGIN");
    try {
        const auto result=ordinary_preimage_recipe_test_access::construction_initial(selected,
            [&](const auto& statement){return read(statement);},limits);
        db.execute("ROLLBACK");return result;
    }catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, PrivateInitialConstructionAcceptsExactOriginalBaseWithoutWeakeningGenericPrefix) {
    PreimageDb db;const auto full=two_declared_shadow_projection();auto selected=full;TypedRead read{db};
    db.execute(full.tables.at("PreimageRow").create_sql);
    EXPECT_THROW((void)inspect_initialization_image(db,full,read,false),ordinary_schema_validation_error);
    const auto proof=inspect_initial_physical(db,selected,read);
    EXPECT_TRUE(selected.tables.at("PreimageRow").shadow_columns.empty());
    EXPECT_EQ(full.tables.at("PreimageRow").shadow_columns,(std::vector<std::string>{"group__link_gid","other__link_gid"}));
    EXPECT_FALSE(proof.plain_shapes.empty());
    EXPECT_NO_THROW((void)inspect_initialization_image(db,selected,read,false));
    EXPECT_THROW((void)inspect_initialization_image(db,full,read,true),ordinary_schema_validation_error);
}
TEST(OrdinarySchemaPreimage, PrivateInitialConstructionAcceptsAllCurrentDeclaredShadowColumns) {
    PreimageDb db;const auto full=two_declared_shadow_projection();auto selected=full;TypedRead read{db};
    db.execute(full.tables.at("PreimageRow").create_sql);
    for(const auto& column:full.tables.at("PreimageRow").shadow_columns)
        db.execute(ordinary_preimage_recipe_test_access::shadow_column("PreimageRow",column));
    EXPECT_NO_THROW((void)inspect_initial_physical(db,selected,read));
    EXPECT_EQ(selected.tables.at("PreimageRow").shadow_columns,full.tables.at("PreimageRow").shadow_columns);
    EXPECT_NO_THROW((void)inspect_initialization_image(db,selected,read,false));
}
TEST(OrdinarySchemaPreimage, PrivateInitialConstructionRefusesUnissuedPartialWrongTypeAndUnknownColumns) {
    for(const int fault:{0,1,2}) {
        PreimageDb db;auto selected=two_declared_shadow_projection();TypedRead read{db};
        db.execute(selected.tables.at("PreimageRow").create_sql);
        if(fault==0)db.execute(ordinary_preimage_recipe_test_access::shadow_column("PreimageRow","group__link_gid"));
        else if(fault==1) {
            db.execute("ALTER TABLE PreimageRow ADD COLUMN group__link_gid BLOB");
            db.execute(ordinary_preimage_recipe_test_access::shadow_column("PreimageRow","other__link_gid"));
        }else db.execute("ALTER TABLE PreimageRow ADD COLUMN invented__link_gid TEXT");
        EXPECT_THROW((void)inspect_initial_physical(db,selected,read),ordinary_schema_validation_error);
    }
}


#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <signal.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <time.h>
namespace lattice::detail {
// Passive pointer identity only. Never dereferences an inherited TLS scope,
// mutates current_, issues a scope or calls a debit sink.
struct ordinary_initialization_storage_test_access final {
    static const void* current_identity()noexcept {
        return ordinary_initialization_storage_scope::current_;
    }
};
}
TEST(OrdinarySchemaPreimage, RealForkUnwindsInheritedStorageScopeBeforeFreshChildDebit) {
    using access=lattice::detail::ordinary_initialization_storage_test_access;
    ASSERT_EQ(access::current_identity(),nullptr);
    actual_storage_debits parent(8*sizeof(std::uint64_t));
    pid_t child=-1;int fork_errno=0;std::exception_ptr parent_later_error;
    {
        storage_scope inherited(&parent,&parent,actual_storage_debits::debit);
        storage_allocator<std::uint64_t> parent_allocator;
        auto* actual=parent_allocator.allocate(1);parent_allocator.deallocate(actual,1);
        ASSERT_EQ(parent.calls,1u);ASSERT_EQ(parent.used,sizeof(std::uint64_t));
        ASSERT_EQ(access::current_identity(),&inherited);
        child=::fork();if(child<0)fork_errno=errno;
        if(child>0) {
            // The parent retains its genuine original scope/account while the
            // child independently unwinds the inherited frame below.
            EXPECT_EQ(access::current_identity(),&inherited);
            try {auto* again=parent_allocator.allocate(1);parent_allocator.deallocate(again,1);}
            catch(...) {parent_later_error=std::current_exception();}
            // Observe/reap the actual child before reporting any real later
            // parent allocation error; never unwind past an unreaped child.
            EXPECT_EQ(parent.calls,2u);EXPECT_EQ(parent.used,2*sizeof(std::uint64_t));
        }
    } // The child really runs the inherited scope destructor here.
    if(child==0) {
        // This passive check compares the stored pointer only. It deliberately
        // fails old001 BEFORE constructing a fresh scope or dereferencing the
        // ended inherited stack frame; stale-stack contents are no pass oracle.
        if(access::current_identity()!=nullptr)::_exit(81);
        if(parent.calls!=1||parent.used!=sizeof(std::uint64_t)||parent.failed)::_exit(82);
        try {
            actual_storage_debits child_account(2*sizeof(std::uint64_t));
            {
                storage_scope fresh(&child_account,&child_account,actual_storage_debits::debit);
                if(access::current_identity()!=&fresh)::_exit(83);
                storage_allocator<std::uint64_t> child_allocator;
                auto* actual=child_allocator.allocate(2);
                if(!actual)::_exit(84);
                child_allocator.deallocate(actual,2);
                if(child_account.calls!=1||child_account.used!=2*sizeof(std::uint64_t)||child_account.failed)::_exit(85);
                if(parent.calls!=1||parent.used!=sizeof(std::uint64_t)||parent.failed)::_exit(86);
            }
            if(access::current_identity()!=nullptr)::_exit(87);
        }catch(...) {::_exit(88);}
        // Never destroy/refcount the inherited parent's exception_ptr payload.
        ::_exit(0);
    }
    ASSERT_GT(child,0)<<"actual fork errno="<<fork_errno;
    EXPECT_EQ(access::current_identity(),nullptr);
    EXPECT_EQ(parent.calls,2u);EXPECT_EQ(parent.used,2*sizeof(std::uint64_t));EXPECT_FALSE(parent.failed);
    int status=0;pid_t ended=0;int ending_errno=0;
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    while(std::chrono::steady_clock::now()<end) {
        ended=::waitpid(child,&status,WNOHANG);
        if(ended==child)break;
        if(ended<0&&errno!=EINTR){ending_errno=errno;break;}
        const timespec pause{0,1000000};(void)::nanosleep(&pause,nullptr);
    }
    if(ended!=child) {
        // A failed child cannot leave an unobserved process ending behind the
        // test. Kill once, then require actual reap within a second finite wait.
        const int killed=::kill(child,SIGKILL);const int kill_errno=killed<0?errno:0;
        const auto cleanup_end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<cleanup_end) {
            ended=::waitpid(child,&status,WNOHANG);
            if(ended==child)break;
            if(ended<0&&errno!=EINTR){ending_errno=errno;break;}
            const timespec pause{0,1000000};(void)::nanosleep(&pause,nullptr);
        }
        if(ended!=child){std::fputs("actual storage fork child ending unknown\n",stderr);::_exit(96);}
        FAIL()<<"actual storage fork child did not finish before deadline; wait errno="<<ending_errno<<" kill errno="<<kill_errno;
    }
    EXPECT_FALSE(parent_later_error)<<"real later parent allocation failed; actual child ending is now known";
    ASSERT_TRUE(WIFEXITED(status))<<"actual child status="<<status;
    EXPECT_EQ(WEXITSTATUS(status),0)<<"81 means inherited TLS was not cleared before any fresh child scope/allocation";
}
#endif


TEST(OrdinarySchemaPreimage, ActualTypedMetadataStorageExactFitRetainsAllOriginalFacts) {
    // Actual SQLite metadata/lexer/receipt work under the private allocation
    // primitive. This measures this producer component, not a full installed
    // startup profile or unconverted result/generator allocation liability.
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_storage_debits measured(2*1024*1024);ordinary_schema_preimage first;
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);TypedRead read{db};
     first=inspect(db,projection,read);EXPECT_GT(read.metadata_rows,0u);}
    ASSERT_GT(measured.used,0u);ASSERT_FALSE(measured.failed);ASSERT_FALSE(first.main_objects.empty());
    actual_storage_debits exact(measured.used);ordinary_schema_preimage again;
    {storage_scope account(&exact,&exact,actual_storage_debits::debit);TypedRead read{db};again=inspect(db,projection,read);}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    EXPECT_EQ(again.schema_cookie,first.schema_cookie);EXPECT_EQ(again.temp_schema_cookie,first.temp_schema_cookie);
    EXPECT_EQ(again.inspected_rows,first.inspected_rows);EXPECT_EQ(again.copied_bytes,first.copied_bytes);
    EXPECT_EQ(again.absent_tables,first.absent_tables);EXPECT_EQ(again.verified_trigger_origins,first.verified_trigger_origins);
    ASSERT_EQ(again.main_objects.size(),first.main_objects.size());
    for(std::size_t index=0;index<first.main_objects.size();++index){
        EXPECT_EQ(again.main_objects[index].type,first.main_objects[index].type);
        EXPECT_EQ(again.main_objects[index].name,first.main_objects[index].name);
        EXPECT_EQ(again.main_objects[index].owner,first.main_objects[index].owner);
        EXPECT_EQ(again.main_objects[index].sql,first.main_objects[index].sql);
    }
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, ActualTypedMetadataStorageInsufficientKeepsFirstErrorAndRealRollback) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_storage_debits measured(2*1024*1024);
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);TypedRead read{db};
     const auto complete=inspect(db,projection,read);ASSERT_TRUE(complete.absent_tables.empty());}
    ASSERT_GT(measured.used,1u);ASSERT_FALSE(measured.failed);
    actual_storage_debits insufficient(measured.used-1);std::exception_ptr first;
    {storage_scope account(&insufficient,&insufficient,actual_storage_debits::debit);TypedRead read{db};
     try{(void)inspect(db,projection,read);}catch(...){first=std::current_exception();}}
    EXPECT_EQ(first,insufficient.actual_refusal);EXPECT_TRUE(insufficient.failed);
    EXPECT_LE(insufficient.used,insufficient.maximum);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
    // Ending the failed scope cannot refund or resurrect that actual debit.
    const auto used=insufficient.used,calls=insufficient.calls;
    TypedRead after{db};const auto intact=inspect(db,projection,after);
    EXPECT_TRUE(intact.absent_tables.empty());EXPECT_EQ(insufficient.used,used);EXPECT_EQ(insufficient.calls,calls);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}


#include <algorithm>
namespace {
struct actual_inventory_transfer {
    TypedRead read;
    const char* actual_sql=nullptr;std::size_t actual_bytes=0;
    bool main_returned=false,deny_node=false,node_refused=false;
    std::uint64_t refused_bytes=0;
    actual_storage_debits account{8*1024*1024};
    explicit actual_inventory_transfer(PreimageDb& db):read{db}{}
    static void debit(void* value,void* owner,std::uint64_t bytes) {
        if(value!=owner)throw std::runtime_error("actual inventory storage owner differs");
        auto& self=*static_cast<actual_inventory_transfer*>(value);
        // With this fixed small real SQLite fixture the first rebound map
        // node after MAIN inventory exceeds its short folded-name copy.
        // Denial happens in the real prospective allocator, before Node new.
        if(self.deny_node&&self.main_returned&&bytes>=sizeof(std::pair<const std::string,ordinary_object_metadata>)) {
            self.node_refused=true;self.refused_bytes=bytes;
            self.account.failed=true;std::rethrow_exception(self.account.actual_refusal);
        }
        actual_storage_debits::debit(&self.account,&self.account,bytes);
    }
    std::vector<database::row_t> operator()(const std::shared_ptr<const ordinary_preimage_statement>& entry) {
        auto result=read(entry);
        if(entry->kind_id()==ordinary_preimage_statement::kind::main_inventory) {
            for(auto& row:result) {
                const auto found=row.find("name");const auto sql=row.find("sql");
                if(found==row.end()||sql==row.end())throw std::runtime_error("actual inventory fields absent");
                const auto* name=std::get_if<std::string>(&found->second);
                if(name&&*name=="AuditPreimageRowInsert") {
                    const auto* value=std::get_if<std::string>(&sql->second);
                    if(!value||value->size()<256)throw std::runtime_error("actual retained SQL must own a noninline allocation");
                    actual_sql=value->data();actual_bytes=value->size();
                }
            }
            main_returned=true;
        }
        return result;
    }
};
ordinary_schema_preimage inspect_actual_inventory(PreimageDb& db,const ordinary_schema_projection& projection,actual_inventory_transfer& source) {
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    db.execute("BEGIN");
    try {auto result=validate_ordinary_preexisting_schema(projection,[&](const auto& entry){return source(entry);},limits);
        db.execute("ROLLBACK");return result;}
    catch(...){db.execute("ROLLBACK");throw;}
}
// Real finite ordinary query adapter. This path deliberately has no startup
// storage scope/profile; the ordinary metadata API still returns its full
// original object list and accepts the same TEXT/BLOB result representation.
std::vector<database::row_t> actual_generic_metadata(PreimageDb& db,const std::string& sql,
    const std::vector<column_value_t>& parameters,std::size_t cap) {
    if(!cap||cap>4096||parameters.size()>1)throw std::runtime_error("actual generic metadata bounds");
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual generic metadata prepare");
    int next=1;for(const auto& parameter:parameters) {
        const auto* value=std::get_if<std::string>(&parameter);
        if(!value||value->size()>256||sqlite3_bind_text(statement.value,next++,value->data(),int(value->size()),SQLITE_STATIC)!=SQLITE_OK)
            throw std::runtime_error("actual generic metadata bind");
    }
    const int columns=sqlite3_column_count(statement.value);
    if(columns<1||columns>16)throw std::runtime_error("actual generic metadata columns");
    std::array<std::string,16> names;
    for(int i=0;i<columns;++i){const auto* name=sqlite3_column_name(statement.value,i);
        if(!name)throw std::runtime_error("actual generic metadata name");std::size_t bytes=0;while(bytes<=256&&name[bytes])++bytes;
        if(bytes>256)throw std::runtime_error("actual generic metadata name bound");names[size_t(i)].assign(name,bytes);}
    std::vector<database::row_t> result;std::size_t copied=0;int rc;
    while((rc=sqlite3_step(statement.value))==SQLITE_ROW) {
        if(result.size()>=cap)throw std::runtime_error("actual generic metadata row cap");
        database::row_t row;
        for(int i=0;i<columns;++i) {
            const auto type=sqlite3_column_type(statement.value,i);
            const int bytes=(type==SQLITE_TEXT||type==SQLITE_BLOB)?sqlite3_column_bytes(statement.value,i):8;
            if(bytes<0||std::size_t(bytes)>1024*1024-copied)throw std::runtime_error("actual generic metadata byte cap");copied+=std::size_t(bytes);
            column_value_t value=nullptr;
            if(type==SQLITE_TEXT){const auto* data=sqlite3_column_text(statement.value,i);if(!data&&bytes)throw std::runtime_error("actual generic text missing");
                value=std::string(data?reinterpret_cast<const char*>(data):"",std::size_t(bytes));}
            else if(type==SQLITE_BLOB){const auto* data=static_cast<const std::uint8_t*>(sqlite3_column_blob(statement.value,i));if(!data&&bytes)throw std::runtime_error("actual generic blob missing");
                value=data?std::vector<std::uint8_t>(data,data+bytes):std::vector<std::uint8_t>{};}
            else if(type==SQLITE_INTEGER)value=std::int64_t(sqlite3_column_int64(statement.value,i));
            else if(type==SQLITE_FLOAT)value=sqlite3_column_double(statement.value,i);
            else if(type!=SQLITE_NULL)throw std::runtime_error("actual generic metadata cell type");
            row.emplace(names[size_t(i)],std::move(value));
        }
        result.push_back(std::move(row));
    }
    if(rc!=SQLITE_DONE)throw std::runtime_error("actual generic metadata step");statement.finish();return result;
}
}
TEST(OrdinarySchemaPreimage, ActualTypedInventoryTransfersOriginalRequestTextIntoFinalProof) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_inventory_transfer source(db);ordinary_schema_preimage proof;
    {storage_scope actual(&source,&source,actual_inventory_transfer::debit);proof=inspect_actual_inventory(db,projection,source);}
    ASSERT_TRUE(source.main_returned);ASSERT_NE(source.actual_sql,nullptr);ASSERT_GT(source.actual_bytes,256u);
    const auto found=std::find_if(proof.main_objects.begin(),proof.main_objects.end(),[](const auto& object){return object.name=="AuditPreimageRowInsert";});
    ASSERT_NE(found,proof.main_objects.end());EXPECT_EQ(found->sql.data(),source.actual_sql);EXPECT_EQ(found->sql.size(),source.actual_bytes);
    EXPECT_NE(found->sql.find("AuditPreimageRowInsert"),std::string::npos);
    EXPECT_TRUE(proof.absent_tables.empty());EXPECT_EQ(proof.verified_trigger_origins.size(),3u);
    EXPECT_GT(source.account.calls,0u);EXPECT_FALSE(source.account.failed);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, ActualInventoryNodeRefusalRetainsFirstDebitErrorAndKnownReadEnding) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_inventory_transfer source(db);source.deny_node=true;std::exception_ptr actual;
    {storage_scope original(&source,&source,actual_inventory_transfer::debit);
     try{(void)inspect_actual_inventory(db,projection,source);}catch(...){actual=std::current_exception();}}
    EXPECT_EQ(actual,source.account.actual_refusal);ASSERT_TRUE(source.main_returned);EXPECT_TRUE(source.node_refused);
    EXPECT_GE(source.refused_bytes,sizeof(std::pair<const std::string,ordinary_object_metadata>));
    EXPECT_TRUE(source.account.failed);EXPECT_EQ(source.read.tvf_queries,0u);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
    const auto ended_used=source.account.used,ended_calls=source.account.calls;
    TypedRead read{db};const auto intact=inspect(db,projection,read);EXPECT_TRUE(intact.absent_tables.empty());
    EXPECT_EQ(source.account.used,ended_used);EXPECT_EQ(source.account.calls,ended_calls);
}
TEST(OrdinarySchemaPreimage, ActualGenericMetadataKeepsCompleteOriginalObjectListWithoutStartupAccount) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};ordinary_schema_metadata ordinary;
    db.execute("BEGIN");
    try {ordinary=validate_ordinary_schema(projection,[&](const auto& sql,const auto& args,std::size_t cap){return actual_generic_metadata(db,sql,args,cap);},limits);
        db.execute("ROLLBACK");}
    catch(...){db.execute("ROLLBACK");throw;}
    ASSERT_FALSE(ordinary.objects.empty());ASSERT_TRUE(ordinary.tables.count("PreimageRow"));
    TypedRead read{db};const auto typed=inspect(db,projection,read);
    for(const auto& object:ordinary.objects) {
        const auto found=std::find_if(typed.main_objects.begin(),typed.main_objects.end(),[&](const auto& value){return value.name==object.name;});
        ASSERT_NE(found,typed.main_objects.end());EXPECT_EQ(found->type,object.type);EXPECT_EQ(found->owner,object.owner);EXPECT_EQ(found->sql,object.sql);
    }
    std::size_t triggers=0;for(const auto& object:ordinary.objects)if(object.type=="trigger")++triggers;
    EXPECT_EQ(triggers,3u);EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}

namespace {
// Actual SQLite typed metadata adapter consuming only retained DATA views. It
// neither installs a profile nor claims Core Request/result allocation closure.
struct BorrowedTypedRead {
    PreimageDb& db;
    size_t tvf_queries=0,metadata_rows=0;
    std::shared_ptr<const ordinary_preimage_statement> retained;
    std::vector<database::row_t> operator()(const std::shared_ptr<const ordinary_preimage_statement>& entry){
        if(!entry||!entry->creator_current())throw std::runtime_error("actual typed metadata entry missing");
        if(entry->parameter_count()==1)retained=entry;
        using kind=ordinary_preimage_statement::kind;
        const auto issued=entry->kind_id();
        if(issued==kind::table_columns||issued==kind::table_foreign_keys||issued==kind::temp_table_foreign_keys||
           issued==kind::index_list||issued==kind::index_columns)++tvf_queries;
        if(entry->sql_view().size()>256*1024||entry->parameter_count()>1)throw std::runtime_error("actual metadata declaration exceeds control bounds");
        Statement statement;const char* tail=nullptr;
        if(sqlite3_prepare_v2(db.value,entry->sql_view().data(),int(entry->sql_view().size()),&statement.value,&tail)!=SQLITE_OK||
           !statement.value||tail!=entry->sql_view().data()+entry->sql_view().size())throw std::runtime_error("actual typed recipe prepare failed");
        int bound_index=1;
        for(size_t index=0;index<entry->parameter_count();++index){
            const auto text=entry->parameter_text(index);
            if(text.size()>256||sqlite3_bind_text64(statement.value,bound_index++,text.data(),
                static_cast<sqlite3_uint64>(text.size()),SQLITE_TRANSIENT,SQLITE_UTF8)!=SQLITE_OK)
                throw std::runtime_error("actual borrowed metadata parameter failed");
        }
        const int columns=sqlite3_column_count(statement.value);
        if(columns<1||size_t(columns)>entry->column_limit())throw std::runtime_error("metadata columns exceeded BEFORE copy");
        std::array<std::string,16> names;size_t name_bytes=0;
        for(int i=0;i<columns;++i){const auto* name=sqlite3_column_name(statement.value,i);if(!name)throw std::runtime_error("actual column name missing");
            size_t count=0;while(count<=256&&name[count])++count;
            if(count>256)throw std::runtime_error("actual column name exceeded BEFORE copy");
            if(count>entry->result_byte_limit()-name_bytes)throw std::runtime_error("metadata key bytes exceeded BEFORE copy");
            name_bytes+=count;names[size_t(i)].assign(name,count);
        }
        std::vector<database::row_t> result;size_t copied=0;int rc;
        while((rc=sqlite3_step(statement.value))==SQLITE_ROW){
            if(result.size()>=entry->row_limit())throw std::runtime_error("metadata rows exceeded BEFORE copy");
            size_t charge=0;std::array<int,16> types{},sizes{};
            for(int i=0;i<columns;++i){types[size_t(i)]=sqlite3_column_type(statement.value,i);
                const int size=(types[size_t(i)]==SQLITE_TEXT||types[size_t(i)]==SQLITE_BLOB)?sqlite3_column_bytes(statement.value,i):8;
                if(size<0)throw std::runtime_error("actual metadata extent invalid");sizes[size_t(i)]=size;
                const auto amount=names[size_t(i)].size()+size_t(size);
                if(amount>entry->result_byte_limit()-charge)throw std::runtime_error("metadata row bytes exceeded BEFORE copy");charge+=amount;
            }
            if(charge>entry->result_byte_limit()-copied)throw std::runtime_error("metadata result bytes exceeded BEFORE copy");
            copied+=charge;database::row_t row;
            for(int i=0;i<columns;++i){column_value_t cell=nullptr;
                switch(types[size_t(i)]){
                  case SQLITE_INTEGER:cell=int64_t(sqlite3_column_int64(statement.value,i));break;
                  case SQLITE_FLOAT:cell=sqlite3_column_double(statement.value,i);break;
                  case SQLITE_TEXT:{const auto* value=sqlite3_column_text(statement.value,i);if(!value&&sizes[size_t(i)])throw std::runtime_error("actual text missing");cell=std::string((value?reinterpret_cast<const char*>(value):""),size_t(sizes[size_t(i)]));break;}
                  case SQLITE_BLOB:{const auto* value=static_cast<const uint8_t*>(sqlite3_column_blob(statement.value,i));if(!value&&sizes[size_t(i)])throw std::runtime_error("actual blob missing");cell=value?std::vector<uint8_t>(value,value+sizes[size_t(i)]):std::vector<uint8_t>{};break;}
                  case SQLITE_NULL:break;
                  default:throw std::runtime_error("actual column type unsupported");
                }
                row.emplace(names[size_t(i)],std::move(cell));
            }
            result.push_back(std::move(row));++metadata_rows;
        }
        if(rc!=SQLITE_DONE)throw std::runtime_error("actual metadata step failed");statement.finish();return result;
    }
};
ordinary_schema_preimage inspect_borrowed(PreimageDb& db,const ordinary_schema_projection& projection,BorrowedTypedRead& read){
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    db.execute("BEGIN");
    try{auto result=validate_ordinary_preexisting_schema(projection,[&](const auto& entry){return read(entry);},limits);
        db.execute("ROLLBACK");return result;}
    catch(...){db.execute("ROLLBACK");throw;}
}
}
TEST(OrdinarySchemaPreimage, BorrowedStatementStorageExactFitRetainsSqlAndIdentifierBeyondProducer) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_storage_debits measured(2*1024*1024);BorrowedTypedRead first{db};ordinary_schema_preimage facts;
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);facts=inspect_borrowed(db,projection,first);}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,0u);ASSERT_TRUE(first.retained);
    ASSERT_EQ(first.retained->parameter_count(),1u);
    const auto retained_sql=std::string(first.retained->sql_view());
    const auto retained_parameter=std::string(first.retained->parameter_text(0));
    EXPECT_FALSE(retained_sql.empty());EXPECT_FALSE(retained_parameter.empty());
    EXPECT_THROW((void)first.retained->parameter_text(1),ordinary_schema_validation_error);
    // The original generic compatibility API still owns identical DATA. This
    // generic statement was never admitted to a Core initialization Request.
    EXPECT_EQ(first.retained->sql(),retained_sql);
    ASSERT_EQ(first.retained->parameters().size(),1u);
    EXPECT_EQ(std::get<std::string>(first.retained->parameters()[0]),retained_parameter);
    actual_storage_debits exact(measured.used);BorrowedTypedRead again{db};ordinary_schema_preimage repeated;
    {storage_scope account(&exact,&exact,actual_storage_debits::debit);repeated=inspect_borrowed(db,projection,again);}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    EXPECT_EQ(repeated.schema_cookie,facts.schema_cookie);EXPECT_EQ(repeated.temp_schema_cookie,facts.temp_schema_cookie);
    EXPECT_EQ(repeated.inspected_rows,facts.inspected_rows);EXPECT_EQ(repeated.copied_bytes,facts.copied_bytes);
    EXPECT_EQ(repeated.main_objects.size(),facts.main_objects.size());EXPECT_EQ(repeated.absent_tables,facts.absent_tables);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, BorrowedStatementStorageInsufficientRetainsFirstErrorAndRollsBack) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_storage_debits measured(2*1024*1024);BorrowedTypedRead first{db};
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);(void)inspect_borrowed(db,projection,first);}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,1u);
    actual_storage_debits short_account(measured.used-1);BorrowedTypedRead denied{db};std::exception_ptr primary;
    {storage_scope account(&short_account,&short_account,actual_storage_debits::debit);
     try{(void)inspect_borrowed(db,projection,denied);}catch(...){primary=std::current_exception();}}
    EXPECT_EQ(primary,short_account.actual_refusal);EXPECT_TRUE(short_account.failed);
    EXPECT_LE(short_account.used,short_account.maximum);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
    const auto before_used=short_account.used,before_calls=short_account.calls;
    BorrowedTypedRead after{db};const auto intact=inspect_borrowed(db,projection,after);
    EXPECT_TRUE(intact.absent_tables.empty());EXPECT_EQ(short_account.used,before_used);EXPECT_EQ(short_account.calls,before_calls);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}


TEST(OrdinarySchemaPreimage, InventoryRequestMoveHasNoNewDebitAndCopyOwnsActualAccountedCharacters) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    BorrowedTypedRead read{db};const auto proof=inspect_borrowed(db,projection,read);
    const auto found=std::find_if(proof.main_objects.begin(),proof.main_objects.end(),[](const auto& f){return f.name=="AuditPreimageRowInsert";});
    ASSERT_NE(found,proof.main_objects.end());ASSERT_GT(found->sql.size(),256u);
    std::string actual(found->sql.data(),found->sql.size());const auto* original=actual.data();
    actual_storage_debits zero(0);ordinary_inventory_text adopted;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);adopted=ordinary_inventory_text(std::move(actual));}
    EXPECT_EQ(adopted.data(),original);EXPECT_EQ(zero.calls,0u);EXPECT_EQ(adopted,found->sql);
    std::exception_ptr primary;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     try{ordinary_inventory_text copy(adopted);(void)copy;}catch(...){primary=std::current_exception();}}
    EXPECT_EQ(primary,zero.actual_refusal);EXPECT_TRUE(zero.failed);EXPECT_GT(zero.calls,0u);
    EXPECT_EQ(adopted.data(),original);EXPECT_EQ(adopted,found->sql);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, FullActualProofCopyCannotEscapeTheOriginalProspectiveDebit) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    BorrowedTypedRead read{db};const auto proof=inspect_borrowed(db,projection,read);
    ASSERT_FALSE(proof.main_objects.empty());ASSERT_FALSE(proof.plain_shapes.empty());
    actual_storage_debits zero(0);std::exception_ptr primary;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     try{ordinary_schema_preimage copy(proof);(void)copy;}catch(...){primary=std::current_exception();}}
    EXPECT_EQ(primary,zero.actual_refusal);EXPECT_TRUE(zero.failed);EXPECT_GT(zero.calls,0u);
    EXPECT_TRUE(proof.absent_tables.empty());EXPECT_EQ(proof.verified_trigger_origins.size(),3u);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, ActualPhysicalColumnPayloadCopyUsesTheSameAccountBeforeAllocation) {
    PreimageDb db;auto catalog=declaration();const std::string long_name="field_"+std::string(180,'x');
    catalog.models.at("PreimageRow").properties.front().name=long_name;
    const auto projection=describe_ordinary_projection(catalog,{});seed(db,projection);BorrowedTypedRead read{db};
    const auto proof=inspect_borrowed(db,projection,read);
    const auto shape=std::find_if(proof.plain_shapes.begin(),proof.plain_shapes.end(),[](const auto& f){return f.name=="PreimageRow";});
    ASSERT_NE(shape,proof.plain_shapes.end());
    const auto column=std::find_if(shape->physical_columns.begin(),shape->physical_columns.end(),[&](const auto& c){return c.name==long_name;});
    ASSERT_NE(column,shape->physical_columns.end());actual_storage_debits zero(0);std::exception_ptr primary;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     try{ordinary_column_metadata copy(*column);(void)copy;}catch(...){primary=std::current_exception();}}
    EXPECT_EQ(primary,zero.actual_refusal);EXPECT_TRUE(zero.failed);EXPECT_GT(zero.calls,0u);
    EXPECT_EQ(column->name,long_name);EXPECT_EQ(column->type,"text");
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, ActualInventoryBlobSpellingRetainsEveryByteThroughAccountedFallback) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    db.execute("CREATE TEMP TABLE UnrelatedTemporary(value TEXT)");db.execute("BEGIN");
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    auto main=actual_generic_metadata(db,"SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema ORDER BY name COLLATE BINARY LIMIT 129",{},128);
    auto temp=actual_generic_metadata(db,"SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT 129",{},128);
    std::string expected;for(auto* inventory:{&main,&temp})for(auto& row:*inventory) {
        auto i=row.find("sql");if(i==row.end())continue;
        if(const auto* value=std::get_if<std::string>(&i->second)) {
            if(row.at("name")==column_value_t(std::string("AuditPreimageRowInsert")))expected=*value;
            i->second=std::vector<uint8_t>(value->begin(),value->end());
        }
    }
    ASSERT_GT(expected.size(),256u);BorrowedTypedRead read{db};actual_storage_debits account(8*1024*1024);ordinary_schema_preimage proof;
    bool main_moved=false,temp_moved=false;std::exception_ptr primary;
    try {
        storage_scope scope(&account,&account,actual_storage_debits::debit);
        proof=validate_ordinary_preexisting_schema(projection,[&](const auto& entry){
            if(entry->kind_id()==ordinary_preimage_statement::kind::main_inventory){if(main_moved)throw std::runtime_error("MAIN inventory reused");main_moved=true;return std::move(main);}
            if(entry->kind_id()==ordinary_preimage_statement::kind::temp_inventory){if(temp_moved)throw std::runtime_error("TEMP inventory reused");temp_moved=true;return std::move(temp);}
            return read(entry);
        },limits);
    }catch(...){primary=std::current_exception();}
    db.execute("ROLLBACK");if(primary)std::rethrow_exception(primary);
    EXPECT_TRUE(main_moved);EXPECT_TRUE(temp_moved);EXPECT_FALSE(account.failed);EXPECT_GT(account.calls,0u);
    const auto found=std::find_if(proof.main_objects.begin(),proof.main_objects.end(),[](const auto& f){return f.name=="AuditPreimageRowInsert";});
    ASSERT_NE(found,proof.main_objects.end());EXPECT_EQ(found->sql,expected);EXPECT_NE(found->sql.data(),expected.data());
    const auto temporary=std::find_if(proof.temp_objects.begin(),proof.temp_objects.end(),[](const auto& f){return f.name=="UnrelatedTemporary";});
    ASSERT_NE(temporary,proof.temp_objects.end());EXPECT_EQ(temporary->owner,"UnrelatedTemporary");
    EXPECT_EQ(temporary->sql,"CREATE TABLE UnrelatedTemporary(value TEXT)");
    EXPECT_TRUE(proof.absent_tables.empty());EXPECT_EQ(proof.verified_trigger_origins.size(),3u);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}
TEST(OrdinarySchemaPreimage, PendingAccountRefusesGenericOwningInputBeforeCallingItsCallback) {
    PreimageDb db;const auto projection=describe_ordinary_projection(declaration(),{});seed(db,projection);
    actual_storage_debits account(8*1024*1024);size_t callbacks=0;std::string reason;
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
    {storage_scope scope(&account,&account,actual_storage_debits::debit);
     try{(void)validate_ordinary_schema(projection,[&](const auto& sql,const auto& parameters,size_t cap){++callbacks;return actual_generic_metadata(db,sql,parameters,cap);},limits);}
     catch(const ordinary_initialization_storage_error& failure){reason=failure.what();}}
    EXPECT_EQ(reason,"startup cannot materialize unaccounted legacy text");EXPECT_EQ(callbacks,0u);
    EXPECT_GT(account.calls,0u);EXPECT_FALSE(account.failed);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}

namespace {
using audit_storage_text=std::basic_string<char,std::char_traits<char>,ordinary_initialization_allocator<char>>;
using audit_storage_column=std::pair<audit_storage_text,column_type>;
using audit_storage_output=std::vector<audit_storage_text,ordinary_initialization_allocator<audit_storage_text>>;
template<class Emit>void storage_audit_program_corpus(Emit&& emit) {
    const audit_storage_text table("PreimageRow"),link("_PreimageRow_group"),virtual_link("_PreimageRow_related");
    std::vector<audit_storage_column,ordinary_initialization_allocator<audit_storage_column>> columns;
    columns.emplace_back(audit_storage_text("label"),column_type::text);
    columns.emplace_back(audit_storage_text("payload"),column_type::blob);
    columns.emplace_back(audit_storage_text("counter"),column_type::integer);
    std::set<audit_storage_text,std::less<audit_storage_text>,ordinary_initialization_allocator<audit_storage_text>> no_history;
    no_history.emplace("label");
    ordinary_model_audit_programs_storage(table,columns,no_history,audit_storage_text{},emit);
    ordinary_link_audit_programs_storage(link,audit_storage_text{},emit);
    ordinary_virtual_link_audit_programs_storage(virtual_link,emit);
}
std::vector<std::string> ordinary_audit_program_corpus() {
    const std::vector<std::pair<std::string,column_type>> columns{
        {"label",column_type::text},{"payload",column_type::blob},{"counter",column_type::integer}};
    const std::set<std::string> no_history{"label"};std::vector<std::string> result;
    const auto emit=[&](const auto& sql){result.push_back(sql);};
    ordinary_model_audit_programs("PreimageRow",columns,no_history,{},emit);
    ordinary_link_audit_programs("_PreimageRow_group",{},emit);
    ordinary_virtual_link_audit_programs("_PreimageRow_related",emit);return result;
}
}
TEST(OrdinarySchemaPreimage, SharedAuditProgramStorageExactFitPreservesOrdinarySqlBytes) {
    const auto reference=ordinary_audit_program_corpus();ASSERT_EQ(reference.size(),7u);
    actual_storage_debits measured(2*1024*1024);audit_storage_output programs;
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);
     storage_audit_program_corpus([&](const auto& sql){programs.push_back(sql);});}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,0u);ASSERT_EQ(programs.size(),reference.size());
    for(size_t index=0;index<reference.size();++index)
        EXPECT_EQ(std::string_view(programs[index]),std::string_view(reference[index]));
    actual_storage_debits exact(measured.used);audit_storage_output repeated;
    {storage_scope account(&exact,&exact,actual_storage_debits::debit);
     storage_audit_program_corpus([&](const auto& sql){repeated.push_back(sql);});}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    ASSERT_EQ(repeated.size(),programs.size());
    for(size_t index=0;index<programs.size();++index)EXPECT_EQ(repeated[index],programs[index]);
}
TEST(OrdinarySchemaPreimage, SharedAuditProgramStorageInsufficientPreservesActualRefusalWithoutRefund) {
    actual_storage_debits measured(2*1024*1024);audit_storage_output programs;
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);
     storage_audit_program_corpus([&](const auto& sql){programs.push_back(sql);});}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,1u);
    actual_storage_debits insufficient(measured.used-1);audit_storage_output partial;std::exception_ptr primary;
    {storage_scope account(&insufficient,&insufficient,actual_storage_debits::debit);
     try{storage_audit_program_corpus([&](const auto& sql){partial.push_back(sql);});}
     catch(...){primary=std::current_exception();}}
    EXPECT_TRUE(insufficient.failed);EXPECT_EQ(primary,insufficient.actual_refusal);
    EXPECT_LE(insufficient.used,insufficient.maximum);
    const auto used=insufficient.used,calls=insufficient.calls;partial.clear();partial.shrink_to_fit();
    EXPECT_EQ(insufficient.used,used);EXPECT_EQ(insufficient.calls,calls);
    EXPECT_EQ(programs.size(),7u);
}

TEST(OrdinarySchemaPreimage, PrivateProjectionBorrowedLookupPreservesExactCaseSensitiveIdentities) {
    ordinary_schema_projection projection;
    for(const auto* name:{"CaseRow","caseRow","_CaseRow"}) {
        ordinary_projection_table table;table.name=name;
        ASSERT_TRUE(projection.tables.emplace(name,std::move(table)).second);
    }
    const ordinary_initialization_text actual_key("caseRow");
    actual_storage_debits no_copy_budget(0);
    {storage_scope account(&no_copy_budget,&no_copy_budget,actual_storage_debits::debit);
     const auto retained=projection.tables.find(std::string_view(actual_key));
     ASSERT_NE(retained,projection.tables.end());EXPECT_EQ(retained->first,"caseRow");
     const auto other=projection.tables.find(std::string_view("CaseRow"));
     ASSERT_NE(other,projection.tables.end());EXPECT_NE(other,retained);
     EXPECT_EQ(projection.tables.find(std::string_view("CASEROW")),projection.tables.end());
     ASSERT_EQ(projection.tables.size(),3u);auto first=projection.tables.begin();
     EXPECT_EQ(first->first,"CaseRow");++first;EXPECT_EQ(first->first,"_CaseRow");++first;EXPECT_EQ(first->first,"caseRow");}
    EXPECT_FALSE(no_copy_budget.failed);EXPECT_EQ(no_copy_budget.used,0u);EXPECT_EQ(no_copy_budget.calls,0u);
}

TEST(OrdinarySchemaPreimage, ProjectionMapAndDeclarationPointerStorageExactFitRetainsDescriptions) {
    const auto catalog=declaration();const auto original=describe_ordinary_projection(catalog,{});
    const auto* model=catalog.find("PreimageRow");ASSERT_NE(model,nullptr);
    const auto logical=[](size_t){};actual_storage_debits measured(2*1024*1024);ordinary_schema_projection first;
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);
     first=original;const auto ordered=ordinary_projection_property_order(*model,false,logical);
     ASSERT_EQ(ordered.size(),model->properties.size());for(size_t i=0;i<ordered.size();++i)EXPECT_EQ(ordered[i],&model->properties[i]);}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,0u);ASSERT_GE(measured.calls,2u);
    actual_storage_debits exact(measured.used);ordinary_schema_projection repeated;
    {storage_scope account(&exact,&exact,actual_storage_debits::debit);
     repeated=original;const auto ordered=ordinary_projection_property_order(*model,false,logical);EXPECT_EQ(ordered.size(),model->properties.size());}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    ASSERT_EQ(first.tables.size(),original.tables.size());ASSERT_EQ(repeated.tables.size(),original.tables.size());
    for(const auto& [name,table]:original.tables){const auto found=repeated.tables.find(std::string_view(name));
        ASSERT_NE(found,repeated.tables.end());EXPECT_EQ(found->second.name,table.name);EXPECT_EQ(found->second.create_sql,table.create_sql);
        EXPECT_EQ(found->second.columns,table.columns);EXPECT_EQ(found->second.audit_programs,table.audit_programs);
        EXPECT_EQ(found->second.index_programs,table.index_programs);EXPECT_EQ(found->second.shadow_columns,table.shadow_columns);}
}
TEST(OrdinarySchemaPreimage, ProjectionMapAndDeclarationPointerStorageRefusesBeforeActualRequest) {
    const auto catalog=declaration();const auto original=describe_ordinary_projection(catalog,{});
    const auto* model=catalog.find("PreimageRow");ASSERT_NE(model,nullptr);const auto logical=[](size_t){};
    actual_storage_debits measured(2*1024*1024);
    {storage_scope account(&measured,&measured,actual_storage_debits::debit);ordinary_schema_projection copy=original;
     const auto ordered=ordinary_projection_property_order(*model,false,logical);EXPECT_EQ(ordered.size(),model->properties.size());}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,1u);
    actual_storage_debits insufficient(measured.used-1);std::exception_ptr primary;
    {storage_scope account(&insufficient,&insufficient,actual_storage_debits::debit);
     try{ordinary_schema_projection copy=original;(void)ordinary_projection_property_order(*model,false,logical);}
     catch(...){primary=std::current_exception();}}
    EXPECT_TRUE(insufficient.failed);EXPECT_EQ(primary,insufficient.actual_refusal);EXPECT_LE(insufficient.used,insufficient.maximum);
    const auto used=insufficient.used,calls=insufficient.calls;ordinary_schema_projection ordinary=original;
    EXPECT_EQ(ordinary.tables.size(),original.tables.size());EXPECT_EQ(insufficient.used,used);EXPECT_EQ(insufficient.calls,calls);
}


TEST(OrdinarySchemaPreimage, ActualMetadataBorrowedLookupRetainsMissingKeyRefusalWithoutOwningKeyCopy) {
    PreimageDb db;auto catalog=declaration();const std::string name="Model_"+std::string(180,'z');
    auto node=catalog.models.extract("PreimageRow");ASSERT_FALSE(node.empty());node.key()=name;node.mapped().table_name=name;catalog.models.insert(std::move(node));
    const auto projection=describe_ordinary_projection(catalog,{});seed(db,projection);
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};ordinary_schema_metadata metadata;
    db.execute("BEGIN");
    try{metadata=validate_ordinary_schema(projection,[&](const auto& sql,const auto& args,size_t cap){return actual_generic_metadata(db,sql,args,cap);},limits);db.execute("ROLLBACK");}
    catch(...){db.execute("ROLLBACK");throw;}
    const auto found=metadata.tables.find(std::string_view(name));ASSERT_NE(found,metadata.tables.end());
    const std::string missing=name+"_absent";actual_storage_debits zero(0);
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     const auto& actual=ordinary_initialization_map_at(metadata.tables,std::string_view(name));EXPECT_EQ(&actual,&found->second);
     EXPECT_THROW((void)ordinary_initialization_map_at(metadata.tables,std::string_view(missing)),std::out_of_range);}
    EXPECT_EQ(zero.calls,0u);EXPECT_FALSE(zero.failed);EXPECT_EQ(found->second.name,name);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_EQ(sqlite3_next_stmt(db.value,nullptr),nullptr);
}

TEST(OrdinarySchemaPreimage, CompleteProjectionGenerationUsesSameCumulativeExactFitAccount) {
    const auto catalog=declaration();const auto expected=describe_ordinary_projection(catalog,{});
    actual_storage_debits measured(8*1024*1024);ordinary_schema_projection first;
    {storage_scope scope(&measured,&measured,actual_storage_debits::debit);first=describe_ordinary_projection(catalog,{});}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,1u);ASSERT_GT(measured.calls,0u);
    actual_storage_debits exact(measured.used);ordinary_schema_projection repeated;
    {storage_scope scope(&exact,&exact,actual_storage_debits::debit);repeated=describe_ordinary_projection(catalog,{});}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    EXPECT_EQ(repeated.models,expected.models);EXPECT_EQ(repeated.logical_bytes,expected.logical_bytes);
    EXPECT_EQ(repeated.columns,expected.columns);ASSERT_EQ(repeated.tables.size(),expected.tables.size());
    for(const auto& [name,table]:expected.tables) {
        const auto found=repeated.tables.find(std::string_view(name));ASSERT_NE(found,repeated.tables.end());
        const auto& actual=found->second;EXPECT_EQ(actual.name,table.name);EXPECT_EQ(actual.create_sql,table.create_sql);
        EXPECT_EQ(actual.role,table.role);EXPECT_EQ(actual.local_id,table.local_id);EXPECT_EQ(actual.columns,table.columns);
        EXPECT_EQ(actual.audit_programs,table.audit_programs);EXPECT_EQ(actual.alternate_audit_programs,table.alternate_audit_programs);
        EXPECT_EQ(actual.other_programs,table.other_programs);EXPECT_EQ(actual.index_programs,table.index_programs);
        EXPECT_EQ(actual.shadow_columns,table.shadow_columns);
    }
    ASSERT_EQ(repeated.derived.size(),expected.derived.size());
    for(size_t i=0;i<expected.derived.size();++i) {
        const auto& actual=repeated.derived[i];const auto& reference=expected.derived[i];
        EXPECT_EQ(actual.model,reference.model);EXPECT_EQ(actual.property,reference.property);EXPECT_EQ(actual.table,reference.table);
        EXPECT_EQ(actual.create_sql,reference.create_sql);EXPECT_EQ(actual.kind,reference.kind);EXPECT_EQ(actual.programs,reference.programs);
    }
}
TEST(OrdinarySchemaPreimage, CompleteProjectionGenerationRefusesInsufficientAccountWithoutRefund) {
    const auto catalog=declaration();actual_storage_debits measured(8*1024*1024);
    {storage_scope scope(&measured,&measured,actual_storage_debits::debit);(void)describe_ordinary_projection(catalog,{});}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.used,1u);
    actual_storage_debits insufficient(measured.used-1);std::exception_ptr primary;
    {storage_scope scope(&insufficient,&insufficient,actual_storage_debits::debit);
     try{(void)describe_ordinary_projection(catalog,{});}catch(...){primary=std::current_exception();}}
    EXPECT_TRUE(insufficient.failed);EXPECT_EQ(primary,insufficient.actual_refusal);EXPECT_LE(insufficient.used,insufficient.maximum);
    const auto used=insufficient.used,calls=insufficient.calls;
    const auto ordinary=describe_ordinary_projection(catalog,{});EXPECT_FALSE(ordinary.tables.empty());
    EXPECT_EQ(insufficient.used,used);EXPECT_EQ(insufficient.calls,calls);
}
TEST(OrdinarySchemaPreimage, SynchronousCallbackViewBorrowsCaptureWithoutAllocationOrExceptionReplacement) {
    using view=ordinary_initialization_function_ref<int(int)>;
    size_t copies=0,calls=0;const auto primary=std::make_exception_ptr(std::runtime_error("actual callback primary"));
    struct actual_callable {
        size_t& copies;size_t& calls;const std::exception_ptr& primary;
        actual_callable(size_t& c,size_t& n,const std::exception_ptr& p):copies(c),calls(n),primary(p){}
        actual_callable(const actual_callable& other):copies(other.copies),calls(other.calls),primary(other.primary){++copies;}
        int operator()(int value)const{++calls;if(value<0)std::rethrow_exception(primary);return value+7;}
    } actual(copies,calls,primary);
    static_assert(!std::is_constructible_v<view,actual_callable&&>);
    actual_storage_debits zero(0);std::exception_ptr observed;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);const view borrowed(actual);const view copied(borrowed);
     EXPECT_TRUE(borrowed);EXPECT_EQ(copied(5),12);try{(void)borrowed(-1);}catch(...){observed=std::current_exception();}}
    EXPECT_EQ(observed,primary);EXPECT_EQ(copies,0u);EXPECT_EQ(calls,2u);EXPECT_EQ(zero.calls,0u);EXPECT_FALSE(zero.failed);
}

TEST(OrdinarySchemaPreimage, ActualAccountedStreamGrowthRetainsExactFirstDebitErrorAndPublishesNoPartialProgram) {
    const ordinary_initialization_text payload(32768,'z');
    const auto generate=[&](){ordinary_program_storage_stream<ordinary_initialization_text> out;
        out<<'('<<payload<<')';return out.str();};
    ordinary_initialization_text published="unchanged";actual_storage_debits zero(0);std::exception_ptr observed;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     try{published=generate();}catch(...){observed=std::current_exception();}}
    EXPECT_TRUE(zero.failed);EXPECT_GT(zero.calls,0u);EXPECT_EQ(zero.used,0u);
    EXPECT_EQ(observed,zero.actual_refusal);EXPECT_EQ(published,"unchanged");
}
TEST(OrdinarySchemaPreimage, ActualAccountedLongStreamGrowthExactFitPreservesOriginalStandardBytes) {
    const ordinary_initialization_text payload(32768,'z');
    std::ostringstream original;original<<'('<<std::string_view(payload)<<')';const auto expected=original.str();
    const auto generate=[&](){ordinary_program_storage_stream<ordinary_initialization_text> out;
        out<<'('<<payload<<')';return out.str();};
    actual_storage_debits measured(1024*1024);ordinary_initialization_text first;
    {storage_scope scope(&measured,&measured,actual_storage_debits::debit);first=generate();}
    ASSERT_FALSE(measured.failed);ASSERT_GT(measured.calls,0u);ASSERT_GT(measured.used,0u);
    EXPECT_EQ(std::string_view(first),std::string_view(expected));
    actual_storage_debits exact(measured.used);ordinary_initialization_text repeated;
    {storage_scope scope(&exact,&exact,actual_storage_debits::debit);repeated=generate();}
    EXPECT_FALSE(exact.failed);EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);
    EXPECT_EQ(std::string_view(repeated),std::string_view(expected));
}

// Pure real library generator controls. No Native registration, profile,
// factory finish or runtime qualification is inferred from these DATA checks.
#include "../../Sources/LatticeCore/src/observer_delivery_meter.hpp"
namespace {
using startup_meter_text=ordinary_initialization_text;
using startup_meter_tables=observer_delivery_meter_vector<startup_meter_text,observer_delivery_meter_table_storage<startup_meter_text>>;
startup_meter_tables actual_startup_meter_input() {
    startup_meter_tables tables;
    tables.push_back({"z_quoted\"table",{{"id"},{"payload'field"}}});
    tables.push_back({"AuditLog",{{"id"},{"changedFields"}}});
    tables.push_back({"A_model",{{"id"},{"globalId"},{"value"}}});
    return tables;
}
}
TEST(OrdinarySchemaPreimage, MeterSharedGeneratorPreservesEveryNormalProgramByteAndOrdinal) {
    const std::vector<observer_delivery_meter_table> normal{{"z_quoted\"table",{{"id"},{"payload'field"}}},
        {"AuditLog",{{"id"},{"changedFields"}}},{"A_model",{{"id"},{"globalId"},{"value"}}}};
    const auto expected=observer_delivery_meter::programs(normal);
    actual_storage_debits account(4*1024*1024);
    observer_delivery_meter_vector<startup_meter_text,observer_delivery_meter_program_storage<startup_meter_text>> actual;
    {storage_scope scope(&account,&account,actual_storage_debits::debit);
     actual=observer_delivery_meter::programs_storage_<startup_meter_text>(actual_startup_meter_input());}
    ASSERT_EQ(actual.size(),expected.size());ASSERT_EQ(actual.size(),9u);EXPECT_GT(account.used,0u);
    for(size_t i=0;i<actual.size();++i) {
        EXPECT_EQ(std::string_view(actual[i].name),std::string_view(expected[i].name));
        EXPECT_EQ(std::string_view(actual[i].table),std::string_view(expected[i].table));
        EXPECT_EQ(std::string_view(actual[i].sql),std::string_view(expected[i].sql));
    }
}
TEST(OrdinarySchemaPreimage, MeterSharedGeneratorExactActualDebitReplaysCompleteOutput) {
    actual_storage_debits measured(4*1024*1024);
    observer_delivery_meter_vector<startup_meter_text,observer_delivery_meter_program_storage<startup_meter_text>> first;
    {storage_scope scope(&measured,&measured,actual_storage_debits::debit);
     first=observer_delivery_meter::programs_storage_<startup_meter_text>(actual_startup_meter_input());}
    ASSERT_GT(measured.used,0u);actual_storage_debits exact(measured.used);
    observer_delivery_meter_vector<startup_meter_text,observer_delivery_meter_program_storage<startup_meter_text>> repeated;
    {storage_scope scope(&exact,&exact,actual_storage_debits::debit);
     repeated=observer_delivery_meter::programs_storage_<startup_meter_text>(actual_startup_meter_input());}
    ASSERT_EQ(repeated.size(),first.size());EXPECT_EQ(exact.used,measured.used);EXPECT_EQ(exact.calls,measured.calls);EXPECT_FALSE(exact.failed);
    for(size_t i=0;i<first.size();++i){EXPECT_EQ(repeated[i].name,first[i].name);EXPECT_EQ(repeated[i].table,first[i].table);EXPECT_EQ(repeated[i].sql,first[i].sql);}
}
TEST(OrdinarySchemaPreimage, MeterSharedGeneratorRetainsFirstActualDebitFailureWithoutPartialReturn) {
    auto input=actual_startup_meter_input();actual_storage_debits zero(0);std::exception_ptr actual;
    observer_delivery_meter_vector<startup_meter_text,observer_delivery_meter_program_storage<startup_meter_text>> published;
    {storage_scope scope(&zero,&zero,actual_storage_debits::debit);
     try{published=observer_delivery_meter::programs_storage_<startup_meter_text>(input);}catch(...){actual=std::current_exception();}}
    ASSERT_TRUE(actual);EXPECT_EQ(actual,zero.actual_refusal);EXPECT_TRUE(zero.failed);EXPECT_TRUE(published.empty());EXPECT_EQ(zero.calls,1u);EXPECT_EQ(zero.used,0u);
}

// Real SQLite schema primitives for the mandatory final meter family. These
// controls do not issue a Host/Open/registry receipt or prove whole activation.
#include "../../Sources/LatticeCore/src/observer_delivery_schema.hpp"
#include <cstdio>
#include <cstdlib>
namespace lattice::detail {
struct ordinary_final_recipe_test_access {
    static ordinary_schema_metadata complete(const ordinary_schema_projection& projection,
        const ordinary_preimage_query& query,const ordinary_metadata_limits& limits) {
        return ordinary_initialization_schema_validation::complete_(projection,query,limits);
    }
};
}
namespace {
ordinary_schema_preimage inspect_final_complete_image(PreimageDb& db,const ordinary_schema_projection& projection,TypedRead& read) {
    const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};db.execute("BEGIN");
    try {
        const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
        auto prefix=ordinary_preimage_recipe_test_access::required_present(projection,query,limits);
        const auto complete=ordinary_final_recipe_test_access::complete(projection,query,limits);
        if(prefix.schema_cookie!=complete.schema_cookie||prefix.temp_schema_cookie!=complete.temp_schema_cookie)
            throw std::runtime_error("actual final primitive changed retained cookies");
        db.execute("ROLLBACK");return prefix;
    }catch(...) {
        const auto primary=std::current_exception();
        try{db.execute("ROLLBACK");}catch(...){std::fputs("actual-final-primitive-rollback-debt\n",stderr);std::_Exit(96);}
        std::rethrow_exception(primary);
    }
}
ordinary_schema_projection actual_final_meter_projection() {
    auto projection=describe_ordinary_projection(declaration(),{});
    ordinary_projection_table audit;audit.name="AuditLog";audit.create_sql=ordinary_preimage_recipe_test_access::audit_table();
    audit.local_id=false;audit.role=ordinary_projection_role::link;
    audit.index_programs={ordinary_preimage_recipe_test_access::audit_pending_index(),ordinary_preimage_recipe_test_access::audit_lookup_index()};
    projection.tables.emplace("AuditLog",std::move(audit));
    const auto programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(
        observer_delivery_schema::tables_storage_<ordinary_initialization_text>(describe_ordinary_projection(declaration(),{})));
    for(const auto& program:programs)projection.tables.at(program.table).other_programs.push_back(program.sql);
    ordinary_projection_table ledger;ledger.name="_lattice_observer_delivery_meter_v1";
    ledger.create_sql.assign(observer_delivery_meter::ledger_ddl());ledger.local_id=false;ledger.role=ordinary_projection_role::link;
    projection.tables.emplace(ledger.name,std::move(ledger));return projection;
}
std::string actual_fixture_text(std::string_view value){return std::string(value.data(),value.size());}
void seed_final_meter_base(PreimageDb& db,const ordinary_schema_projection& projection) {
    for(const auto& [name,table]:projection.tables) {
        if(name==observer_delivery_meter::ledger_name())continue;
        db.execute(actual_fixture_text(table.create_sql));
        for(const auto& program:table.audit_programs)db.execute(actual_fixture_text(program));
        for(const auto& program:table.index_programs)db.execute(actual_fixture_text(program));
    }
}
void seed_final_meter_guards(PreimageDb& db,const ordinary_schema_projection& projection) {
    for(const auto& [name,table]:projection.tables)for(const auto& program:table.other_programs)
        db.execute(actual_fixture_text(program));
}
}
TEST(OrdinarySchemaPreimage, FinalMeterRequiredPresentNeverAcceptsAbsentLedgerOrMissingGuardBundle) {
    PreimageDb db;const auto projection=actual_final_meter_projection();TypedRead read{db};seed_final_meter_base(db,projection);
    EXPECT_THROW((void)inspect_final_complete_image(db,projection,read),ordinary_schema_validation_error);
    db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));
    EXPECT_THROW((void)inspect_final_complete_image(db,projection,read),ordinary_schema_validation_error);
    seed_final_meter_guards(db,projection);
    const auto complete=inspect_final_complete_image(db,projection,read);
    EXPECT_TRUE(complete.absent_tables.empty());EXPECT_EQ(complete.plain_shapes.size(),projection.tables.size());
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_schema WHERE type='trigger' AND name GLOB '_lattice_observer_delivery_before_v1_*'"),"6");
}
TEST(OrdinarySchemaPreimage, FinalMeterStrictImageRefusesOneMissingOrSameNameReplacedRealGuardWithoutRepair) {
    for(bool replace:{false,true}) {
        PreimageDb db;const auto projection=actual_final_meter_projection();TypedRead read{db};
        seed_final_meter_base(db,projection);db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));seed_final_meter_guards(db,projection);
        ASSERT_NO_THROW((void)inspect_final_complete_image(db,projection,read));
        const auto programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(
            observer_delivery_schema::tables_storage_<ordinary_initialization_text>(describe_ordinary_projection(declaration(),{})));
        ASSERT_FALSE(programs.empty());const auto& selected=programs.front();
        db.execute("DROP TRIGGER "+actual_fixture_text(selected.name));
        if(replace)db.execute("CREATE TRIGGER "+actual_fixture_text(selected.name)+" BEFORE INSERT ON "+actual_fixture_text(selected.table)+" BEGIN SELECT 1; END");
        const auto before=metadata_scalar(db,"SELECT group_concat(name||':'||coalesce(sql,''),'|') FROM (SELECT name,sql FROM main.sqlite_schema ORDER BY name)");
        const auto changes=sqlite3_total_changes64(db.value);
        EXPECT_THROW((void)inspect_final_complete_image(db,projection,read),ordinary_schema_validation_error);
        EXPECT_EQ(sqlite3_total_changes64(db.value),changes);
        EXPECT_EQ(metadata_scalar(db,"SELECT group_concat(name||':'||coalesce(sql,''),'|') FROM (SELECT name,sql FROM main.sqlite_schema ORDER BY name)"),before);
    }
}
TEST(OrdinarySchemaPreimage, FinalMeterStrictImageRefusesActualTemporaryLedgerShadowWithoutMainMutation) {
    PreimageDb db;const auto projection=actual_final_meter_projection();TypedRead read{db};
    seed_final_meter_base(db,projection);db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));seed_final_meter_guards(db,projection);
    db.execute("INSERT INTO main._lattice_observer_delivery_meter_v1 VALUES(1,zeroblob(32),7,11)");
    ASSERT_NO_THROW((void)inspect_final_complete_image(db,projection,read));
    db.execute("CREATE TEMP TABLE _lattice_observer_delivery_meter_v1(id INTEGER)");
    const auto changes=sqlite3_total_changes64(db.value);
    EXPECT_THROW((void)inspect_final_complete_image(db,projection,read),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(events AS TEXT)||':'||CAST(bytes AS TEXT)||':'||CAST(length(token) AS TEXT) FROM main._lattice_observer_delivery_meter_v1"),"7:11:32");
}


// Real cold SQLite Vec connections. These primitive controls consume the SAME
// production statement predicate, not an installed Host/Open/registry oracle.
#include <sqlite-vec.h>
#include <lattice/derived_program.hpp>
namespace lattice::detail {
struct ordinary_vector_connect_recipe_test_access {
    static ordinary_schema_metadata complete(const ordinary_schema_projection& projection,
        const ordinary_preimage_query& query,const ordinary_metadata_limits& limits) {
        return ordinary_initialization_schema_validation::complete_(projection,query,limits);
    }
    static bool read(const ordinary_preimage_statement& statement,const char* table,const char* column,const char* schema)noexcept {
        return statement.vector_connect_read_(table,column,schema);
    }
};
}
namespace {
void install_actual_vec(PreimageDb& db) {
    char* error=nullptr;const auto rc=sqlite3_vec_init(db.value,&error,nullptr);
    if(error)sqlite3_free(error);
    if(rc!=SQLITE_OK)throw std::runtime_error("actual Vec registration failed");
}
ordinary_schema_projection actual_vector_connect_projection() {
    auto model=declaration().models.at("PreimageRow");
    property_descriptor vector{};vector.name="embedding";vector.type=column_type::blob;
    vector.kind=property_kind::primitive;vector.is_vector=true;model.properties.push_back(vector);
    recovery_owner_schema catalog;if(!catalog.admit_model(model))throw std::runtime_error("actual vector declaration refused");
    // The actual normal projection emits the genuine lazy vector family.
    return describe_ordinary_projection(catalog,{});
}
void seed_actual_vector_image(PreimageDb& db,const ordinary_schema_projection& projection,bool ivf) {
    seed(db,projection);
    const auto& module=projection.derived.at(0);
    db.execute(vec0_create_table_program(actual_fixture_text(module.table),4,ivf?2:0,ivf?1:0));
}
void actual_cold_backup(PreimageDb& source,PreimageDb& destination) {
    auto* backup=sqlite3_backup_init(destination.value,"main",source.value,"main");
    if(!backup)throw std::runtime_error("actual cold backup allocation failed");
    const int stepped=sqlite3_backup_step(backup,-1),ended=sqlite3_backup_finish(backup);
    if(stepped!=SQLITE_DONE||ended!=SQLITE_OK)throw std::runtime_error("actual cold backup failed");
}
struct ActualVectorInfoGate {
    const ordinary_preimage_statement* current=nullptr;
    std::size_t connect_entries=0,accepted_connect_reads=0,denied_connect_reads=0,info_entries=0;
    bool negative_checks=true;
    static int authorize(void* context,int action,const char* first,const char* second,const char* schema,const char*)noexcept {
        auto& gate=*static_cast<ActualVectorInfoGate*>(context);
        if(action!=SQLITE_READ||!first)return SQLITE_OK;
        const std::string_view target(first);
        if(!target.ends_with("_info"))return SQLITE_OK;
        try {
            if(!gate.current)return SQLITE_DENY;
            using kind=ordinary_preimage_statement::kind;
            if(gate.current->kind_id()==kind::module_vector_info)return SQLITE_OK;
            const bool accepted=ordinary_vector_connect_recipe_test_access::read(*gate.current,first,second,schema);
            if(accepted)++gate.accepted_connect_reads;else ++gate.denied_connect_reads;
            return accepted?SQLITE_OK:SQLITE_DENY;
        }catch(...){return SQLITE_DENY;}
    }
    std::vector<database::row_t> query(PreimageDb& db,TypedRead& reader,
        const std::shared_ptr<const ordinary_preimage_statement>& statement) {
        current=statement.get();
        using kind=ordinary_preimage_statement::kind;
        if(statement->kind_id()==kind::module_vector_columns) {
            ++connect_entries;const auto target=statement->parameter_text(0);
            const auto info=std::string(target)+"_info";
            negative_checks=negative_checks&&
                !ordinary_vector_connect_recipe_test_access::read(*statement,"OtherVector_info","value","main")&&
                !ordinary_vector_connect_recipe_test_access::read(*statement,info.c_str(),"unowned","main")&&
                !ordinary_vector_connect_recipe_test_access::read(*statement,info.c_str(),"value","temp")&&
                !ordinary_vector_connect_recipe_test_access::read(*statement,info.c_str(),"value",nullptr);
        }else if(statement->kind_id()==kind::module_vector_info)++info_entries;
        if(sqlite3_set_authorizer(db.value,authorize,this)!=SQLITE_OK)throw std::runtime_error("actual vector gate install failed");
        try {
            auto result=reader(statement);
            if(sqlite3_set_authorizer(db.value,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("actual vector gate ending failed");
            current=nullptr;return result;
        }catch(...) {
            const auto primary=std::current_exception();current=nullptr;
            if(sqlite3_set_authorizer(db.value,nullptr,nullptr)!=SQLITE_OK){std::fputs("actual-vector-gate-ending-debt\n",stderr);std::_Exit(97);}
            std::rethrow_exception(primary);
        }
    }
};
ordinary_schema_metadata actual_vector_complete(PreimageDb& db,const ordinary_schema_projection& projection,ActualVectorInfoGate& gate) {
    TypedRead reader{db};const ordinary_metadata_limits limits{256,8192,2*1024*1024,256*1024};db.execute("BEGIN");
    try {
        const ordinary_preimage_query query=[&](const auto& statement){return gate.query(db,reader,statement);};
        auto result=ordinary_vector_connect_recipe_test_access::complete(projection,query,limits);
        db.execute("ROLLBACK");return result;
    }catch(...) {
        const auto primary=std::current_exception();
        try{db.execute("ROLLBACK");}catch(...){std::fputs("actual-vector-complete-rollback-debt\n",stderr);std::_Exit(98);}
        std::rethrow_exception(primary);
    }
}
}
TEST(OrdinarySchemaPreimage, FinalVectorColdConnectReadsOnlyItsActuallyPreprovedInfoColumns) {
    for(const bool ivf:{false,true}) {
        PreimageDb original,cold;install_actual_vec(original);install_actual_vec(cold);
        const auto projection=actual_vector_connect_projection();ASSERT_EQ(projection.derived.size(),1u);
        seed_actual_vector_image(original,projection,ivf);actual_cold_backup(original,cold);
        ActualVectorInfoGate gate;const auto proof=actual_vector_complete(cold,projection,gate);
        ASSERT_EQ(proof.derived.size(),1u);EXPECT_TRUE(proof.derived.front().present);
        EXPECT_EQ(gate.connect_entries,1u);EXPECT_GE(gate.accepted_connect_reads,2u);
        EXPECT_EQ(gate.denied_connect_reads,0u);EXPECT_EQ(gate.info_entries,1u);EXPECT_TRUE(gate.negative_checks);
        // A successful complete proof includes the real current hidden command
        // column. No earlier query connected/cached the destination module.
        EXPECT_EQ(metadata_scalar(cold,"SELECT CAST(COUNT(*) AS TEXT) FROM pragma_table_xinfo('"+
            actual_fixture_text(projection.derived.front().table)+"','main') WHERE hidden=1"),"3");
    }
}
TEST(OrdinarySchemaPreimage, FinalVectorUnknownInfoProgramRefusesBeforeAnyConnectRecipe) {
    PreimageDb original,cold;install_actual_vec(original);install_actual_vec(cold);
    const auto projection=actual_vector_connect_projection();seed_actual_vector_image(original,projection,false);
    const auto info=actual_fixture_text(projection.derived.front().table)+"_info";
    original.execute("CREATE TRIGGER UnexpectedVectorInfo AFTER INSERT ON "+info+" BEGIN SELECT 1; END");
    actual_cold_backup(original,cold);const auto before=sqlite3_total_changes64(cold.value);
    ActualVectorInfoGate gate;
    EXPECT_THROW((void)actual_vector_complete(cold,projection,gate),ordinary_schema_validation_error);
    EXPECT_EQ(gate.connect_entries,0u);EXPECT_EQ(gate.accepted_connect_reads,0u);EXPECT_EQ(gate.info_entries,0u);
    EXPECT_EQ(sqlite3_total_changes64(cold.value),before);
    EXPECT_EQ(metadata_scalar(cold,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema WHERE name='UnexpectedVectorInfo'"),"1");
}


// Exact shared production seed/CAS bytes and actual SQLite mutation/endings.
// These do not manufacture a pending Open, profile, continuation or registry.
namespace {
std::string actual_meter_integer_cookie(PreimageDb& db) {
    constexpr std::string_view sql="PRAGMA main.schema_version";
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual meter cookie prepare failed");
    if(sqlite3_step(statement.value)!=SQLITE_ROW||sqlite3_column_count(statement.value)!=1||
       sqlite3_column_type(statement.value,0)!=SQLITE_INTEGER)
        throw std::runtime_error("actual meter cookie must be one INTEGER row");
    const auto cookie=sqlite3_column_int64(statement.value,0);
    if(cookie<0||cookie>INT32_MAX)throw std::runtime_error("actual meter cookie outside SQLite domain");
    if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual meter cookie trailing row or error");
    statement.finish();return std::to_string(cookie);
}
std::string actual_meter_complete_schema_text(PreimageDb& db) {
    // This actual small fixture already declares a1MiB whole preimage cap.
    // Keep every schema SQL byte; no truncation, digest or type coercion.
    constexpr std::size_t fixture_bytes=1024*1024;
    constexpr std::string_view sql="SELECT group_concat(name||':'||coalesce(sql,''),'|') FROM (SELECT name,sql FROM main.sqlite_schema ORDER BY name)";
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual complete meter schema prepare failed");
    if(sqlite3_step(statement.value)!=SQLITE_ROW||sqlite3_column_count(statement.value)!=1||
       sqlite3_column_type(statement.value,0)!=SQLITE_TEXT)
        throw std::runtime_error("actual complete meter schema must be one TEXT row");
    const int count=sqlite3_column_bytes(statement.value,0);const auto* text=sqlite3_column_text(statement.value,0);
    if(count<0||std::size_t(count)>fixture_bytes||!text)throw std::runtime_error("actual complete meter schema exceeds fixture bound or is missing");
    std::string result(reinterpret_cast<const char*>(text),std::size_t(count));
    if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual complete meter schema trailing row or error");
    statement.finish();return result;
}
void actual_meter_marker_cas(PreimageDb& db,std::string_view current,std::string_view key,std::string_view previous) {
    Statement statement;const auto sql=observer_delivery_meter::initialization_marker_cas_();const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual shared marker CAS prepare failed");
    int index=1;
    for(const auto parameter:{current,key,previous})
        if(sqlite3_bind_text(statement.value,index++,parameter.data(),int(parameter.size()),SQLITE_STATIC)!=SQLITE_OK)
            throw std::runtime_error("actual shared marker CAS bind failed");
    if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual shared marker CAS step failed");
    statement.finish();
}
}
TEST(OrdinarySchemaPreimage, FinalMeterActualSeedNeverOverwritesExistingTypedRowOrCounters) {
    PreimageDb db;db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));
    db.execute(actual_fixture_text(observer_delivery_meter::initialization_seed_()));
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(id AS TEXT)||':'||typeof(token)||':'||CAST(length(token) AS TEXT)||':'||CAST(events AS TEXT)||':'||CAST(bytes AS TEXT) FROM main._lattice_observer_delivery_meter_v1"),"1:blob:32:0:0");
    db.execute("UPDATE main._lattice_observer_delivery_meter_v1 SET events=13,bytes=89 WHERE id=1");
    const auto before=sqlite3_total_changes64(db.value);
    EXPECT_THROW(db.execute(actual_fixture_text(observer_delivery_meter::initialization_seed_())),std::runtime_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),before);
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT)||':'||CAST(events AS TEXT)||':'||CAST(bytes AS TEXT)||':'||CAST(length(token) AS TEXT) FROM main._lattice_observer_delivery_meter_v1"),"1:13:89:32");
}
TEST(OrdinarySchemaPreimage, FinalMeterMarkerCasOnlyUpdatesItsExactBoundSurvivingRow) {
    for(const bool second_key:{false,true}) {
        PreimageDb db;db.execute(ordinary_preimage_recipe_test_access::metadata_table());
        const std::string key=second_key?"schema_fingerprint:bbbbbbbbbbbbbbbb":"schema_fingerprint:aaaaaaaaaaaaaaaa";
        const auto previous=actual_meter_integer_cookie(db);
        db.execute("INSERT INTO main._lattice_meta(key,value) VALUES('"+key+"','"+previous+"')");
        db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));
        const auto current=actual_meter_integer_cookie(db);ASSERT_NE(current,previous);
        actual_meter_marker_cas(db,current,key,previous);EXPECT_EQ(sqlite3_changes64(db.value),1);
        EXPECT_EQ(metadata_scalar(db,"SELECT value FROM main._lattice_meta WHERE key='"+key+"'"),current);
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main._lattice_meta WHERE key LIKE 'schema_fingerprint:%'"),"1");
        actual_meter_marker_cas(db,previous,key,previous);EXPECT_EQ(sqlite3_changes64(db.value),0);
        EXPECT_EQ(metadata_scalar(db,"SELECT value FROM main._lattice_meta WHERE key='"+key+"'"),current);
        actual_meter_marker_cas(db,previous,"schema_fingerprint:cccccccccccccccc",current);EXPECT_EQ(sqlite3_changes64(db.value),0);
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main._lattice_meta WHERE key LIKE 'schema_fingerprint:%'"),"1");
    }
}
TEST(OrdinarySchemaPreimage, FinalMeterActualGuardBatchRequiresFullPostimageAndRollsBackToOriginalSchema) {
    for(const bool break_guard:{false,true}) {
        PreimageDb db;const auto projection=actual_final_meter_projection();seed_final_meter_base(db,projection);
        const auto original=actual_meter_complete_schema_text(db);
        db.execute("BEGIN IMMEDIATE");
        try {
            db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));
            db.execute(actual_fixture_text(observer_delivery_meter::initialization_seed_()));seed_final_meter_guards(db,projection);
            if(break_guard) {
                const auto programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(
                    observer_delivery_schema::tables_storage_<ordinary_initialization_text>(describe_ordinary_projection(declaration(),{})));
                if(programs.empty())throw std::runtime_error("actual compiled guard corpus missing");
                db.execute("DROP TRIGGER "+actual_fixture_text(programs.front().name));
            }
            TypedRead read{db};const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
            const ordinary_metadata_limits limits{128,4096,1024*1024,256*1024};
            if(break_guard)EXPECT_THROW((void)ordinary_preimage_recipe_test_access::required_present(projection,query,limits),ordinary_schema_validation_error);
            else {
                const auto actual=ordinary_preimage_recipe_test_access::required_present(projection,query,limits);
                EXPECT_TRUE(actual.absent_tables.empty());
                EXPECT_NO_THROW((void)ordinary_final_recipe_test_access::complete(projection,query,limits));
                EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main._lattice_observer_delivery_meter_v1"),"1");
            }
            db.execute("ROLLBACK");
        }catch(...) {
            const auto primary=std::current_exception();
            try{db.execute("ROLLBACK");}catch(...){std::fputs("actual-meter-batch-rollback-debt\n",stderr);std::_Exit(99);}
            std::rethrow_exception(primary);
        }
        EXPECT_EQ(sqlite3_get_autocommit(db.value),1);
        EXPECT_EQ(actual_meter_complete_schema_text(db),original);
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema WHERE name='_lattice_observer_delivery_meter_v1'"),"0");
    }
}


// Actual SQLite controls of the SAME compiled model/meter variant and exact
// object verifier used by the pending Plan. No Host/Open/WRITE-original or
// registry/account proof is constructed by these schema primitives.
namespace {
struct ExistingModelMeterFixture {
    ordinary_schema_projection model,normal,guarded;
    ExistingModelMeterFixture():model(describe_ordinary_projection(declaration(),{})),normal(model) {
        const auto add=[&](const char* name,const char* sql,std::initializer_list<const char*> indexes) {
            ordinary_projection_table table;table.name=name;table.create_sql=sql;
            table.role=ordinary_projection_role::link;table.local_id=false;
            for(const auto* index:indexes)table.index_programs.emplace_back(index);
            if(!normal.tables.emplace(name,std::move(table)).second)throw std::runtime_error("actual existing-model fixture identity collision");
        };
        add("AuditLog",ordinary_preimage_recipe_test_access::audit_table(),
            {ordinary_preimage_recipe_test_access::audit_pending_index(),ordinary_preimage_recipe_test_access::audit_lookup_index()});
        add("_lattice_meta",ordinary_preimage_recipe_test_access::metadata_table(),{});
        add("_SyncControl",ordinary_preimage_recipe_test_access::sync_control_table(),{});
        guarded=observer_delivery_schema::model_meter_variant_(model,normal);
    }
    auto programs()const {
        return observer_delivery_meter::programs_storage_<ordinary_initialization_text>(
            observer_delivery_schema::tables_storage_<ordinary_initialization_text>(model));
    }
    void seed_guarded(PreimageDb& db)const {
        seed(db,normal);db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));
        db.execute(actual_fixture_text(observer_delivery_meter::initialization_seed_()));
        for(const auto& program:programs())db.execute(actual_fixture_text(program.sql));
    }
};
ordinary_schema_preimage existing_model_image(PreimageDb& db,const ordinary_schema_projection& projection) {
    db.execute("BEGIN IMMEDIATE");
    try {
        TypedRead read{db};const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
        auto actual=ordinary_preimage_recipe_test_access::required_present(projection,query,{256,8192,1024*1024,256*1024});
        db.execute("ROLLBACK");return actual;
    }catch(...) {
        const auto primary=std::current_exception();
        try{db.execute("ROLLBACK");}catch(...){std::fputs("actual-existing-model-image-rollback-debt\n",stderr);std::_Exit(100);}
        std::rethrow_exception(primary);
    }
}
void require_existing_model_image(PreimageDb& db,const ExistingModelMeterFixture& fixture) {
    const auto actual=existing_model_image(db,fixture.guarded);
    observer_delivery_schema::require_model_meter_objects_(fixture.model,actual);
}
}
TEST(OrdinarySchemaPreimage, ExistingMeterWholeCanonicalVariantPreservesOriginalAbsentAndCurrentScopes) {
    PreimageDb db;const ExistingModelMeterFixture fixture;
    {
        auto original=fixture.normal;TypedRead read{db};const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
        db.execute("BEGIN IMMEDIATE");
        const auto empty=ordinary_preimage_recipe_test_access::construction_initial(original,query,{256,8192,1024*1024,256*1024});
        EXPECT_EQ(empty.absent_tables.size(),fixture.normal.tables.size());db.execute("ROLLBACK");
    }
    seed(db,fixture.normal);EXPECT_NO_THROW((void)existing_model_image(db,fixture.normal));
    EXPECT_THROW((void)existing_model_image(db,fixture.guarded),ordinary_schema_validation_error);
    db.execute(actual_fixture_text(observer_delivery_meter::ledger_ddl()));db.execute(actual_fixture_text(observer_delivery_meter::initialization_seed_()));
    for(const auto& program:fixture.programs())db.execute(actual_fixture_text(program.sql));
    const auto changes=sqlite3_total_changes64(db.value);const auto schema=actual_meter_complete_schema_text(db);
    ASSERT_NO_THROW(require_existing_model_image(db,fixture));
    const auto actual=existing_model_image(db,fixture.guarded);EXPECT_TRUE(actual.absent_tables.empty());
    EXPECT_EQ(actual.plain_shapes.size(),fixture.guarded.tables.size());
    // The original unguarded private projection still refuses these programs;
    // no generic null-route or prefix/census allowance was broadened.
    EXPECT_THROW((void)existing_model_image(db,fixture.normal),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);EXPECT_EQ(actual_meter_complete_schema_text(db),schema);
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(events AS TEXT)||':'||CAST(bytes AS TEXT)||':'||CAST(length(token) AS TEXT) FROM main._lattice_observer_delivery_meter_v1"),"0:0:32");
}
TEST(OrdinarySchemaPreimage, ExistingMeterPartialReplacedForeignAndTempFamiliesRefuseWithoutMutation) {
    for(const int defect:{0,1,2,3,4,5}) {
        PreimageDb db;const ExistingModelMeterFixture fixture;fixture.seed_guarded(db);
        const auto programs=fixture.programs();ASSERT_FALSE(programs.empty());const auto& selected=programs.front();
        if(defect==0)db.execute("DROP TRIGGER "+actual_fixture_text(selected.name));
        if(defect==1) {
            db.execute("DROP TRIGGER "+actual_fixture_text(selected.name));
            db.execute("CREATE TRIGGER "+actual_fixture_text(selected.name)+" BEFORE INSERT ON "+actual_fixture_text(selected.table)+" BEGIN SELECT 1; END");
        }
        if(defect==2)db.execute("CREATE TRIGGER ForeignMeterProgram BEFORE INSERT ON PreimageRow BEGIN SELECT 1; END");
        if(defect==3)db.execute("ALTER TABLE _lattice_observer_delivery_meter_v1 RENAME TO _LATTICE_OBSERVER_DELIVERY_METER_V1_alias");
        if(defect==4)db.execute("CREATE TEMP TABLE _lattice_observer_delivery_meter_v1(id INTEGER)");
        if(defect==5)db.execute("CREATE TEMP TRIGGER ForeignTempMeterProgram BEFORE INSERT ON main.PreimageRow BEGIN SELECT 1; END");
        const auto changes=sqlite3_total_changes64(db.value);const auto schema=actual_meter_complete_schema_text(db);
        EXPECT_THROW(require_existing_model_image(db,fixture),ordinary_schema_validation_error)<<defect;
        EXPECT_EQ(sqlite3_total_changes64(db.value),changes)<<defect;EXPECT_EQ(actual_meter_complete_schema_text(db),schema)<<defect;
        EXPECT_EQ(sqlite3_get_autocommit(db.value),1)<<defect;
    }
}
TEST(OrdinarySchemaPreimage, ExistingMeterEquivalentReformattedGuardCannotBecomeCanonicalBytes) {
    PreimageDb db;const ExistingModelMeterFixture fixture;fixture.seed_guarded(db);
    const auto programs=fixture.programs();ASSERT_FALSE(programs.empty());const auto& selected=programs.front();
    auto sql=actual_fixture_text(selected.sql);const auto where=sql.find(" BEFORE ");ASSERT_NE(where,std::string::npos);sql.insert(where,"  ");
    db.execute("DROP TRIGGER "+actual_fixture_text(selected.name));db.execute(sql);
    const auto changes=sqlite3_total_changes64(db.value);const auto schema=actual_meter_complete_schema_text(db);
    // The longstanding token-level physical proof is preserved. The actual
    // selected meter verifier adds its existing byte-exact program contract.
    const auto actual=existing_model_image(db,fixture.guarded);
    EXPECT_THROW(observer_delivery_schema::require_model_meter_objects_(fixture.model,actual),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);EXPECT_EQ(actual_meter_complete_schema_text(db),schema);
}
TEST(OrdinarySchemaPreimage, ExistingMeterOrphanGuardsNeverSelectTheAbsentLegacyFamily) {
    PreimageDb db;const ExistingModelMeterFixture fixture;seed(db,fixture.normal);
    for(const auto& program:fixture.programs())db.execute(actual_fixture_text(program.sql));
    const auto changes=sqlite3_total_changes64(db.value);const auto schema=actual_meter_complete_schema_text(db);
    EXPECT_THROW((void)existing_model_image(db,fixture.normal),ordinary_schema_validation_error);
    EXPECT_THROW(require_existing_model_image(db,fixture),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);EXPECT_EQ(actual_meter_complete_schema_text(db),schema);
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema WHERE name='_lattice_observer_delivery_meter_v1'"),"0");
}


// These controls exercise the classifier called by real stage creation using
// actual SQLite-produced complete facts. They do not manufacture an Open,
// same-episode proof, profile or installed stage success.
TEST(OrdinarySchemaPreimage, ExistingMeterStageClassifierRequiresWholeCompiledAlternativeAndActualObjects) {
    PreimageDb db;const ExistingModelMeterFixture fixture;fixture.seed_guarded(db);
    const auto actual=existing_model_image(db,fixture.guarded);
    using family=observer_delivery_schema::model_stage_family;
    EXPECT_EQ(observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,fixture.guarded,actual),family::complete_meter);
    // The ordinary classifier preserves the old exact-count route. Actual
    // construction subset/identity checks still run in model_stage_from_.
    EXPECT_EQ(observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,fixture.normal,actual),family::ordinary_construction);
    const auto changes=sqlite3_total_changes64(db.value);const auto schema=actual_meter_complete_schema_text(db);
    auto foreign=fixture.guarded;ordinary_projection_table extra;extra.name="UnownedStageTable";extra.create_sql="CREATE TABLE UnownedStageTable(value TEXT)";
    foreign.tables.emplace(extra.name,std::move(extra));
    EXPECT_THROW((void)observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,foreign,actual),ordinary_schema_validation_error);
    auto incomplete=actual;incomplete.absent_tables.emplace_back(observer_delivery_meter::ledger_name());
    EXPECT_THROW((void)observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,fixture.guarded,incomplete),ordinary_schema_validation_error);
    const auto programs=fixture.programs();ASSERT_FALSE(programs.empty());
    db.execute("DROP TRIGGER "+actual_fixture_text(programs.front().name));
    // A genuine actual MAIN inventory from a physical prefix cannot stand in
    // for complete canonical facts, even with the expected projection DATA.
    TypedRead read{db};const ordinary_preimage_query query=[&](const auto& entry){return read(entry);};
    db.execute("BEGIN IMMEDIATE");
    auto partial=ordinary_preimage_recipe_test_access::construction_prefix(fixture.guarded,query,{256,8192,1024*1024,256*1024});
    db.execute("ROLLBACK");
    EXPECT_THROW((void)observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,fixture.guarded,partial),ordinary_schema_validation_error);
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);
    EXPECT_NE(actual_meter_complete_schema_text(db),schema);EXPECT_EQ(sqlite3_get_autocommit(db.value),1);
}
TEST(OrdinarySchemaPreimage, ExistingMeterStageCannotExchangeDeclaredFieldsForAnExtraLedgerCount) {
    PreimageDb db;const ExistingModelMeterFixture fixture;fixture.seed_guarded(db);
    const auto actual=existing_model_image(db,fixture.guarded);const auto changes=sqlite3_total_changes64(db.value);
    const auto schema=actual_meter_complete_schema_text(db);
    for(const int defect:{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14}) {
        auto selected=fixture.guarded;auto& table=selected.tables.at("PreimageRow");
        if(defect==0)selected.models.emplace_back("ForeignStageModel");
        if(defect==1)++selected.logical_bytes;
        if(defect==2)++selected.columns;
        if(defect==3)table.name="ForeignStageName";
        if(defect==4)table.create_sql+=" ";
        if(defect==5)table.role=ordinary_projection_role::link;
        if(defect==6)table.local_id=!table.local_id;
        if(defect==7)table.columns.emplace_back("ForeignStageColumn");
        if(defect==8)table.audit_programs.clear();
        if(defect==9)table.other_programs.clear();
        if(defect==10)table.index_programs.emplace_back("CREATE INDEX ForeignStageIndex ON PreimageRow(globalId)");
        if(defect==11)table.alternate_audit_programs.emplace_back();
        if(defect==12)table.shadow_columns.emplace_back("ForeignStageShadow");
        if(defect==13)selected.derived.push_back({"PreimageRow","foreign","ForeignStageModule","CREATE VIRTUAL TABLE ForeignStageModule USING fts5(value)",ordinary_derived_kind::full_text,{}});
        if(defect==14)selected.tables.at("_lattice_observer_delivery_meter_v1").local_id=true;
        EXPECT_THROW((void)observer_delivery_schema::model_stage_scope_(fixture.model,fixture.normal,selected,actual),ordinary_schema_validation_error)<<defect;
    }
    EXPECT_EQ(sqlite3_total_changes64(db.value),changes);EXPECT_EQ(actual_meter_complete_schema_text(db),schema);
    EXPECT_EQ(sqlite3_get_autocommit(db.value),1);
}


// Genuine SQLite controls for the SAME pure full-catalog output traversal and
// metadata producer used by fresh startup. They do not issue an installation
// original, pending Open/profile, provider receipt, finish or Host grant.
namespace {
recovery_owner_schema actual_fresh_complete_declarations() {
    model_schema target;target.table_name="FreshTarget";
    property_descriptor key{};key.name="value";key.type=column_type::text;key.is_unique=true;key.no_history=true;
    target.properties.push_back(key);
    union_descriptor choice;choice.union_table_name="_FreshChoice";
    union_case word,number;word.case_name="word";number.case_name="number";
    union_case_value word_value{},number_value{};word_value.type=column_type::text;number_value.type=column_type::integer;
    word.values.push_back(word_value);number.values.push_back(number_value);choice.cases={word,number};
    property_descriptor selection{};selection.name="choice";selection.type=column_type::text;
    selection.kind=property_kind::union_type;selection.is_union=true;selection.union_desc=choice;
    model_schema core;core.table_name="FreshCorePeer";core.properties.push_back(selection);
    property_descriptor area{};area.name="area";area.type=column_type::real;area.is_geo_bounds=true;core.properties.push_back(area);
    model_schema swift;swift.table_name="FreshSwiftOwner";swift.properties.push_back(selection);
    property_descriptor title{};title.name="title";title.type=column_type::text;title.is_indexed=true;swift.properties.push_back(title);
    property_descriptor group{};group.name="group";group.type=column_type::text;group.kind=property_kind::link;
    group.target_table=target.table_name;swift.properties.push_back(group);
    property_descriptor items{};items.name="items";items.type=column_type::text;items.kind=property_kind::virtual_list;swift.properties.push_back(items);
    recovery_owner_schema catalog;
    for(const auto* model:{&target,&core,&swift})if(!catalog.admit_model(*model))throw std::runtime_error("actual fresh closed declaration refused");
    catalog.swift_models.insert(swift.table_name);catalog.swift_constraints[swift.table_name]={recovery_constraint{{"group"},false}};
    return catalog;
}
struct ActualFreshFixtureWrite {
    PreimageDb& db;bool active=true;
    explicit ActualFreshFixtureWrite(PreimageDb& actual):db(actual){db.execute("BEGIN IMMEDIATE");}
    ~ActualFreshFixtureWrite(){if(active&&sqlite3_exec(db.value,"ROLLBACK",nullptr,nullptr,nullptr)!=SQLITE_OK)
        ADD_FAILURE()<<"actual fresh control rollback retained Native debt";}
    void commit(){db.execute("COMMIT");active=false;}
    void rollback(){db.execute("ROLLBACK");active=false;}
};
bool actual_fresh_main_has_schema(PreimageDb& db) {
    constexpr std::string_view sql=ordinary_fresh_catalog_presence_program_;
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size()||sqlite3_column_count(statement.value)!=1)
        throw std::runtime_error("actual full MAIN presence prepare failed");
    const int rc=sqlite3_step(statement.value);bool present=false;
    if(rc==SQLITE_ROW){
        const int bytes=sqlite3_column_bytes(statement.value,0);
        if(sqlite3_column_type(statement.value,0)!=SQLITE_TEXT||bytes<=0||bytes>256||!sqlite3_column_text(statement.value,0))
            throw std::runtime_error("actual full MAIN name is not its bounded TEXT result");
        present=true;if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual presence trailing row or error");
    }else if(rc!=SQLITE_DONE)throw std::runtime_error("actual presence step failed");
    statement.finish();return present;
}
std::size_t actual_fresh_emit_compiled_family(PreimageDb& db,const ordinary_schema_projection& projection) {
    std::size_t count=0;
    ordinary_fresh_catalog_programs_(projection,[&](std::string_view sql){db.execute(std::string(sql));++count;},
        [&](std::string_view name,std::string_view column){
            db.execute(ordinary_preimage_recipe_test_access::shadow_column(std::string(name),std::string(column)));++count;
        });
    return count;
}
ordinary_schema_preimage actual_fresh_complete_image(PreimageDb& db,const ordinary_schema_projection& projection) {
    TypedRead read{db};ordinary_preimage_query query=[&](const auto& statement){return read(statement);};
    return ordinary_preimage_recipe_test_access::required_present(projection,query,{256,8192,1024*1024,256*1024});
}
void actual_fresh_store_parent_metadata(PreimageDb& db,std::string_view key,std::string_view value) {
    constexpr std::string_view sql="INSERT INTO main._lattice_meta(key,value) VALUES(?,?)";
    Statement statement;const char* tail=nullptr;
    if(sqlite3_prepare_v2(db.value,sql.data(),int(sql.size()),&statement.value,&tail)!=SQLITE_OK||
       !statement.value||tail!=sql.data()+sql.size())throw std::runtime_error("actual fresh parent metadata prepare failed");
    int index=1;for(const auto text:{key,value})
        if(sqlite3_bind_text(statement.value,index++,text.data(),int(text.size()),SQLITE_STATIC)!=SQLITE_OK)
            throw std::runtime_error("actual fresh parent metadata bind failed");
    if(sqlite3_step(statement.value)!=SQLITE_DONE)throw std::runtime_error("actual fresh parent metadata step failed");
    statement.finish();
}
}
TEST(OrdinarySchemaPreimage, FreshWholeCapturedProgramsCreateCanonicalCoreSwiftLinkUnionShadowAndDerivedImage) {
    PreimageDb db;const auto catalog=actual_fresh_complete_declarations();const auto projection=describe_ordinary_projection(catalog,{});
    ASSERT_EQ(catalog.models.size(),3u);ASSERT_EQ(projection.models.size(),3u);ASSERT_EQ(projection.tables.size(),6u);
    ASSERT_EQ(projection.derived.size(),1u);ASSERT_EQ(projection.tables.at("FreshSwiftOwner").shadow_columns.size(),1u);
    EXPECT_FALSE(actual_fresh_main_has_schema(db));ActualFreshFixtureWrite write(db);
    EXPECT_GT(actual_fresh_emit_compiled_family(db,projection),projection.tables.size());
    const auto proof=actual_fresh_complete_image(db,projection);EXPECT_TRUE(proof.absent_tables.empty());
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM pragma_table_xinfo('FreshSwiftOwner','main') WHERE name='group__link_gid'"),"1");
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema WHERE type='table' AND name IN ('FreshTarget','FreshCorePeer','FreshSwiftOwner','_FreshChoice','_FreshSwiftOwner_FreshTarget_group','_FreshSwiftOwner_items')"),"6");
    write.commit();EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_TRUE(actual_fresh_main_has_schema(db));
}
TEST(OrdinarySchemaPreimage, FreshInternalMetadataRetainsCompleteSharedUnionAndSwiftRelationshipParents) {
    PreimageDb db;const auto catalog=actual_fresh_complete_declarations();const auto projection=describe_ordinary_projection(catalog,{});
    const auto parents=ordinary_fresh_catalog_internal_metadata_(catalog,projection);ASSERT_EQ(parents.size(),3u);
    EXPECT_EQ(std::string_view(parents.at("internal_table:_FreshChoice")),"FreshCorePeer:choice;FreshSwiftOwner:choice");
    EXPECT_EQ(std::string_view(parents.at("internal_table:_FreshSwiftOwner_FreshTarget_group")),"FreshSwiftOwner:group");
    EXPECT_EQ(std::string_view(parents.at("internal_table:_FreshSwiftOwner_items")),"FreshSwiftOwner:items");
    ActualFreshFixtureWrite write(db);db.execute(ordinary_preimage_recipe_test_access::metadata_table());
    for(const auto& [key,value]:parents)actual_fresh_store_parent_metadata(db,key,value);
    EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main._lattice_meta"),"3");
    EXPECT_EQ(metadata_scalar(db,"SELECT value FROM main._lattice_meta WHERE key='internal_table:_FreshChoice'"),"FreshCorePeer:choice;FreshSwiftOwner:choice");
    EXPECT_EQ(metadata_scalar(db,"SELECT value FROM main._lattice_meta WHERE key='internal_table:_FreshSwiftOwner_FreshTarget_group'"),"FreshSwiftOwner:group");
    write.commit();EXPECT_EQ(sqlite3_get_autocommit(db.value),1);
}
TEST(OrdinarySchemaPreimage, FreshPresenceObservesOldRowsAndSQLiteOwnedSchemaWithoutAnyReseedEffects) {
    for(const bool sqlite_owned_only:{false,true}) {
        PreimageDb db;
        if(sqlite_owned_only){db.execute("CREATE TABLE RetiredFreshOriginal(id INTEGER PRIMARY KEY AUTOINCREMENT)");db.execute("DROP TABLE RetiredFreshOriginal");}
        else{db.execute("CREATE TABLE PriorFreshData(value TEXT NOT NULL)");db.execute("INSERT INTO PriorFreshData VALUES('keep')");}
        const auto schema=actual_meter_complete_schema_text(db);const auto changes=sqlite3_total_changes64(db.value);
        ActualFreshFixtureWrite write(db);
        EXPECT_TRUE(actual_fresh_main_has_schema(db));
        EXPECT_THROW(([&]{if(actual_fresh_main_has_schema(db))throw std::runtime_error("observed schema cannot become a reseed grant");})(),std::runtime_error);
        write.rollback();EXPECT_EQ(sqlite3_get_autocommit(db.value),1);
        EXPECT_EQ(sqlite3_total_changes64(db.value),changes);EXPECT_EQ(actual_meter_complete_schema_text(db),schema);
        if(!sqlite_owned_only)EXPECT_EQ(metadata_scalar(db,"SELECT value FROM PriorFreshData"),"keep");
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema WHERE name='FreshSwiftOwner'"),"0");
    }
}
TEST(OrdinarySchemaPreimage, FreshWholeFamilyMissingOrReplacedProgramRefusesAndActualRollbackRestoresEmptyMain) {
    for(const bool replaced:{false,true}) {
        PreimageDb db;const auto catalog=actual_fresh_complete_declarations();const auto projection=describe_ordinary_projection(catalog,{});
        const auto original_cookie=actual_meter_integer_cookie(db);EXPECT_FALSE(actual_fresh_main_has_schema(db));
        ActualFreshFixtureWrite write(db);actual_fresh_emit_compiled_family(db,projection);
        db.execute("DROP TRIGGER AuditFreshTargetInsert");
        if(replaced)db.execute("CREATE TRIGGER AuditFreshTargetInsert AFTER INSERT ON FreshTarget BEGIN SELECT 1; END");
        EXPECT_THROW((void)actual_fresh_complete_image(db,projection),ordinary_schema_validation_error);
        write.rollback();EXPECT_EQ(sqlite3_get_autocommit(db.value),1);EXPECT_FALSE(actual_fresh_main_has_schema(db));
        EXPECT_EQ(actual_meter_integer_cookie(db),original_cookie);
        EXPECT_EQ(metadata_scalar(db,"SELECT CAST(COUNT(*) AS TEXT) FROM main.sqlite_schema"),"0");
    }
}

namespace lattice::detail {
namespace {
// Resolve inside the real operator namespace, where the failed product native
// view/literal calls occur. No using/import or owning conversion hides them.
template<class Left,class Right>
void expect_actual_owned_text_comparison(const Left& left,const Right& right,bool equal) {
    EXPECT_EQ(left==right,equal);EXPECT_EQ(right==left,equal);
    EXPECT_EQ(left!=right,!equal);EXPECT_EQ(right!=left,!equal);
}
}
}
TEST(OrdinarySchemaPreimage, ActualNativeAndBothOwnedTextAliasesCompareFullValuesWithoutAllocatorRequests) {
    using lattice::detail::core_model_registry_text;
    using lattice::detail::ordinary_initialization_text;
    using lattice::detail::ordinary_inventory_text;
    using lattice::detail::expect_actual_owned_text_comparison;
    // Every owning input is built before the selected actual debit scope.
    const std::string native="CaseSensitiveMetadata_"+std::string(96,'x');
    std::string changed=native;changed.front()='c';const auto shorter=native.substr(0,native.size()-1);
    const core_model_registry_text core(native.data(),native.size()),core_changed(changed.data(),changed.size());
    const ordinary_initialization_text init(native.data(),native.size()),init_shorter(shorter.data(),shorter.size());
    const std::string_view native_view(native),changed_view(changed),short_view(shorter);
    const ordinary_inventory_text inventory(native_view);
    const std::string binary("Audit\0Tail",10);const std::string_view binary_view(binary.data(),binary.size());
    const core_model_registry_text core_binary(binary.data(),binary.size());
    const ordinary_initialization_text init_binary(binary.data(),binary.size());
    const ordinary_inventory_text inventory_binary(binary_view);
    const core_model_registry_text core_empty;const ordinary_initialization_text init_empty;
    actual_storage_debits account(0);
    {
        storage_scope selected(&account,&account,actual_storage_debits::debit);
        expect_actual_owned_text_comparison(std::string_view("UPDATE"),"UPDATE",true);
        expect_actual_owned_text_comparison(std::string_view("UPDATE"),"update",false);
        expect_actual_owned_text_comparison(std::string_view("UPDATE"),"UPDAT",false);
        expect_actual_owned_text_comparison(std::string_view("UPDATE"),"DELETE",false);
        expect_actual_owned_text_comparison(native,native_view,true);
        expect_actual_owned_text_comparison(native,changed_view,false);
        expect_actual_owned_text_comparison(native,short_view,false);
        expect_actual_owned_text_comparison(core,native,true);
        expect_actual_owned_text_comparison(core,native_view,true);
        expect_actual_owned_text_comparison(core,changed_view,false);
        expect_actual_owned_text_comparison(core,short_view,false);
        expect_actual_owned_text_comparison(core,core_changed,false);
        expect_actual_owned_text_comparison(init,native,true);
        expect_actual_owned_text_comparison(init,native_view,true);
        expect_actual_owned_text_comparison(init,changed_view,false);
        expect_actual_owned_text_comparison(init,short_view,false);
        expect_actual_owned_text_comparison(init,init_shorter,false);
        expect_actual_owned_text_comparison(core,init,true);
        expect_actual_owned_text_comparison(core_changed,init,false);
        expect_actual_owned_text_comparison(core,init_shorter,false);
        expect_actual_owned_text_comparison(inventory,core,true);
        expect_actual_owned_text_comparison(inventory,init,true);
        expect_actual_owned_text_comparison(inventory,core_changed,false);
        expect_actual_owned_text_comparison(inventory,init_shorter,false);
        expect_actual_owned_text_comparison(core_binary,init_binary,true);
        expect_actual_owned_text_comparison(core_binary,binary_view,true);
        expect_actual_owned_text_comparison(init_binary,binary,true);
        expect_actual_owned_text_comparison(inventory_binary,core_binary,true);
        expect_actual_owned_text_comparison(inventory_binary,init_binary,true);
        expect_actual_owned_text_comparison(core_binary,"Audit\0Tail",false); // C string has length5.
        expect_actual_owned_text_comparison(init_binary,"Audit",false);
        expect_actual_owned_text_comparison(core_empty,init_empty,true);
        expect_actual_owned_text_comparison(core_empty,std::string_view{},true);
        expect_actual_owned_text_comparison(init_empty,"",true);
        EXPECT_EQ(account.calls,0u);EXPECT_EQ(account.used,0u);EXPECT_FALSE(account.failed);
    }
    EXPECT_EQ(account.calls,0u);EXPECT_EQ(account.used,0u);EXPECT_FALSE(account.failed);
}
