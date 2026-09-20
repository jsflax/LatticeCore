#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/derived_profile.hpp"
#include <limits>
#include <map>

namespace {
using namespace lattice;
using namespace lattice::detail;
using Rows = std::vector<database::row_t>;
using Image = std::map<std::string,Rows>;
using Blob = std::vector<uint8_t>;
const std::string model = "DerivedProfileFixtureRow"; // dynamic, never registered globally
const std::string fts = "_DerivedProfileFixtureRow_content_fts";
const std::string vec = "_DerivedProfileFixtureRow_embedding_vec";

configuration profile_config(const std::string& path) {
    configuration config(path);config.audit_retention_seconds=0;config.busy_timeout_ms=100;return config;
}
model_schema profile_schema() {
    model_schema schema;schema.table_name=model;
    property_descriptor content;content.name="content";content.type=column_type::text;
    content.nullable=true;content.no_history=true;content.is_full_text=true;
    property_descriptor embedding;embedding.name="embedding";embedding.type=column_type::blob;
    embedding.nullable=true;embedding.is_vector=true;
    schema.properties={content,embedding};return schema;
}
std::vector<derived_spec> profile_specs() {
    return {{model,"content",derived_kind::fts5_porter_v1,0},
            {model,"embedding",derived_kind::vec0_flat_f32_v1,2}};
}
derived_descriptor descriptor() {return describe_derived({profile_schema()},profile_specs());}
void create(lattice_db& owner) {
    owner.create_model_table_public(profile_schema());
    owner.ensure_fts5_table(model,"content");owner.ensure_vec0_table(model,"embedding",2);
}
void insert(lattice_db& owner,int64_t id,const column_value_t& text,const column_value_t& vector) {
    owner.db().execute("INSERT INTO "+model+"(id,globalId,content,embedding) VALUES(?,?,?,?)",
        {id,lattice::uuid_t::generate().to_string(),text,vector});
}
int64_t number(database& db,const std::string& sql) {
    return std::get<int64_t>(db.query(sql).at(0).at("n"));
}
// Every validation call owns an actual read savepoint, or nests within the
// explicit owner transaction of the fault case. No validation borrows a series
// of unrelated autocommit snapshots. The wrappers only serve fixture custody.
template<class F> auto in_view(database& db,F&& work) -> decltype(work()) {
    db.execute("SAVEPOINT derived_profile_read_view");
    try {auto result=work();db.execute("RELEASE derived_profile_read_view");return result;}
    catch(...) {db.execute("ROLLBACK TO derived_profile_read_view");db.execute("RELEASE derived_profile_read_view");throw;}
}
derived_metadata metadata(database& db,const derived_descriptor& expected,const derived_limits& budget={}) {
    return in_view(db,[&]{return validate_derived_schema(db,expected,budget);});
}
derived_metadata metadata(database& db,const derived_query& query,const derived_descriptor& expected,const derived_limits& budget={}) {
    return in_view(db,[&]{return validate_derived_schema(query,expected,budget);});
}
derived_value_usage initial(database& db,const derived_descriptor& expected,const derived_limits& budget={}) {
    return in_view(db,[&]{return validate_initial_derived_values(db,expected,budget);});
}
derived_value_usage initial(database& db,const derived_query& query,const derived_descriptor& expected,const derived_limits& budget={}) {
    return in_view(db,[&]{return validate_initial_derived_values(query,expected,budget);});
}
Image image(database& db) {
    Image out;
    out["model"]=db.query("SELECT id,globalId,CAST(content AS BLOB) AS content,embedding FROM "+model+" ORDER BY id");
    out["audit"]=db.query("SELECT * FROM AuditLog WHERE tableName=? ORDER BY id",{model});
    out["schema"]=db.query("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY rowid");
    for (const auto& table : {fts+"_data",fts+"_idx",fts+"_docsize",fts+"_config",vec+"_info",vec+"_chunks",vec+"_rowids",vec+"_vector_chunks00"})
        out[table]=db.query("SELECT * FROM \""+table+"\" ORDER BY 1");
    return out;
}
void error_is(const std::function<void()>& operation,const std::string& expected) {
    try {operation();FAIL()<<"expected derived refusal: "<<expected;}
    catch(const derived_profile_error& error) {EXPECT_EQ(std::string(error.what()),expected);}
}
void generated_case(bool file) {
    TempDB disk("derived_profile");lattice_db owner(profile_config(file?disk.str():":memory:"));create(owner);
    insert(owner,-1,std::string("violet\0orchid",13),pack_floats({1,2}));
    insert(owner,0,std::string{},Blob{});insert(owner,7,nullptr,nullptr);
    const auto expected=descriptor();const auto before=image(owner.db());
    const auto changes=number(owner.db(),"SELECT total_changes() AS n");
    owner.begin_transaction();
    const auto facts=metadata(owner.db(),expected);
    EXPECT_GT(facts.inspected_rows,0u);EXPECT_GT(facts.copied_bytes,0u);EXPECT_EQ(facts.objects.size(),17u);
    const auto values=initial(owner.db(),expected);
    EXPECT_EQ(values.rows,3u);EXPECT_EQ(values.bytes,21u);
    EXPECT_EQ(number(owner.db(),"SELECT total_changes() AS n"),changes);
    EXPECT_EQ(image(owner.db()),before); // actual shadows before any MATCH/KNN
    owner.rollback();EXPECT_EQ(image(owner.db()),before);
}
} // namespace

TEST(DerivedProfile, MemoryActualGeneratedMetadataAndValuesAreReadOnly) {generated_case(false);}
TEST(DerivedProfile, FileActualGeneratedMetadataAndValuesAreReadOnly) {generated_case(true);}

TEST(DerivedProfile, DescriptorBindsExactSharedProgramsAndAllDerivedFields) {
    const auto expected=descriptor();ASSERT_EQ(expected.fields().size(),2u);
    EXPECT_TRUE(expected.fields()[0].nullable);EXPECT_TRUE(expected.fields()[0].no_history);
    EXPECT_EQ(expected.fields()[0].create_sql,fts5_porter_program(model,"content").create_table);
    EXPECT_EQ(expected.fields()[1].create_sql,vec0_create_table_program(vec,2,0,0));
    EXPECT_EQ(expected.fields()[1].triggers.size(),4u);
    auto specs=profile_specs();specs.pop_back();EXPECT_THROW(describe_derived({profile_schema()},specs),derived_profile_error);
    specs=profile_specs();specs[1]=specs[0];EXPECT_THROW(describe_derived({profile_schema()},specs),derived_profile_error);
    for(size_t dims:{size_t(0),size_t(8193),std::numeric_limits<size_t>::max()}) {
        specs=profile_specs();specs[1].dimensions=dims;EXPECT_THROW(describe_derived({profile_schema()},specs),derived_profile_error);
    }
    specs=profile_specs();specs[0].dimensions=1;EXPECT_THROW(describe_derived({profile_schema()},specs),derived_profile_error);
    for(int variant=0;variant<6;++variant) {
        auto schema=profile_schema();auto& p=schema.properties[0];
        if(variant==0)p.is_geo_bounds=true;if(variant==1)p.is_union=true;
        if(variant==2)p.kind=property_kind::virtual_list;if(variant==3)p.column_name="renamed";
        if(variant==4)p.is_vector=true;if(variant==5)p.type=column_type::integer;
        EXPECT_THROW(describe_derived({schema},profile_specs()),derived_profile_error)<<variant;
    }
    auto budget=derived_limits{};budget.properties=1;EXPECT_THROW(describe_derived({profile_schema()},profile_specs(),budget),derived_profile_error);
    budget=derived_limits{};budget.sql_bytes=1;EXPECT_THROW(describe_derived({profile_schema()},profile_specs(),budget),derived_profile_error);
}

TEST(DerivedProfile, OwnedFinalValuesPreserveNullEmptyAndFiniteFloat32WithAtomicBudgets) {
    const auto expected=descriptor();derived_value_usage usage;
    database::row_t row{{"content",std::string("a\0b",3)},{"embedding",pack_floats({1,2})}};
    derived_limits budget;budget.initial_rows=3;budget.value_bytes=8;budget.total_value_bytes=11;
    EXPECT_NO_THROW(validate_derived_values(expected,model,row,usage,budget));EXPECT_EQ(usage.rows,1u);EXPECT_EQ(usage.bytes,11u);
    row={{"content",std::string{}},{"embedding",Blob{}}};
    EXPECT_NO_THROW(validate_derived_values(expected,model,row,usage,budget));EXPECT_EQ(usage.rows,2u);EXPECT_EQ(usage.bytes,11u);
    row={{"content",nullptr},{"embedding",nullptr}};
    EXPECT_NO_THROW(validate_derived_values(expected,model,row,usage,budget));EXPECT_EQ(usage.rows,3u);EXPECT_EQ(usage.bytes,11u);
    EXPECT_THROW(validate_derived_values(expected,model,row,usage,budget),derived_profile_error);
    for(int variant=0;variant<7;++variant) {
        derived_value_usage current;row={{"content",std::string("ok")},{"embedding",pack_floats({1,2})}};
        if(variant==0)row.erase("embedding");if(variant==1)row["content"]=int64_t(3);
        if(variant==2)row["embedding"]=std::string("12345678");if(variant==3)row["embedding"]=Blob{1,2,3,4};
        if(variant==4)row["embedding"]=pack_floats({std::numeric_limits<float>::infinity(),0});
        if(variant==5)row["embedding"]=pack_floats({std::numeric_limits<float>::quiet_NaN(),0});
        if(variant==6)row["content"]=std::string(9,'x');
        EXPECT_THROW(validate_derived_values(expected,model,row,current,budget),derived_profile_error)<<variant;
        EXPECT_EQ(current.rows,0u);EXPECT_EQ(current.bytes,0u);
    }
    auto nonnull=profile_schema();nonnull.properties[0].nullable=false;
    const auto strict=describe_derived({nonnull},profile_specs());derived_value_usage empty;
    row={{"content",nullptr},{"embedding",Blob{}}};EXPECT_THROW(validate_derived_values(strict,model,row,empty),derived_profile_error);
}

TEST(DerivedProfile, RequiredTriggerMismatchAndMissingProgramRefuseWithoutRepair) {
    lattice_db owner(profile_config(":memory:"));create(owner);insert(owner,1,std::string("one"),pack_floats({1,2}));
    const auto expected=descriptor();const auto before=image(owner.db());
    for(bool missing:{true,false}) {
        owner.begin_transaction();const auto& t=expected.fields()[1].triggers[3];owner.db().execute("DROP TRIGGER "+t.name);
        if(!missing)owner.db().execute("CREATE TRIGGER "+t.name+" AFTER UPDATE ON "+model+" BEGIN SELECT 1; END");
        const auto corrupt=image(owner.db());
        EXPECT_THROW(metadata(owner.db(),expected),derived_profile_error);EXPECT_EQ(image(owner.db()),corrupt);
        owner.rollback();EXPECT_EQ(image(owner.db()),before);EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}

TEST(DerivedProfile, ExtraFamilyObjectsAndShadowIndexesRefuseWhileUnrelatedObjectsRemainPermitted) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    owner.db().execute("CREATE TABLE _unrelated_derived_notes(value TEXT)");
    EXPECT_NO_THROW(metadata(owner.db(),expected));
    for(const auto& sql:std::vector<std::string>{
        "CREATE TRIGGER "+vec+"_unexpected AFTER INSERT ON "+model+" BEGIN SELECT 1; END",
        "CREATE INDEX extra_chunk_index ON "+vec+"_chunks(size)",
        "CREATE TABLE "+fts+"_extra(x)"}) {
        owner.begin_transaction();owner.db().execute(sql);const auto before=image(owner.db());
        EXPECT_THROW(metadata(owner.db(),expected),derived_profile_error);EXPECT_EQ(image(owner.db()),before);
        owner.rollback();EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}

TEST(DerivedProfile, ExplicitDimensionAndNullabilityMismatchRefuseActualStore) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto before=image(owner.db());
    auto specs=profile_specs();specs[1].dimensions=3;const auto wrong=describe_derived({profile_schema()},specs);
    error_is([&]{metadata(owner.db(),wrong);},"derived module/options/dimensions mismatch");
    auto schema=profile_schema();schema.properties[0].nullable=false;const auto nonnull=describe_derived({schema},profile_specs());
    error_is([&]{metadata(owner.db(),nonnull);},"derived ordinary field metadata refused");
    EXPECT_EQ(image(owner.db()),before);
}

TEST(DerivedProfile, ExactMetadataCapsAndWholeInventoryAdmissionBeforeScopedQueries) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    const auto facts=metadata(owner.db(),expected);
    derived_limits exact;exact.metadata_rows=facts.inspected_rows;exact.metadata_bytes=facts.copied_bytes;
    exact.sql_bytes=std::min(exact.sql_bytes,exact.metadata_bytes);
    EXPECT_NO_THROW(metadata(owner.db(),expected,exact));
    auto short_bytes=exact;--short_bytes.metadata_bytes;short_bytes.sql_bytes=std::min(short_bytes.sql_bytes,short_bytes.metadata_bytes);
    EXPECT_THROW(metadata(owner.db(),expected,short_bytes),derived_profile_error);
    auto short_rows=exact;--short_rows.metadata_rows;
    EXPECT_THROW(metadata(owner.db(),expected,short_rows),derived_profile_error);
    derived_limits tiny;tiny.metadata_rows=1;bool scoped=false;int existence_probes=0;
    derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params) {
        if(sql.find("pragma_table_")!=std::string::npos)scoped=true;
        if(sql.find("SELECT 1 AS present FROM main.sqlite_schema")!=std::string::npos)++existence_probes;
        return owner.db().query(sql,params);
    };
    error_is([&]{metadata(owner.db(),query,expected,tiny);},"derived metadata row cap exceeded");
    EXPECT_FALSE(scoped);EXPECT_EQ(existence_probes,1);
}

TEST(DerivedProfile, OversizedUnrelatedSchemaIsRefusedBeforeCopyOrScopedLookup) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    derived_limits cap;
    ASSERT_NO_THROW(metadata(owner.db(),expected,cap));
    owner.db().execute("CREATE VIEW _oversized_derived_inventory AS SELECT '"+std::string(cap.sql_bytes+1024,'x')+"' AS value");
    bool refused_projection=false,scoped=false;derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params) {
        if(sql.find("pragma_table_")!=std::string::npos)scoped=true;
        auto rows=owner.db().query(sql,params);
        for(const auto& row:rows)if(row.count("_ok")&&std::get<int64_t>(row.at("_ok"))==0) {
            refused_projection=true;for(const auto& [key,value]:row)if(key!="_ok")EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(value))<<key;
        }
        return rows;
    };
    error_is([&]{metadata(owner.db(),query,expected,cap);},"derived metadata type or copy cap refused");
    EXPECT_TRUE(refused_projection);EXPECT_FALSE(scoped);
    EXPECT_EQ(number(owner.db(),"SELECT count(*) AS n FROM sqlite_schema WHERE name='_oversized_derived_inventory'"),1);
}

TEST(DerivedProfile, ActualVersionMetadataTypeAndLengthPreflightAvoidsCorruptValueCopy) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    for(int variant=0;variant<4;++variant) {
        owner.begin_transaction();
        if(variant==0)owner.db().execute("UPDATE "+vec+"_info SET value=? WHERE key='CREATE_VERSION_PATCH'",{std::string(64*1024,'x')});
        if(variant==1)owner.db().execute("UPDATE "+vec+"_info SET value=? WHERE key='CREATE_VERSION_PATCH'",{Blob(64*1024,1)});
        if(variant==2)owner.db().execute("DELETE FROM "+vec+"_info WHERE key='CREATE_VERSION_MAJOR'");
        if(variant==3)owner.db().execute("UPDATE "+vec+"_info SET value=-1 WHERE key='CREATE_VERSION_PATCH'");
        bool saw_guarded=false;size_t largest_dynamic=0;
        derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params) {
            auto rows=owner.db().query(sql,params);
            if(sql.find("FROM main.\""+vec+"_info\"")!=std::string::npos)for(const auto& row:rows) {
                saw_guarded=true;for(const auto& [key,value]:row) {
                    if(const auto* blob=std::get_if<Blob>(&value))largest_dynamic=std::max(largest_dynamic,blob->size());
                    if(const auto* text=std::get_if<std::string>(&value))largest_dynamic=std::max(largest_dynamic,text->size());
                }
            }
            return rows;
        };
        EXPECT_THROW(metadata(owner.db(),query,expected),derived_profile_error)<<variant;
        EXPECT_TRUE(saw_guarded);EXPECT_LE(largest_dynamic,64u);
        owner.rollback();EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}

TEST(DerivedProfile, WrongFtsFormatOrExtraVectorVersionMetadataRefusesWithoutEffects) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    for(const auto& sql:std::vector<std::string>{"UPDATE "+fts+"_config SET v=99 WHERE k='version'",
        "INSERT INTO "+vec+"_info(key,value) VALUES('UNSUPPORTED_EXTRA',1)"}) {
        owner.begin_transaction();owner.db().execute(sql);const auto before=image(owner.db());
        EXPECT_THROW(metadata(owner.db(),expected),derived_profile_error);EXPECT_EQ(image(owner.db()),before);
        owner.rollback();EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}

TEST(DerivedProfile, InitialRowAndValueBudgetsRefuseBeforeOversizedPayloadCopy) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    insert(owner,-9,std::string("iris"),pack_floats({1,2}));insert(owner,0,nullptr,Blob{});
    derived_limits rows;rows.initial_rows=2;
    EXPECT_EQ(initial(owner.db(),expected,rows).rows,2u);
    rows.initial_rows=1;error_is([&]{initial(owner.db(),expected,rows);},"derived initial row cap exceeded");
    owner.db().execute("UPDATE "+model+" SET content=? WHERE id=-9",{std::string(4096,'x')});
    derived_limits cap;cap.value_bytes=16;bool refused_projection=false;
    derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params) {
        auto result=owner.db().query(sql,params);
        if(sql.find("AS ok,typeof(")!=std::string::npos)for(const auto& row:result)if(std::get<int64_t>(row.at("ok"))==0) {
            refused_projection=true;EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(row.at("value")));
        }
        return result;
    };
    error_is([&]{initial(owner.db(),query,expected,cap);},"derived initial value type/copy cap refused");
    EXPECT_TRUE(refused_projection);EXPECT_EQ(number(owner.db(),"SELECT length(content) AS n FROM "+model+" WHERE id=-9"),4096);
}

TEST(DerivedProfile, ReopenMetadataDoesNotTurnLegitimateGrowthIntoImplicitValueRescan) {
    TempDB disk("derived_profile_growth");const auto expected=descriptor();
    {lattice_db owner(profile_config(disk.str()));create(owner);
     for(int i=0;i<4;++i)insert(owner,i,std::string("iris"),pack_floats({float(i),0}));}
    lattice_db reopened(profile_config(disk.str()));derived_limits budget;budget.initial_rows=1;
    int model_reads=0;derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params) {
        if(sql.find("FROM main.\""+model+"\"")!=std::string::npos)++model_reads;
        return reopened.db().query(sql,params);
    };
    EXPECT_NO_THROW(metadata(reopened.db(),query,expected,budget));EXPECT_EQ(model_reads,0);
    EXPECT_THROW(initial(reopened.db(),query,expected,budget),derived_profile_error);EXPECT_GT(model_reads,0);
    EXPECT_EQ(number(reopened.db(),"SELECT count(*) AS n FROM "+model),4);
}

TEST(DerivedProfile, RetainedReadViewKeepsSnapshotAndExistingCancellationHandler) {
    TempDB disk("derived_profile_read_control");lattice_db owner(profile_config(disk.str()));create(owner);
    insert(owner,1,std::string("old"),pack_floats({1,2}));const auto expected=descriptor();
    auto control=std::make_shared<database_read_control>();
    control->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    database reader(disk.str(),database::open_mode::read_only,100,control);reader.execute("BEGIN");
    const auto old=validate_initial_derived_values(reader,expected);ASSERT_EQ(old.rows,1u);ASSERT_EQ(old.bytes,11u);
    insert(owner,2,std::string("new"),pack_floats({3,4}));
    EXPECT_EQ(validate_initial_derived_values(reader,expected).rows,1u);
    reader.execute("ROLLBACK");reader.execute("BEGIN");
    EXPECT_EQ(validate_initial_derived_values(reader,expected).rows,2u);reader.execute("ROLLBACK");
    // Store the stop flag without sqlite3_interrupt: the pre-existing progress
    // callback itself must still stop a later VM after both validator paths.
    control->stop_code.store(1);
    EXPECT_THROW(reader.query("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<100000) SELECT sum(x) FROM n"),db_error);
}

TEST(DerivedProfile, QueryFailuresPropagateAndCannotBecomeEmptySuccessfulAdmission) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    struct cancelled : std::runtime_error {cancelled():std::runtime_error("retained read cancelled") {}};
    int calls=0;derived_query query=[&](const std::string& sql,const std::vector<column_value_t>& params)->Rows {
        if(++calls==3)throw cancelled();return owner.db().query(sql,params);
    };
    EXPECT_THROW(metadata(owner.db(),query,expected),cancelled);EXPECT_EQ(calls,3);
    EXPECT_NO_THROW(metadata(owner.db(),expected));
}

TEST(DerivedProfile, WrongVectorChunkRowidDeclarationAndMissingShadowRefuseBeforeAnyRepair) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    const auto original=image(owner.db());
    for(bool missing:{false,true}) {
        owner.begin_transaction();owner.db().execute("DROP TABLE "+vec+"_vector_chunks00");
        if(!missing)owner.db().execute("CREATE TABLE "+vec+"_vector_chunks00(rowid INTEGER PRIMARY KEY,vectors BLOB NOT NULL)");
        const auto schema=owner.db().query("SELECT name,sql FROM sqlite_schema ORDER BY rowid");
        EXPECT_THROW(metadata(owner.db(),expected),derived_profile_error);
        EXPECT_EQ(owner.db().query("SELECT name,sql FROM sqlite_schema ORDER BY rowid"),schema);
        owner.rollback();EXPECT_EQ(image(owner.db()),original);EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}

TEST(DerivedProfile, MixedCaseExtraFamilyNamesCannotBypassAsciiInsensitiveReservation) {
    lattice_db owner(profile_config(":memory:"));create(owner);const auto expected=descriptor();
    owner.db().execute("CREATE TABLE _unrelated_case_target(value TEXT)");
    const auto original=image(owner.db());
    for(const auto& sql:std::vector<std::string>{
        "CREATE TABLE _derivedprofilefixturerow_CONTENT_FTS_extra(value)",
        "CREATE TRIGGER _DERIVEDPROFILEFIXTUREROW_embedding_VEC_extra AFTER INSERT ON "+model+" BEGIN SELECT 1; END",
        "CREATE INDEX _derivedprofilefixturerow_EMBEDDING_vec_extra ON _unrelated_case_target(value)",
        "CREATE TRIGGER unrelated_case_name AFTER UPDATE ON _DERIVEDPROFILEFIXTUREROW_EMBEDDING_VEC_vector_chunks00 BEGIN SELECT 1; END"}) {
        owner.begin_transaction();owner.db().execute(sql);const auto corrupt=image(owner.db());
        error_is([&]{metadata(owner.db(),expected);},"derived extra family object refused");
        EXPECT_EQ(image(owner.db()),corrupt);owner.rollback();EXPECT_EQ(image(owner.db()),original);
        EXPECT_NO_THROW(metadata(owner.db(),expected));
    }
}
