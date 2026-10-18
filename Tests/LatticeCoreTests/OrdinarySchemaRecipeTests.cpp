#include <gtest/gtest.h>
#include <lattice/ordinary_schema_recipe.hpp>
#include <lattice/db.hpp>
#include <map>

namespace lattice::detail {
// Private pure recipe access only. New ordinary in-memory storage declarations
// below have no target request/current/recovery issuer or fake SQLite UDF.
struct ordinary_schema_recipe_test_access {
    static model_schema physical(const model_schema& value){return ordinary_schema_recipe::swift_physical_model(value);}
    static model_schema physical(const std::string& name,const std::map<std::string,property_descriptor>& value){
        return ordinary_schema_recipe::swift_physical_model(name,value);
    }
    static std::string table(const model_schema& value,std::vector<std::pair<std::string,column_type>>& columns){
        return ordinary_schema_recipe::model_table(value,columns);
    }
    static std::string core_unique(const std::string& table,size_t n,const std::string& column){
        return ordinary_schema_recipe::core_unique_index(table,n,column);
    }
    static std::string unique(const std::string& table,size_t n,const std::vector<std::string>& columns){
        return ordinary_schema_recipe::unique_index(table,n,ordinary_schema_recipe::column_list(columns));
    }
    static std::string indexed(const model_schema& schema,const property_descriptor& prop){return ordinary_schema_recipe::nonunique_index(schema,prop);}
    static std::string insert(){return ordinary_schema_recipe::shadow_insert("Member","_Member_Group_group","group__link_gid","LinkShadow_Insert");}
    static std::string erased(){return ordinary_schema_recipe::shadow_delete("Member","_Member_Group_group","group__link_gid","LinkShadow_Delete");}
    static std::string backfill(){return ordinary_schema_recipe::shadow_backfill("Member","_Member_Group_group","group__link_gid");}
    static std::string link(){return ordinary_schema_recipe::link_table("_Member_Group_group");}
    static std::string virtual_link(){return ordinary_schema_recipe::virtual_link_table("_Member_items");}
    static std::string union_sql(const union_descriptor& desc){return ordinary_schema_recipe::union_table("_Member_choice",desc).sql;}
    static std::string rtree_table(){return ordinary_schema_recipe::rtree_table("_Places_area_rtree");}
    static std::string rtree_insert(){return ordinary_schema_recipe::rtree_insert("Places","area");}
    static std::string rtree_delete(){return ordinary_schema_recipe::rtree_delete("Places","area");}
    static std::string rtree_update(){return ordinary_schema_recipe::rtree_update("Places","area");}
    static std::vector<std::string> geographic_audit(){return ordinary_schema_recipe::geo_list_audit("_Places_regions");}
    static std::string metadata(){return ordinary_schema_recipe::metadata_table();}
    static std::string marker_store(){return ordinary_schema_recipe::metadata_marker_store();}
    static std::string marker_prune(){return ordinary_schema_recipe::metadata_marker_prune();}
    static std::string receiver_store(){return ordinary_schema_recipe::receive_store();}
    static std::string receiver_channel(){return ordinary_schema_recipe::receive_channel();}
};
}
using namespace lattice;
using recipe_access=lattice::detail::ordinary_schema_recipe_test_access;

TEST(OrdinarySchemaRecipeTests, SwiftPhysicalConversionKeepsUnionAndGeoListButNotRelationshipColumns) {
    std::map<std::string,property_descriptor> properties;
    property_descriptor scalar{};scalar.name="text";scalar.type=column_type::text;scalar.no_history=true;properties.emplace(scalar.name,scalar);
    property_descriptor link{};link.name="group";link.kind=property_kind::link;link.target_table="Group";properties.emplace(link.name,link);
    property_descriptor list{};list.name="children";list.kind=property_kind::list;properties.emplace(list.name,list);
    property_descriptor geo=list;geo.name="regions";geo.is_geo_bounds=true;properties.emplace(geo.name,geo);
    property_descriptor owned{};owned.name="choice";owned.kind=property_kind::union_type;owned.type=column_type::integer;owned.nullable=false;properties.emplace(owned.name,owned);
    model_schema original;original.table_name="Member";
    for(const auto& [name,value]:properties)original.properties.push_back(value);
    const auto by_map=recipe_access::physical(original.table_name,properties);
    const auto by_catalog=recipe_access::physical(original);
    ASSERT_EQ(by_map.properties.size(),3u);ASSERT_EQ(by_catalog.properties.size(),by_map.properties.size());
    for(size_t i=0;i<by_map.properties.size();++i)EXPECT_EQ(by_map.properties[i].name,by_catalog.properties[i].name);
    ASSERT_EQ(by_map.properties.front().name,"choice");EXPECT_EQ(by_map.properties.front().type,column_type::text);EXPECT_TRUE(by_map.properties.front().nullable);
    EXPECT_EQ(by_map.properties.back().name,"text");EXPECT_TRUE(by_map.properties.back().no_history);
}

TEST(OrdinarySchemaRecipeTests, TableRecipePreservesIdentityNullableAndExpandedGeoStorage) {
    model_schema schema;schema.table_name="Places";
    property_descriptor required{};required.name="name";required.type=column_type::text;
    property_descriptor optional=required;optional.name="note";optional.nullable=true;
    property_descriptor geo{};geo.name="area";geo.type=column_type::real;geo.is_geo_bounds=true;
    property_descriptor list{};list.name="regions";list.kind=property_kind::list;list.is_geo_bounds=true;
    schema.properties={required,optional,geo,list};std::vector<std::pair<std::string,column_type>> columns;
    const auto sql=recipe_access::table(schema,columns);
    EXPECT_EQ(sql.find("CREATE TABLE IF NOT EXISTS Places(id INTEGER PRIMARY KEY AUTOINCREMENT, globalId TEXT UNIQUE"),0u);
    EXPECT_NE(sql.find("COLLATE NOCASE DEFAULT (lower(hex(randomblob(4)))"),std::string::npos);
    EXPECT_NE(sql.find(", name TEXT NOT NULL, note TEXT, area_minLat REAL, area_maxLat REAL, area_minLon REAL, area_maxLon REAL)"),std::string::npos);
    EXPECT_EQ(sql.find("regions"),std::string::npos);ASSERT_EQ(columns.size(),6u);
    EXPECT_EQ(columns[2].first,"area_minLat");EXPECT_EQ(columns[5].first,"area_maxLon");
}

TEST(OrdinarySchemaRecipeTests, IndexRecipesPreserveOrdinalsColumnOrderAndExistingNames) {
    EXPECT_EQ(recipe_access::core_unique("Member",12,"name"),"CREATE UNIQUE INDEX IF NOT EXISTS unique_Member_12 ON Member(name)");
    EXPECT_EQ(recipe_access::unique("Member",3,{"group__link_gid","name"}),"CREATE UNIQUE INDEX IF NOT EXISTS unique_Member_3 ON Member(group__link_gid, name)");
    model_schema schema;schema.table_name="Member";property_descriptor prop{};prop.name="name";prop.is_indexed=true;
    EXPECT_EQ(recipe_access::indexed(schema,prop),"CREATE INDEX IF NOT EXISTS idx_Member_name ON Member(name)");
}

TEST(OrdinarySchemaRecipeTests, ShadowDeleteKeepsActualOldRHSFenceAndBackfillChoice) {
    EXPECT_EQ(recipe_access::insert(),"CREATE TRIGGER IF NOT EXISTS LinkShadow_Insert AFTER INSERT ON _Member_Group_group BEGIN   UPDATE Member   SET group__link_gid = NEW.rhs   WHERE globalId = NEW.lhs; END");
    EXPECT_EQ(recipe_access::erased(),"CREATE TRIGGER IF NOT EXISTS LinkShadow_Delete AFTER DELETE ON _Member_Group_group BEGIN   UPDATE Member   SET group__link_gid = NULL   WHERE globalId = OLD.lhs     AND group__link_gid = OLD.rhs; END");
    EXPECT_EQ(recipe_access::backfill(),"UPDATE Member SET group__link_gid = (SELECT rhs FROM _Member_Group_group WHERE lhs = Member.globalId LIMIT 1) WHERE group__link_gid IS NULL");
}

TEST(OrdinarySchemaRecipeTests, RelationshipKindsKeepTheirDistinctPhysicalKeys) {
    database db(":memory:");
    db.execute(recipe_access::link());db.execute(recipe_access::virtual_link());
    db.execute("INSERT INTO _Member_Group_group(lhs,rhs) VALUES('member','group')");
    EXPECT_THROW(db.execute("INSERT INTO _Member_Group_group(lhs,rhs) VALUES('member','group')"),db_error);
    db.execute("INSERT INTO _Member_Group_group(lhs,rhs) VALUES('member','other')");
    db.execute("INSERT INTO _Member_items(lhs,rhs,rhs_type) VALUES('member','same','Text')");
    db.execute("INSERT INTO _Member_items(lhs,rhs,rhs_type) VALUES('member','same','Number')");
    EXPECT_THROW(db.execute("INSERT INTO _Member_items(lhs,rhs,rhs_type) VALUES('member','same','Text')"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _Member_items(lhs,rhs,rhs_type) VALUES('member','new',NULL)"),db_error);
    EXPECT_EQ(db.query("SELECT lhs FROM _Member_Group_group",{}).size(),2u);
    EXPECT_EQ(db.query("SELECT lhs FROM _Member_items",{}).size(),2u);
}
TEST(OrdinarySchemaRecipeTests, UnionCasesRequireTheirOwnValuesAndExcludeOtherCaseValues) {
    union_descriptor desc;union_case text,number;text.case_name="text";number.case_name="number";
    union_case_value a,b;a.type=column_type::text;b.type=column_type::integer;
    text.values.push_back(a);number.values.push_back(b);desc.cases={text,number};
    database db(":memory:");db.execute(recipe_access::union_sql(desc));
    db.execute("INSERT INTO _Member_choice(\"case\",text) VALUES('text','value')");
    db.execute("INSERT INTO _Member_choice(\"case\",number) VALUES('number',7)");
    EXPECT_THROW(db.execute("INSERT INTO _Member_choice(\"case\",text,number) VALUES('text','value',7)"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _Member_choice(\"case\") VALUES('text')"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _Member_choice(\"case\",text) VALUES('unknown','value')"),db_error);
    EXPECT_EQ(db.query("SELECT id FROM _Member_choice",{}).size(),2u);
}
TEST(OrdinarySchemaRecipeTests, GeographicUpdateDeletesOldRowBeforeConditionalReplacement) {
    model_schema schema;schema.table_name="Places";property_descriptor area{};
    area.name="area";area.type=column_type::real;area.is_geo_bounds=true;schema.properties={area};
    std::vector<std::pair<std::string,column_type>> columns;database db(":memory:");
    db.execute(recipe_access::table(schema,columns));db.execute(recipe_access::rtree_table());
    db.execute(recipe_access::rtree_insert());db.execute(recipe_access::rtree_update());db.execute(recipe_access::rtree_delete());
    db.execute("INSERT INTO Places(globalId,area_minLat,area_maxLat,area_minLon,area_maxLon) VALUES('place',1,2,3,4)");
    ASSERT_EQ(db.query("SELECT id FROM _Places_area_rtree",{}).size(),1u);
    db.execute("UPDATE Places SET area_minLat=5,area_maxLat=6,area_minLon=7,area_maxLon=8 WHERE globalId='place'");
    auto changed=db.query("SELECT minLat,maxLat FROM _Places_area_rtree",{});ASSERT_EQ(changed.size(),1u);
    EXPECT_DOUBLE_EQ(std::get<double>(changed[0].at("minLat")),5.0);EXPECT_DOUBLE_EQ(std::get<double>(changed[0].at("maxLat")),6.0);
    db.execute("UPDATE Places SET area_minLat=NULL,area_maxLat=NULL,area_minLon=NULL,area_maxLon=NULL WHERE globalId='place'");
    EXPECT_TRUE(db.query("SELECT id FROM _Places_area_rtree",{}).empty());
    db.execute("UPDATE Places SET area_minLat=1,area_maxLat=2,area_minLon=3,area_maxLon=4 WHERE globalId='place'");
    ASSERT_EQ(db.query("SELECT id FROM _Places_area_rtree",{}).size(),1u);db.execute("DELETE FROM Places WHERE globalId='place'");
    EXPECT_TRUE(db.query("SELECT id FROM _Places_area_rtree",{}).empty());
}
TEST(OrdinarySchemaRecipeTests, ReceiveStoreAndChannelKeepStrictSeparateNativeShapes) {
    database db(":memory:");db.execute(recipe_access::receiver_store());db.execute(recipe_access::receiver_channel());
    db.execute("INSERT INTO _lattice_receive_guard_store VALUES(1,1,0,0,0,0,0)");
    EXPECT_THROW(db.execute("INSERT INTO _lattice_receive_guard_store VALUES(2,1,0,0,0,0,0)"),db_error);
    db.execute("INSERT INTO _lattice_receive_guard VALUES(X'01',1,1,0,0,X'02')");
    EXPECT_THROW(db.execute("INSERT INTO _lattice_receive_guard VALUES('text',2,1,0,0,NULL)"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _lattice_receive_guard VALUES(X'',2,1,0,0,NULL)"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _lattice_receive_guard VALUES(X'03',1,1,0,0,NULL)"),db_error);
    EXPECT_THROW(db.execute("INSERT INTO _lattice_receive_guard VALUES(X'03',2,NULL,0,0,NULL)"),db_error);
    auto values=db.query("SELECT typeof(channel) AS storage FROM _lattice_receive_guard",{});ASSERT_EQ(values.size(),1u);
    EXPECT_EQ(std::get<std::string>(values[0].at("storage")),"blob");
}

TEST(OrdinarySchemaRecipeTests, SharedMarkerRecipesKeepExactLegacyPrograms) {
    EXPECT_EQ(recipe_access::marker_store(),"INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)");
    EXPECT_EQ(recipe_access::marker_prune(),"DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%' AND key <> ? AND value <> ?");
}
TEST(OrdinarySchemaRecipeTests, MarkerReplacementAndPrunePreserveVersionSameCookieAndRollbackFullPrefix) {
    // Genuine ordinary profile-free SQLite only. This checks the SAME shared
    // producers' value semantics; it is not Host/private-operator activation.
    database db(":memory:");db.execute(recipe_access::metadata());
    const std::string current="schema_fingerprint:1111111111111111",stale="schema_fingerprint:2222222222222222",
        equal="schema_fingerprint:3333333333333333";
    db.execute("INSERT INTO _lattice_meta(key,value) VALUES('schema_version','7'),('caller_prefix','keep')");
    db.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",{current,std::string("old")});
    db.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",{stale,std::string("4")});
    db.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",{equal,std::string("5")});
    const auto image=[&] {
        std::map<std::string,std::string> result;
        for(const auto& row:db.query("SELECT key,value FROM _lattice_meta ORDER BY key"))
            result.emplace(std::get<std::string>(row.at("key")),std::get<std::string>(row.at("value")));
        return result;
    };
    const auto prefix=image();db.execute("BEGIN IMMEDIATE");
    db.execute(recipe_access::marker_store(),{current,std::string("5")});
    db.execute(recipe_access::marker_prune(),{current,std::string("5")});
    const std::map<std::string,std::string> expected{{"schema_version","7"},{"caller_prefix","keep"},{current,"5"},{equal,"5"}};
    EXPECT_EQ(image(),expected);EXPECT_FALSE(image().count(stale));
    db.execute("ROLLBACK");EXPECT_EQ(image(),prefix);EXPECT_FALSE(db.is_in_transaction());
}
