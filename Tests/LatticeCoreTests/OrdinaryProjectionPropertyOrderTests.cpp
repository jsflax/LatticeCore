#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/ordinary_projection_recipe.hpp"
#include "../../Sources/LatticeCore/src/ordinary_schema_validation.hpp"
#include <algorithm>
#include <sstream>

using namespace lattice;
using namespace lattice::detail;
namespace {
// Pure declaration/recipe fixtures have no database/source/currentness grant.
// The protected production serializer is called without constructing an owner.
struct projection_property_serializer: lattice_db {
    static void serialize(std::ostringstream& out,const property_descriptor& value) {
        serialize_property_for_fingerprint(out,value);
    }
};
struct projection_budget {
    size_t bytes=0;
    void add(size_t n) {
        if(n>16*1024*1024-bytes)throw std::runtime_error("test actual recipe budget exceeded");
        bytes+=n;
    }
};
property_descriptor primitive(const char* name,column_type type=column_type::text) {
    property_descriptor p{};p.name=name;p.type=type;p.kind=property_kind::primitive;return p;
}
recovery_owner_schema declared(std::vector<model_schema> models,bool swift) {
    recovery_owner_schema result;
    for(const auto& model:models) {
        if(!result.admit_model(model))throw std::runtime_error("test declaration preflight refused");
        if(swift){result.swift_models.insert(model.table_name);result.swift_order.push_back(model.table_name);result.swift_constraints.emplace(model.table_name,std::vector<recovery_constraint>{});}
    }
    return result;
}
recovery_owner_schema rich_catalog() {
    model_schema parent;parent.table_name="Parent";
    auto zeta=primitive("zeta");zeta.no_history=true;
    auto alpha=primitive("alpha",column_type::integer);alpha.is_indexed=true;
    auto beta=primitive("beta");beta.is_indexed=true;
    auto area=primitive("area",column_type::real);area.is_geo_bounds=true;
    auto regions=primitive("regions");regions.kind=property_kind::list;regions.is_geo_bounds=true;
    auto search=primitive("search");search.is_full_text=true;
    auto embedding=primitive("embedding",column_type::blob);embedding.is_vector=true;
    auto child=primitive("child",column_type::integer);child.kind=property_kind::link;child.target_table="Child";child.link_table="_Parent_Child_child";
    auto items=primitive("items");items.kind=property_kind::virtual_list;
    auto choice=primitive("choice");choice.kind=property_kind::union_type;choice.is_union=true;choice.union_desc.union_table_name="_Parent_choice";
    union_case text,linked;text.case_name="text";linked.case_name="linked";
    union_case_value t,l;t.type=column_type::text;l.type=column_type::text;l.is_link=true;l.link_target="Child";
    text.values.push_back(t);linked.values.push_back(l);choice.union_desc.cases={text,linked};
    parent.properties={zeta,search,child,alpha,area,items,embedding,regions,choice,beta};
    model_schema child_model;child_model.table_name="Child";child_model.properties={primitive("title")};
    model_schema sibling;sibling.table_name="Sibling";sibling.properties={primitive("title")};
    auto result=declared({parent,child_model,sibling},true);
    result.swift_constraints["Parent"]={{{"child","zeta"},false},{{"beta","alpha"},true}};
    return result;
}
std::string declaration_bytes(const recovery_owner_schema& catalog) {
    std::ostringstream out;
    for(const auto& [name,model]:catalog.models){out<<name<<'\n';for(const auto& p:model.properties)projection_property_serializer::serialize(out,p);}
    for(const auto& [name,constraints]:catalog.swift_constraints){out<<name<<'\n';for(const auto& c:constraints){out<<c.allows_upsert;for(const auto& column:c.columns)out<<column.size()<<':'<<column;}}
    return out.str();
}
struct described {
    ordinary_schema_projection projection;
    ordinary_export_frame frame;
    size_t charged=0;
};
described describe(const recovery_owner_schema& catalog) {
    described result;projection_budget budget;
    result.projection=describe_ordinary_projection(catalog,{});budget.add(result.projection.logical_bytes);
    auto hash=ordinary_projection_recipe::begin(catalog,result.projection,projection_property_serializer::serialize,result.frame,budget);
    for(const auto& [name,table]:result.projection.tables)result.frame.tables.push_back(ordinary_projection_recipe::table(hash,name,table,budget));
    ordinary_projection_recipe::finish(hash,result.projection,result.frame,budget);result.charged=budget.bytes;return result;
}
void same_projection(const described& a,const described& b) {
    EXPECT_EQ(a.frame.schema,b.frame.schema);EXPECT_EQ(a.frame.scope,b.frame.scope);EXPECT_EQ(a.frame.models,b.frame.models);
    ASSERT_EQ(a.projection.tables.size(),b.projection.tables.size());
    for(const auto& [name,t]:a.projection.tables){const auto& u=b.projection.tables.at(name);
        EXPECT_EQ(t.name,u.name);EXPECT_EQ(t.create_sql,u.create_sql);EXPECT_EQ(t.role,u.role);EXPECT_EQ(t.local_id,u.local_id);
        EXPECT_EQ(t.columns,u.columns);EXPECT_EQ(t.audit_programs,u.audit_programs);EXPECT_EQ(t.alternate_audit_programs,u.alternate_audit_programs);
        EXPECT_EQ(t.other_programs,u.other_programs);EXPECT_EQ(t.index_programs,u.index_programs);EXPECT_EQ(t.shadow_columns,u.shadow_columns);}
    ASSERT_EQ(a.projection.derived.size(),b.projection.derived.size());
    for(size_t i=0;i<a.projection.derived.size();++i){const auto& x=a.projection.derived[i];const auto& y=b.projection.derived[i];
        EXPECT_EQ(x.model,y.model);EXPECT_EQ(x.property,y.property);EXPECT_EQ(x.table,y.table);EXPECT_EQ(x.kind,y.kind);EXPECT_EQ(x.create_sql,y.create_sql);EXPECT_EQ(x.programs,y.programs);}
    ASSERT_EQ(a.frame.tables.size(),b.frame.tables.size());
    for(size_t i=0;i<a.frame.tables.size();++i){EXPECT_EQ(a.frame.tables[i].name,b.frame.tables[i].name);EXPECT_EQ(a.frame.tables[i].columns,b.frame.tables[i].columns);EXPECT_EQ(a.frame.tables[i].audit_names,b.frame.tables[i].audit_names);EXPECT_EQ(a.frame.tables[i].types,b.frame.tables[i].types);}
}
property_descriptor& property(recovery_owner_schema& catalog,const char* name) {
    auto& properties=catalog.models.at("Parent").properties;
    const auto found=std::find_if(properties.begin(),properties.end(),[&](const auto& p){return p.name==name;});
    if(found==properties.end())throw std::runtime_error("test property absent");return *found;
}
}

TEST(OrdinaryProjectionPropertyOrder, SwiftPermutationsKeepRichLogicalRecipeAndNeverMutateTheCatalog) {
    auto original=rich_catalog();const auto original_bytes=declaration_bytes(original);const auto expected=describe(original);
    ASSERT_FALSE(expected.projection.derived.empty());ASSERT_GT(expected.projection.tables.size(),3u);
    auto reversed=original;for(auto& [name,model]:reversed.models)std::reverse(model.properties.begin(),model.properties.end());
    const auto reversed_bytes=declaration_bytes(reversed);ASSERT_NE(reversed_bytes,original_bytes);same_projection(expected,describe(reversed));
    auto rotated=original;auto& fields=rotated.models.at("Parent").properties;std::rotate(fields.begin(),fields.begin()+3,fields.end());
    const auto rotated_bytes=declaration_bytes(rotated);same_projection(expected,describe(rotated));
    EXPECT_EQ(declaration_bytes(original),original_bytes);EXPECT_EQ(declaration_bytes(reversed),reversed_bytes);EXPECT_EQ(declaration_bytes(rotated),rotated_bytes);
}

TEST(OrdinaryProjectionPropertyOrder, TwoSwiftIndexedPropertiesKeepCanonicalProgramOrderAndFingerprint) {
    model_schema model;model.table_name="IndexedRow";auto z=primitive("zeta");z.is_indexed=true;auto a=primitive("alpha",column_type::integer);a.is_indexed=true;model.properties={z,a};
    auto catalog=declared({model},true);const auto before=describe(catalog);std::reverse(catalog.models.at("IndexedRow").properties.begin(),catalog.models.at("IndexedRow").properties.end());const auto after=describe(catalog);
    same_projection(before,after);const auto& indexes=before.projection.tables.at("IndexedRow").index_programs;ASSERT_EQ(indexes.size(),2u);
    EXPECT_EQ(indexes[0],ordinary_property_index_program("IndexedRow","alpha"));EXPECT_EQ(indexes[1],ordinary_property_index_program("IndexedRow","zeta"));
}

TEST(OrdinaryProjectionPropertyOrder, NativeDeclarationAndUniqueOrdinalOrderRemainUnchanged) {
    model_schema model;model.table_name="NativeRow";auto z=primitive("zeta");z.is_unique=true;auto a=primitive("alpha",column_type::integer);a.is_unique=true;model.properties={z,a};
    auto catalog=declared({model},false);const auto before=describe(catalog);const auto original_bytes=declaration_bytes(catalog);
    EXPECT_EQ(before.projection.tables.at("NativeRow").columns[1].name,"zeta");EXPECT_EQ(before.projection.tables.at("NativeRow").columns[2].name,"alpha");
    EXPECT_EQ(before.projection.tables.at("NativeRow").index_programs[0],ordinary_unique_index_program("NativeRow",0,{"zeta"}));
    EXPECT_EQ(declaration_bytes(catalog),original_bytes);std::reverse(catalog.models.at("NativeRow").properties.begin(),catalog.models.at("NativeRow").properties.end());const auto after=describe(catalog);
    EXPECT_NE(before.frame.schema,after.frame.schema);EXPECT_EQ(after.projection.tables.at("NativeRow").columns[1].name,"alpha");
    EXPECT_EQ(after.projection.tables.at("NativeRow").index_programs[0],ordinary_unique_index_program("NativeRow",0,{"alpha"}));
}

TEST(OrdinaryProjectionPropertyOrder, RealTypeNoHistoryRelationshipDerivedAndConstraintChangesRemainDistinct) {
    const auto original=rich_catalog();const auto expected=describe(original).frame.schema;
    {auto changed=original;property(changed,"zeta").no_history=false;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;property(changed,"alpha").type=column_type::text;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;property(changed,"child").target_table="Sibling";property(changed,"child").link_table="_Parent_Sibling_child";EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;property(changed,"search").is_full_text=false;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;property(changed,"embedding").is_vector=false;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;property(changed,"area").is_geo_bounds=false;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;changed.swift_constraints.at("Parent")[0].allows_upsert=true;EXPECT_NE(describe(changed).frame.schema,expected);}
    {auto changed=original;std::reverse(property(changed,"choice").union_desc.cases.begin(),property(changed,"choice").union_desc.cases.end());EXPECT_NE(describe(changed).frame.schema,expected);}
}

TEST(OrdinaryProjectionPropertyOrder, BoundedPointerScratchChargesBeforeAllocationAndNeverMutatesTheBorrowedStorage) {
    model_schema oversized;oversized.properties.resize(ordinary_sync_limits::properties+1);size_t calls=0;
    EXPECT_THROW(ordinary_projection_property_order(oversized,true,[&](size_t){++calls;}),std::runtime_error);EXPECT_EQ(calls,0u);
    model_schema model;model.properties={primitive("zeta"),primitive("alpha")};const auto before=model.properties[0].name;size_t charged=0;
    EXPECT_THROW(ordinary_projection_property_order(model,true,[&](size_t n){charged=n;throw std::runtime_error("actual scratch budget refuses");}),std::runtime_error);
    EXPECT_EQ(charged,2*sizeof(const property_descriptor*));EXPECT_EQ(model.properties[0].name,before);
    auto pointers=ordinary_projection_property_order(model,true,[](size_t){});ASSERT_EQ(pointers.size(),2u);EXPECT_EQ(pointers[0],&model.properties[1]);EXPECT_EQ(pointers[1],&model.properties[0]);
}

TEST(OrdinaryProjectionPropertyOrder, ActualGeneratedMetadataAcceptsPermutedSwiftDeclarationButNeverHealsTampering) {
    model_schema model;model.table_name="MetadataRow";auto z=primitive("zeta");z.no_history=true;z.is_indexed=true;auto a=primitive("alpha",column_type::integer);a.is_indexed=true;model.properties={z,a};
    database db(":memory:");const auto physical=ordinary_model_program(model);db.execute(physical.create_table);
    std::vector<std::string> actual_audits;ordinary_model_audit_programs(model.table_name,physical.columns,{"zeta"},{},[&](const std::string& sql){actual_audits.push_back(sql);db.execute(sql);});
    db.execute(ordinary_property_index_program(model.table_name,"zeta"));db.execute(ordinary_property_index_program(model.table_name,"alpha"));
    auto catalog=declared({model},true);std::reverse(catalog.models.at(model.table_name).properties.begin(),catalog.models.at(model.table_name).properties.end());const auto projection=describe(catalog).projection;
    const ordinary_metadata_query query=[&](const std::string& sql,const std::vector<column_value_t>& args,size_t cap){auto rows=db.query(sql,args);if(rows.size()>cap)throw std::runtime_error("actual metadata query overrun");return rows;};
    const auto snapshot=[&]{return db.query("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name");};const auto before=snapshot();
    const auto observed=validate_ordinary_schema(projection,query);EXPECT_GT(observed.inspected_rows,0u);EXPECT_TRUE(observed.tables.count(model.table_name));EXPECT_EQ(snapshot(),before);
    auto tampered=actual_audits.at(1);const auto end=tampered.rfind(" END");ASSERT_NE(end,std::string::npos);tampered.insert(end," SELECT 1;");
    db.execute("DROP TRIGGER AuditMetadataRowInsert");db.execute(tampered);const auto damaged=snapshot();
    EXPECT_THROW(validate_ordinary_schema(projection,query),ordinary_schema_validation_error);EXPECT_EQ(snapshot(),damaged);
}
