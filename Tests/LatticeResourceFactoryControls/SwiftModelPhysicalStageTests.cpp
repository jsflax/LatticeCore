#include <gtest/gtest.h>
#include <lattice.hpp>
#include "ActualModelProducerTestAccess.hpp"
#include <lattice/ordinary_schema_recipe.hpp>
#include <lattice/detail/observer_delivery_startup.hpp>
#include "../../Sources/LatticeCore/src/database_initialization_budget.hpp"
#include "../../Sources/LatticeCore/src/database_sql_dispatcher.hpp"
#include "../../Sources/LatticeCore/src/database_initialization_test_probe.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_startup_test_probe.hpp"
#include "../../Sources/LatticeCore/src/ordinary_schema_validation.hpp"
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#if defined(__APPLE__) || defined(__linux__)
#include <pwd.h>
#include <unistd.h>
#endif

namespace lattice::detail {
// Existing private test access. No new issuer, Native handle or SQL purpose.
struct database_initialization_budget_test_access {
    using recipe=observer_delivery_initialization_sql::recipe;
    static auto& open(database& db) {
        db.require_creator_();if(!db.delivery_startup_)throw db_error("genuine installed pending Open is required");
        return *db.delivery_startup_;
    }
    static auto inspect(database& db){return open(db).initialization_budget_(db)->inspect_for_test_();}
    static auto begin(database& db) {
        auto start=observer_delivery_initialization_sql::fixed_(open(db),db,recipe::schema_transaction_begin);
        auto commit=observer_delivery_initialization_sql::fixed_(open(db),db,recipe::schema_transaction_commit);
        auto rollback=observer_delivery_initialization_sql::fixed_(open(db),db,recipe::schema_transaction_rollback);
        return db.begin_initialization_(start,commit,rollback);
    }
    static bool autocommit(database& db) {
        db.require_creator_();database::catalog_native_admission_ admitted(db);return sqlite3_get_autocommit(db.db_)!=0;
    }
    static bool raw_without_core(database& db){return !autocommit(db)&&!db.core_transaction_original_;}
    static auto schema_count(database& db) {
        auto issued=observer_delivery_initialization_sql::fixed_(open(db),db,recipe::connection_schema_count);
        const auto rows=db.query_initialization_(issued);
        if(rows.size()!=1||rows.front().size()!=1||!std::holds_alternative<std::int64_t>(rows.front().begin()->second))
            throw db_error("actual schema count result differs");
        return std::get<std::int64_t>(rows.front().begin()->second);
    }
    static void machinery(database& db) {
        auto table=open(db).metadata_table_recipe_(db);db.execute_initialization_(table);
        auto version=open(db).metadata_initial_version_recipe_(db);db.execute_initialization_(version);
        auto control=open(db).sync_control_table_recipe_(db);db.execute_initialization_(control);
        auto seed=open(db).sync_control_initial_recipe_(db);db.execute_initialization_(seed);
        using operation=observer_delivery_open_startup::audit_operation;
        for(const auto step:{operation::table,operation::pending_index,operation::lookup_index})open(db).initialize_audit_(db,step);
    }
    static auto projection(database& db){return describe_ordinary_projection(open(db).initialization_catalog_(db),{});}
    static void schema(lattice_db& owner,std::string_view sql){auto& db=owner.db();open(db).initialize_model_recipe_(owner,db,sql);}
    static auto columns(lattice_db& owner,const std::string& table){auto& db=owner.db();return open(db).model_table_info_(owner,db,table);}
    static void complete(lattice_db& owner){auto& db=owner.db();open(db).require_model_catalog_present_(owner,db);}
    static int restrict(database& db,int(*callback)(void*,int,const char*,const char*,const char*,const char*)noexcept,void* data)noexcept {
        return db.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,callback,data);
    }
    static int clear(database& db,void* data)noexcept{return db.clear_sql_authorizer_(database_authorizer_slot::owned_test_restriction,data);}
};
struct ordinary_schema_recipe_test_access {
    static auto physical(const swift_schema_entry& entry){return ordinary_schema_recipe::swift_physical_model(entry.table_name,entry.properties);}
    static auto shadow(const std::string& table,const std::string& column){return ordinary_schema_recipe::shadow_column(table,column);}
};
}
namespace {
using access=lattice::detail::database_initialization_budget_test_access;
using recipes=lattice::detail::ordinary_schema_recipe_test_access;
using probe=lattice::detail::observer_delivery_startup_test_probe;
using lattice::detail::ordinary_schema_validation_error;
enum class mode {complete,early_complete,deny_alter,duplicate,successor};
struct stopped {};
struct observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    mode operation;const lattice::SchemaVector* actual_schemas=nullptr;unsigned bound=0,denials=0;
    bool raw=false,base=false,first=false,second=false,full=false,repeated=false,successor_base=false,
         refused=false,exact_refusal=false,sticky=false,first_auth=false,clean=true,no_fabricated=false,
         same_account=false,cumulative=false,ended=false,restored=false,public_refused=false;
    explicit observation(mode value):operation(value){}
};
thread_local observation* selected=nullptr;
std::string actual_logical_root() {
#if defined(__APPLE__) || defined(__linux__)
    struct passwd data{},*found=nullptr;char bytes[16384];
    if(::getpwuid_r(::geteuid(),&data,bytes,sizeof(bytes),&found)!=0||!found||!found->pw_dir||found->pw_dir[0]!='/')
        throw std::runtime_error("actual account home unavailable");
    return std::string(found->pw_dir)+"/.claude/memory.sqlite";
#else
    throw std::runtime_error("actual installed Swift factory is unsupported");
#endif
}
lattice::SchemaVector actual_schemas() {
    lattice::property_descriptor label{};label.name="label";label.type=lattice::column_type::text;
    lattice::property_descriptor value{};value.name="value";value.type=lattice::column_type::text;
    lattice::property_descriptor group{};group.name="group";group.kind=lattice::property_kind::link;group.target_table="StageTarget";
    auto other=group;other.name="other";
    lattice::SwiftSchema parent{{"label",label},{"group",group},{"other",other}},target{{"value",value}};
    return {{"StageOwner",parent,lattice::ConstraintVector{{{"group"},false},{{"other"},false}}},{"StageTarget",target}};
}
int deny_actual_alter(void* data,int action,const char* first,const char* second,const char*,const char*)noexcept {
    auto& result=*static_cast<observation*>(data);
    if(action==SQLITE_ALTER_TABLE&&first&&second&&!std::strcmp(first,"main")&&!std::strcmp(second,"StageOwner")) {
        ++result.denials;return SQLITE_DENY;
    }
    return SQLITE_OK;
}
void prepare_actual_base(lattice::lattice_db& owner,const lattice::SchemaVector& schemas) {
    auto& db=owner.db();access::machinery(db);
    // These are the SAME actual public Swift-input physical model producers.
    // The base hook cannot touch the not-yet-constructed derived Swift object.
    for(const auto& entry:schemas)lattice::observer_delivery_custody_test_access::create_model(owner,recipes::physical(entry));
    // Explicit metadata primitives over the SAME retained immutable catalog;
    // this is not a claim that internal link metadata migration is finished.
    const auto projection=access::projection(db);
    for(const auto& [name,table]:projection.tables)if(table.role==lattice::detail::ordinary_projection_role::link) {
        access::schema(owner,table.create_sql);
        for(const auto& program:table.audit_programs)access::schema(owner,program);
    }
}
void after_bound(lattice::lattice_db& owner,const probe::owner_binding_observation&) {
    if(!selected||selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*selected;if(++result.bound!=1||!result.actual_schemas)throw std::runtime_error("Swift stage control selection differs");
    auto& db=owner.db();const auto before=access::inspect(db);const auto schema_before=access::schema_count(db);
    if(result.operation==mode::successor) {
        auto first=access::begin(db);prepare_actual_base(owner,*result.actual_schemas);
        access::schema(owner,recipes::shadow("StageOwner","group__link_gid"));
        const auto partial=access::columns(owner,"StageOwner");
        if(partial.size()!=4||partial.count("other__link_gid"))throw std::runtime_error("genuine first episode partial shape differs");
        first.rollback();if(!access::autocommit(db))throw std::runtime_error("first actual episode rollback did not end");
    }
    auto transaction=access::begin(db);result.raw=access::raw_without_core(db);
    prepare_actual_base(owner,*result.actual_schemas);
    const auto base=access::columns(owner,"StageOwner");
    result.base=base.size()==3&&base.at("id")=="INTEGER"&&base.at("globalId")=="TEXT"&&base.at("label")=="TEXT";
    if(result.operation==mode::successor)result.successor_base=result.base&&!base.count("group__link_gid")&&!base.count("other__link_gid");
    struct restriction {
        lattice::database& db;observation& result;bool held=false;
        void clear(){if(held){if(access::clear(db,&result)!=SQLITE_OK)std::_Exit(97);held=false;}}
        ~restriction(){clear();}
    } role{db,result};
    if(result.operation==mode::deny_alter) {
        if(access::restrict(db,deny_actual_alter,&result)!=SQLITE_OK)throw std::runtime_error("actual ALTER restriction failed");
        role.held=true;
    }
    try {
        access::schema(owner,recipes::shadow("StageOwner","group__link_gid"));
        const auto first=access::columns(owner,"StageOwner");
        result.first=first.size()==4&&first.at("group__link_gid")=="TEXT"&&!first.count("other__link_gid");
        if(result.operation==mode::early_complete)access::complete(owner);
        else if(result.operation==mode::duplicate)access::schema(owner,recipes::shadow("StageOwner","group__link_gid"));
        else {
            access::schema(owner,recipes::shadow("StageOwner","other__link_gid"));
            const auto second=access::columns(owner,"StageOwner");
            result.second=second.size()==5&&second.at("group__link_gid")=="TEXT"&&second.at("other__link_gid")=="TEXT";
            const auto projection=access::projection(db);
            for(const auto& [name,table]:projection.tables) {
                for(const auto& program:table.other_programs)access::schema(owner,program);
                for(const auto& program:table.index_programs)access::schema(owner,program);
            }
            access::complete(owner);result.full=true;
            access::complete(owner);result.repeated=true;
        }
    }catch(const ordinary_schema_validation_error& error) {
        if(result.operation!=mode::early_complete)throw;
        result.refused=true;result.exact_refusal=std::string_view(error.what())=="ordinary table constraints/column count mismatch";
    }catch(const lattice::db_error& error) {
        result.refused=true;
        if(result.operation==mode::duplicate)result.exact_refusal=std::string_view(error.what())=="model operator lacks actual required-present parent/table dependency";
    }
    role.clear();const auto after=access::inspect(db);
    result.same_account=after.identity==before.identity&&after.deadline==before.deadline;
    result.cumulative=after.workspace>before.workspace&&after.statements>before.statements;
    result.sticky=after.failed;result.first_auth=after.first_api_rc==SQLITE_AUTH;
    result.clean=after.cleanup_api_rc==0;result.no_fabricated=after.first_api_rc==0&&after.cleanup_api_rc==0;
    transaction.rollback();result.ended=access::autocommit(db);
    const auto ended=access::inspect(db);result.first_auth=ended.first_api_rc==SQLITE_AUTH;result.clean=ended.cleanup_api_rc==0;
    if(result.operation==mode::complete||result.operation==mode::successor)result.restored=access::schema_count(db)==schema_before;
    try{(void)db.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw stopped{};
}
struct hook {
    explicit hook(observation& value) {
        if(selected||probe::after_owner_bound||lattice::detail::database_initialization_test_probe::after_pending||
           lattice::detail::database_initialization_test_probe::after_functions)throw std::runtime_error("installed stage hook already selected");
        selected=&value;probe::after_owner_bound=after_bound;
    }
    ~hook(){probe::after_owner_bound=nullptr;selected=nullptr;}
};
void run(observation& result) {
    const auto schemas=actual_schemas();result.actual_schemas=&schemas;hook guard(result);bool did_stop=false;
    lattice::swift_configuration config(actual_logical_root());
    try{lattice::swift_lattice owner(config,schemas);}catch(const stopped&){did_stop=true;}
    ASSERT_TRUE(did_stop);ASSERT_EQ(result.bound,1u);EXPECT_TRUE(result.raw);EXPECT_TRUE(result.base);
    EXPECT_TRUE(result.same_account);EXPECT_TRUE(result.cumulative);EXPECT_TRUE(result.ended);EXPECT_TRUE(result.public_refused);
}
TEST(SwiftModelPhysicalStage, GenuineBaseTwoColumnsProgramsIndexesAndFullProofRollback) {
    observation result(mode::complete);run(result);EXPECT_TRUE(result.first);EXPECT_TRUE(result.second);EXPECT_TRUE(result.full);
    EXPECT_TRUE(result.repeated);EXPECT_TRUE(result.restored);EXPECT_FALSE(result.refused);EXPECT_FALSE(result.sticky);
    EXPECT_TRUE(result.no_fabricated);EXPECT_TRUE(result.clean);
}
TEST(SwiftModelPhysicalStage, RequiredPresentRefusesActualIntermediateColumnStage) {
    observation result(mode::early_complete);run(result);EXPECT_TRUE(result.first);EXPECT_TRUE(result.refused);
    EXPECT_TRUE(result.exact_refusal);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_fabricated);EXPECT_TRUE(result.clean);
}
TEST(SwiftModelPhysicalStage, DuplicateColumnCannotReceiveAnotherWriteAuthority) {
    observation result(mode::duplicate);run(result);EXPECT_TRUE(result.first);EXPECT_TRUE(result.refused);
    EXPECT_TRUE(result.exact_refusal);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_fabricated);EXPECT_TRUE(result.clean);
}
TEST(SwiftModelPhysicalStage, ActualAlterDenialPreservesFirstAuthAndReservedRollback) {
    observation result(mode::deny_alter);run(result);EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);
    EXPECT_GT(result.denials,0u);EXPECT_TRUE(result.first_auth);EXPECT_TRUE(result.clean);
}
TEST(SwiftModelPhysicalStage, ActualSuccessorEpisodeCannotInheritRolledBackIntermediateColumns) {
    observation result(mode::successor);run(result);EXPECT_TRUE(result.successor_base);EXPECT_TRUE(result.first);EXPECT_TRUE(result.second);
    EXPECT_TRUE(result.full);EXPECT_TRUE(result.restored);EXPECT_FALSE(result.refused);EXPECT_FALSE(result.sticky);EXPECT_TRUE(result.clean);
}
} // namespace

static_assert(!std::is_polymorphic_v<lattice::lattice_db>,"registry validation must not introduce a Base vtable");
namespace {
using registry_access=lattice::observer_delivery_custody_test_access;
lattice::SchemaVector actual_registry_schemas() {
    using namespace lattice;
    property_descriptor label{};label.name="label";label.type=column_type::text;
    property_descriptor target{};target.name="target";target.type=column_type::integer;
    target.kind=property_kind::link;target.target_table="RegistryTarget";
    auto children=target;children.name="children";children.kind=property_kind::list;
    property_descriptor members{};members.name="members";members.type=column_type::integer;members.kind=property_kind::virtual_list;
    auto selected=members;selected.name="selected";selected.kind=property_kind::virtual_link;
    property_descriptor zones{};zones.name="zones";zones.type=column_type::integer;zones.kind=property_kind::list;zones.is_geo_bounds=true;
    property_descriptor choice{};choice.name="choice";choice.type=column_type::text;
    choice.kind=property_kind::union_type;choice.is_union=true;choice.target_table="_RegistryChoice";
    choice.union_desc.union_table_name="_RegistryChoice";
    choice.union_desc.cases={{"none",{}},{"named",{{"title",column_type::text,false,{}}}}};
    SwiftSchema properties{{label.name,label},{target.name,target},{children.name,children},{members.name,members},
        {selected.name,selected},{zones.name,zones},{choice.name,choice}};
    return {{"RegistryOwner",properties,ConstraintVector{{{"label"},false}}},{"RegistryTarget",SwiftSchema{{label.name,label}}}};
}
void require_actual_registries(lattice::lattice_db& owner) {
    registry_access::require_model_registries(owner,registry_access::actual_model_catalog(owner));
}
}
TEST(PendingModelRegistries, ActualCoreOwnerCatalogIdentityAndUnknownBucketRemainClosed) {
    lattice::configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::lattice_db owner(config,true);ASSERT_NO_THROW(require_actual_registries(owner));
    const auto copied=registry_access::actual_model_catalog(owner);
    EXPECT_THROW(registry_access::require_model_registries(owner,copied),lattice::db_error);
    auto& unknown=registry_access::unknown_link_registry(owner);ASSERT_TRUE(unknown.empty());
    unknown.insert("_not_a_declared_runtime_link");EXPECT_THROW(require_actual_registries(owner),lattice::db_error);
    unknown.erase("_not_a_declared_runtime_link");EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}
TEST(PendingModelRegistries, ActualSwiftPopulationKeepsEveryCoreCascadeFamilyAndSameDerivedBinding) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);
    ASSERT_TRUE(registry_access::actual_derived_registry_binding(owner));ASSERT_NO_THROW(require_actual_registries(owner));
    auto& known=registry_access::known_link_registry(owner);ASSERT_FALSE(known.empty());
    auto known_node=known.extract(known.begin());EXPECT_THROW(require_actual_registries(owner),lattice::db_error);
    known.insert(std::move(known_node));EXPECT_NO_THROW(require_actual_registries(owner));
    auto& virtuals=registry_access::virtual_link_registry(owner);ASSERT_FALSE(virtuals.empty());
    auto virtual_node=virtuals.extract(virtuals.begin());EXPECT_THROW(require_actual_registries(owner),lattice::db_error);
    virtuals.insert(std::move(virtual_node));EXPECT_NO_THROW(require_actual_registries(owner));
    auto& lists=registry_access::geo_list_registry(owner);ASSERT_FALSE(lists.empty());
    auto list_node=lists.extract(lists.begin());EXPECT_THROW(require_actual_registries(owner),lattice::db_error);
    lists.insert(std::move(list_node));EXPECT_NO_THROW(require_actual_registries(owner));
    const auto copied=registry_access::actual_model_catalog(owner);
    EXPECT_THROW(registry_access::require_model_registries(owner,copied),lattice::db_error);
    EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}
TEST(PendingModelRegistries, ActualBorrowedSwiftDescriptorAndConstraintFaultsRefuseWithoutReplacingMaps) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);ASSERT_NO_THROW(require_actual_registries(owner));
    // Explicit DATA faults in this genuinely nonconst owner's borrowed maps.
    // No declaration, Open, owner or SQL authority is fabricated.
    auto* properties=const_cast<lattice::RuntimeSwiftSchema*>(owner.get_properties_for_table("RegistryOwner"));ASSERT_NE(properties,nullptr);
    auto property=properties->extract("label");ASSERT_FALSE(property.empty());
    EXPECT_THROW(require_actual_registries(owner),lattice::db_error);properties->insert(std::move(property));
    EXPECT_NO_THROW(require_actual_registries(owner));
    auto& label=properties->at("label");const auto original_flag=label.no_history;label.no_history=!original_flag;
    EXPECT_THROW(require_actual_registries(owner),lattice::db_error);label.no_history=original_flag;
    auto* constraints=const_cast<lattice::RuntimeConstraintVector*>(owner.get_constraints_for_table("RegistryOwner"));
    ASSERT_NE(constraints,nullptr);ASSERT_EQ(constraints->size(),1u);const auto original_upsert=constraints->front().allows_upsert;
    constraints->front().allows_upsert=!original_upsert;EXPECT_THROW(require_actual_registries(owner),lattice::db_error);
    constraints->front().allows_upsert=original_upsert;EXPECT_NO_THROW(require_actual_registries(owner));
    EXPECT_EQ(owner.get_properties_for_table("RegistryOwner"),properties);
    EXPECT_EQ(owner.get_constraints_for_table("RegistryOwner"),constraints);owner.close();
}
TEST(PendingModelRegistries, ActualEmptySwiftConstructorBindsAndChecksItsOwnCatalog) {
    lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(std::move(config));
    EXPECT_TRUE(registry_access::actual_derived_registry_binding(owner));
    EXPECT_TRUE(owner.dynamic_table_names().empty());EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}

#include "../../Sources/LatticeCore/src/ordinary_initialization_storage.hpp"
namespace {
struct core_registry_debit_denied {};
struct actual_core_registry_counter {
    const std::uint64_t creator=lattice::detail::database_native_process();
    lattice::lattice_db* owner=nullptr;
    std::uint64_t calls=0,bytes=0,deny_at=0;
    static void debit(void* account,void* actual,std::uint64_t bytes) {
        auto& value=*static_cast<actual_core_registry_counter*>(account);
        if(value.creator!=lattice::detail::database_native_process()||actual!=value.owner)
            throw core_registry_debit_denied{};
        ++value.calls;
        if(value.deny_at&&value.calls==value.deny_at)throw core_registry_debit_denied{};
        constexpr std::uint64_t finite_test_limit=std::uint64_t(32)<<20;
        if(value.bytes>finite_test_limit||bytes>finite_test_limit-value.bytes)throw core_registry_debit_denied{};
        value.bytes+=bytes;
    }
};
template<class Checkpoint>void expect_core_registry_data_equal(const Checkpoint& actual,const Checkpoint& expected) {
    EXPECT_EQ(actual.links_by_target,expected.links_by_target);EXPECT_EQ(actual.links_unknown,expected.links_unknown);
    EXPECT_EQ(actual.virtual_links,expected.virtual_links);EXPECT_EQ(actual.lists_by_parent,expected.lists_by_parent);
}
}
TEST(PendingCoreRegistryPopulation, ActualOwnerCompleteCatalogRebuildUsesRealAllocatorRequestsAndKeepsSwiftTypes) {
    static_assert(std::is_same_v<lattice::SwiftSchema,std::unordered_map<std::string,lattice::property_descriptor>>);
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);const auto expected=registry_access::core_registry_snapshot(owner);
    const auto* swift_properties=owner.get_properties_for_table("RegistryOwner");
    const auto* swift_constraints=owner.get_constraints_for_table("RegistryOwner");
    actual_core_registry_counter counter;counter.owner=&owner;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        ASSERT_NO_THROW(registry_access::replace_core_registry_data(owner,registry_access::actual_model_catalog(owner)));
    }
    EXPECT_GT(counter.calls,5u);EXPECT_GT(counter.bytes,0u);
    expect_core_registry_data_equal(registry_access::core_registry_snapshot(owner),expected);
    EXPECT_EQ(owner.get_properties_for_table("RegistryOwner"),swift_properties);
    EXPECT_EQ(owner.get_constraints_for_table("RegistryOwner"),swift_constraints);
    EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}
TEST(PendingCoreRegistryPopulation, ActualPreallocationDenialRetainsAllOriginalRegistriesAndFirstException) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);actual_core_registry_counter observed;observed.owner=&owner;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&observed,&owner,actual_core_registry_counter::debit);
        ASSERT_NO_THROW(registry_access::replace_core_registry_data(owner,registry_access::actual_model_catalog(owner)));
    }
    ASSERT_GT(observed.calls,5u);const auto expected=registry_access::core_registry_snapshot(owner);
    for(const auto denied_request:{std::uint64_t(1),observed.calls/2,observed.calls}) {
        actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=denied_request;
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            EXPECT_THROW(registry_access::replace_core_registry_data(owner,registry_access::actual_model_catalog(owner)),core_registry_debit_denied);
        }
        EXPECT_EQ(counter.calls,denied_request);expect_core_registry_data_equal(registry_access::core_registry_snapshot(owner),expected);
        EXPECT_NO_THROW(require_actual_registries(owner));
    }
    owner.close();
}
TEST(PendingCoreRegistryPopulation, ActualCopiedCatalogAndMissingScopeRefuseBeforeAnyRegistryDebit) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);const auto expected=registry_access::core_registry_snapshot(owner);
    EXPECT_THROW(registry_access::replace_core_registry_data(owner,registry_access::actual_model_catalog(owner)),lattice::db_error);
    const auto copied=registry_access::actual_model_catalog(owner);actual_core_registry_counter counter;counter.owner=&owner;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        EXPECT_THROW(registry_access::replace_core_registry_data(owner,copied),lattice::db_error);
    }
    EXPECT_EQ(counter.calls,0u);EXPECT_EQ(counter.bytes,0u);
    expect_core_registry_data_equal(registry_access::core_registry_snapshot(owner),expected);
    EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}

TEST(PendingSwiftRegistryPopulation, ActualSameCatalogNestedRuntimeUsesRequestsAndLeavesInputTypesUnchanged) {
    static_assert(std::is_same_v<lattice::SwiftSchema,std::unordered_map<std::string,lattice::property_descriptor>>);
    static_assert(!std::is_same_v<lattice::SwiftSchema,lattice::RuntimeSwiftSchema>);
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);actual_core_registry_counter counter;counter.owner=&owner;
    const auto cascade=registry_access::core_registry_snapshot(owner);
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        ASSERT_NO_THROW(registry_access::replace_swift_registry_data(owner,registry_access::actual_model_catalog(owner)));
    }
    EXPECT_GT(counter.calls,10u);EXPECT_GT(counter.bytes,0u);EXPECT_NO_THROW(require_actual_registries(owner));
    expect_core_registry_data_equal(registry_access::core_registry_snapshot(owner),cascade);
    const auto* properties=owner.get_properties_for_table("RegistryOwner");ASSERT_NE(properties,nullptr);
    const auto copy=properties->ordinary_copy();ASSERT_EQ(copy.size(),schemas.front().properties.size());
    EXPECT_EQ(copy.at("choice").union_desc.cases[1].values[0].param_name,"title");
    EXPECT_EQ(owner.get_constraints_for_table("RegistryOwner")->front().columns.front(),std::string_view("label"));
    EXPECT_EQ(owner.get_property_info("RegistryOwner","choice").union_case_count(),2u);owner.close();
}
TEST(PendingSwiftRegistryPopulation, ActualDeniedFirstMiddleAndLastRequestsKeepEveryOriginalBorrowAndError) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);actual_core_registry_counter observed;observed.owner=&owner;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&observed,&owner,actual_core_registry_counter::debit);
        ASSERT_NO_THROW(registry_access::replace_swift_registry_data(owner,registry_access::actual_model_catalog(owner)));
    }
    ASSERT_GT(observed.calls,10u);const auto* properties=owner.get_properties_for_table("RegistryOwner");
    const auto* constraints=owner.get_constraints_for_table("RegistryOwner");const auto cascade=registry_access::core_registry_snapshot(owner);
    for(const auto denied:{std::uint64_t(1),observed.calls/2,observed.calls}) {
        actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=denied;
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            EXPECT_THROW(registry_access::replace_swift_registry_data(owner,registry_access::actual_model_catalog(owner)),core_registry_debit_denied);
        }
        EXPECT_EQ(counter.calls,denied);EXPECT_EQ(owner.get_properties_for_table("RegistryOwner"),properties);
        EXPECT_EQ(owner.get_constraints_for_table("RegistryOwner"),constraints);EXPECT_NO_THROW(require_actual_registries(owner));
        expect_core_registry_data_equal(registry_access::core_registry_snapshot(owner),cascade);
    }
    owner.close();
}
TEST(PendingSwiftRegistryPopulation, ActualOwnedInfoAllocatesNothingAndOwningBoundaryRefusesBeforeDebit) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=1;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        const auto view=owner.get_property_info("RegistryOwner","choice");ASSERT_TRUE(view.valid());
        EXPECT_TRUE(view.is_union());EXPECT_EQ(view.union_case_count(),2u);EXPECT_EQ(view.name_size(),6u);
        EXPECT_THROW(owner.get_properties_for_table("RegistryOwner")->ordinary_copy(),lattice::detail::ordinary_initialization_storage_error);
        EXPECT_EQ(counter.calls,0u);EXPECT_EQ(counter.bytes,0u);
    }
    const auto copied=registry_access::actual_model_catalog(owner);
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        EXPECT_THROW(registry_access::replace_swift_registry_data(owner,copied),lattice::db_error);
    }
    EXPECT_EQ(counter.calls,0u);EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}
TEST(PendingSwiftRegistryPopulation, ActualLongNestedCharacterAndVectorRequestsCannotEscapeTheSameDebit) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);lattice::property_descriptor input{};
    input.name=std::string(91,'n');input.target_table=std::string(93,'t');input.link_table=std::string(95,'l');
    input.column_name=std::string(97,'c');input.is_union=true;input.no_history=true;
    input.union_desc.union_table_name=std::string(99,'u');
    input.union_desc.cases={{std::string(101,'k'),{{std::string(103,'p'),lattice::column_type::text,true,std::string(105,'r')}}}};
    actual_core_registry_counter observed;observed.owner=&owner;
    {
        lattice::detail::ordinary_initialization_storage_scope debit(&observed,&owner,actual_core_registry_counter::debit);
        const lattice::detail::swift_runtime_property_descriptor runtime(input);
        EXPECT_EQ(runtime.name.size(),91u);EXPECT_TRUE(runtime.no_history);
        EXPECT_EQ(runtime.union_desc.cases.front().values.front().link_target.size(),105u);
    }
    // Eight owning strings plus the actual two nested vectors exceed SSO and
    // all make genuine allocator requests, independently of map/bucket storage.
    ASSERT_GE(observed.calls,10u);
    for(std::uint64_t denied=1;denied<=observed.calls;++denied) {
        actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=denied;
        lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
        EXPECT_THROW(lattice::detail::swift_runtime_property_descriptor{input},core_registry_debit_denied);
        EXPECT_EQ(counter.calls,denied);
    }
    EXPECT_EQ(input.name,std::string(91,'n'));EXPECT_EQ(input.union_desc.cases.front().values.front().link_target,std::string(105,'r'));
    EXPECT_NO_THROW(require_actual_registries(owner));owner.close();
}

namespace {
// Explicit DATA-only faults in a genuinely nonconst runtime descriptor. Both
// guards restore before the owner ends, including exception/ASSERT unwinding.
struct actual_property_info_flags_restore {
    const std::uint64_t creator=lattice::detail::database_native_process();
    lattice::detail::swift_runtime_property_descriptor& value;
    const std::array<bool,8> original;
    explicit actual_property_info_flags_restore(lattice::detail::swift_runtime_property_descriptor& actual)
        :value(actual),original{actual.nullable,actual.is_vector,actual.is_geo_bounds,actual.is_full_text,
             actual.is_indexed,actual.is_unique,actual.is_union,actual.no_history}{}
    ~actual_property_info_flags_restore()noexcept {
        if(creator!=lattice::detail::database_native_process())return;
        value.nullable=original[0];value.is_vector=original[1];value.is_geo_bounds=original[2];
        value.is_full_text=original[3];value.is_indexed=original[4];value.is_unique=original[5];
        value.is_union=original[6];value.no_history=original[7];
    }
};
struct actual_property_info_name_restore {
    const std::uint64_t creator=lattice::detail::database_native_process();
    lattice::detail::swift_runtime_text& value;
    lattice::detail::swift_runtime_text original;
    explicit actual_property_info_name_restore(lattice::detail::swift_runtime_text& actual):value(actual){value.swap(original);}
    ~actual_property_info_name_restore()noexcept {
        if(creator==lattice::detail::database_native_process())value.swap(original);
    }
};
}
TEST(PendingSwiftRegistryPopulation, ActualOwnedPropertyInfoCopiesCompleteScalarDataWithoutAllocating) {
    static_assert(std::is_trivially_copyable_v<lattice::swift_property_info>);
    static_assert(std::is_trivially_destructible_v<lattice::swift_property_info>);
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);ASSERT_NO_THROW(require_actual_registries(owner));
    auto* properties=const_cast<lattice::RuntimeSwiftSchema*>(owner.get_properties_for_table("RegistryOwner"));
    ASSERT_NE(properties,nullptr);auto& property=properties->at("choice");
    const std::string table="RegistryOwner",column="choice",missing="notDeclared";
    const std::array<std::uint8_t,6> expected{'c','h','o','i','c','e'};
    const auto original_kind=property.kind;const auto original_cases=property.union_desc.cases.size();
    const auto* const original_name=property.name.data();
    actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=1;
    lattice::swift_property_info snapshot;
    {
        actual_property_info_flags_restore restore(property);
        property.nullable=property.is_vector=property.is_geo_bounds=property.is_full_text=true;
        property.is_indexed=property.is_unique=property.is_union=property.no_history=true;
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            snapshot=owner.get_property_info(table,column);EXPECT_TRUE(snapshot.valid());
            const auto copied=snapshot;EXPECT_TRUE(copied.valid());EXPECT_EQ(copied.name_size(),expected.size());
            for(std::size_t index=0;index<expected.size();++index)EXPECT_EQ(copied.name_byte(index),expected[index]);
            EXPECT_EQ(copied.name_byte(expected.size()),-1);
            EXPECT_EQ(copied.kind_value(),static_cast<std::int32_t>(original_kind));
            EXPECT_EQ(copied.union_case_count(),original_cases);
            EXPECT_TRUE(copied.nullable());EXPECT_TRUE(copied.is_vector());EXPECT_TRUE(copied.is_geo_bounds());
            EXPECT_TRUE(copied.is_full_text());EXPECT_TRUE(copied.is_indexed());EXPECT_TRUE(copied.is_unique());
            EXPECT_TRUE(copied.is_union());EXPECT_TRUE(copied.no_history());
            const auto absent_property=owner.get_property_info(table,missing);EXPECT_FALSE(absent_property.valid());
            const auto absent_table=owner.get_property_info(missing,column);EXPECT_FALSE(absent_table.valid());
            EXPECT_EQ(property.name.data(),original_name);EXPECT_EQ(property.kind,original_kind);
            EXPECT_EQ(property.union_desc.cases.size(),original_cases);
            EXPECT_TRUE(property.nullable);EXPECT_TRUE(property.is_vector);EXPECT_TRUE(property.is_geo_bounds);
            EXPECT_TRUE(property.is_full_text);EXPECT_TRUE(property.is_indexed);EXPECT_TRUE(property.is_unique);
            EXPECT_TRUE(property.is_union);EXPECT_TRUE(property.no_history);
        }
    }
    EXPECT_EQ(counter.calls,0u);EXPECT_EQ(counter.bytes,0u);
    EXPECT_NO_THROW(require_actual_registries(owner));
    // Restoring the owner's flags cannot change an already returned summary.
    EXPECT_TRUE(snapshot.nullable());EXPECT_TRUE(snapshot.is_vector());EXPECT_TRUE(snapshot.is_geo_bounds());
    EXPECT_TRUE(snapshot.is_full_text());EXPECT_TRUE(snapshot.is_indexed());EXPECT_TRUE(snapshot.is_unique());
    EXPECT_TRUE(snapshot.is_union());EXPECT_TRUE(snapshot.no_history());owner.close();
}
TEST(PendingSwiftRegistryPopulation, ActualOwnedPropertyInfoKeeps256BytesAndRefuses257WithoutTruncation) {
    auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
    lattice::swift_lattice owner(config,schemas);ASSERT_NO_THROW(require_actual_registries(owner));
    auto* properties=const_cast<lattice::RuntimeSwiftSchema*>(owner.get_properties_for_table("RegistryOwner"));
    ASSERT_NE(properties,nullptr);auto& property=properties->at("choice");
    const std::string table="RegistryOwner",column="choice";
    // Allocate replacements BEFORE selecting the real prospective debit scope.
    // DATA faults reach the real getter without pretending 257 is admitted DDL.
    lattice::detail::swift_runtime_text exact(256,'n'),oversize(257,'x');
    std::array<std::uint8_t,256> expected{};
    for(std::size_t index=0;index<expected.size();++index){expected[index]=static_cast<std::uint8_t>('A'+index%26);exact[index]=static_cast<char>(expected[index]);}
    actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=1;
    lattice::swift_property_info accepted;
    {
        actual_property_info_name_restore restore(property.name);
        property.name.swap(exact);const auto* const exact_storage=property.name.data();
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            accepted=owner.get_property_info(table,column);EXPECT_TRUE(accepted.valid());EXPECT_EQ(accepted.name_size(),256u);
            for(std::size_t index=0;index<expected.size();++index){
                EXPECT_EQ(accepted.name_byte(index),expected[index]);EXPECT_EQ(static_cast<std::uint8_t>(property.name[index]),expected[index]);
            }
            EXPECT_EQ(accepted.name_byte(256),-1);EXPECT_EQ(property.name.size(),256u);EXPECT_EQ(property.name.data(),exact_storage);
        }
        property.name.swap(oversize);const auto* const oversize_storage=property.name.data();
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            const auto refused=owner.get_property_info(table,column);const auto copied=refused;
            EXPECT_FALSE(refused.valid());EXPECT_FALSE(copied.valid());EXPECT_EQ(copied.name_size(),0u);
            EXPECT_EQ(copied.name_byte(0),-1);EXPECT_EQ(copied.name_byte(256),-1);EXPECT_EQ(copied.kind_value(),-1);
            EXPECT_EQ(copied.union_case_count(),0u);EXPECT_FALSE(copied.nullable());EXPECT_FALSE(copied.is_vector());
            EXPECT_FALSE(copied.is_geo_bounds());EXPECT_FALSE(copied.is_full_text());EXPECT_FALSE(copied.is_indexed());
            EXPECT_FALSE(copied.is_unique());EXPECT_FALSE(copied.is_union());EXPECT_FALSE(copied.no_history());
            EXPECT_EQ(property.name.size(),257u);EXPECT_EQ(property.name.data(),oversize_storage);
            for(const auto byte:property.name)EXPECT_EQ(byte,'x');
        }
    }
    EXPECT_EQ(counter.calls,0u);EXPECT_EQ(counter.bytes,0u);EXPECT_NO_THROW(require_actual_registries(owner));
    const auto copied=accepted;EXPECT_TRUE(copied.valid());EXPECT_EQ(copied.name_size(),256u);
    for(std::size_t index=0;index<expected.size();++index)EXPECT_EQ(copied.name_byte(index),expected[index]);
    owner.close();
}
TEST(PendingSwiftRegistryPopulation, ActualOwnedPropertyInfoAndCopiesRemainCorrectAfterOwnerDestruction) {
    lattice::swift_property_info snapshot;
    {
        auto schemas=actual_registry_schemas();lattice::swift_configuration config(":memory:");config.audit_retention_seconds=0;
        lattice::swift_lattice owner(config,schemas);const std::string table="RegistryOwner",column="choice";
        actual_core_registry_counter counter;counter.owner=&owner;counter.deny_at=1;
        {
            lattice::detail::ordinary_initialization_storage_scope debit(&counter,&owner,actual_core_registry_counter::debit);
            snapshot=owner.get_property_info(table,column);EXPECT_TRUE(snapshot.valid());
        }
        EXPECT_EQ(counter.calls,0u);EXPECT_EQ(counter.bytes,0u);owner.close();
    } // The actual stack swift_lattice and its runtime descriptors are destroyed.
    const auto copied=snapshot;const std::array<std::uint8_t,6> expected{'c','h','o','i','c','e'};
    EXPECT_TRUE(snapshot.valid());EXPECT_TRUE(copied.valid());EXPECT_EQ(copied.name_size(),expected.size());
    for(std::size_t index=0;index<expected.size();++index)EXPECT_EQ(copied.name_byte(index),expected[index]);
    EXPECT_EQ(copied.name_byte(expected.size()),-1);EXPECT_EQ(copied.name_byte(std::numeric_limits<std::size_t>::max()),-1);
    EXPECT_EQ(copied.kind_value(),static_cast<std::int32_t>(lattice::property_kind::union_type));
    EXPECT_TRUE(copied.is_union());EXPECT_EQ(copied.union_case_count(),2u);EXPECT_FALSE(copied.no_history());
    const lattice::swift_property_info invalid;EXPECT_FALSE(invalid.valid());EXPECT_EQ(invalid.name_size(),0u);
    EXPECT_EQ(invalid.name_byte(0),-1);EXPECT_EQ(invalid.kind_value(),-1);EXPECT_EQ(invalid.union_case_count(),0u);
}
