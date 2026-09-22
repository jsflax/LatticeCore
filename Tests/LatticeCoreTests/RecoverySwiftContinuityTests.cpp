#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_producer_continuity.hpp"
#include <chrono>
#include <future>

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
using namespace lattice;
using namespace lattice::detail;
SchemaVector continuous_swift_schemas() {
    SchemaVector out;
    for(const char* name:{"ContinuousSwiftRow","ContinuousSwiftLocal"}){
        swift_schema_entry entry;entry.table_name=name;
        property_descriptor p{};p.name="value";p.type=column_type::text;p.kind=property_kind::primitive;
        entry.properties[p.name]=p;
        p.name="note";p.no_history=true;entry.properties[p.name]=p;
        out.push_back(entry);
    }
    return out;
}
continuous_policy continuous_swift_policy() {
    continuous_policy out;
    out.scopes=4;out.records=128;out.field_bytes=128;out.journal_bytes=2*1024*1024;
    out.channels=4;out.binding_field_bytes=128;out.binding_bytes=8192;
    out.profiles=4;out.stamps=128;out.producer_field_bytes=128;out.manifest_bytes=1048576;out.producer_bytes=8*1024*1024;
    out.owners=8;out.physical_routes=4;out.operations=4;out.frozen_entries=128;out.frozen_bytes=2*1024*1024;
    for(const char* name:{"a","b"}){
        continuous_contribution c;c.channel=name;c.authority="authority";c.source="source";c.epoch="epoch";
        c.scope=std::string("scope-")+name;c.schema="schema";c.profile_digest=std::string("grant-")+name;
        c.receipt_namespace="shared-receipts";c.models={"ContinuousSwiftRow"};c.incoming_grant_claim={'g'};
        out.contributions.push_back(c);
        out.routes.push_back({std::string("wss:wss://continuous-swift.invalid/")+name,std::string("wss://continuous-swift.invalid/")+name});
    }
    return out;
}
std::unique_ptr<swift_lattice_ref> open_swift_continuous(const swift_configuration& config,const SchemaVector& schemas,
    const continuous_policy& policy,continuous_result& result) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create_continuous(config,schemas,policy,result));
#else
    return std::make_unique<swift_lattice_ref>(swift_lattice_ref::create_continuous(config,schemas,policy,result));
#endif
}
void require_swift_commit(const continuous_result& result) {
    if(result.phase()!=2||result.has_error())throw db_error("Swift continuity: "+result.primary_error()+result.cleanup_error()+result.postcommit_error()+result.notification_error());
}
int64_t scalar(swift_lattice& owner,const std::string& sql) {return std::get<int64_t>(owner.db().query(sql).at(0).at("n"));}
class swift_continuity_transport final:public sync_transport {
    std::atomic<transport_state> state_{transport_state::closed};
public:
    void connect(const std::string&,const std::map<std::string,std::string>&)override{state_=transport_state::connecting;}
    void disconnect()override{state_=transport_state::closed;}
    transport_state state()const override{return state_.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message&)override{}
    void set_on_open(on_open_handler)override{}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
class swift_continuity_network final:public network_factory {
public:
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{return std::make_unique<swift_continuity_transport>();}
};
class RecoverySwiftContinuity:public ::testing::Test {
protected:
    TempDB unique{"swift_continuity"};
    std::filesystem::path container=unique.str()+".lattice-continuous";
    SchemaVector schemas=continuous_swift_schemas();
    continuous_policy policy=continuous_swift_policy();
    std::vector<std::unique_ptr<swift_lattice_ref>> facades;
    std::shared_ptr<network_factory> prior_factory;
    swift_configuration config(){swift_configuration out((container/"store.sqlite").string(),std::make_shared<immediate_scheduler>());out.audit_retention_seconds=0;out.busy_timeout_ms=100;return out;}
    swift_lattice_ref& open(){continuous_result result;auto ref=open_swift_continuous(config(),schemas,policy,result);require_swift_commit(result);
        if(!ref||!ref->valid())throw db_error("missing actual Swift continuity facade");
        facades.push_back(std::move(ref));stop_notifier();return *facades.back();}
    void stop_notifier(){if(auto* n=instance_registry::instance().get_or_create_notifier(facades.back()->get()->config().path))n->stop_listening();}
    void insert(swift_lattice_ref& ref,const std::string& value="before",const std::string& table="ContinuousSwiftRow"){
        auto* owner=ref.get();owner->begin_transaction();
        try{swift_dynamic_object row;row.table_name=table;row.properties=schemas.at(table=="ContinuousSwiftRow"?0:1).properties;
            row.values["value"]=value;row.values["note"]=std::string("private");dynamic_object object(row);owner->add(object);owner->commit();}
        catch(...){if(owner->db().is_in_transaction())owner->rollback();throw;}
    }
    auto snapshot(swift_lattice_ref& ref){auto& db=ref.get()->db();std::vector<std::vector<database::row_t>> out;
        out.push_back(db.query("PRAGMA schema_version"));out.push_back(db.query("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name"));
        for(const char* name:{"_lattice_producer_continuity","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_profile","_lattice_obligation_producer_stamp","AuditLog","ContinuousSwiftRow"})
            out.push_back(db.query(std::string("SELECT * FROM ")+name+" ORDER BY 1,2"));return out;}
    void SetUp()override{prior_factory=get_network_factory();set_network_factory(std::make_shared<swift_continuity_network>());}
    void TearDown()override{for(auto& ref:facades)if(ref&&ref->valid())ref->get()->close();facades.clear();set_network_factory(prior_factory);std::error_code error;std::filesystem::remove_all(container,error);}
};
TEST_F(RecoverySwiftContinuity, ActualDerivedOwnerAndManagedRowsRetainCompleteCatalog) {
    auto& ref=open();auto actual=swift_lattice_ref::shared_for_lattice(ref.get());
    ASSERT_TRUE(actual);EXPECT_EQ(actual.get(),ref.get());
    insert(ref);auto rows=ref.get()->objects("ContinuousSwiftRow");ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(rows[0].lattice_shared(),actual);EXPECT_EQ(rows[0].get_string("value"),"before");
    EXPECT_TRUE(actual->get_properties_for_table("ContinuousSwiftRow")->at("note").no_history);
    EXPECT_EQ(scalar(*actual,"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp"),2);
    auto begun=ref.begin_continuous(1);require_swift_commit(begun);ASSERT_TRUE(begun.barrier().valid());
    auto done=begun.barrier().finish();require_swift_commit(done);EXPECT_TRUE(done.frozen());EXPECT_EQ(done.local_unsent_count(),1);
    require_swift_commit(done.barrier().cancel());
}
TEST_F(RecoverySwiftContinuity, ManagedDynamicObjectRetainsActualOwnerAfterFacadeRelease) {
    auto& ref=open();insert(ref);auto rows=ref.get()->objects("ContinuousSwiftRow");ASSERT_EQ(rows.size(),1u);
    dynamic_object held(rows[0]);std::weak_ptr<swift_lattice> weak=held.lattice;
    ASSERT_TRUE(held.lattice);EXPECT_EQ(held.lattice.get(),ref.get());facades.clear();
    EXPECT_FALSE(weak.expired());EXPECT_EQ(held.get_string("value"),"before");
    held.lattice->close();
}
TEST_F(RecoverySwiftContinuity, TwoRealSwiftFacadesShareRowsAndOneBarrier) {
    auto& first=open();auto& second=open();EXPECT_NE(first.hash_value(),second.hash_value());
    insert(first,"one");insert(second,"two");EXPECT_EQ(second.get()->objects("ContinuousSwiftRow").size(),2u);
    auto begun=first.begin_continuous(1);require_swift_commit(begun);
    EXPECT_THROW(insert(second,"refused"),db_error);
    EXPECT_NO_THROW(insert(second,"local","ContinuousSwiftLocal"));
    auto done=begun.barrier().finish();require_swift_commit(done);EXPECT_EQ(done.local_unsent_count(),2);
    require_swift_commit(done.barrier().cancel());insert(second,"three");
    EXPECT_EQ(scalar(*first.get(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp"),6);
}
TEST_F(RecoverySwiftContinuity, ExactClosedReopenRetainsBarrierAndFencesOldOwner) {
    continuous_barrier old;
    {auto& ref=open();insert(ref);auto start=ref.begin_continuous(1);require_swift_commit(start);old=start.barrier();
     auto frozen=old.finish();require_swift_commit(frozen);ref.get()->close();facades.clear();}
    // old retains the closed physical owner; it cannot be silently rebound.
    EXPECT_TRUE(old.finish().has_error());old=continuous_barrier{};
    auto& fresh=open();auto inspected=fresh.inspect_continuous();require_swift_commit(inspected);ASSERT_TRUE(inspected.barrier().valid());
    EXPECT_THROW(insert(fresh,"closed"),db_error);
    auto frozen=inspected.barrier().finish();require_swift_commit(frozen);EXPECT_EQ(frozen.local_unsent_count(),1);
    require_swift_commit(frozen.barrier().cancel());insert(fresh,"after");
    EXPECT_EQ(scalar(*fresh.get(),"SELECT COUNT(*) AS n FROM ContinuousSwiftRow"),2);
}
TEST_F(RecoverySwiftContinuity, FullNoHistoryAndConstraintChangesRefuseWithoutDDLOrDataChange) {
    auto& ref=open();insert(ref);const auto before=snapshot(ref);
    auto changed=schemas;changed[0].properties.at("note").no_history=false;
    continuous_result result;auto refused=open_swift_continuous(config(),changed,policy,result);
    EXPECT_TRUE(result.has_error());EXPECT_FALSE(refused&&refused->valid());EXPECT_EQ(before,snapshot(ref));
    changed=schemas;changed[0].constraints.push_back(swift_constraint({"value"},true));
    refused=open_swift_continuous(config(),changed,policy,result);
    EXPECT_TRUE(result.has_error());EXPECT_FALSE(refused&&refused->valid());EXPECT_EQ(before,snapshot(ref));
    auto c=config();++c.target_schema_version;
    refused=open_swift_continuous(c,schemas,policy,result);
    EXPECT_TRUE(result.has_error());EXPECT_FALSE(refused&&refused->valid());EXPECT_EQ(before,snapshot(ref));
}
TEST_F(RecoverySwiftContinuity, CallerSchemaMutationDoesNotChangeRetainedCatalog) {
    auto& ref=open();schemas[0].properties.at("note").no_history=false;
    EXPECT_TRUE(ref.get()->get_properties_for_table("ContinuousSwiftRow")->at("note").no_history);
    auto unchanged=continuous_swift_schemas();continuous_result result;auto second=open_swift_continuous(config(),unchanged,policy,result);
    require_swift_commit(result);ASSERT_TRUE(second&&second->valid());second->get()->close();
}
TEST_F(RecoverySwiftContinuity, MigrationAndOversizedRecipeRefuseBeforeContainerCreation) {
    auto c=config();c.setRowMigrationCallback(nullptr,[](const char*,void*){});
    continuous_result result;auto refused=open_swift_continuous(c,schemas,policy,result);
    EXPECT_TRUE(result.has_error());EXPECT_FALSE(refused&&refused->valid());EXPECT_FALSE(std::filesystem::exists(container));
    schemas[0].table_name=std::string(1048577,'x');
    refused=open_swift_continuous(config(),schemas,policy,result);
    EXPECT_TRUE(result.has_error());EXPECT_FALSE(refused&&refused->valid());EXPECT_FALSE(std::filesystem::exists(container));
}
TEST_F(RecoverySwiftContinuity, OrdinarySwiftFactoryAndLegacyNativeCannotReattach) {
    auto& ref=open();insert(ref);const auto before=snapshot(ref);
    EXPECT_THROW((swift_lattice(config(),schemas)),db_error);
    EXPECT_THROW((lattice_db(config())),db_error);
    EXPECT_EQ(before,snapshot(ref));
}
TEST_F(RecoverySwiftContinuity, ConfiguredWSSFacadeOwnsSameDerivedSchemaBeforeRoutePublication) {
    auto c=config();c.websocket_url=policy.routes[0].endpoint;
    continuous_result result;auto ref=open_swift_continuous(c,schemas,policy,result);require_swift_commit(result);
    ASSERT_TRUE(ref&&ref->valid());facades.push_back(std::move(ref));stop_notifier();
    std::vector<std::shared_ptr<swift_lattice>> owners;
    instance_registry::instance().for_each_alive(facades.front()->get()->config().path,[&](lattice_db* raw){auto actual=swift_lattice_ref::shared_for_lattice(raw);if(actual)owners.push_back(actual);});
    ASSERT_EQ(owners.size(),2u);EXPECT_NE(owners[0],owners[1]);
    for(const auto& owner:owners){ASSERT_TRUE(owner->get_properties_for_table("ContinuousSwiftRow"));EXPECT_TRUE(owner->get_properties_for_table("ContinuousSwiftRow")->at("note").no_history);}
    insert(*facades.front());
    EXPECT_EQ(owners[0]->objects("ContinuousSwiftRow").size(),1u);EXPECT_EQ(owners[1]->objects("ContinuousSwiftRow").size(),1u);
}
TEST_F(RecoverySwiftContinuity, FailedRoutePublicationReportsKnownCommitAndExactReopen) {
    policy.owners=1;auto c=config();c.websocket_url=policy.routes[0].endpoint;
    continuous_result result;auto refused=open_swift_continuous(c,schemas,policy,result);
    EXPECT_FALSE(refused&&refused->valid());EXPECT_EQ(result.phase(),2);EXPECT_FALSE(result.postcommit_error().empty());
    EXPECT_TRUE(result.primary_error().empty());EXPECT_TRUE(std::filesystem::exists(container/"store.sqlite"));
    // Equivalent explicit reopen omits dialing. It validates the committed
    // profile; it does not adopt an orphan or overwrite a failed enrollment.
    auto& fresh=open();EXPECT_EQ(scalar(*fresh.get(),"SELECT COUNT(*) AS n FROM _lattice_producer_continuity"),1);
    insert(fresh);auto start=fresh.begin_continuous(1);require_swift_commit(start);
    auto frozen=start.barrier().finish();require_swift_commit(frozen);EXPECT_EQ(frozen.local_unsent_count(),1);
    require_swift_commit(frozen.barrier().cancel());
}
TEST_F(RecoverySwiftContinuity, FailedBarrierWriteRetainsFailureAndClosesAdmission) {
    auto& ref=open();insert(ref);
    recovery_local_producer_test_hooks::authorizer_fault fault{ref.get(),[](int action,const char* table,const char* column,const char*)noexcept{
        return action==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_producer_continuity")==0&&std::strcmp(column,"phase")==0?SQLITE_IGNORE:SQLITE_OK;}};
    const auto prior=recovery_local_producer_test_hooks::fault;
    struct restore{const recovery_local_producer_test_hooks::authorizer_fault* value;~restore(){recovery_local_producer_test_hooks::fault=value;}} reset{prior};
    recovery_local_producer_test_hooks::fault=&fault;
    auto failed=ref.begin_continuous(1);EXPECT_NE(failed.phase(),2);EXPECT_TRUE(failed.has_error());EXPECT_FALSE(failed.barrier().valid());
    recovery_local_producer_test_hooks::fault=prior;
    EXPECT_THROW(insert(ref,"closed"),db_error);
    EXPECT_EQ(scalar(*ref.get(),"SELECT COUNT(*) AS n FROM ContinuousSwiftRow"),1);
}
TEST_F(RecoverySwiftContinuity, DefaultBarrierCannotConstructAdmission) {
    continuous_barrier absent;EXPECT_FALSE(absent.valid());EXPECT_TRUE(absent.finish().has_error());EXPECT_TRUE(absent.cancel().has_error());
    EXPECT_FALSE(std::filesystem::exists(container));
}
} // namespace
#endif
