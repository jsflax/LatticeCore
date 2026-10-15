#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <exception>

struct ExportDiscoveryRow { std::string value; };
LATTICE_SCHEMA(ExportDiscoveryRow,value);

namespace {
using namespace lattice;
using namespace lattice::detail;
const recovery_obligation_producer_discovery_limits caps{
    {4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
std::shared_ptr<lattice_db> open_discovery(const std::string& path) {
    configuration config(path);config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(config);
    if(!config.is_in_memory()) {
        auto* notifier=instance_registry::instance().get_or_create_notifier(path);
        if(notifier)notifier->stop_listening();
    }
    return owner;
}
void require_discovery_commit(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.state!=recovery_install_state::committed)
        throw std::runtime_error("discovery fixture did not commit");
}
recovery_obligation_address enroll_discovery(const std::shared_ptr<lattice_db>& owner) {
    recovery_obligation_address address;
    require_discovery_commit(recovery_writer_access::install(owner,[&](database&) {
        receive_install_store receiver(owner,caps.installations);receiver.initialize();
        receive_install_binding binding{"discovery","authority","source","epoch","scope","schema"};
        receiver.bind(binding);
        recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();
        address=journal.bind({binding,"grant","receipts"}).address;
    }));
    require_discovery_commit(recovery_local_producer_adapter::enroll_for_qualification(
        owner,{address,{"ExportDiscoveryRow"},{'g'}},caps));
    return address;
}
int64_t total_changes(lattice_db& owner) {
    return std::get<int64_t>(owner.db().query("SELECT total_changes() AS n").at(0).at("n"));
}
struct discovery_observers {
    std::shared_ptr<lattice_db> owner;
    uint64_t audit=0,model=0,invalidation=0;
    ~discovery_observers() {
        if(audit)owner->remove_table_observer("AuditLog",audit);
        if(model)owner->remove_table_observer("ExportDiscoveryRow",model);
        if(invalidation)owner->remove_invalidation_hook(invalidation);
    }
};
void ordinary_callback_discovery(const std::string& path,bool retired) {
    auto owner=open_discovery(path);
    if(retired) {
        const auto address=enroll_discovery(owner);
        require_discovery_commit(recovery_local_producer_adapter::retire_for_qualification(owner,address,caps));
    }
    int audit_calls=0,model_inserts=0,commits=0;
    std::exception_ptr error;
    discovery_observers observers{owner};
    observers.invalidation=owner->add_invalidation_hook_detailed([&](const auto&,auto reason) {
        if(reason==lattice_db::invalidation_reason::commit)++commits;
    });
    observers.model=owner->add_table_observer("ExportDiscoveryRow",[&](const auto& batch) {
        for(const auto& event:batch)if(std::get<1>(event)=="INSERT")++model_inserts;
    });
    observers.audit=owner->add_table_observer("AuditLog",[&](const auto& batch) {
        if(batch.empty())return;
        ++audit_calls;
        try {
            const auto before=total_changes(*owner);
            EXPECT_FALSE(recovery_export_adapter::protected_store(owner));
            auto preparation=recovery_export_adapter::prepare_pending(owner,"legacy-route",1,10,{},false);
            EXPECT_FALSE(preparation.protected_store);
            EXPECT_FALSE(preparation.frame);
            EXPECT_EQ(total_changes(*owner),before);
        }catch(...){error=std::current_exception();}
    });
    const auto row=owner->add(ExportDiscoveryRow{"during-original-notifications"});
    if(error)try{std::rethrow_exception(error);}catch(const std::exception& e){ADD_FAILURE()<<e.what();}
    EXPECT_EQ(audit_calls,1);
    EXPECT_EQ(model_inserts,1);
    EXPECT_EQ(commits,1);
    EXPECT_EQ(owner->query_read("SELECT value FROM ExportDiscoveryRow WHERE globalId=?",{row.global_id()}).size(),1u);
    EXPECT_FALSE(owner->db().is_in_transaction());
}
}

TEST(RecoveryExportDiscovery, MemoryLegacyDiscoveryPreservesOriginalNotifications) {
    ordinary_callback_discovery(":memory:",false);
}
TEST(RecoveryExportDiscovery, FileLegacyDiscoveryPreservesOriginalNotifications) {
    TempDB file{"export_discovery_file"};ordinary_callback_discovery(file.str(),false);
}
TEST(RecoveryExportDiscovery, MemoryRetiredFamilyPreservesOriginalNotifications) {
    ordinary_callback_discovery(":memory:",true);
}
TEST(RecoveryExportDiscovery, FileRetiredFamilyPreservesOriginalNotifications) {
    TempDB file{"export_discovery_retired"};ordinary_callback_discovery(file.str(),true);
}

TEST(RecoveryExportDiscovery, PreopenedSiblingCannotClassifyEnrolledStoreAsLegacy) {
    TempDB file{"export_discovery_sibling"};
    auto stale=open_discovery(file.str());auto current=open_discovery(file.str());
    enroll_discovery(current);
    bool legacy=false;
    try{legacy=!recovery_export_adapter::protected_store(stale);}catch(const db_error&){}
    EXPECT_FALSE(legacy);
    EXPECT_THROW(recovery_export_adapter::prepare_pending(stale,"route",1,10,{},false),db_error);
    EXPECT_TRUE(recovery_export_adapter::protected_store(current));
    EXPECT_FALSE(stale->db().is_in_transaction());
}

TEST(RecoveryExportDiscovery, StaleReadSnapshotCannotReportAbsenceAfterSiblingEnrollment) {
    TempDB file{"export_discovery_stale_read"};
    auto stale=open_discovery(file.str());auto current=open_discovery(file.str());
    stale->db().execute("BEGIN DEFERRED");
    stale->db().query("SELECT name FROM main.sqlite_schema LIMIT 1");
    enroll_discovery(current);
    EXPECT_THROW(recovery_export_adapter::protected_store(stale),db_error);
    EXPECT_THROW(recovery_export_adapter::prepare_pending(stale,"route",1,10,{},false),db_error);
    EXPECT_TRUE(stale->db().is_in_transaction());
    stale->db().execute("ROLLBACK");
    EXPECT_TRUE(recovery_export_adapter::protected_store(current));
}

TEST(RecoveryExportDiscovery, IncompleteProducerFamilyCannotBecomeLegacyAbsence) {
    auto owner=open_discovery(":memory:");
    owner->db().execute("CREATE TABLE _lattice_obligation_producer_profile(channel BLOB)");
    const auto before=owner->db().query("SELECT name,sql FROM sqlite_schema ORDER BY name");
    EXPECT_THROW(recovery_export_adapter::protected_store(owner),db_error);
    EXPECT_THROW(recovery_export_adapter::prepare_pending(owner,"route",1,10,{},false),db_error);
    EXPECT_EQ(owner->db().query("SELECT name,sql FROM sqlite_schema ORDER BY name"),before);
    EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST(RecoveryExportDiscovery, EnrolledCallbackStillCannotEnterPrivateClaimInstallation) {
    auto owner=open_discovery(":memory:");const auto address=enroll_discovery(owner);
    int calls=0;std::string refusal;
    discovery_observers observers{owner};
    observers.audit=owner->add_table_observer("AuditLog",[&](const auto& batch) {
        if(batch.empty())return;
        ++calls;
        try{(void)recovery_export_adapter::prepare_pending(owner,"route",1,10,{},false);}
        catch(const db_error& e){refusal=e.what();}
    });
    owner->add(ExportDiscoveryRow{"protected-callback"});
    EXPECT_EQ(calls,1);
    EXPECT_NE(refusal.find("existing notification delivery is unsettled"),std::string::npos);
    EXPECT_TRUE(owner->db().query("SELECT original FROM _lattice_obligation_entry WHERE first_export IS NOT NULL").empty());
    auto outside=recovery_export_adapter::prepare_pending(owner,"route",1,10,{},false);
    ASSERT_TRUE(outside.protected_store);ASSERT_TRUE(outside.frame);
    EXPECT_EQ(outside.frame->entries().size(),1u);
    (void)address;
}
