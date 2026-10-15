#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include <lattice/sync.hpp>
#include <functional>
#include <cstring>

struct LegacyAdmissionRow {std::string value;};
LATTICE_SCHEMA(LegacyAdmissionRow,value);

namespace {
using namespace lattice;
using namespace lattice::detail;
std::shared_ptr<lattice_db> legacy_owner(const std::string& path=":memory:") {
    configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(c);owner->add(LegacyAdmissionRow{"seed"});
    if(path!=":memory:")if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
    return owner;
}
int64_t number(database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).at("n"));}
void require_commit(const recovery_install_result& r) {
    if(r.primary_error)std::rethrow_exception(r.primary_error);
    if(r.cleanup_error)std::rethrow_exception(r.cleanup_error);
    if(r.postcommit_error)std::rethrow_exception(r.postcommit_error);
    if(r.state!=recovery_install_state::committed)throw std::runtime_error("fixture did not commit");
}
void enroll(const std::shared_ptr<lattice_db>& owner) {
    recovery_obligation_producer_discovery_limits limits{{4,256,128,2*1024*1024},{4,128,8192},{4,256,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    require_commit(recovery_writer_access::install(owner,[&](database&) {
        receive_install_store receiver(owner,limits.installations);receiver.initialize();
        receive_install_binding binding{"incoming","authority","source","epoch","scope","schema"};receiver.bind(binding);
        recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.initialize();
        address=journal.bind({binding,"grant","receipts"}).address;
    }));
    require_commit(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"LegacyAdmissionRow"},{'g'}},limits));
}
struct admission_hook {
    static thread_local std::function<void()>* current;
    std::function<void()> action;std::function<void()>* previous;
    void (*old_capture)();void (*old_admitted)();
    static void invoke(){(*current)();}
    admission_hook(std::function<void()> f,bool after_write=false):action(std::move(f)),previous(current),
        old_capture(legacy_sync_write_test_hooks::after_writer_capture),old_admitted(legacy_sync_write_test_hooks::after_write_admission) {
        current=&action;
        if(after_write)legacy_sync_write_test_hooks::after_write_admission=&invoke;
        else legacy_sync_write_test_hooks::after_writer_capture=&invoke;
    }
    ~admission_hook(){current=previous;legacy_sync_write_test_hooks::after_writer_capture=old_capture;legacy_sync_write_test_hooks::after_write_admission=old_admitted;}
};
thread_local std::function<void()>* admission_hook::current=nullptr;
void expected_refusal(const std::function<void()>& work,const std::string& stage) {
    try{work();FAIL()<<"expected refusal: "<<stage;}
    catch(const db_error& error){EXPECT_NE(std::string(error.what()).find(stage),std::string::npos)<<error.what();}
}
}

TEST(LegacySyncWriteAdmission, MetadataUnitsCommitOnMemoryAndFileWithoutProfiles) {
    TempDB file{"legacy_metadata_units"};
    for(const auto& path:{std::string(":memory:"),file.str()}) {
        SCOPED_TRACE(path);
        auto owner=legacy_owner(path);
        register_replication_slot(owner->db(),"legacy",false);
        advance_replication_slot(owner->db(),"legacy",7);
        advance_upload_floor(owner->db(),"legacy",6);
        set_replication_slot_observer(owner->db(),"legacy",true);
        EXPECT_EQ(number(owner->db(),"SELECT confirmed_audit_id AS n FROM _lattice_replication_slots WHERE sync_id='legacy'"),7);
        EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),6);
        EXPECT_EQ(number(owner->db(),"SELECT is_observer AS n FROM _lattice_replication_slots WHERE sync_id='legacy'"),1);
        EXPECT_FALSE(owner->db().is_in_transaction());
        remove_replication_slot(owner->db(),"legacy");
        EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_replication_slots WHERE sync_id='legacy'"),0);
    }
}

TEST(LegacySyncWriteAdmission, ExistingCallerOwnsSuccessAndMidBodyFailureUntilRollback) {
    auto owner=legacy_owner();register_replication_slot(owner->db(),"legacy",false);
    owner->begin_transaction();
    ASSERT_NO_THROW(advance_upload_floor(owner->db(),"legacy",3));
    EXPECT_EQ(recovery_writer_access::active_writer(*owner),&owner->db());
    EXPECT_THROW(recovery_writer_access::legacy_sync_write(*owner,[&](database& writer) {
        writer.execute("UPDATE _lattice_replication_slots SET upload_floor=9 WHERE sync_id='legacy'");
        throw std::runtime_error("caller owns this partial turn");
    }),std::runtime_error);
    EXPECT_EQ(recovery_writer_access::active_writer(*owner),&owner->db());
    EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),9);
    owner->rollback();EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
}

TEST(LegacySyncWriteAdmission, EnrolledCallerRefusalDoesNotConsumeItsOrdinaryWrite) {
    auto owner=legacy_owner();register_replication_slot(owner->db(),"legacy",false);enroll(owner);
    owner->begin_transaction();owner->add(LegacyAdmissionRow{"caller pending"});
    expected_refusal([&]{advance_upload_floor(owner->db(),"legacy",10);},"producer");
    EXPECT_EQ(recovery_writer_access::active_writer(*owner),&owner->db());
    EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE value='caller pending'"),1);
    owner->rollback();
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE value='caller pending'"),0);
}

TEST(LegacySyncWriteAdmission, PreopenedOwnerRechecksSiblingEnrollmentAfterCapture) {
    TempDB file{"legacy_late_enrollment"};auto older=legacy_owner(file.str());auto newer=legacy_owner(file.str());
    register_replication_slot(older->db(),"legacy",false);
    int captures=0,bodies=0;
    {
        admission_hook hook([&]{++captures;enroll(newer);});
        expected_refusal([&]{recovery_writer_access::legacy_sync_write(*older,[&](database& writer) {
            ++bodies;writer.execute("UPDATE _lattice_replication_slots SET upload_floor=99 WHERE sync_id='legacy'");
        });},"producer");
    }
    EXPECT_EQ(captures,1);EXPECT_EQ(bodies,0);EXPECT_EQ(read_upload_floor(older->db(),"legacy"),0);
    EXPECT_FALSE(older->db().is_in_transaction());
    EXPECT_EQ(number(newer->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_profile"),1);
}

TEST(LegacySyncWriteAdmission, ReopenedAckChunkCannotMutateAfterSiblingEnrollment) {
    TempDB file{"legacy_ack_chunk_enrollment"};auto older=legacy_owner(file.str());auto newer=legacy_owner(file.str());
    for(int i=0;i<101;++i)older->add(LegacyAdmissionRow{"ack-unit"});
    std::vector<std::string> ids;
    for(const auto& row:older->db().query("SELECT a.globalId FROM AuditLog a JOIN LegacyAdmissionRow r ON a.globalRowId=r.globalId WHERE r.value='ack-unit' ORDER BY a.id"))
        ids.push_back(std::get<std::string>(row.at("globalId")));
    ASSERT_EQ(ids.size(),101u);int captures=0;
    {
        admission_hook hook([&]{if(++captures==2)enroll(newer);});
        expected_refusal([&]{mark_audit_entries_synced(*older,ids);},"producer");
    }
    EXPECT_EQ(captures,2);
    EXPECT_EQ(number(older->db(),"SELECT COUNT(*) AS n FROM AuditLog a JOIN LegacyAdmissionRow r ON a.globalRowId=r.globalId WHERE r.value='ack-unit' AND a.isSynchronized=1"),100);
    EXPECT_EQ(number(older->db(),"SELECT COUNT(*) AS n FROM AuditLog a JOIN LegacyAdmissionRow r ON a.globalRowId=r.globalId WHERE r.value='ack-unit' AND a.isSynchronized=0"),1);
    EXPECT_FALSE(older->db().is_in_transaction());
}

TEST(LegacySyncWriteAdmission, RawExistingWriteRetainsCallerResponsibilityWithoutRecoveryAuthority) {
    auto owner=legacy_owner();register_replication_slot(owner->db(),"legacy",false);
    owner->db().begin_transaction();int bodies=0;
    ASSERT_NO_THROW(recovery_writer_access::legacy_sync_write(owner->db(),[&](database& writer) {
        ++bodies;EXPECT_EQ(recovery_writer_access::active_writer(*owner),nullptr);
        writer.execute("UPDATE _lattice_replication_slots SET upload_floor=8 WHERE sync_id='legacy'");
    }));
    EXPECT_EQ(bodies,1);EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),8);
    EXPECT_THROW(recovery_writer_access::legacy_sync_write(owner->db(),[](database& writer) {
        writer.execute("UPDATE _lattice_replication_slots SET upload_floor=10 WHERE sync_id='legacy'");
        throw std::runtime_error("raw caller retains partial work");
    }),std::runtime_error);
    EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),10);
    owner->db().rollback();EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
}

TEST(LegacySyncWriteAdmission, NoWalAdmissionCommitThenSuccessorRefusesEffectsWithoutRollback) {
    auto owner=legacy_owner();int bodies=0;
    {
        admission_hook hook([&] {
            owner->db().commit();owner->begin_transaction();
            owner->db().execute("INSERT INTO LegacyAdmissionRow(globalId,value) VALUES('successor','retained')");
        },true);
        expected_refusal([&]{recovery_writer_access::legacy_sync_write(*owner,[&](database&){++bodies;});},"consumed");
    }
    EXPECT_EQ(bodies,0);EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='successor'"),1);
    owner->rollback();
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='successor'"),0);
}

TEST(LegacySyncWriteAdmission, FilePostcommitThrowNeverRollsBackObserverSuccessor) {
    TempDB file{"legacy_postcommit_successor"};auto owner=legacy_owner(file.str());int callbacks=0;
    const auto token=owner->add_table_observer("LegacyAdmissionRow",[&](const auto&) {
        ++callbacks;owner->begin_transaction();
        owner->db().execute("INSERT INTO LegacyAdmissionRow(globalId,value) VALUES('successor','retained')");
        throw std::runtime_error("original commit observer failed after successor");
    });
    EXPECT_THROW(recovery_writer_access::legacy_sync_write(*owner,[](database& writer) {
        writer.execute("INSERT INTO LegacyAdmissionRow(globalId,value) VALUES('original','committed')");
    }),std::runtime_error);
    owner->remove_table_observer("LegacyAdmissionRow",token);
    EXPECT_EQ(callbacks,1);EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='original'"),1);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='successor'"),1);
    owner->rollback();
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='original'"),1);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM LegacyAdmissionRow WHERE globalId='successor'"),0);
}

TEST(LegacySyncWriteAdmission, HelperOwnedAbortRollsBackAndStandaloneNoHookUnitCommits) {
    auto owner=legacy_owner();register_replication_slot(owner->db(),"legacy",false);
    EXPECT_THROW(recovery_writer_access::legacy_sync_write(*owner,[](database& writer) {
        writer.execute("UPDATE _lattice_replication_slots SET upload_floor=9 WHERE sync_id='legacy'");
        throw std::runtime_error("unit failed");
    }),std::runtime_error);
    EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
    database standalone(":memory:");standalone.execute("CREATE TABLE _private(value INTEGER)");
    recovery_writer_access::legacy_sync_write(standalone,[](database& writer){writer.execute("INSERT INTO _private VALUES(7)");});
    EXPECT_EQ(number(standalone,"SELECT value AS n FROM _private"),7);EXPECT_FALSE(standalone.is_in_transaction());
}

TEST(LegacySyncWriteAdmission, FailedOwnedRollbackRetainsErrorsAndFencesFurtherWrites) {
    auto owner=legacy_owner();register_replication_slot(owner->db(),"legacy",false);
    auto* raw=owner->db().handle();int refused=0;
    sqlite3_set_authorizer(raw,[](void* context,int action,const char* first,const char*,const char*,const char*) {
        if(action==SQLITE_TRANSACTION&&first&&std::strcmp(first,"ROLLBACK")==0){++*static_cast<int*>(context);return SQLITE_DENY;}
        return SQLITE_OK;
    },&refused);
    try {
        recovery_writer_access::legacy_sync_write(*owner,[](database& writer){
            writer.execute("UPDATE _lattice_replication_slots SET upload_floor=9 WHERE sync_id='legacy'");
            throw std::runtime_error("primary body failure");
        });
        FAIL()<<"missing cleanup failure";
    } catch(const legacy_sync_write_error& error) {
        EXPECT_NE(error.primary_error,nullptr);EXPECT_NE(error.cleanup_error,nullptr);
    }
    sqlite3_set_authorizer(raw,nullptr,nullptr);
    EXPECT_EQ(refused,1);EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_THROW(owner->db().commit(),db_error);
    EXPECT_THROW(owner->db().query("UPDATE _lattice_replication_slots SET upload_floor=12 RETURNING upload_floor"),db_error);
    EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),9);
    owner->db().rollback();EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
    ASSERT_NO_THROW(advance_upload_floor(owner->db(),"legacy",4));EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),4);
}

TEST(LegacySyncWriteAdmission, DeferredAndReadCallersUpgradeThenRetainRollbackResponsibility) {
    TempDB file{"legacy_deferred_callers"};
    for(const auto& path:{std::string(":memory:"),file.str()})for(const bool pin_read:{false,true}) {
        SCOPED_TRACE(path);
        SCOPED_TRACE(pin_read);
        auto owner=legacy_owner(path);register_replication_slot(owner->db(),"legacy",false);
        auto* raw=owner->db().handle();owner->db().execute("BEGIN");
        if(pin_read)EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
        EXPECT_EQ(sqlite3_txn_state(raw,"main"),pin_read?SQLITE_TXN_READ:SQLITE_TXN_NONE);
        ASSERT_NO_THROW(advance_upload_floor(owner->db(),"legacy",7));
        EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_EQ(sqlite3_txn_state(raw,"main"),SQLITE_TXN_WRITE);
        EXPECT_EQ(recovery_writer_access::active_writer(*owner),nullptr);
        EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),7);
        owner->db().rollback();EXPECT_EQ(read_upload_floor(owner->db(),"legacy"),0);
    }
}

TEST(LegacySyncWriteAdmission, StaleReadBeforeSiblingEnrollmentCannotUpgradeOrMutateMetadata) {
    TempDB file{"legacy_stale_read_enrollment"};auto older=legacy_owner(file.str());auto newer=legacy_owner(file.str());
    register_replication_slot(older->db(),"legacy",false);
    auto* raw=older->db().handle();older->db().execute("BEGIN");
    EXPECT_EQ(read_upload_floor(older->db(),"legacy"),0);ASSERT_EQ(sqlite3_txn_state(raw,"main"),SQLITE_TXN_READ);
    enroll(newer); // Genuine committed profile/trigger installation after the retained read snapshot.
    int admitted=0,bodies=0;
    {
        admission_hook hook([&]{++admitted;},true);
        expected_refusal([&]{recovery_writer_access::legacy_sync_write(older->db(),[&](database& writer) {
            ++bodies;writer.execute("UPDATE _lattice_replication_slots SET upload_floor=99 WHERE sync_id='legacy'");
        });},"database is locked");
    }
    EXPECT_EQ(admitted,1);EXPECT_EQ(bodies,1); // Passed the old snapshot's absence check, reached the real upgrade.
    EXPECT_EQ(sqlite3_extended_errcode(raw),SQLITE_BUSY_SNAPSHOT);
    EXPECT_TRUE(older->db().is_in_transaction());EXPECT_EQ(sqlite3_txn_state(raw,"main"),SQLITE_TXN_READ);
    EXPECT_EQ(read_upload_floor(older->db(),"legacy"),0);EXPECT_EQ(read_upload_floor(newer->db(),"legacy"),0);
    older->db().rollback();EXPECT_FALSE(older->db().is_in_transaction());
    expected_refusal([&]{advance_upload_floor(older->db(),"legacy",99);},"producer");
    EXPECT_EQ(read_upload_floor(older->db(),"legacy"),0);
}
