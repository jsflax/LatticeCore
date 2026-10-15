#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_refresh.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <chrono>
#include <future>

namespace lattice::detail {
struct recovery_install_admission_test_access {
    static recovery_install_result after_capture(std::shared_ptr<lattice_db> owner,
        const std::function<void(database&)>& body, const std::function<void()>& captured) {
        return recovery_writer_access::install_impl(std::move(owner), body, {}, captured);
    }
};
}
namespace {
using namespace lattice::detail;
using state=recovery_install_state;
std::string error_text(const recovery_install_result& result) {
    if(result.primary_error)try{std::rethrow_exception(result.primary_error);}
    catch(const std::exception& e){return e.what();}catch(...){return "non-standard primary error";}
    return "no primary error";
}
std::shared_ptr<lattice::lattice_db> admission_owner(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=50;
    c.sched=std::make_shared<lattice::immediate_scheduler>();
    auto owner=std::make_shared<lattice::lattice_db>(c);
    recovery_refresh_test_access::use_manual_preparation(*owner);
    if(auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
    return owner;
}
// An actual other-thread SQLite mutex hold. Ready/release handshakes establish
// the schedule; elapsed time is only a finite deadlock guard, never a pass.
struct held_writer_mutex {
    std::promise<void> ready,release;
    std::future<void> ready_future=ready.get_future();
    std::future<void> release_future=release.get_future();
    std::thread worker;
    explicit held_writer_mutex(sqlite3* db):worker([this,db] {
        auto* mutex=sqlite3_db_mutex(db);sqlite3_mutex_enter(mutex);ready.set_value();
        if(release_future.wait_for(std::chrono::seconds(10))!=std::future_status::ready)std::abort();
        sqlite3_mutex_leave(mutex);
    }) {if(ready_future.wait_for(std::chrono::seconds(10))!=std::future_status::ready)std::abort();}
    ~held_writer_mutex(){release.set_value();worker.join();}
};
void raw_witness_advance(const std::string& path) {
    // No lattice_db owner, notifier or AuditLog write. A new witness is
    // available to the private reader at the forced publication rendezvous.
    lattice::database raw(path);
    raw.execute("UPDATE _lattice_recovery_witness SET generation=generation+1 WHERE id=1");
}
}

TEST(RecoveryRefreshAdmission, AbsentWitnessPollDoesNotInspectAContendedWriter) {
    TempDB path("refresh-admission-absent");auto owner=admission_owner(path.str());int calls=0;
    const auto token=owner->add_recovery_refresh_observer([&]{++calls;});ASSERT_NE(token,0u);
    size_t inspections=999;
    {
        held_writer_mutex held(owner->db().handle());
        EXPECT_FALSE(recovery_refresh_test_access::prepare_once(*owner,inspections));
        EXPECT_EQ(inspections,0u);
    }
    EXPECT_EQ(calls,0);bool body=false;
    const auto result=recovery_writer_access::install(owner,[&](auto&){body=true;});
    EXPECT_EQ(result.state,state::committed)<<error_text(result);EXPECT_TRUE(body);
    owner->remove_recovery_refresh_observer(token);
}

TEST(RecoveryRefreshAdmission, UnchangedAcknowledgedWitnessPollDoesNotInspectAContendedWriter) {
    TempDB path("refresh-admission-unchanged");auto owner=admission_owner(path.str());
    const auto initial=recovery_writer_access::install(owner,[&](auto&){bump_recovery_witness(*owner);});
    ASSERT_EQ(initial.state,state::committed)<<error_text(initial);
    int calls=0;const auto token=owner->add_recovery_refresh_observer([&]{++calls;});ASSERT_EQ(calls,1);
    size_t inspections=999;
    {
        held_writer_mutex held(owner->db().handle());
        EXPECT_FALSE(recovery_refresh_test_access::prepare_once(*owner,inspections));
        EXPECT_EQ(inspections,0u);
    }
    EXPECT_EQ(calls,1);owner->remove_recovery_refresh_observer(token);
}

TEST(RecoveryRefreshAdmission, ChangedWitnessStillInspectsWriterAndDefersWhileItIsOwned) {
    TempDB path("refresh-admission-changed");auto owner=admission_owner(path.str());
    const auto initial=recovery_writer_access::install(owner,[&](auto&){bump_recovery_witness(*owner);});
    ASSERT_EQ(initial.state,state::committed)<<error_text(initial);
    int calls=0;const auto token=owner->add_recovery_refresh_observer([&]{++calls;});ASSERT_EQ(calls,1);
    raw_witness_advance(path.str());size_t inspections=0;
    {
        held_writer_mutex held(owner->db().handle());
        EXPECT_FALSE(recovery_refresh_test_access::prepare_once(*owner,inspections));
        EXPECT_EQ(inspections,1u); // actual refresh keeps nonwaiting snapshot admission
    }
    EXPECT_EQ(calls,1);
    EXPECT_TRUE(recovery_refresh_test_access::prepare_once(*owner,inspections));EXPECT_EQ(inspections,2u);
    owner->request_recovery_refresh();EXPECT_EQ(calls,2);owner->remove_recovery_refresh_observer(token);
}

TEST(RecoveryRefreshAdmission, ChangedWitnessReaderPublicationAfterWriterCaptureDoesNotRevokeInstall) {
    TempDB path("refresh-admission-reader-publication");auto owner=admission_owner(path.str());
    owner->add(TestPerson{"schema-seed",1,std::nullopt}); // actual modeled schema and audit triggers
    const auto initial=recovery_writer_access::install(owner,[&](auto&){bump_recovery_witness(*owner);});
    ASSERT_EQ(initial.state,state::committed)<<error_text(initial);
    const auto token=owner->add_recovery_refresh_observer([]{});raw_witness_advance(path.str());
    auto* original_writer=&owner->db();bool prepared=false,body=false;size_t inspections=0;
    const auto result=recovery_install_admission_test_access::after_capture(owner,[&](auto& writer){
        body=true;EXPECT_EQ(&writer,original_writer);
        writer.execute("INSERT INTO TestPerson(globalId,name,age) VALUES('admitted','admitted',1)");
    },[&]{
        prepared=recovery_refresh_test_access::prepare_once(*owner,inspections);
        EXPECT_EQ(&owner->db(),original_writer);
    });
    EXPECT_TRUE(prepared);EXPECT_EQ(inspections,2u);EXPECT_TRUE(body);
    EXPECT_EQ(result.state,state::committed)<<error_text(result);
    EXPECT_EQ(owner->db().query("SELECT globalId FROM TestPerson WHERE globalId='admitted'").size(),1u);
    owner->remove_recovery_refresh_observer(token);
}

TEST(RecoveryRefreshAdmission, ReadOnlyReopenAfterWriterCaptureDoesNotRevokeInstall) {
    TempDB path("refresh-admission-read-reopen");auto owner=admission_owner(path.str());bool body=false,reopened=false;
    const auto result=recovery_install_admission_test_access::after_capture(owner,[&](auto&){body=true;},[&]{
        owner->reopen_read_db();reopened=true;
    });
    EXPECT_TRUE(reopened);EXPECT_TRUE(body);EXPECT_EQ(result.state,state::committed)<<error_text(result);
}

TEST(RecoveryRefreshAdmission, WriterReplacementAfterCaptureStillRefusesBeforeBody) {
    TempDB path("refresh-admission-writer-reopen");auto owner=admission_owner(path.str());bool body=false,reopened=false;
    const auto result=recovery_install_admission_test_access::after_capture(owner,[&](auto&){body=true;},[&]{
        owner->reopen_write_db();reopened=true;
    });
    EXPECT_TRUE(reopened);EXPECT_FALSE(body);EXPECT_EQ(result.state,state::refused);
    ASSERT_TRUE(result.primary_error);EXPECT_NE(error_text(result).find("admission invalidated"),std::string::npos);
    const auto successor=recovery_writer_access::install(owner,[&](auto&){body=true;});
    EXPECT_EQ(successor.state,state::committed)<<error_text(successor);EXPECT_TRUE(body);
}

TEST(RecoveryRefreshAdmission, LogicalCloseAfterCaptureStillRefusesBeforeBody) {
    TempDB path("refresh-admission-close");auto owner=admission_owner(path.str());bool body=false,closed=false;
    const auto result=recovery_install_admission_test_access::after_capture(owner,[&](auto&){body=true;},[&]{
        owner->close();closed=true;
    });
    EXPECT_TRUE(closed);EXPECT_FALSE(body);EXPECT_EQ(result.state,state::refused);EXPECT_TRUE(result.primary_error);
}
