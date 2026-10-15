#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <lattice/sync.hpp>
#include <array>
#include <functional>

namespace lattice::detail {
// Same definition as RecoveryInstallTransactionTests.cpp: private tail access,
// not an alternate production hook or a second definition with different ODR tokens.
struct recovery_install_test_access {
    static recovery_install_result with_tail(std::shared_ptr<lattice_db> owner,
        const std::function<void(database&)>& body, const std::function<void()>& tail) {
        return recovery_writer_access::install_impl(std::move(owner), body, tail);
    }
};
}
namespace {
using namespace lattice;
using access = detail::recovery_writer_access;
using test_access = detail::recovery_install_test_access;
using result_state = detail::recovery_install_state;
namespace hooks = detail::recovery_channel_reset_test_hooks;
constexpr const char* channel = "owned-maintenance-successor";
thread_local std::function<void()> reset_action;
struct reset_hook {
    void (*previous_hook)();
    std::function<void()> previous_action;
    explicit reset_hook(std::function<void()> action)
        : previous_hook(hooks::after_write_admission), previous_action(std::move(reset_action)) {
        reset_action = std::move(action);
        hooks::after_write_admission = [] { reset_action(); };
    }
    ~reset_hook() { hooks::after_write_admission = previous_hook; reset_action = std::move(previous_action); }
};
struct rollback_pending {
    std::shared_ptr<lattice_db> owner;
    ~rollback_pending() { try { if (owner->db().is_in_transaction()) owner->rollback(); } catch (...) {} }
};
int64_t scalar(database& db, const std::string& sql) {
    return std::get<int64_t>(db.query(sql).at(0).at("n"));
}
int64_t count(database& db, const std::string& id) {
    return std::get<int64_t>(db.query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId=?", {id}).at(0).at("n"));
}
std::string error_text(const std::exception_ptr& error) {
    if(!error)return "<missing>";
    try {std::rethrow_exception(error);}
    catch(const std::exception& e){return e.what();}
    catch(...){return "<nonstandard>";}
}
void insert(database& db, const std::string& id) {
    db.execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,2)", {id,id});
}
std::shared_ptr<lattice_db> store(const std::string& path) {
    configuration config(path); config.audit_retention_seconds=0; config.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(config);
    if (!config.is_in_memory()) {
        auto* notifier=instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    owner->add(TestPerson{"seed",1,std::nullopt});
    auto& db=owner->db();
    const auto target=std::get<std::string>(db.query("SELECT globalId FROM TestPerson WHERE name='seed'").at(0).at("globalId"));
    db.execute("INSERT INTO _lattice_sync_state VALUES(?,?,0)", {scalar(db,"SELECT MAX(id) AS n FROM AuditLog"),std::string(channel)});
    db.execute("INSERT INTO _lattice_sync_set VALUES(?,'TestPerson',?)", {std::string(channel),target});
    register_replication_slot(db,channel);
    db.execute("UPDATE _lattice_replication_slots SET confirmed_audit_id=9,upload_floor=7 WHERE sync_id=?", {std::string(channel)});
    return owner;
}
auto metadata(database& db) {
    const std::vector<column_value_t> args{std::string(channel)};
    return std::array<std::vector<database::row_t>,3>{
        db.query("SELECT * FROM _lattice_sync_state WHERE sync_id=? ORDER BY audit_entry_id",args),
        db.query("SELECT * FROM _lattice_sync_set WHERE sync_id=? ORDER BY table_name,global_row_id",args),
        db.query("SELECT * FROM _lattice_replication_slots WHERE sync_id=?",args)};
}
struct observation {
    std::shared_ptr<lattice_db> owner;
    lattice_db::observer_id token;
    std::vector<std::vector<std::string>> batches;
    explicit observation(std::shared_ptr<lattice_db> o):owner(std::move(o)) {
        token=owner->add_table_observer("TestPerson",[this](const auto& changes) {
            std::vector<std::string> ids;
            for (const auto& c:changes) ids.push_back(std::get<3>(c));
            batches.push_back(std::move(ids));
        });
    }
    ~observation(){owner->remove_table_observer("TestPerson",token);}
};

void successor_matrix(bool file, bool install, bool attempt_commit) {
    for (bool write_original:{false,true}) for (bool throw_after_successor:{false,true}) {
        SCOPED_TRACE(::testing::Message()<<"file="<<file<<" install="<<install
            <<" commit="<<attempt_commit<<" original_write="<<write_original
            <<" throw="<<throw_after_successor);
        TempDB disk{"maintenance_successor"};
        auto owner=store(file?disk.str():std::string(":memory:"));
        rollback_pending cleanup{owner};
        auto& db=owner->db();
        const auto before=metadata(db);
        observation seen(owner);
        bool vetoed=false, successor_opened=false;
        int bodies=0,tails=0;
        const auto body=[&](database& writer) {
            ++bodies;
            if (write_original) insert(writer,"original");
            if (attempt_commit) {
                try { writer.commit(); }
                catch (const db_error&) { vetoed=true; }
                EXPECT_TRUE(vetoed) << "real SQLite COMMIT must be vetoed, including the file no-write case";
            } else writer.rollback();
            EXPECT_FALSE(writer.is_in_transaction());
            if (install) EXPECT_EQ(access::active_writer(*owner),nullptr);
            // An actual public successor is legitimate after the original was
            // consumed. The old private frame must not adopt its authority.
            owner->begin_transaction();
            insert(writer,"successor");
            successor_opened=true;
            if (install) EXPECT_EQ(access::active_writer(*owner),nullptr);
            if (throw_after_successor) throw std::runtime_error("after successor");
        };
        if (install) {
            const auto result=test_access::with_tail(owner,body,[&]{++tails;});
            EXPECT_EQ(result.state,result_state::rolled_back);
            EXPECT_NE(result.primary_error,nullptr);
            EXPECT_EQ(result.cleanup_error,nullptr);
            EXPECT_EQ(result.postcommit_error,nullptr);
            EXPECT_FALSE(result.unexpected_commit_observed);
        } else {
            reset_hook hook([&]{body(db);});
            EXPECT_THROW(owner->reset_sync_state(channel),std::exception);
        }
        ASSERT_EQ(bodies,1);
        ASSERT_TRUE(successor_opened);
        EXPECT_EQ(tails,0);
        ASSERT_TRUE(db.is_in_transaction());
        EXPECT_EQ(access::active_writer(*owner),&db);
        EXPECT_EQ(metadata(db),before);
        EXPECT_EQ(count(db,"original"),0);
        EXPECT_EQ(count(db,"successor"),1);
        EXPECT_TRUE(seen.batches.empty());
        // Positive custody oracle: merely leaving the successor open is not
        // enough. Its own later COMMIT must deliver exactly its buffered row.
        ASSERT_NO_THROW(owner->commit());
        ASSERT_EQ(seen.batches.size(),1u);
        EXPECT_EQ(seen.batches[0],(std::vector<std::string>{"successor"}));
        EXPECT_EQ(metadata(db),before);
        EXPECT_EQ(count(db,"original"),0);
        EXPECT_EQ(count(db,"successor"),1);
        insert(db,"ordinary-next");
        ASSERT_EQ(seen.batches.size(),2u);
        EXPECT_EQ(seen.batches[1],(std::vector<std::string>{"ordinary-next"}));
        const auto retry=access::install(owner,[](auto& writer){insert(writer,"install-next");});
        EXPECT_EQ(retry.state,result_state::committed);
        EXPECT_EQ(retry.primary_error,nullptr);
        ASSERT_EQ(seen.batches.size(),3u);
        EXPECT_EQ(seen.batches[2],(std::vector<std::string>{"install-next"}));
    }
}

void finalization_success(bool install) {
    for (bool file:{false,true}) for (bool with_write:{false,true}) {
        SCOPED_TRACE(::testing::Message()<<"file="<<file<<" write="<<with_write);
        TempDB disk{"maintenance_finalization"};
        auto owner=store(file?disk.str():std::string(":memory:"));
        observation seen(owner);
        int tails=0;
        auto body=[&](database& db){if(with_write)insert(db,"finalized");};
        if (install) {
            const auto result=test_access::with_tail(owner,body,[&]{++tails;});
            EXPECT_EQ(result.state,result_state::committed);
            EXPECT_EQ(result.primary_error,nullptr);
            EXPECT_FALSE(result.unexpected_commit_observed);
            EXPECT_EQ(tails,1);
        } else {
            reset_hook hook([&]{body(owner->db());});
            ASSERT_NO_THROW(owner->reset_sync_state(channel));
            const auto after=metadata(owner->db());
            EXPECT_TRUE(after[0].empty()); EXPECT_TRUE(after[1].empty());
            ASSERT_EQ(after[2].size(),1u);
            EXPECT_EQ(std::get<int64_t>(after[2][0].at("confirmed_audit_id")),0);
            EXPECT_EQ(std::get<int64_t>(after[2][0].at("upload_floor")),0);
        }
        EXPECT_FALSE(owner->db().is_in_transaction());
        EXPECT_EQ(count(owner->db(),"finalized"),with_write?1:0);
        EXPECT_EQ(seen.batches.size(),with_write?1u:0u);
        if(with_write)EXPECT_EQ(seen.batches[0],(std::vector<std::string>{"finalized"}));
    }
}

void denied_finalization(bool install) {
    for(bool file:{false,true}) {
        TempDB disk{"maintenance_denied_finalization"};
        auto owner=store(file?disk.str():std::string(":memory:"));
        auto& db=owner->db(); const auto before=metadata(db);
        observation seen(owner);
        auto* handle=db.handle();
        struct fault { int attempts=0; bool armed=false; } injected;
        ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* first,
            const char*,const char*,const char*) noexcept -> int {
            auto& f=*static_cast<fault*>(raw);
            if(f.armed&&action==SQLITE_TRANSACTION&&first&&std::strcmp(first,"COMMIT")==0){++f.attempts;return SQLITE_DENY;}
            return SQLITE_OK;
        },&injected),SQLITE_OK);
        struct restore {sqlite3* handle;~restore(){sqlite3_set_authorizer(handle,nullptr,nullptr);}} reset{handle};
        int tails=0;
        const auto body=[&](database& writer){insert(writer,"refused-final");injected.armed=true;};
        if(install) {
            const auto result=test_access::with_tail(owner,body,[&]{++tails;});
            EXPECT_EQ(result.state,result_state::rolled_back);
            EXPECT_NE(result.primary_error,nullptr);
            EXPECT_EQ(result.cleanup_error,nullptr);
            EXPECT_FALSE(result.unexpected_commit_observed);
        } else {
            reset_hook hook([&]{body(db);});
            EXPECT_THROW(owner->reset_sync_state(channel),db_error);
        }
        injected.armed=false;
        EXPECT_EQ(injected.attempts,1); EXPECT_EQ(tails,0);
        EXPECT_FALSE(db.is_in_transaction());
        EXPECT_EQ(count(db,"refused-final"),0); EXPECT_EQ(metadata(db),before);
        EXPECT_TRUE(seen.batches.empty());
        const auto retry=access::install(owner,[](auto& writer){insert(writer,"retry");});
        EXPECT_EQ(retry.state,result_state::committed);
        ASSERT_EQ(seen.batches.size(),1u);
        EXPECT_EQ(seen.batches[0],(std::vector<std::string>{"retry"}));
    }
}
// A committed successor is an ordinary transaction, not a successful install.
// File callbacks retain existing WAL timing; memory callbacks must be drained
// after the failed private frame releases its outer scopes.
void committed_successor_matrix(bool file, bool install) {
    for(bool throw_body:{false,true}) for(bool throw_observer:{false,true}) {
        SCOPED_TRACE(::testing::Message()<<"file="<<file<<" install="<<install
            <<" body_throw="<<throw_body<<" observer_throw="<<throw_observer);
        TempDB disk{"maintenance_committed_successor"};
        auto owner=store(file?disk.str():std::string(":memory:"));
        rollback_pending cleanup{owner};
        auto& db=owner->db(); const auto before=metadata(db);
        auto* handle=db.handle();
        int notifications=0,tails=0; bool entered=false,body_completed=false;
        auto observer=owner->add_table_observer("TestPerson",[&](const auto& changes) {
            for(const auto& change:changes) {
                if(std::get<3>(change)!="committed-successor")continue;
                ++notifications;
                if(!file) {
                    EXPECT_TRUE(body_completed);
                    EXPECT_EQ(access::active_writer(*owner),nullptr);
                    // Positive physical-lock witness from another thread. A
                    // recursive same-thread mutex probe would prove nothing.
                    int lock_result=SQLITE_MISUSE;
                    std::thread check([&]{
                        auto* mutex=sqlite3_db_mutex(handle);
                        lock_result=sqlite3_mutex_try(mutex);
                        if(lock_result==SQLITE_OK)sqlite3_mutex_leave(mutex);
                    });
                    check.join();
                    EXPECT_EQ(lock_result,SQLITE_OK);
                } else EXPECT_FALSE(body_completed); // Existing ordinary WAL path.
                if(throw_observer)throw std::runtime_error("ordinary successor observer");
            }
        });
        struct remove_observer {
            std::shared_ptr<lattice_db> owner;lattice_db::observer_id token;
            ~remove_observer(){owner->remove_table_observer("TestPerson",token);}
        } remove{owner,observer};
        const auto body=[&](database& writer){
            entered=true;
            insert(writer,"original");
            writer.rollback();
            owner->begin_transaction();
            insert(writer,"committed-successor");
            try { owner->commit(); }
            catch(...) { body_completed=true; throw; }
            body_completed=true;
            if(throw_body)throw std::runtime_error("body after successor commit");
        };
        if(install) {
            const auto result=test_access::with_tail(owner,body,[&]{++tails;});
            EXPECT_EQ(result.state,result_state::rolled_back);
            EXPECT_NE(result.primary_error,nullptr);
            EXPECT_EQ(result.cleanup_error,nullptr);
            EXPECT_EQ(result.postcommit_error,nullptr);
            EXPECT_FALSE(result.unexpected_commit_observed);
            if(!file&&throw_observer) {
                EXPECT_NE(result.notification_error,nullptr);
                EXPECT_EQ(error_text(result.notification_error),"ordinary successor observer");
            } else EXPECT_EQ(result.notification_error,nullptr);
            if(file&&throw_observer)
                EXPECT_EQ(error_text(result.primary_error),"ordinary successor observer");
            else if(throw_body)
                EXPECT_EQ(error_text(result.primary_error),"body after successor commit");
        } else {
            std::exception_ptr caught;
            {
                reset_hook hook([&]{body(db);});
                try {owner->reset_sync_state(channel);}
                catch(...){caught=std::current_exception();}
            }
            ASSERT_NE(caught,nullptr);
            bool dual=false;
            try {std::rethrow_exception(caught);}
            catch(const detail::recovery_channel_reset_notification_error& e) {
                dual=true;
                EXPECT_NE(e.primary_error,nullptr); EXPECT_NE(e.notification_error,nullptr);
                EXPECT_EQ(error_text(e.notification_error),"ordinary successor observer");
                if(throw_body)EXPECT_EQ(error_text(e.primary_error),"body after successor commit");
            }
            catch(const std::runtime_error& e) {
                if(file&&throw_observer)EXPECT_STREQ(e.what(),"ordinary successor observer");
            }
            EXPECT_EQ(dual,!file&&throw_observer);
        }
        ASSERT_TRUE(entered);ASSERT_TRUE(body_completed);
        EXPECT_EQ(tails,0);EXPECT_EQ(notifications,1);
        EXPECT_FALSE(db.is_in_transaction());
        EXPECT_EQ(metadata(db),before);
        EXPECT_EQ(count(db,"original"),0);
        EXPECT_EQ(count(db,"committed-successor"),1);
        insert(db,"ordinary-after");
        EXPECT_EQ(notifications,1) << "failure unwind must not replay the successor batch";
        const auto retry=access::install(owner,[](auto& writer){insert(writer,"private-after");});
        EXPECT_EQ(retry.state,result_state::committed);
        EXPECT_EQ(notifications,1);
    }
}
} // namespace

TEST(MaintenanceSuccessor, MemoryResetPrematureCommitVetoPreservesSuccessorNotifications) { successor_matrix(false,false,true); }
TEST(MaintenanceSuccessor, FileResetPrematureCommitVetoIncludesNoWriteTransaction) { successor_matrix(true,false,true); }
TEST(MaintenanceSuccessor, MemoryResetExplicitRollbackPreservesSuccessorNotifications) { successor_matrix(false,false,false); }
TEST(MaintenanceSuccessor, FileResetExplicitRollbackPreservesSuccessorNotifications) { successor_matrix(true,false,false); }
TEST(MaintenanceSuccessor, MemoryInstallPrematureCommitVetoPreservesSuccessorNotifications) { successor_matrix(false,true,true); }
TEST(MaintenanceSuccessor, FileInstallPrematureCommitVetoIncludesNoWriteTransaction) { successor_matrix(true,true,true); }
TEST(MaintenanceSuccessor, MemoryInstallExplicitRollbackPreservesSuccessorNotifications) { successor_matrix(false,true,false); }
TEST(MaintenanceSuccessor, FileInstallExplicitRollbackPreservesSuccessorNotifications) { successor_matrix(true,true,false); }
TEST(MaintenanceSuccessor, ResetOwnFinalizationSucceedsForEmptyAndWrittenFileAndMemory) { finalization_success(false); }
TEST(MaintenanceSuccessor, InstallOwnFinalizationSucceedsForEmptyAndWrittenFileAndMemory) { finalization_success(true); }
TEST(MaintenanceSuccessor, ResetDeniedFinalCommitRollsBackOnlyOriginalAndAllowsRetry) { denied_finalization(false); }
TEST(MaintenanceSuccessor, InstallDeniedFinalCommitRollsBackOnlyOriginalAndAllowsRetry) { denied_finalization(true); }

TEST(MaintenanceSuccessor, ObservedUnsupportedPrematureWalCommitNeverActivatesInstall) {
    TempDB disk{"maintenance_foreign_commit_hook"};
    auto owner=store(disk.str());
    rollback_pending cleanup{owner};
    observation seen(owner);
    auto* handle=owner->db().handle();
    // Intentionally violate the private hook-custody contract. This tests only
    // an ACTUALLY OBSERVED premature main-WAL commit, not arbitrary replacement
    // hooks, memory/no-WAL commits, or unknown physical outcomes.
    sqlite3_commit_hook(handle,[](void*) noexcept -> int{return 0;},nullptr);
    int tails=0;
    const auto result=test_access::with_tail(owner,[&](auto& writer){
        insert(writer,"premature-durable");
        writer.commit();
        EXPECT_EQ(access::active_writer(*owner),nullptr);
        owner->begin_transaction();
        insert(writer,"successor");
    },[&]{++tails;});
    EXPECT_EQ(result.state,result_state::ownership_lost);
    EXPECT_TRUE(result.unexpected_commit_observed);
    EXPECT_NE(result.primary_error,nullptr);
    EXPECT_EQ(result.cleanup_error,nullptr);
    EXPECT_EQ(result.postcommit_error,nullptr);
    EXPECT_EQ(tails,0);
    EXPECT_EQ(count(owner->db(),"premature-durable"),1);
    ASSERT_TRUE(owner->db().is_in_transaction());
    EXPECT_TRUE(seen.batches.empty());
    ASSERT_NO_THROW(owner->commit());
    ASSERT_EQ(seen.batches.size(),1u);
    EXPECT_EQ(seen.batches[0],(std::vector<std::string>{"successor"}));
}

TEST(MaintenanceSuccessor, MemoryResetDrainsCommittedSuccessorAndRetainsNotificationFailure) { committed_successor_matrix(false,false); }
TEST(MaintenanceSuccessor, FileResetPreservesCommittedSuccessorAndActualWalCallbackFailure) { committed_successor_matrix(true,false); }
TEST(MaintenanceSuccessor, MemoryInstallDrainsCommittedSuccessorWithoutActivation) { committed_successor_matrix(false,true); }
TEST(MaintenanceSuccessor, FileInstallPreservesCommittedSuccessorAndActualWalCallbackFailure) { committed_successor_matrix(true,true); }

// Packet002: actual generated producer custody, kept separate from the original
// 17 case bodies above. No raw handle escape or fabricated producer receipt.
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
struct MaintenanceProducerRow {std::string value;};
LATTICE_SCHEMA(MaintenanceProducerRow,value);
namespace {
using namespace lattice::detail;
void require_maintenance_commit(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed) {
        if(result.primary_error)std::rethrow_exception(result.primary_error);
        throw std::runtime_error("producer fixture transaction did not commit");
    }
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
}
void enrolled_successor_matrix(bool file) {
    for(bool commit_inside:{false,true}) for(bool attempt_commit:{false,true}) {
        SCOPED_TRACE(::testing::Message()<<"file="<<file<<" commit_inside="<<commit_inside
            <<" premature_commit="<<attempt_commit);
        TempDB disk{"maintenance_producer_successor"};
        configuration config(file?disk.str():std::string(":memory:"));
        config.audit_retention_seconds=0;config.busy_timeout_ms=100;
        auto owner=std::make_shared<lattice_db>(config);
        rollback_pending cleanup{owner};
        if(file) {
            auto* notifier=instance_registry::instance().get_or_create_notifier(disk.str());
            if(notifier)notifier->stop_listening();
        }
        const recovery_obligation_producer_discovery_limits caps{
            {4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
        recovery_obligation_address address;
        ASSERT_NO_THROW(require_maintenance_commit(access::install(owner,[&](auto&) {
            receive_install_store receiver(owner,caps.installations);receiver.initialize();
            receive_install_binding binding{"maintenance-producer","authority","source","epoch","scope","schema"};
            receiver.bind(binding);
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();
            address=journal.bind({binding,"grant","receipts"}).address;
        })));
        ASSERT_NO_THROW(require_maintenance_commit(recovery_local_producer_adapter::enroll_for_qualification(
            owner,{address,{"MaintenanceProducerRow"},{'g'}},caps)));
        auto& writer=owner->db();
        int tails=0;bool vetoed=false;std::string original_target,successor_target;
        const auto failed=test_access::with_tail(owner,[&](auto& db) {
            // Actual phase-2 generated model fence accepts this suppressed
            // recovery effect. Its transaction and suppression are consumed.
            db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            original_target=owner->add(MaintenanceProducerRow{"suppressed-original"}).global_id();
            EXPECT_EQ(scalar(db,"SELECT COUNT(*) AS n FROM MaintenanceProducerRow"),1);
            EXPECT_EQ(scalar(db,"SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='MaintenanceProducerRow'"),0);
            if(attempt_commit) {
                try{db.commit();}catch(const db_error&){vetoed=true;}
                EXPECT_TRUE(vetoed);
            } else db.rollback();
            EXPECT_FALSE(db.is_in_transaction());
            EXPECT_EQ(access::active_writer(*owner),nullptr);
            EXPECT_EQ(scalar(db,"SELECT disabled AS n FROM _SyncControl WHERE id=1"),0);
            owner->begin_transaction();
            // Must now take the real local phase-1 generated path although the
            // old private frame remains on this stack. An old phase-2 grant
            // would either refuse this write or omit its genuine origin stamp.
            successor_target=owner->add(MaintenanceProducerRow{"local-successor"}).global_id();
            EXPECT_EQ(access::active_writer(*owner),nullptr);
            if(commit_inside)owner->commit();
        },[&]{++tails;});
        ASSERT_EQ(failed.state,result_state::rolled_back);
        EXPECT_NE(failed.primary_error,nullptr);
        EXPECT_EQ(failed.cleanup_error,nullptr);EXPECT_EQ(failed.notification_error,nullptr);
        EXPECT_EQ(failed.postcommit_error,nullptr);EXPECT_FALSE(failed.unexpected_commit_observed);
        ASSERT_FALSE(original_target.empty());ASSERT_FALSE(successor_target.empty());
        EXPECT_EQ(tails,0);EXPECT_EQ(writer.is_in_transaction(),!commit_inside);
        if(!commit_inside)ASSERT_NO_THROW(owner->commit());
        EXPECT_EQ(scalar(writer,"SELECT COUNT(*) AS n FROM MaintenanceProducerRow WHERE value='suppressed-original'"),0);
        EXPECT_EQ(scalar(writer,"SELECT COUNT(*) AS n FROM MaintenanceProducerRow WHERE value='local-successor'"),1);
        const auto audits=writer.query("SELECT * FROM AuditLog WHERE tableName='MaintenanceProducerRow' ORDER BY id");
        ASSERT_EQ(audits.size(),1u);
        const auto& audit=audits[0];
        const auto audit_id=std::get<int64_t>(audit.at("id"));
        const auto original=std::get<std::string>(audit.at("globalId"));
        ASSERT_GT(audit_id,0);
        EXPECT_EQ(std::get<std::string>(audit.at("globalRowId")),successor_target);
        EXPECT_EQ(std::get<int64_t>(audit.at("isFromRemote")),0);
        EXPECT_EQ(std::get<int64_t>(audit.at("synthesized")),0);
        EXPECT_EQ(scalar(writer,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry"),1);
        EXPECT_EQ(scalar(writer,"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp"),1);
        std::optional<recovery_obligation_entry> entry;
        std::optional<recovery_obligation_producer_stamp> stamp;
        const auto read_facts=[&](database&) {
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);
            entry=journal.find(address,original);
            stamp=recovery_obligation_producer_store::read_stamp(owner,caps.obligations,
                caps.installations,caps.producers,address,original);
        };
        ASSERT_NO_THROW(require_maintenance_commit(access::install(owner,read_facts)));
        ASSERT_TRUE(entry);ASSERT_TRUE(stamp);
        EXPECT_EQ(entry->record.audit_id,audit_id);EXPECT_EQ(entry->record.original_id,original);
        EXPECT_EQ(entry->record.target_id,successor_target);
        EXPECT_EQ(entry->record.origin,recovery_obligation_origin::local_candidate);
        EXPECT_FALSE(entry->first_export_claim);EXPECT_EQ(entry->stage,recovery_obligation_stage::open);
        EXPECT_EQ(stamp->audit_id,audit_id);EXPECT_EQ(stamp->contribution_incarnation,address.incarnation);
        EXPECT_EQ(stamp->program_revision,1);EXPECT_EQ(stamp->record_sequence,entry->sequence);
        // Actual sender preparation: bounded validation + durable claim COMMIT,
        // not a caller-made frame or fabricated source acknowledgment.
        auto exported=recovery_export_adapter::prepare_pending(owner,"maintenance-route",1,8,{},false);
        ASSERT_TRUE(exported.protected_store);ASSERT_TRUE(exported.frame);
        ASSERT_EQ(exported.frame->entries().size(),1u);
        const auto& wire=exported.frame->entries()[0];
        EXPECT_EQ(wire.global_id,original);EXPECT_EQ(wire.global_row_id,successor_target);
        EXPECT_EQ(std::get<std::string>(wire.changed_fields.at("value").value),"local-successor");
        ASSERT_NO_THROW(require_maintenance_commit(access::install(owner,read_facts)));
        ASSERT_TRUE(entry);ASSERT_TRUE(entry->first_export_claim);ASSERT_TRUE(stamp);
        EXPECT_GT(*entry->first_export_claim,0);EXPECT_EQ(entry->stage,recovery_obligation_stage::open);
        EXPECT_EQ(stamp->audit_id,audit_id);
        EXPECT_EQ(writer.query("SELECT * FROM AuditLog WHERE tableName='MaintenanceProducerRow' ORDER BY id"),audits);
        EXPECT_EQ(tails,0);
    }
}
} // namespace
TEST(MaintenanceSuccessor, EnrolledMemorySuccessorGeneratesOriginalStampAndProtectedExport) { enrolled_successor_matrix(false); }
TEST(MaintenanceSuccessor, EnrolledFileSuccessorGeneratesOriginalStampAndProtectedExport) { enrolled_successor_matrix(true); }
