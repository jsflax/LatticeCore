#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice/sync.hpp>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <filesystem>
#include <exception>
#include <stdexcept>
#include <mutex>
#include <thread>

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <unistd.h>
#include <spawn.h>
#include <sys/wait.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
struct HistoryOwnedRow { int64_t value = 0; int64_t companion = 0; };
LATTICE_SCHEMA(HistoryOwnedRow, value, companion);
namespace lattice {
struct history_generation_test_access {
    static int64_t force(lattice_db& owner, int64_t batch, const std::function<void(bool)>& between = {}) {
        return owner.generate_history_owned_(batch, true, true, between);
    }
    static int64_t generate(lattice_db& owner, int64_t batch, const std::function<void(bool)>& between) {
        return owner.generate_history_owned_(batch, true, false, between);
    }
};
}
namespace {
using namespace lattice;
void require(bool value, const char* why) { if (!value) throw std::runtime_error(why); }
struct File {
    std::string path = (std::filesystem::temp_directory_path() /
        ("history-owned-" + std::to_string(getpid()) + "-" + lattice::uuid_t::generate().to_string() + ".sqlite")).string();
    ~File() {
        if (std::uncaught_exceptions()) return; // Preserve failed fixture bytes.
        std::error_code error;
        for (const auto& suffix : {"", "-wal", "-shm"}) std::filesystem::remove(path + suffix, error);
    }
};
configuration config(const std::string& path) { configuration c(path); c.audit_retention_seconds=0; c.busy_timeout_ms=1000; return c; }
int64_t scalar(database& db, const std::string& sql) { return std::get<int64_t>(db.query(sql).at(0).begin()->second); }
void seed(lattice_db& owner) { owner.add(HistoryOwnedRow{10, 110}); owner.add(HistoryOwnedRow{20, 120}); }
void settled(lattice_db& owner, int64_t flag=0) {
    require(!owner.db().is_in_transaction(), "no owned transaction survives");
    require(scalar(owner.db(), "SELECT disabled FROM _SyncControl WHERE id=1")==flag, "saved sync flag restored");
}
void verify_peer(lattice_db& owner) {
    File path; lattice_db peer(config(path.path));
    // Create the same schema without keeping a seeded model or audit entry.
    peer.add(HistoryOwnedRow{}); peer.db().execute("DELETE FROM HistoryOwnedRow"); peer.db().execute("DELETE FROM AuditLog");
    auto entries=events_after(owner.db(),std::nullopt);
    const auto applied=apply_remote_changes(peer, entries);
    require(applied.size()==entries.size(), "every retained entry applied/acknowledged");
    require(owner.db().query("SELECT globalId,value,companion FROM HistoryOwnedRow ORDER BY globalId") ==
            peer.db().query("SELECT globalId,value,companion FROM HistoryOwnedRow ORDER BY globalId"),
            "fresh peer receives complete final values, not only audit counts");
}
struct Observe {
    lattice_db& owner; std::string table; lattice_db::observer_id token;
    Observe(lattice_db& o, std::string t, std::function<void()> action) : owner(o), table(std::move(t)) {
        // Only the owner's synchronous writer delivery is the deterministic seam.
        const auto writer=std::this_thread::get_id();
        token=owner.add_table_observer(table,[writer,action=std::move(action)](const auto&) {
            if (std::this_thread::get_id()==writer) action();
        });
    }
    ~Observe() { owner.remove_table_observer(table,token); }
};
void reset_interleave(int mode, bool memory) {
    File path; lattice_db owner(memory?configuration{}:config(path.path)); seed(owner);
    const auto before=scalar(owner.db(),"SELECT MAX(id) FROM AuditLog");
    bool fired=false; std::exception_ptr error;
    Observe callback(owner,"_lattice_meta",[&] {
        if (fired) return; fired=true;
        try {
            settled(owner);
            if (mode==0) owner.db().execute("UPDATE HistoryOwnedRow SET value=71 WHERE id=1");
            else if(mode==1) owner.db().execute("DELETE FROM HistoryOwnedRow WHERE id=2");
            else {
                auto row=owner.db().query("SELECT globalId FROM HistoryOwnedRow WHERE id=1");
                audit_log_entry entry; entry.global_id=lattice::uuid_t::generate().to_string(); entry.table_name="HistoryOwnedRow";
                entry.operation="INSERT"; entry.global_row_id=std::get<std::string>(row[0].at("globalId"));
                entry.timestamp="1700000000.0"; entry.changed_fields_names={"value"};
                entry.changed_fields["value"]=any_property(int64_t{81});
                require(apply_remote_changes(owner,{entry}).size()==1,"remote update applied");
            }
        } catch(...) { error=std::current_exception(); }
    });
    const auto count=history_generation_test_access::force(owner,1);
    if(error)std::rethrow_exception(error);
    require(fired,"reset metadata observer ran"); require(count==(mode==1?1:2),"force snapshot count exact");
    require(scalar(owner.db(),"SELECT MIN(id) FROM AuditLog")>before,"audit IDs not restarted");
    settled(owner); verify_peer(owner);
}
void after_snapshot(int mode) {
    File path; lattice_db owner(config(path.path)); seed(owner);
    bool fired=false; std::exception_ptr error;
    auto callback=[&](bool prepared) {
        if(prepared)return;
        if(fired) return; fired=true;
        try {
            settled(owner);
            if(mode==0)owner.db().execute("UPDATE HistoryOwnedRow SET value=91 WHERE id=1");
            else if(mode==1)owner.db().execute("DELETE FROM HistoryOwnedRow WHERE id=1");
            else {
                const auto gid=std::get<std::string>(owner.db().query("SELECT globalId FROM HistoryOwnedRow WHERE id=1")[0].at("globalId"));
                owner.db().execute("DELETE FROM HistoryOwnedRow WHERE id=1");
                owner.db().execute("INSERT INTO HistoryOwnedRow(globalId,value,companion) VALUES(?,92,192)",{gid});
                owner.add(HistoryOwnedRow{93,193}); // Above the captured frontier.
            }
        } catch(...) { error=std::current_exception(); }
    };
    require(history_generation_test_access::force(owner,1,callback)==2,"reset-time frontier stays finite");
    if(error)std::rethrow_exception(error);
    require(fired,"first snapshot callback ran"); settled(owner); verify_peer(owner);
}
void reset_or_prune(bool prune) {
    File path; lattice_db owner(config(path.path)); seed(owner);
    lattice_db other(config(path.path));
    bool fired=false; std::exception_ptr callback_error;
    auto callback=[&](bool prepared) {
        if(prune==prepared)return;
        if(fired)return;fired=true;
        try {
            if(prune) {
                auto max=scalar(other.db(),"SELECT MAX(id) FROM AuditLog");
                require(other.delete_audit_below_(max,false)>0,"real qualified prune removed history");
            } else require(other.force_compact_audit_log()==2,"second handle completes its generation");
        }catch(...){callback_error=std::current_exception();}
    };
    std::string error;
    try{history_generation_test_access::force(owner,1,callback);}catch(const std::exception& e){error=e.what();}
    if(callback_error)std::rethrow_exception(callback_error);
    require(fired,"destructive interleave was reached");
    require(error.find("invalidated by another reset or prune")!=std::string::npos,"superseded history must fail honestly");
    settled(owner); settled(other);
}
std::string executable_path() {
    char buffer[4096];
#if defined(__APPLE__)
    uint32_t size=sizeof(buffer);
    require(_NSGetExecutablePath(buffer,&size)==0,"bounded executable path");
    return buffer;
#else
    const auto size=readlink("/proc/self/exe",buffer,sizeof(buffer)-1);
    require(size>0&&size<static_cast<ssize_t>(sizeof(buffer)-1),"bounded executable path");
    buffer[size]=0;return buffer;
#endif
}
void second_process_reset() {
    File path;lattice_db owner(config(path.path));seed(owner);
    bool fired=false;std::exception_ptr error;
    auto callback=[&](bool prepared) {
        if(!prepared)return;
        if(fired)return;fired=true;
        try {
            const auto executable=executable_path();
            std::vector<std::string> strings;
            for(char** entry=environ;*entry;++entry)
                if(std::strncmp(*entry,"LATTICE_HISTORY_RESET_PEER=",27)!=0)strings.emplace_back(*entry);
            strings.push_back("LATTICE_HISTORY_RESET_PEER="+path.path);
            std::vector<char*> environment;
            for(auto& value:strings)environment.push_back(value.data());environment.push_back(nullptr);
            std::string filter="--gtest_filter=HistoryGenerationOwnership.AnotherProcessResetInvalidatesGeneration";
            std::string color="--gtest_color=no";
            char* args[]={const_cast<char*>(executable.c_str()),filter.data(),color.data(),nullptr};
            pid_t child=-1;
            require(posix_spawn(&child,executable.c_str(),nullptr,nullptr,args,environment.data())==0,"spawn fresh peer executable");
            int status=0;pid_t waited;
            do{waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);
            require(waited==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"fresh peer reset completed and reaped");
        }catch(...){error=std::current_exception();}
    };
    std::string result;
    try{history_generation_test_access::force(owner,1,callback);}catch(const std::exception& e){result=e.what();}
    if(error)std::rethrow_exception(error);
    require(fired&&result.find("invalidated by another reset or prune")!=std::string::npos,
            "actual other process reset invalidates suspended generator");settled(owner);
}
void recursive_or_successor(bool successor) {
    File path;lattice_db owner(config(path.path));seed(owner);
    bool fired=false,recursive_failed=false;std::exception_ptr error;
    Observe callback(owner,"_lattice_meta",[&] {
        if(fired)return;fired=true;
        try {
            if(successor) { owner.db().begin_transaction();owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('successor','uncommitted')"); }
            else { try{owner.generate_history();}catch(const db_error&){recursive_failed=true;} }
        }catch(...){error=std::current_exception();}
    });
    bool rejected=false;
    try{history_generation_test_access::force(owner,1);}catch(const db_error&){rejected=true;}
    if(error)std::rethrow_exception(error);
    require(fired,"callback reached");
    if(successor) {
        require(rejected&&owner.db().is_in_transaction(),"caller successor is rejected, not rolled back");
        require(scalar(owner.db(),"SELECT COUNT(*) FROM _lattice_meta WHERE key='successor'")==1,"successor remains pending");
        owner.db().rollback();
    } else require(!rejected&&recursive_failed,"recursive generation fails without aborting outer operation");
    settled(owner);
}
void flags_and_predicate(int64_t flag) {
    File path;lattice_db owner(config(path.path));seed(owner);
    owner.db().execute("DELETE FROM AuditLog");
    owner.db().execute("UPDATE HistoryOwnedRow SET value=30 WHERE id=1");
    owner.db().execute("UPDATE _SyncControl SET disabled=? WHERE id=1",{flag});
    require(owner.generate_history(1)==1,"standalone keeps any-history eligibility semantics");
    require(owner.generate_history(1)==0,"standalone remains idempotent");settled(owner,flag);
    require(history_generation_test_access::force(owner,1)==2,"force snapshots both rows regardless previous audit operation");settled(owner,flag);
}
struct AuthorizerReset { sqlite3* db; ~AuthorizerReset(){sqlite3_set_authorizer(db,nullptr,nullptr);} };
struct Denial { bool fail_restore=false; bool saw_disable=false; static int authorize(void* raw,int action,const char* a,const char* b,const char*,const char*) {
    auto& s=*static_cast<Denial*>(raw);
    if(!s.fail_restore&&action==SQLITE_READ&&a&&b&&std::strcmp(a,"_SyncControl")==0&&std::strcmp(b,"disabled")==0)return SQLITE_DENY;
    if(s.fail_restore&&action==SQLITE_UPDATE&&a&&std::strcmp(a,"_SyncControl")==0) { if(s.saw_disable)return SQLITE_DENY;s.saw_disable=true; }
    return SQLITE_OK;
}};
void failed_reset(bool restore) {
    File path;lattice_db owner(config(path.path));seed(owner);
    const auto prior=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    Denial denial;denial.fail_restore=restore;auto* raw=owner.db().handle();
    require(sqlite3_set_authorizer(raw,Denial::authorize,&denial)==SQLITE_OK,"install owned test authorizer");
    AuthorizerReset authorizer{raw};
    bool rejected=false;
    try{owner.force_compact_audit_log();}catch(const db_error&){rejected=true;}
    sqlite3_set_authorizer(raw,nullptr,nullptr);
    require(rejected,"denied flag read/restore fails");
    require(owner.db().query("SELECT * FROM AuditLog ORDER BY id")==prior,"failed reset rolls back history exactly");settled(owner);
    require(owner.force_compact_audit_log()==2,"admission lease released after failure");
}
void caller_transaction() {
    File path;lattice_db owner(config(path.path));seed(owner);owner.db().begin_transaction();
    owner.db().execute("UPDATE HistoryOwnedRow SET value=44 WHERE id=1");
    bool rejected=false;try{owner.force_compact_audit_log();}catch(const db_error&){rejected=true;}
    require(rejected&&owner.db().is_in_transaction(),"must not join caller transaction");
    require(scalar(owner.db(),"SELECT value FROM HistoryOwnedRow WHERE id=1")==44,"caller data retained");owner.db().rollback();settled(owner);
}
void malformed_revision() {
    File path;lattice_db owner(config(path.path));seed(owner);
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('history_generation_revision_v1','broken')");
    const auto prior=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    bool rejected=false;try{owner.force_compact_audit_log();}catch(const db_error&){rejected=true;}
    require(rejected&&owner.db().query("SELECT * FROM AuditLog ORDER BY id")==prior,"unknown revision never repaired by destructive reset");settled(owner);
}
void owned_count_with_ack() {
    File path;lattice_db owner(config(path.path));seed(owner);owner.db().execute("DELETE FROM AuditLog");
    struct Admission {
        std::mutex mutex;std::condition_variable changed;
        bool entered=false,attempted=false,stop=false;std::exception_ptr worker_error;
        static int trace(unsigned event,void* raw,void* statement,void*) noexcept {
            auto& s=*static_cast<Admission*>(raw);
            const char* sql=sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
            if(event!=SQLITE_TRACE_PROFILE||!sql||std::strncmp(sql,"INSERT INTO main.AuditLog",25)!=0)return 0;
            std::unique_lock lock(s.mutex);if(s.entered)return 0;s.entered=true;s.changed.notify_all();
            // Only wait for attempted admission, NEVER for the blocked SQL to finish.
            s.changed.wait(lock,[&]{return s.attempted;});return 0;
        }
    } admission;
    auto* raw=owner.db().handle();
    require(sqlite3_trace_v2(raw,SQLITE_TRACE_PROFILE,Admission::trace,&admission)==SQLITE_OK,"install bounded count seam");
    struct TraceReset{sqlite3* db;~TraceReset(){sqlite3_trace_v2(db,0,nullptr,nullptr);}} trace{raw};
    std::thread worker([&] {
        {std::unique_lock lock(admission.mutex);admission.changed.wait(lock,[&]{return admission.entered||admission.stop;});
         if(admission.stop)return;admission.attempted=true;admission.changed.notify_all();}
        try {
            const auto rows=owner.db().query("SELECT globalId FROM AuditLog ORDER BY id LIMIT 1");
            require(rows.size()==1,"generated row visible after admission release");
            mark_audit_entries_synced(owner,{std::get<std::string>(rows[0].at("globalId"))});
        }catch(...){admission.worker_error=std::current_exception();}
    });
    std::exception_ptr operation_error;int64_t count=-1;
    try {
        count=history_generation_test_access::generate(owner,2,[&](bool prepared) {
            if(prepared||!worker.joinable())return;
            // The first batch has committed and released SQLite ownership.
            // Let the competing public ACK finish before the next unit asks
            // for an idle connection; never wait for it from the trace hook.
            worker.join();
        });
    }catch(...){operation_error=std::current_exception();}
    {std::lock_guard lock(admission.mutex);admission.stop=true;}admission.changed.notify_all();if(worker.joinable())worker.join();
    sqlite3_trace_v2(raw,0,nullptr,nullptr);
    if(operation_error)std::rethrow_exception(operation_error);
    if(admission.worker_error)std::rethrow_exception(admission.worker_error);
    require(admission.entered&&admission.attempted,"competing ACK attempted during batch completion");
    require(count==2&&scalar(owner.db(),"SELECT COUNT(*) FROM AuditLog")==2,"owned count stays two");
    require(scalar(owner.db(),"SELECT COUNT(*) FROM AuditLog WHERE isSynchronized=1")==1,"public ACK completed");settled(owner);
}
void failed_prune_revision() {
    File path;lattice_db owner(config(path.path));seed(owner);owner.force_compact_audit_log();
    const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto revision=owner.db().query("SELECT value FROM _lattice_meta WHERE key='history_generation_revision_v1'");
    const auto maximum=scalar(owner.db(),"SELECT MAX(id) FROM AuditLog");
    auto* raw=owner.db().handle();
    require(sqlite3_set_authorizer(raw,[](void*,int action,const char* name,const char*,const char*,const char*) {
        return action==SQLITE_INSERT&&name&&std::strcmp(name,"_lattice_meta")==0?SQLITE_DENY:SQLITE_OK;
    },nullptr)==SQLITE_OK,"install revision fault");AuthorizerReset authorizer{raw};
    bool failed=false;try{owner.delete_audit_below_(maximum,false);}catch(const db_error&){failed=true;}
    sqlite3_set_authorizer(raw,nullptr,nullptr);
    require(failed,"failed revision update aborts prune");
    require(owner.db().query("SELECT * FROM AuditLog ORDER BY id")==before,"history deletion rolled back");
    require(owner.db().query("SELECT value FROM _lattice_meta WHERE key='history_generation_revision_v1'")==revision,"revision rollback exact");settled(owner);
}
void unsupported_frontier_shape(int shape) {
    File path;lattice_db owner(config(path.path));seed(owner);
    if(shape==0) {
        owner.db().execute("CREATE TABLE UnsupportedHistory(id INTEGER, globalId TEXT,value INTEGER,other INTEGER,PRIMARY KEY(id,other))");
        owner.db().execute("INSERT INTO UnsupportedHistory VALUES(1,'one',10,1),(1,'two',20,2)");
    } else if(shape==1) {
        owner.db().execute("CREATE TABLE UnsupportedHistory(id INTEGER PRIMARY KEY DESC,globalId TEXT,value INTEGER)");
        owner.db().execute("INSERT INTO UnsupportedHistory VALUES(NULL,'one',10),(NULL,'two',20)");
    } else if(shape==2) {
        owner.db().execute("CREATE TABLE _UnsupportedHistory(lhs TEXT,rhs TEXT,globalId TEXT,RoWiD INTEGER)");
        owner.db().execute("INSERT INTO _UnsupportedHistory VALUES('a','b','one',1),('a','c','two',1)");
    } else {
        owner.db().execute("CREATE TABLE UnsupportedHistory(id INTEGER PRIMARY KEY,globalId TEXT,value INTEGER) WITHOUT ROWID");
        owner.db().execute("INSERT INTO UnsupportedHistory VALUES(1,'one',10)");
    }
    const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    bool rejected=false;try{history_generation_test_access::force(owner,1);}catch(const db_error&){rejected=true;}
    require(rejected,"unsupported physical ordering rejected before reset");
    require(owner.db().query("SELECT * FROM AuditLog ORDER BY id")==before,"pre-reset history bytes retained");settled(owner);
}
void oversized_batch_rejected() {
    File path;lattice_db owner(config(path.path));seed(owner);
    const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    bool rejected=false;try{history_generation_test_access::force(owner,static_cast<int64_t>(std::numeric_limits<int>::max())+1);}catch(const db_error&){rejected=true;}
    require(rejected&&owner.db().query("SELECT * FROM AuditLog ORDER BY id")==before,"oversized batch fails before reset");settled(owner);
}
void empty_prune_preserves_revision() {
    File path;lattice_db owner(config(path.path));seed(owner);owner.force_compact_audit_log();
    const auto revision=owner.db().query("SELECT value FROM _lattice_meta WHERE key='history_generation_revision_v1'");
    require(scalar(owner.db(),"SELECT MIN(id) FROM AuditLog")>1,"sequence retained above prune bound");
    require(owner.delete_audit_below_(1,false)==0,"empty exact predicate removes nothing");
    require(owner.db().query("SELECT value FROM _lattice_meta WHERE key='history_generation_revision_v1'")==revision,"empty prune does not invalidate generation");settled(owner);
}
void existing_receipts_survive_history_generation() {
    File path;
    lattice_db owner(config(path.path));
    seed(owner);
    owner.db().execute("CREATE TABLE IF NOT EXISTS main._lattice_applied_receipts ("
                       "  globalId TEXT PRIMARY KEY)", {});
    owner.db().execute("INSERT INTO main._lattice_applied_receipts(globalId) VALUES(?)",
                      {std::string("00000000-0000-4000-8000-000000000071")});
    const auto before = owner.db().query(
        "SELECT globalId FROM main._lattice_applied_receipts ORDER BY globalId");
    require(before.size() == 1, "preexisting dedup receipt established");
    require(owner.force_compact_audit_log() == 2, "force snapshots both fixture rows");
    require(owner.db().query("SELECT globalId FROM main._lattice_applied_receipts ORDER BY globalId") == before,
            "force preserves exact preexisting dedup receipt");
    settled(owner);
    require(owner.generate_history(1) == 0, "standalone eligibility remains idempotent");
    require(owner.db().query("SELECT globalId FROM main._lattice_applied_receipts ORDER BY globalId") == before,
            "standalone preserves exact preexisting dedup receipt");
    settled(owner);
}

void real_schema_change_between_batches_is_rejected() {
    File path;
    lattice_db owner(config(path.path));
    seed(owner);
    lattice_db other(config(path.path));
    bool fired = false;
    auto callback = [&](bool prepared) {
        if (!prepared || fired) return;
        fired = true;
        settled(owner);
        other.db().execute("ALTER TABLE HistoryOwnedRow ADD COLUMN history_generation_external INTEGER");
        settled(other);
    };
    std::string error;
    try { history_generation_test_access::force(owner, 1, callback); }
    catch (const db_error& failure) { error = failure.what(); }
    require(fired, "actual second-handle schema mutation reached between owned units");
    require(error.find("history generation schema changed between batches") != std::string::npos,
            "known receipt setup does not weaken strict schema-change rejection");
    const auto columns = other.db().query("PRAGMA main.table_info(HistoryOwnedRow)");
    bool added = false;
    for (const auto& column : columns)
        if (std::get<std::string>(column.at("name")) == "history_generation_external") added = true;
    require(added, "other-handle schema change remains committed");
    require(scalar(owner.db(), "SELECT COUNT(*) FROM AuditLog WHERE synthesized=1 AND tableName='HistoryOwnedRow'") == 0,
            "no snapshot batch runs after stale schema is detected");
    settled(owner);
    settled(other);
}

void bounded(const std::function<void()>& body) {
    struct Style {std::string old=GTEST_FLAG_GET(death_test_style);~Style(){GTEST_FLAG_SET(death_test_style,old);}} style;
    GTEST_FLAG_SET(death_test_style,"threadsafe");
    ASSERT_EXIT({
        sigset_t set;sigemptyset(&set);sigaddset(&set,SIGALRM);
        if(std::signal(SIGALRM,SIG_DFL)==SIG_ERR||sigprocmask(SIG_UNBLOCK,&set,nullptr)!=0)_exit(2);
        alarm(10);
        try{body();std::fputs("history_ownership_complete\n",stderr);_exit(0);}
        catch(const std::exception& e){std::fprintf(stderr,"history_ownership_failure: %s\n",e.what());_exit(1);}
    },::testing::ExitedWithCode(0),"history_ownership_complete");
}
}
TEST(HistoryGenerationOwnership, UpdateBetweenResetAndSnapshotCreatesFreshPeerFile){bounded([]{reset_interleave(0,false);});}
TEST(HistoryGenerationOwnership, UpdateBetweenResetAndSnapshotCreatesFreshPeerMemory){bounded([]{reset_interleave(0,true);});}
TEST(HistoryGenerationOwnership, DeleteBeforeSnapshotIsNotResurrected){bounded([]{reset_interleave(1,false);});}
TEST(HistoryGenerationOwnership, RemotePartialUpsertStillGetsCompleteSnapshot){bounded([]{reset_interleave(2,false);});}
TEST(HistoryGenerationOwnership, AuditedUpdateAfterSnapshotSurvives){bounded([]{after_snapshot(0);});}
TEST(HistoryGenerationOwnership, DeleteAfterSnapshotSurvives){bounded([]{after_snapshot(1);});}
TEST(HistoryGenerationOwnership, DeleteReinsertAndAboveFrontierInsertSurvive){bounded([]{after_snapshot(2);});}
TEST(HistoryGenerationOwnership, AnotherHandleResetInvalidatesGeneration){bounded([]{reset_or_prune(false);});}
TEST(HistoryGenerationOwnership, AnotherHandlePruneInvalidatesGeneration){bounded([]{reset_or_prune(true);});}
TEST(HistoryGenerationOwnership, AnotherProcessResetInvalidatesGeneration){
    if(const auto* path=std::getenv("LATTICE_HISTORY_RESET_PEER")) {
        sigset_t set;sigemptyset(&set);sigaddset(&set,SIGALRM);
        ASSERT_NE(std::signal(SIGALRM,SIG_DFL),SIG_ERR);ASSERT_EQ(sigprocmask(SIG_UNBLOCK,&set,nullptr),0);
        alarm(5);{lattice_db other(config(path));EXPECT_EQ(other.force_compact_audit_log(),2);}alarm(0);
        return;
    }
    bounded(second_process_reset);
}
TEST(HistoryGenerationOwnership, RecursiveGenerationFailsFast){bounded([]{recursive_or_successor(false);});}
TEST(HistoryGenerationOwnership, CallbackSuccessorTransactionIsNotRolledBack){bounded([]{recursive_or_successor(true);});}
TEST(HistoryGenerationOwnership, FlagZeroAndStandaloneEligibilityPreserved){bounded([]{flags_and_predicate(0);});}
TEST(HistoryGenerationOwnership, FlagOneAndStandaloneEligibilityPreserved){bounded([]{flags_and_predicate(1);});}
TEST(HistoryGenerationOwnership, DeniedFlagReadLeavesHistoryUnchanged){bounded([]{failed_reset(false);});}
TEST(HistoryGenerationOwnership, DeniedFlagRestoreRollsBackReset){bounded([]{failed_reset(true);});}
TEST(HistoryGenerationOwnership, CallerTransactionIsNotJoined){bounded(caller_transaction);}
TEST(HistoryGenerationOwnership, MalformedRevisionFailsBeforeReset){bounded(malformed_revision);}
TEST(HistoryGenerationOwnership, PublicAckCannotOverwriteOwnedCount){bounded(owned_count_with_ack);}
TEST(HistoryGenerationOwnership, FailedPruneRevisionRestoresHistoryAndFlag){bounded(failed_prune_revision);}
TEST(HistoryGenerationOwnership, CompositeIntegerKeyRejectedBeforeReset){bounded([]{unsupported_frontier_shape(0);});}
TEST(HistoryGenerationOwnership, DescendingNonAliasKeyRejectedBeforeReset){bounded([]{unsupported_frontier_shape(1);});}
TEST(HistoryGenerationOwnership, ShadowedLinkRowidRejectedBeforeReset){bounded([]{unsupported_frontier_shape(2);});}
TEST(HistoryGenerationOwnership, WithoutRowidModelRejectedBeforeReset){bounded([]{unsupported_frontier_shape(3);});}
TEST(HistoryGenerationOwnership, OversizedBatchRejectedBeforeReset){bounded(oversized_batch_rejected);}
TEST(HistoryGenerationOwnership, EmptyPrunePreservesRevision){bounded(empty_prune_preserves_revision);}
TEST(HistoryGenerationOwnership, ExistingReceiptsSurviveForceAndStandaloneHistory) {
    bounded(existing_receipts_survive_history_generation);
}
TEST(HistoryGenerationOwnership, RealSchemaChangeBetweenBatchesIsRejected) {
    bounded(real_schema_change_between_batches_is_rejected);
}
#endif
