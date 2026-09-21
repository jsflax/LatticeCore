#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <chrono>
#include <cstring>
#include <atomic>
#include <cstdlib>
#include <cerrno>
#include <csignal>
#if defined(__APPLE__) || defined(__linux__)
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

struct RetentionRow { std::string body; };
LATTICE_SCHEMA(RetentionRow,body);
namespace lattice::detail {
struct canonical_retention_test_access {
    static sync_recovery::owned_canonical_capture capture(canonical_writer_adapter& adapter,
        std::shared_ptr<lattice_db> owner,const canonical_retention_ticket& ticket,
        const sync_recovery::canonical_capture_limits& limits,const std::function<void(size_t,uint64_t)>& after) {
        return adapter.capture_reserved_impl(std::move(owner),ticket,{},limits,after);
    }
};
}
#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using namespace lattice::detail;
using phase=recovery_install_state;
namespace sr=lattice::detail::sync_recovery;
configuration config(const std::string& path){configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
canonical_writer_profile profile(){return {{"retained-source","epoch","scope","schema"},{128,65536,128,65536,32,64,64},{"RetentionRow"},false};}
std::shared_ptr<lattice_db> open_owner(const std::string& path) {
    auto result=std::make_shared<lattice_db>(config(path));
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
    return result;
}
int64_t scalar(database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).begin()->second);}
void committed(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=phase::committed)throw std::runtime_error("retention transaction did not commit");
}
struct watchdog {
    std::mutex mutex;std::condition_variable cv;bool done=false;
    std::thread thread{[this]{std::unique_lock<std::mutex> lock(mutex);if(!cv.wait_for(lock,std::chrono::seconds(15),[&]{return done;}))std::abort();}};
    ~watchdog(){{std::lock_guard<std::mutex> lock(mutex);done=true;}cv.notify_one();thread.join();}
};
class CanonicalTransferRetention:public ::testing::Test {
protected:
    TempDB file{"canonical-retention"};
    std::shared_ptr<lattice_db> owner=open_owner(file.str()), sibling=open_owner(file.str());
    canonical_writer_profile p=profile();
    canonical_retention_limits retention{4,10000};
    sr::canonical_capture_limits limits{{{65536,16,4096,8192,2,32,64,524288},8,16,16},p.limits,32,64,2};
    std::unique_ptr<canonical_writer_adapter> adapter;
    void attach(){adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,retention);}
    canonical_retention_ticket reserve(std::optional<int64_t> base={},int64_t duration=10000){
        auto result=adapter->reserve_recovery_owned(owner,base,duration);committed(result.settlement);
        if(!result.reservation)throw std::runtime_error("committed reservation not returned");return *result.reservation;
    }
    int64_t count(){return scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt");}
    int64_t head(){return scalar(owner->db(),"SELECT head FROM _lattice_canonical_store");}
    int64_t floor(){return scalar(owner->db(),"SELECT floor FROM _lattice_canonical_store");}
};
struct Fault {
    enum kind {insert_ignore,insert_deny,counter_ignore,commit_deny,cleanup_deny,release_deny};
    static thread_local Fault* active;
    kind value;int hits=0,cleanup_hits=0;
    canonical_upstream_test_hooks::authorizer_fault probe;
    const canonical_upstream_test_hooks::authorizer_fault* previous;
    Fault* prior;
    Fault(database& db,kind k):value(k),probe{canonical_writer_custody_test_access::fault_handle(db),restrict},previous(canonical_retention_test_hooks::fault),prior(active){active=this;canonical_retention_test_hooks::fault=&probe;}
    ~Fault(){canonical_retention_test_hooks::fault=previous;active=prior;}
    static int restrict(int action,const char* one,const char* two,const char* origin) noexcept {
        auto& f=*active;const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};
        if(origin)return SQLITE_OK;
        if(f.value==cleanup_deny&&action==SQLITE_TRANSACTION&&same(one,"ROLLBACK")){++f.cleanup_hits;return SQLITE_DENY;}
        if(f.hits)return SQLITE_OK;
        if(action==SQLITE_INSERT&&same(one,"_lattice_canonical_attempt")&&
           (f.value==insert_ignore||f.value==insert_deny||f.value==cleanup_deny)){++f.hits;return f.value==insert_ignore?SQLITE_IGNORE:SQLITE_DENY;}
        if(f.value==counter_ignore&&action==SQLITE_UPDATE&&same(one,"_lattice_canonical_retention")&&same(two,"next_attempt")){++f.hits;return SQLITE_IGNORE;}
        if(f.value==commit_deny&&action==SQLITE_TRANSACTION&&same(one,"COMMIT")){++f.hits;return SQLITE_DENY;}
        if(f.value==release_deny&&action==SQLITE_DELETE&&same(one,"_lattice_canonical_attempt")){++f.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local Fault* Fault::active=nullptr;
}

TEST_F(CanonicalTransferRetention, ActualOwnerReservesBeforeCaptureAndTailRemainsProtected) {
    attach();owner->add(RetentionRow{"before"});auto ticket=reserve();const auto pinned=ticket.protected_base();
    EXPECT_EQ(count(),1);EXPECT_EQ(pinned,head());owner->add(RetentionRow{"between reservation and capture"});
    const auto capture=adapter->capture_reserved_owned(owner,ticket,{},limits);
    ASSERT_TRUE(capture.capture);EXPECT_EQ(capture.selection,sr::source_capture_selection::full);
    EXPECT_EQ(capture.capture->rows.size(),2u);EXPECT_GT(capture.head,pinned);
    EXPECT_NE(adapter->prune_recovery_owned(owner,capture.head).state,phase::committed);EXPECT_EQ(floor(),0);
    committed(adapter->prune_recovery_owned(owner,pinned));EXPECT_EQ(floor(),pinned);
    const auto receipts=scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_receipt");
    committed(adapter->release_recovery_owned(owner,ticket));committed(adapter->prune_recovery_owned(owner,head()));
    EXPECT_EQ(count(),0);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_touch"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_receipt"),receipts);
    EXPECT_THROW(adapter->capture_recovery_owned(owner,p.binding,{}, {},limits),db_error);
    static_assert(!canonical_writer_adapter::serving_capability);
}
TEST_F(CanonicalTransferRetention, PreopenedSiblingAndOrdinaryOwnedCallbacksCannotMutateRegistryOrFloor) {
    attach();owner->add(RetentionRow{"covered"});auto ticket=reserve(0);
    for(const auto* sql:{"DELETE FROM _lattice_canonical_attempt","UPDATE _lattice_canonical_retention SET incarnation=99",
        "UPDATE _lattice_canonical_store SET floor=head","DELETE FROM _lattice_canonical_touch"}) {
        EXPECT_THROW(sibling->db().execute(sql),db_error);EXPECT_THROW(owner->db().execute(sql),db_error);
    }
    sibling->begin_transaction();canonical_change_store primitive(*sibling,p.binding,p.limits);
    EXPECT_THROW(primitive.advance_floor(head(),head()),db_error);sibling->rollback();
    auto ordinary=recovery_writer_access::install(owner,[](database& db){db.execute("DELETE FROM _lattice_canonical_attempt");});
    EXPECT_EQ(ordinary.state,phase::rolled_back);EXPECT_NE(ordinary.primary_error,nullptr);EXPECT_EQ(count(),1);EXPECT_EQ(floor(),0);
    owner->begin_transaction();auto refused=adapter->reserve_recovery_owned(owner,0,10000);
    EXPECT_EQ(refused.settlement.state,phase::refused);EXPECT_TRUE(owner->db().is_in_transaction());owner->rollback();
    EXPECT_EQ(adapter->reserve_recovery_owned(sibling,0,10000).settlement.state,phase::refused);
    committed(adapter->release_recovery_owned(owner,ticket));
}
TEST_F(CanonicalTransferRetention, DeltaBaseIsFrozenAndRetiredOrAheadRequestsDoNotReserve) {
    attach();owner->add(RetentionRow{"one"});auto ticket=reserve(0);owner->add(RetentionRow{"two"});
    auto captured=adapter->capture_reserved_owned(owner,ticket,{},limits);ASSERT_TRUE(captured.capture);
    EXPECT_EQ(captured.selection,sr::source_capture_selection::delta);EXPECT_EQ(captured.requested_base,std::optional<int64_t>(0));
    committed(adapter->release_recovery_owned(owner,ticket));committed(adapter->prune_recovery_owned(owner,head()));
    EXPECT_NE(adapter->reserve_recovery_owned(owner,0,10000).settlement.state,phase::committed);
    EXPECT_NE(adapter->reserve_recovery_owned(owner,head()+1,10000).settlement.state,phase::committed);EXPECT_EQ(count(),0);
}
TEST_F(CanonicalTransferRetention, FiniteCapacityAndSourceExpiryReleaseOnlyUnadvertisedAttempts) {
    retention.attempts=1;attach();owner->add(RetentionRow{"covered"});auto ticket=reserve(0,1);
    EXPECT_NE(adapter->reserve_recovery_owned(owner,0,10000).settlement.state,phase::committed);
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    EXPECT_THROW(adapter->capture_reserved_owned(owner,ticket,{},limits),db_error);
    committed(adapter->expire_recovery_owned(owner));EXPECT_EQ(count(),0);
    committed(adapter->release_recovery_owned(owner,ticket)); // exact absent retry
    auto next=reserve(0,1);std::this_thread::sleep_for(std::chrono::milliseconds(3));
    committed(adapter->prune_recovery_owned(owner,head()));EXPECT_EQ(count(),0);
    EXPECT_NE(adapter->reserve_recovery_owned(owner,{},0).settlement.state,phase::committed);
    EXPECT_NE(adapter->reserve_recovery_owned(owner,{},retention.duration_ms+1).settlement.state,phase::committed);
}
TEST_F(CanonicalTransferRetention, IgnoredWritesAndDeniedCommitKeepOriginalRegistryAndCounter) {
    attach();
    for(auto kind:{Fault::insert_ignore,Fault::insert_deny,Fault::counter_ignore,Fault::commit_deny}) {
        Fault fault(owner->db(),kind);auto result=adapter->reserve_recovery_owned(owner,{},10000);
        EXPECT_EQ(fault.hits,1);EXPECT_EQ(result.settlement.state,phase::rolled_back);EXPECT_NE(result.settlement.primary_error,nullptr);
        EXPECT_EQ(result.settlement.cleanup_error,nullptr);EXPECT_FALSE(result.reservation);EXPECT_EQ(count(),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT next_attempt FROM _lattice_canonical_retention"),0);
    }
    auto ticket=reserve();committed(adapter->release_recovery_owned(owner,ticket));
}
TEST_F(CanonicalTransferRetention, FailedReleaseRetainsProtectionUntilRealCleanup) {
    attach();owner->add(RetentionRow{"one"});auto ticket=reserve(0);
    {Fault fault(owner->db(),Fault::release_deny);const auto result=adapter->release_recovery_owned(owner,ticket);
     EXPECT_EQ(result.state,phase::rolled_back);EXPECT_NE(result.primary_error,nullptr);EXPECT_EQ(count(),1);}
    EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
    committed(adapter->release_recovery_owned(owner,ticket));committed(adapter->prune_recovery_owned(owner,head()));
}
TEST_F(CanonicalTransferRetention, CleanupFailureStaysDistinctAndFencesWriter) {
    attach();Fault fault(owner->db(),Fault::cleanup_deny);auto result=adapter->reserve_recovery_owned(owner,{},10000);
    EXPECT_EQ(result.settlement.state,phase::unsettled);EXPECT_NE(result.settlement.primary_error,nullptr);
    EXPECT_NE(result.settlement.cleanup_error,nullptr);EXPECT_EQ(fault.cleanup_hits,1);EXPECT_FALSE(result.reservation);
    EXPECT_TRUE(owner->db().is_closed());EXPECT_NE(adapter->expire_recovery_owned(owner).state,phase::committed);
}
TEST_F(CanonicalTransferRetention, PostcommitErrorReturnsDurableTicketAndDoesNotReplayReservation) {
    attach();const auto hook=owner->add_invalidation_hook([](const auto&,auto){throw std::runtime_error("retention observer");});
    auto result=adapter->reserve_recovery_owned(owner,{},10000);owner->remove_invalidation_hook(hook);
    EXPECT_EQ(result.settlement.state,phase::committed);EXPECT_EQ(result.settlement.primary_error,nullptr);
    EXPECT_NE(result.settlement.postcommit_error,nullptr);ASSERT_TRUE(result.reservation);EXPECT_EQ(count(),1);
    committed(adapter->release_recovery_owned(owner,*result.reservation));EXPECT_EQ(count(),0);
}
TEST_F(CanonicalTransferRetention, CaptureFailureEndsReadCustodyBeforeExplicitRelease) {
    attach();owner->add(RetentionRow{"one"});owner->add(RetentionRow{"two"});auto ticket=reserve(0);
    auto small=limits;small.rows.wire.total_rows=1;EXPECT_ANY_THROW(adapter->capture_reserved_owned(owner,ticket,{},small));
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);EXPECT_EQ(count(),1);
    committed(adapter->release_recovery_owned(owner,ticket));EXPECT_EQ(count(),0);
}
TEST_F(CanonicalTransferRetention, PruneAndReserveSerializeOnTheActualRegistry) {
    watchdog bounded;attach();owner->add(RetentionRow{"one"});const auto target=head();
    std::atomic<int> ready{0};std::atomic<bool> start{false};canonical_retention_result reserved;recovery_install_result pruned;
    std::thread first,second;
    try {
        first=std::thread([&]{++ready;while(!start.load())std::this_thread::yield();reserved=adapter->reserve_recovery_owned(owner,0,10000);});
        second=std::thread([&]{++ready;while(!start.load())std::this_thread::yield();pruned=adapter->prune_recovery_owned(owner,target);});
    } catch(...) {start.store(true);if(first.joinable())first.join();if(second.joinable())second.join();throw;}
    while(ready.load()!=2)std::this_thread::yield();start.store(true);first.join();second.join();
    const bool have_reservation=reserved.settlement.state==phase::committed;
    EXPECT_NE(have_reservation,pruned.state==phase::committed);
    if(have_reservation){ASSERT_TRUE(reserved.reservation);EXPECT_EQ(floor(),0);EXPECT_EQ(count(),1);committed(adapter->release_recovery_owned(owner,*reserved.reservation));}
    else {EXPECT_EQ(floor(),target);EXPECT_EQ(count(),0);}
}
TEST_F(CanonicalTransferRetention, ExpiryCannotRemoveProtectionWhileCaptureStillOwnsReadCustody) {
    watchdog bounded;attach();owner->add(RetentionRow{"one"});auto ticket=reserve(0,1000);
    bool reached=false;std::atomic<bool> started{false},done{false};std::thread expiry;recovery_install_result result;
    EXPECT_THROW(canonical_retention_test_access::capture(*adapter,owner,ticket,limits,[&](size_t,uint64_t){
        if(reached)return;reached=true;
        expiry=std::thread([&]{started.store(true);result=adapter->expire_recovery_owned(owner);done.store(true);});
        while(!started.load())std::this_thread::yield();std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        EXPECT_FALSE(done.load());EXPECT_EQ(scalar(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
    }),db_error);
    if(expiry.joinable())expiry.join();ASSERT_TRUE(reached);committed(result);EXPECT_TRUE(done.load());
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);EXPECT_EQ(count(),0);
}
TEST_F(CanonicalTransferRetention, RetiredCaptureKeepsFileCustodyUntilItsGenerationEnds) {
    watchdog bounded;attach();owner->add(RetentionRow{"one"});auto ticket=reserve();auto* entered=adapter.get();bool retired=false;
    EXPECT_THROW(canonical_retention_test_access::capture(*entered,owner,ticket,limits,[&](size_t,uint64_t){
        if(retired)return;retired=true;adapter.reset();
        EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(owner,p,retention),db_error);
        EXPECT_EQ(scalar(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
    }),db_error);
    ASSERT_TRUE(retired);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);attach();EXPECT_EQ(count(),0);
}
TEST_F(CanonicalTransferRetention, RetiredAndReplacedOwnersCannotUseOldIncarnation) {
    attach();auto old=reserve();
    EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(sibling,p,retention),db_error);
    adapter.reset();attach();EXPECT_EQ(count(),0);
    EXPECT_EQ(adapter->release_recovery_owned(owner,old).state,phase::rolled_back);
    EXPECT_THROW(adapter->capture_reserved_owned(owner,old,{},limits),db_error);
    auto current=reserve();owner->reopen_write_db();
    EXPECT_NE(adapter->release_recovery_owned(owner,current).state,phase::committed);
    adapter.reset();attach();EXPECT_EQ(count(),0);auto final=reserve();owner->close();
    EXPECT_NE(adapter->release_recovery_owned(owner,final).state,phase::committed);
    EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(sibling,p,retention),db_error);
    adapter.reset();auto successor=canonical_writer_adapter::attach_retention_for_qualification(sibling,p,retention);
    EXPECT_EQ(scalar(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
}
TEST_F(CanonicalTransferRetention, DetectedMainPathMovementRefusesBeforeRegistryMutation) {
    attach();auto ticket=reserve();TempDB moved{"canonical-retention-moved"};
    std::filesystem::rename(file.path,moved.path);
    struct restore {
        std::filesystem::path from,to;
        ~restore(){std::error_code error;std::filesystem::rename(from,to,error);if(error)std::abort();}
    } restore_name{moved.path,file.path};
    const auto result=adapter->reserve_recovery_owned(owner,{},10000);
    EXPECT_NE(result.settlement.state,phase::committed);EXPECT_FALSE(result.reservation);EXPECT_EQ(count(),1);
}
TEST_F(CanonicalTransferRetention, ExactReopenRejectsMissingAlteredAndExtraPersistentGuards) {
    for(int mode=0;mode!=3;++mode) {
        TempDB corrupt{"canonical-retention-corrupt"};auto victim=open_owner(corrupt.str()), mutator=open_owner(corrupt.str());
        auto attached=canonical_writer_adapter::attach_retention_for_qualification(victim,p,retention);
        auto reserved=attached->reserve_recovery_owned(victim,{},10000);committed(reserved.settlement);attached.reset();
        const std::string name="_lattice_canonical_attempt_retention_DELETE";
        const auto correct=std::get<std::string>(mutator->db().query("SELECT sql FROM sqlite_master WHERE name=?",{name}).at(0).at("sql"));
        if(mode<2)mutator->db().execute("DROP TRIGGER "+name);
        if(mode==1)mutator->db().execute("CREATE TRIGGER "+name+" BEFORE DELETE ON _lattice_canonical_attempt BEGIN SELECT 1; END");
        if(mode==2)mutator->db().execute("CREATE TRIGGER unexpected_retention BEFORE DELETE ON _lattice_canonical_attempt BEGIN SELECT 1; END");
        const auto before=mutator->db().query("SELECT * FROM _lattice_canonical_attempt");
        EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(victim,p,retention),db_error);
        EXPECT_EQ(mutator->db().query("SELECT * FROM _lattice_canonical_attempt"),before);
        if(mode==1)mutator->db().execute("DROP TRIGGER "+name);
        if(mode<2)mutator->db().execute(correct);
        else mutator->db().execute("DROP TRIGGER unexpected_retention");
        // Failed attachment must release only its own flock custody. The exact
        // restored profile can then reopen and fence the abandoned attempt.
        auto restored=canonical_writer_adapter::attach_retention_for_qualification(victim,p,retention);
        EXPECT_EQ(scalar(victim->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
    }
}
TEST_F(CanonicalTransferRetention, ExistingLegacyCoverageCannotSilentlyUpgradeAndProtectedCannotDowngrade) {
    auto legacy=canonical_writer_adapter::attach(*owner,p);legacy.reset();
    EXPECT_THROW(attach(),db_error);EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_retention"));
    TempDB protected_file{"canonical-retention-downgrade"};auto protected_owner=open_owner(protected_file.str());
    auto attached=canonical_writer_adapter::attach_retention_for_qualification(protected_owner,p,retention);attached.reset();
    EXPECT_THROW(canonical_writer_adapter::attach(*protected_owner,p),db_error);
}

TEST(CanonicalTransferRetentionRestart, FreshProcessesFencePriorUnadvertisedIncarnation) {
    constexpr const char* variable="LATTICE_RETENTION_PEER=";
    if(const char* encoded=std::getenv("LATTICE_RETENTION_PEER")) {
        sigset_t signals;sigemptyset(&signals);sigaddset(&signals,SIGALRM);
        ASSERT_NE(std::signal(SIGALRM,SIG_DFL),SIG_ERR);ASSERT_EQ(sigprocmask(SIG_UNBLOCK,&signals,nullptr),0);alarm(30);
        const std::string input(encoded);ASSERT_GT(input.size(),2u);const bool seed=input[0]=='s';
        auto owner=open_owner(input.substr(2));auto p=profile();
        auto adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000});
        EXPECT_EQ(scalar(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),seed?1:2);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
        if(seed)owner->add(RetentionRow{"persists across restart"});
        else EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM RetentionRow"),1);
        auto reserved=adapter->reserve_recovery_owned(owner,{},10000);committed(reserved.settlement);ASSERT_TRUE(reserved.reservation);
        if(!seed)committed(adapter->release_recovery_owned(owner,*reserved.reservation));
        alarm(0);return;
    }
    // Both producer and reopener are fresh filtered children, so unrelated
    // process-global test schema mutations cannot change only one catalog.
    TempDB file{"retention-restart"};char executable[4096];
#if defined(__APPLE__)
    uint32_t length=sizeof(executable);ASSERT_EQ(_NSGetExecutablePath(executable,&length),0);
#else
    const auto length=readlink("/proc/self/exe",executable,sizeof(executable)-1);ASSERT_GT(length,0);
    ASSERT_LT(length,static_cast<ssize_t>(sizeof(executable)-1));executable[length]=0;
#endif
    for(const auto* phase_name:{"seed","reopen"}) {
        const std::string phase(phase_name);const auto* parent_log=std::getenv("LATTICE_TEST_LOG_PATH");
        const auto native=parent_log&&*parent_log?std::string(parent_log)+"."+file.path.filename().string()+"."+phase+".native.log":file.str()+"."+phase+".native.log";
        std::vector<std::string> values;
        for(char** value=environ;*value;++value)if(std::strncmp(*value,variable,std::strlen(variable))&&std::strncmp(*value,"LATTICE_TEST_LOG_PATH=",22))values.emplace_back(*value);
        values.push_back(std::string(variable)+(phase=="seed"?"s:":"r:")+file.str());values.push_back("LATTICE_TEST_LOG_PATH="+native);
        std::vector<char*> environment;for(auto& value:values)environment.push_back(value.data());environment.push_back(nullptr);
        std::string filter="--gtest_filter=CanonicalTransferRetentionRestart.FreshProcessesFencePriorUnadvertisedIncarnation";
        std::string color="--gtest_color=no",repeat="--gtest_repeat=1",output="--gtest_output=";
        char* arguments[]={executable,filter.data(),color.data(),repeat.data(),output.data(),nullptr};
        posix_spawn_file_actions_t actions;ASSERT_EQ(posix_spawn_file_actions_init(&actions),0);
        struct destroy {posix_spawn_file_actions_t& value;~destroy(){posix_spawn_file_actions_destroy(&value);}} cleanup{actions};
        const auto log=file.str()+"."+phase+".log";
        ASSERT_EQ(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600),0);
        ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO),0);
        pid_t child=-1;ASSERT_EQ(posix_spawn(&child,executable,&actions,nullptr,arguments,environment.data()),0);
        int status=0;pid_t waited=-1;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(45);
        do {waited=waitpid(child,&status,WNOHANG);if(waited==child || (waited<0&&errno!=EINTR))break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));}while(std::chrono::steady_clock::now()<deadline);
        ASSERT_FALSE(waited<0&&errno!=EINTR)<<"owned child observation failed; no unverifiable signal";
        if(waited!=child){kill(child,SIGKILL);do{waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);FAIL()<<"retention child deadline: "<<log;}
        ASSERT_TRUE(WIFEXITED(status))<<log;ASSERT_EQ(WEXITSTATUS(status),0)<<log;
    }
}
#endif
