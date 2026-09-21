#include "TestHelpers.hpp"
#include "RetentionCustodyPhaseDiagnostics.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <chrono>
#include <cerrno>
#include <csignal>
#include <stdexcept>
#if defined(__APPLE__) || defined(__linux__)
#include <spawn.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

struct RetentionCustodyRow {std::string body;};
LATTICE_SCHEMA(RetentionCustodyRow,body);
#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using namespace lattice::detail;
using phase=recovery_install_state;
struct OwnedFile {
    TempDB name{"retention-directory-custody"};
    std::filesystem::path directory=name.str()+".directory",path=directory/"source.sqlite";
    OwnedFile(){std::filesystem::create_directory(directory);}
    ~OwnedFile(){std::error_code ignored;std::filesystem::remove_all(directory,ignored);}
    std::string str()const{return path.string();}
};
canonical_writer_profile custody_profile(){return {{"custody-source","epoch","scope","schema"},{128,65536,128,65536,32,64,64},{"RetentionCustodyRow"},false};}
std::shared_ptr<lattice_db> custody_owner(const std::string& path) {
    configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(c);
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
    return owner;
}
std::filesystem::path custody_path(lattice_db& owner) {
    const auto identity=owner.db().physical_identity("main",{},true);
    if(!identity)throw std::runtime_error("missing actual identity");
    return std::filesystem::path(identity->filename).parent_path()/
        (".lattice-retention-"+std::to_string(identity->device)+"-"+std::to_string(identity->inode));
}
int64_t number(database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).begin()->second);}
void require_commit(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=phase::committed)throw std::runtime_error("custody transaction did not commit");
}
// Each child starts with a fresh native schema registry and its own native log.
// No SQLite APIs execute in a forked copy of a live parent connection.
bool child_case(const std::string& filter,const std::string& payload,const std::string& prefix,const std::string& phase_name,int timeout_seconds=45) {
    char executable[4096];
#if defined(__APPLE__)
    uint32_t length=sizeof(executable);if(_NSGetExecutablePath(executable,&length)!=0)throw std::runtime_error("executable path too long");
#else
    const auto length=readlink("/proc/self/exe",executable,sizeof(executable)-1);
    if(length<=0 || length>=static_cast<ssize_t>(sizeof(executable)-1))throw std::runtime_error("executable path unavailable");executable[length]=0;
#endif
    constexpr const char* variable="LATTICE_CUSTODY_PEER=";
    const auto* parent_log=std::getenv("LATTICE_TEST_LOG_PATH");
    const auto native=parent_log&&*parent_log?std::string(parent_log)+"."+std::filesystem::path(prefix).filename().string()+"."+phase_name+".native.log":prefix+"."+phase_name+".native.log";
    std::vector<std::string> values;
    for(char** value=environ;*value;++value)if(std::strncmp(*value,variable,std::strlen(variable))&&std::strncmp(*value,"LATTICE_TEST_LOG_PATH=",22))values.emplace_back(*value);
    values.push_back(std::string(variable)+payload);values.push_back("LATTICE_TEST_LOG_PATH="+native);
    std::vector<char*> environment;for(auto& value:values)environment.push_back(value.data());environment.push_back(nullptr);
    std::string selected="--gtest_filter="+filter,color="--gtest_color=no",repeat="--gtest_repeat=1",output="--gtest_output=";
    char* arguments[]={executable,selected.data(),color.data(),repeat.data(),output.data(),nullptr};
    posix_spawn_file_actions_t actions;
    if(posix_spawn_file_actions_init(&actions)!=0)throw std::runtime_error("spawn actions unavailable");
    struct destroy {posix_spawn_file_actions_t& value;~destroy(){posix_spawn_file_actions_destroy(&value);}} cleanup{actions};
    const auto log=prefix+"."+phase_name+".terminal.log";
    if(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)!=0 ||
       posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO)!=0)throw std::runtime_error("child log setup failed");
    pid_t child=-1;if(posix_spawn(&child,executable,&actions,nullptr,arguments,environment.data())!=0)throw std::runtime_error("child spawn failed");
    int status=0;pid_t waited=-1;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(timeout_seconds);
    do {waited=waitpid(child,&status,WNOHANG);if(waited==child || (waited<0&&errno!=EINTR))break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));}while(std::chrono::steady_clock::now()<deadline);
    if(waited<0&&errno!=EINTR)throw std::runtime_error("owned child observation failed; no unverifiable signal");
    if(waited!=child){kill(child,SIGKILL);do{waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);throw std::runtime_error("custody child deadline: "+log);}
    return WIFEXITED(status)&&WEXITSTATUS(status)==0;
}
void child_deadline(unsigned seconds=30) {
    struct sigaction action{};action.sa_handler=SIG_DFL;sigemptyset(&action.sa_mask);
    if(sigaction(SIGALRM,&action,nullptr)!=0)throw std::runtime_error("child signal setup failed");
    sigset_t unblocked;sigemptyset(&unblocked);sigaddset(&unblocked,SIGALRM);
    if(pthread_sigmask(SIG_UNBLOCK,&unblocked,nullptr)!=0)throw std::runtime_error("child signal unblock failed");alarm(seconds);
}
class CanonicalRetentionCustody:public ::testing::Test {
protected:
    OwnedFile file;
    std::shared_ptr<lattice_db> owner=custody_owner(file.str()), sibling=custody_owner(file.str());
    canonical_writer_profile p=custody_profile();
    canonical_retention_limits limits{4,10000};
    std::unique_ptr<canonical_writer_adapter> adapter;
    void attach(){adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,limits);}
    void reserve(){const auto result=adapter->reserve_recovery_owned(owner,{},10000);require_commit(result.settlement);if(!result.reservation)throw std::runtime_error("reservation absent");}
};
struct RestoreName {
    std::filesystem::path saved,original;
    ~RestoreName(){std::error_code error;std::filesystem::remove_all(original,error);error.clear();std::filesystem::rename(saved,original,error);if(error)std::abort();}
};
}

TEST_F(CanonicalRetentionCustody, ProfileBindsMainParentAndDirectoryAndReopens) {
    attach();reserve();const auto path=custody_path(*owner);
    struct stat main{},parent{},custody{};
    ASSERT_EQ(stat(file.str().c_str(),&main),0);ASSERT_EQ(stat(path.parent_path().c_str(),&parent),0);ASSERT_EQ(lstat(path.c_str(),&custody),0);
    ASSERT_TRUE(S_ISDIR(custody.st_mode));EXPECT_EQ(custody.st_mode&07777,0700);EXPECT_EQ(custody.st_uid,geteuid());
    const auto row=owner->db().query("SELECT * FROM _lattice_canonical_retention").at(0);
    EXPECT_EQ(std::get<int64_t>(row.at("version")),2);
    for(const auto& pair:std::vector<std::pair<std::string,int64_t>>{{"main_device",static_cast<int64_t>(main.st_dev)},{"main_inode",static_cast<int64_t>(main.st_ino)},{"parent_device",static_cast<int64_t>(parent.st_dev)},{"parent_inode",static_cast<int64_t>(parent.st_ino)},{"custody_device",static_cast<int64_t>(custody.st_dev)},{"custody_inode",static_cast<int64_t>(custody.st_ino)}})
        EXPECT_EQ(std::get<int64_t>(row.at(pair.first)),pair.second);
    adapter.reset();attach();EXPECT_EQ(number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),2);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
    EXPECT_TRUE(std::filesystem::exists(path));static_assert(!canonical_writer_adapter::serving_capability);
}
TEST_F(CanonicalRetentionCustody, MissingReplacedSymlinkAndRegularArtifactRefuseWithoutCleanup) {
    retention_fixture_diagnostics::phases diagnostics("CanonicalRetentionCustody.MissingReplacedSymlinkAndRegularArtifactRefuseWithoutCleanup");
    diagnostics.call("initial-owner-attach",[&]{attach();});
    diagnostics.call("initial-owner-reserve",[&]{reserve();});
    const auto path=diagnostics.call("initial-custody-path",[&]{return custody_path(*owner);});const auto saved=path.string()+".saved";
    const auto profile=diagnostics.call("initial-owner-profile-query",[&]{return owner->db().query("SELECT * FROM _lattice_canonical_retention");}),
        attempts=diagnostics.call("initial-owner-attempts-query",[&]{return owner->db().query("SELECT * FROM _lattice_canonical_attempt");});
    for(int mode=0;mode<4;++mode) {
        diagnostics.mode(mode==0?"0-missing":mode==1?"1-replaced-directory":mode==2?"2-symlink":"3-main-hardlink");
        diagnostics.call("rename-custody-to-saved",[&]{std::filesystem::rename(path,saved);});
        auto restoration=diagnostics.restored("restore-custody-name");RestoreName restore{saved,path};
        if(mode==1){ASSERT_EQ(diagnostics.call("create-replacement-directory",[&]{return mkdir(path.c_str(),0700);}),0);}
        if(mode==2)diagnostics.call("create-custody-symlink",[&]{std::filesystem::create_directory_symlink(saved,path);});
        // A hardlink to the main file must fail O_DIRECTORY without acquiring
        // an fd; even failed attachment cleanup cannot close the main inode.
        if(mode==3)diagnostics.call("create-custody-main-hardlink",[&]{std::filesystem::create_hard_link(file.path,path);});
        EXPECT_NE(diagnostics.call("owner-expire-under-fault",[&]{auto result=adapter->expire_recovery_owned(owner);diagnostics.settlement("owner-expire-under-fault",result);return result;}).state,phase::committed);
        EXPECT_THROW(diagnostics.call("sibling-attach-under-fault",[&]{return canonical_writer_adapter::attach_retention_for_qualification(sibling,p,limits);}),db_error);
        EXPECT_EQ(diagnostics.call("sibling-profile-query-under-fault",[&]{return sibling->db().query("SELECT * FROM _lattice_canonical_retention");}),profile);
        EXPECT_EQ(diagnostics.call("sibling-attempts-query-under-fault",[&]{return sibling->db().query("SELECT * FROM _lattice_canonical_attempt");}),attempts);
    }
    diagnostics.mode("final");
    diagnostics.call("final-adapter-reset",[&]{adapter.reset();});
    diagnostics.call("final-owner-reattach",[&]{attach();});
    EXPECT_EQ(diagnostics.call("final-owner-incarnation-query",[&]{return number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention");}),2);
}
TEST_F(CanonicalRetentionCustody, MissingArtifactOnRestartIsNeverRecreatedOrRebound) {
    attach();reserve();const auto path=custody_path(*owner);const auto saved=path.string()+".saved";
    const auto before=owner->db().query("SELECT * FROM _lattice_canonical_retention");adapter.reset();
    std::filesystem::rename(path,saved);RestoreName restore{saved,path};
    EXPECT_THROW(attach(),db_error);EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(sibling->db().query("SELECT * FROM _lattice_canonical_retention"),before);
    EXPECT_EQ(number(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
    ASSERT_EQ(mkdir(path.c_str(),0700),0);EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(sibling->db().query("SELECT * FROM _lattice_canonical_retention"),before);
    EXPECT_EQ(number(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
}
TEST_F(CanonicalRetentionCustody, MalformedDirectoryModeAndMainHardlinkRefuseBeforeCleanup) {
    retention_fixture_diagnostics::phases diagnostics("CanonicalRetentionCustody.MalformedDirectoryModeAndMainHardlinkRefuseBeforeCleanup");
    diagnostics.call("initial-owner-attach",[&]{attach();});
    diagnostics.call("initial-owner-reserve",[&]{reserve();});
    const auto path=diagnostics.call("initial-custody-path",[&]{return custody_path(*owner);});
    diagnostics.call("initial-adapter-reset",[&]{adapter.reset();});
    const auto before=diagnostics.call("initial-sibling-profile-query",[&]{return sibling->db().query("SELECT * FROM _lattice_canonical_retention");});
    ASSERT_EQ(diagnostics.call("chmod-custody-0755",[&]{return chmod(path.c_str(),0755);}),0);
    EXPECT_THROW(diagnostics.call("owner-attach-under-mode-fault",[&]{attach();}),db_error);
    ASSERT_EQ(diagnostics.call("restore-custody-mode-0700",[&]{return chmod(path.c_str(),0700);}),0);
    const auto alias=file.directory/"main-alias";
    diagnostics.call("create-main-alias",[&]{std::filesystem::create_hard_link(file.path,alias);});
    EXPECT_THROW(diagnostics.call("owner-attach-under-main-alias",[&]{attach();}),db_error);
    diagnostics.call("remove-main-alias",[&]{std::filesystem::remove(alias);});
    EXPECT_EQ(diagnostics.call("sibling-profile-query-after-restore",[&]{return sibling->db().query("SELECT * FROM _lattice_canonical_retention");}),before);
    EXPECT_EQ(diagnostics.call("sibling-attempt-count-after-restore",[&]{return number(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt");}),1);
    diagnostics.call("final-owner-reattach",[&]{attach();});
    EXPECT_EQ(diagnostics.call("final-owner-incarnation-query",[&]{return number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention");}),2);
}
TEST_F(CanonicalRetentionCustody, ParentMovementRefusesBeforeAttemptOrIncarnationMutation) {
    attach();reserve();const auto before=owner->db().query("SELECT * FROM _lattice_canonical_retention");
    const auto moved=file.directory.string()+".moved";std::filesystem::rename(file.directory,moved);RestoreName restore{moved,file.directory};
    std::filesystem::create_directory(file.directory);
    EXPECT_NE(adapter->expire_recovery_owned(owner).state,phase::committed);
    adapter.reset();EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(sibling->db().query("SELECT * FROM _lattice_canonical_retention"),before);
    EXPECT_EQ(number(sibling->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
}
TEST_F(CanonicalRetentionCustody, FailedFirstEnrollmentAdoptsOnlySafeOrphanAndPreservesHistory) {
    owner->add(RetentionCustodyRow{"before failed enrollment"});auto invalid=p;invalid.models={"MissingRetentionModel"};
    EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(owner,invalid,limits),db_error);
    const auto path=custody_path(*owner);ASSERT_TRUE(std::filesystem::is_directory(path));
    EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_retention"));EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_coverage"));
    const auto audits=sibling->db().query("SELECT * FROM AuditLog");ASSERT_FALSE(audits.empty());
    attach();EXPECT_EQ(sibling->db().query("SELECT * FROM AuditLog"),audits);
    EXPECT_EQ(number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),1);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) FROM RetentionCustodyRow"),1);
}
TEST_F(CanonicalRetentionCustody, VersionOneAndChangedDurableBindingCannotRebind) {
    attach();reserve();adapter.reset();
    const auto original=sibling->db().query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_retention_retention_UPDATE'");
    ASSERT_EQ(original.size(),1u);sibling->db().execute("DROP TRIGGER _lattice_canonical_retention_retention_UPDATE");
    sibling->db().execute("UPDATE _lattice_canonical_retention SET version=1");
    sibling->db().execute(std::get<std::string>(original[0].at("sql")));
    const auto attempts=sibling->db().query("SELECT * FROM _lattice_canonical_attempt");
    EXPECT_THROW(attach(),db_error);EXPECT_EQ(sibling->db().query("SELECT * FROM _lattice_canonical_attempt"),attempts);
    sibling->db().execute("DROP TRIGGER _lattice_canonical_retention_retention_UPDATE");
    sibling->db().execute("UPDATE _lattice_canonical_retention SET version=2,custody_inode=custody_inode+1");
    sibling->db().execute(std::get<std::string>(original[0].at("sql")));
    EXPECT_THROW(attach(),db_error);EXPECT_EQ(sibling->db().query("SELECT * FROM _lattice_canonical_attempt"),attempts);
    EXPECT_EQ(number(sibling->db(),"SELECT incarnation FROM _lattice_canonical_retention"),1);
}

TEST(CanonicalRetentionCustodyProcess, CompetingAttachmentAndTeardownKeepSqliteLocks) {
    if(const auto* input=std::getenv("LATTICE_CUSTODY_PEER")) {
        child_deadline();const std::string value(input);ASSERT_GT(value.size(),2u);
        const bool busy=value[0]=='b';ASSERT_TRUE(busy||value[0]=='f');ASSERT_EQ(value[1],':');
        ASSERT_EQ(sqlite3_initialize(),SQLITE_OK);auto* vfs=sqlite3_vfs_find(nullptr);ASSERT_NE(vfs,nullptr);
        ASSERT_TRUE(std::strcmp(vfs->zName,"unix")==0||std::strcmp(vfs->zName,"unix-excl")==0);
        ASSERT_GT(vfs->szOsFile,0);ASSERT_GT(vfs->mxPathname,0);ASSERT_LE(vfs->mxPathname,65536);
        std::vector<char> filename(static_cast<size_t>(vfs->mxPathname)+3,0);
        ASSERT_EQ(vfs->xFullPathname(vfs,value.c_str()+2,vfs->mxPathname+1,filename.data()),SQLITE_OK);
        auto* file=static_cast<sqlite3_file*>(sqlite3_malloc(vfs->szOsFile));ASSERT_NE(file,nullptr);std::memset(file,0,vfs->szOsFile);
        struct close_vfs {sqlite3_file* file;~close_vfs(){if(file->pMethods){EXPECT_EQ(file->pMethods->xUnlock(file,SQLITE_LOCK_NONE),SQLITE_OK);EXPECT_EQ(file->pMethods->xClose(file),SQLITE_OK);}sqlite3_free(file);}} cleanup{file};
        int flags=0;ASSERT_EQ(vfs->xOpen(vfs,filename.data(),file,SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_READWRITE,&flags),SQLITE_OK);
        ASSERT_NE(file->pMethods,nullptr);ASSERT_NE(flags&SQLITE_OPEN_READWRITE,0);
        const auto shared=file->pMethods->xLock(file,SQLITE_LOCK_SHARED);
        const auto exclusive=shared==SQLITE_OK?file->pMethods->xLock(file,SQLITE_LOCK_EXCLUSIVE):shared;
        EXPECT_EQ(exclusive,busy?SQLITE_BUSY:SQLITE_OK);
        // Read/lock probe only: no xWrite, SQL or SQLite journal-mode mutation.
        alarm(0);return;
    }
    OwnedFile file;auto owner=custody_owner(file.str()),sibling=custody_owner(file.str());const auto p=custody_profile();
    auto adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000});
    owner->begin_transaction();owner->add(RetentionCustodyRow{"uncommitted lock holder"});
    const std::string filter="CanonicalRetentionCustodyProcess.CompetingAttachmentAndTeardownKeepSqliteLocks";
    ASSERT_TRUE(child_case(filter,"b:"+file.str(),file.name.str(),"baseline"));
    EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(sibling,p,{4,10000}),db_error);
    ASSERT_TRUE(child_case(filter,"b:"+file.str(),file.name.str(),"rejected-competitor"));
    // Substitute a hardlink to the managed inode at the custody name. A failed
    // O_DIRECTORY open must not acquire/close it even on exception cleanup.
    const auto path=custody_path(*owner);const auto saved=path.string()+".saved";
    {std::filesystem::rename(path,saved);RestoreName restore{saved,path};std::filesystem::create_hard_link(file.path,path);
     EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(sibling,p,{4,10000}),db_error);
     ASSERT_TRUE(child_case(filter,"b:"+file.str(),file.name.str(),"rejected-file-substitution"));}
    adapter.reset();ASSERT_TRUE(owner->db().is_in_transaction());
    ASSERT_TRUE(child_case(filter,"b:"+file.str(),file.name.str(),"session-teardown"));
    owner->rollback();owner->close();sibling->close();owner.reset();sibling.reset();
    ASSERT_TRUE(child_case(filter,"f:"+file.str(),file.name.str(),"all-connections-closed"));
}
TEST(CanonicalRetentionCustodyProcess, FailedEnrollmentOrphanSurvivesFreshProcessRestart) {
    if(const auto* input=std::getenv("LATTICE_CUSTODY_PEER")) {
        child_deadline();const std::string value(input);ASSERT_GT(value.size(),2u);ASSERT_EQ(value[1],':');
        auto owner=custody_owner(value.substr(2));auto p=custody_profile();
        if(value[0]=='s') {
            owner->add(RetentionCustodyRow{"durable before failed first enrollment"});auto invalid=p;invalid.models={"MissingRetentionModel"};
            EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(owner,invalid,{4,10000}),db_error);
            EXPECT_TRUE(std::filesystem::is_directory(custody_path(*owner)));
            EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_retention"));
            // Abrupt process exit leaves the real failed enrollment's artifact.
            std::_Exit(::testing::Test::HasFailure()?1:0);
        }
        ASSERT_EQ(value[0],'r');const auto audits=owner->db().query("SELECT * FROM AuditLog");ASSERT_FALSE(audits.empty());
        auto adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000});
        EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audits);
        EXPECT_EQ(number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),1);
        EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) FROM RetentionCustodyRow"),1);alarm(0);return;
    }
    OwnedFile file;const std::string filter="CanonicalRetentionCustodyProcess.FailedEnrollmentOrphanSurvivesFreshProcessRestart";
    ASSERT_TRUE(child_case(filter,"s:"+file.str(),file.name.str(),"failed-enrollment"));
    ASSERT_TRUE(child_case(filter,"r:"+file.str(),file.name.str(),"adopt-orphan"));
}
TEST(CanonicalRetentionCustodyProcess, BoundDirectoryExcludesFreshProcessUntilSessionEnds) {
    const std::string filter="CanonicalRetentionCustodyProcess.BoundDirectoryExcludesFreshProcessUntilSessionEnds";
    const auto* input=std::getenv("LATTICE_CUSTODY_PEER");
    if(!input) {
        // The holder and both competitors have fresh matching schema catalogs,
        // including when the outer suite has mutated its process registry.
        OwnedFile file;ASSERT_TRUE(child_case(filter,"h:"+file.str(),file.name.str(),"fresh-holder"));return;
    }
    const std::string value(input);ASSERT_GT(value.size(),2u);ASSERT_EQ(value[1],':');
    child_deadline(value[0]=='h'?30:5);
    auto owner=custody_owner(value.substr(2));auto p=custody_profile();
    if(value[0]=='b') {
        const auto profile=owner->db().query("SELECT * FROM _lattice_canonical_retention");
        const auto attempts=owner->db().query("SELECT * FROM _lattice_canonical_attempt");ASSERT_EQ(attempts.size(),1u);
        EXPECT_THROW(canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000}),db_error);
        EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_retention"),profile);
        EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_attempt"),attempts);
    } else if(value[0]=='f') {
        auto adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000});
        EXPECT_EQ(number(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),2);
        EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
    } else {
        ASSERT_EQ(value[0],'h');auto adapter=canonical_writer_adapter::attach_retention_for_qualification(owner,p,{4,10000});
        const auto reserved=adapter->reserve_recovery_owned(owner,{},10000);require_commit(reserved.settlement);ASSERT_TRUE(reserved.reservation);
        const auto prefix=std::filesystem::path(value.substr(2)).parent_path().string()+".holder";
        ASSERT_TRUE(child_case(filter,"b:"+value.substr(2),prefix,"competing-process",8));
        EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
        adapter.reset();owner->close();owner.reset();
        ASSERT_TRUE(child_case(filter,"f:"+value.substr(2),prefix,"released-process",8));
    }
    alarm(0);
}

#endif
