#include "TestHelpers.hpp"
#include <lattice/owned_server_export.hpp>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <cerrno>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
namespace {
lattice::configuration restart_config(const std::string& path){lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
void checked_close(lattice::lattice_db& owner){const auto closed=owner.close_checked();EXPECT_FALSE(static_cast<bool>(closed.error));EXPECT_FALSE(static_cast<bool>(closed.cleanup_error));EXPECT_TRUE(closed.cleanup_complete);}
struct restart_files {
    TempDB server{"owned_restart_source"},receiver{"owned_restart_receiver"};
    std::string native_log;
    restart_files() {
        const char* parent=std::getenv("LATTICE_TEST_LOG_PATH");
        native_log=(parent&&*parent?std::string(parent):server.str())+'.'+server.path.filename().string()+".owned-restart.native.log";
    }
};
struct quarantine_slot {std::mutex mutex;bool reserved=false;pid_t pid=-1;std::unique_ptr<restart_files> files;};
// One charged process-lived failure slot. Unproved child cleanup retains its
// files, numeric observation and reservation; it never silently reuses them.
quarantine_slot& quarantine(){static auto* value=new quarantine_slot;return *value;}
struct restart_child {
    std::unique_ptr<restart_files> files;
    pid_t pid=-1;bool settled=false;int status=-1;
    restart_child() {
        auto& slot=quarantine();{std::lock_guard lock(slot.mutex);if(slot.reserved)throw std::runtime_error("owned restart child capacity unavailable");slot.reserved=true;}
        try{files=std::make_unique<restart_files>();}catch(...){std::lock_guard lock(slot.mutex);slot.reserved=false;throw;}
    }
    restart_child(const restart_child&)=delete;
    bool observe_and_reap(std::chrono::steady_clock::time_point deadline) {
        while(std::chrono::steady_clock::now()<deadline) {
            siginfo_t info{};const int result=::waitid(P_PID,static_cast<id_t>(pid),&info,WEXITED|WNOHANG|WNOWAIT);
            if(result<0){if(errno==EINTR)continue;return false;}
            if(info.si_pid==pid) {
                int state=0;const auto joined=::waitpid(pid,&state,WNOHANG);
                if(joined<0&&errno==EINTR)continue;
                if(joined!=pid)return false;
                const auto group=pid;pid=-1;status=state;
                while(std::chrono::steady_clock::now()<deadline) {
                    if(::kill(-group,0)<0&&errno==ESRCH){settled=true;return true;}
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
                return false; // No signaling after leader custody was reaped.
            }
            if(info.si_pid!=0)return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return false;
    }
    ~restart_child() {
        if(pid>0&&!settled) {
            const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(10);
            siginfo_t info{};int observed=-1;
            do {if(std::chrono::steady_clock::now()>=deadline)break;observed=::waitid(P_PID,static_cast<id_t>(pid),&info,WEXITED|WNOHANG|WNOWAIT);}while(observed<0&&errno==EINTR);
            if(observed==0&&(info.si_pid==0||info.si_pid==pid)) {
                // Its unreaped group leader is still our actual child.
                if(::kill(-pid,SIGKILL)<0&&errno!=ESRCH)ADD_FAILURE()<<"owned restart child cleanup signal failed";
                (void)observe_and_reap(deadline);
            }
        }
        auto& slot=quarantine();
        if((pid>0||status!=-1)&&!settled) {
            ADD_FAILURE()<<"owned restart process/group completion unproved; custody retained";
            std::lock_guard lock(slot.mutex);slot.pid=pid;slot.files=std::move(files);
        } else {
            // File destruction stays outside the fixed custody leaf.
            files.reset();std::lock_guard lock(slot.mutex);slot.reserved=false;
        }
    }
    void start(const std::string& expected_source) {
        const auto args=::testing::internal::GetArgvs();if(args.empty())throw std::runtime_error("owned restart executable unavailable");
        std::string executable=std::filesystem::absolute(args.front()).string();
        std::string filter="--gtest_filter=OrdinaryOwnedRestart.ProcessHelper",repeat="--gtest_repeat=1",color="--gtest_color=no",output="--gtest_output=";
        char* argv[]{executable.data(),filter.data(),repeat.data(),color.data(),output.data(),nullptr};
        std::vector<std::string> environment;
        for(char** item=environ;*item;++item){const std::string value(*item);if(!value.starts_with("LATTICE_OWNED_RESTART_")&&!value.starts_with("LATTICE_TEST_LOG_PATH=")&&!value.starts_with("GTEST_"))environment.push_back(value);}
        environment.push_back("LATTICE_OWNED_RESTART_SERVER="+files->server.str());environment.push_back("LATTICE_OWNED_RESTART_RECEIVER="+files->receiver.str());
        environment.push_back("LATTICE_OWNED_RESTART_SOURCE="+expected_source);environment.push_back("LATTICE_TEST_LOG_PATH="+files->native_log);
        const int log=::open(files->native_log.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
        if(log<0)throw std::runtime_error("owned restart native log collision");if(::close(log)<0)throw std::runtime_error("owned restart native log close failed");
        std::vector<char*> env;for(auto& value:environment)env.push_back(value.data());env.push_back(nullptr);
        posix_spawn_file_actions_t actions;if(::posix_spawn_file_actions_init(&actions))throw std::runtime_error("owned restart actions unavailable");
        struct action_owner {posix_spawn_file_actions_t& value;~action_owner(){::posix_spawn_file_actions_destroy(&value);}} own_actions{actions};
        const auto terminal=files->native_log+".terminal.log";
        if(::posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,terminal.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)||::posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO))throw std::runtime_error("owned restart log actions unavailable");
        posix_spawnattr_t attrs;if(::posix_spawnattr_init(&attrs))throw std::runtime_error("owned restart attributes unavailable");
        struct attribute_owner {posix_spawnattr_t& value;~attribute_owner(){::posix_spawnattr_destroy(&value);}} own_attributes{attrs};
        if(::posix_spawnattr_setpgroup(&attrs,0)||::posix_spawnattr_setflags(&attrs,POSIX_SPAWN_SETPGROUP))throw std::runtime_error("owned restart child group unavailable");
        const int rc=::posix_spawn(&pid,executable.c_str(),&actions,&attrs,argv,env.data());
        if(rc){pid=-1;throw std::runtime_error("owned restart spawn failed");}
    }
};
}

TEST(OrdinaryOwnedRestart, ActualNewProcessContinuesSourceAndReceiverThenRejectsStaleMaterialization) {
    restart_child child;
    std::optional<lattice::owned_server_export> first;
    {
        lattice::lattice_db source{restart_config(child.files->server.str())},receiver{restart_config(child.files->receiver.str())};
        source.add(TestPerson{"parent-cut",1,std::nullopt});first=lattice::capture_owned_server_export(source,{"TestPerson"});
        ASSERT_EQ(lattice::apply_owned_server_export(receiver,*first).size(),first->entries().size());checked_close(receiver);checked_close(source);
    }
    child.start(first->source_id());ASSERT_GT(child.pid,0);
    ASSERT_TRUE(child.observe_and_reap(std::chrono::steady_clock::now()+std::chrono::seconds(30)))<<child.files->native_log;
    ASSERT_TRUE(WIFEXITED(child.status));ASSERT_EQ(WEXITSTATUS(child.status),0)<<child.files->native_log;
    lattice::lattice_db source{restart_config(child.files->server.str())},receiver{restart_config(child.files->receiver.str())};
    EXPECT_EQ(std::get<int64_t>(source.db().query("SELECT cut FROM _lattice_ordered_source_v1").at(0).at("cut")),2);
    EXPECT_EQ(std::get<int64_t>(receiver.db().query("SELECT cut FROM _lattice_ordered_receiver_v1").at(0).at("cut")),2);
    EXPECT_EQ(lattice::apply_owned_server_export(receiver,*first).size(),first->entries().size());
    EXPECT_EQ(std::get<std::string>(receiver.db().query("SELECT name FROM TestPerson").at(0).at("name")),"child-cut");
    const auto third=lattice::capture_owned_server_export(source,{"TestPerson"});EXPECT_EQ(third.source_id(),first->source_id());EXPECT_EQ(third.cut(),3);
    checked_close(receiver);checked_close(source);
}

TEST(OrdinaryOwnedRestart, ProcessHelper) {
    const char* server=std::getenv("LATTICE_OWNED_RESTART_SERVER"),*receiver_path=std::getenv("LATTICE_OWNED_RESTART_RECEIVER"),*expected=std::getenv("LATTICE_OWNED_RESTART_SOURCE");
    if(!server||!receiver_path||!expected)GTEST_SKIP()<<"Dedicated actual process-restart child; parent receipt is separate";
    lattice::lattice_db source{restart_config(server)},receiver{restart_config(receiver_path)};
    source.db().execute("UPDATE TestPerson SET name='child-cut'");const auto second=lattice::capture_owned_server_export(source,{"TestPerson"});
    ASSERT_EQ(second.source_id(),expected);ASSERT_EQ(second.cut(),2);ASSERT_EQ(lattice::apply_owned_server_export(receiver,second).size(),second.entries().size());
    ASSERT_EQ(std::get<std::string>(receiver.db().query("SELECT name FROM TestPerson").at(0).at("name")),"child-cut");
    checked_close(receiver);checked_close(source);
}
#endif
