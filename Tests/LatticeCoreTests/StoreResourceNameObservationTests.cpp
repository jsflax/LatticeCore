#include <gtest/gtest.h>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <LatticeCore.hpp>
#include <lattice/store_resource_context.hpp>
#include "../../Sources/LatticeCore/src/ordinary_resource_applications_policy.hpp"
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
namespace {
struct ObservationDirectory {
    std::filesystem::path directory,main;
    ObservationDirectory(){
        const char* tmp=std::getenv("TEST_TMPDIR");if(!tmp)tmp=std::getenv("TMPDIR");
        if(!tmp||!std::filesystem::path(tmp).is_absolute())throw std::runtime_error("owned explicit tmp required");
        auto name=(std::filesystem::path(tmp)/"main-name-observation-XXXXXX").string();
        if(!::mkdtemp(name.data()))throw std::runtime_error("owned observation fixture creation failed");
        directory=name;main=directory/"store.sqlite";
    }
    ~ObservationDirectory(){std::error_code error;std::filesystem::remove_all(directory,error);EXPECT_FALSE(error);}
};
struct Descriptor {int value=-1;~Descriptor(){if(value>=0)::close(value);}};
void child_alarm()noexcept{
    struct sigaction action{};action.sa_handler=SIG_DFL;sigemptyset(&action.sa_mask);
    if(::sigaction(SIGALRM,&action,nullptr))::_exit(91);
    sigset_t unblocked;sigemptyset(&unblocked);sigaddset(&unblocked,SIGALRM);
    if(::sigprocmask(SIG_UNBLOCK,&unblocked,nullptr))::_exit(92);::alarm(5);
}
struct OwnedChild {
    const pid_t pid;bool reaped=false;int status=0;
    bool wait(std::chrono::steady_clock::time_point end)noexcept{
        while(!reaped&&std::chrono::steady_clock::now()<end){
            const auto actual=::waitpid(pid,&status,WNOHANG);
            if(actual==pid){reaped=true;return true;}
            if(actual<0&&errno!=EINTR)return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }return reaped;
    }
    ~OwnedChild()noexcept{
        if(reaped)return;
        // This exact unreaped direct child is still the owned original; no
        // group census or remembered/reused pid is promoted to authority.
        if(::kill(pid,SIGKILL)&&errno!=ESRCH){std::fputs("name-observation-ending-debt:signal-unproved\n",stderr);::_exit(95);}
        if(!wait(std::chrono::steady_clock::now()+std::chrono::seconds(5))){std::fputs("name-observation-ending-debt:reap-unproved\n",stderr);::_exit(95);}
    }
};
TEST(StoreResourceNameObservation, DirectoryDuplicatePreservesActualMainPosixLock){
    ObservationDirectory fixture;
    // A fixture control descriptor is opened BEFORE any SQLite owner exists.
    // The production observation must never open or close another MAIN fd.
    Descriptor control{::open(fixture.main.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600)};
    ASSERT_GE(control.value,0);
    {
        lattice::lattice_db owner(fixture.main.string());
        struct flock held{};held.l_type=F_WRLCK;held.l_whence=SEEK_SET;held.l_start=0;held.l_len=1;
        ASSERT_EQ(::fcntl(control.value,F_SETLK,&held),0);
        auto observation=owner.capture_store_name_observation();ASSERT_TRUE(observation.current());
        Descriptor directory{observation.duplicate_directory_for_events()};ASSERT_GE(directory.value,0);
        struct stat actual{};ASSERT_EQ(::fstat(directory.value,&actual),0);ASSERT_TRUE(S_ISDIR(actual.st_mode));
        const int directory_flags=::fcntl(directory.value,F_GETFD);
    ASSERT_GE(directory_flags,0);ASSERT_NE(directory_flags&FD_CLOEXEC,0);
        ASSERT_EQ(::close(std::exchange(directory.value,-1)),0);
        const auto child=::fork();
        if(child==0){
            child_alarm();const int probe=::open(fixture.main.c_str(),O_RDWR|O_NOFOLLOW|O_CLOEXEC);
            if(probe<0)::_exit(20);
            struct flock test{};test.l_type=F_WRLCK;test.l_whence=SEEK_SET;test.l_start=0;test.l_len=1;
            errno=0;const int rc=::fcntl(probe,F_SETLK,&test);const auto error=errno;
            const int closed=::close(probe);
            ::_exit(rc<0&&(error==EACCES||error==EAGAIN)&&closed==0?0:21);
        }
        if(child<0){ADD_FAILURE()<<"owned child creation failed";return;}
        OwnedChild ending{child};ASSERT_TRUE(ending.wait(std::chrono::steady_clock::now()+std::chrono::seconds(5)));
        ASSERT_TRUE(WIFEXITED(ending.status));EXPECT_EQ(WEXITSTATUS(ending.status),0);
        EXPECT_TRUE(observation.current());
    } // Actual SQLite owners end before the fixture control MAIN descriptor.
}
TEST(StoreResourceNameObservation, OriginalNameCannotRefreshToReplacement){
    ObservationDirectory fixture;lattice::detail::store_resource_name_observation observation;
    {
        lattice::lattice_db owner(fixture.main.string());observation=owner.capture_store_name_observation();
        ASSERT_TRUE(observation.current());
    } // No live SQLite owner exists during the following namespace mutation.
    const auto retired=fixture.directory/"retired.sqlite";
    ASSERT_EQ(::rename(fixture.main.c_str(),retired.c_str()),0);EXPECT_FALSE(observation.current());
    Descriptor replacement{::open(fixture.main.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600)};
    ASSERT_GE(replacement.value,0);EXPECT_FALSE(observation.current());
    EXPECT_EQ(observation.duplicate_directory_for_events(),-1);
}
TEST(StoreResourceNameObservation, ForeignCopyCannotTouchOrReleaseParentCustody){
    ObservationDirectory fixture;lattice::detail::store_resource_name_observation observation;
    {lattice::lattice_db owner(fixture.main.string());observation=owner.capture_store_name_observation();}
    ASSERT_TRUE(observation.current());
    const auto child=::fork();
    if(child==0){
        child_alarm();const bool current=observation.current();const int duplicate=observation.duplicate_directory_for_events();
        observation={};::_exit(!current&&duplicate<0?0:22);
    }
    if(child<0){ADD_FAILURE()<<"owned child creation failed";return;}
    OwnedChild ending{child};ASSERT_TRUE(ending.wait(std::chrono::steady_clock::now()+std::chrono::seconds(5)));
    ASSERT_TRUE(WIFEXITED(ending.status));EXPECT_EQ(WEXITSTATUS(ending.status),0);EXPECT_TRUE(observation.current());
}
TEST(StoreResourceApplicationsMetadata, PreservesExactRootAdminExceptionWithoutGeneralWritableParents){
    using namespace lattice::detail::ordinary_installation::resource_applications_policy;
    struct stat value{};value.st_mode=S_IFDIR|0775;value.st_uid=0;value.st_gid=80;value.st_ino=1;value.st_dev=2;
    EXPECT_FALSE(strict_directory(value,501));EXPECT_TRUE(directory(value,501,80));
    EXPECT_FALSE(directory(value,501,81));EXPECT_FALSE(directory(value,501,std::nullopt));
    auto wrong=value;wrong.st_mode=S_IFDIR|0777;EXPECT_FALSE(directory(wrong,501,80));
    wrong=value;wrong.st_uid=501;EXPECT_FALSE(directory(wrong,501,80));
    wrong=value;wrong.st_gid=81;EXPECT_FALSE(unchanged(value,wrong));
    value.st_mode=S_IFDIR|0755;EXPECT_TRUE(strict_directory(value,501));
}
}
#endif
