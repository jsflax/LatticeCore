#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_owned_launch.hpp"
#include <pwd.h>
#include <fstream>
#include <sstream>
#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <exception>
#include <future>
#include <filesystem>
#include <cstdlib>
#include <thread>
#include <utility>

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <libproc.h>
#endif
extern char** environ;

namespace {
namespace launch = lattice::detail::ordinary_launch;
using namespace std::chrono_literals;
struct fd_owner {
    int value = -1;
    explicit fd_owner(int fd) : value(fd) {}
    ~fd_owner() { if (value >= 0) ::close(value); }
    fd_owner(const fd_owner&) = delete;
    fd_owner& operator=(const fd_owner&) = delete;
};
launch::deadline within(std::chrono::milliseconds amount = 5000ms) {
    return std::chrono::steady_clock::now() + amount;
}
// Only fixture-owned children are observed here. No blocking waitpid, guessed
// process group, or signaling after custody has been released is permitted.
struct fixture_child {
    pid_t pid = -1;
    bool group = false;
    explicit fixture_child(pid_t value, bool owns_group = false) : pid(value), group(owns_group) {}
    fixture_child(const fixture_child&) = delete;
    bool terminal(launch::deadline end) {
        while (std::chrono::steady_clock::now() < end) {
            siginfo_t info{};
            if (::waitid(P_PID, static_cast<id_t>(pid), &info, WEXITED | WNOHANG | WNOWAIT) == 0) {
                if (info.si_pid == pid) return true;
            } else if (errno != EINTR) {
                pid = -1; // Lost custody: never signal this reusable numeric identity.
                return false;
            }
            std::this_thread::sleep_for(2ms);
        }
        return false;
    }
    int join(launch::deadline end) {
        if (pid <= 0 || !terminal(end)) return -1;
        while (std::chrono::steady_clock::now() < end) {
            int status = 0;
            const auto result = ::waitpid(pid, &status, WNOHANG);
            if (result == pid) { pid = -1; return status; }
            if (result < 0 && errno != EINTR) { pid = -1; return -1; }
            std::this_thread::sleep_for(2ms);
        }
        return -1;
    }
    ~fixture_child() {
        if (pid <= 0) return;
        const auto end = within();
        const auto owned = pid;
        // WNOWAIT validates our still-owned child before group signaling and
        // preserves the leader reservation even if it has already exited.
        siginfo_t info{};
        int result;
        do { result = ::waitid(P_PID, static_cast<id_t>(owned), &info, WEXITED | WNOHANG | WNOWAIT); }
        while (result < 0 && errno == EINTR && std::chrono::steady_clock::now() < end);
        if (result != 0) {
            ADD_FAILURE() << "owned-launch fixture cleanup lost child custody";
            return;
        }
        (void)::kill(group ? -owned : owned, SIGKILL);
        if (join(end) < 0) {
            ADD_FAILURE() << "owned-launch fixture cleanup deadline: child completion unproved";
            return;
        }
        if (group) {
            // Observation only after reaping. Never signal a reusable PGID.
            while (std::chrono::steady_clock::now() < end) {
                if (::kill(-owned, 0) < 0 && errno == ESRCH) return;
                std::this_thread::sleep_for(2ms);
            }
            ADD_FAILURE() << "owned-launch fixture cleanup deadline: descendant completion unproved";
        }
    }
};
struct helper_area {
    std::filesystem::path path;
    helper_area() {
        std::array<char,65536> buffer{};passwd account{},*found=nullptr;
        if(::getpwuid_r(::geteuid(),&account,buffer.data(),buffer.size(),&found)||found!=&account||
           account.pw_uid!=::geteuid()||!account.pw_dir)throw std::runtime_error("real fixture account unavailable");
        const auto base=std::filesystem::path(account.pw_dir)/"localdev"/"reclamation-evidence";
        std::filesystem::create_directories(base);
        auto pattern = (base / "lattice-reclamation-XXXXXX").string();
        std::vector<char> bytes(pattern.begin(), pattern.end()); bytes.push_back(0);
        if (!::mkdtemp(bytes.data())) throw std::runtime_error("owned-launch helper directory unavailable");
        path = bytes.data();
    }
    ~helper_area() = default; // Preserve first failures and successful receipts; VM retirement owns cleanup.
};
bool is_single_threaded_helper() {
#if defined(__APPLE__)
    proc_taskinfo info{};
    return ::proc_pidinfo(::getpid(), PROC_PIDTASKINFO, 0, &info, sizeof(info)) == sizeof(info) &&
        info.pti_threadnum == 1;
#else
    std::size_t count = 0;
    for (const auto& item : std::filesystem::directory_iterator("/proc/self/task")) {
        (void)item;
        if (++count > 1) return false;
    }
    return count == 1;
#endif
}
pid_t spawn_fork_helper(const helper_area& area) {
    const auto args = ::testing::internal::GetArgvs();
    if (args.empty()) throw std::runtime_error("owned-launch helper executable unavailable");
    std::string executable = std::filesystem::absolute(args.front()).string();
    std::string filter = "--gtest_filter=ManagedTargetReclamation.ConstructorProcessHelper";
    std::string repeat = "--gtest_repeat=1", color = "--gtest_color=no", output = "--gtest_output=";
    char* argv[] = {executable.data(), filter.data(), repeat.data(), color.data(), output.data(), nullptr};
    std::vector<std::string> values;
    for (char** item = environ; *item; ++item) {
        const std::string value(*item);
        if (!value.starts_with("LATTICE_RECLAMATION_HELPER_") &&
            !value.starts_with("LATTICE_TEST_LOG_PATH=") &&
            !value.starts_with("GTEST_TOTAL_SHARDS=") && !value.starts_with("GTEST_SHARD_INDEX=") &&
            !value.starts_with("GTEST_SHARD_STATUS_FILE=")) values.push_back(value);
    }
    values.push_back("LATTICE_RECLAMATION_HELPER_DIRECTORY=" + area.path.string());
    const auto* parent_log = std::getenv("LATTICE_TEST_LOG_PATH");
    const auto native = parent_log && *parent_log ?
        std::string(parent_log) + "." + area.path.filename().string() + ".native.log" :
        area.path.string() + ".native.log";
    // Preserve both logs outside the disposable marker directory.
    fd_owner reserved(::open(native.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
    if (reserved.value < 0) throw std::runtime_error("owned-launch helper log collision");
    values.push_back("LATTICE_TEST_LOG_PATH=" + native);
    std::vector<char*> env; for (auto& value : values) env.push_back(value.data()); env.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (::posix_spawn_file_actions_init(&actions)) throw std::runtime_error("owned-launch helper actions unavailable");
    struct action_owner { posix_spawn_file_actions_t& value; ~action_owner() { ::posix_spawn_file_actions_destroy(&value); } } own_actions{actions};
    const auto terminal_log = native + ".terminal.log";
    if (::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, terminal_log.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600) ||
        ::posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO))
        throw std::runtime_error("owned-launch helper log actions unavailable");
    posix_spawnattr_t attributes;
    if (::posix_spawnattr_init(&attributes)) throw std::runtime_error("owned-launch helper attributes unavailable");
    struct attribute_owner { posix_spawnattr_t& value; ~attribute_owner() { ::posix_spawnattr_destroy(&value); } } own_attributes{attributes};
    short flags = POSIX_SPAWN_SETPGROUP;
#if defined(__APPLE__)
    flags |= POSIX_SPAWN_CLOEXEC_DEFAULT;
#elif defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 34)
    if (::posix_spawn_file_actions_addclosefrom_np(&actions, 3))
        throw std::runtime_error("owned-launch helper descriptor closure unavailable");
#else
    throw std::runtime_error("owned-launch helper requires closefrom spawn support");
#endif
#else
    throw std::runtime_error("owned-launch helper requires closefrom spawn support");
#endif
    if (::posix_spawnattr_setpgroup(&attributes, 0) || ::posix_spawnattr_setflags(&attributes, flags))
        throw std::runtime_error("owned-launch helper group unavailable");
    pid_t child = -1;
    if (::posix_spawn(&child, executable.c_str(), &actions, &attributes, argv, env.data()))
        throw std::runtime_error("owned-launch helper spawn failed");
    return child;
}

}

TEST(ManagedTargetReclamation, GenuineInstalledConstructorReceipt) {
    helper_area area;
    const auto end=within(30000ms);
    fixture_child helper(spawn_fork_helper(area),true);
    const auto group=helper.pid;
    const auto status=helper.join(end);
    ASSERT_GE(status,0);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_TRUE(std::filesystem::exists(area.path/"constructor-and-controller-joined.signal"));
    // Observe only after reaping. No signaling a potentially reusable PGID.
    EXPECT_TRUE(::kill(-group,0)<0 && errno==ESRCH);
}
TEST(ManagedTargetReclamation, ConstructorProcessHelper) {
    const auto* path=std::getenv("LATTICE_RECLAMATION_HELPER_DIRECTORY");
    if(!path)GTEST_SKIP()<<"Requires the real parent-owned process fixture";
    ASSERT_EQ(::getpgrp(),::getpid());
    ASSERT_TRUE(is_single_threaded_helper());
    const auto end=within(30000ms);
    std::array<char,65536> buffer{};passwd account{},*found=nullptr;
    ASSERT_EQ(::getpwuid_r(::geteuid(),&account,buffer.data(),buffer.size(),&found),0);
    ASSERT_EQ(found,&account);ASSERT_EQ(account.pw_uid,::geteuid());ASSERT_NE(account.pw_dir,nullptr);
    const auto home=std::filesystem::path(account.pw_dir);
    const auto controller=(home/".engram-installation/upgrade-bootstrap.v1/controller/memory-installation-launcher").string();
    const auto log=(std::filesystem::path(path)/"controller.stdout").string();
    fd_owner output(::open(log.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600));ASSERT_GE(output.value,0);
    fd_owner input(::open("/dev/null",O_RDONLY|O_CLOEXEC));ASSERT_GE(input.value,0);
    launch::launch_specification specification;
    specification.executable=controller;specification.working_directory=(home/"localdev").string();
    specification.arguments={"begin-staged-engram-cohort-recovery"};
    specification.environment={"PATH=/usr/bin:/bin"};specification.input=input.value;
    specification.output=output.value;
    auto child=launch::owned_child::spawn(specification);
    const auto identity=child.identity();ASSERT_EQ(identity.parent,::getpid());
    std::optional<launch::terminal_observation> terminal;
    while(!terminal&&std::chrono::steady_clock::now()<end){terminal=child.observe_terminal(end);
        if(!terminal)std::this_thread::sleep_for(2ms);}
    ASSERT_TRUE(terminal.has_value());ASSERT_EQ(terminal->child,identity);
    ASSERT_TRUE(WIFEXITED(terminal->wait_status));ASSERT_EQ(WEXITSTATUS(terminal->wait_status),0);
    const auto joined=child.stop_and_join(end);ASSERT_EQ(joined.child,identity);ASSERT_EQ(joined.wait_status,terminal->wait_status);
    struct stat output_stat{};ASSERT_EQ(::fstat(output.value,&output_stat),0);
    ASSERT_GE(output_stat.st_size,0);ASSERT_LE(output_stat.st_size,launch::maximum_frame_bytes+64);
    std::ifstream stream(log);std::string receipt(static_cast<std::size_t>(output_stat.st_size),'\0');
    stream.read(receipt.data(),receipt.size());ASSERT_EQ(stream.gcount(),receipt.size());
    ASSERT_EQ(receipt.find("constructor-reclamation-qualified "),0u);
    ASSERT_NE(receipt.find("\"linksAfter\":0"),std::string::npos);
    // Marker follows actual child terminal/reap and closed controller receipt,
    // which itself follows both real stamped runtime exits/target census.
    fd_owner marker(::open((std::filesystem::path(path)/"constructor-and-controller-joined.signal").c_str(),
        O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600));ASSERT_GE(marker.value,0);
}
#endif
