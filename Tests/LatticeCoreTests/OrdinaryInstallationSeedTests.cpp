#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_installation_seed.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <cstdlib>
#include <thread>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <libproc.h>
#endif
extern char** environ;
namespace {
namespace seed = lattice::detail::ordinary_installation;
namespace launch = lattice::detail::ordinary_launch;
using namespace std::chrono_literals;
struct seed_area {
    std::filesystem::path path; int fd = -1;
    seed_area() {
        auto name = (std::filesystem::temp_directory_path() / "lattice-seed-XXXXXX").string();
        std::vector<char> bytes(name.begin(), name.end()); bytes.push_back(0);
        if (!::mkdtemp(bytes.data())) throw std::runtime_error("seed test directory unavailable");
        path = bytes.data(); fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("seed test descriptor unavailable");
    }
    ~seed_area() { if (fd >= 0) ::close(fd); std::error_code ignored; std::filesystem::remove_all(path, ignored); }
};
auto seed_deadline() { return std::chrono::steady_clock::now() + 5s; }
seed::executable_fact actual_seed_child() {
    const auto arguments = ::testing::internal::GetArgvs();
    if (arguments.empty()) throw std::runtime_error("test executable unavailable");
    const auto executable = std::filesystem::canonical(std::filesystem::absolute(arguments.front()).parent_path() / "LatticeInstallationSeedChild");
    struct stat value{};
    if (::stat(executable.c_str(), &value) || value.st_size <= 0 || value.st_size > 1024ll*1024*1024)
        throw std::runtime_error("seed helper identity unavailable");
    std::ifstream input(executable, std::ios::binary);
    picosha2::hash256_one_by_one hasher; std::array<char, 64 * 1024> bytes{}; std::size_t count = 0;
    while (input) {
        input.read(bytes.data(), bytes.size()); const auto size = input.gcount();
        hasher.process(bytes.begin(), bytes.begin() + size); count += static_cast<std::size_t>(size);
    }
    if (!input.eof() || count != static_cast<std::size_t>(value.st_size)) throw std::runtime_error("seed helper bytes unavailable");
    hasher.finish(); seed::digest hash{}; hasher.get_hash_bytes(hash.begin(), hash.end());
    return {executable.string(), {static_cast<std::uint64_t>(value.st_dev), static_cast<std::uint64_t>(value.st_ino)}, hash};
}
void actual_seed_refuses(const char* mode) {
    seed_area area; const auto cohorts = seed::created_launch_cohort::unresolved_cohorts();
    const auto children = launch::owned_child::unresolved_lifetimes();
    enum class refusal { none, wrong_offer, channel_io, physical_file, terminal, expired, unexpected };
    auto actual = refusal::none;
    try { (void)seed::seeded_installation_store::create_engram(area.fd, mode, actual_seed_child(), seed_deadline()); }
    catch (const seed::seed_error& error) {
        if (error.code == seed::seed_error_code::invalid_offer) actual = refusal::wrong_offer;
        else if (error.code == seed::seed_error_code::terminal_unproved) actual = refusal::terminal;
        else if (error.code == seed::seed_error_code::expired) actual = refusal::expired;
        else actual = refusal::unexpected;
    } catch (const launch::error& error) {
        if (error.code == launch::error_code::io) actual = refusal::channel_io;
        else if (error.code == launch::error_code::timed_out) actual = refusal::expired;
        else actual = refusal::unexpected;
    } catch (const seed::cohort_error& error) {
        actual = error.code == seed::cohort_error_code::changed ? refusal::physical_file : refusal::unexpected;
    } catch (...) { actual = refusal::unexpected; }
    if (std::string(mode) == "bad-reply") EXPECT_EQ(actual, refusal::wrong_offer);
    else if (std::string(mode) == "callback-failure") EXPECT_EQ(actual, refusal::channel_io);
    else if (std::string(mode) == "missing-file") EXPECT_EQ(actual, refusal::physical_file);
    else if (std::string(mode) == "reply-nonzero-exit") EXPECT_EQ(actual, refusal::terminal);
    else if (std::string(mode) == "reply-still-running") EXPECT_EQ(actual, refusal::expired);
    else FAIL() << "Unknown seed fixture case";
    const bool wrote_file = std::string(mode) == "reply-nonzero-exit" || std::string(mode) == "reply-still-running";
    EXPECT_EQ(std::filesystem::is_regular_file(area.path / mode / "data/memory.sqlite"), wrote_file);
    EXPECT_EQ(seed::created_launch_cohort::unresolved_cohorts(), cohorts);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), children);
    EXPECT_TRUE(std::filesystem::is_regular_file(area.path / mode / "launch.v1"));
    // The failed physical namespace is not reused as a new installation.
    EXPECT_THROW((void)seed::seeded_installation_store::create_engram(area.fd, mode, actual_seed_child(), seed_deadline()), seed::cohort_error);
}
void actual_receiver_refuses(bool wrong_birth) {
    seed_area area;
    const auto children = launch::owned_child::unresolved_lifetimes();
    auto owner = seed::created_launch_cohort::create_before_child(area.fd, "receiver-refusal");
    auto space = owner.create_store_namespace("data"); const auto directory = space.directory_identity();
    const auto inherited = space.duplicate_for_child(); ASSERT_GE(inherited, 3);
    launch::launch_specification spec;
    spec.executable = actual_seed_child().path; spec.working_directory = (area.path / "receiver-refusal/data").string();
    spec.arguments = {"--lattice-seed-store-v1"}; spec.environment = {"PATH=/usr/bin:/bin"}; spec.inherited_directories = {inherited};
    std::size_t index;
    try { index = owner.launch(spec); } catch (...) { ::close(inherited); throw; }
    ASSERT_EQ(::close(inherited), 0);
    launch::frame offer{1, 2, 3};
    if (wrong_birth) {
        offer = {'L','A','T','S','E','E','1',0};
        auto append = [&](std::uint64_t value) { for (unsigned shift = 0; shift < 64; shift += 8) offer.push_back(static_cast<std::uint8_t>(value >> shift)); };
        append(1); offer.insert(offer.end(), 16, 1);
        auto process = [&](const launch::process_identity& value) {
            append(static_cast<std::uint64_t>(value.pid)); append(static_cast<std::uint64_t>(value.parent)); append(value.birth_major); append(value.birth_minor);
        };
        process(launch::current_process()); auto child = owner.child_identity(index);
        child.birth_major = child.birth_major == std::numeric_limits<std::uint64_t>::max() ? child.birth_major - 1 : child.birth_major + 1;
        process(child); append(directory.device); append(directory.inode); append(static_cast<std::uint64_t>(seed::product::engram));
        ASSERT_EQ(offer.size(), 120u);
    }
    const auto end = seed_deadline(); owner.send(index, offer, end);
    try { (void)owner.receive(index, end); ADD_FAILURE() << "Invalid startup offer unexpectedly received a challenge"; }
    catch (const launch::error& error) { EXPECT_EQ(error.code, launch::error_code::io); }
    std::optional<launch::terminal_observation> terminal;
    while (!(terminal = owner.observe_terminal(index, end))) std::this_thread::sleep_for(1ms);
    EXPECT_TRUE(WIFEXITED(terminal->wait_status)); EXPECT_EQ(WEXITSTATUS(terminal->wait_status), 74);
    (void)owner.close_and_join(end);
    EXPECT_FALSE(std::filesystem::exists(area.path / "receiver-refusal/data/memory.sqlite"));
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), children);
}
}
void seed_success_case() {
    seed_area area; const auto cohorts = seed::created_launch_cohort::unresolved_cohorts();
    const auto children = launch::owned_child::unresolved_lifetimes();
    auto value = seed::seeded_installation_store::create_engram(area.fd, "success", actual_seed_child(), seed_deadline());
    const auto terminal = value.initializer_terminal(); EXPECT_TRUE(WIFEXITED(terminal.wait_status)); EXPECT_EQ(WEXITSTATUS(terminal.wait_status), 0);
    int status = 0; errno = 0; EXPECT_EQ(::waitpid(static_cast<pid_t>(terminal.child.pid), &status, WNOHANG), -1); EXPECT_EQ(errno, ECHILD);
    struct stat main{}, parent{};
    ASSERT_EQ(::stat((area.path / "success/data/memory.sqlite").c_str(), &main), 0);
    ASSERT_EQ(::stat((area.path / "success/data").c_str(), &parent), 0);
    EXPECT_EQ(value.main_file(), (seed::file_identity{static_cast<std::uint64_t>(main.st_dev), static_cast<std::uint64_t>(main.st_ino)}));
    EXPECT_EQ(value.parent_directory(), (seed::file_identity{static_cast<std::uint64_t>(parent.st_dev), static_cast<std::uint64_t>(parent.st_ino)}));
    EXPECT_EQ(seed::created_launch_cohort::unresolved_cohorts(), cohorts); EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), children);
    auto moved = std::move(value); EXPECT_EQ(moved.main_file().inode, static_cast<std::uint64_t>(main.st_ino));
    EXPECT_THROW((void)value.main_file(), seed::seed_error);
    // The fixture file is deliberately not SQLite. This tests actual process
    // custody/channel/origin, not application schema or ordinary-store adoption.
}
void seed_changed_binary_case() {
    seed_area area; auto expected = actual_seed_child(); expected.content[0] ^= 1;
    EXPECT_THROW((void)seed::seeded_installation_store::create_engram(area.fd, "never-created", expected, seed_deadline()), seed::file_error);
    EXPECT_FALSE(std::filesystem::exists(area.path / "never-created"));
}
namespace {
struct seed_fixture_child {
    pid_t pid = -1;
    bool failed_terminal = false;
    explicit seed_fixture_child(pid_t value) : pid(value) {}
    seed_fixture_child(const seed_fixture_child&) = delete;
    bool terminal(launch::deadline end) {
        while (std::chrono::steady_clock::now() < end) {
            siginfo_t value{};
            if (::waitid(P_PID, static_cast<id_t>(pid), &value, WEXITED | WNOHANG | WNOWAIT) == 0) {
                if (value.si_pid == pid) { failed_terminal = value.si_code != CLD_EXITED || value.si_status != 0; return true; }
            } else if (errno != EINTR) { pid = -1; return false; }
            std::this_thread::sleep_for(2ms);
        }
        return false;
    }
    int join(launch::deadline end) {
        if (pid <= 0 || !terminal(end)) return -1;
        // Failure containment while the actual leader is still unreaped; a
        // failed helper cannot leave its known initializer group running.
        if (failed_terminal) (void)::kill(-pid, SIGKILL);
        while (std::chrono::steady_clock::now() < end) {
            int status = 0; const auto result = ::waitpid(pid, &status, WNOHANG);
            if (result == pid) { pid = -1; return status; }
            if (result < 0 && errno != EINTR) { pid = -1; return -1; }
            std::this_thread::sleep_for(2ms);
        }
        return -1;
    }
    ~seed_fixture_child() {
        if (pid <= 0) return;
        const auto end = std::chrono::steady_clock::now() + 5s; const auto owned = pid;
        siginfo_t value{}; int result = -1;
        do { result = ::waitid(P_PID, static_cast<id_t>(owned), &value, WEXITED | WNOHANG | WNOWAIT); }
        while (result < 0 && errno == EINTR && std::chrono::steady_clock::now() < end);
        if (result != 0) { ADD_FAILURE() << "seed fixture lost cleanup custody"; return; }
        // The actual unreaped group leader still reserves this process group.
        (void)::kill(-owned, SIGKILL);
        if (join(end) < 0) { ADD_FAILURE() << "seed fixture cleanup completion unproved"; return; }
        while (std::chrono::steady_clock::now() < end) {
            if (::kill(-owned, 0) < 0 && errno == ESRCH) return;
            std::this_thread::sleep_for(2ms);
        }
        ADD_FAILURE() << "seed fixture descendant cleanup unproved";
    }
};
void isolated_seed_case(const char* mode) {
    seed_area area;
    const auto arguments = ::testing::internal::GetArgvs(); ASSERT_FALSE(arguments.empty());
    auto executable = std::filesystem::canonical(std::filesystem::absolute(arguments.front())).string();
    std::string filter = "--gtest_filter=OrdinaryInstallationSeed.ProcessHelper";
    std::string repeat = "--gtest_repeat=1", color = "--gtest_color=no", output = "--gtest_output=";
    char* argv[] = {executable.data(), filter.data(), repeat.data(), color.data(), output.data(), nullptr};
    std::vector<std::string> environment;
    for (char** item = environ; *item; ++item) {
        const std::string value(*item);
        if (!value.starts_with("LATTICE_INSTALLATION_SEED_") && !value.starts_with("LATTICE_TEST_LOG_PATH=") &&
            !value.starts_with("GTEST_TOTAL_SHARDS=") && !value.starts_with("GTEST_SHARD_INDEX=") &&
            !value.starts_with("GTEST_SHARD_STATUS_FILE=")) environment.push_back(value);
    }
    environment.push_back(std::string("LATTICE_INSTALLATION_SEED_CASE=") + mode);
    environment.push_back("LATTICE_INSTALLATION_SEED_MARKER=" + (area.path / "complete").string());
    const auto* parent_log = std::getenv("LATTICE_TEST_LOG_PATH");
    const auto log = parent_log && *parent_log ? std::string(parent_log) + "." + area.path.filename().string() + ".seed.log" : area.path.string() + ".seed.log";
    const auto reserved = ::open((log + ".native").c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(reserved, 0); ASSERT_EQ(::close(reserved), 0);
    environment.push_back("LATTICE_TEST_LOG_PATH=" + log + ".native");
    std::vector<char*> env; for (auto& value : environment) env.push_back(value.data()); env.push_back(nullptr);
    posix_spawn_file_actions_t actions; ASSERT_EQ(::posix_spawn_file_actions_init(&actions), 0);
    struct actions_owner { posix_spawn_file_actions_t& value; ~actions_owner() { ::posix_spawn_file_actions_destroy(&value); } } own_actions{actions};
    ASSERT_EQ(::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600), 0);
    ASSERT_EQ(::posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO), 0);
    posix_spawnattr_t attributes; ASSERT_EQ(::posix_spawnattr_init(&attributes), 0);
    struct attributes_owner { posix_spawnattr_t& value; ~attributes_owner() { ::posix_spawnattr_destroy(&value); } } own_attributes{attributes};
    short flags = POSIX_SPAWN_SETPGROUP;
#if defined(__APPLE__)
    flags |= POSIX_SPAWN_CLOEXEC_DEFAULT;
#elif defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 34)
    ASSERT_EQ(::posix_spawn_file_actions_addclosefrom_np(&actions, 3), 0);
#else
    FAIL() << "seed fixture requires closefrom spawn support";
#endif
#else
    FAIL() << "seed fixture requires closefrom spawn support";
#endif
    ASSERT_EQ(::posix_spawnattr_setpgroup(&attributes, 0), 0); ASSERT_EQ(::posix_spawnattr_setflags(&attributes, flags), 0);
    pid_t pid = -1; ASSERT_EQ(::posix_spawn(&pid, executable.c_str(), &actions, &attributes, argv, env.data()), 0);
    seed_fixture_child child(pid);
    // Separate bounded guardian for the five-second operation and five-second
    // actual cleanup; no blocking waitpid and no group signal after reaping.
    const auto status = child.join(std::chrono::steady_clock::now() + 15s);
    ASSERT_GE(status, 0) << log; ASSERT_TRUE(WIFEXITED(status)) << log; ASSERT_EQ(WEXITSTATUS(status), 0) << log;
    ASSERT_TRUE(std::filesystem::is_regular_file(area.path / "complete")) << log;
    const auto absent_by = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < absent_by) {
        if (::kill(-pid, 0) < 0 && errno == ESRCH) return;
        std::this_thread::sleep_for(2ms);
    }
    FAIL() << "seed fixture group remains after exact controller completion";
}
}
TEST(OrdinaryInstallationSeed, RealReceiverAndActualZeroExitProduceOnlyRetainedPhysicalFacts) { isolated_seed_case("success"); }
TEST(OrdinaryInstallationSeed, ChangedApprovedExecutableRefusesBeforeNamespaceCreation) { isolated_seed_case("changed-binary"); }
TEST(OrdinaryInstallationSeed, WrongNonceReplyCannotCompleteTheOwnedInitializer) { isolated_seed_case("bad-reply"); }
TEST(OrdinaryInstallationSeed, ActualCallbackFailureIsNotACompletedInitialization) { isolated_seed_case("callback-failure"); }
TEST(OrdinaryInstallationSeed, PositiveReplyWithoutThePhysicalFileStillRefuses) { isolated_seed_case("missing-file"); }
TEST(OrdinaryInstallationSeed, PositiveReplyDoesNotReplaceTheActualNonzeroExit) { isolated_seed_case("reply-nonzero-exit"); }
TEST(OrdinaryInstallationSeed, PositiveReplyWithRunningChildTimesOutAndJoinsActualCustody) { isolated_seed_case("reply-still-running"); }
TEST(OrdinaryInstallationSeed, MalformedOfferRefusesBeforeCallingTheRealReceiverCallback) { isolated_seed_case("malformed-offer"); }
TEST(OrdinaryInstallationSeed, WrongKernelChildBirthRefusesBeforeCallingTheRealReceiverCallback) { isolated_seed_case("wrong-child-birth"); }
TEST(OrdinaryInstallationSeed, ProcessHelper) {
    const auto* mode = std::getenv("LATTICE_INSTALLATION_SEED_CASE");
    const auto* marker = std::getenv("LATTICE_INSTALLATION_SEED_MARKER");
    if (!mode || !marker) GTEST_SKIP() << "Requires actual parent-owned controller fixture";
    ASSERT_EQ(::getpgrp(), ::getpid());
#if defined(__APPLE__)
    proc_taskinfo info{}; ASSERT_EQ(::proc_pidinfo(::getpid(), PROC_PIDTASKINFO, 0, &info, sizeof(info)), sizeof(info));
    ASSERT_EQ(info.pti_threadnum, 1);
#else
    std::size_t threads = 0;
    for (const auto& item : std::filesystem::directory_iterator("/proc/self/task")) { (void)item; ++threads; }
    ASSERT_EQ(threads, 1u);
#endif
    if (std::string(mode) == "success") seed_success_case();
    else if (std::string(mode) == "changed-binary") seed_changed_binary_case();
    else if (std::string(mode) == "malformed-offer") actual_receiver_refuses(false);
    else if (std::string(mode) == "wrong-child-birth") actual_receiver_refuses(true);
    else actual_seed_refuses(mode);
    if (::testing::Test::HasFailure()) return;
    const auto fd = ::open(marker, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600); ASSERT_GE(fd, 0);
    const char done = 1; const auto wrote = ::write(fd, &done, 1); const auto closed = ::close(fd);
    ASSERT_EQ(wrote, 1); ASSERT_EQ(closed, 0);
}
#endif
