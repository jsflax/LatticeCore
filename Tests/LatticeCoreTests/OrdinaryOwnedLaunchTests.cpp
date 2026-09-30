#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_owned_launch.hpp"
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
launch::launch_specification shell(const std::string& program) {
    launch::launch_specification result;
    result.executable = "/bin/sh";
    result.working_directory = "/";
    result.arguments = {"-c", program};
    result.environment = {"PATH=/usr/bin:/bin"};
    return result;
}
template <typename Function> void expect_error(launch::error_code code, Function&& function) {
    try { function(); ADD_FAILURE() << "Expected a fixed launch failure"; }
    catch (const launch::error& error) { EXPECT_EQ(error.code, code); }
    catch (...) { ADD_FAILURE() << "Unexpected exception type"; }
}
void raw_write(int fd, const std::vector<std::uint8_t>& bytes) {
    std::size_t offset = 0;
    while (offset != bytes.size()) {
        auto amount = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (amount < 0 && errno == EINTR) continue;
        ASSERT_GT(amount, 0);
        offset += static_cast<std::size_t>(amount);
    }
}
thread_local launch::test_hooks::wait_boundary interrupted_boundary;
thread_local launch::deadline interruption_deadline;
thread_local unsigned interrupted_attempts = 0;
bool interrupt_retirement_wait(launch::test_hooks::wait_boundary boundary) {
    if (boundary != interrupted_boundary) return false;
    if (++interrupted_attempts != 1)
        throw std::runtime_error("retirement attempted another wait beyond its deadline");
    // An actual signal could consume the remaining budget before EINTR is
    // returned. Reproduce that boundary without a process-wide signal storm.
    std::this_thread::sleep_until(interruption_deadline);
    return true;
}
struct interrupted_wait_scope {
    launch::test_hooks::interrupt_wait previous = launch::test_hooks::interrupt;
    const launch::test_hooks::wait_boundary previous_boundary = interrupted_boundary;
    const launch::deadline previous_deadline = interruption_deadline;
    const unsigned previous_attempts = interrupted_attempts;
    interrupted_wait_scope(launch::test_hooks::wait_boundary boundary, launch::deadline end) {
        interrupted_boundary = boundary; interruption_deadline = end; interrupted_attempts = 0;
        launch::test_hooks::interrupt = interrupt_retirement_wait;
    }
    ~interrupted_wait_scope() {
        launch::test_hooks::interrupt = previous;
        interrupted_boundary = previous_boundary;
        interruption_deadline = previous_deadline;
        interrupted_attempts = previous_attempts;
    }
};

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
        auto pattern = (std::filesystem::temp_directory_path() / "lattice-owned-launch-XXXXXX").string();
        std::vector<char> bytes(pattern.begin(), pattern.end()); bytes.push_back(0);
        if (!::mkdtemp(bytes.data())) throw std::runtime_error("owned-launch helper directory unavailable");
        path = bytes.data();
    }
    ~helper_area() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
};
bool wait_marker(const std::filesystem::path& path) {
    const auto end = within();
    while (std::chrono::steady_clock::now() < end) {
        if (std::filesystem::exists(path)) return true;
        std::this_thread::sleep_for(2ms);
    }
    return false;
}
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
    std::string filter = "--gtest_filter=OrdinaryOwnedLaunch.ProcessHelper";
    std::string repeat = "--gtest_repeat=1", color = "--gtest_color=no", output = "--gtest_output=";
    char* argv[] = {executable.data(), filter.data(), repeat.data(), color.data(), output.data(), nullptr};
    std::vector<std::string> values;
    for (char** item = environ; *item; ++item) {
        const std::string value(*item);
        if (!value.starts_with("LATTICE_ORDINARY_LAUNCH_HELPER_") &&
            !value.starts_with("LATTICE_TEST_LOG_PATH=") &&
            !value.starts_with("GTEST_TOTAL_SHARDS=") && !value.starts_with("GTEST_SHARD_INDEX=") &&
            !value.starts_with("GTEST_SHARD_STATUS_FILE=")) values.push_back(value);
    }
    values.push_back("LATTICE_ORDINARY_LAUNCH_HELPER_DIRECTORY=" + area.path.string());
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

TEST(OrdinaryOwnedLaunch, RealChildEchoRetainsBirthAndActualTerminalObservation) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto identity = child.identity();
    EXPECT_GT(identity.pid, 0);
    EXPECT_EQ(identity.parent, ::getpid());
    EXPECT_GT(identity.birth_major, 0u);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    const launch::frame payload{0, 1, 2, 0, 255, 127};
    child.send(payload, within());
    EXPECT_EQ(child.receive(within()), payload);
    const auto terminal = child.stop_and_join(within());
    EXPECT_EQ(terminal.child, identity);
    EXPECT_TRUE(WIFEXITED(terminal.wait_status) || WIFSIGNALED(terminal.wait_status));
    EXPECT_EQ(child.stop_and_join(within()).wait_status, terminal.wait_status);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(identity.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST(OrdinaryOwnedLaunch, ExplicitDirectorySurvivesAndUnrelatedDescriptorDoesNot) {
    fd_owner original(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    ASSERT_GE(original.value, 0);
    fd_owner unrelated(::fcntl(original.value, F_DUPFD, 200));
    ASSERT_GE(unrelated.value, 200);
    auto specification = shell("test -d /dev/fd/4 || exit 81; test ! -e /dev/fd/" +
        std::to_string(unrelated.value) + " || exit 82; exec /bin/cat <&3 >&3");
    specification.inherited_directories.push_back(original.value);
    auto child = launch::owned_child::spawn(specification);
    const launch::frame payload{7, 3, 9};
    child.send(payload, within());
    EXPECT_EQ(child.receive(within()), payload);
    (void)child.stop_and_join(within());
    EXPECT_GE(::fcntl(unrelated.value, F_GETFD), 0); // Parent custody was preserved.
}

TEST(OrdinaryOwnedLaunch, MoveReplacementRetiresTheDisplacedActualChild) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto first = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    auto second = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto displaced = first.identity(), retained = second.identity();
    first = std::move(second);
    EXPECT_EQ(first.identity(), retained);
    expect_error(launch::error_code::unavailable, [&] { (void)second.identity(); });
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(displaced.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    auto moved = std::move(first);
    const launch::frame payload{1, 8, 3};
    moved.send(payload, within());
    EXPECT_EQ(moved.receive(within()), payload);
    (void)moved.stop_and_join(within());
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
}

TEST(OrdinaryOwnedLaunch, WrapperDestructionJoinsAfterOriginalExchangeFailure) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    launch::process_identity identity;
    {
        auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
        identity = child.identity();
        // Nothing was sent: the actual child remains waiting, so this is a
        // genuine receive deadline, not a synthetic failure callback.
        expect_error(launch::error_code::timed_out, [&] { (void)child.receive(within(10ms)); });
        expect_error(launch::error_code::io, [&] { child.send({2}, within()); });
    }
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(identity.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST(OrdinaryOwnedLaunch, FailedSpawnDoesNotLeaveAFabricatedOwnedLifetime) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto specification = shell("exit 0");
    specification.executable = "/nonexistent-lattice-owned-launch-fixture/program";
    expect_error(launch::error_code::launch_failed, [&] { (void)launch::owned_child::spawn(specification); });
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
}

TEST(OrdinaryOwnedLaunch, InvalidChannelAdoptionClosesItsActualDescriptorOnFailure) {
    const auto descriptor = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    ASSERT_GE(descriptor, 0);
    expect_error(launch::error_code::invalid_specification, [&] { launch::inherited_channel invalid(descriptor); });
    errno = 0;
    EXPECT_EQ(::fcntl(descriptor, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(OrdinaryOwnedLaunch, ActualPartialFrameFailureCannotBeReinterpretedByLaterRead) {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    fd_owner writer(sockets[0]);
    launch::inherited_channel reader(sockets[1]);
    raw_write(writer.value, {0, 0, 0, 4, 1, 2});
    expect_error(launch::error_code::timed_out, [&] { (void)reader.receive(within(10ms)); });
    raw_write(writer.value, {3, 4, 0, 0, 0, 1, 9});
    expect_error(launch::error_code::io, [&] { (void)reader.receive(within()); });
}

TEST(OrdinaryOwnedLaunch, OversizedAndEmptyWireLengthsRefuseBeforePayloadAllocation) {
    for (const std::array<std::uint8_t, 4> header : {
             std::array<std::uint8_t, 4>{0, 0, 0, 0},
             std::array<std::uint8_t, 4>{0, 2, 0, 1}}) {
        int sockets[2] = {-1, -1};
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        fd_owner writer(sockets[0]);
        launch::inherited_channel reader(sockets[1]);
        raw_write(writer.value, std::vector<std::uint8_t>(header.begin(), header.end()));
        expect_error(launch::error_code::malformed_frame, [&] { (void)reader.receive(within()); });
    }
}

TEST(OrdinaryOwnedLaunch, OtherThreadRefusesBeforeConsumingTheCurrentFrame) {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    launch::inherited_channel writer(sockets[0]), reader(sockets[1]);
    const launch::frame expected{5, 1, 6};
    writer.send(expected, within());
    std::exception_ptr failure;
    std::jthread other([&] {
        try { (void)reader.receive(within()); } catch (...) { failure = std::current_exception(); }
    });
    other.join();
    ASSERT_NE(failure, nullptr);
    expect_error(launch::error_code::wrong_thread, [&] { std::rethrow_exception(failure); });
    EXPECT_EQ(reader.receive(within()), expected);
}

TEST(OrdinaryOwnedLaunch, MaximumFrameCrossesTheActualSocketWithoutTruncation) {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    fd_owner writer_descriptor(sockets[0]);
    launch::inherited_channel reader(sockets[1]);
    launch::frame expected(launch::maximum_frame_bytes);
    for (std::size_t i = 0; i != expected.size(); ++i) expected[i] = static_cast<std::uint8_t>(i * 31);
    std::exception_ptr send_failure;
    std::jthread sending([&] {
        try {
            launch::inherited_channel writer(std::exchange(writer_descriptor.value, -1));
            writer.send(expected, within());
        } catch (...) { send_failure = std::current_exception(); }
    });
    EXPECT_EQ(reader.receive(within()), expected);
    sending.join();
    EXPECT_EQ(send_failure, nullptr);
}

TEST(OrdinaryOwnedLaunch, RetirementCancelsAndJoinsTheActualChannelReaderBeforeClose) {
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    std::promise<void> entered;
    auto waiting = entered.get_future();
    std::exception_ptr read_failure;
    std::jthread reading([&] {
        entered.set_value();
        try { (void)child.receive(within()); } catch (...) { read_failure = std::current_exception(); }
    });
    ASSERT_EQ(waiting.wait_until(within()), std::future_status::ready);
    const auto terminal = child.stop_and_join(within());
    reading.join();
    ASSERT_NE(read_failure, nullptr);
    // Retirement can occur between transfer's cancellation check and recv.
    // A real EOF/I/O refusal may win that race; neither result is a successful
    // frame. Preserve the production operation's original failure precedence.
    try { std::rethrow_exception(read_failure); }
    catch (const launch::error& error) {
        EXPECT_TRUE(error.code == launch::error_code::canceled || error.code == launch::error_code::io);
    } catch (...) { ADD_FAILURE() << "Unexpected reader exception type"; }
    EXPECT_EQ(terminal.child, child.identity());
    ASSERT_TRUE(WIFSIGNALED(terminal.wait_status));
    EXPECT_TRUE(WTERMSIG(terminal.wait_status) == SIGTERM || WTERMSIG(terminal.wait_status) == SIGKILL);
}

TEST(OrdinaryOwnedLaunch, ActiveAndExpiredTerminalObservationLeaveTheRealChannelUsable) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto identity = child.identity();
    child.send({3, 9, 1}, within());
    EXPECT_FALSE(child.observe_terminal(within()).has_value());
    EXPECT_EQ(child.receive(within()), (launch::frame{3, 9, 1}));
    expect_error(launch::error_code::timed_out, [&] {
        (void)child.observe_terminal(std::chrono::steady_clock::now());
    });
    child.send({4, 9, 2}, within());
    EXPECT_EQ(child.receive(within()), (launch::frame{4, 9, 2}));
    EXPECT_EQ(child.identity(), identity);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    (void)child.stop_and_join(within());
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
}

TEST(OrdinaryOwnedLaunch, ActualTerminalObservationReapsAndRetainsTheExactReceipt) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto identity = child.identity();
    child.send({7, 6}, within());
    EXPECT_EQ(child.receive(within()), (launch::frame{7, 6}));
    // The fixture causes a real terminal event; observe_terminal itself never
    // requests child retirement and cannot invent this status.
    ASSERT_EQ(::kill(static_cast<pid_t>(identity.pid), SIGTERM), 0);
    std::optional<launch::terminal_observation> terminal;
    const auto end = within();
    while (std::chrono::steady_clock::now() < end && !(terminal = child.observe_terminal(end)))
        std::this_thread::sleep_for(2ms);
    ASSERT_TRUE(terminal.has_value());
    EXPECT_EQ(terminal->child, identity);
    ASSERT_TRUE(WIFSIGNALED(terminal->wait_status));
    EXPECT_EQ(WTERMSIG(terminal->wait_status), SIGTERM);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
    const auto again = child.observe_terminal(within());
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->child, identity);
    EXPECT_EQ(again->wait_status, terminal->wait_status);
    EXPECT_EQ(child.stop_and_join(within()).wait_status, terminal->wait_status);
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(identity.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST(OrdinaryOwnedLaunch, LostReaperObservationIsAnErrorRatherThanAnActiveResult) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    {
        auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
        const auto identity = child.identity();
        ASSERT_EQ(::kill(static_cast<pid_t>(identity.pid), SIGTERM), 0);
        fixture_child external_reaper(static_cast<pid_t>(identity.pid));
        const auto status = external_reaper.join(within());
        ASSERT_GE(status, 0);
        expect_error(launch::error_code::terminal_unproved, [&] { (void)child.observe_terminal(within()); });
        EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    }
    // Actual child is gone, but the owner did not reap it. Its negative custody
    // evidence survives wrapper destruction and no false active result escapes.
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
}

TEST(OrdinaryOwnedLaunch, InterruptedObservationDeadlineRetainsTheActualChildForLaterJoin) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto identity = child.identity();
    // Prove actual child/channel readiness before the unchanged five-second budget.
    child.send({4, 2}, within());
    EXPECT_EQ(child.receive(within()), (launch::frame{4, 2}));
    {
        const auto end = within();
        interrupted_wait_scope fault(launch::test_hooks::wait_boundary::observe, end);
        expect_error(launch::error_code::timed_out, [&] { (void)child.stop_and_join(end); });
        EXPECT_EQ(interrupted_attempts, 1u);
    }
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    EXPECT_EQ(child.identity(), identity);
    const auto terminal = child.stop_and_join(within());
    EXPECT_EQ(terminal.child, identity);
    EXPECT_TRUE(WIFEXITED(terminal.wait_status) || WIFSIGNALED(terminal.wait_status));
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
}

TEST(OrdinaryOwnedLaunch, InterruptedReapDeadlineRetainsActualTerminalCustodyForLaterJoin) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
    const auto identity = child.identity();
    child.send({8, 2}, within());
    EXPECT_EQ(child.receive(within()), (launch::frame{8, 2}));
    // Stop must reach a real WNOWAIT terminal observation before the reap-only
    // fault can run. Its interruption consumes the same passed stop deadline.
    {
        const auto end = within();
        interrupted_wait_scope fault(launch::test_hooks::wait_boundary::reap, end);
        expect_error(launch::error_code::timed_out, [&] { (void)child.stop_and_join(end); });
        EXPECT_EQ(interrupted_attempts, 1u);
    }
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    const auto terminal = child.stop_and_join(within());
    EXPECT_EQ(terminal.child, identity);
    ASSERT_TRUE(WIFSIGNALED(terminal.wait_status));
    EXPECT_TRUE(WTERMSIG(terminal.wait_status) == SIGTERM || WTERMSIG(terminal.wait_status) == SIGKILL);
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before);
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(identity.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST(OrdinaryOwnedLaunch, LostExclusiveReaperCustodyStaysUnprovedAfterWrapperDestruction) {
    const auto before = launch::owned_child::unresolved_lifetimes();
    {
        auto child = launch::owned_child::spawn(shell("exec /bin/cat <&3 >&3"));
        const auto identity = child.identity();
        // Deliberately violate the dedicated-supervisor reaper contract. Even
        // though this fixture really reaps the child, the owner did not observe
        // that event and cannot relabel its own custody as positively settled.
        ASSERT_EQ(::kill(static_cast<pid_t>(identity.pid), SIGTERM), 0);
        int status = 0; pid_t observed = 0;
        const auto end = within();
        do {
            observed = ::waitpid(static_cast<pid_t>(identity.pid), &status, WNOHANG);
            if (observed == identity.pid || (observed < 0 && errno != EINTR)) break;
            std::this_thread::sleep_for(2ms);
        } while (std::chrono::steady_clock::now() < end);
        // Failure leaves the actual owned_child wrapper responsible for its
        // existing bounded stop/join path; this fixture never blocks waitpid.
        ASSERT_EQ(observed, identity.pid);
        expect_error(launch::error_code::terminal_unproved, [&] { (void)child.stop_and_join(within()); });
        EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
    }
    // Intentional process-lifetime refusal evidence. No live child remains, and
    // no later action may signal that now-unowned/reusable numeric PID.
    EXPECT_EQ(launch::owned_child::unresolved_lifetimes(), before + 1);
}

TEST(OrdinaryOwnedLaunch, ForkedWrapperRefusesUseAndCloseDoesNotShutdownParentChannel) {
    helper_area area;
    fixture_child helper(spawn_fork_helper(area), true);
    // The child publishes this only after joining its actual fork child and
    // checking both original parent-channel reads. Never fork the suite runner.
    ASSERT_TRUE(wait_marker(area.path / "fork-joined.signal"));
    const auto status = helper.join(within());
    ASSERT_GE(status, 0);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(OrdinaryOwnedLaunch, ProcessHelper) {
    const auto* path = std::getenv("LATTICE_ORDINARY_LAUNCH_HELPER_DIRECTORY");
    if (!path) GTEST_SKIP() << "Requires the real parent-owned process fixture";
    ASSERT_EQ(::getpgrp(), ::getpid());
    ASSERT_TRUE(is_single_threaded_helper());
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    launch::inherited_channel writer(sockets[0]), reader(sockets[1]);
    const launch::frame payload{2, 7, 1};
    writer.send(payload, within());
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        int result = 1;
        {
            auto inherited = std::move(reader);
            try { (void)inherited.receive(within()); }
            catch (const launch::error& error) {
                if (error.code == launch::error_code::inherited_use) result = 0;
            } catch (...) {}
        } // close inherited copy only; never shutdown the shared socket description.
        ::_exit(result);
    }
    fixture_child descendant(child);
    const auto status = descendant.join(within());
    ASSERT_GE(status, 0);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(reader.receive(within()), payload);
    writer.send({3, 8}, within());
    EXPECT_EQ(reader.receive(within()), (launch::frame{3, 8}));
    if (!::testing::Test::HasFailure()) {
        const auto marker = std::filesystem::path(path) / "fork-joined.signal";
        fd_owner written(::open(marker.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
        ASSERT_GE(written.value, 0);
    }
}
#endif
