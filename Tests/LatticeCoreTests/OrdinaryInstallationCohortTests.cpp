#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_installation_cohort.hpp"
#include <filesystem>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <cerrno>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
namespace cohort = lattice::detail::ordinary_installation;
namespace transport = lattice::detail::ordinary_launch;
using namespace std::chrono_literals;
struct cohort_area {
    std::filesystem::path path;
    int fd = -1;
    cohort_area() {
        auto name = (std::filesystem::temp_directory_path() / "lattice-cohort-XXXXXX").string();
        std::vector<char> bytes(name.begin(), name.end()); bytes.push_back(0);
        if (!::mkdtemp(bytes.data())) throw std::runtime_error("cohort test directory unavailable");
        path = bytes.data(); fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("cohort test directory descriptor unavailable");
    }
    ~cohort_area() {
        if (fd >= 0) ::close(fd);
        std::error_code ignored; std::filesystem::remove_all(path, ignored);
    }
};
transport::deadline cohort_deadline() { return std::chrono::steady_clock::now() + 5s; }
transport::launch_specification cohort_echo() {
    transport::launch_specification value;
    value.executable = "/bin/sh"; value.working_directory = "/";
    value.arguments = {"-c", "exec /bin/cat <&3 >&3"}; value.environment = {"PATH=/usr/bin:/bin"}; return value;
}
template <typename F> void expect_cohort_error(cohort::cohort_error_code code, F&& function) {
    try { function(); ADD_FAILURE() << "Expected cohort refusal"; }
    catch (const cohort::cohort_error& error) { EXPECT_EQ(error.code, code); }
    catch (...) { ADD_FAILURE() << "Unexpected cohort exception"; }
}
std::string cohort_shell_quote(const std::string& value) {
    std::string result = "'";
    for (const char byte : value) { if (byte == '\'') result += "'\\''"; else result += byte; }
    return result + "'";
}
void expect_reaped(const transport::process_identity& child) {
    int status = 0; errno = 0;
    EXPECT_EQ(::waitpid(static_cast<pid_t>(child.pid), &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}
}

TEST(OrdinaryInstallationCohort, ActualChildrenAreRetainedBehindAStickyDurableLaunchGate) {
    cohort_area area;
    const auto before = cohort::created_launch_cohort::unresolved_cohorts();
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    EXPECT_TRUE(std::filesystem::is_regular_file(area.path / "launch" / "launch.v1"));
    EXPECT_TRUE(std::filesystem::is_regular_file(area.path / "launch" / "launch.lock"));
    const auto first = owner.launch(cohort_echo()), second = owner.launch(cohort_echo());
    const auto one = owner.child_identity(first), two = owner.child_identity(second);
    EXPECT_NE(one.pid, two.pid);
    owner.send(first, {9, 2}, cohort_deadline()); owner.send(second, {6, 2}, cohort_deadline());
    EXPECT_EQ(owner.receive(first, cohort_deadline()), (transport::frame{9, 2}));
    EXPECT_EQ(owner.receive(second, cohort_deadline()), (transport::frame{6, 2}));
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before + 1);
    const auto closed = owner.close_and_join(cohort_deadline());
    ASSERT_EQ(closed.children.size(), 2u);
    EXPECT_EQ(closed.children[0].child, one); EXPECT_EQ(closed.children[1].child, two);
    EXPECT_GT(closed.revision, 1u);
    expect_reaped(one); expect_reaped(two);
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before);
    expect_cohort_error(cohort::cohort_error_code::closed, [&] { (void)owner.launch(cohort_echo()); });
    const auto repeated = owner.close_and_join(cohort_deadline());
    EXPECT_EQ(repeated.revision, closed.revision);
    ASSERT_EQ(repeated.children.size(), closed.children.size());
    EXPECT_EQ(repeated.children[0].wait_status, closed.children[0].wait_status);
    EXPECT_EQ(repeated.children[1].wait_status, closed.children[1].wait_status);
}

TEST(OrdinaryInstallationCohort, DurableAdmissionClosurePrecedesAnyActualChildSignal) {
    cohort_area area;
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    const auto index = owner.launch(cohort_echo()); const auto identity = owner.child_identity(index);
    owner.send(index, {5, 5}, cohort_deadline());
    EXPECT_EQ(owner.receive(index, cohort_deadline()), (transport::frame{5, 5}));
    owner.close_launch_admission(cohort_deadline());
    expect_cohort_error(cohort::cohort_error_code::closed, [&] { (void)owner.launch(cohort_echo()); });
    siginfo_t observed{};
    ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(identity.pid), &observed, WEXITED | WNOHANG | WNOWAIT), 0);
    EXPECT_EQ(observed.si_pid, 0); // The gate is closed while the actual child remains alive.
    expect_cohort_error(cohort::cohort_error_code::closed, [&] { owner.send(index, {9}, cohort_deadline()); });
    owner.close_launch_admission(cohort_deadline());
    const auto result = owner.close_and_join(cohort_deadline());
    ASSERT_EQ(result.children.size(), 1u); EXPECT_EQ(result.children[0].child, identity);
    expect_reaped(identity);
}

TEST(OrdinaryInstallationCohort, MoveReplacementRetiresTheDisplacedActualCohort) {
    cohort_area area;
    const auto before = cohort::created_launch_cohort::unresolved_cohorts();
    auto first = cohort::created_launch_cohort::create_before_child(area.fd, "first");
    auto second = cohort::created_launch_cohort::create_before_child(area.fd, "second");
    const auto displaced = first.child_identity(first.launch(cohort_echo()));
    const auto retained_index = second.launch(cohort_echo());
    const auto retained = second.child_identity(retained_index);
    first = std::move(second);
    expect_reaped(displaced);
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before + 1);
    EXPECT_EQ(first.child_identity(retained_index), retained);
    first.send(retained_index, {1, 2, 3}, cohort_deadline());
    EXPECT_EQ(first.receive(retained_index, cohort_deadline()), (transport::frame{1, 2, 3}));
    expect_cohort_error(cohort::cohort_error_code::unavailable, [&] { (void)second.close_and_join(cohort_deadline()); });
    (void)first.close_and_join(cohort_deadline());
    expect_reaped(retained);
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before);
}

TEST(OrdinaryInstallationCohort, OnlyAnActuallyCreatedNamespaceSurvivesItsOriginalChildJoin) {
    cohort_area area;
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    auto space = owner.create_store_namespace("data");
    const auto parent_identity = space.directory_identity();
    const auto inherited = space.duplicate_for_child();
    ASSERT_GE(inherited, 3);
    auto specification = cohort_echo();
    specification.inherited_directories = {inherited};
    const auto data_path = area.path / "launch" / "data" / "physical-file";
    specification.arguments = {"-c", "test -d /dev/fd/4 || exit 71; printf seeded > " +
        cohort_shell_quote(data_path.string()) + " || exit 72; exec /bin/cat <&3 >&3"};
    std::size_t child;
    try { child = owner.launch(specification); } catch (...) { ::close(inherited); throw; }
    ASSERT_EQ(::close(inherited), 0);
    owner.send(child, {3, 3}, cohort_deadline());
    EXPECT_EQ(owner.receive(child, cohort_deadline()), (transport::frame{3, 3}));
    expect_cohort_error(cohort::cohort_error_code::incomplete, [&] { (void)space.inspect_after_join("physical-file"); });
    expect_cohort_error(cohort::cohort_error_code::closed, [&] { (void)owner.create_store_namespace("late"); });
    (void)owner.close_and_join(cohort_deadline());
    EXPECT_EQ(space.directory_identity(), parent_identity);
    const auto file_identity = space.inspect_after_join("physical-file");
    struct stat actual{}; ASSERT_EQ(::stat(data_path.c_str(), &actual), 0);
    EXPECT_EQ(file_identity.device, static_cast<std::uint64_t>(actual.st_dev));
    EXPECT_EQ(file_identity.inode, static_cast<std::uint64_t>(actual.st_ino));
    expect_cohort_error(cohort::cohort_error_code::closed, [&] { (void)space.duplicate_for_child(); });
    // This is a physical namespace fixture, not a populated SQLite or app
    // acceptance test and not an issuance of a recovery/context capability.
}

TEST(OrdinaryInstallationCohort, AnExistingDataDirectoryCannotBeClaimedByNamespaceCreation) {
    cohort_area area;
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    ASSERT_TRUE(std::filesystem::create_directory(area.path / "launch" / "existing"));
    expect_cohort_error(cohort::cohort_error_code::unavailable, [&] { (void)owner.create_store_namespace("existing"); });
    EXPECT_TRUE(std::filesystem::is_empty(area.path / "launch" / "existing"));
    (void)owner.close_and_join(cohort_deadline());
}

TEST(OrdinaryInstallationCohort, UnknownFileOwnershipRefusalCannotBeClearedByLaterPathRepair) {
    cohort_area area;
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    auto space = owner.create_store_namespace("data");
    const auto file = area.path / "launch" / "data" / "physical-file";
    auto specification = cohort_echo();
    specification.arguments = {"-c", "printf seeded > " + cohort_shell_quote(file.string()) + " || exit 72; exec /bin/cat <&3 >&3"};
    const auto child = owner.launch(specification);
    owner.send(child, {2, 1}, cohort_deadline());
    EXPECT_EQ(owner.receive(child, cohort_deadline()), (transport::frame{2, 1}));
    (void)owner.close_and_join(cohort_deadline());
    const auto alias = area.path / "unexpected-link";
    std::filesystem::create_hard_link(file, alias);
    expect_cohort_error(cohort::cohort_error_code::changed, [&] { (void)space.inspect_after_join("physical-file"); });
    ASSERT_TRUE(std::filesystem::remove(alias));
    expect_cohort_error(cohort::cohort_error_code::incomplete, [&] { (void)space.inspect_after_join("physical-file"); });
}

TEST(OrdinaryInstallationCohort, ExistingDirectoryIsNotReinterpretedAsANewOwnedInstallation) {
    cohort_area area;
    ASSERT_EQ(::mkdirat(area.fd, "existing", 0700), 0);
    expect_cohort_error(cohort::cohort_error_code::unavailable, [&] {
        (void)cohort::created_launch_cohort::create_before_child(area.fd, "existing");
    });
    EXPECT_TRUE(std::filesystem::is_empty(area.path / "existing"));
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "created");
    expect_cohort_error(cohort::cohort_error_code::unavailable, [&] {
        (void)cohort::created_launch_cohort::create_before_child(area.fd, "created");
    });
    EXPECT_TRUE(owner.close_and_join(cohort_deadline()).children.empty());
}

TEST(OrdinaryInstallationCohort, UnknownFailedSpawnCannotBeRelabeledAnEmptyCompletedCohort) {
    cohort_area area;
    const auto before = cohort::created_launch_cohort::unresolved_cohorts();
    const auto native_before = transport::owned_child::unresolved_lifetimes();
    {
        auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
        auto specification = cohort_echo(); specification.executable = "/nonexistent-lattice-cohort-fixture/program";
        EXPECT_THROW(owner.launch(specification), transport::error);
        EXPECT_EQ(transport::owned_child::unresolved_lifetimes(), native_before);
        expect_cohort_error(cohort::cohort_error_code::incomplete, [&] { (void)owner.launch(cohort_echo()); });
        expect_cohort_error(cohort::cohort_error_code::incomplete, [&] { (void)owner.close_and_join(cohort_deadline()); });
    }
    // This fixture proves no native child exists, but the durable cohort layer
    // cannot promote an unreturned spawn into a successful completion receipt.
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before + 1);
}

TEST(OrdinaryInstallationCohort, ReplacedStableGateRefusesBeforeAnotherActualChildCanLaunch) {
    cohort_area area;
    const auto native_before = transport::owned_child::unresolved_lifetimes();
    auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
    const auto stable = area.path / "launch" / "launch.lock";
    std::filesystem::rename(stable, area.path / "launch" / "original.lock");
    const auto replacement = ::open(stable.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    ASSERT_GE(replacement, 0); ASSERT_EQ(::close(replacement), 0);
    expect_cohort_error(cohort::cohort_error_code::changed, [&] { (void)owner.launch(cohort_echo()); });
    EXPECT_EQ(transport::owned_child::unresolved_lifetimes(), native_before);
    EXPECT_THROW(owner.close_and_join(cohort_deadline()), cohort::cohort_error);
}

TEST(OrdinaryInstallationCohort, WrapperCleanupRetainsTheOriginalRealChannelFailure) {
    cohort_area area;
    const auto before = cohort::created_launch_cohort::unresolved_cohorts();
    transport::process_identity identity;
    std::exception_ptr primary;
    {
        auto owner = cohort::created_launch_cohort::create_before_child(area.fd, "launch");
        const auto child = owner.launch(cohort_echo()); identity = owner.child_identity(child);
        try { (void)owner.receive(child, std::chrono::steady_clock::now() + 10ms); }
        catch (...) { primary = std::current_exception(); }
    }
    ASSERT_NE(primary, nullptr);
    try { std::rethrow_exception(primary); }
    catch (const transport::error& error) { EXPECT_EQ(error.code, transport::error_code::timed_out); }
    catch (...) { ADD_FAILURE() << "Original channel failure was replaced"; }
    expect_reaped(identity);
    EXPECT_EQ(cohort::created_launch_cohort::unresolved_cohorts(), before);
}
#endif
