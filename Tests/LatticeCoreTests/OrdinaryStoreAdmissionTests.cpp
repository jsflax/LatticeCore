#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_store_admission.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <filesystem>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <thread>
#include <vector>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace {
namespace admission = lattice::detail::ordinary_admission;
admission::identifier id(std::uint8_t value) { admission::identifier result{}; result[0] = value; return result; }
admission::store_binding binding() { return {id(1), id(2), id(3), {4,5}, {4,6}}; }
admission::record sample() {
    admission::record result; result.binding = binding(); result.control = {4,7};
    result.entry = {4,8}; result.generation = {4,9}; return result;
}
void rehash(admission::encoded_record& bytes) {
    picosha2::hash256(bytes.begin(), bytes.begin() + 224, bytes.begin() + 224, bytes.end());
}
}

TEST(OrdinaryStoreAdmission, FixedCodecRoundTripHasNoActiveState) {
    auto value = sample(); auto bytes = admission::encode(value);
    EXPECT_EQ(bytes.size(), 256u);
    EXPECT_EQ(admission::decode(bytes), value);
    value.revision = 2; value.state = admission::stage::retirement_requested; value.cutover = id(10);
    EXPECT_EQ(admission::decode(admission::encode(value)), value);
}

TEST(OrdinaryStoreAdmission, CanonicalBytesMatchIndependentV1Fixture) {
    // Independently packed with Python stdlib struct/hashlib, not this codec.
    const std::string expected =
        "4c415441444d3100010000000001000001000000000000000100000000000000"
        "0100000000000000000000000000000002000000000000000000000000000000"
        "0300000000000000000000000000000000000000000000000000000000000000"
        "0400000000000000050000000000000004000000000000000600000000000000"
        "0400000000000000070000000000000004000000000000000800000000000000"
        "0400000000000000090000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "d617308e8504a73f4e2279557471dee10d2523e6a57fe1740d781ee190d6660d";
    EXPECT_EQ(picosha2::bytes_to_hex_string(admission::encode(sample())), expected);
}

TEST(OrdinaryStoreAdmission, EveryChangedEncodedByteIsRejected) {
    const auto original = admission::encode(sample());
    for (std::size_t index = 0; index < original.size(); ++index) {
        auto bytes = original; bytes[index] ^= 1;
        EXPECT_THROW(admission::decode(bytes), admission::error);
    }
}

TEST(OrdinaryStoreAdmission, RechecksVersionLengthReservedAndStateDespiteValidChecksum) {
    for (const auto index : {0u, 8u, 12u, 13u, 25u, 31u, 176u, 223u}) {
        auto bytes = admission::encode(sample()); bytes[index] ^= 1; rehash(bytes);
        EXPECT_THROW(admission::decode(bytes), admission::error);
    }
    auto bytes = admission::encode(sample()); bytes[24] = 3; rehash(bytes);
    EXPECT_THROW(admission::decode(bytes), admission::error);
}

TEST(OrdinaryStoreAdmission, RejectsEmptyIdentityInvalidCutoverAndSharedLockIdentity) {
    auto value = sample(); value.binding.epoch = {};
    EXPECT_THROW(admission::encode(value), admission::error);
    value = sample(); value.cutover = id(10);
    EXPECT_THROW(admission::encode(value), admission::error);
    value = sample(); value.state = admission::stage::retirement_requested;
    EXPECT_THROW(admission::encode(value), admission::error);
    value = sample(); value.generation = value.entry;
    EXPECT_THROW(admission::encode(value), admission::error);
    value = sample(); value.revision = 0;
    EXPECT_THROW(admission::encode(value), admission::error);
}

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
namespace {
struct descriptor {
    int fd = -1;
    explicit descriptor(int fd = -1) : fd(fd) {}
    ~descriptor() { if (fd >= 0) ::close(fd); }
    descriptor(const descriptor&) = delete;
};
struct area {
    std::filesystem::path path;
    int fd = -1;
    area() {
        const auto pattern = (std::filesystem::temp_directory_path() / "lattice-admission-XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end()); name.push_back(0);
        if (!::mkdtemp(name.data())) throw std::runtime_error("admission test directory creation failed");
        path = name.data(); fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("admission test directory open failed");
    }
    ~area() { if (fd >= 0) ::close(fd); std::error_code ignored; std::filesystem::remove_all(path, ignored); }
};
thread_local admission::test_hooks::boundary selected_fault;
void inject(admission::test_hooks::boundary point) {
    if (point == selected_fault) throw std::runtime_error("injected journal fault");
}
struct fault_scope {
    admission::test_hooks::fault previous = admission::test_hooks::current;
    explicit fault_scope(admission::test_hooks::boundary point) {
        selected_fault = point; admission::test_hooks::current = inject;
    }
    ~fault_scope() { admission::test_hooks::current = previous; }
};
void overwrite(int dir, const admission::record& value) {
    descriptor output(::openat(dir, "admission.v1", O_WRONLY | O_TRUNC | O_CLOEXEC));
    const auto bytes = admission::encode(value);
    ASSERT_GE(output.fd, 0);
    ASSERT_EQ(::write(output.fd, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
    ASSERT_EQ(::fsync(output.fd), 0);
}
struct child_owner {
    pid_t pid = -1;
    bool group = false;
    explicit child_owner(pid_t value, bool own_group = false) : pid(value), group(own_group) {}
    ~child_owner() {
        if (pid <= 0) return;
        const auto owned = pid;
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        // WNOWAIT checks actual child custody without releasing the leader's
        // identity. Every attempt, including EINTR retries, uses this budget.
        siginfo_t info{};
        int observed;
        do {
            if (std::chrono::steady_clock::now() >= end) {
                ADD_FAILURE() << "admission child cleanup custody observation deadline";
                return;
            }
            observed = ::waitid(P_PID, static_cast<id_t>(owned), &info, WEXITED | WNOHANG | WNOWAIT);
        } while (observed < 0 && errno == EINTR);
        if (observed != 0 || (info.si_pid != 0 && info.si_pid != owned)) {
            pid = -1; // Lost custody: never signal a reusable numeric identity.
            ADD_FAILURE() << "admission child cleanup custody unproved";
            return;
        }
        if (std::chrono::steady_clock::now() >= end) {
            ADD_FAILURE() << "admission child cleanup deadline before termination";
            return;
        }
        // Signal the group only while its unreaped leader remains our child;
        // never signal a remembered numeric PGID after releasing that custody.
        if (::kill(group ? -owned : owned, SIGKILL) < 0 && errno != ESRCH)
            ADD_FAILURE() << "admission child cleanup termination failed";
        bool joined = false;
        while (std::chrono::steady_clock::now() < end) {
            int status = 0;
            const auto waited = ::waitpid(owned, &status, WNOHANG);
            if (waited == owned) { pid = -1; joined = true; break; }
            if (waited < 0 && errno != EINTR) {
                pid = -1;
                ADD_FAILURE() << "admission child cleanup lost leader custody";
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        if (!joined) {
            ADD_FAILURE() << "admission child cleanup deadline: leader completion unproved";
            return;
        }
        if (group) {
            bool gone = false;
            // Observation only after reaping; no later signal is authorized.
            while (std::chrono::steady_clock::now() < end) {
                if (::kill(-owned, 0) < 0 && errno == ESRCH) { gone = true; break; }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (!gone) ADD_FAILURE() << "admission child group completion unproved";
        }
    }
    int join() {
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(group ? 30 : 10);
        while (std::chrono::steady_clock::now() < end) {
            int status = 0; const auto result = ::waitpid(pid, &status, WNOHANG);
            if (result == pid) { pid = -1; return status; }
            if (result < 0 && errno != EINTR) {
                pid = -1; // Lost custody: report failure, never signal an unverifiable PID.
                return -1;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return -1; // Destructor still attempts bounded cleanup after this deadline.
    }
};
bool wait_file(const std::filesystem::path& path) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < end) {
        if (std::filesystem::exists(path)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}
void signal_file(int dir, const char* name) {
    descriptor marker(::openat(dir, name, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600));
    if (marker.fd < 0) throw std::runtime_error("admission test signal creation failed");
}
pid_t start_helper(const area& owned, const std::string& mode) {
    const auto args = ::testing::internal::GetArgvs();
    if (args.empty()) throw std::runtime_error("admission helper executable unavailable");
    std::string executable = std::filesystem::absolute(args.front()).string();
    std::string filter = "--gtest_filter=OrdinaryStoreAdmission.ProcessHelper";
    std::string repeat = "--gtest_repeat=1", color = "--gtest_color=no", output = "--gtest_output=";
    char* argv[] = {executable.data(), filter.data(), repeat.data(), color.data(), output.data(), nullptr};
    std::vector<std::string> environment;
    for (char** item = environ; *item; ++item) {
        const std::string value(*item);
        if (!value.starts_with("LATTICE_ORDINARY_JOURNAL_HELPER_") &&
            !value.starts_with("LATTICE_TEST_LOG_PATH=")) environment.push_back(value);
    }
    environment.push_back("LATTICE_ORDINARY_JOURNAL_HELPER_MODE=" + mode);
    environment.push_back("LATTICE_ORDINARY_JOURNAL_HELPER_DIRECTORY=" + owned.path.string());
    const char* parent_log = std::getenv("LATTICE_TEST_LOG_PATH");
    const std::string native = parent_log && *parent_log ?
        std::string(parent_log) + "." + owned.path.filename().string() + ".ordinary-" + mode + ".native.log" :
        owned.path.string() + ".ordinary-" + mode + ".native.log";
    // Native and terminal logs survive disposable control-area cleanup. The
    // hosted parent config places this evidence under its owned localdev root.
    descriptor reserved(::open(native.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
    if (reserved.fd < 0) throw std::runtime_error("admission helper native log collision");
    environment.push_back("LATTICE_TEST_LOG_PATH=" + native);
    std::vector<char*> env; for (auto& value : environment) env.push_back(value.data()); env.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (::posix_spawn_file_actions_init(&actions)) throw std::runtime_error("admission helper actions unavailable");
    struct actions_owner { posix_spawn_file_actions_t& value; ~actions_owner() { ::posix_spawn_file_actions_destroy(&value); } } own_actions{actions};
    const std::string terminal = native + ".terminal.log";
    if (::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, terminal.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600) ||
        ::posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO))
        throw std::runtime_error("admission helper log actions unavailable");
    posix_spawnattr_t attributes;
    if (::posix_spawnattr_init(&attributes)) throw std::runtime_error("admission helper attributes unavailable");
    struct attributes_owner { posix_spawnattr_t& value; ~attributes_owner() { ::posix_spawnattr_destroy(&value); } } own_attributes{attributes};
    if (::posix_spawnattr_setpgroup(&attributes, 0) || ::posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP))
        throw std::runtime_error("admission helper owned group unavailable");
    pid_t child = -1;
    if (::posix_spawn(&child, executable.c_str(), &actions, &attributes, argv, env.data()))
        throw std::runtime_error("admission helper spawn failed");
    return child;
}
}

TEST(OrdinaryStoreAdmission, ExclusiveBootstrapAndExactReopenPreserveObservedBinding) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    const auto original = first.read();
    EXPECT_EQ(original.binding, binding());
    EXPECT_EQ(original.state, admission::stage::unadopted);
    EXPECT_EQ(admission::journal::open_existing(owned.fd, binding()).read(), original);
    EXPECT_THROW(admission::journal::create_unadopted(owned.fd, binding()), admission::error);
    auto changed = binding(); changed.epoch = id(99);
    EXPECT_THROW(admission::journal::open_existing(owned.fd, changed), admission::error);
}

TEST(OrdinaryStoreAdmission, JournalMoveRetainsItsOwnDuplicatedDirectoryCustody) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    const auto original = first.read();
    auto moved = std::move(first);
    EXPECT_THROW(first.read(), admission::error);
    ASSERT_EQ(::close(owned.fd), 0); owned.fd = -1;
    EXPECT_EQ(moved.read(), original);
    EXPECT_EQ(moved.begin_retirement(id(19)).state, admission::stage::retirement_requested);
}

TEST(OrdinaryStoreAdmission, HoldMoveAssignmentReleasesOnlyDisplacedGeneration) {
    area left_area, right_area;
    auto left = admission::journal::create_unadopted(left_area.fd, binding());
    auto right = admission::journal::create_unadopted(right_area.fd, binding());
    auto displaced = left.try_hold_generation(); auto incoming = right.try_hold_generation();
    EXPECT_TRUE(left.generation_busy()); EXPECT_TRUE(right.generation_busy());
    displaced = std::move(incoming);
    EXPECT_FALSE(static_cast<bool>(incoming));
    EXPECT_FALSE(left.generation_busy()); EXPECT_TRUE(right.generation_busy());
    displaced = {}; EXPECT_FALSE(right.generation_busy());
}

TEST(OrdinaryStoreAdmission, RetirementClosesNewHoldsButRetainsActualOldHold) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    auto other = admission::journal::open_existing(owned.fd, binding());
    auto hold = first.try_hold_generation();
    EXPECT_TRUE(other.generation_busy());
    const auto retired = other.begin_retirement(id(11));
    EXPECT_EQ(retired.state, admission::stage::retirement_requested);
    EXPECT_TRUE(first.generation_busy());
    EXPECT_THROW(first.try_hold_generation(), admission::error);
    hold = {};
    EXPECT_FALSE(other.generation_busy()); // OS contention only; no adoption API exists.
    EXPECT_EQ(other.read().state, admission::stage::retirement_requested);
}

TEST(OrdinaryStoreAdmission, SameCutoverRetryIsIdempotentAndDifferentCutoverCannotReplaceIt) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    const auto retired = first.begin_retirement(id(12));
    EXPECT_EQ(retired.revision, 2u);
    auto reopened = admission::journal::open_existing(owned.fd, binding());
    EXPECT_EQ(reopened.begin_retirement(id(12)), retired);
    EXPECT_THROW(reopened.begin_retirement(id(13)), admission::error);
    EXPECT_THROW(reopened.begin_retirement({}), admission::error);
    EXPECT_EQ(first.read(), retired);
}

TEST(OrdinaryStoreAdmission, RevisionExhaustionDoesNotWriteOrWrap) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    auto last = first.read(); last.revision = std::numeric_limits<std::uint64_t>::max(); overwrite(owned.fd, last);
    EXPECT_THROW(first.begin_retirement(id(14)), admission::error);
    EXPECT_EQ(first.read(), last);
    EXPECT_FALSE(std::filesystem::exists(owned.path / "admission.pending"));
}

TEST(OrdinaryStoreAdmission, MissingMalformedAndTrailingSnapshotsNeverBecomeEmpty) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    descriptor output(::openat(owned.fd, "admission.v1", O_WRONLY | O_APPEND | O_CLOEXEC));
    ASSERT_GE(output.fd, 0); ASSERT_EQ(::write(output.fd, "x", 1), 1);
    EXPECT_THROW(first.read(), admission::error);
    ASSERT_EQ(::unlinkat(owned.fd, "admission.v1", 0), 0);
    EXPECT_THROW(first.try_hold_generation(), admission::error);
    EXPECT_THROW(admission::journal::create_unadopted(owned.fd, binding()), admission::error);
}

TEST(OrdinaryStoreAdmission, ControlObjectReplacementAndHardlinkAreRefused) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    auto hold = first.try_hold_generation();
    ASSERT_EQ(::renameat(owned.fd, "generation.lock", owned.fd, "old-generation.lock"), 0);
    descriptor replacement(::openat(owned.fd, "generation.lock", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
    ASSERT_GE(replacement.fd, 0);
    EXPECT_THROW(first.read(), admission::error);
    ASSERT_EQ(::unlinkat(owned.fd, "generation.lock", 0), 0);
    ASSERT_EQ(::renameat(owned.fd, "old-generation.lock", owned.fd, "generation.lock"), 0);
    ASSERT_EQ(::linkat(owned.fd, "generation.lock", owned.fd, "alias.lock", 0), 0);
    EXPECT_THROW(first.read(), admission::error);
}

TEST(OrdinaryStoreAdmission, ControlPermissionsAndSymlinksAreRefused) {
    area owned;
    ASSERT_EQ(::fchmod(owned.fd, 0755), 0);
    EXPECT_THROW(admission::journal::create_unadopted(owned.fd, binding()), admission::error);
    ASSERT_EQ(::fchmod(owned.fd, 0700), 0);
    auto first = admission::journal::create_unadopted(owned.fd, binding());
    ASSERT_EQ(::fchmodat(owned.fd, "entry.lock", 0644, 0), 0);
    EXPECT_THROW(first.read(), admission::error);
    ASSERT_EQ(::fchmodat(owned.fd, "entry.lock", 0600, 0), 0);
    ASSERT_EQ(::renameat(owned.fd, "entry.lock", owned.fd, "real-entry.lock"), 0);
    ASSERT_EQ(::symlinkat("real-entry.lock", owned.fd, "entry.lock"), 0);
    EXPECT_THROW(first.read(), admission::error);
}

TEST(OrdinaryStoreAdmission, CoherentSnapshotAndLockReplacementCannotRebindRetainedJournal) {
    for (const bool replace_entry : {false, true}) {
        area owned; auto retained = admission::journal::create_unadopted(owned.fd, binding());
        auto old_hold = retained.try_hold_generation();
        auto replacement_record = retained.read();
        ASSERT_EQ(::renameat(owned.fd, "generation.lock", owned.fd, "old-generation.lock"), 0);
        descriptor replacement_generation(::openat(owned.fd, "generation.lock", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        ASSERT_GE(replacement_generation.fd, 0);
        struct stat st{}; ASSERT_EQ(::fstat(replacement_generation.fd, &st), 0);
        replacement_record.generation = {static_cast<std::uint64_t>(st.st_dev), static_cast<std::uint64_t>(st.st_ino)};
        if (replace_entry) {
            ASSERT_EQ(::renameat(owned.fd, "entry.lock", owned.fd, "old-entry.lock"), 0);
            descriptor replacement_entry(::openat(owned.fd, "entry.lock", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
            ASSERT_GE(replacement_entry.fd, 0); ASSERT_EQ(::fstat(replacement_entry.fd, &st), 0);
            replacement_record.entry = {static_cast<std::uint64_t>(st.st_dev), static_cast<std::uint64_t>(st.st_ino)};
        }
        ASSERT_EQ(::renameat(owned.fd, "admission.v1", owned.fd, "old-admission.v1"), 0);
        descriptor snapshot(::openat(owned.fd, "admission.v1", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        ASSERT_GE(snapshot.fd, 0); overwrite(owned.fd, replacement_record);
        EXPECT_THROW(retained.read(), admission::error);
        EXPECT_THROW(retained.try_hold_generation(), admission::error);
        EXPECT_THROW(retained.begin_retirement(id(20)), admission::error);
        descriptor old_generation(::openat(owned.fd, "old-generation.lock", O_RDWR | O_CLOEXEC));
        ASSERT_GE(old_generation.fd, 0);
        ASSERT_EQ(::flock(old_generation.fd, LOCK_EX | LOCK_NB), -1);
        EXPECT_TRUE(errno == EWOULDBLOCK || errno == EAGAIN);
        // A fresh observation lacks external installation identity authority.
        // Its OS probe can be quiet despite the old retained real hold; neither
        // fresh reopen nor a quiet probe is ever an adoption capability.
        auto freshly_observed = admission::journal::open_existing(owned.fd, binding());
        EXPECT_FALSE(freshly_observed.generation_busy());
        old_hold = {};
        EXPECT_EQ(::flock(old_generation.fd, LOCK_EX | LOCK_NB), 0);
    }
}

TEST(OrdinaryStoreAdmission, AnchoredFreshOpenPreservesExactControlsAndRetirementIntent) {
    area owned;
    admission::record original;
    {
        auto first = admission::journal::create_unadopted(owned.fd, binding());
        original = first.read();
    }
    const admission::control_binding expected{original.control, original.entry, original.generation};
    auto reopened = admission::journal::open_existing(owned.fd, binding(), expected);
    EXPECT_EQ(reopened.read(), original);
    auto hold = reopened.try_hold_generation();
    EXPECT_TRUE(reopened.generation_busy());
    const auto retirement = reopened.begin_retirement(id(21));
    auto after_intent = admission::journal::open_existing(owned.fd, binding(), expected);
    EXPECT_EQ(after_intent.read(), retirement);
    EXPECT_EQ(after_intent.begin_retirement(id(21)), retirement);
    EXPECT_THROW(after_intent.try_hold_generation(), admission::error);
    EXPECT_TRUE(after_intent.generation_busy());
    hold = {};
    EXPECT_FALSE(after_intent.generation_busy());
}

TEST(OrdinaryStoreAdmission, AnchoredFreshOpenRefusesMissingWrongAndAliasedControls) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    const auto original = first.read();
    const admission::control_binding expected{original.control, original.entry, original.generation};
    for (unsigned index = 0; index < 3; ++index) {
        auto wrong = expected;
        auto* changed = index == 0 ? &wrong.control : index == 1 ? &wrong.entry : &wrong.generation;
        const auto original_identity = *changed;
        changed->inode = 0;
        EXPECT_THROW(admission::journal::open_existing(owned.fd, binding(), wrong), admission::error);
        *changed = {original_identity.device, original_identity.inode == 1 ? 2u : 1u};
        EXPECT_THROW(admission::journal::open_existing(owned.fd, binding(), wrong), admission::error);
        EXPECT_EQ(first.read(), original);
    }
    auto aliased = expected; aliased.generation = aliased.entry;
    EXPECT_THROW(admission::journal::open_existing(owned.fd, binding(), aliased), admission::error);
    area other; auto other_journal = admission::journal::create_unadopted(other.fd, binding());
    EXPECT_THROW(admission::journal::open_existing(other.fd, binding(), expected), admission::error);
    EXPECT_EQ(first.read(), original);
    EXPECT_FALSE(std::filesystem::exists(owned.path / "admission.pending"));
    EXPECT_FALSE(other_journal.generation_busy());
}

TEST(OrdinaryStoreAdmission, ExternalControlExpectationRejectsCoherentReplacementAfterWrapperGone) {
    area owned; admission::record original; admission::generation_hold old_hold;
    {
        auto first = admission::journal::create_unadopted(owned.fd, binding());
        original = first.read(); old_hold = first.try_hold_generation();
    }
    // This expectation survives independently of every journal wrapper. The
    // installation adapter must retain/persist it outside admission.v1.
    const admission::control_binding expected{original.control, original.entry, original.generation};
    auto replacement = original;
    ASSERT_EQ(::renameat(owned.fd, "entry.lock", owned.fd, "old-entry.lock"), 0);
    ASSERT_EQ(::renameat(owned.fd, "generation.lock", owned.fd, "old-generation.lock"), 0);
    descriptor new_entry(::openat(owned.fd, "entry.lock", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
    descriptor new_generation(::openat(owned.fd, "generation.lock", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
    ASSERT_GE(new_entry.fd, 0); ASSERT_GE(new_generation.fd, 0);
    struct stat st{}; ASSERT_EQ(::fstat(new_entry.fd, &st), 0);
    replacement.entry = {static_cast<std::uint64_t>(st.st_dev), static_cast<std::uint64_t>(st.st_ino)};
    ASSERT_EQ(::fstat(new_generation.fd, &st), 0);
    replacement.generation = {static_cast<std::uint64_t>(st.st_dev), static_cast<std::uint64_t>(st.st_ino)};
    ASSERT_EQ(::renameat(owned.fd, "admission.v1", owned.fd, "old-admission.v1"), 0);
    descriptor snapshot(::openat(owned.fd, "admission.v1", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
    ASSERT_GE(snapshot.fd, 0); overwrite(owned.fd, replacement);
    EXPECT_THROW(admission::journal::open_existing(owned.fd, binding(), expected), admission::error);
    descriptor old_generation(::openat(owned.fd, "old-generation.lock", O_RDWR | O_CLOEXEC));
    ASSERT_GE(old_generation.fd, 0);
    ASSERT_EQ(::flock(old_generation.fd, LOCK_EX | LOCK_NB), -1);
    EXPECT_TRUE(errno == EWOULDBLOCK || errno == EAGAIN);
    // The old observational overload deliberately remains weaker; a quiet
    // new lock cannot erase the real retained hold on the old generation.
    auto observed = admission::journal::open_existing(owned.fd, binding());
    EXPECT_EQ(observed.read(), replacement);
    EXPECT_FALSE(observed.generation_busy());
    old_hold = {};
    EXPECT_EQ(::flock(old_generation.fd, LOCK_EX | LOCK_NB), 0);
}

TEST(OrdinaryStoreAdmission, InterruptedPreRenamePublicationKeepsGateClosedAndOriginalBytes) {
    for (const auto point : {admission::test_hooks::boundary::before_write,
                             admission::test_hooks::boundary::after_write,
                             admission::test_hooks::boundary::after_file_sync}) {
        area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
        const auto original = first.read();
        { fault_scope fault(point); EXPECT_THROW(first.begin_retirement(id(15)), std::runtime_error); }
        EXPECT_TRUE(std::filesystem::exists(owned.path / "admission.pending"));
        EXPECT_THROW(first.try_hold_generation(), admission::error);
        EXPECT_THROW(first.begin_retirement(id(15)), admission::error);
        EXPECT_THROW(admission::journal::open_existing(owned.fd, binding()), admission::error);
        descriptor original_file(::openat(owned.fd, "admission.v1", O_RDONLY | O_CLOEXEC));
        admission::encoded_record bytes{};
        ASSERT_EQ(::read(original_file.fd, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
        EXPECT_EQ(admission::decode(bytes), original);
    }
}

TEST(OrdinaryStoreAdmission, LostPostRenameReplyReopensSameIntentAndCannotAdmitNewHold) {
    for (const auto point : {admission::test_hooks::boundary::after_rename,
                             admission::test_hooks::boundary::after_directory_sync}) {
        area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
        { fault_scope fault(point); EXPECT_THROW(first.begin_retirement(id(16)), std::runtime_error); }
        auto reopened = admission::journal::open_existing(owned.fd, binding());
        const auto value = reopened.begin_retirement(id(16));
        EXPECT_EQ(value.revision, 2u); EXPECT_EQ(value.cutover, id(16));
        EXPECT_THROW(reopened.try_hold_generation(), admission::error);
    }
}

TEST(OrdinaryStoreAdmission, PartialBootstrapIsNotReusedAsFreshInstallation) {
    area owned;
    { fault_scope fault(admission::test_hooks::boundary::after_write);
      EXPECT_THROW(admission::journal::create_unadopted(owned.fd, binding()), std::runtime_error); }
    EXPECT_THROW(admission::journal::create_unadopted(owned.fd, binding()), admission::error);
    EXPECT_THROW(admission::journal::open_existing(owned.fd, binding()), admission::error);
    EXPECT_TRUE(std::filesystem::exists(owned.path / "admission.pending"));
}

TEST(OrdinaryStoreAdmission, EntryContentionIsImmediateAndDoesNotMutateIntent) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    descriptor held(::openat(owned.fd, "entry.lock", O_RDWR | O_CLOEXEC));
    ASSERT_GE(held.fd, 0); ASSERT_EQ(::flock(held.fd, LOCK_EX | LOCK_NB), 0);
    EXPECT_THROW(first.begin_retirement(id(17)), admission::error);
    EXPECT_THROW(first.try_hold_generation(), admission::error);
    EXPECT_FALSE(std::filesystem::exists(owned.path / "admission.pending"));
}

TEST(OrdinaryStoreAdmission, ForkedUseRefusesAndChildCloseDoesNotUnlockParentHold) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    child_owner child(start_helper(owned, "fork-inheritance"), true);
    // The actual helper writes this only after its own child is joined and all
    // inheritance assertions passed. Do not reap its leader before that proof.
    ASSERT_TRUE(wait_file(owned.path / "fork-joined.signal"));
    const auto status = child.join();
    ASSERT_GE(status, 0);
    ASSERT_TRUE(WIFEXITED(status)); ASSERT_EQ(WEXITSTATUS(status), 0);
    EXPECT_FALSE(first.generation_busy());
}

TEST(OrdinaryStoreAdmission, ChildOwnsIndependentHoldUntilActualJoinedCompletion) {
    area owned; auto first = admission::journal::create_unadopted(owned.fd, binding());
    child_owner child(start_helper(owned, "hold"), true);
    ASSERT_TRUE(wait_file(owned.path / "held.signal"));
    EXPECT_TRUE(first.generation_busy());
    EXPECT_EQ(first.begin_retirement(id(18)).cutover, id(18));
    EXPECT_TRUE(first.generation_busy());
    EXPECT_THROW(first.try_hold_generation(), admission::error);
    signal_file(owned.fd, "release.signal");
    const auto status = child.join(); ASSERT_GE(status, 0);
    ASSERT_TRUE(WIFEXITED(status)); ASSERT_EQ(WEXITSTATUS(status), 0);
    EXPECT_FALSE(first.generation_busy());
    EXPECT_EQ(first.read().state, admission::stage::retirement_requested);
}

TEST(OrdinaryStoreAdmission, ProcessHelper) {
    const char* mode = std::getenv("LATTICE_ORDINARY_JOURNAL_HELPER_MODE");
    const char* path = std::getenv("LATTICE_ORDINARY_JOURNAL_HELPER_DIRECTORY");
    if (!mode || !path) GTEST_SKIP() << "Requires the real parent-owned process fixture";
    descriptor directory(::open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    ASSERT_GE(directory.fd, 0);
    auto owner = admission::journal::open_existing(directory.fd, binding());
    if (std::string(mode) == "hold") {
        auto hold = owner.try_hold_generation();
        signal_file(directory.fd, "held.signal");
        ASSERT_TRUE(wait_file(std::filesystem::path(path) / "release.signal"));
        ::_exit(0); // Do not run lease destructors: actual process exit closes it.
    }
    ASSERT_EQ(std::string(mode), "fork-inheritance");
    // This fresh, helper-only executable has started no Lattice workers.
    // The suite process is never forked with its unrelated thread history.
    auto hold = owner.try_hold_generation();
    const auto pid = ::fork(); ASSERT_GE(pid, 0);
    if (pid == 0) {
        bool refused = false;
        try { (void)owner.read(); } catch (const admission::error& e) { refused = e.code == admission::error_code::inherited_use; }
        hold = {};
        ::_exit(refused ? 0 : 2);
    }
    child_owner child(pid); const auto status = child.join(); ASSERT_GE(status, 0);
    ASSERT_TRUE(WIFEXITED(status)); ASSERT_EQ(WEXITSTATUS(status), 0);
    EXPECT_TRUE(owner.generation_busy());
    hold = {}; EXPECT_FALSE(owner.generation_busy());
    if (!::testing::Test::HasFailure()) signal_file(directory.fd, "fork-joined.signal");
}
#endif

// These cases exercise stored canonical facts and actual OS lease exclusion.
// They do not invoke the private normal-origin issuer or issue an open context.
TEST(OrdinaryStoreAdmission, NewAdmittedStateUsesOnlyExactNonceReservedArea) {
    auto value=sample();value.revision=2;value.state=admission::stage::ordinary_admitted;value.normal_launch=id(10);
    const auto bytes=admission::encode(value);
    EXPECT_EQ(admission::decode(bytes),value);
    for(std::size_t i=0;i!=value.normal_launch.size();++i)EXPECT_EQ(bytes[176+i],value.normal_launch[i]);
    for(std::size_t i=192;i!=224;++i)EXPECT_EQ(bytes[i],0);
    EXPECT_EQ(value.cutover,admission::identifier{});
    // Independently packed from the original 256-byte v1 envelope and SHA256.
    const std::string expected="4c415441444d31000100000000010000020000000000000003000000000000000100000000000000000000000000000002000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000000004000000000000000500000000000000040000000000000006000000000000000400000000000000070000000000000004000000000000000800000000000000040000000000000009000000000000000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008fb985d125851a2b1649643787a4f18cd24c18dff7e7fc4ca9dccf0e40915e1f";
    EXPECT_EQ(picosha2::bytes_to_hex_string(bytes),expected);
}

TEST(OrdinaryStoreAdmission, OldStatesStillRejectEveryNonceReservedByte) {
    for(const bool retired:{false,true}) {
        auto value=sample();if(retired){value.state=admission::stage::retirement_requested;value.revision=2;value.cutover=id(10);}
        const auto original=admission::encode(value);
        for(std::size_t offset=176;offset!=224;++offset){auto bytes=original;bytes[offset]=1;rehash(bytes);EXPECT_THROW(admission::decode(bytes),admission::error);}
        value.normal_launch=id(11);EXPECT_THROW(admission::encode(value),admission::error);
    }
}

TEST(OrdinaryStoreAdmission, AdmittedStateRejectsZeroNonceCutoverOldRevisionAndOtherReservedBytes) {
    auto value=sample();value.revision=2;value.state=admission::stage::ordinary_admitted;
    EXPECT_THROW(admission::encode(value),admission::error);
    value.normal_launch=id(10);value.cutover=id(11);EXPECT_THROW(admission::encode(value),admission::error);
    value.cutover={};value.revision=1;EXPECT_THROW(admission::encode(value),admission::error);
    value.revision=2;const auto original=admission::encode(value);
    for(const auto offset:{9u,10u,11u,14u,15u,25u,31u,192u,223u}){auto bytes=original;bytes[offset]^=1;rehash(bytes);EXPECT_THROW(admission::decode(bytes),admission::error);}
    auto cleared=original;std::fill(cleared.begin()+176,cleared.begin()+192,0);rehash(cleared);EXPECT_THROW(admission::decode(cleared),admission::error);
}

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
namespace {
admission::record stored_normal_fact(const admission::record& original) {
    auto value=original;value.revision=2;value.state=admission::stage::ordinary_admitted;value.normal_launch=id(30);return value;
}
admission::control_binding controls(const admission::record& original){return {original.control,original.entry,original.generation};}
}

TEST(OrdinaryStoreAdmission, StoredAdmittedFactsCannotGrantObservationalOrBootstrapHold) {
    area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
    const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
    auto observational=admission::journal::open_existing(owned.fd,binding());
    EXPECT_EQ(observational.read(),fact);
    EXPECT_THROW(bootstrap.hold_ordinary(fact),admission::error);
    EXPECT_THROW(observational.hold_ordinary(fact),admission::error);
    EXPECT_THROW(observational.try_hold_generation(),admission::error);
    EXPECT_FALSE(observational.generation_busy());
    EXPECT_EQ(observational.read(),fact);EXPECT_FALSE(std::filesystem::exists(owned.path/"admission.pending"));
}

TEST(OrdinaryStoreAdmission, AnchoredStoredFactHoldRetainsActualGenerationAndExactExpectedRecord) {
    area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
    const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
    auto anchored=admission::journal::open_existing(owned.fd,binding(),controls(original));
    auto hold=anchored.hold_ordinary(fact);EXPECT_TRUE(static_cast<bool>(hold));EXPECT_TRUE(anchored.generation_busy());
    descriptor generation(::openat(owned.fd,"generation.lock",O_RDWR|O_NOFOLLOW|O_CLOEXEC));ASSERT_GE(generation.fd,0);
    EXPECT_EQ(::flock(generation.fd,LOCK_EX|LOCK_NB),-1);EXPECT_TRUE(errno==EWOULDBLOCK||errno==EAGAIN);
    for(const unsigned change:{0u,1u,2u,3u}) {
        auto stale=fact;if(change==0)++stale.revision;else if(change==1)stale.normal_launch=id(31);else if(change==2)stale.binding.main.inode+=1;else stale.generation.inode+=1;
        EXPECT_THROW(anchored.hold_ordinary(stale),admission::error);EXPECT_EQ(anchored.read(),fact);
    }
    EXPECT_THROW(anchored.hold_ordinary(original),admission::error);
    EXPECT_FALSE(std::filesystem::exists(owned.path/"admission.pending"));
    hold={};EXPECT_FALSE(anchored.generation_busy());EXPECT_EQ(::flock(generation.fd,LOCK_EX|LOCK_NB),0);
}

TEST(OrdinaryStoreAdmission, RetirementFromStoredAdmittedFactPreservesOldCodecAndExistingRealHold) {
    area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
    const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
    auto anchored=admission::journal::open_existing(owned.fd,binding(),controls(original));auto hold=anchored.hold_ordinary(fact);
    const auto retired=anchored.begin_retirement(id(32));EXPECT_EQ(retired.state,admission::stage::retirement_requested);
    EXPECT_EQ(retired.revision,3u);EXPECT_EQ(retired.cutover,id(32));EXPECT_EQ(retired.normal_launch,admission::identifier{});
    const auto bytes=admission::encode(retired);for(std::size_t i=176;i!=224;++i)EXPECT_EQ(bytes[i],0);
    EXPECT_EQ(admission::decode(bytes),retired);EXPECT_EQ(anchored.begin_retirement(id(32)),retired);
    EXPECT_THROW(anchored.begin_retirement(id(33)),admission::error);
    EXPECT_THROW(anchored.hold_ordinary(fact),admission::error);
    EXPECT_THROW(anchored.hold_ordinary(retired),admission::error);
    EXPECT_THROW(anchored.try_hold_generation(),admission::error);EXPECT_TRUE(anchored.generation_busy());
    hold={};EXPECT_FALSE(anchored.generation_busy());EXPECT_EQ(anchored.read(),retired);
}

TEST(OrdinaryStoreAdmission, AnchoredStoredFactHoldRefusesRealEntryContentionWithoutPublication) {
    area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
    const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
    auto anchored=admission::journal::open_existing(owned.fd,binding(),controls(original));
    descriptor entry(::openat(owned.fd,"entry.lock",O_RDWR|O_NOFOLLOW|O_CLOEXEC));ASSERT_GE(entry.fd,0);ASSERT_EQ(::flock(entry.fd,LOCK_EX|LOCK_NB),0);
    EXPECT_THROW(anchored.hold_ordinary(fact),admission::error);EXPECT_FALSE(std::filesystem::exists(owned.path/"admission.pending"));
    ASSERT_EQ(::close(entry.fd),0);entry.fd=-1;EXPECT_EQ(anchored.read(),fact);EXPECT_FALSE(anchored.generation_busy());
}

TEST(OrdinaryStoreAdmission, AnchoredStoredFactHoldCannotRelearnCoherentGenerationReplacement) {
    area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
    const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
    auto anchored=admission::journal::open_existing(owned.fd,binding(),controls(original));auto retained=anchored.hold_ordinary(fact);
    ASSERT_EQ(::renameat(owned.fd,"generation.lock",owned.fd,"old-normal-generation.lock"),0);
    descriptor replacement(::openat(owned.fd,"generation.lock",O_CREAT|O_EXCL|O_RDWR|O_NOFOLLOW|O_CLOEXEC,0600));ASSERT_GE(replacement.fd,0);
    struct stat current{};ASSERT_EQ(::fstat(replacement.fd,&current),0);auto changed=fact;
    changed.generation={static_cast<std::uint64_t>(current.st_dev),static_cast<std::uint64_t>(current.st_ino)};overwrite(owned.fd,changed);
    EXPECT_THROW(anchored.hold_ordinary(changed),admission::error);
    EXPECT_THROW(anchored.hold_ordinary(fact),admission::error);
    EXPECT_THROW(admission::journal::open_existing(owned.fd,binding(),controls(original)),admission::error);
    descriptor old_generation(::openat(owned.fd,"old-normal-generation.lock",O_RDWR|O_NOFOLLOW|O_CLOEXEC));ASSERT_GE(old_generation.fd,0);
    EXPECT_EQ(::flock(old_generation.fd,LOCK_EX|LOCK_NB),-1);EXPECT_TRUE(errno==EWOULDBLOCK||errno==EAGAIN);
    retained={};EXPECT_EQ(::flock(old_generation.fd,LOCK_EX|LOCK_NB),0);
}

TEST(OrdinaryStoreAdmission, PostRenameRetirementFailureNeverRestoresAdmittedHold) {
    for(const auto point:{admission::test_hooks::boundary::after_rename,admission::test_hooks::boundary::after_directory_sync}) {
        area owned;auto bootstrap=admission::journal::create_unadopted(owned.fd,binding());const auto original=bootstrap.read();
        const auto fact=stored_normal_fact(original);overwrite(owned.fd,fact);
        auto anchored=admission::journal::open_existing(owned.fd,binding(),controls(original));auto retained=anchored.hold_ordinary(fact);
        {fault_scope fault(point);EXPECT_THROW(anchored.begin_retirement(id(34)),std::runtime_error);}
        auto reopened=admission::journal::open_existing(owned.fd,binding(),controls(original));
        const auto retired=reopened.begin_retirement(id(34));EXPECT_EQ(retired.revision,3u);EXPECT_EQ(retired.normal_launch,admission::identifier{});
        EXPECT_THROW(reopened.hold_ordinary(fact),admission::error);EXPECT_TRUE(reopened.generation_busy());
        retained={};EXPECT_FALSE(reopened.generation_busy());
    }
}
#endif
