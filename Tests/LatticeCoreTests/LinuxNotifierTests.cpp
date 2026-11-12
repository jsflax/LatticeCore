#if defined(__linux__) || defined(__ANDROID__)

#include <gtest/gtest.h>
#include "lattice/cross_process_notifier.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <vector>

#if defined(__linux__) && !defined(__ANDROID__)
#include <LatticeCore.hpp>
#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <sys/inotify.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

extern char** environ;
struct NotifierResourceRow { int64_t sequence = 0; };
LATTICE_SCHEMA(NotifierResourceRow, sequence);
#endif

namespace {

class LinuxNotifier : public ::testing::Test {
protected:
    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() /
                        "lattice-notifier-XXXXXX").string();
        std::vector<char> bytes(pattern.begin(), pattern.end());
        bytes.push_back('\0');
        auto* directory = ::mkdtemp(bytes.data());
        ASSERT_NE(directory, nullptr);
        directory_ = directory;
        auto path = (directory_ / "database").string();
        listener_ = lattice::make_cross_process_notifier(path);
        sender_ = lattice::make_cross_process_notifier(path);
        ASSERT_NE(listener_, nullptr);
        ASSERT_NE(sender_, nullptr);
    }

    void TearDown() override {
        if (listener_) listener_->stop_listening();
        sender_.reset();
        listener_.reset();
        if (!directory_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(directory_, error);
            EXPECT_FALSE(error) << error.message();
        }
    }

    void start() {
        listener_->start_listening([this] {
            std::lock_guard<std::mutex> lock(mutex_);
            ++notifications_;
            changed_.notify_all();
        });
        ASSERT_TRUE(listener_->is_listening());
    }

    bool waitFor(int count, std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, timeout, [&] { return notifications_ >= count; });
    }

    int notifications() {
        std::lock_guard<std::mutex> lock(mutex_);
        return notifications_;
    }

    std::filesystem::path directory_;
    std::mutex mutex_;
    std::condition_variable changed_;
    int notifications_ = 0;
    std::unique_ptr<lattice::cross_process_notifier> listener_;
    std::unique_ptr<lattice::cross_process_notifier> sender_;
};

TEST_F(LinuxNotifier, SignalFileWriteDeliversNotification) {
    start();
    sender_->post_notification();
    EXPECT_TRUE(waitFor(1));
}

TEST_F(LinuxNotifier, IdleStopJoinsAndCanBeRepeated) {
    start();
    listener_->stop_listening();
    EXPECT_FALSE(listener_->is_listening());
    listener_->stop_listening();
    EXPECT_FALSE(listener_->is_listening());
}

TEST_F(LinuxNotifier, CompletedStopPreventsFurtherCallbacks) {
    start();
    sender_->post_notification();
    ASSERT_TRUE(waitFor(1));
    listener_->stop_listening();
    const int stoppedCount = notifications();
    sender_->post_notification();
    EXPECT_FALSE(waitFor(stoppedCount + 1, std::chrono::milliseconds(100)));
    EXPECT_EQ(notifications(), stoppedCount);
}

TEST_F(LinuxNotifier, RestartReceivesNewNotifications) {
    start();
    sender_->post_notification();
    ASSERT_TRUE(waitFor(1));
    listener_->stop_listening();
    const int stoppedCount = notifications();
    start();
    sender_->post_notification();
    EXPECT_TRUE(waitFor(stoppedCount + 1));
}

#if defined(__linux__) && !defined(__ANDROID__)
uint64_t inotify_limit(const char* name) {
    std::ifstream input(std::string("/proc/sys/fs/inotify/") + name);
    uint64_t value = 0;
    if (!(input >> value) || value == 0) throw std::runtime_error("Cannot read inotify limit");
    return value;
}

size_t inotify_descriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const auto target = std::filesystem::read_symlink(entry.path(), error).string();
        if (error == std::errc::no_such_file_or_directory) continue;
        if (error) throw std::runtime_error("Cannot inspect process descriptors");
        if (target == "anon_inode:inotify" || target == "anon_inode:[inotify]") ++count;
    }
    return count;
}

struct ResourceFiles {
    std::filesystem::path directory;
    ResourceFiles() {
        auto pattern = (std::filesystem::temp_directory_path() / "notifier-resources-XXXXXX").string();
        std::vector<char> bytes(pattern.begin(), pattern.end()); bytes.push_back('\0');
        const char* made = ::mkdtemp(bytes.data());
        if (!made) throw std::runtime_error("Cannot create notifier fixture directory");
        directory = made;
    }
    ~ResourceFiles() {
        std::error_code error;
        std::filesystem::remove_all(directory, error);
        EXPECT_FALSE(error) << error.message();
    }
    std::string store(size_t index) const { return (directory / (std::to_string(index) + ".sqlite")).string(); }
};

lattice::configuration resource_config(const std::string& path) {
    lattice::configuration config(path);
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 2000;
    return config;
}

struct ResourceObservation {
    struct State { std::mutex mutex; std::condition_variable changed; int inserts = 0; };
    lattice::lattice_db& owner;
    std::shared_ptr<State> state = std::make_shared<State>();
    lattice::lattice_db::observer_id token;
    explicit ResourceObservation(lattice::lattice_db& db) : owner(db) {
        token = owner.add_table_observer("NotifierResourceRow", [captured = state](const auto& changes) {
            std::lock_guard<std::mutex> lock(captured->mutex);
            for (const auto& change : changes) if (std::get<1>(change) == "INSERT") ++captured->inserts;
            captured->changed.notify_all();
        });
    }
    ~ResourceObservation() { owner.remove_table_observer("NotifierResourceRow", token); }
    bool wait_for_insert(std::chrono::milliseconds bound) {
        std::unique_lock<std::mutex> lock(state->mutex);
        return state->changed.wait_for(lock, bound, [&] { return state->inserts != 0; });
    }
    int inserts() { std::lock_guard<std::mutex> lock(state->mutex); return state->inserts; }
};

void insert_from_child(const ResourceFiles& files, const std::string& path, int sequence) {
    char binary[4096];
    const auto length = readlink("/proc/self/exe", binary, sizeof(binary) - 1);
    ASSERT_GT(length, 0);
    ASSERT_LT(length, static_cast<ssize_t>(sizeof(binary) - 1));
    binary[length] = '\0';
    std::vector<std::string> values;
    for (char** entry = environ; *entry; ++entry) {
        const std::string value(*entry);
        // Do not let a child truncate the parent log/XML or inherit a test shard.
        if (value.rfind("LATTICE_NOTIFIER_RESOURCE_", 0) == 0 ||
            value.rfind("LATTICE_TEST_LOG_PATH=", 0) == 0 ||
            value.rfind("GTEST_OUTPUT=", 0) == 0 || value.rfind("GTEST_SHARD_", 0) == 0 ||
            value.rfind("GTEST_REPEAT=", 0) == 0 ||
            value.rfind("GTEST_TOTAL_SHARDS=", 0) == 0) continue;
        values.push_back(value);
    }
    values.push_back("LATTICE_NOTIFIER_RESOURCE_PATH=" + path);
    values.push_back("LATTICE_NOTIFIER_RESOURCE_SEQUENCE=" + std::to_string(sequence));
    const auto log_path = files.directory / ("child-" + std::to_string(sequence) + ".log");
    values.push_back("LATTICE_TEST_LOG_PATH=" + log_path.string());
    std::vector<char*> environment;
    for (auto& value : values) environment.push_back(value.data());
    environment.push_back(nullptr);
    char filter[] = "--gtest_filter=LinuxNotifierResources.OpenedFileStoresMustHaveActiveListeners";
    char* arguments[] = {binary, filter, nullptr};
    pid_t child = -1;
    ASSERT_EQ(posix_spawn(&child, binary, nullptr, nullptr, arguments, environment.data()), 0);
    int status = 0;
    pid_t waited;
    do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        // Keep a failed child's diagnostics in the ordinary job output before fixture cleanup.
        std::ifstream log(log_path);
        std::cout << "NOTIFIER_RESOURCE child_sequence=" << sequence << " failure_log_begin\n"
                  << log.rdbuf() << "\nNOTIFIER_RESOURCE failure_log_end" << std::endl;
    }
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    std::cout << "NOTIFIER_RESOURCE child_sequence=" << sequence << " exit=0" << std::endl;
}

TEST(LinuxNotifierResources, OpenedFileStoresMustHaveActiveListeners) {
    // Re-exec just this test for each real external commit; never fork with live listener threads.
    if (const char* path = std::getenv("LATTICE_NOTIFIER_RESOURCE_PATH")) {
        const char* sequence = std::getenv("LATTICE_NOTIFIER_RESOURCE_SEQUENCE");
        ASSERT_NE(sequence, nullptr);
        // Bound the owned child through construction, commit, close and destructor; the parent reaps it.
        ASSERT_NE(std::signal(SIGALRM, SIG_DFL), SIG_ERR);
        sigset_t unblocked;
        ASSERT_EQ(sigemptyset(&unblocked), 0);
        ASSERT_EQ(sigaddset(&unblocked, SIGALRM), 0);
        ASSERT_EQ(sigprocmask(SIG_UNBLOCK, &unblocked, nullptr), 0);
        ::alarm(60);
        {
            lattice::lattice_db child(resource_config(path));
            child.add(NotifierResourceRow{std::stoll(sequence)});
            child.close();
        }
        ::alarm(0);
        return;
    }
    ASSERT_EQ(std::getenv("LATTICE_DISABLE_XPROC"), nullptr);
    const auto limit = inotify_limit("max_user_instances");
    const auto watches = inotify_limit("max_user_watches");
    rlimit descriptors{};
    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &descriptors), 0);
    const size_t requested = static_cast<size_t>(limit >= 384 ? 400 : limit + 16);
    ResourceFiles files;
    const auto template_path = (files.directory / "template.sqlite").string();
    {
        // Build the test binary's schema once; every copied file still gets a real independent open.
        lattice::lattice_db seed(resource_config(template_path));
        ASSERT_FALSE(seed.db().wal_checkpoint(true).busy);
    }
    const auto template_wal = template_path + "-wal";
    ASSERT_TRUE(!std::filesystem::exists(template_wal) || std::filesystem::file_size(template_wal) == 0);
    const auto initial_descriptors = inotify_descriptors();
    std::cout << "NOTIFIER_RESOURCE max_user_instances=" << limit
              << " max_user_watches=" << watches << " inotify_descriptors_start=" << initial_descriptors
              << " rlimit_nofile=" << descriptors.rlim_cur << " requested_stores=" << requested << std::endl;
    std::vector<std::unique_ptr<lattice::lattice_db>> stores(requested);
    size_t opened = 0, reported = 0, silent = 0, first_silent = requested;
    auto& registry = lattice::instance_registry::instance();
    for (size_t index = 0; index < requested; ++index) {
        ASSERT_TRUE(std::filesystem::copy_file(template_path, files.store(index)));
        try {
            stores[index] = std::make_unique<lattice::lattice_db>(resource_config(files.store(index)));
        } catch (const std::exception& error) {
            ++reported;
            std::cout << "NOTIFIER_RESOURCE store=" << index << " opened=0 error=" << error.what() << std::endl;
            continue;
        }
        ++opened;
        auto* notifier = registry.get_or_create_notifier(files.store(index));
        const bool listening = notifier && notifier->is_listening();
        if (!listening) { ++silent; if (first_silent == requested) first_silent = index; }
        std::cout << "NOTIFIER_RESOURCE store=" << index << " opened=1 listening=" << listening << std::endl;
    }
    std::cout << "NOTIFIER_RESOURCE opened=" << opened << " reported_errors=" << reported
              << " silently_not_listening=" << silent << " inotify_descriptors_peak=" << inotify_descriptors() << std::endl;
    EXPECT_EQ(opened + reported, requested);
    // This is the required contract, not an inverted expectation of the current defect.
    EXPECT_EQ(silent, 0u) << "Every successful file-store open must listen or report unavailable observation";
    if (first_silent == requested) {
        std::cout << "NOTIFIER_RESOURCE exhaustion_not_observed_within_cap=1" << std::endl;
        return;
    }

    const auto path = files.store(first_silent);
    auto* failed = registry.get_or_create_notifier(path);
    ASSERT_NE(failed, nullptr);
    ASSERT_FALSE(failed->is_listening());
    stores[first_silent]->add(NotifierResourceRow{0});
    auto observer = std::make_unique<ResourceObservation>(*stores[first_silent]);
    ASSERT_NO_FATAL_FAILURE(insert_from_child(files, path, 1));
    EXPECT_EQ(stores[first_silent]->db().query("SELECT sequence FROM NotifierResourceRow WHERE sequence=1").size(), 1u);
    EXPECT_FALSE(observer->wait_for_insert(std::chrono::milliseconds(250)));
    EXPECT_EQ(observer->inserts(), 0);

    // Free capacity while retaining the failed path: neither lookup nor a new owner retries setup.
    for (size_t index = 0; index < stores.size(); ++index) if (index != first_silent) stores[index].reset();
    auto sibling = std::make_unique<lattice::lattice_db>(resource_config(path));
    EXPECT_EQ(registry.get_or_create_notifier(path), failed);
    EXPECT_FALSE(failed->is_listening());
    std::cout << "NOTIFIER_RESOURCE retained_path_listening=" << failed->is_listening()
              << " inotify_descriptors_after_release=" << inotify_descriptors() << std::endl;
    ASSERT_NO_FATAL_FAILURE(insert_from_child(files, path, 2));
    EXPECT_EQ(stores[first_silent]->db().query("SELECT sequence FROM NotifierResourceRow WHERE sequence=2").size(), 1u);
    EXPECT_FALSE(observer->wait_for_insert(std::chrono::milliseconds(250)));
    EXPECT_EQ(observer->inserts(), 0);
    observer.reset();
    sibling.reset();
    stores[first_silent].reset();

    auto reopened = std::make_unique<lattice::lattice_db>(resource_config(path));
    auto* restored = registry.get_or_create_notifier(path);
    ASSERT_NE(restored, nullptr);
    ASSERT_TRUE(restored->is_listening());
    ResourceObservation recovered(*reopened);
    ASSERT_NO_FATAL_FAILURE(insert_from_child(files, path, 3));
    EXPECT_TRUE(recovered.wait_for_insert(std::chrono::seconds(30)));
    EXPECT_EQ(recovered.inserts(), 1);
    EXPECT_EQ(reopened->db().query("SELECT sequence FROM NotifierResourceRow WHERE sequence=3").size(), 1u);
    std::cout << "NOTIFIER_RESOURCE reopened_listening=" << restored->is_listening()
              << " recovered_insert_callbacks=" << recovered.inserts() << std::endl;
}

void repaired_signal_path_retries_setup() {
    ASSERT_EQ(std::getenv("LATTICE_DISABLE_XPROC"), nullptr);
    ResourceFiles files;
    const auto path = files.store(0);
    const auto signal_path = path + "-signal";
    {
        lattice::lattice_db seed(resource_config(path));
        ASSERT_FALSE(seed.db().wal_checkpoint(true).busy);
    }

    // This owned loop makes watch setup fail independently of the host's
    // inotify quota, without obstructing the database file's own open.
    ASSERT_TRUE(std::filesystem::remove(signal_path));
    const auto target = std::filesystem::path(signal_path).filename().string();
    ASSERT_EQ(::symlink(target.c_str(), signal_path.c_str()), 0);
    const int probe = ::inotify_init1(IN_CLOEXEC);
    ASSERT_GE(probe, 0);
    errno = 0;
    const int watch = ::inotify_add_watch(probe, signal_path.c_str(), IN_CLOSE_WRITE);
    const int watch_error = errno;
    const int close_result = ::close(probe);
    ASSERT_EQ(close_result, 0);
    ASSERT_EQ(watch, -1);
    ASSERT_EQ(watch_error, ELOOP);
    std::fprintf(stderr, "NOTIFIER_SETUP forced_watch_errno=%d expected_errno=%d\n", watch_error, ELOOP);

    auto first = std::make_unique<lattice::lattice_db>(resource_config(path));
    auto& registry = lattice::instance_registry::instance();
    auto* initial = registry.get_or_create_notifier(path);
    ASSERT_NE(initial, nullptr);
    ASSERT_FALSE(initial->is_listening());
    std::fprintf(stderr, "NOTIFIER_SETUP first_open_returned=1 initial_listening=0\n");

    // Repair only the fixture fault while keeping the original owner alive.
    ASSERT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(signal_path)));
    ASSERT_TRUE(std::filesystem::remove(signal_path));
    const int signal = ::open(signal_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(signal, 0);
    ASSERT_EQ(::close(signal), 0);
    auto sibling = std::make_unique<lattice::lattice_db>(resource_config(path));
    auto* retried = registry.get_or_create_notifier(path);
    ASSERT_NE(retried, nullptr);
    const bool recovered = retried->is_listening();
    std::fprintf(stderr, "NOTIFIER_SETUP repaired=1 sibling_open_returned=1 same_notifier=%d retry_listening=%d\n",
                 retried == initial, recovered);
    EXPECT_TRUE(recovered) << "A new owner must retry failed listener setup after the signal path is repaired";
    {
        ResourceObservation observation(*first);
        ASSERT_NO_FATAL_FAILURE(insert_from_child(files, path, 1));
        EXPECT_EQ(first->db().query("SELECT sequence FROM NotifierResourceRow WHERE sequence=1").size(), 1u);
        if (recovered) {
            EXPECT_TRUE(observation.wait_for_insert(std::chrono::seconds(30)));
            EXPECT_EQ(observation.inserts(), 1);
        } else {
            // The listening-state assertion above is the failing contract;
            // this bounded wait additionally records the missed external wake.
            EXPECT_FALSE(observation.wait_for_insert(std::chrono::milliseconds(250)));
            EXPECT_EQ(observation.inserts(), 0);
        }
        std::fprintf(stderr, "NOTIFIER_SETUP repaired_path_insert_callbacks=%d\n", observation.inserts());
    }

    sibling.reset();
    first.reset();
    EXPECT_EQ(registry.get_or_create_notifier(path), nullptr)
        << "A diagnostic lookup must not resurrect a listener after the last owner closes";
    auto reopened = std::make_unique<lattice::lattice_db>(resource_config(path));
    auto* fresh = registry.get_or_create_notifier(path);
    ASSERT_NE(fresh, nullptr);
    ASSERT_TRUE(fresh->is_listening());
    ResourceObservation observation(*reopened);
    ASSERT_NO_FATAL_FAILURE(insert_from_child(files, path, 2));
    EXPECT_EQ(reopened->db().query("SELECT sequence FROM NotifierResourceRow WHERE sequence=2").size(), 1u);
    EXPECT_TRUE(observation.wait_for_insert(std::chrono::seconds(30)));
    EXPECT_EQ(observation.inserts(), 1);
    std::fprintf(stderr, "NOTIFIER_SETUP full_release_reopened_listening=1 insert_callbacks=%d\n", observation.inserts());
}

TEST(LinuxNotifierResources, RepairedSignalPathRetriesSetupForAnOpenStore) {
    struct RestoreStyle {
        std::string prior = GTEST_FLAG_GET(death_test_style);
        ~RestoreStyle() { GTEST_FLAG_SET(death_test_style, prior); }
    } restore_style;
    // Threadsafe death tests re-exec: the fixture fault and all native owners
    // belong to a fresh process, not a fork retaining other tests' threads.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR) _exit(2);
        sigset_t unblocked;
        if (sigemptyset(&unblocked) != 0 || sigaddset(&unblocked, SIGALRM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) _exit(2);
        ::alarm(150);
        try {
            repaired_signal_path_retries_setup();
            if (::testing::Test::HasFailure()) {
                const auto* result = ::testing::UnitTest::GetInstance()->current_test_info()->result();
                for (int i = 0, emitted = 0; i < result->total_part_count() && emitted < 16; ++i) {
                    const auto& part = result->GetTestPartResult(i);
                    if (part.failed()) {
                        ++emitted;
                        std::fprintf(stderr, "native_assertion %.512s:%d: %.4096s\n",
                                     part.file_name() ? part.file_name() : "unknown", part.line_number(), part.summary());
                    }
                }
                std::fflush(stderr);
                _exit(1);
            }
            std::fputs("notifier_setup_probe_complete\n", stderr);
            std::fflush(stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "notifier_setup_probe_failure: %s\n", error.what());
            std::fflush(stderr);
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "notifier_setup_probe_complete");
}
TEST(LinuxNotifierResources, IndependentPathsUseAtMostOneAdditionalInotifyInstance) {
    ResourceFiles files;
    const auto initial = inotify_descriptors();
    std::vector<std::unique_ptr<lattice::cross_process_notifier>> listeners;
    for (size_t index = 0; index < 32; ++index) {
        auto listener = lattice::make_cross_process_notifier(files.store(index));
        ASSERT_NE(listener, nullptr);
        listener->start_listening([] {});
        ASSERT_TRUE(listener->is_listening()) << index;
        listeners.push_back(std::move(listener));
    }
    const auto peak = inotify_descriptors();
    EXPECT_LE(peak, initial + 1) << "File paths must share their process's inotify instance";
    listeners.clear();
    EXPECT_LE(inotify_descriptors(), initial + 1);
}

TEST(LinuxNotifierResources, StoppingOneSubscriberPreservesTheSameSignalWatch) {
    ResourceFiles files;
    std::mutex mutex;
    std::condition_variable changed;
    int first_calls = 0, second_calls = 0;
    auto first = lattice::make_cross_process_notifier(files.store(0));
    auto second = lattice::make_cross_process_notifier(files.store(0));
    auto sender = lattice::make_cross_process_notifier(files.store(0));
    first->start_listening([&] {
        std::lock_guard<std::mutex> lock(mutex);
        ++first_calls;
        changed.notify_all();
    });
    second->start_listening([&] {
        std::lock_guard<std::mutex> lock(mutex);
        ++second_calls;
        changed.notify_all();
    });
    ASSERT_TRUE(first->is_listening());
    ASSERT_TRUE(second->is_listening());
    sender->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(5), [&] {
            return first_calls > 0 && second_calls > 0;
        }));
    }
    first->stop_listening();
    int stopped_count, prior_second;
    {
        std::lock_guard<std::mutex> lock(mutex);
        stopped_count = first_calls;
        prior_second = second_calls;
    }
    sender->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(5), [&] {
            return second_calls > prior_second;
        }));
        EXPECT_EQ(first_calls, stopped_count);
    }
    EXPECT_FALSE(first->is_listening());
    EXPECT_TRUE(second->is_listening());
    second->stop_listening();
}

TEST(LinuxNotifierResources, ABlockedPathCallbackDoesNotBlockAnotherPath) {
    ResourceFiles files;
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false, release = false, delivered = false;
    auto blocked = lattice::make_cross_process_notifier(files.store(0));
    auto independent = lattice::make_cross_process_notifier(files.store(1));
    // Always release the held callback before either notifier is destroyed,
    // including an assertion or exception during fixture setup.
    struct Release {
        std::mutex& mutex;
        std::condition_variable& changed;
        bool& flag;
        ~Release() {
            std::lock_guard<std::mutex> lock(mutex);
            flag = true;
            changed.notify_all();
        }
    } release_on_exit{mutex, changed, release};
    blocked->start_listening([&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release; });
    });
    independent->start_listening([&] {
        std::lock_guard<std::mutex> lock(mutex);
        delivered = true;
        changed.notify_all();
    });
    ASSERT_TRUE(blocked->is_listening());
    ASSERT_TRUE(independent->is_listening());
    blocked->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(changed.wait_for(lock, std::chrono::seconds(5), [&] { return entered; }));
    }
    independent->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(5), [&] { return delivered; }));
        release = true;
        changed.notify_all();
    }
    blocked->stop_listening();
    independent->stop_listening();
}

TEST(LinuxNotifierResources, MovedSignalInvalidatesTheWatchAndCanBeRestarted) {
    ResourceFiles files;
    const auto path = files.store(0);
    std::mutex mutex;
    std::condition_variable changed;
    int calls = 0;
    auto listener = lattice::make_cross_process_notifier(path);
    auto callback = [&] {
        std::lock_guard<std::mutex> lock(mutex);
        ++calls;
        changed.notify_all();
    };
    listener->start_listening(callback);
    ASSERT_TRUE(listener->is_listening());
    std::filesystem::rename(path + "-signal", path + "-old-signal");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (listener->is_listening() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_FALSE(listener->is_listening()) << "The watch no longer follows the advertised signal path";
    listener->start_listening(callback);
    ASSERT_TRUE(listener->is_listening());
    EXPECT_TRUE(std::filesystem::is_regular_file(path + "-signal"));
    int before;
    {
        std::lock_guard<std::mutex> lock(mutex);
        before = calls;
    }
    listener->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(5), [&] { return calls > before; }));
    }
    listener->stop_listening();
}

bool release_notifier_from_its_callback() {
    ResourceFiles files;
    std::mutex mutex;
    std::condition_variable changed;
    bool released = false;
    auto listener = lattice::make_cross_process_notifier(files.store(0));
    auto sender = lattice::make_cross_process_notifier(files.store(0));
    listener->start_listening([&] {
        listener.reset();
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
        changed.notify_all();
    });
    if (!listener->is_listening()) return false;
    sender->post_notification();
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, std::chrono::seconds(5), [&] { return released; });
}

TEST(LinuxNotifierResources, ACallbackCanReleaseItsLastNotifierOwner) {
    struct RestoreStyle {
        std::string prior = GTEST_FLAG_GET(death_test_style);
        ~RestoreStyle() { GTEST_FLAG_SET(death_test_style, prior); }
    } restore_style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR) _exit(2);
        sigset_t unblocked;
        if (sigemptyset(&unblocked) != 0 || sigaddset(&unblocked, SIGALRM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) _exit(2);
        ::alarm(30);
        try {
            if (!release_notifier_from_its_callback() || ::testing::Test::HasFailure()) _exit(3);
            std::fputs("notifier_callback_release_complete\n", stderr);
            std::fflush(stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "notifier_callback_release_failure: %s\n", error.what());
            std::fflush(stderr);
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "notifier_callback_release_complete");
}

bool replace_failed_callback_with_reentrant_capture_cleanup() {
    ResourceFiles files;
    const auto path = files.store(0);
    const auto signal_path = path + "-signal";
    bool disposed = false;
    auto listener = lattice::make_cross_process_notifier(path);
    if (!std::filesystem::remove(signal_path)) return false;
    const auto target = std::filesystem::path(signal_path).filename().string();
    if (::symlink(target.c_str(), signal_path.c_str()) != 0) return false;
    auto capture = std::shared_ptr<int>(new int(1), [&, observed = listener.get()](int* value) {
        std::fputs("capture_cleanup_entered\n", stderr);
        std::fflush(stderr);
        // Destroying a callback capture may query the still-live notifier.
        // The final copy must therefore be released outside internal locks.
        (void)observed->is_listening();
        disposed = true;
        delete value;
        std::fputs("capture_cleanup_returned\n", stderr);
        std::fflush(stderr);
    });
    listener->start_listening([capture] {});
    capture.reset();
    if (listener->is_listening()) return false;
    if (!std::filesystem::remove(signal_path)) return false;
    listener->start_listening([] {});
    const bool recovered = listener->is_listening();
    listener->stop_listening();
    return recovered && disposed;
}

TEST(LinuxNotifierResources, RetriedCallbackReleasesCapturesOutsideNotifierLocks) {
    struct RestoreStyle {
        std::string prior = GTEST_FLAG_GET(death_test_style);
        ~RestoreStyle() { GTEST_FLAG_SET(death_test_style, prior); }
    } restore_style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR) _exit(2);
        sigset_t unblocked;
        if (sigemptyset(&unblocked) != 0 || sigaddset(&unblocked, SIGALRM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) _exit(2);
        ::alarm(30);
        try {
            if (!replace_failed_callback_with_reentrant_capture_cleanup() || ::testing::Test::HasFailure()) _exit(3);
            std::fputs("notifier_capture_cleanup_complete\n", stderr);
            std::fflush(stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "notifier_capture_cleanup_failure: %s\n", error.what());
            std::fflush(stderr);
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "notifier_capture_cleanup_complete");
}

bool worker_releases_replaced_callback_outside_state_lock() {
    ResourceFiles files;
    const auto path = files.store(0);
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false, release = false, disposed = false;
    auto listener = lattice::make_cross_process_notifier(path);
    auto sender = lattice::make_cross_process_notifier(path);
    struct Release {
        std::mutex& mutex;
        std::condition_variable& changed;
        bool& release;
        ~Release() {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
            changed.notify_all();
        }
    } release_on_exit{mutex, changed, release};
    auto capture = std::shared_ptr<int>(new int(1), [&, observed = listener.get()](int* value) {
        std::fputs("worker_capture_cleanup_entered\n", stderr);
        std::fflush(stderr);
        observed->stop_listening();
        delete value;
        std::lock_guard<std::mutex> lock(mutex);
        disposed = true;
        changed.notify_all();
        std::fputs("worker_capture_cleanup_returned\n", stderr);
        std::fflush(stderr);
    });
    listener->start_listening([&, capture] {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release; });
    });
    capture.reset();
    sender->post_notification();
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!changed.wait_for(lock, std::chrono::seconds(5), [&] { return entered; })) return false;
    }
    std::filesystem::rename(path + "-signal", path + "-old-signal");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (listener->is_listening() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (listener->is_listening()) return false;
    // Replace the stored copy while the running worker still owns the original.
    listener->start_listening([] {});
    if (!listener->is_listening()) return false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        release = true;
        changed.notify_all();
        if (!changed.wait_for(lock, std::chrono::seconds(5), [&] { return disposed; })) return false;
    }
    return !listener->is_listening();
}

TEST(LinuxNotifierResources, WorkerCallbackReleasesCapturesOutsideStateLock) {
    struct RestoreStyle {
        std::string prior = GTEST_FLAG_GET(death_test_style);
        ~RestoreStyle() { GTEST_FLAG_SET(death_test_style, prior); }
    } restore_style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR) _exit(2);
        sigset_t unblocked;
        if (sigemptyset(&unblocked) != 0 || sigaddset(&unblocked, SIGALRM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) _exit(2);
        ::alarm(30);
        try {
            if (!worker_releases_replaced_callback_outside_state_lock() || ::testing::Test::HasFailure()) _exit(3);
            std::fputs("worker_capture_cleanup_complete\n", stderr);
            std::fflush(stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "worker_capture_cleanup_failure: %s\n", error.what());
            std::fflush(stderr);
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "worker_capture_cleanup_complete");
}
#endif // Linux resource regression; not Android

} // namespace

#endif // __linux__ || __ANDROID__
