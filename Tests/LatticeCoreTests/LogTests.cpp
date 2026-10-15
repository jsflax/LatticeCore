#include "TestHelpers.hpp"
#include <thread>
#include <vector>
#include <fstream>
#include <memory>

namespace {
// The test environment owns the borrowed destination until suite teardown.
// Restore under the same mutex as log_write before any temporary FILE closes.
class ScopedLogState {
public:
    ScopedLogState() {
        std::lock_guard<std::mutex> lock(lattice::g_log_mutex());
        file_ = lattice::g_log_file.load(std::memory_order_acquire);
        level_ = lattice::get_log_level();
    }
    ScopedLogState(const ScopedLogState&) = delete;
    ScopedLogState& operator=(const ScopedLogState&) = delete;
    ~ScopedLogState() { restore(); }

    void restore() {
        if (restored_) return;
        std::lock_guard<std::mutex> lock(lattice::g_log_mutex());
        lattice::g_log_file.store(file_, std::memory_order_release);
        lattice::set_log_level(level_);
        restored_ = true;
    }

private:
    FILE* file_ = nullptr;
    lattice::log_level level_ = lattice::log_level::off;
    bool restored_ = false;
};

using LogFile = std::unique_ptr<FILE, decltype(&fclose)>;

// An allocation/thread-creation exception must also retire already-started
// writers before ScopedLogState restores the destination and LogFile closes it.
struct JoinLogWriters {
    std::vector<std::thread>& threads;
    std::atomic<bool>* done = nullptr;
    ~JoinLogWriters() {
        if (done) done->store(true);
        for (auto& thread : threads) if (thread.joinable()) thread.join();
    }
};
} // namespace

// ============================================================================
// Thread-safe logging tests
// ============================================================================

TEST(LogTest, ConcurrentLogWrites) {
    // Hammer LOG_INFO from multiple threads simultaneously.
    // Before the mutex fix, this would crash with fflush/fprintf races.
    TempDB tmp{"log_test"};
    std::string logPath = tmp.str() + ".log";
    LogFile f(fopen(logPath.c_str(), "w"), &fclose);
    ASSERT_NE(f.get(), nullptr);

    ScopedLogState prior;
    lattice::set_log_file(f.get());
    lattice::g_log_level.store(lattice::log_level::debug, std::memory_order_relaxed);

    std::vector<std::thread> threads;
    JoinLogWriters join{threads};
    for (int t = 0; t < 8; t++) {
        threads.emplace_back([t]() {
            for (int i = 0; i < 100; i++) {
                LOG_INFO("thread", "thread=%d iter=%d message=%s", t, i, "test log entry");
                LOG_DEBUG("thread", "debug thread=%d iter=%d", t, i);
            }
        });
    }
    for (auto& t : threads) t.join();

    prior.restore();
    f.reset();

    // Verify file has content
    std::ifstream in(logPath);
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_GT(content.size(), 1000u) << "Log file should have substantial content";
    // 8 threads * 200 lines = 1600 lines expected
    int lineCount = std::count(content.begin(), content.end(), '\n');
    EXPECT_EQ(lineCount, 1600) << "Should have exactly 1600 log lines";
}

TEST(LogTest, SetLogFileDuringWrites) {
    // Switch log file while other threads are writing.
    // Before the mutex fix, this could crash with use-after-close.
    TempDB tmp1{"log_switch_1"};
    TempDB tmp2{"log_switch_2"};
    std::string path1 = tmp1.str() + ".log";
    std::string path2 = tmp2.str() + ".log";

    LogFile f1(fopen(path1.c_str(), "w"), &fclose);
    LogFile f2(fopen(path2.c_str(), "w"), &fclose);
    ASSERT_NE(f1.get(), nullptr);
    ASSERT_NE(f2.get(), nullptr);

    ScopedLogState prior;
    lattice::set_log_file(f1.get());
    lattice::g_log_level.store(lattice::log_level::debug, std::memory_order_relaxed);

    std::atomic<bool> done{false};

    // Writer threads
    std::vector<std::thread> writers;
    JoinLogWriters join{writers, &done};
    for (int t = 0; t < 4; t++) {
        writers.emplace_back([&done, t]() {
            int i = 0;
            while (!done.load()) {
                LOG_INFO("writer", "thread=%d iter=%d", t, i++);
            }
        });
    }

    // Switcher thread — rapidly switches between f1 and f2
    std::thread switcher([&done, f1 = f1.get(), f2 = f2.get()]() {
        for (int i = 0; i < 100; i++) {
            lattice::set_log_file((i % 2 == 0) ? f1 : f2);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        done.store(true);
    });

    switcher.join();
    for (auto& w : writers) w.join();

    prior.restore();
    f1.reset();
    f2.reset();

    // Both files should have content and no corruption
    for (const auto& path : {path1, path2}) {
        std::ifstream in(path);
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_GT(content.size(), 0u) << path << " should have content";
    }
}

TEST(LogTest, ScopedDestinationRestoresBorrowedFileAndLevelOnReturnAndThrow) {
    TempDB prior_tmp{"log_prior"};
    TempDB redirected_tmp{"log_redirected"};
    const auto prior_path = prior_tmp.str() + ".log";
    const auto redirected_path = redirected_tmp.str() + ".log";
    LogFile prior_file(fopen(prior_path.c_str(), "w"), &fclose);
    LogFile redirected_file(fopen(redirected_path.c_str(), "w"), &fclose);
    ASSERT_NE(prior_file.get(), nullptr);
    ASSERT_NE(redirected_file.get(), nullptr);
    ScopedLogState suite;
    lattice::set_log_file(prior_file.get());
    lattice::set_log_level(lattice::log_level::info);

    for (const bool throw_after_write : {false, true}) {
        bool caught = false;
        try {
            [&] {
                ScopedLogState nested;
                lattice::set_log_file(redirected_file.get());
                lattice::set_log_level(lattice::log_level::debug);
                LOG_DEBUG("restore", "redirected");
                if (throw_after_write) throw 17;
                return;
            }();
        } catch (int value) {
            EXPECT_EQ(value, 17);
            caught = true;
        }
        EXPECT_EQ(caught, throw_after_write);
        EXPECT_EQ(lattice::g_log_file.load(std::memory_order_acquire), prior_file.get());
        EXPECT_EQ(lattice::get_log_level(), lattice::log_level::info);
        LOG_INFO("restore", "prior");
        LOG_DEBUG("restore", "must stay suppressed");
    }
    suite.restore();
    prior_file.reset();
    redirected_file.reset();

    std::ifstream prior_in(prior_path);
    std::ifstream redirected_in(redirected_path);
    const std::string prior_content((std::istreambuf_iterator<char>(prior_in)), std::istreambuf_iterator<char>());
    const std::string redirected_content((std::istreambuf_iterator<char>(redirected_in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(prior_content, "[restore] prior\n[restore] prior\n");
    EXPECT_EQ(redirected_content, "[restore] redirected\n[restore] redirected\n");
}
