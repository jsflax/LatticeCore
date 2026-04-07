#include "TestHelpers.hpp"
#include <thread>
#include <vector>
#include <fstream>

// ============================================================================
// Thread-safe logging tests
// ============================================================================

TEST(LogTest, ConcurrentLogWrites) {
    // Hammer LOG_INFO from multiple threads simultaneously.
    // Before the mutex fix, this would crash with fflush/fprintf races.
    TempDB tmp{"log_test"};
    std::string logPath = tmp.str() + ".log";
    FILE* f = fopen(logPath.c_str(), "w");
    ASSERT_NE(f, nullptr);

    lattice::set_log_file(f);
    lattice::g_log_level.store(lattice::log_level::debug, std::memory_order_relaxed);

    std::vector<std::thread> threads;
    for (int t = 0; t < 8; t++) {
        threads.emplace_back([t]() {
            for (int i = 0; i < 100; i++) {
                LOG_INFO("thread", "thread=%d iter=%d message=%s", t, i, "test log entry");
                LOG_DEBUG("thread", "debug thread=%d iter=%d", t, i);
            }
        });
    }
    for (auto& t : threads) t.join();

    lattice::set_log_file(nullptr);
    fclose(f);

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

    FILE* f1 = fopen(path1.c_str(), "w");
    FILE* f2 = fopen(path2.c_str(), "w");
    ASSERT_NE(f1, nullptr);
    ASSERT_NE(f2, nullptr);

    lattice::set_log_file(f1);
    lattice::g_log_level.store(lattice::log_level::debug, std::memory_order_relaxed);

    std::atomic<bool> done{false};

    // Writer threads
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; t++) {
        writers.emplace_back([&done, t]() {
            int i = 0;
            while (!done.load()) {
                LOG_INFO("writer", "thread=%d iter=%d", t, i++);
            }
        });
    }

    // Switcher thread — rapidly switches between f1 and f2
    std::thread switcher([&done, f1, f2]() {
        for (int i = 0; i < 100; i++) {
            lattice::set_log_file((i % 2 == 0) ? f1 : f2);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        done.store(true);
    });

    switcher.join();
    for (auto& w : writers) w.join();

    lattice::set_log_file(nullptr);
    fclose(f1);
    fclose(f2);

    // Both files should have content and no corruption
    for (const auto& path : {path1, path2}) {
        std::ifstream in(path);
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_GT(content.size(), 0u) << path << " should have content";
    }
}
