#if defined(__linux__) || defined(__ANDROID__)

#include <gtest/gtest.h>
#include "lattice/cross_process_notifier.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <vector>

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

} // namespace

#endif // __linux__ || __ANDROID__
