#if defined(__linux__) || defined(__ANDROID__)

#include "lattice/cross_process_notifier.hpp"
#include "lattice/log.hpp"

#include <sys/inotify.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <thread>
#include <atomic>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace lattice {
namespace {

// The poll thread never runs database callbacks. Each path retains an independent
// worker, so a slow reader cannot hold up notification delivery to another path.
struct linux_listener_state : std::enable_shared_from_this<linux_listener_state> {
    explicit linux_listener_state(std::string path) : signal_path(std::move(path)) {}

    bool start_worker(std::function<void()> next_callback) {
        std::lock_guard<std::mutex> lock(mutex);
        callback = std::move(next_callback);
        if (worker.joinable()) return true;
        try {
            worker = std::thread([self = shared_from_this()] {
                std::unique_lock<std::mutex> lock(self->mutex);
                for (;;) {
                    self->changed.wait(lock, [&] { return self->stopping || self->pending; });
                    if (self->stopping) return;
                    self->pending = false;
                    auto callback = self->callback;
                    lock.unlock();
                    if (callback) callback();
                    lock.lock();
                }
            });
        } catch (const std::system_error& error) {
            LOG_ERROR("xproc", "Cannot start notification worker: %s", error.what());
            return false;
        }
        return true;
    }

    void enqueue() {
        std::lock_guard<std::mutex> lock(mutex);
        if (stopping || !listening.load()) return;
        pending = true;
        changed.notify_one();
    }

    void request_stop() {
        std::lock_guard<std::mutex> lock(mutex);
        stopping = true;
        pending = false;
        listening.store(false);
        changed.notify_one();
    }

    void finish_stop() {
        if (!worker.joinable()) return;
        if (worker.get_id() == std::this_thread::get_id()) {
            // A callback may release the last owner. Its shared state survives
            // this detach until the current callback returns; no next callback runs.
            worker.detach();
        } else {
            worker.join();
        }
    }

    const std::string signal_path;
    std::atomic<bool> listening{false};
    std::mutex mutex;
    std::condition_variable changed;
    bool stopping = false;
    bool pending = false;
    std::function<void()> callback;
    std::thread worker;
};

// Process lifetime matches the instance registry. One descriptor and one poll
// thread are shared; watches and callback workers are released with their paths.
class linux_notification_service {
public:
    static linux_notification_service& instance() {
        static auto* service = new linux_notification_service;
        return *service;
    }

    void subscribe(const std::shared_ptr<linux_listener_state>& state) {
        // A failed poll thread may be joined here, but never a callback worker.
        // The poll loop neither enters the registry nor takes this lifecycle lock.
        std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
        if (!ensure_running()) return;
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) return;
        const int watch = inotify_add_watch(inotify_fd_, state->signal_path.c_str(),
            IN_CLOSE_WRITE | IN_DELETE_SELF | IN_MOVE_SELF);
        if (watch < 0) {
            LOG_ERROR("xproc", "inotify_add_watch failed for %s: %s",
                      state->signal_path.c_str(), strerror(errno));
            return;
        }
        auto& subscribers = watches_[watch];
        // inotify can return the same descriptor for two names of one inode.
        // Removing either subscriber must not remove the other's watch.
        subscribers.erase(std::remove_if(subscribers.begin(), subscribers.end(),
            [](const auto& weak) { return weak.expired(); }), subscribers.end());
        const bool present = std::any_of(subscribers.begin(), subscribers.end(),
            [&](const auto& weak) { return weak.lock() == state; });
        if (!present) subscribers.push_back(state);
        state->listening.store(true);
    }

    void unsubscribe(const std::shared_ptr<linux_listener_state>& state) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = watches_.begin(); it != watches_.end();) {
            auto& subscribers = it->second;
            subscribers.erase(std::remove_if(subscribers.begin(), subscribers.end(),
                [&](const auto& weak) {
                    auto current = weak.lock();
                    return !current || current == state;
                }), subscribers.end());
            if (subscribers.empty()) {
                if (running_) (void)inotify_rm_watch(inotify_fd_, it->first);
                it = watches_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    using subscribers = std::vector<std::weak_ptr<linux_listener_state>>;

    bool ensure_running() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (running_) return true;
        }
        // Failure cleanup has already released mutex_. Joining cannot wait on
        // a database callback, and the next generation cannot reuse its fds yet.
        if (poll_thread_.joinable()) poll_thread_.join();
        std::lock_guard<std::mutex> lock(mutex_);
        inotify_fd_ = inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
        if (inotify_fd_ < 0) {
            LOG_ERROR("xproc", "inotify_init1 failed: %s", strerror(errno));
            close_descriptors();
            return false;
        }
        running_ = true;
        try {
            poll_thread_ = std::thread([this] { poll_loop(); });
        } catch (const std::system_error& error) {
            running_ = false;
            close_descriptors();
            LOG_ERROR("xproc", "Cannot start notification poll thread: %s", error.what());
            return false;
        }
        return true;
    }

    void close_descriptors() {
        if (inotify_fd_ >= 0) close(inotify_fd_);
        inotify_fd_ = -1;
    }

    void poll_failed() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& item : watches_) {
            for (auto& weak : item.second) {
                if (auto state = weak.lock()) state->listening.store(false);
            }
        }
        watches_.clear();
        close_descriptors();
        running_ = false;
    }

    void dispatch_event(const inotify_event& event) {
        std::vector<std::shared_ptr<linux_listener_state>> ready;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (event.mask & IN_Q_OVERFLOW) {
                // Signals carry no rows. Catching up every cursor is sufficient
                // when the kernel has coalesced or dropped individual signals.
                for (auto& item : watches_) {
                    for (auto& weak : item.second) {
                        if (auto state = weak.lock()) ready.push_back(std::move(state));
                    }
                }
            } else {
                auto it = watches_.find(event.wd);
                if (it == watches_.end()) return;
                if (event.mask & (IN_IGNORED | IN_DELETE_SELF | IN_MOVE_SELF)) {
                    for (auto& weak : it->second) {
                        if (auto state = weak.lock()) state->listening.store(false);
                    }
                    if (!(event.mask & IN_IGNORED)) (void)inotify_rm_watch(inotify_fd_, event.wd);
                    watches_.erase(it);
                } else if (event.mask & IN_CLOSE_WRITE) {
                    for (auto& weak : it->second) {
                        if (auto state = weak.lock()) ready.push_back(std::move(state));
                    }
                }
            }
        }
        // No callback-state lock is acquired while holding the service lock.
        for (auto& state : ready) state->enqueue();
    }

    void poll_loop() {
        alignas(inotify_event) char buffer[16384];
        for (;;) {
            pollfd descriptor{inotify_fd_, POLLIN, 0};
            const int result = poll(&descriptor, 1, -1);
            if (result < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("xproc", "Notification poll failed: %s", strerror(errno));
                break;
            }
            if (descriptor.revents & (POLLHUP | POLLERR | POLLNVAL)) {
                LOG_ERROR("xproc", "Notification descriptor failed: revents=%d", descriptor.revents);
                break;
            }
            if (!(descriptor.revents & POLLIN)) continue;
            bool failed = false;
            for (;;) {
                const auto length = read(inotify_fd_, buffer, sizeof(buffer));
                if (length < 0 && errno == EINTR) continue;
                if (length < 0 && errno == EAGAIN) break;
                if (length <= 0) {
                    LOG_ERROR("xproc", "Notification read failed: %s", length < 0 ? strerror(errno) : "end of file");
                    failed = true;
                    break;
                }
                size_t offset = 0;
                while (offset + sizeof(inotify_event) <= static_cast<size_t>(length)) {
                    const auto& event = *reinterpret_cast<const inotify_event*>(buffer + offset);
                    const auto size = sizeof(inotify_event) + event.len;
                    if (size > static_cast<size_t>(length) - offset) {
                        LOG_ERROR("xproc", "Incomplete inotify event");
                        failed = true;
                        break;
                    }
                    dispatch_event(event);
                    offset += size;
                }
                if (failed) break;
            }
            if (failed) break;
        }
        poll_failed();
    }

    std::mutex lifecycle_mutex_;
    std::mutex mutex_;
    bool running_ = false;
    int inotify_fd_ = -1;
    std::thread poll_thread_;
    std::unordered_map<int, subscribers> watches_;
};

} // namespace

class linux_cross_process_notifier final : public cross_process_notifier {
public:
    explicit linux_cross_process_notifier(const std::string& db_path)
        : signal_path_(db_path + "-signal") {
        int fd = open(signal_path_.c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
        if (fd >= 0) close(fd);
    }

    ~linux_cross_process_notifier() override { stop_listening(); }

    void post_notification() override {
        int fd = open(signal_path_.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
        if (fd >= 0) {
            char byte = 1;
            (void)write(fd, &byte, 1);
            close(fd);
        } else {
            LOG_ERROR("xproc", "Failed to open signal file: %s", strerror(errno));
        }
    }

    void start_listening(std::function<void()> callback) override {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (state_ && state_->listening.load()) return;
        // A cached notifier can outlive a removed signal file. Reapply the same
        // create-if-missing step as a fresh owner before trying its watch again.
        const int signal = open(signal_path_.c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
        if (signal >= 0) close(signal);
        if (!state_) state_ = std::make_shared<linux_listener_state>(signal_path_);
        if (!state_->start_worker(std::move(callback))) return;
        linux_notification_service::instance().subscribe(state_);
    }

    void stop_listening() override {
        std::shared_ptr<linux_listener_state> stopped;
        {
            std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            stopped = std::move(state_);
            if (!stopped) return;
            stopped->request_stop();
        }
        linux_notification_service::instance().unsubscribe(stopped);
        stopped->finish_stop();
    }

    [[nodiscard]] bool is_listening() const noexcept override {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        return state_ && state_->listening.load();
    }

private:
    std::string signal_path_;
    mutable std::mutex lifecycle_mutex_;
    std::shared_ptr<linux_listener_state> state_;
};

std::unique_ptr<cross_process_notifier> make_cross_process_notifier(const std::string& db_path) {
    if (db_path.empty() || db_path == ":memory:" ||
        db_path.find(":memory:") != std::string::npos ||
        db_path.find("mode=memory") != std::string::npos) {
        return nullptr;
    }
    return std::make_unique<linux_cross_process_notifier>(db_path);
}

} // namespace lattice

#endif // __linux__ || __ANDROID__
