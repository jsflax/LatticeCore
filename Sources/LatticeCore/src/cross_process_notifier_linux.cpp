#if defined(__linux__) || defined(__ANDROID__)

#include "lattice/cross_process_notifier.hpp"
#include "lattice/log.hpp"

#include <sys/inotify.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <thread>
#include <atomic>

namespace lattice {

class linux_cross_process_notifier final : public cross_process_notifier {
public:
    explicit linux_cross_process_notifier(const std::string& db_path)
        : signal_path_(db_path + "-signal") {
        // Ensure signal file exists
        int fd = open(signal_path_.c_str(), O_CREAT | O_WRONLY, 0644);
        if (fd >= 0) close(fd);
        LOG_DEBUG("xproc", "Linux notifier created with signal file: %s", signal_path_.c_str());
    }

    ~linux_cross_process_notifier() override {
        stop_listening();
    }

    void post_notification() override {
        LOG_DEBUG("xproc", "Posting Linux notification via: %s", signal_path_.c_str());
        int fd = open(signal_path_.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd >= 0) {
            char byte = 1;
            // Write + close triggers IN_CLOSE_WRITE
            (void)write(fd, &byte, 1);
            close(fd);
        } else {
            LOG_ERROR("xproc", "Failed to open signal file: %s", strerror(errno));
        }
    }

    void start_listening(std::function<void()> callback) override {
        if (listening_.load()) return;

        callback_ = std::move(callback);

        // Create shutdown pipe
        if (pipe(shutdown_pipe_) != 0) {
            LOG_ERROR("xproc", "Failed to create shutdown pipe: %s", strerror(errno));
            return;
        }

        // Create inotify instance
        inotify_fd_ = inotify_init();
        if (inotify_fd_ < 0) {
            LOG_ERROR("xproc", "inotify_init failed: %s", strerror(errno));
            close(shutdown_pipe_[0]);
            close(shutdown_pipe_[1]);
            return;
        }

        // Watch for IN_CLOSE_WRITE on the signal file
        watch_fd_ = inotify_add_watch(inotify_fd_, signal_path_.c_str(), IN_CLOSE_WRITE);
        if (watch_fd_ < 0) {
            LOG_ERROR("xproc", "inotify_add_watch failed: %s", strerror(errno));
            close(inotify_fd_);
            close(shutdown_pipe_[0]);
            close(shutdown_pipe_[1]);
            return;
        }

        listening_.store(true);

        // Background event loop thread
        thread_ = std::thread([this] {
            LOG_DEBUG("xproc", "Linux listener thread started");
            char buf[sizeof(struct inotify_event) + NAME_MAX + 1];

            while (listening_.load()) {
                fd_set fds;
                FD_ZERO(&fds);
                FD_SET(inotify_fd_, &fds);
                FD_SET(shutdown_pipe_[0], &fds);

                int max_fd = std::max(inotify_fd_, shutdown_pipe_[0]) + 1;
                int ret = select(max_fd, &fds, nullptr, nullptr, nullptr);

                if (ret < 0) {
                    if (errno == EINTR) continue;
                    LOG_ERROR("xproc", "select failed: %s", strerror(errno));
                    break;
                }

                // Shutdown signal
                if (FD_ISSET(shutdown_pipe_[0], &fds)) {
                    LOG_DEBUG("xproc", "Shutdown signal received");
                    break;
                }

                // inotify event
                if (FD_ISSET(inotify_fd_, &fds)) {
                    ssize_t len = read(inotify_fd_, buf, sizeof(buf));
                    if (len > 0 && callback_) {
                        callback_();
                    }
                }
            }
            LOG_DEBUG("xproc", "Linux listener thread exiting");
        });

        LOG_DEBUG("xproc", "Linux listener registered for: %s", signal_path_.c_str());
    }

    void stop_listening() override {
        if (!listening_.exchange(false)) return;

        // Wake the event loop
        char byte = 0;
        (void)write(shutdown_pipe_[1], &byte, 1);

        if (thread_.joinable()) {
            thread_.join();
        }

        if (watch_fd_ >= 0) inotify_rm_watch(inotify_fd_, watch_fd_);
        if (inotify_fd_ >= 0) close(inotify_fd_);
        close(shutdown_pipe_[0]);
        close(shutdown_pipe_[1]);

        watch_fd_ = -1;
        inotify_fd_ = -1;
        callback_ = nullptr;

        LOG_DEBUG("xproc", "Linux listener stopped for: %s", signal_path_.c_str());
    }

    [[nodiscard]] bool is_listening() const noexcept override {
        return listening_.load();
    }

private:
    std::string signal_path_;
    int inotify_fd_ = -1;
    int watch_fd_ = -1;
    int shutdown_pipe_[2] = {-1, -1};
    std::atomic<bool> listening_{false};
    std::function<void()> callback_;
    std::thread thread_;
};

std::unique_ptr<cross_process_notifier> make_cross_process_notifier(const std::string& db_path) {
    if (db_path.empty() || db_path == ":memory:") {
        return nullptr;
    }
    return std::make_unique<linux_cross_process_notifier>(db_path);
}

} // namespace lattice

#endif // __linux__ || __ANDROID__
