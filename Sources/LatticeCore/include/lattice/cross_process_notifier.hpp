#pragma once

#include <functional>
#include <memory>
#include <string>

namespace lattice {

/// Abstract interface for cross-process database change notifications.
/// Implementations use platform-specific IPC to signal when a commit occurs,
/// allowing other processes with the same DB open to refresh their observers.
class cross_process_notifier {
public:
    virtual ~cross_process_notifier() = default;

    /// Post a "something changed" notification to other processes.
    virtual void post_notification() = 0;

    /// Start listening for notifications from other processes.
    /// The callback is invoked on a background thread/queue when a notification arrives.
    virtual void start_listening(std::function<void()> callback) = 0;

    /// Stop listening and clean up resources.
    virtual void stop_listening() = 0;

    /// Returns true if currently listening for notifications.
    [[nodiscard]] virtual bool is_listening() const noexcept = 0;
};

/// Factory: creates the platform-appropriate notifier for the given DB path.
/// Returns nullptr for in-memory databases or unsupported platforms.
std::unique_ptr<cross_process_notifier> make_cross_process_notifier(const std::string& db_path);

} // namespace lattice
