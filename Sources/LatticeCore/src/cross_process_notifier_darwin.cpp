#ifdef __APPLE__

#include "lattice/cross_process_notifier.hpp"
#include "lattice/log.hpp"

#include <notify.h>
#include <dispatch/dispatch.h>
#include <CommonCrypto/CommonDigest.h>
#include <climits>
#include <cstdlib>

namespace lattice {

/// Derives a Darwin notification key from the database path.
/// Uses SHA-256 of the resolved absolute path to avoid key collisions.
static std::string derive_notification_key(const std::string& db_path) {
    // Resolve symlinks and relative components
    char resolved[PATH_MAX];
    const char* real = realpath(db_path.c_str(), resolved);
    const std::string canonical = real ? std::string(real) : db_path;

    // SHA-256 hash
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(canonical.data(), static_cast<CC_LONG>(canonical.size()), hash);

    // Convert to hex string
    char hex[CC_SHA256_DIGEST_LENGTH * 2 + 1];
    for (int i = 0; i < CC_SHA256_DIGEST_LENGTH; ++i) {
        snprintf(hex + i * 2, 3, "%02x", hash[i]);
    }

    return "com.lattice.db." + std::string(hex, CC_SHA256_DIGEST_LENGTH * 2);
}

class darwin_cross_process_notifier final : public cross_process_notifier {
public:
    explicit darwin_cross_process_notifier(const std::string& db_path)
        : key_(derive_notification_key(db_path)) {
        LOG_DEBUG("xproc", "Darwin notifier created with key: %s", key_.c_str());
    }

    ~darwin_cross_process_notifier() override {
        stop_listening();
    }

    void post_notification() override {
        LOG_DEBUG("xproc", "Posting Darwin notification: %s", key_.c_str());
        notify_post(key_.c_str());
    }

    void start_listening(std::function<void()> callback) override {
        if (listening_) return;

        callback_ = std::move(callback);

        uint32_t status = notify_register_dispatch(
            key_.c_str(),
            &token_,
            dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
            ^(int) {
                if (callback_) {
                    callback_();
                }
            }
        );

        if (status == NOTIFY_STATUS_OK) {
            listening_ = true;
            LOG_DEBUG("xproc", "Darwin listener registered for: %s", key_.c_str());
        } else {
            LOG_ERROR("xproc", "Failed to register Darwin notification (status=%u)", status);
        }
    }

    void stop_listening() override {
        if (!listening_) return;
        notify_cancel(token_);
        listening_ = false;
        callback_ = nullptr;
        LOG_DEBUG("xproc", "Darwin listener cancelled for: %s", key_.c_str());
    }

    [[nodiscard]] bool is_listening() const noexcept override {
        return listening_;
    }

private:
    std::string key_;
    int token_ = 0;
    bool listening_ = false;
    std::function<void()> callback_;
};

std::unique_ptr<cross_process_notifier> make_cross_process_notifier(const std::string& db_path) {
    // No cross-process notifications for in-memory databases
    if (db_path.empty() || db_path == ":memory:") {
        return nullptr;
    }
    return std::make_unique<darwin_cross_process_notifier>(db_path);
}

} // namespace lattice

#endif // __APPLE__
