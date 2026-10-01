#pragma once

#include "lattice/db.hpp"
#include <atomic>
#include <functional>
#include <memory>

namespace lattice::detail {

// Evidence for one physical connection, not store-adoption authority. Retainers
// must keep this object even after the wrapper disappears. In particular, an
// unproved close and raw escape must taint any eventual store generation.
class database_retirement_state {
    friend class lattice::database;
public:
    database_retirement_state() = default;
    enum class phase { live, closed, close_unproved };
    static constexpr int not_attempted = -1;
    phase state() const noexcept { return phase_.load(std::memory_order_acquire); }
    bool raw_handle_escaped() const noexcept { return raw_escaped_.load(std::memory_order_acquire); }
    // Observe a terminal phase first when consuming the corresponding codes.
    int checked_close_result() const noexcept { return checked_close_.load(std::memory_order_acquire); }
    int fallback_close_result() const noexcept { return fallback_close_.load(std::memory_order_acquire); }
private:
    std::atomic<phase> phase_{phase::live};
    std::atomic<bool> raw_escaped_{false};
    std::atomic<int> checked_close_{not_attempted}, fallback_close_{not_attempted};
    void record_close(int checked, int fallback) noexcept {
        checked_close_.store(checked, std::memory_order_relaxed);
        fallback_close_.store(fallback, std::memory_order_relaxed);
        phase_.store(checked == SQLITE_OK ? phase::closed : phase::close_unproved,
                     std::memory_order_release);
    }
};

struct database_retirement_access {
    // Internal readers receive no mutable state and no live SQLite pointer.
    static std::shared_ptr<const database_retirement_state> retain(const database& owner) noexcept {
        return owner.retirement_;
    }
};

namespace database_retirement_test_hooks {
// Constructor-boundary observation/fault only. It cannot replace close results.
// Production leaves current null. The callback is inside constructor cleanup.
struct probe {
    std::function<void(database&, sqlite3*)> after_open;
};
inline thread_local const probe* current = nullptr;
}

} // namespace lattice::detail
