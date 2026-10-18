#pragma once

#include "lattice/db.hpp"
#include <atomic>
#include <functional>
#include <memory>
#include <cstdint>
#include <array>
#include <limits>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace lattice::detail {

// Evidence for one physical connection, not store-adoption authority. Retainers
// must keep this object even after the wrapper disappears. In particular, an
// unproved close and raw escape must taint any eventual store generation.
class database_retirement_state {
    friend class lattice::database;
    friend class ordinary_export_access;
public:
    database_retirement_state() noexcept {
        for (auto& cell : topology_wait_cells_)
            cell.deadline.store(no_topology_waiter_, std::memory_order_relaxed);
    }
    enum class phase { live, closed, close_unproved };
    static constexpr int not_attempted = -1;
    phase state() const noexcept { return phase_.load(std::memory_order_acquire); }
    bool owning_wrapper_moved() const noexcept { return owning_wrapper_moved_.load(std::memory_order_acquire); }
    bool raw_handle_escaped() const noexcept { return raw_escaped_.load(std::memory_order_acquire); }
    // Observe a terminal phase first when consuming the corresponding codes.
    int checked_close_result() const noexcept { return checked_close_.load(std::memory_order_acquire); }
    int fallback_close_result() const noexcept { return fallback_close_.load(std::memory_order_acquire); }
private:
    // Private bounded witness for accepted-baseline capacity continuity.
    // Plain metadata only; its guarded deleter never touches an inherited native leaf.
    std::shared_ptr<void> ordinary_baseline_capacity_witness_;
    // Per-physical-connection handoff DATA follows the original through moves.
    // A cell exists only while its real topology scope retries SQLite admission;
    // it carries that operation's original deadline, never SQL/close authority.
    using topology_clock_ = std::chrono::steady_clock;
    using topology_tick_ = topology_clock_::duration::rep;
    static constexpr topology_tick_ no_topology_waiter_ = std::numeric_limits<topology_tick_>::min();
    struct topology_wait_cell_ {
        // Zero is free; UINT64_MAX is a reserved, not-yet-published cell.
        std::atomic<uint64_t> token{0};
        std::atomic<topology_tick_> deadline;
    };
    std::array<topology_wait_cell_, 64> topology_wait_cells_;
    std::atomic<uint64_t> topology_wait_serial_{0}; // Never wraps or resets.
    std::atomic<size_t> topology_waiter_count_{0};
    // Source-private six-table history capacity continuity; not issuer state.
    std::shared_ptr<void> ordinary_history_capacity_witness_;
    // Native connection-owned, monotone saturating program-mutation sequence.
    // SQL/callers cannot restore it through schema_version or durable metadata.
    std::atomic<uint64_t> ordinary_history_mutation_epoch_{0};
    static uint64_t ordinary_history_process() noexcept {
#if defined(__APPLE__) || defined(__linux__)
        return static_cast<uint64_t>(::getpid());
#else
        return 0;
#endif
    }
    const uint64_t ordinary_history_creator_pid_=ordinary_history_process();
    std::atomic<phase> phase_{phase::live};
    // Denial-only UNKNOWN catalog cleanup. Actual SQLite admission publishes
    // this once before unlock; the physical retirement original carries it
    // through public wrapper moves. No clear/retry/refund API exists.
    std::atomic<bool> catalog_cleanup_quarantined_{false};
    std::atomic<bool> raw_escaped_{false};
    std::atomic<bool> owning_wrapper_moved_{false}; // Real move, not caller-set completion.
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
