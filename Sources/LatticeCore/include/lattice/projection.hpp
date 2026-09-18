#pragma once

#include "types.hpp"
#include <cstdint>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace lattice {
class lattice_db;
class projection_service;
struct projection_operation_state;
struct projection_handle_lifetime;

enum class projection_status : int32_t {
    batch = 0, done = 1, cancelled = 2, deadline_exceeded = 3,
    row_budget_exceeded = 4, byte_budget_exceeded = 5, snapshot_expired = 6,
    unsupported = 7, schema_changed = 8, database_failure = 9,
    invalid_request = 10, closed = 11, concurrent_next = 12, admission_rejected = 13
};

struct physical_store_identity;
struct database_read_control;
struct projection_store_ticket_state;

/// A WAL hook touches only these atomics. Weak registry references retain no
/// database/parent. A detached/closed writer deactivates its pressure sources.
struct projection_pressure_source {
    std::shared_ptr<const physical_store_identity> identity;
    std::shared_ptr<void> registry_anchor; // Keeps only the per-store epoch alive.
    std::atomic<uint64_t> raised{0}, acknowledged{0};
    std::atomic<bool> active{true};
    void raise() noexcept { if (active.load()) raised.fetch_add(1); }
    bool pending() const noexcept { return active.load() && raised.load() != acknowledged.load(); }
    void acknowledge(uint64_t through) noexcept {
        auto old = acknowledged.load();
        while (old < through && !acknowledged.compare_exchange_weak(old, through)) {}
    }
};

/// Admission ticket captures retirement epochs before any private open/pin.
/// Publication checks all epochs and all live pressure tokens atomically under
/// the registry mutex. A stale/busy ticket fails immediately, without restart.
class projection_store_ticket {
public:
    projection_store_ticket() = default;
    projection_status publish(uint64_t operation_id, const std::shared_ptr<database_read_control>& control) const;
    void release() noexcept;
private:
    friend projection_store_ticket capture_projection_stores(const std::vector<std::shared_ptr<const physical_store_identity>>&);
    std::shared_ptr<projection_store_ticket_state> state_;
};
projection_store_ticket capture_projection_stores(const std::vector<std::shared_ptr<const physical_store_identity>>& identities);
std::shared_ptr<projection_pressure_source> make_projection_pressure_source(const std::shared_ptr<const physical_store_identity>& identity);
void retire_projection_store(const std::shared_ptr<const physical_store_identity>& identity,
                             projection_status reason = projection_status::snapshot_expired);
size_t projection_store_readers(const std::shared_ptr<const physical_store_identity>& identity);

struct projection_bounds {
    std::string column;
    double min_lat = 0, max_lat = 0, min_lon = 0, max_lon = 0;
};

/// Internal SQL shape, produced by the bridge's typed stored-column adapter.
/// Distinct groups first, then group_by; SQLite chooses an unspecified member
/// for non-grouped values. No representative identity/stability is promised.
struct projection_query {
    std::string table;
    std::vector<std::string> columns;
    std::string where_clause, order_by, group_by, distinct_by;
    /// Stored columns needed by ORDER BY after distinct/group nesting. A native
    /// raw ORDER BY caller must supply these dependencies; the builder never
    /// widens the inner projection to every stored field as a fallback.
    std::vector<std::string> order_columns;
    std::vector<column_value_t> parameters;
    int64_t limit = -1;
    int64_t offset = 0;
    int64_t max_rows = 100000;
    int64_t max_copied_bytes = 8 * 1024 * 1024;
    int64_t timeout_ms = 5000;
    std::optional<projection_bounds> bounds;
    bool has_bounds = false; // Compatibility flag alone is an invalid request.
};

/// Immutable shared backing; only the current batch belongs to the library.
/// Copied bytes count NULL=0, INTEGER/REAL=8, UTF-8 text bytes and BLOB bytes.
/// This is an extraction budget, not SQLite workspace or total allocator/RSS.
class projection_read_batch {
public:
    int32_t status_code() const noexcept { return static_cast<int32_t>(status_); }
    const std::string& error_message() const noexcept { return error_; }
    int64_t row_count() const noexcept;
    int64_t column_count() const noexcept { return columns_; }
    column_value_t value(int64_t row, int64_t column) const;
    int64_t cumulative_rows() const noexcept { return cumulative_rows_; }
    int64_t cumulative_copied_bytes() const noexcept { return cumulative_bytes_; }
    bool is_complete() const noexcept { return status_ == projection_status::done; }
private:
    friend struct projection_operation_state;
    friend class projection_read_operation;
    projection_status status_ = projection_status::invalid_request;
    std::string error_;
    int64_t columns_ = 0, cumulative_rows_ = 0, cumulative_bytes_ = 0;
    std::shared_ptr<const std::vector<column_value_t>> cells_;
};

/// Value handle with a shared PImpl; no FRT, actor, or platform-age dependency.
/// next_batch is synchronous and must run on an SDK worker. cancel/close only
/// signal; idle-resource cleanup belongs to the shared Core watchdog.
/// The watchdog has 10ms cadence; cancellation is cooperative during OS/VFS
/// calls. A full batch is not a terminal lookahead: one further pull observes
/// DONE (possibly empty). DONE/error releases resources before returning,
/// except concurrent-next misuse, whose caller must await when_released.
/// 64 pending handles and 2 active leases per parent are explicit bounds.
/// Result payload budgets exclude SQLite workspace and caller-retained copies.
class projection_read_operation {
public:
    projection_read_operation() = default;
    uint64_t operation_id() const noexcept;
    projection_read_batch next_batch(int64_t max_rows) const;
    void cancel() const noexcept;
    void close() const noexcept;
    bool is_terminal() const noexcept;
    bool has_resources() const noexcept;
    /// One fixed callback slot. First nonnull registration always succeeds;
    /// false means null callback or duplicate registration. Invoked once outside
    /// all locks when terminal and resource-free (inline if already released).
    /// Register after close/cancel; pre-open registration alone does not close.
    bool when_released(void* context, void (*callback)(void*)) const noexcept;
private:
    friend class lattice_db;
    friend class projection_service;
    explicit projection_read_operation(std::shared_ptr<projection_operation_state> state);
    std::shared_ptr<projection_handle_lifetime> handle_;
};
} // namespace lattice
