#include "lattice/sync_commit_probe.hpp"

#if defined(LATTICE_SYNC_COMMIT_PROBE)
#include <chrono>
#include <cstring>
#include <limits>
#include <type_traits>

namespace lattice {
namespace {
struct probe_slot {
    const void* owner = nullptr;
    sqlite3* connection = nullptr;
    bool active = false;
    bool armed = false;
    sync_commit_probe_receipt receipt;
};
// Trivial constant initialization; no allocation or destructor registration on
// first access from a SQLite frame. The token never crosses native threads.
constinit thread_local probe_slot slot;
static_assert(std::is_trivially_copyable_v<probe_slot>);
static_assert(std::is_trivially_destructible_v<probe_slot>);

void increment_bounded(uint64_t& value) noexcept {
    if (value != std::numeric_limits<uint64_t>::max()) ++value;
}
} // namespace

uint64_t sync_commit_probe_clock_ns() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

namespace sync_commit_probe_detail {
int32_t arm(const void* owner, sqlite3* connection,
            uint64_t operation, uint64_t attempt) noexcept {
    if (!operation || !attempt) return 1;
    if (slot.active) return 2;
    slot.owner = owner;
    slot.connection = connection;
    slot.receipt = {};
    slot.receipt.status = 1;
    slot.receipt.operation_id = operation;
    slot.receipt.attempt_id = attempt;
    slot.receipt.armed_ns = sync_commit_probe_clock_ns();
    slot.receipt.owner_identity = reinterpret_cast<uintptr_t>(owner);
    slot.receipt.connection_identity = reinterpret_cast<uintptr_t>(connection);
    slot.receipt.thread_identity = reinterpret_cast<uintptr_t>(&slot);
    slot.active = true;
    slot.armed = true;
    return 0;
}

void record(const void* owner, sqlite3* connection, const char* schema) noexcept {
    if (!slot.active || !slot.armed) return;
    if (slot.owner != owner || slot.connection != connection) {
        increment_bounded(slot.receipt.ignored_owner_commits);
        return;
    }
    if (!schema || std::strcmp(schema, "main") != 0) {
        increment_bounded(slot.receipt.ignored_schema_commits);
        return;
    }
    // Consume BEFORE any future observer/successor commit can execute. Keep
    // active set until finish: callback reentrancy cannot rearm this scope.
    slot.armed = false;
    slot.receipt.postcommit_ns = sync_commit_probe_clock_ns();
    slot.receipt.status = 0;
}

sync_commit_probe_receipt snapshot() noexcept { return slot.receipt; }

void rolled_back(const void* owner, sqlite3* connection) noexcept {
    if (slot.active && slot.owner == owner && slot.connection == connection)
        slot.armed = false;
}

sync_commit_probe_receipt finish(const void* owner, uint64_t operation,
                                  uint64_t attempt) noexcept {
    if (!slot.active) return {};
    if (slot.owner != owner || slot.receipt.operation_id != operation ||
        slot.receipt.attempt_id != attempt) {
        sync_commit_probe_receipt mismatch;
        mismatch.status = 3;
        return mismatch;
    }
    const auto receipt = slot.receipt;
    slot = {};
    return receipt;
}
} // namespace sync_commit_probe_detail
} // namespace lattice
#endif
