#pragma once
#include <lattice/db.hpp>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <string>

namespace lattice::detail {
// Private source-qualification bounds, not a shipped configurable budget.
// The qualification graph exercises up to 256 channels and 4 KiB opaque keys.
// Production rollout must choose/configure its budget explicitly. Stored
// values never authorize an allocation above these independent ceilings.
struct receive_guard_limits {
    int64_t channels = 256;
    int64_t channel_bytes = 1024 * 1024;
    int64_t key_bytes = 4096;
    int64_t checkpoint_bytes = 4096;
};
enum class receive_guard_state : int64_t { idle, in_progress, recovery_required, retired, canonical_installed };
enum class receive_guard_reason : int64_t { none, entry_failed, interrupted, legacy_unverified, retired };
struct receive_guard_snapshot {
    bool present = false;
    bool legacy_origin = false;
    bool capacity_refused = false;
    std::string channel;
    int64_t incarnation = 0, generation = 0;
    // Captured budget postimage, used to reject trigger-side counter drift.
    int64_t store_version = 1, store_incarnation = 0, store_channels = 0, store_channel_bytes = 0;
    receive_guard_state state = receive_guard_state::idle;
    receive_guard_reason reason = receive_guard_reason::none;
    // Legacy idle + null is initialized beginning, never an AuditLog fallback.
    // Canonical-installed + null is deliberately not a legacy cursor.
    std::optional<std::string> checkpoint;
    bool operator==(const receive_guard_snapshot& other) const;
};
struct receive_guard_token {
    receive_guard_snapshot admitted;
    bool may_advance = false;
    bool capacity_refused = false;
    // Provisional until the caller verifies its owned intake COMMIT.
};
struct receive_admission_error : db_error {
    std::exception_ptr cause;
    bool durable_intake;
    receive_admission_error(std::exception_ptr error, bool committed)
        : db_error("receive admission refused; stop this route and replay from its prior checkpoint"),
          cause(std::move(error)), durable_intake(committed) {}
};
namespace receive_guard_test_hooks {
// Private deterministic seams; no transport, timers, or public callbacks.
extern thread_local std::function<void()> after_intake_commit;
}

class canonical_install_admission;
struct scoped_recovery_result;
struct receive_delivery_guard_access {
    // Called only by the owned schema-open path, before producer enrollment.
    static void initialize_schema(database&, bool legacy_origin);
    // Actual retained sync/reset ownership is checked, not just autocommit.
    static receive_guard_token begin(lattice_db&, database&, const std::string&);
    static receive_guard_snapshot require_current(lattice_db&, database&, const receive_guard_token&);
    static receive_guard_snapshot finish(lattice_db&, database&, const receive_guard_token&,
        const receive_guard_snapshot& chunk_start, const std::optional<std::string>& prefix, bool failed, bool final_chunk);
    static receive_guard_snapshot read_owned(lattice_db&, database&, const std::string&);
    static void verify_owned(lattice_db&, database&, const receive_guard_snapshot&);
    static receive_guard_snapshot retire(lattice_db&, database&, const receive_guard_snapshot& before);
    // Controller point read retains the physical writer and owns one read view.
    // Refuses an existing caller transaction; it never commits/rolls it back.
    static receive_guard_snapshot read(lattice_db&, const std::string&);
    static std::optional<std::string> legacy_checkpoint(lattice_db&, const std::string&);
    static bool manages_cursor(database&);
    static void require_history_unblocked(database&);
private:
    friend scoped_recovery_result install_staged_canonical_range(const canonical_install_admission&);
    // Explicit canonical-install transition only: preserves the actual guard
    // incarnation, fences old deliveries, and never creates a legacy cursor.
    // The caller must commit this in the actual canonical installation frame.
    // A guard snapshot is not source, scope, route or UNSENT authority.
    static receive_guard_snapshot complete_canonical(lattice_db&, database&, const receive_guard_snapshot&);
    // Exact already-installed retry; checks channel incarnation/generation and
    // canonical state/mirror, permitting unrelated channel budget growth.
    static receive_guard_snapshot verify_canonical_completed(lattice_db&, database&, const receive_guard_snapshot&);
    static sqlite3* owned(lattice_db&, database&);
};
} // namespace lattice::detail
