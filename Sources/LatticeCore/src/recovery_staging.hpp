#pragma once

#include "receive_ledger.hpp"
#include "sync_recovery_protocol.hpp"
#include <exception>

namespace lattice::detail {

// Counts reserve the complete declared snapshot at begin; stored_bytes charges
// actual serialized BLOB/key bytes including JSON/escaping and the state image.
// These logical bounds are not SQLite file-size, allocator or RSS guarantees.
struct recovery_staging_limits {
    int64_t channels, pages, rows, canonical_bytes, stored_bytes;
    bool operator==(const recovery_staging_limits&) const = default;
};
struct recovery_staging_usage {
    int64_t channels = 0, pages = 0, rows = 0, canonical_bytes = 0, stored_bytes = 0;
};
enum class recovery_staging_code {
    transaction_required, invalid_argument, corrupt_state, limits_mismatch,
    capacity, stale_attempt, conflicting_page, digest_mismatch, not_verified, cleanup_failed
};
class recovery_staging_error : public std::runtime_error {
public:
    recovery_staging_code code;
    std::exception_ptr primary_error, cleanup_error;
    recovery_staging_error(recovery_staging_code c, const char* reason,
                           std::exception_ptr primary = {}, std::exception_ptr cleanup = {})
        : std::runtime_error(reason), code(c), primary_error(primary), cleanup_error(cleanup) {}
};
struct recovery_staging_snapshot {
    sync_recovery::staging_state state;
    // Only exact staged content bytes are verified. This is not source
    // authority, schema validity, installed models or checkpoint acceptance.
    bool content_verified = false;
};

// SHA-256 over the codec's canonical three length-prefixed strings per row.
// Literal bytes only; callers still validate UTF-8/schema/ordering separately.
std::string canonical_rows_sha256(const std::vector<sync_recovery::row>& rows);

class recovery_staging {
    lattice_db& owner_;
    receive_ledger ledger_; // Same writer context; tokens are not global capabilities.
    sync_recovery::limits codec_;
    recovery_staging_limits limits_;
    int64_t channel_bytes_;
    database& connection() const;
    std::string configuration() const;
    void check_schema() const;
    void audit_usage() const;
    recovery_staging_snapshot addressed(const receive_ledger_token&,
                                         const sync_recovery::binding&) const;
    recovery_staging_snapshot verify_storage(const receive_ledger_token&,
                                             const sync_recovery::binding&, bool whole) const;
public:
    recovery_staging(lattice_db&, receive_ledger_limits,
                     sync_recovery::limits, recovery_staging_limits);
    // All operations require this thread's actual owned main WRITE transaction.
    // The caller retains the owner through checked COMMIT/ROLLBACK; returned
    // state is provisional until COMMIT. Ledger initialization is a prerequisite.
    void initialize();
    void audit() const;
    // Cached logical usage; audit/resume/end recount actual durable storage.
    recovery_staging_usage usage() const;
    // Same token/binding/manifest resumes exactly; a different attempt refuses.
    // No implicit discard, eviction, token generation bump or reset exists.
    recovery_staging_snapshot begin(const receive_ledger_token&, const sync_recovery::manifest&,
                                    const sync_recovery::binding& expected);
    // Reopen/resume re-streams stored pages and checks their actual counts/hash.
    recovery_staging_snapshot resume(const receive_ledger_token&, const sync_recovery::binding&) const;
    // Existing identical canonical page is idempotent without another charge;
    // current token and attempt checks run first, even for duplicates.
    recovery_staging_snapshot append(const receive_ledger_token&, const sync_recovery::page&);
    recovery_staging_snapshot verify_end(const receive_ledger_token&, const sync_recovery::end&);
    // One owned page, never models or installation authority. The installer
    // retains one owned writer transaction across verify_end, page reads and
    // installation so content cannot change after whole-snapshot verification.
    sync_recovery::page read_verified_page(const receive_ledger_token&, const sync_recovery::binding&,
                                           uint64_t page_index) const;
};
} // namespace lattice::detail
