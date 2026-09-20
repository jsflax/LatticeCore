#pragma once
#include "receive_install_state.hpp"
#include "sync_canonical_range.hpp"

namespace lattice::detail {
struct canonical_staging_limits {
    int64_t channels, content_pages, identities, content_bytes;
    int64_t receipt_pages, receipts, receipt_bytes, stored_bytes;
    bool operator==(const canonical_staging_limits&) const = default;
};
struct canonical_staging_usage {
    int64_t channels=0, content_pages=0, identities=0, content_bytes=0;
    int64_t receipt_pages=0, receipts=0, receipt_bytes=0, stored_bytes=0;
    bool operator==(const canonical_staging_usage&) const = default;
};
enum class canonical_staging_code {
    transaction_required, invalid_argument, limits_mismatch, corrupt_state,
    capacity, stale_attempt, stale_route, conflicting_page, digest_mismatch,
    not_verified, cleanup_failed
};
class canonical_staging_error : public std::runtime_error {
public:
    canonical_staging_code code;
    std::exception_ptr primary_error, cleanup_error;
    canonical_staging_error(canonical_staging_code c,const char* reason,
        std::exception_ptr primary={},std::exception_ptr cleanup={})
        : std::runtime_error(reason),code(c),primary_error(primary),cleanup_error(cleanup) {}
};
struct canonical_staging_snapshot {
    canonical_range::sequence_state state;
    uint64_t route_generation=0;
    // Whole retained C/E and exact page sequence only. This is not peer/source
    // authority, schema completeness, lease validity or installed model proof.
    bool content_verified=false;
    receive_install_binding installation_binding;
    receive_install_identity installation_identity;
};
struct canonical_staging_begin {
    receive_install_disposition disposition;
    // Empty for exact last-installed retry after explicit staging release.
    std::optional<canonical_staging_snapshot> staged;
};
class canonical_range_staging {
    // Must be the actual owning shared_ptr, never a borrowed/no-op alias.
    std::shared_ptr<lattice_db> owner_;
    receive_install_store installation_;
    receive_install_limits install_limits_;
    canonical_range::limits codec_;
    canonical_staging_limits limits_;
    database& connection() const;
    std::string configuration() const;
    void check_schema() const;
    void audit_usage() const;
    canonical_staging_snapshot addressed(const canonical_range::attempt&,const std::string& manifest,
        std::optional<uint64_t> route) const;
    canonical_staging_snapshot verify_storage(const canonical_staging_snapshot&,bool whole) const;
    void remove_staged(const canonical_staging_snapshot&);
public:
    canonical_range_staging(std::shared_ptr<lattice_db>,receive_install_limits,
        canonical_range::limits,canonical_staging_limits);
    // Every operation requires this thread's actual owned main WRITE frame.
    // No callbacks or outer transaction settlement. Results remain provisional
    // until caller verifies COMMIT; cleanup_failed requires outer rollback.
    void initialize(); // installation store must already be initialized
    void audit() const;
    canonical_staging_usage usage() const; // indexed counters, not a full audit
    canonical_staging_begin begin(const canonical_range::attempt&,const canonical_range::request&,
        const canonical_range::manifest&,uint64_t route_generation);
    canonical_staging_snapshot resume(const canonical_range::attempt&,const std::string& manifest,uint64_t route) const;
    canonical_staging_snapshot append(const canonical_range::frame&);
    canonical_staging_snapshot verify_end(const canonical_range::frame&);
    // Controller must authenticate replacement and fence old physical callbacks
    // before this exact compare-and-swap. A received frame never rebinds itself.
    canonical_staging_snapshot rebind(const canonical_range::attempt&,const std::string& manifest,
        uint64_t expected_route,uint64_t replacement_route);
    // Hold the SAME owned transaction across full verification, these indexed
    // reads and installation. A saved bool from another transaction is no token.
    canonical_range::message read_verified_page(const canonical_range::attempt&,const std::string& manifest,
        uint64_t route,canonical_range::stream_kind,uint64_t index) const;
    // Explicit controller-fenced cleanup. Abandon clears exact active staging
    // and install identity atomically while retaining sequence high water and
    // last installed evidence. Release removes only exactly installed staging.
    // Neither operation touches models, pending intent, outbox or source epochs.
    void abandon_active(const canonical_range::attempt&,const std::string& manifest,uint64_t route);
    void release_installed(const canonical_range::attempt&,const std::string& manifest,uint64_t route);
};
} // namespace lattice::detail
