#pragma once
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#if defined(NDEBUG)
#error "Receiving-owner reclamation qualification is forbidden in release builds"
#endif
#if !defined(__APPLE__)
#error "This receiving-owner qualification requires the genuine macOS installed image harness"
#endif
#include "ordinary_target_staging_connection.hpp"
#include <cstdint>
#include <memory>
namespace lattice::detail {
class ordinary_receiver_wss_request;
class ordinary_receiver_wss_access;
class ordinary_group_target_receiving_owner;
// Qualification-only observation of a real authenticated owner. No method
// constructs an admission/request/target or accepts paths, responses or grants.
class ordinary_receiver_reclamation_fixture final {
    friend class ordinary_receiver_wss_access;
    friend class ordinary_group_target_receiving_owner;
    struct state;
    static state& original();
    static void checkpoint(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        ordinary_group_target_receiving_owner&);
    static void capture(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        const std::shared_ptr<ordinary_group_target_receiving_owner>&)noexcept;
    using disposition=ordinary_target_staging_connection::abandoned_retirement_result;
    static disposition observe_disposition(ordinary_group_target_receiving_owner&,disposition)noexcept;
    static void observe_ordinary_fallback(ordinary_group_target_receiving_owner&)noexcept;
public:
    enum bit : std::uint64_t {
        captured=1ull<<0,checkpoint_reached=1ull<<1,disposition_joined=1ull<<2,no_fallback=1ull<<3,
        first_sqlite_close_ok=1ull<<4,first_unlink_ok=1ull<<5,held_zero_links=1ull<<6,name_absent=1ull<<7,
        first_directory_sync_ok=1ull<<8,first_main_close_ok=1ull<<9,native_files_joined=1ull<<10,
        collected=1ull<<11,worker_joined=1ull<<12,service_released=1ull<<13,
        actual_unstarted_lane_cancelled=1ull<<14,actual_close_cleanup_complete=1ull<<15,
        extra_entry_created_and_closed=1ull<<16,disposition_debt=1ull<<17,
        unlink_never_entered=1ull<<18,still_uncollected=1ull<<19,actual_close_no_error=1ull<<20
    };
    static std::uint64_t close_and_observe()noexcept;
};
}
#endif
