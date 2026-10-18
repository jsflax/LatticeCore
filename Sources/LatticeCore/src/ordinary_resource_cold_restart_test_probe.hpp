#pragma once
#include <cstddef>

namespace lattice::detail::resource_cold_restart_test {
// An observation/stop seam only. No hook returns installation authority.
enum class cut : unsigned {
    record_pending_created, record_body_synced, record_renamed_before_parent_sync, reservation_durable, vault_created, member_created, entry_created, physical_created,
    stub_created, stub_synced, rename_probe_exclusive, rename_probe_swapped, rename_probe_restored, preparation_synced, ticket_durable,
    member_published, member_parents_synced, all_issuance_durable,
    denial_intent, directory_denied, alias_denied, aliases_denied,
    role_ended, retirement_durable, directory_restored, journal_durable,
    stub_transferred, transfer_synced, aliases_exchanged, exchange_synced,
    original_moved, original_move_synced, original_mode_restored,
    member_record, state_record, root_record, active_record, count
};
inline constexpr const char* names[] = {
    "record-pending-created", "record-body-synced", "record-renamed-before-parent-sync", "reservation-durable", "vault-created", "member-created", "entry-created", "physical-created",
    "stub-created", "stub-synced", "rename-probe-exclusive", "rename-probe-swapped", "rename-probe-restored", "preparation-synced", "ticket-durable",
    "member-published", "member-parents-synced", "all-issuance-durable",
    "denial-intent", "directory-denied", "alias-denied", "aliases-denied",
    "role-ended", "retirement-durable", "directory-restored", "journal-durable",
    "stub-transferred", "transfer-synced", "aliases-exchanged", "exchange-synced",
    "original-moved", "original-move-synced", "original-mode-restored",
    "member-record", "state-record", "root-record", "active-record"
};
static_assert(sizeof(names)/sizeof(*names)==static_cast<unsigned>(cut::count));
#if defined(LATTICE_RESOURCE_COLD_RESTART_TESTING)
using callback_type = void(*)(cut) noexcept;
inline thread_local callback_type callback = nullptr;
inline void at(cut value) noexcept { if(callback) callback(value); }
#else
inline void at(cut) noexcept {}
#endif
}
