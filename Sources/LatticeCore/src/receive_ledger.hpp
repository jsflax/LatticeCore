#pragma once

#include <cstdint>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice {
class lattice_db;
class database;
namespace detail {

// Private storage primitive. No wire, apply, retention, migration, or public
// SDK contract is implied. Values are encoded byte strings, never ordered IDs.
struct receive_ledger_limits {
    int64_t channels;
    int64_t channel_id_bytes;
    int64_t identities;
    int64_t identity_bytes;
    int64_t identities_per_channel;
    int64_t identity_bytes_per_channel;
    int64_t checkpoint_bytes_per_channel;
    bool operator==(const receive_ledger_limits&) const = default;
};

enum class receive_ledger_error_code {
    transaction_required, invalid_argument, corrupt_state, limits_mismatch,
    stale_token, identity_missing, disposition_conflict, invalid_acceptance,
    sequence_exhausted, cleanup_failed
};
class receive_ledger_error : public std::runtime_error {
public:
    receive_ledger_error_code code;
    std::exception_ptr primary_error, cleanup_error;
    receive_ledger_error(receive_ledger_error_code c, const std::string& message,
                         std::exception_ptr primary = {}, std::exception_ptr cleanup = {})
        : std::runtime_error(message), code(c), primary_error(primary), cleanup_error(cleanup) {}
};

// Tokens are provisional until the caller verifies its owning transaction
// COMMIT. Discard every token from a rolled-back/uncertain intake. Only committed
// incarnations/generations are nonreused; this is not a nonce allocator for
// unpublished tokens. Copying this value does not confer owner/lifetime rights.
struct receive_ledger_token {
    std::string channel;
    int64_t incarnation = 0;
    int64_t generation = 0;
    bool operator==(const receive_ledger_token&) const = default;
};

enum class receive_checkpoint_kind { absent, initialized_null, value };
enum class receive_intake_disposition : int64_t {
    known_schema = 0, unknown_schema_policy = 1, filter_policy = 2
};
enum class receive_acceptance : int64_t { pending = 0, applied = 1, no_op = 2, policy = 3 };
struct receive_identity_request {
    std::string id;
    receive_intake_disposition disposition;
};
struct receive_identity_state {
    receive_intake_disposition disposition;
    receive_acceptance acceptance;
};
struct receive_ledger_snapshot {
    receive_checkpoint_kind kind = receive_checkpoint_kind::absent;
    std::optional<std::string> checkpoint;
    receive_ledger_token token;
    bool overflow = false;
    int64_t identities = 0;
    int64_t identity_bytes = 0;
    int64_t pending = 0;
};
struct receive_store_usage {
    int64_t channels = 0;
    int64_t channel_id_bytes = 0;
    int64_t identities = 0;
    int64_t identity_bytes = 0;
    bool channel_overflow = false;
};
struct receive_reservation {
    receive_ledger_token token;
    bool admitted = false;
    int64_t new_identities = 0;
    int64_t new_identity_bytes = 0;
};

class receive_ledger {
    lattice_db& owner_;
    receive_ledger_limits limits_;
    database& connection() const;
    int64_t check_schema() const;
    receive_ledger_snapshot channel_row(const std::string&) const;
public:
    // Every operation requires an already active owned write transaction on
    // owner_'s current physical writer. This prototype uses Core's explicit
    // transaction ownership check; future maintenance-scope integration needs
    // its own reviewed private authority, not an autocommit-only substitute.
    // Owner/connection must survive the complete caller-owned transaction.
    // cleanup_failed retains both failures and requires outer rollback/refusal;
    // it is never permission to commit a partially restored helper operation.
    receive_ledger(lattice_db& owner, receive_ledger_limits limits);
    // Initialize once on store admission. Existing stores receive a full
    // integrity/budget audit here; hot identity operations validate addressed
    // state with indexed lookups, not repeated whole-ledger scans.
    void initialize();
    void audit() const;
    receive_ledger_snapshot read(const std::string& channel) const;
    receive_store_usage usage() const;
    // Fresh initialized-NULL channels only. No legacy import or reset.
    // nullopt means fixed channel capacity was refused; COMMIT the sticky
    // store overflow record before treating that refusal as durable.
    std::optional<receive_ledger_token> create(const std::string& channel);
    void assert_current(const receive_ledger_token&) const;
    // Fences the preceding generation. Reserves the entire unique-ID set or
    // none of its new IDs. Existing dispositions are immutable. A refusal
    // still returns the new provisional generation and persists overflow.
    // Commit intake before effects; storage/commit failure permits no effects.
    receive_reservation reserve(const receive_ledger_token&,
                                const std::vector<receive_identity_request>&);
    std::optional<receive_identity_state> identity(const receive_ledger_token&,
                                                  const std::string& id) const;
    // Call within the SAME entry transaction/savepoint as all acceptance
    // effects. This is bookkeeping, not proof that the caller applied them.
    // Known-schema identities cannot complete through policy acceptance.
    void complete(const receive_ledger_token&, const std::string& id, receive_acceptance);
    // Explicit destructive channel retirement, never ordinary disconnect or
    // pending==0 cleanup. Old committed tokens cannot recreate missing state.
    // Caller must fence its other work before choosing this operation.
    void retire(const receive_ledger_token&);
};
} // namespace detail
} // namespace lattice
