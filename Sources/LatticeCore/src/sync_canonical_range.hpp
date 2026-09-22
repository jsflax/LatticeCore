#pragma once
#include "sync_recovery_values.hpp"
#include <optional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

// Private, inert v2 framing. No dispatcher, SQL staging, source/receipt authority,
// physical route admission, controller or installer calls are introduced here.
namespace lattice::detail::canonical_range {
using protocol_error = sync_recovery::protocol_error;
struct attempt {
    std::string receiver_incarnation, channel_incarnation, channel;
    // Durable per-bound-channel attempt counter, shared with receiver install.
    // Physical reconnect never resets it; it must exceed expected revision.
    uint64_t sequence = 0;
    std::string attempt_id;
    bool operator==(const attempt&) const = default;
};
struct source_binding {
    std::string authority, source_id, epoch, scope_digest, schema_digest;
    bool operator==(const source_binding&) const = default;
};
struct identity {
    std::string table, id;
    bool operator==(const identity&) const = default;
};
enum class frontier_kind { uninitialized, beginning_null, position };
struct frontier {
    frontier_kind kind = frontier_kind::uninitialized;
    std::optional<uint64_t> value;
    bool operator==(const frontier&) const = default;
};
struct expected_install {
    uint64_t revision = 0;
    std::optional<source_binding> binding;
    frontier base;
    bool operator==(const expected_install&) const = default;
};
enum class mode { full, delta };
struct wire_limits {
    uint64_t frame_bytes = 0, payload_bytes = 0, items_per_page = 0;
    uint64_t content_pages = 0, content_identities = 0, content_bytes = 0;
    uint64_t receipt_pages = 0, receipts = 0, receipt_bytes = 0;
    bool operator==(const wire_limits&) const = default;
};
struct limits {
    wire_limits maximum;
    size_t depth = 0, nodes = 0, string_bytes = 0;
    size_t request_entries = 0, request_targets = 0, request_target_bytes = 0;
    size_t restart_bytes = 0;
    uint64_t lease_ms = 0;
    sync_recovery::value_limits values{};
};
struct receipt_request {
    std::string original_id;
    // null is unknown provenance, not an implicitly covered legacy namespace.
    std::optional<std::string> namespace_id;
    std::vector<identity> targets;
    bool operator==(const receipt_request&) const = default;
};
struct request {
    source_binding source;
    mode selection = mode::full;
    std::optional<uint64_t> base; // capture B; full requires null
    expected_install expected;  // independently exact prior receiver state
    wire_limits budget;
    std::vector<receipt_request> receipts;
    std::string request_digest;
    bool operator==(const request&) const = default;
};
struct lease {
    std::string id;
    uint64_t duration_ms = 0;
    bool operator==(const lease&) const = default;
};
struct totals {
    uint64_t content_pages = 0, identities = 0, present = 0, tombstones = 0, content_bytes = 0;
    uint64_t receipt_pages = 0, receipts = 0, receipt_bytes = 0;
    uint64_t rebase_identities = 0, rebase_bytes = 0;
    bool operator==(const totals&) const = default;
};
struct manifest {
    std::string request_digest;
    source_binding source;
    mode selection = mode::full;
    std::optional<uint64_t> base;
    uint64_t head = 0;
    lease protection;
    totals counts;
    std::string content_digest, receipt_digest, rebase_digest, manifest_digest;
    bool operator==(const manifest&) const = default;
};
struct present {
    std::string payload; // exact strict SQLite scalar value-codec JSON bytes
    bool operator==(const present&) const = default;
};
struct tombstone { bool operator==(const tombstone&) const = default; };
struct content_item {
    identity key;
    std::variant<present, tombstone> value;
    bool operator==(const content_item&) const = default;
};
enum class decision { applied, no_op, policy };
struct committed {
    std::string namespace_id, coverage_id;
    decision outcome = decision::applied;
    uint64_t position = 0;
    std::optional<identity> accepted_target;
    bool operator==(const committed&) const = default;
};
struct not_committed {
    std::string namespace_id, coverage_id;
    bool operator==(const not_committed&) const = default;
};
enum class unknown_reason { legacy, missing_coverage, retired_coverage, source_changed, unproved_provenance };
struct unknown {
    unknown_reason reason = unknown_reason::unproved_provenance;
    bool operator==(const unknown&) const = default;
};
struct receipt_item {
    std::string original_id;
    std::variant<committed, not_committed, unknown> value;
    bool operator==(const receipt_item&) const = default;
};
struct content_page {
    std::string manifest_digest;
    uint64_t index = 0, count = 0, bytes = 0;
    std::string digest;
    std::vector<content_item> items;
    bool operator==(const content_page&) const = default;
};
struct receipt_page {
    std::string manifest_digest;
    uint64_t index = 0, count = 0, bytes = 0;
    std::string digest;
    std::vector<receipt_item> items;
    bool operator==(const receipt_page&) const = default;
};
struct end {
    std::string manifest_digest;
    bool operator==(const end&) const = default;
};
using message = std::variant<request, manifest, content_page, receipt_page, end>;
struct frame {
    attempt logical;
    uint64_t route_generation = 0; // spelling only; caller MUST authenticate/fence
    message body;
    bool operator==(const frame&) const = default;
};

// All public helpers validate their input budgets/DTO bounds. Hash functions
// ignore only the digest field they compute, not the other bound fields.
std::string request_sha256(const attempt&, const request&, const limits&);
std::string manifest_sha256(const manifest&, const limits&);
std::string rebase_sha256(const attempt&, const request&, const limits&);
uint64_t content_record_bytes(const content_item&, const limits&);
uint64_t receipt_record_bytes(const receipt_item&, const limits&);
std::string page_sha256(const content_page&, const limits&);
std::string page_sha256(const receipt_page&, const limits&);
enum class stream_kind { content, receipts };
// Bounded streaming form of the same canonical encoding used by whole helpers.
// Retains only manifest/limits, counters and the last key, never prior records.
// Discard after any append/finish refusal. A digest is integrity, not authority.
class stream_hasher {
    struct state;
    std::unique_ptr<state> state_;
public:
    stream_hasher(const manifest&, stream_kind, const limits&);
    ~stream_hasher();
    stream_hasher(stream_hasher&&) noexcept;
    stream_hasher& operator=(stream_hasher&&) noexcept;
    stream_hasher(const stream_hasher&) = delete;
    stream_hasher& operator=(const stream_hasher&) = delete;
    void append(const content_item&);
    void append(const receipt_item&);
    std::string finish();
};
// Whole-stream helpers consume caller-owned bounded vectors; never issue SQL.
// M binds page counts, not boundaries for an equal page count. Exact retained
// page bytes/page hashes, not M alone, enforce immutable retransmission.
std::string content_sha256(const manifest&, const std::vector<content_item>&, const limits&);
std::string receipts_sha256(const manifest&, const std::vector<receipt_item>&, const limits&);
frame decode(std::string_view, const limits&);
std::string encode(const frame&, const limits&);

enum class phase { receiving, sequence_complete_unverified };
struct sequence_state {
    attempt logical;
    request frozen_request;
    manifest offer;
    phase status = phase::receiving;
    uint64_t next_content_page = 0, next_receipt_page = 0;
    uint64_t identities = 0, present_count = 0, tombstone_count = 0, content_bytes = 0;
    uint64_t receipt_count = 0, receipt_bytes = 0;
    std::optional<identity> last_identity;
    std::vector<uint8_t> rebase_seen; // one bounded 0/1 flag per sorted union identity
    bool operator==(const sequence_state&) const = default;
};
sequence_state begin(const attempt&, const request&, const manifest&, const limits&);
// Pure proposals: success/refusal never mutates current. Physical route checks
// are caller work even for identical retry. Sequence completion is NOT proof
// of whole hashes, durable page existence, source authority or installation.
sequence_state propose(const sequence_state&, const frame&, const limits&);
std::string encode_state(const sequence_state&, const limits&);
sequence_state decode_state(std::string_view, const attempt& expected, const limits&);
} // namespace lattice::detail::canonical_range
