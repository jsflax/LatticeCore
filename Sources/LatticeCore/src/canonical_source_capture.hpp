#pragma once
#include "canonical_change_store.hpp"
#include "sync_snapshot_source.hpp"

namespace lattice::detail::sync_recovery {
struct canonical_capture_request {
    std::string original_id; // normalized UUID comparison key
    std::vector<canonical_identity> targets; // complete receipt-rebase union
};
struct canonical_capture_limits {
    source_limits rows;
    canonical_store_limits store;
    uint64_t requests, requested_targets, marker_batch;
};
struct canonical_source_row {
    canonical_identity key; // normalized UUID, even for tombstones
    // Present payload includes the source's actual globalId spelling. Local
    // integer IDs remain absent. nullopt is an explicit absent current row.
    std::optional<std::string> payload;
};
struct canonical_source_receipt {
    std::string original_id;
    // Missing is UNKNOWN. This capture does not prove negative coverage or
    // manufacture a negotiated operation namespace from receipt absence.
    std::optional<canonical_receipt> stored;
};
struct unsealed_canonical_capture {
    int64_t head=0, floor=0, schema_cookie=0;
    std::vector<source_layout> layouts;
    std::vector<canonical_source_row> rows;
    std::vector<canonical_source_receipt> receipts;
    uint64_t copied_logical_bytes=0;
};
// Private read-only, synchronous, file-WAL-only storage capture. The caller
// retains the actual owner throughout. Source admission/complete schema,
// namespace coverage, epoch rotation, leases, durable spool and wire sealing
// are separate obligations. No source serving capability is conferred.
// Full (null base): every source row plus all requested absent targets.
// Delta: last-touch identities in (B,H] plus EVERY requested target, including
// a receipt accepted before B. Binding, H/F, receipts and values use one view.
// Finite policy refusal returns no usable prefix or head. Logical copied-byte
// accounting is not v2 canonical byte accounting, SQL work or process RSS.
unsealed_canonical_capture capture_canonical_source(lattice_db&,
    const canonical_store_binding&, const std::vector<source_relation>&,
    std::optional<int64_t> base, const std::vector<canonical_capture_request>&,
    const canonical_capture_limits&);
namespace source_test_hooks {
unsealed_canonical_capture capture_canonical(lattice_db&,
    const canonical_store_binding&, const std::vector<source_relation>&,
    std::optional<int64_t>, const std::vector<canonical_capture_request>&,
    const canonical_capture_limits&, const std::function<void(size_t,uint64_t)>& after_batch);
}
} // namespace lattice::detail::sync_recovery
