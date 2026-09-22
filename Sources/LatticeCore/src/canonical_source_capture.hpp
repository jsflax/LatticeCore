#pragma once
#include "canonical_change_store.hpp"
#include "sync_snapshot_source.hpp"

namespace lattice::detail { class canonical_writer_adapter; }
namespace lattice::detail::sync_recovery {
struct canonical_capture_request {
    std::string original_id; // normalized UUID comparison key
    std::vector<canonical_identity> targets; // complete receipt-rebase union
    std::optional<std::string> namespace_id; // explicit v2 request; legacy must omit
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
// A declared numeric base is a receiver claim, not evidence of installation.
// No authority/receipt namespace is inferred from these source binding bytes.
enum class source_capture_selection { full, delta, requires_full_request };
struct owned_canonical_capture {
    canonical_store_binding binding;
    std::string descriptor_digest;
    source_capture_selection selection = source_capture_selection::full;
    std::optional<int64_t> requested_base;
    int64_t head=0, floor=0;
    // Absent for a retired delta base: only a NEW explicit full request may
    // obtain rows. H/F alone never authorize a frontier, send or installation.
    std::optional<unsealed_canonical_capture> capture;
};
// Internal factoring, callable only by the actual adapter. It does not mint
// source authority and does not replace the explicitly trusted legacy helper.
class canonical_source_session_access {
    friend class lattice::detail::canonical_writer_adapter;
    static owned_canonical_capture capture(lattice_db&, const canonical_store_binding&,
        const std::vector<source_relation>&, std::optional<int64_t>,
        const std::vector<canonical_capture_request>&, const canonical_capture_limits&,
        const std::function<void(uint64_t)>& verify_generation,
        const std::function<void(size_t,uint64_t)>& after_batch,
        const canonical_namespace_profile* = nullptr);
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
