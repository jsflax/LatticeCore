#pragma once
#include "sync_canonical_range.hpp"

namespace lattice::detail::canonical_range {
struct package_limits {
    limits codec;
    // Independent per-package bounds, including manifest/end and JSON
    // envelopes. No service-wide admission, input ownership or RSS guarantee.
    uint64_t retained_wire_bytes = 0;
    uint64_t frames = 0;
};

class encoded_package;
encoded_package assemble_package(const attempt&, uint64_t route_generation,
    const request&, uint64_t head, const lease&,
    const std::vector<content_item>&, const std::vector<receipt_item>&,
    const package_limits&);

// A move-only, immutable encoding result. Integrity/structural completeness
// only: NOT a send permit, authenticated source, namespace coverage proof,
// durable spool, live tail lease or installation admission. In particular,
// the supplied lease/receipt DTOs are not upgraded into authority here.
// The caller must retain this object through uses of its const byte views.
class encoded_package {
    friend encoded_package assemble_package(const attempt&, uint64_t,
        const request&, uint64_t, const lease&,
        const std::vector<content_item>&, const std::vector<receipt_item>&,
        const package_limits&);
    manifest offer_;
    std::vector<std::string> frames_;
    uint64_t bytes_ = 0;
    encoded_package() = default;
public:
    encoded_package(encoded_package&&) noexcept = default;
    encoded_package& operator=(encoded_package&&) noexcept = default;
    encoded_package(const encoded_package&) = delete;
    encoded_package& operator=(const encoded_package&) = delete;
    const manifest& offer() const noexcept { return offer_; }
    const std::vector<std::string>& frames() const noexcept { return frames_; }
    uint64_t retained_wire_bytes() const noexcept { return bytes_; }
};

// Synchronous, pure framing. Input vectors must remain unchanged for the call;
// the result retains no input references and exposes nothing before success.
// Preserves exact input order and receipt classifications: no sorting away
// duplicates, truncation, inferred negative receipts or mutable retry pages.
// Page packing bounds actual escaped JSON size, parser nodes/depth and item
// count. Conservative decimal-envelope space can leave a few bytes unused.
// These finite implementation caps allow <=512 MiB and <=65536 output frames;
// they do not promise that every application snapshot fits recovery policy.
}
