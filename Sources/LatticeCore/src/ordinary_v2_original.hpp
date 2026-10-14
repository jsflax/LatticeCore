#pragma once
#include <lattice/sync.hpp>
#include <functional>
#include <span>

namespace lattice::detail {
using ordinary_v2_retained_charge=std::function<void(size_t)>;
// Pure, synchronous value codec, never an issuer. The caller retains unchanged
// input bytes/entry through return and supplies its real retained-budget charge.
// Charge runs before the first retained allocation/copy. Decoding checks the
// entire flat versioned envelope, counts, kinds, scalar bounds and duplicate
// names before admission; bounded stack-only views do not escape this call.
bool is_ordinary_v2_original(std::span<const uint8_t>)noexcept;
std::vector<uint8_t> encode_ordinary_v2_original(const audit_log_entry&,const ordinary_v2_retained_charge&);
audit_log_entry decode_ordinary_v2_original(std::span<const uint8_t>,const ordinary_v2_retained_charge&);
// Separate local-retention envelope. Provisional public inputs may have no
// sender-local AuditLog id (zero), or an arbitrary signed peer-local id. Keep
// that value verbatim; it is not a wire ordering coordinate or source grant.
// Only already admitted local helper/conflict custody selects these APIs.
// The strict original codec above and the authenticated wire parser continue
// to require positive original ids and never accept this envelope's magic.
bool is_ordinary_v2_retained_entry(std::span<const uint8_t>)noexcept;
std::vector<uint8_t> encode_ordinary_v2_retained_entry(const audit_log_entry&,const ordinary_v2_retained_charge&);
audit_log_entry decode_ordinary_v2_retained_entry(std::span<const uint8_t>,const ordinary_v2_retained_charge&);
}
