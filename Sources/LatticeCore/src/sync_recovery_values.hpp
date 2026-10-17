#pragma once
#include "sync_recovery_protocol.hpp"
#include <lattice/types.hpp>
#include <map>

namespace lattice::detail::sync_recovery {
using row_values = std::map<std::string, column_value_t>;
struct value_limits {
    size_t raw_bytes;
    size_t fields;
    size_t name_bytes;
    size_t value_bytes;
    // Sum of field-name bytes, one type byte, and the actual scalar bytes
    // (eight for INTEGER/REAL, zero for NULL). Not an RSS guarantee.
    size_t decoded_bytes;
};

// Private snapshot payload codec, not the tolerant legacy AuditLog decoder.
// Preserves SQLite scalar types and exact bytes. Schema/required columns,
// authority, pending overlays and installation are separate checks.
row_values decode_values(std::string_view payload, const value_limits&);
std::string encode_values(const row_values&, const value_limits&);
} // namespace lattice::detail::sync_recovery
