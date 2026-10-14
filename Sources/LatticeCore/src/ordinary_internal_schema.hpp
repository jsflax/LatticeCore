#pragma once
#include "ordinary_schema_validation.hpp"

namespace lattice::detail {
struct ordinary_internal_schema_metadata {
    int64_t schema_cookie=0,temp_schema_cookie=0;
    size_t inspected_rows=0,copied_bytes=0;
    std::map<std::string,ordinary_table_metadata> tables;
    std::vector<ordinary_object_metadata> objects;
    // Exact stored spelling, including union parent order. Set comparison is
    // only for descriptor membership; these values are never issuance facts.
    std::map<std::string,std::string> internal_parents;
};
// Must run AFTER validate_ordinary_schema's targeted selected-module and
// shadow preflight, on that same retained view, adapter and aggregate pre-copy
// budget. Matching cookies alone do not prove this ordering or view custody.
// The caller retains the real engine's trusted SQL functions/collations and
// cannot allow application callbacks or schema mutation between inspections.
// Expected internal parents come from the actual registered catalog/projection,
// never wire metadata. This is read-only observation, not enrollment authority.
ordinary_internal_schema_metadata validate_ordinary_internal_schema(
    const recovery_owner_schema&,const ordinary_schema_projection&,
    const ordinary_schema_metadata& inspected_projection,
    const ordinary_metadata_query&,const ordinary_metadata_limits& = {});
} // namespace lattice::detail
