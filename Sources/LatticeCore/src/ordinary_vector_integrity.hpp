#pragma once
#include "ordinary_schema_validation.hpp"
#include <stdexcept>

namespace lattice::detail {
struct ordinary_vector_integrity_limits {
    size_t rows=8192, bytes=128*1024*1024;
};
struct ordinary_vector_integrity {
    size_t rows=0, chunks=0, cells=0, centroids=0;
    size_t inspected_rows=0, copied_bytes=0;
    bool trained=false;
};
class ordinary_vector_integrity_error : public std::runtime_error {
public: using std::runtime_error::runtime_error;
};
// Read-only post-rebuild proof for the pinned, generated single-f32-vector
// flat/IVF profile. Exact module/shadow SQL must already have been validated.
// The trusted callback retains the same serialized main WRITE throughout and
// applies its cumulative byte bound BEFORE copying each TEXT/BLOB. It must not
// call application code. This additionally bounds the aggregate observations.
//
// `actual` describes the module now present (including dimensions inferred for
// a formerly absent index). `expect_trained` describes the operation attempted,
// not evidence: the actual marker, centroids, maps, slots and bytes are checked.
// No mutation, repair, training, source/receipt authority or KNN quality claim.
// This is a fresh-rebuild profile: empty chunks/cells are refused, and trained
// centroids must be the complete dense min(nlist, actual vector count) set.
ordinary_vector_integrity validate_ordinary_vector_integrity(
    const ordinary_derived_metadata& actual, const ordinary_metadata_query&,
    bool expect_trained, const ordinary_vector_integrity_limits& = {});
} // namespace lattice::detail
