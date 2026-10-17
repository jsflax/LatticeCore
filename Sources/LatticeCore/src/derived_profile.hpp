#pragma once

#include <lattice/db.hpp>
#include <lattice/schema.hpp>
#include <lattice/derived_program.hpp>
#include <functional>
#include <stdexcept>

namespace lattice::detail {

enum class derived_kind { fts5_porter_v1, vec0_flat_f32_v1 };
struct derived_spec {
    std::string model;
    std::string property;
    derived_kind kind = derived_kind::fts5_porter_v1;
    size_t dimensions = 0;
};

// Counts and returned field-byte charges are bounded independently, including
// before-copy SQL projections. Internal copies, SQL/module work and allocator
// overhead are NOT an RSS or SQLite VM bound. No function installs, replaces
// or clears a progress handler; existing retained read control stays in force.
struct derived_limits {
    size_t properties = 32;
    size_t metadata_rows = 4096;
    size_t metadata_bytes = 4 * 1024 * 1024;
    size_t sql_bytes = 128 * 1024;
    size_t initial_rows = 4096;
    size_t value_bytes = 8 * 1024 * 1024;
    size_t total_value_bytes = 64 * 1024 * 1024;
};
struct derived_profile_error : std::runtime_error {
    using std::runtime_error::runtime_error;
};
struct derived_field {
    derived_spec spec;
    bool nullable = false;
    bool no_history = false;
    std::string table;
    std::string create_sql;
    std::vector<derived_trigger_program> triggers;
};
class derived_descriptor {
public:
    const std::vector<derived_field>& fields() const noexcept { return fields_; }
private:
    std::vector<derived_field> fields_;
    friend derived_descriptor describe_derived(const std::vector<model_schema>&,
        const std::vector<derived_spec>&, const derived_limits&);
};

derived_descriptor describe_derived(const std::vector<model_schema>& models,
    const std::vector<derived_spec>& specs, const derived_limits& limits = {});

// The trusted internal callback must execute the supplied SELECT on one
// already-retained main snapshot/owned writer, preserving that custody for all
// calls. It must propagate cancellation/errors. A schema cookie alone does not
// establish a pinned view. This component never creates a transaction/lease.
using derived_query = std::function<std::vector<database::row_t>(
    const std::string&, const std::vector<column_value_t>&)>;
struct derived_object_fact {
    std::string type;
    std::string name;
    std::string owner;
    std::string sql;
};
struct derived_metadata {
    int64_t schema_cookie = 0;
    size_t inspected_rows = 0;
    size_t copied_bytes = 0; // cumulative accepted projected field bytes
    std::vector<derived_object_fact> objects;
};

// Facts only, valid within the caller's view. No source/route, origin, receipt,
// complete-owner-trigger, index-content or installation authority is returned.
// The complete raw main schema inventory is admitted before scoped lookups;
// a large unrelated schema can therefore conservatively refuse this profile.
derived_metadata validate_derived_schema(const derived_query& query,
    const derived_descriptor& expected, const derived_limits& limits = {});
derived_metadata validate_derived_schema(database& db,
    const derived_descriptor& expected, const derived_limits& limits = {});

struct derived_value_usage { size_t rows = 0; size_t bytes = 0; };
// Already-owned final row values; only declared derived fields are inspected.
// Missing fields refuse. Usage is changed only after the complete row passes.
void validate_derived_values(const derived_descriptor& expected,
    const std::string& model, const database::row_t& values,
    derived_value_usage& usage, const derived_limits& limits = {});

// Explicit one-time admission scan, separate from metadata/reopen validation.
// Requires the same retained view as validate_derived_schema. Never call this
// automatically on reopen or per frame; growth is not a new startup row cap.
// SELECT projections check sizes/types before copying BLOB/TEXT. No MATCH/KNN,
// lazy ensure, reconciliation, repairs, shadow writes or model changes occur.
derived_value_usage validate_initial_derived_values(const derived_query& query,
    const derived_descriptor& expected, const derived_limits& limits = {});
derived_value_usage validate_initial_derived_values(database& db,
    const derived_descriptor& expected, const derived_limits& limits = {});

} // namespace lattice::detail
