#pragma once
#include "observer_delivery_meter.hpp"
#include "ordinary_schema_validation.hpp"
#include <lattice/recovery_schema.hpp>

namespace lattice::detail {
// Read-only actual-schema/profile observation, never enrollment or SQL grant.
// A detected versioned MAIN ledger requires the complete generated guard
// bundle; absence preserves the original strict validator. No trust boolean,
// supplied digest, arbitrary SQL alternative or per-trigger mixing is accepted.
struct observer_delivery_schema {
    static std::vector<observer_delivery_meter_table> tables(const recovery_owner_schema&);
    static std::vector<observer_delivery_meter_program> programs(const recovery_owner_schema&);
    static ordinary_schema_metadata validate(const recovery_owner_schema&,
        const ordinary_schema_projection&,const ordinary_metadata_query&,
        const ordinary_metadata_limits& = {});
    // Uses the existing pre-copy bounded retained-view metadata adapter. The
    // caller must already own its actual main transaction/serialized handle;
    // this performs no BEGIN, DDL, callback, enrollment or marker repair.
    static void validate_database(database&,const recovery_owner_schema&);
};
}
