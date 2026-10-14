#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace lattice::detail {
// Passive observation retained by the closed local route. Neither this value,
// its checksum, nor its durable presence grants import, deletion, enrollment,
// completed repair or a canonical receipt. The actual source capture must
// separately prove the upstream association before requesting materialization.
struct ordinary_managed_contribution_observation final {
    std::string source,scope,schema,digest,record_seal;
    std::int64_t epoch=0,cut=0;
    std::string table,row_gid,original_gid;
    // Exact original intent remains in envelope (including field/name JSON).
    // Projection changes do not rewrite it or identify a new authored edit.
    // digest and record_seal preserve observed custody coordinates only.
    std::vector<std::uint8_t> envelope,descriptor,projection;
};
struct ordinary_managed_contribution_observation_result final {
    std::string observation_seal;
    bool exact_replay=false;
    // Diagnostic coexistence only. It is NOT a chosen winner, a reason to
    // delete a row, or proof the alternate contribution is currently visible.
    bool other_lineage_observed=false;
};
}
