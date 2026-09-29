#pragma once
#include "recovery_receipt_coverage.hpp"

namespace lattice::detail {
using canonical_coverage_query=std::function<std::vector<database::row_t>(const std::string&,const std::vector<column_value_t>&)>;
struct canonical_coverage_state {
    int64_t mutation=0,origins=0,origin_bytes=0,cells=0,cell_bytes=0;
    bool operator==(const canonical_coverage_state&)const=default;
};
// Internal bounded storage utilities. These functions grant no connection,
// transaction or upstream authority; their caller supplies an already-owned
// writer or held source read view and the actual immutable enrolled profile.
std::map<std::string,std::string> canonical_coverage_schema();
void initialize_canonical_coverage(database&,const canonical_coverage_profile&);
canonical_coverage_state read_canonical_coverage(const canonical_coverage_query&,const canonical_coverage_profile&);
void audit_canonical_coverage(const canonical_coverage_query&,const canonical_coverage_profile&);
enum class canonical_coverage_lookup { no_original,legacy_original_namespace,missing,covered };
canonical_coverage_lookup lookup_canonical_coverage(const canonical_coverage_query&,const canonical_coverage_profile&,
    const std::string& original,const std::string& ns,const recovery_receipt_binding&,const std::string& digest,
    const std::optional<std::string>& operation=std::nullopt);
int64_t canonical_origin_charge(const recovery_producer_registration&);
int64_t canonical_coverage_charge(const std::string&);
}
