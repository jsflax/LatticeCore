#pragma once
#include "ordinary_v2_original.hpp"
#include <memory>
#include <string_view>

namespace lattice::detail {
struct ordinary_v2_audit_values {
    changed_fields_map fields;
    std::vector<std::string> names;
};
using ordinary_v2_workspace_acquire=std::function<std::shared_ptr<void>(size_t)>;
// Borrowed input is unchanged through this synchronous call. The real private
// workspace ticket is retained across all SAX/DOM use; DOMs die before it.
// Output allocations are separately charged before copying. A parsed value
// grants no source, schema, original identity, or receiving authority.
ordinary_v2_audit_values parse_ordinary_v2_audit_values(std::string_view fields,std::string_view names,
    const ordinary_v2_retained_charge&,const ordinary_v2_workspace_acquire&);
}
