#pragma once
#include "ordinary_schema_projection.hpp"
#include <lattice/db.hpp>
#include <functional>
#include <optional>

namespace lattice::detail {
// Trusted synchronous adapter over the caller's retained main snapshot. It
// must charge TEXT/BLOB bytes BEFORE copying and reject the (row_limit+1)th
// row. The validator additionally checks aggregate result bounds. No callback
// may run arbitrary application code or release/change the retained view.
using ordinary_metadata_query = std::function<std::vector<database::row_t>(
    const std::string&, const std::vector<column_value_t>&, size_t row_limit)>;
struct ordinary_metadata_limits {
    size_t objects=65536, rows=262144, bytes=32*1024*1024, sql_bytes=256*1024;
};
struct ordinary_column_metadata {
    std::string name,type;
    int64_t cid=0,not_null=0,primary_key=0,hidden=0;
    std::optional<std::string> default_sql;
};
struct ordinary_object_metadata { std::string type,name,owner,sql; };
struct ordinary_unique_index_metadata {
    std::string name,table,create_sql;
    std::vector<std::string> columns;
};
struct ordinary_table_metadata {
    std::string name;
    std::vector<ordinary_column_metadata> columns;
};
struct ordinary_derived_metadata {
    ordinary_derived_kind kind;
    std::string model,property,table,create_sql;
    bool present=false;
    // nprobe==0 means the generated declaration omitted that option; the
    // pinned module supplies its default. It never means zero actual probes.
    size_t dimensions=0,ivf_nlist=0,ivf_nprobe=0;
};
struct ordinary_schema_metadata {
    int64_t schema_cookie=0,temp_schema_cookie=0;
    size_t inspected_rows=0,copied_bytes=0;
    // Includes verified module shadow tables; application row scope is still
    // the caller's projection.tables, never this metadata map's key set.
    std::map<std::string,ordinary_table_metadata> tables;
    std::vector<ordinary_object_metadata> objects;
    // Only explicit generated uniqueness constraints. Never PK/globalId
    // autoindexes. The caller owns atomic suspension and exact restoration.
    std::vector<ordinary_unique_index_metadata> unique_indexes;
    std::vector<ordinary_derived_metadata> derived;
};
class ordinary_schema_validation_error : public std::runtime_error {
public: using std::runtime_error::runtime_error;
};
// Describes observed metadata only. No source/receipt/install authority, row
// scans, SQL mutation, repair, or callback installation. Physical ALTER-column
// defaults/nullability/order may differ between logically equal declarations;
// actual physical constraints remain visible to the caller's value preflight.
// Only selected modules are connected. Module shadow ownership derives from
// exact pinned module programs and actual closed DDL/index/column metadata;
// this API does not separately observe SQLite's internal TF_Shadow bit.
ordinary_schema_metadata validate_ordinary_schema(const ordinary_schema_projection&,
    const ordinary_metadata_query&, const ordinary_metadata_limits& = {});
} // namespace lattice::detail
