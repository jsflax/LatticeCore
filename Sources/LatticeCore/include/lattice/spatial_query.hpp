#pragma once

#include <array>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice {

/// One physical arm of a model query. Metadata discovery belongs to the
/// caller's snapshot/connection; this builder performs no SQL or allocation of
/// row values. A list has an intermediate parent_id/globalId relationship.
struct spatial_query_arm {
    std::string schema;
    bool is_list = false;
};

inline std::string spatial_quoted_identifier(const std::string& name) {
    if (name.empty() || name.find('\0') != std::string::npos)
        throw std::invalid_argument("invalid spatial SQL identifier");
    std::string quoted = "\"";
    for (const char c : name) { quoted += c; if (c == '"') quoted += '"'; }
    return quoted + '"';
}

inline std::string spatial_quoted_literal(const std::string& value) {
    if (value.find('\0') != std::string::npos)
        throw std::invalid_argument("invalid spatial SQL literal");
    std::string quoted = "'";
    for (const char c : value) { quoted += c; if (c == '\'') quoted += '\''; }
    return quoted + '\'';
}

/// Coordinates are bound, in minLat/maxLat/minLon/maxLon order. Numbered
/// placeholders are reused for every physical arm; predicate bindings may
/// precede them. The source discriminator follows rebuild_attached_views:
/// main uses "main", while an attached arm stores its quoted SQL identifier
/// (including the quotes) in _source.
///
/// EXISTS prevents matching list entries from multiplying model rows. Physical
/// schema qualification plus the source guard prevents an equal local id (or
/// replicated globalId) in another file from borrowing this arm's match.
inline std::string build_spatial_membership_predicate(
    const std::string& table, const std::string& geo_column,
    const std::vector<spatial_query_arm>& arms, bool routed,
    const std::array<std::string, 4>& coordinate_sql) {
    const auto model = spatial_quoted_identifier(table);
    // Longer than the outer name, so an unusual model named like an internal
    // alias cannot shadow the correlated table reference inside EXISTS.
    const auto r = spatial_quoted_identifier(table + "_lattice_geo_r");
    const auto l = spatial_quoted_identifier(table + "_lattice_geo_l");
    const auto sidecar = "_" + table + "_" + geo_column;
    (void)spatial_quoted_identifier(geo_column);
    for (const auto& placeholder : coordinate_sql) {
        if (placeholder.size() < 2 || placeholder[0] != '?' || placeholder[1] == '0' ||
            placeholder.find_first_not_of("0123456789", 1) != std::string::npos)
            throw std::invalid_argument("spatial bounds require numbered parameters");
    }
    if (arms.empty()) return "0";
    std::string predicate = "(";
    bool first = true;
    for (const auto& arm : arms) {
        if (!first) predicate += " OR ";
        first = false;
        const auto schema = spatial_quoted_identifier(arm.schema);
        const auto rtree = schema + '.' + spatial_quoted_identifier(sidecar + "_rtree");
        predicate += '(';
        if (routed) {
            const auto label = arm.schema == "main" ? std::string("main") : schema;
            predicate += model + ".\"_source\" = " + spatial_quoted_literal(label) + " AND ";
        }
        predicate += "EXISTS (SELECT 1 FROM " + rtree + " AS " + r;
        if (arm.is_list) {
            predicate += " JOIN " + schema + '.' + spatial_quoted_identifier(sidecar) +
                " AS " + l + " ON " + l + ".\"id\" = " + r + ".\"id\"";
        }
        predicate += " WHERE ";
        predicate += arm.is_list
            ? l + ".\"parent_id\" = " + model + ".\"globalId\""
            : r + ".\"id\" = " + model + ".\"id\"";
        predicate += " AND " + r + ".\"minLat\" <= " + coordinate_sql[1] +
            " AND " + r + ".\"maxLat\" >= " + coordinate_sql[0] +
            " AND " + r + ".\"minLon\" <= " + coordinate_sql[3] +
            " AND " + r + ".\"maxLon\" >= " + coordinate_sql[2] + "))";
    }
    return predicate + ')';
}

} // namespace lattice
