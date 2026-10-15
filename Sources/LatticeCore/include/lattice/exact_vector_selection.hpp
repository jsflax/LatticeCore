#pragma once

#include "db.hpp"
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice::detail {

// Internal candidate selection only. This is not a public nearest policy and
// does not pick a physical row to hydrate for a replicated globalId.
enum class exact_vector_metric { l2, cosine, l1 };

struct exact_vector_candidate {
    std::string global_id;
    double distance;
};

struct exact_vector_predicate {
    // Trusted query-builder expression, referring to the physical model as m.
    // As with database::query, values are positional: use ordinary anonymous ?
    // parameters, not numbered/named parameters, and supply exactly the matching
    // number of values. This is a single trusted expression, not arbitrary SQL.
    // These are caller preconditions, not a new SQL parser/validation boundary.
    // SQL is copied verbatim; SQLite handles quoted/commented question marks.
    // Bindings are repeated per arm, after the query vector and before k.
    std::string sql;
    std::vector<column_value_t> bindings;
};

inline std::string exact_vector_identifier(const std::string& name) {
    if (name.empty() || name.find('\0') != std::string::npos)
        throw std::invalid_argument("invalid exact vector SQL identifier");
    std::string result = "\"";
    for (const auto c : name) { result += c; if (c == '"') result += '"'; }
    return result + '"';
}

// Caller supplies the already discovered, current physical schemas (raw names,
// not SQL-quoted names), each containing the model and its populated float32
// vec0 sidecar. Caller authenticates registered model/property/type and supplies
// a dimension-compatible query. Identifier quoting does not prove that schema.
// Caller owns connection/topology lifetime and any required store gate.
// There is no discovery, reconciliation/backfill, transaction, or raw-handle
// escape here. An empty vector, zero k, or no arms returns empty without SQL.
// Otherwise one SELECT ranks all eligible sidecar rows using sqlite-vec's
// requested metric, MINs distance per globalId across arms, then limits by k.
// This has SQLite's statement/attached-database read semantics, not an atomic
// wall-clock snapshot across independently written database files.
//
// Exact means no candidate truncation before eligibility and metric ranking;
// metric arithmetic is still sqlite-vec's arithmetic on float32 inputs.
// Undefined/nonfinite distances in ANY eligible row reject the read, even
// outside the final top k.
// Existing public nearest validation/defaults/hydration remain separate.
inline std::vector<exact_vector_candidate> select_exact_vector_candidates(
    database& db, const std::string& model, const std::string& vector_column,
    const std::vector<std::string>& schemas, const std::vector<float>& query,
    size_t k, exact_vector_metric metric,
    const exact_vector_predicate& predicate = {}) {
    if (query.empty() || k == 0 || schemas.empty()) return {};
    if (query.size() > 8192 || k > static_cast<size_t>(std::numeric_limits<int64_t>::max()))
        throw std::invalid_argument("exact vector dimensions or k out of range");
    bool nonzero = false;
    for (const auto value : query) {
        if (!std::isfinite(value)) throw std::invalid_argument("nonfinite exact query vector");
        nonzero = nonzero || value != 0.0f;
    }
    const char* function = nullptr;
    switch (metric) {
        case exact_vector_metric::l2: function = "vec_distance_L2"; break;
        case exact_vector_metric::cosine:
            if (!nonzero) throw std::invalid_argument("undefined exact cosine query");
            function = "vec_distance_cosine";
            break;
        case exact_vector_metric::l1: function = "vec_distance_L1"; break;
        default: throw std::invalid_argument("invalid exact vector metric");
    }
    if (predicate.sql.empty() && !predicate.bindings.empty())
        throw std::invalid_argument("exact predicate bindings without SQL");
    if (db.is_closed()) throw db_error("exact vector database is closed");

    const auto table = exact_vector_identifier(model);
    (void)exact_vector_identifier(vector_column);
    const auto sidecar = exact_vector_identifier("_" + model + "_" + vector_column + "_vec");
    std::vector<uint8_t> blob(query.size() * sizeof(float));
    std::memcpy(blob.data(), query.data(), blob.size());
    std::vector<column_value_t> bindings{std::move(blob)};
    std::string sql = "WITH __lattice_exact_query AS (SELECT ? AS embedding), "
        "__lattice_exact_rows AS (";
    for (size_t i = 0; i < schemas.size(); ++i) {
        if (i) sql += " UNION ALL ";
        const auto schema = exact_vector_identifier(schemas[i]);
        sql += "SELECT m.globalId AS gid, " + std::string(function) +
            "(v.embedding, (SELECT embedding FROM __lattice_exact_query)) AS distance "
            "FROM " + schema + '.' + sidecar + " AS v JOIN " + schema + '.' + table +
            " AS m ON m.globalId = v.global_id";
        if (!predicate.sql.empty()) {
            // Newlines also preserve a valid trailing -- comment in the input.
            sql += " WHERE (\n" + predicate.sql + "\n)";
            bindings.insert(bindings.end(), predicate.bindings.begin(), predicate.bindings.end());
        }
    }
    // MIN ignores NULL, so carry a separate invalid bit through grouping and
    // across the entire eligible set BEFORE LIMIT. Never turn NULL into zero or
    // silently let a finite replica hide another replica's undefined distance.
    sql += "), __lattice_exact_groups AS (SELECT gid, MIN(distance) AS distance, "
        "MAX(CASE WHEN distance IS NULL OR distance > 1.7976931348623157e308 "
        "OR distance < -1.7976931348623157e308 THEN 1 ELSE 0 END) AS invalid "
        "FROM __lattice_exact_rows GROUP BY gid) "
        "SELECT gid, distance, MAX(invalid) OVER () AS invalid_distance "
        "FROM __lattice_exact_groups ORDER BY distance ASC, gid COLLATE BINARY ASC LIMIT ?";
    bindings.emplace_back(static_cast<int64_t>(k));

    const auto rows = db.query(sql, bindings);
    std::vector<exact_vector_candidate> result;
    result.reserve(rows.size());
    for (const auto& row : rows) {
        if (std::get<int64_t>(row.at("invalid_distance")) != 0)
            throw db_error("undefined or nonfinite exact vector distance");
        const auto distance = std::get<double>(row.at("distance"));
        if (!std::isfinite(distance)) throw db_error("nonfinite exact vector distance");
        result.push_back({std::get<std::string>(row.at("gid")), distance});
    }
    return result;
}

} // namespace lattice::detail
