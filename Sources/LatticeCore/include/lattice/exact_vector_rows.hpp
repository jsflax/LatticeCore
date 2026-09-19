#pragma once

#include "exact_vector_selection.hpp"
#include <algorithm>
#include <cctype>
#include <set>
#include <type_traits>
#include <utility>

namespace lattice::detail {

// Internal physical-row stage, not a public nearest policy or managed-model
// lifetime boundary. The owner authenticates the registered model, vector
// property, COMPLETE physical column list, and current schema/token pairs on
// this connection. It retains the connection and topology for this operation.
// Root references in each trusted predicate refer to that physical row as m;
// relationship subqueries retain the caller's explicitly rendered scope.
struct exact_vector_row_arm {
    std::string schema; // raw schema name; not an SQL-quoted spelling
    int64_t attachment_token = 0; // main == 0, authenticated attachments > 0
    exact_vector_predicate predicate;
};

struct exact_vector_row {
    database::row_t row; // full supplied column image + _source/attach token
    double distance;
};

struct exact_vector_rows_plan {
    std::string sql;
    std::vector<column_value_t> bindings;
};

inline std::string exact_vector_fold_identifier(std::string value) {
    // SQLite identifier equality folds ASCII, not arbitrary Unicode case.
    for (auto& c : value) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return value;
}

// No sidecar, shortlist, repair or later globalId lookup participates. NULL is
// an absent optional vector; every other eligible canonical value must be a
// dimension-compatible float32 BLOB. SQLite-vec validates its component values.
// The SQL uses MATERIALIZED CTEs (SQLite >= 3.35) and window functions. Full
// scans/materialization may use unbounded SQLite temporary storage; k bounds
// returned rows only. Independently written attached files retain SQLite's
// statement read semantics, not an atomic wall-clock snapshot across files.
inline exact_vector_rows_plan build_exact_vector_rows_plan(
    const std::string& model, const std::string& vector_column,
    const std::vector<std::string>& columns,
    const std::vector<exact_vector_row_arm>& arms,
    const std::vector<float>& query, size_t k, exact_vector_metric metric) {
    const auto table = exact_vector_identifier(model);
    const auto vector = exact_vector_identifier(vector_column);
    if (exact_vector_fold_identifier(vector_column) == "id" ||
        exact_vector_fold_identifier(vector_column) == "globalid")
        throw std::invalid_argument("exact row vector is an identity column");
    if (query.empty() || query.size() > 8192 ||
        k > static_cast<size_t>(std::numeric_limits<int64_t>::max()) || arms.empty())
        throw std::invalid_argument("invalid exact row query dimensions, limit or arms");
    bool nonzero = false;
    for (const auto value : query) {
        if (!std::isfinite(value)) throw std::invalid_argument("nonfinite exact row query vector");
        nonzero = nonzero || value != 0.0f;
    }
    const char* function = nullptr;
    switch (metric) {
        case exact_vector_metric::l2: function = "vec_distance_L2"; break;
        case exact_vector_metric::l1: function = "vec_distance_L1"; break;
        case exact_vector_metric::cosine:
            if (!nonzero) throw std::invalid_argument("undefined exact row cosine query");
            function = "vec_distance_cosine"; break;
        default: throw std::invalid_argument("invalid exact row metric");
    }
    std::set<std::string> names;
    std::string physical_projection;
    for (const auto& column : columns) {
        const auto quoted = exact_vector_identifier(column);
        const auto folded = exact_vector_fold_identifier(column);
        if (!names.insert(folded).second || folded == "_source" ||
            folded == "_lattice_attach_token" || folded.starts_with("__lattice_exact_"))
            throw std::invalid_argument("duplicate or reserved exact row column");
        if (!physical_projection.empty()) physical_projection += ", ";
        physical_projection += "m." + quoted;
    }
    // These exact names are the existing managed-row image contract.
    const auto has = [&](const std::string& name) {
        return std::find(columns.begin(), columns.end(), name) != columns.end();
    };
    if (!has("id") || !has("globalId") || !has(vector_column))
        throw std::invalid_argument("exact row columns omit identity or vector");

    std::set<std::string> schemas;
    for (const auto& arm : arms) {
        (void)exact_vector_identifier(arm.schema);
        const auto folded = exact_vector_fold_identifier(arm.schema);
        if (!schemas.insert(folded).second || folded == "temp" ||
            (folded == "main" && arm.schema != "main") ||
            (arm.schema == "main" ? arm.attachment_token != 0 : arm.attachment_token <= 0))
            throw std::invalid_argument("invalid or duplicate exact row schema/token");
        if (arm.predicate.sql.find('\0') != std::string::npos ||
            (arm.predicate.sql.empty() && !arm.predicate.bindings.empty()))
            throw std::invalid_argument("invalid exact row predicate");
    }

    std::vector<uint8_t> blob(query.size() * sizeof(float));
    std::memcpy(blob.data(), query.data(), blob.size());
    exact_vector_rows_plan plan;
    plan.bindings.emplace_back(std::move(blob));
    plan.sql = "WITH __lattice_exact_query AS (SELECT ? AS embedding), "
        "__lattice_exact_eligible AS MATERIALIZED (";
    for (size_t i = 0; i < arms.size(); ++i) {
        const auto& arm = arms[i];
        const auto schema = exact_vector_identifier(arm.schema);
        if (i) plan.sql += " UNION ALL ";
        plan.sql += "SELECT m.\"id\", m.\"globalId\", m." + vector +
            ", ? AS _source, ? AS _lattice_attach_token FROM " + schema + '.' + table +
            " AS m WHERE m." + vector + " IS NOT NULL";
        plan.bindings.emplace_back(arm.schema == "main" ? std::string("main") : schema);
        plan.bindings.emplace_back(arm.attachment_token);
        if (!arm.predicate.sql.empty()) {
            plan.sql += " AND (\n" + arm.predicate.sql + "\n)";
            plan.bindings.insert(plan.bindings.end(), arm.predicate.bindings.begin(), arm.predicate.bindings.end());
        }
    }
    // Materialize eligibility BEFORE invoking the metric. Never let bad values
    // in an excluded physical row poison an otherwise valid read. Canonical
    // TEXT/JSON and wrong-size BLOBs are invalid, not implicit conversions.
    plan.sql += "), __lattice_exact_measured AS MATERIALIZED (SELECT *, CASE WHEN "
        "typeof(\"id\") = 'integer' AND typeof(\"globalId\") = 'text' AND typeof(" + vector +
        ") = 'blob' AND length(" + vector + ") = " + std::to_string(query.size() * sizeof(float)) +
        " THEN " + function + '(' + vector + ", (SELECT embedding FROM __lattice_exact_query)) "
        "ELSE NULL END AS __lattice_exact_distance FROM __lattice_exact_eligible), "
        "__lattice_exact_ranked AS MATERIALIZED (SELECT *, "
        "MAX(CASE WHEN __lattice_exact_distance IS NULL OR "
        "__lattice_exact_distance > 1.7976931348623157e308 OR "
        "__lattice_exact_distance < -1.7976931348623157e308 THEN 1 ELSE 0 END) OVER () "
        "AS __lattice_exact_invalid, ROW_NUMBER() OVER (PARTITION BY \"globalId\" COLLATE BINARY "
        "ORDER BY __lattice_exact_distance ASC, CASE WHEN _source = 'main' THEN 0 ELSE 1 END ASC, "
        "_source COLLATE BINARY ASC, \"id\" ASC) AS __lattice_exact_rank "
        "FROM __lattice_exact_measured), __lattice_exact_winners AS MATERIALIZED ("
        "SELECT \"id\", \"globalId\", _source, _lattice_attach_token, "
        "__lattice_exact_distance, __lattice_exact_invalid "
        "FROM __lattice_exact_ranked WHERE __lattice_exact_rank = 1 "
        "ORDER BY __lattice_exact_distance ASC, \"globalId\" COLLATE BINARY ASC LIMIT ?) SELECT * FROM (";
    plan.bindings.emplace_back(static_cast<int64_t>(k));
    // Fetch payloads only for final winners, in this SAME read statement. The
    // physical id alone and the logical gid alone are both insufficient joins.
    // IS keeps a malformed NULL identity from hiding the global invalid flag.
    for (size_t i = 0; i < arms.size(); ++i) {
        const auto& arm = arms[i];
        const auto schema = exact_vector_identifier(arm.schema);
        if (i) plan.sql += " UNION ALL ";
        plan.sql += "SELECT " + physical_projection +
            ", w._source, w._lattice_attach_token, w.__lattice_exact_distance, w.__lattice_exact_invalid "
            "FROM " + schema + '.' + table + " AS m JOIN __lattice_exact_winners AS w "
            "ON w._source = ? AND w._lattice_attach_token = ? AND m.\"id\" IS w.\"id\" "
            "AND m.\"globalId\" COLLATE BINARY IS w.\"globalId\" COLLATE BINARY";
        plan.bindings.emplace_back(arm.schema == "main" ? std::string("main") : schema);
        plan.bindings.emplace_back(arm.attachment_token);
    }
    plan.sql += ") ORDER BY __lattice_exact_distance ASC, \"globalId\" COLLATE BINARY ASC";
    return plan;
}

// The bbox collector's narrow read contract, scoped locally so the existing
// public bridge is unchanged: one readonly statement, exact anonymous binding
// count, checked binds/steps, owning scalar/blob copies, and RAII finalization.
// Unlike database::query this NEVER drains deferred observation callbacks.
// No additional connection lock is acquired; the caller owns the read/topology
// gate and database lifetime. It may reuse an already-held recursive gate.
inline std::vector<database::row_t> collect_exact_vector_rows(
    database& db, const exact_vector_rows_plan& plan) {
    if (db.is_closed() || !db.internal_handle()) throw db_error("exact row connection is closed");
    if (plan.sql.find('\0') != std::string::npos ||
        plan.sql.size() >= static_cast<size_t>(std::numeric_limits<int>::max()) ||
        plan.bindings.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        throw std::invalid_argument("exact row statement bounds");
    sqlite3_stmt* statement = nullptr;
    struct finalize {
        sqlite3_stmt*& value;
        ~finalize() { if (value) sqlite3_finalize(value); }
    } cleanup{statement};
    const char* tail = nullptr;
    database::record_statement();
    if (sqlite3_prepare_v2(db.internal_handle(), plan.sql.c_str(), -1, &statement, &tail) != SQLITE_OK)
        throw db_error(sqlite3_errmsg(db.internal_handle()));
    while (tail && *tail && std::isspace(static_cast<unsigned char>(*tail))) ++tail;
    if (!statement || (tail && *tail) || !sqlite3_stmt_readonly(statement) ||
        sqlite3_bind_parameter_count(statement) != static_cast<int>(plan.bindings.size()))
        throw db_error("exact row query requires one readonly statement and matching bindings");
    for (size_t i = 0; i < plan.bindings.size(); ++i) {
        const int index = static_cast<int>(i + 1);
        if (sqlite3_bind_parameter_name(statement, index))
            throw db_error("exact row query requires anonymous positional bindings");
        const int result = std::visit([&](const auto& value) -> int {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>) return sqlite3_bind_null(statement, index);
            else if constexpr (std::is_same_v<T, int64_t>) return sqlite3_bind_int64(statement, index, value);
            else if constexpr (std::is_same_v<T, double>) return sqlite3_bind_double(statement, index, value);
            else if constexpr (std::is_same_v<T, std::string>)
                return sqlite3_bind_text64(statement, index, value.data(), value.size(), SQLITE_TRANSIENT, SQLITE_UTF8);
            else return value.empty() ? sqlite3_bind_zeroblob(statement, index, 0) :
                sqlite3_bind_blob64(statement, index, value.data(), value.size(), SQLITE_TRANSIENT);
        }, plan.bindings[i]);
        if (result != SQLITE_OK) throw db_error("exact row parameter binding failed");
    }
    std::vector<std::string> columns;
    std::set<std::string> names;
    for (int i = 0; i < sqlite3_column_count(statement); ++i) {
        const char* name = sqlite3_column_name(statement, i);
        if (!name || !names.insert(exact_vector_fold_identifier(name)).second)
            throw db_error("exact row duplicate or unavailable column name");
        columns.emplace_back(name);
    }
    std::vector<database::row_t> rows;
    int result;
    while ((result = sqlite3_step(statement)) == SQLITE_ROW) {
        database::row_t row;
        for (int i = 0; i < static_cast<int>(columns.size()); ++i) {
            column_value_t value;
            switch (sqlite3_column_type(statement, i)) {
                case SQLITE_INTEGER: value = static_cast<int64_t>(sqlite3_column_int64(statement, i)); break;
                case SQLITE_FLOAT: value = sqlite3_column_double(statement, i); break;
                case SQLITE_TEXT: {
                    const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(statement, i));
                    if (!text) throw db_error("exact row text allocation failed");
                    value = std::string(text, sqlite3_column_bytes(statement, i)); break;
                }
                case SQLITE_BLOB: {
                    const auto* blob = static_cast<const uint8_t*>(sqlite3_column_blob(statement, i));
                    const int length = sqlite3_column_bytes(statement, i);
                    if (length && !blob) throw db_error("exact row blob allocation failed");
                    value = length ? std::vector<uint8_t>(blob, blob + length) : std::vector<uint8_t>{}; break;
                }
                default: value = nullptr; break;
            }
            row.emplace(columns[static_cast<size_t>(i)], std::move(value));
        }
        rows.push_back(std::move(row));
    }
    if (result != SQLITE_DONE) throw db_error(sqlite3_errmsg(db.internal_handle()));
    return rows;
}

inline std::vector<exact_vector_row> select_exact_vector_rows(
    database& db, const std::string& model, const std::string& vector_column,
    const std::vector<std::string>& columns, const std::vector<exact_vector_row_arm>& arms,
    const std::vector<float>& query, size_t k, exact_vector_metric metric) {
    auto plan = build_exact_vector_rows_plan(model, vector_column, columns, arms, query, k, metric);
    // Even k=0 prepares/binds the statement, so bad request/SQL shape is not an
    // empty success. It does not promise metric evaluation of stored rows at k=0.
    auto rows = collect_exact_vector_rows(db, plan);
    std::vector<exact_vector_row> result;
    result.reserve(rows.size());
    for (auto& row : rows) {
        if (std::get<int64_t>(row.at("__lattice_exact_invalid")) != 0)
            throw db_error("invalid canonical exact vector row or distance");
        const auto distance = std::get<double>(row.at("__lattice_exact_distance"));
        if (!std::isfinite(distance)) throw db_error("nonfinite exact row distance");
        row.erase("__lattice_exact_invalid");
        row.erase("__lattice_exact_distance");
        result.push_back({std::move(row), distance});
    }
    return result;
}

} // namespace lattice::detail
