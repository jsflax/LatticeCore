#include "sync_snapshot_source.hpp"
#include "sync_recovery_values.hpp"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cmath>
#include <set>

namespace lattice::detail::sync_recovery {
namespace {
void check(bool value, const char* message) { if (!value) throw protocol_error(message); }
std::string quote_source_identifier(const std::string& name) {
    check(!name.empty() && name.size() <= 256, "source identifier length refused");
    std::string result = "\"";
    for (unsigned char c : name) {
        check(c >= 32 && c != 127, "source identifier control byte refused");
        if (c == '"') result += '"';
        result += static_cast<char>(c);
    }
    try { (void)nlohmann::json(name).dump(); }
    catch (const nlohmann::json::type_error&) { throw protocol_error("source identifier is not UTF-8"); }
    return result + '"';
}
std::string string_value(const database::row_t& row, const char* key) {
    const auto it = row.find(key);
    check(it != row.end() && std::holds_alternative<std::string>(it->second), "invalid source schema/string");
    return std::get<std::string>(it->second);
}
std::string byte_string(const database::row_t& row, const char* key) {
    const auto it = row.find(key);
    check(it != row.end() && std::holds_alternative<std::vector<uint8_t>>(it->second), "invalid source byte string");
    const auto& bytes = std::get<std::vector<uint8_t>>(it->second);
    return bytes.empty() ? std::string() : std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}
int64_t integer(const database::row_t& row, const char* key) {
    const auto it = row.find(key);
    check(it != row.end() && std::holds_alternative<int64_t>(it->second), "invalid source integer");
    return std::get<int64_t>(it->second);
}
std::string upper(std::string value) {
    for (auto& c : value) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
    return value;
}
std::string nocase_key(std::string value) {
    for (auto& c : value) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return value;
}

struct view {
    lattice_db& owner;
    uint64_t generation;
    explicit view(lattice_db& db) : owner(db), generation(db.acquire_read_generation()) {
        check(generation != 0, "source view unavailable");
    }
    ~view() noexcept {
        if (generation) {
            try { owner.release_read_generation(generation); }
            catch (...) {} // Preserve a failed capture; no successful artifact is returned.
        }
    }
    void finish() {
        const auto held = generation; generation = 0;
        owner.release_read_generation(held); // success path propagates cleanup failure
    }
    std::vector<database::row_t> query(const std::string& sql, const std::vector<column_value_t>& params = {}) {
        auto result = owner.query_at_generation(generation, sql, params);
        check(result.has_value(), "source view retired or read failed"); return std::move(*result);
    }
};

void validate_budget(const source_limits& b) {
    // Same finite policy range as the codec, without manufacturing a manifest
    // or any authority claim for this source-only operation.
    check(b.wire.frame_bytes > 0 && b.wire.frame_bytes <= 16 * 1024 * 1024 &&
          b.wire.string_bytes >= 64 && b.wire.string_bytes <= b.wire.frame_bytes &&
          b.wire.rows_per_page > 0 && b.wire.rows_per_page <= 4096 &&
          b.wire.pages > 0 && b.wire.pages <= INT64_MAX &&
          b.wire.total_rows > 0 && b.wire.total_rows <= INT64_MAX &&
          b.wire.total_bytes > 0 && b.wire.total_bytes <= INT64_MAX &&
          b.wire.depth > 0 && b.wire.depth <= 64 && b.wire.nodes > 0 && b.wire.nodes <= 65536,
          "invalid source wire budget");
    check(b.tables > 0 && b.tables <= 64 && b.columns_per_table > 0 && b.columns_per_table <= 128 &&
          b.indexes_per_table > 0 && b.indexes_per_table <= 128, "invalid source schema budget");
}

source_layout layout(view& v, const source_relation& relation, const source_limits& b) {
    check(relation.complete_table_scope, "filtered or undeclared source scope refused");
    check(relation.kind == relation_kind::model || relation.kind == relation_kind::link, "unknown source relation kind");
    (void)quote_source_identifier(relation.table);
    const auto folded = upper(relation.table);
    check(folded != "AUDITLOG" && folded != "_SYNCCONTROL" && folded.rfind("_LATTICE_", 0) != 0 &&
          folded.rfind("SQLITE_", 0) != 0, "internal source relation refused");
    const auto definition = v.query("SELECT type FROM pragma_table_list "
                                    "WHERE schema='main' AND name=? COLLATE BINARY LIMIT 2", {relation.table});
    check(definition.size() == 1 && string_value(definition.front(), "type") == "table",
          "source must be a concrete main table");
    // CASE bounds metadata strings before the database wrapper allocates them.
    auto columns = v.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=256 THEN name END AS name,"
        "CASE WHEN length(CAST(type AS BLOB))<=32 THEN type END AS type,\"notnull\",pk,hidden "
        "FROM pragma_table_xinfo(?) ORDER BY cid LIMIT ?", {relation.table, static_cast<int64_t>(b.columns_per_table + 1)});
    check(!columns.empty() && columns.size() <= b.columns_per_table, "source column budget exceeded");
    source_layout result{relation.table, relation.kind, {}, {}};
    std::set<std::string> names;
    bool id = false, global = false; size_t primary_columns = 0;
    for (const auto& c : columns) {
        source_column col{string_value(c, "name"), upper(string_value(c, "type")), integer(c, "notnull") != 0, integer(c, "pk")};
        (void)quote_source_identifier(col.name);
        check(integer(c, "hidden") == 0 && col.primary_key_order >= 0, "generated/hidden source column refused");
        check(col.sql_type == "INTEGER" || col.sql_type == "REAL" || col.sql_type == "TEXT" || col.sql_type == "BLOB",
              "unsupported source declared type");
        check(names.insert(col.name).second, "duplicate source column");
        if (col.primary_key_order) ++primary_columns;
        if (col.name == "id") { id = true; check(col.sql_type == "INTEGER" && col.primary_key_order == 1, "source id is not integer primary key"); }
        if (col.name == "globalId") { global = true; check(col.sql_type == "TEXT", "source globalId is not text"); }
        result.columns.push_back(std::move(col));
    }
    check(global, "source globalId absent");
    if (relation.kind == relation_kind::model) check(id && primary_columns == 1, "unsupported model identity schema");
    else {
        check(!id && names.count("lhs") && names.count("rhs") && (names.size() == 3 ||
              (names.size() == 4 && names.count("rhs_type"))), "unsupported link identity schema");
        for (const auto& c : result.columns) check(c.sql_type == "TEXT", "unsupported link column type");
        const bool polymorphic = names.count("rhs_type") != 0;
        for (const auto& c : result.columns) {
            const int64_t expected_pk = c.name == "lhs" ? 1 : c.name == "rhs_type" ? 2 :
                c.name == "rhs" ? (polymorphic ? 3 : 2) : 0;
            check(c.primary_key_order == expected_pk && (c.name == "globalId" || c.not_null),
                  "unsupported link primary key schema");
        }
    }
    // Keyset paging requires a durable, nonpartial UNIQUE globalId index.
    // A plain index or inferred row uniqueness would silently skip duplicates.
    const auto indexes = v.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=256 THEN name END AS name,\"unique\",partial "
        "FROM pragma_index_list(?) LIMIT ?", {relation.table, static_cast<int64_t>(b.indexes_per_table + 1)});
    check(indexes.size() <= b.indexes_per_table, "source index budget exceeded");
    for (const auto& index : indexes) {
        if (integer(index, "unique") != 1 || integer(index, "partial") != 0) continue;
        const auto name = string_value(index, "name"); (void)quote_source_identifier(name);
        const auto fields = v.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=256 THEN name END AS name,"
            "CASE WHEN length(CAST(coll AS BLOB))<=16 THEN coll END AS coll "
            "FROM pragma_index_xinfo(?) WHERE \"key\"=1 ORDER BY seqno LIMIT 2", {name});
        if (fields.size() == 1) {
            const auto field = fields.front().find("name");
            if (field != fields.front().end() && std::holds_alternative<std::string>(field->second) &&
                std::get<std::string>(field->second) == "globalId") {
                const auto collation = upper(string_value(fields.front(), "coll"));
                if (collation == "BINARY" || (collation == "NOCASE" && result.identity_collation.empty()))
                    result.identity_collation = collation;
            }
        }
    }
    check(!result.identity_collation.empty(), "source globalId uniqueness not proven"); return result;
}

row copy_row(view& v, const source_layout& table, const std::string& id, const source_limits& b,
             uint64_t remaining_content) {
    const uint64_t identity_bytes = 24 + table.table.size() + id.size();
    check(identity_bytes < b.wire.frame_bytes && identity_bytes < remaining_content,
          "source content budget exceeded");
    const auto payload_limit = std::min<uint64_t>(b.wire.string_bytes,
        std::min<uint64_t>(b.wire.frame_bytes, remaining_content) - identity_bytes);
    std::vector<std::string> columns;
    std::string sizes = "SELECT ";
    uint64_t payload_upper = 2; // enclosing JSON object
    for (const auto& c : table.columns) {
        if (c.name == "id" || c.name == "globalId") continue;
        if (!columns.empty()) sizes += ',';
        const auto n = std::to_string(columns.size());
        sizes += "typeof(" + quote_source_identifier(c.name) + ") AS t" + n + ",length(CAST(" + quote_source_identifier(c.name) + " AS BLOB)) AS n" + n;
        // JSON property spelling + kind/value envelope, numeric spelling.
        payload_upper += 6 * c.name.size() + 96;
        columns.push_back(c.name);
    }
    check(!columns.empty(), "source relation without payload columns refused");
    check(payload_upper <= payload_limit, "source row structural budget exceeded");
    sizes += " FROM main." + quote_source_identifier(table.table) + " WHERE \"globalId\"=? COLLATE " + table.identity_collation + " LIMIT 2";
    const auto measured = v.query(sizes, {id});
    check(measured.size() == 1, "source row identity changed");
    std::vector<std::string> storage_classes;
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto type = string_value(measured.front(), ("t" + std::to_string(i)).c_str());
        check(type == "null" || type == "integer" || type == "real" || type == "text" || type == "blob", "unsupported source storage class");
        storage_classes.push_back(type);
        if (type == "null") continue;
        const auto bytes = integer(measured.front(), ("n" + std::to_string(i)).c_str());
        check(bytes >= 0 && static_cast<uint64_t>(bytes) <= (payload_limit - payload_upper) / 6,
              "source value exceeds allocation budget");
        payload_upper += static_cast<uint64_t>(bytes) * 6;
    }
    std::string select = "SELECT ";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i) select += ',';
        const auto column = quote_source_identifier(columns[i]);
        // The inherited generic TEXT extractor is NUL-terminated. Read text
        // as sized BLOB bytes here; the preceding same-view typeof determines
        // whether to reconstruct TEXT or preserve an actual BLOB.
        select += storage_classes[i] == "text" ? "CAST(" + column + " AS BLOB) AS " + column : column;
    }
    select += " FROM main." + quote_source_identifier(table.table) + " WHERE \"globalId\"=? COLLATE " + table.identity_collation + " LIMIT 2";
    const auto values = v.query(select, {id});
    check(values.size() == 1, "source row identity changed");
    row_values payload;
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& name = columns[i];
        const auto it = values.front().find(name); check(it != values.front().end(), "source field absent");
        if (storage_classes[i] == "text") {
            const auto* bytes = std::get_if<std::vector<uint8_t>>(&it->second);
            check(bytes != nullptr, "source text byte extraction failed");
            const auto text = bytes->empty() ? std::string() :
                std::string(reinterpret_cast<const char*>(bytes->data()), bytes->size());
            payload.emplace(name, text);
            continue;
        }
        if (const auto* real = std::get_if<double>(&it->second)) check(std::isfinite(*real), "nonfinite source value refused");
        payload.emplace(name, it->second);
    }
    row result{table.table, id, {}};
    const auto maximum = static_cast<size_t>(payload_limit);
    result.payload = encode_values(payload,
        {maximum, b.columns_per_table, 256, maximum, maximum});
    check(result.payload.size() <= payload_limit, "source payload exceeds budget"); return result;
}

unsealed_materialization capture(lattice_db& owner, const std::vector<source_relation>& scope, const source_limits& b,
                                 const std::function<void(size_t, uint64_t)>& after_capture_batch) {
    validate_budget(b); check(!scope.empty() && scope.size() <= b.tables, "source scope budget exceeded");
    for (const auto& relation : scope) {
        (void)quote_source_identifier(relation.table);
        check(relation.table.size() <= b.wire.string_bytes, "source identity exceeds string budget");
    }
    auto sorted = scope;
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& z) { return a.table < z.table; });
    for (size_t i = 0; i < sorted.size(); ++i) {
        (void)quote_source_identifier(sorted[i].table);
        check(i == 0 || sorted[i-1].table != sorted[i].table, "duplicate source relation");
    }
    view held(owner);
    unsealed_materialization result;
    const auto head = held.query("SELECT COALESCE(MAX(id),0) AS head FROM main.AuditLog");
    check(head.size() == 1 && integer(head.front(), "head") >= 0, "invalid source audit head");
    result.audit_head_candidate = static_cast<uint64_t>(integer(head.front(), "head"));
    const auto schema = held.query("PRAGMA main.schema_version");
    check(schema.size() == 1, "invalid source schema cookie"); result.schema_cookie = integer(schema.front(), "schema_version");
    const auto revision = held.query("SELECT CASE WHEN length(CAST(value AS BLOB))<=36 THEN CAST(value AS BLOB) END AS value "
                                    "FROM main._lattice_meta WHERE key='history_generation_revision_v1' LIMIT 2");
    check(revision.size() <= 1, "invalid local history metadata");
    if (!revision.empty()) result.local_history_revision = byte_string(revision.front(), "value");
    for (const auto& relation : sorted) result.layouts.push_back(layout(held, relation, b));
    std::vector<row> copied_rows;
    size_t captured_batches = 0;
    for (const auto& table : result.layouts) {
        std::optional<std::string> last;
        std::optional<std::string> last_index_key;
        // The source may have BINARY-unique IDs while a destination uses
        // NOCASE. Refuse aliases, never collapse them. This set is bounded by
        // the already charged row count and identity bytes of this table.
        std::set<std::string> folded_ids;
        for (;;) {
            std::string sql = "SELECT CASE WHEN typeof(source_row.\"globalId\")='text' AND "
                "length(CAST(source_row.\"globalId\" AS BLOB)) BETWEEN 1 AND 256 "
                "THEN CAST(source_row.\"globalId\" AS BLOB) END AS globalId FROM main." + quote_source_identifier(table.table) + " AS source_row";
            std::vector<column_value_t> params;
            if (last) { sql += " WHERE source_row.\"globalId\" > ? COLLATE " + table.identity_collation; params.push_back(*last); }
            // Qualify the physical column: ordering the CASE alias would
            // force a full sort rather than an indexed keyset traversal.
            sql += " ORDER BY source_row.\"globalId\" COLLATE " + table.identity_collation + " LIMIT ?";
            // Read only bounded identities; fetch each payload only after its
            // same-view byte-length preflight. Probe one row at an exhausted cap.
            const auto remaining = b.wire.total_rows - result.rows;
            const auto count = std::min<uint64_t>(b.wire.rows_per_page, remaining + 1);
            params.push_back(static_cast<int64_t>(count));
            const auto ids = held.query(sql, params);
            if (ids.empty()) break;
            for (const auto& item : ids) {
                const auto id = byte_string(item, "globalId"); (void)quote_source_identifier(id);
                check(id.size() <= b.wire.string_bytes, "source identity exceeds string budget");
                const auto index_key = table.identity_collation == "NOCASE" ? nocase_key(id) : id;
                check(!last_index_key || *last_index_key < index_key, "source row order invalid");
                check(result.rows < b.wire.total_rows, "source row budget exceeded");
                check(folded_ids.insert(nocase_key(id)).second, "case-aliased source identity refused");
                auto copied = copy_row(held, table, id, b, b.wire.total_bytes - result.content_bytes);
                const auto bytes = canonical_row_bytes(copied);
                check(bytes <= b.wire.frame_bytes && bytes <= b.wire.total_bytes - result.content_bytes,
                      "source content budget exceeded");
                ++result.rows; result.content_bytes += bytes;
                copied_rows.push_back(std::move(copied)); last = id; last_index_key = index_key;
            }
            ++captured_batches;
            if (after_capture_batch) after_capture_batch(captured_batches, held.generation);
        }
    }
    // SQLite follows the actual durable index; only the explicitly bounded
    // owned result is sorted for codec byte order. This preserves mixed-case
    // identities without asking SQLite to sort an entire unbounded table.
    std::sort(copied_rows.begin(), copied_rows.end(), [](const row& a, const row& z) {
        return a.table < z.table || (a.table == z.table && a.global_id < z.global_id);
    });
    std::vector<row> output;
    uint64_t page_bytes = 0;
    const auto flush = [&] {
        if (output.empty()) return;
        check(result.pages.size() < b.wire.pages, "source page budget exceeded");
        result.pages.push_back(std::move(output)); output = {}; page_bytes = 0;
    };
    for (auto& value : copied_rows) {
        const auto bytes = canonical_row_bytes(value);
        if (output.size() == b.wire.rows_per_page || bytes > b.wire.frame_bytes - page_bytes) flush();
        page_bytes += bytes; output.push_back(std::move(value));
    }
    flush();
    (void)held.query("SELECT 1 AS live"); // never succeed after a test/lifecycle retirement
    held.finish();
    return result;
}
} // namespace

unsealed_materialization materialize_source(lattice_db& owner, const std::vector<source_relation>& scope, const source_limits& b) {
    return capture(owner, scope, b, {});
}
namespace source_test_hooks {
unsealed_materialization materialize(lattice_db& owner, const std::vector<source_relation>& scope, const source_limits& b,
                                     const std::function<void(size_t, uint64_t)>& after_capture_batch) {
    return capture(owner, scope, b, after_capture_batch);
}
}
} // namespace lattice::detail::sync_recovery
