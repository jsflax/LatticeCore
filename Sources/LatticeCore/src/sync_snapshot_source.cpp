#include "sync_snapshot_source.hpp"
#include "canonical_source_capture.hpp"
#include "sync_recovery_values.hpp"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cmath>
#include <set>
#include <map>
#include <tuple>

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

using source_key = std::pair<std::string,std::string>;
std::string canonical_uuid(const std::string& id) {
    check(id.size()==36,"canonical source requires UUID identities");
    auto key=nocase_key(id);
    for(size_t i=0;i<key.size();++i) {
        const char c=key[i];
        check((i==8||i==13||i==18||i==23)?c=='-':
            ((c>='0'&&c<='9')||(c>='a'&&c<='f')),"canonical source requires UUID identities");
    }
    return key;
}
std::vector<uint8_t> source_bytes(const std::string& s) { return {s.begin(),s.end()}; }
std::string source_integer_column(const std::string& name) {
    return "CASE WHEN typeof("+name+")='integer' THEN "+name+" END AS "+name;
}

unsealed_canonical_capture capture_canonical_impl(lattice_db& owner,
    const canonical_store_binding& binding,const std::vector<source_relation>& scope,
    std::optional<int64_t> base,const std::vector<canonical_capture_request>& requests,
    const canonical_capture_limits& b,const std::function<void(size_t,uint64_t)>& after_batch) {
    validate_budget(b.rows);
    check(!scope.empty()&&scope.size()<=b.rows.tables&&b.requests>0&&b.requests<=4096&&
        b.requested_targets>0&&b.requested_targets<=4096&&b.marker_batch>0&&b.marker_batch<=4096&&
        requests.size()<=b.requests,"invalid canonical capture limits");
    const auto& l=b.store;
    check(l.markers>=0&&l.marker_bytes>=0&&l.receipts>=0&&l.receipt_bytes>=0&&l.batch_identities>0&&
        l.identity_bytes>=36&&l.identity_bytes<=256&&l.operation_bytes>=36&&l.operation_bytes<=256,
        "invalid canonical storage limits");
    for(const auto* id:{&binding.source,&binding.epoch,&binding.scope,&binding.schema})
        check(!id->empty()&&id->size()<=static_cast<size_t>(l.identity_bytes),"canonical source binding too large");
    auto sorted=scope;
    std::sort(sorted.begin(),sorted.end(),[](const auto& a,const auto& z){return a.table<z.table;});
    for(size_t i=0;i<sorted.size();++i) {
        (void)quote_source_identifier(sorted[i].table);
        check(i==0||sorted[i-1].table!=sorted[i].table,"duplicate canonical source relation");
    }
    uint64_t target_count=0;
    std::optional<std::string> previous_original;
    for(const auto& q:requests) {
        check(canonical_uuid(q.original_id)==q.original_id,"canonical original key is not normalized");
        check(!previous_original||*previous_original<q.original_id,"duplicate or unordered canonical receipt request");
        previous_original=q.original_id;
        check(!q.targets.empty()&&q.targets.size()<=b.requested_targets-target_count,"canonical requested target limit exceeded");
        target_count+=q.targets.size();std::set<source_key> unique;
        for(const auto& t:q.targets) {
            check(t.table.size()<=256&&canonical_uuid(t.global_id)==t.global_id,
                "canonical target key is not normalized");
            check(unique.emplace(t.table,t.global_id).second,"duplicate canonical requested target");
        }
    }
    view held(owner);unsealed_canonical_capture result;
    std::string state_sql="SELECT ";
    for(const char* name:{"id","version","head","floor","markers","marker_bytes","receipts","receipt_bytes",
        "max_markers","max_marker_bytes","max_receipts","max_receipt_bytes","max_batch","max_identity","max_operation"})
        state_sql+=source_integer_column(name)+",";
    state_sql+="CASE WHEN typeof(source)='blob' AND length(source)<=256 THEN source END AS source,"
        "CASE WHEN typeof(epoch)='blob' AND length(epoch)<=256 THEN epoch END AS epoch,"
        "CASE WHEN typeof(scope)='blob' AND length(scope)<=256 THEN scope END AS scope,"
        "CASE WHEN typeof(schema_id)='blob' AND length(schema_id)<=256 THEN schema_id END AS schema_id "
        "FROM main._lattice_canonical_store LIMIT 2";
    const auto state=held.query(state_sql);
    check(state.size()==1,"canonical source store singleton missing");const auto& metadata=state.front();
    check(integer(metadata,"id")==1&&integer(metadata,"version")==1&&byte_string(metadata,"source")==binding.source&&
        byte_string(metadata,"epoch")==binding.epoch&&byte_string(metadata,"scope")==binding.scope&&
        byte_string(metadata,"schema_id")==binding.schema,"canonical source binding mismatch");
    check(integer(metadata,"max_markers")==l.markers&&integer(metadata,"max_marker_bytes")==l.marker_bytes&&
        integer(metadata,"max_receipts")==l.receipts&&integer(metadata,"max_receipt_bytes")==l.receipt_bytes&&
        integer(metadata,"max_batch")==l.batch_identities&&integer(metadata,"max_identity")==l.identity_bytes&&
        integer(metadata,"max_operation")==l.operation_bytes,"canonical source limits mismatch");
    result.head=integer(metadata,"head");result.floor=integer(metadata,"floor");
    check(result.floor>=0&&result.head>=result.floor,"canonical source frontier is corrupt");
    for(const auto& field:{std::pair{"markers",l.markers},std::pair{"marker_bytes",l.marker_bytes},
                          std::pair{"receipts",l.receipts},std::pair{"receipt_bytes",l.receipt_bytes}})
        check(integer(metadata,field.first)>=0&&integer(metadata,field.first)<=field.second,"canonical source counter is corrupt");
    if(base)check(*base>=result.floor&&*base<=result.head,"canonical source base retired or ahead");
    // Validate the entire retained metadata set BEFORE filtering by (B,H]. A
    // bad SQLite storage class or an ahead-of-head position must not disappear
    // behind the range predicate and turn a partial delta into a claimed H.
    // These queries return constants/aggregates, never unbounded stored bytes.
    const auto tables=held.query("SELECT wr FROM pragma_table_list WHERE schema='main' AND name IN "
        "('_lattice_canonical_store','_lattice_canonical_touch','_lattice_canonical_receipt')");
    check(tables.size()==3,"canonical source metadata schema missing");
    for(const auto& table:tables)check(integer(table,"wr")==1,"canonical source metadata must be WITHOUT ROWID");
    check(held.query("SELECT 1 AS invalid FROM main._lattice_canonical_touch WHERE "
        "typeof(relation)!='blob' OR length(relation) NOT BETWEEN 1 AND ? OR "
        "typeof(identity)!='blob' OR length(identity)!=36 OR typeof(position)!='integer' OR position<=? OR position>? OR "
        "typeof(charge)!='integer' OR charge!=24+length(relation)+length(identity) LIMIT 1",
        {l.identity_bytes,result.floor,result.head}).empty(),"canonical source retained marker is corrupt");
    check(held.query("SELECT 1 AS invalid FROM main._lattice_canonical_receipt WHERE "
        "typeof(original_id)!='blob' OR length(original_id)!=36 OR typeof(position)!='integer' OR position<=0 OR position>? OR "
        "typeof(outcome)!='integer' OR outcome NOT IN(1,2,3) OR (relation IS NULL)!=(identity IS NULL) OR "
        "(relation IS NOT NULL AND (typeof(relation)!='blob' OR length(relation) NOT BETWEEN 1 AND ? OR "
        "typeof(identity)!='blob' OR length(identity)!=36)) OR typeof(charge)!='integer' OR "
        "charge!=32+length(original_id)+COALESCE(length(relation),0)+COALESCE(length(identity),0) LIMIT 1",
        {result.head,l.identity_bytes}).empty(),"canonical source retained receipt is corrupt");
    const auto actual_markers=held.query("SELECT COUNT(*) AS n,COALESCE(SUM(charge),0) AS bytes FROM main._lattice_canonical_touch");
    const auto actual_receipts=held.query("SELECT COUNT(*) AS n,COALESCE(SUM(charge),0) AS bytes FROM main._lattice_canonical_receipt");
    check(actual_markers.size()==1&&actual_receipts.size()==1&&
        integer(actual_markers[0],"n")==integer(metadata,"markers")&&
        integer(actual_markers[0],"bytes")==integer(metadata,"marker_bytes")&&
        integer(actual_receipts[0],"n")==integer(metadata,"receipts")&&
        integer(actual_receipts[0],"bytes")==integer(metadata,"receipt_bytes"),
        "canonical source counters differ from retained storage");
    const auto cookie=held.query("PRAGMA main.schema_version");
    check(cookie.size()==1,"canonical source schema cookie missing");result.schema_cookie=integer(cookie.front(),"schema_version");
    std::map<std::string,size_t> layouts;
    for(const auto& relation:sorted) {
        layouts.emplace(relation.table,result.layouts.size());result.layouts.push_back(layout(held,relation,b.rows));
        check(result.layouts.back().identity_collation=="NOCASE","canonical source requires indexed NOCASE identities");
    }
    auto charge=[&](uint64_t bytes) {
        check(bytes<=b.rows.wire.total_bytes-result.copied_logical_bytes,"canonical capture logical-byte budget exceeded");
        result.copied_logical_bytes+=bytes;
    };
    std::set<source_key> selected;
    auto select=[&](const source_key& k) {
        check(layouts.count(k.first),"canonical identity is outside complete declared scope");
        check(canonical_uuid(k.second)==k.second,"canonical stored key is not normalized");
        if(selected.count(k))return;
        check(selected.size()<b.rows.wire.total_rows,"canonical source identity budget exceeded");
        charge(32+k.first.size()+k.second.size());selected.insert(k);
    };
    size_t batches=0;
    if(after_batch)after_batch(batches,held.generation);
    if(!base) {
        for(const auto& table:result.layouts) {
            std::optional<std::string> last;std::optional<std::string> last_index;
            std::set<std::string> normalized_ids;
            for(;;) {
                std::string sql="SELECT CASE WHEN typeof(r.globalId)='text' AND length(CAST(r.globalId AS BLOB))=36 "
                    "THEN CAST(r.globalId AS BLOB) END AS globalId FROM main."+quote_source_identifier(table.table)+" AS r";
                std::vector<column_value_t> params;
                if(last){sql+=" WHERE r.globalId>? COLLATE "+table.identity_collation;params.push_back(*last);}
                sql+=" ORDER BY r.globalId COLLATE "+table.identity_collation+" LIMIT ?";
                params.push_back(static_cast<int64_t>(std::min<uint64_t>(b.rows.wire.rows_per_page,b.rows.wire.total_rows-selected.size()+1)));
                const auto ids=held.query(sql,params);if(ids.empty())break;
                for(const auto& row:ids) {
                    const auto id=byte_string(row,"globalId");const auto normalized=canonical_uuid(id);
                    const auto index=table.identity_collation=="NOCASE"?normalized:id;
                    check(!last_index||*last_index<index,"canonical source keyset is not increasing");
                    check(normalized_ids.insert(normalized).second,"canonical source UUID alias collision");
                    select({table.table,normalized});last=id;last_index=index;
                }
                if(after_batch)after_batch(++batches,held.generation);
            }
        }
    } else {
        std::optional<std::tuple<int64_t,std::string,std::string>> cursor;
        for(;;) {
            std::string sql="SELECT "+source_integer_column("position")+",CASE WHEN typeof(relation)='blob' AND length(relation) BETWEEN 1 AND 256 THEN relation END AS relation,"
                "CASE WHEN typeof(identity)='blob' AND length(identity)=36 THEN identity END AS identity,"+source_integer_column("charge")+" "
                "FROM main._lattice_canonical_touch AS m INDEXED BY _lattice_canonical_touch_position WHERE m.position>? AND m.position<=?";
            std::vector<column_value_t> params{*base,result.head};
            if(cursor){sql+=" AND (m.position,m.relation,m.identity)>(?,?,?)";params.push_back(std::get<0>(*cursor));
                params.push_back(source_bytes(std::get<1>(*cursor)));params.push_back(source_bytes(std::get<2>(*cursor)));}
            sql+=" ORDER BY m.position,m.relation,m.identity LIMIT ?";params.push_back(static_cast<int64_t>(b.marker_batch));
            const auto markers=held.query(sql,params);if(markers.empty())break;
            for(const auto& marker:markers) {
                const auto position=integer(marker,"position");const auto relation=byte_string(marker,"relation");const auto id=byte_string(marker,"identity");
                check(position>*base&&position<=result.head&&integer(marker,"charge")==static_cast<int64_t>(24+relation.size()+id.size()),
                    "canonical source marker is corrupt");
                const auto next=std::tuple{position,relation,id};check(!cursor||*cursor<next,"canonical marker keyset is not increasing");
                select({relation,id});cursor=next;
            }
            if(after_batch)after_batch(++batches,held.generation);
        }
    }
    for(const auto& asked:requests) {
        for(const auto& target:asked.targets)select({target.table,target.global_id});
        const auto rows=held.query("SELECT "+source_integer_column("position")+","+source_integer_column("outcome")+","
            "CASE WHEN relation IS NULL THEN NULL WHEN typeof(relation)='blob' AND length(relation) BETWEEN 1 AND 256 THEN relation END AS relation,"
            "CASE WHEN identity IS NULL THEN NULL WHEN typeof(identity)='blob' AND length(identity)=36 THEN identity END AS identity,"
            "typeof(relation) AS rt,typeof(identity) AS it,"+source_integer_column("charge")+" FROM main._lattice_canonical_receipt WHERE original_id=? LIMIT 2",
            {source_bytes(asked.original_id)});
        check(rows.size()<=1,"canonical source receipt identity collision");
        canonical_source_receipt fact{asked.original_id,{}};charge(48+asked.original_id.size());
        if(!rows.empty()) {
            const auto& row=rows.front();const auto position=integer(row,"position"),outcome=integer(row,"outcome");
            check(position>0&&position<=result.head&&outcome>=1&&outcome<=3,"canonical source receipt is corrupt");
            const auto rt=string_value(row,"rt"),it=string_value(row,"it");std::optional<canonical_identity> target;
            uint64_t bytes=32+asked.original_id.size();
            if(rt=="blob"&&it=="blob") {
                target=canonical_identity{byte_string(row,"relation"),byte_string(row,"identity")};
                check(canonical_uuid(target->global_id)==target->global_id&&layouts.count(target->table),"canonical receipt target is outside scope");
                check(std::find(asked.targets.begin(),asked.targets.end(),*target)!=asked.targets.end(),"canonical receipt target differs from requested original");
                bytes+=target->table.size()+target->global_id.size();charge(target->table.size()+target->global_id.size());
            } else check(rt=="null"&&it=="null","canonical source receipt target is corrupt");
            check(integer(row,"charge")==static_cast<int64_t>(bytes),"canonical receipt charge is corrupt");
            fact.stored=canonical_receipt{{asked.original_id,static_cast<canonical_receipt_outcome>(outcome),target},position};
        }
        result.receipts.push_back(std::move(fact));
    }
    for(const auto& key:selected) {
        const auto& table=result.layouts.at(layouts.at(key.first));
        const auto found=held.query("SELECT CASE WHEN typeof(globalId)='text' AND length(CAST(globalId AS BLOB))=36 "
            "THEN CAST(globalId AS BLOB) END AS globalId FROM main."+quote_source_identifier(key.first)+
            " WHERE globalId=? COLLATE NOCASE LIMIT 2",{key.second});
        check(found.size()<=1,"canonical source UUID alias collision");
        canonical_source_row row{{key.first,key.second},{}};
        if(!found.empty()) {
            const auto actual_id=byte_string(found.front(),"globalId");check(canonical_uuid(actual_id)==key.second,"canonical source row identity changed");
            const auto remaining=b.rows.wire.total_bytes-result.copied_logical_bytes;
            auto raw=copy_row(held,table,actual_id,b.rows,remaining);
            const value_limits scalar_limit{static_cast<size_t>(b.rows.wire.string_bytes),b.rows.columns_per_table,256,
                static_cast<size_t>(b.rows.wire.string_bytes),static_cast<size_t>(b.rows.wire.string_bytes)};
            auto values=decode_values(raw.payload,scalar_limit);values.emplace("globalId",actual_id);
            row.payload=encode_values(values,scalar_limit);charge(row.payload->size());
        }
        result.rows.push_back(std::move(row));
        if(after_batch)after_batch(++batches,held.generation);
    }
    (void)held.query("SELECT 1 AS live");held.finish();return result;
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

unsealed_canonical_capture capture_canonical_source(lattice_db& owner,const canonical_store_binding& binding,
    const std::vector<source_relation>& scope,std::optional<int64_t> base,
    const std::vector<canonical_capture_request>& requests,const canonical_capture_limits& budget) {
    return capture_canonical_impl(owner,binding,scope,base,requests,budget,{});
}
namespace source_test_hooks {
unsealed_canonical_capture capture_canonical(lattice_db& owner,const canonical_store_binding& binding,
    const std::vector<source_relation>& scope,std::optional<int64_t> base,
    const std::vector<canonical_capture_request>& requests,const canonical_capture_limits& budget,
    const std::function<void(size_t,uint64_t)>& after_batch) {
    return capture_canonical_impl(owner,binding,scope,base,requests,budget,after_batch);
}
}
} // namespace lattice::detail::sync_recovery
