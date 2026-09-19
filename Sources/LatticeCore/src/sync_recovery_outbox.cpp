#include "sync_recovery_outbox.hpp"
#include "recovery_writer_access.hpp"
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <string_view>

namespace lattice::detail {
namespace {
using code = recovery_outbox_error_code;
[[noreturn]] void fail(code c, const char* message, int rc = 0) {
    throw recovery_outbox_error(c, message, rc);
}
void sql_ok(int rc) {
    if (rc != SQLITE_OK) fail(code::sql_error, "outbox SQLite operation failed", rc);
}
struct statement {
    sqlite3_stmt* value = nullptr;
    statement(sqlite3* db, const std::string& sql) {
        database::record_statement();
        const int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &value, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(value); value = nullptr;
            sql_ok(rc);
        }
        if (!value || !sqlite3_stmt_readonly(value)) {
            sqlite3_finalize(value); value = nullptr;
            fail(code::sql_error, "outbox requires a read-only statement");
        }
    }
    statement(const statement&) = delete;
    ~statement() { sqlite3_finalize(value); }
    void text(int at, const std::string& text) {
        if (text.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
            fail(code::budget_exceeded, "outbox binding is too large");
        // Every bound string outlives this statement, including repeated steps.
        sql_ok(sqlite3_bind_text(value, at, text.data(), static_cast<int>(text.size()), SQLITE_STATIC));
    }
    void integer(int at, int64_t n) { sql_ok(sqlite3_bind_int64(value, at, n)); }
    bool next() {
        const int rc = sqlite3_step(value);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        fail(code::sql_error, "outbox SQLite read failed", rc);
    }
};
struct budget {
    const recovery_outbox_limits& limits;
    uint64_t fields = 0, bytes = 0, columns = 0;
    void charge(uint64_t n) {
        if (fields >= limits.fields || n > limits.field_bytes ||
            bytes > limits.logical_bytes || n > limits.logical_bytes - bytes)
            fail(code::budget_exceeded, "outbox scalar budget exceeded before copy");
        ++fields; bytes += n;
    }
    void count(uint64_t count, uint64_t maximum, const char* message) {
        if (count >= maximum) fail(code::budget_exceeded, message);
    }
};
recovery_scalar scalar(statement& s, int at, budget& b) {
    const int type = sqlite3_column_type(s.value, at);
    switch (type) {
    case SQLITE_NULL: b.charge(1); return nullptr;
    case SQLITE_INTEGER: b.charge(8); return static_cast<int64_t>(sqlite3_column_int64(s.value, at));
    case SQLITE_FLOAT: {
        b.charge(8); const double v = sqlite3_column_double(s.value, at);
        if (!std::isfinite(v)) fail(code::corrupt_state, "outbox nonfinite numeric value");
        return v;
    }
    case SQLITE_TEXT: case SQLITE_BLOB: {
        const int n = sqlite3_column_bytes(s.value, at);
        if (n < 0) fail(code::sql_error, "outbox invalid SQLite value length");
        b.charge(static_cast<uint64_t>(n)); // before any C++ string/vector allocation
        if (type == SQLITE_TEXT) {
            const auto* p = sqlite3_column_text(s.value, at);
            if (!p) fail(code::sql_error, "outbox text unavailable", SQLITE_NOMEM);
            return std::string(reinterpret_cast<const char*>(p), static_cast<size_t>(n));
        }
        if (n == 0) return std::vector<uint8_t>{};
        const auto* p = static_cast<const uint8_t*>(sqlite3_column_blob(s.value, at));
        if (!p) fail(code::sql_error, "outbox blob unavailable", SQLITE_NOMEM);
        return std::vector<uint8_t>(p, p + n);
    }
    default: fail(code::corrupt_state, "outbox unknown SQLite scalar type");
    }
}
int64_t integer(statement& s, int at, budget& b) {
    auto v = scalar(s, at, b);
    if (!std::holds_alternative<int64_t>(v)) fail(code::corrupt_state, "outbox expected INTEGER");
    return std::get<int64_t>(v);
}
bool flag(statement& s, int at, budget& b) {
    const auto v = integer(s, at, b);
    if (v != 0 && v != 1) fail(code::corrupt_state, "outbox invalid boolean flag");
    return v != 0;
}
std::string text(statement& s, int at, budget& b, bool empty_allowed = true) {
    auto v = scalar(s, at, b);
    if (!std::holds_alternative<std::string>(v)) fail(code::corrupt_state, "outbox expected TEXT");
    auto result = std::get<std::string>(std::move(v));
    if (!empty_allowed && result.empty()) fail(code::corrupt_state, "outbox empty required identity");
    return result;
}
std::string quote(const std::string& name) {
    if (name.empty() || name.find('\0') != std::string::npos)
        fail(code::unsupported_schema, "outbox empty or NUL schema identifier");
    std::string result = "\"";
    for (const char c : name) { result += c; if (c == '"') result += c; }
    return result + '"';
}
std::string upper(std::string value) {
    for (auto& c : value) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
    return value;
}
std::optional<std::string> metadata(sqlite3* db, const std::string& key, budget& b) {
    statement s(db, "SELECT value FROM main._lattice_meta WHERE key=? LIMIT 2");
    s.text(1, key);
    if (!s.next()) return std::nullopt;
    auto result = text(s, 0, b);
    if (s.next()) fail(code::corrupt_state, "outbox duplicate metadata key");
    return result;
}
const recovery_outbox_column* column(const recovery_outbox_table& t, const std::string& name) {
    for (const auto& c : t.columns) if (c.name == name) return &c;
    return nullptr;
}
recovery_outbox_table table(sqlite3* db, const std::string& name, budget& b) {
    recovery_outbox_table result;
    b.charge(name.size()); result.name = name;
    const auto quoted = quote(name);
    // A view/virtual table is not a preserved model. Exact stored name matching
    // also avoids silently treating a spelling alias as a second authority.
    statement definition(db, "SELECT sql FROM main.sqlite_schema WHERE type='table' AND name=? LIMIT 2");
    definition.text(1, name);
    if (!definition.next()) fail(code::unsupported_schema, "outbox referenced table is missing");
    result.create_sql = text(definition, 0, b, false);
    if (definition.next()) fail(code::corrupt_state, "outbox ambiguous table definition");
    statement shape(db, "PRAGMA main.table_list(" + quoted + ")");
    bool found = false;
    while (shape.next()) {
        auto schema = text(shape, 0, b);
        auto actual_name = text(shape, 1, b);
        if (schema != "main" || actual_name != name) continue;
        if (found || text(shape, 2, b) != "table" || integer(shape, 4, b) != 0)
            fail(code::unsupported_schema, "outbox requires an ordinary rowid table");
        found = true;
    }
    if (!found) fail(code::unsupported_schema, "outbox table shape is unavailable");
    statement info(db, "PRAGMA main.table_xinfo(" + quoted + ")");
    int64_t expected_ordinal = 0;
    int primary_count = 0;
    while (info.next()) {
        b.count(result.columns.size(), b.limits.columns_per_table, "outbox per-table column limit exceeded");
        b.count(b.columns, b.limits.total_columns, "outbox total column limit exceeded");
        recovery_outbox_column c;
        c.ordinal = integer(info, 0, b);
        c.name = text(info, 1, b, false);
        c.declared_type = text(info, 2, b, false);
        c.not_null = flag(info, 3, b);
        auto default_value = scalar(info, 4, b);
        if (auto* v = std::get_if<std::string>(&default_value)) c.default_sql = std::move(*v);
        else if (!std::holds_alternative<std::nullptr_t>(default_value))
            fail(code::corrupt_state, "outbox invalid column default metadata");
        c.primary_key_position = integer(info, 5, b);
        if (c.ordinal != expected_ordinal++ || c.primary_key_position < 0 || integer(info, 6, b) != 0)
            fail(code::unsupported_schema, "outbox hidden/generated or inconsistent columns");
        if (c.primary_key_position) ++primary_count;
        const auto folded = upper(c.name);
        if (folded == "ROWID" || folded == "_ROWID_" || folded == "OID" || column(result, c.name))
            fail(code::unsupported_schema, "outbox shadowed rowid or duplicate column");
        quote(c.name);
        const auto type = upper(c.declared_type);
        if (type != "INTEGER" && type != "REAL" && type != "TEXT" && type != "BLOB")
            fail(code::unsupported_schema, "outbox unsupported declared column type");
        result.columns.push_back(std::move(c)); ++b.columns;
    }
    const auto* global = column(result, "globalId");
    const auto* id = column(result, "id");
    const auto* lhs = column(result, "lhs");
    const auto* rhs = column(result, "rhs");
    const auto* rhs_type = column(result, "rhs_type");
    if (!global || upper(global->declared_type) != "TEXT" || global->primary_key_position)
        fail(code::unsupported_schema, "outbox missing model/link global identity");
    bool global_unique = false, separate_primary = false;
    statement indexes(db, "PRAGMA main.index_list(" + quoted + ")");
    while (indexes.next()) {
        const auto index_name = text(indexes,1,b,false);
        const bool unique = flag(indexes,2,b);
        const auto origin = text(indexes,3,b,false);
        const bool partial = flag(indexes,4,b);
        separate_primary = separate_primary || origin == "pk";
        if (!unique || partial) continue;
        statement keys(db, "PRAGMA main.index_xinfo(" + quote(index_name) + ")");
        int key_count = 0; bool only_global = true;
        while (keys.next()) {
            if (!flag(keys,5,b)) continue;
            ++key_count;
            // Expression keys may have NULL names; they cannot establish the
            // required globalId uniqueness and must not be decoded as text.
            auto key_name = scalar(keys,2,b);
            only_global = only_global && std::holds_alternative<std::string>(key_name) &&
                std::get<std::string>(key_name) == "globalId";
        }
        global_unique = global_unique || (key_count == 1 && only_global);
    }
    if (!global_unique) fail(code::unsupported_schema, "outbox global identity has no complete unique key");
    result.trigger_flags = metadata(db, "trigger_flags:" + name, b);
    result.internal_parent = metadata(db, "internal_table:" + name, b);
    if (id) {
        if (result.internal_parent || upper(id->declared_type) != "INTEGER" ||
            id->primary_key_position != 1 || primary_count != 1 || separate_primary)
            fail(code::unsupported_schema, "outbox internal/union/geo or nonstandard model shape");
        if (!result.trigger_flags) fail(code::corrupt_state, "outbox model trigger_flags metadata missing");
        result.kind = recovery_outbox_table_kind::model;
        std::set<std::string> seen;
        const auto& flags = *result.trigger_flags;
        for (size_t at = 0; at < flags.size();) {
            const auto end = flags.find(',', at);
            const auto part = flags.substr(at, end == std::string::npos ? end : end-at);
            if (part.empty() || part == "id" || part == "globalId" || !column(result, part) || !seen.insert(part).second)
                fail(code::corrupt_state, "outbox corrupt NoHistory flags metadata");
            if (end == std::string::npos) break;
            at = end + 1;
            if (at == flags.size()) fail(code::corrupt_state, "outbox trailing empty NoHistory flag");
        }
    } else {
        if (!lhs || !rhs || !lhs->not_null || !rhs->not_null ||
            upper(lhs->declared_type) != "TEXT" || upper(rhs->declared_type) != "TEXT" ||
            lhs->primary_key_position != 1 || rhs->primary_key_position != (rhs_type ? 3 : 2) ||
            primary_count != (rhs_type ? 3 : 2) || result.columns.size() != (rhs_type ? 4u : 3u))
            fail(code::unsupported_schema, "outbox unsupported link shape");
        if (rhs_type && (!rhs_type->not_null || upper(rhs_type->declared_type) != "TEXT" || rhs_type->primary_key_position != 2))
            fail(code::unsupported_schema, "outbox unsupported polymorphic link discriminator");
        if (!result.internal_parent || (result.trigger_flags && !result.trigger_flags->empty()))
            fail(code::corrupt_state, "outbox missing link ownership or invalid NoHistory metadata");
        result.kind = rhs_type ? recovery_outbox_table_kind::polymorphic_link : recovery_outbox_table_kind::link;
    }
    return result;
}
void payload(sqlite3* db, const recovery_outbox_audit& a, const recovery_outbox_table& t, budget& b) {
    statement valid(db, "SELECT json_valid(?1),json_valid(?2),"
        "CASE WHEN json_valid(?1) THEN json_type(?1) END,"
        "CASE WHEN json_valid(?2) THEN json_type(?2) END");
    valid.text(1, a.changed_fields_json); valid.text(2, a.changed_names_json);
    if (!valid.next() || integer(valid,0,b) != 1 || integer(valid,1,b) != 1 ||
        text(valid,2,b) != "object" || text(valid,3,b) != "array")
        fail(code::corrupt_state, "outbox malformed audit JSON");
    statement fields(db, "SELECT key FROM json_each(?)"); fields.text(1,a.changed_fields_json);
    std::set<std::string> keys;
    while (fields.next()) {
        auto key = text(fields,0,b,false);
        if (!column(t,key) || !keys.insert(key).second)
            fail(code::corrupt_state, "outbox unknown or duplicate audit field");
    }
    statement names(db, "SELECT type,value FROM json_each(?)"); names.text(1,a.changed_names_json);
    std::set<std::string> changed;
    size_t name_count = 0;
    bool filter_removal = false;
    while (names.next()) {
        ++name_count;
        auto type = text(names,0,b);
        if (type == "null") continue; // native UPDATE trigger emits NULL slots
        if (type != "text") fail(code::corrupt_state, "outbox nontext changed field name");
        auto key = text(names,1,b,false);
        if (key == "__lattice_filter_removal") {
            filter_removal = true;
            continue;
        }
        if (!column(t,key) || !keys.count(key) || !changed.insert(key).second)
            fail(code::corrupt_state, "outbox incomplete or duplicate changed field name");
    }
    // Existing no-relay purge/narrowing producer: this is a routing marker,
    // not a model property or permission to turn it into a relayed DELETE.
    // Keep its raw payload unchanged and admit only the exact producer shape.
    if (filter_removal && (a.operation != "DELETE" || a.row_id != 0 || !keys.empty() || name_count != 1))
        fail(code::corrupt_state, "outbox malformed no-relay filter-removal marker");
}
recovery_outbox_current_row current(sqlite3* db, size_t table_index,
    const recovery_outbox_table& t, const std::string& id, budget& b) {
    recovery_outbox_current_row result;
    result.table_index = table_index;
    b.charge(id.size()); result.lookup_global_id = id;
    std::string sql = "SELECT rowid";
    for (const auto& c : t.columns) sql += "," + quote(c.name);
    sql += " FROM main." + quote(t.name) + " WHERE \"globalId\"=? LIMIT 2";
    statement row(db,sql); row.text(1,id);
    if (!row.next()) return result; // explicit absence is part of the capture
    result.present = true;
    result.local_row_id = integer(row,0,b);
    for (size_t i = 0; i < t.columns.size(); ++i) {
        auto value = scalar(row,static_cast<int>(i + 1),b);
        const auto& c = t.columns[i];
        if ((c.not_null || c.name == "globalId" || c.name == "id") && std::holds_alternative<std::nullptr_t>(value))
            fail(code::corrupt_state, "outbox required current value is NULL");
        const auto type = upper(c.declared_type);
        if (!std::holds_alternative<std::nullptr_t>(value) &&
            ((type == "TEXT" && !std::holds_alternative<std::string>(value)) ||
             (type == "BLOB" && !std::holds_alternative<std::vector<uint8_t>>(value)) ||
             (type == "INTEGER" && !std::holds_alternative<int64_t>(value)) ||
             (type == "REAL" && !std::holds_alternative<double>(value) && !std::holds_alternative<int64_t>(value))))
            fail(code::corrupt_state, "outbox current value violates declared scalar type");
        if (c.name == "id" && std::get<int64_t>(value) != *result.local_row_id)
            fail(code::unsupported_schema, "outbox model id is not its physical rowid");
        if ((c.name == "globalId" || (t.kind != recovery_outbox_table_kind::model &&
              (c.name == "lhs" || c.name == "rhs" || c.name == "rhs_type"))) && std::get<std::string>(value).empty())
            fail(code::corrupt_state, "outbox empty current identity");
        result.values.push_back(std::move(value));
    }
    if (row.next()) fail(code::corrupt_state, "outbox ambiguous current global identity");
    return result;
}
} // namespace

recovery_outbox_capture capture_pending_outbox(lattice_db& owner,
    const std::string& sync_id, const recovery_outbox_limits& limits) {
    if (sync_id.empty()) fail(code::invalid_argument, "outbox requires a channel identity");
    auto* writer = recovery_writer_access::active_writer(owner);
    if (!writer) fail(code::transaction_required, "outbox requires this thread's owned main write transaction");
    auto* db = writer->handle();
    budget b{limits};
    recovery_outbox_capture result;
    b.charge(sync_id.size()); result.sync_id = sync_id;
    // An explicit unresolved receipt without its AuditLog body cannot be
    // reconstructed from current rows. Do not silently lose it in the join.
    statement orphan(db, "SELECT 1 FROM main._lattice_sync_state ss "
        "LEFT JOIN main.AuditLog a ON a.id=ss.audit_entry_id WHERE ss.sync_id=? AND "
        "(typeof(ss.is_synchronized)!='integer' OR ss.is_synchronized!=1) AND a.id IS NULL LIMIT 1");
    orphan.text(1,sync_id);
    if (orphan.next()) fail(code::corrupt_state, "outbox pending receipt has no retained audit record");
    std::map<std::string,size_t> tables;
    std::set<std::pair<size_t,std::string>> rows;
    int64_t after = 0;
    for (;;) {
        // Keyset page of one. Corrupt flags on a potentially pending identity
        // are admitted to strict decoding, never silently treated as an ACK.
        const std::string select = "SELECT a.id,a.globalId,a.tableName,a.operation,a.rowId,a.globalRowId,"
            "a.changedFields,a.changedFieldsNames,a.isFromRemote,a.isSynchronized,a.timestamp,a.synthesized,"
            "ss.is_synchronized,ss.audit_entry_id FROM main.AuditLog a "
            "LEFT JOIN main._lattice_sync_state ss ON ss.audit_entry_id=a.id AND ss.sync_id=?1 WHERE ";
        statement s(db, select + (after ? "a.id>?2 AND " : "") +
            "((ss.audit_entry_id IS NOT NULL AND "
            "(typeof(ss.is_synchronized)!='integer' OR ss.is_synchronized!=1)) OR "
            "(ss.audit_entry_id IS NULL AND (typeof(a.isSynchronized)!='integer' OR a.isSynchronized!=1))) "
            "ORDER BY a.id LIMIT 1");
        s.text(1,sync_id); if (after) s.integer(2,after);
        if (!s.next()) break;
        b.count(result.audit.size(),limits.audit_records,"outbox audit record limit exceeded");
        recovery_outbox_audit a;
        a.id = integer(s,0,b);
        if (a.id <= after) fail(code::corrupt_state, "outbox invalid audit sequence");
        a.global_id = text(s,1,b,false); a.table_name = text(s,2,b,false);
        a.operation = text(s,3,b,false); a.row_id = integer(s,4,b);
        a.global_row_id = text(s,5,b,false);
        a.changed_fields_json = text(s,6,b,false); a.changed_names_json = text(s,7,b,false);
        a.from_remote = flag(s,8,b); a.globally_synchronized = flag(s,9,b);
        a.timestamp = scalar(s,10,b); a.synthesized = flag(s,11,b);
        if (a.row_id < 0 || (a.operation != "INSERT" && a.operation != "UPDATE" && a.operation != "DELETE") ||
            std::holds_alternative<std::nullptr_t>(a.timestamp) || std::holds_alternative<std::vector<uint8_t>>(a.timestamp) ||
            (std::holds_alternative<std::string>(a.timestamp) && std::get<std::string>(a.timestamp).empty()))
            fail(code::corrupt_state, "outbox invalid audit operation/row/timestamp");
        if (sqlite3_column_type(s.value,13) != SQLITE_NULL) {
            if (integer(s,13,b) != a.id || flag(s,12,b))
                fail(code::corrupt_state, "outbox invalid pending channel state");
            a.channel_synchronized = 0;
        } else if (a.globally_synchronized || sqlite3_column_type(s.value,12) != SQLITE_NULL) {
            fail(code::corrupt_state, "outbox inconsistent implicit pending state");
        }
        size_t index;
        auto existing = tables.find(a.table_name);
        if (existing == tables.end()) {
            b.count(result.tables.size(),limits.tables,"outbox table limit exceeded");
            index = result.tables.size();
            result.tables.push_back(table(db,a.table_name,b));
            tables.emplace(a.table_name,index);
        } else index = existing->second;
        payload(db,a,result.tables[index],b);
        const auto key = std::make_pair(index,a.global_row_id);
        if (!rows.count(key)) {
            b.count(result.current_rows.size(),limits.current_rows,"outbox current row limit exceeded");
            result.current_rows.push_back(current(db,index,result.tables[index],a.global_row_id,b));
            rows.insert(key);
        }
        after = a.id;
        result.audit.push_back(std::move(a));
    }
    if (recovery_writer_access::active_writer(owner) != writer)
        fail(code::transaction_required, "outbox writer admission changed during capture");
    result.charged_fields = b.fields; result.charged_logical_bytes = b.bytes;
    return result;
}
} // namespace lattice::detail
