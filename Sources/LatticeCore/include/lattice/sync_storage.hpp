#pragma once

#include "db.hpp"
#include "sync.hpp"

namespace lattice::detail {

// Epoch 9 joins the released six-column outbox and the asset-reader ten-column
// outbox. Receipt identity and legacy disposition never alter the wire payload.
inline constexpr int sync_storage_writer_epoch = 9;
inline constexpr size_t sync_storage_retained_bytes = 2 * sync_max_encoded_message_bytes;
inline constexpr size_t sync_storage_retained_rows = 4096;
inline constexpr size_t sync_storage_envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;

inline std::string sync_storage_quote(const std::string& value) {
    std::string result = "\"";
    for (char c : value) { if (c == '"') result += '"'; result += c; }
    return result + '"';
}

inline std::string sync_storage_guard_sql(const std::string& table, const std::string& operation) {
    return "CREATE TRIGGER " + sync_storage_quote("_lattice_writer_v9_" + table + "_" + operation) +
        " BEFORE " + operation + " ON " + sync_storage_quote(table) +
        " BEGIN SELECT CASE WHEN lattice_storage_writer_epoch() != 9 "
        "THEN RAISE(ABORT,'Lattice writer storage epoch mismatch') END; END";
}

inline void register_sync_storage_writer(database& db) {
    if (sqlite3_create_function_v2(db.handle(), "lattice_storage_writer_epoch", 0,
            SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, nullptr,
            [](sqlite3_context* context, int, sqlite3_value**) {
                sqlite3_result_int(context, sync_storage_writer_epoch);
            }, nullptr, nullptr, nullptr) != SQLITE_OK)
        throw db_error("Cannot register Lattice storage writer capability");
}

inline void ensure_sync_storage_guard(database& db, const std::string& table) {
    for (const std::string operation : {"INSERT", "UPDATE", "DELETE"}) {
        const auto name = "_lattice_writer_v9_" + table + "_" + operation;
        const auto expected = sync_storage_guard_sql(table, operation);
        const auto rows = db.query("SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=?", {name});
        if (rows.empty()) db.execute(expected);
        else if (rows.size() != 1 || !std::holds_alternative<std::string>(rows[0].at("sql")) ||
                 std::get<std::string>(rows[0].at("sql")) != expected)
            throw db_error("Lattice storage writer guard has an unexpected definition");
    }
}

inline void ensure_sync_storage_guards(database& db) {
    if (!db.is_in_transaction()) throw db_error("Storage guard installation requires a write transaction");
    // The guard covers NoHistory tables too. An old connection already open at
    // cutover lacks this capability function, so its subsequent DML aborts even
    // when it disables audit generation. Older opens also fail on _SyncControl
    // or _lattice_meta rather than rewriting the new fingerprint.
    const auto tables = db.query("SELECT name FROM pragma_table_list WHERE schema='main' AND type='table' "
                                "AND name NOT GLOB 'sqlite_*'");
    for (const auto& row : tables) ensure_sync_storage_guard(db, std::get<std::string>(row.at("name")));
}

inline bool only_sync_storage_guards(database& db, const std::string& table) {
    const auto rows = db.query("SELECT name,sql FROM main.sqlite_schema WHERE type='trigger' AND tbl_name=?", {table});
    if (rows.empty()) return true; // Explicit checked bootstrap/repair can precede guard installation.
    if (rows.size() != 3) return false;
    for (const auto& row : rows) {
        bool exact = false;
        for (const std::string operation : {"INSERT", "UPDATE", "DELETE"})
            exact |= std::holds_alternative<std::string>(row.at("name")) &&
                std::holds_alternative<std::string>(row.at("sql")) &&
                std::get<std::string>(row.at("name")) == "_lattice_writer_v9_" + table + "_" + operation &&
                std::get<std::string>(row.at("sql")) == sync_storage_guard_sql(table, operation);
        if (!exact) return false;
    }
    return true;
}

inline std::string sync_outbox_v9_sql(const std::string& name) {
    return "CREATE TABLE " + sync_storage_quote(name) + " ("
        "receipt_id INTEGER PRIMARY KEY AUTOINCREMENT, sync_id TEXT NOT NULL, "
        "source_audit_id INTEGER, source_global_id TEXT, wire_global_id TEXT NOT NULL, table_name TEXT NOT NULL, "
        "global_row_id TEXT NOT NULL, membership INTEGER NOT NULL CHECK(membership IN(-1,0,1)), "
        "wire_json TEXT NOT NULL, routing_key TEXT NOT NULL, encoded_charge INTEGER NOT NULL CHECK(encoded_charge>0), "
        "legacy_receipt INTEGER NOT NULL DEFAULT 0 CHECK(legacy_receipt IN(0,1)), "
        "CHECK(legacy_receipt=1 OR (typeof(source_audit_id)='integer' AND source_audit_id>0 "
        "AND typeof(source_global_id)='text' AND length(source_global_id)>0)), UNIQUE(sync_id,wire_global_id))";
}
inline void create_sync_outbox_v9(database& db, const std::string& name) {
    db.execute(sync_outbox_v9_sql(name));
    // receipt_id preserves released FIFO independently of source audit order.
    // Modern admission transactionally excludes another source/entity receipt;
    // migration must retain the weaker released SQL contract, including NULL
    // provenance and already-admitted same-entity sequences.
}

inline std::vector<std::string> sync_outbox_columns(database& db) {
    const auto rows = db.query("PRAGMA main.table_xinfo('_lattice_sync_outbox')");
    std::vector<std::string> result;
    for (const auto& row : rows) {
        if (!std::holds_alternative<int64_t>(row.at("hidden")) || std::get<int64_t>(row.at("hidden")) != 0 ||
            !std::holds_alternative<std::string>(row.at("name")))
            throw db_error("Unsupported sync outbox column shape");
        result.push_back(std::get<std::string>(row.at("name")));
    }
    return result;
}

inline void ensure_sync_outbox_schema(database& db) {
    if (!db.is_in_transaction()) throw db_error("Sync outbox migration requires its owning transaction");
    const auto object = db.query("SELECT type FROM main.sqlite_schema WHERE name='_lattice_sync_outbox'");
    if (object.empty()) { create_sync_outbox_v9(db, "_lattice_sync_outbox"); return; }
    if (object.size() != 1 || std::get<std::string>(object[0].at("type")) != "table")
        throw db_error("Sync outbox must be a real table");
    const std::vector<std::string> six = {"id", "sync_id", "source_audit_id", "source_global_id", "event_global_id", "payload"};
    const std::vector<std::string> ten = {"sync_id", "source_audit_id", "source_global_id", "wire_global_id", "table_name",
        "global_row_id", "membership", "wire_json", "routing_key", "encoded_charge"};
    auto canonical = ten; canonical.insert(canonical.begin(), "receipt_id"); canonical.push_back("legacy_receipt");
    const auto columns = sync_outbox_columns(db);
    if (columns == canonical) {
        const auto shape = db.query("SELECT sql FROM main.sqlite_schema WHERE name='_lattice_sync_outbox'");
        if (shape.size()!=1 || !std::holds_alternative<std::string>(shape[0].at("sql")) ||
            std::get<std::string>(shape[0].at("sql"))!=sync_outbox_v9_sql("_lattice_sync_outbox"))
            throw db_error("Canonical sync outbox constraints do not match");
        return;
    }
    const bool legacy = columns == six;
    if (!legacy && columns != ten) throw db_error("Unknown sync outbox predecessor; no records changed");
    if (!db.query("SELECT 1 FROM main.sqlite_schema WHERE name='_lattice_sync_outbox_v9_migration'").empty())
        throw db_error("Sync outbox migration destination already exists");

    // Scalar preflight precedes payload hydration, under the SAME write snapshot.
    // A refusal leaves the predecessor intact; it never deletes a receipt to fit.
    const std::string payload = legacy ? "payload" : "wire_json";
    const std::string wire_id = legacy ? "event_global_id" : "wire_global_id";
    std::string metadata = "length(CAST(sync_id AS BLOB))+COALESCE(length(CAST(source_global_id AS BLOB)),0)+"
        "length(CAST(" + wire_id + " AS BLOB))+256";
    if (!legacy) metadata += "+length(CAST(table_name AS BLOB))+length(CAST(global_row_id AS BLOB))+length(CAST(routing_key AS BLOB))";
    std::string invalid_provenance = legacy
        ? "(source_audit_id IS NOT NULL AND (typeof(source_audit_id)!='integer' OR source_audit_id<=0)) OR "
          "(source_global_id IS NOT NULL AND (typeof(source_global_id)!='text' OR length(CAST(source_global_id AS BLOB)) NOT BETWEEN 1 AND 65536)) OR "
          "typeof(id)!='integer' OR "
        : "typeof(source_audit_id)!='integer' OR source_audit_id<=0 OR "
          "typeof(source_global_id)!='text' OR length(CAST(source_global_id AS BLOB)) NOT BETWEEN 1 AND 65536 OR "
          "typeof(table_name)!='text' OR length(CAST(table_name AS BLOB)) NOT BETWEEN 1 AND 65536 OR "
          "typeof(global_row_id)!='text' OR length(CAST(global_row_id AS BLOB)) NOT BETWEEN 1 AND 65536 OR "
          "typeof(routing_key)!='text' OR length(CAST(routing_key AS BLOB)) NOT BETWEEN 1 AND 65536 OR "
          "typeof(membership)!='integer' OR membership NOT IN(-1,0,1) OR "
          "typeof(encoded_charge)!='integer' OR encoded_charge NOT BETWEEN 1 AND " +
          std::to_string(sync_max_encoded_message_bytes) + " OR ";
    const auto invalid = db.query("SELECT 1 FROM main._lattice_sync_outbox WHERE " + invalid_provenance +
        "typeof(sync_id)!='text' OR length(CAST(sync_id AS BLOB)) NOT BETWEEN 1 AND 4096 OR "
        "typeof(" + wire_id + ")!='text' OR length(CAST(" + wire_id + " AS BLOB)) NOT BETWEEN 1 AND 65536 OR "
        "typeof(" + payload + ")!='text' OR length(CAST(" + payload + " AS BLOB))>" +
        std::to_string(sync_max_encoded_message_bytes - sync_storage_envelope) + " LIMIT 1");
    if (!invalid.empty()) throw db_error("Sync outbox predecessor has an invalid or oversized receipt");
    const auto over_budget = db.query("SELECT 1 FROM main._lattice_sync_outbox GROUP BY sync_id HAVING COUNT(*)>4096 "
        "OR SUM(length(CAST(" + payload + " AS BLOB))+" + metadata + ")>" + std::to_string(sync_storage_retained_bytes) + " LIMIT 1");
    if (!over_budget.empty()) throw db_error("Sync outbox predecessor exceeds retained channel budget");

    create_sync_outbox_v9(db, "_lattice_sync_outbox_v9_migration");
    std::optional<int64_t> last_rowid;
    for (;;) {
        const auto rows = db.query("SELECT rowid AS migration_rowid, sync_id, source_audit_id, source_global_id, " +
            wire_id + " AS wire_id, " + payload + " AS wire_json" + (legacy ? "" :
                ",table_name,global_row_id,membership,routing_key,encoded_charge") +
            " FROM main._lattice_sync_outbox" + (last_rowid ? " WHERE rowid>?" : "") + " ORDER BY rowid LIMIT 1",
            last_rowid ? std::vector<column_value_t>{*last_rowid} : std::vector<column_value_t>{});
        if (rows.empty()) break;
        const auto& row = rows[0];
        last_rowid = std::get<int64_t>(row.at("migration_rowid"));
        const auto& bytes = std::get<std::string>(row.at("wire_json"));
        const auto wire = audit_log_entry::from_json(bytes);
        const auto source = std::get_if<int64_t>(&row.at("source_audit_id"));
        if (!wire || wire->global_id != std::get<std::string>(row.at("wire_id")) ||
            wire->table_name.empty() || wire->global_row_id.empty() ||
            (legacy ? wire->id != 0 : !source || wire->id != *source))
            throw db_error("Sync outbox predecessor wire identity mismatch");
        constexpr size_t envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;
        const int64_t charge = static_cast<int64_t>(bytes.size() + envelope);
        if (charge > static_cast<int64_t>(sync_max_encoded_message_bytes))
            throw db_error("Sync outbox predecessor frame exceeds encoded limit");
        int64_t membership = 0; // Released six-column code committed membership at staging.
        std::string routing = "legacy-v7"; // Must pass current policy before replay/rebinding.
        if (!legacy) {
            membership = std::get<int64_t>(row.at("membership"));
            routing = std::get<std::string>(row.at("routing_key"));
            if (wire->table_name != std::get<std::string>(row.at("table_name")) ||
                wire->global_row_id != std::get<std::string>(row.at("global_row_id")) ||
                std::get<int64_t>(row.at("encoded_charge")) != charge || membership < -1 || membership > 1 ||
                routing.empty() || routing.size() > 65536)
                throw db_error("Sync outbox predecessor metadata mismatch");
        }
        db.execute("INSERT INTO main._lattice_sync_outbox_v9_migration VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", {
            legacy ? column_value_t(*last_rowid) : column_value_t(nullptr), row.at("sync_id"), row.at("source_audit_id"), row.at("source_global_id"), wire->global_id, wire->table_name,
            wire->global_row_id, membership, bytes, routing, charge, int64_t{legacy ? 1 : 0}});
    }
    const auto actual_budget = db.query("SELECT 1 FROM main._lattice_sync_outbox_v9_migration GROUP BY sync_id "
        "HAVING SUM(length(CAST(wire_json AS BLOB))+COALESCE(length(CAST(source_global_id AS BLOB)),0)+"
        "length(CAST(wire_global_id AS BLOB))+length(CAST(table_name AS BLOB))+"
        "length(CAST(global_row_id AS BLOB))+length(CAST(routing_key AS BLOB))+128)>" +
        std::to_string(sync_storage_retained_bytes) + " LIMIT 1");
    if (!actual_budget.empty()) throw db_error("Migrated outbox metadata exceeds retained channel budget");
    // DROP/RENAME and writer guards commit with the unchanged bytes, IDs and
    // pre-existing source disposition/membership/floors. No transport is run.
    db.execute("DROP TABLE main._lattice_sync_outbox");
    db.execute("ALTER TABLE main._lattice_sync_outbox_v9_migration RENAME TO _lattice_sync_outbox");
}

} // namespace lattice::detail
