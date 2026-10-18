#pragma once
#include "derived_program.hpp"
#include <array>
#include <cstddef>
#include <sstream>
#include <string>

namespace lattice::detail {
// Pure ordinary geographic SQL. This describes existing setup, including its
// lazy shortcuts, and grants no schema/recovery authority. Admission callers
// must bound/validate declarations and compare the complete retained schema.
// Emitters execute synchronously and store no callback; ordinary setup uses
// them to keep the original statement construction/execution sequence.
template<class Text>
Text ordinary_geo_rtree_table_program_storage(const Text& rtree_table) {
    ordinary_program_storage_stream<Text> sql;
    sql << "CREATE VIRTUAL TABLE " << rtree_table << " USING rtree("
        << "id, "           // Matches main table id
        << "minLat, maxLat, "
        << "minLon, maxLon"
        << ")";
    return sql.str();
}
inline std::string ordinary_geo_rtree_table_program(const std::string& rtree_table) {
    return ordinary_geo_rtree_table_program_storage<std::string>(rtree_table);
}

template<class Text,class Emit>
void ordinary_geo_model_rtree_programs_storage(const Text& model_table, const Text& column_name, const Text& rtree_table, Emit&& emit) {
    // Column names in main table
    Text minLat = column_name + "_minLat";
    Text maxLat = column_name + "_maxLat";
    Text minLon = column_name + "_minLon";
    Text maxLon = column_name + "_maxLon";

    // Create triggers to keep R*Tree in sync with main table.
    // Use main.-qualified model_table in ON clause so triggers work
    // even when a TEMP UNION ALL view shadows the model table (from attach()).
    // Note: SQLite forbids qualified names inside trigger bodies.
    // INSERT trigger - add to R*Tree when row is inserted with non-null geo data
    ordinary_program_storage_stream<Text> insert_trigger;
    insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                   << "AFTER INSERT ON main." << model_table << " "
                   << "WHEN NEW." << minLat << " IS NOT NULL "
                   << "BEGIN "
                   << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                   << "VALUES (NEW.id, NEW." << minLat << ", NEW." << maxLat << ", "
                   << "NEW." << minLon << ", NEW." << maxLon << "); "
                   << "END";
    emit(insert_trigger.str());

    // UPDATE trigger - update R*Tree when geo columns change
    ordinary_program_storage_stream<Text> update_trigger;
    update_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_update "
                   << "AFTER UPDATE OF " << minLat << ", " << maxLat << ", "
                   << minLon << ", " << maxLon << " ON main." << model_table << " "
                   << "BEGIN "
                   << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                   << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                   << "SELECT NEW.id, NEW." << minLat << ", NEW." << maxLat << ", "
                   << "NEW." << minLon << ", NEW." << maxLon << " "
                   << "WHERE NEW." << minLat << " IS NOT NULL; "
                   << "END";
    emit(update_trigger.str());

    // DELETE trigger - remove from R*Tree when row is deleted
    ordinary_program_storage_stream<Text> delete_trigger;
    delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                   << "AFTER DELETE ON main." << model_table << " "
                   << "BEGIN "
                   << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                   << "END";
    emit(delete_trigger.str());
}
template<class Emit>
void ordinary_geo_model_rtree_programs(const std::string& model_table, const std::string& column_name, const std::string& rtree_table, Emit&& emit) {
    ordinary_geo_model_rtree_programs_storage<std::string>(model_table,column_name,rtree_table,std::forward<Emit>(emit));
}

template<class Text>
Text ordinary_geo_list_table_program_storage(const Text& list_table) {
    ordinary_program_storage_stream<Text> sql;
    sql << "CREATE TABLE IF NOT EXISTS " << list_table << "("
        << "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        << "parent_id TEXT NOT NULL, "  // globalId of parent row
        << "minLat REAL NOT NULL, "
        << "maxLat REAL NOT NULL, "
        << "minLon REAL NOT NULL, "
        << "maxLon REAL NOT NULL, "
        << "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
        << "lower(hex(randomblob(4))) || '-' || "
        << "lower(hex(randomblob(2))) || '-' || "
        << "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
        << "substr('89AB', 1 + (abs(random()) % 4), 1) || "
        << "substr(lower(hex(randomblob(2))),2) || '-' || "
        << "lower(hex(randomblob(6)))"
        << ")"
        << ")";
    return sql.str();
}
inline std::string ordinary_geo_list_table_program(const std::string& list_table) {
    return ordinary_geo_list_table_program_storage<std::string>(list_table);
}

template<class Text>
Text ordinary_geo_list_parent_index_program_storage(const Text& list_table) {
    Text idx_sql = "CREATE INDEX IF NOT EXISTS idx_" +
        list_table + "_parent ON " + list_table + "(parent_id)";
    return idx_sql;
}
inline std::string ordinary_geo_list_parent_index_program(const std::string& list_table) {
    return ordinary_geo_list_parent_index_program_storage<std::string>(list_table);
}

template<class Text,class Emit>
void ordinary_geo_list_rtree_programs_storage(const Text& list_table, const Text& rtree_table, Emit&& emit) {
    // Create triggers to keep R*Tree in sync with list table
    // INSERT trigger
    ordinary_program_storage_stream<Text> insert_trigger;
    insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                   << "AFTER INSERT ON " << list_table << " "
                   << "BEGIN "
                   << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                   << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                   << "END";
    emit(insert_trigger.str());

    // UPDATE trigger
    ordinary_program_storage_stream<Text> update_trigger;
    update_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_update "
                   << "AFTER UPDATE OF minLat, maxLat, minLon, maxLon ON " << list_table << " "
                   << "BEGIN "
                   << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                   << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                   << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                   << "END";
    emit(update_trigger.str());

    // DELETE trigger
    ordinary_program_storage_stream<Text> delete_trigger;
    delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                   << "AFTER DELETE ON " << list_table << " "
                   << "BEGIN "
                   << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                   << "END";
    emit(delete_trigger.str());
}
template<class Emit>
void ordinary_geo_list_rtree_programs(const std::string& list_table, const std::string& rtree_table, Emit&& emit) {
    ordinary_geo_list_rtree_programs_storage<std::string>(list_table,rtree_table,std::forward<Emit>(emit));
}

template<class Text,class Emit>
void ordinary_geo_list_audit_programs_storage(const Text& list_table, Emit&& emit) {
    // INSERT trigger for AuditLog
    Text insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Insert"
        " AFTER INSERT ON " + list_table +
        " WHEN NOT sync_disabled()"
        " BEGIN"
        "   INSERT INTO AuditLog(tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames)"
        "   VALUES("
        "       '" + list_table + "',"
        "       'INSERT',"
        "       NEW.id,"
        "       NEW.globalId,"
        "       json_object("
        "           'parent_id', json_object('kind', 2, 'value', NEW.parent_id),"
        "           'minLat', json_object('kind', 3, 'value', NEW.minLat),"
        "           'maxLat', json_object('kind', 3, 'value', NEW.maxLat),"
        "           'minLon', json_object('kind', 3, 'value', NEW.minLon),"
        "           'maxLon', json_object('kind', 3, 'value', NEW.maxLon)"
        "       ),"
        "       json_array('parent_id', 'minLat', 'maxLat', 'minLon', 'maxLon')"
        "   );"
        " END";
    emit(insert_trigger);

    // DELETE trigger for AuditLog
    Text delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Delete"
        " AFTER DELETE ON " + list_table +
        " WHEN NOT sync_disabled()"
        " BEGIN"
        "   INSERT INTO AuditLog(tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames)"
        "   VALUES("
        "       '" + list_table + "',"
        "       'DELETE',"
        "       OLD.id,"
        "       OLD.globalId,"
        "       '{}',"
        "       '[]'"
        "   );"
        " END";
    emit(delete_trigger);
}
template<class Emit>
void ordinary_geo_list_audit_programs(const std::string& list_table, Emit&& emit) {
    ordinary_geo_list_audit_programs_storage<std::string>(list_table,std::forward<Emit>(emit));
}

struct ordinary_rtree_definition {
    std::string table;
    std::string create_table;
    std::array<derived_trigger_program,3> triggers;
};
template<class Text,class Definition>
Definition ordinary_rtree_program_storage(const Text& model, const Text& column) {
    Definition p;
    p.table = "_" + model + "_" + column + "_rtree";
    p.create_table = ordinary_geo_rtree_table_program_storage<Text>(p.table);
    const std::array<const char*,3> suffixes{"_insert", "_update", "_delete"};
    std::size_t at = 0;
    ordinary_geo_model_rtree_programs_storage(model, column, p.table, [&](const Text& sql) {
        const auto index = at++;
        p.triggers.at(index) = {p.table + suffixes.at(index), model, sql};
    });
    return p;
}
inline ordinary_rtree_definition ordinary_rtree_program(const std::string& model, const std::string& column) {
    return ordinary_rtree_program_storage<std::string,ordinary_rtree_definition>(model,column);
}

struct ordinary_geo_list_definition {
    std::string table;
    std::string create_table;
    std::string parent_index_name;
    std::string parent_index;
    std::string rtree_table;
    std::string rtree_create_table;
    std::array<derived_trigger_program,3> rtree_triggers;
    std::array<derived_trigger_program,2> audit_triggers;
};
template<class Text,class Definition>
Definition ordinary_geo_list_program_storage(const Text& list_table) {
    Definition p;
    p.table = list_table;
    p.create_table = ordinary_geo_list_table_program_storage<Text>(list_table);
    p.parent_index_name = "idx_" + list_table + "_parent";
    p.parent_index = ordinary_geo_list_parent_index_program_storage<Text>(list_table);
    p.rtree_table = list_table + "_rtree";
    p.rtree_create_table = ordinary_geo_rtree_table_program_storage<Text>(p.rtree_table);
    const std::array<const char*,3> suffixes{"_insert", "_update", "_delete"};
    std::size_t at = 0;
    ordinary_geo_list_rtree_programs_storage(list_table, p.rtree_table, [&](const Text& sql) {
        const auto index = at++;
        p.rtree_triggers.at(index) = {p.rtree_table + suffixes.at(index), list_table, sql};
    });
    const std::array<const char*,2> operations{"Insert", "Delete"};
    at = 0;
    ordinary_geo_list_audit_programs_storage(list_table, [&](const Text& sql) {
        const auto index = at++;
        p.audit_triggers.at(index) = {"Audit" + list_table + operations.at(index), list_table, sql};
    });
    return p;
}
inline ordinary_geo_list_definition ordinary_geo_list_program(const std::string& list_table) {
    return ordinary_geo_list_program_storage<std::string,ordinary_geo_list_definition>(list_table);
}
} // namespace lattice::detail
