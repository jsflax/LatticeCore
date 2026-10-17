#pragma once
#include "schema.hpp"
#include <algorithm>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace lattice::detail {
// Pure descriptions of the SQL used by actual ordinary setup. These functions
// do not execute SQL, inspect a database, or confer schema/source authority.
// Callers validating a store must first bound declaration inputs, then compare
// these programs with the complete current schema under their retained view.
inline std::string ordinary_sql_type_string(column_type type) {
    switch (type) {
        case column_type::integer: return "INTEGER";
        case column_type::real: return "REAL";
        case column_type::text: return "TEXT";
        case column_type::blob: return "BLOB";
    }
    return "TEXT";
}
template<class Emit>
void ordinary_model_audit_programs(const std::string& table_name, const std::vector<std::pair<std::string,column_type>>& columns, const std::set<std::string>& no_history, const std::string& canonical_receipt_tail, Emit&& emit) {
    // Skip if no columns to track
    if (columns.empty()) return;

    // Helper to wrap BLOB columns with hex() for JSON compatibility
    auto value_expr = [](const std::string& prefix, const std::string& col, column_type type) {
        if (type == column_type::blob) {
            return "hex(" + prefix + "." + col + ")";
        }
        return prefix + "." + col;
    };

    // Build the changed fields comparison for UPDATE trigger
    std::string update_when_clause;
    std::string json_fields;
    std::string json_names;

    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& [col, type] = columns[i];
        if (i > 0) {
            update_when_clause += " OR ";
            json_fields += ",";
            json_names += ",";
        }
        update_when_clause += "OLD." + col + " IS NOT NEW." + col;
        // For UPDATE: only include changed fields (simple values like Swift's format).
        // A no_history column records THAT it changed (it still gates the
        // trigger and appears in changedFieldsNames) but never its value:
        // a streamed column rewritten ~10×/s otherwise copies its whole,
        // growing body into every audit row (a 325 KB think block left
        // 1.5 GB of history). Sync late-binds the live value at upload.
        if (no_history.count(col)) {
            json_fields += "'" + col + "', NULL";
        } else {
            json_fields += "'" + col + "', "
                "CASE WHEN OLD." + col + " IS NOT NEW." + col + " THEN " + value_expr("NEW", col, type) + " ELSE NULL END";
        }
        json_names += "CASE WHEN OLD." + col + " IS NOT NEW." + col + " THEN '" + col + "' ELSE NULL END";
    }

    // INSERT JSON (all fields)
    std::string insert_json_fields;
    std::string insert_json_names;
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& [col, type] = columns[i];
        if (i > 0) {
            insert_json_fields += ",";
            insert_json_names += ",";
        }
        insert_json_fields += "'" + col + "', " + value_expr("NEW", col, type);
        insert_json_names += "'" + col + "'";
    }

    // DELETE JSON (all old fields)
    std::string delete_json_fields;
    std::string delete_json_names;
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& [col, type] = columns[i];
        if (i > 0) {
            delete_json_fields += ",";
            delete_json_names += ",";
        }
        delete_json_fields += "'" + col + "', " + value_expr("OLD", col, type);
        delete_json_names += "'" + col + "'";
    }

    // UPDATE trigger
    std::string update_trigger = "CREATE TRIGGER IF NOT EXISTS AuditLog_Update_" + table_name +
        " AFTER UPDATE ON " + table_name +
        " WHEN ((sync_disabled() = 0) AND (" + update_when_clause + "))"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + table_name + "',"
        "       'UPDATE',"
        "       OLD.id,"
        "       OLD.globalId,"
        "       json_object(" + json_fields + "),"
        "       json_array(" + json_names + "),"
        "       unixepoch('subsec')"
        "   );" + canonical_receipt_tail +
        " END";
    emit(update_trigger);

    // INSERT trigger
    std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + table_name + "Insert"
        " AFTER INSERT ON " + table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + table_name + "',"
        "       'INSERT',"
        "       NEW.id,"
        "       NEW.globalId,"
        "       json_object(" + insert_json_fields + "),"
        "       json_array(" + insert_json_names + "),"
        "       unixepoch('subsec')"
        "   );" + canonical_receipt_tail +
        " END";
    emit(insert_trigger);

    // DELETE trigger
    std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + table_name + "Delete"
        " AFTER DELETE ON " + table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + table_name + "',"
        "       'DELETE',"
        "       OLD.id,"
        "       OLD.globalId,"
        "       json_object(" + delete_json_fields + "),"
        "       json_array(" + delete_json_names + "),"
        "       unixepoch('subsec')"
        "   );" + canonical_receipt_tail +
        " END";
    emit(delete_trigger);
}

template<class Emit>
void ordinary_link_audit_programs(const std::string& link_table_name, const std::string& canonical_receipt_tail, Emit&& emit) {
    // INSERT trigger for link table
    std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Insert"
        " AFTER INSERT ON " + link_table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + link_table_name + "',"
        "       'INSERT',"
        "       0,"
        "       NEW.globalId,"
        "       json_object('lhs', NEW.lhs, 'rhs', NEW.rhs),"
        "       json_array('lhs', 'rhs'),"
        "       unixepoch('subsec')"
        "   );" + canonical_receipt_tail +
        " END";
    emit(insert_trigger);

    // DELETE trigger for link table
    std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Delete"
        " AFTER DELETE ON " + link_table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + link_table_name + "',"
        "       'DELETE',"
        "       0,"
        "       OLD.globalId,"
        "       json_object('lhs', OLD.lhs, 'rhs', OLD.rhs),"
        "       json_array('lhs', 'rhs'),"
        "       unixepoch('subsec')"
        "   );" + canonical_receipt_tail +
        " END";
    emit(delete_trigger);
}

template<class Emit>
void ordinary_virtual_link_audit_programs(const std::string& link_table_name, Emit&& emit) {
    // INSERT trigger for virtual link table (includes rhs_type)
    std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Insert"
        " AFTER INSERT ON " + link_table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + link_table_name + "',"
        "       'INSERT',"
        "       0,"
        "       NEW.globalId,"
        "       json_object('lhs', NEW.lhs, 'rhs', NEW.rhs, 'rhs_type', NEW.rhs_type),"
        "       json_array('lhs', 'rhs', 'rhs_type'),"
        "       unixepoch('subsec')"
        "   );"
        " END";
    emit(insert_trigger);

    // DELETE trigger for virtual link table
    std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Delete"
        " AFTER DELETE ON " + link_table_name +
        " WHEN (sync_disabled() = 0)"
        " BEGIN"
        "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
        "   VALUES ("
        "       '" + link_table_name + "',"
        "       'DELETE',"
        "       0,"
        "       OLD.globalId,"
        "       json_object('lhs', OLD.lhs, 'rhs', OLD.rhs, 'rhs_type', OLD.rhs_type),"
        "       json_array('lhs', 'rhs', 'rhs_type'),"
        "       unixepoch('subsec')"
        "   );"
        " END";
    emit(delete_trigger);
}

inline std::string ordinary_link_table_program(const std::string& link_table_name) {
    std::string sql = "CREATE TABLE IF NOT EXISTS " + link_table_name + "("
        "lhs TEXT NOT NULL, "
        "rhs TEXT NOT NULL, "
        "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
            "lower(hex(randomblob(4))) || '-' || "
            "lower(hex(randomblob(2))) || '-' || "
            "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
            "substr('89AB', 1 + (abs(random()) % 4), 1) || "
            "substr(lower(hex(randomblob(2))),2) || '-' || "
            "lower(hex(randomblob(6)))"
        "), "
        "PRIMARY KEY(lhs, rhs)"
    ")";
    return sql;
}

inline std::string ordinary_virtual_link_table_program(const std::string& link_table_name) {
    std::string sql = "CREATE TABLE IF NOT EXISTS " + link_table_name + "("
        "lhs TEXT NOT NULL, "
        "rhs TEXT NOT NULL, "
        "rhs_type TEXT NOT NULL, "
        "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
            "lower(hex(randomblob(4))) || '-' || "
            "lower(hex(randomblob(2))) || '-' || "
            "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
            "substr('89AB', 1 + (abs(random()) % 4), 1) || "
            "substr(lower(hex(randomblob(2))),2) || '-' || "
            "lower(hex(randomblob(6)))"
        "), "
        "PRIMARY KEY(lhs, rhs_type, rhs)"
    ")";
    return sql;
}

struct ordinary_union_definition {
    std::string create_table;
    std::vector<std::pair<std::string,column_type>> columns;
};
inline ordinary_union_definition ordinary_union_program(const std::string& union_table_name, const union_descriptor& desc) {
    // Build column info from descriptor
    std::vector<std::pair<std::string, column_type>> columns;
    columns.push_back({"\"case\"", column_type::text});
    std::map<std::string, std::vector<std::string>> case_to_cols;
    std::vector<std::string> all_case_cols;

    std::ostringstream sql;
    sql << "CREATE TABLE " << union_table_name << "("
        << "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        << "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
        <<   "lower(hex(randomblob(4))) || '-' || "
        <<   "lower(hex(randomblob(2))) || '-' || "
        <<   "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
        <<   "substr('89ab', 1 + (abs(random()) % 4), 1) || "
        <<   "substr(lower(hex(randomblob(2))),2) || '-' || "
        <<   "lower(hex(randomblob(6)))"
        << "), "
        << "\"case\" TEXT NOT NULL";

    for (const auto& c : desc.cases) {
        std::vector<std::string> cols;
        if (c.values.empty()) {
            case_to_cols[c.case_name] = {};
            continue;
        }
        if (c.values.size() == 1 && c.values[0].param_name.empty()) {
            std::string col = c.case_name;
            sql << ", " << col << " " << ordinary_sql_type_string(c.values[0].type);
            columns.push_back({col, c.values[0].type});
            cols.push_back(col);
        } else {
            for (const auto& val : c.values) {
                std::string col = c.case_name + "__" + val.param_name;
                sql << ", " << col << " " << ordinary_sql_type_string(val.type);
                columns.push_back({col, val.type});
                cols.push_back(col);
            }
        }
        case_to_cols[c.case_name] = cols;
        all_case_cols.insert(all_case_cols.end(), cols.begin(), cols.end());
    }

    // CHECK constraint
    if (!all_case_cols.empty()) {
        sql << ", CHECK(";
        bool first_case = true;
        for (const auto& c : desc.cases) {
            if (!first_case) sql << " OR ";
            first_case = false;
            sql << "(\"case\" = '" << c.case_name << "'";
            auto my_cols_it = case_to_cols.find(c.case_name);
            const auto& my_cols = (my_cols_it != case_to_cols.end())
                ? my_cols_it->second : std::vector<std::string>{};
            for (const auto& col : my_cols) {
                sql << " AND " << col << " IS NOT NULL";
            }
            for (const auto& col : all_case_cols) {
                if (std::find(my_cols.begin(), my_cols.end(), col) == my_cols.end()) {
                    sql << " AND " << col << " IS NULL";
                }
            }
            sql << ")";
        }
        sql << ")";
    }

    sql << ")";
    return {sql.str(),std::move(columns)};
}

inline std::string ordinary_union_case_index_program(const std::string& union_table_name) {
    return "CREATE INDEX IF NOT EXISTS idx_" + union_table_name + "_case ON " + union_table_name + "(\"case\")";
}

inline std::string ordinary_union_cascade_program(const std::string& parent_table, const std::string& field_name, const std::string& union_table_name) {
    std::string trigger_name = "_cascade_delete_" + parent_table + "_" + field_name + "_union";
    std::string sql =
        "CREATE TRIGGER IF NOT EXISTS " + trigger_name +
        " BEFORE DELETE ON " + parent_table +
        " WHEN OLD." + field_name + " IS NOT NULL AND OLD." + field_name + " != ''"
        " BEGIN"
        "   DELETE FROM " + union_table_name + " WHERE globalId = OLD." + field_name + ";"
        " END";
    return sql;
}

struct ordinary_model_definition {
    std::string create_table;
    std::vector<std::pair<std::string,column_type>> columns;
};
inline ordinary_model_definition ordinary_model_program(const model_schema& schema) {
    // Create table with SQL-generated UUID default for globalId
    // This matches Lattice.swift's table creation
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << schema.table_name << "(";
    sql << "id INTEGER PRIMARY KEY AUTOINCREMENT, ";
    sql << "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
           "lower(hex(randomblob(4))) || '-' || "
           "lower(hex(randomblob(2))) || '-' || "
           "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
           "substr('89AB', 1 + (abs(random()) % 4), 1) || "
           "substr(lower(hex(randomblob(2))),2) || '-' || "
           "lower(hex(randomblob(6)))"
           ")";

    // Collect column names and types for triggers
    std::vector<std::pair<std::string, column_type>> columns;

    for (const auto& prop : schema.properties) {
        // Skip list properties - they use separate tables (link_list, geo_bounds_list)
        if (prop.kind == property_kind::list) {
            continue;
        }
        if (prop.is_geo_bounds) {
            // geo_bounds expands to 4 REAL columns
            sql << ", " << prop.name << "_minLat REAL";
            sql << ", " << prop.name << "_maxLat REAL";
            sql << ", " << prop.name << "_minLon REAL";
            sql << ", " << prop.name << "_maxLon REAL";
            // Add all 4 columns to trigger list
            columns.push_back({prop.name + "_minLat", column_type::real});
            columns.push_back({prop.name + "_maxLat", column_type::real});
            columns.push_back({prop.name + "_minLon", column_type::real});
            columns.push_back({prop.name + "_maxLon", column_type::real});
        } else {
            sql << ", " << prop.name << " ";
            switch (prop.type) {
                case column_type::integer: sql << "INTEGER"; break;
                case column_type::real: sql << "REAL"; break;
                case column_type::text: sql << "TEXT"; break;
                case column_type::blob: sql << "BLOB"; break;
            }
            if (!prop.nullable) {
                sql << " NOT NULL";
            }
            columns.push_back({prop.name, prop.type});
        }
    }
    sql << ")";
    return {sql.str(),std::move(columns)};
}

template<class Emit>
void ordinary_link_shadow_programs(const std::string& parent_table, const std::string& link_table, const std::string& shadow_col, Emit&& emit) {
    const std::string ins_trig =
        "LinkShadow_" + link_table + "_Insert_" + shadow_col;
    emit(
        "CREATE TRIGGER IF NOT EXISTS " + ins_trig +
        " AFTER INSERT ON " + link_table +
        " BEGIN"
        "   UPDATE " + parent_table +
        "   SET " + shadow_col + " = NEW.rhs"
        "   WHERE globalId = NEW.lhs;"
        " END");

    const std::string del_trig =
        "LinkShadow_" + link_table + "_Delete_" + shadow_col;
    // Guard with `shadow = OLD.rhs` so a stale delete doesn't
    // clobber a freshly re-assigned link.
    emit(
        "CREATE TRIGGER IF NOT EXISTS " + del_trig +
        " AFTER DELETE ON " + link_table +
        " BEGIN"
        "   UPDATE " + parent_table +
        "   SET " + shadow_col + " = NULL"
        "   WHERE globalId = OLD.lhs"
        "     AND " + shadow_col + " = OLD.rhs;"
        " END");

}

inline std::string ordinary_unique_index_program(const std::string& table, size_t ordinal, const std::vector<std::string>& columns) {
    std::ostringstream sql;
    sql << "CREATE UNIQUE INDEX IF NOT EXISTS unique_" << table << "_" << ordinal
        << " ON " << table << "(";
    for(size_t i=0;i<columns.size();++i) {
        if(i)sql << ", ";
        sql << columns[i];
    }
    sql << ")";
    return sql.str();
}

inline std::string ordinary_property_index_program(const std::string& table, const std::string& property) {
    std::ostringstream sql;
    sql << "CREATE INDEX IF NOT EXISTS idx_" << table << "_" << property
        << " ON " << table << "(" << property << ")";
    return sql.str();
}

} // namespace lattice::detail
