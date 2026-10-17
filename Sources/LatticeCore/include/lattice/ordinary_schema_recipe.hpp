#pragma once
#include "schema.hpp"
#include <set>
#include <algorithm>
#include <map>
#include <vector>
#include <sstream>
#include <utility>

namespace lattice { class lattice_db; class swift_lattice; class database; }
namespace lattice::detail {
class ordinary_export_access;
struct ordinary_schema_recipe_test_access;
struct ordinary_receive_recipe_access;

// Private passive recipe producer shared by the actual normal owner callsites
// and the future exact-original target channel. SQL text is not an open/current
// or import grant. No public method, raw handle, execution callback or factory.
class ordinary_schema_recipe final {
    friend class ::lattice::lattice_db;
    friend class ::lattice::database;
    friend class ::lattice::swift_lattice;
    friend class ordinary_export_access;
    friend struct ordinary_schema_recipe_test_access;
    friend struct ordinary_receive_recipe_access;
    static const char* metadata_table() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS _lattice_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        )";}
    static const char* metadata_initial_version() noexcept {return "INSERT OR IGNORE INTO _lattice_meta(key, value) VALUES('schema_version', '1')";}
    static const char* sync_control_table() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS _SyncControl (
                id INTEGER PRIMARY KEY CHECK(id=1),
                disabled INTEGER NOT NULL DEFAULT 0
            )
        )";}
    static const char* sync_control_initial() noexcept {return "INSERT OR IGNORE INTO _SyncControl(id, disabled) VALUES(1, 0)";}
    static const char* audit_table() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS AuditLog(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE COLLATE NOCASE DEFAULT (
                    lower(hex(randomblob(4)))   || '-' ||
                    lower(hex(randomblob(2)))   || '-' ||
                    '4' || substr(lower(hex(randomblob(2))),2) || '-' ||
                    substr('89AB', 1 + (abs(random()) % 4), 1) ||
                      substr(lower(hex(randomblob(2))),2)     || '-' ||
                    lower(hex(randomblob(6)))
                ),
                tableName TEXT,
                operation TEXT,
                rowId INTEGER,
                globalRowId TEXT,
                changedFields TEXT,
                changedFieldsNames TEXT,
                isFromRemote INTEGER DEFAULT 0,
                isSynchronized INTEGER DEFAULT 0,
                timestamp REAL DEFAULT (unixepoch('subsec')),
                synthesized INTEGER DEFAULT 0
            )
        )";}
    static const char* audit_pending_index() noexcept {return R"(
            CREATE INDEX IF NOT EXISTS idx_audit_log_pending_sync
                ON AuditLog(isSynchronized)
                WHERE isSynchronized = 0
        )";}
    static const char* audit_lookup_index() noexcept {return R"(
            CREATE INDEX IF NOT EXISTS idx_audit_log_change_lookup
                ON AuditLog(tableName, rowId, operation)
        )";}
    static void append_swift_physical_property(model_schema& schema,const property_descriptor& desc) {
        if (desc.kind == property_kind::union_type) {
            // Union FK column: TEXT storing the union row's globalId
            auto col_desc = desc;
            col_desc.type = column_type::text;
            col_desc.nullable = true;
            schema.properties.push_back(col_desc);
        } else if (desc.kind == property_kind::primitive ||
                   (desc.kind == property_kind::list && desc.is_geo_bounds)) {
            schema.properties.push_back(desc);
        }
    }
    template<class PropertyMap>
    static model_schema swift_physical_model(const std::string& table,const PropertyMap& properties) {
        model_schema schema;
        schema.table_name=table;
        for(const auto& [name,desc]:properties)append_swift_physical_property(schema,desc);
        return schema; // Preserve the actual source map iteration order.
    }
    static model_schema swift_physical_model(const model_schema& declarations) {
        model_schema schema;
        schema.table_name=declarations.table_name;
        for(const auto& desc:declarations.properties)append_swift_physical_property(schema,desc);
        return schema; // SAME catalog order captured by the actual bridge.
    }
    static std::string model_table(const model_schema& schema,
        std::vector<std::pair<std::string,column_type>>& columns) {
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
        // Columns are retained by the actual caller for the same audit recipe.

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
        return sql.str();
    }
    static std::string column_list(const std::vector<std::string>& resolved_cols) {
        std::ostringstream cols_ss;
        for (size_t j = 0; j < resolved_cols.size(); ++j) {
            if (j > 0) cols_ss << ", ";
            cols_ss << resolved_cols[j];
        }
        return cols_ss.str();
    }
    static std::string core_unique_index(const std::string& table,size_t ordinal,const std::string& column) {
        // Core's original decimal spelling uses to_string, unlike the bridge
        // stream. Preserve both actual normal producers, including locale.
        const std::string idx_name="unique_" + table + "_" + std::to_string(ordinal);
        return "CREATE UNIQUE INDEX IF NOT EXISTS " + idx_name +
            " ON " + table + "(" + column + ")";
    }
    static std::string unique_index(const std::string& table,size_t ordinal,const std::string& col_list) {
        std::ostringstream idx_name;
        idx_name << "unique_" << table << "_" << ordinal;
        std::ostringstream sql;
        sql << "CREATE UNIQUE INDEX IF NOT EXISTS " << idx_name.str()
            << " ON " << table << "(" << col_list << ")";
        return sql.str();
    }
    static std::string nonunique_index(const model_schema& schema,const property_descriptor& prop) {
        std::ostringstream sql;
        sql << "CREATE INDEX IF NOT EXISTS idx_" << schema.table_name << "_" << prop.name
            << " ON " << schema.table_name << "(" << prop.name << ")";
        return sql.str();
    }
    static std::string shadow_column(const std::string& table,const std::string& shadow_col) {
        return "ALTER TABLE " + table + " ADD COLUMN " + shadow_col + " TEXT";
    }
    static std::string shadow_insert(const std::string& table,const std::string& link_table,
        const std::string& shadow_col,const std::string& ins_trig) {
        return "CREATE TRIGGER IF NOT EXISTS " + ins_trig +
            " AFTER INSERT ON " + link_table +
            " BEGIN"
            "   UPDATE " + table +
            "   SET " + shadow_col + " = NEW.rhs"
            "   WHERE globalId = NEW.lhs;"
            " END";
    }
    static std::string shadow_delete(const std::string& table,const std::string& link_table,
        const std::string& shadow_col,const std::string& del_trig) {
        return "CREATE TRIGGER IF NOT EXISTS " + del_trig +
            " AFTER DELETE ON " + link_table +
            " BEGIN"
            "   UPDATE " + table +
            "   SET " + shadow_col + " = NULL"
            "   WHERE globalId = OLD.lhs"
            "     AND " + shadow_col + " = OLD.rhs;"
            " END";
    }
    static std::string shadow_backfill(const std::string& table,const std::string& link_table,
        const std::string& shadow_col) {
        return "UPDATE " + table +
            " SET " + shadow_col + " ="
            " (SELECT rhs FROM " + link_table +
            " WHERE lhs = " + table + ".globalId LIMIT 1)"
            " WHERE " + shadow_col + " IS NULL";
    }

    // Passive exact normal fresh-store/all-kind recipes, no SQL handle or authority.
    static std::string link_table(const std::string& link_table_name) {
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
    static std::string virtual_link_table(const std::string& link_table_name) {
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
    struct union_table_program {std::string sql;std::vector<std::pair<std::string,column_type>> columns;};
    static union_table_program union_table(const std::string& union_table_name,const union_descriptor& desc) {
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
                sql << ", " << col << " " << sql_type_string(c.values[0].type);
                columns.push_back({col, c.values[0].type});
                cols.push_back(col);
            } else {
                for (const auto& val : c.values) {
                    std::string col = c.case_name + "__" + val.param_name;
                    sql << ", " << col << " " << sql_type_string(val.type);
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
    static std::string sql_type_string(column_type type) {
        switch (type) {
            case column_type::integer: return "INTEGER";
            case column_type::real: return "REAL";
            case column_type::text: return "TEXT";
            case column_type::blob: return "BLOB";
        }
        return "TEXT";
    }

    static constexpr const char* receive_store() noexcept {return "CREATE TABLE _lattice_receive_guard_store (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,legacy_origin INTEGER NOT NULL,last_incarnation INTEGER NOT NULL,channels INTEGER NOT NULL,channel_bytes INTEGER NOT NULL,capacity_refused INTEGER NOT NULL) WITHOUT ROWID";}
    static constexpr const char* receive_channel() noexcept {return "CREATE TABLE _lattice_receive_guard (channel BLOB PRIMARY KEY NOT NULL CHECK(typeof(channel)='blob' AND length(channel)>0),incarnation INTEGER UNIQUE NOT NULL,generation INTEGER NOT NULL,state INTEGER NOT NULL,reason INTEGER NOT NULL,checkpoint BLOB) WITHOUT ROWID";}
    static constexpr const char* receive_index() noexcept {return "CREATE INDEX _lattice_receive_guard_state ON _lattice_receive_guard(state)";}

    static std::string union_cascade(const std::string& parent_table,const std::string& field_name,const std::string& union_table_name) {
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
    static std::string rtree_table(const std::string& rtree_table) {
            std::ostringstream sql;
            sql << "CREATE VIRTUAL TABLE " << rtree_table << " USING rtree("
                << "id, "           // Matches main table id
                << "minLat, maxLat, "
                << "minLon, maxLon"
                << ")";
        return sql.str();
    }
    static std::string rtree_insert(const std::string& model_table,const std::string& column_name) {
        std::string rtree_table="_"+model_table+"_"+column_name+"_rtree";
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                       << "AFTER INSERT ON main." << model_table << " "
                       << "WHEN NEW." << minLat << " IS NOT NULL "
                       << "BEGIN "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW." << minLat << ", NEW." << maxLat << ", "
                       << "NEW." << minLon << ", NEW." << maxLon << "); "
                       << "END";
        return insert_trigger.str();
    }
    static std::string rtree_update(const std::string& model_table,const std::string& column_name) {
        std::string rtree_table="_"+model_table+"_"+column_name+"_rtree";
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";
        std::ostringstream update_trigger;
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
        return update_trigger.str();
    }
    static std::string rtree_delete(const std::string& model_table,const std::string& column_name) {
        std::string rtree_table="_"+model_table+"_"+column_name+"_rtree";
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                       << "AFTER DELETE ON main." << model_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "END";
        return delete_trigger.str();
    }
    static std::string rtree_population(const std::string& model_table,const std::string& column_name) {
        std::string rtree_table="_"+model_table+"_"+column_name+"_rtree";
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";
        std::ostringstream populate_sql;
        populate_sql << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                     << "SELECT id, " << minLat << ", " << maxLat << ", " << minLon << ", " << maxLon << " "
                     << "FROM " << model_table << " "
                     << "WHERE " << minLat << " IS NOT NULL";
        return populate_sql.str();
    }
    static std::string geo_list_table(const std::string& list_table) {
        std::ostringstream sql;
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
    static std::string geo_list_parent_index(const std::string& list_table) {
        std::string idx_sql = "CREATE INDEX IF NOT EXISTS idx_" +
            list_table + "_parent ON " + list_table + "(parent_id)";
        return idx_sql;
    }
    static std::string geo_list_rtree_table(const std::string& rtree_table) {
            std::ostringstream rtree_sql;
            rtree_sql << "CREATE VIRTUAL TABLE " << rtree_table << " USING rtree("
                      << "id, "  // Matches list table id
                      << "minLat, maxLat, "
                      << "minLon, maxLon"
                      << ")";
        return rtree_sql.str();
    }
    static std::string geo_list_rtree_insert(const std::string& list_table) {
        std::string rtree_table=list_table+"_rtree";
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                       << "AFTER INSERT ON " << list_table << " "
                       << "BEGIN "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                       << "END";
        return insert_trigger.str();
    }
    static std::string geo_list_rtree_update(const std::string& list_table) {
        std::string rtree_table=list_table+"_rtree";
        std::ostringstream update_trigger;
        update_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_update "
                       << "AFTER UPDATE OF minLat, maxLat, minLon, maxLon ON " << list_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                       << "END";
        return update_trigger.str();
    }
    static std::string geo_list_rtree_delete(const std::string& list_table) {
        std::string rtree_table=list_table+"_rtree";
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                       << "AFTER DELETE ON " << list_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "END";
        return delete_trigger.str();
    }
    static std::vector<std::string> geo_list_audit(const std::string& list_table) {
        std::vector<std::string> emitted;
        // INSERT trigger for AuditLog
        std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Insert"
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
        emitted.push_back(insert_trigger);

        // DELETE trigger for AuditLog
        std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Delete"
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
        emitted.push_back(delete_trigger);
        return emitted;
    }
    static std::vector<std::string> virtual_link_audit(const std::string& link_table_name) {
        std::vector<std::string> emitted;
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
        emitted.push_back(insert_trigger);

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
        emitted.push_back(delete_trigger);
        return emitted;
    }
    static const char* sync_set() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS _lattice_sync_set (
                sync_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                global_row_id TEXT NOT NULL,
                PRIMARY KEY (sync_id, table_name, global_row_id)
            )
        )";}
    static const char* sync_state() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS _lattice_sync_state (
                audit_entry_id INTEGER NOT NULL,
                sync_id TEXT NOT NULL,
                is_synchronized INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (audit_entry_id, sync_id)
            )
        )";}
    static const char* sync_state_pending() noexcept {return R"(
            CREATE INDEX IF NOT EXISTS idx_sync_state_pending
                ON _lattice_sync_state(sync_id, is_synchronized)
                WHERE is_synchronized = 0
        )";}
    static const char* replication_slots() noexcept {return R"(
            CREATE TABLE IF NOT EXISTS _lattice_replication_slots (
                sync_id TEXT PRIMARY KEY,
                confirmed_audit_id INTEGER NOT NULL DEFAULT 0,
                last_received_event_id TEXT,
                last_active_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        )";}
    static const char* replication_upload_floor() noexcept {return "ALTER TABLE _lattice_replication_slots ADD COLUMN upload_floor INTEGER NOT NULL DEFAULT 0";}
};
} // namespace lattice::detail
