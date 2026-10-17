#pragma once
#include <array>
#include <string>
#include <sstream>

namespace lattice::detail {
// Describes generated SQL only. It confers no schema/source/route authority,
// performs no SQL and does not validate or repair an existing index. Ordinary
// setup and future bounded recovery admission share these exact definitions.
struct derived_trigger_program {
    std::string name;
    std::string owner_table;
    std::string sql;
};
struct fts5_porter_definition {
    std::string table;
    std::string create_table;
    std::array<derived_trigger_program, 3> triggers;
    std::string populate;
};

inline std::string fts5_porter_table_name(const std::string& model, const std::string& column) {
    return "_" + model + "_" + column + "_fts";
}

// Identifier handling intentionally matches existing ordinary setup. A strict
// recovery descriptor must validate bounded identifiers before calling this
// generator, then validate the actual complete schema/program/shadow layout.
inline fts5_porter_definition fts5_porter_program(const std::string& model,
                                                const std::string& column) {
    fts5_porter_definition p;
    p.table = fts5_porter_table_name(model, column);
    const auto& f = p.table;
    p.create_table = "CREATE VIRTUAL TABLE " + f + " USING fts5(" + column +
        ", content='" + model + "', content_rowid='id', tokenize='porter')";
    p.triggers = {{
        {f + "_insert", model,
         "CREATE TRIGGER IF NOT EXISTS " + f + "_insert AFTER INSERT ON main." + model +
         " BEGIN INSERT INTO " + f + "(rowid, " + column + ") VALUES (NEW.id, NEW." + column + "); END"},
        {f + "_update", model,
         "CREATE TRIGGER IF NOT EXISTS " + f + "_update AFTER UPDATE OF " + column + " ON main." + model +
         " BEGIN INSERT INTO " + f + "(" + f + ", rowid, " + column + ") VALUES ('delete', OLD.id, OLD." + column +
         "); INSERT INTO " + f + "(rowid, " + column + ") VALUES (NEW.id, NEW." + column + "); END"},
        {f + "_delete", model,
         "CREATE TRIGGER IF NOT EXISTS " + f + "_delete BEFORE DELETE ON main." + model +
         " BEGIN INSERT INTO " + f + "(" + f + ", rowid, " + column + ") VALUES ('delete', OLD.id, OLD." + column + "); END"}
    }};
    p.populate = "INSERT INTO " + f + "(rowid, " + column + ") SELECT id, " + column + " FROM main." + model +
        " WHERE " + column + " IS NOT NULL";
    return p;
}

// The flat and IVF modes use the same existing four mutation programs. Vector
// dimensions and module options are encoded only by the separate CREATE helper
// at actual table creation; a zero dimension never invents an empty sidecar.
struct vec0_trigger_definition {
    std::string table;
    std::string owner_table;
    std::array<std::string, 4> sql;
    std::array<std::string, 4> names;
};
inline vec0_trigger_definition vec0_program(const std::string& bare_table,
                                           const std::string& column_name) {
    const std::string vec_table = "_" + bare_table + "_" + column_name + "_vec";
    // Keep the established nonempty UPDATE + conditional INSERT
    // programs intact. Empty/NULL updates have their own clear program.
    // INSERT trigger
    std::ostringstream insert_trigger;
    insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_insert "
                   << "AFTER INSERT ON main." << bare_table << " "
                   << "WHEN NEW." << column_name << " IS NOT NULL "
                   << "AND length(NEW." << column_name << ") > 0 "
                   << "BEGIN "
                   << "UPDATE " << vec_table << " SET embedding = NEW." << column_name
                   << " WHERE global_id = NEW.globalId; "
                   << "INSERT INTO " << vec_table << "(global_id, embedding) "
                   << "SELECT NEW.globalId, NEW." << column_name << " "
                   << "WHERE NOT EXISTS (SELECT 1 FROM " << vec_table
                   << " WHERE global_id = NEW.globalId); "
                   << "END";

    // UPDATE trigger
    std::ostringstream update_trigger;
    update_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_update "
                   << "AFTER UPDATE OF " << column_name << " ON main." << bare_table << " "
                   << "WHEN NEW." << column_name << " IS NOT NULL "
                   << "AND length(NEW." << column_name << ") > 0 "
                   << "BEGIN "
                   << "UPDATE " << vec_table << " SET embedding = NEW." << column_name
                   << " WHERE global_id = NEW.globalId; "
                   << "INSERT INTO " << vec_table << "(global_id, embedding) "
                   << "SELECT NEW.globalId, NEW." << column_name << " "
                   << "WHERE NOT EXISTS (SELECT 1 FROM " << vec_table
                   << " WHERE global_id = NEW.globalId); "
                   << "END";

    // DELETE trigger
    std::ostringstream delete_trigger;
    delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_delete "
                   << "AFTER DELETE ON main." << bare_table << " "
                   << "BEGIN "
                   << "DELETE FROM " << vec_table << " WHERE global_id = OLD.globalId; "
                   << "END";

    std::ostringstream clear_trigger;
    clear_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_clear "
                  << "AFTER UPDATE OF " << column_name << " ON main." << bare_table << " "
                  << "WHEN NEW." << column_name << " IS NULL "
                  << "OR length(NEW." << column_name << ") = 0 "
                  << "BEGIN DELETE FROM " << vec_table
                  << " WHERE global_id = OLD.globalId; END";
    return {vec_table, bare_table,
            {insert_trigger.str(), update_trigger.str(), delete_trigger.str(), clear_trigger.str()},
            {vec_table + "_insert", vec_table + "_update", vec_table + "_delete", vec_table + "_clear"}};
}

inline std::string vec0_create_table_program(const std::string& vec_table,
                                            int dimensions, int ivf_nlist, int ivf_nprobe) {
    std::ostringstream sql;
    sql << "CREATE VIRTUAL TABLE " << vec_table << " USING vec0("
        << "global_id TEXT PRIMARY KEY, embedding float[" << dimensions << "]"
        << (ivf_nlist > 0
            ? " indexed by ivf(nlist=" + std::to_string(ivf_nlist)
              + (ivf_nprobe > 0 ? ", nprobe=" + std::to_string(ivf_nprobe) : "") + ")"
            : "") << ")";
    return sql.str();
}
} // namespace lattice::detail
