#pragma once
#include <array>
#include <string>

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
} // namespace lattice::detail
