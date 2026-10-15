#include "TestHelpers.hpp"
#include <lattice/derived_program.hpp>

namespace {
using namespace lattice;
constexpr const char* model = "DerivedFtsProgramRow";
constexpr const char* column = "content";

configuration test_config(const std::string& path) {
    configuration config(path);
    config.audit_retention_seconds = 0;
    return config;
}

void create_model(lattice_db& owner) {
    owner.db().execute("CREATE TABLE DerivedFtsProgramRow("
                       "id INTEGER PRIMARY KEY, globalId TEXT UNIQUE COLLATE NOCASE, content TEXT)");
}

void insert(lattice_db& owner, int64_t id, const column_value_t& value) {
    owner.db().execute("INSERT INTO DerivedFtsProgramRow(id,globalId,content) VALUES(?,?,?)",
                       {id, lattice::uuid_t::generate().to_string(), value});
}

std::vector<int64_t> hits(lattice_db& owner, const std::string& term) {
    std::vector<int64_t> result;
    for (const auto& row : owner.db().query(
             "SELECT rowid AS id FROM _DerivedFtsProgramRow_content_fts "
             "WHERE _DerivedFtsProgramRow_content_fts MATCH ? ORDER BY rowid", {term})) {
        result.push_back(std::get<int64_t>(row.at("id")));
    }
    return result;
}

// Read actual index shadow data before MATCH; external-content enumeration
// would read the model and could not prove that its index rolled back.
std::vector<std::vector<database::row_t>> shadow(lattice_db& owner) {
    std::vector<std::vector<database::row_t>> result;
    for (const auto* suffix : {"data", "idx", "docsize", "config"}) {
        result.push_back(owner.db().query(
            "SELECT * FROM _DerivedFtsProgramRow_content_fts_" + std::string(suffix) +
            " ORDER BY 1"));
    }
    return result;
}

void definitions_match(lattice_db& owner) {
    const auto descriptor = detail::fts5_porter_program(model, column);
    const auto table = owner.db().query(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", {descriptor.table});
    ASSERT_EQ(table.size(), 1u);
    EXPECT_EQ(std::get<std::string>(table[0].at("sql")), descriptor.create_table);
    const auto rows = owner.db().query(
        "SELECT name,tbl_name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=? ORDER BY name",
        {std::string(model)});
    ASSERT_EQ(rows.size(), descriptor.triggers.size());
    for (const auto& expected : descriptor.triggers) {
        const auto found = std::find_if(rows.begin(), rows.end(), [&](const auto& row) {
            return std::get<std::string>(row.at("name")) == expected.name;
        });
        ASSERT_NE(found, rows.end());
        EXPECT_EQ(std::get<std::string>(found->at("tbl_name")), expected.owner_table);
        auto sql = expected.sql;
        const std::string prefix = "CREATE TRIGGER IF NOT EXISTS ";
        ASSERT_EQ(sql.find(prefix), 0u);
        sql.replace(0, prefix.size(), "CREATE TRIGGER ");
        EXPECT_EQ(std::get<std::string>(found->at("sql")), sql);
    }
}

void rollback_matrix(bool file) {
    TempDB disk{"derived_fts_program"};
    lattice_db owner(test_config(file ? disk.str() : std::string(":memory:")));
    create_model(owner);
    insert(owner, 1, std::string("running wolves"));
    insert(owner, 2, std::string("violet"));
    insert(owner, 3, nullptr);
    owner.ensure_fts5_table(model, column);
    definitions_match(owner);
    EXPECT_EQ(hits(owner, "run"), (std::vector<int64_t>{1}));
    EXPECT_EQ(hits(owner, "violet"), (std::vector<int64_t>{2}));
    const auto before = shadow(owner);
    const auto model_before = owner.db().query("SELECT * FROM DerivedFtsProgramRow ORDER BY id");

    owner.begin_transaction();
    owner.db().execute("UPDATE DerivedFtsProgramRow SET content='emerald' WHERE id=1");
    owner.db().execute("DELETE FROM DerivedFtsProgramRow WHERE id=2");
    insert(owner, 4, std::string("saffron"));
    EXPECT_EQ(hits(owner, "emerald"), (std::vector<int64_t>{1}));
    EXPECT_TRUE(hits(owner, "run").empty());
    owner.rollback();

    EXPECT_EQ(shadow(owner), before);
    EXPECT_EQ(owner.db().query("SELECT * FROM DerivedFtsProgramRow ORDER BY id"), model_before);
    EXPECT_EQ(hits(owner, "run"), (std::vector<int64_t>{1}));
    EXPECT_EQ(hits(owner, "violet"), (std::vector<int64_t>{2}));
    EXPECT_TRUE(hits(owner, "emerald").empty());
    EXPECT_TRUE(hits(owner, "saffron").empty());

    owner.begin_transaction();
    owner.db().execute("UPDATE DerivedFtsProgramRow SET content='' WHERE id=1");
    owner.db().execute("DELETE FROM DerivedFtsProgramRow WHERE id=2");
    owner.db().execute("UPDATE DerivedFtsProgramRow SET content='turquoise' WHERE id=3");
    owner.commit();
    EXPECT_TRUE(hits(owner, "run").empty());
    EXPECT_TRUE(hits(owner, "violet").empty());
    EXPECT_EQ(hits(owner, "turquoise"), (std::vector<int64_t>{3}));
    owner.db().execute("UPDATE DerivedFtsProgramRow SET content=NULL WHERE id=3");
    EXPECT_TRUE(hits(owner, "turquoise").empty());
}
} // namespace

TEST(DerivedFtsProgram, MemoryActualProgramsPreserveIndexRollbackAndClear) {
    rollback_matrix(false);
}

TEST(DerivedFtsProgram, FileActualProgramsPreserveIndexRollbackAndClear) {
    rollback_matrix(true);
}

TEST(DerivedFtsProgram, ReopenAndTriggerRecreationKeepExistingIndexSearchable) {
    TempDB disk{"derived_fts_reopen"};
    {
        lattice_db owner(test_config(disk.str()));
        create_model(owner);
        insert(owner, 7, std::string("running violet"));
        owner.ensure_fts5_table(model, column);
        const auto before = shadow(owner);
        const auto descriptor = detail::fts5_porter_program(model, column);
        for (const auto& trigger : descriptor.triggers) owner.db().execute("DROP TRIGGER " + trigger.name);
        owner.ensure_fts5_table(model, column);
        definitions_match(owner);
        EXPECT_EQ(shadow(owner), before);
        EXPECT_EQ(hits(owner, "run"), (std::vector<int64_t>{7}));
        owner.db().execute("UPDATE DerivedFtsProgramRow SET content='emerald' WHERE id=7");
        EXPECT_TRUE(hits(owner, "run").empty());
        EXPECT_EQ(hits(owner, "emerald"), (std::vector<int64_t>{7}));
    }
    {
        lattice_db reopened(test_config(disk.str()));
        reopened.ensure_fts5_table(model, column);
        definitions_match(reopened);
        EXPECT_EQ(hits(reopened, "emerald"), (std::vector<int64_t>{7}));
        EXPECT_TRUE(hits(reopened, "violet").empty());
        reopened.db().execute("DELETE FROM DerivedFtsProgramRow WHERE id=7");
        EXPECT_TRUE(hits(reopened, "emerald").empty());
    }
}
