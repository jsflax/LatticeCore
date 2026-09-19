#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <memory>
#include <string>
#include <vector>

using namespace lattice;

TEST(DatabaseText, BindingPreservesBytesAgainstRawSQLiteOracle) {
    database db(":memory:");
    db.execute("CREATE TABLE TextBytes(id INTEGER PRIMARY KEY,value TEXT)");
    struct example { std::string text; const char* hex; };
    const std::vector<example> examples{
        {std::string("right\0attached", 14), "7269676874006174746163686564"},
        {std::string("\0leading", 8), "006C656164696E67"},
        {std::string("trailing\0", 9), "747261696C696E6700"},
        {std::string("\0", 1), "00"},
        {std::string("caf\xC3\xA9\0z", 7), "636166C3A9007A"},
        {std::string{}, ""},
    };
    const auto expect_bytes = [&](int64_t id, const example& expected) {
        // No Lattice extraction: two truncation bugs cannot cancel out.
        // length(TEXT) stops at NUL, so explicitly measure the BLOB bytes.
        sqlite3_stmt* raw = nullptr;
        const int rc = sqlite3_prepare_v2(db.handle(),
            "SELECT value,length(CAST(value AS BLOB)),hex(CAST(value AS BLOB)) "
            "FROM TextBytes WHERE id=?", -1, &raw, nullptr);
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, &sqlite3_finalize);
        ASSERT_EQ(rc, SQLITE_OK);
        ASSERT_EQ(sqlite3_bind_int64(raw, 1, id), SQLITE_OK);
        ASSERT_EQ(sqlite3_step(raw), SQLITE_ROW);
        ASSERT_EQ(sqlite3_column_type(raw, 0), SQLITE_TEXT);
        EXPECT_EQ(sqlite3_column_int64(raw, 1), static_cast<int64_t>(expected.text.size()));
        const auto* hex = sqlite3_column_text(raw, 2);
        ASSERT_NE(hex, nullptr);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(hex)), expected.hex);
        ASSERT_EQ(sqlite3_step(raw), SQLITE_DONE);
    };
    int64_t id = 1;
    for (const auto& example : examples) {
        SCOPED_TRACE(id);
        db.execute("INSERT INTO TextBytes(id,value) VALUES(?,?)", {id, example.text});
        ASSERT_NO_FATAL_FAILURE(expect_bytes(id, example));
        ++id;
    }
    // The same central binder serves the other ordinary statement funnels.
    EXPECT_EQ(db.insert("TextBytes", {{"id", id}, {"value", examples[0].text}}), id);
    ASSERT_NO_FATAL_FAILURE(expect_bytes(id, examples[0]));
    db.update("TextBytes", id, {{"value", examples[1].text}});
    ASSERT_NO_FATAL_FAILURE(expect_bytes(id, examples[1]));
    const auto bound = db.query("SELECT hex(CAST(? AS BLOB)) AS bytes", {examples[0].text});
    ASSERT_EQ(bound.size(), 1u);
    EXPECT_EQ(std::get<std::string>(bound[0].at("bytes")), examples[0].hex);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
}

TEST(DatabaseText, ExtractionPreservesRawInsertedBytesInRowsAndLiveFields) {
    database db(":memory:");
    // Direct SQL constants bypass every Lattice binding path. Therefore this
    // test still exposes extraction truncation if binding alone is repaired.
    ASSERT_EQ(sqlite3_exec(db.handle(),
        "CREATE TABLE TextBytes(id INTEGER PRIMARY KEY,value TEXT);"
        "INSERT INTO TextBytes VALUES"
        "(1,CAST(X'7269676874006174746163686564' AS TEXT)),"
        "(2,CAST(X'006C656164696E67' AS TEXT)),"
        "(3,CAST(X'747261696C696E6700' AS TEXT)),"
        "(4,CAST(X'00' AS TEXT)),"
        "(5,CAST(X'636166C3A9007A' AS TEXT)),(6,''),(7,NULL);",
        nullptr, nullptr, nullptr), SQLITE_OK);
    const std::vector<std::string> expected{
        std::string("right\0attached", 14), std::string("\0leading", 8),
        std::string("trailing\0", 9), std::string("\0", 1),
        std::string("caf\xC3\xA9\0z", 7), std::string{},
    };
    const auto rows = db.query("SELECT id,value FROM TextBytes ORDER BY id");
    ASSERT_EQ(rows.size(), expected.size() + 1);
    for (size_t index = 0; index < expected.size(); ++index) {
        SCOPED_TRACE(index);
        ASSERT_TRUE(std::holds_alternative<std::string>(rows[index].at("value")));
        EXPECT_EQ(std::get<std::string>(rows[index].at("value")), expected[index]);
        managed<std::string> live;
        live.assign(&db, nullptr, "TextBytes", "value", static_cast<int64_t>(index + 1));
        EXPECT_EQ(live.detach(), expected[index]);
    }
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows.back().at("value")));
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
}

