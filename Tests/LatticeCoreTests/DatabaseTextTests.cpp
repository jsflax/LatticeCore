#include <gtest/gtest.h>
#include <algorithm>
#include <LatticeCore.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace lattice {
struct database_query_test_access {
    static void set(std::function<void(sqlite3_stmt*)> hook) {
        database::test_hook_query_row_ = std::move(hook);
    }
};
} // namespace lattice

using namespace lattice;

TEST(GroupedCounts, ScalarProjectionDoesNotReadPayloadAndUsesOneStatement) {
    database db(":memory:");
    db.execute("CREATE TABLE Item(id INTEGER PRIMARY KEY,globalId TEXT,topic TEXT,project TEXT,content TEXT,embedding BLOB)");
    const std::string project = "O'Brien ? --";
    for (int64_t i = 0; i < 180; ++i) {
        db.execute("INSERT INTO Item VALUES(?,?,?,?,?,zeroblob(4096))",
                   {i + 1, std::to_string(i), "topic-" + std::to_string(i), project, std::string(4096, 'x')});
    }
    // Refuse even preparing a read of a body/vector column. The real SQL
    // builder must remain a projection, independent of payload size.
    ASSERT_EQ(sqlite3_set_authorizer(db.handle(), [](void*, int action, const char*,
                                                   const char* column, const char*, const char*) {
        if (action == SQLITE_READ && column
            && (std::string(column) == "content" || std::string(column) == "embedding")) return SQLITE_DENY;
        return SQLITE_OK;
    }, nullptr), SQLITE_OK);
    int statements = 0;
    ASSERT_EQ(sqlite3_trace_v2(db.handle(), SQLITE_TRACE_STMT,
        [](unsigned, void* context, void*, void*) { ++*static_cast<int*>(context); return 0; },
        &statements), SQLITE_OK);
    const auto sql = lattice_db::build_grouped_counts_sql("Item", "topic", "project = ?", "globalId");
    const auto rows = db.query(sql, {project});
    EXPECT_EQ(statements, 1);
    ASSERT_EQ(rows.size(), 180u);
    for (const auto& row : rows) {
        EXPECT_EQ(row.size(), 2u);
        EXPECT_EQ(std::get<int64_t>(row.at("cnt")), 1);
    }
    EXPECT_TRUE(db.query(sql, {std::string("not the project")}).empty());
    sqlite3_trace_v2(db.handle(), 0, nullptr, nullptr);
    sqlite3_set_authorizer(db.handle(), nullptr, nullptr);
}

TEST(GroupedCounts, ConflictingCopiesRetainIndependentPerGroupDistinctCounts) {
    database db(":memory:");
    db.execute("CREATE TABLE Item(id INTEGER PRIMARY KEY,globalId TEXT,topic TEXT,deletedAt REAL)");
    // Both keys have an unambiguous representative. Shared's two copies
    // still count in BOTH emitted groups; sum(counts) is deliberately > total.
    db.execute("INSERT INTO Item VALUES(1,'shared','a',NULL),(2,'shared','b',NULL),"
               "(3,'only-a','a',NULL),(4,'only-b','b',NULL),(5,'deleted','x',1)");
    const auto sql = lattice_db::build_grouped_counts_sql("Item", "topic", "deletedAt IS NULL", "globalId");
    const auto rows = db.query(sql);
    ASSERT_EQ(rows.size(), 2u);
    for (const auto& row : rows) EXPECT_EQ(std::get<int64_t>(row.at("cnt")), 2);
    // Compare to the existing two-stage contract, not a partition of one
    // arbitrarily chosen physical copy per globalId.
    const auto old_groups = db.query(lattice_db::build_query_rows_sql(
        "Item", "deletedAt IS NULL", std::nullopt, std::nullopt, std::nullopt, "topic", "globalId"));
    ASSERT_EQ(old_groups.size(), rows.size());
    for (const auto& group : old_groups) {
        const auto key = std::get<std::string>(group.at("topic"));
        const auto old_count = db.query(lattice_db::build_count_sql(
            "Item", "deletedAt IS NULL AND topic = ?", std::nullopt, "globalId"), {key});
        auto found = std::find_if(rows.begin(), rows.end(), [&](const auto& row) {
            return std::get<std::string>(row.at("key")) == key;
        });
        ASSERT_NE(found, rows.end());
        EXPECT_EQ(std::get<int64_t>(found->at("cnt")), std::get<int64_t>(old_count[0].at("cnt")));
    }
}

TEST(GroupedCounts, ConflictOnlyRepresentativeIsUnspecifiedWithOrWithoutCoveringIndex) {
    database db(":memory:");
    db.execute("CREATE TABLE Item(id INTEGER PRIMARY KEY,globalId TEXT,topic TEXT)");
    db.execute("INSERT INTO Item VALUES(1,'shared','a'),(2,'shared','b')");
    for (int variant = 0; variant < 2; ++variant) {
        if (variant == 1) db.execute("CREATE INDEX covering ON Item(globalId,topic DESC)");
        const auto rows = db.query(lattice_db::build_grouped_counts_sql(
            "Item", "topic", std::nullopt, "globalId"));
        ASSERT_EQ(rows.size(), 1u);
        const auto key = std::get<std::string>(rows[0].at("key"));
        EXPECT_TRUE(key == "a" || key == "b");
        EXPECT_EQ(std::get<int64_t>(rows[0].at("cnt")), 1);
        const auto old_groups = db.query(lattice_db::build_query_rows_sql(
            "Item", std::nullopt, std::nullopt, std::nullopt, std::nullopt, "topic", "globalId"));
        EXPECT_EQ(old_groups.size(), 1u);
        // Both APIs promise one representative, not which physical copy a
        // bare-column GROUP BY picks under a different query plan.
    }
}

TEST(GroupedCounts, ClosedOwnerOrBorrowRefusesEmptySuccess) {
    for (bool close_owner : {false, true}) {
        SCOPED_TRACE(close_owner);
        lattice_db owner;
        owner.db().execute("CREATE TABLE GroupedCloseProbe(topic TEXT)");
        EXPECT_TRUE(owner.grouped_counts("GroupedCloseProbe", "topic").empty());
        owner.db().execute("INSERT INTO GroupedCloseProbe VALUES('one')");
        const auto rows = owner.grouped_counts("GroupedCloseProbe", "topic");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("cnt")), 1);
        const auto retained = owner.borrow_read_connection();
        if (close_owner) owner.close();
        else retained->close();
        EXPECT_EQ(owner.is_closed(), close_owner);
        EXPECT_TRUE(retained->is_closed());
        EXPECT_THROW(owner.grouped_counts("GroupedCloseProbe", "topic"), db_error);
    }
}

TEST(GroupedCounts, ConnectionClosedDuringQueryRefusesCompletedRows) {
    lattice_db owner;
    owner.db().execute("CREATE TABLE GroupedCloseProbe(topic TEXT)");
    owner.db().execute("INSERT INTO GroupedCloseProbe VALUES('one')");
    const auto retained = owner.borrow_read_connection();
    int rows_seen = 0;
    struct ClearRowHook {
        ~ClearRowHook() { database_query_test_access::set(nullptr); }
    } clear_hook;
    database_query_test_access::set([&](sqlite3_stmt*) {
        ++rows_seen;
        // Logical close only: the retained borrow keeps the physical handle
        // alive while the query finishes extracting this row.
        retained->close();
    });
    EXPECT_THROW(owner.grouped_counts("GroupedCloseProbe", "topic"), db_error);
    EXPECT_EQ(rows_seen, 1);
    EXPECT_FALSE(owner.is_closed());
    EXPECT_TRUE(retained->is_closed());
}

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

