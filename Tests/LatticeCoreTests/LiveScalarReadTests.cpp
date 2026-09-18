#include "TestHelpers.hpp"
#include <lattice.hpp>

namespace {
using namespace lattice;
void make_scalar_table(database& db) {
    db.execute("CREATE TABLE ScalarRead(id INTEGER PRIMARY KEY,i,r,t)");
    db.execute("INSERT INTO ScalarRead VALUES(1,7,2.5,'first')");
}
struct fields {
    managed<int64_t> integer{int64_t(-7)};
    managed<double> real{-2.5};
    managed<std::string> text;
    explicit fields(database& db, const std::string& table = "ScalarRead") {
        text.unmanaged_value = "fallback";
        integer.assign(&db, nullptr, table, "i", 1);
        real.assign(&db, nullptr, table, "r", 1);
        text.assign(&db, nullptr, table, "t", 1);
    }
};
SchemaVector scalar_schemas() {
    swift_schema_entry entry;
    entry.table_name = "ScalarModel";
    for (const auto& spec : std::vector<std::pair<std::string,column_type>>{
             {"i",column_type::integer},{"r",column_type::real},{"t",column_type::text}}) {
        property_descriptor p; p.name = spec.first; p.type = spec.second;
        entry.properties[p.name] = p;
    }
    return {entry};
}
}

TEST(LiveScalarRead, EveryGetterUsesOneFreshStatementAndSeesSameHandleUpdates) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    const auto before = database::thread_statement_count();
    EXPECT_EQ(value.integer.detach(), 7);
    EXPECT_DOUBLE_EQ(value.real.detach(), 2.5);
    EXPECT_EQ(value.text.detach(), "first");
    EXPECT_EQ(database::thread_statement_count() - before, 3u);
    db.execute("UPDATE ScalarRead SET i=9,r=4.5,t='second'");
    EXPECT_EQ(value.integer.detach(), 9);
    EXPECT_DOUBLE_EQ(value.real.detach(), 4.5);
    EXPECT_EQ(value.text.detach(), "second");
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
}

TEST(LiveScalarRead, OtherConnectionWritesAreVisibleBetweenGetters) {
    TempDB path("live_scalar_other_writer");
    database reader(path.str()); make_scalar_table(reader); fields value(reader);
    database writer(path.str());
    EXPECT_EQ(value.text.detach(), "first");
    writer.execute("UPDATE ScalarRead SET i=11,r=6.5,t='external'");
    EXPECT_EQ(value.integer.detach(), 11);
    EXPECT_DOUBLE_EQ(value.real.detach(), 6.5);
    EXPECT_EQ(value.text.detach(), "external");
}

TEST(LiveScalarRead, OwningTransactionReadYourWritesAndRollbackArePreserved) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    db.begin_transaction();
    db.execute("UPDATE ScalarRead SET i=11,r=6.5,t='uncommitted'");
    EXPECT_EQ(value.integer.detach(), 11);
    EXPECT_DOUBLE_EQ(value.real.detach(), 6.5);
    EXPECT_EQ(value.text.detach(), "uncommitted");
    EXPECT_TRUE(db.is_in_transaction());
    db.rollback();
    EXPECT_EQ(value.integer.detach(), 7);
    EXPECT_DOUBLE_EQ(value.real.detach(), 2.5);
    EXPECT_EQ(value.text.detach(), "first");
}

TEST(LiveScalarRead, SameIDsStayOnPhysicalRoutesAfterAttachingAndPersistWrites) {
    TempDB main_path("live_scalar_main"), arm_path("live_scalar_arm");
    database main(main_path.str()), arm(arm_path.str());
    make_scalar_table(main); make_scalar_table(arm);
    fields local(main); // Bound before the same-named TEMP union exists.
    arm.execute("UPDATE ScalarRead SET i=70,r=25.5,t='attached'");
    const auto alias = managed_quote_identifier("a.\"quoted");
    main.execute("ATTACH DATABASE ? AS " + alias, {arm_path.str()});
    main.execute("CREATE TEMP VIEW ScalarRead AS SELECT * FROM main.ScalarRead UNION ALL SELECT * FROM " + alias + ".ScalarRead");
    fields attached(main, alias + ".ScalarRead");
    EXPECT_EQ(local.integer.detach(), 7); EXPECT_EQ(attached.integer.detach(), 70);
    EXPECT_DOUBLE_EQ(local.real.detach(), 2.5); EXPECT_DOUBLE_EQ(attached.real.detach(), 25.5);
    EXPECT_EQ(local.text.detach(), "first"); EXPECT_EQ(attached.text.detach(), "attached");
    attached.integer = 90; attached.real = 29.5; attached.text = "arm update";
    fields physical_arm(arm);
    EXPECT_EQ(physical_arm.integer.detach(), 90);
    EXPECT_DOUBLE_EQ(physical_arm.real.detach(), 29.5);
    EXPECT_EQ(physical_arm.text.detach(), "arm update");
    EXPECT_EQ(local.integer.detach(), 7); EXPECT_EQ(local.text.detach(), "first");
}

TEST(LiveScalarRead, NullWrongTypeMissingAndClosedKeepExplicitFallbacks) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    db.execute("UPDATE ScalarRead SET i=NULL,r=NULL,t=NULL");
    EXPECT_EQ(value.integer.detach(), -7); EXPECT_DOUBLE_EQ(value.real.detach(), -2.5); EXPECT_EQ(value.text.detach(), "fallback");
    db.execute("UPDATE ScalarRead SET i=1.5,r=7,t=5");
    EXPECT_EQ(value.integer.detach(), -7); EXPECT_DOUBLE_EQ(value.real.detach(), -2.5); EXPECT_EQ(value.text.detach(), "fallback");
    db.execute("DELETE FROM ScalarRead");
    EXPECT_EQ(value.integer.detach(), -7); EXPECT_DOUBLE_EQ(value.real.detach(), -2.5); EXPECT_EQ(value.text.detach(), "fallback");
    db.close(); const auto before = database::thread_statement_count();
    EXPECT_EQ(value.integer.detach(), -7); EXPECT_DOUBLE_EQ(value.real.detach(), -2.5); EXPECT_EQ(value.text.detach(), "fallback");
    EXPECT_EQ(database::thread_statement_count() - before, 3u);
}

TEST(LiveScalarRead, ExistingTextAndColumnNameConventionsAreUnchanged) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    // Existing extract_column truncates at embedded NUL. This optimization
    // deliberately does not change that independent conversion convention.
    db.execute("UPDATE ScalarRead SET t=CAST(X'610062' AS TEXT)");
    EXPECT_EQ(value.text.detach(), "a");
    db.execute("UPDATE ScalarRead SET t=''"); EXPECT_EQ(value.text.detach(), "");
    // Existing map lookup uses the original assigned name, not the SQL alias.
    value.integer.column_name = "i AS other";
    EXPECT_EQ(value.integer.detach(), -7);
}

TEST(LiveScalarRead, PrepareAndStepFailuresFinalizeAndRetainErrorConvention) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    value.integer.table_name = "NoSuchTable";
    const auto before = database::thread_statement_count();
    EXPECT_THROW(value.integer.detach(), db_error);
    EXPECT_EQ(database::thread_statement_count() - before, 1u);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    ASSERT_EQ(sqlite3_create_function_v2(db.handle(), "fail_read", 0, SQLITE_UTF8,
        nullptr, [](sqlite3_context* context, int, sqlite3_value**) {
            sqlite3_result_error(context, "scalar step failure", -1);
        }, nullptr, nullptr, nullptr), SQLITE_OK);
    db.execute("CREATE VIEW ReadFailure AS SELECT 1 AS id,fail_read() AS i");
    value.integer.table_name = "ReadFailure";
    EXPECT_THROW(value.integer.detach(), db_error);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_FALSE(db.is_in_transaction());
}

TEST(LiveScalarRead, SettledCallbackRunsAfterFinalizationAndCanReenter) {
    database db(":memory:"); make_scalar_table(db); fields value(db);
    int delivered = 0;
    db.set_txn_hooks([&] {
        ++delivered;
        EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
        db.execute("UPDATE ScalarRead SET i=12");
    }, [] {});
    // This is the same dirty flag an update hook leaves for the public
    // statement funnels; no SQLite callback or external lock is held here.
    db.mark_txn_dirty();
    EXPECT_EQ(value.integer.detach(), 7);
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(value.integer.detach(), 12);
    EXPECT_EQ(delivered, 1);
}

TEST(LiveScalarRead, MaterializedModeStaysPinnedAndBridgeErrorsRemainSealed) {
    lattice::swift_lattice owner(swift_configuration(":memory:"), scalar_schemas());
    swift_dynamic_object source;
    source.table_name = "ScalarModel"; source.properties = scalar_schemas()[0].properties;
    source.values["i"] = int64_t(7); source.values["r"] = 2.5; source.values["t"] = std::string("first");
    dynamic_object_ref object(source);
    owner.add_preserving_global_id(*object.get(), fake_uuid(1));
    object.enable_row_cache();
    owner.db().execute("UPDATE ScalarModel SET i=9,r=4.5,t='second'");
    const auto before = database::thread_statement_count();
    EXPECT_EQ(object.get_int("i"), 7); EXPECT_DOUBLE_EQ(object.get_double("r"), 2.5); EXPECT_EQ(object.get_string("t"), "first");
    EXPECT_EQ(database::thread_statement_count() - before, 0u);
    object.disable_row_cache();
    EXPECT_EQ(object.get_int("i"), 9); EXPECT_DOUBLE_EQ(object.get_double("r"), 4.5); EXPECT_EQ(object.get_string("t"), "second");
    owner.db().execute("DROP TABLE ScalarModel");
    EXPECT_NO_THROW({ EXPECT_EQ(object.get_int("i"), 0); });
    EXPECT_FALSE(last_bridge_error().empty());
    last_bridge_error().clear();
}
