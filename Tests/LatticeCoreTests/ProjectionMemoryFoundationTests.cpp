#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/projection_memory.hpp"
#include <future>

// Private borrower/storage foundations and the public native activation gate.
// Module and complete-service behavior have their own focused test suites.
namespace {
using namespace lattice;
using namespace std::chrono_literals;

std::shared_ptr<database_read_control> capture_control() {
    auto control = std::make_shared<database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + 3s;
    return control;
}

template<class F> void expect_capture_failure(projection_status status, F&& body) {
    try { body(); FAIL() << "expected capture failure"; }
    catch (const projection_capture_failure& error) { EXPECT_EQ(error.status, status) << error.what(); }
}

struct CallbackCounts { int functions = 0, connects = 0, progress = 0, authorizations = 0; };
void spy_function(sqlite3_context* context, int, sqlite3_value**) {
    ++static_cast<CallbackCounts*>(sqlite3_user_data(context))->functions;
    sqlite3_result_int(context, 1);
}
int spy_connect(sqlite3*, void* context, int, const char* const*, sqlite3_vtab**, char**) {
    ++static_cast<CallbackCounts*>(context)->connects;
    return SQLITE_ERROR;
}
int spy_progress(void* context) { ++static_cast<CallbackCounts*>(context)->progress; return 0; }
int spy_authorizer(void* context, int, const char*, const char*, const char*, const char*) {
    ++static_cast<CallbackCounts*>(context)->authorizations;
    return SQLITE_OK;
}
} // namespace

TEST(ProjectionMemoryFoundation, ScopeRestoresActualBusyPolicyAndFinalizesBeforeReturn) {
    database db(":memory:");
    db.execute("PRAGMA busy_timeout=17");
    db.execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY, value TEXT)");
    db.execute("INSERT INTO MemoryFixture VALUES(1,'hello')");
    sqlite3_stmt* statement = nullptr;
    auto control = capture_control();
    {
        database_projection_capture capture(db, control, statement);
        capture.prepare_read("SELECT upper(value), length(value) FROM MemoryFixture");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "HELLO");
        EXPECT_EQ(sqlite3_column_int64(statement, 1), 5);
        EXPECT_EQ(control->target, nullptr);
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout").at(0).at("timeout")), 17);
    // Old capture cancellation has no target and cannot interrupt this write.
    control->stop(static_cast<int32_t>(projection_status::cancelled));
    EXPECT_NO_THROW(db.execute("UPDATE MemoryFixture SET value='after' WHERE id=1"));
    EXPECT_EQ(std::get<std::string>(db.query("SELECT value FROM MemoryFixture").at(0).at("value")), "after");
}

TEST(ProjectionMemoryFoundation, RawEscapeRefusesBeforeTouchingExternalPolicies) {
    database db(":memory:");
    CallbackCounts counts;
    auto* handle = db.handle();
    struct clear_callbacks {
        sqlite3* handle;
        ~clear_callbacks() {
            sqlite3_progress_handler(handle, 0, nullptr, nullptr);
            sqlite3_set_authorizer(handle, nullptr, nullptr);
            sqlite3_create_function(handle, "capture_spy", 0, SQLITE_UTF8, nullptr, nullptr, nullptr, nullptr);
        }
    } cleanup{handle};
    ASSERT_EQ(sqlite3_create_function(handle, "capture_spy", 0, SQLITE_UTF8, &counts, spy_function, nullptr, nullptr), SQLITE_OK);
    sqlite3_progress_handler(handle, 1, spy_progress, &counts);
    ASSERT_EQ(sqlite3_set_authorizer(handle, spy_authorizer, &counts), SQLITE_OK);
    sqlite3_stmt* statement = nullptr;
    expect_capture_failure(projection_status::unsupported, [&] {
        database_projection_capture capture(db, capture_control(), statement);
    });
    EXPECT_EQ(counts.functions, 0);
    EXPECT_EQ(counts.progress, 0);
    EXPECT_EQ(counts.authorizations, 0);
    ASSERT_EQ(sqlite3_exec(handle, "SELECT capture_spy()", nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(counts.functions, 1);
    EXPECT_GT(counts.progress, 0);
    EXPECT_GT(counts.authorizations, 0);
    // User callback contexts never outlive their owner in this test.
    sqlite3_progress_handler(handle, 0, nullptr, nullptr);
    sqlite3_set_authorizer(handle, nullptr, nullptr);
    sqlite3_create_function(handle, "capture_spy", 0, SQLITE_UTF8, nullptr, nullptr, nullptr, nullptr);
}

TEST(ProjectionMemoryFoundation, DeniesFunctionAndEponymousModuleBeforeCallback) {
    database db(":memory:");
    db.execute("CREATE VIEW HiddenModule AS SELECT * FROM capture_module_spy");
    CallbackCounts counts;
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(db, capture_control(), statement);
        // A private scope handle is a test seam only. Installing these here
        // bypasses the public escape gate to exercise the second denial layer.
        auto* handle = capture.handle();
        ASSERT_EQ(sqlite3_create_function(handle, "capture_spy", 0, SQLITE_UTF8, &counts, spy_function, nullptr, nullptr), SQLITE_OK);
        expect_capture_failure(projection_status::unsupported, [&] { capture.prepare_read("SELECT capture_spy()"); });
        EXPECT_EQ(counts.functions, 0);
        sqlite3_create_function(handle, "capture_spy", 0, SQLITE_UTF8, nullptr, nullptr, nullptr, nullptr);
    }
    {
        database_projection_capture capture(db, capture_control(), statement);
        static const sqlite3_module module = [] { sqlite3_module m{}; m.iVersion = 1; m.xConnect = spy_connect; return m; }();
        ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "capture_module_spy", &module, &counts, nullptr), SQLITE_OK);
        EXPECT_THROW(capture.prepare_read("SELECT * FROM capture_module_spy"), db_error);
        EXPECT_EQ(counts.connects, 0);
        EXPECT_THROW(capture.prepare_read("SELECT * FROM HiddenModule"), db_error);
        EXPECT_EQ(counts.connects, 0);
        sqlite3_create_module_v2(capture.handle(), "capture_module_spy", nullptr, nullptr, nullptr);
    }
}

TEST(ProjectionMemoryFoundation, SuspendedStatementRefusesUntilItsOwnerFinalizes) {
    database db(":memory:");
    sqlite3_stmt* owned = nullptr;
    sqlite3_stmt* suspended = nullptr;
    struct finish { sqlite3_stmt*& statement; ~finish() { if (statement) sqlite3_finalize(statement); } } cleanup{suspended};
    {
        database_projection_capture capture(db, capture_control(), owned);
        // Emulate a trusted Core statement owner without using public handle()
        // (the public escape gate is tested independently).
        ASSERT_EQ(sqlite3_prepare_v2(capture.handle(), "SELECT 1 UNION ALL SELECT 2", -1, &suspended, nullptr), SQLITE_OK);
        ASSERT_EQ(sqlite3_step(suspended), SQLITE_ROW);
    }
    expect_capture_failure(projection_status::admission_rejected, [&] {
        database_projection_capture capture(db, capture_control(), owned);
    });
    sqlite3_finalize(suspended); suspended = nullptr;
    EXPECT_NO_THROW({ database_projection_capture capture(db, capture_control(), owned); });
}

TEST(ProjectionMemoryFoundation, RawHandleGetterCannotEscapeBeforeScopeRestores) {
    database db(":memory:");
    db.execute("PRAGMA busy_timeout=29");
    sqlite3_stmt* statement = nullptr;
    std::promise<void> entered;
    auto entry = entered.get_future();
    std::future<sqlite3*> result;
    {
        database_projection_capture capture(db, capture_control(), statement);
        result = std::async(std::launch::async, [&] {
            entered.set_value();
            return db.handle();
        });
        EXPECT_EQ(entry.wait_for(1s), std::future_status::ready);
        EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    }
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_NE(result.get(), nullptr);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout").at(0).at("timeout")), 29);
    expect_capture_failure(projection_status::unsupported, [&] {
        database_projection_capture capture(db, capture_control(), statement);
    });
}

TEST(ProjectionMemoryFoundation, RejectsUncommittedAndRecursiveReadsWithoutRollingBackWriter) {
    database db(":memory:");
    db.execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY)");
    db.execute("BEGIN");
    db.execute("INSERT INTO MemoryFixture VALUES(1)");
    sqlite3_stmt* statement = nullptr;
    expect_capture_failure(projection_status::admission_rejected, [&] {
        database_projection_capture capture(db, capture_control(), statement);
    });
    EXPECT_TRUE(db.is_in_transaction());
    db.execute("COMMIT");
    {
        database_projection_capture capture(db, capture_control(), statement);
        sqlite3_stmt* nested_statement = nullptr;
        expect_capture_failure(projection_status::admission_rejected, [&] {
            database_projection_capture nested(db, capture_control(), nested_statement);
        });
        capture.prepare_read("SELECT id FROM MemoryFixture");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_EQ(sqlite3_column_int64(statement, 0), 1);
    }
    EXPECT_FALSE(db.is_in_transaction());
}

TEST(ProjectionMemoryFoundation, CancellationAndDeniedWriteRestorePolicyWithoutCallbacks) {
    database db(":memory:");
    db.execute("PRAGMA busy_timeout=23");
    db.execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY)");
    sqlite3_stmt* statement = nullptr;
    auto control = capture_control();
    control->stop(static_cast<int32_t>(projection_status::cancelled));
    expect_capture_failure(projection_status::cancelled, [&] { database_projection_capture capture(db, control, statement); });
    {
        database_projection_capture capture(db, capture_control(), statement);
        expect_capture_failure(projection_status::unsupported, [&] { capture.prepare_read("INSERT INTO MemoryFixture VALUES(1)"); });
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout").at(0).at("timeout")), 23);
    EXPECT_TRUE(db.query("SELECT * FROM MemoryFixture").empty());
    EXPECT_NO_THROW(db.execute("INSERT INTO MemoryFixture VALUES(2)"));
}

TEST(ProjectionMemoryFoundation, CapturedCellsStayImmutableAndChargeRetainedBatch) {
    database db(":memory:");
    db.execute("CREATE TABLE MemoryFixture(i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)");
    const std::string text("a\0\xE2\x98\x83", 5);
    const std::vector<uint8_t> blob{0, 255, 0, 8};
    // The legacy general TEXT binder uses length=-1. Seed exact bytes through
    // BLOB conversion so this read test does not silently truncate its NUL.
    const std::vector<uint8_t> text_bytes(text.begin(), text.end());
    db.execute("INSERT INTO MemoryFixture VALUES(?,?,CAST(? AS TEXT),?,?)", {int64_t(9), 1.25, text_bytes, blob, nullptr});
    const auto seeded = db.query("SELECT hex(t) AS bytes, length(CAST(t AS BLOB)) AS size FROM MemoryFixture");
    ASSERT_EQ(std::get<std::string>(seeded.at(0).at("bytes")), "6100E29883");
    ASSERT_EQ(std::get<int64_t>(seeded.at(0).at("size")), 5);
    auto account = std::make_shared<projection_capture_account>();
    auto budget = account->reserve(32 * 1024 * 1024);
    auto storage = std::make_unique<projection_capture_storage>(budget, 5);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(db, capture_control(), statement);
        capture.prepare_read("SELECT i,r,t,b,n FROM MemoryFixture");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        storage->append(statement);
        ASSERT_EQ(sqlite3_step(statement), SQLITE_DONE);
    }
    auto batch = storage->take(1);
    EXPECT_TRUE(storage->empty());
    storage.reset();
    budget->finish();
    db.execute("UPDATE MemoryFixture SET i=44,t='new'");
    ASSERT_EQ(batch->row_count(), 1);
    EXPECT_EQ(batch->copied_bytes(), 25);
    EXPECT_EQ(std::get<int64_t>(batch->value(0, 0)), 9);
    EXPECT_EQ(std::get<double>(batch->value(0, 1)), 1.25);
    EXPECT_EQ(std::get<std::string>(batch->value(0, 2)), text);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(batch->value(0, 3)), blob);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(batch->value(0, 4)));
    expect_capture_failure(projection_status::admission_rejected, [&] { (void)account->reserve(projection_capture_account::ceiling); });
    batch.reset(); budget.reset();
    EXPECT_NO_THROW((void)account->reserve(projection_capture_account::ceiling));
}

TEST(ProjectionMemoryFoundation, ZeroPayloadMetadataStillConsumesCaptureQuota) {
    database db(":memory:");
    auto account = std::make_shared<projection_capture_account>();
    auto budget = account->reserve(1024);
    sqlite3_stmt* statement = nullptr;
    std::string sql = "SELECT NULL";
    for (int i = 1; i < 64; ++i) sql += ",NULL";
    database_projection_capture capture(db, capture_control(), statement);
    capture.prepare_read(sql);
    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    expect_capture_failure(projection_status::capture_budget_exceeded, [&] {
        projection_capture_storage storage(budget, 64);
        storage.append(statement);
    });
    budget->finish();
    EXPECT_NO_THROW((void)account->reserve(projection_capture_account::ceiling));
}

TEST(ProjectionMemoryFoundation, PublicMemoryRequestsCaptureAndReleaseBorrower) {
    lattice_db parent;
    parent.db().execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL)");
    parent.db().execute("INSERT INTO MemoryFixture VALUES(?,?)", {int64_t(7), fake_uuid(7)});
    projection_query query;
    query.table = "MemoryFixture";
    query.columns = {"id"};
    auto operation = parent.start_projection(query);
    const auto result = operation.next_batch(2);
    ASSERT_EQ(result.status_code(), static_cast<int32_t>(projection_status::done)) << result.error_message();
    ASSERT_EQ(result.row_count(), 1);
    EXPECT_EQ(std::get<int64_t>(result.value(0, 0)), 7);
    EXPECT_FALSE(operation.has_resources());
    EXPECT_NO_THROW(parent.db().execute("UPDATE MemoryFixture SET id=8"));
}
