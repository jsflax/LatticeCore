#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/projection_capture_policy.hpp"
#include <atomic>
#include <chrono>

namespace {
using namespace lattice;
using namespace std::chrono_literals;
std::shared_ptr<database_read_control> policy_control() {
    auto result = std::make_shared<database_read_control>();
    result->deadline = std::chrono::steady_clock::now() + 3s;
    return result;
}
projection_query policy_query(std::string table = "PolicyModel") {
    projection_query result; result.table = std::move(table); result.columns = {"value"};
    return result;
}
std::string policy_uri() {
    static std::atomic<uint64_t> next{0};
    return "file:lattice-policy-" + std::to_string(next.fetch_add(1)) + "-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "?mode=memory&cache=shared";
}
void seed_policy(database& db) {
    db.execute("CREATE TABLE PolicyModel(id INTEGER PRIMARY KEY, value TEXT)");
    db.execute("INSERT INTO PolicyModel VALUES(1,'first'),(2,'second')");
}
template<class F> void policy_failure(projection_status status, F&& body) {
    try { body(); FAIL() << "expected capture rejection"; }
    catch (const projection_capture_failure& failure) { EXPECT_EQ(failure.status, status) << failure.what(); }
}
struct counts { int connects = 0, functions = 0; };
int spy_connect(sqlite3*, void* context, int, const char* const*, sqlite3_vtab**, char**) {
    ++static_cast<counts*>(context)->connects;
    return SQLITE_ERROR;
}
void spy_function(sqlite3_context* context, int, sqlite3_value**) {
    ++static_cast<counts*>(sqlite3_user_data(context))->functions;
    sqlite3_result_int(context, 1);
}
const sqlite3_module* spy_module() {
    static const sqlite3_module module = [] { sqlite3_module result{}; result.iVersion = 1; result.xConnect = spy_connect; return result; }();
    return &module;
}
const sqlite3_module* stored_spy_module() {
    static const sqlite3_module module = [] {
        sqlite3_module result{}; result.iVersion = 1;
        result.xCreate = result.xConnect = [](sqlite3* db, void* context, int, const char* const*,
                                             sqlite3_vtab** out, char**) noexcept -> int {
            ++static_cast<counts*>(context)->connects;
            const int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(value TEXT)");
            if (rc != SQLITE_OK) return rc;
            try { *out = new sqlite3_vtab{}; return SQLITE_OK; }
            catch (...) { return SQLITE_NOMEM; }
        };
        result.xBestIndex = [](sqlite3_vtab*, sqlite3_index_info*) { return SQLITE_OK; };
        result.xDisconnect = result.xDestroy = [](sqlite3_vtab* table) { delete table; return SQLITE_OK; };
        result.xOpen = [](sqlite3_vtab* table, sqlite3_vtab_cursor** out) noexcept -> int {
            try { *out = new sqlite3_vtab_cursor{}; (*out)->pVtab = table; return SQLITE_OK; }
            catch (...) { return SQLITE_NOMEM; }
        };
        result.xClose = [](sqlite3_vtab_cursor* cursor) { delete cursor; return SQLITE_OK; };
        result.xFilter = [](sqlite3_vtab_cursor*, int, const char*, int, sqlite3_value**) { return SQLITE_OK; };
        result.xNext = [](sqlite3_vtab_cursor*) { return SQLITE_OK; };
        result.xEof = [](sqlite3_vtab_cursor*) { return 1; };
        result.xColumn = [](sqlite3_vtab_cursor*, sqlite3_context* context, int) { sqlite3_result_null(context); return SQLITE_OK; };
        result.xRowid = [](sqlite3_vtab_cursor*, sqlite3_int64* row) { *row = 0; return SQLITE_OK; };
        return result;
    }();
    return &module;
}
} // namespace

TEST(ProjectionMemoryPolicy, OrdinaryColumnsAndQuotedUtf8NamesAreReadWithoutHydration) {
    database db(":memory:");
    db.execute("CREATE TABLE \"odd \"\" model\"(id INTEGER, \"välue\" TEXT)");
    db.execute("INSERT INTO \"odd \"\" model\" VALUES(1,'ready')");
    auto account = std::make_shared<projection_capture_account>();
    auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(db, policy_control(), statement);
        auto query = policy_query("odd \" model"); query.columns = {"välue"};
        projection_capture_policy policy(capture, budget, query);
        bool routed = true; policy.validate_columns(query, routed);
        EXPECT_FALSE(routed);
        EXPECT_TRUE(policy.matches_schemas({"main"}));
        policy.prepare_read("SELECT \"välue\" FROM \"odd \"\" model\"");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "ready");
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_NO_THROW(db.execute("UPDATE \"odd \"\" model\" SET \"välue\"='after'"));
    budget->finish();
    EXPECT_NO_THROW(account->reserve(projection_capture_account::ceiling));
}

TEST(ProjectionMemoryPolicy, PrimaryKeyOnlyAndCountStarAllowValidatedTableAccess) {
    for (bool attached : {false, true}) {
        SCOPED_TRACE(attached ? "same-name safe attached table" : "main only");
        database db(":memory:");
        db.execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL)");
        db.execute("INSERT INTO MemoryFixture VALUES(1,'first'),(2,'second')");
        if (attached) {
            db.execute("ATTACH DATABASE ':memory:' AS remote");
            db.execute("CREATE TABLE remote.MemoryFixture(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL)");
            db.execute("INSERT INTO remote.MemoryFixture VALUES(3,'attached')");
        }
        // Keep the originally failing id-only SELECT exact. Count and ordering
        // exercise the same table-level authorizer action through real prepare.
        for (const std::string sql : {"SELECT \"id\" FROM \"MemoryFixture\"",
                                     "SELECT count(*) FROM \"MemoryFixture\"",
                                     "SELECT \"id\" FROM \"MemoryFixture\" ORDER BY \"id\""}) {
            SCOPED_TRACE(sql);
            auto account = std::make_shared<projection_capture_account>();
            auto budget = account->reserve(1024 * 1024);
            sqlite3_stmt* statement = nullptr;
            {
                database_projection_capture capture(db, policy_control(), statement);
                auto query = policy_query("MemoryFixture"); query.columns = {"id"};
                projection_capture_policy policy(capture, budget, query);
                bool routed = true; policy.validate_columns(query, routed);
                EXPECT_FALSE(routed);
                policy.prepare_read(sql);
                ASSERT_TRUE(sqlite3_stmt_readonly(statement));
                ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
                if (sql.find("count(*)") != std::string::npos) {
                    EXPECT_EQ(sqlite3_column_int64(statement, 0), 2);
                } else {
                    EXPECT_EQ(sqlite3_column_int64(statement, 0), 1);
                    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
                    EXPECT_EQ(sqlite3_column_int64(statement, 0), 2);
                }
                EXPECT_EQ(sqlite3_step(statement), SQLITE_DONE);
            }
            EXPECT_EQ(statement, nullptr);
            EXPECT_FALSE(db.is_in_transaction());
        }
    }
}

TEST(ProjectionMemoryPolicy, SchemaLessTableAccessKeepsOtherReadShapesDenied) {
    database db(":memory:"); seed_policy(db);
    struct denied_read { const char* name; const char* column; const char* schema; };
    const denied_read cases[]{
        {"PolicyModel", nullptr, nullptr},
        {"PolicyModel", "value", nullptr},
        {"PolicyModel", "", ""},
        {"PolicyModel", "", "unknown"},
        {"UnknownPolicy", "", nullptr},
        {nullptr, "", nullptr}
    };
    for (const auto& item : cases) {
        auto account = std::make_shared<projection_capture_account>();
        auto budget = account->reserve(1024 * 1024);
        sqlite3_stmt* statement = nullptr;
        database_projection_capture capture(db, policy_control(), statement);
        projection_capture_policy policy(capture, budget, policy_query());
        // Direct private-policy calls distinguish NULL from empty callback
        // arguments without changing the generated production query.
        EXPECT_EQ(policy.authorize(SQLITE_READ, "PolicyModel", "", nullptr, nullptr), SQLITE_OK);
        EXPECT_EQ(policy.authorize(SQLITE_READ, item.name, item.column, item.schema, nullptr), SQLITE_DENY);
        EXPECT_EQ(statement, nullptr);
    }
}

TEST(ProjectionMemoryPolicy, SameNamedUnsafeAttachedTableIsRejectedBeforeCallback) {
    counts observed;
    const auto uri = policy_uri(); database seed(uri);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture setup(seed, policy_control(), statement);
        ASSERT_EQ(sqlite3_create_module_v2(setup.handle(), "policy_stored", stored_spy_module(), &observed, nullptr), SQLITE_OK);
    }
    seed.execute("CREATE VIRTUAL TABLE MemoryFixture USING policy_stored");
    ASSERT_EQ(observed.connects, 1);
    database owner(":memory:");
    owner.execute("CREATE TABLE MemoryFixture(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL)");
    owner.execute("INSERT INTO MemoryFixture VALUES(1,'main')");
    owner.execute("ATTACH DATABASE ? AS remote", {uri});
    auto account = std::make_shared<projection_capture_account>();
    auto budget = account->reserve(1024 * 1024);
    database_projection_capture capture(owner, policy_control(), statement);
    ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_stored", stored_spy_module(), &observed, nullptr), SQLITE_OK);
    auto query = policy_query("MemoryFixture"); query.columns = {"id"};
    try { projection_capture_policy policy(capture, budget, query); FAIL() << "expected same-name unsafe table rejection"; }
    catch (const projection_capture_failure& failure) {
        EXPECT_EQ(failure.status, projection_status::unsupported);
        EXPECT_NE(std::string(failure.what()).find("virtual table is not an approved bounds RTree"), std::string::npos)
            << failure.what();
    }
    EXPECT_EQ(observed.connects, 1);
    EXPECT_EQ(statement, nullptr);
}

TEST(ProjectionMemoryPolicy, UnknownDirectNestedAndLegacyQuotedModulesNeverConnect) {
    database db(":memory:"); seed_policy(db);
    db.execute("CREATE VIEW HiddenPolicy AS SELECT * FROM [policy_spy]");
    db.execute("CREATE VIEW OuterPolicy AS SELECT * FROM 'HiddenPolicy'");
    counts observed;
    const std::vector<std::string> expressions{
        "id IN (SELECT value FROM policy_spy)",
        "id IN (SELECT value FROM `policy_spy`)",
        "id IN (SELECT value FROM HiddenPolicy)",
        "id IN (SELECT value FROM OuterPolicy)"};
    for (const auto& expression : expressions) {
        auto account = std::make_shared<projection_capture_account>();
        auto budget = account->reserve(1024 * 1024);
        sqlite3_stmt* statement = nullptr;
        database_projection_capture capture(db, policy_control(), statement);
        // Test seam deliberately bypasses the public raw-handle rejection.
        ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_spy", spy_module(), &observed, nullptr), SQLITE_OK);
        auto query = policy_query(); query.where_clause = expression;
        policy_failure(projection_status::unsupported, [&] { projection_capture_policy policy(capture, budget, query); });
        EXPECT_EQ(observed.connects, 0);
        EXPECT_EQ(statement, nullptr);
        EXPECT_NO_THROW(capture.prepare_read("SELECT value FROM PolicyModel"));
        ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_spy", nullptr, nullptr, nullptr), SQLITE_OK);
    }
}

TEST(ProjectionMemoryPolicy, UnreachableModuleViewDoesNotWidenReadAuthorization) {
    database db(":memory:"); seed_policy(db);
    db.execute("CREATE VIEW UnrelatedPolicy AS SELECT * FROM policy_spy");
    counts observed;
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    database_projection_capture capture(db, policy_control(), statement);
    ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_spy", spy_module(), &observed, nullptr), SQLITE_OK);
    {
        projection_capture_policy policy(capture, budget, policy_query());
        policy.prepare_read("SELECT value FROM PolicyModel WHERE id=1");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_EQ(observed.connects, 0);
    }
    ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_spy", nullptr, nullptr, nullptr), SQLITE_OK);
}

TEST(ProjectionMemoryPolicy, JsonEachBoundCollectionRetainsSQLiteSubquerySemantics) {
    for (bool warm : {false, true}) {
        SCOPED_TRACE(warm ? "warm builtin module" : "fresh builtin module");
        database db(":memory:"); seed_policy(db);
        // Some stock SQLite builds omit json_each from module_list until first
        // use. Both cold and previously connected modules must keep the same
        // predicate semantics; this is not a module inventory equality test.
        if (warm) EXPECT_TRUE(db.query("SELECT value FROM json_each('[]')").empty());
        auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
        sqlite3_stmt* statement = nullptr;
        database_projection_capture capture(db, policy_control(), statement);
        auto query = policy_query(); query.where_clause = "id IN (SELECT value FROM json_each(?1))";
        projection_capture_policy policy(capture, budget, query);
        policy.prepare_read("SELECT value FROM PolicyModel WHERE id IN (SELECT value FROM json_each(?1)) ORDER BY id");
        ASSERT_EQ(sqlite3_bind_text(statement, 1, "[2]", -1, SQLITE_STATIC), SQLITE_OK);
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "second");
        EXPECT_EQ(sqlite3_step(statement), SQLITE_DONE);
    }
}

TEST(ProjectionMemoryPolicy, DynamicPragmaAndMalformedSqlAreRejectedBeforePrepare) {
    database db(":memory:"); seed_policy(db);
    for (const auto& expression : {"id IN (SELECT cid FROM pragma_table_info('PolicyModel'))", "id = 'unterminated"}) {
        auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
        sqlite3_stmt* statement = nullptr;
        database_projection_capture capture(db, policy_control(), statement);
        auto query = policy_query(); query.where_clause = expression;
        policy_failure(projection_status::unsupported, [&] { projection_capture_policy policy(capture, budget, query); });
        EXPECT_EQ(statement, nullptr);
    }
}

TEST(ProjectionMemoryPolicy, DeniedFunctionCannotExecuteAndPolicyStaysTerminal) {
    database db(":memory:"); seed_policy(db);
    counts observed;
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    database_projection_capture capture(db, policy_control(), statement);
    ASSERT_EQ(sqlite3_create_function(capture.handle(), "policy_function_spy", 0, SQLITE_UTF8,
        &observed, spy_function, nullptr, nullptr), SQLITE_OK);
    {
        projection_capture_policy policy(capture, budget, policy_query());
        policy_failure(projection_status::unsupported, [&] { policy.prepare_read("SELECT value FROM PolicyModel WHERE policy_function_spy()"); });
        EXPECT_EQ(observed.functions, 0);
        policy_failure(projection_status::unsupported, [&] { policy.prepare_read("SELECT value FROM PolicyModel"); });
    }
    ASSERT_EQ(sqlite3_create_function(capture.handle(), "policy_function_spy", 0, SQLITE_UTF8,
        nullptr, nullptr, nullptr, nullptr), SQLITE_OK);
}

TEST(ProjectionMemoryPolicy, SqlInstalledLikeOverrideIsNotTrustedAsBuiltin) {
    database db(":memory:"); seed_policy(db);
    db.execute("PRAGMA case_sensitive_like=ON");
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    database_projection_capture capture(db, policy_control(), statement);
    projection_capture_policy policy(capture, budget, policy_query());
    policy_failure(projection_status::unsupported, [&] { policy.prepare_read("SELECT value FROM PolicyModel WHERE value LIKE 'f%'"); });
}

TEST(ProjectionMemoryPolicy, CatalogGuardsPreventSharedSchemaReplacementUntilRelease) {
    const auto uri = policy_uri(); database owner(uri), other(uri); seed_policy(owner);
    other.execute("PRAGMA busy_timeout=0");
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(owner, policy_control(), statement);
        projection_capture_policy policy(capture, budget, policy_query());
        EXPECT_THROW(other.execute("DROP TABLE PolicyModel"), db_error);
        policy.prepare_read("SELECT value FROM PolicyModel WHERE id=1");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "first");
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_NO_THROW(other.execute("DROP TABLE PolicyModel"));
}

TEST(ProjectionMemoryPolicy, EmptyMainCatalogStillPinsUntilTempModelReadCompletes) {
    const auto uri = policy_uri(); database owner(uri), other(uri);
    owner.execute("CREATE TEMP TABLE PolicyModel(id INTEGER, value TEXT)");
    owner.execute("INSERT INTO PolicyModel VALUES(1,'temp')");
    other.execute("PRAGMA busy_timeout=0");
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(owner, policy_control(), statement);
        projection_capture_policy policy(capture, budget, policy_query());
        EXPECT_THROW(other.execute("CREATE TABLE Fresh(id INTEGER)"), db_error);
        policy.prepare_read("SELECT value FROM PolicyModel");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "temp");
    }
    EXPECT_NO_THROW(other.execute("CREATE TABLE Fresh(id INTEGER)"));
}

TEST(ProjectionMemoryPolicy, FreshAttachedCoreRTreeConnectsOnlyAfterShadowValidation) {
    const auto uri = policy_uri(); database seed(uri); seed_policy(seed);
    seed.execute("CREATE VIRTUAL TABLE _PolicyModel_geo_rtree USING rtree(id,minLat,maxLat,minLon,maxLon)");
    seed.execute("INSERT INTO _PolicyModel_geo_rtree VALUES(1,1,2,3,4)");
    database owner(":memory:");
    owner.execute("ATTACH DATABASE ? AS \"remote store\"", {uri});
    owner.execute("CREATE TEMP VIEW PolicyModel AS SELECT *, 'remote store' AS _source FROM \"remote store\".PolicyModel");
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(owner, policy_control(), statement);
        auto query = policy_query(); query.bounds = projection_bounds{"geo", 1, 2, 3, 4};
        projection_capture_policy policy(capture, budget, query);
        EXPECT_TRUE(policy.matches_schemas({"main", "remote store"}));
        EXPECT_FALSE(policy.matches_schemas({"main"}));
        bool routed = false; policy.validate_columns(query, routed); EXPECT_TRUE(routed);
        EXPECT_TRUE(policy.table_exists("remote store", "_PolicyModel_geo_rtree"));
        policy.prepare_read("SELECT value FROM PolicyModel WHERE EXISTS(SELECT 1 FROM \"remote store\"._PolicyModel_geo_rtree AS r WHERE r.id=PolicyModel.id AND r.minLat<=2 AND r.maxLat>=1)");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "first");
        EXPECT_EQ(sqlite3_step(statement), SQLITE_DONE);
    }
    EXPECT_NO_THROW(seed.execute("UPDATE PolicyModel SET value='after' WHERE id=1"));
}

TEST(ProjectionMemoryPolicy, RTreeExtraArgumentsAndShadowTriggersAreRefused) {
    for (bool trigger : {false, true}) {
        SCOPED_TRACE(trigger ? "shadow trigger" : "extra argument");
        const auto uri = policy_uri();
        database seed(uri); seed_policy(seed);
        seed.execute(trigger
            ? "CREATE VIRTUAL TABLE _PolicyModel_geo_rtree USING rtree(id,minLat,maxLat,minLon,maxLon)"
            : "CREATE VIRTUAL TABLE _PolicyModel_geo_rtree USING rtree(id,minLat,maxLat,minLon,maxLon,+payload)");
        if (trigger) {
            // Defensive SQLite refuses installing this historical/foreign
            // schema. Relax only the disposable seed handle for setup, then
            // restore it before capturing on a fresh, unescaped connection.
            auto* handle = seed.handle();
            int previous = -1, current = -1;
            ASSERT_EQ(sqlite3_db_config(handle, SQLITE_DBCONFIG_DEFENSIVE, -1, &previous), SQLITE_OK);
            struct restore_defensive {
                sqlite3* handle; int previous;
                ~restore_defensive() { sqlite3_db_config(handle, SQLITE_DBCONFIG_DEFENSIVE, previous, nullptr); }
            } restore{handle, previous};
            ASSERT_EQ(sqlite3_db_config(handle, SQLITE_DBCONFIG_DEFENSIVE, 0, &current), SQLITE_OK);
            ASSERT_EQ(current, 0);
            seed.execute("CREATE TRIGGER shadow_spy AFTER INSERT ON _PolicyModel_geo_rtree_node BEGIN SELECT 1; END");
            ASSERT_EQ(sqlite3_db_config(handle, SQLITE_DBCONFIG_DEFENSIVE, previous, &current), SQLITE_OK);
            ASSERT_EQ(current, previous);
            ASSERT_EQ(seed.query("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='shadow_spy' AND tbl_name='_PolicyModel_geo_rtree_node'").size(), 1u);
        }
        database db(uri);
        auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
        sqlite3_stmt* statement = nullptr;
        database_projection_capture capture(db, policy_control(), statement);
        auto query = policy_query(); query.bounds = projection_bounds{"geo", 0, 1, 0, 1};
        try { projection_capture_policy policy(capture, budget, query); FAIL() << "expected unsafe RTree rejection"; }
        catch (const projection_capture_failure& failure) {
            EXPECT_EQ(failure.status, projection_status::unsupported);
            EXPECT_NE(std::string(failure.what()).find(trigger ? "shadow has triggers" : "arguments"), std::string::npos)
                << failure.what();
        }
        EXPECT_EQ(statement, nullptr);
    }
}

TEST(ProjectionMemoryPolicy, MetadataBudgetFailureRestoresFoundationPolicyAndReleasesPins) {
    database db(":memory:"); seed_policy(db);
    db.execute("CREATE TABLE LargeCatalog(value TEXT DEFAULT '" + std::string(65536, 'x') + "')");
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(4096);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(db, policy_control(), statement);
        policy_failure(projection_status::capture_budget_exceeded, [&] { projection_capture_policy policy(capture, budget, policy_query()); });
        capture.prepare_read("SELECT value FROM PolicyModel WHERE id=1");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_NO_THROW(db.execute("DROP TABLE LargeCatalog"));
    budget->finish();
    EXPECT_NO_THROW(account->reserve(projection_capture_account::ceiling));
}

TEST(ProjectionMemoryPolicy, AttachedSchemaGuardPreventsReplacementUntilRelease) {
    const auto uri = policy_uri(); database attached(uri); seed_policy(attached);
    attached.execute("PRAGMA busy_timeout=0");
    database owner(":memory:");
    owner.execute("ATTACH DATABASE ? AS remote", {uri});
    auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture capture(owner, policy_control(), statement);
        projection_capture_policy policy(capture, budget, policy_query());
        EXPECT_TRUE(policy.matches_schemas({"main", "remote"}));
        EXPECT_THROW(attached.execute("DROP TABLE PolicyModel"), db_error);
        policy.prepare_read("SELECT value FROM remote.PolicyModel WHERE id=1");
        ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)), "first");
    }
    EXPECT_EQ(statement, nullptr);
    EXPECT_NO_THROW(attached.execute("DROP TABLE PolicyModel"));
}

TEST(ProjectionMemoryPolicy, PersistedNamedVirtualTableIsRejectedBeforeFreshConnectionCallback) {
    counts observed;
    const auto uri = policy_uri(); database seed(uri);
    sqlite3_stmt* statement = nullptr;
    {
        database_projection_capture setup(seed, policy_control(), statement);
        ASSERT_EQ(sqlite3_create_module_v2(setup.handle(), "policy_stored", stored_spy_module(), &observed, nullptr), SQLITE_OK);
    }
    seed.execute("CREATE VIRTUAL TABLE UnsafeStored USING policy_stored");
    const int created = observed.connects;
    ASSERT_EQ(created, 1);
    database owner(":memory:");
    owner.execute("ATTACH DATABASE ? AS remote", {uri});
    owner.execute("CREATE TEMP VIEW HiddenStored AS SELECT * FROM remote.UnsafeStored");
    for (const auto& table : {"UnsafeStored", "HiddenStored"}) {
        auto account = std::make_shared<projection_capture_account>(); auto budget = account->reserve(1024 * 1024);
        database_projection_capture capture(owner, policy_control(), statement);
        ASSERT_EQ(sqlite3_create_module_v2(capture.handle(), "policy_stored", stored_spy_module(), &observed, nullptr), SQLITE_OK);
        policy_failure(projection_status::unsupported, [&] { projection_capture_policy policy(capture, budget, policy_query(table)); });
        EXPECT_EQ(observed.connects, created);
        EXPECT_EQ(statement, nullptr);
    }
}
