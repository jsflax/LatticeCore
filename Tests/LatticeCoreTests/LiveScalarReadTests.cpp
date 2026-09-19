#include "ManagedAttachmentTestSupport.hpp"
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
#if LATTICE_HAS_FRT
    auto owner_ref = std::unique_ptr<swift_lattice_ref>(
        swift_lattice_ref::create(swift_configuration(":memory:"), scalar_schemas()));
#else
    auto owner_ref = std::make_unique<swift_lattice_ref>(
        swift_lattice_ref::create(swift_configuration(":memory:"), scalar_schemas()));
#endif
    auto& owner = *owner_ref->get();
    swift_dynamic_object source;
    source.table_name = "ScalarModel"; source.properties = scalar_schemas()[0].properties;
    source.values["i"] = int64_t(7); source.values["r"] = 2.5; source.values["t"] = std::string("first");
    dynamic_object_ref object(source);
    owner.add_preserving_global_id(*object.get(), fake_uuid(1));
    ASSERT_EQ(object.get()->lattice.get(), owner_ref->get());
    ASSERT_NE(object.managed_primary_key(), 0);
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

#include <map>

namespace {
// Core owners may keep idle engine-owned statements (for example, Rtree's
// internal statements from globally registered TestHelpers models). Preserve
// their complete identity and SQL inventory instead of requiring an empty list.
// A leaked scalar statement or a still-busy statement must still fail the test.
using ScalarRouteStatements = std::map<sqlite3_stmt*, std::pair<bool, std::string>>;
ScalarRouteStatements scalar_route_statements(sqlite3* raw) {
    ScalarRouteStatements statements;
    for (auto* statement = sqlite3_next_stmt(raw, nullptr); statement;
         statement = sqlite3_next_stmt(raw, statement)) {
        const auto* sql = sqlite3_sql(statement);
        EXPECT_EQ(sqlite3_stmt_busy(statement), 0) << (sql ? sql : "<null SQL>");
        statements.emplace(statement, std::make_pair(sql != nullptr, sql ? sql : ""));
    }
    return statements;
}

// Keep the pre-optimization public field-wrapper route as the differential
// oracle. The direct path is exercised through actual dynamic_object getters.
std::unique_ptr<swift_lattice_ref> scalar_route_owner(
    const std::string& path, const SchemaVector& schemas = scalar_schemas()) {
#if LATTICE_HAS_FRT
    auto result = std::unique_ptr<swift_lattice_ref>(
        swift_lattice_ref::create(swift_configuration(path), schemas));
#else
    auto result = std::make_unique<swift_lattice_ref>(
        swift_lattice_ref::create(swift_configuration(path), schemas));
#endif
    result->get()->stop_audit_maintenance();
    return result;
}
std::unique_ptr<dynamic_object_ref> scalar_route_object(swift_lattice_ref& owner,
                                                       int64_t integer = 7) {
    swift_dynamic_object source;
    source.table_name = "ScalarModel";
    source.properties = scalar_schemas()[0].properties;
    source.values["i"] = integer;
    source.values["r"] = 2.5;
    source.values["t"] = std::string("first");
    auto result = std::make_unique<dynamic_object_ref>(source);
    owner.get()->add_preserving_global_id(*result->get(), fake_uuid(1));
    return result;
}
template <typename T>
T scalar_route_legacy(swift_lattice_ref& owner, const dynamic_object_ref& object,
                      const std::string& name) {
    (void)owner;
    // Exercise the actual bound wrapper, including its captured generation.
    // Reconstructing from table/id alone would intentionally lack provenance.
    auto field = managed_attachment_test_access::field<T>(object, name);
    return field.detach();
}
struct ScalarRouteAuthorization {
    int read_result = SQLITE_OK;
    std::vector<std::string> actions;
    std::string* caller_name = nullptr;
    bool capture_failed = false;
    static int callback(void* raw, int action, const char* a, const char* b,
                        const char* c, const char* d) noexcept {
        auto& state = *static_cast<ScalarRouteAuthorization*>(raw);
        try {
            state.actions.push_back(std::to_string(action) + "|" + (a ? a : "<null>") +
                "|" + (b ? b : "<null>") + "|" + (c ? c : "<null>") + "|" +
                (d ? d : "<null>"));
            if (state.caller_name) {
                *state.caller_name = "changed_by_authorizer";
                state.caller_name = nullptr;
            }
            return action == SQLITE_READ && b && std::strcmp(b, "i") == 0
                ? state.read_result : SQLITE_OK;
        } catch (...) {
            state.capture_failed = true;
            return SQLITE_DENY;
        }
    }
};
struct ScalarRouteAuthorizerReset {
    sqlite3* raw;
    ~ScalarRouteAuthorizerReset() { sqlite3_set_authorizer(raw, nullptr, nullptr); }
};
}

TEST(ScalarGetterRoute, SixHundredActualDynamicReadsRemainLiveAndFresh) {
    auto owner = scalar_route_owner(":memory:");
    auto object = scalar_route_object(*owner);
    auto* raw = owner->get()->db().handle();
    const auto statements = scalar_route_statements(raw);
    ScalarRouteAuthorization authorization;
    ScalarRouteAuthorizerReset reset{raw};
    ASSERT_EQ(sqlite3_set_authorizer(raw, ScalarRouteAuthorization::callback,
                                   &authorization), SQLITE_OK);
    const auto before = database::thread_statement_count();
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(object->get_int("i"), 7);
        EXPECT_DOUBLE_EQ(object->get_double("r"), 2.5);
        EXPECT_EQ(object->get_string("t"), "first");
        EXPECT_TRUE(object->get_bool("i"));
        EXPECT_FLOAT_EQ(object->get_float("r"), 2.5f);
        EXPECT_EQ(object->get_int("id"), object->managed_primary_key());
    }
    EXPECT_EQ(database::thread_statement_count() - before, 600u);
    EXPECT_FALSE(authorization.capture_failed);
    EXPECT_FALSE(authorization.actions.empty());
    EXPECT_EQ(scalar_route_statements(raw), statements);
    sqlite3_set_authorizer(raw, nullptr, nullptr);
    owner->get()->db().execute("UPDATE ScalarModel SET i=17,r=4.5,t='later'");
    EXPECT_EQ(object->get_int("i"), 17);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 4.5);
    EXPECT_EQ(object->get_string("t"), "later");
    EXPECT_EQ(scalar_route_statements(raw), statements);
}

TEST(ScalarGetterRoute, MutableAuthorizerWithoutReinstallMatchesLegacyActions) {
    auto owner = scalar_route_owner(":memory:");
    auto object = scalar_route_object(*owner);
    auto* raw = owner->get()->db().handle();
    const auto statements = scalar_route_statements(raw);
    ScalarRouteAuthorization authorization;
    ScalarRouteAuthorizerReset reset{raw};
    ASSERT_EQ(sqlite3_set_authorizer(raw, ScalarRouteAuthorization::callback,
                                   &authorization), SQLITE_OK);
    for (int policy : {SQLITE_OK, SQLITE_DENY, SQLITE_IGNORE, SQLITE_OK}) {
        authorization.read_result = policy; // No setter between these changes.
        authorization.actions.clear();
        if (policy == SQLITE_DENY) {
            EXPECT_THROW(scalar_route_legacy<int64_t>(*owner, *object, "i"), db_error);
        } else {
            EXPECT_EQ(scalar_route_legacy<int64_t>(*owner, *object, "i"),
                      policy == SQLITE_IGNORE ? 0 : 7);
        }
        const auto legacy_actions = authorization.actions;
        EXPECT_EQ(scalar_route_statements(raw), statements);
        authorization.actions.clear();
        if (policy == SQLITE_DENY) {
            EXPECT_THROW(object->get()->get_int("i"), db_error);
        } else {
            EXPECT_EQ(object->get()->get_int("i"), policy == SQLITE_IGNORE ? 0 : 7);
        }
        EXPECT_EQ(authorization.actions, legacy_actions);
        EXPECT_FALSE(authorization.capture_failed);
        EXPECT_EQ(scalar_route_statements(raw), statements);
    }
    // Removal and replacement continue to affect the next fresh preparation.
    ASSERT_EQ(sqlite3_set_authorizer(raw, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(object->get_int("i"), 7);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    authorization.read_result = SQLITE_DENY;
    ASSERT_EQ(sqlite3_set_authorizer(raw, ScalarRouteAuthorization::callback,
                                   &authorization), SQLITE_OK);
    EXPECT_THROW(object->get()->get_int("i"), db_error);
    EXPECT_EQ(scalar_route_statements(raw), statements);
}

TEST(ScalarGetterRoute, AuthorizerCannotChangeTheSnapshottedRequestedName) {
    auto owner = scalar_route_owner(":memory:");
    auto object = scalar_route_object(*owner);
    auto* raw = owner->get()->db().handle();
    const auto statements = scalar_route_statements(raw);
    ScalarRouteAuthorization authorization;
    ScalarRouteAuthorizerReset reset{raw};
    ASSERT_EQ(sqlite3_set_authorizer(raw, ScalarRouteAuthorization::callback,
                                   &authorization), SQLITE_OK);
    std::string name = "i";
    authorization.caller_name = &name;
    EXPECT_EQ(scalar_route_legacy<int64_t>(*owner, *object, name), 7);
    EXPECT_EQ(name, "changed_by_authorizer");
    const auto legacy_actions = authorization.actions;
    EXPECT_EQ(scalar_route_statements(raw), statements);
    name = "i";
    authorization.actions.clear();
    authorization.caller_name = &name;
    EXPECT_EQ(object->get()->get_int(name), 7);
    EXPECT_EQ(name, "changed_by_authorizer");
    EXPECT_EQ(authorization.actions, legacy_actions);
    EXPECT_FALSE(authorization.capture_failed);
    EXPECT_EQ(scalar_route_statements(raw), statements);
}

TEST(ScalarGetterRoute, OwningTransactionAndExternalWritesKeepPhysicalVisibility) {
    TempDB path("scalar_route_visibility");
    auto owner = scalar_route_owner(path.str());
    auto object = scalar_route_object(*owner);
    auto& db = owner->get()->db();
    const auto statements = scalar_route_statements(db.handle());
    db.begin_transaction();
    db.execute("UPDATE ScalarModel SET i=19,r=8.5,t='uncommitted'");
    EXPECT_EQ(object->get_int("i"), 19);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 8.5);
    EXPECT_EQ(object->get_string("t"), "uncommitted");
    EXPECT_TRUE(db.is_in_transaction());
    EXPECT_EQ(scalar_route_statements(db.handle()), statements);
    db.rollback();
    EXPECT_EQ(object->get_int("i"), 7);
    EXPECT_EQ(scalar_route_statements(db.handle()), statements);
    // Bypass the ref factory's same-path cache so this is a second physical
    // connection with the normal audit UDFs and triggers installed.
    lattice::swift_lattice other(swift_configuration(path.str()), scalar_schemas());
    other.stop_audit_maintenance();
    ASSERT_NE(other.db().handle(), db.handle());
    const auto other_statements = scalar_route_statements(other.db().handle());
    other.db().execute("UPDATE ScalarModel SET i=23,r=9.5,t='external'");
    EXPECT_EQ(object->get_int("i"), 23);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 9.5);
    EXPECT_EQ(object->get_string("t"), "external");
    EXPECT_EQ(scalar_route_statements(db.handle()), statements);
    EXPECT_EQ(scalar_route_statements(other.db().handle()), other_statements);
}

TEST(ScalarGetterRoute, AttachedEqualIDsAndQuotedSchemaKeepLegacyRoutes) {
    TempDB local_path("scalar_route_local"), arm_path("scalar_route_arm_\"quote");
    auto local = scalar_route_owner(local_path.str()), arm = scalar_route_owner(arm_path.str());
    auto local_object = scalar_route_object(*local, 7);
    auto arm_object = scalar_route_object(*arm, 71);
    ASSERT_EQ(local_object->managed_primary_key(), arm_object->managed_primary_key());
    ASSERT_TRUE(local->get()->attach(*arm->get()));
    auto rows = local->get()->objects("ScalarModel", std::string("i = 71"));
    ASSERT_EQ(rows.size(), 1u);
    dynamic_object_ref attached(rows[0]);
    EXPECT_NE(attached.get_table_name(), local_object->get_table_name());
    EXPECT_NE(managed_route(attached.get_table_name()).schema_sql, "main");
    const auto local_statements = scalar_route_statements(local->get()->db().handle());
    const auto arm_statements = scalar_route_statements(arm->get()->db().handle());
    EXPECT_EQ(attached.get_int("i"), scalar_route_legacy<int64_t>(*local, attached, "i"));
    EXPECT_EQ(attached.get_int("i"), 71);
    EXPECT_EQ(local_object->get_int("i"), 7);
    EXPECT_EQ(scalar_route_statements(local->get()->db().handle()), local_statements);
    EXPECT_EQ(scalar_route_statements(arm->get()->db().handle()), arm_statements);
    attached.set_int("i", 81);
    EXPECT_EQ(arm_object->get_int("i"), 81);
    EXPECT_EQ(local_object->get_int("i"), 7);
    EXPECT_EQ(scalar_route_statements(local->get()->db().handle()), local_statements);
    EXPECT_EQ(scalar_route_statements(arm->get()->db().handle()), arm_statements);
}

TEST(ScalarGetterRoute, UnmanagedAndMaterializedPathsRetainTheirSQLBoundaries) {
    swift_dynamic_object source;
    source.table_name = "ScalarModel";
    source.values["i"] = int64_t(37);
    source.values["r"] = 3.5;
    source.values["t"] = std::string("unmanaged");
    dynamic_object_ref unmanaged(source);
    auto before = database::thread_statement_count();
    EXPECT_EQ(unmanaged.get_int("i"), 37);
    EXPECT_DOUBLE_EQ(unmanaged.get_double("r"), 3.5);
    EXPECT_EQ(unmanaged.get_string("t"), "unmanaged");
    EXPECT_EQ(database::thread_statement_count() - before, 0u);
    auto schemas = scalar_schemas();
    schemas[0].properties.at("t").nullable = true;
    auto owner = scalar_route_owner(":memory:", schemas);
    auto object = scalar_route_object(*owner);
    auto* raw = owner->get()->db().handle();
    const auto statements = scalar_route_statements(raw);
    object->enable_row_cache();
    owner->get()->db().execute("UPDATE ScalarModel SET i=11,r=8.5,t='new'");
    before = database::thread_statement_count();
    EXPECT_EQ(object->get_int("i"), 7);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 2.5);
    EXPECT_EQ(object->get_string("t"), "first");
    EXPECT_EQ(object->get_int("id"), object->managed_primary_key());
    EXPECT_EQ(database::thread_statement_count() - before, 0u);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    // A missing expression in the snapshot still executes its live SELECT.
    before = database::thread_statement_count();
    EXPECT_EQ(object->get_int("NULL"), 0);
    EXPECT_EQ(database::thread_statement_count() - before, 1u);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    // Numeric variant mismatches and known-NULL text all fall through to
    // live reads. NULL is valid for this nullable column and normal audit JSON;
    // a BLOB stored in a declared TEXT column is not valid audit JSON.
    owner->get()->db().execute("UPDATE ScalarModel SET i='wrong',r='wrong',t=NULL");
    const auto stored = owner->get()->db().query("SELECT typeof(t) AS t_type FROM ScalarModel");
    ASSERT_EQ(stored.size(), 1u);
    EXPECT_EQ(std::get<std::string>(stored[0].at("t_type")), "null");
    object->refresh_row_cache();
    owner->get()->db().execute("UPDATE ScalarModel SET i=11,r=8.5,t='new'");
    before = database::thread_statement_count();
    EXPECT_EQ(object->get_int("i"), 11);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 8.5);
    EXPECT_EQ(object->get_string("t"), "new");
    EXPECT_EQ(database::thread_statement_count() - before, 3u);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    object->disable_row_cache();
    before = database::thread_statement_count();
    EXPECT_EQ(object->get_int("i"), 11);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 8.5);
    EXPECT_EQ(object->get_string("t"), "new");
    EXPECT_EQ(database::thread_statement_count() - before, 3u);
    EXPECT_EQ(scalar_route_statements(raw), statements);
}

TEST(ScalarGetterRoute, CustomFieldSQLFallbackErrorsAndCloseMatchLegacy) {
    auto owner = scalar_route_owner(":memory:");
    auto object = scalar_route_object(*owner);
    auto* raw = owner->get()->db().handle();
    const auto statements = scalar_route_statements(raw);
    for (const auto* name : {"i", "i + 1", "i AS other", "NULL", "'wrong type'"}) {
        EXPECT_EQ(object->get()->get_int(name), scalar_route_legacy<int64_t>(*owner, *object, name));
        EXPECT_EQ(scalar_route_statements(raw), statements);
    }
    EXPECT_EQ(object->get_int("i + 1"), 8);
    EXPECT_THROW(object->get()->get_int("no_such_column"), db_error);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    last_bridge_error().clear();
    EXPECT_EQ(object->get_int("no_such_column"), 0);
    EXPECT_FALSE(last_bridge_error().empty());
    EXPECT_EQ(scalar_route_statements(raw), statements);
    last_bridge_error().clear();
    ASSERT_EQ(sqlite3_create_function_v2(raw, "fail_scalar_route", 0, SQLITE_UTF8, nullptr,
        [](sqlite3_context* c, int, sqlite3_value**) {
            sqlite3_result_error(c, "scalar route step failure", -1);
        }, nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_THROW(object->get()->get_int("fail_scalar_route()"), db_error);
    EXPECT_EQ(scalar_route_statements(raw), statements);
    owner->get()->db().close();
    EXPECT_EQ(object->get_int("i"), 0);
    EXPECT_DOUBLE_EQ(object->get_double("r"), 0.0);
    EXPECT_EQ(object->get_string("t"), "");
}

TEST(ScalarGetterRoute, SettledCallbackStillSeesFinalizedStatementAndCanReenter) {
    auto owner = scalar_route_owner(":memory:");
    auto object = scalar_route_object(*owner);
    auto& db = owner->get()->db();
    auto* raw = db.handle();
    const auto statements = scalar_route_statements(raw);
    int callbacks = 0;
    struct ClearHooks {
        database& db;
        ~ClearHooks() { db.set_txn_hooks({}, {}); }
    } clear_hooks{db};
    db.set_txn_hooks([&] {
        ++callbacks;
        EXPECT_EQ(scalar_route_statements(raw), statements);
        EXPECT_EQ(object->get_int("i"), 7);
        EXPECT_EQ(scalar_route_statements(raw), statements);
    }, [] {});
    db.mark_txn_dirty();
    EXPECT_EQ(object->get_int("i"), 7);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(scalar_route_statements(raw), statements);
}

TEST(ScalarGetterSQL, BuilderPreservesExactSQLAcrossIdentifierAndExpressionSpellings) {
    std::vector<std::string> tables{
        "ScalarModel", "_Model9", "9Model", "main.ScalarModel", "arm.ScalarModel",
        "\"a.\"\"quoted\".ScalarModel", "\"main\".\"ScalarModel\"", "a.b.c",
        "\"a\"\"b\"", "\"unfinished", "two words", "模型", std::string(1'024, 'x')
    };
    // Exercise parser boundaries without replacing its existing interpretation
    // with a new identifier grammar. The expected bytes use the old expression.
    const std::string alphabet = "aA_09.\" \t";
    for (const char a : alphabet) {
        tables.emplace_back(1, a);
        for (const char b : alphabet) tables.push_back(std::string{a, b});
    }
    const std::vector<std::string> columns{
        "i", "", "i + 1", "i AS other", "i AS i, r AS i", "'é'", std::string("i\0tail", 6)
    };
    size_t compared = 0;
    for (const auto& table : tables) {
        std::string route;
        try { route = managed_table_sql(table); }
        catch (const std::invalid_argument&) { continue; } // covered below
        for (const auto& column : columns) {
            SCOPED_TRACE(table);
            EXPECT_EQ(detail::managed_scalar_select_sql(table, column),
                      "SELECT " + column + " FROM " + route + " WHERE id = ?");
            ++compared;
        }
    }
    EXPECT_GT(compared, 600u);
}

TEST(ScalarGetterSQL, InvalidTableRoutesKeepTheExistingException) {
    for (const std::string table : {std::string{}, std::string("\0", 1),
            std::string("a\0b", 3), std::string("main."), std::string("\"\""),
            std::string("main.\"\""), std::string("\"arm\".")}) {
        SCOPED_TRACE(table);
        std::string original_error;
        try { (void)managed_table_sql(table); FAIL() << "expected invalid original route"; }
        catch (const std::invalid_argument& error) { original_error = error.what(); }
        try { (void)detail::managed_scalar_select_sql(table, "i"); FAIL() << "expected invalid new route"; }
        catch (const std::invalid_argument& error) { EXPECT_EQ(error.what(), original_error); }
    }
}
