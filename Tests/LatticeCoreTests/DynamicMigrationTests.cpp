// Standalone-friendly: no global model registry or fixed-path test log.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>

namespace {
using namespace lattice;

struct MigrationFile {
    std::filesystem::path path;
    MigrationFile() {
        static std::atomic<unsigned> sequence{0};
        path = std::filesystem::temp_directory_path() /
            ("dynamic-migration-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
             std::to_string(sequence.fetch_add(1)) + ".sqlite");
        if (std::filesystem::exists(path)) throw std::runtime_error("fixture must be fresh");
    }
    ~MigrationFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + "-wal", ignored);
        std::filesystem::remove(path.string() + "-shm", ignored);
    }
};

configuration config(const MigrationFile& file, int version = 1) {
    configuration result(file.path.string());
    result.target_schema_version = version;
    result.busy_timeout_ms = 100;
    result.audit_retention_seconds = 0;
    return result;
}

property_descriptor property(const std::string& name, column_type type = column_type::text,
                             bool nullable = false) {
    property_descriptor result;
    result.name = name;
    result.type = type;
    result.kind = property_kind::primitive;
    result.nullable = nullable;
    return result;
}

SchemaVector schemas(bool next = false) {
    swift_schema_entry item;
    item.table_name = "DynamicMigrationItem";
    item.properties = {{"value", property("value")}, {"copy", property("copy")},
                       {"score", property("score", next ? column_type::text : column_type::integer)}};
    if (next) item.properties["detail"] = property("detail", column_type::text, true);
    else item.properties["removed"] = property("removed");
    swift_schema_entry discard;
    discard.table_name = "DynamicMigrationDiscard";
    discard.properties = {{"value", property("value")}};
    return {item, discard};
}

using Callback = std::function<void(swift_migration_context_ref*)>;
void enumerate(const swift_migration_context_ref& ref, const std::string& table,
               const std::function<void(int64_t, const migration_row&)>& callback) {
#ifdef __BLOCKS__
    ref.enumerate_objects(table, ^(int64_t id, const migration_row& row) { callback(id, row); });
#else
    ref.enumerate_objects(table, callback);
#endif
}

std::shared_ptr<lattice::swift_lattice> open(const MigrationFile& file, const SchemaVector& schema,
                                  int version, const Callback& callback) {
#ifdef __BLOCKS__
    swift_migration_block_t block = ^(swift_migration_context_ref* context) { callback(context); };
#else
    swift_migration_block_t block = callback;
#endif
    auto ref = swift_lattice_ref::create_with_migration(config(file, version), schema, block);
#if LATTICE_HAS_FRT
    std::unique_ptr<swift_lattice_ref> owner(ref);
    return swift_lattice_ref::shared_for_lattice(ref->get());
#else
    return swift_lattice_ref::shared_for_lattice(ref.get());
#endif
}

void seed(const MigrationFile& file) {
    lattice::swift_lattice store(swift_configuration(config(file)), schemas());
    store.db().execute("INSERT INTO DynamicMigrationItem(value,copy,score,removed) VALUES('before','',7,'old')");
    store.db().execute("INSERT INTO DynamicMigrationDiscard(value) VALUES('discard')");
}

int version(database& db) {
    return std::stoi(std::get<std::string>(
        db.query("SELECT value FROM _lattice_meta WHERE key='schema_version'").at(0).at("value")));
}

struct StoredImage {
    std::vector<database::row_t> items, discard, audit, schema, metadata, cookie;
    bool operator==(const StoredImage&) const = default;
};
StoredImage image(const MigrationFile& file) {
    database reader(file.path.string(), database::open_mode::read_only);
    return {reader.query("SELECT * FROM DynamicMigrationItem ORDER BY id"),
            reader.query("SELECT * FROM DynamicMigrationDiscard ORDER BY id"),
            reader.query("SELECT * FROM AuditLog ORDER BY id"),
            reader.query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"),
            reader.query("SELECT * FROM _lattice_meta ORDER BY key"),
            reader.query("PRAGMA schema_version")};
}
void expectImageEquals(const StoredImage& actual, const StoredImage& expected) {
    EXPECT_EQ(actual.items, expected.items) << "items";
    EXPECT_EQ(actual.discard, expected.discard) << "discard";
    EXPECT_EQ(actual.audit, expected.audit) << "audit";
    EXPECT_EQ(actual.schema, expected.schema) << "schema";
    EXPECT_EQ(actual.metadata, expected.metadata) << "metadata";
    EXPECT_EQ(actual.cookie, expected.cookie) << "schema cookie";
}
StoredImage imageAfterBasePhase(const MigrationFile& file, int target_version) {
    // Base registry setup commits before dynamic setup. Its version-dependent
    // fingerprint is outside this transaction's rollback contract. Establish
    // that phase explicitly, then compare ALL rows/schema/markers unchanged.
    { lattice_db base(config(file, target_version)); }
    return image(file);
}

TEST(DynamicMigration, DetectsDynamicChangesAndAppliesTransformsOnce) {
    MigrationFile file;
    seed(file);
    int callbacks = 0;
    const Callback callback = [&](swift_migration_context_ref* ctx) {
        ++callbacks;
        const auto changes = ctx->pending_changes();
        ASSERT_EQ(changes.size(), 1u);
        EXPECT_EQ(changes[0].table_name, "DynamicMigrationItem");
        EXPECT_EQ(changes[0].added_columns, std::vector<std::string>{"detail"});
        EXPECT_EQ(changes[0].removed_columns, std::vector<std::string>{"removed"});
        EXPECT_EQ(changes[0].changed_columns, std::vector<std::string>{"score"});
        EXPECT_TRUE(ctx->query_sql("SELECT name FROM pragma_table_info('DynamicMigrationItem') WHERE name='detail'").empty());
        const auto rows = ctx->query_sql("SELECT id,removed FROM DynamicMigrationItem");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("removed")), "old");
        const auto id = std::get<int64_t>(rows[0].at("id"));
        ctx->rename_property("DynamicMigrationItem", "value", "copy");
        ctx->execute_sql("UPDATE DynamicMigrationItem SET value='immediate'");
        ctx->set_row_value("DynamicMigrationItem", id, "detail", std::string("queued"));
        ctx->set_row_value("DynamicMigrationItem", id, "score", std::string("seven"));
        ctx->delete_all("DynamicMigrationDiscard");
    };
    {
        auto upgraded = open(file, schemas(true), 2, callback);
        EXPECT_EQ(callbacks, 1);
        const auto rows = upgraded->db().query("SELECT value,copy,detail,score FROM DynamicMigrationItem");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("value")), "immediate");
        EXPECT_EQ(std::get<std::string>(rows[0].at("copy")), "before");
        EXPECT_EQ(std::get<std::string>(rows[0].at("detail")), "queued");
        EXPECT_EQ(std::get<std::string>(rows[0].at("score")), "seven");
        EXPECT_TRUE(upgraded->db().query("SELECT id FROM DynamicMigrationDiscard").empty());
        EXPECT_EQ(version(upgraded->db()), 2);
    }
    const auto committed = image(file);
    { auto reopened = open(file, schemas(true), 2, callback); }
    EXPECT_EQ(callbacks, 1);
    expectImageEquals(image(file), committed);
}

TEST(DynamicMigration, ThrowingCallbackRollsBackAndCanRetry) {
    MigrationFile file;
    seed(file);
    const auto before = imageAfterBasePhase(file, 2);
    int calls = 0;
    EXPECT_THROW(open(file, schemas(true), 2, [&](swift_migration_context_ref* ctx) {
        ++calls;
        ctx->execute_sql("UPDATE DynamicMigrationItem SET value='must roll back'");
        ctx->delete_all("DynamicMigrationDiscard");
        ctx->execute_sql("CREATE TABLE CallbackScratch(value TEXT)");
        throw std::runtime_error("migration refused");
    }), std::runtime_error);
    EXPECT_EQ(calls, 1);
    expectImageEquals(image(file), before);
    {
        auto recovered = open(file, schemas(), 1, [&](auto*) { ++calls; });
        EXPECT_EQ(version(recovered->db()), 1);
    }
    EXPECT_EQ(calls, 1);
    { auto retry = open(file, schemas(true), 2, [&](auto*) { ++calls; }); }
    EXPECT_EQ(calls, 2);
}

TEST(DynamicMigration, QueuedSQLFailureRollsBackDataDDLVersionAndMarkers) {
    MigrationFile file;
    seed(file);
    const auto before = imageAfterBasePhase(file, 2);
    EXPECT_THROW(open(file, schemas(true), 2, [](swift_migration_context_ref* ctx) {
        ctx->execute_sql("UPDATE DynamicMigrationItem SET copy='must roll back'");
        ctx->set_row_value("DynamicMigrationItem", 1, "value", nullptr);
        ctx->set_row_value("DynamicMigrationItem", 1, "detail", std::string("must roll back"));
    }), db_error);
    expectImageEquals(image(file), before);
    auto retry = open(file, schemas(true), 2, [](swift_migration_context_ref* ctx) {
        ctx->set_row_value("DynamicMigrationItem", 1, "detail", std::string("retry"));
    });
    EXPECT_EQ(version(retry->db()), 2);
    EXPECT_EQ(std::get<std::string>(retry->db().query(
        "SELECT detail FROM DynamicMigrationItem")[0].at("detail")), "retry");
}

TEST(DynamicMigration, NewTablesAndVersionOnlyChangesDoNotInvokeSchemaCallback) {
    MigrationFile file;
    int calls = 0;
    { auto first = open(file, schemas(), 1, [&](auto*) { ++calls; }); }
    { auto version_only = open(file, schemas(), 3, [&](auto*) { ++calls; });
      EXPECT_EQ(version(version_only->db()), 3); }
    EXPECT_EQ(calls, 0);
}

TEST(DynamicMigration, SchemaChangesWithoutVersionBumpStillInvokeGenericCallback) {
    MigrationFile file;
    seed(file);
    int calls = 0;
    auto changed = open(file, schemas(true), 1, [&](auto*) { ++calls; });
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(version(changed->db()), 1);
    EXPECT_TRUE(changed->db().get_table_info("DynamicMigrationItem").contains("detail"));
}

TEST(DynamicMigration, DowngradeRejectsBeforeCallbackOrDynamicDDL) {
    MigrationFile file;
    seed(file);
    { auto upgraded = open(file, schemas(true), 2, [](auto*) {}); }
    const auto before = imageAfterBasePhase(file, 1);
    auto downgrade_schema = schemas();
    swift_schema_entry extra;
    extra.table_name = "MustNotBeCreated";
    extra.properties = {{"value", property("value")}};
    downgrade_schema.push_back(extra);
    int calls = 0;
    EXPECT_THROW(open(file, downgrade_schema, 1, [&](auto*) { ++calls; }), std::runtime_error);
    EXPECT_EQ(calls, 0);
    expectImageEquals(image(file), before);
}

TEST(DynamicMigration, PreservesRowHooksAndAppliesGenericQueuedValuesAfterThem) {
    MigrationFile file;
    seed(file);
    {
        lattice::swift_lattice initial(swift_configuration(config(file)), schemas());
        initial.db().execute("INSERT INTO DynamicMigrationItem(value,copy,score,removed) VALUES('second','',8,'old')");
    }
    swift_configuration cfg(config(file, 2));
    cfg.addMigrationSchema(2, "DynamicMigrationItem", schemas()[0].properties, schemas(true)[0].properties);
    int generic_calls = 0;
    int enumerated = 0;
    cfg.migration_block = [&](migration_context& ctx) {
        ++generic_calls;
        // Use the actual Swift/JS adapter path, which must not queue complete
        // old rows while reading or while forwarding one explicit field.
        swift_migration_context_ref ref(&ctx);
        enumerate(ref, "DynamicMigrationItem", [&](int64_t, const migration_row&) { ++enumerated; });
        ref.set_row_value("DynamicMigrationItem", 1, "detail", std::string("generic-last"));
        ref.apply_row_updates();
    };
    int row_calls = 0;
    cfg.setRowMigrationCallback(&row_calls, [](const char* table, void* context) {
        ++*static_cast<int*>(context);
        EXPECT_STREQ(table, "DynamicMigrationItem");
        EXPECT_EQ(migration_get_current_version(), 2);
#if LATTICE_HAS_FRT
        auto* next = migration_get_new_row();
#else
        auto value = migration_get_new_row();
        auto* next = &value;
#endif
        next->set_string("copy", "row-only");
        next->set_string("detail", "row-first");
    });
    lattice::swift_lattice upgraded(cfg, schemas(true));
    EXPECT_EQ(generic_calls, 1);
    EXPECT_EQ(row_calls, 2);
    EXPECT_EQ(enumerated, 2);
    EXPECT_EQ(migration_get_current_version(), 0);
    const auto rows = upgraded.db().query("SELECT copy,detail FROM DynamicMigrationItem ORDER BY id");
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("copy")), "row-only");
    EXPECT_EQ(std::get<std::string>(rows[0].at("detail")), "generic-last");
    EXPECT_EQ(std::get<std::string>(rows[1].at("copy")), "row-only");
    EXPECT_EQ(std::get<std::string>(rows[1].at("detail")), "row-first");
}

TEST(DynamicMigration, ReadOnlyEnumerationDoesNotRestoreOldValuesOverLaterWrites) {
    MigrationFile file;
    seed(file);
    int enumerated = 0;
    auto upgraded = open(file, schemas(true), 2, [&](swift_migration_context_ref* ctx) {
        enumerate(*ctx, "DynamicMigrationItem", [&](int64_t id, const migration_row& old_row) {
            ++enumerated;
            EXPECT_EQ(id, 1);
            EXPECT_EQ(std::get<std::string>(old_row.at("value")), "before");
        });
        ctx->execute_sql("UPDATE DynamicMigrationItem SET copy='after enumeration'");
    });
    EXPECT_EQ(enumerated, 1);
    EXPECT_EQ(std::get<std::string>(upgraded->db().query(
        "SELECT copy FROM DynamicMigrationItem")[0].at("copy")), "after enumeration");
}

TEST(DynamicMigration, FailedVersionedRowUpdatesRollBackAndClearMigrationState) {
    MigrationFile file;
    auto required_schema = schemas();
    {
        lattice::swift_lattice initial(swift_configuration(config(file)), required_schema);
        initial.db().execute("INSERT INTO DynamicMigrationItem(value,copy,score,removed) VALUES('one','',1,'old')");
        initial.db().execute("INSERT INTO DynamicMigrationItem(value,copy,score,removed) VALUES('two','',2,'old')");
        // A real SQLite constraint makes the second queued row update fail;
        // this fixture does not depend on property-index descriptor support.
        initial.db().execute("CREATE UNIQUE INDEX DynamicMigrationUniqueValue ON DynamicMigrationItem(value)");
    }
    const auto before = imageAfterBasePhase(file, 2);
    swift_configuration cfg(config(file, 2));
    cfg.addMigrationSchema(2, "DynamicMigrationItem", required_schema[0].properties, required_schema[0].properties);
    int row_calls = 0;
    cfg.setRowMigrationCallback(&row_calls, [](const char*, void* context) {
        ++*static_cast<int*>(context);
#if LATTICE_HAS_FRT
        auto* next = migration_get_new_row();
#else
        auto value = migration_get_new_row();
        auto* next = &value;
#endif
        next->set_string("value", "duplicate");
    });
    EXPECT_THROW({ lattice::swift_lattice refused(cfg, required_schema); }, db_error);
    EXPECT_EQ(row_calls, 2);
    EXPECT_EQ(migration_get_current_version(), 0);
#if LATTICE_HAS_FRT
    EXPECT_EQ(migration_get_old_row(), nullptr);
    EXPECT_EQ(migration_get_new_row(), nullptr);
#endif
    expectImageEquals(image(file), before);
}

#if LATTICE_HAS_FRT
TEST(DynamicMigration, CallbackReleasesItsContextRetainWhenThrowing) {
    MigrationFile file;
    seed(file);
    swift_migration_context_ref* retained = nullptr;
    EXPECT_THROW(open(file, schemas(true), 2, [&](swift_migration_context_ref* ctx) {
        ctx->retain();
        retained = ctx;
        throw std::runtime_error("retained callback refused");
    }), std::runtime_error);
    ASSERT_NE(retained, nullptr);
    // Do not access the expired migration. Inspect only ownership: the callback
    // must have released its retain, leaving exactly the test's retain alive.
    const bool final_release = retained->release();
    EXPECT_TRUE(final_release);
    if (final_release || retained->release()) delete retained;
}
#endif
} // namespace
