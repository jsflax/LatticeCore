#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <fstream>
#include <iterator>

namespace {
lattice::SchemaVector asset_schemas(bool changed = false) {
    lattice::swift_schema_entry entry;
    entry.table_name = "ExistingAssetFixture";
    lattice::property_descriptor property;
    property.name = "payload"; property.type = lattice::column_type::text;
    property.kind = lattice::property_kind::primitive; property.is_indexed = true;
    entry.properties["payload"] = property;
    if (changed) { property.name = "newField"; entry.properties["newField"] = property; }
    return {entry};
}
lattice::swift_configuration asset_config(const std::string& path, int version = 6, bool strict = false) {
    lattice::swift_configuration config(path);
    config.target_schema_version = version;
    config.required_existing_schema_version = strict ? version : 0;
    return config;
}
std::shared_ptr<lattice::swift_lattice> seed_asset(const std::string& path, int version = 6) {
    auto result = std::make_shared<lattice::swift_lattice>(asset_config(path, version), asset_schemas());
    result->db().execute("INSERT INTO ExistingAssetFixture(globalId,payload) VALUES(?,?)",
        {std::string("00000000-0000-0000-0000-000000000001"), std::string("saved")});
    return result;
}
std::string file_image(const std::string& path) {
    std::ifstream stream(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}
struct Files {
    std::string main, wal;
    explicit Files(const std::string& path) : main(file_image(path)), wal(file_image(path + "-wal")) {}
    bool operator==(const Files& other) const { return main == other.main && wal == other.wal; }
};
void mutate_asset(const std::string& path, const std::string& sql) {
    lattice::database db(path);
    // Deliberate current-version corruption setup, not an old writer bypass.
    lattice::detail::register_sync_storage_writer(db);
    db.execute(sql);
}
struct CountingScheduler : lattice::scheduler {
    int shutdowns = 0;
    void invoke(std::function<void()>&& work) override { work(); }
    bool is_on_thread() const noexcept override { return true; }
    bool is_same_as(const lattice::scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override { return shutdowns == 0; }
    void shutdown() override { ++shutdowns; }
};
}

TEST(ExistingSchemaAccess, RejectsOldNewMissingAndMalformedMetadataWithoutWriting) {
    for (const auto& mutation : std::vector<std::string>{
        "UPDATE _lattice_meta SET value='5' WHERE key='schema_version'",
        "UPDATE _lattice_meta SET value='7' WHERE key='schema_version'",
        "UPDATE _lattice_meta SET value='0' WHERE key='schema_version'",
        "UPDATE _lattice_meta SET value='6bad' WHERE key='schema_version'",
        "UPDATE _lattice_meta SET value=x'36' WHERE key='schema_version'",
        "UPDATE _lattice_meta SET value='2147483648' WHERE key='schema_version'",
        "DELETE FROM _lattice_meta WHERE key='schema_version'",
        "DROP TABLE _lattice_meta",
        "UPDATE _lattice_meta SET value=value || 'bad' WHERE key LIKE 'schema_fingerprint:%'"
    }) {
        TempDB file{"existing_refusal"};
        auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
        mutate_asset(file.str(), mutation);
        const Files before(file.str());
        for (bool read_only : {false, true}) {
            auto config = asset_config(file.str(), 6, true); config.read_only = read_only;
            EXPECT_THROW(std::make_shared<lattice::swift_lattice>(config, asset_schemas()), std::exception) << mutation;
            EXPECT_TRUE(Files(file.str()) == before) << mutation;
        }
    }
}

TEST(ExistingSchemaAccess, MissingFileAndMismatchedTypedSchemaCannotBeCreatedOrRepaired) {
    TempDB missing{"existing_missing"};
    auto config = asset_config(missing.str(), 6, true);
    EXPECT_THROW(std::make_shared<lattice::swift_lattice>(config, asset_schemas()), std::exception);
    EXPECT_FALSE(std::filesystem::exists(missing.path));
    TempDB file{"existing_shape"};
    auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
    const Files before(file.str());
    config = asset_config(file.str(), 6, true);
    EXPECT_THROW(std::make_shared<lattice::swift_lattice>(config, asset_schemas(true)), std::exception);
    EXPECT_TRUE(Files(file.str()) == before);
}

TEST(ExistingSchemaAccess, RefusedLastWriterDoesNotCheckpointCommittedWAL) {
    TempDB file{"existing_wal_refusal"};
    auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
    sqlite3* raw = nullptr;
    ASSERT_EQ(sqlite3_open_v2(file.str().c_str(), &raw, SQLITE_OPEN_READWRITE, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_db_config(raw, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_create_function_v2(raw,"lattice_storage_writer_epoch",0,
        SQLITE_UTF8|SQLITE_DETERMINISTIC|SQLITE_INNOCUOUS,nullptr,
        [](sqlite3_context* context,int,sqlite3_value**) {sqlite3_result_int(context,lattice::detail::sync_storage_writer_epoch);},
        nullptr,nullptr,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(raw, "UPDATE _lattice_meta SET value='5' WHERE key='schema_version'", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_close(raw), SQLITE_OK);
    const Files before(file.str());
    ASSERT_FALSE(before.wal.empty());
    EXPECT_THROW(std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas()), std::exception);
    EXPECT_TRUE(Files(file.str()) == before);
}

TEST(ExistingSchemaAccess, RejectedSwiftSchemaDoesNotShutDownSharedScheduler) {
    TempDB file{"existing_scheduler"};
    auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
    auto scheduler = std::make_shared<CountingScheduler>();
    auto config = asset_config(file.str(), 6, true); config.sched = scheduler;
    const auto alive = lattice::lattice_db::alive_count().load();
    EXPECT_THROW(std::make_shared<lattice::swift_lattice>(config, asset_schemas(true)), std::exception);
    EXPECT_EQ(scheduler->shutdowns, 0);
    EXPECT_TRUE(scheduler->can_invoke());
    EXPECT_EQ(lattice::lattice_db::alive_count().load(), alive);
}

TEST(ExistingSchemaAccess, AdmittedHandleWritesAndNeverReopensOrAttachesByPath) {
    TempDB file{"existing_writer"}, other_file{"existing_other"};
    auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
    auto writer = std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas());
    EXPECT_EQ(writer->acquire_read_generation(), 0u);
    writer->db().execute("UPDATE ExistingAssetFixture SET payload='changed'");
    const auto rows = writer->db().query("SELECT payload FROM ExistingAssetFixture");
    ASSERT_EQ(rows.size(), 1u); EXPECT_EQ(std::get<std::string>(rows.front().at("payload")), "changed");
    auto other = seed_asset(other_file.str());
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*writer).attach(*other), lattice::db_error);
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*other).attach(*writer), lattice::db_error);
    // The Swift bridge deliberately catches C++ exceptions and reports false + its checked error.
    EXPECT_FALSE(writer->attach(*other));
    ASSERT_TRUE(writer->last_attach_error().has_value());
    EXPECT_NE(writer->last_attach_error()->find("Existing schema handles"), std::string::npos);
    EXPECT_FALSE(other->attach(*writer));
    ASSERT_TRUE(other->last_attach_error().has_value());
    EXPECT_NE(other->last_attach_error()->find("Existing schema handles"), std::string::npos);
    EXPECT_THROW(writer->reopen_write_db(), lattice::db_error);
    EXPECT_THROW(writer->reopen_read_db(), lattice::db_error);
    EXPECT_THROW(writer->close_write_db(), lattice::db_error);
    EXPECT_THROW(writer->close_read_db(), lattice::db_error);
    EXPECT_EQ(writer->stored_schema_version(), 6);
    writer->close(); other->close();
}

TEST(ExistingSchemaAccess, StatisticsCookieDriftPreservesStrictReadOnlyAndWriterAdmission) {
    TempDB file{"existing_statistics"};
    auto seeded = seed_asset(file.str());
    const auto old_cookie = seeded->db().query("PRAGMA main.schema_version");
    seeded->db().execute("ANALYZE");
    seeded->db().execute("PRAGMA optimize");
    const auto new_cookie = seeded->db().query("PRAGMA main.schema_version");
    EXPECT_NE(old_cookie, new_cookie);
    ASSERT_FALSE(seeded->db().query("SELECT name FROM main.sqlite_schema WHERE name='sqlite_stat1'").empty());
    const Files before(file.str());
    for (bool read_only : {false, true}) {
        auto config = asset_config(file.str(), 6, true); config.read_only = read_only;
        auto admitted = std::make_shared<lattice::swift_lattice>(config, asset_schemas());
        EXPECT_EQ(admitted->stored_schema_version(), 6);
        const auto rows = admitted->db().query("SELECT payload FROM ExistingAssetFixture");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows.front().at("payload")), "saved");
        admitted->close(); admitted.reset();
        EXPECT_TRUE(Files(file.str()) == before);
    }
    seeded->close();
}

TEST(ExistingSchemaAccess, StructuralDriftCannotHideBehindAnUnchangedCookie) {
    for (int mutation = 0; mutation < 3; ++mutation) {
        TempDB file{"existing_structure"};
        auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
        {
            lattice::database db(file.str());
            const auto cookies = db.query("PRAGMA main.schema_version");
            const auto cookie = std::get<int64_t>(cookies.front().at("schema_version"));
            if (mutation == 0) db.execute("DROP INDEX idx_ExistingAssetFixture_payload");
            else if (mutation == 1) db.execute("ALTER TABLE ExistingAssetFixture ADD COLUMN surprise TEXT");
            else {
                const auto triggers = db.query("SELECT name FROM main.sqlite_schema "
                    "WHERE type='trigger' AND tbl_name='ExistingAssetFixture' LIMIT 1");
                ASSERT_EQ(triggers.size(), 1u);
                const auto name = std::get<std::string>(triggers.front().at("name"));
                ASSERT_EQ(name.find('"'), std::string::npos);
                db.execute("DROP TRIGGER \"" + name + "\"");
            }
            // Even an equal cookie cannot authorize a structurally different store.
            db.execute("PRAGMA main.schema_version = " + std::to_string(cookie));
        }
        const Files before(file.str());
        for (bool read_only : {false, true}) {
            auto config = asset_config(file.str(), 6, true); config.read_only = read_only;
            EXPECT_THROW(std::make_shared<lattice::swift_lattice>(config, asset_schemas()), lattice::db_error);
            EXPECT_TRUE(Files(file.str()) == before);
        }
    }
}

TEST(ExistingSchemaAccess, MissingOrMalformedStructuralReceiptRequiresOwnerActivation) {
    for (const auto& mutation : std::vector<std::string>{
        "DELETE FROM _lattice_meta WHERE key LIKE 'schema_structure:%'",
        "UPDATE _lattice_meta SET value='bad' WHERE key LIKE 'schema_structure:%'",
        "UPDATE _lattice_meta SET value=x'626164' WHERE key LIKE 'schema_structure:%'",
        "UPDATE _lattice_meta SET value=value || 'trailing' WHERE key LIKE 'schema_structure:%'",
        "UPDATE _lattice_meta SET value=replace(value,'lattice-schema-structure:1','lattice-schema-structure:2') "
            "WHERE key LIKE 'schema_structure:%'"
    }) {
        TempDB file{"existing_receipt"};
        auto seeded = seed_asset(file.str()); seeded->close(); seeded.reset();
        mutate_asset(file.str(), mutation);
        const Files before(file.str());
        EXPECT_THROW(std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas()), lattice::db_error);
        EXPECT_TRUE(Files(file.str()) == before);
        auto owner = std::make_shared<lattice::swift_lattice>(asset_config(file.str()), asset_schemas());
        owner->close(); owner.reset();
        auto admitted = std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas());
        EXPECT_EQ(admitted->stored_schema_version(), 6);
        admitted->close();
    }
}

TEST(ExistingSchemaAccess, ReceiptCapacityDoesNotLimitNormalOwnerSchema) {
    TempDB file{"existing_large_schema"};
    auto seeded = seed_asset(file.str());
    seeded->db().execute("CREATE VIEW receipt_large_fixture AS SELECT '" + std::string(256 * 1024, 'x') + "' AS payload");
    seeded->close(); seeded.reset();
    auto owner = std::make_shared<lattice::swift_lattice>(asset_config(file.str()), asset_schemas());
    EXPECT_EQ(owner->stored_schema_version(), 6);
    const auto markers = owner->db().query("SELECT value FROM _lattice_meta WHERE key LIKE 'schema_structure:%'");
    ASSERT_FALSE(markers.empty());
    for (const auto& row : markers)
        EXPECT_EQ(std::get<std::string>(row.at("value")), "lattice-schema-structure:capacity:1");
    owner->close(); owner.reset();
    const Files before(file.str());
    EXPECT_THROW(std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas()), lattice::db_error);
    EXPECT_TRUE(Files(file.str()) == before);
    auto reopened = std::make_shared<lattice::swift_lattice>(asset_config(file.str()), asset_schemas());
    EXPECT_EQ(reopened->stored_schema_version(), 6);
    reopened->close();
}

TEST(ExistingSchemaAccess, CachedOldHandleCannotAuthorizeReplacementOrLoweredVersion) {
    TempDB file{"existing_cache"}, replacement{"existing_replacement"};
    auto schemas = asset_schemas();
    auto normal = seed_asset(file.str()); normal->close(); normal.reset();
    auto config = asset_config(file.str(), 6, true);
    auto first = lattice::detail::LatticeCache::instance().get_or_create(config, schemas);
    auto second = lattice::detail::LatticeCache::instance().get_or_create(config, schemas);
    EXPECT_NE(first.get(), second.get());
    auto old = seed_asset(replacement.str(), 5); old->close(); old.reset();
    const auto retired = file.str() + ".retired";
    std::filesystem::rename(file.path, retired);
    std::filesystem::copy_file(replacement.path, file.path);
    const Files before(file.str());
    EXPECT_THROW(lattice::detail::LatticeCache::instance().get_or_create(config, schemas), std::exception);
    EXPECT_TRUE(Files(file.str()) == before);
    first->close(); second->close();
    std::filesystem::remove(retired);
}

TEST(AuditCursor, ReadsOnlyMainScalarColumnsOnOneRouteAndRejectsMalformedIdentity) {
    TempDB file{"audit_cursor"};
    auto seeded = seed_asset(file.str());
    seeded->db().execute("UPDATE ExistingAssetFixture SET payload=?", {std::string(1024 * 1024, 'x')});
    seeded->close(); seeded.reset();
    auto reader = std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas());
    auto& db = reader->read_db();
    int payload_reads = 0;
    sqlite3_set_authorizer(db.handle(), [](void* context, int action, const char* table, const char* column,
                                         const char*, const char*) -> int {
        if (action == SQLITE_READ && table && column && std::string(table) == "AuditLog" &&
            (std::string(column) == "changedFields" || std::string(column) == "changedFieldsNames")) {
            ++*static_cast<int*>(context); return SQLITE_DENY;
        }
        return SQLITE_OK;
    }, &payload_reads);
    const auto cursor = reader->read_audit_cursor();
    EXPECT_GT(cursor.head, 0); EXPECT_GT(cursor.floor, 0); EXPECT_LE(cursor.floor, cursor.head);
    EXPECT_EQ(cursor.head_global_id.size(), 36u); EXPECT_EQ(payload_reads, 0);
    db.execute("CREATE TEMP VIEW AuditLog AS SELECT 999999 AS id, 'bad' AS globalId");
    const auto same = reader->read_audit_cursor();
    EXPECT_EQ(same.head, cursor.head); EXPECT_EQ(same.head_global_id, cursor.head_global_id);
    db.execute("DROP VIEW temp.AuditLog");
    sqlite3_set_authorizer(db.handle(), nullptr, nullptr);
    db.execute("UPDATE main.AuditLog SET globalId='bad' WHERE id=?", {cursor.head});
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*reader).read_audit_cursor(), lattice::db_error);
    db.execute("DELETE FROM main.AuditLog");
    const auto empty = reader->read_audit_cursor();
    EXPECT_EQ(empty.head, 0); EXPECT_EQ(empty.floor, 0); EXPECT_TRUE(empty.head_global_id.empty());
    reader->close();
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*reader).read_audit_cursor(), lattice::db_error);
}


TEST(ExistingSchemaAuditReadFence, DetectsMutationThenPruneEvenWhenHeadAndFloorReturnToOriginal) {
    TempDB file{"audit_fence_prune"};
    auto writer = seed_asset(file.str());
    auto& core = static_cast<lattice::lattice_db&>(*writer);
    auto& db = writer->db();
    lattice::register_replication_slot(db, "fence-reader");
    // The legacy resume row survives pruning while the slot has no saved remote cursor.
    lattice::ensure_cursor_column(db);
    db.execute("UPDATE _lattice_replication_slots SET last_received_event_id=NULL WHERE sync_id='fence-reader'");
    db.execute("UPDATE AuditLog SET isFromRemote=1");
    const auto before = core.read_audit_read_fence();
    ASSERT_FALSE(before.has_history_generation);
    ASSERT_GT(before.cursor.head, 0);
    std::string generation;
    for (const auto& value : {std::string("first change"), std::string("second change")}) {
        db.execute("UPDATE ExistingAssetFixture SET payload=?", {value});
        ASSERT_GT(core.read_audit_read_fence().cursor.head, before.cursor.head);
        db.execute("UPDATE _lattice_replication_slots SET upload_floor=(SELECT MAX(id) FROM AuditLog)");
        ASSERT_GT(core.safe_compact_audit_log(), 0);
        const auto after = core.read_audit_read_fence();
        EXPECT_EQ(after.cursor.head, before.cursor.head);
        EXPECT_EQ(after.cursor.floor, before.cursor.floor);
        EXPECT_EQ(after.cursor.head_global_id, before.cursor.head_global_id);
        ASSERT_TRUE(after.has_history_generation);
        EXPECT_EQ(after.history_generation.size(), 36u);
        EXPECT_NE(after.history_generation, generation);
        generation = after.history_generation;
        const auto rows = db.query("SELECT payload FROM ExistingAssetFixture");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows.front().at("payload")), value);
    }
    ASSERT_GT(core.force_compact_audit_log(), 0);
    const auto reset = core.read_audit_read_fence();
    EXPECT_TRUE(reset.has_history_generation);
    EXPECT_NE(reset.history_generation, generation);
    writer->close();
}

TEST(ExistingSchemaAuditReadFence, StrictReadOnlyCaptureDoesNotReadPayloadOrCreateMetadata) {
    TempDB file{"audit_fence_readonly"};
    auto seeded = seed_asset(file.str());
    seeded->db().execute("UPDATE ExistingAssetFixture SET payload=?", {std::string(1024 * 1024, 'x')});
    seeded->close(); seeded.reset();
    const Files before(file.str());
    auto config = asset_config(file.str(), 6, true); config.read_only = true;
    auto reader = std::make_shared<lattice::swift_lattice>(config, asset_schemas());
    int forbidden = 0;
    sqlite3_set_authorizer(reader->read_db().handle(), [](void* context, int action,
        const char* table, const char* column, const char*, const char*) -> int {
        const bool payload = action == SQLITE_READ && table && column && std::string(table) == "AuditLog" &&
            (std::string(column) == "changedFields" || std::string(column) == "changedFieldsNames");
        if (payload || action == SQLITE_INSERT || action == SQLITE_UPDATE || action == SQLITE_DELETE) {
            ++*static_cast<int*>(context); return SQLITE_DENY;
        }
        return SQLITE_OK;
    }, &forbidden);
    const auto result = static_cast<lattice::lattice_db&>(*reader).read_audit_read_fence();
    EXPECT_GT(result.cursor.head, 0); EXPECT_FALSE(result.has_history_generation);
    EXPECT_TRUE(result.history_generation.empty()); EXPECT_EQ(forbidden, 0);
    sqlite3_set_authorizer(reader->read_db().handle(), nullptr, nullptr);
    reader->close(); reader.reset();
    EXPECT_TRUE(Files(file.str()) == before);
}

TEST(ExistingSchemaAuditReadFence, RejectsMalformedAmbiguousMissingAndDeniedMetadataAndClosedHandle) {
    TempDB file{"audit_fence_failures"};
    auto seed = seed_asset(file.str()); seed->close(); seed.reset();
    auto writer = std::make_shared<lattice::swift_lattice>(asset_config(file.str(), 6, true), asset_schemas());
    auto& core = static_cast<lattice::lattice_db&>(*writer);
    auto& db = writer->db();
    for (const auto& expression : std::vector<std::string>{
        "'bad'", "'00000000-0000-0000-0000-00000000000z'", "42", "x'30303030'", "zeroblob(1048576)"}) {
        db.execute("INSERT OR REPLACE INTO _lattice_meta(key,value) VALUES('history_generation_revision_v1'," + expression + ")");
        EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error) << expression;
    }
    db.execute("DELETE FROM _lattice_meta WHERE key='history_generation_revision_v1'");
    const auto initial = core.read_audit_read_fence();
    EXPECT_FALSE(initial.has_history_generation);
    // A TEMP shadow cannot redefine the owned main metadata route.
    db.execute("CREATE TEMP VIEW _lattice_meta AS SELECT 'history_generation_revision_v1' AS key, 'bad' AS value");
    EXPECT_FALSE(core.read_audit_read_fence().has_history_generation);
    db.execute("DROP VIEW temp._lattice_meta");
    sqlite3_set_authorizer(db.handle(), [](void*, int action, const char* table, const char*, const char*, const char*) -> int {
        return action == SQLITE_READ && table && std::string(table) == "_lattice_meta" ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error);
    sqlite3_set_authorizer(db.handle(), nullptr, nullptr);
    db.execute("ALTER TABLE _lattice_meta RENAME TO fixture_original_meta");
    db.execute("CREATE TABLE _lattice_meta(key TEXT,value TEXT)");
    db.execute("INSERT INTO _lattice_meta VALUES('history_generation_revision_v1','00000000-0000-0000-0000-000000000001')");
    db.execute("INSERT INTO _lattice_meta SELECT * FROM _lattice_meta");
    EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error);
    db.execute("DROP TABLE _lattice_meta");
    EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error);
    writer->close();
    EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error);
}
