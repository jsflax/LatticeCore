#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
// Additive server audit-fence regression backport in the existing bridge-linked test target.
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <filesystem>
#include <fstream>
#include <iterator>

namespace {
struct ServerFenceTempDB {
    std::filesystem::path path;
    explicit ServerFenceTempDB(const std::string& name)
        : path(std::filesystem::temp_directory_path() /
            (name + "-" + lattice::uuid_t::generate().to_string() + ".sqlite")) {}
    ~ServerFenceTempDB() {
        std::error_code error;
        for (const auto& suffix : {"", "-wal", "-shm"})
            std::filesystem::remove(path.string() + suffix, error);
    }
    std::string str() const { return path.string(); }
};
lattice::SchemaVector server_fence_schemas(bool changed = false) {
    lattice::swift_schema_entry entry;
    entry.table_name = "ExistingAssetFixture";
    lattice::property_descriptor property;
    property.name = "payload"; property.type = lattice::column_type::text;
    property.kind = lattice::property_kind::primitive; property.is_indexed = true;
    entry.properties["payload"] = property;
    if (changed) { property.name = "newField"; entry.properties["newField"] = property; }
    return {entry};
}
lattice::swift_configuration server_fence_config(const std::string& path, int version = 6, bool strict = false) {
    lattice::swift_configuration config(path);
    config.target_schema_version = version;
    // This server pin has no strict-version constructor; ordinary read-only is tested below.
    return config;
}
std::shared_ptr<lattice::swift_lattice> server_fence_seed(const std::string& path, int version = 6) {
    auto result = std::make_shared<lattice::swift_lattice>(server_fence_config(path, version), server_fence_schemas());
    result->db().execute("INSERT INTO ExistingAssetFixture(globalId,payload) VALUES(?,?)",
        {std::string("00000000-0000-0000-0000-000000000001"), std::string("saved")});
    return result;
}
std::string server_fence_image(const std::string& path) {
    std::ifstream stream(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}
struct ServerFenceFiles {
    std::string main, wal;
    explicit ServerFenceFiles(const std::string& path) : main(server_fence_image(path)), wal(server_fence_image(path + "-wal")) {}
    bool operator==(const ServerFenceFiles& other) const { return main == other.main && wal == other.wal; }
};
}

TEST(AuditReadFence, DetectsMutationThenPruneEvenWhenHeadAndFloorReturnToOriginal) {
    ServerFenceTempDB file{"audit_fence_prune"};
    auto writer = server_fence_seed(file.str());
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

TEST(AuditReadFence, ReadOnlyCaptureDoesNotReadPayloadOrCreateMetadata) {
    ServerFenceTempDB file{"audit_fence_readonly"};
    auto seeded = server_fence_seed(file.str());
    seeded->db().execute("UPDATE ExistingAssetFixture SET payload=?", {std::string(1024 * 1024, 'x')});
    seeded->close(); seeded.reset();
    const ServerFenceFiles before(file.str());
    auto config = server_fence_config(file.str(), 6, true); config.read_only = true;
    auto reader = std::make_shared<lattice::swift_lattice>(config, server_fence_schemas());
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
    EXPECT_TRUE(ServerFenceFiles(file.str()) == before);
}

TEST(AuditReadFence, RejectsMalformedAmbiguousMissingAndDeniedMetadataAndClosedHandle) {
    ServerFenceTempDB file{"audit_fence_failures"};
    auto seed = server_fence_seed(file.str()); seed->close(); seed.reset();
    auto writer = std::make_shared<lattice::swift_lattice>(server_fence_config(file.str(), 6, true), server_fence_schemas());
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
    auto& read = writer->read_db();
    read.execute("CREATE TEMP VIEW _lattice_meta AS SELECT 'history_generation_revision_v1' AS key, 'bad' AS value");
    EXPECT_FALSE(core.read_audit_read_fence().has_history_generation);
    read.execute("DROP VIEW temp._lattice_meta");
    sqlite3_set_authorizer(read.handle(), [](void*, int action, const char* table, const char*, const char*, const char*) -> int {
        return action == SQLITE_READ && table && std::string(table) == "_lattice_meta" ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    EXPECT_THROW(core.read_audit_read_fence(), lattice::db_error);
    sqlite3_set_authorizer(read.handle(), nullptr, nullptr);
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
