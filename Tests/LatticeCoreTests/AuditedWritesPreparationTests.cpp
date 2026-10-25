#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice/sync.hpp>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

struct AuditedPreparationRow { int64_t value = 0; };
LATTICE_SCHEMA(AuditedPreparationRow, value);

namespace {
using namespace lattice;

struct PreparationFile {
    std::string path = (std::filesystem::temp_directory_path() /
        ("audited-preparation-" + lattice::uuid_t::generate().to_string() + ".sqlite")).string();
    ~PreparationFile() {
        std::error_code ignored;
        for (const auto* suffix : {"", "-wal", "-shm", "-signal"})
            std::filesystem::remove(path + suffix, ignored);
    }
};

configuration preparation_config(const std::string& path) {
    configuration result(path);
    result.busy_timeout_ms = 1000;
    result.audit_retention_seconds = 0;
    return result;
}

int64_t scalar(database& db, const std::string& sql) {
    return std::get<int64_t>(db.query(sql).at(0).begin()->second);
}

int64_t flag(lattice_db& db) {
    return scalar(db.db(), "SELECT disabled FROM main._SyncControl WHERE id=1");
}

void expect_local_audit(lattice_db& db) {
    const auto row = db.add(AuditedPreparationRow{91});
    const auto audits = db.db().query(
        "SELECT operation, isFromRemote FROM AuditLog WHERE tableName='AuditedPreparationRow' "
        "AND rowId=? AND globalRowId=?", {row.id(), row.global_id()});
    ASSERT_EQ(audits.size(), 1u);
    EXPECT_EQ(std::get<std::string>(audits[0].at("operation")), "INSERT");
    EXPECT_EQ(std::get<int64_t>(audits[0].at("isFromRemote")), 0);
}

struct AuthorizerGuard {
    sqlite3* connection;
    ~AuthorizerGuard() { sqlite3_set_authorizer(connection, nullptr, nullptr); }
};

struct ApplyHookGuard {
    ~ApplyHookGuard() { test_hooks::apply_chunk_begin = {}; }
};

std::vector<audit_log_entry> remote_entries() {
    lattice_db source;
    source.add(AuditedPreparationRow{42});
    return events_after(source.db(), std::nullopt);
}
}

TEST(AuditedWritesPreparation, ExplicitOptInNormalizesLegacyFlagAndPersistsAcrossReopen) {
    PreparationFile file;
    {
        lattice_db db(preparation_config(file.path));
        db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        db.add(AuditedPreparationRow{1});
        EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);
    }
    {
        lattice_db db(preparation_config(file.path));
        EXPECT_EQ(flag(db), 1); // Ordinary opens retain the generic contract.
        EXPECT_TRUE(db.prepare_for_audited_writes());
        EXPECT_FALSE(db.prepare_for_audited_writes());
        EXPECT_EQ(flag(db), 0);
        db.begin_transaction();
        expect_local_audit(db);
        EXPECT_TRUE(db.insert_local_metadata_if_absent("receipt", "operation", "immutable"));
        db.commit();
        EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM AuditLog"), 1);
    }
    lattice_db reopened(preparation_config(file.path));
    EXPECT_EQ(flag(reopened), 0);
    EXPECT_EQ(reopened.local_metadata("receipt", "operation"), "immutable");
    EXPECT_EQ(scalar(reopened.db(), "SELECT COUNT(*) FROM AuditLog"), 1);
}

TEST(AuditedWritesPreparation, RejectsUnexpectedSchemaAndRowsWithoutRepairingThem) {
    const std::vector<std::string> corruptions = {
        "DROP TABLE _SyncControl",
        "DROP TABLE _SyncControl; CREATE VIEW _SyncControl AS SELECT 1 AS id, 1 AS disabled",
        "ALTER TABLE _SyncControl ADD COLUMN extra INTEGER",
        "DROP TABLE _SyncControl; CREATE TABLE _SyncControl(id INTEGER PRIMARY KEY CHECK(id=1), disabled TEXT NOT NULL DEFAULT 0); INSERT INTO _SyncControl VALUES(1,'1')",
        "DROP TABLE _SyncControl; CREATE TABLE _SyncControl(id INTEGERPRIMARYKEY CHECK(id=1), disabled INTEGER NOT NULL DEFAULT 0); INSERT INTO _SyncControl VALUES(1,1)",
        "CREATE TRIGGER unexpected_control_trigger AFTER UPDATE ON _SyncControl BEGIN SELECT 1; END",
        "DELETE FROM _SyncControl",
        "PRAGMA ignore_check_constraints=ON; UPDATE _SyncControl SET id=2",
        "PRAGMA ignore_check_constraints=ON; INSERT INTO _SyncControl VALUES(2,1)",
        "UPDATE _SyncControl SET disabled=2",
        "UPDATE _SyncControl SET disabled='invalid'",
        "CREATE TEMP TABLE _SyncControl(id INTEGER, disabled INTEGER); INSERT INTO temp._SyncControl VALUES(1,1)",
        "CREATE TEMP TRIGGER unexpected_temp_control_trigger AFTER UPDATE ON main._SyncControl BEGIN SELECT 1; END",
    };
    for (const auto& corruption : corruptions) {
        SCOPED_TRACE(corruption);
        lattice_db db;
        db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        db.db().execute(corruption);
        const auto schema = db.db().query("SELECT type,name,sql FROM main.sqlite_schema ORDER BY type,name");
        std::vector<database::row_t> control;
        const bool has_control = !db.db().query(
            "SELECT 1 FROM main.sqlite_schema WHERE name='_SyncControl'").empty();
        if (has_control) control = db.db().query("SELECT * FROM main._SyncControl");
        EXPECT_THROW(db.prepare_for_audited_writes(), db_error);
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_EQ(db.db().query("SELECT type,name,sql FROM main.sqlite_schema ORDER BY type,name"), schema);
        if (has_control) EXPECT_EQ(db.db().query("SELECT * FROM main._SyncControl"), control);
    }
}

TEST(AuditedWritesPreparation, RejectsReadOnlyAndExistingCallerTransaction) {
    PreparationFile file;
    lattice_db db(preparation_config(file.path));
    db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    db.begin_transaction();
    db.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('preparation-caller','pending')");
    EXPECT_THROW(db.prepare_for_audited_writes(), db_error);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(flag(db), 1);
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM _lattice_meta WHERE key='preparation-caller'"), 1);
    db.rollback();
    auto readonly = preparation_config(file.path);
    readonly.read_only = true;
    lattice_db reader(readonly);
    EXPECT_THROW(reader.prepare_for_audited_writes(), db_error);
    EXPECT_EQ(flag(db), 1);
}

TEST(AuditedWritesPreparation, FailedUpdateOrCommitRollsBackAndCanBeRetried) {
    for (bool deny_commit : {false, true}) {
        SCOPED_TRACE(deny_commit);
        lattice_db db;
        db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        struct Denial {
            bool commit;
            static int callback(void* context, int action, const char* first, const char*, const char*, const char*) {
                auto& self = *static_cast<Denial*>(context);
                if (self.commit && action == SQLITE_TRANSACTION && first && std::strcmp(first, "COMMIT") == 0)
                    return SQLITE_DENY;
                if (!self.commit && action == SQLITE_UPDATE && first && std::strcmp(first, "_SyncControl") == 0)
                    return SQLITE_DENY;
                return SQLITE_OK;
            }
        } denial{deny_commit};
        AuthorizerGuard guard{db.db().handle()};
        ASSERT_EQ(sqlite3_set_authorizer(guard.connection, Denial::callback, &denial), SQLITE_OK);
        EXPECT_THROW(db.prepare_for_audited_writes(), db_error);
        sqlite3_set_authorizer(guard.connection, nullptr, nullptr);
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_EQ(flag(db), 1);
        EXPECT_TRUE(db.prepare_for_audited_writes());
        expect_local_audit(db);
    }
}

TEST(AuditedWritesPreparation, ApplySamplesFlagAfterConcurrentPreparationBeforeBegin) {
    PreparationFile file;
    lattice_db receiver(preparation_config(file.path));
    lattice_db writer(preparation_config(file.path));
    receiver.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    const auto entries = remote_entries();
    bool prepared = false;
    ApplyHookGuard guard;
    test_hooks::apply_chunk_begin = [&](size_t) {
        if (!prepared) prepared = writer.prepare_for_audited_writes();
    };
    EXPECT_EQ(apply_remote_changes(receiver, entries).size(), entries.size());
    EXPECT_TRUE(prepared);
    EXPECT_EQ(flag(receiver), 0);
    expect_local_audit(receiver);
}

TEST(AuditedWritesPreparation, NoRelayDeleteSamplesFlagAfterConcurrentPreparationBeforeBegin) {
    PreparationFile file;
    lattice_db owner(preparation_config(file.path));
    lattice_db writer(preparation_config(file.path));
    const auto row = owner.add(AuditedPreparationRow{5});
    owner.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    struct Preparation {
        lattice_db* writer;
        bool attempted = false;
        bool changed = false;
        bool failed = false;
        static int callback(void* context, int action, const char* first, const char*, const char*, const char*) noexcept {
            auto& self = *static_cast<Preparation*>(context);
            if (!self.attempted && action == SQLITE_TRANSACTION && first && std::strcmp(first, "BEGIN") == 0) {
                self.attempted = true;
                try { self.changed = self.writer->prepare_for_audited_writes(); }
                catch (...) { self.failed = true; }
            }
            return SQLITE_OK;
        }
    } preparation{&writer};
    AuthorizerGuard guard{owner.db().handle()};
    ASSERT_EQ(sqlite3_set_authorizer(guard.connection, Preparation::callback, &preparation), SQLITE_OK);
    EXPECT_EQ(owner.delete_rows_no_relay("AuditedPreparationRow", {row.global_id()}), 1);
    sqlite3_set_authorizer(guard.connection, nullptr, nullptr);
    EXPECT_TRUE(preparation.attempted);
    EXPECT_TRUE(preparation.changed);
    EXPECT_FALSE(preparation.failed);
    EXPECT_EQ(flag(owner), 0);
    expect_local_audit(owner);
}

TEST(AuditedWritesPreparation, GenericApplyAndNoRelayDeletePreserveDeliberatelyDisabledAuditing) {
    lattice_db db;
    const auto row = db.add(AuditedPreparationRow{5});
    db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    const auto entries = remote_entries();
    EXPECT_EQ(apply_remote_changes(db, entries).size(), entries.size());
    EXPECT_EQ(flag(db), 1);
    EXPECT_EQ(db.delete_rows_no_relay("AuditedPreparationRow", {row.global_id()}), 1);
    EXPECT_EQ(flag(db), 1);
}

TEST(AuditedWritesPreparation, SuppressionCannotRollBackAnUnownedCallerTransaction) {
    lattice_db db;
    const auto row = db.add(AuditedPreparationRow{5});
    const auto entries = remote_entries();
    db.begin_transaction();
    db.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('suppression-caller','pending')");
    EXPECT_TRUE(apply_remote_changes(db, entries).empty());
    EXPECT_THROW(db.delete_rows_no_relay("AuditedPreparationRow", {row.global_id()}), db_error);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM _lattice_meta WHERE key='suppression-caller'"), 1);
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM AuditedPreparationRow"), 1);
    db.rollback();
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) FROM _lattice_meta WHERE key='suppression-caller'"), 0);
    EXPECT_EQ(flag(db), 0);
}
