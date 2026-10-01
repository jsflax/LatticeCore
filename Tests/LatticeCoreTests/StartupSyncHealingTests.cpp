#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <cstring>

namespace {
lattice::configuration healing_config(const TempDB& file) {
    lattice::configuration config(file.str());
    config.audit_retention_seconds = 0;
    return config;
}
int64_t value(lattice::lattice_db& db, const std::string& sql) {
    return std::get<int64_t>(db.db().query(sql).at(0).at("n"));
}
int64_t original(lattice::lattice_db& db) {
    db.add(TestPerson{"retained intent", 24, std::nullopt});
    return value(db, "SELECT MAX(id) AS n FROM AuditLog");
}
void slot(lattice::lattice_db& db, const std::string& channel) {
    lattice::register_replication_slot(db.db(), channel);
}
void ack(lattice::lattice_db& db, int64_t id, const std::string& channel, int64_t accepted = 1) {
    db.db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,?)",
                    {id, channel, accepted});
}
int64_t synchronized(lattice::lattice_db& db, int64_t id) {
    return value(db, "SELECT isSynchronized AS n FROM AuditLog WHERE id=" + std::to_string(id));
}
int64_t state_count(lattice::lattice_db& db, int64_t id) {
    return value(db, "SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE audit_entry_id=" + std::to_string(id));
}
void expect_pending(lattice::lattice_db& db, int64_t id, const std::string& channel) {
    const auto pending = lattice::query_audit_log_for_sync(db.db(), channel);
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending.front().id, id);
    EXPECT_EQ(pending.front().table_name, "TestPerson");
}
}

TEST(StartupSyncHealing, RemovedChannelAckCannotReplaceLiveChannelAck) {
    TempDB file{"startup_heal_removed"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db);
    slot(db, "live");
    ack(db, id, "retired");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 1);
    expect_pending(db, id, "live");
    EXPECT_FALSE(db.db().is_in_transaction());
}

TEST(StartupSyncHealing, StalePlusOneLiveAckDoesNotCompleteTwoLiveChannels) {
    TempDB file{"startup_heal_partial"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db);
    slot(db, "live-a"); slot(db, "live-b");
    ack(db, id, "live-a"); ack(db, id, "retired");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 2);
    expect_pending(db, id, "live-b");
    EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(), "live-a").empty());
}

TEST(StartupSyncHealing, AllLiveAcksCollapseAndClearRetiredStateToo) {
    TempDB file{"startup_heal_complete"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db);
    slot(db, "live-a"); slot(db, "live-b");
    ack(db, id, "live-a"); ack(db, id, "live-b"); ack(db, id, "retired");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 1);
    EXPECT_EQ(state_count(db, id), 0);
    EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(), "live-a").empty());
    EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(), "live-b").empty());
    EXPECT_EQ(value(db, "SELECT COUNT(*) AS n FROM TestPerson WHERE name='retained intent'"), 1);
}

TEST(StartupSyncHealing, NoRegisteredSlotsDoesNotDiscardUnresolvedEvidence) {
    TempDB file{"startup_heal_no_slots"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db);
    ack(db, id, "retired");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 1);
    EXPECT_FALSE(db.db().is_in_transaction());
}

TEST(StartupSyncHealing, ActualReopenPreservesWorkForUnacknowledgedLiveSlot) {
    TempDB file{"startup_heal_reopen"};
    int64_t id = 0;
    {
        lattice::lattice_db db{healing_config(file)};
        id = original(db); slot(db, "live"); ack(db, id, "retired");
    }
    lattice::lattice_db reopened{healing_config(file)};
    EXPECT_EQ(synchronized(reopened, id), 0);
    EXPECT_EQ(state_count(reopened, id), 1);
    expect_pending(reopened, id, "live");
}

TEST(StartupSyncHealing, CleanupFailureRollsBackFlagAndStateThenRetrySucceeds) {
    TempDB file{"startup_heal_rollback"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db); slot(db, "live"); ack(db, id, "live");
    db.db().execute("CREATE TRIGGER fail_heal_cleanup BEFORE DELETE ON _lattice_sync_state BEGIN SELECT RAISE(ABORT,'healing cleanup refused'); END");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 1);
    EXPECT_FALSE(db.db().is_in_transaction());
    db.db().execute("DROP TRIGGER fail_heal_cleanup");
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 1);
    EXPECT_EQ(state_count(db, id), 0);
}

TEST(StartupSyncHealing, CommitRefusalRollsBackBothWritesThenRetrySucceeds) {
    TempDB file{"startup_heal_commit"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db); slot(db, "live"); ack(db, id, "live");
    struct deny_commit {
        sqlite3* handle;
        explicit deny_commit(sqlite3* h) : handle(h) {
            EXPECT_EQ(sqlite3_set_authorizer(handle, [](void*, int action, const char* one,
                const char*, const char*, const char*) noexcept -> int {
                return action == SQLITE_TRANSACTION && one && std::strcmp(one, "COMMIT") == 0
                    ? SQLITE_DENY : SQLITE_OK;
            }, nullptr), SQLITE_OK);
        }
        ~deny_commit() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    };
    { deny_commit refused(db.db().handle()); db.heal_collapsed_sync_state(); }
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 1);
    EXPECT_FALSE(db.db().is_in_transaction());
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 1);
    EXPECT_EQ(state_count(db, id), 0);
}

TEST(StartupSyncHealing, ExistingCallerTransactionIsNeverSettledByHealing) {
    TempDB file{"startup_heal_owner"};
    lattice::lattice_db db{healing_config(file)};
    const auto id = original(db); slot(db, "live"); ack(db, id, "live");
    db.db().begin_transaction();
    db.db().execute("UPDATE TestPerson SET name='caller temporary value'");
    db.heal_collapsed_sync_state();
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(synchronized(db, id), 0);
    EXPECT_EQ(state_count(db, id), 1);
    EXPECT_EQ(value(db, "SELECT COUNT(*) AS n FROM TestPerson WHERE name='caller temporary value'"), 1);
    db.db().rollback();
    EXPECT_EQ(value(db, "SELECT COUNT(*) AS n FROM TestPerson WHERE name='retained intent'"), 1);
    db.heal_collapsed_sync_state();
    EXPECT_EQ(synchronized(db, id), 1);
    EXPECT_EQ(state_count(db, id), 0);
}
