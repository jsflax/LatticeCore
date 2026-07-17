#include "TestHelpers.hpp"

// ============================================================================
// Multi-channel sync set tests (A1: per-sync_id _lattice_sync_set)
//
// Two filtered synchronizers on one database must keep fully independent
// membership state. With the old shared-set shape, channel A's classify saw
// channel B's rows in the set and synthesized real DELETEs for them
// (`!matches && in_set`), and reconcile Phase 1 emitted filter-removal
// DELETEs for the other channel's rows — the cross-channel mirror-wipe
// mechanism behind the March data-loss incident class.
// ============================================================================

namespace {

int64_t count_rows(lattice::lattice_db& db, const std::string& sql) {
    auto rows = db.db().query(sql);
    if (rows.empty()) return -1;
    auto& val = rows[0].begin()->second;
    return std::holds_alternative<int64_t>(val) ? std::get<int64_t>(val) : -1;
}

std::string global_id_of(lattice::lattice_db& db, const std::string& name) {
    auto rows = db.db().query(
        "SELECT globalId FROM TestPerson WHERE name = ?", {name});
    if (rows.empty()) return "";
    auto it = rows[0].find("globalId");
    return it != rows[0].end() && std::holds_alternative<std::string>(it->second)
        ? std::get<std::string>(it->second) : "";
}

} // namespace

// ----------------------------------------------------------------------------
// CRUD isolation: two filtered channels populate disjoint membership and
// never synthesize deletes for each other's rows.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, PerSyncIdCrudIsolation) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"mcss_crud"};

    // Writer creates the table + two rows before any synchronizer exists.
    auto writer = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    writer->add(TestPerson{"Alice", 30, std::nullopt});
    writer->add(TestPerson{"Bob", 60, std::nullopt});
    lattice::lattice_db reader(lattice::configuration(tmp.str()));
    const std::string alice_gid = global_id_of(reader, "Alice");
    const std::string bob_gid = global_id_of(reader, "Bob");
    ASSERT_FALSE(alice_gid.empty());
    ASSERT_FALSE(bob_gid.empty());

    // Channel A: only under-50s.
    lattice::sync_config cfg_a;
    cfg_a.websocket_url = "ws://localhost:8080/sync";
    cfg_a.authorization_token = "t";
    cfg_a.sync_id = "chan-a";
    cfg_a.all_active_sync_ids = {"chan-a", "chan-b"};
    cfg_a.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("age < 50")}};
    lattice::synchronizer sync_a(std::move(writer), cfg_a);
    auto* ws_a = mock_factory->last_websocket();
    ASSERT_NE(ws_a, nullptr);
    sync_a.connect();
    sync_a.sync_now();

    // Channel B: only 50-and-overs. Fresh handle on the same file.
    auto db_b = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::sync_config cfg_b = cfg_a;
    cfg_b.sync_id = "chan-b";
    cfg_b.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("age >= 50")}};
    lattice::synchronizer sync_b(std::move(db_b), cfg_b);
    auto* ws_b = mock_factory->last_websocket();
    ASSERT_NE(ws_b, nullptr);
    ASSERT_NE(ws_a, ws_b);
    sync_b.connect();
    sync_b.sync_now();

    // Membership is disjoint and correctly attributed.
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set"
        " WHERE sync_id = 'chan-a' AND global_row_id = '" + alice_gid + "'"), 1);
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set"
        " WHERE sync_id = 'chan-b' AND global_row_id = '" + bob_gid + "'"), 1);
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set"
        " WHERE sync_id = 'chan-a' AND global_row_id = '" + bob_gid + "'"), 0);
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set"
        " WHERE sync_id = 'chan-b' AND global_row_id = '" + alice_gid + "'"), 0);

    // THE regression assertion: neither channel synthesized a DELETE for the
    // other channel's row (the old shared-set `!matches && in_set` branch).
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM AuditLog WHERE operation = 'DELETE'"), 0);
    for (const auto& msg : ws_a->get_sent_messages()) {
        EXPECT_EQ(msg.as_string().find("\"DELETE\""), std::string::npos)
            << "channel A sent a DELETE frame";
    }
    for (const auto& msg : ws_b->get_sent_messages()) {
        EXPECT_EQ(msg.as_string().find("\"DELETE\""), std::string::npos)
            << "channel B sent a DELETE frame";
    }

    sync_a.disconnect();
    sync_b.disconnect();
}

// ----------------------------------------------------------------------------
// Reconcile Phase 1 must only consider THIS channel's membership rows:
// another channel's rows must not produce filter-removal DELETEs.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, ReconcileRemovalsScopedToChannel) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"mcss_reconcile"};

    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    db->add(TestPerson{"Carol", 40, std::nullopt});
    lattice::lattice_db reader(lattice::configuration(tmp.str()));
    const std::string carol_gid = global_id_of(reader, "Carol");
    ASSERT_FALSE(carol_gid.empty());

    // Seed membership for a DIFFERENT channel ("chan-b") directly.
    reader.db().execute(
        "INSERT INTO _lattice_sync_set (sync_id, table_name, global_row_id)"
        " VALUES ('chan-b', 'TestPerson', ?)", {carol_gid});

    // Channel A's filter matches nothing — with the shared-set shape its
    // Phase 1 would have seen chan-b's row and emitted a filter-removal
    // DELETE for it.
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "t";
    cfg.sync_id = "chan-a";
    cfg.all_active_sync_ids = {"chan-a", "chan-b"};
    cfg.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("name = 'NO_SUCH_ROW'")}};
    lattice::synchronizer sync(std::move(db), cfg);
    sync.connect();   // on_websocket_open runs reconcile_sync_filter
    sync.sync_now();

    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM AuditLog"
        " WHERE changedFieldsNames LIKE '%__lattice_filter_removal%'"), 0)
        << "channel A emitted a filter-removal DELETE for channel B's row";
    EXPECT_EQ(count_rows(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'chan-b'"), 1)
        << "channel B's membership row was clobbered";

    sync.disconnect();
}

// ----------------------------------------------------------------------------
// Migration: old 2-column shape rebuilds to per-sync_id. Exactly one
// registered replication slot → rows attributed to it.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, MigrationSingleSlotAttribution) {
    TempDB tmp{"mcss_mig1"};
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        // Surgery: recreate the PRE-A1 shape with live rows + one slot, and
        // clear the schema fingerprint so the reopen takes the ensure path
        // (real old DBs carry an epoch-3 fingerprint, which mismatches too).
        db.db().execute("DROP TABLE _lattice_sync_set");
        db.db().execute(
            "CREATE TABLE _lattice_sync_set ("
            " table_name TEXT NOT NULL, global_row_id TEXT NOT NULL,"
            " PRIMARY KEY (table_name, global_row_id))");
        db.db().execute(
            "INSERT INTO _lattice_sync_set VALUES ('TestPerson', 'gid-1'), ('TestPerson', 'gid-2')");
        db.db().execute(
            "INSERT OR IGNORE INTO _lattice_replication_slots (sync_id) VALUES ('ipc:solo')");
        db.db().execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
    }
    lattice::lattice_db reopened{lattice::configuration(tmp.str())};
    EXPECT_EQ(count_rows(reopened,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'ipc:solo'"), 2);
    EXPECT_EQ(count_rows(reopened, "SELECT COUNT(*) FROM _lattice_sync_set"), 2);
    // Leftover staging table must be gone.
    EXPECT_EQ(count_rows(reopened,
        "SELECT COUNT(*) FROM sqlite_master WHERE name = '_lattice_sync_set_v1'"), 0);
}

// ----------------------------------------------------------------------------
// Migration: multiple slots → attribution is ambiguous → rows drop (Phase 2
// re-synthesizes membership idempotently).
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, MigrationMultiSlotDrops) {
    TempDB tmp{"mcss_mig2"};
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        db.db().execute("DROP TABLE _lattice_sync_set");
        db.db().execute(
            "CREATE TABLE _lattice_sync_set ("
            " table_name TEXT NOT NULL, global_row_id TEXT NOT NULL,"
            " PRIMARY KEY (table_name, global_row_id))");
        db.db().execute("INSERT INTO _lattice_sync_set VALUES ('TestPerson', 'gid-1')");
        db.db().execute(
            "INSERT OR IGNORE INTO _lattice_replication_slots (sync_id)"
            " VALUES ('ipc:one'), ('wss:two')");
        db.db().execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
    }
    lattice::lattice_db reopened{lattice::configuration(tmp.str())};
    EXPECT_EQ(count_rows(reopened, "SELECT COUNT(*) FROM _lattice_sync_set"), 0);
    // New shape in place (sync_id column exists → this INSERT succeeds).
    reopened.db().execute(
        "INSERT INTO _lattice_sync_set (sync_id, table_name, global_row_id)"
        " VALUES ('x', 'T', 'g')");
    EXPECT_EQ(count_rows(reopened, "SELECT COUNT(*) FROM _lattice_sync_set"), 1);
}

// ----------------------------------------------------------------------------
// A6: the eager-collapse count must ignore sync_state rows from channels
// with no live replication slot — stale confirmations from a removed
// channel must not collapse an entry before a live channel relays it.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, CollapseCountIgnoresDeadChannels) {
    TempDB tmp{"mcss_collapse"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Relay", 20, std::nullopt});

    auto audit = db.db().query(
        "SELECT globalId FROM AuditLog WHERE operation = 'INSERT' LIMIT 1");
    ASSERT_EQ(audit.size(), 1u);
    const std::string entry_gid = std::get<std::string>(audit[0].at("globalId"));
    auto id_rows = db.db().query(
        "SELECT id FROM AuditLog WHERE globalId = ?", {entry_gid});
    const int64_t entry_id = std::get<int64_t>(id_rows[0].at("id"));

    // Two LIVE channels (slots registered): "live-a" (about to confirm) and
    // "live-b" (has NOT relayed yet). Plus a stale confirmation from a DEAD
    // channel "dead-z" with no slot.
    db.db().execute(
        "INSERT OR IGNORE INTO _lattice_replication_slots (sync_id) VALUES ('live-a'), ('live-b')");
    db.db().execute(
        "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized)"
        " VALUES (?, 'dead-z', 1)", {entry_id});

    lattice::mark_audit_entries_synced_for(db, {entry_gid}, "live-a",
                                           {"live-a", "live-b"});

    EXPECT_EQ(count_rows(db,
        "SELECT isSynchronized FROM AuditLog WHERE id = " + std::to_string(entry_id)), 0)
        << "dead-z's stale confirmation must not collapse the entry — "
           "live-b has not relayed it (silent relay loss)";
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_state WHERE audit_entry_id = "
        + std::to_string(entry_id)), 2)
        << "sync_state rows (dead-z's stale + live-a's fresh; live-b's absence"
           " = pending) must survive until every LIVE channel confirms";

    // When live-b also confirms, the entry collapses despite dead-z.
    lattice::mark_audit_entries_synced_for(db, {entry_gid}, "live-b",
                                           {"live-a", "live-b"});
    EXPECT_EQ(count_rows(db,
        "SELECT isSynchronized FROM AuditLog WHERE id = " + std::to_string(entry_id)), 1);
}

// ----------------------------------------------------------------------------
// A6: remove_sync_channel_state retires a channel completely (state, set,
// slot) and leaves other channels untouched.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, RemoveSyncChannelState) {
    TempDB tmp{"mcss_retire"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.db().execute(
        "INSERT INTO _lattice_sync_set (sync_id, table_name, global_row_id)"
        " VALUES ('gone', 'T', 'g1'), ('stays', 'T', 'g1')");
    db.db().execute(
        "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized)"
        " VALUES (1, 'gone', 1), (1, 'stays', 0)");
    db.db().execute(
        "INSERT OR IGNORE INTO _lattice_replication_slots (sync_id) VALUES ('gone'), ('stays')");

    db.remove_sync_channel_state("gone");

    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'gone'"), 0);
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id = 'gone'"), 0);
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_replication_slots WHERE sync_id = 'gone'"), 0)
        << "the dead slot must go too — it pins safe compaction forever";
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'stays'"), 1);
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id = 'stays'"), 1);
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_replication_slots WHERE sync_id = 'stays'"), 1);
}

// ----------------------------------------------------------------------------
// Audit-trigger self-heal: a hand-installed `WHEN (0)` stub squatting an
// audit trigger's name (the Jul 2026 six-day silent Memory-sync outage) is
// detected and replaced with the real trigger on the next ensure pass.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, AuditTriggerSelfHeal) {
    TempDB tmp{"trigger_heal"};
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        db.add(TestPerson{"Before", 1, std::nullopt});
        // Tamper exactly like the production incident: neutered stubs that
        // name-squat so CREATE IF NOT EXISTS can't restore the real ones.
        db.db().execute("DROP TRIGGER AuditTestPersonInsert");
        db.db().execute(
            "CREATE TRIGGER AuditTestPersonInsert AFTER INSERT ON TestPerson"
            " WHEN (0) BEGIN SELECT 1; END");
        db.db().execute("DROP TRIGGER IF EXISTS AuditLog_Update_TestPerson");
    }
    // Tampering bumped the schema cookie → reopen takes the slow ensure path.
    lattice::lattice_db healed{lattice::configuration(tmp.str())};
    auto sql_rows = healed.db().query(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='AuditTestPersonInsert'");
    ASSERT_EQ(sql_rows.size(), 1u);
    EXPECT_NE(std::get<std::string>(sql_rows[0].at("sql")).find("sync_disabled"),
              std::string::npos) << "the stub must be replaced by the real trigger";

    healed.add(TestPerson{"After", 2, std::nullopt});
    auto audited = healed.db().query(
        "SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='TestPerson'"
        " AND operation='INSERT'");
    EXPECT_GE(std::get<int64_t>(audited[0].at("n")), 1)
        << "writes after the heal must audit again";
}

// ----------------------------------------------------------------------------
// reset_sync_state wipes only the named channel's membership.
// ----------------------------------------------------------------------------
TEST(MultiChannelSyncSet, ResetSyncStateScoped) {
    TempDB tmp{"mcss_reset"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.db().execute(
        "INSERT INTO _lattice_sync_set (sync_id, table_name, global_row_id) VALUES"
        " ('chan-a', 'T', 'g1'), ('chan-a', 'T', 'g2'), ('chan-b', 'T', 'g1')");
    db.reset_sync_state("chan-a");
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'chan-a'"), 0);
    EXPECT_EQ(count_rows(db,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'chan-b'"), 1);
}
