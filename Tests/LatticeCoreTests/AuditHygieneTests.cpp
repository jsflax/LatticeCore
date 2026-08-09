#include "TestHelpers.hpp"
#include <lattice/sync.hpp>

// ============================================================================
// Audit-log hygiene — the Aug 2026 audit-explosion incident's red suite.
//
// A 28K-memory production hub reached 4.69M audit rows and an 11GB WAL
// through four compounding defects (fix-program increments 1-3):
//
//   1a. rebuild_table migrations copied every row through LIVE audit
//       triggers — ~218K full-payload audit rows per schema migration.
//   1b. Apply-minted audit rows carried TEXT ISO timestamps in the REAL
//       `timestamp` column — invisible to all date math.
//   2a. A channel whose filter matches nothing NEVER advances its
//       upload_floor (enumeration returns zero rows, floor logic runs on
//       enumerated rows only) — its slot then vetoes all compaction.
//   2b. safe_compact_audit_log keyed on MIN(confirmed_audit_id), which is
//       (a) zero forever for filtered-quiet channels and (b) a HOLEY
//       high-watermark (chunk-max of acked subsets) — using it as a
//       deletion bound either compacts nothing or deletes entries a
//       channel still needs. The contiguous frontier is upload_floor.
//
// The origin-mark pinning tests guard behavior that already works (apply
// marks the receiving sync_id synced; relay to OTHER channels stays
// pending) — any "fix" to the echo that suppresses the mint or the relay
// must fail them.
// ============================================================================

namespace {

int64_t audit_count(lattice::database& db, const std::string& table) {
    auto rows = db.query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE tableName = ?", {table});
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

int64_t max_audit_id(lattice::database& db) {
    auto rows = db.query("SELECT COALESCE(MAX(id), 0) AS m FROM AuditLog", {});
    return rows.empty() ? 0 : std::get<int64_t>(rows[0].at("m"));
}

} // namespace

// ---------------------------------------------------------------------------
// Increment 1a — schema migrations must mint ZERO audit rows.
// ---------------------------------------------------------------------------
TEST(AuditHygiene, RebuildTableMintsNoAuditRows) {
    TempDB tmp{"rebuild_audit"};

    // A database from an "older binary": extra column forces the reopen
    // below down the column-removed path -> rebuild_table full copy.
    {
        lattice::database old_db(tmp.str());
        old_db.execute(R"(
            CREATE TABLE TestPerson (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                email TEXT,
                legacy_field TEXT
            )
        )");
        for (int i = 0; i < 50; ++i) {
            old_db.execute(
                "INSERT INTO TestPerson (globalId, name, age, legacy_field) "
                "VALUES (?, ?, ?, 'old')",
                {std::string("rb-uuid-") + std::to_string(i),
                 std::string("P") + std::to_string(i), int64_t(30)});
        }
    }

    // Reopen with the current schema: legacy_field is gone -> rebuild.
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        auto persons = db.objects<TestPerson>();
        ASSERT_EQ(persons.size(), 50u) << "rebuild must preserve rows";

        // The copy is a schema-shape operation, not user writes: the audit
        // log must not record it. (Production: one 0.14.0 first-open minted
        // 218K rows this way; ~11 dev-loop passes minted 2.23M in a day.)
        EXPECT_EQ(audit_count(db.db(), "TestPerson"), 0)
            << "rebuild_table copied rows through live audit triggers";
    }
}

// ---------------------------------------------------------------------------
// Increment 1b — apply-minted audit rows carry REAL timestamps.
// ---------------------------------------------------------------------------
TEST(AuditHygiene, AppliedRemoteEntryHasRealTimestamp) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"apply_ts"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "test-sync";
    config.all_active_sync_ids = {"test-sync"};

    lattice::synchronizer sync(std::move(db), config);
    auto* mock_ws = mock_factory->last_websocket();
    sync.connect();

    lattice::audit_log_entry remote;
    remote.global_id = "ts-audit-1";
    remote.table_name = "TestPerson";
    remote.operation = "INSERT";
    remote.row_id = 1;
    remote.global_row_id = "ts-person-1";
    remote.changed_fields = {{"name", lattice::any_property("TsPerson")},
                             {"age", lattice::any_property(41)}};
    remote.changed_fields_names = {"name", "age"};
    remote.timestamp = "2026-08-09T12:00:00Z";

    mock_ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_audit_log({remote}).to_json()));

    // The echo row minted for the applied change (isFromRemote=1) must
    // store a numeric timestamp — TEXT in the REAL column falls out of all
    // date math (age-based compaction, timeline queries). 2.0M production
    // rows were invisible this way.
    auto rows = reader.db().query(
        "SELECT typeof(timestamp) AS t FROM AuditLog "
        "WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1", {});
    ASSERT_FALSE(rows.empty()) << "apply must mint a relay-pending echo row";
    EXPECT_EQ(std::get<std::string>(rows[0].at("t")), "real")
        << "apply wrote a TEXT timestamp into the REAL column";

    sync.disconnect();
}

// ---------------------------------------------------------------------------
// Pinning (already-correct behavior; guards against naive echo "fixes").
// ---------------------------------------------------------------------------
TEST(AuditHygiene, AppliedEntryOriginMarkedButRelaysToOtherChannels) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"apply_mark"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "origin-chan";
    // Two live channels: the origin and a relay target.
    config.all_active_sync_ids = {"origin-chan", "relay-chan"};

    lattice::synchronizer sync(std::move(db), config);
    auto* mock_ws = mock_factory->last_websocket();
    sync.connect();

    lattice::audit_log_entry remote;
    remote.global_id = "mark-audit-1";
    remote.table_name = "TestPerson";
    remote.operation = "INSERT";
    remote.row_id = 2;
    remote.global_row_id = "mark-person-1";
    remote.changed_fields = {{"name", lattice::any_property("MarkPerson")},
                             {"age", lattice::any_property(52)}};
    remote.changed_fields_names = {"name", "age"};
    remote.timestamp = "2026-08-09T12:00:00Z";

    mock_ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_audit_log({remote}).to_json()));

    auto echo = reader.db().query(
        "SELECT id FROM AuditLog WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1", {});
    ASSERT_FALSE(echo.empty());
    int64_t echo_id = std::get<int64_t>(echo[0].at("id"));

    // Origin-marked: the receiving channel never re-uploads what it applied.
    auto marked = reader.db().query(
        "SELECT is_synchronized FROM _lattice_sync_state "
        "WHERE audit_entry_id = ? AND sync_id = 'origin-chan'", {echo_id});
    ASSERT_FALSE(marked.empty()) << "apply must origin-mark the echo row";
    EXPECT_EQ(std::get<int64_t>(marked[0].at("is_synchronized")), 1);

    auto origin_pending = lattice::query_audit_log_for_sync(
        reader.db(), "origin-chan");
    for (const auto& e : origin_pending) {
        EXPECT_NE(e.global_id, "mark-audit-1")
            << "origin channel re-enumerated its own applied entry (echo!)";
    }

    // …but the RELAY channel must still see it (cross-transport relay).
    auto relay_pending = lattice::query_audit_log_for_sync(
        reader.db(), "relay-chan");
    bool relayed = false;
    for (const auto& e : relay_pending) {
        if (e.table_name == "TestPerson" && e.global_row_id == "mark-person-1") {
            relayed = true;
        }
    }
    EXPECT_TRUE(relayed)
        << "applied entries must stay pending for OTHER channels — a fix "
           "that suppresses the echo mint breaks hub->group relay";

    sync.disconnect();
}

// ---------------------------------------------------------------------------
// Increment 2a — a filtered-to-nothing channel advances its floor on an
// empty pass instead of pinning history forever.
// ---------------------------------------------------------------------------
TEST(AuditHygiene, EmptyPassAdvancesUploadFloor) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"empty_floor"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    // Local writes: real audit entries this channel's filter will exclude.
    db->add(TestPerson{"FloorPerson1", 30, std::nullopt});
    db->add(TestPerson{"FloorPerson2", 31, std::nullopt});
    const int64_t max_id = max_audit_id(reader.db());
    ASSERT_GT(max_id, 0);

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "filtered-chan";
    config.all_active_sync_ids = {"filtered-chan"};
    // Real table, never-true predicate — the production group-channel shape
    // (exposedTeams empty => the filter's WHERE matches nothing).
    config.sync_filter = {{lattice::sync_filter_entry{"TestPerson", "1 = 0"}}};

    lattice::synchronizer sync(std::move(db), config);
    sync.connect();
    sync.sync_now();

    // The pass enumerated nothing and had nothing in flight: the floor must
    // advance to the scan horizon, or this slot pins compaction at 0
    // forever (production: two group slots at confirmed=0/floor-stuck
    // vetoed collapse of 4.67M rows).
    EXPECT_GE(lattice::read_upload_floor(reader.db(), "filtered-chan"), max_id)
        << "empty pass left the floor behind — this channel vetoes compaction";

    sync.disconnect();
}

// ---------------------------------------------------------------------------
// Widening a filter must re-expose entries the empty-pass advance skipped.
//
// The floor is a SCAN BOUND, so advancing it past rows that were excluded
// only by the filter makes them unreachable — and reconcile's Phase-2 dedup
// deliberately does not re-synthesize rows whose INSERT entries are still
// pending. Without the rewind, exposing a project to a group relays nothing
// (caught by Engram's unexposedGroupGetsNothingRatherThanEverything, which
// exists precisely because an empty spoke is indistinguishable from a relay
// that never started).
// ---------------------------------------------------------------------------
TEST(AuditHygiene, WideningFilterReExposesSkippedEntries) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"filter_widen"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    db->add(TestPerson{"WidenPerson", 33, std::nullopt});

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "widen-chan";
    config.all_active_sync_ids = {"widen-chan"};
    config.sync_filter = {{lattice::sync_filter_entry{"TestPerson", "1 = 0"}}};

    lattice::synchronizer sync(std::move(db), config);
    sync.connect();
    sync.sync_now();  // empty pass -> floor advances past the row

    ASSERT_GT(lattice::read_upload_floor(reader.db(), "widen-chan"), 0)
        << "fixture requires the empty-pass advance to have run";

    // Widen: the row now matches. It must become enumerable again.
    sync.update_sync_filter({lattice::sync_filter_entry{"TestPerson", std::nullopt}});
    for (int i = 0; i < 100; ++i) {
        if (!lattice::query_audit_log_for_sync(reader.db(), "widen-chan").empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    auto pending = lattice::query_audit_log_for_sync(reader.db(), "widen-chan");
    EXPECT_FALSE(pending.empty())
        << "widening the filter left previously-skipped entries below the floor "
           "— they can never be sent";

    sync.disconnect();
}

// ---------------------------------------------------------------------------
// Increment 2b — compaction keys on the contiguous upload_floor, never the
// holey confirmed_audit_id.
// ---------------------------------------------------------------------------
TEST(AuditHygiene, SafeCompactUsesFloorNotHoleyConfirmed) {
    TempDB tmp{"compact_floor"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    // Ten real audit entries (sync-visible writes).
    for (int i = 0; i < 10; ++i) {
        db.add(TestPerson{std::string("C") + std::to_string(i), 20 + i, std::nullopt});
    }
    ASSERT_EQ(audit_count(db.db(), "TestPerson"), 10);
    const int64_t maxid = max_audit_id(db.db());

    // One channel: a partial apply acked a SUBSET whose chunk-max advanced
    // confirmed to maxid, while ids (maxid-4..maxid-1) were never durably
    // processed — confirmed is a holey high-watermark. The contiguous
    // frontier (floor) sits 5 back.
    db.db().execute(
        "INSERT INTO _lattice_replication_slots "
        "(sync_id, confirmed_audit_id, upload_floor, last_active_at) "
        "VALUES ('holey-chan', ?, ?, datetime('now'))",
        {maxid, maxid - 5});

    int64_t deleted = db.safe_compact_audit_log();
    (void)deleted;

    // Entries above the floor must SURVIVE — they are still pending for the
    // channel even though confirmed jumped past them. Compacting to
    // confirmed would delete un-uploaded history (unrecoverable: pending
    // enumeration needs the AuditLog row).
    auto survivors = db.db().query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE id > ?", {maxid - 5});
    ASSERT_FALSE(survivors.empty());
    EXPECT_EQ(std::get<int64_t>(survivors[0].at("c")), 5)
        << "compaction keyed on holey confirmed_audit_id deleted pending entries";

    // And entries at/below the floor are gone (compaction still works).
    auto compacted = db.db().query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE id <= ?", {maxid - 5});
    ASSERT_FALSE(compacted.empty());
    EXPECT_EQ(std::get<int64_t>(compacted[0].at("c")), 0)
        << "floor-covered entries must compact";
}

// ---------------------------------------------------------------------------
// Compaction preserves the WSS download-resume cursor.
// ---------------------------------------------------------------------------
TEST(AuditHygiene, SafeCompactPreservesDownloadCursorRow) {
    TempDB tmp{"compact_cursor"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    // Local writes, then a "downloaded" row (the newest isFromRemote row is
    // the ?last-event-id resume cursor), then more local writes.
    for (int i = 0; i < 3; ++i) {
        db.add(TestPerson{std::string("D") + std::to_string(i), 20 + i, std::nullopt});
    }
    db.db().execute(
        "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
        " changedFields, changedFieldsNames, timestamp, isFromRemote, isSynchronized) "
        "VALUES ('cursor-row-gid', 'TestPerson', 'INSERT', 99, 'cursor-person', "
        " '{}', '[]', unixepoch('subsec'), 1, 0)");
    for (int i = 3; i < 6; ++i) {
        db.add(TestPerson{std::string("D") + std::to_string(i), 20 + i, std::nullopt});
    }
    const int64_t maxid = max_audit_id(db.db());

    // A slot whose floor covers EVERYTHING — without cursor preservation,
    // compaction would delete the isFromRemote row and the next WSS connect
    // (which resumes from the newest isFromRemote globalId) would
    // re-download the peer's entire history.
    db.db().execute(
        "INSERT INTO _lattice_replication_slots "
        "(sync_id, confirmed_audit_id, upload_floor, last_active_at) "
        "VALUES ('full-chan', ?, ?, datetime('now'))",
        {maxid, maxid});

    db.safe_compact_audit_log();

    auto cursor = db.db().query(
        "SELECT globalId FROM AuditLog WHERE isFromRemote = 1 ORDER BY id DESC LIMIT 1", {});
    ASSERT_FALSE(cursor.empty())
        << "compaction deleted the download-resume cursor row";
    EXPECT_EQ(std::get<std::string>(cursor[0].at("globalId")), "cursor-row-gid");

    // Everything else below the floor is gone.
    auto rest = db.db().query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE isFromRemote = 0", {});
    ASSERT_FALSE(rest.empty());
    EXPECT_EQ(std::get<int64_t>(rest[0].at("c")), 0);
}
