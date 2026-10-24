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

// ===========================================================================
// B2 pinning — no-op suppression scope, receipts, cursor, catch-up fallback.
// ===========================================================================

// Absent-row UPDATE keeps minting in per-sync mode: absence can be a
// filter-removal value-hole with the row live downstream (hub) — today's
// phantom relay row is what carries the edit onward. Suppressing it would
// permanently diverge hub vs server (red-team blocker #1 against naive B2).
TEST(AuditHygiene, AbsentRowUpdateStillMintsRelayRow) {
    TempDB tmp{"b2_valuehole"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    lattice::audit_log_entry up;
    up.global_id = "vh-audit-1";
    up.table_name = "TestPerson";
    up.operation = "UPDATE";
    up.global_row_id = "vh-person-absent";
    up.changed_fields = {{"name", lattice::any_property(std::string("Ghost"))}};
    up.changed_fields_names = {"name"};
    up.timestamp = "2026-08-10T12:00:00Z";

    auto acked = lattice::apply_remote_changes_for(db, {up}, "chan-a");
    ASSERT_EQ(acked.size(), 1u) << "absent-row UPDATE must still be acked";

    auto minted = db.db().query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE globalId = 'vh-audit-1'", {});
    EXPECT_EQ(std::get<int64_t>(minted[0].at("c")), 1)
        << "absent-row UPDATE must mint the phantom relay row (value-hole)";
}

// A suppressed entry re-delivered AFTER an intervening local edit must be
// dedup'd via its RECEIPT — not re-applied over the newer local value
// (red-team: suppression deletes the AuditLog row the dedup keyed on).
TEST(AuditHygiene, SuppressedEntryRedeliveryDoesNotRevertLocalEdit) {
    TempDB tmp{"b2_receipt"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    db.add(TestPerson{"ReceiptP", 40, std::nullopt});
    auto row = db.db().query("SELECT globalId FROM TestPerson LIMIT 1", {});
    const auto row_gid = std::get<std::string>(row[0].at("globalId"));

    // Remote UPDATE carrying the CURRENT value — a genuine no-op: suppressed.
    lattice::audit_log_entry noop;
    noop.global_id = "rcpt-audit-1";
    noop.table_name = "TestPerson";
    noop.operation = "UPDATE";
    noop.global_row_id = row_gid;
    noop.changed_fields = {{"name", lattice::any_property(std::string("ReceiptP"))}};
    noop.changed_fields_names = {"name"};
    noop.timestamp = "2026-08-10T12:00:00Z";

    auto acked1 = lattice::apply_remote_changes_for(db, {noop}, "chan-a");
    ASSERT_EQ(acked1.size(), 1u);
    auto minted = db.db().query(
        "SELECT COUNT(*) AS c FROM AuditLog WHERE globalId = 'rcpt-audit-1'", {});
    ASSERT_EQ(std::get<int64_t>(minted[0].at("c")), 0) << "no-op must be suppressed";

    // Intervening LOCAL edit.
    db.db().execute("UPDATE TestPerson SET name = 'EditedLocally' WHERE globalId = ?",
                    {row_gid});

    // The SAME entry re-delivered (at-least-once transport) must hit the
    // receipt and be acked WITHOUT re-applying the stale value.
    auto acked2 = lattice::apply_remote_changes_for(db, {noop}, "chan-a");
    ASSERT_EQ(acked2.size(), 1u) << "re-delivery must still be acked";
    auto name = db.db().query(
        "SELECT name FROM TestPerson WHERE globalId = ?", {row_gid});
    EXPECT_EQ(std::get<std::string>(name[0].at("name")), "EditedLocally")
        << "receipt-less re-delivery re-applied a stale value over a local edit";
}

// A fully-suppressed (zero-mint) delivery must still advance the download
// cursor, or reconnect re-downloads the whole no-op tail forever.
TEST(AuditHygiene, NoopOnlyChunkAdvancesDownloadCursor) {
    TempDB tmp{"b2_cursor"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    lattice::ensure_cursor_column(db.db());
    lattice::register_replication_slot(db.db(), "chan-a");

    db.add(TestPerson{"CursorP", 41, std::nullopt});
    auto row = db.db().query("SELECT globalId FROM TestPerson LIMIT 1", {});
    const auto row_gid = std::get<std::string>(row[0].at("globalId"));

    lattice::audit_log_entry noop;
    noop.global_id = "cur-audit-1";
    noop.table_name = "TestPerson";
    noop.operation = "UPDATE";
    noop.global_row_id = row_gid;
    noop.changed_fields = {{"name", lattice::any_property(std::string("CursorP"))}};
    noop.changed_fields_names = {"name"};
    noop.timestamp = "2026-08-10T12:00:00Z";

    auto acked = lattice::apply_remote_changes_for(db, {noop}, "chan-a");
    ASSERT_EQ(acked.size(), 1u);

    auto cursor = db.db().query(
        "SELECT last_received_event_id AS c FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_FALSE(cursor.empty());
    EXPECT_TRUE(std::holds_alternative<std::string>(cursor[0].at("c")) &&
                std::get<std::string>(cursor[0].at("c")) == "cur-audit-1")
        << "zero-mint chunk left the download cursor behind";
}

// Catch-up with an UNKNOWN checkpoint must serve full history, not silence:
// "id > (SELECT id ... )" over a compacted-away globalId matched nothing and
// the client diverged forever (red-team blocker; required for B6 server
// compaction independently of B2).
TEST(AuditHygiene, UnknownCheckpointServesFullHistory) {
    TempDB tmp{"b2_checkpoint"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    db.add(TestPerson{"Ck1", 30, std::nullopt});
    db.add(TestPerson{"Ck2", 31, std::nullopt});

    auto known = lattice::events_after(db.db(), std::nullopt);
    ASSERT_GE(known.size(), 2u);

    auto after_unknown = lattice::events_after(
        db.db(), std::optional<std::string>("gone-with-the-compaction"));
    EXPECT_EQ(after_unknown.size(), known.size())
        << "unknown checkpoint must fall back to full history, not empty";
}

// ===========================================================================
// H3 (Aug 13 2026) — chunk-failure containment: acks survive a failed chunk.
//
// A chunk-level failure (BEGIN lock timeout, COMMIT busy) used to unwind out
// of apply_remote_changes, destroying the applied_ids of EARLIER, already-
// COMMITTED chunks — durably applied entries were acked to nobody, so the
// sender re-sent the whole window forever (spoke→hub frozen at
// progress_acked=215008, four windows cycling at their 300s backoff caps).
// ===========================================================================

namespace {

std::vector<lattice::audit_log_entry> h3_insert_entries(size_t n) {
    std::vector<lattice::audit_log_entry> entries;
    entries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        lattice::audit_log_entry e;
        e.global_id = "h3-audit-" + std::to_string(i);
        e.table_name = "TestPerson";
        e.operation = "INSERT";
        e.global_row_id = "h3-person-" + std::to_string(i);
        e.changed_fields = {
            {"name", lattice::any_property(std::string("P") + std::to_string(i))},
            {"age", lattice::any_property(int64_t(20 + (i % 50)))}};
        e.changed_fields_names = {"name", "age"};
        e.timestamp = "2026-08-13T12:00:00Z";
        entries.push_back(std::move(e));
    }
    return entries;
}

// RAII reset: a failing assertion must not leave the hook installed for
// every later test in the binary.
struct ChunkHookGuard {
    ~ChunkHookGuard() { lattice::test_hooks::apply_chunk_begin = nullptr; }
};

}  // namespace

TEST(AuditHygiene, ChunkFailureStillAcksCommittedChunks) {
    TempDB tmp{"h3_ack_survival"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    lattice::ensure_cursor_column(db.db());
    lattice::register_replication_slot(db.db(), "chan-a");

    // apply chunk size is 50: three chunks [0,50) [50,100) [100,120).
    auto entries = h3_insert_entries(120);

    ChunkHookGuard guard;
    int hook_calls = 0;
    lattice::test_hooks::apply_chunk_begin = [&hook_calls](size_t chunk_start) {
        ++hook_calls;
        if (chunk_start == 50) throw std::runtime_error("injected chunk failure");
    };

    auto acked = lattice::apply_remote_changes_for(db, entries, "chan-a");

    // Chunk 1 committed and MUST be acked; the failed chunk (after one
    // retry) and everything after it are not — the sender re-sends them.
    ASSERT_EQ(acked.size(), 50u)
        << "committed chunk's entries must be returned (= acked) despite the later failure";
    for (size_t i = 0; i < acked.size(); ++i) {
        EXPECT_EQ(acked[i], "h3-audit-" + std::to_string(i));
    }
    EXPECT_EQ(hook_calls, 3) << "chunk 0, failed chunk, exactly one retry";

    auto persons = db.db().query(
        "SELECT COUNT(*) AS c FROM TestPerson WHERE globalId LIKE 'h3-person-%'", {});
    EXPECT_EQ(std::get<int64_t>(persons[0].at("c")), 50)
        << "only the committed chunk's rows may exist";

    // The download cursor must sit at the last COMMITTED entry — a cursor
    // that leaked from the rolled-back chunk would create a download hole.
    auto slot = db.db().query(
        "SELECT last_received_event_id AS lid FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_FALSE(slot.empty());
    EXPECT_EQ(std::get<std::string>(slot[0].at("lid")), "h3-audit-49");
}

TEST(AuditHygiene, ChunkFailureRetriesOnceAndRecovers) {
    TempDB tmp{"h3_retry_recovers"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    lattice::ensure_cursor_column(db.db());
    lattice::register_replication_slot(db.db(), "chan-a");

    auto entries = h3_insert_entries(120);

    ChunkHookGuard guard;
    int failures_left = 1;  // transient: first attempt at chunk 50 fails, retry succeeds
    lattice::test_hooks::apply_chunk_begin = [&failures_left](size_t chunk_start) {
        if (chunk_start == 50 && failures_left > 0) {
            --failures_left;
            throw std::runtime_error("injected transient failure");
        }
    };

    auto acked = lattice::apply_remote_changes_for(db, entries, "chan-a");

    ASSERT_EQ(acked.size(), 120u) << "a transient chunk failure must be absorbed by the retry";
    auto persons = db.db().query(
        "SELECT COUNT(*) AS c FROM TestPerson WHERE globalId LIKE 'h3-person-%'", {});
    EXPECT_EQ(std::get<int64_t>(persons[0].at("c")), 120);

    auto slot = db.db().query(
        "SELECT last_received_event_id AS lid FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_FALSE(slot.empty());
    EXPECT_EQ(std::get<std::string>(slot[0].at("lid")), "h3-audit-119");
}

// ===========================================================================
// Remote-apply preamble failures own neither a transaction nor an ACK. These
// cases deny real SQLite prepares rather than throwing through a C callback.
// The returned IDs below are Core's ACK candidates, not a transport ACK test.
// ===========================================================================

namespace {

struct ApplyPreambleSnapshot {
    std::vector<lattice::database::row_t> persons;
    std::vector<lattice::database::row_t> audit;
    std::vector<lattice::database::row_t> sync_state;
    std::vector<lattice::database::row_t> slots;
    std::vector<lattice::database::row_t> sync_control;
};

ApplyPreambleSnapshot apply_preamble_snapshot(lattice::database& db) {
    return {
        db.query("SELECT * FROM TestPerson ORDER BY id", {}),
        db.query("SELECT * FROM AuditLog ORDER BY id", {}),
        db.query("SELECT * FROM _lattice_sync_state ORDER BY audit_entry_id, sync_id", {}),
        db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id", {}),
        db.query("SELECT * FROM _SyncControl ORDER BY id", {})};
}

struct ApplyPreambleDenial {
    enum class Point { receipts_create, cursor_probe } point;
    int denied = 0;
    int rollback_attempts = 0;

    static int authorize(void* context, int action, const char* first,
                         const char* second, const char*, const char*) noexcept {
        auto& state = *static_cast<ApplyPreambleDenial*>(context);
        if (action == SQLITE_TRANSACTION && first &&
            std::strcmp(first, "ROLLBACK") == 0) {
            ++state.rollback_attempts;
        }
        const bool receipts = state.point == Point::receipts_create &&
            action == SQLITE_CREATE_TABLE && first &&
            std::strcmp(first, "_lattice_applied_receipts") == 0;
        const bool cursor = state.point == Point::cursor_probe &&
            action == SQLITE_PRAGMA && first && second &&
            std::strcmp(first, "table_info") == 0 &&
            std::strcmp(second, "_lattice_replication_slots") == 0;
        if (receipts || cursor) {
            ++state.denied;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
};

struct ApplyPreambleAuthorizerReset {
    sqlite3* connection;
    ~ApplyPreambleAuthorizerReset() {
        sqlite3_set_authorizer(connection, nullptr, nullptr);
    }
};

void expect_apply_preamble_denial(ApplyPreambleDenial::Point point) {
    TempDB tmp{point == ApplyPreambleDenial::Point::receipts_create
                   ? "apply_cold_receipts_denial" : "apply_cursor_probe_denial"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"ExistingLocalRow", 64, std::nullopt});
    lattice::register_replication_slot(db.db(), "chan-a");
    lattice::register_replication_slot(db.db(), "relay-chan");
    db.db().execute(
        "UPDATE _lattice_replication_slots SET last_received_event_id = ?, "
        "last_active_at = '2026-08-13 11:59:00' WHERE sync_id = 'chan-a'",
        {std::string("sentinel-before-apply")});
    db.db().execute(
        "INSERT INTO _lattice_sync_state (audit_entry_id, sync_id, is_synchronized) "
        "SELECT id, 'chan-a', 0 FROM AuditLog ORDER BY id LIMIT 1", {});

    auto receipts_schema = db.db().query(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name = '_lattice_applied_receipts'", {});
    ASSERT_TRUE(receipts_schema.empty()) << "cold fixture must reach actual CREATE";
    if (point == ApplyPreambleDenial::Point::cursor_probe) {
        // Isolate the later PRAGMA denial: its preceding CREATE is already a
        // no-op, so this failure must leave all existing rows and DDL intact.
        db.db().execute(
            "CREATE TABLE _lattice_applied_receipts (globalId TEXT PRIMARY KEY)", {});
        db.db().execute(
            "INSERT INTO _lattice_applied_receipts VALUES ('existing-receipt')", {});
    }
    const auto receipt_rows_before = point == ApplyPreambleDenial::Point::cursor_probe
        ? db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId", {})
        : std::vector<lattice::database::row_t>{};
    const auto before = apply_preamble_snapshot(db.db());
    ASSERT_FALSE(before.persons.empty());
    ASSERT_FALSE(before.audit.empty());
    ASSERT_FALSE(before.sync_state.empty());
    ASSERT_EQ(before.slots.size(), 2u);
    ASSERT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
    auto* connection = db.db().handle();
    ASSERT_EQ(sqlite3_get_autocommit(connection), 1);

    ApplyPreambleDenial denial{point};
    ASSERT_EQ(sqlite3_set_authorizer(connection, ApplyPreambleDenial::authorize, &denial),
              SQLITE_OK);
    ApplyPreambleAuthorizerReset reset{connection};
    const auto entries = h3_insert_entries(1);
    std::vector<std::string> acked;
    ASSERT_NO_THROW(acked = lattice::apply_remote_changes_for(db, entries, "chan-a"));
    EXPECT_GT(denial.denied, 0) << "the intended real preamble prepare must be denied";
    EXPECT_EQ(denial.rollback_attempts, 0)
        << "preamble failure has not acquired a transaction to roll back";
    EXPECT_TRUE(acked.empty()) << "an unstarted apply has no ACK candidates";
    EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
    EXPECT_EQ(sqlite3_get_autocommit(connection), 1);
    ASSERT_EQ(sqlite3_set_authorizer(connection, nullptr, nullptr), SQLITE_OK);

    const auto after = apply_preamble_snapshot(db.db());
    EXPECT_EQ(after.persons, before.persons);
    EXPECT_EQ(after.audit, before.audit);
    EXPECT_EQ(after.sync_state, before.sync_state);
    EXPECT_EQ(after.slots, before.slots) << "including the exact sentinel download cursor";
    EXPECT_EQ(after.sync_control, before.sync_control);
    if (point == ApplyPreambleDenial::Point::receipts_create) {
        EXPECT_TRUE(db.db().query(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name = '_lattice_applied_receipts'", {}).empty());
    } else {
        EXPECT_EQ(db.db().query(
            "SELECT * FROM _lattice_applied_receipts ORDER BY globalId", {}),
            receipt_rows_before);
    }

    // Exactly the same entry must remain retryable after the actual denial
    // is removed; a second delivery then hits dedup rather than minting twice.
    ASSERT_NO_THROW(acked = lattice::apply_remote_changes_for(db, entries, "chan-a"));
    ASSERT_EQ(acked, std::vector<std::string>{entries[0].global_id});
    const auto applied_once = apply_preamble_snapshot(db.db());
    ASSERT_EQ(applied_once.persons.size(), before.persons.size() + 1);
    ASSERT_EQ(applied_once.audit.size(), before.audit.size() + 1);
    const auto cursor = db.db().query(
        "SELECT last_received_event_id AS c FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_EQ(cursor.size(), 1u);
    EXPECT_EQ(std::get<std::string>(cursor[0].at("c")), entries[0].global_id);
    ASSERT_NO_THROW(acked = lattice::apply_remote_changes_for(db, entries, "chan-a"));
    EXPECT_EQ(acked, std::vector<std::string>{entries[0].global_id});
    const auto redelivered = apply_preamble_snapshot(db.db());
    EXPECT_EQ(redelivered.persons, applied_once.persons);
    EXPECT_EQ(redelivered.audit, applied_once.audit);
    EXPECT_EQ(redelivered.sync_state, applied_once.sync_state);
    EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
    EXPECT_EQ(sqlite3_get_autocommit(connection), 1);
}

struct ApplyObserverReset {
    lattice::lattice_db& db;
    lattice::lattice_db::observer_id id;
    ~ApplyObserverReset() { db.remove_table_observer("TestPerson", id); }
};

} // namespace

TEST(AuditHygiene, ColdReceiptsCreateDenialLeavesApplyRetryableAndCursorUnchanged) {
    expect_apply_preamble_denial(ApplyPreambleDenial::Point::receipts_create);
}

TEST(AuditHygiene, CursorProbeDenialLeavesApplyRetryableAndCursorUnchanged) {
    expect_apply_preamble_denial(ApplyPreambleDenial::Point::cursor_probe);
}

TEST(AuditHygiene, NestedObserverApplyCannotClearTheOuterApplyFlag) {
    TempDB tmp{"apply_nested_observer"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"ExistingLocalRow", 65, std::nullopt});
    lattice::register_replication_slot(db.db(), "chan-a");
    auto entries = h3_insert_entries(2);
    const std::vector<lattice::audit_log_entry> outer{entries[0]};
    const std::vector<lattice::audit_log_entry> nested{entries[1]};

    int outer_observer_calls = 0;
    bool active_before_nested = false;
    bool active_after_nested = false;
    bool observer_saw_settled_transaction = false;
    std::vector<std::string> nested_acked;
    auto observer = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            bool saw_outer = false;
            for (const auto& event : events) {
                if (std::get<3>(event) == outer[0].global_row_id) saw_outer = true;
            }
            if (!saw_outer || outer_observer_calls++ != 0) return;
            // This is real unlocked, transaction-settled delivery. It runs
            // while the enclosing Core apply still owns its remote flag.
            observer_saw_settled_transaction =
                sqlite3_get_autocommit(db.db().handle()) == 1 &&
                !db.db().is_in_transaction();
            active_before_nested = db.applying_remote_changes_.load(std::memory_order_acquire);
            nested_acked = lattice::apply_remote_changes_for(db, nested, "chan-a");
            active_after_nested = db.applying_remote_changes_.load(std::memory_order_acquire);
        });
    ApplyObserverReset reset{db, observer};

    auto outer_acked = lattice::apply_remote_changes_for(db, outer, "chan-a");
    EXPECT_EQ(outer_acked, std::vector<std::string>{outer[0].global_id});
    ASSERT_EQ(outer_observer_calls, 1) << "must enter through actual outer row delivery";
    EXPECT_TRUE(observer_saw_settled_transaction);
    EXPECT_TRUE(active_before_nested);
    EXPECT_TRUE(nested_acked.empty()) << "an active outer apply refuses nested ACK candidates";
    EXPECT_TRUE(active_after_nested) << "nested refusal must not release the outer owner";
    EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
    auto nested_rows = db.db().query(
        "SELECT * FROM TestPerson WHERE globalId = ?", {nested[0].global_row_id});
    EXPECT_TRUE(nested_rows.empty());
    auto cursor = db.db().query(
        "SELECT last_received_event_id AS c FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_EQ(cursor.size(), 1u);
    EXPECT_EQ(std::get<std::string>(cursor[0].at("c")), outer[0].global_id);

    auto retry_acked = lattice::apply_remote_changes_for(db, nested, "chan-a");
    EXPECT_EQ(retry_acked, std::vector<std::string>{nested[0].global_id});
    nested_rows = db.db().query(
        "SELECT * FROM TestPerson WHERE globalId = ?", {nested[0].global_row_id});
    ASSERT_EQ(nested_rows.size(), 1u);
    const auto audit_once = db.db().query(
        "SELECT * FROM AuditLog WHERE globalId = ? ORDER BY id", {nested[0].global_id});
    ASSERT_EQ(audit_once.size(), 1u);
    EXPECT_EQ(lattice::apply_remote_changes_for(db, nested, "chan-a"), retry_acked);
    EXPECT_EQ(db.db().query(
        "SELECT * FROM TestPerson WHERE globalId = ?", {nested[0].global_row_id}), nested_rows);
    EXPECT_EQ(db.db().query(
        "SELECT * FROM AuditLog WHERE globalId = ? ORDER BY id", {nested[0].global_id}), audit_once);
    cursor = db.db().query(
        "SELECT last_received_event_id AS c FROM _lattice_replication_slots "
        "WHERE sync_id = 'chan-a'", {});
    ASSERT_EQ(cursor.size(), 1u);
    EXPECT_EQ(std::get<std::string>(cursor[0].at("c")), nested[0].global_id);
    EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
    EXPECT_EQ(sqlite3_get_autocommit(db.db().handle()), 1);
}

// Malformed wire positions must remain barriers, not become ACKable empty
// mutations or disappear before contiguous resume-cursor accounting.
namespace malformed_audit_ack {
void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
std::string replace_once(std::string input, const std::string& from, const std::string& to) {
    const auto at = input.find(from);
    require(at != std::string::npos && input.find(from, at + from.size()) == std::string::npos,
            "wire fixture replacement must address exactly one serialized member");
    input.replace(at, from.size(), to);
    return input;
}
std::string quote_wire_string(const std::string& text) {
    std::string result = "\"";
    for (unsigned char c : text) {
        require(c >= 0x20, "wire fixture strings contain no control characters");
        if (c == '\\' || c == '"') result += '\\';
        result += static_cast<char>(c);
    }
    return result + '"';
}
std::string frame(const std::vector<std::string>& entries) {
    std::string result = "{\"kind\":\"auditLog\",\"auditLog\":[";
    for (size_t i = 0; i < entries.size(); ++i) {
        if (i) result += ',';
        result += entries[i];
    }
    return result + "]}";
}
std::string frame(const std::vector<lattice::audit_log_entry>& entries) {
    std::vector<std::string> encoded;
    for (const auto& entry : entries) encoded.push_back(entry.to_json());
    return frame(encoded);
}
void initialize(lattice::lattice_db& db) {
    lattice::register_replication_slot(db.db(), "malformed-chan");
    lattice::ensure_cursor_column(db.db());
    db.db().execute("CREATE TABLE IF NOT EXISTS _lattice_applied_receipts (globalId TEXT PRIMARY KEY)");
    db.db().execute("UPDATE _lattice_replication_slots SET last_received_event_id='sentinel-before-frame' "
                    "WHERE sync_id='malformed-chan'");
}
std::string cursor(lattice::lattice_db& db) {
    const auto rows = db.db().query("SELECT last_received_event_id AS value FROM "
                                    "_lattice_replication_slots WHERE sync_id='malformed-chan'");
    require(rows.size() == 1, "actual receiving slot must exist");
    return std::get<std::string>(rows[0].at("value"));
}
void expect_same(const ApplyPreambleSnapshot& before, const ApplyPreambleSnapshot& after) {
    EXPECT_EQ(after.persons, before.persons);
    EXPECT_EQ(after.audit, before.audit);
    EXPECT_EQ(after.sync_state, before.sync_state);
    EXPECT_EQ(after.slots, before.slots);
    EXPECT_EQ(after.sync_control, before.sync_control);
}

#ifndef __EMSCRIPTEN__
// Drive the actual synchronizer's existing scheduled work explicitly. This
// adds no worker/timer or SQL. Closing admission discards tasks before the
// synchronizer can die, including on a fatal assertion.
class wire_queue final : public lattice::scheduler {
    const std::thread::id owner_ = std::this_thread::get_id();
    mutable std::mutex mutex_;
    std::queue<std::function<void()>> work_;
    bool open_ = true;
public:
    void invoke(std::function<void()>&& work) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (open_) work_.push(std::move(work));
    }
    bool is_on_thread() const noexcept override { return owner_ == std::this_thread::get_id(); }
    bool is_same_as(const lattice::scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        return open_;
    }
    void drain() {
        require(is_on_thread(), "wire fixture has one actual pump thread");
        for (size_t count = 0; ; ++count) {
            std::function<void()> work;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (work_.empty()) return;
                require(count < 256, "unexpected wire fixture work loop");
                work = std::move(work_.front());
                work_.pop();
            }
            work();
        }
    }
    void shutdown() override {
        std::queue<std::function<void()>> discarded;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            open_ = false;
            work_.swap(discarded);
        }
    }
};
struct queue_ending {
    std::shared_ptr<wire_queue> queue;
    ~queue_ending() { queue->shutdown(); }
};
// Settle the seeded local upload through the real protocol before taking any
// baseline snapshot or clearing frames. send_entries can start an ACK watchdog
// even with the pacer disabled; clearing the mock's frames does not ACK it.
void settle_seed_upload(lattice::synchronizer_base& sync, lattice::mock_sync_transport& wire,
                        wire_queue& queue) {
    std::vector<std::string> ids;
    for (const auto& message : wire.get_sent_messages()) {
        const auto event = lattice::server_sent_event::from_json(message.as_string());
        require(event.has_value(), "actual seeded upload must emit valid protocol frames");
        if (event->event_type == lattice::server_sent_event::type::audit_log)
            for (const auto& entry : event->audit_logs) ids.push_back(entry.global_id);
    }
    require(!ids.empty(), "wire fixture must settle a real seeded outbound upload");
    wire.simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack(std::move(ids)).to_json()));
    queue.drain(); // actual mark_as_synced and its queued upload continuation
    require(sync.get_progress().pending_upload == 0,
            "actual ACK handling must drain the seeded upload's in-flight entries");
}
std::vector<std::string> sent_acks(lattice::mock_sync_transport& wire) {
    std::vector<std::string> ids;
    for (const auto& message : wire.get_sent_messages()) {
        const auto event = lattice::server_sent_event::from_json(message.as_string());
        require(event.has_value(), "actual synchronizer must emit valid protocol frames");
        if (event->event_type == lattice::server_sent_event::type::ack)
            ids.insert(ids.end(), event->acked_ids.begin(), event->acked_ids.end());
    }
    return ids;
}
#endif
} // namespace malformed_audit_ack

#ifndef __EMSCRIPTEN__
TEST(AuditHygiene, MalformedWirePositionRejectsWholeFrameWithoutAckOrCursorLeapAndRetries) {
    using namespace malformed_audit_ack;
    // Every variant has valid outer JSON and valid neighbors on both sides.
    // The intended UPDATE addresses an existing known row with changed values.
    for (int variant = 0; variant < 16; ++variant) {
        SCOPED_TRACE(variant);
        TempDB file{"malformed-wire-position"};
        auto queue = std::make_shared<wire_queue>();
        lattice::configuration config(file.str(), queue);
        auto owner = std::make_unique<lattice::lattice_db>(config);
        auto& db = *owner;
        const auto target = db.add(TestPerson{"BeforeMalformed", 70, std::nullopt});
        initialize(db);
        auto entries = h3_insert_entries(3);
        entries[1].operation = "UPDATE";
        entries[1].global_row_id = target.global_id();
        const auto valid_middle = entries[1].to_json();
        const auto names_member = "\"changedFieldsNames\":" + entries[1].changed_fields_names_to_json();
        const auto fields_member = "\"changedFields\":" + entries[1].changed_fields_to_json();
        std::string bad;
        switch (variant) {
            case 0: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":" + quote_wire_string("[")); break;
            case 1: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":{}"); break;
            case 2: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":[\"name\",17]"); break;
            case 3: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":" + quote_wire_string("{}")); break;
            case 4: bad = replace_once(valid_middle, fields_member, "\"changedFields\":" + quote_wire_string("{")); break;
            case 5: bad = replace_once(valid_middle, fields_member, "\"changedFields\":[]"); break;
            case 6: bad = replace_once(valid_middle, "\"operation\":\"UPDATE\"", "\"operation\":\"MERGE\""); break;
            case 7: bad = "null"; break;
            case 8: bad = replace_once(valid_middle, "\"globalId\":" + quote_wire_string(entries[1].global_id), "\"globalId\":17"); break;
            case 9: bad = replace_once(valid_middle, names_member + ",", ""); break;
            case 10: bad = replace_once(valid_middle, fields_member + ",", ""); break;
            case 11: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":[\"name\",{}]"); break;
            case 12: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":[\"name\",[]]"); break;
            case 13: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":[\"name\",true]"); break;
            case 14: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":null"); break;
            default: bad = replace_once(valid_middle, names_member, "\"changedFieldsNames\":" + quote_wire_string("[\"name\",17]")); break;
        }
        const auto malformed_frame = frame(std::vector<std::string>{entries[0].to_json(), bad, entries[2].to_json()});
        EXPECT_FALSE(lattice::audit_log_entry::from_json(bad).has_value());
        EXPECT_FALSE(lattice::server_sent_event::from_json(malformed_frame).has_value());

        auto transport = std::make_unique<lattice::mock_sync_transport>();
        auto* wire = transport.get();
        lattice::sync_config sync_config;
        sync_config.sync_id = "malformed-chan";
        sync_config.checkpoint_passive_interval_ms = 0; // pacer off; uploads still arm ACK watchdogs
        lattice::synchronizer sync(std::move(owner), sync_config, std::move(transport));
        queue_ending ending{queue}; // before sync destruction on every exit
        sync.connect();
        queue->drain();
        settle_seed_upload(sync, *wire, *queue);
        wire->clear_sent_messages();
        const auto before = apply_preamble_snapshot(db.db());
        const auto receipts_before = db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId");
        wire->simulate_message(lattice::transport_message::from_string(malformed_frame));
        queue->drain();
        EXPECT_TRUE(sent_acks(*wire).empty());
        expect_same(before, apply_preamble_snapshot(db.db()));
        EXPECT_EQ(db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId"), receipts_before);
        EXPECT_EQ(cursor(db), "sentinel-before-frame");
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));

        wire->clear_sent_messages();
        wire->simulate_message(lattice::transport_message::from_string(frame(entries)));
        queue->drain();
        const std::vector<std::string> expected{entries[0].global_id, entries[1].global_id, entries[2].global_id};
        EXPECT_EQ(sent_acks(*wire), expected);
        EXPECT_EQ(cursor(db), entries.back().global_id);
        const auto applied = apply_preamble_snapshot(db.db());
        EXPECT_EQ(applied.persons.size(), 3u);
        const auto changed = db.db().query("SELECT name,age FROM TestPerson WHERE globalId=?", {target.global_id()});
        ASSERT_EQ(changed.size(), 1u);
        EXPECT_EQ(std::get<std::string>(changed[0].at("name")), "P1");
        EXPECT_EQ(std::get<int64_t>(changed[0].at("age")), 21);
        const auto receipts_applied = db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId");
        wire->clear_sent_messages();
        wire->simulate_message(lattice::transport_message::from_string(frame(entries)));
        queue->drain();
        EXPECT_EQ(sent_acks(*wire), expected);
        const auto repeated = apply_preamble_snapshot(db.db());
        EXPECT_EQ(repeated.persons, applied.persons);
        EXPECT_EQ(repeated.audit, applied.audit);
        EXPECT_EQ(repeated.sync_state, applied.sync_state);
        EXPECT_EQ(db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId"), receipts_applied);
        sync.disconnect();
    }
}

TEST(AuditHygiene, NativeNullableUpdateNamesAckWholeFrameAndRedeliverWithoutMutation) {
    using namespace malformed_audit_ack;
    for (const bool string_container : {false, true}) {
        SCOPED_TRACE(string_container);
        // Read the actual trigger output, before the native upload path filters null slots.
        // Swift AuditLog Codable preserves these slots in its [String?] field on the wire.
        TempDB source_file{"nullable-names-source"};
        lattice::lattice_db source{lattice::configuration(source_file.str())};
        const auto source_row = source.add(TestPerson{"BeforeNullable", 70, "keep@example.test"});
        source.db().execute("UPDATE TestPerson SET name='AfterNullable' WHERE globalId=?", {source_row.global_id()});
        const auto raw = source.db().query("SELECT changedFields,changedFieldsNames FROM AuditLog "
                                          "WHERE tableName='TestPerson' AND operation='UPDATE' ORDER BY id DESC LIMIT 1");
        ASSERT_EQ(raw.size(), 1u);
        const auto names = std::get<std::string>(raw[0].at("changedFieldsNames"));
        ASSERT_NE(names.find("null"), std::string::npos);

        TempDB file{"nullable-names-receiver"};
        auto queue = std::make_shared<wire_queue>();
        lattice::configuration config(file.str(), queue);
        auto owner = std::make_unique<lattice::lattice_db>(config);
        auto& db = *owner;
        const auto target = db.add(TestPerson{"BeforeNullable", 70, "keep@example.test"});
        initialize(db);
        auto entries = h3_insert_entries(3);
        entries[1].operation = "UPDATE";
        entries[1].global_row_id = target.global_id();
        entries[1].changed_fields = lattice::audit_log_entry::parse_changed_fields(
            std::get<std::string>(raw[0].at("changedFields")));
        entries[1].changed_fields_names = {"name"};
        const auto middle = replace_once(entries[1].to_json(),
            "\"changedFieldsNames\":" + entries[1].changed_fields_names_to_json(),
            "\"changedFieldsNames\":" + (string_container ? quote_wire_string(names) : names));
        const auto delivery = frame(std::vector<std::string>{entries[0].to_json(), middle, entries[2].to_json()});
        const auto parsed = lattice::audit_log_entry::from_json(middle);
        EXPECT_TRUE(parsed.has_value());
        if (parsed) EXPECT_EQ(parsed->changed_fields_names, (std::vector<std::string>{"name"}));
        EXPECT_TRUE(lattice::server_sent_event::from_json(delivery).has_value());

        auto transport = std::make_unique<lattice::mock_sync_transport>();
        auto* wire = transport.get();
        lattice::sync_config sync_config;
        sync_config.sync_id = "malformed-chan";
        sync_config.checkpoint_passive_interval_ms = 0;
        lattice::synchronizer sync(std::move(owner), sync_config, std::move(transport));
        queue_ending ending{queue};
        sync.connect();
        queue->drain();
        settle_seed_upload(sync, *wire, *queue);
        wire->clear_sent_messages();
        wire->simulate_message(lattice::transport_message::from_string(delivery));
        queue->drain();
        const std::vector<std::string> expected{entries[0].global_id, entries[1].global_id, entries[2].global_id};
        EXPECT_EQ(sent_acks(*wire), expected);
        EXPECT_EQ(cursor(db), entries.back().global_id);
        const auto changed = db.db().query("SELECT name,age,email FROM TestPerson WHERE globalId=?", {target.global_id()});
        ASSERT_EQ(changed.size(), 1u);
        EXPECT_EQ(std::get<std::string>(changed[0].at("name")), "AfterNullable");
        EXPECT_EQ(std::get<int64_t>(changed[0].at("age")), 70);
        EXPECT_EQ(std::get<std::string>(changed[0].at("email")), "keep@example.test");
        const auto applied = apply_preamble_snapshot(db.db());
        EXPECT_EQ(applied.persons.size(), 3u);
        EXPECT_EQ(db.db().query("SELECT id FROM AuditLog WHERE globalId=?", {entries[1].global_id}).size(), 1u);
        const auto receipts = db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId");
        wire->clear_sent_messages();
        wire->simulate_message(lattice::transport_message::from_string(delivery));
        queue->drain();
        EXPECT_EQ(sent_acks(*wire), expected);
        EXPECT_EQ(cursor(db), entries.back().global_id);
        expect_same(applied, apply_preamble_snapshot(db.db()));
        EXPECT_EQ(db.db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId"), receipts);
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_FALSE(db.applying_remote_changes_.load(std::memory_order_acquire));
        sync.disconnect();
    }
}

TEST(AuditHygiene, ValidWireSchemaSkewAndJsonStringContainersStillAckAndAdvanceCursor) {
    using namespace malformed_audit_ack;
    TempDB file{"valid-wire-skew"};
    auto queue = std::make_shared<wire_queue>();
    lattice::configuration config(file.str(), queue);
    auto owner = std::make_unique<lattice::lattice_db>(config);
    auto& db = *owner;
    const auto target = db.add(TestPerson{"Stable", 50, std::nullopt});
    initialize(db);
    auto entries = h3_insert_entries(4);
    entries[0].operation = "UPDATE";
    entries[0].global_row_id = target.global_id();
    entries[0].changed_fields = {{"name", lattice::any_property("Stable")}};
    entries[0].changed_fields_names = {"name"}; // real value-identical receipt-backed no-op
    entries[1].changed_fields["future_column"] = lattice::any_property("newer-peer");
    entries[1].changed_fields_names.push_back("future_column"); // known fields still apply
    entries[2].operation = "UPDATE";
    entries[2].global_row_id = target.global_id();
    entries[2].changed_fields = {{"future_column", lattice::any_property("newer-peer")}};
    entries[2].changed_fields_names = {"future_column"}; // unknown-only UPDATE no-op
    entries[3].table_name = "FutureModelFromNewerPeer"; // supported unknown-table no-op
    std::vector<std::string> encoded;
    for (const auto& entry : entries) {
        auto json = entry.to_json();
        json = replace_once(json, "\"changedFields\":" + entry.changed_fields_to_json(),
                            "\"changedFields\":" + quote_wire_string(entry.changed_fields_to_json()));
        json = replace_once(json, "\"changedFieldsNames\":" + entry.changed_fields_names_to_json(),
                            "\"changedFieldsNames\":" + quote_wire_string(entry.changed_fields_names_to_json()));
        encoded.push_back(std::move(json));
    }
    auto transport = std::make_unique<lattice::mock_sync_transport>();
    auto* wire = transport.get();
    lattice::sync_config sync_config;
    sync_config.sync_id = "malformed-chan";
    sync_config.checkpoint_passive_interval_ms = 0;
    lattice::synchronizer sync(std::move(owner), sync_config, std::move(transport));
    queue_ending ending{queue};
    sync.connect();
    queue->drain();
    settle_seed_upload(sync, *wire, *queue);
    wire->clear_sent_messages();
    wire->simulate_message(lattice::transport_message::from_string(frame(encoded)));
    queue->drain();
    EXPECT_EQ(sent_acks(*wire), (std::vector<std::string>{entries[0].global_id, entries[1].global_id,
                                                       entries[2].global_id, entries[3].global_id}));
    EXPECT_EQ(cursor(db), entries.back().global_id);
    const auto rows = db.db().query("SELECT name,age FROM TestPerson WHERE globalId=?", {target.global_id()});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "Stable");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("age")), 50);
    EXPECT_EQ(db.db().query("SELECT id FROM TestPerson WHERE globalId=?", {entries[1].global_row_id}).size(), 1u);
    EXPECT_FALSE(db.db().table_exists(entries[3].table_name));
    EXPECT_EQ(db.db().query("SELECT globalId FROM _lattice_applied_receipts WHERE globalId=?",
                            {entries[0].global_id}).size(), 1u);
    sync.disconnect();
}
#endif

TEST(AuditHygiene, DirectUnsupportedOperationDoesNotAckAndStopsCursorBeforeValidLaterEntry) {
    using namespace malformed_audit_ack;
    TempDB file{"typed-invalid-operation"};
    lattice::lattice_db db{lattice::configuration(file.str())};
    const auto target = db.add(TestPerson{"StillLocal", 60, std::nullopt});
    initialize(db);
    auto entries = h3_insert_entries(3);
    entries[1].operation = "MERGE";
    entries[1].global_row_id = target.global_id();
    EXPECT_EQ(lattice::apply_remote_changes_for(db, entries, "malformed-chan"),
              (std::vector<std::string>{entries[0].global_id, entries[2].global_id}));
    EXPECT_EQ(cursor(db), entries[0].global_id);
    EXPECT_TRUE(db.db().query("SELECT * FROM AuditLog WHERE globalId=?", {entries[1].global_id}).empty());
    EXPECT_TRUE(db.db().query("SELECT * FROM _lattice_applied_receipts WHERE globalId=?", {entries[1].global_id}).empty());
    const auto original = db.db().query("SELECT name,age FROM TestPerson WHERE globalId=?", {target.global_id()});
    ASSERT_EQ(original.size(), 1u);
    EXPECT_EQ(std::get<std::string>(original[0].at("name")), "StillLocal");
    EXPECT_EQ(std::get<int64_t>(original[0].at("age")), 60);
    entries[1].operation = "UPDATE";
    EXPECT_EQ(lattice::apply_remote_changes_for(db, entries, "malformed-chan"),
              (std::vector<std::string>{entries[0].global_id, entries[1].global_id, entries[2].global_id}));
    EXPECT_EQ(cursor(db), entries.back().global_id);
    EXPECT_EQ(db.db().query("SELECT * FROM AuditLog WHERE globalId=?", {entries[1].global_id}).size(), 1u);
    const auto changed = db.db().query("SELECT name,age FROM TestPerson WHERE globalId=?", {target.global_id()});
    ASSERT_EQ(changed.size(), 1u);
    EXPECT_EQ(std::get<std::string>(changed[0].at("name")), "P1");
    EXPECT_EQ(std::get<int64_t>(changed[0].at("age")), 21);
}

TEST(AuditHygiene, InvalidTypedPositionCannotBorrowEarlierAckOfTheSameIdForCursorProgress) {
    using namespace malformed_audit_ack;
    TempDB file{"typed-invalid-duplicate-id"};
    lattice::lattice_db db{lattice::configuration(file.str())};
    initialize(db);
    auto entries = h3_insert_entries(3);
    entries[1].operation = "MERGE";
    entries[1].global_id = entries[0].global_id; // a different invalid wire position
    EXPECT_EQ(lattice::apply_remote_changes_for(db, entries, "malformed-chan"),
              (std::vector<std::string>{entries[0].global_id, entries[2].global_id}));
    EXPECT_EQ(cursor(db), entries[0].global_id);
    EXPECT_TRUE(db.db().query("SELECT id FROM TestPerson WHERE globalId=?", {entries[1].global_row_id}).empty());
    EXPECT_TRUE(db.db().query("SELECT id FROM AuditLog WHERE operation='MERGE'").empty());
}
