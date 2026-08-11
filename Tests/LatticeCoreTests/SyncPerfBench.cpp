#include "TestHelpers.hpp"
#include <lattice/sync.hpp>

#include <chrono>
#include <cstdio>
#include <unordered_set>

// ============================================================================
// Sync performance benches + red gates — the Aug 2026 "hard evidence" harness.
//
// Every fix in the 0.14.2 wave is gated by a number or an assertion here,
// captured FIRST on the pre-fix tree (tag latticecore-1.3.2-bench) so the
// evidence table has an honest "before" column from the same code.
//
//   UploadDrainThroughput   rows/s + resend amplification under scripted ack
//                           latency (instant vs delayed-past-deadline)
//   ApplyFrameLatency       ms per 1000-entry apply frame at log_level off vs
//                           info (the production disease was ~34K LOG_INFO
//                           writes per frame inside the apply transaction)
//   RedundantDeliveryMinting  DISTINCT audit entries carrying IDENTICAL row
//                           values must stop minting relay rows (B2); frame 1
//                           is genuine and must keep minting
//   LateAckFloorRecovery    rows marked synchronized while sitting in the
//                           open set must not pin the upload floor (B1) —
//                           the state is engineered via the exact function
//                           the late-ack path calls, no timing races
//
// Heavy sizes are opt-in: LATTICE_BENCH_FULL=1 selects production-scale Ns
// (the baseline-capture configuration); default sizes keep CI fast while
// preserving every red/green assertion.
// ============================================================================

namespace {

using clock_type = std::chrono::steady_clock;

int64_t ms_since(clock_type::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        clock_type::now() - t0).count();
}

bool full_bench() {
    const char* v = std::getenv("LATTICE_BENCH_FULL");
    return v && v[0] == '1';
}

int64_t max_audit_id(lattice::database& db) {
    auto rows = db.query("SELECT COALESCE(MAX(id), 0) AS m FROM AuditLog", {});
    return rows.empty() ? 0 : std::get<int64_t>(rows[0].at("m"));
}

int64_t audit_row_count(lattice::database& db) {
    auto rows = db.query("SELECT COUNT(*) AS c FROM AuditLog", {});
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

// Bulk-seed TestPerson through the live audit triggers (the same path a real
// writer takes) in ONE transaction — a per-row ORM add() would dominate the
// bench's own setup time at 50K rows.
void seed_persons(lattice::database& db, int n, const char* prefix) {
    db.execute("BEGIN IMMEDIATE", {});
    db.execute(
        "INSERT INTO TestPerson (globalId, name, age) "
        "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < " +
            std::to_string(n) + ") "
        "SELECT '" + std::string(prefix) + "-' || i, 'P' || i, 20 + (i % 60) FROM seq",
        {});
    db.execute("COMMIT", {});
}

// Parse every not-yet-seen outbound frame on the mock transport and return the
// audit global_ids it carried. Uses the real wire decoder — if the frame shape
// drifts, the bench fails loudly instead of silently counting nothing.
std::vector<std::string> harvest_sent_ids(lattice::mock_sync_transport* ws,
                                          size_t& cursor) {
    std::vector<std::string> ids;
    const auto& sent = ws->get_sent_messages();
    for (; cursor < sent.size(); ++cursor) {
        auto event = lattice::server_sent_event::from_json(sent[cursor].as_string());
        if (!event || event->event_type != lattice::server_sent_event::type::audit_log)
            continue;
        for (const auto& e : event->audit_logs) ids.push_back(e.global_id);
    }
    return ids;
}

void deliver_ack(lattice::mock_sync_transport* ws, std::vector<std::string> ids) {
    if (ids.empty()) return;
    ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack(std::move(ids)).to_json()));
}

struct DrainResult {
    int64_t wall_ms = 0;
    size_t distinct_acked = 0;
    size_t total_sent = 0;
    int64_t final_floor = 0;
    int64_t max_id = 0;
};

DrainResult run_drain(int n, int ack_delay_ms, int ack_timeout_ms) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"bench_drain"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    seed_persons(db->db(), n, "drain");

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "bench";
    config.all_active_sync_ids = {"bench"};
    config.ack_timeout_base_ms = ack_timeout_ms;

    lattice::synchronizer sync(std::move(db), config);
    auto* ws = mock_factory->last_websocket();
    sync.connect();

    DrainResult r;
    r.max_id = max_audit_id(reader.db());

    size_t cursor = 0;
    std::unordered_set<std::string> acked;
    const auto t0 = clock_type::now();
    const int64_t budget_ms = 180'000;

    while (static_cast<int>(acked.size()) < n && ms_since(t0) < budget_ms) {
        sync.sync_now();
        auto ids = harvest_sent_ids(ws, cursor);
        r.total_sent += ids.size();
        if (ids.empty()) {
            // Nothing new on the wire — give detached watchdog threads a beat.
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        if (ack_delay_ms > 0) {
            // The delayed profile: acks land only AFTER the resend deadline
            // has fired, the production shape (server applied the frame
            // slower than the client's patience).
            std::this_thread::sleep_for(std::chrono::milliseconds(ack_delay_ms));
        }
        std::unordered_set<std::string> fresh;
        for (auto& id : ids) if (acked.insert(id).second) fresh.insert(id);
        deliver_ack(ws, {fresh.begin(), fresh.end()});
    }

    r.wall_ms = ms_since(t0);
    r.distinct_acked = acked.size();
    r.final_floor = lattice::read_upload_floor(reader.db(), "bench");
    sync.disconnect();
    return r;
}

lattice::audit_log_entry make_insert_entry(const std::string& audit_gid,
                                           const std::string& row_gid,
                                           const std::string& table,
                                           int64_t row_id,
                                           std::map<std::string, lattice::any_property> fields) {
    lattice::audit_log_entry e;
    e.global_id = audit_gid;
    e.table_name = table;
    e.operation = "INSERT";
    e.row_id = row_id;
    e.global_row_id = row_gid;
    for (auto& [k, v] : fields) {
        e.changed_fields.emplace(k, v);
        e.changed_fields_names.push_back(k);
    }
    e.timestamp = "2026-08-10T12:00:00Z";
    return e;
}

} // namespace

// ---------------------------------------------------------------------------
// Upload throughput + amplification under scripted ack latency.
// ---------------------------------------------------------------------------
TEST(SyncPerfBench, UploadDrainThroughput) {
    const int n = full_bench() ? 50'000 : 5'000;

    // Profile 1: instant acks — the pump's ceiling.
    auto instant = run_drain(n, /*ack_delay_ms=*/0, /*ack_timeout_ms=*/10'000);
    // Profile 2: acks delayed past the resend deadline — production's shape.
    auto delayed = run_drain(n, /*ack_delay_ms=*/350, /*ack_timeout_ms=*/200);

    auto report = [n](const char* label, const DrainResult& r) {
        const double rows_s = r.wall_ms > 0 ? (double)r.distinct_acked * 1000.0 / r.wall_ms : 0;
        const double amp = r.distinct_acked > 0 ? (double)r.total_sent / r.distinct_acked : 0;
        std::printf("BENCH UploadDrainThroughput[%s]: n=%d acked=%zu wall_ms=%lld "
                    "rows_per_s=%.0f sent=%zu amplification=%.2f floor=%lld max_id=%lld\n",
                    label, n, r.distinct_acked, (long long)r.wall_ms, rows_s,
                    r.total_sent, amp, (long long)r.final_floor, (long long)r.max_id);
        ::testing::Test::RecordProperty(std::string(label) + "_rows_per_s", (int)rows_s);
        ::testing::Test::RecordProperty(std::string(label) + "_amplification_x100", (int)(amp * 100));
    };
    report("instant", instant);
    report("delayed", delayed);

    // Completion is the only hard gate; throughput/amplification are evidence.
    EXPECT_EQ(instant.distinct_acked, (size_t)n) << "instant-ack drain did not complete";
    EXPECT_EQ(delayed.distinct_acked, (size_t)n) << "delayed-ack drain did not complete";
}

// ---------------------------------------------------------------------------
// Server-apply latency proxy: ms per 1000-entry frame, log off vs info.
// ---------------------------------------------------------------------------
TEST(SyncPerfBench, ApplyFrameLatency) {
    TempDB tmp{"bench_apply"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    const int existing = full_bench() ? 500'000 : 50'000;
    const int frames = full_bench() ? 5 : 3;
    const int frame_size = 1000;

    // A relay-shaped database: a large AuditLog and a populated target table
    // (the per-entry dedup SELECT + pre/post row lookups pay real index costs).
    db.db().execute("BEGIN IMMEDIATE", {});
    db.db().execute(
        "INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId, "
        "                      changedFields, changedFieldsNames, isFromRemote, isSynchronized) "
        "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < " +
            std::to_string(existing) + ") "
        "SELECT 'hist-' || i, 'TestPerson', 'INSERT', i, 'hist-row-' || i, "
        "       '{}', '[]', 0, 1 FROM seq", {});
    db.db().execute("COMMIT", {});
    seed_persons(db.db(), full_bench() ? 50'000 : 5'000, "apply-seed");

    auto run_frames = [&](const char* label, int base) -> int64_t {
        std::vector<int64_t> per_frame;
        for (int f = 0; f < frames; ++f) {
            std::vector<lattice::audit_log_entry> entries;
            entries.reserve(frame_size);
            for (int i = 0; i < frame_size; ++i) {
                const std::string suffix =
                    std::string(label) + "-" + std::to_string(base + f) + "-" + std::to_string(i);
                entries.push_back(make_insert_entry(
                    "af-" + suffix, "af-row-" + suffix, "TestPerson", 0,
                    {{"globalId", lattice::any_property(std::string("af-row-") + suffix)},
                     {"name", lattice::any_property(std::string("Applied") + suffix)},
                     {"age", lattice::any_property(int64_t(33))}}));
            }
            const auto t0 = clock_type::now();
            auto applied = lattice::apply_remote_changes(db, entries);
            per_frame.push_back(ms_since(t0));
            EXPECT_EQ(applied.size(), (size_t)frame_size);
        }
        std::sort(per_frame.begin(), per_frame.end());
        return per_frame[per_frame.size() / 2];
    };

    // Quiet path — LatticeCore's shipping default.
    lattice::set_log_level(lattice::log_level::off);
    const int64_t off_ms = run_frames("off", 0);

    // The production disease: LOG_LEVEL=info emits per-entry + per-field +
    // per-SQL lines from inside the apply loop (writes to the test env's log
    // file — production wrote to fly's log pipe, same class of cost).
    lattice::set_log_level(lattice::log_level::info);
    const int64_t info_ms = run_frames("info", frames);

    // Restore the suite-wide level the test environment configured.
    lattice::set_log_level(lattice::log_level::debug);

    std::printf("BENCH ApplyFrameLatency: existing=%d median_off_ms=%lld median_info_ms=%lld "
                "info_overhead_x=%.1f\n",
                existing, (long long)off_ms, (long long)info_ms,
                off_ms > 0 ? (double)info_ms / off_ms : 0.0);
    RecordProperty("median_off_ms", (int)off_ms);
    RecordProperty("median_info_ms", (int)info_ms);
}

// ---------------------------------------------------------------------------
// B2's red gate: DISTINCT audit entries carrying IDENTICAL values (the 2.3M
// Edge-UPDATE flood class) must stop minting relay rows; the first, genuine
// delivery must keep minting.
// ---------------------------------------------------------------------------
TEST(SyncPerfBench, RedundantDeliveryMinting) {
    TempDB tmp{"bench_noop"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    // Blob-bearing table: production's flood carried Memory.embedding as a
    // trigger-format hex string on the wire; a blob-free fixture would pass
    // even with a broken value-equality guard (BLOB vs TEXT-hex compares
    // always-unequal).
    db.db().execute(
        "CREATE TABLE IF NOT EXISTS TestVec ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  globalId TEXT UNIQUE COLLATE NOCASE,"
        "  label TEXT,"
        "  embedding BLOB)", {});

    const int frame_size = 1000;
    const int redundant_frames = 4;
    const std::string hex_blob = "deadbeefcafe0123456789abcdef00ff";

    auto build_frame = [&](int frame_idx) {
        std::vector<lattice::audit_log_entry> entries;
        entries.reserve(frame_size);
        for (int i = 0; i < frame_size; ++i) {
            // DISTINCT audit ids per frame; SAME row identity + SAME values.
            entries.push_back(make_insert_entry(
                "rdm-" + std::to_string(frame_idx) + "-" + std::to_string(i),
                "vec-row-" + std::to_string(i), "TestVec", 0,
                {{"globalId", lattice::any_property("vec-row-" + std::to_string(i))},
                 {"label", lattice::any_property(std::string("edge"))},
                 {"embedding", lattice::any_property(hex_blob)}}));
        }
        return entries;
    };

    // Per-sync mode — the client relay path where the flood minted.
    const int64_t before = audit_row_count(db.db());
    auto first = lattice::apply_remote_changes_for(db, build_frame(0), "bench-chan");
    const int64_t after_first = audit_row_count(db.db());
    EXPECT_EQ(first.size(), (size_t)frame_size);
    EXPECT_EQ(after_first - before, frame_size)
        << "genuine first delivery must mint one relay row per entry";

    int64_t redundant_minted = 0;
    const auto t0 = clock_type::now();
    for (int f = 1; f <= redundant_frames; ++f) {
        const int64_t pre = audit_row_count(db.db());
        auto acked = lattice::apply_remote_changes_for(db, build_frame(f), "bench-chan");
        EXPECT_EQ(acked.size(), (size_t)frame_size)
            << "no-op entries must still be ACKED (sender retires them)";
        redundant_minted += audit_row_count(db.db()) - pre;
    }
    const int64_t redundant_ms = ms_since(t0);

    std::printf("BENCH RedundantDeliveryMinting: first_frame_minted=%lld "
                "redundant_frames=%d redundant_minted=%lld redundant_wall_ms=%lld\n",
                (long long)(after_first - before), redundant_frames,
                (long long)redundant_minted, (long long)redundant_ms);
    RecordProperty("redundant_minted", (int)redundant_minted);

    // RED on 1.3.2: every redundant frame mints frame_size relay rows
    // (observed in production as 2.3M isFromRemote Edge UPDATEs in 4h).
    // B2's value-guarded writes turn this green.
    EXPECT_EQ(redundant_minted, 0)
        << "value-identical redundant deliveries minted relay rows — the audit "
           "flood class is open";
}

// ---------------------------------------------------------------------------
// B1's red gate: entries marked synchronized in the DATABASE while still held
// in the in-memory open set (the late/foreign-ack shape — production had
// 245,550 such rows above a frozen floor) must not pin the upload floor.
// State is engineered through mark_audit_entries_synced_for — the exact
// function the late-ack path calls — so the test has no timing races.
// ---------------------------------------------------------------------------
TEST(SyncPerfBench, LateAckFloorRecovery) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"bench_lateack"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    const int n = 10;
    for (int i = 0; i < n; ++i) {
        db->add(TestPerson{"Late" + std::to_string(i), 30 + i, std::nullopt});
    }

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.sync_id = "bench";
    config.all_active_sync_ids = {"bench"};

    lattice::synchronizer sync(std::move(db), config);
    auto* ws = mock_factory->last_websocket();
    sync.connect();
    sync.sync_now();

    size_t cursor = 0;
    auto sent = harvest_sent_ids(ws, cursor);
    ASSERT_EQ(sent.size(), (size_t)n) << "all seeded entries should be in flight";

    // Late acks land for a PREFIX of the window: the DB rows get marked
    // synchronized (bypassing the in-flight map, exactly like an ack that
    // arrives after the deadline release or from another process's tenure)
    // while the synchronizer's open set still holds them.
    const size_t marked = 8;
    std::vector<std::string> late(sent.begin(), sent.begin() + marked);
    lattice::mark_audit_entries_synced_for(reader, late, "bench", {"bench"});

    auto id_rows = reader.db().query(
        "SELECT MAX(id) AS m FROM AuditLog WHERE globalId IN ('" + late[0] + "','" +
        late[marked - 1] + "')", {});
    const int64_t marked_max = std::get<int64_t>(id_rows[0].at("m"));

    // Subsequent passes skip synchronized rows, so nothing ever resolves the
    // stale open entries — on 1.3.2 the floor stays pinned BELOW rows the
    // database says are done, and every safe_compact is vetoed with it.
    sync.sync_now();
    sync.sync_now();

    const int64_t floor_after = lattice::read_upload_floor(reader.db(), "bench");
    std::printf("BENCH LateAckFloorRecovery: marked_max=%lld floor_after=%lld\n",
                (long long)marked_max, (long long)floor_after);

    EXPECT_GE(floor_after, marked_max)
        << "upload floor is pinned below rows the database has already marked "
           "synchronized — late acks never resolve the open set (B1)";

    // Full recovery: acking the tail must carry the floor to the top.
    deliver_ack(ws, {sent.begin() + marked, sent.end()});
    sync.sync_now();
    const int64_t floor_final = lattice::read_upload_floor(reader.db(), "bench");
    EXPECT_GE(floor_final, max_audit_id(reader.db()))
        << "floor did not reach MAX after every entry was acked";

    sync.disconnect();
}
