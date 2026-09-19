#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string_view>
#include <thread>

// Keep the simultaneous start outside owned BEGIN. The old INSERT trace
// barrier would now wait for a second writer while holding the first write txn.
namespace lattice {
struct retention_claim_test_access {
    static void tick(lattice_db& owner, const std::function<void()>& before_setup) {
        (void)owner.run_audit_retention_tick_(false, lattice_db::retention_tick_limits{}, {}, before_setup);
    }
};
}

// ============================================================================
// Audit-history retention (1.5.0) — the "17 GB room store" red suite.
//
// A consumer that rewrote one row ~10×/s grew an AuditLog to 142K rows /
// 17 GB with under 1 MB of live data, because:
//   - nothing ever pruned history on a store WITHOUT sync partners
//     (safe_compact_audit_log keys on replication slots and returns -1
//     without any), and
//   - the only other tool, force_compact_audit_log, RESET the AUTOINCREMENT
//     sequence: every sibling process's cross-process cursor (seeded from
//     MAX(id), read forward) and the relay's pk-based push cursor then sat
//     above every new id — siblings went deaf until they reopened.
//
// prune_audit_log() is the cursor-safe tear-out: an insertion-time bound
// from recorded watermarks (never the row's own `timestamp`, which applied
// remote rows carry from their origin), capped by non-observer slot floors,
// never touching sqlite_sequence. force_compact now keeps the sequence.
// ============================================================================

namespace {

int64_t audit_rows(lattice::database& db) {
    auto rows = db.query("SELECT COUNT(*) AS c FROM AuditLog", {});
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

int64_t max_audit_id(lattice::database& db) {
    auto rows = db.query("SELECT COALESCE(MAX(id), 0) AS m FROM AuditLog", {});
    return rows.empty() ? 0 : std::get<int64_t>(rows[0].at("m"));
}

int64_t audit_sequence(lattice::database& db) {
    auto rows = db.query("SELECT seq FROM sqlite_sequence WHERE name = 'AuditLog'", {});
    return rows.empty() ? 0 : std::get<int64_t>(rows[0].at("seq"));
}

double now_epoch(lattice::database& db) {
    auto rows = db.query("SELECT unixepoch('subsec') AS t", {});
    const auto& v = rows[0].at("t");
    if (const auto* d = std::get_if<double>(&v)) return *d;
    return static_cast<double>(std::get<int64_t>(v));
}

/// Pretend a watermark was taken `age_seconds` ago at the CURRENT max id —
/// the deterministic stand-in for "a retention window elapsed".
void backdate_watermark(lattice::database& db, int64_t age_seconds) {
    const auto when = static_cast<int64_t>(now_epoch(db)) - age_seconds;
    db.execute("INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)",
               {std::string("audit_wm:") + std::to_string(when),
                std::to_string(max_audit_id(db))});
}

bool slots_have_column(lattice::database& db, const std::string& col) {
    for (const auto& row : db.query("PRAGMA table_info(_lattice_replication_slots)", {})) {
        auto it = row.find("name");
        if (it != row.end() && std::get<std::string>(it->second) == col) return true;
    }
    return false;
}

template <typename Pred>
bool poll_until(Pred pred, int timeout_ms = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

} // namespace

// ---------------------------------------------------------------------------
// prune_audit_log: age bound from watermarks, sequence untouched.
// ---------------------------------------------------------------------------
TEST(AuditRetention, PruneKeepsSequenceAndFreshRows) {
    TempDB tmp{"retention_basic"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};

    for (int i = 0; i < 100; ++i) db.add(TestPerson{"old" + std::to_string(i), i, std::nullopt});
    ASSERT_EQ(audit_rows(db.db()), 100);
    const int64_t old_max = max_audit_id(db.db());

    // A watermark taken "20 minutes ago" at id old_max: everything at or
    // below it has existed a full retention window.
    backdate_watermark(db.db(), 1200);

    for (int i = 0; i < 20; ++i) db.add(TestPerson{"fresh" + std::to_string(i), i, std::nullopt});
    const int64_t max_before = max_audit_id(db.db());
    const int64_t seq_before = audit_sequence(db.db());
    ASSERT_EQ(seq_before, max_before) << "test premise: AUTOINCREMENT tracks MAX(id)";

    // Nothing is old under a 1-hour retention (the only watermark is 20 min old).
    EXPECT_EQ(db.prune_audit_log(3600), 0);
    EXPECT_EQ(audit_rows(db.db()), 120);

    // Under a 10-minute retention the 100 old rows go, the 20 fresh ones stay.
    const int64_t removed = db.prune_audit_log(600);
    EXPECT_EQ(removed, 100);
    EXPECT_EQ(audit_rows(db.db()), 20);
    auto oldest = db.db().query("SELECT MIN(id) AS m FROM AuditLog", {});
    EXPECT_EQ(std::get<int64_t>(oldest[0].at("m")), old_max + 1);

    // Cursor safety: ids and the sequence are exactly where they were.
    EXPECT_EQ(max_audit_id(db.db()), max_before);
    EXPECT_EQ(audit_sequence(db.db()), seq_before);
    db.add(TestPerson{"after", 1, std::nullopt});
    EXPECT_EQ(max_audit_id(db.db()), max_before + 1) << "a new entry continues the sequence";
}

TEST(AuditRetention, PruneWithoutWatermarkIsANoOpThatStartsSampling) {
    TempDB tmp{"retention_nowm"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    for (int i = 0; i < 5; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});

    EXPECT_EQ(db.prune_audit_log(600), 0) << "no watermark old enough → nothing is provably dead";
    EXPECT_EQ(audit_rows(db.db()), 5);
    auto wm = db.db().query("SELECT COUNT(*) AS c FROM _lattice_meta WHERE key LIKE 'audit_wm:%'", {});
    EXPECT_EQ(std::get<int64_t>(wm[0].at("c")), 1) << "the call itself records a watermark";
}

TEST(AuditRetention, PruneRespectsWriterSlotFloor) {
    TempDB tmp{"retention_floor"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    for (int i = 0; i < 100; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    const int64_t maxid = max_audit_id(db.db());
    backdate_watermark(db.db(), 1200);

    // A real synchronizer that has only resolved the first half.
    lattice::register_replication_slot(db.db(), "wss:writer");
    lattice::advance_upload_floor(db.db(), "wss:writer", maxid - 50);

    EXPECT_EQ(db.prune_audit_log(600), 50);
    auto survivors = db.db().query("SELECT COUNT(*) AS c FROM AuditLog WHERE id > ?", {maxid - 50});
    EXPECT_EQ(std::get<int64_t>(survivors[0].at("c")), 50) << "un-uploaded history must survive";
}

// ---------------------------------------------------------------------------
// Observer slots: excluded from every floor, added lazily on legacy files.
// ---------------------------------------------------------------------------
TEST(AuditRetention, PruneIgnoresObserverSlots) {
    TempDB tmp{"retention_observer"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    for (int i = 0; i < 100; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    backdate_watermark(db.db(), 1200);

    // Our own read-only dial: registered as observer, floor never advances.
    lattice::register_replication_slot(db.db(), "wss:observer-token", /*is_observer=*/true);

    EXPECT_EQ(db.prune_audit_log(600), 100) << "an observer slot at floor 0 must not pin retention";
    EXPECT_EQ(audit_rows(db.db()), 0);
}

TEST(AuditRetention, ObserverSlotDoesNotPinSafeCompactFloor) {
    TempDB tmp{"compact_observer"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    for (int i = 0; i < 100; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    const int64_t maxid = max_audit_id(db.db());

    lattice::register_replication_slot(db.db(), "wss:writer");
    lattice::advance_upload_floor(db.db(), "wss:writer", maxid - 10);
    lattice::register_replication_slot(db.db(), "wss:observer", /*is_observer=*/true);

    EXPECT_EQ(db.safe_compact_audit_log(), 90) << "floor is the WRITER's, not MIN over the observer's 0";

    // Only observer slots left ⇒ slot-aware compaction has nothing to key on.
    lattice::remove_replication_slot(db.db(), "wss:writer");
    EXPECT_EQ(db.safe_compact_audit_log(), -1);

    // Re-registration re-classifies.
    lattice::register_replication_slot(db.db(), "wss:observer", /*is_observer=*/false);
    db.set_replication_slot_observer("wss:observer", true);
    auto flag = db.db().query("SELECT is_observer AS o FROM _lattice_replication_slots WHERE sync_id = 'wss:observer'", {});
    EXPECT_EQ(std::get<int64_t>(flag[0].at("o")), 1);
}

TEST(AuditRetention, LegacySlotsTableGainsObserverColumnLazily) {
    TempDB tmp{"legacy_slots"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    ASSERT_FALSE(slots_have_column(db.db(), "is_observer"))
        << "test premise: the base DDL is unchanged — the column arrives lazily, not via an epoch bump";

    // A slot written by an older binary (no is_observer column).
    db.db().execute(
        "INSERT INTO _lattice_replication_slots (sync_id, confirmed_audit_id, upload_floor, last_active_at) "
        "VALUES ('wss:old', 0, 0, datetime('now'))", {});
    for (int i = 0; i < 3; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});

    EXPECT_NO_THROW(db.safe_compact_audit_log());
    EXPECT_TRUE(slots_have_column(db.db(), "is_observer"));
    auto flag = db.db().query("SELECT is_observer AS o FROM _lattice_replication_slots WHERE sync_id = 'wss:old'", {});
    EXPECT_EQ(std::get<int64_t>(flag[0].at("o")), 0) << "legacy slots default to writer";
}

// ---------------------------------------------------------------------------
// force_compact_audit_log keeps the id sequence — siblings stay subscribed.
// ---------------------------------------------------------------------------
TEST(AuditRetention, ForceCompactKeepsIdSequence) {
    TempDB tmp{"force_seq"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    for (int i = 0; i < 10; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    const int64_t before = max_audit_id(db.db());

    const int64_t snapshots = db.force_compact_audit_log();
    EXPECT_EQ(snapshots, 10);
    auto oldest = db.db().query("SELECT MIN(id) AS m FROM AuditLog", {});
    EXPECT_GT(std::get<int64_t>(oldest[0].at("m")), before)
        << "regenerated snapshots must take ids ABOVE the old maximum";
    EXPECT_GE(audit_sequence(db.db()), before + 10);
}

TEST(AuditRetention, ForceCompactDoesNotDeafenSiblings) {
    TempDB tmp{"force_sibling"};
    lattice::lattice_db a{lattice::configuration(tmp.str())};
    lattice::lattice_db b{lattice::configuration(tmp.str())};
    for (int i = 0; i < 10; ++i) a.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});

    // b's cursor is seeded at the current MAX(id) and only ever moves forward.
    std::atomic<int> b_fires{0};
    auto id = b.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) { b_fires++; });

    a.force_compact_audit_log();
    a.add(TestPerson{"after-compact", 99, std::nullopt});

    EXPECT_TRUE(poll_until([&] { return b_fires.load() >= 1; }))
        << "a sibling handle must keep receiving changes after a compaction";
    b.remove_table_observer("TestPerson", id);
}

TEST(AuditRetention, SiblingCursorSurvivesPrune) {
    TempDB tmp{"prune_sibling"};
    lattice::lattice_db a{lattice::configuration(tmp.str())};
    lattice::lattice_db b{lattice::configuration(tmp.str())};
    for (int i = 0; i < 50; ++i) a.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    backdate_watermark(a.db(), 1200);

    std::atomic<int> b_fires{0};
    auto id = b.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) { b_fires++; });

    EXPECT_EQ(a.prune_audit_log(600), 50);
    a.add(TestPerson{"after-prune", 99, std::nullopt});

    EXPECT_TRUE(poll_until([&] { return b_fires.load() >= 1; }))
        << "pruning below every cursor must not disturb delivery";
    b.remove_table_observer("TestPerson", id);
}

// ---------------------------------------------------------------------------
// The maintenance thread: armed by configuration, coordinated via _lattice_meta.
// ---------------------------------------------------------------------------
TEST(AuditRetention, AutoPruneRunsFromMaintenanceThread) {
    TempDB tmp{"retention_thread"};
    lattice::configuration cfg(tmp.str());
    cfg.audit_retention_seconds = 2;   // thread period = 1 s
    lattice::lattice_db db{cfg};
    for (int i = 0; i < 30; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    ASSERT_EQ(audit_rows(db.db()), 30);
    backdate_watermark(db.db(), 60);

    EXPECT_TRUE(poll_until([&] { return audit_rows(db.db()) == 0; }, 5000))
        << "the retention thread must prune without any caller involvement";
    auto stamp = db.db().query("SELECT COUNT(*) AS c FROM _lattice_meta WHERE key = 'audit_prune_at'", {});
    EXPECT_EQ(std::get<int64_t>(stamp[0].at("c")), 1) << "the cross-process prune stamp is written";

    // Fresh writes are NOT pruned within the window (no watermark old enough yet).
    for (int i = 0; i < 5; ++i) db.add(TestPerson{"fresh" + std::to_string(i), i, std::nullopt});
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_EQ(audit_rows(db.db()), 5);
}

TEST(AuditRetention, ReadOnlyAndUnconfiguredOpensRunNoThread) {
    TempDB tmp{"retention_off"};
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};   // retention 0 = off
        for (int i = 0; i < 3; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
        backdate_watermark(db.db(), 60);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        EXPECT_EQ(audit_rows(db.db()), 3) << "retention off keeps everything (pre-1.5 behavior)";
    }
    lattice::configuration ro(tmp.str());
    ro.read_only = true;
    ro.audit_retention_seconds = 1;
    EXPECT_NO_THROW({ lattice::lattice_db db{ro}; });   // must not start a writer thread
}

namespace {

struct RetentionTrace {
    std::mutex mutex;
    std::condition_variable ready;
    int arrivals = 0;
    bool expired = false;
    std::atomic<int> prune_calls{0};

    static int trace(unsigned kind, void* context, void* statement, void*) {
        if (kind != SQLITE_TRACE_STMT) return 0;
        auto& state = *static_cast<RetentionTrace*>(context);
        const char* text = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
        if (!text) return 0;
        const std::string_view sql(text);
        if (sql.find("SELECT MAX(CAST(value AS INTEGER)) AS m FROM _lattice_meta") == 0)
            ++state.prune_calls;
        return 0;
    }

    void before_setup() {
        std::unique_lock<std::mutex> lock(mutex);
        ++arrivals;
        ready.notify_all();
        if (!ready.wait_for(lock, std::chrono::seconds(3), [&] { return arrivals == 2; }))
            expired = true;
    }
};

struct RetentionTraceRegistration {
    sqlite3* handle;
    ~RetentionTraceRegistration() { sqlite3_trace_v2(handle, 0, nullptr, nullptr); }
};

int64_t retention_claims(lattice::database& db) {
    const auto rows = db.query("SELECT COUNT(*) AS c FROM _lattice_meta WHERE key = 'audit_prune_at'", {});
    return std::get<int64_t>(rows.at(0).at("c"));
}

} // namespace

TEST(AuditRetention, SimultaneousHandlesClaimOnlyOnePrunePass) {
    TempDB tmp{"retention_claim"};
    lattice::configuration cfg(tmp.str());
    cfg.audit_retention_seconds = 600;
    lattice::lattice_db a{cfg};
    lattice::lattice_db b{cfg};
    a.stop_audit_maintenance();
    b.stop_audit_maintenance();
    for (int i = 0; i < 5; ++i) a.add(TestPerson{"old", i, std::nullopt});
    backdate_watermark(a.db(), 900);
    const auto sequence = audit_sequence(a.db());

    RetentionTrace trace;
    RetentionTraceRegistration trace_a{a.db().handle()}, trace_b{b.db().handle()};
    ASSERT_EQ(sqlite3_trace_v2(trace_a.handle, SQLITE_TRACE_STMT, &RetentionTrace::trace, &trace), SQLITE_OK);
    ASSERT_EQ(sqlite3_trace_v2(trace_b.handle, SQLITE_TRACE_STMT, &RetentionTrace::trace, &trace), SQLITE_OK);
    std::thread first([&] { lattice::retention_claim_test_access::tick(a, [&] { trace.before_setup(); }); });
    std::thread second([&] { lattice::retention_claim_test_access::tick(b, [&] { trace.before_setup(); }); });
    first.join();
    second.join();

    EXPECT_FALSE(trace.expired);
    EXPECT_EQ(trace.arrivals, 2);
    EXPECT_EQ(trace.prune_calls.load(), 1);
    EXPECT_EQ(retention_claims(a.db()), 1);
    EXPECT_EQ(audit_rows(a.db()), 0);
    EXPECT_EQ(audit_sequence(a.db()), sequence);
}

TEST(AuditRetention, RecentClaimSkipsPruneButStillSamples) {
    TempDB tmp{"retention_claim_recent"};
    lattice::configuration cfg(tmp.str());
    cfg.audit_retention_seconds = 600;
    lattice::lattice_db db{cfg};
    db.stop_audit_maintenance();
    db.add(TestPerson{"kept", 1, std::nullopt});
    backdate_watermark(db.db(), 900);
    const auto stamp = std::to_string(now_epoch(db.db()));
    db.db().execute("INSERT INTO _lattice_meta(key, value) VALUES('audit_prune_at', ?)", {stamp});
    db.run_audit_retention_tick();
    EXPECT_EQ(audit_rows(db.db()), 1);
    const auto claims = db.db().query("SELECT value FROM _lattice_meta WHERE key = 'audit_prune_at'", {});
    ASSERT_EQ(claims.size(), 1);
    EXPECT_EQ(std::get<std::string>(claims[0].at("value")), stamp);
    const auto samples = db.db().query("SELECT COUNT(*) AS c FROM _lattice_meta WHERE key LIKE 'audit_wm:%'", {});
    EXPECT_EQ(std::get<int64_t>(samples[0].at("c")), 2);
}

TEST(AuditRetention, ExpiredOrMalformedClaimCanBeReplaced) {
    for (const auto& previous : {std::string("0"), std::string("not-a-timestamp")}) {
        SCOPED_TRACE(previous);
        TempDB tmp{"retention_claim_expired"};
        lattice::configuration cfg(tmp.str());
        cfg.audit_retention_seconds = 600;
        lattice::lattice_db db{cfg};
        db.stop_audit_maintenance();
        db.add(TestPerson{"old", 1, std::nullopt});
        backdate_watermark(db.db(), 900);
        db.db().execute("INSERT INTO _lattice_meta(key, value) VALUES('audit_prune_at', ?)", {previous});
        db.run_audit_retention_tick();
        EXPECT_EQ(audit_rows(db.db()), 0);
        const auto claims = db.db().query("SELECT value FROM _lattice_meta WHERE key = 'audit_prune_at'", {});
        ASSERT_EQ(claims.size(), 1);
        EXPECT_NE(std::get<std::string>(claims[0].at("value")), previous);
    }
}

TEST(AuditRetention, FailedPruneReleasesClaimForNextTick) {
    TempDB tmp{"retention_claim_retry"};
    lattice::configuration cfg(tmp.str());
    cfg.audit_retention_seconds = 600;
    lattice::lattice_db db{cfg};
    db.stop_audit_maintenance();
    db.add(TestPerson{"old", 1, std::nullopt});
    backdate_watermark(db.db(), 900);
    struct Authorizer {
        sqlite3* handle;
        bool denied = false;
        ~Authorizer() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    } authorizer{db.db().handle()};
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle,
        [](void* context, int action, const char* table, const char*, const char*, const char*) {
            auto& state = *static_cast<Authorizer*>(context);
            if (action == SQLITE_DELETE && table && std::strcmp(table, "AuditLog") == 0) {
                state.denied = true;
                return SQLITE_DENY;
            }
            return SQLITE_OK;
        }, &authorizer), SQLITE_OK);
    EXPECT_NO_THROW(db.run_audit_retention_tick());
    EXPECT_TRUE(authorizer.denied);
    EXPECT_EQ(audit_rows(db.db()), 1);
    EXPECT_EQ(retention_claims(db.db()), 0);
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle, nullptr, nullptr), SQLITE_OK);
    db.run_audit_retention_tick();
    EXPECT_EQ(audit_rows(db.db()), 0);
    EXPECT_EQ(retention_claims(db.db()), 1);
}

TEST(AuditRetention, ManualTickAlsoHonorsDisabledAndReadOnlyConfiguration) {
    TempDB tmp{"retention_tick_disabled"};
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        db.add(TestPerson{"kept", 1, std::nullopt});
        backdate_watermark(db.db(), 900);
        db.run_audit_retention_tick();
        EXPECT_EQ(audit_rows(db.db()), 1);
        EXPECT_EQ(retention_claims(db.db()), 0);
    }
    lattice::configuration cfg(tmp.str());
    cfg.audit_retention_seconds = 600;
    cfg.read_only = true;
    lattice::lattice_db reader{cfg};
    EXPECT_NO_THROW(reader.run_audit_retention_tick());
    EXPECT_EQ(audit_rows(reader.db()), 1);
    EXPECT_EQ(retention_claims(reader.db()), 0);
}
