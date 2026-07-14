#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <filesystem>
#include <future>
#include <optional>

// ============================================================================
// Live Results item A, Commit 3 (lattice repo
// docs/design-results-item-A-SPEC.md): synchronous invalidation hooks (§2.3),
// the read-generation keeper pool (§2.2/§2.5), WAL-retention policy (§3 —
// TTL, force-retire protocol, threshold eviction, per-path aggregation), the
// memory-family keeper refusal (§4.1), and the per-store write gate (§4.1
// mechanism 2).
// ============================================================================

using lattice::lattice_db;
using lattice::configuration;
using inv_reason = lattice::lattice_db::invalidation_reason;

namespace {

std::string gen_mem_uri(const std::string& name) {
    return "file:" + name + "?mode=memory&cache=shared";
}

int64_t wal_file_size(const TempDB& db) {
    std::error_code ec;
    auto size = std::filesystem::file_size(db.path.string() + "-wal", ec);
    return ec ? 0 : static_cast<int64_t>(size);
}

int64_t count_people_at(lattice_db& db, uint64_t gen) {
    auto rows = db.query_at_generation(gen, "SELECT COUNT(*) AS c FROM TestPerson");
    if (!rows || rows->empty()) return -1;
    return std::get<int64_t>((*rows)[0].at("c"));
}

// A statement slow enough that force-retire's sqlite3_interrupt provably
// lands mid-flight (aborts in ms once interrupted).
constexpr const char* kSlowQuery =
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) "
    "SELECT COUNT(*) AS c FROM c";

} // namespace

// ----------------------------------------------------------------------------
// Invalidation hook (§2.3)
// ----------------------------------------------------------------------------

// The load-bearing ordering property: the hook fires inline on the writer's
// thread, after the commit settled, and BEFORE any scheduler dispatch — i.e.
// it has already run when add() returns, while the table-observer callback
// (which rides scheduler_->invoke) is still queued. This is exactly the
// property the existing addTableObserver trampoline does NOT have (spec §2.3).
TEST(InvalidationHook, FiresInlineOnWriterThreadBeforeSchedulerDispatch) {
    TempDB tmp("inv_inline");
    auto sched = std::make_shared<lattice::main_thread_scheduler>();  // queues until pumped
    lattice_db db{configuration(tmp.str(), sched)};

    std::atomic<int> hook_fires{0};
    std::atomic<int> observer_fires{0};
    std::atomic<bool> hook_on_writer_thread{false};
    std::atomic<bool> observer_ran_before_hook{false};
    std::mutex tables_mutex;
    std::vector<std::string> tables_seen;

    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice_db::change_event>&) {
            observer_fires.fetch_add(1, std::memory_order_relaxed);
        });

    const auto writer_tid = std::this_thread::get_id();
    db.add_invalidation_hook([&](const std::vector<std::string>& tables, inv_reason reason) {
        if (reason != inv_reason::commit) return;
        hook_fires.fetch_add(1, std::memory_order_relaxed);
        hook_on_writer_thread.store(std::this_thread::get_id() == writer_tid,
                                    std::memory_order_relaxed);
        if (observer_fires.load(std::memory_order_relaxed) > 0) {
            observer_ran_before_hook.store(true, std::memory_order_relaxed);
        }
        std::lock_guard<std::mutex> lock(tables_mutex);
        tables_seen = tables;
    });

    db.add(TestPerson{"a", 1, std::nullopt});

    // Already delivered when add() returned — no await, no hop.
    EXPECT_EQ(hook_fires.load(), 1);
    EXPECT_TRUE(hook_on_writer_thread.load());
    // ...and strictly BEFORE the scheduler-dispatched observer fan-out.
    EXPECT_EQ(observer_fires.load(), 0) << "observer dispatch must still be queued";
    EXPECT_FALSE(observer_ran_before_hook.load());
    {
        std::lock_guard<std::mutex> lock(tables_mutex);
        EXPECT_TRUE(std::find(tables_seen.begin(), tables_seen.end(), "TestPerson") !=
                    tables_seen.end())
            << "payload must carry the batch's changed table names";
    }

    sched->process_pending();
    EXPECT_EQ(observer_fires.load(), 1);
}

// Cross-instance fan-out: a commit through one handle fires the hooks of
// EVERY alive same-path instance, synchronously on the writer's thread —
// the synchronizer-handle → app-handle topology (spec §2.3).
TEST(InvalidationHook, CrossInstanceFanOutIsSynchronous) {
    TempDB tmp("inv_xinst");
    lattice_db writer_handle{configuration(tmp.str())};
    lattice_db observer_handle{configuration(tmp.str())};

    std::atomic<int> fires{0};
    std::atomic<bool> on_writer_thread{false};
    const auto writer_tid = std::this_thread::get_id();
    observer_handle.add_invalidation_hook(
        [&](const std::vector<std::string>&, inv_reason reason) {
            if (reason != inv_reason::commit) return;
            fires.fetch_add(1, std::memory_order_relaxed);
            on_writer_thread.store(std::this_thread::get_id() == writer_tid,
                                   std::memory_order_relaxed);
        });

    writer_handle.add(TestPerson{"x", 1, std::nullopt});

    EXPECT_EQ(fires.load(), 1) << "second same-path handle must hear the commit";
    EXPECT_TRUE(on_writer_thread.load()) << "fan-out must be inline, not dispatched";
}

// Rollback signals (spec §2.3): a rolled-back transaction delivers no change
// batch by design, so the rollback hook must bump — without it a capture
// that raced the transaction could serve a poisoned id vector forever.
TEST(InvalidationHook, RollbackSignals) {
    TempDB tmp("inv_rollback");
    lattice_db db{configuration(tmp.str())};

    std::atomic<int> rollback_fires{0};
    std::atomic<int> commit_fires{0};
    db.add_invalidation_hook([&](const std::vector<std::string>&, inv_reason reason) {
        if (reason == inv_reason::rollback) rollback_fires.fetch_add(1, std::memory_order_relaxed);
        if (reason == inv_reason::commit) commit_fires.fetch_add(1, std::memory_order_relaxed);
    });

    db.begin_transaction();
    db.add(TestPerson{"doomed", 1, std::nullopt});
    db.rollback();

    EXPECT_EQ(rollback_fires.load(), 1);
    EXPECT_EQ(commit_fires.load(), 0) << "a rolled-back txn must not signal a commit";

    // A subsequent committed write signals exactly its own commit.
    db.add(TestPerson{"kept", 2, std::nullopt});
    EXPECT_EQ(commit_fires.load(), 1);
    EXPECT_EQ(rollback_fires.load(), 1);
}

// remove_invalidation_hook stops delivery; removal from inside a callback is
// legal (hooks are copied out before invocation).
TEST(InvalidationHook, RemoveStopsDelivery) {
    TempDB tmp("inv_remove");
    lattice_db db{configuration(tmp.str())};

    std::atomic<int> fires{0};
    uint64_t token = 0;
    token = db.add_invalidation_hook([&](const std::vector<std::string>&, inv_reason reason) {
        if (reason != inv_reason::commit) return;
        fires.fetch_add(1, std::memory_order_relaxed);
        db.remove_invalidation_hook(token);  // self-unregistration mid-callback
    });

    db.add(TestPerson{"a", 1, std::nullopt});
    db.add(TestPerson{"b", 2, std::nullopt});
    EXPECT_EQ(fires.load(), 1) << "removed hook must not fire again";
}

// Memory-store mark-synced commits reach the hooks too (no WAL hook there;
// sync.cpp's notify_observers signals explicitly — spec §2.3).
TEST(InvalidationHook, MemoryMarkSyncedSignalsAllHandles) {
    const auto uri = gen_mem_uri("inv_marksync");
    lattice_db a{configuration(uri)};
    lattice_db b{configuration(uri)};

    a.add(TestPerson{"row", 1, std::nullopt});
    auto gid_rows = a.db().query("SELECT globalId FROM AuditLog ORDER BY id DESC LIMIT 1");
    ASSERT_FALSE(gid_rows.empty());
    const auto gid = std::get<std::string>(gid_rows[0].at("globalId"));

    std::atomic<int> b_commit_fires{0};
    std::mutex tables_mutex;
    std::vector<std::string> tables_seen;
    b.add_invalidation_hook([&](const std::vector<std::string>& tables, inv_reason reason) {
        if (reason != inv_reason::commit) return;
        b_commit_fires.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(tables_mutex);
        tables_seen = tables;
    });

    lattice::mark_audit_entries_synced(a, {gid});

    EXPECT_GE(b_commit_fires.load(), 1)
        << "mark-synced commit on a memory store must signal sibling handles";
    std::lock_guard<std::mutex> lock(tables_mutex);
    EXPECT_TRUE(std::find(tables_seen.begin(), tables_seen.end(), "AuditLog") !=
                tables_seen.end());
}

// T10-shaped ABBA watchdog for the hook's leaf-lock contract (spec §2.3):
// a coordinator-shaped hook takes its own leaf lock (atomics-only body);
// a reader thread uses the two-phase pattern (take the lock, RELEASE it,
// then run SQL / pin generations) while a writer loops. Any lock held
// across SQL on the reader side would ABBA against the hook frame — the
// bounded watchdog turns that hang into a failure.
TEST(InvalidationHook, LeafLockABBAWatchdog) {
    TempDB tmp("inv_abba");
    lattice_db db{configuration(tmp.str())};

    std::mutex coordinator_lock;  // the leaf lock the hook takes
    std::atomic<uint64_t> epoch{0};
    db.add_invalidation_hook([&](const std::vector<std::string>&, inv_reason) {
        std::lock_guard<std::mutex> lock(coordinator_lock);
        epoch.fetch_add(1, std::memory_order_relaxed);
    });

    std::atomic<bool> stop{false};
    std::atomic<bool> writer_done{false};

    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            // Phase 1: snapshot coordinator state under the leaf lock...
            uint64_t seen;
            {
                std::lock_guard<std::mutex> lock(coordinator_lock);
                seen = epoch.load(std::memory_order_relaxed);
            }
            (void)seen;
            // Phase 2: ...lock RELEASED, now run SQL (pin + read + release).
            auto gen = db.acquire_read_generation();
            if (gen != 0) {
                (void)db.query_at_generation(gen, "SELECT COUNT(*) AS c FROM TestPerson");
                db.release_read_generation(gen);
            }
        }
    });

    std::thread writer([&] {
        for (int i = 0; i < 300; ++i) {
            db.add(TestPerson{"w", i, std::nullopt});
        }
        writer_done.store(true, std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!writer_done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    stop.store(true, std::memory_order_release);
    if (!writer_done.load(std::memory_order_acquire)) {
        writer.detach();
        reader.detach();
        FAIL() << "ABBA hang: hook-side lock vs connection mutex";
    }
    writer.join();
    reader.join();
    EXPECT_GE(epoch.load(), 300u);
}

// ----------------------------------------------------------------------------
// Read-generation pool (§2.2/§2.5)
// ----------------------------------------------------------------------------

// The core guarantee: a held generation reads one MVCC snapshot — a reader
// at generation N sees pre-commit state after the writer commits; a fresh
// generation sees post-commit state.
TEST(ReadGeneration, KeeperMVCCIsolation) {
    TempDB tmp("gen_mvcc");
    lattice_db db{configuration(tmp.str())};
    for (int i = 0; i < 10; ++i) {
        db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});
    }

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    EXPECT_EQ(count_people_at(db, gen), 10);

    // Writer commits AFTER the pin: one insert, three deletes.
    db.add(TestPerson{"late", 99, std::nullopt});
    db.db().execute("DELETE FROM TestPerson WHERE age < 3");

    EXPECT_EQ(count_people_at(db, gen), 10)
        << "a held generation must keep reading its pinned snapshot";

    auto fresh = db.acquire_read_generation();
    ASSERT_NE(fresh, 0u);
    EXPECT_EQ(count_people_at(db, fresh), 8)
        << "a fresh generation must see the post-commit state";

    db.release_read_generation(gen);
    db.release_read_generation(fresh);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

// Pool cap (default 3, spec §2.2(e)): a fourth concurrent acquisition
// force-retires the OLDEST generation; reads against it re-resolve (nullopt,
// the tolerant-ladder trigger), and retired connections are pooled — no
// open/close churn in steady state.
TEST(ReadGeneration, PoolCapForceRetiresOldest) {
    TempDB tmp("gen_poolcap");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"x", 1, std::nullopt});

    auto g1 = db.acquire_read_generation();
    auto g2 = db.acquire_read_generation();
    auto g3 = db.acquire_read_generation();
    ASSERT_NE(g1, 0u);
    ASSERT_NE(g2, 0u);
    ASSERT_NE(g3, 0u);
    EXPECT_EQ(db.local_read_generations_outstanding(), 3u);

    auto g4 = db.acquire_read_generation();
    ASSERT_NE(g4, 0u);
    EXPECT_EQ(db.local_read_generations_outstanding(), 3u) << "pool cap must hold";
    EXPECT_FALSE(db.query_at_generation(g1, "SELECT 1 AS one").has_value())
        << "oldest generation was force-retired; its reads must refuse";
    EXPECT_TRUE(db.query_at_generation(g4, "SELECT 1 AS one").has_value());

    // Releasing an already-retired id is a harmless no-op.
    db.release_read_generation(g1);
    db.release_read_generation(g2);
    db.release_read_generation(g3);
    db.release_read_generation(g4);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

// retain/release refcounting: a retained generation survives one release and
// retires on the last one.
TEST(ReadGeneration, RetainReleaseRefcounting) {
    TempDB tmp("gen_refcount");
    lattice_db db{configuration(tmp.str())};
    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    ASSERT_TRUE(db.retain_read_generation(gen));

    db.release_read_generation(gen);
    EXPECT_TRUE(db.query_at_generation(gen, "SELECT 1 AS one").has_value())
        << "retained generation must survive the first release";

    db.release_read_generation(gen);
    EXPECT_FALSE(db.query_at_generation(gen, "SELECT 1 AS one").has_value());
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    EXPECT_FALSE(db.retain_read_generation(gen)) << "retired ids must refuse retain";
}

// §4.1: memory-family stores get NO keepers — private :memory: (a held read
// txn on the single write connection would wedge all writes) and named
// shared-cache (held read locks fail same-name writers with SQLITE_LOCKED).
TEST(ReadGeneration, MemoryFamilyRefusesKeepers) {
    lattice_db isolated;  // :memory:
    EXPECT_EQ(isolated.acquire_read_generation(), 0u);
    EXPECT_EQ(isolated.local_read_generations_outstanding(), 0u);

    lattice_db named{configuration(gen_mem_uri("gen_refuse"))};
    EXPECT_EQ(named.acquire_read_generation(), 0u);
    EXPECT_EQ(named.read_generations_outstanding(), 0u);
}

// ----------------------------------------------------------------------------
// WAL-retention policy (§3)
// ----------------------------------------------------------------------------

// Idle TTL (§3.2): a generation past the TTL with no active reads
// self-retires on the maintenance tick — which runs fine with no
// synchronizer anywhere in sight (the §3.2 "enforcement actor exists for
// every storage/config" requirement).
TEST(ReadGeneration, TTLRetiresIdleGenerations) {
    TempDB tmp("gen_ttl");
    lattice_db db{configuration(tmp.str())};
    db.set_read_generation_ttl_ms(50);

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);

    // Recently active → survives.
    (void)db.query_at_generation(gen, "SELECT 1 AS one");
    db.run_read_pool_maintenance();
    EXPECT_EQ(db.local_read_generations_outstanding(), 1u);

    // Idle past the TTL → retired, even though the logical hold (refcount)
    // was never released: "active reads" means in-flight statements, not
    // logical accesses (§2.2).
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    db.run_read_pool_maintenance();
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    EXPECT_FALSE(db.query_at_generation(gen, "SELECT 1 AS one").has_value());
    db.release_read_generation(gen);  // late release: no-op
}

// The "active reads" definition (§2.2/§3.2): an in-flight statement on the
// keeper blocks TTL retirement; the same generation retires once idle.
TEST(ReadGeneration, InFlightStatementBlocksTTLRetire) {
    TempDB tmp("gen_ttl_active");
    lattice_db db{configuration(tmp.str())};
    db.set_read_generation_ttl_ms(30);

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);

    std::atomic<bool> started{false};
    std::thread reader([&] {
        started.store(true, std::memory_order_release);
        (void)db.query_at_generation(gen,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 20000000) "
            "SELECT COUNT(*) AS c FROM c");
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(60));  // > TTL

    db.run_read_pool_maintenance();
    const auto outstanding_mid_read = db.local_read_generations_outstanding();
    reader.join();
    EXPECT_EQ(outstanding_mid_read, 1u)
        << "a generation with an in-flight statement must not TTL-retire";

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    db.run_read_pool_maintenance();
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    db.release_read_generation(gen);
}

// Force-retire protocol (§3.4): retiring flag → sqlite3_interrupt → bounded
// COMMIT retry. The wedged in-flight read surfaces as nullopt (tolerant
// ladder), never an exception; the pool is immediately reusable — no
// SQLITE_BUSY leak, no connection stuck mid-transaction.
TEST(ReadGeneration, ForceRetireInterruptsInFlightStatement) {
    TempDB tmp("gen_force");
    lattice_db db{configuration(tmp.str())};
    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);

    std::atomic<bool> started{false};
    std::optional<std::vector<lattice::database::row_t>> result =
        std::vector<lattice::database::row_t>{};
    std::thread reader([&] {
        started.store(true, std::memory_order_release);
        result = db.query_at_generation(gen, kSlowQuery);
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    db.retire_all_read_generations();
    reader.join();

    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    EXPECT_FALSE(result.has_value())
        << "the interrupted read must surface as empty (ladder), not throw";

    auto fresh = db.acquire_read_generation();
    ASSERT_NE(fresh, 0u) << "pool must be reusable right after a force-retire";
    EXPECT_TRUE(db.query_at_generation(fresh, "SELECT 1 AS one").has_value());
    db.release_read_generation(fresh);
}

// Item-A adversarial finding 1 (query_at_generation TOCTOU), pinned
// deterministically via the test seam: a reader passes the liveness check
// and bumps in_flight, then stalls BEFORE its statement starts. A concurrent
// force-retire interrupts (a no-op — sqlite3_interrupt does not affect
// statements that start after it returns), COMMITs, and — pre-fix — pooled
// the connection, where the next acquire re-pinned it; the delayed SELECT
// then ran at the WRONG snapshot with no error and no stale sentinel. The
// fix is two-sided: (a) commit_and_pool refuses to pool while in_flight > 0
// (drops the connection instead), and (b) query_at_generation re-checks
// `retiring` after the statement returns — retiring is set before the
// COMMIT under the pool lock, so any wrong-snapshot read must observe it
// and yield nullopt (tolerant ladder).
TEST(ReadGeneration, ForceRetireRaceNeverServesWrongSnapshot) {
    TempDB tmp("gen_toctou");
    lattice_db db{configuration(tmp.str())};
    for (int i = 0; i < 5; ++i) db.add(TestPerson{"p" + std::to_string(i), i, std::nullopt});

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    EXPECT_EQ(count_people_at(db, gen), 5);

    std::promise<void> reader_in_gap;
    std::promise<void> resume_reader;
    auto resume_future = resume_reader.get_future().share();
    std::atomic<bool> gap_used{false};
    // Fires exactly once (the delayed reader); later generation reads in
    // this test pass straight through.
    db.test_hook_generation_query_gap_ = [&] {
        if (!gap_used.exchange(true)) {
            reader_in_gap.set_value();
            resume_future.wait();
        }
    };

    std::optional<std::vector<lattice::database::row_t>> result;
    std::thread reader([&] {
        result = db.query_at_generation(gen, "SELECT COUNT(*) AS c FROM TestPerson");
    });
    // Reader has passed the liveness check and bumped in_flight; its
    // statement has NOT started.
    reader_in_gap.get_future().wait();

    // Force-retire while the reader is stalled in the gap: the interrupt
    // no-ops, the COMMIT succeeds immediately.
    db.retire_all_read_generations();
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    // (a) the keeper must NOT be pooled with the reader still in flight — a
    // re-acquire would otherwise re-pin it underneath the delayed statement.
    EXPECT_EQ(db.idle_read_pool_size(), 0u)
        << "connection with an in-flight reader must be dropped, not pooled";

    // Advance head state so a wrong-snapshot read is detectable, and re-pin
    // a successor generation (pre-fix, this adopted the reader's connection
    // out of the pool and BEGAN a new transaction on it).
    db.add(TestPerson{"late", 99, std::nullopt});
    auto successor = db.acquire_read_generation();
    ASSERT_NE(successor, 0u);

    resume_reader.set_value();
    reader.join();

    // (b) the delayed read ran post-COMMIT — head state, not gen's snapshot.
    // It must surface as nullopt (re-resolve via the tolerant ladder), never
    // as wrong-snapshot rows attributed to the retired generation.
    EXPECT_FALSE(result.has_value())
        << "wrong-snapshot read leaked through a force-retire";

    EXPECT_EQ(count_people_at(db, successor), 6)
        << "the successor generation reads the post-commit state";
    db.release_read_generation(successor);
    db.test_hook_generation_query_gap_ = nullptr;
}

// The finding-1 fix must not regress steady-state pooling: a QUIESCENT
// retire (no in-flight statements) still returns the keeper to the idle
// pool, and the next acquisition reuses it — no open/close churn (§2.5).
TEST(ReadGeneration, QuiescentRetirePoolsConnection) {
    TempDB tmp("gen_pool_reuse");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"x", 1, std::nullopt});

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    EXPECT_TRUE(db.query_at_generation(gen, "SELECT 1 AS one").has_value());
    EXPECT_EQ(db.idle_read_pool_size(), 0u);

    db.release_read_generation(gen);
    EXPECT_EQ(db.idle_read_pool_size(), 1u)
        << "a quiescent retire must pool the keeper connection";

    auto next = db.acquire_read_generation();
    ASSERT_NE(next, 0u);
    EXPECT_EQ(db.idle_read_pool_size(), 0u)
        << "the next acquisition must reuse the pooled connection";
    EXPECT_TRUE(db.query_at_generation(next, "SELECT 1 AS one").has_value());
    db.release_read_generation(next);
}

// §3.4 hard bound: under a synthetic write burst with a keeper treadmill
// (always a live generation, re-pinned after every eviction — the regime
// where the log can otherwise NEVER rewind), the WAL stays a sawtooth
// bounded by threshold + one detection window, and the post-burst gap
// truncates it.
TEST(ReadGeneration, WalThresholdEvictionOpensReaderGap) {
    TempDB tmp("gen_walcap");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"seed", 0, std::nullopt});

    const int64_t threshold = 1 << 20;  // 1 MB — small for test speed
    db.set_wal_keeper_eviction_threshold_bytes(threshold);

    auto gen = db.acquire_read_generation();
    ASSERT_NE(gen, 0u);

    const std::string payload(2048, 'x');
    int64_t max_wal = 0;
    for (int i = 0; i < 600; ++i) {
        db.add(TestPerson{payload, i, std::nullopt});
        if (i % 10 == 9) {
            // Detection window: one maintenance tick per 10 commits.
            max_wal = std::max(max_wal, wal_file_size(tmp));
            db.run_read_pool_maintenance();
            if (db.local_read_generations_outstanding() == 0) {
                gen = db.acquire_read_generation();  // treadmill re-pin
                ASSERT_NE(gen, 0u);
            }
        }
    }
    // Burst over: retire and let the gap truncate.
    db.retire_all_read_generations();
    db.run_read_pool_maintenance();
    max_wal = std::max(max_wal, wal_file_size(tmp));

    EXPECT_LE(max_wal, 2 * threshold)
        << "WAL high-water must stay within threshold + one detection window";
    EXPECT_LE(wal_file_size(tmp), threshold)
        << "the coordinated reader gap must rewind/truncate the log";
    EXPECT_FALSE(db.wal_eviction_pending());
}

// T11 — cross-instance policing (§3.3, adversarial finding #4): the keeper
// lives on a DIFFERENT same-path instance than the one running the policy
// (the synchronizer owns its own lattice_db). Outstanding counts aggregate
// per path; the advance request reaches the holder's hooks; TRUNCATE
// succeeds on the next cycle once the holder re-pins.
TEST(ReadGeneration, CrossInstancePolicingAggregatesAndAdvances) {
    TempDB tmp("gen_xpolice");
    lattice_db app{configuration(tmp.str())};      // holds the keeper
    lattice_db pacer_side{configuration(tmp.str())};  // synchronizer-topology instance
    app.add(TestPerson{"x", 1, std::nullopt});

    auto gen = app.acquire_read_generation();
    ASSERT_NE(gen, 0u);

    // Policy reads aggregate per path — NOT per instance.
    EXPECT_EQ(pacer_side.local_read_generations_outstanding(), 0u);
    EXPECT_EQ(pacer_side.read_generations_outstanding(), 1u)
        << "outstanding must aggregate across same-path instances";

    // Give the checkpoint frames beyond the keeper's read-mark, then get
    // beaten by it — the §3.3 busy path.
    pacer_side.add(TestPerson{"y", 2, std::nullopt});
    auto res = pacer_side.db().wal_checkpoint(/*truncate=*/true, /*busy_budget_ms=*/50);
    EXPECT_NE(res.busy, 0) << "a held keeper snapshot must beat TRUNCATE";

    // The advance request is delivered through the invalidation-hook path,
    // fanned per path — it must reach the HOLDER's coordinator.
    std::atomic<int> advance_fires{0};
    app.add_invalidation_hook([&](const std::vector<std::string>&, inv_reason reason) {
        if (reason == inv_reason::advance) advance_fires.fetch_add(1, std::memory_order_relaxed);
    });
    pacer_side.request_generation_advance();
    EXPECT_EQ(advance_fires.load(), 1);

    // Coordinator reaction (re-pin == release + next-access acquire): the
    // NEXT cycle truncates behind it.
    app.release_read_generation(gen);
    EXPECT_EQ(pacer_side.read_generations_outstanding(), 0u);
    res = pacer_side.db().wal_checkpoint(/*truncate=*/true, /*busy_budget_ms=*/250);
    EXPECT_EQ(res.busy, 0) << "TRUNCATE must succeed once the keeper released";
}

// Item-A adversarial finding 2: the WAL-threshold is consumed by each
// instance's OWN WAL hook, and the commits that grow the WAL fastest arrive
// through OTHER same-path instances (the synchronizer's dedicated
// lattice_db applying sync chunks, IPC instances, second app handles) — so
// the setter must fan out per path, and instances opened later must adopt
// the per-path value. Pinned exactly as specified: set on instance A, write
// through same-path instance B, B's wal_eviction_pending flips at A's
// threshold.
TEST(ReadGeneration, WalEvictionThresholdPropagatesAcrossSamePathInstances) {
    TempDB tmp("gen_thresh_prop");
    lattice_db a{configuration(tmp.str())};  // "app" handle: receives the setting
    lattice_db b{configuration(tmp.str())};  // pre-existing sibling (sync topology)

    const int64_t threshold = 64 * 1024;
    a.set_wal_keeper_eviction_threshold_bytes(threshold);
    EXPECT_EQ(b.wal_keeper_eviction_threshold_bytes(), threshold)
        << "the setter must fan out to alive same-path instances";

    lattice_db c{configuration(tmp.str())};  // opened AFTER the set
    EXPECT_EQ(c.wal_keeper_eviction_threshold_bytes(), threshold)
        << "instances opened later must adopt the per-path value";

    // Keeper on A pins the log so it can only grow; commits through B must
    // trip B's OWN WAL hook at A's threshold.
    auto gen = a.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    const std::string payload(4096, 'x');
    for (int i = 0; i < 64 && !b.wal_eviction_pending(); ++i) {
        b.add(TestPerson{payload, i, std::nullopt});
    }
    EXPECT_TRUE(b.wal_eviction_pending())
        << "commits through a sibling instance must trip the propagated threshold";

    // The pending eviction is serviced per path from any instance's
    // maintenance: keepers retire, the gap truncates, ALL flags clear.
    a.release_read_generation(gen);
    a.run_read_pool_maintenance();
    EXPECT_FALSE(b.wal_eviction_pending());
    EXPECT_LE(wal_file_size(tmp), threshold)
        << "the coordinated reader gap must rewind/truncate the log";
}

// Isolated `:memory:` instances share the literal ":memory:" registry key
// WITHOUT sharing storage — per-path fan-out must not leak tunables between
// them (they have no WAL either way; this guards the registry keying).
TEST(ReadGeneration, WalEvictionThresholdIsolatedMemoryStaysLocal) {
    lattice_db mem_a;
    lattice_db mem_b;
    const auto default_threshold = mem_b.wal_keeper_eviction_threshold_bytes();
    ASSERT_NE(default_threshold, 123);

    mem_a.set_wal_keeper_eviction_threshold_bytes(123);
    EXPECT_EQ(mem_a.wal_keeper_eviction_threshold_bytes(), 123);
    EXPECT_EQ(mem_b.wal_keeper_eviction_threshold_bytes(), default_threshold)
        << "isolated :memory: instances must not leak tunables through the shared registry key";

    lattice_db mem_c;
    EXPECT_EQ(mem_c.wal_keeper_eviction_threshold_bytes(), default_threshold)
        << "isolated :memory: instances must not adopt another store's tunables at open";
}

// Close ordering (§4.6): generations retire before connection teardown;
// post-close generation reads return empty — no UAF, no throw, no new
// acquisitions.
TEST(ReadGeneration, ReadsAfterCloseReturnEmpty) {
    TempDB tmp("gen_close");
    auto db = std::make_unique<lattice_db>(configuration(tmp.str()));
    db->add(TestPerson{"x", 1, std::nullopt});
    auto gen = db->acquire_read_generation();
    ASSERT_NE(gen, 0u);

    db->close();

    EXPECT_FALSE(db->query_at_generation(gen, "SELECT 1 AS one").has_value());
    EXPECT_EQ(db->acquire_read_generation(), 0u);
    EXPECT_EQ(db->local_read_generations_outstanding(), 0u);
    db.reset();  // destructor after close(): idempotent teardown
}

// A generation read racing close() on another thread must come back empty or
// with data — never crash, never throw (§4.6: in-flight reads hold the
// keeper wrapper via shared_ptr; logical close short-circuits).
TEST(ReadGeneration, CloseWhileReadInFlightIsContained) {
    TempDB tmp("gen_close_race");
    auto db = std::make_unique<lattice_db>(configuration(tmp.str()));
    db->add(TestPerson{"x", 1, std::nullopt});
    auto gen = db->acquire_read_generation();
    ASSERT_NE(gen, 0u);

    std::atomic<bool> started{false};
    std::atomic<bool> reader_done{false};
    std::thread reader([&] {
        started.store(true, std::memory_order_release);
        (void)db->query_at_generation(gen, kSlowQuery);  // interrupted by close's retire-all
        reader_done.store(true, std::memory_order_release);
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    db->close();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!reader_done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!reader_done.load(std::memory_order_acquire)) {
        reader.detach();
        FAIL() << "generation read wedged across close()";
    }
    reader.join();
    db.reset();
}

// ----------------------------------------------------------------------------
// Per-store write gate (§4.1 mechanism 2)
// ----------------------------------------------------------------------------

// Chunked writer transactions vs concurrent capture transactions on a named
// shared-cache store: with the gate, zero surfaced SQLITE_LOCKED in either
// direction (a capture's table read locks would otherwise fail the writer's
// in-transaction statements immediately, and vice versa), bounded watchdog.
TEST(SharedCacheGate, ChunkedWritesVsCapturesNoLockedErrors) {
    const auto uri = gen_mem_uri("gate_capture");
    lattice_db writer{configuration(uri)};
    lattice_db reader{configuration(uri)};
    writer.add(TestPerson{"seed", 0, std::nullopt});

    ASSERT_NE(writer.store_write_gate(), nullptr) << "shared-cache stores must publish a gate";
    ASSERT_EQ(writer.store_write_gate(), reader.store_write_gate())
        << "same-path handles must share ONE per-path gate";
    lattice_db isolated;  // plain :memory: — no cross-connection hazard
    EXPECT_EQ(isolated.store_write_gate(), nullptr);

    std::atomic<bool> stop{false};
    std::atomic<bool> writer_done{false};
    std::atomic<int> writer_errors{0};
    std::atomic<int> capture_errors{0};
    std::atomic<int> captures_completed{0};

    // The §4.1 capture shape (what Commit 4's query_ids_at does): hold the
    // gate, run the id scan inside a capture transaction on the sibling
    // handle's own connection.
    std::thread capture_thread([&] {
        while (!stop.load(std::memory_order_acquire)) {
            try {
                lattice_db::store_write_gate_hold gate(reader);
                reader.db().execute("BEGIN");
                (void)reader.db().query("SELECT id FROM TestPerson ORDER BY age, id");
                reader.db().execute("COMMIT");
                captures_completed.fetch_add(1, std::memory_order_relaxed);
            } catch (const std::exception&) {
                capture_errors.fetch_add(1, std::memory_order_relaxed);
                try {
                    if (reader.db().is_in_transaction()) reader.db().execute("ROLLBACK");
                } catch (...) {}
            }
        }
    });

    std::thread writer_thread([&] {
        try {
            for (int chunk = 0; chunk < 50; ++chunk) {
                writer.write([&] {
                    for (int i = 0; i < 20; ++i) {
                        writer.add(TestPerson{"w", chunk * 20 + i, std::nullopt});
                    }
                });
            }
        } catch (const std::exception&) {
            writer_errors.fetch_add(1, std::memory_order_relaxed);
        }
        writer_done.store(true, std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!writer_done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    stop.store(true, std::memory_order_release);
    if (!writer_done.load(std::memory_order_acquire)) {
        writer_thread.detach();
        capture_thread.detach();
        FAIL() << "gate deadlocked writer vs captures";
    }
    writer_thread.join();
    capture_thread.join();

    EXPECT_EQ(writer_errors.load(), 0)
        << "capture read locks must never surface SQLITE_LOCKED to the writer";
    EXPECT_EQ(capture_errors.load(), 0)
        << "writer transactions must never surface SQLITE_LOCKED to a capture";
    EXPECT_GT(captures_completed.load(), 0);
    auto final_count = writer.db().query("SELECT COUNT(*) AS c FROM TestPerson");
    EXPECT_EQ(std::get<int64_t>(final_count[0].at("c")), 1 + 50 * 20);
}
