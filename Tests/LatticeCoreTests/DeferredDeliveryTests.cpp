#include "TestHelpers.hpp"
#include <lattice/sync.hpp>

// ============================================================================
// Transaction-settled observer delivery for in-memory lattices
// (docs/design-deferred-memory-delivery.md). Memory/Emscripten DBs no longer
// deliver from inside sqlite3_update_hook: the hook only buffers + marks the
// connection dirty, and the db.cpp statement wrappers drain via
// flush_changes() once the transaction settles (autocommit restored) — the
// same post-commit point the WAL hook gives file DBs. Also pins the DEFECT-2
// fix: sync mark-synced notifications fan out to every same-name shared-cache
// handle (NamedMemory.MarkSynced* below) instead of only the local instance.
// ============================================================================

namespace {
std::string mem_uri(const std::string& name) {
    return "file:" + name + "?mode=memory&cache=shared";
}
} // namespace

// The required repro (DEFECT 1): before the fix, delivery ran per statement
// inside the writer's open transaction, so an observer reading through a
// second same-name shared-cache handle hit SQLITE_LOCKED (shared-cache table
// locks ignore the busy timeout) and the resulting db_error unwound through
// SQLite's C frames — UB. After the fix: exactly one post-commit batch, and
// reads through the second handle see the full committed state.
TEST(DeferredDelivery, ObserverReadsThroughSecondHandleDuringMultiWriteTxn) {
    const auto uri = mem_uri("dd_txn_read");
    lattice::lattice_db a{lattice::configuration(uri)};   // writer
    lattice::lattice_db b{lattice::configuration(uri)};   // observer, same name

    std::atomic<int> fires{0};
    std::atomic<int> events_in_first_fire{0};
    std::atomic<bool> writer_in_txn_at_fire{true};
    std::atomic<int64_t> count_at_fire{-1};
    std::mutex error_mutex;
    std::string callback_error;

    b.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            if (fires.fetch_add(1, std::memory_order_relaxed) == 0) {
                events_in_first_fire.store(static_cast<int>(events.size()),
                                           std::memory_order_relaxed);
            }
            try {
                // Reads through b's OWN connection to the shared-cache store.
                // Pre-fix these ran inside a's open transaction and failed
                // immediately with SQLITE_LOCKED.
                auto rows = b.db().query("SELECT COUNT(*) AS c FROM TestPerson");
                if (!rows.empty()) {
                    count_at_fire.store(std::get<int64_t>(rows[0].at("c")),
                                        std::memory_order_relaxed);
                }
                for (const auto& ev : events) {
                    const auto& gid = std::get<3>(ev);
                    if (!gid.empty()) {
                        (void)b.find_by_global_id<TestPerson>(gid);
                    }
                }
                writer_in_txn_at_fire.store(a.db().is_in_transaction(),
                                            std::memory_order_relaxed);
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(error_mutex);
                callback_error = e.what();
            }
        });

    // Watchdog: run the transaction on a worker so a delivery deadlock fails
    // the test (bounded poll) instead of hanging the whole suite.
    std::atomic<bool> done{false};
    std::thread writer([&] {
        a.write([&] {
            a.add(TestPerson{"p1", 1, std::nullopt});
            a.add(TestPerson{"p2", 2, std::nullopt});
            a.add(TestPerson{"p3", 3, std::nullopt});
        });
        done.store(true, std::memory_order_release);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!done.load(std::memory_order_acquire)) {
        writer.detach();
        FAIL() << "multi-write transaction deadlocked against observer delivery";
    }
    writer.join();

    {
        std::lock_guard<std::mutex> lock(error_mutex);
        EXPECT_EQ(callback_error, "") << "observer read through second handle threw";
    }
    EXPECT_EQ(fires.load(), 1) << "one explicit transaction must be exactly one fire";
    EXPECT_EQ(events_in_first_fire.load(), 3);
    EXPECT_FALSE(writer_in_txn_at_fire.load()) << "delivery must be post-commit";
    EXPECT_EQ(count_at_fire.load(), 3) << "observer must read the full committed state";
}

// One explicit transaction = one fire per (observer × table), events in
// commit order; a subsequent autocommit add() is one more single-event fire.
// This is the documented add_table_observer contract — file DBs always had it
// via the WAL hook; memory DBs get it from the transaction-settled drain.
TEST(DeferredDelivery, PerTransactionBatchingAndOrder) {
    lattice::lattice_db db;

    std::vector<std::vector<lattice::lattice_db::change_event>> person_batches;
    std::vector<std::vector<lattice::lattice_db::change_event>> audit_batches;
    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            person_batches.push_back(events);
        });
    db.add_table_observer("AuditLog",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            audit_batches.push_back(events);
        });

    db.write([&] {
        db.add(TestPerson{"A", 1, std::nullopt});
        db.add(TestPerson{"B", 2, std::nullopt});
        db.add(TestPerson{"C", 3, std::nullopt});
    });

    ASSERT_EQ(person_batches.size(), 1u) << "one transaction must be one fire";
    ASSERT_EQ(person_batches[0].size(), 3u);
    for (const auto& ev : person_batches[0]) {
        EXPECT_EQ(std::get<1>(ev), "INSERT");
    }
    // Buffer order is update-hook firing order is commit order: rowids ascend.
    EXPECT_LT(std::get<2>(person_batches[0][0]), std::get<2>(person_batches[0][1]));
    EXPECT_LT(std::get<2>(person_batches[0][1]), std::get<2>(person_batches[0][2]));

    ASSERT_EQ(audit_batches.size(), 1u) << "AuditLog slice must arrive in the same single fire";
    ASSERT_EQ(audit_batches[0].size(), 3u);
    for (const auto& ev : audit_batches[0]) {
        EXPECT_EQ(std::get<1>(ev), "INSERT");
        // globalId is resolved at flush time (backfill) — the hook buffers
        // AuditLog INSERTs with an empty gid.
        EXPECT_FALSE(std::get<3>(ev).empty()) << "AuditLog globalId backfill failed";
    }
    EXPECT_LT(std::get<2>(audit_batches[0][0]), std::get<2>(audit_batches[0][1]));
    EXPECT_LT(std::get<2>(audit_batches[0][1]), std::get<2>(audit_batches[0][2]));

    db.add(TestPerson{"D", 4, std::nullopt});
    ASSERT_EQ(person_batches.size(), 2u) << "autocommit add must be exactly one more fire";
    EXPECT_EQ(person_batches[1].size(), 1u);
}

// Pins promptness: with the (default) immediate scheduler, an autocommit
// add() delivers before add() returns — the drain runs inside the statement
// wrapper's tail. Guards against any future scheduler-hop regression.
TEST(DeferredDelivery, AutocommitDeliversBeforeAddReturns) {
    lattice::lattice_db db;

    bool fired = false;
    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) { fired = true; });

    db.add(TestPerson{"prompt", 1, std::nullopt});
    EXPECT_TRUE(fired) << "immediate scheduler must deliver before add() returns";
}

namespace {
// Shared body for the rollback-silence pin (memory + file variants): a
// rolled-back transaction delivers nothing, and the NEXT committed write
// carries exactly its own event — no phantoms from the discarded buffer.
void expect_rollback_silence(lattice::lattice_db& db) {
    // Ensure the model table exists outside the doomed transaction so the
    // rollback covers DML only.
    db.add(TestPerson{"pre-existing", 0, std::nullopt});

    std::vector<std::vector<lattice::lattice_db::change_event>> batches;
    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            batches.push_back(events);
        });

    db.begin_transaction();
    db.add(TestPerson{"doomed-1", 1, std::nullopt});
    db.add(TestPerson{"doomed-2", 2, std::nullopt});
    db.rollback();
    EXPECT_TRUE(batches.empty()) << "rolled-back transaction must deliver nothing";

    // The next commit is the flush point that (pre-fix, file DBs) delivered
    // the rolled-back rows as phantoms.
    db.add(TestPerson{"survivor", 3, std::nullopt});
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].size(), 1u) << "phantom events from the rolled-back txn leaked";
    EXPECT_EQ(std::get<1>(batches[0][0]), "INSERT");
}
} // namespace

TEST(DeferredDelivery, RollbackDeliversNothingMemory) {
    lattice::lattice_db db{lattice::configuration(mem_uri("dd_rollback"))};
    expect_rollback_silence(db);
}

// Same body against a file lattice — pins the sqlite3_rollback_hook buffer
// discard for the latent file-DB phantom-notification bug (design doc §2.4).
TEST(DeferredDelivery, RollbackDeliversNothingFile) {
    TempDB tmp{"dd_rollback_file"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    expect_rollback_silence(db);
}

// A write from inside an observer callback lands in the buffer while
// is_flushing_ suppresses the nested flush; the bounded drain-until-empty
// loop must deliver it as a second fire instead of stranding it until the
// next unrelated write (pre-existing bug fixed by the design).
TEST(DeferredDelivery, ObserverWritebackIsDeliveredNotStranded) {
    lattice::lattice_db db;

    std::vector<size_t> batch_sizes;
    int fires = 0;
    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            batch_sizes.push_back(events.size());
            if (++fires == 1) {
                db.add(TestPerson{"written-back", 2, std::nullopt});
            }
        });

    db.add(TestPerson{"original", 1, std::nullopt});

    EXPECT_EQ(fires, 2) << "observer write-back must be delivered, not stranded";
    ASSERT_EQ(batch_sizes.size(), 2u);
    EXPECT_EQ(batch_sizes[0], 1u);
    EXPECT_EQ(batch_sizes[1], 1u);
}

// Observer exceptions now unwind through plain C++ frames (the statement
// wrapper's drain) to the writer's add() call — after the commit durably
// happened — instead of crossing SQLite's C frames (UB). Delivery state must
// not wedge: the next write still delivers.
TEST(DeferredDelivery, ExceptionFromObserverSurfacesToWriter) {
    lattice::lattice_db db;

    int fires = 0;
    db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            if (++fires == 1) throw std::runtime_error("observer boom");
        });

    bool caught = false;
    try {
        db.add(TestPerson{"thrower", 1, std::nullopt});
    } catch (const std::runtime_error& e) {
        caught = (std::string(e.what()) == "observer boom");
    }
    EXPECT_TRUE(caught) << "observer exception must surface at the add() call site";

    // The write committed before delivery began.
    auto rows = db.db().query("SELECT COUNT(*) AS c FROM TestPerson");
    ASSERT_FALSE(rows.empty());
    EXPECT_EQ(std::get<int64_t>(rows[0].at("c")), 1);

    // Buffer/flags must not be wedged by the unwind.
    db.add(TestPerson{"after", 2, std::nullopt});
    EXPECT_EQ(fires, 2) << "delivery wedged after observer exception";
}

// ============================================================================
// DEFECT 2 — mark-synced fan-out. These extend the NamedMemory suite
// (NamedMemoryTests.cpp): sync's notify_observers must use the same
// storage_shared_across_instances() predicate as flush_changes.
// ============================================================================

namespace {
// Fetch the newest AuditLog globalId (the entry to mark synced).
std::string latest_audit_gid(lattice::lattice_db& db) {
    auto rows = db.db().query("SELECT globalId FROM AuditLog ORDER BY id DESC LIMIT 1");
    return rows.empty() ? "" : std::get<std::string>(rows[0].at("globalId"));
}

// Observer that counts AuditLog UPDATE events for one specific globalId.
std::function<void(const std::vector<lattice::lattice_db::change_event>&)>
count_updates_for(const std::string& gid, std::atomic<int>& counter) {
    return [gid, &counter](const std::vector<lattice::lattice_db::change_event>& events) {
        for (const auto& ev : events) {
            if (std::get<1>(ev) == "UPDATE" && std::get<3>(ev) == gid) {
                counter.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };
}
} // namespace

// Fails before the fix: notify_observers delivered mark-synced AuditLog
// UPDATEs only to the local instance for ANY memory DB, so same-name
// shared-cache siblings (e.g. the app handle observing sync progress while
// the sync handle marks entries) never heard isSynchronized=1.
TEST(NamedMemory, MarkSyncedFansOutToAllHandles) {
    const auto uri = mem_uri("dd_marksync_fan");
    lattice::lattice_db a{lattice::configuration(uri)};
    lattice::lattice_db b{lattice::configuration(uri)};

    a.add(TestPerson{"synced", 1, std::nullopt});
    const auto gid = latest_audit_gid(a);
    ASSERT_FALSE(gid.empty());

    // Register AFTER the write so only the mark-synced UPDATE is counted.
    std::atomic<int> updates_a{0}, updates_b{0};
    a.add_table_observer("AuditLog", count_updates_for(gid, updates_a));
    b.add_table_observer("AuditLog", count_updates_for(gid, updates_b));

    lattice::mark_audit_entries_synced(a, {gid});

    EXPECT_EQ(updates_a.load(), 1) << "marking handle must hear the UPDATE exactly once";
    EXPECT_EQ(updates_b.load(), 1) << "sibling same-name handle must hear the UPDATE exactly once";
}

// Guards the fan-out predicate: plain :memory: instances collide on the
// registry key (":memory:") but have ISOLATED storage — naive registry
// fan-out would notify b about a row that doesn't exist in its database.
TEST(NamedMemory, MarkSyncedIsolatedMemoryDoesNotCrossNotify) {
    lattice::lattice_db a{lattice::configuration(":memory:")};
    lattice::lattice_db b{lattice::configuration(":memory:")};

    a.add(TestPerson{"iso", 1, std::nullopt});
    const auto gid = latest_audit_gid(a);
    ASSERT_FALSE(gid.empty());

    std::atomic<int> fired_b{0};
    b.add_table_observer("AuditLog",
        [&fired_b](const std::vector<lattice::lattice_db::change_event>&) {
            fired_b.fetch_add(1, std::memory_order_relaxed);
        });

    lattice::mark_audit_entries_synced(a, {gid});

    EXPECT_EQ(fired_b.load(), 0) << "isolated :memory: instances must not cross-notify";
}

// The nested mark-synced path (caller already holds the transaction, e.g.
// receive_sync_data): delivering inline would be a DEFECT-1-shaped in-txn
// cross-connection notification. It must ride the drain at the caller's
// commit instead — nothing before, both handles exactly once after.
TEST(NamedMemory, MarkSyncedInsideCallerTransactionDefersToCommit) {
    const auto uri = mem_uri("dd_marksync_txn");
    lattice::lattice_db a{lattice::configuration(uri)};
    lattice::lattice_db b{lattice::configuration(uri)};

    a.add(TestPerson{"deferred", 1, std::nullopt});
    const auto gid = latest_audit_gid(a);
    ASSERT_FALSE(gid.empty());

    std::atomic<int> updates_a{0}, updates_b{0};
    a.add_table_observer("AuditLog", count_updates_for(gid, updates_a));
    b.add_table_observer("AuditLog", count_updates_for(gid, updates_b));

    a.begin_transaction();
    lattice::mark_audit_entries_synced(a, {gid});   // nested: joins the open txn
    EXPECT_EQ(updates_a.load(), 0) << "must not deliver while the caller's txn is open";
    EXPECT_EQ(updates_b.load(), 0) << "must not deliver while the caller's txn is open";
    a.commit();

    EXPECT_EQ(updates_a.load(), 1) << "deferred mark-synced must deliver at commit";
    EXPECT_EQ(updates_b.load(), 1) << "deferred mark-synced must fan out at commit";
}
