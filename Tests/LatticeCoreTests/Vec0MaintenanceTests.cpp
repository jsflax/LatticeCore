#include "Vec0MaintenanceTestSupport.hpp"

using namespace vec0_maintenance_test;

// Core-only: these cases also compile into Linux, where the Swift bridge
// SyncOnlyVecReconcile cases are intentionally absent from the existing target.
TEST(Vec0Maintenance, MemoryCallbacksRunAfterStatementUnlockAndRejectMaintenanceReentry) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    ASSERT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));

    int callback_count = 0;
    int mutex_result = -1;
    int64_t count_at_callback = -1;
    int64_t callback_vacuum_result = -2;
    std::string callback_error;
    uint64_t nested_statement_count = 1;
    const auto observer = db.add_table_observer("_MaintenanceDoc_embedding_vec_rowids",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            if (++callback_count != 1) return;
            try {
                // A same-thread try would succeed recursively even while
                // locked. This separate thread proves the writer was released.
                mutex_result = probe_writer_mutex_from_another_thread(db);
                count_at_callback = memory_index_count(db);
                // The statement has settled, but the memory maintenance gate
                // still protects the outer operation. Recursive destruction
                // must reject before any SQL despite SQLite already being free.
                const auto before = lattice::database::thread_statement_count();
                callback_vacuum_result = db.vacuum_vec0("MaintenanceDoc", "embedding");
                nested_statement_count = lattice::database::thread_statement_count() - before;
            } catch (const std::exception& e) {
                callback_error = e.what();
            }
        });

    joined_task<int64_t> outer([&] {
        return db.vacuum_vec0("MaintenanceDoc", "embedding");
    });
    EXPECT_TRUE(outer.ready()) << "memory maintenance/delivery did not finish within budget";
    outer.join();
    EXPECT_EQ(outer.get(), 2);
    db.remove_table_observer("_MaintenanceDoc_embedding_vec_rowids", observer);

    EXPECT_GE(callback_count, 1);
    EXPECT_EQ(callback_error, "");
    EXPECT_EQ(mutex_result, SQLITE_OK);
    EXPECT_EQ(count_at_callback, 1) << "memory observers retain per-statement delivery";
    EXPECT_EQ(callback_vacuum_result, -1) << "the outer maintenance frame remains active";
    EXPECT_EQ(nested_statement_count, 0u);
    EXPECT_EQ(memory_index_count(db), 2);
    EXPECT_EQ(db.vacuum_vec0("MaintenanceDoc", "embedding"), 2);
}

TEST(Vec0Maintenance, FailedMaintenanceReleasesWriterAndAllowsRecovery) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    ASSERT_EQ(db.vacuum_vec0("MaintenanceDoc", "embedding"), 2);
    bool injected = false;
    lattice::vec0_maintenance_test_access::set(db,
        [&](const char* operation, const char* phase) {
            if (std::string(operation) == "vacuum" && std::string(phase) == "after-drop") {
                injected = true;
                throw lattice::db_error("test maintenance failure after drop");
            }
        });

    joined_task<int64_t> failed([&] {
        return db.vacuum_vec0("MaintenanceDoc", "embedding");
    });
    EXPECT_TRUE(failed.ready()) << "failed maintenance did not unwind within budget";
    failed.join();
    EXPECT_EQ(failed.get(), -1);
    lattice::vec0_maintenance_test_access::set(db, {});

    EXPECT_TRUE(injected);
    EXPECT_EQ(probe_writer_mutex_from_another_thread(db), SQLITE_OK);
    EXPECT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
    EXPECT_EQ(db.vacuum_vec0("MaintenanceDoc", "embedding"), 2);
    EXPECT_EQ(memory_index_count(db), 2);
}

TEST(Vec0Maintenance, MemoryMultirowStatementDeliversOneCompleteIdentityBatch) {
    lattice::lattice_db db;
    int callbacks = 0;
    std::vector<std::string> observed_ids;
    std::vector<int> transaction_states, mutex_results;
    const auto observer = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            ++callbacks;
            transaction_states.push_back(sqlite3_txn_state(db.db().handle(), nullptr));
            mutex_results.push_back(probe_writer_mutex_from_another_thread(db));
            for (const auto& event : events) observed_ids.push_back(std::get<3>(event));
        });

    // The real model update hook queries globalId for the second row while
    // the first row is already dirty. Autocommit alone does not prove the
    // outer INSERT has finished; no callback may escape that nested query.
    lattice::vec0_maintenance_test_access::run(db, [&] {
        db.db().execute(
            "INSERT INTO TestPerson (globalId, name, age) VALUES "
            "('multi-one', 'first', 1), ('multi-two', 'second', 2)");
    });
    db.remove_table_observer("TestPerson", observer);

    EXPECT_EQ(callbacks, 1);
    std::sort(observed_ids.begin(), observed_ids.end());
    EXPECT_EQ(observed_ids, (std::vector<std::string>{"multi-one", "multi-two"}));
    EXPECT_EQ(transaction_states, (std::vector<int>{SQLITE_TXN_NONE}));
    EXPECT_EQ(mutex_results, (std::vector<int>{SQLITE_OK}));
}

TEST(Vec0Maintenance, MemoryCommittedNotificationSurvivesLaterRealRollback) {
    lattice::lattice_db db;
    db.db().execute(
        "CREATE TEMP TRIGGER maintenance_rollback BEFORE INSERT ON TestPerson "
        "WHEN NEW.name = 'fault' BEGIN "
        "SELECT RAISE(ROLLBACK, 'maintenance-real-rollback'); END");
    std::vector<std::string> observed_ids;
    const auto observer = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>& events) {
            for (const auto& event : events) observed_ids.push_back(std::get<3>(event));
        });

    bool first_committed = false;
    bool second_inserted = false;
    std::string error;
    try {
        lattice::vec0_maintenance_test_access::run(db, [&] {
            db.db().execute(
                "INSERT INTO TestPerson (globalId, name, age) VALUES ('kept', 'first', 1)");
            first_committed = !db.db().is_in_transaction();
            db.db().begin_transaction();
            db.db().execute(
                "INSERT INTO TestPerson (globalId, name, age) VALUES ('rolled-back', 'second', 2)");
            second_inserted = true;
            db.db().execute(
                "INSERT INTO TestPerson (globalId, name, age) VALUES ('fault-row', 'fault', 3)");
        });
    } catch (const std::exception& e) {
        error = e.what();
    }

    EXPECT_TRUE(first_committed);
    EXPECT_TRUE(second_inserted);
    EXPECT_NE(error.find("maintenance-real-rollback"), std::string::npos);
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(probe_writer_mutex_from_another_thread(db), SQLITE_OK);
    EXPECT_EQ(observed_ids, (std::vector<std::string>{"kept"}))
        << "earlier committed notification survives; rolled-back rows never escape";
    const auto rows = db.db().query("SELECT globalId FROM TestPerson ORDER BY globalId");
    EXPECT_EQ(rows.size(), 1u);
    if (!rows.empty()) EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")), "kept");

    lattice::vec0_maintenance_test_access::run(db, [&] {
        db.db().execute(
            "INSERT INTO TestPerson (globalId, name, age) VALUES ('recovered', 'recovery', 4)");
    });
    EXPECT_EQ(observed_ids, (std::vector<std::string>{"kept", "recovered"}));
    db.remove_table_observer("TestPerson", observer);
}

TEST(Vec0Maintenance, RawDatabaseSettlesAfterStatementAndPreservesCommittedDelivery) {
    lattice::database db(":memory:");
    db.execute("CREATE TABLE RawProbe (value INTEGER)");
    db.execute(
        "CREATE TEMP TRIGGER raw_rollback BEFORE INSERT ON RawProbe WHEN NEW.value = 3 "
        "BEGIN SELECT RAISE(ROLLBACK, 'raw-real-rollback'); END");
    int settled = 0;
    int rollbacks = 0;
    std::vector<int> transaction_states, mutex_results;
    db.set_txn_hooks([&] {
        ++settled;
        transaction_states.push_back(sqlite3_txn_state(db.handle(), nullptr));
        mutex_results.push_back(probe_writer_mutex_from_another_thread(db));
    }, [&] { ++rollbacks; });
    struct update_context {
        lattice::database* db;
        int calls = 0;
    } context{&db};
    sqlite3_update_hook(db.handle(), [](void* opaque, int, const char*, const char*, sqlite3_int64) {
        auto& context = *static_cast<update_context*>(opaque);
        ++context.calls;
        context.db->mark_txn_dirty();
        // Raw hooks only arm dirty state. Nested raw-hook SQL is outside the
        // library-owned setup_change_hook scope exercised by the test above.
    }, &context);

    db.execute("INSERT INTO RawProbe VALUES (1)");
    EXPECT_EQ(settled, 1);
    EXPECT_EQ(transaction_states, (std::vector<int>{SQLITE_TXN_NONE}));
    EXPECT_EQ(mutex_results, (std::vector<int>{SQLITE_OK}));
    std::string error;
    try {
        db.begin_transaction();
        db.execute("INSERT INTO RawProbe VALUES (2)");
        db.execute("INSERT INTO RawProbe VALUES (3)");
    } catch (const std::exception& e) {
        error = e.what();
    }
    EXPECT_NE(error.find("raw-real-rollback"), std::string::npos);
    EXPECT_GE(rollbacks, 1);
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(settled, 1) << "rolled-back writes must not produce settled delivery";
    const auto rows = db.query("SELECT value FROM RawProbe ORDER BY value");
    EXPECT_EQ(rows.size(), 1u);
    if (!rows.empty()) EXPECT_EQ(std::get<int64_t>(rows[0].at("value")), 1);

    db.execute("INSERT INTO RawProbe VALUES (4)");
    EXPECT_EQ(settled, 2);
    EXPECT_EQ(transaction_states, (std::vector<int>{SQLITE_TXN_NONE, SQLITE_TXN_NONE}));
    EXPECT_EQ(mutex_results, (std::vector<int>{SQLITE_OK, SQLITE_OK}));
    EXPECT_EQ(context.calls, 3);
    sqlite3_update_hook(db.handle(), nullptr, nullptr);
    db.set_txn_hooks({}, {});
}
