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

// Released 2.2.2 cold-sync regression controls. These Core-only cases are
// deliberately outside Swift-bridge/platform guards and run on Linux too.
namespace {

const std::string initialization_marker = "vec0_initialized_v1:_MaintenanceDoc_embedding_vec";

int64_t initialization_scalar(lattice::database& db, const std::string& sql) {
    const auto rows = db.query(sql);
    if (rows.size() != 1 || rows[0].size() != 1) throw lattice::db_error("test scalar shape");
    return std::get<int64_t>(rows[0].begin()->second);
}

void initialization_expect_ids(lattice::database& db, const std::vector<std::string>& expected) {
    const auto rows = db.query("SELECT id FROM main._MaintenanceDoc_embedding_vec_rowids ORDER BY id");
    std::vector<std::string> actual;
    for (const auto& row : rows) actual.push_back(std::get<std::string>(row.at("id")));
    EXPECT_EQ(actual, expected);
}

void initialization_expect_marker(lattice::database& db) {
    const auto rows = db.query("SELECT value FROM main._lattice_meta WHERE key=?", {initialization_marker});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("value")),
              std::to_string(initialization_scalar(db, "PRAGMA main.schema_version")));
}

void initialization_remove_marker(lattice::database& db) {
    db.execute("DELETE FROM main._lattice_meta WHERE key=?", {initialization_marker});
}

// Actual SQLite authorization, not an exception standing in for a Native
// failure. A selected CREATE/fill/COMMIT/cleanup operation is denied by C.
struct initialization_authorizer {
    lattice::database& db;
    std::function<bool(int, const char*, const char*)> deny;
    int denied = 0;
    static int callback(void* pointer, int action, const char* first, const char* second,
                        const char*, const char*) noexcept {
        auto& self = *static_cast<initialization_authorizer*>(pointer);
        try {
            if (self.deny && self.deny(action, first, second)) {
                ++self.denied;
                return SQLITE_DENY;
            }
            return SQLITE_OK;
        } catch (...) { return SQLITE_DENY; }
    }
    explicit initialization_authorizer(lattice::database& value) : db(value) {
        if (sqlite3_set_authorizer(db.handle(), &callback, this) != SQLITE_OK)
            throw lattice::db_error("test authorizer installation failed");
    }
    ~initialization_authorizer() { sqlite3_set_authorizer(db.handle(), nullptr, nullptr); }
};

} // namespace

TEST(ColdVectorInitialization, FirstLegacyCosineQueryFillsHistoricalMainRowsAndLaterTriggers) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    ASSERT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
    const auto found = db.knn_query("MaintenanceDoc", "embedding", pack_floats({1, 0, 0, 0}), 1,
                                    lattice::lattice_db::distance_metric::cosine);
    ASSERT_EQ(found.size(), 1u);
    EXPECT_EQ(found[0].global_id, "one");
    EXPECT_LT(found[0].distance, 0.001);
    initialization_expect_ids(db.db(), {"one", "two"});
    initialization_expect_marker(db.db());
    db.db().execute("INSERT INTO MaintenanceDoc(globalId,embedding) VALUES('three',?)",
                    {pack_floats({0, 0, 1, 0})});
    initialization_expect_ids(db.db(), {"one", "three", "two"});
}

TEST(ColdVectorInitialization, PreProtocolEqualCountsStillFillMissingIDsAndRepairAllTriggers) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    // Emulate an existing 2.2.2 sidecar with no transactional initializer.
    // An orphan keeps counts equal while a genuine model ID is missing.
    initialization_remove_marker(db.db());
    db.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    db.db().execute("INSERT INTO _MaintenanceDoc_embedding_vec(global_id,embedding) VALUES('orphan',?)",
                    {pack_floats({0, 0, 0, 1})});
    db.db().execute("DROP TRIGGER _MaintenanceDoc_embedding_vec_update");
    db.db().execute("DROP TRIGGER _MaintenanceDoc_embedding_vec_delete");
    ASSERT_EQ(memory_index_count(db), 2);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    initialization_expect_ids(db.db(), {"one", "orphan", "two"});
    initialization_expect_marker(db.db());
    EXPECT_EQ(initialization_scalar(db.db(), "SELECT COUNT(*) FROM main.sqlite_schema "
        "WHERE type='trigger' AND name IN ('_MaintenanceDoc_embedding_vec_insert',"
        "'_MaintenanceDoc_embedding_vec_update','_MaintenanceDoc_embedding_vec_delete')"), 3);
    const auto changed = pack_floats({0, 0, 1, 0});
    db.db().execute("UPDATE MaintenanceDoc SET embedding=? WHERE globalId='one'", {changed});
    const auto current = db.db().query("SELECT embedding FROM _MaintenanceDoc_embedding_vec WHERE global_id='one'");
    ASSERT_EQ(current.size(), 1u);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(current[0].at("embedding")), changed);
    db.db().execute("DELETE FROM MaintenanceDoc WHERE globalId='two'");
    initialization_expect_ids(db.db(), {"one", "orphan"});
    // Orphan deletion and stale-value auditing are separate maintenance work;
    // initialization repairs missing current main IDs, not all corruption.
}

TEST(ColdVectorInitialization, ActualCreateFillAndCommitFailuresRollbackAndRetryOnSameOwner) {
    for (const auto& failure : std::vector<std::string>{"trigger", "fill", "commit"}) {
        SCOPED_TRACE(failure);
        lattice::lattice_db db;
        create_memory_fixture(db);
        initialization_authorizer fault(db.db());
        bool fill_ready = false;
        lattice::vec0_maintenance_test_access::set(db, [&](const char* operation, const char* phase) {
            if (std::string(operation) == "ensure" && std::string(phase) == "before-fill") fill_ready = true;
        });
        fault.deny = [&](int action, const char* first, const char*) {
            return (failure == "trigger" && action == SQLITE_CREATE_TRIGGER && first &&
                    std::string(first) == "_MaintenanceDoc_embedding_vec_update") ||
                   (failure == "fill" && fill_ready && action == SQLITE_INSERT && first &&
                    std::string(first) == "_MaintenanceDoc_embedding_vec") ||
                   (failure == "commit" && action == SQLITE_TRANSACTION && first && std::string(first) == "COMMIT");
        };
        EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
        lattice::vec0_maintenance_test_access::set(db, {});
        fault.deny = {};
        EXPECT_GT(fault.denied, 0) << "the selected failure must come from actual SQLite authorization";
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
        EXPECT_TRUE(db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}).empty());
        EXPECT_EQ(initialization_scalar(db.db(), "SELECT COUNT(*) FROM MaintenanceDoc"), 2);
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        initialization_expect_ids(db.db(), {"one", "two"});
        initialization_expect_marker(db.db());
    }
}

TEST(ColdVectorInitialization, CallerTransactionOwnsSuccessfulMarkerAndRollback) {
    TempDB path{"cold_vec_outer_transaction"};
    lattice::lattice_db db(path.str());
    create_memory_fixture(db);
    lattice::database other(path.str());
    db.db().begin_transaction();
    db.db().execute("INSERT INTO MaintenanceDoc(globalId,embedding) VALUES('caller',?)",
                    {pack_floats({0, 0, 1, 0})});
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    EXPECT_TRUE(db.db().is_in_transaction()) << "ensure must not commit caller work";
    initialization_expect_marker(db.db());
    EXPECT_FALSE(other.table_exists("_MaintenanceDoc_embedding_vec"));
    EXPECT_EQ(initialization_scalar(other, "SELECT COUNT(*) FROM MaintenanceDoc"), 2);
    db.db().rollback();
    EXPECT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
    EXPECT_TRUE(db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}).empty());
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    initialization_expect_ids(db.db(), {"one", "two"});
}

TEST(ColdVectorInitialization, FailedCallerSavepointPreservesEarlierWorkAndCanRetry) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.db().begin_transaction();
    db.db().execute("INSERT INTO MaintenanceDoc(globalId,embedding) VALUES('caller',?)",
                    {pack_floats({0, 0, 1, 0})});
    lattice::vec0_maintenance_test_access::set(db, [](const char* operation, const char* phase) {
        if (std::string(operation) == "ensure" && std::string(phase) == "after-marker")
            throw lattice::db_error("test failure after transactional marker publication");
    });
    EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
    lattice::vec0_maintenance_test_access::set(db, {});
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
    EXPECT_TRUE(db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}).empty());
    EXPECT_EQ(initialization_scalar(db.db(), "SELECT COUNT(*) FROM MaintenanceDoc WHERE globalId='caller'"), 1);
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    EXPECT_TRUE(db.db().is_in_transaction());
    db.db().commit();
    initialization_expect_ids(db.db(), {"caller", "one", "two"});
    initialization_expect_marker(db.db());
}

TEST(ColdVectorInitialization, FailedCleanupRefusesReuseBeforeFurtherVectorSQL) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    initialization_authorizer fault(db.db());
    fault.deny = [](int action, const char* first, const char*) {
        return action == SQLITE_SAVEPOINT && first && std::string(first) == "ROLLBACK";
    };
    lattice::vec0_maintenance_test_access::set(db, [](const char* operation, const char* phase) {
        if (std::string(operation) == "ensure" && std::string(phase) == "after-fill")
            throw lattice::db_error("test failure requiring savepoint cleanup");
    });
    EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
    lattice::vec0_maintenance_test_access::set(db, {});
    fault.deny = {};
    EXPECT_GT(fault.denied, 0);
    EXPECT_FALSE(db.db().is_in_transaction());
    const auto before = lattice::database::thread_statement_count();
    EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
    EXPECT_EQ(lattice::database::thread_statement_count(), before);
    EXPECT_FALSE(db.db().table_exists("_MaintenanceDoc_embedding_vec"));
}

TEST(ColdVectorInitialization, SeparateFileWriterCannotInterleaveWithCurrentMainBackfill) {
    TempDB path{"cold_vec_interleaved_writer"};
    lattice::configuration config(path.str());
    config.busy_timeout_ms = 2'000;
    lattice::lattice_db db(config);
    create_memory_fixture(db);
    lattice::database other(path.str(), lattice::database::open_mode::read_write, 2'000);
    event_signal held, attempted, release;
    std::atomic<bool> timed_out{false}, writer_committed{false};
    lattice::vec0_maintenance_test_access::set(db, [&](const char* operation, const char* phase) {
        if (std::string(operation) == "ensure" && std::string(phase) == "before-fill") {
            held.send();
            if (!release.wait()) timed_out.store(true);
        }
    });
    initialization_authorizer writer_probe(other);
    writer_probe.deny = [&](int action, const char* first, const char*) {
        if (action == SQLITE_TRANSACTION && first && std::string(first) == "BEGIN") attempted.send();
        return false;
    };
    joined_task<void> initializer([&] { db.ensure_vec0_table("MaintenanceDoc", "embedding", 4); });
    std::unique_ptr<joined_task<void>> writer;
    release_on_exit cleanup{release};
    const bool paused = held.wait();
    EXPECT_TRUE(paused);
    if (paused) {
        writer = std::make_unique<joined_task<void>>([&] {
            other.begin_transaction();
            other.execute("UPDATE MaintenanceDoc SET embedding=? WHERE globalId='one'", {pack_floats({0, 0, 1, 0})});
            other.execute("DELETE FROM MaintenanceDoc WHERE globalId='two'");
            other.execute("INSERT INTO MaintenanceDoc(globalId,embedding) VALUES('three',?)", {pack_floats({0, 0, 0, 1})});
            other.commit();
            writer_committed.store(true);
        });
        EXPECT_TRUE(attempted.wait()) << "contender must reach actual SQLite BEGIN admission";
        EXPECT_FALSE(writer_committed.load());
    }
    release.send();
    EXPECT_TRUE(initializer.ready());
    if (writer) EXPECT_TRUE(writer->ready());
    initializer.join();
    if (writer) writer->join();
    lattice::vec0_maintenance_test_access::set(db, {});
    initializer.get();
    if (writer) writer->get();
    EXPECT_FALSE(timed_out.load());
    EXPECT_TRUE(writer_committed.load());
    initialization_expect_ids(db.db(), {"one", "three"});
    const auto current = db.db().query("SELECT embedding FROM _MaintenanceDoc_embedding_vec WHERE global_id='one'");
    ASSERT_EQ(current.size(), 1u);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(current[0].at("embedding")), pack_floats({0, 0, 1, 0}));
    EXPECT_EQ(initialization_scalar(db.db(), "SELECT COUNT(*) FROM MaintenanceDoc"), 2);
}

TEST(ColdVectorInitialization, TwoVectorFamiliesSettleWithoutRepeatedDDLOrGapScans) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.db().execute("ALTER TABLE MaintenanceDoc ADD COLUMN second BLOB");
    db.db().execute("UPDATE MaintenanceDoc SET second=embedding");
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    db.ensure_vec0_table("MaintenanceDoc", "second", 4);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4); // one shared-cookie refresh, no DDL
    const auto settled_cookie = initialization_scalar(db.db(), "PRAGMA main.schema_version");
    int fills = 0;
    lattice::vec0_maintenance_test_access::set(db, [&](const char* operation, const char* phase) {
        if (std::string(operation) == "ensure" && std::string(phase) == "before-fill") ++fills;
    });
    initialization_authorizer no_hot_writes(db.db());
    no_hot_writes.deny = [](int action, const char*, const char*) {
        return action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT || action == SQLITE_CREATE_TRIGGER ||
               action == SQLITE_DROP_TRIGGER || action == SQLITE_CREATE_VTABLE;
    };
    for (int repeat = 0; repeat < 3; ++repeat) {
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "second", 4));
    }
    EXPECT_EQ(no_hot_writes.denied, 0);
    no_hot_writes.deny = {};
    db.db().execute("INSERT INTO MaintenanceDoc(globalId,embedding,second) VALUES('three',?,?)",
                    {pack_floats({0, 0, 1, 0}), pack_floats({0, 0, 1, 0})});
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "second", 4));
    lattice::vec0_maintenance_test_access::set(db, {});
    EXPECT_EQ(fills, 0);
    EXPECT_EQ(initialization_scalar(db.db(), "PRAGMA main.schema_version"), settled_cookie);
    initialization_expect_ids(db.db(), {"one", "three", "two"});
    EXPECT_EQ(initialization_scalar(db.db(), "SELECT COUNT(*) FROM _MaintenanceDoc_second_vec_rowids"), 3);
}

TEST(ColdVectorInitialization, ReadOnlyHistoricalIndexAuditsGapsWithoutWriteAdmission) {
    TempDB path{"cold_vec_readonly"};
    lattice::lattice_db writer(path.str());
    create_memory_fixture(writer);
    writer.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    initialization_remove_marker(writer.db());
    lattice::configuration config(path.str());
    config.read_only = true;
    lattice::lattice_db reader(config);
    initialization_authorizer no_write(reader.db());
    no_write.deny = [](int action, const char*, const char*) {
        return action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT || action == SQLITE_INSERT ||
               action == SQLITE_UPDATE || action == SQLITE_DELETE || action == SQLITE_CREATE_TRIGGER;
    };
    EXPECT_NO_THROW(reader.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    writer.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    EXPECT_THROW(reader.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
    EXPECT_EQ(no_write.denied, 0) << "read-only gap refusal must precede any write/BEGIN attempt";
    EXPECT_FALSE(reader.db().is_in_transaction());
    EXPECT_TRUE(writer.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}).empty());
}

TEST(ColdVectorInitialization, ExistingPartialIndexRollbackPreservesCallerAndSameOwnerRetry) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    initialization_remove_marker(db.db());
    db.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    db.db().begin_transaction();
    db.db().execute("INSERT INTO MaintenanceDoc(globalId) VALUES('caller-without-vector')");
    lattice::vec0_maintenance_test_access::set(db, [](const char* operation, const char* phase) {
        if (std::string(operation) == "ensure" && std::string(phase) == "after-marker")
            throw lattice::db_error("test rollback of existing vec0 writes");
    });
    EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
    lattice::vec0_maintenance_test_access::set(db, {});
    EXPECT_TRUE(db.db().is_in_transaction());
    initialization_expect_ids(db.db(), {"one"});
    EXPECT_TRUE(db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}).empty());
    EXPECT_EQ(initialization_scalar(db.db(),
        "SELECT COUNT(*) FROM MaintenanceDoc WHERE globalId='caller-without-vector'"), 1);
    // Exercise the actual existing virtual-table object's post-ROLLBACK TO
    // state; recreating/reopening the owner would hide stale cached state.
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    EXPECT_TRUE(db.db().is_in_transaction());
    db.db().commit();
    initialization_expect_ids(db.db(), {"one", "two"});
    initialization_expect_marker(db.db());
}

TEST(WarmVectorReadiness, UnrelatedSchemaChangeNeedsOneReadAuditAndNoWriteOrRepeatedScan) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    const auto marker = db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker});
    db.db().execute("CREATE TABLE UnrelatedWarmReadiness(value INTEGER)");
    int main_reads = 0;
    const auto write_action = [](int action) {
        return action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT || action == SQLITE_INSERT ||
            action == SQLITE_UPDATE || action == SQLITE_DELETE || action == SQLITE_CREATE_TRIGGER ||
            action == SQLITE_DROP_TRIGGER || action == SQLITE_CREATE_VTABLE;
    };
    {
        initialization_authorizer audit(db.db());
        audit.deny = [&](int action, const char* first, const char*) {
            if (action == SQLITE_READ && first && std::string(first) == "MaintenanceDoc") ++main_reads;
            return write_action(action);
        };
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        EXPECT_EQ(audit.denied, 0);
        EXPECT_GT(main_reads, 0) << "the first stale-cookie proof must inspect actual main identities";
    }
    // Changing SQLite's authorizer invalidates prepared statements. A hidden
    // repeat of the gap SELECT cannot evade this witness through statement caching.
    {
        initialization_authorizer hot(db.db());
        hot.deny = [&](int action, const char* first, const char*) {
            return write_action(action) ||
                (action == SQLITE_READ && first && std::string(first) == "MaintenanceDoc");
        };
        for (int repeat = 0; repeat < 3; ++repeat)
            EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        EXPECT_EQ(hot.denied, 0);
    }
    EXPECT_EQ(db.db().query("SELECT value FROM _lattice_meta WHERE key=?", {initialization_marker}), marker);
    EXPECT_FALSE(db.db().is_in_transaction());
    initialization_expect_ids(db.db(), {"one", "two"});
}

TEST(WarmVectorReadiness, RestoredExactTriggerCannotHideAMissingIdentityBehindAnOldProof) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    db.db().execute("CREATE TABLE UnrelatedBeforeTriggerRepair(value INTEGER)");
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    const auto trigger = db.db().query("SELECT sql FROM sqlite_schema WHERE name='_MaintenanceDoc_embedding_vec_update'");
    ASSERT_EQ(trigger.size(), 1u);
    const auto program = std::get<std::string>(trigger[0].at("sql"));
    db.db().execute("DROP TRIGGER _MaintenanceDoc_embedding_vec_update");
    db.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    db.db().execute(program);
    {
        initialization_authorizer deny_repair(db.db());
        deny_repair.deny = [](int action, const char* first, const char*) {
            return action == SQLITE_TRANSACTION && first && std::string(first) == "BEGIN";
        };
        EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
        EXPECT_GT(deny_repair.denied, 0) << "the missing identity must require real write admission";
    }
    EXPECT_FALSE(db.db().is_in_transaction());
    initialization_expect_ids(db.db(), {"one"});
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    initialization_expect_ids(db.db(), {"one", "two"});
    initialization_expect_marker(db.db());
}

TEST(WarmVectorReadiness, CallerRollbackCannotPublishATemporaryCompletenessProof) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    db.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    db.db().execute("CREATE TABLE UnrelatedBeforeProofRollback(value INTEGER)");
    const auto cookie = initialization_scalar(db.db(), "PRAGMA main.schema_version");
    db.db().begin_transaction();
    db.db().execute("INSERT INTO _MaintenanceDoc_embedding_vec(global_id,embedding) "
                    "SELECT globalId,embedding FROM MaintenanceDoc WHERE globalId='two'");
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    EXPECT_TRUE(db.db().is_in_transaction());
    initialization_expect_ids(db.db(), {"one", "two"});
    db.db().rollback();
    EXPECT_EQ(initialization_scalar(db.db(), "PRAGMA main.schema_version"), cookie);
    initialization_expect_ids(db.db(), {"one"});
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    initialization_expect_ids(db.db(), {"one", "two"});
    initialization_expect_marker(db.db());
}

TEST(WarmVectorReadiness, BoundedProofEvictionAndLongNamesFallBackToReadAudits) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    std::vector<std::string> families;
    for (int i = 0; i < 9; ++i) {
        const auto name = "extra" + std::to_string(i);
        db.db().execute("ALTER TABLE MaintenanceDoc ADD COLUMN " + name + " BLOB");
        db.db().execute("UPDATE MaintenanceDoc SET " + name + "=embedding");
        families.push_back(name);
    }
    const std::string long_name(160, 'x');
    db.db().execute("ALTER TABLE MaintenanceDoc ADD COLUMN " + long_name + " BLOB");
    db.db().execute("UPDATE MaintenanceDoc SET " + long_name + "=embedding");
    for (const auto& column : families) db.ensure_vec0_table("MaintenanceDoc", column, 4);
    db.ensure_vec0_table("MaintenanceDoc", long_name, 4);
    db.db().execute("CREATE TABLE UnrelatedBeforeProofEviction(value INTEGER)");
    for (const auto& column : families) db.ensure_vec0_table("MaintenanceDoc", column, 4);
    for (const auto& column : std::vector<std::string>{families.front(), long_name, long_name}) {
        int main_reads = 0;
        initialization_authorizer audit(db.db());
        audit.deny = [&](int action, const char* first, const char*) {
            if (action == SQLITE_READ && first && std::string(first) == "MaintenanceDoc") ++main_reads;
            return action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT || action == SQLITE_INSERT ||
                action == SQLITE_UPDATE || action == SQLITE_DELETE;
        };
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", column, 4));
        EXPECT_GT(main_reads, 0) << "evicted or oversized names must be checked without growing the cache";
        EXPECT_EQ(audit.denied, 0);
    }
}

TEST(WarmVectorReadiness, LiveImplicitReadCannotConsumeAnAutocommitProof) {
    lattice::lattice_db db;
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    db.db().execute("CREATE TABLE UnrelatedBeforeImplicitRead(value INTEGER)");
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    sqlite3_stmt* raw = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.db().handle(), "SELECT globalId FROM MaintenanceDoc", -1, &raw, nullptr), SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> held(raw, &sqlite3_finalize);
    ASSERT_EQ(sqlite3_step(held.get()), SQLITE_ROW);
    ASSERT_EQ(sqlite3_get_autocommit(db.db().handle()), 1);
    ASSERT_EQ(sqlite3_txn_state(db.db().handle(), "main"), SQLITE_TXN_READ);
    {
        initialization_authorizer implicit(db.db());
        implicit.deny = [](int action, const char* first, const char*) {
            return action == SQLITE_READ && first && std::string(first) == "MaintenanceDoc";
        };
        EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
        EXPECT_GT(implicit.denied, 0) << "an unfinished implicit statement must not consume the settled proof";
    }
    held.reset();
    ASSERT_EQ(sqlite3_txn_state(db.db().handle(), "main"), SQLITE_TXN_NONE);
    {
        initialization_authorizer settled(db.db());
        settled.deny = [](int action, const char* first, const char*) {
            return action == SQLITE_READ && first && std::string(first) == "MaintenanceDoc";
        };
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        EXPECT_EQ(settled.denied, 0);
    }
}

TEST(WarmVectorReadiness, ReopenedWriterDoesNotInheritTheRetiredConnectionsProof) {
    TempDB path{"warm_vec_reopened_writer"};
    lattice::lattice_db db(path.str());
    db.stop_audit_maintenance();
    auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path.str());
    ASSERT_NE(notifier, nullptr);
    notifier->stop_listening();
    create_memory_fixture(db);
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    db.db().execute("CREATE TABLE UnrelatedBeforeWriterReplacement(value INTEGER)");
    db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
    const auto cookie = initialization_scalar(db.db(), "PRAGMA main.schema_version");
    db.close_read_db();
    auto retired = db.borrow_read_connection();
    db.db().execute("DELETE FROM _MaintenanceDoc_embedding_vec WHERE global_id='two'");
    db.close_write_db();
    db.reopen_write_db();
    ASSERT_NE(&db.db(), retired.get());
    ASSERT_EQ(initialization_scalar(db.db(), "PRAGMA main.schema_version"), cookie);
    initialization_expect_ids(db.db(), {"one"});
    EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
    initialization_expect_ids(db.db(), {"one", "two"});
    initialization_expect_marker(db.db());
}

TEST(WarmVectorReadiness, NoncanonicalAndFutureMarkersRetainWritableRepair) {
    for (const bool future : {false, true}) {
        lattice::lattice_db db;
        create_memory_fixture(db);
        db.ensure_vec0_table("MaintenanceDoc", "embedding", 4);
        const auto cookie = initialization_scalar(db.db(), "PRAGMA main.schema_version");
        const auto value = future ? std::to_string(cookie + 1) : "0" + std::to_string(cookie);
        db.db().execute("UPDATE _lattice_meta SET value=? WHERE key=?", {value, initialization_marker});
        {
            initialization_authorizer deny_repair(db.db());
            deny_repair.deny = [](int action, const char* first, const char*) {
                return action == SQLITE_TRANSACTION && first && std::string(first) == "BEGIN";
            };
            EXPECT_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4), lattice::db_error);
            EXPECT_GT(deny_repair.denied, 0);
        }
        EXPECT_NO_THROW(db.ensure_vec0_table("MaintenanceDoc", "embedding", 4));
        initialization_expect_marker(db.db());
    }
}
