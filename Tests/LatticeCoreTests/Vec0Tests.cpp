#include "TestHelpers.hpp"

// ============================================================================
// Vec0 (sqlite-vec) Tests
// ============================================================================

// Shared setup: create a table with a vector column and populate vec0
class Vec0Test : public ::testing::Test {
protected:
    TempDB tmp{"vec0"};

    void createVectorTable(lattice::lattice_db& db, int dims = 3) {
        db.db().execute(
            "CREATE TABLE IF NOT EXISTS VectorDoc ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "globalId TEXT UNIQUE NOT NULL, "
            "title TEXT NOT NULL, "
            "embedding BLOB"
            ")");
        db.ensure_vec0_table("VectorDoc", "embedding", dims);
    }

    void insertDoc(lattice::lattice_db& db, const std::string& gid,
                   const std::string& title, const std::vector<float>& vec) {
        auto blob = pack_floats(vec);
        db.db().execute(
            "INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?)",
            {gid, title, blob});
    }
};

TEST_F(Vec0Test, SingleConnectionInsertAndQuery) {
    lattice::lattice_db db;
    createVectorTable(db);

    insertDoc(db, "a", "Doc A", {1.0f, 0.0f, 0.0f});
    insertDoc(db, "b", "Doc B", {0.0f, 1.0f, 0.0f});
    insertDoc(db, "c", "Doc C", {0.0f, 0.0f, 1.0f});

    // Verify vec0 has entries
    auto vec_rows = db.db().query(
        "SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    ASSERT_EQ(std::get<int64_t>(vec_rows[0].at("cnt")), 3);

    // KNN query for nearest to [1,0,0]
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 3,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 3u);

    // First result is exact match
    auto first = db.db().query(
        "SELECT title FROM VectorDoc WHERE globalId = ?",
        {results[0].global_id});
    ASSERT_EQ(std::get<std::string>(first[0].at("title")), "Doc A");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

TEST_F(Vec0Test, CrossConnectionVisibility) {
    // THE BUG: db_a writes vec0 data, db_b can't see it via knn_query
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    createVectorTable(db_a);

    insertDoc(db_a, "x", "DocX", {1.0f, 0.0f, 0.0f});
    insertDoc(db_a, "y", "DocY", {0.0f, 1.0f, 0.0f});

    // Verify db_a sees data
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results_a = db_a.knn_query("VectorDoc", "embedding", qvec, 2,
                                     lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results_a.size(), 2u);

    // Open second connection
    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    // db_b should be able to query vec0 — may require reconciliation
    auto results_b = db_b.knn_query("VectorDoc", "embedding", qvec, 2,
                                     lattice::lattice_db::distance_metric::l2);
    // After knn_query's built-in reconciliation, results should be visible
    EXPECT_EQ(results_b.size(), 2u);
}

TEST_F(Vec0Test, ShadowTablesVisibleCrossConnection) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    createVectorTable(db_a);

    insertDoc(db_a, "s1", "Shadow1", {1.0f, 0.0f, 0.0f});

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    // The main table data should be visible cross-connection
    auto rows = db_b.db().query(
        "SELECT COUNT(*) as cnt FROM VectorDoc");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("cnt")), 1);

    // The vec0 virtual table should be registered
    auto tables = db_b.db().query(
        "SELECT name FROM sqlite_master WHERE name = '_VectorDoc_embedding_vec'");
    EXPECT_EQ(tables.size(), 1u);
}

TEST_F(Vec0Test, ReconcileFixesVisibility) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    createVectorTable(db_a);

    insertDoc(db_a, "r1", "Reconcile1", {1.0f, 0.0f, 0.0f});
    insertDoc(db_a, "r2", "Reconcile2", {0.0f, 1.0f, 0.0f});

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    // Explicitly reconcile vec0 on db_b
    db_b.reconcile_vec0("VectorDoc", "embedding");

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db_b.knn_query("VectorDoc", "embedding", qvec, 2,
                                   lattice::lattice_db::distance_metric::l2);
    EXPECT_EQ(results.size(), 2u);
}

TEST_F(Vec0Test, KnnQuerySelfHeals) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    createVectorTable(db_a);

    insertDoc(db_a, "h1", "Heal1", {1.0f, 0.0f, 0.0f});

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    // knn_query should self-heal (reconcile internally)
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db_b.knn_query("VectorDoc", "embedding", qvec, 1,
                                   lattice::lattice_db::distance_metric::l2);
    EXPECT_EQ(results.size(), 1u);
}

TEST_F(Vec0Test, UpdateTrigger) {
    lattice::lattice_db db;
    createVectorTable(db);

    insertDoc(db, "u1", "Update1", {1.0f, 0.0f, 0.0f});

    // Update the embedding
    auto new_emb = pack_floats({0.0f, 0.0f, 1.0f});
    db.db().execute(
        "UPDATE VectorDoc SET embedding = ? WHERE globalId = 'u1'",
        {new_emb});

    // KNN query for [0,0,1] should return this doc as nearest
    auto qvec = pack_floats({0.0f, 0.0f, 1.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "u1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

TEST_F(Vec0Test, DeleteTrigger) {
    lattice::lattice_db db;
    createVectorTable(db);

    insertDoc(db, "d1", "Delete1", {1.0f, 0.0f, 0.0f});
    insertDoc(db, "d2", "Delete2", {0.0f, 1.0f, 0.0f});

    // Delete one doc
    db.db().execute("DELETE FROM VectorDoc WHERE globalId = 'd1'");

    // vec0 should only have 1 entry
    auto cnt = db.db().query(
        "SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    EXPECT_EQ(std::get<int64_t>(cnt[0].at("cnt")), 1);
}

TEST_F(Vec0Test, EmptyVectorSkipped) {
    lattice::lattice_db db;
    createVectorTable(db);

    // Insert with empty blob — should not crash
    db.db().execute(
        "INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?)",
        {std::string("empty"), std::string("EmptyDoc"), std::vector<uint8_t>{}});

    // KNN query should still work (return 0 results)
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                lattice::lattice_db::distance_metric::l2);
    EXPECT_EQ(results.size(), 0u);
}

TEST_F(Vec0Test, CosineDistance) {
    lattice::lattice_db db;
    createVectorTable(db);

    insertDoc(db, "cos1", "Parallel", {1.0f, 0.0f, 0.0f});
    insertDoc(db, "cos2", "Orthogonal", {0.0f, 1.0f, 0.0f});
    insertDoc(db, "cos3", "Similar", {0.9f, 0.1f, 0.0f});

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 3,
                                lattice::lattice_db::distance_metric::cosine);
    ASSERT_EQ(results.size(), 3u);

    // First should be exact match (distance ~0), then Similar, then Orthogonal
    EXPECT_EQ(results[0].global_id, "cos1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.01);
}

TEST_F(Vec0Test, LargeEmbedding) {
    lattice::lattice_db db;
    createVectorTable(db, 768);  // NLP-sized embedding

    // Create a 768-dim vector
    std::vector<float> large_vec(768, 0.0f);
    large_vec[0] = 1.0f;
    large_vec[100] = 0.5f;
    large_vec[767] = 0.3f;

    insertDoc(db, "large1", "LargeDoc", large_vec);

    // Query should round-trip correctly
    auto qvec = pack_floats(large_vec);
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

// ============================================================================
// Vec0 Concurrency Tests — guards against "database is locked" storms
// ============================================================================

// Concurrent writes on one connection + knn_query on another.
// Before the fix, the knn_query reconcile loop did individual DELETE+INSERT
// per row outside a transaction, causing massive lock contention.
TEST_F(Vec0Test, ConcurrentWriteAndQuery_NoLockStorm) {
    TempDB cwtmp{"vec0_concurrent_wq"};

    lattice::lattice_db writer(lattice::configuration(cwtmp.str()));
    createVectorTable(writer);

    // Seed some initial data so both connections see the table
    for (int i = 0; i < 5; i++) {
        insertDoc(writer, "seed-" + std::to_string(i),
                  "Seed" + std::to_string(i),
                  {static_cast<float>(i), 0.0f, 1.0f});
    }

    lattice::lattice_db reader(lattice::configuration(cwtmp.str()));

    std::atomic<bool> done{false};
    std::atomic<int> write_count{0};
    std::atomic<int> query_count{0};

    // Writer thread: insert 50 docs with embeddings
    std::thread writer_thread([&]() {
        for (int i = 0; i < 50; i++) {
            try {
                insertDoc(writer, "cw-" + std::to_string(i),
                          "ConcDoc" + std::to_string(i),
                          {static_cast<float>(i), 1.0f, 0.0f});
                write_count++;
            } catch (...) {
                // Count will be < 50 if locks fail
            }
        }
        done = true;
    });

    // Reader thread: repeatedly knn_query (triggers reconcile on count mismatch)
    std::thread reader_thread([&]() {
        auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
        while (!done.load()) {
            try {
                reader.knn_query("VectorDoc", "embedding", qvec, 5,
                                 lattice::lattice_db::distance_metric::l2);
                query_count++;
            } catch (...) {
                // Should not throw — lock errors are the bug
            }
        }
        // One final query after writes finish
        try {
            reader.knn_query("VectorDoc", "embedding", qvec, 5,
                             lattice::lattice_db::distance_metric::l2);
            query_count++;
        } catch (...) {}
    });

    writer_thread.join();
    reader_thread.join();

    // All writes should succeed (no lock timeouts)
    EXPECT_EQ(write_count.load(), 50)
        << "Writer should complete all 50 inserts without lock failures";

    // Reader should have completed multiple queries
    EXPECT_GE(query_count.load(), 1)
        << "Reader should complete at least 1 knn_query during concurrent writes";
}

// Two connections both triggering reconcile simultaneously.
// Before the fix, both would do N individual DELETE+INSERT pairs outside
// transactions, creating O(N) lock acquisitions that overwhelm busy_timeout.
TEST_F(Vec0Test, DualReconcile_NoLockStorm) {
    TempDB drtmp{"vec0_dual_reconcile"};

    // Write data on connection A
    lattice::lattice_db db_a(lattice::configuration(drtmp.str()));
    createVectorTable(db_a);

    for (int i = 0; i < 50; i++) {
        insertDoc(db_a, "dr-" + std::to_string(i),
                  "DualRec" + std::to_string(i),
                  {static_cast<float>(i), 0.5f, 0.5f});
    }

    // Open two new connections — both have stale vec0 (count mismatch)
    lattice::lattice_db db_b(lattice::configuration(drtmp.str()));
    lattice::lattice_db db_c(lattice::configuration(drtmp.str()));

    std::atomic<int> b_success{0};
    std::atomic<int> c_success{0};

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});

    // Both connections query simultaneously — each triggers full reconcile
    std::thread t_b([&]() {
        try {
            auto results = db_b.knn_query("VectorDoc", "embedding", qvec, 5,
                                           lattice::lattice_db::distance_metric::l2);
            if (!results.empty()) b_success = 1;
        } catch (...) {}
    });

    std::thread t_c([&]() {
        try {
            auto results = db_c.knn_query("VectorDoc", "embedding", qvec, 5,
                                           lattice::lattice_db::distance_metric::l2);
            if (!results.empty()) c_success = 1;
        } catch (...) {}
    });

    t_b.join();
    t_c.join();

    // Both should succeed without lock timeout
    EXPECT_EQ(b_success.load(), 1)
        << "Connection B knn_query should succeed during concurrent reconcile";
    EXPECT_EQ(c_success.load(), 1)
        << "Connection C knn_query should succeed during concurrent reconcile";
}

// apply_remote_changes with embedding data should not cause lock contention
// with a concurrent reader. Before the fix, sync did per-row reconcile_vec0
// AFTER commit, each as an individual autocommit write.
TEST_F(Vec0Test, SyncApplyWithVec0_NoLockStorm) {
    TempDB satmp{"vec0_sync_apply"};

    lattice::lattice_db sync_db(lattice::configuration(satmp.str()));
    sync_db.db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "title TEXT NOT NULL, "
        "embedding BLOB)");
    sync_db.ensure_vec0_table("VectorDoc", "embedding", 3);

    // Seed a few rows so the reader connection sees the table
    insertDoc(sync_db, "preseed-1", "Pre1", {1.0f, 0.0f, 0.0f});
    insertDoc(sync_db, "preseed-2", "Pre2", {0.0f, 1.0f, 0.0f});

    // Build 50 remote entries with embedding data (hex-encoded blobs)
    std::vector<lattice::audit_log_entry> entries;
    for (int i = 0; i < 50; i++) {
        auto emb = pack_floats({static_cast<float>(i), 0.5f, 0.5f});
        std::string hex;
        for (auto byte : emb) {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02X", byte);
            hex += buf;
        }

        lattice::audit_log_entry entry;
        entry.global_id = "sync-entry-" + std::to_string(i);
        entry.table_name = "VectorDoc";
        entry.operation = "INSERT";
        entry.row_id = i + 10;
        entry.global_row_id = "sync-vec-" + std::to_string(i);
        entry.changed_fields = {
            {"title", lattice::any_property("SyncDoc" + std::to_string(i))},
            {"embedding", lattice::any_property(hex)}
        };
        entry.changed_fields_names = {"title", "embedding"};
        entry.timestamp = "2024-01-01T00:00:" + std::to_string(i) + "Z";
        entries.push_back(std::move(entry));
    }

    // Open a reader connection
    lattice::lattice_db reader(lattice::configuration(satmp.str()));
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});

    std::atomic<bool> sync_done{false};
    std::atomic<int> query_count{0};

    // Sync thread: apply all 50 entries
    std::thread sync_thread([&]() {
        lattice::apply_remote_changes(sync_db, entries);
        sync_done = true;
    });

    // Reader thread: query vec0 during sync
    std::thread reader_thread([&]() {
        while (!sync_done.load()) {
            try {
                reader.knn_query("VectorDoc", "embedding", qvec, 5,
                                 lattice::lattice_db::distance_metric::l2);
                query_count++;
            } catch (...) {}
        }
        // Final query after sync
        try {
            reader.knn_query("VectorDoc", "embedding", qvec, 5,
                             lattice::lattice_db::distance_metric::l2);
            query_count++;
        } catch (...) {}
    });

    sync_thread.join();
    reader_thread.join();

    // Verify sync wrote all rows
    auto cnt = sync_db.db().query(
        "SELECT COUNT(*) as cnt FROM VectorDoc");
    EXPECT_EQ(std::get<int64_t>(cnt[0].at("cnt")), 52)  // 2 pre-seeded + 50 synced
        << "All synced rows should be present";

    // Reader should have completed queries without lock failures
    EXPECT_GE(query_count.load(), 1)
        << "Reader should complete knn_queries during concurrent sync";
}

// Regression: UNIQUE constraint failed on vec0 INSERT when row already has
// a vec0 entry (e.g. from reconciliation or prior insert). The trigger must
// handle this via UPDATE+conditional INSERT instead of DELETE+INSERT, because
// vec0 DELETE is unreliable inside triggers.
TEST_F(Vec0Test, UpsertWithExistingVec0Entry_NoUniqueConstraint) {
    lattice::lattice_db db;
    createVectorTable(db);

    // Insert a row — trigger populates vec0
    insertDoc(db, "dup1", "Original", {1.0f, 0.0f, 0.0f});

    auto vec_cnt = db.db().query("SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    EXPECT_EQ(std::get<int64_t>(vec_cnt[0].at("cnt")), 1);

    // Now do an INSERT...ON CONFLICT DO UPDATE (upsert) with a new embedding.
    // This fires the UPDATE trigger. The vec0 entry already exists —
    // the trigger must not fail with UNIQUE constraint.
    auto new_vec = pack_floats({0.0f, 1.0f, 0.0f});
    EXPECT_NO_THROW(
        db.db().execute(
            "INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?) "
            "ON CONFLICT(globalId) DO UPDATE SET embedding = excluded.embedding",
            {std::string("dup1"), std::string("Updated"), new_vec})
    ) << "Upsert with existing vec0 entry must not throw UNIQUE constraint";

    // Vec0 should still have exactly 1 entry with the UPDATED embedding
    vec_cnt = db.db().query("SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    EXPECT_EQ(std::get<int64_t>(vec_cnt[0].at("cnt")), 1);

    // KNN query should find the updated vector
    auto qvec = pack_floats({0.0f, 1.0f, 0.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "dup1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001)
        << "Vec0 should reflect the updated embedding, not the original";
}

// Regression: INSERT trigger must handle pre-existing vec0 entry
// (e.g. populated by reconciliation from another connection).
TEST_F(Vec0Test, InsertTrigger_PreExistingVec0Entry) {
    lattice::lattice_db db;
    createVectorTable(db);

    // Manually populate vec0 as if reconciliation ran
    auto vec_data = pack_floats({1.0f, 0.0f, 0.0f});
    db.upsert_vec0("VectorDoc", "embedding", "pre-pop", vec_data);

    // Now INSERT into the model table with the same globalId.
    // The INSERT trigger fires and must not fail despite vec0 already
    // having an entry for this globalId.
    auto new_vec = pack_floats({0.0f, 0.0f, 1.0f});
    EXPECT_NO_THROW(
        db.db().execute(
            "INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?)",
            {std::string("pre-pop"), std::string("PrePop"), new_vec})
    ) << "INSERT with pre-existing vec0 entry must not throw UNIQUE constraint";

    // Vec0 should have exactly 1 entry with the new embedding
    auto vec_cnt = db.db().query("SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    EXPECT_EQ(std::get<int64_t>(vec_cnt[0].at("cnt")), 1);

    auto qvec = pack_floats({0.0f, 0.0f, 1.0f});
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "pre-pop");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001)
        << "Vec0 should reflect the INSERT embedding, not the pre-populated one";
}

// ============================================================================
// IVF (Inverted File Index) Tests
// ============================================================================

class IVFTest : public ::testing::Test {
protected:
    TempDB tmp{"ivf"};
    TempDB tmp2{"ivf2"};

    void createVectorTable(lattice::lattice_db& db, int dims = 32) {
        db.db().execute(
            "CREATE TABLE IF NOT EXISTS VectorDoc ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "globalId TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))), "
            "title TEXT NOT NULL, "
            "embedding BLOB"
            ")");
        db.ensure_vec0_table("VectorDoc", "embedding", dims);
    }

    std::vector<float> randomVec(int dims = 32) {
        static std::mt19937 rng(42);
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        std::vector<float> v(dims);
        for (auto& x : v) x = dist(rng);
        return v;
    }

    void insertDoc(lattice::lattice_db& db, const std::string& gid,
                   const std::string& title, const std::vector<float>& vec) {
        auto blob = pack_floats(vec);
        db.db().execute(
            "INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?)",
            {gid, title, blob});
    }

    void bulkInsert(lattice::lattice_db& db, int count, int dims = 32) {
        for (int i = 0; i < count; i++) {
            insertDoc(db, "gid_" + std::to_string(i),
                      "doc_" + std::to_string(i), randomVec(dims));
        }
    }
};

// Table creation is flat by default; IVF added by train_vec0
TEST_F(IVFTest, TableCreation) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);

    // vec0 table should exist (flat, no IVF yet)
    EXPECT_TRUE(db.db().table_exists("_VectorDoc_embedding_vec"));
    EXPECT_FALSE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_centroids00"))
        << "Default table should be flat, not IVF";

    // After training, IVF shadow tables should appear
    bulkInsert(db, 50);
    db.train_vec0("VectorDoc", "embedding");
    EXPECT_TRUE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_centroids00"));
    EXPECT_TRUE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_cells00"));
    EXPECT_TRUE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_rowid_map00"));
}

// Insert and query BEFORE training — brute-force fallback
TEST_F(IVFTest, QueryBeforeTraining) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);

    insertDoc(db, "a", "close", {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f});
    insertDoc(db, "b", "far",   {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                  0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f});

    auto query = pack_floats({1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                              0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                              0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                              0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f});

    auto results = db.knn_query("VectorDoc", "embedding", query, 2,
                                lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].global_id, "a") << "Closest should be 'close'";
}

// Training with sufficient vectors
TEST_F(IVFTest, TrainAndQuery) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);

    bulkInsert(db, 200);

    // Train IVF
    bool trained = db.train_vec0("VectorDoc", "embedding");
    EXPECT_TRUE(trained) << "Training should succeed with 200 vectors";

    // Verify trained flag
    auto info = db.db().query(
        "SELECT value FROM _VectorDoc_embedding_vec_info WHERE key = 'ivf_trained_0'");
    ASSERT_FALSE(info.empty());
    EXPECT_EQ(std::get<int64_t>(info[0].at("value")), 1);

    // Query should still return results
    auto query = pack_floats(randomVec());
    auto results = db.knn_query("VectorDoc", "embedding", query, 10,
                                lattice::lattice_db::distance_metric::l2);
    EXPECT_GT(results.size(), 0u) << "Trained IVF should return results";
}

// Training is idempotent — second call skips
TEST_F(IVFTest, TrainIdempotent) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 50);

    bool first = db.train_vec0("VectorDoc", "embedding");
    EXPECT_TRUE(first);

    bool second = db.train_vec0("VectorDoc", "embedding");
    EXPECT_TRUE(second) << "Second train should return true (already trained)";
}

// Training skipped with too few vectors
TEST_F(IVFTest, TrainSkippedFewVectors) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 5);

    bool trained = db.train_vec0("VectorDoc", "embedding");
    EXPECT_FALSE(trained) << "Training should skip with only 5 vectors";
}

// Insert AFTER training — should auto-assign to centroid
TEST_F(IVFTest, InsertAfterTraining) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 100);

    db.train_vec0("VectorDoc", "embedding");

    // Insert new doc after training
    insertDoc(db, "new_doc", "new", randomVec());

    // Should be findable via query
    auto query = pack_floats(randomVec());
    auto results = db.knn_query("VectorDoc", "embedding", query, 110,
                                lattice::lattice_db::distance_metric::l2);
    // IVF is approximate — may not return all 101 due to nprobe
    EXPECT_GE(results.size(), 50u) << "Should find most docs including post-training insert";
}

// Vacuum rebuilds flat, then train_vec0 converts to IVF + trains
TEST_F(IVFTest, VacuumThenTrain) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 100);

    // Vacuum rebuilds as flat
    int64_t count = db.vacuum_vec0("VectorDoc", "embedding");
    EXPECT_EQ(count, 100);
    EXPECT_FALSE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_centroids00"))
        << "Vacuum should create flat table, not IVF";

    // Explicit train converts to IVF and trains centroids
    bool trained = db.train_vec0("VectorDoc", "embedding");
    EXPECT_TRUE(trained);

    auto info = db.db().query(
        "SELECT value FROM _VectorDoc_embedding_vec_info WHERE key = 'ivf_trained_0'");
    ASSERT_FALSE(info.empty());
    EXPECT_EQ(std::get<int64_t>(info[0].at("value")), 1);
}

// WAL state after training — subsequent inserts should work
TEST_F(IVFTest, InsertAfterTrainNoWALConflict) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 100);

    db.train_vec0("VectorDoc", "embedding");

    // Insert 50 more docs — should not get SQLITE_BUSY_SNAPSHOT
    for (int i = 0; i < 50; i++) {
        EXPECT_NO_THROW(
            insertDoc(db, "post_train_" + std::to_string(i),
                      "post_" + std::to_string(i), randomVec())
        ) << "Insert after training should not fail (iteration " << i << ")";
    }
}

// Two connections: training on one, reads on other
TEST_F(IVFTest, CrossConnectionAfterTraining) {
    {
        lattice::lattice_db db1(tmp.str());
        createVectorTable(db1);
        bulkInsert(db1, 100);
        db1.train_vec0("VectorDoc", "embedding");
    }
    // Reopen — simulates another process opening the DB after training
    lattice::lattice_db db2(tmp.str());
    createVectorTable(db2, 32); // ensure table (already exists)

    auto query = pack_floats(randomVec());
    auto results = db2.knn_query("VectorDoc", "embedding", query, 10,
                                 lattice::lattice_db::distance_metric::l2);
    EXPECT_GT(results.size(), 0u) << "Second connection should read trained IVF data";
}

// Attach two DBs with IVF vec0 tables
TEST_F(IVFTest, AttachWithIVF) {
    // Create and populate two DBs
    {
        lattice::lattice_db db1(tmp.str());
        createVectorTable(db1);
        bulkInsert(db1, 50);
    }
    {
        lattice::lattice_db db2(tmp2.str());
        createVectorTable(db2);
        for (int i = 0; i < 50; i++) {
            insertDoc(db2, "db2_" + std::to_string(i),
                      "db2_doc_" + std::to_string(i), randomVec());
        }
    }

    // Open both and attach
    lattice::lattice_db db1(tmp.str());
    lattice::lattice_db db2_reopen(tmp2.str());
    createVectorTable(db1, 32);
    createVectorTable(db2_reopen, 32);
    EXPECT_NO_THROW(db1.attach(db2_reopen))
        << "Attaching a DB with IVF vec0 tables should not crash";

    // Query should work across both
    auto query = pack_floats(randomVec());
    auto results = db1.knn_query("VectorDoc", "embedding", query, 100,
                                 lattice::lattice_db::distance_metric::l2);
    EXPECT_GT(results.size(), 50u)
        << "Attached query should return results from both DBs";
}

// MATCH query with WHERE filter
TEST_F(IVFTest, MatchQueryWithFilter) {
    lattice::lattice_db db(tmp.str());
    createVectorTable(db);
    bulkInsert(db, 100);
    db.train_vec0("VectorDoc", "embedding");

    // Query with a WHERE filter
    auto query = pack_floats(randomVec());
    auto results = db.knn_query("VectorDoc", "embedding", query, 10,
                                lattice::lattice_db::distance_metric::l2,
                                std::string("title LIKE 'doc_1%'"));
    // Should only return docs matching the filter
    for (const auto& r : results) {
        // Verify by querying back
        auto rows = db.db().query(
            "SELECT title FROM VectorDoc WHERE globalId = ?", {r.global_id});
        ASSERT_FALSE(rows.empty());
        auto title = std::get<std::string>(rows[0].at("title"));
        EXPECT_TRUE(title.substr(0, 5) == "doc_1")
            << "Filtered result should match: " << title;
    }
}

// ============================================================================
// H3 (Aug 13 2026) — vec0 reconcile must be idempotent and atomic.
//
// Every process that receives an xproc change notification reconciles the
// same rows. The old blind DELETE+INSERT (two autocommit write transactions)
// interleaved ACROSS processes — P1.DELETE, P2.DELETE, P1.INSERT,
// P2.INSERT→UNIQUE — into 28,308 "UNIQUE constraint failed on
// _Memory_embedding_vec" errors hammering the hub's write lock, which
// starved the IPC apply path into a livelock (spoke→hub acks frozen).
// ============================================================================

// Reconciling a row the index already holds with identical bytes must be a
// READ, not a rewrite: the writer's triggers already indexed it. Pinned via
// PRAGMA data_version on an observer connection — it moves iff some other
// connection commits a write.
TEST_F(Vec0Test, ReconcileOfCurrentRowIsReadOnly) {
    TempDB rtmp{"vec0_readonly_reconcile"};
    lattice::lattice_db db_a(lattice::configuration(rtmp.str()));
    createVectorTable(db_a);
    insertDoc(db_a, "ro1", "ReadOnly1", {1.0f, 0.0f, 0.0f});  // trigger indexes it

    lattice::lattice_db db_b(lattice::configuration(rtmp.str()));

    lattice::database obs(rtmp.str());
    auto data_version = [&obs]() {
        return std::get<int64_t>(obs.query("PRAGMA data_version")[0].at("data_version"));
    };
    const auto before = data_version();

    db_b.reconcile_vec0("VectorDoc", "ro1");

    EXPECT_EQ(data_version(), before)
        << "reconcile of an already-correct index row wrote to the database "
           "(the blind-rewrite storm of the Aug 13 incident)";

    // The row must remain correct and queryable from the reconciling side.
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db_b.knn_query("VectorDoc", "embedding", qvec, 1,
                                  lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "ro1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

// A CHANGED embedding must still be refreshed (idempotence must not decay
// into never-writing), and the write must survive concurrent reconcilers.
TEST_F(Vec0Test, ReconcileOfChangedRowRefreshesTheIndex) {
    TempDB ctmp{"vec0_changed_reconcile"};
    lattice::lattice_db db_a(lattice::configuration(ctmp.str()));
    createVectorTable(db_a);
    insertDoc(db_a, "ch1", "Changed1", {1.0f, 0.0f, 0.0f});

    lattice::lattice_db db_b(lattice::configuration(ctmp.str()));

    // Simulate a trigger-missed index row: stale bytes planted directly.
    db_b.upsert_vec0("VectorDoc", "embedding", "ch1",
                     pack_floats({9.0f, 9.0f, 9.0f}));

    // Reconcile against the model's truth must repair it.
    db_b.reconcile_vec0("VectorDoc", "ch1");

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db_b.knn_query("VectorDoc", "embedding", qvec, 1,
                                  lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "ch1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001)
        << "reconcile must refresh a stale index row to the model's bytes";

    auto cnt = db_b.db().query(
        "SELECT COUNT(*) AS c FROM _VectorDoc_embedding_vec_rowids WHERE id = 'ch1'");
    EXPECT_EQ(std::get<int64_t>(cnt[0].at("c")), 1);
}

// Two connections hammering upsert_vec0 on the SAME id: the old autocommit
// DELETE+INSERT pair interleaves across connections into UNIQUE failures.
// The fix (UPDATE-then-conditional-INSERT under one write transaction) makes
// collisions structurally impossible.
TEST_F(Vec0Test, ConcurrentUpsertSameRow_NoUniqueThrow) {
    TempDB utmp{"vec0_upsert_race"};
    lattice::lattice_db db_a(lattice::configuration(utmp.str()));
    createVectorTable(db_a);
    lattice::lattice_db db_b(lattice::configuration(utmp.str()));

    std::atomic<int> failures{0};
    auto hammer = [&failures](lattice::lattice_db& db, int seed) {
        for (int i = 0; i < 300; ++i) {
            auto v = pack_floats({static_cast<float>(seed), static_cast<float>(i), 0.0f});
            try {
                db.upsert_vec0("VectorDoc", "embedding", "contested", v);
            } catch (const std::exception&) {
                failures.fetch_add(1);
            }
        }
    };
    std::thread ta([&] { hammer(db_a, 1); });
    std::thread tb([&] { hammer(db_b, 2); });
    ta.join();
    tb.join();

    EXPECT_EQ(failures.load(), 0)
        << "interleaved upserts of one id must never collide (UNIQUE) or fail";
    auto cnt = db_a.db().query(
        "SELECT COUNT(*) AS c FROM _VectorDoc_embedding_vec_rowids WHERE id = 'contested'");
    EXPECT_EQ(std::get<int64_t>(cnt[0].at("c")), 1)
        << "exactly one index row must survive the race";
}


TEST_F(Vec0Test, IvfOwnedLookupsEndBeforeTheNextCallerOperation) {
    lattice::lattice_db db;
    createVectorTable(db);
    for (int i = 0; i < 32; ++i)
        insertDoc(db, "seed-" + std::to_string(i), "seed", {float(i), 1.0f, 2.0f});
    ASSERT_TRUE(db.train_vec0("VectorDoc", "embedding"));
    const auto trained = db.db().query(
        "SELECT value FROM _VectorDoc_embedding_vec_info WHERE key = 'ivf_trained_0'");
    ASSERT_EQ(trained.size(), 1u);
    ASSERT_EQ(std::get<int64_t>(trained[0].at("value")), 1);
    // Diagnostic raw access is no admission grant. Observe actual VM state;
    // never guess ownership from SQL or prune/finalize a retained statement.
    auto* native = db.db().handle();
    auto expect_no_busy_original = [&] {
        for (auto* statement = sqlite3_next_stmt(native, nullptr); statement;
             statement = sqlite3_next_stmt(native, statement))
            EXPECT_FALSE(sqlite3_stmt_busy(statement)) << sqlite3_sql(statement);
    };
    expect_no_busy_original();
    db.begin_transaction();
    for (int i = 0; i < 50; ++i) {
        insertDoc(db, "next-" + std::to_string(i), "after training", {float(i), 3.0f, 4.0f});
        expect_no_busy_original();
        ASSERT_TRUE(db.db().is_in_transaction());
    }
    db.commit();
    ASSERT_FALSE(db.db().is_in_transaction());
    const auto total = db.db().query("SELECT COUNT(*) AS n FROM VectorDoc");
    ASSERT_EQ(std::get<int64_t>(total[0].at("n")), 82);
    const auto expected = pack_floats({49.0f, 3.0f, 4.0f});
    const auto point = db.db().query(
        "SELECT embedding FROM _VectorDoc_embedding_vec WHERE global_id = ?", {std::string("next-49")});
    ASSERT_EQ(point.size(), 1u);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(point[0].at("embedding")), expected);
    expect_no_busy_original();
    db.begin_transaction();
    db.db().execute("DELETE FROM VectorDoc WHERE globalId = ?", {std::string("next-49")});
    expect_no_busy_original();
    db.commit();
    EXPECT_FALSE(db.db().is_in_transaction());
    const auto remaining = db.db().query("SELECT COUNT(*) AS n FROM _VectorDoc_embedding_vec");
    EXPECT_EQ(std::get<int64_t>(remaining[0].at("n")), 81);
    expect_no_busy_original();
}

#include <map>
#include <set>

class IvfSparseMutationTest : public Vec0Test {
protected:
    using image = std::map<std::string, std::vector<float>>;
    void seed_ivf(lattice::lattice_db& db, image& expected) {
        db.db().execute("CREATE TABLE VectorDoc (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "globalId TEXT UNIQUE NOT NULL, title TEXT NOT NULL, embedding BLOB)");
        // Actual IVF from its first vector, initially unassigned. Public train
        // below preserves this same table instead of rebuilding a flat index.
        db.ensure_vec0_table("VectorDoc", "embedding", 3, 2, 2);
        ASSERT_TRUE(db.db().table_exists("_VectorDoc_embedding_vec_ivf_centroids00"));
        for (int i = 0; i < 32; ++i) {
            const auto id = "row-" + std::to_string(i);
            expected[id] = {float(i), 1.0f, 2.0f};
            insertDoc(db, id, "seed", expected[id]);
        }
    }
    void erase(lattice::lattice_db& db, image& expected, const std::string& id) {
        db.db().execute("DELETE FROM VectorDoc WHERE globalId = ?", {id});
        expected.erase(id);
    }
    void add(lattice::lattice_db& db, image& expected, const std::string& id,
             std::vector<float> value) {
        insertDoc(db, id, "replacement", value);
        expected[id] = std::move(value);
    }
    void update(lattice::lattice_db& db, image& expected, const std::string& id,
                std::vector<float> value) {
        db.db().execute("UPDATE VectorDoc SET embedding = ? WHERE globalId = ?",
                        {pack_floats(value), id});
        expected[id] = std::move(value);
    }
    void expect_image(lattice::lattice_db& db, const image& expected) {
        const auto count = db.db().query("SELECT COUNT(*) AS n FROM VectorDoc");
        ASSERT_EQ(std::get<int64_t>(count.at(0).at("n")), int64_t(expected.size()));
        const auto cells = db.db().query(
            "SELECT n_vectors, validity FROM _VectorDoc_embedding_vec_ivf_cells00");
        int64_t actual_live = 0;
        for (const auto& cell : cells) {
            int live = 0;
            for (const auto byte : std::get<std::vector<uint8_t>>(cell.at("validity")))
                for (int bit = 0; bit < 8; ++bit) if (byte & (1 << bit)) ++live;
            EXPECT_EQ(std::get<int64_t>(cell.at("n_vectors")), live);
            actual_live += live;
        }
        EXPECT_EQ(actual_live, int64_t(expected.size()));
        const auto map_count = db.db().query(
            "SELECT COUNT(*) AS n FROM _VectorDoc_embedding_vec_ivf_rowid_map00");
        EXPECT_EQ(std::get<int64_t>(map_count.at(0).at("n")), int64_t(expected.size()));
        const auto aliases = db.db().query(
            "SELECT cell_id, slot, COUNT(*) AS n FROM _VectorDoc_embedding_vec_ivf_rowid_map00 "
            "GROUP BY cell_id, slot HAVING COUNT(*) <> 1");
        EXPECT_TRUE(aliases.empty());
        for (const auto& item : expected) {
            const auto point = db.db().query(
                "SELECT embedding FROM _VectorDoc_embedding_vec WHERE global_id = ?", {item.first});
            ASSERT_EQ(point.size(), 1u) << item.first;
            EXPECT_EQ(std::get<std::vector<uint8_t>>(point.at(0).at("embedding")), pack_floats(item.second))
                << item.first;
            const auto nearest = db.knn_query("VectorDoc", "embedding", pack_floats(item.second), 1,
                                              lattice::lattice_db::distance_metric::l2);
            ASSERT_EQ(nearest.size(), 1u) << item.first;
            EXPECT_EQ(nearest.at(0).global_id, item.first);
        }
    }
};

TEST_F(IvfSparseMutationTest, DeleteFirstAndMiddleReuseFreeSlotsWithoutOverwritingSurvivors) {
    lattice::lattice_db db;
    image expected;
    seed_ivf(db, expected);
    erase(db, expected, "row-0");
    erase(db, expected, "row-15");
    // Holes alone are already scannable. The old bug overwrote high survivors
    // on the NEXT insert because live count was mistaken for a free slot.
    expect_image(db, expected);
    add(db, expected, "new-32", {32.0f, 1.0f, 2.0f});
    add(db, expected, "new-33", {33.0f, 1.0f, 2.0f});
    expect_image(db, expected);
    update(db, expected, "row-5", {105.0f, 2.0f, 3.0f});
    expect_image(db, expected);
}

TEST_F(IvfSparseMutationTest, TrainingAndTrainedMutationPreserveSparseValuesAndNearestIDs) {
    lattice::lattice_db db;
    image expected;
    seed_ivf(db, expected);
    erase(db, expected, "row-0");
    erase(db, expected, "row-15");
    expect_image(db, expected);
    ASSERT_TRUE(db.train_vec0("VectorDoc", "embedding"));
    expect_image(db, expected);
    erase(db, expected, "row-2");
    add(db, expected, "near-deleted", {2.25f, 1.0f, 2.0f});
    update(db, expected, "row-10", {10.25f, 1.0f, 2.0f});
    expect_image(db, expected);
}

TEST_F(IvfSparseMutationTest, CallerRollbackAndReopenKeepExactOriginalThenCommittedMappings) {
    image original, committed;
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        seed_ivf(db, original);
        ASSERT_TRUE(db.train_vec0("VectorDoc", "embedding"));
        auto changed = original;
        db.begin_transaction();
        erase(db, changed, "row-0");
        erase(db, changed, "row-15");
        add(db, changed, "rolled-back", {40.0f, 1.0f, 2.0f});
        update(db, changed, "row-31", {131.0f, 2.0f, 3.0f});
        expect_image(db, changed);
        db.rollback();
        EXPECT_FALSE(db.db().is_in_transaction());
        expect_image(db, original);
    }
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        expect_image(db, original);
        committed = original;
        db.begin_transaction();
        erase(db, committed, "row-0");
        erase(db, committed, "row-15");
        add(db, committed, "committed", {40.0f, 1.0f, 2.0f});
        update(db, committed, "row-31", {131.0f, 2.0f, 3.0f});
        db.commit();
        EXPECT_FALSE(db.db().is_in_transaction());
        expect_image(db, committed);
    }
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        expect_image(db, committed);
    }
}

TEST_F(IvfSparseMutationTest, LateRowMapFailurePreservesCallerPrefixAndRestoresEveryCell) {
    lattice::lattice_db db;
    image expected;
    seed_ivf(db, expected);
    db.begin_transaction();
    add(db, expected, "caller-prefix", {40.0f, 1.0f, 2.0f});
    erase(db, expected, "row-0");
    expect_image(db, expected);
    // Real failure AFTER the candidate slot's bitmap/payload/count writes.
    // No fake result counter and no repair SQL for the attempted vector.
    db.db().execute(
        "CREATE TRIGGER sparse_map_deny BEFORE INSERT ON _VectorDoc_embedding_vec_ivf_rowid_map00 "
        "BEGIN SELECT RAISE(ABORT, 'IVF rowmap denial'); END");
    bool failed = false;
    try {
        insertDoc(db, "must-not-exist", "denied", {50.0f, 1.0f, 2.0f});
    } catch (const lattice::db_error& error) {
        failed = true;
        EXPECT_NE(std::string(error.what()).find("IVF rowmap denial"), std::string::npos);
    }
    EXPECT_TRUE(failed);
    ASSERT_TRUE(db.db().is_in_transaction());
    db.db().execute("DROP TRIGGER sparse_map_deny");
    expect_image(db, expected);
    add(db, expected, "after-denial", {51.0f, 1.0f, 2.0f});
    db.commit();
    EXPECT_FALSE(db.db().is_in_transaction());
    expect_image(db, expected);
}


TEST_F(IvfSparseMutationTest, QuantizedCommandsPreserveSparseSlotsAndActualCentroidIDs) {
    for (const std::string quantizer : {"int8", "binary"}) {
        lattice::lattice_db db;
        db.db().execute("CREATE VIRTUAL TABLE _q USING vec0(global_id TEXT PRIMARY KEY, "
                        "embedding float[8] indexed by ivf(nlist=2,nprobe=2,quantizer=" + quantizer + ",oversample=2))");
        image expected;
        for (int i=0;i<32;++i) {
            std::vector<float> v(8);
            for (int bit=0;bit<8;++bit) v[bit]=(i&(1<<bit))?0.75f:-0.75f;
            const auto id="q-"+std::to_string(i);expected[id]=v;
            db.db().execute("INSERT INTO _q(global_id,embedding) VALUES (?,?)",{id,pack_floats(v)});
        }
        db.db().execute("DELETE FROM _q WHERE global_id IN ('q-0','q-15')");
        expected.erase("q-0");expected.erase("q-15");
        const std::vector<float> extra{0.5f,-0.5f,0.5f,-0.5f,0.5f,-0.5f,0.5f,-0.5f};
        db.db().execute("INSERT INTO _q(global_id,embedding) VALUES (?,?)",{std::string("extra"),pack_floats(extra)});
        expected["extra"]=extra;
        auto verify=[&] {
            for(const auto& item:expected){
                const auto point=db.db().query("SELECT embedding FROM _q WHERE global_id=?",{item.first});
                ASSERT_EQ(point.size(),1u);
                EXPECT_EQ(std::get<std::vector<uint8_t>>(point.at(0).at("embedding")),pack_floats(item.second));
            }
            const auto found=db.db().query(
                "SELECT global_id FROM _q WHERE embedding MATCH ? AND k=? ORDER BY distance",
                {pack_floats(extra),int64_t(expected.size())});
            std::set<std::string> ids;
            for(const auto& row:found)ids.insert(std::get<std::string>(row.at("global_id")));
            EXPECT_EQ(found.size(),expected.size());EXPECT_EQ(ids.size(),expected.size());
            for(const auto& item:expected)EXPECT_EQ(ids.count(item.first),1u);
        };
        verify();
        // Non-contiguous actual centroid IDs must not become array indices.
        db.db().execute("INSERT INTO _q(_q,embedding) VALUES ('set-centroid:17',?)",{pack_floats(std::vector<float>(8,-0.75f))});
        db.db().execute("INSERT INTO _q(_q,embedding) VALUES ('set-centroid:43',?)",{pack_floats(std::vector<float>(8,0.75f))});
        db.db().execute("INSERT INTO _q(_q) VALUES ('assign-vectors')");
        const auto bad=db.db().query("SELECT 1 FROM _q_ivf_cells00 WHERE centroid_id NOT IN (17,43)");
        EXPECT_TRUE(bad.empty());verify();
        db.db().execute("INSERT INTO _q(_q) VALUES ('clear-centroids')");
        const auto assigned=db.db().query("SELECT 1 FROM _q_ivf_cells00 WHERE centroid_id<>-1");
        EXPECT_TRUE(assigned.empty());verify();
        const auto changed=std::vector<float>(8,0.25f);
        db.db().execute("UPDATE _q SET embedding=? WHERE global_id='q-31'",{pack_floats(changed)});
        expected["q-31"]=changed;verify();
    }
}

class IvfCommandRollbackTest : public Vec0Test {
protected:
    enum class command { train, clear, set };
    using point_image = std::map<std::string, std::vector<float>>;
    using shadow_image = std::vector<std::vector<lattice::database::row_t>>;

    void put(lattice::lattice_db& db, point_image& expected,
             const std::string& id, const std::vector<float>& value) {
        db.db().execute("INSERT INTO _q(global_id,embedding) VALUES (?,?)",
                        {id, pack_floats(value)});
        expected[id] = value;
    }
    void seed(lattice::lattice_db& db, const std::string& quantizer, point_image& expected) {
        db.db().execute("CREATE VIRTUAL TABLE _q USING vec0(global_id TEXT PRIMARY KEY, "
                        "embedding float[8] indexed by ivf(nlist=2,nprobe=2,quantizer=" +
                        quantizer + ",oversample=" + (quantizer == "none" ? "1" : "2") + "))");
        db.db().execute("CREATE TABLE ivf_command_prefix(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL UNIQUE, value INTEGER NOT NULL UNIQUE)");
        db.db().execute("INSERT INTO ivf_command_prefix(globalId,value) VALUES ('prefix-11',11)");
        for (int id = 0; id < 32; ++id) {
            std::vector<float> value(8);
            for (int bit = 0; bit < 8; ++bit)
                value[bit] = (id & (1 << bit)) ? 0.75f : -0.75f;
            put(db, expected, "q-" + std::to_string(id), value);
        }
        db.db().execute("DELETE FROM _q WHERE global_id IN ('q-0','q-15')");
        expected.erase("q-0"); expected.erase("q-15");
        put(db, expected, "sparse-extra", std::vector<float>(8, 0.125f));
    }
    ::testing::AssertionResult values_match(lattice::lattice_db& db, const point_image& expected) {
        const auto count = db.db().query("SELECT COUNT(*) AS n FROM _q");
        if (count.size() != 1 || std::get<int64_t>(count.at(0).at("n")) != int64_t(expected.size()))
            return ::testing::AssertionFailure() << "point count differs";
        for (const auto& item : expected) {
            const auto point = db.db().query("SELECT embedding FROM _q WHERE global_id=?", {item.first});
            if (point.size() != 1 ||
                std::get<std::vector<uint8_t>>(point.at(0).at("embedding")) != pack_floats(item.second))
                return ::testing::AssertionFailure() << "full point bytes differ for " << item.first;
        }
        const auto found = db.db().query(
            "SELECT global_id,embedding FROM _q WHERE embedding MATCH ? AND k=128 ORDER BY distance",
            {pack_floats(std::vector<float>(8, 0.0f))});
        std::set<std::string> seen;
        for (const auto& row : found) {
            const auto& id = std::get<std::string>(row.at("global_id"));
            const auto item = expected.find(id);
            if (item == expected.end() || !seen.insert(id).second ||
                std::get<std::vector<uint8_t>>(row.at("embedding")) != pack_floats(item->second))
                return ::testing::AssertionFailure() << "KNN identity or full vector bytes differ for " << id;
        }
        if (found.size() != expected.size() || seen.size() != expected.size())
            return ::testing::AssertionFailure() << "KNN omitted or duplicated an original";
        return ::testing::AssertionSuccess();
    }
    shadow_image shadows(lattice::lattice_db& db, const std::string& quantizer) {
        shadow_image result;
        for (const auto* sql : {
                 "SELECT rowid,* FROM _q_ivf_cells00 ORDER BY rowid",
                 "SELECT * FROM _q_ivf_rowid_map00 ORDER BY rowid",
                 "SELECT * FROM _q_rowids ORDER BY rowid",
                 "SELECT key,value FROM _q_info ORDER BY key",
                 "SELECT centroid_id,centroid FROM _q_ivf_centroids00 ORDER BY centroid_id",
                 "SELECT value FROM ivf_command_prefix ORDER BY value"})
            result.push_back(db.db().query(sql));
        if (quantizer != "none")
            result.push_back(db.db().query("SELECT rowid,vector FROM _q_ivf_vectors00 ORDER BY rowid"));
        return result;
    }
    int64_t trained(lattice::lattice_db& db) {
        const auto rows = db.db().query(
            "SELECT CAST(value AS INTEGER) AS trained FROM _q_info WHERE key='ivf_trained_0'");
        return std::get<int64_t>(rows.at(0).at("trained"));
    }
    void exercise(const std::string& quantizer, command actual, bool rollback_to) {
        lattice::lattice_db db;
        point_image original;
        seed(db, quantizer, original);
        ASSERT_TRUE(values_match(db, original)); // prime actual untrained cache
        const bool originally_trained = actual == command::clear;
        if (originally_trained) {
            db.db().execute("INSERT INTO _q(_q) VALUES ('compute-centroids')");
            ASSERT_TRUE(values_match(db, original));
        }
        ASSERT_EQ(trained(db), originally_trained ? 1 : 0);
        auto expected = original;
        auto restored_shadows = shadows(db, quantizer);

        db.begin_transaction();
        db.db().execute("INSERT INTO ivf_command_prefix(globalId,value) VALUES ('prefix-22',22)");
        put(db, expected, "caller-prefix", std::vector<float>(8, 0.25f));
        ASSERT_TRUE(values_match(db, expected));
        if (rollback_to) {
            // A genuine caller prefix predates this actual savepoint. Neither
            // rollback-to nor the module's cache invalidation may discard it.
            restored_shadows = shadows(db, quantizer);
            db.db().execute("SAVEPOINT ivf_command_rollback");
        }
        const auto before_command = shadows(db, quantizer);
        if (actual == command::train) {
            db.db().execute("INSERT INTO _q(_q) VALUES ('compute-centroids')");
        } else if (actual == command::clear) {
            db.db().execute("INSERT INTO _q(_q) VALUES ('clear-centroids')");
        } else {
            db.db().execute("INSERT INTO _q(_q,embedding) VALUES ('set-centroid:17',?)",
                            {pack_floats(std::vector<float>(8, -0.75f))});
            const auto custom = db.db().query("SELECT centroid_id FROM _q_ivf_centroids00 ORDER BY centroid_id");
            ASSERT_EQ(custom.size(), 1u);
            ASSERT_EQ(std::get<int64_t>(custom.at(0).at("centroid_id")), 17);
        }
        ASSERT_TRUE(db.db().is_in_transaction());
        ASSERT_EQ(trained(db), originally_trained ? 0 : 1);
        ASSERT_NE(shadows(db, quantizer), before_command) << "the actual command must change SQL state";
        // Standalone set-centroid is observed through actual marker/centroid
        // bytes. No unqualified intermediate set-centroid query is required.
        if (actual != command::set) ASSERT_TRUE(values_match(db, expected));

        if (rollback_to) {
            db.db().execute("ROLLBACK TO ivf_command_rollback");
            ASSERT_TRUE(db.db().is_in_transaction());
        } else {
            db.rollback();
            ASSERT_FALSE(db.db().is_in_transaction());
            expected = original;
        }
        // Exact cell/map/rowid/marker/centroid/full-precision blob values and
        // the original public point+KNN oracle must BOTH recover.
        ASSERT_EQ(shadows(db, quantizer), restored_shadows);
        ASSERT_EQ(trained(db), originally_trained ? 1 : 0);
        ASSERT_TRUE(values_match(db, expected));
        const auto prefix = db.db().query("SELECT value FROM ivf_command_prefix ORDER BY value");
        ASSERT_EQ(prefix.size(), rollback_to ? 2u : 1u);
        ASSERT_EQ(std::get<int64_t>(prefix.at(0).at("value")), 11);
        if (rollback_to) {
            ASSERT_EQ(std::get<int64_t>(prefix.at(1).at("value")), 22);
            db.db().execute("RELEASE ivf_command_rollback");
            ASSERT_TRUE(db.db().is_in_transaction());
        }
        put(db, expected, "after-rollback", std::vector<float>(8, 0.5f));
        ASSERT_TRUE(values_match(db, expected));
        if (rollback_to) db.commit();
        ASSERT_FALSE(db.db().is_in_transaction());
        ASSERT_TRUE(values_match(db, expected));
    }
};

TEST_F(IvfCommandRollbackTest, ActualTrainingCallerRollbackRestoresFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::train, false));
    }
}
TEST_F(IvfCommandRollbackTest, ActualClearCallerRollbackRestoresFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::clear, false));
    }
}
TEST_F(IvfCommandRollbackTest, ActualCustomCentroidCallerRollbackRestoresFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::set, false));
    }
}
TEST_F(IvfCommandRollbackTest, ActualTrainingRollbackToRetainsCallerPrefixFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::train, true));
    }
}
TEST_F(IvfCommandRollbackTest, ActualClearRollbackToRetainsCallerPrefixFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::clear, true));
    }
}
TEST_F(IvfCommandRollbackTest, ActualCustomCentroidRollbackToRetainsCallerPrefixFullPointsKnnAndShadowBytes) {
    for (const std::string quantizer : {"none", "int8", "binary"}) {
        SCOPED_TRACE(quantizer);
        ASSERT_NO_FATAL_FAILURE(exercise(quantizer, command::set, true));
    }
}
