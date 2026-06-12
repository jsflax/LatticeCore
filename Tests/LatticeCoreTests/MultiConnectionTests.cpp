#include "TestHelpers.hpp"

// ============================================================================
// Multi-Connection Tests — two lattice_db instances on the same file
// ============================================================================

class MultiConnTest : public ::testing::Test {
protected:
    TempDB tmp{"multiconn"};
};

TEST_F(MultiConnTest, WriteOnAReadOnB) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    db_a.add(TestPerson{"Alice", 30, std::nullopt});

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    auto persons = db_b.objects<TestPerson>();
    EXPECT_EQ(persons.size(), 1u);
    EXPECT_EQ(std::string(persons[0].name), "Alice");
}

TEST_F(MultiConnTest, BidirectionalWrites) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    db_a.add(TestPerson{"FromA", 20, std::nullopt});
    db_b.add(TestPerson{"FromB", 25, std::nullopt});

    // Both should see both rows
    EXPECT_EQ(db_a.objects<TestPerson>().size(), 2u);
    EXPECT_EQ(db_b.objects<TestPerson>().size(), 2u);
}

TEST_F(MultiConnTest, UpdateVisibility) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    auto person = db_a.add(TestPerson{"Original", 30, std::nullopt});
    person.name = "Updated";

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    auto persons = db_b.objects<TestPerson>();
    ASSERT_EQ(persons.size(), 1u);
    EXPECT_EQ(std::string(persons[0].name), "Updated");
}

TEST_F(MultiConnTest, DeleteVisibility) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    auto person = db_a.add(TestPerson{"ToDelete", 30, std::nullopt});
    db_a.remove(person);

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    EXPECT_EQ(db_b.objects<TestPerson>().size(), 0u);
}

TEST_F(MultiConnTest, AuditLogCrossConnection) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    db_a.db().execute("DELETE FROM AuditLog");
    db_a.add(TestPerson{"Audited", 30, std::nullopt});

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    auto logs = db_b.db().query(
        "SELECT COUNT(*) as cnt FROM AuditLog WHERE tableName = 'TestPerson'");
    EXPECT_GE(std::get<int64_t>(logs[0].at("cnt")), 1);
}

TEST_F(MultiConnTest, ConcurrentReads) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    // Seed data
    db_a.write([&]() {
        for (int i = 0; i < 50; i++) {
            db_a.add(TestPerson{"P" + std::to_string(i), i, std::nullopt});
        }
    });

    // Two readers in parallel
    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    lattice::lattice_db db_c(lattice::configuration(tmp.str()));

    std::atomic<size_t> count_b{0}, count_c{0};

    std::thread t1([&] { count_b = db_b.objects<TestPerson>().size(); });
    std::thread t2([&] { count_c = db_c.objects<TestPerson>().size(); });

    t1.join();
    t2.join();

    EXPECT_EQ(count_b.load(), 50u);
    EXPECT_EQ(count_c.load(), 50u);
}

TEST_F(MultiConnTest, WALCheckpointVisibility) {
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    db_a.add(TestPerson{"PreCheckpoint", 30, std::nullopt});

    // Force WAL checkpoint
    db_a.db().execute("PRAGMA wal_checkpoint(TRUNCATE)");

    lattice::lattice_db db_b(lattice::configuration(tmp.str()));
    auto persons = db_b.objects<TestPerson>();
    ASSERT_EQ(persons.size(), 1u);
    EXPECT_EQ(std::string(persons[0].name), "PreCheckpoint");
}

// ============================================================================
// File-based database persistence (from legacy tests)
// ============================================================================

TEST_F(MultiConnTest, FilePersistence) {
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestTrip{"Persistent Trip", 7, std::nullopt});
    }
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        auto trips = db.objects<TestTrip>();
        ASSERT_EQ(trips.size(), 1u);
        EXPECT_EQ(std::string(trips[0].name), "Persistent Trip");
        EXPECT_EQ(int(trips[0].days), 7);
    }
}

// ============================================================================
// Open-path contention — concurrent first opens must wait, not throw
// ============================================================================

TEST(MultiConnection, ConcurrentFirstOpenWaitsForWriteLock) {
    TempDB tmp{"concopen"};

    // External connection holds the write lock on a FRESH database while a
    // lattice_db opens it. The slow path's BEGIN IMMEDIATE must wait via
    // begin_transaction's backoff and then succeed — previously the bare
    // ensure writes threw "database is locked" and killed the open.
    lattice::database raw(tmp.str());
    raw.begin_transaction();

    std::atomic<bool> opened{false};
    std::atomic<bool> failed{false};
    std::thread opener([&] {
        try {
            lattice::lattice_db db(tmp.str());
            db.add(TestPerson{"Concurrent", 1, std::nullopt});
            opened.store(true);
        } catch (...) {
            failed.store(true);
        }
    });

    // Hold the lock long enough that the opener definitely contends.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_FALSE(opened.load());  // opener should still be waiting
    raw.rollback();

    opener.join();
    EXPECT_TRUE(opened.load());
    EXPECT_FALSE(failed.load());
}

TEST(MultiConnection, BeginTransactionDeadlineBounded) {
    // begin_transaction must give up within ~30s wall clock when the lock is
    // never released. The old code busy-waited the statement timeout inside
    // EVERY retry attempt while only counting its own sleeps — worst case
    // measured in tens of minutes.
    TempDB tmp{"deadline"};
    lattice::database holder(tmp.str());
    holder.execute("CREATE TABLE t(x INTEGER)");
    holder.begin_transaction();

    lattice::database contender(tmp.str());
    auto start = std::chrono::steady_clock::now();
    EXPECT_THROW(contender.begin_transaction(), lattice::db_error);
    auto waited = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start);

    EXPECT_GE(waited.count(), 25);
    EXPECT_LE(waited.count(), 45);

    holder.rollback();
}
