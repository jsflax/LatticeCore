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
