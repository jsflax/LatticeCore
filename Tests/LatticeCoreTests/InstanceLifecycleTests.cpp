#include "TestHelpers.hpp"
#include <sys/socket.h>
#include <unistd.h>

// ============================================================================
// Instance Lifecycle Tests — registry, close/reopen, memory modes
// ============================================================================

TEST(Lifecycle, InMemoryIsolation) {
    lattice::lattice_db db1;
    lattice::lattice_db db2;

    db1.add(TestPerson{"OnlyInDb1", 25, std::nullopt});
    db2.add(TestDog{"OnlyInDb2", 10.0, true});

    // Each sees only its own data
    EXPECT_EQ(db1.objects<TestPerson>().size(), 1u);
    EXPECT_EQ(db1.objects<TestDog>().size(), 0u);
    EXPECT_EQ(db2.objects<TestPerson>().size(), 0u);
    EXPECT_EQ(db2.objects<TestDog>().size(), 1u);
}

TEST(Lifecycle, CloseReopenReadDb) {
    TempDB tmp{"closereopen"};

    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Alice", 30, std::nullopt});
        db.add(TestDog{"Max", 20.0, true});
    }

    // Reopen and verify
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
        EXPECT_EQ(db.objects<TestDog>().size(), 1u);

        auto person = db.objects<TestPerson>();
        EXPECT_EQ(std::string(person[0].name), "Alice");
    }
}

TEST(Lifecycle, MultipleReopens) {
    TempDB tmp{"multireopen"};

    for (int i = 0; i < 5; i++) {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Person" + std::to_string(i), i * 10, std::nullopt});
    }

    lattice::lattice_db db(lattice::configuration(tmp.str()));
    EXPECT_EQ(db.objects<TestPerson>().size(), 5u);
}

TEST(Lifecycle, ConcurrentReads) {
    TempDB tmp{"concurrent"};

    // Seed data
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.write([&]() {
            for (int i = 0; i < 10; i++) {
                db.add(TestPerson{"P" + std::to_string(i), i, std::nullopt});
            }
        });
    }

    // Multiple instances reading concurrently
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&] {
            try {
                lattice::lattice_db db(lattice::configuration(tmp.str()));
                auto count = db.objects<TestPerson>().size();
                if (count == 10) success_count++;
            } catch (...) {
                // Database busy is expected under contention
            }
        });
    }

    for (auto& t : threads) t.join();
    // At least one reader should succeed
    EXPECT_GE(success_count.load(), 1);
}

TEST(Lifecycle, DestructorCleansUp) {
    TempDB tmp{"cleanup"};

    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Temp", 25, std::nullopt});
        // db goes out of scope — destructor runs
    }

    // Should still be able to reopen
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
}

TEST(Lifecycle, EmptyDatabase) {
    lattice::lattice_db db;

    // All queries on empty DB should return 0
    EXPECT_EQ(db.objects<TestPerson>().size(), 0u);
    EXPECT_EQ(db.objects<TestDog>().size(), 0u);
    EXPECT_EQ(db.objects<TestTrip>().size(), 0u);

    EXPECT_FALSE(db.find<TestPerson>(1).has_value());
    EXPECT_FALSE(db.find_by_global_id<TestPerson>("nonexistent").has_value());
}

// IPC socket path resolution
TEST(Lifecycle, IPCSocketPathResolve) {
    auto path = lattice::resolve_ipc_socket_path("test-channel");
    EXPECT_FALSE(path.empty());
    EXPECT_NE(path.find("test-channel"), std::string::npos);
#ifdef __APPLE__
    EXPECT_NE(path.find("Lattice/ipc"), std::string::npos);
#endif
}

// IPC length-prefix framing
TEST(Lifecycle, LengthPrefixFraming) {
    // Create a socketpair for testing
    int fds[2];
    int ret = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    ASSERT_EQ(ret, 0);

    // Test single byte
    std::vector<uint8_t> data = {0x42};
    ASSERT_TRUE(lattice::write_length_prefixed(fds[0], data.data(), data.size()));
    auto received = lattice::read_length_prefixed(fds[1]);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0], 0x42);

    // Test multi-byte message
    std::string msg = "Hello, length-prefix!";
    ASSERT_TRUE(lattice::write_length_prefixed(
        fds[0], msg.data(), static_cast<uint32_t>(msg.size())));
    received = lattice::read_length_prefixed(fds[1]);
    ASSERT_EQ(received.size(), msg.size());
    EXPECT_EQ(std::string(received.begin(), received.end()), msg);

    // Test large message (1MB) — needs threads to avoid socketpair deadlock
    std::vector<uint8_t> large(1024 * 1024, 0xAB);
    std::vector<uint8_t> large_received;
    std::thread writer([&] {
        lattice::write_length_prefixed(
            fds[0], large.data(), static_cast<uint32_t>(large.size()));
    });
    large_received = lattice::read_length_prefixed(fds[1]);
    writer.join();
    EXPECT_EQ(large_received.size(), large.size());

    close(fds[0]);
    close(fds[1]);
}
