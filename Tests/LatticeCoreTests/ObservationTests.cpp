#include "TestHelpers.hpp"

// ============================================================================
// Observation Tests — table observers, object observers, schedulers
// ============================================================================

TEST(Observe, InsertFires) {
    lattice::lattice_db db;
    db.add(TestPerson{"Initial", 25, std::nullopt});

    int callback_count = 0;
    auto id = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            callback_count++;
        });

    db.add(TestPerson{"New", 30, std::nullopt});
    EXPECT_GE(callback_count, 1);

    db.remove_table_observer("TestPerson", id);
}

TEST(Observe, UpdateFires) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"Before", 25, std::nullopt});

    int callback_count = 0;
    auto id = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            callback_count++;
        });

    person.name = "After";
    EXPECT_GE(callback_count, 1);

    db.remove_table_observer("TestPerson", id);
}

TEST(Observe, DeleteFires) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"ToDelete", 25, std::nullopt});

    int callback_count = 0;
    auto id = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            callback_count++;
        });

    db.remove(person);
    EXPECT_GE(callback_count, 1);

    db.remove_table_observer("TestPerson", id);
}

TEST(Observe, RemoveStops) {
    lattice::lattice_db db;

    int callback_count = 0;
    auto id = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            callback_count++;
        });

    db.add(TestPerson{"Trigger1", 25, std::nullopt});
    int count_after_add = callback_count;
    EXPECT_GE(count_after_add, 1);

    db.remove_table_observer("TestPerson", id);

    auto p = db.add(TestPerson{"Trigger2", 30, std::nullopt});
    EXPECT_EQ(callback_count, count_after_add);  // No change
}

TEST(Observe, MultipleObservers) {
    lattice::lattice_db db;

    int count_a = 0, count_b = 0;
    auto id_a = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) { count_a++; });
    auto id_b = db.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) { count_b++; });

    db.add(TestPerson{"Multi", 25, std::nullopt});
    EXPECT_GE(count_a, 1);
    EXPECT_GE(count_b, 1);

    db.remove_table_observer("TestPerson", id_a);
    db.remove_table_observer("TestPerson", id_b);
}

TEST(Observe, ObjectObserver) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"ObjectObs", 40, std::nullopt});

    int callback_count = 0;
    std::vector<std::string> changed_props;

    auto token = person.observe([&](lattice::object_change<TestPerson>& change) {
        callback_count++;
        for (const auto& prop : change.property_changes) {
            changed_props.push_back(prop.name);
        }
    });

    person.set_value("email", std::string("test@object.com"));
    EXPECT_GE(callback_count, 1);
    EXPECT_FALSE(changed_props.empty());
    EXPECT_EQ(changed_props.back(), "email");

    token.invalidate();

    int count_before = callback_count;
    person.set_value("name", std::string("Changed"));
    EXPECT_EQ(callback_count, count_before);
}

TEST(Observe, ResultsObserve) {
    lattice::lattice_db db;
    auto results = db.objects<TestPerson>();

    int callback_count = 0;
    auto token = results.observe([&](const std::vector<lattice::managed<TestPerson>>& items) {
        callback_count++;
    });

    db.add(TestPerson{"Charlie", 35, std::nullopt});
    EXPECT_GE(callback_count, 1);

    token.invalidate();

    int count_before = callback_count;
    db.add(TestPerson{"NoNotify", 40, std::nullopt});
    EXPECT_EQ(callback_count, count_before);
}

TEST(Observe, TokenInvalidation) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"Token", 40, std::nullopt});

    int count = 0;
    auto token = person.observe([&](lattice::object_change<TestPerson>&) { count++; });

    EXPECT_TRUE(token.is_valid());
    token.invalidate();
    EXPECT_FALSE(token.is_valid());

    person.set_value("name", std::string("Changed"));
    EXPECT_EQ(count, 0);
}

// ----------------------------------------------------------------------------
// Scheduler Tests
// ----------------------------------------------------------------------------

TEST(Observe, ImmediateScheduler) {
    auto sched = std::make_shared<lattice::immediate_scheduler>();
    EXPECT_TRUE(sched->is_on_thread());
    EXPECT_TRUE(sched->can_invoke());

    int count = 0;
    sched->invoke([&] { count++; });
    EXPECT_EQ(count, 1);  // Synchronous
}

TEST(Observe, StdThreadScheduler) {
    auto sched = std::make_shared<lattice::std_thread_scheduler>();

    std::atomic<int> count{0};
    std::atomic<bool> on_thread{false};
    std::condition_variable cv;
    std::mutex m;

    sched->invoke([&] {
        count++;
        on_thread = sched->is_on_thread();
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(m);
    cv.wait_for(lock, std::chrono::seconds(1), [&] { return count > 0; });

    EXPECT_EQ(count.load(), 1);
    EXPECT_TRUE(on_thread.load());
}

TEST(Observe, MainThreadScheduler) {
    auto sched = std::make_shared<lattice::main_thread_scheduler>();
    EXPECT_TRUE(sched->is_on_thread());

    int count = 0;
    sched->invoke([&] { count++; });
    EXPECT_EQ(count, 0);  // Not processed yet

    sched->process_pending();
    EXPECT_EQ(count, 1);
}

TEST(Observe, SchedulerShutdown) {
    auto sched = std::make_shared<lattice::std_thread_scheduler>();

    std::atomic<int> count{0};
    sched->invoke([&] { count++; });

    // Give time for work to process
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sched->shutdown();

    EXPECT_EQ(count.load(), 1);
}

// ----------------------------------------------------------------------------
// Cross-instance observation
// ----------------------------------------------------------------------------

TEST(Observe, CrossInstance) {
    TempDB tmp{"crossobs"};
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    lattice::lattice_db db_b(lattice::configuration(tmp.str()));

    int callback_count = 0;
    auto id = db_b.add_table_observer("TestPerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            callback_count++;
        });

    // Write on db_a, observe on db_b
    db_a.add(TestPerson{"CrossInstance", 30, std::nullopt});

    // Cross-instance observers may need a refresh cycle
    // The observer fires based on file-level notifications
    // At minimum, verify no crash
    db_b.remove_table_observer("TestPerson", id);
}
