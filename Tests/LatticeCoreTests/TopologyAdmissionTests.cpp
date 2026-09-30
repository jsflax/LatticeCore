#include "TestHelpers.hpp"
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <pwd.h>
#include <thread>
#include <unistd.h>

namespace lattice {
// Actual handles and map observations only. No replacement connection, granted
// route, simulated SQLite result, or adjustable production deadline is exposed.
struct topology_admission_test_access {
    static std::shared_ptr<database> writer(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        return owner.db_;
    }
    static std::shared_ptr<database> reader(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        return owner.read_db_;
    }
    static sqlite3* raw(database& db) { return db.internal_handle(); }
    static bool valid(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.attach_mutex_);
        return owner.attachment_topology_valid_;
    }
    static size_t tokens(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.attach_mutex_);
        return owner.attached_route_tokens_.size();
    }
    static bool topology_unlocked(lattice_db& owner) {
        if (!owner.attach_mutex_.try_lock()) return false;
        owner.attach_mutex_.unlock(); return true;
    }
    static std::mutex& ownership_mutex(lattice_db& owner) { return owner.connection_ownership_mutex_; }
    static std::mutex& topology_mutex(lattice_db& owner) { return owner.attach_mutex_; }
    static void retire_reader(lattice_db& owner) { owner.close_read_db(); }
    static void with_metadata(lattice_db& owner, lattice_db& arm, std::shared_ptr<const void> value) {
        owner.attach_with_metadata(arm, std::move(value));
    }
};
}

namespace {
using namespace lattice;
using namespace std::chrono_literals;
using topology_access = topology_admission_test_access;
using clock_type = std::chrono::steady_clock;

struct local_paths {
    std::filesystem::path root;
    local_paths() {
        const auto* account = getpwuid(getuid());
        if (!account || !account->pw_dir) throw std::runtime_error("test account home unavailable");
        const auto parent = std::filesystem::path(account->pw_dir) / "localdev";
        std::filesystem::create_directories(parent);
        std::string pattern = (parent / "lattice-topology-admission-XXXXXX").string();
        auto* result = mkdtemp(pattern.data());
        if (!result) throw std::runtime_error("localdev test scratch creation failed");
        root = result;
    }
    ~local_paths() { std::error_code error; std::filesystem::remove_all(root, error); }
    std::string file(const char* name) const { return (root / name).string(); }
};
struct pair_fixture {
    local_paths paths;
    lattice_db parent{paths.file("parent.sqlite")}, arm{paths.file("arm.sqlite")};
    pair_fixture() {
        for (auto* owner : {&parent, &arm}) {
            owner->db().execute("CREATE TABLE Fixture (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL, n INTEGER)");
            owner->db().execute("INSERT INTO Fixture VALUES (1, 'row-one', 7)");
        }
    }
};

// A test must never strand the hosted suite while diagnosing a regression.
// This is failure-only process termination, not positive completion evidence.
struct terminal_watchdog {
    std::mutex mutex;
    std::condition_variable changed;
    bool done = false;
    std::thread worker;
    terminal_watchdog() : worker([this] {
        std::unique_lock<std::mutex> lock(mutex);
        if (!changed.wait_for(lock, 15s, [&] { return done; })) std::abort();
    }) {}
    ~terminal_watchdog() {
        { std::lock_guard<std::mutex> lock(mutex); done = true; }
        changed.notify_all(); worker.join();
    }
};

// This separate thread owns the REAL recursive SQLite connection mutex. A
// cleanup release after its budget is a failed oracle, even if SQL later works.
struct held_connection {
    std::shared_ptr<database> retained;
    std::mutex mutex;
    std::condition_variable changed;
    bool ready = false, released = false, forced = false;
    int after_release_rc = SQLITE_ERROR;
    std::thread worker;
    explicit held_connection(std::shared_ptr<database> db,
                             std::chrono::milliseconds cleanup_budget = 5s)
        : retained(std::move(db)), worker([this, cleanup_budget] {
            auto* raw = topology_access::raw(*retained);
            auto* sqlite_mutex = sqlite3_db_mutex(raw);
            if (!sqlite_mutex) {
                std::lock_guard<std::mutex> lock(mutex); ready = true; forced = true;
                changed.notify_all(); return;
            }
            sqlite3_mutex_enter(sqlite_mutex);
            {
                std::unique_lock<std::mutex> lock(mutex);
                ready = true; changed.notify_all();
                if (!changed.wait_for(lock, cleanup_budget, [&] { return released; })) forced = true;
            }
            // A genuine statement after refusal proves the holder was neither
            // interrupted nor logically rolled back by the waiting operation.
            after_release_rc = sqlite3_exec(raw, "SELECT 1", nullptr, nullptr, nullptr);
            sqlite3_mutex_leave(sqlite_mutex);
        }) {}
    bool await_ready() {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, 3s, [&] { return ready; });
    }
    bool forced_release() {
        std::lock_guard<std::mutex> lock(mutex); return forced;
    }
    void release() {
        { std::lock_guard<std::mutex> lock(mutex); released = true; }
        changed.notify_all();
        if (worker.joinable()) worker.join();
    }
    ~held_connection() { release(); }
};

struct authorizer {
    sqlite3* handle;
    std::function<int(int)> observe;
    std::exception_ptr error;
    authorizer(database& db, std::function<int(int)> body)
        : handle(topology_access::raw(db)), observe(std::move(body)) {
        const int rc = sqlite3_set_authorizer(handle,
            [](void* context, int action, const char*, const char*, const char*, const char*) noexcept {
                auto& self = *static_cast<authorizer*>(context);
                try { return self.observe(action); }
                catch (...) { self.error = std::current_exception(); return SQLITE_DENY; }
            }, this);
        if (rc != SQLITE_OK) throw std::runtime_error("actual authorizer setup failed");
    }
    ~authorizer() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
};

bool physically_attached(database& db) {
    for (const auto& row : db.query("PRAGMA database_list")) {
        const auto found = row.find("name");
        if (found != row.end() && std::get<std::string>(found->second) == "arm") return true;
    }
    return false;
}
std::string refusal(const std::function<void()>& action) {
    try { action(); return {}; }
    catch (const db_error& error) { return error.what(); }
}
void expect_deadline(const std::string& result) {
    EXPECT_NE(result.find("topology admission deadline exceeded"), std::string::npos) << result;
}
void expect_no_attachment(pair_fixture& fixture) {
    EXPECT_FALSE(physically_attached(fixture.parent.db()));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_TRUE(topology_access::valid(fixture.parent));
}
}

TEST(TopologyAdmission, RecipientMutexRefusesBeforeCleanupAndLeavesNoTopologyEffects) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    held_connection held(topology_access::writer(fixture.parent));
    ASSERT_TRUE(held.await_ready());
    const auto started = clock_type::now();
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    const auto elapsed = clock_type::now() - started;
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_GE(elapsed, 2s);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    expect_no_attachment(fixture);
    EXPECT_NO_THROW(fixture.parent.attach(fixture.arm));
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, SourceMutexUsesSameTwoSecondBudgetRatherThanIdentityBusyTimeout) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    held_connection held(topology_access::writer(fixture.arm));
    ASSERT_TRUE(held.await_ready());
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    expect_no_attachment(fixture);
    EXPECT_NO_THROW(fixture.parent.attach(fixture.arm));
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, ContendedReaderLeavesActualPartialWriterAttachFenced) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto reader = topology_access::reader(fixture.parent);
    ASSERT_NE(reader, nullptr);
    held_connection held(reader);
    ASSERT_TRUE(held.await_ready());
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_FALSE(physically_attached(*reader));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_FALSE(topology_access::valid(fixture.parent));
    // Existing ATTACH is not cross-handle atomic. No manufactured retry token
    // or pretend rollback is used to "repair" the actual partial binding.
    EXPECT_THROW(fixture.parent.attach(fixture.arm), db_error);
}

TEST(TopologyAdmission, DetachRefusalKeepsPhysicalBindingButRetiresItsRoute) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    fixture.parent.attach(fixture.arm);
    ASSERT_EQ(topology_access::tokens(fixture.parent), 1u);
    held_connection held(topology_access::writer(fixture.parent));
    ASSERT_TRUE(held.await_ready());
    const auto error = refusal([&] { fixture.parent.detach(fixture.arm); });
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_FALSE(topology_access::valid(fixture.parent));
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
    EXPECT_FALSE(physically_attached(fixture.parent.db()));
    EXPECT_TRUE(topology_access::valid(fixture.parent));
}

TEST(TopologyAdmission, EarlierActualSQLTimeIsChargedBeforeLaterReaderAdmission) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto reader = topology_access::reader(fixture.parent);
    ASSERT_NE(reader, nullptr);
    // A fresh per-reader two-second clock would run past this failure-only
    // release: 1.25 seconds spent in the actual first ATTACH plus two more.
    held_connection held(reader, 3s);
    ASSERT_TRUE(held.await_ready());
    bool delayed = false;
    authorizer observe(fixture.parent.db(), [&](int action) {
        if (action == SQLITE_ATTACH && !delayed) {
            delayed = true;
            std::this_thread::sleep_for(1250ms);
        }
        return SQLITE_OK;
    });
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    const bool forced = held.forced_release();
    held.release();
    EXPECT_TRUE(delayed);
    EXPECT_EQ(observe.error, nullptr);
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_FALSE(physically_attached(*reader));
    EXPECT_FALSE(topology_access::valid(fixture.parent));
}

TEST(TopologyAdmission, ClosedCapturedReaderNeverBecomesMetadataEmptySuccess) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto reader = topology_access::reader(fixture.parent);
    ASSERT_NE(reader, nullptr);
    bool closed_actual_reader = false;
    authorizer observe(fixture.parent.db(), [&](int action) {
        if (action == SQLITE_ATTACH && !closed_actual_reader) {
            closed_actual_reader = true;
            reader->close();
        }
        return SQLITE_OK;
    });
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    EXPECT_TRUE(closed_actual_reader);
    EXPECT_NE(error.find("database closed"), std::string::npos) << error;
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_FALSE(topology_access::valid(fixture.parent));
}

TEST(TopologyAdmission, RetiredReaderRemainsCapturedAndRevisionChangeRefusesPublication) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto reader = topology_access::reader(fixture.parent);
    ASSERT_NE(reader, nullptr);
    bool retired = false;
    authorizer observe(fixture.parent.db(), [&](int action) {
        if (action == SQLITE_ATTACH && !retired) {
            retired = true;
            topology_access::retire_reader(fixture.parent);
        }
        return SQLITE_OK;
    });
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    EXPECT_TRUE(retired);
    EXPECT_NE(error.find("ownership changed"), std::string::npos) << error;
    EXPECT_EQ(topology_access::reader(fixture.parent), nullptr);
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_TRUE(physically_attached(*reader)); // exact retained predecessor
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_FALSE(topology_access::valid(fixture.parent));
}

TEST(TopologyAdmission, RecursiveAuthorizerMutationRefusesBeforeReacquiringTopologyMutex) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    bool attempted = false;
    std::string inner_error;
    authorizer observe(fixture.parent.db(), [&](int action) {
        if (action == SQLITE_ATTACH && !attempted) {
            attempted = true;
            inner_error = refusal([&] { fixture.parent.detach(fixture.arm); });
        }
        return SQLITE_OK;
    });
    EXPECT_NO_THROW(fixture.parent.attach(fixture.arm));
    EXPECT_TRUE(attempted);
    EXPECT_NE(inner_error.find("recursive attachment topology mutation"), std::string::npos);
    EXPECT_TRUE(topology_access::valid(fixture.parent));
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
}

TEST(TopologyAdmission, RetiredExtensionCaptureIsDestroyedAfterTopologyAndSQLiteUnlock) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto writer = topology_access::writer(fixture.parent);
    struct observations { bool armed = true, deleted = false, topology_free = false, sqlite_free = false; };
    auto seen = std::make_shared<observations>();
    struct disarm_on_exit {
        std::shared_ptr<observations> seen;
        ~disarm_on_exit() { seen->armed = false; }
    } disarm{seen}; // also safe on a failed attach/detach before fixture destruction
    auto metadata = std::shared_ptr<const void>(new int(1), [&, seen, writer](const void* value) {
        delete static_cast<const int*>(value);
        seen->deleted = true;
        if (!seen->armed) return;
        std::thread check([&] {
            seen->topology_free = topology_access::topology_unlocked(fixture.parent);
            auto* mutex = sqlite3_db_mutex(topology_access::raw(*writer));
            seen->sqlite_free = mutex && sqlite3_mutex_try(mutex) == SQLITE_OK;
            if (seen->sqlite_free) sqlite3_mutex_leave(mutex);
        });
        check.join();
    });
    topology_access::with_metadata(fixture.parent, fixture.arm, metadata);
    metadata.reset();
    EXPECT_FALSE(seen->deleted);
    fixture.parent.detach(fixture.arm);
    EXPECT_TRUE(seen->deleted);
    EXPECT_TRUE(seen->topology_free);
    EXPECT_TRUE(seen->sqlite_free);
}

namespace {
void create_memory_fixture(lattice_db& owner) {
    owner.db().execute("CREATE TABLE Fixture (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL, n INTEGER)");
    owner.db().execute("INSERT INTO Fixture VALUES (1, 'row-one', 7)");
}
int actual_commit_without_wrapper_tail(database& db) {
    db.execute("BEGIN");
    db.execute("UPDATE main.Fixture SET n = n + 1");
    // The existing update hook marks this actual memory write dirty. A raw
    // COMMIT has no C++ drain; the next real wrapper must settle it correctly.
    return sqlite3_exec(topology_access::raw(db), "COMMIT", nullptr, nullptr, nullptr);
}
struct hook_reset {
    database& db;
    ~hook_reset() {
        try { db.set_txn_hooks({}, {}); }
        catch (...) { std::abort(); } // cleanup cannot be reported as a passing test
    }
};
}

TEST(TopologyAdmission, PendingRealCommitDeliversOffLockAndAllowsSubsequentPublicDetach) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent;
    lattice_db arm(paths.file("arm.sqlite"));
    create_memory_fixture(parent); create_memory_fixture(arm);
    auto writer = topology_access::writer(parent);
    bool called = false, attached_at_delivery = false, topology_free = false, sqlite_free = false;
    hook_reset cleanup{parent.db()};
    parent.db().set_txn_hooks([&] {
        called = true;
        attached_at_delivery = physically_attached(parent.db());
        std::thread probe([&] {
            topology_free = topology_access::topology_unlocked(parent);
            auto* mutex = sqlite3_db_mutex(topology_access::raw(*writer));
            sqlite_free = mutex && sqlite3_mutex_try(mutex) == SQLITE_OK;
            if (sqlite_free) sqlite3_mutex_leave(mutex);
        });
        probe.join();
        parent.detach(arm); // succeeds only after retiring the outer frame/lock
    }, [] {});
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    ASSERT_FALSE(called);
    EXPECT_NO_THROW(parent.attach(arm));
    EXPECT_TRUE(called);
    EXPECT_TRUE(attached_at_delivery);
    EXPECT_TRUE(topology_free);
    EXPECT_TRUE(sqlite_free);
    EXPECT_FALSE(physically_attached(parent.db()));
    const auto rows = parent.db().query("SELECT n FROM main.Fixture WHERE id = 1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
}

TEST(TopologyAdmission, ThrowingFirstDeliveryDoesNotDropSecondActualCommitObligation) {
    terminal_watchdog watchdog;
    lattice_db parent;
    lattice_db arm("file:topology_settlement_arm?mode=memory&cache=shared");
    create_memory_fixture(parent); create_memory_fixture(arm);
    int parent_deliveries = 0, arm_deliveries = 0;
    hook_reset parent_cleanup{parent.db()}, arm_cleanup{arm.db()};
    parent.db().set_txn_hooks([&] { ++parent_deliveries; throw std::runtime_error("first delivery failure"); }, [] {});
    arm.db().set_txn_hooks([&] { ++arm_deliveries; }, [] {});
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    ASSERT_EQ(actual_commit_without_wrapper_tail(arm.db()), SQLITE_OK);
    EXPECT_EQ(parent_deliveries, 0);
    EXPECT_EQ(arm_deliveries, 0);
    std::string error;
    try { parent.attach(arm); } catch (const std::runtime_error& value) { error = value.what(); }
    EXPECT_EQ(error, "first delivery failure");
    EXPECT_EQ(parent_deliveries, 1);
    EXPECT_EQ(arm_deliveries, 1);
    EXPECT_TRUE(topology_access::valid(parent)); // SQL settled; callback failure is preserved separately.
    EXPECT_EQ(topology_access::tokens(parent), 1u);
    EXPECT_NO_THROW(parent.detach(arm));
}

TEST(TopologyAdmission, LogicalCloseWhileWaitingRetainsHandleAndRefusesAfterActualAdmission) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto source = topology_access::writer(fixture.arm);
    held_connection held(source);
    ASSERT_TRUE(held.await_ready());
    std::jthread closer([&] {
        std::this_thread::sleep_for(100ms);
        source->close(); // actual public logical close, never frees the retained SQLite handle
        held.release();
    });
    const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
    closer.join();
    EXPECT_NE(error.find("database closed"), std::string::npos) << error;
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_FALSE(held.forced_release());
    expect_no_attachment(fixture);
}

namespace {
struct held_topology_leaf {
    std::mutex& target;
    std::mutex mutex;
    std::condition_variable changed;
    bool ready = false, released = false, forced = false;
    std::thread worker;
    explicit held_topology_leaf(std::mutex& value) : target(value), worker([this] {
        std::unique_lock<std::mutex> actual(target);
        std::unique_lock<std::mutex> state(mutex);
        ready = true; changed.notify_all();
        if (!changed.wait_for(state, 5s, [&] { return released; })) forced = true;
    }) {}
    bool await_ready() {
        std::unique_lock<std::mutex> state(mutex);
        return changed.wait_for(state, 3s, [&] { return ready; });
    }
    void release() {
        { std::lock_guard<std::mutex> state(mutex); released = true; }
        changed.notify_all();
        if (worker.joinable()) worker.join();
    }
    ~held_topology_leaf() { release(); }
};
}

TEST(TopologyAdmission, OwnershipAndTopologyLeavesCannotPrecedeTheOriginalDeadline) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    std::mutex* leaves[] = {&topology_access::ownership_mutex(fixture.arm),
        &topology_access::ownership_mutex(fixture.parent), &topology_access::topology_mutex(fixture.parent)};
    for (size_t index = 0; index != 3; ++index) {
        SCOPED_TRACE(index);
        held_topology_leaf held(*leaves[index]);
        ASSERT_TRUE(held.await_ready());
        const auto error = refusal([&] { fixture.parent.attach(fixture.arm); });
        held.release();
        expect_deadline(error);
        EXPECT_FALSE(held.forced);
        expect_no_attachment(fixture);
    }
    EXPECT_NO_THROW(fixture.parent.attach(fixture.arm));
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

namespace {
// The arm's first metadata entry occurs AFTER the parent metadata tail has
// captured its pending real COMMIT. Hold only that real source SQLite callback
// while a separate worker rolls back a later parent transaction. No simulated
// settlement flag or synthetic row/event is supplied to production.
struct rollback_between_topology_stages {
    database& parent;
    std::mutex mutex;
    std::condition_variable changed;
    bool requested = false, done = false, forced = false;
    int result = SQLITE_ERROR;
    std::thread worker;
    explicit rollback_between_topology_stages(database& value)
        : parent(value), worker([this] {
            {
                std::unique_lock<std::mutex> lock(mutex);
                if (!changed.wait_for(lock, 3s, [&] { return requested; })) {
                    forced = true; done = true; changed.notify_all(); return;
                }
            }
            // Real ROLLBACK invokes the existing engine rollback hook, which
            // clears only the later transaction's buffer after the correction.
            result = sqlite3_exec(topology_access::raw(parent),
                "BEGIN; UPDATE main.Fixture SET n = 99 WHERE id = 1; ROLLBACK;",
                nullptr, nullptr, nullptr);
            {
                std::lock_guard<std::mutex> lock(mutex); done = true;
            }
            changed.notify_all();
        }) {}
    bool perform() {
        std::unique_lock<std::mutex> lock(mutex);
        requested = true; changed.notify_all();
        if (!changed.wait_for(lock, 3s, [&] { return done; })) { forced = true; return false; }
        return !forced;
    }
    void join() { if (worker.joinable()) worker.join(); }
    ~rollback_between_topology_stages() {
        { std::lock_guard<std::mutex> lock(mutex); requested = true; }
        changed.notify_all(); join();
    }
};
}

TEST(TopologyAdmission, LaterRealRollbackCannotEraseCapturedCommittedModelNotifications) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent;
    lattice_db arm(paths.file("arm.sqlite"));
    create_memory_fixture(parent); create_memory_fixture(arm);
    std::vector<std::vector<lattice_db::change_event>> batches;
    bool topology_free = false;
    parent.add_table_observer("Fixture", [&](const auto& events) {
        std::thread probe([&] { topology_free = topology_access::topology_unlocked(parent); });
        probe.join();
        batches.push_back(events);
    });
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    ASSERT_TRUE(batches.empty());
    rollback_between_topology_stages rollback(parent.db());
    bool reached_later_stage = false, completed_rollback = false;
    authorizer observe(arm.db(), [&](int action) {
        if (action == SQLITE_SELECT && !reached_later_stage) {
            reached_later_stage = true;
            completed_rollback = rollback.perform();
            return SQLITE_DENY; // Actual later metadata failure remains failure.
        }
        return SQLITE_OK;
    });
    const auto error = refusal([&] { parent.attach(arm); });
    rollback.join();
    EXPECT_TRUE(reached_later_stage);
    EXPECT_TRUE(completed_rollback);
    EXPECT_FALSE(rollback.forced);
    EXPECT_EQ(rollback.result, SQLITE_OK);
    EXPECT_FALSE(error.empty());
    EXPECT_TRUE(topology_free);
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].size(), 1u);
    EXPECT_EQ(std::get<0>(batches[0][0]), "Fixture");
    EXPECT_EQ(std::get<1>(batches[0][0]), "UPDATE");
    EXPECT_EQ(std::get<2>(batches[0][0]), 1);
    EXPECT_EQ(std::get<3>(batches[0][0]), "row-one");
    const auto rows = parent.db().query("SELECT n FROM main.Fixture WHERE id = 1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
    EXPECT_EQ(batches.size(), 1u); // Later queries cannot redeliver the captured batch.
}

TEST(TopologyAdmission, CapturedCommitCallbackWriteUsesOrdinaryOuterDrainExactlyOnce) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent;
    lattice_db arm(paths.file("arm.sqlite"));
    create_memory_fixture(parent); create_memory_fixture(arm);
    std::vector<std::vector<lattice_db::change_event>> batches;
    int callback_depth = 0, maximum_depth = 0;
    parent.add_table_observer("Fixture", [&](const auto& events) {
        ++callback_depth; maximum_depth = std::max(maximum_depth, callback_depth);
        batches.push_back(events);
        if (batches.size() == 1)
            parent.db().execute("UPDATE main.Fixture SET n = n + 1 WHERE id = 1");
        --callback_depth;
    });
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    ASSERT_TRUE(batches.empty());
    EXPECT_NO_THROW(parent.attach(arm));
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(maximum_depth, 1);
    for (const auto& batch : batches) {
        ASSERT_EQ(batch.size(), 1u);
        EXPECT_EQ(std::get<1>(batch[0]), "UPDATE");
        EXPECT_EQ(std::get<2>(batch[0]), 1);
        EXPECT_EQ(std::get<3>(batch[0]), "row-one");
    }
    const auto rows = parent.db().query("SELECT n FROM main.Fixture WHERE id = 1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 9);
    EXPECT_EQ(batches.size(), 2u);
    EXPECT_NO_THROW(parent.detach(arm));
}

TEST(TopologyAdmission, FailedViewCreationCannotDiscardCapturedModelAndAuditEvents) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent;
    lattice_db arm(paths.file("arm.sqlite"));
    create_memory_fixture(parent); create_memory_fixture(arm);
    parent.db().execute("CREATE TRIGGER fixture_audit AFTER UPDATE ON Fixture BEGIN "
        "INSERT INTO AuditLog(tableName, operation, rowId, globalRowId, changedFieldsNames) "
        "VALUES ('Fixture', 'UPDATE', NEW.id, NEW.globalId, '[\"n\"]'); END");
    std::vector<std::vector<lattice_db::change_event>> model_batches, audit_batches;
    parent.add_table_observer("Fixture", [&](const auto& events) { model_batches.push_back(events); });
    parent.add_table_observer("AuditLog", [&](const auto& events) { audit_batches.push_back(events); });
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    ASSERT_TRUE(model_batches.empty());
    ASSERT_TRUE(audit_batches.empty());
    bool refused_view = false;
    {
        authorizer deny_view(parent.db(), [&](int action) {
            if (action == SQLITE_CREATE_TEMP_VIEW) { refused_view = true; return SQLITE_DENY; }
            return SQLITE_OK;
        });
        EXPECT_FALSE(refusal([&] { parent.attach(arm); }).empty());
    }
    EXPECT_TRUE(refused_view);
    EXPECT_FALSE(topology_access::valid(parent));
    EXPECT_EQ(topology_access::tokens(parent), 0u);
    ASSERT_EQ(model_batches.size(), 1u);
    ASSERT_EQ(model_batches[0].size(), 1u);
    EXPECT_EQ(std::get<3>(model_batches[0][0]), "row-one");
    ASSERT_EQ(audit_batches.size(), 1u);
    ASSERT_EQ(audit_batches[0].size(), 1u);
    const auto rows = parent.db().query("SELECT id, globalId FROM main.AuditLog WHERE tableName = 'Fixture'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<2>(audit_batches[0][0]), std::get<int64_t>(rows[0].at("id")));
    EXPECT_EQ(std::get<3>(audit_batches[0][0]), std::get<std::string>(rows[0].at("globalId")));
    EXPECT_EQ(model_batches.size(), 1u);
    EXPECT_EQ(audit_batches.size(), 1u);
    EXPECT_NO_THROW(parent.detach(arm));
}

namespace {
struct actual_meta_read_denial {
    sqlite3* raw;
    bool observed = false;
    explicit actual_meta_read_denial(database& owner) : raw(topology_access::raw(owner)) {
        if (sqlite3_set_authorizer(raw,
            [](void* value, int action, const char* first, const char*, const char*, const char*) noexcept {
                auto& self = *static_cast<actual_meta_read_denial*>(value);
                if (action == SQLITE_READ && first && std::strcmp(first, "_lattice_meta") == 0) {
                    self.observed = true; return SQLITE_DENY;
                }
                return SQLITE_OK;
            }, this) != SQLITE_OK) throw std::runtime_error("actual metadata authorizer setup failed");
    }
    ~actual_meta_read_denial() { sqlite3_set_authorizer(raw, nullptr, nullptr); }
};
}

TEST(TopologyAdmission, RefusedSnapshotLookupKeepsActualCommittedBufferForLaterDrain) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent;
    lattice_db arm(paths.file("arm.sqlite"));
    create_memory_fixture(parent); create_memory_fixture(arm);
    std::vector<std::vector<lattice_db::change_event>> batches;
    parent.add_table_observer("Fixture", [&](const auto& events) { batches.push_back(events); });
    ASSERT_EQ(actual_commit_without_wrapper_tail(parent.db()), SQLITE_OK);
    {
        actual_meta_read_denial deny(parent.db());
        EXPECT_FALSE(refusal([&] { parent.attach(arm); }).empty());
        EXPECT_TRUE(deny.observed);
        EXPECT_TRUE(batches.empty());
    }
    EXPECT_EQ(topology_access::tokens(parent), 0u);
    EXPECT_TRUE(topology_access::valid(parent));
    const auto rows = parent.db().query("SELECT n FROM main.Fixture WHERE id = 1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
    ASSERT_EQ(batches.size(), 1u);
    ASSERT_EQ(batches[0].size(), 1u);
    EXPECT_EQ(std::get<3>(batches[0][0]), "row-one");
    EXPECT_NO_THROW(parent.attach(arm));
    EXPECT_EQ(batches.size(), 1u);
    EXPECT_NO_THROW(parent.detach(arm));
}
