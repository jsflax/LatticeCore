#include "TestHelpers.hpp"
#include <condition_variable>
#include <array>
#include <exception>
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
    static size_t actual_waiters(const database& db) { return db.topology_waiter_count_(); }
    template<typename F> static void with_original_vec0_hold(lattice_db& owner, F&& body) {
        owner.with_vec0_serialization("topology-turn-test", false, std::forward<F>(body));
    }
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
    static std::recursive_timed_mutex& mutation_store_gate(lattice_db& owner) {
        return owner.store_write_gate_?*owner.store_write_gate_:owner.vec0_memory_maintenance_gate_;
    }
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

namespace {
bool await_actual(const std::function<bool()>& predicate) {
    const auto end = clock_type::now() + 3s;
    while (!predicate()) {
        if (clock_type::now() >= end) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}
void report_actual_error(const char* label, const std::exception_ptr& error) {
    if (!error) return;
    try { std::rethrow_exception(error); }
    catch (const std::exception& actual) { ADD_FAILURE() << label << ": " << actual.what(); }
    catch (...) { ADD_FAILURE() << label << ": actual non-std exception"; }
}
}

TEST(TopologyAdmission, ContinuousSameParentWriterOffersRealTopologyTurns) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto writer = topology_access::writer(fixture.parent);
    std::atomic<bool> stop{false};
    std::atomic<int64_t> completed{0};
    std::exception_ptr writer_error, topology_error;
    std::thread writes([&] {
        try {
            while (!stop.load()) {
                // Exactly the original failing legacy wrapper route; no yield,
                // sleep, lock release hook or workload reduction is supplied.
                fixture.parent.db().execute("UPDATE main.Fixture SET n = n + 1 WHERE id = 1");
                completed.fetch_add(1);
            }
        } catch (...) { writer_error = std::current_exception(); }
    });
    const bool started = await_actual([&] { return completed.load() != 0; });
    try {
        if (started) for (int turn = 0; turn != 32; ++turn) {
            fixture.parent.attach(fixture.arm);
            fixture.parent.detach(fixture.arm);
        }
    } catch (...) { topology_error = std::current_exception(); }
    stop.store(true); writes.join();
    ASSERT_TRUE(started);
    report_actual_error("writer", writer_error);
    report_actual_error("topology", topology_error);
    EXPECT_EQ(topology_access::actual_waiters(*writer), 0u);
    const auto rows = writer->query("SELECT n FROM main.Fixture WHERE id=1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 7 + completed.load());
    expect_no_attachment(fixture);
}

TEST(TopologyAdmission, ActualSqlFunctionReentryDoesNotWaitOnItsOwnTopologyWaiter) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto writer = topology_access::writer(fixture.parent);
    struct callback_context {
        database& writer;
        std::atomic<bool> entered{false};
        bool saw_waiter = false, returned = false;
        std::exception_ptr error;
    } context{*writer};
    auto* raw = topology_access::raw(*writer); // Actual friend-owned test registration; no escaped flag reset.
    const auto function = [](sqlite3_context* result, int, sqlite3_value**) noexcept {
        auto& actual = *static_cast<callback_context*>(sqlite3_user_data(result));
        try {
            actual.entered.store(true);
            actual.saw_waiter = await_actual([&] { return topology_access::actual_waiters(actual.writer) != 0; });
            if (!actual.saw_waiter) throw std::runtime_error("actual parent topology waiter did not register");
            actual.writer.execute("UPDATE main.Fixture SET n=n+1 WHERE id=1");
            actual.returned = true;
            sqlite3_result_int(result, 1);
        } catch (...) {
            actual.error = std::current_exception();
            sqlite3_result_error(result, "actual recursive writer failed", -1);
        }
    };
    ASSERT_EQ(sqlite3_create_function_v2(raw, "actual_recursive_writer", 0, SQLITE_UTF8,
        &context, function, nullptr, nullptr, nullptr), SQLITE_OK);
    struct clear_function {
        sqlite3* raw;
        ~clear_function() {
            if (sqlite3_create_function_v2(raw, "actual_recursive_writer", 0, SQLITE_UTF8,
                    nullptr, nullptr, nullptr, nullptr, nullptr) != SQLITE_OK) std::abort();
        }
    } cleanup{raw};
    std::exception_ptr writer_error, topology_error;
    std::thread topology([&] {
        try {
            if (!await_actual([&] { return context.entered.load(); }))
                throw std::runtime_error("actual SQL function did not enter");
            fixture.parent.attach(fixture.arm);
        } catch (...) { topology_error = std::current_exception(); }
    });
    try { writer->execute("SELECT actual_recursive_writer()"); }
    catch (...) { writer_error = std::current_exception(); }
    topology.join();
    EXPECT_TRUE(context.saw_waiter);
    EXPECT_TRUE(context.returned);
    report_actual_error("actual SQL callback", context.error);
    report_actual_error("outer writer", writer_error);
    report_actual_error("topology", topology_error);
    EXPECT_EQ(topology_access::actual_waiters(*writer), 0u);
    const auto rows = writer->query("SELECT n FROM main.Fixture WHERE id=1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
    if (!topology_error) EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, Vec0OwnedNativeBodyReentryDoesNotWaitOnItsOwnTopologyWaiter) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto writer = topology_access::writer(fixture.parent);
    std::atomic<bool> entered{false};
    bool saw_real_waiter = false;
    std::exception_ptr body_error, topology_error;
    std::thread topology([&] {
        try {
            if (!await_actual([&] { return entered.load(); }))
                throw std::runtime_error("actual vec0 body did not enter");
            fixture.parent.attach(fixture.arm);
        } catch (...) { topology_error = std::current_exception(); }
    });
    try {
        // The genuine existing file vec0 serialization owns SQLite across
        // this body. No fake mutex/current-scope flag is installed.
        topology_access::with_original_vec0_hold(fixture.parent, [&] {
            entered.store(true);
            saw_real_waiter = await_actual([&] { return topology_access::actual_waiters(*writer) != 0; });
            if (!saw_real_waiter) throw std::runtime_error("actual parent topology waiter did not register");
            writer->execute("UPDATE main.Fixture SET n=n+1 WHERE id=1");
        });
    } catch (...) { body_error = std::current_exception(); }
    topology.join();
    EXPECT_TRUE(saw_real_waiter);
    report_actual_error("vec0 body", body_error);
    report_actual_error("topology", topology_error);
    EXPECT_EQ(topology_access::actual_waiters(*writer), 0u);
    const auto rows = writer->query("SELECT n FROM main.Fixture WHERE id=1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
    if (!topology_error) EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, ActualRawRecursiveMutexOwnerKeepsItsExistingSqlReentry) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto writer = topology_access::writer(fixture.parent);
    // Genuine public raw escape, not the legacy database& accessor. This
    // caller really owns SQLite recursively while a different thread waits.
    auto* raw = writer->handle();
    auto* mutex = sqlite3_db_mutex(raw);
    ASSERT_NE(mutex, nullptr);
    sqlite3_mutex_enter(mutex);
    std::exception_ptr writer_error, topology_error;
    std::thread topology([&] {
        try { fixture.parent.attach(fixture.arm); }
        catch (...) { topology_error = std::current_exception(); }
    });
    const bool pending = await_actual([&] { return topology_access::actual_waiters(*writer) != 0; });
    try { if (pending) writer->execute("UPDATE main.Fixture SET n=n+1 WHERE id=1"); }
    catch (...) { writer_error = std::current_exception(); }
    sqlite3_mutex_leave(mutex);
    topology.join();
    ASSERT_TRUE(pending);
    report_actual_error("raw recursive writer", writer_error);
    report_actual_error("topology", topology_error);
    EXPECT_EQ(topology_access::actual_waiters(*writer), 0u);
    const auto rows = writer->query("SELECT n FROM main.Fixture WHERE id=1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 8);
    if (!topology_error) EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, SixtyFifthRealNativeWaiterRefusesAndCloseEndsAllOriginalCells) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    auto source = topology_access::writer(fixture.arm);
    held_connection held(source);
    ASSERT_TRUE(held.await_ready());
    std::array<std::thread, 64> waiting;
    std::array<std::exception_ptr, 64> errors;
    std::array<std::atomic<bool>, 64> returned{};
    std::exception_ptr setup_error, overflow_error;
    try {
        for (size_t index = 0; index != waiting.size(); ++index) {
            waiting[index] = std::thread([&, index] {
                try { fixture.parent.attach(fixture.arm); }
                catch (...) { errors[index] = std::current_exception(); }
                returned[index].store(true);
            });
        }
    } catch (...) { setup_error = std::current_exception(); }
    // All public calls must have reached their genuine source mutex retry.
    // No cell/count/deadline or completed result is fabricated by this fixture.
    const bool full = !setup_error && await_actual([&] { return topology_access::actual_waiters(*source) == 64; });
    std::string overflow;
    try { if (full) overflow = refusal([&] { fixture.parent.attach(fixture.arm); }); }
    catch (...) { overflow_error = std::current_exception(); }
    source->close(); // Real logical close while the exact originals still wait.
    held.release();
    for (auto& thread : waiting) if (thread.joinable()) thread.join();
    report_actual_error("actual waiter thread setup", setup_error);
    report_actual_error("overflow call", overflow_error);
    ASSERT_TRUE(full);
    EXPECT_EQ(overflow, "topology admission waiter capacity exceeded");
    EXPECT_FALSE(held.forced_release());
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_EQ(topology_access::actual_waiters(*source), 0u);
    for (size_t index = 0; index != errors.size(); ++index) {
        SCOPED_TRACE(index);
        EXPECT_TRUE(returned[index].load());
        ASSERT_NE(errors[index], nullptr);
        try { std::rethrow_exception(errors[index]); }
        catch (const db_error& error) {
            EXPECT_NE(std::string(error.what()).find("database closed"), std::string::npos);
        }
        catch (...) { ADD_FAILURE() << "original waiter did not preserve a real database close refusal"; }
    }
    expect_no_attachment(fixture);
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
    EXPECT_EQ(topology_access::actual_waiters(*topology_access::writer(fixture.parent)), 0u);
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

TEST(TopologyAdmission, PartialViewDropRefusalRetriesOnlyActualTemporaryViews) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    fixture.parent.attach(fixture.arm);
    auto writer = topology_access::writer(fixture.parent);
    auto reader = topology_access::reader(fixture.parent);
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);
    const auto temp_views = [](database& db) {
        return std::get<int64_t>(db.query(
            "SELECT count(*) AS n FROM sqlite_temp_master WHERE type='view' AND name='Fixture'").at(0).at("n"));
    };
    ASSERT_EQ(temp_views(*writer), 1);
    ASSERT_EQ(temp_views(*reader), 1);
    held_connection held(reader);
    ASSERT_TRUE(held.await_ready());
    const auto error = refusal([&] { fixture.parent.detach(fixture.arm); });
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_FALSE(forced);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    // The first real handle completed its DROP before the same operation
    // refused on the reader. Its main table remains the original real table.
    ASSERT_EQ(temp_views(*writer), 0);
    ASSERT_EQ(temp_views(*reader), 1);
    EXPECT_EQ(std::get<int64_t>(writer->query("SELECT n FROM main.Fixture WHERE id=1").at(0).at("n")), 7);
    EXPECT_EQ(std::get<int64_t>(reader->query("SELECT n FROM main.Fixture WHERE id=1").at(0).at("n")), 7);
    EXPECT_TRUE(physically_attached(*writer));
    EXPECT_TRUE(physically_attached(*reader));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    EXPECT_FALSE(topology_access::valid(fixture.parent));
    // This is a genuine later public cleanup episode after the actual holder
    // returned; no first refusal, route token, or original deadline is reset.
    ASSERT_NO_THROW(fixture.parent.detach(fixture.arm));
    EXPECT_FALSE(physically_attached(*writer));
    EXPECT_FALSE(physically_attached(*reader));
    EXPECT_EQ(temp_views(*writer), 0);
    EXPECT_EQ(temp_views(*reader), 0);
    EXPECT_EQ(std::get<int64_t>(writer->query("SELECT n FROM main.Fixture WHERE id=1").at(0).at("n")), 7);
    EXPECT_EQ(std::get<int64_t>(reader->query("SELECT n FROM main.Fixture WHERE id=1").at(0).at("n")), 7);
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
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

#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"

// These are additional actual-resource cases for the fresh f0 source graph.
// Existing donor cases above and the original pressure stress stay exact.
namespace {
configuration configured_topology(const std::string& path, int timeout_ms) {
    configuration value(path);
    value.busy_timeout_ms = timeout_ms;
    return value;
}
}

TEST(TopologyAdmission, ConfiguredRecipientBudgetRefusesHeldRealMutexBeforeHolderCleanup) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent{configured_topology(paths.file("parent.sqlite"), 150)};
    lattice_db arm{paths.file("arm.sqlite")};
    create_memory_fixture(parent); create_memory_fixture(arm);
    held_connection held(topology_access::writer(parent), 500ms);
    ASSERT_TRUE(held.await_ready());
    const auto started = clock_type::now();
    const auto error = refusal([&] { parent.attach(arm); });
    const auto elapsed = clock_type::now() - started;
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_GE(elapsed, 150ms);
    EXPECT_FALSE(forced);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_FALSE(physically_attached(parent.db()));
    EXPECT_EQ(topology_access::tokens(parent), 0u);
    EXPECT_TRUE(topology_access::valid(parent));
    EXPECT_NO_THROW(parent.attach(arm));
    EXPECT_NO_THROW(parent.detach(arm));
}

TEST(TopologyAdmission, ConfiguredSourceBudgetSharesTheActualPublicOperationEpisode) {
    terminal_watchdog watchdog;
    local_paths paths;
    lattice_db parent{paths.file("parent.sqlite")};
    lattice_db arm{configured_topology(paths.file("arm.sqlite"), 150)};
    create_memory_fixture(parent); create_memory_fixture(arm);
    held_connection held(topology_access::writer(arm), 500ms);
    ASSERT_TRUE(held.await_ready());
    const auto started = clock_type::now();
    const auto error = refusal([&] { parent.attach(arm); });
    const auto elapsed = clock_type::now() - started;
    const bool forced = held.forced_release();
    held.release();
    expect_deadline(error);
    EXPECT_GE(elapsed, 150ms);
    EXPECT_FALSE(forced);
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_FALSE(physically_attached(parent.db()));
    EXPECT_EQ(topology_access::tokens(parent), 0u);
    EXPECT_TRUE(topology_access::valid(parent));
    EXPECT_NO_THROW(parent.attach(arm));
    EXPECT_NO_THROW(parent.detach(arm));
}

TEST(TopologyAdmission, ReleasingRealSourceMutexBeforeDeadlineAllowsActualTopologyPublication) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    held_connection held(topology_access::writer(fixture.arm));
    ASSERT_TRUE(held.await_ready());
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false, returned = false;
    std::string error;
    std::exception_ptr caller_error;
    std::thread caller([&] {
        { std::lock_guard<std::mutex> lock(mutex); entered = true; }
        changed.notify_all();
        try { error = refusal([&] { fixture.parent.attach(fixture.arm); }); }
        catch (...) { caller_error = std::current_exception(); }
        { std::lock_guard<std::mutex> lock(mutex); returned = true; }
        changed.notify_all();
    });
    bool started = false, returned_before_release = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        started = changed.wait_for(lock, 3s, [&] { return entered; });
        returned_before_release = changed.wait_for(lock, 50ms, [&] { return returned; });
    }
    held.release(); caller.join();
    EXPECT_TRUE(started);
    EXPECT_FALSE(returned_before_release);
    EXPECT_FALSE(held.forced_release());
    EXPECT_EQ(held.after_release_rc, SQLITE_OK);
    EXPECT_EQ(caller_error, nullptr);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_TRUE(physically_attached(fixture.parent.db()));
    EXPECT_EQ(topology_access::tokens(fixture.parent), 1u);
    EXPECT_TRUE(topology_access::valid(fixture.parent));
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
}

TEST(TopologyAdmission, SustainedActualWriterCompletesOrExplicitlyRefusesWithoutResettingAdmission) {
    terminal_watchdog watchdog;
    pair_fixture fixture;
    fixture.parent.set_wal_keeper_eviction_threshold_bytes(1);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> committed{0};
    std::exception_ptr writer_error;
    std::thread writer([&] {
        try {
            while (!stop.load()) {
                fixture.parent.db().execute("UPDATE main.Fixture SET n=n+1 WHERE id=1");
                committed.fetch_add(1);
            }
        } catch (...) { writer_error = std::current_exception(); }
    });
    const auto first_commit_deadline = clock_type::now() + 3s;
    while (committed.load() == 0 && clock_type::now() < first_commit_deadline)
        std::this_thread::sleep_for(1ms);
    const bool actual_writer_started = committed.load() != 0;
    if (!actual_writer_started) { stop.store(true); writer.join(); }
    ASSERT_TRUE(actual_writer_started);
    std::string error;
    unsigned completed_cycles = 0;
    try {
        for (; completed_cycles != 32; ++completed_cycles) {
            fixture.parent.attach(fixture.arm);
            fixture.parent.detach(fixture.arm);
        }
    } catch (const db_error& failure) { error = failure.what(); }
    catch (...) {
        stop.store(true); writer.join();
        throw;
    }
    stop.store(true); writer.join();
    EXPECT_EQ(writer_error, nullptr);
    EXPECT_GT(committed.load(), 0u);
    if (completed_cycles != 32) expect_deadline(error);
    else EXPECT_TRUE(error.empty());
    // A refusal is an explicit result of this new admission contract. The
    // original pressure stress still requires all32 cycles without failure.
    // Actual cleanup/refusal is never inferred from a scalar writer counter.
    EXPECT_NO_THROW(fixture.parent.detach(fixture.arm));
    const bool physical_binding_remains = physically_attached(fixture.parent.db());
    EXPECT_EQ(topology_access::tokens(fixture.parent), 0u);
    if (physical_binding_remains) {
        // Cross-handle ATTACH was never atomic. If refusal precedes book-
        // keeping, detach has no authenticated alias to retire. Keep that
        // actual partial binding fenced; do not claim absent resources or
        // manufacture a route to clean it up. The original partial case above
        // independently requires this refusal behavior.
        EXPECT_LT(completed_cycles, 32u);
        EXPECT_FALSE(topology_access::valid(fixture.parent));
        EXPECT_THROW(fixture.parent.attach(fixture.arm), std::runtime_error);
    } else {
        EXPECT_TRUE(topology_access::valid(fixture.parent));
    }
}

TEST(TopologyAdmission, CapturedTopologyObserverRefusesAuthenticInstallBeforeBeginThenLaterCommits) {
    terminal_watchdog watchdog;
    auto parent = std::make_shared<lattice_db>();
    lattice_db arm{configuration("file:bounded_topology_install_arm?mode=memory&cache=shared")};
    create_memory_fixture(*parent); create_memory_fixture(arm);
    int callbacks = 0, begin_attempts = 0;
    bool body_ran = false;
    lattice::detail::recovery_install_result nested;
    const auto token = parent->add_table_observer("Fixture", [&](const auto&) {
        ++callbacks;
        if (callbacks != 1) return;
        nested = lattice::detail::recovery_writer_access::install(parent, [&](auto& writer) {
            body_ran = true;
            writer.execute("UPDATE main.Fixture SET n=999 WHERE id=1");
        });
    });
    parent->db().execute("BEGIN");
    parent->db().execute("UPDATE main.Fixture SET n=n+1 WHERE id=1");
    ASSERT_EQ(sqlite3_exec(topology_access::raw(parent->db()), "COMMIT", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(callbacks, 0);
    {
        authorizer observe(parent->db(), [&](int action) {
            // Count actual BEGIN preparations only through the existing
            // authorizer below; this generic wrapper does not expose text.
            (void)action; return SQLITE_OK;
        });
        auto* raw = topology_access::raw(parent->db());
        ASSERT_EQ(sqlite3_set_authorizer(raw,
            [](void* value, int action, const char* first, const char*, const char*, const char*) noexcept {
                if (action == SQLITE_TRANSACTION && first && std::strcmp(first, "BEGIN") == 0)
                    ++*static_cast<int*>(value);
                return SQLITE_OK;
            }, &begin_attempts), SQLITE_OK);
        EXPECT_NO_THROW(parent->attach(arm));
        EXPECT_EQ(callbacks, 1);
        EXPECT_FALSE(body_ran);
        EXPECT_EQ(begin_attempts, 0);
        EXPECT_EQ(nested.state, lattice::detail::recovery_install_state::refused);
        EXPECT_NE(nested.primary_error, nullptr);
        EXPECT_EQ(nested.cleanup_error, nullptr);
        EXPECT_FALSE(parent->db().is_in_transaction());
        const auto later = lattice::detail::recovery_writer_access::install(parent, [](auto& writer) {
            writer.execute("UPDATE main.Fixture SET n=n+1 WHERE id=1");
        });
        EXPECT_EQ(later.state, lattice::detail::recovery_install_state::committed);
        EXPECT_EQ(later.primary_error, nullptr);
        EXPECT_EQ(later.postcommit_error, nullptr);
        EXPECT_EQ(later.notification_error, nullptr);
        EXPECT_EQ(begin_attempts, 1);
        EXPECT_EQ(callbacks, 2);
        const auto rows = parent->db().query("SELECT n FROM main.Fixture WHERE id=1");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 9);
    }
    parent->remove_table_observer("Fixture", token);
    EXPECT_NO_THROW(parent->detach(arm));
}


// Denial-only controller retry controls use real owners/cells, never a grant.
namespace {
using controller_intent=lattice::detail::recovery_writer_access::controller_admission_intent;
struct intent_reader_state {
    std::mutex mutex;std::condition_variable changed;
    bool started=false,done=false;int64_t value=0;std::exception_ptr error;
};
struct intent_thread_ending {
    controller_intent& intent;std::thread& worker;
    ~intent_thread_ending(){intent.release();if(worker.joinable())worker.join();}
};
TEST(ControllerAdmissionIntent, ExactCellBypassDoesNotReleaseFreshReaderBeforeExplicitRetirement) {
    local_paths paths;auto owner=std::make_shared<lattice_db>(paths.file("intent.sqlite"));
    const auto writer=topology_access::writer(*owner);ASSERT_TRUE(writer);
    controller_intent intent;intent.begin(owner,clock_type::now()+2s);
    EXPECT_EQ(topology_access::actual_waiters(*writer),1u);
    EXPECT_EQ(sqlite3_get_autocommit(topology_access::raw(*writer)),1);
    intent_reader_state state;std::thread worker;
    intent_thread_ending ending{intent,worker};
    worker=std::thread([&]{
        {std::lock_guard lock(state.mutex);state.started=true;}state.changed.notify_all();
        try{const auto rows=writer->query("SELECT 911 AS n");state.value=std::get<int64_t>(rows.at(0).at("n"));}
        catch(...){state.error=std::current_exception();}
        {std::lock_guard lock(state.mutex);state.done=true;}state.changed.notify_all();
    });
    {std::unique_lock lock(state.mutex);ASSERT_TRUE(state.changed.wait_for(lock,1s,[&]{return state.started;}));}
    {
        controller_intent::turn own_turn(intent);
        EXPECT_EQ(std::get<int64_t>(writer->query("SELECT 17 AS n").at(0).at("n")),17);
        std::unique_lock lock(state.mutex);EXPECT_FALSE(state.changed.wait_for(lock,100ms,[&]{return state.done;}));
    }
    EXPECT_EQ(topology_access::actual_waiters(*writer),1u);
    intent.release();EXPECT_EQ(topology_access::actual_waiters(*writer),0u);
    {std::unique_lock lock(state.mutex);ASSERT_TRUE(state.changed.wait_for(lock,1s,[&]{return state.done;}));}
    worker.join();EXPECT_FALSE(state.error);EXPECT_EQ(state.value,911);
    EXPECT_EQ(sqlite3_get_autocommit(topology_access::raw(*writer)),1);owner->close();
}
TEST(ControllerAdmissionIntent, RepeatedBeginCannotExtendFirstPublishedDeadline) {
    local_paths paths;auto owner=std::make_shared<lattice_db>(paths.file("deadline.sqlite"));
    const auto writer=topology_access::writer(*owner);ASSERT_TRUE(writer);
    controller_intent intent;intent.begin(owner,clock_type::now()+100ms);
    intent.begin(owner,clock_type::now()+2s); // may tighten, never renew
    EXPECT_EQ(topology_access::actual_waiters(*writer),1u);
    intent_reader_state state;std::thread worker;
    intent_thread_ending ending{intent,worker};
    worker=std::thread([&]{try{
        const auto rows=writer->query("SELECT 912 AS n");state.value=std::get<int64_t>(rows.at(0).at("n"));
    }catch(...){state.error=std::current_exception();}
        {std::lock_guard lock(state.mutex);state.done=true;}state.changed.notify_all();});
    {std::unique_lock lock(state.mutex);ASSERT_TRUE(state.changed.wait_for(lock,1s,[&]{return state.done;}));}
    worker.join();EXPECT_FALSE(state.error);EXPECT_EQ(state.value,912);
    // Expiry allows ordinary work; it is not a refund/ending claim. The real
    // owner must still retire its own fixed cell, exactly once.
    EXPECT_EQ(topology_access::actual_waiters(*writer),1u);
    intent.release();intent.release();EXPECT_EQ(topology_access::actual_waiters(*writer),0u);owner->close();
}
TEST(ControllerAdmissionIntent, ClosedOwnerCannotPublishDenialOrOpenTransaction) {
    local_paths paths;auto owner=std::make_shared<lattice_db>(paths.file("closed.sqlite"));
    const auto writer=topology_access::writer(*owner);ASSERT_TRUE(writer);owner->close();
    controller_intent intent;EXPECT_THROW(intent.begin(owner,clock_type::now()+2s),db_error);
    EXPECT_EQ(topology_access::actual_waiters(*writer),0u);intent.release();
}
} // namespace

#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include <future>

namespace {
class publication_operation_owner : public lattice::lattice_db {
public:
    explicit publication_operation_owner(const lattice::configuration& config):lattice_db(config){}
    template<class F> auto unit(F&& body){return with_mutation_operation(std::forward<F>(body));}
};
// The foreign thread takes the ACTUAL existing owner publication mutex.
// Its readiness promise is sent only after that mutex has really been locked.
struct actual_publication_hold {
    std::promise<void> ready_,release_;
    std::future<void> ready=ready_.get_future(),released=release_.get_future();
    bool released_once=false;
    std::atomic<bool> forced{false};
    std::thread worker;
    explicit actual_publication_hold(lattice::lattice_db& owner):worker([this,&owner]{
        std::unique_lock lock(topology_access::ownership_mutex(owner));
        ready_.set_value();
        if(released.wait_for(10s)!=std::future_status::ready)forced.store(true);
    }){
        if(ready.wait_for(5s)!=std::future_status::ready){finish();throw std::runtime_error("actual publication holder did not start");}
    }
    void release()noexcept{if(!released_once){released_once=true;release_.set_value();}}
    void finish()noexcept{release();if(worker.joinable())worker.join();}
    ~actual_publication_hold(){finish();}
};
struct actual_publication_wait_observer {
    std::function<void(lattice::lattice_db&)> callback;
    actual_publication_wait_observer* previous;
    void (*prior)(lattice::lattice_db&);
    static inline thread_local actual_publication_wait_observer* current=nullptr;
    explicit actual_publication_wait_observer(std::function<void(lattice::lattice_db&)> fn)
        :callback(std::move(fn)),previous(current),
         prior(lattice::detail::recovery_admission_test_hooks::after_mutation_publication_wait_unlocked){
        current=this;
        lattice::detail::recovery_admission_test_hooks::after_mutation_publication_wait_unlocked=[](auto& actual){current->callback(actual);};
    }
    ~actual_publication_wait_observer(){
        lattice::detail::recovery_admission_test_hooks::after_mutation_publication_wait_unlocked=prior;current=previous;
    }
};
struct publication_before_join {
    actual_publication_hold& held;
    ~publication_before_join(){held.finish();}
};
std::vector<int64_t> publication_values(lattice::database& db){
    std::vector<int64_t> values;
    for(const auto& row:db.query("SELECT age FROM TestPerson ORDER BY id"))
        values.push_back(std::get<int64_t>(row.at("age")));
    return values;
}
TEST(MutationPublicationAdmission, ActualForeignPublicationMutexWaitsOffAllLeavesThenCommitsOnce) {
    terminal_watchdog bounded;
    lattice::configuration config(":memory:");config.busy_timeout_ms=1000;
    publication_operation_owner owner(config);owner.add(TestPerson{"seed",7,{}});
    const auto writer=topology_access::writer(owner);ASSERT_TRUE(writer);
    auto* native=topology_access::raw(*writer);auto* native_mutex=sqlite3_db_mutex(native);ASSERT_NE(native_mutex,nullptr);
    auto& store=topology_access::mutation_store_gate(owner);
    std::vector<int64_t> delivered;
    const auto token=owner.add_table_observer("TestPerson",[&](const auto& changes){
        for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
    });
    actual_publication_hold held(owner);
    std::promise<void> observed_;auto observed=observed_.get_future();
    std::atomic<unsigned> waits{0},bodies{0};
    auto mutation=std::async(std::launch::async,[&]{
        actual_publication_wait_observer observer([&](auto& actual){
            if(&actual!=&owner)throw std::runtime_error("wrong publication wait owner");
            if(waits.fetch_add(1)==0)observed_.set_value();
        });
        owner.unit([&](lattice::database& actual){
            ++bodies;
            actual.insert("TestPerson",{{"globalId",std::string("publication-once")},{"name",std::string("publication")},{"age",int64_t(11)}});
        });
    });
    publication_before_join cleanup{held}; // release/join real holder before future on ASSERT
    ASSERT_EQ(observed.wait_for(5s),std::future_status::ready);
    EXPECT_GT(waits.load(),0u);EXPECT_EQ(bodies.load(),0u);
    // The only positive wait witness is the producer's FAILED actual try-lock.
    // These independent probes prove no Native/store leaf survives that wait.
    const int entered=sqlite3_mutex_try(native_mutex);EXPECT_EQ(entered,SQLITE_OK);
    if(entered==SQLITE_OK){EXPECT_TRUE(sqlite3_get_autocommit(native));sqlite3_mutex_leave(native_mutex);}
    const bool store_entered=store.try_lock();EXPECT_TRUE(store_entered);if(store_entered)store.unlock();
    held.finish();EXPECT_FALSE(held.forced.load());
    ASSERT_EQ(mutation.wait_for(5s),std::future_status::ready);ASSERT_NO_THROW(mutation.get());
    EXPECT_EQ(bodies.load(),1u);EXPECT_FALSE(writer->is_in_transaction());
    EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7,11}));EXPECT_EQ(delivered.size(),1u);
    owner.handle_cross_process_notification();EXPECT_EQ(delivered.size(),1u);
    owner.remove_table_observer("TestPerson",token);owner.close();
}
TEST(MutationPublicationAdmission, ActualForeignPublicationMutexHonorsConfiguredCapAndExplicitZero) {
    terminal_watchdog bounded;
    for(const auto configured:{0,80,30000}){
        lattice::configuration config(":memory:");config.busy_timeout_ms=configured;
        publication_operation_owner owner(config);owner.add(TestPerson{"seed",7,{}});
        const auto writer=topology_access::writer(owner);ASSERT_TRUE(writer);
        auto* native=topology_access::raw(*writer);auto* native_mutex=sqlite3_db_mutex(native);ASSERT_NE(native_mutex,nullptr);
        std::vector<int64_t> delivered;
        const auto token=owner.add_table_observer("TestPerson",[&](const auto& changes){
            for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
        });
        actual_publication_hold held(owner);
        std::promise<void> observed_;auto observed=observed_.get_future();
        std::atomic<unsigned> waits{0},bodies{0};
        auto mutation=std::async(std::launch::async,[&]()->std::exception_ptr{
            actual_publication_wait_observer observer([&](auto& actual){
                if(&actual!=&owner)throw std::runtime_error("wrong publication budget owner");
                if(waits.fetch_add(1)==0)observed_.set_value();
            });
            try{owner.unit([&](lattice::database& actual){++bodies;
                actual.insert("TestPerson",{{"globalId",std::string("publication-must-not-run")},{"name",std::string("publication")},{"age",int64_t(11)}});
            });return {};}catch(...){return std::current_exception();}
        });
        publication_before_join cleanup{held};
        if(configured>0)ASSERT_EQ(observed.wait_for(5s),std::future_status::ready);
        const auto completion_bound=configured==30000?3500ms:600ms;
        ASSERT_EQ(mutation.wait_for(completion_bound),std::future_status::ready);
        const auto first=mutation.get();ASSERT_TRUE(first);
        try{std::rethrow_exception(first);FAIL()<<"actual publication contention must refuse before effects";}
        catch(const lattice::db_error& error){
            EXPECT_EQ(std::string(error.what()),configured==0?
                "mutation publication busy; no eligible wait budget":"mutation admission busy budget exhausted");
        }
        EXPECT_EQ(bodies.load(),0u);EXPECT_TRUE(delivered.empty());
        if(configured==0)EXPECT_EQ(waits.load(),0u);else EXPECT_GT(waits.load(),0u);
        const int entered=sqlite3_mutex_try(native_mutex);EXPECT_EQ(entered,SQLITE_OK);
        if(entered==SQLITE_OK){EXPECT_TRUE(sqlite3_get_autocommit(native));sqlite3_mutex_leave(native_mutex);}
        held.finish();EXPECT_FALSE(held.forced.load());
        EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7}));EXPECT_FALSE(writer->is_in_transaction());
        owner.add(TestPerson{"after",33,{}});EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7,33}));
        EXPECT_EQ(delivered.size(),1u);owner.remove_table_observer("TestPerson",token);owner.close();
    }
}
} // namespace

namespace {
TEST(MutationPublicationAdmission, ActualAdmittedBodyBorrowsPublicationForNestedPublicBulkOnce) {
    terminal_watchdog bounded;
    for(const auto configured:{0,1000}) {
        lattice::configuration config(":memory:");config.busy_timeout_ms=configured;
        publication_operation_owner owner(config);owner.add(TestPerson{"seed",7,{}});
        const auto writer=topology_access::writer(owner);ASSERT_TRUE(writer);
        std::vector<int64_t> delivered;
        const auto token=owner.add_table_observer("TestPerson",[&](const auto& changes){
            for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
        });
        unsigned outer_calls=0,waits=0;
        actual_publication_wait_observer observer([&](auto&){++waits;});
        owner.unit([&](lattice::database& actual){
            ++outer_calls;EXPECT_EQ(&actual,writer.get());EXPECT_TRUE(actual.is_in_transaction());
            actual_publication_hold held(owner); // real foreign lock AFTER outer admission
            EXPECT_THROW(owner.close_write_db(),lattice::db_error); // actual parent counter fences replacement
            const auto added=owner.add_bulk(std::vector<TestPerson>{{"nested-one",11,{}},{"nested-two",22,{}}});
            EXPECT_EQ(added.size(),2u);EXPECT_TRUE(delivered.empty());
            EXPECT_EQ(publication_values(actual),(std::vector<int64_t>{7,11,22}));
            held.finish();EXPECT_FALSE(held.forced.load());
        });
        EXPECT_EQ(outer_calls,1u);EXPECT_EQ(waits,0u);EXPECT_FALSE(writer->is_in_transaction());
        EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7,11,22}));EXPECT_EQ(delivered.size(),2u);
        owner.handle_cross_process_notification();EXPECT_EQ(delivered.size(),2u);
        owner.remove_table_observer("TestPerson",token);owner.close();
    }
}
TEST(MutationPublicationAdmission, ActualNestedBulkAbortPreservesOuterPrefixUnderPublicationContention) {
    terminal_watchdog bounded;
    lattice::configuration config(":memory:");config.busy_timeout_ms=0;
    publication_operation_owner owner(config);owner.add(TestPerson{"seed",7,{}});
    const auto writer=topology_access::writer(owner);ASSERT_TRUE(writer);
    writer->execute("CREATE TRIGGER nested_publication_abort BEFORE INSERT ON TestPerson WHEN NEW.age=33 BEGIN SELECT RAISE(ABORT,'actual nested publication abort'); END");
    std::vector<int64_t> delivered;
    const auto token=owner.add_table_observer("TestPerson",[&](const auto& changes){
        for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
    });
    std::exception_ptr first;unsigned waits=0;
    actual_publication_wait_observer observer([&](auto&){++waits;});
    owner.unit([&](lattice::database& actual){
        actual.insert("TestPerson",{{"globalId",std::string("nested-prefix")},{"name",std::string("prefix")},{"age",int64_t(11)}});
        actual_publication_hold held(owner);
        try{(void)owner.add_bulk(std::vector<TestPerson>{{"nested-first",22,{}},{"nested-abort",33,{}}});}
        catch(...){first=std::current_exception();}
        EXPECT_EQ(publication_values(actual),(std::vector<int64_t>{7,11}));EXPECT_TRUE(delivered.empty());
        actual.insert("TestPerson",{{"globalId",std::string("nested-suffix")},{"name",std::string("suffix")},{"age",int64_t(44)}});
        held.finish();EXPECT_FALSE(held.forced.load());
    });
    ASSERT_TRUE(first);
    try{std::rethrow_exception(first);FAIL()<<"real second-row trigger must abort the nested savepoint";}
    catch(const lattice::db_error& error){EXPECT_NE(std::string(error.what()).find("actual nested publication abort"),std::string::npos);}
    EXPECT_EQ(waits,0u);EXPECT_FALSE(writer->is_in_transaction());
    EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7,11,44}));EXPECT_EQ(delivered.size(),2u);
    owner.handle_cross_process_notification();EXPECT_EQ(delivered.size(),2u);
    owner.remove_table_observer("TestPerson",token);owner.close();
}
TEST(MutationPublicationAdmission, CallerOrDifferentOwnerCannotBorrowAnAdmittedBodyPublication) {
    terminal_watchdog bounded;
    lattice::configuration config(":memory:");config.busy_timeout_ms=1000;
    publication_operation_owner owner(config);owner.add(TestPerson{"seed",7,{}});
    publication_operation_owner other(config);other.add(TestPerson{"other-seed",17,{}});
    const auto writer=topology_access::writer(owner),other_writer=topology_access::writer(other);ASSERT_TRUE(writer);ASSERT_TRUE(other_writer);
    unsigned waits=0,forbidden_bodies=0;
    actual_publication_wait_observer observer([&](auto&){++waits;});
    owner.begin_transaction();
    {
        actual_publication_hold held(owner);
        EXPECT_THROW(owner.unit([&](auto&){++forbidden_bodies;}),lattice::db_error);
        held.finish();EXPECT_FALSE(held.forced.load());
    }
    owner.rollback();
    owner.unit([&](lattice::database& actual){
        actual_publication_hold held(other);
        EXPECT_THROW(other.unit([&](auto&){++forbidden_bodies;}),lattice::db_error);
        held.finish();EXPECT_FALSE(held.forced.load());
        actual.insert("TestPerson",{{"globalId",std::string("different-owner-prefix")},{"name",std::string("prefix")},{"age",int64_t(11)}});
    });
    EXPECT_EQ(waits,0u);EXPECT_EQ(forbidden_bodies,0u);EXPECT_FALSE(writer->is_in_transaction());EXPECT_FALSE(other_writer->is_in_transaction());
    EXPECT_EQ(publication_values(*writer),(std::vector<int64_t>{7,11}));EXPECT_EQ(publication_values(*other_writer),(std::vector<int64_t>{17}));
    owner.close();other.close();
}
} // namespace
