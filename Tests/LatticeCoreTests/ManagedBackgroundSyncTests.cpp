#include "TestHelpers.hpp"
#include <lattice/network.hpp>
#include <lattice/sync_storage.hpp>
#include <chrono>
#include <future>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

#ifndef __EMSCRIPTEN__
namespace lattice {
struct background_sync_lifecycle_test_access {
    static bool retention_running(lattice_db& db) { return db.audit_maint_thread_.joinable(); }
    static bool retention_stopped(lattice_db& db) { return db.audit_maint_stop_.load(); }
    static bool parent_suspended(lattice_db& db) { return db.background_sync_suspended_.load(); }
    static void notify(lattice_db& db) { db.handle_cross_process_notification(); }
    static auto xproc_connection(lattice_db& db) { return db.borrow_xproc_read_connection(); }
    static bool reserve_identity_key(const configuration& cfg) {
        return lattice_db::try_register_sync_key(cfg.path, cfg.effective_websocket_sync_identity());
    }
    static void release_identity_key(const configuration& cfg) {
        lattice_db::unregister_sync_key(cfg.path, cfg.effective_websocket_sync_identity());
    }
};
}
namespace {
using admission = lattice::lattice_db::background_sync_admission;
using phase = lattice::synchronizer_base::finite_cycle_phase;
using namespace std::chrono_literals;
struct managed_fixture {
    std::shared_ptr<lattice::network_factory> previous = lattice::get_network_factory();
    std::shared_ptr<lattice::mock_network_factory> network = std::make_shared<lattice::mock_network_factory>();
    TempDB file{"managed_background"};
    managed_fixture() { lattice::set_network_factory(network); }
    ~managed_fixture() {
        lattice::set_network_factory(previous);
        std::error_code ignored;
        std::filesystem::remove(file.str() + ".sync.lock", ignored);
    }
    lattice::configuration config(bool managed = true) {
        lattice::configuration cfg(file.str(), "ws://fixture.invalid/managed", "test");
        cfg.background_sync_managed = managed;
        cfg.busy_timeout_ms = 3000;
        return cfg;
    }
    bool lock_available() const {
        const int fd = ::open((file.str() + ".sync.lock").c_str(), O_CREAT | O_RDWR, 0600);
        if (fd < 0) return false;
        const bool acquired = ::flock(fd, LOCK_EX | LOCK_NB) == 0;
        if (acquired) ::flock(fd, LOCK_UN);
        ::close(fd);
        return acquired;
    }
};
template<class F> bool await_condition(F&& condition) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!condition()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}
}

TEST(ManagedBackgroundSync, StartsSuspendedAndPreservesPrimaryIdentityAcrossGenerations) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    auto* original_writer = &owner.db();
    const auto row = owner.add(TestPerson{"local", 31, std::nullopt});
    EXPECT_FALSE(owner.is_sync_agent());
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
    ASSERT_TRUE(fixture.lock_available());
    auto first = owner.resume_background_sync({}, true);
    ASSERT_EQ(first.admission, admission::started);
    EXPECT_NE(first.generation, 0u);
    EXPECT_FALSE(fixture.lock_available());
    EXPECT_EQ(owner.get_background_sync_status(first.generation).phase, phase::running);
    ASSERT_TRUE(owner.suspend_background_sync(first.generation));
    EXPECT_TRUE(fixture.lock_available());
    EXPECT_FALSE(owner.is_sync_agent());
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
    EXPECT_EQ(&owner.db(), original_writer);
    EXPECT_TRUE(owner.find_by_global_id<TestPerson>(row.global_id()).has_value());
    EXPECT_NO_THROW(owner.add(TestPerson{"still writable", 32, std::nullopt}));
    auto next = owner.resume_background_sync({}, true);
    ASSERT_EQ(next.admission, admission::started);
    EXPECT_GT(next.generation, first.generation);
    EXPECT_TRUE(owner.suspend_background_sync(next.generation));
}

TEST(ManagedBackgroundSync, SuspensionDoesNotHandOffToDormantOrdinarySibling) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    const auto run = owner.resume_background_sync({}, true);
    ASSERT_EQ(run.admission, admission::started);
    lattice::lattice_db dormant(fixture.config(false));
    ASSERT_FALSE(dormant.is_sync_agent());
    ASSERT_TRUE(owner.suspend_background_sync(run.generation));
    EXPECT_FALSE(dormant.is_sync_agent());
    EXPECT_TRUE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, StaleExpirationCannotCancelSuccessor) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    auto old_token = std::make_shared<std::atomic<bool>>(false);
    auto old = owner.resume_background_sync(old_token, true);
    ASSERT_EQ(old.admission, admission::started);
    ASSERT_TRUE(owner.suspend_background_sync(old.generation));
    auto next_token = std::make_shared<std::atomic<bool>>(false);
    auto next = owner.resume_background_sync(next_token, true);
    ASSERT_EQ(next.admission, admission::started);
    old_token->store(true);
    EXPECT_FALSE(owner.cancel_background_sync(old.generation));
    EXPECT_FALSE(owner.suspend_background_sync(old.generation));
    EXPECT_FALSE(next_token->load());
    EXPECT_EQ(owner.current_background_sync_generation(), next.generation);
    EXPECT_EQ(owner.get_background_sync_status(next.generation).phase, phase::running);
    EXPECT_TRUE(owner.suspend_background_sync(next.generation));
}

TEST(ManagedBackgroundSync, ExistingOrdinaryOwnerIsLeftUntouched) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config(false));
    auto token = std::make_shared<std::atomic<bool>>(false);
    ASSERT_TRUE(owner.is_sync_agent());
    auto run = owner.resume_background_sync(token, true);
    EXPECT_EQ(run.admission, admission::already_running);
    EXPECT_EQ(run.generation, 0u);
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
    EXPECT_FALSE(token->load());
    EXPECT_TRUE(owner.is_sync_connected());
    EXPECT_FALSE(owner.cancel_background_sync(1));
    EXPECT_FALSE(owner.suspend_background_sync(1));
    EXPECT_TRUE(owner.is_sync_agent());
}

TEST(ManagedBackgroundSync, OrdinaryManualFiniteRestoresPriorRetentionAndKeepsParentReaders) {
    managed_fixture fixture;
    auto cfg = fixture.config(false);
    cfg.sync_autostart = false;
    cfg.audit_retention_seconds = 3600;
    lattice::lattice_db owner(cfg);
    ASSERT_TRUE(lattice::background_sync_lifecycle_test_access::retention_running(owner));
    const auto keeper = owner.acquire_read_generation();
    ASSERT_NE(keeper, 0u);
    auto run = owner.resume_background_sync({}, true);
    ASSERT_EQ(run.admission, admission::started);
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::retention_running(owner));
    EXPECT_TRUE(owner.suspend_background_sync(run.generation));
    EXPECT_TRUE(lattice::background_sync_lifecycle_test_access::retention_running(owner));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::retention_stopped(owner));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::parent_suspended(owner));
    EXPECT_TRUE(owner.query_at_generation(keeper, "SELECT 1 AS n").has_value());
    owner.release_read_generation(keeper);
    owner.connect_sync();
    EXPECT_TRUE(owner.is_sync_agent());
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
}

TEST(ManagedBackgroundSync, ManagedConfigurationRejectsMemoryAndIPCBeforeOpeningStore) {
    managed_fixture fixture;
    auto cfg = fixture.config();
    cfg.ipc_targets.emplace_back();
    cfg.ipc_targets.back().channel = "unsupported-managed-ipc";
    EXPECT_THROW((lattice::lattice_db(cfg)), lattice::db_error);
    EXPECT_FALSE(std::filesystem::exists(fixture.file.path));
    lattice::configuration memory(":memory:", "ws://fixture.invalid/managed", "test");
    memory.background_sync_managed = true;
    EXPECT_THROW((lattice::lattice_db(memory)), lattice::db_error);
}

TEST(ManagedBackgroundSync, PreallocatedCancellationInterruptsPrivateSetupWithoutTouchingAppTransaction) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    owner.db().begin_transaction();
    owner.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('application-owned','kept')");
    auto token = std::make_shared<std::atomic<bool>>(false);
    std::exception_ptr error;
    lattice::lattice_db::background_sync_start result;
    std::thread worker([&] {
        try { result = owner.resume_background_sync(token, true); }
        catch (...) { error = std::current_exception(); }
    });
    const bool admitted = await_condition([&] { return owner.current_background_sync_generation() != 0; });
    const auto cancelled_at = std::chrono::steady_clock::now();
    token->store(true);
    worker.join();
    EXPECT_TRUE(admitted);
    EXPECT_FALSE(error);
    EXPECT_EQ(result.admission, admission::cancelled);
    EXPECT_LT(std::chrono::steady_clock::now() - cancelled_at, 1500ms);
    EXPECT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(owner.db().query("SELECT value FROM _lattice_meta WHERE key='application-owned'").size(), 1u);
    owner.db().rollback();
    EXPECT_TRUE(fixture.lock_available());
    EXPECT_NO_THROW(owner.add(TestPerson{"after cancellation", 20, std::nullopt}));
}

TEST(ManagedBackgroundSync, CallbackCloseRetainsGenerationUntilOffThreadSettlement) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    auto run = owner.resume_background_sync({}, true);
    ASSERT_EQ(run.admission, admission::started);
    std::promise<void> closed, release_worker;
    auto release = release_worker.get_future().share();
    std::atomic<bool> invoked{false};
    owner.set_on_sync_state_change([&](bool connected) {
        if (connected && !invoked.exchange(true)) {
            owner.close();
            closed.set_value();
            release.wait();
        }
    });
    const bool reached = closed.get_future().wait_for(5s) == std::future_status::ready;
    EXPECT_TRUE(reached);
    EXPECT_TRUE(owner.is_closed());
    EXPECT_EQ(owner.current_background_sync_generation(), run.generation);
    auto settle = std::async(std::launch::async, [&] {
        return owner.suspend_background_sync(run.generation);
    });
    EXPECT_EQ(settle.wait_for(20ms), std::future_status::timeout);
    release_worker.set_value();
    EXPECT_TRUE(settle.get());
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
    EXPECT_TRUE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, AccessorsCanRaceSuspensionWithoutBorrowingRetiredOwner) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    auto run = owner.resume_background_sync({}, true);
    ASSERT_EQ(run.admission, admission::started);
    std::atomic<bool> stop{false};
    std::thread reader([&] {
        while (!stop.load()) {
            (void)owner.is_sync_agent();
            (void)owner.is_sync_connected();
            (void)owner.get_sync_progress();
            (void)owner.get_background_sync_status(run.generation);
        }
    });
    const bool settled = owner.suspend_background_sync(run.generation);
    stop = true;
    reader.join();
    EXPECT_TRUE(settled);
    EXPECT_TRUE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, ManagedSuspensionRetiresKeepersAndRejectsLateAcquisition) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    EXPECT_EQ(owner.acquire_read_generation(), 0u);
    const auto run = owner.resume_background_sync({}, false);
    ASSERT_EQ(run.admission, admission::started);
    const auto keeper = owner.acquire_read_generation();
    ASSERT_NE(keeper, 0u);
    std::atomic<bool> stop{false};
    std::thread acquire([&] {
        while (!stop.load()) {
            const auto generation = owner.acquire_read_generation();
            if (generation) owner.release_read_generation(generation);
        }
    });
    const bool settled = owner.suspend_background_sync(run.generation);
    stop = true;
    acquire.join();
    EXPECT_TRUE(settled);
    EXPECT_FALSE(owner.query_at_generation(keeper, "SELECT 1 AS n").has_value());
    EXPECT_EQ(owner.acquire_read_generation(), 0u);
    EXPECT_EQ(owner.read_generations_outstanding(), 0u);
    const auto foreground = owner.resume_background_sync({}, false);
    ASSERT_EQ(foreground.admission, admission::started);
    const auto new_keeper = owner.acquire_read_generation();
    EXPECT_NE(new_keeper, 0u);
    owner.release_read_generation(new_keeper);
    EXPECT_TRUE(owner.suspend_background_sync(foreground.generation));
}

TEST(ManagedBackgroundSync, OrdinaryManualCancellationRestoresParentRetention) {
    managed_fixture fixture;
    auto cfg = fixture.config(false);
    cfg.sync_autostart = false;
    cfg.audit_retention_seconds = 3600;
    lattice::lattice_db owner(cfg);
    auto token = std::make_shared<std::atomic<bool>>(false);
    const auto run = owner.resume_background_sync(token, true);
    ASSERT_EQ(run.admission, admission::started);
    ASSERT_TRUE(owner.cancel_background_sync(run.generation));
    EXPECT_TRUE(owner.suspend_background_sync(run.generation));
    EXPECT_TRUE(lattice::background_sync_lifecycle_test_access::retention_running(owner));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::parent_suspended(owner));
    EXPECT_TRUE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, OrdinaryManualSetupFailureRestoresParentRetention) {
    managed_fixture fixture;
    struct failing_factory : lattice::mock_network_factory {
        std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
            throw std::runtime_error("fixture transport creation failure");
        }
    };
    auto cfg = fixture.config(false);
    cfg.sync_autostart = false;
    cfg.audit_retention_seconds = 3600;
    lattice::lattice_db owner(cfg);
    lattice::set_network_factory(std::make_shared<failing_factory>());
    EXPECT_THROW(owner.resume_background_sync({}, true), std::runtime_error);
    EXPECT_EQ(owner.current_background_sync_generation(), 0u);
    EXPECT_TRUE(lattice::background_sync_lifecycle_test_access::retention_running(owner));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::parent_suspended(owner));
    EXPECT_TRUE(fixture.lock_available());
    EXPECT_NO_THROW(owner.add(TestPerson{"after setup failure", 4, std::nullopt}));
}

TEST(ManagedBackgroundSync, OrdinaryCallbackCloseDuringSetupDoesNotPublishClosedOwner) {
    managed_fixture fixture;
    std::promise<void> callback_done;
    auto ready = callback_done.get_future().share();
    struct waiting_transport : lattice::mock_sync_transport {
        std::shared_future<void> ready;
        explicit waiting_transport(std::shared_future<void> value) : ready(std::move(value)) {}
        void connect(const std::string& url, const std::map<std::string, std::string>& headers = {}) override {
            mock_sync_transport::connect(url, headers);
            if (ready.wait_for(5s) != std::future_status::ready)
                throw std::runtime_error("callback close did not return during setup");
        }
    };
    struct waiting_factory : lattice::mock_network_factory {
        std::shared_future<void> ready;
        explicit waiting_factory(std::shared_future<void> value) : ready(std::move(value)) {}
        std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
            return std::make_unique<waiting_transport>(ready);
        }
    };
    auto cfg = fixture.config(false);
    cfg.sync_autostart = false;
    lattice::lattice_db owner(cfg);
    lattice::set_network_factory(std::make_shared<waiting_factory>(ready));
    std::atomic<bool> invoked{false};
    owner.set_on_sync_state_change([&](bool connected) {
        if (connected && !invoked.exchange(true)) {
            owner.close();
            callback_done.set_value();
        }
    });
    EXPECT_NO_THROW(owner.connect_sync());
    EXPECT_TRUE(owner.is_closed());
    EXPECT_TRUE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, SuspendedAutonomousNotificationsDoNotRunSQL) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    auto connection = lattice::background_sync_lifecycle_test_access::xproc_connection(owner);
    std::atomic<int> statements{0};
    sqlite3_trace_v2(connection->handle(), SQLITE_TRACE_STMT,
        [](unsigned, void* raw, void*, void*) -> int {
            ++*static_cast<std::atomic<int>*>(raw); return 0;
        }, &statements);
    lattice::background_sync_lifecycle_test_access::notify(owner);
    EXPECT_EQ(statements.load(), 0);
    const auto run = owner.resume_background_sync({}, false);
    ASSERT_EQ(run.admission, admission::started);
    lattice::background_sync_lifecycle_test_access::notify(owner);
    EXPECT_GT(statements.load(), 0);
    ASSERT_TRUE(owner.suspend_background_sync(run.generation));
    statements = 0;
    lattice::background_sync_lifecycle_test_access::notify(owner);
    EXPECT_EQ(statements.load(), 0);
    sqlite3_trace_v2(connection->handle(), 0, nullptr, nullptr);
}

TEST(ManagedBackgroundSync, CallbackSubscriptionsDuringSetupKeepLatestHandlers) {
    managed_fixture fixture;
    struct setup_barrier {
        std::promise<void> entered, released;
        std::shared_future<void> release = released.get_future().share();
    };
    struct waiting_factory : lattice::mock_network_factory {
        std::shared_ptr<setup_barrier> barrier;
        explicit waiting_factory(std::shared_ptr<setup_barrier> value) : barrier(std::move(value)) {}
        std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
            barrier->entered.set_value();
            if (barrier->release.wait_for(5s) != std::future_status::ready)
                throw std::runtime_error("setup subscription barrier timed out");
            return mock_network_factory::create_sync_transport();
        }
    };
    auto barrier = std::make_shared<setup_barrier>();
    auto network = std::make_shared<waiting_factory>(barrier);
    lattice::set_network_factory(network);
    lattice::lattice_db owner(fixture.config());
    owner.add(TestPerson{"upload after subscription", 5, std::nullopt});
    std::atomic<int> old_state{0}, old_error{0}, old_progress{0};
    std::atomic<int> state{0}, error{0}, progress{0};
    owner.set_on_sync_state_change([&](bool) { ++old_state; });
    owner.set_on_sync_error([&](const std::string&) { ++old_error; });
    owner.set_on_sync_progress([&](const auto&) { ++old_progress; });
    lattice::lattice_db::background_sync_start run;
    std::exception_ptr setup_error;
    std::thread resume([&] {
        try { run = owner.resume_background_sync({}, true); }
        catch (...) { setup_error = std::current_exception(); }
    });
    const bool entered = barrier->entered.get_future().wait_for(5s) == std::future_status::ready;
    EXPECT_NO_THROW(owner.set_on_sync_state_change([&](bool connected) { if (connected) ++state; }));
    EXPECT_NO_THROW(owner.set_on_sync_error([&](const std::string&) { ++error; }));
    EXPECT_NO_THROW(owner.set_on_sync_progress([&](const auto&) { ++progress; }));
    barrier->released.set_value();
    resume.join();
    ASSERT_TRUE(entered);
    ASSERT_FALSE(setup_error);
    ASSERT_EQ(run.admission, admission::started);
    EXPECT_TRUE(await_condition([&] { return state.load() > 0 && progress.load() > 0; }));
    network->last_websocket()->simulate_error("fixture error after subscription");
    EXPECT_TRUE(await_condition([&] { return error.load() > 0; }));
    EXPECT_EQ(old_state.load(), 0);
    EXPECT_EQ(old_error.load(), 0);
    EXPECT_EQ(old_progress.load(), 0);
    EXPECT_TRUE(owner.suspend_background_sync(run.generation));
}

namespace {
enum class reentrant_subscription { state, error, progress };
struct scoped_callback_death_test_style {
    std::string old = GTEST_FLAG_GET(death_test_style);
    scoped_callback_death_test_style() { GTEST_FLAG_SET(death_test_style, "threadsafe"); }
    ~scoped_callback_death_test_style() { GTEST_FLAG_SET(death_test_style, old); }
};
void callback_replacement_releases_outside_locks(reentrant_subscription kind) {
    managed_fixture fixture;
    lattice::lattice_db owner(fixture.config());
    int destroyed = 0;
    struct capture {
        lattice::lattice_db& owner;
        int& destroyed;
        reentrant_subscription kind;
        capture(lattice::lattice_db& db, int& count, reentrant_subscription value)
            : owner(db), destroyed(count), kind(value) {}
        ~capture() {
            ++destroyed;
            // Reenter the exact registration lock, then exercise close's
            // lifecycle transition. Neither may run under a retired slot lock.
            switch (kind) {
            case reentrant_subscription::state: owner.set_on_sync_state_change({}); break;
            case reentrant_subscription::error: owner.set_on_sync_error({}); break;
            case reentrant_subscription::progress: owner.set_on_sync_progress({}); break;
            }
            owner.close();
        }
    };
    auto value = std::make_shared<capture>(owner, destroyed, kind);
    switch (kind) {
    case reentrant_subscription::state:
        owner.set_on_sync_state_change([value](bool) {}); value.reset();
        owner.set_on_sync_state_change({}); break;
    case reentrant_subscription::error:
        owner.set_on_sync_error([value](const std::string&) {}); value.reset();
        owner.set_on_sync_error({}); break;
    case reentrant_subscription::progress:
        owner.set_on_sync_progress([value](const auto&) {}); value.reset();
        owner.set_on_sync_progress({}); break;
    }
    if (destroyed != 1 || !owner.is_closed()) _exit(2);
}
}

TEST(ManagedBackgroundSync, ReplacingStateCallbackCanReenterAndCloseFromCaptureDestructor) {
    scoped_callback_death_test_style style;
    EXPECT_EXIT({ alarm(5); callback_replacement_releases_outside_locks(reentrant_subscription::state); _exit(0); },
                ::testing::ExitedWithCode(0), "");
}
TEST(ManagedBackgroundSync, ReplacingErrorCallbackCanReenterAndCloseFromCaptureDestructor) {
    scoped_callback_death_test_style style;
    EXPECT_EXIT({ alarm(5); callback_replacement_releases_outside_locks(reentrant_subscription::error); _exit(0); },
                ::testing::ExitedWithCode(0), "");
}
TEST(ManagedBackgroundSync, ReplacingProgressCallbackCanReenterAndCloseFromCaptureDestructor) {
    scoped_callback_death_test_style style;
    EXPECT_EXIT({ alarm(5); callback_replacement_releases_outside_locks(reentrant_subscription::progress); _exit(0); },
                ::testing::ExitedWithCode(0), "");
}

TEST(ManagedBackgroundSync, ForegroundLocalReceiptWorksWhileAnotherStoreOwnsWSS) {
    managed_fixture fixture;
    lattice::lattice_db sync_owner(fixture.config(false));
    ASSERT_TRUE(sync_owner.is_sync_agent());
    ASSERT_TRUE(sync_owner.is_sync_connected());
    auto cfg = fixture.config();
    cfg.audit_retention_seconds = 3600;
    lattice::lattice_db local(cfg);
    auto connection = lattice::background_sync_lifecycle_test_access::xproc_connection(local);
    std::atomic<int> statements{0};
    sqlite3_trace_v2(connection->handle(), SQLITE_TRACE_STMT,
        [](unsigned, void* raw, void*, void*) -> int {
            ++*static_cast<std::atomic<int>*>(raw); return 0;
        }, &statements);
    const auto run = local.resume_background_sync({}, false);
    ASSERT_EQ(run.admission, admission::started);
    ASSERT_NE(run.generation, 0u);
    EXPECT_FALSE(local.is_sync_agent());
    EXPECT_EQ(local.current_background_sync_generation(), run.generation);
    EXPECT_TRUE(lattice::background_sync_lifecycle_test_access::retention_running(local));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::parent_suspended(local));
    const auto keeper = local.acquire_read_generation();
    ASSERT_NE(keeper, 0u);
    lattice::background_sync_lifecycle_test_access::notify(local);
    EXPECT_GT(statements.load(), 0);
    EXPECT_EQ(local.resume_background_sync({}, false).admission, admission::already_running);
    EXPECT_EQ(local.current_background_sync_generation(), run.generation);
    EXPECT_FALSE(local.is_sync_agent());
    EXPECT_TRUE(local.suspend_background_sync(run.generation));
    EXPECT_FALSE(local.query_at_generation(keeper, "SELECT 1 AS n").has_value());
    EXPECT_EQ(local.acquire_read_generation(), 0u);
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::retention_running(local));
    statements = 0;
    lattice::background_sync_lifecycle_test_access::notify(local);
    EXPECT_EQ(statements.load(), 0);
    sqlite3_trace_v2(connection->handle(), 0, nullptr, nullptr);
    EXPECT_TRUE(sync_owner.is_sync_agent());
    EXPECT_TRUE(sync_owner.is_sync_connected());
    EXPECT_FALSE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, FiniteAttemptStillRejectsForeignWSSOwnership) {
    managed_fixture fixture;
    lattice::lattice_db sync_owner(fixture.config(false));
    lattice::lattice_db local(fixture.config());
    const auto run = local.resume_background_sync({}, true);
    EXPECT_EQ(run.admission, admission::not_owner);
    EXPECT_EQ(local.current_background_sync_generation(), 0u);
    EXPECT_EQ(local.acquire_read_generation(), 0u);
    EXPECT_TRUE(lattice::background_sync_lifecycle_test_access::parent_suspended(local));
    EXPECT_TRUE(sync_owner.is_sync_connected());
    EXPECT_FALSE(fixture.lock_available());
}

TEST(ManagedBackgroundSync, ExplicitConnectRetriesWSSFromLocalReceiptWithFreshGeneration) {
    managed_fixture fixture;
    lattice::lattice_db sync_owner(fixture.config(false));
    lattice::lattice_db local(fixture.config());
    auto old_token = std::make_shared<std::atomic<bool>>(false);
    const auto run = local.resume_background_sync(old_token, false);
    ASSERT_EQ(run.admission, admission::started);
    ASSERT_FALSE(local.is_sync_agent());
    sync_owner.close();
    ASSERT_TRUE(fixture.lock_available());
    EXPECT_NO_THROW(local.connect_sync());
    const auto next = local.current_background_sync_generation();
    EXPECT_GT(next, run.generation);
    EXPECT_TRUE(local.is_sync_agent());
    EXPECT_TRUE(local.is_sync_connected());
    old_token->store(true);
    EXPECT_FALSE(local.cancel_background_sync(run.generation));
    EXPECT_FALSE(local.suspend_background_sync(run.generation));
    EXPECT_TRUE(local.is_sync_connected());
    EXPECT_TRUE(local.suspend_background_sync(next));
    EXPECT_TRUE(fixture.lock_available());
}

namespace {
void managed_local_file_lifecycle(bool read_only) {
    managed_fixture fixture;
    {
        lattice::lattice_db seed(fixture.file.str());
        seed.add(TestPerson{"local borrowed row", 21, std::nullopt});
    }
    lattice::configuration cfg(fixture.file.str());
    cfg.background_sync_managed = true;
    cfg.read_only = read_only;
    cfg.audit_retention_seconds = 3600;
    lattice::lattice_db local(cfg);
    EXPECT_EQ(local.acquire_read_generation(), 0u);
    EXPECT_EQ(local.resume_background_sync({}, true).admission, admission::unsupported);
    const auto run = local.resume_background_sync({}, false);
    ASSERT_EQ(run.admission, admission::started);
    EXPECT_NE(run.generation, 0u);
    EXPECT_FALSE(local.is_sync_agent());
    EXPECT_EQ(fixture.network->last_websocket(), nullptr);
    EXPECT_EQ(lattice::background_sync_lifecycle_test_access::retention_running(local), !read_only);
    const auto keeper = local.acquire_read_generation();
    ASSERT_NE(keeper, 0u);
    const auto rows = local.query_at_generation(keeper, "SELECT name FROM TestPerson");
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows->front().at("name")), "local borrowed row");
    EXPECT_EQ(local.resume_background_sync({}, true).admission, admission::unsupported);
    EXPECT_EQ(local.current_background_sync_generation(), run.generation);
    EXPECT_TRUE(local.suspend_background_sync(run.generation));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::retention_running(local));
    EXPECT_FALSE(local.query_at_generation(keeper, "SELECT 1 AS n").has_value());
    EXPECT_EQ(local.acquire_read_generation(), 0u);
    EXPECT_EQ(local.current_background_sync_generation(), 0u);
    EXPECT_EQ(fixture.network->last_websocket(), nullptr);
}
}
TEST(ManagedBackgroundSync, WritableLocalBorrowerHasManagedLifecycleWithoutWSS) {
    managed_local_file_lifecycle(false);
}
TEST(ManagedBackgroundSync, ReadOnlyLocalBorrowerHasManagedLifecycleWithoutWSS) {
    managed_local_file_lifecycle(true);
}

TEST(ManagedBackgroundSync, SuspensionCancelsActualRetentionTickWithoutTouchingForeignTransaction) {
    managed_fixture fixture;
    constexpr int busy_budget_ms = 3000;
    lattice::configuration cfg(fixture.file.str());
    cfg.background_sync_managed = true;
    cfg.busy_timeout_ms = busy_budget_ms;
    cfg.audit_retention_seconds = 1;
    lattice::lattice_db local(cfg);
    lattice::database foreign(fixture.file.str());
    // This is a competing current writer, not the pre-cutover rejection fixture.
    lattice::detail::register_sync_storage_writer(foreign);
    const auto run = local.resume_background_sync({}, false);
    ASSERT_EQ(run.admission, admission::started);
    foreign.begin_transaction();
    foreign.execute("INSERT INTO _lattice_meta(key,value) VALUES('foreign-retention-lock','kept')");
    std::atomic<bool> tick_entered{false};
    struct trace_guard {
        sqlite3* connection;
        ~trace_guard() { sqlite3_trace_v2(connection, 0, nullptr, nullptr); }
    } trace{local.db().handle()};
    sqlite3_trace_v2(trace.connection, SQLITE_TRACE_STMT,
        [](unsigned, void* raw, void* statement, void*) -> int {
            const auto* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
            if (sql && std::strncmp(sql, "BEGIN", 5) == 0)
                static_cast<std::atomic<bool>*>(raw)->store(true, std::memory_order_release);
            return 0;
        }, &tick_entered);
    // This is the actual timer-owned retention thread, not a direct invocation
    // of its helper. The foreign transaction forces its BEGIN into a busy wait.
    const bool entered = await_condition([&] { return tick_entered.load(std::memory_order_acquire); });
    const auto requested = std::chrono::steady_clock::now();
    const bool settled = local.suspend_background_sync(run.generation);
    const auto elapsed = std::chrono::steady_clock::now() - requested;
    EXPECT_TRUE(entered);
    EXPECT_TRUE(settled);
    EXPECT_LT(elapsed, std::chrono::milliseconds(busy_budget_ms / 2));
    EXPECT_FALSE(lattice::background_sync_lifecycle_test_access::retention_running(local));
    EXPECT_TRUE(foreign.is_in_transaction());
    EXPECT_EQ(foreign.query("SELECT value FROM _lattice_meta WHERE key='foreign-retention-lock'").size(), 1u);
    foreign.rollback();
    EXPECT_TRUE(local.db().query("SELECT value FROM _lattice_meta WHERE key='foreign-retention-lock'").empty());
    EXPECT_NO_THROW(local.add(TestPerson{"app writer after retention cancellation", 7, std::nullopt}));
}
namespace {
class identity_transport : public lattice::mock_sync_transport {
    mutable std::mutex observation_mutex_;
    std::string target_;
public:
    void connect(const std::string& target,
                 const std::map<std::string, std::string>& headers = {}) override {
        { std::lock_guard<std::mutex> lock(observation_mutex_); target_ = target; }
        lattice::mock_sync_transport::connect(target, headers);
    }
    std::string target() const {
        std::lock_guard<std::mutex> lock(observation_mutex_); return target_;
    }
};
class identity_network : public lattice::mock_network_factory {
    mutable std::mutex observation_mutex_;
    identity_transport* latest_ = nullptr;
    size_t count_ = 0;
public:
    std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
        auto value = std::make_unique<identity_transport>();
        { std::lock_guard<std::mutex> lock(observation_mutex_); latest_ = value.get(); ++count_; }
        return value;
    }
    identity_transport* latest() const {
        std::lock_guard<std::mutex> lock(observation_mutex_); return latest_;
    }
    size_t count() const {
        std::lock_guard<std::mutex> lock(observation_mutex_); return count_;
    }
};
std::vector<lattice::audit_log_entry> identity_uploads(identity_transport& wire) {
    std::vector<lattice::audit_log_entry> result;
    for (const auto& message : wire.get_sent_messages()) {
        const auto event = lattice::server_sent_event::from_json(message.as_string());
        if (event && event->event_type == lattice::server_sent_event::type::audit_log)
            result.insert(result.end(), event->audit_logs.begin(), event->audit_logs.end());
    }
    return result;
}
}

TEST(ManagedBackgroundSync, ChangedDialURLReplaysOriginalIdentityReceiptAndCursor) {
    managed_fixture fixture;
    auto network = std::make_shared<identity_network>();
    lattice::set_network_factory(network);
    const std::string original_url = "ws://fixture.invalid/sync?schema=5";
    const std::string updated_url = "ws://fixture.invalid/sync?schema=6";
    const std::string original_id = "wss:" + original_url;
    const std::string unused_id = "wss:" + updated_url;
    auto original = fixture.config(); original.websocket_url = original_url;
    original.sync_filter = std::vector<lattice::sync_filter_entry>{{"TestPerson", "age = 41"}};
    EXPECT_TRUE(original.websocket_sync_identity.empty());
    EXPECT_EQ(original.effective_websocket_sync_identity(), original_url);
    auto first = std::make_unique<lattice::lattice_db>(original);
    first->add(TestPerson{"uncertain local delivery", 41, std::nullopt});
    const auto first_run = first->resume_background_sync({}, true);
    ASSERT_EQ(first_run.admission, admission::started);
    auto* first_wire = network->latest(); ASSERT_NE(first_wire, nullptr);
    ASSERT_TRUE(await_condition([&] { return identity_uploads(*first_wire).size() == 1; }));
    const auto sent = identity_uploads(*first_wire).front();

    // Receive through the real transport handler so the old cursor is written by normal apply.
    lattice::audit_log_entry remote;
    remote.global_id = "remote-cursor-before-schema-change";
    remote.global_row_id = "remote-row-before-schema-change";
    remote.table_name = "TestPerson"; remote.operation = "INSERT";
    remote.timestamp = "2026-11-01 00:00:00";
    remote.changed_fields = {{"name", lattice::any_property("remote")},
                             {"age", lattice::any_property(int64_t(7))}};
    remote.changed_fields_names = {"name", "age"};
    first_wire->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_audit_log({remote}).to_json()));
    ASSERT_TRUE(await_condition([&] {
        auto rows = first->db().query("SELECT last_received_event_id FROM _lattice_replication_slots WHERE sync_id=?", {original_id});
        return rows.size() == 1 && rows.front().at("last_received_event_id") == lattice::column_value_t(remote.global_id);
    }));
    const auto retained = first->db().query("SELECT * FROM _lattice_sync_outbox WHERE sync_id=?", {original_id});
    ASSERT_EQ(retained.size(), 1u);
    EXPECT_EQ(retained.front().at("wire_global_id"), lattice::column_value_t(sent.global_id));
    // The local upload was sent, but never ACKed. Cancel/join must retain that exact obligation.
    ASSERT_TRUE(first->suspend_background_sync(first_run.generation));
    EXPECT_EQ(first->db().query("SELECT * FROM _lattice_sync_outbox WHERE sync_id=?", {original_id}), retained);
    first.reset();
    ASSERT_TRUE(fixture.lock_available());

    auto updated = fixture.config(); updated.websocket_url = updated_url;
    updated.sync_filter = original.sync_filter;
    updated.websocket_sync_identity = original_url;
    EXPECT_EQ(updated.effective_websocket_sync_identity(), original_url);
    lattice::lattice_db next(updated);
    EXPECT_EQ(next.db().query("SELECT * FROM _lattice_sync_outbox WHERE sync_id=?", {original_id}), retained);
    const auto second_run = next.resume_background_sync({}, true);
    ASSERT_EQ(second_run.admission, admission::started);
    auto* second_wire = network->latest(); ASSERT_NE(second_wire, nullptr);
    ASSERT_EQ(network->count(), 2u);
    ASSERT_TRUE(await_condition([&] { return identity_uploads(*second_wire).size() == 1; }));
    const auto target = second_wire->target();
    EXPECT_EQ(target.find(updated_url), 0u);
    EXPECT_EQ(target.find("schema=5"), std::string::npos);
    EXPECT_NE(target.find("last-event-id=" + remote.global_id), std::string::npos);
    EXPECT_EQ(identity_uploads(*second_wire).front().to_json(), sent.to_json());
    EXPECT_EQ(next.db().query("SELECT * FROM _lattice_sync_outbox WHERE sync_id=?", {original_id}), retained);
    for (const auto* table : {"_lattice_sync_outbox", "_lattice_sync_state", "_lattice_sync_set", "_lattice_replication_slots"}) {
        EXPECT_TRUE(next.db().query(std::string("SELECT 1 FROM ") + table + " WHERE sync_id=?", {unused_id}).empty());
    }
    EXPECT_EQ(next.db().query("SELECT 1 FROM TestPerson").size(), 2u);
    second_wire->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack({sent.global_id}).to_json()));
    ASSERT_TRUE(await_condition([&] {
        return next.db().query("SELECT 1 FROM _lattice_sync_outbox WHERE sync_id=?", {original_id}).empty();
    }));
    const auto final_cursor = next.db().query("SELECT last_received_event_id FROM _lattice_replication_slots WHERE sync_id=?", {original_id});
    ASSERT_EQ(final_cursor.size(), 1u);
    EXPECT_EQ(final_cursor.front().at("last_received_event_id"), lattice::column_value_t(remote.global_id));
    EXPECT_TRUE(next.db().query("SELECT 1 FROM _lattice_replication_slots WHERE sync_id=?", {unused_id}).empty());
    EXPECT_TRUE(next.suspend_background_sync(second_run.generation));
}

TEST(ManagedBackgroundSync, SameLogicalIdentityDoesNotEvictOwnerOrReserveASecondKey) {
    managed_fixture fixture;
    auto network = std::make_shared<identity_network>();
    lattice::set_network_factory(network);
    auto original = fixture.config(false);
    original.websocket_url = "ws://fixture.invalid/sync?schema=5";
    auto first = std::make_unique<lattice::lattice_db>(original);
    ASSERT_TRUE(first->is_sync_agent());
    auto* first_wire = network->latest(); ASSERT_NE(first_wire, nullptr);
    ASSERT_TRUE(await_condition([&] { return first_wire->state() == lattice::transport_state::open; }));
    // Preserve the old same-dial no-kick behavior even with a distinct explicit peer.
    {
        auto distinct = original;
        distinct.websocket_sync_identity = "ws://fixture.invalid/distinct-logical-peer";
        lattice::lattice_db same_dial(distinct);
        ASSERT_TRUE(first->is_sync_agent());
        EXPECT_FALSE(same_dial.is_sync_agent());
        ASSERT_EQ(network->count(), 1u);
        EXPECT_EQ(first_wire->state(), lattice::transport_state::open);
    }
    auto updated = fixture.config(false);
    updated.websocket_url = "ws://fixture.invalid/sync?schema=6";
    updated.websocket_sync_identity = original.websocket_url;
    // Direct reservation checks the actual setup key independently of native flock admission.
    const bool duplicate_key = lattice::background_sync_lifecycle_test_access::reserve_identity_key(updated);
    if (duplicate_key) lattice::background_sync_lifecycle_test_access::release_identity_key(updated);
    EXPECT_FALSE(duplicate_key);
    lattice::lattice_db dormant(updated);
    ASSERT_TRUE(first->is_sync_agent());
    EXPECT_FALSE(dormant.is_sync_agent());
    ASSERT_EQ(network->count(), 1u);
    EXPECT_EQ(first_wire->state(), lattice::transport_state::open);
    first.reset();
    dormant.connect_sync();
    ASSERT_TRUE(dormant.is_sync_agent());
    auto* next_wire = network->latest(); ASSERT_NE(next_wire, nullptr);
    ASSERT_TRUE(await_condition([&] { return next_wire->state() == lattice::transport_state::open; }));
    EXPECT_EQ(next_wire->target().find(updated.websocket_url), 0u);
    EXPECT_EQ(network->count(), 2u);
    EXPECT_TRUE(dormant.db().query("SELECT 1 FROM _lattice_replication_slots WHERE sync_id=?",
        {"wss:" + updated.websocket_url}).empty());
}

TEST(ManagedBackgroundSync, InvalidIdentityIsRejectedBeforeOpenAndExactBoundaryIsAccepted) {
    managed_fixture fixture;
    auto cfg = fixture.config();
    ASSERT_FALSE(std::filesystem::exists(fixture.file.path));
    cfg.websocket_sync_identity = std::string(4093, 'x');
    EXPECT_THROW({ lattice::lattice_db invalid(cfg); }, lattice::db_error);
    EXPECT_FALSE(std::filesystem::exists(fixture.file.path));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + "-wal"));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + "-shm"));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + ".sync.lock"));
    cfg.websocket_sync_identity = std::string("old\0peer", 8);
    EXPECT_THROW({ lattice::lattice_db invalid(cfg); }, lattice::db_error);
    EXPECT_FALSE(std::filesystem::exists(fixture.file.path));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + "-wal"));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + "-shm"));
    EXPECT_FALSE(std::filesystem::exists(fixture.file.str() + ".sync.lock"));
    cfg.websocket_sync_identity = std::string(4092, 'x');
    lattice::lattice_db accepted(cfg);
    EXPECT_EQ(cfg.effective_websocket_sync_identity(), cfg.websocket_sync_identity);
    EXPECT_TRUE(std::filesystem::exists(fixture.file.path));
    EXPECT_FALSE(accepted.is_sync_agent());
    EXPECT_NO_THROW(accepted.add(TestPerson{"identity boundary", 1, std::nullopt}));
}
#endif
