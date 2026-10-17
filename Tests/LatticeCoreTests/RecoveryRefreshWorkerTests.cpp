#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_witness.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <deque>
#include <chrono>

namespace {
using namespace lattice::detail;
class refresh_delivery_queue final : public lattice::scheduler {
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::function<void()>> queue_;
    bool accepting_ = true;
    std::thread::id submitted_from_;
public:
    void invoke(std::function<void()>&& fn) override {
        { std::lock_guard<std::mutex> lock(mutex_);
          if (!accepting_) return;
          submitted_from_ = std::this_thread::get_id(); queue_.push_back(std::move(fn)); }
        ready_.notify_one();
    }
    bool is_on_thread() const noexcept override { return false; }
    bool is_same_as(const scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override { return true; }
    std::thread::id submitted_from() {
        std::lock_guard<std::mutex> lock(mutex_); return submitted_from_;
    }
    size_t count() { std::lock_guard<std::mutex> lock(mutex_); return queue_.size(); }
    bool wait_ready() {
        std::unique_lock<std::mutex> lock(mutex_);
        return ready_.wait_for(lock, std::chrono::seconds(10), [&] { return !queue_.empty(); });
    }
    std::function<void()> take_ready() {
        if (!wait_ready()) return {};
        std::function<void()> fn;
        { std::lock_guard<std::mutex> lock(mutex_); fn.swap(queue_.front()); queue_.pop_front(); }
        return fn;
    }
    bool run_one() {
        auto fn = take_ready();
        if (!fn) return false;
        fn(); return true;
    }
    void discard() {
        std::deque<std::function<void()>> retired;
        { std::lock_guard<std::mutex> lock(mutex_); retired.swap(queue_); }
    }
    void shutdown() override {
        { std::lock_guard<std::mutex> lock(mutex_); accepting_ = false; }
        discard();
    }
};
class RecoveryRefreshWorker : public ::testing::Test {
protected:
    TempDB path{"recovery-refresh-worker"};
    std::shared_ptr<refresh_delivery_queue> scheduler = std::make_shared<refresh_delivery_queue>();
    std::shared_ptr<lattice::lattice_db> owner;
    void SetUp() override {
        lattice::configuration c(path.str()); c.sched = scheduler; c.audit_retention_seconds = 0;
        owner = std::make_shared<lattice::lattice_db>(c);
        owner->add(TestPerson{"before", 1, std::nullopt});
        owner->db().execute("DELETE FROM AuditLog");
        auto result = recovery_writer_access::install(owner, [&](auto&) { bump_recovery_witness(*owner); });
        ASSERT_EQ(result.state, recovery_install_state::committed);
        if (auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path.str())) notifier->stop_listening();
        scheduler->discard();
    }
    void TearDown() override {
        if (owner) owner->close();
        scheduler->shutdown(); owner.reset();
    }
    void external_commit(const std::string& name) {
        lattice::database raw(path.str());
        ASSERT_EQ(sqlite3_create_function_v2(raw.handle(), "sync_disabled", 0, SQLITE_UTF8, nullptr,
            [](sqlite3_context* c, int, sqlite3_value**) { sqlite3_result_int(c, 1); }, nullptr, nullptr, nullptr), SQLITE_OK);
        raw.execute("BEGIN IMMEDIATE");
        raw.execute("UPDATE TestPerson SET name=?", {name});
        raw.execute("UPDATE _lattice_recovery_witness SET generation=generation+1 WHERE id=1");
        raw.execute("COMMIT");
    }
    std::string name() {
        return std::get<std::string>(owner->read_db().query("SELECT name FROM TestPerson").at(0).at("name"));
    }
};
}

TEST_F(RecoveryRefreshWorker, PreparationPublishesOffOwnerSchedulerBeforeCallbackDelivery) {
    const auto origin = std::this_thread::get_id();
    auto old_reader = owner->borrow_read_connection();
    int calls = 0;
    const auto token = owner->add_recovery_refresh_observer([&] {
        EXPECT_EQ(std::this_thread::get_id(), origin); ++calls; EXPECT_EQ(name(), "before");
    });
    ASSERT_TRUE(scheduler->wait_ready());
    EXPECT_NE(scheduler->submitted_from(), origin);
    EXPECT_NE(owner->borrow_read_connection(), old_reader);
    EXPECT_EQ(calls, 0); // No owner-scheduler work has run yet.
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 1);
    owner->remove_recovery_refresh_observer(token);
}

TEST_F(RecoveryRefreshWorker, PeriodicPollRepairsTheLostFinalHintWithoutAuditHistory) {
    int calls = 0; std::string observed;
    const auto token = owner->add_recovery_refresh_observer([&] { ++calls; observed = name(); });
    ASSERT_TRUE(scheduler->run_one()); ASSERT_EQ(calls, 1);
    external_commit("missed-last-commit");
    // No Core writer, instance registry fanout, IPC listener or explicit request.
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 2); EXPECT_EQ(observed, "missed-last-commit");
    EXPECT_TRUE(owner->db().query("SELECT * FROM AuditLog").empty());
    owner->remove_recovery_refresh_observer(token);
}

TEST_F(RecoveryRefreshWorker, DiscardedDeliveryRemainsPendingForPeriodicRetry) {
    int calls = 0;
    const auto token = owner->add_recovery_refresh_observer([&] { ++calls; });
    ASSERT_TRUE(scheduler->wait_ready()); scheduler->discard();
    EXPECT_EQ(calls, 0);
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 1);
    owner->remove_recovery_refresh_observer(token);
}

TEST_F(RecoveryRefreshWorker, CallbackFailureRemainsPendingForPeriodicRetry) {
    int calls = 0;
    const auto token = owner->add_recovery_refresh_observer([&] {
        ++calls; if (calls == 1) throw std::runtime_error("failed refresh consumer");
    });
    ASSERT_TRUE(scheduler->run_one()); ASSERT_EQ(calls, 1);
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 2);
    owner->remove_recovery_refresh_observer(token);
}

TEST_F(RecoveryRefreshWorker, CloseFencesQueuedCallbacksAndAllowsAnIndependentSuccessor) {
    int calls = 0;
    owner->add_recovery_refresh_observer([&] { ++calls; });
    // close() shuts down this scheduler and discards its queue. Retain the
    // actual queued closure separately to exercise its own owner guard.
    auto stale = scheduler->take_ready(); ASSERT_TRUE(stale);
    owner->close(); owner.reset();
    stale(); EXPECT_EQ(calls, 0); stale = {};
    scheduler = std::make_shared<refresh_delivery_queue>();
    lattice::configuration c(path.str()); c.sched = scheduler; c.audit_retention_seconds = 0;
    owner = std::make_shared<lattice::lattice_db>(c);
    const auto token = owner->add_recovery_refresh_observer([&] { ++calls; });
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 1);
    owner->remove_recovery_refresh_observer(token);
}

TEST_F(RecoveryRefreshWorker, TypedOnlyRegistrationRepairsMissedCommitAndRefreshesHeldFields) {
    auto held = owner->objects<TestPerson>()[0];
    int calls = 0; std::string observed;
    auto token = held.observe([&](auto& change) {
        ++calls; observed = held.name.detach();
        EXPECT_FALSE(change.is_deleted); EXPECT_EQ(change.object, &held);
        std::set<std::string> fields;
        for (const auto& field : change.property_changes) fields.insert(field.name);
        EXPECT_TRUE(fields.count("name")); EXPECT_TRUE(fields.count("age"));
    });
    // No payload-free listener or legacy object observer activates this owner.
    ASSERT_TRUE(scheduler->run_one()); ASSERT_EQ(calls, 1);
    external_commit("typed-missed-last");
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 2); EXPECT_EQ(observed, "typed-missed-last");
    EXPECT_TRUE(owner->db().query("SELECT * FROM AuditLog").empty());
    token.invalidate();
}

TEST_F(RecoveryRefreshWorker, TypedCancellationFencesQueuedRefreshAndResubscriptionGetsCurrentWitness) {
    auto held = owner->objects<TestPerson>()[0]; int calls = 0;
    auto token = held.observe([&](auto&) { ++calls; });
    ASSERT_TRUE(scheduler->wait_ready()); token.invalidate();
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 0);
    auto successor = held.observe([&](auto&) { ++calls; EXPECT_EQ(held.name.detach(), "before"); });
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 1);
    successor.invalidate();
}

TEST_F(RecoveryRefreshWorker, RepeatedHintsKeepOneQueuedDeliveryAndDoNotLoseLaterWork) {
    int calls = 0;
    const auto token = owner->add_recovery_refresh_observer([&] { ++calls; });
    ASSERT_TRUE(scheduler->wait_ready());
    for (int i = 0; i != 1000; ++i) owner->request_recovery_refresh();
    EXPECT_EQ(scheduler->count(), 1u);
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 1);
    external_commit("after-hints");
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(calls, 2); EXPECT_EQ(name(), "after-hints");
    owner->remove_recovery_refresh_observer(token);
}

TEST(ManagedRecoveryInterest, WeakOffLockHookTracksRegistrationDeletionAndRetirement) {
    managed_observation_state state;
    std::vector<std::pair<uint64_t, bool>> seen;
    auto hook = std::make_shared<const managed_observation_state::interest_callback>(
        [&](uint64_t revision, bool active) {
            seen.emplace_back(revision, active);
            // Reentrant state access proves the hook runs outside its leaf lock.
            EXPECT_EQ(!state.observed_tables().empty(), active);
        });
    state.set_interest_observer(hook);
    auto token = state.observe("TestPerson", 1, "id", {"name"}, [](bool, const auto&) {});
    EXPECT_EQ(state.observed_tables(), (std::set<std::string>{"TestPerson"}));
    std::vector<std::function<void()>> work;
    state.append("TestPerson", "DELETE", 1, "id", "", work);
    ASSERT_EQ(work.size(), 1u);
    EXPECT_FALSE(state.observed_tables().empty()); // queued deletion still owns interest
    work.front()();
    auto second = state.observe("TestPerson", 2, "second", {"name"}, [](bool, const auto&) {});
    state.retire();
    EXPECT_EQ(seen, (std::vector<std::pair<uint64_t, bool>>{{0,false},{1,true},{2,false},{3,true},{4,false}}));
    hook.reset(); token.invalidate(); second.invalidate(); // weak hook and owner-free cancellation
}


// Automatic storage maintenance is independent of witness subscriptions and
// owner-scheduler delivery. These tests exercise actual worker/SQLite returns.
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_refresh.hpp"
#include <lattice/owned_catalog.hpp>
#include <filesystem>

namespace {
using namespace std::chrono_literals;
template<class Predicate> bool automatic_wait(Predicate predicate) {
    const auto end = std::chrono::steady_clock::now() + 10s;
    do {
        if (predicate()) return true;
        std::this_thread::sleep_for(5ms);
    } while (std::chrono::steady_clock::now() < end);
    return predicate();
}
std::shared_ptr<void> automatic_pause(lattice::lattice_db& owner) {
    std::shared_ptr<void> token;
    const bool paused = automatic_wait([&] {
        token = recovery_refresh_test_access::pause_automatic_maintenance(owner);
        return bool(token); // Retry ONLY actual running, not an exception.
    });
    if (!paused) throw std::runtime_error("actual automatic turn did not leave pause admission");
    return token;
}
uint64_t actual_pressure(lattice::lattice_db& owner) {
    // The real default 16MiB hook threshold is unchanged. An actual pinned
    // keeper prevents the SQLite autocheckpoint from eliminating this WAL.
    const auto generation = owner.acquire_read_generation();
    if (!generation) throw std::runtime_error("actual keeper unavailable");
    for (int n = 0; n != 4; ++n)
        owner.db().execute("UPDATE TestPerson SET name=?", {std::string(5 * 1024 * 1024, char('a' + n))});
    if (!owner.wal_eviction_pending()) throw std::runtime_error("real writes did not raise pressure");
    return generation;
}
struct automatic_receiver {
    int schema = 0, rows = 0; size_t maximum = 0;
    static bool accept(void* raw, int32_t kind, const uint8_t*, int64_t count) {
        auto& self = *static_cast<automatic_receiver*>(raw);
        if (count < 0 || count > 65536) return false;
        self.maximum = std::max(self.maximum, size_t(count));
        if (kind == 0) ++self.schema;
        if (kind == 1) ++self.rows;
        return true;
    }
};
auto automatic_capture(const std::shared_ptr<lattice::lattice_db>& owner, automatic_receiver& receiver) {
    return lattice::make_catalog_stream_operation(owner,
        R"({"version":1,"tables":["TestPerson"]})", 30000000000LL).capture(&receiver, automatic_receiver::accept);
}
struct actual_native_hold {
    std::mutex mutex; std::condition_variable changed;
    bool entered = false, release = false;
    std::thread thread;
    explicit actual_native_hold(sqlite3* original) : thread([&, original] {
        auto* native = sqlite3_db_mutex(original); sqlite3_mutex_enter(native);
        { std::lock_guard<std::mutex> lock(mutex); entered = true; changed.notify_all(); }
        { std::unique_lock<std::mutex> lock(mutex); changed.wait(lock, [&] { return release; }); }
        sqlite3_mutex_leave(native);
    }) {}
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, 10s, [&] { return entered; });
    }
    void open() { { std::lock_guard<std::mutex> lock(mutex); release = true; } changed.notify_all(); }
    ~actual_native_hold() { open(); if (thread.joinable()) thread.join(); }
};
}

TEST_F(RecoveryRefreshWorker, AutomaticQuietEnrollmentIsIdempotentWithoutSqlOrReaderMint) {
    const auto reader = owner->borrow_read_connection(), xproc = owner->borrow_xproc_read_connection();
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    ASSERT_TRUE(automatic_wait([&] { return recovery_refresh_test_access::automatic_snapshot(*owner).quiet_returns > 0; }));
    const auto before = recovery_refresh_test_access::automatic_snapshot(*owner);
    const auto statements = lattice::database::total_statement_count();
    for (int n = 0; n != 8; ++n) EXPECT_TRUE(owner->enable_automatic_read_pool_maintenance());
    ASSERT_TRUE(automatic_wait([&] { return recovery_refresh_test_access::automatic_snapshot(*owner).quiet_returns >= before.quiet_returns + 2; }));
    EXPECT_EQ(lattice::database::total_statement_count(), statements);
    EXPECT_EQ(owner->borrow_read_connection(), reader); EXPECT_EQ(owner->borrow_xproc_read_connection(), xproc);
    EXPECT_EQ(owner->local_read_generations_outstanding(), 0u);
    const auto after = recovery_refresh_test_access::automatic_snapshot(*owner);
    EXPECT_TRUE(after.enabled); EXPECT_EQ(after.completed, 0u); EXPECT_EQ(after.deferred, 0u);
}

TEST_F(RecoveryRefreshWorker, AutomaticMemoryAndReadonlyAreIneligibleAndClosedRefuses) {
    lattice::lattice_db memory{lattice::configuration(":memory:")};
    EXPECT_FALSE(memory.enable_automatic_read_pool_maintenance());
    lattice::configuration readonly(path.str()); readonly.read_only = true;
    lattice::lattice_db ro(readonly);
    EXPECT_FALSE(ro.enable_automatic_read_pool_maintenance());
    EXPECT_FALSE(recovery_refresh_test_access::automatic_snapshot(ro).enabled);
    owner->close();
    EXPECT_THROW(owner->enable_automatic_read_pool_maintenance(), lattice::db_error);
}

TEST_F(RecoveryRefreshWorker, AutomaticFreshEnrollmentFailurePreservesQueuedWitnessAndCanRetry) {
    int callbacks = 0;
    const auto observer = owner->add_recovery_refresh_observer([&] { ++callbacks; });
    ASSERT_TRUE(scheduler->wait_ready());
    auto fault = recovery_refresh_test_access::fail_next_automatic_enrollment();
    EXPECT_THROW(owner->enable_automatic_read_pool_maintenance(), std::bad_alloc);
    EXPECT_FALSE(recovery_refresh_test_access::automatic_snapshot(*owner).enabled);
    ASSERT_TRUE(scheduler->run_one()); EXPECT_EQ(callbacks, 1);
    EXPECT_TRUE(owner->enable_automatic_read_pool_maintenance());
    EXPECT_TRUE(owner->enable_automatic_read_pool_maintenance());
    owner->remove_recovery_refresh_observer(observer);
}

TEST_F(RecoveryRefreshWorker, AutomaticPausePreservesRealPressureThenActualWorkerMakesReaderGap) {
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause = automatic_pause(*owner);
    const auto generation = actual_pressure(*owner);
    automatic_receiver refused;
    const auto first = automatic_capture(owner, refused);
    EXPECT_FALSE(first.success()); EXPECT_FALSE(first.cleanup_failed()); EXPECT_EQ(refused.schema, 0);
    const auto paused = recovery_refresh_test_access::automatic_snapshot(*owner);
    std::this_thread::sleep_for(1100ms);
    EXPECT_TRUE(owner->wal_eviction_pending());
    EXPECT_EQ(recovery_refresh_test_access::automatic_snapshot(*owner).attempts, paused.attempts);
    pause.reset();
    ASSERT_TRUE(automatic_wait([&] { return !owner->wal_eviction_pending(); }));
    EXPECT_GT(recovery_refresh_test_access::automatic_snapshot(*owner).completed, 0u);
    EXPECT_EQ(owner->local_read_generations_outstanding(), 0u);
    EXPECT_FALSE(owner->query_at_generation(generation, "SELECT 1"));
    std::error_code error;
    EXPECT_EQ(std::filesystem::file_size(path.str() + "-wal", error), 0u); EXPECT_FALSE(error);
    automatic_receiver accepted;
    const auto result = automatic_capture(owner, accepted);
    EXPECT_TRUE(result.success()) << result.error_message(); EXPECT_FALSE(result.cleanup_failed());
    EXPECT_EQ(accepted.schema, 1); EXPECT_EQ(accepted.rows, 1); EXPECT_LE(accepted.maximum, 65536u);
}

TEST_F(RecoveryRefreshWorker, AutomaticTurnIsIndependentOfAnActuallyQueuedWitnessDelivery) {
    int callbacks = 0;
    const auto observer = owner->add_recovery_refresh_observer([&] { ++callbacks; });
    ASSERT_TRUE(scheduler->wait_ready()); // Retains the actual witness in_flight ticket.
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause = automatic_pause(*owner); actual_pressure(*owner); pause.reset();
    ASSERT_TRUE(automatic_wait([&] { return !owner->wal_eviction_pending(); }));
    EXPECT_EQ(callbacks, 0); EXPECT_EQ(scheduler->count(), 1u);
    EXPECT_GT(recovery_refresh_test_access::automatic_snapshot(*owner).completed, 0u);
    owner->remove_recovery_refresh_observer(observer);
}

TEST_F(RecoveryRefreshWorker, AutomaticHeldNativeOwnerDefersWhileOtherOwnerMaintainsAndCloses) {
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause_a = automatic_pause(*owner); actual_pressure(*owner);
    TempDB other_path("automatic-maintenance-independent");
    lattice::configuration config(other_path.str()); config.sched = std::make_shared<refresh_delivery_queue>();
    auto other = std::make_shared<lattice::lattice_db>(config);
    other->add(TestPerson{"other", 1, std::nullopt});
    ASSERT_TRUE(other->enable_automatic_read_pool_maintenance());
    auto pause_b = automatic_pause(*other); actual_pressure(*other);
    auto* native = canonical_writer_custody_test_access::fault_handle(owner->db()); ASSERT_NE(native, nullptr);
    actual_native_hold held(native);
    const bool entered = held.wait(); EXPECT_TRUE(entered);
    const auto deferred = recovery_refresh_test_access::automatic_snapshot(*owner).deferred;
    pause_a.reset();
    const bool actually_deferred = entered && automatic_wait([&] {
        return recovery_refresh_test_access::automatic_snapshot(*owner).deferred > deferred;
    });
    EXPECT_TRUE(actually_deferred);
    pause_b.reset();
    const bool other_maintained = actually_deferred && automatic_wait([&] { return !other->wal_eviction_pending(); });
    EXPECT_TRUE(other_maintained);
    std::atomic<bool> closed{false}; std::exception_ptr close_error;
    std::thread closer([&] { try { other->close(); closed.store(true); } catch (...) { close_error = std::current_exception(); } });
    const bool other_closed_before_holder_release = automatic_wait([&] { return closed.load(); });
    EXPECT_TRUE(other_closed_before_holder_release);
    EXPECT_TRUE(owner->wal_eviction_pending()); // No acknowledgement of the busy original.
    held.open(); closer.join();
    if (close_error) std::rethrow_exception(close_error);
}

TEST_F(RecoveryRefreshWorker, AutomaticFailedCommitRetainsActualKeeperUntilGenuineEnding) {
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause = automatic_pause(*owner);
    const auto generation = owner->acquire_read_generation(); ASSERT_NE(generation, 0u);
    auto keeper = recovery_refresh_test_access::actual_read_keeper(*owner, generation); ASSERT_TRUE(keeper);
    auto* native = canonical_writer_custody_test_access::fault_handle(*keeper); ASSERT_NE(native, nullptr);
    ASSERT_EQ(sqlite3_set_authorizer(native, [](void*, int action, const char* first, const char*, const char*, const char*) {
        return action == SQLITE_TRANSACTION && first && std::string_view(first) == "COMMIT" ? SQLITE_DENY : SQLITE_OK;
    }, nullptr), SQLITE_OK);
    struct restore_authorizer { sqlite3* h; ~restore_authorizer() { sqlite3_set_authorizer(h, nullptr, nullptr); } } restore{native};
    owner->set_read_generation_ttl_ms(1);
    const auto before = recovery_refresh_test_access::automatic_snapshot(*owner).deferred;
    pause.reset();
    ASSERT_TRUE(automatic_wait([&] { return recovery_refresh_test_access::automatic_snapshot(*owner).deferred > before; }));
    pause = automatic_pause(*owner);
    EXPECT_EQ(owner->local_read_generations_outstanding(), 1u);
    EXPECT_EQ(sqlite3_get_autocommit(native), 0);
    EXPECT_EQ(recovery_refresh_test_access::actual_read_keeper(*owner, generation), keeper);
    EXPECT_FALSE(owner->query_at_generation(generation, "SELECT 1"));
    ASSERT_EQ(sqlite3_set_authorizer(native, nullptr, nullptr), SQLITE_OK);
    pause.reset();
    ASSERT_TRUE(automatic_wait([&] { return owner->local_read_generations_outstanding() == 0; }));
    EXPECT_EQ(sqlite3_get_autocommit(native), 1);
}

TEST_F(RecoveryRefreshWorker, AutomaticCloseFencesPausedInterestWithoutAWorkerJoinCycle) {
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause = automatic_pause(*owner);
    owner->close();
    EXPECT_TRUE(owner->is_closed());
    EXPECT_THROW(owner->enable_automatic_read_pool_maintenance(), lattice::db_error);
    EXPECT_THROW(recovery_refresh_test_access::pause_automatic_maintenance(*owner), lattice::db_error);
    pause.reset(); // Only releases the actual state fence; never touches owner SQL.
}


TEST_F(RecoveryRefreshWorker, AutomaticCloseJoinsAnActuallyAdmittedKeeperCommitBeforeReturning) {
    ASSERT_TRUE(owner->enable_automatic_read_pool_maintenance());
    auto pause = automatic_pause(*owner);
    const auto generation = owner->acquire_read_generation(); ASSERT_NE(generation, 0u);
    auto keeper = recovery_refresh_test_access::actual_read_keeper(*owner, generation); ASSERT_TRUE(keeper);
    auto* native = canonical_writer_custody_test_access::fault_handle(*keeper); ASSERT_NE(native, nullptr);
    struct commit_gate {
        sqlite3* native; std::mutex mutex; std::condition_variable changed;
        bool entered = false, released = false;
        static int authorize(void* context, int action, const char* first, const char*, const char*, const char*) {
            if (action != SQLITE_TRANSACTION || !first || std::string_view(first) != "COMMIT") return SQLITE_OK;
            auto& self = *static_cast<commit_gate*>(context);
            std::unique_lock<std::mutex> lock(self.mutex);
            self.entered = true; self.changed.notify_all();
            self.changed.wait(lock, [&] { return self.released; });
            return SQLITE_OK;
        }
        bool wait() { std::unique_lock<std::mutex> lock(mutex); return changed.wait_for(lock, 10s, [&] { return entered; }); }
        void open() { { std::lock_guard<std::mutex> lock(mutex); released = true; } changed.notify_all(); }
        ~commit_gate() { open(); sqlite3_set_authorizer(native, nullptr, nullptr); }
    } gate{native};
    ASSERT_EQ(sqlite3_set_authorizer(native, commit_gate::authorize, &gate), SQLITE_OK);
    owner->set_read_generation_ttl_ms(1); pause.reset();
    ASSERT_TRUE(gate.wait()); // Actual worker is inside COMMIT's authorizer with Native held.
    std::atomic<bool> ended{false}; std::exception_ptr close_error;
    std::thread closer([&] { try { owner->close(); ended.store(true); } catch (...) { close_error = std::current_exception(); } });
    const bool logical_close_seen = automatic_wait([&] { return owner->is_closed(); });
    EXPECT_TRUE(logical_close_seen);
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(ended.load()); // The admitted actual owner hold still requires join.
    gate.open(); closer.join(); // Release and join on every post-launch path.
    if (close_error) std::rethrow_exception(close_error);
    EXPECT_TRUE(ended.load()); EXPECT_TRUE(owner->is_closed());
    EXPECT_EQ(sqlite3_get_autocommit(native), 1);
}

TEST_F(RecoveryRefreshWorker, RegistryTlsPublicationExceptionReturnsBothActualAdmissionHolds) {
    for (const bool creator_bound : {false, true}) {
        const auto actual = recovery_refresh_test_access::registry_depth_failure(*owner, creator_bound);
        EXPECT_TRUE(actual.caught_bad_alloc); EXPECT_TRUE(actual.depth_publication_reached); EXPECT_FALSE(actual.callback_entered);
        EXPECT_EQ(actual.holds_before, 0u); EXPECT_EQ(actual.holds_after, actual.holds_before);
    }
    owner->close(); EXPECT_TRUE(owner->is_closed());
}
