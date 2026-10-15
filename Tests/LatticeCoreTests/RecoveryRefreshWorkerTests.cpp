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
