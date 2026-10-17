#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <lattice/sync.hpp>
#include <deque>
#include <set>

namespace {
using lattice::detail::recovery_writer_access;
using lattice::detail::recovery_install_state;

// Intentionally keeps queued functions through shutdown. This witnesses the
// observation admission fence independently of a scheduler's drain policy.
class HeldObserverScheduler final : public lattice::scheduler {
public:
    void invoke(std::function<void()>&& fn) override {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push_back(std::move(fn));
    }
    bool is_on_thread() const noexcept override { return true; }
    bool is_same_as(const scheduler* other) const noexcept override { return other == this; }
    bool can_invoke() const noexcept override { return true; }
    void drain() {
        for (;;) {
            std::function<void()> fn;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (queue.empty()) return;
                fn = std::move(queue.front()); queue.pop_front();
            }
            fn();
        }
    }
private:
    std::mutex mutex;
    std::deque<std::function<void()>> queue;
};

std::shared_ptr<lattice::lattice_db> observer_owner(const std::string& path = ":memory:",
                                                  std::shared_ptr<lattice::scheduler> scheduler = {}) {
    lattice::configuration config(path);
    config.sched = std::move(scheduler); config.audit_retention_seconds = 0;
    auto owner = std::make_shared<lattice::lattice_db>(config);
    // Same-process file fanout is under test, not background notifier timing.
    if (!config.is_in_memory()) {
        auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path);
        notifier->stop_listening();
    }
    return owner;
}

std::set<std::string> fields(const lattice::object_change<TestPerson>& change) {
    std::set<std::string> result;
    for (const auto& field : change.property_changes) result.insert(field.name);
    return result;
}

void recovery_name(const std::shared_ptr<lattice::lattice_db>& owner, int64_t id,
                   const std::string& name) {
    auto result = recovery_writer_access::install(owner, [&](auto& writer) {
        writer.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        writer.execute("UPDATE main.TestPerson SET name=? WHERE id=?", {name, id});
        writer.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    });
    std::string primary;
    if(result.primary_error)try{std::rethrow_exception(result.primary_error);}
    catch(const std::exception& e){primary=e.what();}catch(...){primary="non-standard primary error";}
    EXPECT_EQ(result.state, recovery_install_state::committed)<<primary;
    EXPECT_FALSE(result.primary_error); EXPECT_FALSE(result.postcommit_error);
}
}

TEST(ManagedObservation, OrdinarySettersCommitOnceAndRollbackDoesNotNotify) {
    for (bool file : {false, true}) {
        TempDB path("typed-observer-setters");
        auto owner = observer_owner(file ? path.str() : ":memory:");
        auto person = owner->add(TestPerson{"before", 1, std::nullopt});
        int calls = 0;
        auto token = person.observe([&](auto& change) {
            ++calls; EXPECT_FALSE(change.is_deleted);
            EXPECT_EQ(change.object, &person); EXPECT_TRUE(fields(change).count("name"));
        });
        person.set_value("name", std::string("type-erased")); EXPECT_EQ(calls, 1);
        person.name = std::string("scalar"); EXPECT_EQ(calls, 2);
        owner->db().begin_transaction();
        person.name = std::string("rolled back"); EXPECT_EQ(calls, 2);
        owner->db().rollback(); EXPECT_EQ(calls, 2);
        EXPECT_EQ(person.name.detach(), "scalar");
        owner->db().begin_transaction();
        person.name = std::string("committed"); EXPECT_EQ(calls, 2);
        owner->db().commit(); EXPECT_EQ(calls, 3);
    }
}

TEST(ManagedObservation, SameUuidAndRowIdInDifferentStoresStayIsolated) {
    for (bool file : {false, true}) {
        TempDB left_path("typed-left"), right_path("typed-right");
        auto left = observer_owner(file ? left_path.str() : ":memory:");
        auto right = observer_owner(file ? right_path.str() : ":memory:");
        const std::string gid = "00000000-0000-4000-8000-000000009201";
        for (auto* owner : {left.get(), right.get()})
            owner->db().execute("INSERT INTO TestPerson(id,globalId,name,age) VALUES(1,?,'before',1)", {gid});
        auto a = left->objects<TestPerson>()[0]; auto b = right->objects<TestPerson>()[0];
        int first = 0, second = 0;
        auto ta = a.observe([&](auto&) { ++first; });
        auto tb = b.observe([&](auto&) { ++second; });
        recovery_name(left, a.id(), "left"); EXPECT_EQ(first, 1); EXPECT_EQ(second, 0);
        b.name = std::string("right"); EXPECT_EQ(first, 1); EXPECT_EQ(second, 1);
    }
}

TEST(ManagedObservation, SameFileSiblingRecoveryReachesHeldObjectWithFinalValues) {
    TempDB path("typed-sibling"); auto reader = observer_owner(path.str());
    auto held = reader->add(TestPerson{"before", 1, std::nullopt});
    auto writer = observer_owner(path.str()); int calls = 0;
    auto token = held.observe([&](auto& change) {
        ++calls; EXPECT_EQ(held.name.detach(), "remote");
        EXPECT_TRUE(fields(change).count("name")); EXPECT_TRUE(fields(change).count("age"));
    });
    recovery_name(writer, held.id(), "remote"); EXPECT_EQ(calls, 1);
}

TEST(ManagedObservation, RemoteApplyUsesPublicTypedObserverAndExactFields) {
    for (bool file : {false, true}) {
        TempDB path("typed-remote"); auto owner = observer_owner(file ? path.str() : ":memory:");
        auto held = owner->add(TestPerson{"before", 1, std::nullopt});
        int calls = 0;
        auto token = held.observe([&](auto& change) {
            ++calls; EXPECT_EQ(held.name.detach(), "remote");
            EXPECT_EQ(fields(change), (std::set<std::string>{"name"}));
        });
        lattice::audit_log_entry entry;
        entry.global_id = "00000000-0000-4000-8000-000000009202";
        entry.table_name = "TestPerson"; entry.operation = "UPDATE";
        entry.global_row_id = held.global_id(); entry.changed_fields_names = {"name"};
        entry.changed_fields = {{"name", lattice::any_property(std::string("remote"))}};
        entry.timestamp = "1789819200.0";
        EXPECT_EQ(lattice::apply_remote_changes(*owner, {entry}), (std::vector<std::string>{entry.global_id}));
        EXPECT_EQ(calls, 1);
    }
}

TEST(ManagedObservation, CommittedDeleteRetiresRegistrationBeforeRowIdReuse) {
    auto owner = observer_owner(); auto held = owner->add(TestPerson{"before", 1, std::nullopt});
    const auto id = held.id(); int calls = 0;
    auto token = held.observe([&](auto& change) { ++calls; EXPECT_TRUE(change.is_deleted); EXPECT_TRUE(change.property_changes.empty()); });
    owner->remove(held); EXPECT_EQ(calls, 1);
    owner->db().execute("INSERT INTO TestPerson(id,globalId,name,age) VALUES(?,'different','new',2)", {id});
    owner->db().execute("UPDATE TestPerson SET name='newer' WHERE id=?", {id}); EXPECT_EQ(calls, 1);
}

TEST(ManagedObservation, DeleteInsideOuterTransactionWaitsForCommit) {
    auto owner = observer_owner(); auto held = owner->add(TestPerson{"before", 1, std::nullopt});
    int calls = 0; auto token = held.observe([&](auto& c) { ++calls; EXPECT_TRUE(c.is_deleted); });
    owner->db().begin_transaction(); owner->remove(held); EXPECT_EQ(calls, 0);
    owner->db().commit(); EXPECT_EQ(calls, 1);
}

TEST(ManagedObservation, QueuedUpdateThenDeleteRemainOrderedAndCancellationFencesBoth) {
    auto queue = std::make_shared<HeldObserverScheduler>(); auto owner = observer_owner(":memory:", queue);
    auto held = owner->add(TestPerson{"before", 1, std::nullopt}); queue->drain();
    std::vector<bool> events; auto token = held.observe([&](auto& c) { events.push_back(c.is_deleted); });
    held.name = std::string("next"); owner->remove(held); EXPECT_TRUE(events.empty());
    queue->drain(); EXPECT_EQ(events, (std::vector<bool>{false, true}));
    auto other = owner->add(TestPerson{"other", 2, std::nullopt}); queue->drain();
    int cancelled = 0; auto second = other.observe([&](auto&) { ++cancelled; });
    other.name = std::string("queued"); owner->remove(other); second.invalidate();
    queue->drain(); EXPECT_EQ(cancelled, 0);
}

TEST(ManagedObservation, SelfInvalidationAndCaptureDestructionRunOutsideObserverLocks) {
    auto owner = observer_owner(); auto held = owner->add(TestPerson{"before", 1, std::nullopt});
    int calls = 0; lattice::notification_token token;
    token = held.observe([&](auto&) { ++calls; token.invalidate(); });
    held.name = std::string("one"); held.name = std::string("two"); EXPECT_EQ(calls, 1);
    struct CancelOther { std::function<void()> on_destroy; ~CancelOther() { on_destroy(); } };
    auto second = held.observe([](auto&) {});
    auto capture = std::make_shared<CancelOther>(); capture->on_destroy = [&] { second.invalidate(); };
    auto third = held.observe([capture](auto&) {}); capture.reset();
    third.invalidate(); EXPECT_FALSE(second.is_valid());
}

TEST(ManagedObservation, TokenCanOutliveOwnerAndQueuedWorkDoesNotDereferenceWrapper) {
    auto queue = std::make_shared<HeldObserverScheduler>(); lattice::notification_token token;
    int calls = 0;
    {
        auto owner = observer_owner(":memory:", queue);
        auto held = owner->add(TestPerson{"before", 1, std::nullopt}); queue->drain();
        token = held.observe([&](auto&) { ++calls; }); held.name = std::string("queued");
        owner.reset();
    }
    queue->drain(); EXPECT_EQ(calls, 0); token.invalidate();
}

TEST(ManagedObservation, CloseFencesQueuedAndNewAdmission) {
    auto queue = std::make_shared<HeldObserverScheduler>(); auto owner = observer_owner(":memory:", queue);
    auto held = owner->add(TestPerson{"before", 1, std::nullopt}); queue->drain();
    int calls = 0; auto token = held.observe([&](auto&) { ++calls; });
    auto deleted = owner->add(TestPerson{"delete-queued", 2, std::nullopt}); queue->drain();
    auto capture = std::make_shared<int>(9); std::weak_ptr<int> weak = capture;
    auto deletion_token = deleted.observe([&, capture](auto&) { ++calls; }); capture.reset();
    held.name = std::string("queued"); owner->remove(deleted);
    owner->close(); EXPECT_TRUE(weak.expired()); queue->drain(); EXPECT_EQ(calls, 0);
    EXPECT_THROW(held.observe([](auto&) {}), std::logic_error); token.invalidate();
}

TEST(ManagedObservation, CallbackCanCloseOwnerAndRetireQueuedSiblingCallbacks) {
    auto owner = observer_owner(); auto held = owner->add(TestPerson{"before", 1, std::nullopt});
    int first = 0, second = 0;
    auto one = held.observe([&](auto&) { ++first; owner->close(); });
    auto two = held.observe([&](auto&) { ++second; });
    recovery_name(owner, held.id(), "closed-after-commit");
    EXPECT_EQ(first, 1); EXPECT_EQ(second, 0); EXPECT_TRUE(owner->is_closed());
    one.invalidate(); two.invalidate();
}

TEST(ManagedObservation, AdmittedCallbackSurvivesNonblockingCancellation) {
    auto queue = std::make_shared<HeldObserverScheduler>(); auto owner = observer_owner(":memory:", queue);
    auto held = owner->add(TestPerson{"before", 1, std::nullopt}); queue->drain();
    std::mutex mutex; std::condition_variable cv; bool entered = false, release = false;
    std::atomic<int> returned{0};
    auto capture = std::make_shared<int>(7); std::weak_ptr<int> weak = capture;
    auto token = held.observe([&, capture](auto&) {
        std::unique_lock<std::mutex> lock(mutex); entered = true; cv.notify_all();
        cv.wait(lock, [&] { return release; }); EXPECT_EQ(*capture, 7); ++returned;
    });
    capture.reset(); held.name = std::string("queued");
    std::thread delivery([&] { queue->drain(); });
    bool witnessed;
    {
        std::unique_lock<std::mutex> lock(mutex);
        witnessed = cv.wait_for(lock, std::chrono::seconds(5), [&] { return entered; });
    }
    // The controlled callback has entered. Cancellation must return without
    // requiring its release, and its admitted immutable capture stays alive.
    token.invalidate(); EXPECT_FALSE(weak.expired());
    { std::lock_guard<std::mutex> lock(mutex); release = true; } cv.notify_all();
    delivery.join(); EXPECT_TRUE(witnessed); EXPECT_EQ(returned.load(), 1); EXPECT_TRUE(weak.expired());
    held.name = std::string("after"); queue->drain(); EXPECT_EQ(returned.load(), 1);
}

TEST(ManagedObservation, AttachedRegistrationRefusesAndAttachedWritesCannotAliasMain) {
    TempDB main_path("typed-main"), arm_path("typed-arm");
    auto owner = observer_owner(main_path.str()); auto arm = observer_owner(arm_path.str());
    const std::string gid = "00000000-0000-4000-8000-000000009203";
    for (auto* db : {owner.get(), arm.get()})
        db->db().execute("INSERT INTO TestPerson(id,globalId,name,age) VALUES(1,?,'before',1)", {gid});
    auto held = owner->objects<TestPerson>()[0]; int calls = 0;
    auto token = held.observe([&](auto&) { ++calls; });
    owner->attach(*arm);
    const auto alias = arm_path.path.stem().string();
    bool attached_seen = false;
    for (auto item : owner->objects<TestPerson>()) {
        if (lattice::managed_route(item.table_name()).schema_sql == "main") continue;
        attached_seen = true; EXPECT_THROW(item.observe([](auto&) {}), std::invalid_argument);
    }
    EXPECT_TRUE(attached_seen);
    owner->db().execute("UPDATE " + lattice::managed_quote_identifier(alias) + ".TestPerson SET name='attached' WHERE id=1");
    EXPECT_EQ(calls, 0); EXPECT_EQ(held.name.detach(), "before");
    held.name = std::string("main"); EXPECT_EQ(calls, 1);
    owner->detach(*arm);
}

TEST(ManagedObservation, ExplicitNotificationsUseOwnerAndSchedulerWithoutGlobalBroadcast) {
    auto queue = std::make_shared<HeldObserverScheduler>(); auto owner = observer_owner(":memory:", queue);
    auto held = owner->add(TestPerson{"before", 1, std::nullopt}); queue->drain();
    std::vector<bool> events;
    auto token = held.observe([&](auto& c) { events.push_back(c.is_deleted); if (!c.is_deleted) EXPECT_EQ(fields(c), (std::set<std::string>{"name"})); });
    held.notify_property_change("name"); EXPECT_TRUE(events.empty()); queue->drain();
    held.notify_deleted(); queue->drain(); EXPECT_EQ(events, (std::vector<bool>{false, true}));
}

TEST(ManagedObservation, ExactGlobalIdentityGuardsPhysicalRowAndReplacementNotifies) {
    auto owner = observer_owner(); auto held = owner->add(TestPerson{"before", 1, std::nullopt});
    int calls = 0; auto token = held.observe([&](auto&) { ++calls; });
    owner->notify_changes_batched({{"TestPerson", "UPDATE", held.id(), "wrong-identity", "[\"name\"]"}});
    owner->notify_changes_batched({{"TestPerson", "DELETE", held.id(), "wrong-identity", ""}});
    EXPECT_EQ(calls, 0);
    owner->db().execute("INSERT OR REPLACE INTO TestPerson(id,globalId,name,age) VALUES(?,?,'replacement',2)",
        {held.id(), held.global_id()});
    EXPECT_EQ(calls, 1); EXPECT_EQ(held.name.detach(), "replacement");
}

#include <cstdint>
#include <exception>

namespace {
class SharingPausedScheduler final : public lattice::scheduler {
    mutable std::mutex mutex_;
    std::deque<std::function<void()>> queue_;
public:
    void invoke(std::function<void()>&& work) override {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(std::move(work));
    }
    bool is_on_thread() const noexcept override { return true; }
    bool is_same_as(const scheduler* other) const noexcept override { return other == this; }
    bool can_invoke() const noexcept override { return true; }
    size_t pending() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    bool run_one() {
        std::function<void()> work;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.empty()) return false;
            work.swap(queue_.front()); queue_.pop_front();
        }
        work(); // Actual payload destruction, including throw, is off the leaf.
        return true;
    }
    void shutdown() override {
        std::deque<std::function<void()>> retired;
        { std::lock_guard<std::mutex> lock(mutex_); queue_.swap(retired); }
    }
};
struct sharing_copy_state {
    size_t copies = 0, live = 0;
    std::vector<int> local_values;
    std::exception_ptr copy_error;
};
struct sharing_copy_callback {
    std::shared_ptr<sharing_copy_state> state;
    mutable int local_value = 0;
    explicit sharing_copy_callback(std::shared_ptr<sharing_copy_state> value) : state(std::move(value)) { ++state->live; }
    sharing_copy_callback(const sharing_copy_callback& value) : state(value.state), local_value(value.local_value) {
        ++state->copies;
        if (state->copy_error) std::rethrow_exception(state->copy_error);
        ++state->live;
    }
    sharing_copy_callback(sharing_copy_callback&& value) noexcept : state(std::move(value.state)), local_value(value.local_value) {}
    ~sharing_copy_callback() { if (state) --state->live; }
    template<typename T> void operator()(const T&) const { state->local_values.push_back(++local_value); }
};
struct sharing_capture_marker {
    std::atomic<int>& destroyed;
    explicit sharing_capture_marker(std::atomic<int>& value) : destroyed(value) {}
    ~sharing_capture_marker() { ++destroyed; }
};
}

TEST(ObserverDeliverySharing, ActualTransactionsKeepDistinctOriginalBatchesAfterUnregister) {
    auto queue = std::make_shared<SharingPausedScheduler>();
    auto owner = observer_owner(":memory:", queue);
    const auto first = owner->add(TestPerson{"first", 1, std::nullopt});
    const auto second = owner->add(TestPerson{"second", 2, std::nullopt});
    ASSERT_EQ(queue->pending(), 0u);
    using event = lattice::lattice_db::change_event;
    std::vector<std::pair<int, std::vector<event>>> delivered;
    std::vector<std::uintptr_t> vectors;
    const auto a = owner->add_table_observer("TestPerson", [&](const auto& batch) {
        vectors.push_back(reinterpret_cast<std::uintptr_t>(&batch)); delivered.emplace_back(1, batch);
    });
    const auto b = owner->add_table_observer("TestPerson", [&](const auto& batch) {
        vectors.push_back(reinterpret_cast<std::uintptr_t>(&batch)); delivered.emplace_back(2, batch);
    });
    owner->begin_transaction();
    owner->db().execute("UPDATE TestPerson SET age=11 WHERE id=?", {first.id()});
    owner->db().execute("UPDATE TestPerson SET age=12 WHERE id=?", {second.id()});
    owner->commit();
    owner->db().execute("UPDATE TestPerson SET age=21 WHERE id=?", {first.id()});
    ASSERT_EQ(queue->pending(), 2u);
    EXPECT_TRUE(delivered.empty());
    owner->remove_table_observer("TestPerson", a);
    owner->remove_table_observer("TestPerson", b);
    owner->db().execute("UPDATE TestPerson SET age=31 WHERE id=?", {second.id()});
    EXPECT_EQ(queue->pending(), 2u);
    ASSERT_TRUE(queue->run_one()); ASSERT_TRUE(queue->run_one());
    ASSERT_EQ(delivered.size(), 4u); ASSERT_EQ(vectors.size(), 4u);
    EXPECT_EQ(delivered[0].first, 1); EXPECT_EQ(delivered[1].first, 2);
    EXPECT_EQ(delivered[2].first, 1); EXPECT_EQ(delivered[3].first, 2);
    EXPECT_EQ(delivered[0].second, delivered[1].second);
    EXPECT_EQ(delivered[2].second, delivered[3].second);
    ASSERT_EQ(delivered[0].second.size(), 2u); ASSERT_EQ(delivered[2].second.size(), 1u);
    EXPECT_EQ(std::get<2>(delivered[0].second[0]), first.id());
    EXPECT_EQ(std::get<2>(delivered[0].second[1]), second.id());
    EXPECT_EQ(std::get<2>(delivered[2].second[0]), first.id());
    EXPECT_EQ(vectors[0], vectors[1]); // The same immutable table vector, not one per observer.
    EXPECT_EQ(vectors[2], vectors[3]);
    EXPECT_EQ(queue->pending(), 0u);
}

TEST(ObserverDeliverySharing, ObjectFieldsShareBackingButPreserveInputRowOrderAndSnapshots) {
    auto queue = std::make_shared<SharingPausedScheduler>(); auto owner = observer_owner(":memory:", queue);
    const auto row = owner->add(TestPerson{"fields", 1, std::nullopt});
    std::vector<std::pair<int, std::string>> delivered;
    std::vector<std::uintptr_t> strings;
    const auto a = owner->add_object_observer("TestPerson", row.id(), [&](const auto& fields) {
        strings.push_back(reinterpret_cast<std::uintptr_t>(&fields)); delivered.emplace_back(1, fields);
    });
    const auto b = owner->add_object_observer("TestPerson", row.id(), [&](const auto& fields) {
        strings.push_back(reinterpret_cast<std::uintptr_t>(&fields)); delivered.emplace_back(2, fields);
    });
    std::vector<lattice::lattice_db::change_event> input{
        {"TestPerson", "UPDATE", row.id(), row.global_id(), "[\"name\"]"},
        {"TestPerson", "UPDATE", row.id(), row.global_id(), "[\"age\"]"}};
    owner->notify_changes_batched(input); // The real public production delivery entry.
    std::get<4>(input[0]) = "caller changed its own data"; input.clear();
    owner->notify_changes_batched({{"TestPerson", "UPDATE", row.id(), row.global_id(), "[\"name\",\"age\"]"}});
    owner->remove_all_object_observers("TestPerson", row.id());
    ASSERT_EQ(queue->pending(), 2u);
    ASSERT_TRUE(queue->run_one()); ASSERT_TRUE(queue->run_one());
    EXPECT_EQ(delivered, (std::vector<std::pair<int, std::string>>{
        {1,"[\"name\"]"},{2,"[\"name\"]"},{1,"[\"age\"]"},{2,"[\"age\"]"},
        {1,"[\"name\",\"age\"]"},{2,"[\"name\",\"age\"]"}}));
    ASSERT_EQ(strings.size(), 6u);
    EXPECT_EQ(strings[0], strings[1]); EXPECT_EQ(strings[2], strings[3]); EXPECT_EQ(strings[4], strings[5]);
    EXPECT_EQ(queue->pending(), 0u);
}

TEST(ObserverDeliverySharing, OpaqueCopiesAndMutableValueIsolationRemainPerQueuedTransaction) {
    auto queue = std::make_shared<SharingPausedScheduler>(); auto owner = observer_owner(":memory:", queue);
    const auto row = owner->add(TestPerson{"copies", 1, std::nullopt});
    auto table = std::make_shared<sharing_copy_state>(), object = std::make_shared<sharing_copy_state>();
    const auto a = owner->add_table_observer("TestPerson", sharing_copy_callback{table});
    const auto b = owner->add_object_observer("TestPerson", row.id(), sharing_copy_callback{object});
    const auto table_copies = table->copies, object_copies = object->copies;
    const auto table_live = table->live, object_live = object->live;
    for (int64_t age : {2,3,4}) owner->db().execute("UPDATE TestPerson SET age=? WHERE id=?", {age, row.id()});
    ASSERT_EQ(queue->pending(), 3u);
    EXPECT_EQ(table->copies, table_copies + 3); EXPECT_EQ(object->copies, object_copies + 3);
    EXPECT_EQ(table->live, table_live + 3); EXPECT_EQ(object->live, object_live + 3);
    EXPECT_TRUE(table->local_values.empty()); EXPECT_TRUE(object->local_values.empty());
    owner->remove_table_observer("TestPerson", a); owner->remove_object_observer("TestPerson", row.id(), b);
    EXPECT_EQ(table->live, 3u); EXPECT_EQ(object->live, 3u);
    ASSERT_TRUE(queue->run_one()); ASSERT_TRUE(queue->run_one()); ASSERT_TRUE(queue->run_one());
    EXPECT_EQ(table->local_values, (std::vector<int>{1,1,1}));
    EXPECT_EQ(object->local_values, (std::vector<int>{1,1,1}));
    EXPECT_EQ(table->live, 0u); EXPECT_EQ(object->live, 0u);
}

TEST(ObserverDeliverySharing, OpaqueCopyFailureStillOccursDuringNotificationBeforeSubmission) {
    for (bool table : {true, false}) {
        auto queue = std::make_shared<SharingPausedScheduler>(); auto owner = observer_owner(":memory:", queue);
        const auto row = owner->add(TestPerson{"copy-failure", 1, std::nullopt});
        auto state = std::make_shared<sharing_copy_state>();
        const auto id = table ? owner->add_table_observer("TestPerson", sharing_copy_callback{state})
            : owner->add_object_observer("TestPerson", row.id(), sharing_copy_callback{state});
        const auto expected = std::make_exception_ptr(std::runtime_error("actual opaque callback copy failure"));
        state->copy_error = expected;
        std::exception_ptr actual;
        try { owner->notify_changes_batched({{"TestPerson", "UPDATE", row.id(), row.global_id(), "[\"name\"]"}}); }
        catch (...) { actual = std::current_exception(); }
        EXPECT_EQ(actual, expected); EXPECT_EQ(queue->pending(), 0u); EXPECT_EQ(state->live, 1u);
        state->copy_error = {};
        if (table) owner->remove_table_observer("TestPerson", id);
        else owner->remove_object_observer("TestPerson", row.id(), id);
        EXPECT_EQ(state->live, 0u);
    }
}

TEST(ObserverDeliverySharing, ThrowingActualQueuedCallbackReleasesItsCaptureAndPropagates) {
    std::atomic<int> destroyed{0};
    auto queue = std::make_shared<SharingPausedScheduler>(); auto owner = observer_owner(":memory:", queue);
    const auto row = owner->add(TestPerson{"throw", 1, std::nullopt});
    auto marker = std::make_shared<sharing_capture_marker>(destroyed);
    const std::weak_ptr<sharing_capture_marker> weak = marker;
    const auto expected = std::make_exception_ptr(std::runtime_error("actual observer callback failure"));
    const auto id = owner->add_table_observer("TestPerson", [held = std::move(marker), expected](const auto&) { std::rethrow_exception(expected); });
    owner->db().execute("UPDATE TestPerson SET age=8 WHERE id=?", {row.id()});
    owner->remove_table_observer("TestPerson", id);
    ASSERT_EQ(queue->pending(), 1u); ASSERT_FALSE(weak.expired());
    std::exception_ptr actual;
    try { queue->run_one(); } catch (...) { actual = std::current_exception(); }
    EXPECT_EQ(actual, expected); EXPECT_TRUE(weak.expired()); EXPECT_EQ(destroyed.load(), 1);
    EXPECT_EQ(queue->pending(), 0u);
    EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT age FROM TestPerson WHERE id=?", {row.id()}).at(0).at("age")), 8);
}

TEST(ObserverDeliverySharing, ActualSchedulerCancellationReleasesQueuedUnregisteredCaptures) {
    std::atomic<int> destroyed{0};
    auto queue = std::make_shared<SharingPausedScheduler>(); auto owner = observer_owner(":memory:", queue);
    const auto row = owner->add(TestPerson{"cancel", 1, std::nullopt});
    auto marker = std::make_shared<sharing_capture_marker>(destroyed);
    const std::weak_ptr<sharing_capture_marker> weak = marker;
    int calls = 0;
    const auto id = owner->add_table_observer("TestPerson", [held = std::move(marker), &calls](const auto&) { ++calls; });
    owner->db().execute("UPDATE TestPerson SET age=17 WHERE id=?", {row.id()});
    owner->remove_table_observer("TestPerson", id);
    ASSERT_EQ(queue->pending(), 1u); ASSERT_FALSE(weak.expired());
    queue->shutdown();
    EXPECT_TRUE(weak.expired()); EXPECT_EQ(destroyed.load(), 1); EXPECT_EQ(calls, 0);
    EXPECT_EQ(queue->pending(), 0u);
    EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT age FROM TestPerson WHERE id=?", {row.id()}).at(0).at("age")), 17);
}
