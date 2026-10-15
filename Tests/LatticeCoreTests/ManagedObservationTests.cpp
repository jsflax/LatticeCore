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
