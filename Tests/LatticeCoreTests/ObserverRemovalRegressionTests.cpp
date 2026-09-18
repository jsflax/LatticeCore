#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <cstdio>
#include <array>
#include <atomic>
#include <thread>
#include <unordered_set>
#include <memory>
#include <vector>

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <csignal>
#include <unistd.h>

namespace {
enum class RemovalKind { table, invalidation, object, all_objects };
using observer_id = lattice::lattice_db::observer_id;

observer_id add_probe(lattice::lattice_db& owner, RemovalKind kind,
                      int64_t row, std::function<void()> callback) {
    if (kind == RemovalKind::table) {
        return owner.add_table_observer("RemovalProbe",
            [callback = std::move(callback)](const auto&) { callback(); });
    }
    if (kind == RemovalKind::invalidation) {
        return owner.add_invalidation_hook_detailed(
            [callback = std::move(callback)](const auto&, auto) { callback(); });
    }
    return owner.add_object_observer("RemovalProbe", row,
        [callback = std::move(callback)](const auto&) { callback(); });
}

void remove_probe(lattice::lattice_db& owner, RemovalKind kind,
                  int64_t row, observer_id id) {
    if (kind == RemovalKind::table) owner.remove_table_observer("RemovalProbe", id);
    else if (kind == RemovalKind::invalidation) owner.remove_invalidation_hook(id);
    else owner.remove_object_observer("RemovalProbe", row, id);
}

struct CancelSiblingOnRelease {
    lattice::lattice_db* owner;
    RemovalKind kind;
    int64_t sibling_row;
    observer_id sibling = 0;
    int* destructor_count;
    ~CancelSiblingOnRelease() {
        std::fputs("capture_destructor_enter\n", stderr);
        remove_probe(*owner, kind, sibling_row, sibling);
        ++*destructor_count;
        std::fputs("capture_destructor_exit\n", stderr);
    }
};

int removal_case(RemovalKind kind, bool retain_until_after_removal) {
    // No TestHelpers environment, file database, notifier, or queued callbacks.
    lattice::lattice_db owner;
    std::vector<int> events;
    events.reserve(8);
    int destructor_count = 0;
    const int64_t survivor_row = kind == RemovalKind::all_objects ? 2 : 1;
    add_probe(owner, kind, survivor_row, [&] { events.push_back(10); });
    auto retained = std::shared_ptr<CancelSiblingOnRelease>(
        new CancelSiblingOnRelease{&owner, kind, survivor_row, 0, &destructor_count});
    const auto primary = add_probe(owner, kind, 1, [retained] {});
    retained->sibling = add_probe(owner, kind, survivor_row, [&] { events.push_back(99); });
    add_probe(owner, kind, survivor_row, [&] { events.push_back(20); });
    if (kind == RemovalKind::all_objects) {
        add_probe(owner, kind, 1, [&] { events.push_back(999); });
    }
    if (!retain_until_after_removal) retained.reset();
    std::fputs("remove_primary_enter\n", stderr);
    if (kind == RemovalKind::all_objects) owner.remove_all_object_observers("RemovalProbe", 1);
    else remove_probe(owner, kind, 1, primary);
    std::fputs("remove_primary_exit\n", stderr);
    retained.reset();
    // Repeated removal and a missing ID must remain no-ops.
    if (kind == RemovalKind::all_objects) {
        owner.remove_all_object_observers("RemovalProbe", 1);
        owner.remove_all_object_observers("MissingProbe", 1);
    } else {
        remove_probe(owner, kind, 1, primary);
        remove_probe(owner, kind, 1, static_cast<observer_id>(-1));
    }
    if (kind == RemovalKind::invalidation) {
        owner.fire_invalidation_hooks({"RemovalProbe"}, lattice::lattice_db::invalidation_reason::commit);
    } else {
        std::vector<lattice::lattice_db::change_event> changes{
            {"RemovalProbe", "INSERT", 1, "probe-1", "[]"}};
        if (kind == RemovalKind::all_objects) changes.emplace_back("RemovalProbe", "INSERT", 2, "probe-2", "[]");
        owner.notify_changes_batched(changes);
    }
    if (destructor_count != 1 || events != std::vector<int>{10, 20}) return 1;
    std::fputs("removal_complete survivors=10,20 destructor_count=1\n", stderr);
    return 0;
}

int concurrent_registration_case() {
    constexpr size_t worker_count = 4;
    constexpr size_t per_worker = 1024;
    constexpr size_t total = worker_count * per_worker;
    struct Registration { observer_id id = 0; bool table = false; };
    std::array<Registration, total> registrations{};
    std::array<std::atomic<unsigned>, total> deliveries{};
    lattice::lattice_db owner; // Destroy callbacks before their captured counters.
    std::atomic<unsigned> ready{0};
    std::atomic<bool> start{false};
    std::array<std::thread, worker_count> workers;
    for (size_t worker = 0; worker < worker_count; ++worker) {
        workers[worker] = std::thread([&, worker] {
            ready.fetch_add(1);
            while (!start.load()) std::this_thread::yield();
            for (size_t index = worker * per_worker; index < (worker + 1) * per_worker; ++index) {
                const bool table = worker % 2 == 0;
                const auto id = table
                    ? owner.add_table_observer("TokenProbe", [&, index](const auto&) {
                        deliveries[index].fetch_add(1);
                    })
                    : owner.add_object_observer("TokenProbe", 17, [&, index](const auto&) {
                        deliveries[index].fetch_add(1);
                    });
                registrations[index] = {id, table};
            }
        });
    }
    while (ready.load() != worker_count) std::this_thread::yield();
    start.store(true);
    for (auto& worker : workers) worker.join();
    std::unordered_set<observer_id> tokens;
    for (const auto& registration : registrations) {
        if (registration.id == 0 || !tokens.insert(registration.id).second) {
            std::fputs("registration_duplicate_or_zero_token\n", stderr);
            return 10;
        }
    }
    const std::vector<lattice::lattice_db::change_event> batch{
        {"TokenProbe", "INSERT", 17, "token-probe-17", "[]"}};
    owner.notify_changes_batched(batch);
    for (const auto& count : deliveries) if (count.load() != 1) return 11;
    auto remove = [&](const Registration& registration) {
        if (registration.table) owner.remove_table_observer("TokenProbe", registration.id);
        else owner.remove_object_observer("TokenProbe", 17, registration.id);
    };
    for (size_t index = 0; index < total; index += 3) remove(registrations[index]);
    owner.notify_changes_batched(batch);
    for (size_t index = 0; index < total; ++index)
        if (deliveries[index].load() != (index % 3 == 0 ? 1u : 2u)) return 12;
    for (const auto& registration : registrations) remove(registration);
    owner.notify_changes_batched(batch);
    for (size_t index = 0; index < total; ++index)
        if (deliveries[index].load() != (index % 3 == 0 ? 1u : 2u)) return 13;
    std::fputs("registration_complete unique=4096 subset_cancelled survivors_delivered final_silence\n", stderr);
    return 0;
}

void require_bounded_removal(RemovalKind kind, bool retained_control) {
    // Exec a fresh child even in a suite that has background threads. The
    // child's alarm makes an old under-lock destruction fail in five seconds
    // instead of leaving a blocked test thread or wedging the full suite.
    struct RestoreDeathTestStyle {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~RestoreDeathTestStyle() {
            ::testing::FLAGS_gtest_death_test_style = std::move(previous);
        }
    } restore_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({
        std::signal(SIGALRM, SIG_DFL);
        alarm(5);
        const auto result = removal_case(kind, retained_control);
        _exit(result);
    }, ::testing::ExitedWithCode(0), "removal_complete survivors=10,20 destructor_count=1");
}
} // namespace

TEST(ObserverRemovalRegression, TableFinalCaptureCanCancelSibling) {
    require_bounded_removal(RemovalKind::table, false);
}
TEST(ObserverRemovalRegression, TableRetainedCaptureControl) {
    require_bounded_removal(RemovalKind::table, true);
}
TEST(ObserverRemovalRegression, InvalidationFinalCaptureCanCancelSibling) {
    require_bounded_removal(RemovalKind::invalidation, false);
}
TEST(ObserverRemovalRegression, InvalidationRetainedCaptureControl) {
    require_bounded_removal(RemovalKind::invalidation, true);
}
TEST(ObserverRemovalRegression, ObjectFinalCaptureCanCancelSibling) {
    require_bounded_removal(RemovalKind::object, false);
}
TEST(ObserverRemovalRegression, ObjectRetainedCaptureControl) {
    require_bounded_removal(RemovalKind::object, true);
}
TEST(ObserverRemovalRegression, AllObjectFinalCaptureCanCancelSibling) {
    require_bounded_removal(RemovalKind::all_objects, false);
}
TEST(ObserverRemovalRegression, AllObjectRetainedCaptureControl) {
    require_bounded_removal(RemovalKind::all_objects, true);
}
TEST(ObserverTokenRegistration, ConcurrentKindsRemainUniqueAndIndependentlyCancellable) {
    struct RestoreDeathTestStyle {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~RestoreDeathTestStyle() { ::testing::FLAGS_gtest_death_test_style = std::move(previous); }
    } restore_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({
        std::signal(SIGALRM, SIG_DFL);
        alarm(5);
        const auto result = concurrent_registration_case();
        _exit(result);
    }, ::testing::ExitedWithCode(0), "registration_complete unique=4096 subset_cancelled survivors_delivered final_silence");
}

#else
TEST(ObserverRemovalRegression, RequiresNativeBoundedChildProcesses) {
    GTEST_SKIP() << "Observer removal reentrancy regression requires native POSIX death tests";
}
#endif
