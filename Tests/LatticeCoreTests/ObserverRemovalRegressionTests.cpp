#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <cstdio>
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
#else
TEST(ObserverRemovalRegression, RequiresNativeBoundedChildProcesses) {
    GTEST_SKIP() << "Observer removal reentrancy regression requires native POSIX death tests";
}
#endif
