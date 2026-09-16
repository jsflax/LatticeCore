#pragma once

#include "TestHelpers.hpp"
#include <algorithm>
#include <chrono>
#include <future>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

// The production header grants only this test-side friend access. There is no
// public setter or Swift bridge test API; register before workers, clear after
// every worker has joined.
namespace lattice {
struct vec0_maintenance_test_access {
    static void set(lattice_db& db,
                    std::function<void(const char*, const char*)> hook) {
        db.test_hook_vec0_maintenance_ = std::move(hook);
    }
    template<typename F>
    static std::invoke_result_t<F> run(lattice_db& db, F&& body) {
        return db.with_vec0_maintenance("test", std::forward<F>(body));
    }
};
} // namespace lattice

namespace vec0_maintenance_test {

inline constexpr auto wait_budget = std::chrono::seconds(5);

class event_signal {
    std::promise<void> promise_;
    std::shared_future<void> future_ = promise_.get_future().share();
    std::atomic<bool> sent_{false};
public:
    void send() {
        if (!sent_.exchange(true)) promise_.set_value();
    }
    bool wait(std::chrono::seconds budget = wait_budget) const {
        return future_.wait_for(budget) == std::future_status::ready;
    }
};

template<typename T>
class joined_task {
    std::future<T> result_;
    std::thread thread_;
public:
    template<typename F>
    explicit joined_task(F&& fn) {
        std::packaged_task<T()> work(std::forward<F>(fn));
        result_ = work.get_future();
        thread_ = std::thread(std::move(work));
    }
    ~joined_task() { join(); }
    bool ready() const {
        return result_.wait_for(wait_budget) == std::future_status::ready;
    }
    std::thread::id id() const { return thread_.get_id(); }
    void join() { if (thread_.joinable()) thread_.join(); }
    T get() { return result_.get(); }
};

// Always release the incumbent before joining either worker. Waits on our
// barriers and completion futures are bounded; std::thread::join cannot cancel
// a broken SQLite mutex implementation. A hosted job timeout is the final
// containment for that failure, and no worker is detached with dangling refs.
struct release_on_exit {
    event_signal& release;
    ~release_on_exit() { release.send(); }
};

inline int try_writer_mutex(sqlite3_mutex* mutex) {
    const int result = sqlite3_mutex_try(mutex);
    if (result == SQLITE_OK) sqlite3_mutex_leave(mutex);
    return result;
}

inline int probe_writer_mutex_from_another_thread(lattice::database& db) {
    auto* mutex = sqlite3_db_mutex(db.handle());
    int result = -1;
    std::thread probe([&] { result = try_writer_mutex(mutex); });
    probe.join();
    return result;
}

inline int probe_writer_mutex_from_another_thread(lattice::lattice_db& db) {
    return probe_writer_mutex_from_another_thread(db.db());
}

inline void create_memory_fixture(lattice::lattice_db& db) {
    db.db().execute(
        "CREATE TABLE MaintenanceDoc (id INTEGER PRIMARY KEY, "
        "globalId TEXT UNIQUE NOT NULL, embedding BLOB)");
    db.db().execute(
        "INSERT INTO MaintenanceDoc (globalId, embedding) VALUES (?, ?)",
        {std::string("one"), pack_floats({1.0f, 0.0f, 0.0f, 0.0f})});
    db.db().execute(
        "INSERT INTO MaintenanceDoc (globalId, embedding) VALUES (?, ?)",
        {std::string("two"), pack_floats({0.0f, 1.0f, 0.0f, 0.0f})});
}

inline int64_t memory_index_count(lattice::lattice_db& db) {
    const auto rows = db.db().query(
        "SELECT COUNT(*) AS c FROM _MaintenanceDoc_embedding_vec_rowids");
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

} // namespace vec0_maintenance_test
