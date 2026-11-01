#include "TestHelpers.hpp"
#include <lattice/db.hpp>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>

#if !defined(__EMSCRIPTEN__)
namespace {
using namespace std::chrono_literals;
struct cancellation_gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    void signal() { std::lock_guard<std::mutex> lock(mutex); ready = true; cv.notify_all(); }
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 5s, [&] { return ready; });
    }
};
int busy_timeout(lattice::database& db) {
    const auto rows = db.query("PRAGMA busy_timeout");
    return rows.empty() ? -1 : static_cast<int>(std::get<int64_t>(rows[0].at("timeout")));
}
enum class blocked_operation { begin, checkpoint, insert_after_try_begin };

void cancelled_wait_preserves_shared_writer(blocked_operation operation,
        bool bound = false, bool scoped_over_binding = false,
        bool finish_scope_before_sql = false) {
    constexpr int budget_ms = 3000;
    TempDB file{"cancel_busy"};
    lattice::database shared(file.str(), lattice::database::open_mode::read_write, budget_ms);
    shared.execute("CREATE TABLE cancellable_rows(value INTEGER)");
    auto cancellation = std::make_shared<std::atomic<bool>>(false);
    auto& cancelled = *cancellation;
    std::unique_ptr<lattice::database> unrelated;
    if (bound) {
        shared.bind_cancellation_token(cancellation);
        unrelated = std::make_unique<lattice::database>(
            file.str(), lattice::database::open_mode::read_write, budget_ms);
    }
    lattice::database holder(file.str());
    holder.begin_transaction();
    holder.execute("INSERT INTO cancellable_rows VALUES(1)");
    std::atomic<bool> armed{false}, local_cancelled{false};
    cancellation_gate entered, app_started;
    struct trace_context { std::atomic<bool>& armed; cancellation_gate& entered; } trace{armed, entered};
    sqlite3_trace_v2(shared.handle(), SQLITE_TRACE_STMT,
        [](unsigned, void* raw, void*, void*) -> int {
            auto& value = *static_cast<trace_context*>(raw);
            if (value.armed.load()) value.entered.signal();
            return 0;
        }, &trace);
    bool expected_failure = false, try_begin_failed = false;
    std::exception_ptr unexpected_failure, app_failure;
    std::chrono::steady_clock::time_point settled;
    std::thread worker([&] {
        try {
            std::optional<lattice::database::cancellable_operation> ownership;
            if (!bound || scoped_over_binding)
                ownership.emplace(shared, bound ? local_cancelled : cancelled);
            if (finish_scope_before_sql) ownership.reset();
            if (operation == blocked_operation::insert_after_try_begin)
                try_begin_failed = !shared.try_begin_immediate();
            armed = true;
            try {
                if (operation == blocked_operation::begin) {
                    shared.begin_transaction();
                    shared.rollback();
                } else if (operation == blocked_operation::checkpoint) {
                    const auto result = shared.wal_checkpoint(true, budget_ms);
                    expected_failure = result.busy != 0 || result.rc != SQLITE_OK;
                } else {
                    shared.execute("INSERT INTO cancellable_rows VALUES(2)");
                }
            } catch (const lattice::db_error&) {
                expected_failure = true;
            }
            armed = false;
        } catch (...) { unexpected_failure = std::current_exception(); }
        settled = std::chrono::steady_clock::now();
    });
    const bool began = entered.wait();
    std::thread app([&] {
        app_started.signal();
        try { (unrelated ? *unrelated : shared).execute("INSERT INTO cancellable_rows VALUES(3)"); }
        catch (...) { app_failure = std::current_exception(); }
    });
    const bool app_queued = app_started.wait();
    const auto requested = std::chrono::steady_clock::now();
    cancelled = true;
    worker.join();
    const auto cancellation_ms = std::chrono::duration_cast<std::chrono::milliseconds>(settled - requested).count();
    const bool holder_preserved = holder.is_in_transaction();
    holder.rollback();
    app.join();
    sqlite3_trace_v2(shared.handle(), 0, nullptr, nullptr);
    ASSERT_TRUE(began);
    ASSERT_TRUE(app_queued);
    ASSERT_FALSE(unexpected_failure);
    ASSERT_FALSE(app_failure);
    EXPECT_TRUE(expected_failure);
    EXPECT_TRUE(holder_preserved);
    // Broad relative bound: an overwritten handler or BEGIN retry loop waits
    // the full budget. No dependency on the standalone probe's 7 ms result.
    EXPECT_LT(cancellation_ms, budget_ms / 2);
    if (operation == blocked_operation::insert_after_try_begin) EXPECT_TRUE(try_begin_failed);
    auto& retry = unrelated ? *unrelated : shared;
    EXPECT_EQ(busy_timeout(retry), budget_ms);
    EXPECT_FALSE(shared.is_in_transaction());
    const auto rows = shared.query("SELECT value FROM cancellable_rows");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("value")), 3);
    retry.begin_transaction();
    retry.execute("INSERT INTO cancellable_rows VALUES(4)");
    retry.rollback();
    EXPECT_EQ(shared.query("SELECT value FROM cancellable_rows").size(), 1u);
}
} // namespace

TEST(DatabaseCancellation, ExistingApplicationTransactionIsNotAdoptedOrRolledBack) {
    TempDB file{"cancel_existing_transaction"};
    lattice::database db(file.str(), lattice::database::open_mode::read_write, 1500);
    db.execute("CREATE TABLE kept(value INTEGER)");
    db.begin_transaction();
    db.execute("INSERT INTO kept VALUES(41)");
    std::atomic<bool> cancelled{false};
    EXPECT_THROW((lattice::database::cancellable_operation(db, cancelled)), lattice::db_error);
    EXPECT_TRUE(db.is_in_transaction());
    const auto rows = db.query("SELECT value FROM kept");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("value")), 41);
    db.rollback();
    EXPECT_TRUE(db.query("SELECT value FROM kept").empty());
    EXPECT_EQ(busy_timeout(db), 1500);
}

TEST(DatabaseCancellation, ActiveStatementAndNestedScopeRejectAdmission) {
    TempDB file{"cancel_active_statement"};
    lattice::database db(file.str());
    std::atomic<bool> cancelled{false};
    sqlite3_stmt* statement = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.handle(), "SELECT 1 UNION ALL SELECT 2", -1, &statement, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    EXPECT_THROW((lattice::database::cancellable_operation(db, cancelled)), lattice::db_error);
    EXPECT_EQ(sqlite3_step(statement), SQLITE_ROW);
    sqlite3_finalize(statement);
    {
        lattice::database::cancellable_operation first(db, cancelled);
        EXPECT_THROW((lattice::database::cancellable_operation(db, cancelled)), lattice::db_error);
        EXPECT_EQ(db.query("SELECT 5 AS n").size(), 1u);
    }
    EXPECT_EQ(busy_timeout(db), lattice::kDefaultBusyTimeoutMs);
}

TEST(DatabaseCancellation, OtherThreadsActiveSQLIsNotWaitedForOrInterrupted) {
    TempDB file{"cancel_active_thread"};
    lattice::database db(file.str());
    struct context { cancellation_gate entered, finish; std::atomic<bool> released{false}; } state;
    ASSERT_EQ(sqlite3_create_function_v2(db.handle(), "hold_for_cancel_test", 0, SQLITE_UTF8,
        &state, [](sqlite3_context* sql, int, sqlite3_value**) {
            auto& value = *static_cast<context*>(sqlite3_user_data(sql));
            value.entered.signal();
            value.released = value.finish.wait();
            sqlite3_result_int(sql, 17);
        }, nullptr, nullptr, nullptr), SQLITE_OK);
    std::exception_ptr worker_failure;
    int64_t result = 0;
    std::thread worker([&] {
        try { result = std::get<int64_t>(db.query("SELECT hold_for_cancel_test() AS n")[0].at("n")); }
        catch (...) { worker_failure = std::current_exception(); }
    });
    const bool entered = state.entered.wait();
    std::atomic<bool> cancelled{false};
    bool rejected = false;
    const auto start = std::chrono::steady_clock::now();
    try { lattice::database::cancellable_operation scope(db, cancelled); }
    catch (const lattice::db_error&) { rejected = true; }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    state.finish.signal();
    worker.join();
    ASSERT_TRUE(entered);
    EXPECT_TRUE(rejected);
    EXPECT_LT(elapsed, 1s);
    EXPECT_TRUE(state.released);
    EXPECT_FALSE(worker_failure);
    EXPECT_EQ(result, 17);
}

TEST(DatabaseCancellation, CancelledBeginDoesNotRetryTheRemainingBudget) {
    cancelled_wait_preserves_shared_writer(blocked_operation::begin);
}
TEST(DatabaseCancellation, CheckpointBudgetChangesPreserveCancellation) {
    cancelled_wait_preserves_shared_writer(blocked_operation::checkpoint);
}
TEST(DatabaseCancellation, TryBeginRestoresTheScopedCancellationHandler) {
    cancelled_wait_preserves_shared_writer(blocked_operation::insert_after_try_begin);
}

TEST(DatabaseCancellation, NoncancelledScopePreservesWaitAndCommitBehavior) {
    TempDB file{"cancel_allowed_wait"};
    lattice::database db(file.str(), lattice::database::open_mode::read_write, 1500);
    db.execute("CREATE TABLE allowed(value INTEGER)");
    lattice::database holder(file.str());
    holder.begin_transaction();
    std::atomic<bool> cancelled{false};
    cancellation_gate starting;
    std::exception_ptr failure;
    std::thread worker([&] {
        try {
            lattice::database::cancellable_operation scope(db, cancelled);
            starting.signal();
            db.begin_transaction();
            db.execute("INSERT INTO allowed VALUES(99)");
            db.commit();
        } catch (...) { failure = std::current_exception(); }
    });
    const bool started = starting.wait();
    holder.rollback();
    worker.join();
    ASSERT_TRUE(started);
    ASSERT_FALSE(failure);
    EXPECT_EQ(std::get<int64_t>(db.query("SELECT value FROM allowed")[0].at("value")), 99);
    EXPECT_EQ(busy_timeout(db), 1500);
}

TEST(DatabaseCancellation, BoundPrivateBeginCancellationLeavesSamePathAppConnectionUsable) {
    cancelled_wait_preserves_shared_writer(blocked_operation::begin, true);
}
TEST(DatabaseCancellation, BoundCheckpointPreservesCancellationAcrossBudgetChanges) {
    cancelled_wait_preserves_shared_writer(blocked_operation::checkpoint, true);
}
TEST(DatabaseCancellation, BoundTryBeginRestoresCancellationForFollowingSQL) {
    cancelled_wait_preserves_shared_writer(blocked_operation::insert_after_try_begin, true);
}
TEST(DatabaseCancellation, ScopedOperationAlsoObservesItsPrivateBoundToken) {
    cancelled_wait_preserves_shared_writer(blocked_operation::insert_after_try_begin, true, true);
}
TEST(DatabaseCancellation, ScopedOperationRestoresPrivateBoundHandlerOnExit) {
    cancelled_wait_preserves_shared_writer(blocked_operation::insert_after_try_begin, true, true, true);
}

TEST(DatabaseCancellation, BindingRejectsExistingApplicationTransactionAndReplacement) {
    TempDB file{"cancel_binding_admission"};
    lattice::database db(file.str(), lattice::database::open_mode::read_write, 1500);
    db.execute("CREATE TABLE kept(value INTEGER)");
    db.begin_transaction();
    db.execute("INSERT INTO kept VALUES(41)");
    auto token = std::make_shared<std::atomic<bool>>(false);
    EXPECT_THROW(db.bind_cancellation_token(token), lattice::db_error);
    EXPECT_TRUE(db.is_in_transaction());
    EXPECT_EQ(std::get<int64_t>(db.query("SELECT value FROM kept")[0].at("value")), 41);
    db.rollback();
    EXPECT_EQ(busy_timeout(db), 1500);
    EXPECT_TRUE(db.query("SELECT value FROM kept").empty());
    db.bind_cancellation_token(token);
    EXPECT_THROW(db.bind_cancellation_token(token), lattice::db_error);
    EXPECT_THROW(db.bind_cancellation_token(std::make_shared<std::atomic<bool>>(false)), lattice::db_error);
    EXPECT_THROW(db.bind_cancellation_token(nullptr), lattice::db_error);
}

TEST(DatabaseCancellation, BoundTokenOutlivesCallerAndSurvivesMovingIdleDatabase) {
    TempDB file{"cancel_binding_lifetime"};
    std::weak_ptr<std::atomic<bool>> weak;
    {
        lattice::database original(file.str(), lattice::database::open_mode::read_write, 1500);
        original.execute("CREATE TABLE kept(value INTEGER)");
        auto token = std::make_shared<std::atomic<bool>>(false);
        weak = token;
        original.bind_cancellation_token(token);
        token.reset();
        ASSERT_FALSE(weak.expired());
        lattice::database moved(std::move(original));
        lattice::database assigned(":memory:");
        assigned = std::move(moved);
        ASSERT_FALSE(weak.expired());
        assigned.begin_transaction();
        assigned.execute("INSERT INTO kept VALUES(1)");
        weak.lock()->store(true);
        // Cancellation must never strand an already-owned transaction.
        EXPECT_NO_THROW(assigned.rollback());
        EXPECT_FALSE(assigned.is_in_transaction());
        EXPECT_TRUE(assigned.query("SELECT value FROM kept").empty());
        EXPECT_THROW(assigned.begin_transaction(), lattice::db_error);
    }
    EXPECT_TRUE(weak.expired());
}
#endif
