#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

// No TestHelpers.hpp global logging or model registration. The supervising
// runner supplies TMPDIR and LATTICE_TEST_LOG_PATH below its isolated work root.
namespace lattice {
struct database_query_test_access {
    static void set(std::function<void(sqlite3_stmt*)> hook) {
        database::test_hook_query_row_ = std::move(hook);
    }
};
} // namespace lattice

namespace {
using namespace lattice;
constexpr auto wait_budget = std::chrono::seconds(5);

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

struct OwnedFile {
    std::filesystem::path path;
    OwnedFile() {
        static std::atomic<unsigned> sequence{0};
        path = std::filesystem::temp_directory_path() /
            ("query-lifetime-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
             std::to_string(sequence.fetch_add(1)) + ".sqlite");
        require(!std::filesystem::exists(path), "fresh query fixture required");
    }
    ~OwnedFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + "-wal", ignored);
        std::filesystem::remove(path.string() + "-shm", ignored);
    }
};

struct Fixture {
    OwnedFile file;
    database db{file.path.string(), database::open_mode::read_write, 100};
    Fixture() {
        db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
        db.execute("INSERT INTO QueryProbe VALUES(1,1)");
    }
};

int64_t count(database& db) {
    return std::get<int64_t>(db.query("SELECT COUNT(*) AS n FROM QueryProbe").at(0).at("n"));
}

struct RowHook {
    explicit RowHook(std::function<void(sqlite3_stmt*)> hook) {
        database_query_test_access::set(std::move(hook));
    }
    ~RowHook() { database_query_test_access::set(nullptr); }
};

struct Event {
    std::promise<void> promise;
    std::shared_future<void> future = promise.get_future().share();
    std::atomic<bool> sent{false};
    void send() { if (!sent.exchange(true)) promise.set_value(); }
    bool wait() const { return future.wait_for(wait_budget) == std::future_status::ready; }
};

template<typename T>
class JoinedTask {
    std::future<T> result_;
    std::thread thread_;
public:
    template<typename F>
    explicit JoinedTask(F&& body) {
        std::packaged_task<T()> task(std::forward<F>(body));
        result_ = task.get_future();
        thread_ = std::thread(std::move(task));
    }
    ~JoinedTask() { if (thread_.joinable()) thread_.join(); }
    T get() { return result_.get(); }
};

struct ReleaseOnExit {
    Event& event;
    ~ReleaseOnExit() { event.send(); }
};

int try_mutex(sqlite3_mutex* mutex) {
    const int rc = sqlite3_mutex_try(mutex);
    if (rc == SQLITE_OK) sqlite3_mutex_leave(mutex);
    return rc;
}

int mutex_probe(database& db) {
    int rc = -1;
    std::thread probe([&] { rc = try_mutex(sqlite3_db_mutex(db.handle())); });
    probe.join();
    return rc;
}

struct QueryResult {
    size_t rows = 0;
    std::string error;
};

struct Overlap {
    bool row_reached = false;
    bool contender_observed = false;
    bool completed_before_release = false;
    int probe = -1, busy = -1, readonly = -1;
    QueryResult query;
    std::string writer_error;
};

Overlap overlap(database& db, const std::string& sql, std::function<void()> writer) {
    Event row, release, observed;
    std::atomic<int> probe{-1}, busy{-1}, readonly{-1};
    std::atomic<bool> completed{false};
    // Destruction releases the incumbent before joining either worker. No
    // detached thread can retain stack references. Barrier waits are bounded;
    // a broken SQLite mutex still needs the supervising job's final timeout.
    std::unique_ptr<JoinedTask<std::string>> contender;
    JoinedTask<QueryResult> query([&] {
        QueryResult result;
        try {
            bool first = true;
            RowHook hook([&](sqlite3_stmt* statement) {
                if (!std::exchange(first, false)) return;
                busy.store(sqlite3_stmt_busy(statement));
                readonly.store(sqlite3_stmt_readonly(statement));
                row.send();
                require(release.wait(), "query row barrier expired");
            });
            result.rows = db.query(sql).size();
        } catch (const std::exception& error) { result.error = error.what(); }
        return result;
    });
    ReleaseOnExit unpause{release};
    Overlap result;
    result.row_reached = row.wait();
    if (result.row_reached) {
        contender = std::make_unique<JoinedTask<std::string>>([&] {
            const int rc = try_mutex(sqlite3_db_mutex(db.handle()));
            probe.store(rc);
            // Fixed code proves another thread owns the mutex before release.
            // Old code sees OK: let its actual COMMIT finish/fail BEFORE waking
            // the main thread, deterministically retaining the old rc5 path.
            if (rc == SQLITE_BUSY) observed.send();
            std::string error;
            try { writer(); }
            catch (const std::exception& failure) { error = failure.what(); }
            completed.store(true);
            observed.send();
            return error;
        });
        result.contender_observed = observed.wait();
        result.completed_before_release = completed.load();
    }
    release.send();
    result.query = query.get();
    if (contender) result.writer_error = contender->get();
    result.probe = probe.load();
    result.busy = busy.load();
    result.readonly = readonly.load();
    return result;
}

void expect_protected(const Overlap& result, int readonly) {
    EXPECT_TRUE(result.row_reached);
    EXPECT_TRUE(result.contender_observed);
    EXPECT_EQ(result.probe, SQLITE_BUSY);
    EXPECT_FALSE(result.completed_before_release);
    EXPECT_EQ(result.busy, 1);
    EXPECT_EQ(result.readonly, readonly);
    EXPECT_EQ(result.query.rows, 1u);
    EXPECT_EQ(result.query.error, "");
    EXPECT_EQ(result.writer_error, "");
}

using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
Statement held_row(database& db, const char* sql) {
    sqlite3_stmt* raw = nullptr;
    const int rc = sqlite3_prepare_v2(db.handle(), sql, -1, &raw, nullptr);
    Statement statement(raw, &sqlite3_finalize);
    require(rc == SQLITE_OK, "control prepare failed");
    require(sqlite3_step(raw) == SQLITE_ROW, "control did not reach ROW");
    return statement;
}

void mark_writes(database& db) {
    sqlite3_update_hook(db.handle(), [](void* context, int, const char*, const char*, sqlite3_int64) {
        static_cast<database*>(context)->mark_txn_dirty();
    }, &db);
}
} // namespace

TEST(QueryStatementLifetime, UnprotectedCheckpointReproducesExactCommitError) {
    Fixture f;
    auto statement = held_row(f.db, "PRAGMA wal_checkpoint(TRUNCATE)");
    ASSERT_EQ(sqlite3_stmt_busy(statement.get()), 1);
    ASSERT_EQ(sqlite3_stmt_readonly(statement.get()), 0);
    f.db.begin_transaction();
    f.db.execute("INSERT INTO QueryProbe VALUES(2,2)");
    std::string error;
    try { f.db.commit(); } catch (const db_error& failure) { error = failure.what(); }
    EXPECT_NE(error.find("cannot commit transaction - SQL statements in progress"), std::string::npos);
    EXPECT_EQ(sqlite3_extended_errcode(f.db.handle()), SQLITE_BUSY);
    EXPECT_TRUE(f.db.is_in_transaction());
    statement.reset();
    ASSERT_NO_THROW(f.db.commit());
    EXPECT_EQ(count(f.db), 2);
}

TEST(QueryStatementLifetime, ReadOnlyRowControlAllowsSameHandleCommit) {
    Fixture f;
    auto statement = held_row(f.db, "SELECT value FROM QueryProbe");
    ASSERT_EQ(sqlite3_stmt_readonly(statement.get()), 1);
    f.db.begin_transaction();
    f.db.execute("INSERT INTO QueryProbe VALUES(2,2)");
    ASSERT_NO_THROW(f.db.commit());
    EXPECT_FALSE(f.db.is_in_transaction());
    statement.reset();
    EXPECT_EQ(count(f.db), 2);
}

TEST(QueryStatementLifetime, CheckpointOnSeparateHandleAllowsWriterCommit) {
    Fixture f;
    database peer(f.file.path.string(), database::open_mode::read_write, 100);
    auto statement = held_row(peer, "PRAGMA wal_checkpoint(TRUNCATE)");
    ASSERT_EQ(sqlite3_stmt_readonly(statement.get()), 0);
    f.db.begin_transaction();
    f.db.execute("INSERT INTO QueryProbe VALUES(2,2)");
    ASSERT_NO_THROW(f.db.commit());
    statement.reset();
    EXPECT_EQ(count(f.db), 2);
}

TEST(QueryStatementLifetime, CheckpointQueryRetainsMutexUntilStatementFinalizes) {
    Fixture f;
    const auto result = overlap(f.db, "PRAGMA wal_checkpoint(TRUNCATE)", [&] {
        f.db.begin_transaction();
        f.db.execute("INSERT INTO QueryProbe VALUES(2,2)");
        f.db.commit();
    });
    expect_protected(result, 0);
    if (f.db.is_in_transaction()) f.db.rollback();
    EXPECT_EQ(sqlite3_next_stmt(f.db.handle(), nullptr), nullptr);
    EXPECT_EQ(count(f.db), 2);
}

TEST(QueryStatementLifetime, ReturningQueryPreventsConcurrentCommitAtRowGap) {
    Fixture f;
    f.db.begin_transaction();
    const auto result = overlap(f.db, "INSERT INTO QueryProbe VALUES(2,2) RETURNING id", [&] { f.db.commit(); });
    expect_protected(result, 0);
    if (f.db.is_in_transaction()) f.db.rollback();
    EXPECT_EQ(sqlite3_next_stmt(f.db.handle(), nullptr), nullptr);
    EXPECT_EQ(count(f.db), 2);
}

TEST(QueryStatementLifetime, ReadQueryPreservesAllBoundRows) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    db.execute("INSERT INTO QueryProbe VALUES(1,11),(2,22),(3,33)");
    const auto rows = db.query("SELECT id,value FROM QueryProbe WHERE id >= ? ORDER BY id", {int64_t(2)});
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("value")), 22);
    EXPECT_EQ(std::get<int64_t>(rows[1].at("value")), 33);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
}

TEST(QueryStatementLifetime, CppMaterializationFailureFinalizesAndUnlocks) {
    database db(":memory:");
    {
        RowHook hook([](sqlite3_stmt*) { throw std::bad_alloc(); });
        EXPECT_THROW(db.query("SELECT 1 AS value"), std::bad_alloc);
    }
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
    EXPECT_EQ(std::get<int64_t>(db.query("SELECT 2 AS value").at(0).at("value")), 2);
}

TEST(QueryStatementLifetime, ReturningMaterializationFailureCanRollbackWithoutPhantoms) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0;
    db.set_txn_hooks([&] { ++delivered; }, [] {});
    db.begin_transaction();
    {
        RowHook hook([](sqlite3_stmt*) { throw std::bad_alloc(); });
        EXPECT_THROW(db.query("INSERT INTO QueryProbe VALUES(1,1) RETURNING id"), std::bad_alloc);
    }
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
    EXPECT_TRUE(db.is_in_transaction());
    db.rollback();
    EXPECT_EQ(delivered, 0);
    EXPECT_EQ(count(db), 0);
    db.query("INSERT INTO QueryProbe VALUES(2,2) RETURNING id");
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(count(db), 1);
}

TEST(QueryStatementLifetime, PrepareAndStepErrorsLeaveNoStatementOrMutexOwner) {
    database db(":memory:");
    EXPECT_THROW(db.query("SELECT FROM invalid"), db_error);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
    ASSERT_EQ(sqlite3_create_function_v2(db.handle(), "query_fail", 0, SQLITE_UTF8, nullptr,
        [](sqlite3_context* context, int, sqlite3_value**) { sqlite3_result_error(context, "synthetic step error", -1); },
        nullptr, nullptr, nullptr), SQLITE_OK);
    std::string message;
    try { db.query("SELECT query_fail()"); } catch (const db_error& error) { message = error.what(); }
    EXPECT_EQ(message, "Query failed: synthetic step error");
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
    EXPECT_EQ(db.query("SELECT 1").size(), 1u);
}

TEST(QueryStatementLifetime, ReturningDeliveryRunsAfterUnlockAndCanReenter) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0, lock_result = -1;
    bool no_statement = false;
    int64_t seen = -1;
    db.set_txn_hooks([&] {
        ++delivered;
        lock_result = mutex_probe(db);
        no_statement = sqlite3_next_stmt(db.handle(), nullptr) == nullptr;
        seen = count(db);
    }, [] {});
    const auto rows = db.query("INSERT INTO QueryProbe VALUES(1,1) RETURNING id");
    EXPECT_EQ(rows.size(), 1u);
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(lock_result, SQLITE_OK);
    EXPECT_TRUE(no_statement);
    EXPECT_EQ(seen, 1);
}

TEST(QueryStatementLifetime, NestedStatementDefersDeliveryUntilOuterQueryUnlocks) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0, lock_result = -1;
    bool delivered_inside = false, first = true;
    db.set_txn_hooks([&] { ++delivered; lock_result = mutex_probe(db); }, [] {});
    {
        RowHook hook([&](sqlite3_stmt*) {
            if (!std::exchange(first, false)) return;
            db.execute("INSERT INTO QueryProbe VALUES(1,1)");
            delivered_inside = delivered != 0;
        });
        EXPECT_EQ(db.query("SELECT 1 AS value").size(), 1u);
    }
    EXPECT_FALSE(delivered_inside);
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(lock_result, SQLITE_OK);
    EXPECT_EQ(count(db), 1);
}

TEST(QueryStatementLifetime, DeliveryExceptionEscapesAfterFinalizeAndDoesNotWedge) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0;
    db.set_txn_hooks([&] {
        if (++delivered == 1) throw std::runtime_error("synthetic delivery exception");
    }, [] {});
    EXPECT_THROW(db.query("INSERT INTO QueryProbe VALUES(1,1) RETURNING id"), std::runtime_error);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(mutex_probe(db), SQLITE_OK);
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(count(db), 1);
    EXPECT_NO_THROW(db.query("INSERT INTO QueryProbe VALUES(2,2) RETURNING id"));
    EXPECT_EQ(delivered, 2);
    EXPECT_EQ(count(db), 2);
}

TEST(QueryStatementLifetime, OuterMaterializationFailureStillDeliversCommittedNestedWrite) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0, lock_result = -1;
    bool no_statement = false, delivered_inside = false;
    db.set_txn_hooks([&] {
        ++delivered;
        lock_result = mutex_probe(db);
        no_statement = sqlite3_next_stmt(db.handle(), nullptr) == nullptr;
    }, [] {});
    {
        RowHook hook([&](sqlite3_stmt*) {
            db.execute("INSERT INTO QueryProbe VALUES(1,1)");
            delivered_inside = delivered != 0;
            throw std::bad_alloc();
        });
        EXPECT_THROW(db.query("SELECT 1 AS value"), std::bad_alloc);
    }
    EXPECT_FALSE(delivered_inside);
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(lock_result, SQLITE_OK);
    EXPECT_TRUE(no_statement);
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(count(db), 1);
}

TEST(QueryStatementLifetime, AutocommitReturningFailureDeliversAfterFinalize) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0, lock_result = -1;
    bool no_statement = false;
    db.set_txn_hooks([&] {
        ++delivered;
        lock_result = mutex_probe(db);
        no_statement = sqlite3_next_stmt(db.handle(), nullptr) == nullptr;
    }, [] {});
    {
        RowHook hook([](sqlite3_stmt*) { throw std::bad_alloc(); });
        EXPECT_THROW(db.query("INSERT INTO QueryProbe VALUES(1,1) RETURNING id"), std::bad_alloc);
    }
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(lock_result, SQLITE_OK);
    EXPECT_TRUE(no_statement);
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(count(db), 1);
}

TEST(QueryStatementLifetime, DeliveryFailureDoesNotReplaceMaterializationException) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int delivered = 0, lock_result = -1;
    db.set_txn_hooks([&] {
        lock_result = mutex_probe(db);
        if (++delivered == 1) throw std::runtime_error("secondary delivery failure");
    }, [] {});
    {
        RowHook hook([&](sqlite3_stmt*) {
            db.execute("INSERT INTO QueryProbe VALUES(1,1)");
            throw std::bad_alloc();
        });
        EXPECT_THROW(db.query("SELECT 1 AS value"), std::bad_alloc);
    }
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(lock_result, SQLITE_OK);
    EXPECT_EQ(sqlite3_next_stmt(db.handle(), nullptr), nullptr);
    EXPECT_EQ(count(db), 1);
    EXPECT_NO_THROW(db.query("INSERT INTO QueryProbe VALUES(2,2) RETURNING id"));
    EXPECT_EQ(delivered, 2);
    EXPECT_EQ(count(db), 2);
}

namespace {
void install_step_failure(database& db, int& calls) {
    require(sqlite3_create_function_v2(db.handle(), "fail_second", 1, SQLITE_UTF8, &calls,
        [](sqlite3_context* context, int, sqlite3_value** values) {
            auto& n = *static_cast<int*>(sqlite3_user_data(context));
            if (++n == 2) sqlite3_result_error(context, "outer second-row failure", -1);
            else sqlite3_result_int(context, sqlite3_value_int(values[0]));
        }, nullptr, nullptr, nullptr) == SQLITE_OK, "register failure function");
}

bool is_outer(sqlite3_stmt* stmt) {
    const char* sql = sqlite3_sql(stmt);
    return sql && std::string(sql).find("/* outer */") != std::string::npos;
}

struct ObservedFixture {
    OwnedFile file;
    std::unique_ptr<lattice_db> owner;
    std::vector<std::string> ids;
    std::vector<int> locks;
    size_t calls = 0;
    lattice_db::observer_id observer;
    explicit ObservedFixture(bool disk = false) {
        configuration config;
        if (disk) config.path = file.path.string();
        owner = std::make_unique<lattice_db>(config);
        owner->db().execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY, globalId TEXT, value INTEGER)");
        observer = owner->add_table_observer("QueryProbe", [&](const std::vector<lattice_db::change_event>& events) {
            ++calls;
            locks.push_back(mutex_probe(owner->db()));
            for (const auto& event : events) ids.push_back(std::get<3>(event));
        });
    }
    ~ObservedFixture() { owner->remove_table_observer("QueryProbe", observer); }
    database& db() { return owner->db(); }
};
}

TEST(QueryStatementLifetime, CommittedNestedInsertSurvivesOuterSqlStepFailure) {
    ObservedFixture f;
    int scalar_calls = 0;
    install_step_failure(f.db(), scalar_calls);
    bool injected = false;
    {
        RowHook hook([&](sqlite3_stmt* stmt) {
            if (!is_outer(stmt) || std::exchange(injected, true)) return;
            f.db().execute("INSERT INTO QueryProbe VALUES(1,'committed',1)");
            EXPECT_TRUE(f.ids.empty());
        });
        EXPECT_THROW(f.db().query("SELECT fail_second(value) /* outer */ FROM (SELECT 1 AS value UNION ALL SELECT 2)"), db_error);
    }
    EXPECT_TRUE(injected);
    EXPECT_EQ(scalar_calls, 2);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"committed"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(f.db()), 1);
}

TEST(QueryStatementLifetime, CommittedNestedInsertSurvivesCaughtNestedSqlError) {
    ObservedFixture f;
    ASSERT_EQ(sqlite3_create_function_v2(f.db().handle(), "nested_fail", 0, SQLITE_UTF8, nullptr,
        [](sqlite3_context* context, int, sqlite3_value**) { sqlite3_result_error(context, "nested step failure", -1); },
        nullptr, nullptr, nullptr), SQLITE_OK);
    bool injected = false;
    {
        RowHook hook([&](sqlite3_stmt* stmt) {
            if (!is_outer(stmt) || std::exchange(injected, true)) return;
            f.db().execute("INSERT INTO QueryProbe VALUES(1,'committed',1)");
            EXPECT_THROW(f.db().query("SELECT nested_fail()"), db_error);
            EXPECT_TRUE(f.ids.empty());
        });
        EXPECT_EQ(f.db().query("SELECT 1 /* outer */").size(), 1u);
    }
    EXPECT_TRUE(injected);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"committed"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(f.db()), 1);
}

TEST(QueryStatementLifetime, LaterRollbackDiscardsOnlyItsCurrentTransaction) {
    ObservedFixture f;
    bool injected = false;
    {
        RowHook hook([&](sqlite3_stmt* stmt) {
            if (!is_outer(stmt) || std::exchange(injected, true)) return;
            f.db().execute("INSERT INTO QueryProbe VALUES(1,'committed',1)");
            f.db().begin_transaction();
            f.db().execute("INSERT INTO QueryProbe VALUES(2,'rolled-back',2)");
            f.db().rollback();
            EXPECT_TRUE(f.ids.empty());
        });
        EXPECT_EQ(f.db().query("SELECT 1 /* outer */").size(), 1u);
    }
    EXPECT_EQ(f.ids, (std::vector<std::string>{"committed"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(f.db()), 1);
    f.db().execute("INSERT INTO QueryProbe VALUES(3,'recovered',3)");
    EXPECT_EQ(f.ids, (std::vector<std::string>{"committed", "recovered"}));
}

TEST(QueryStatementLifetime, FileWalObserverRunsAfterQueryUnlock) {
    ObservedFixture f(true);
    EXPECT_EQ(f.db().query("INSERT INTO QueryProbe VALUES(1,'wal-returning',1) RETURNING id").size(), 1u);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"wal-returning"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    std::string retained_sql;
    auto* mutex = sqlite3_db_mutex(f.db().handle());
    sqlite3_mutex_enter(mutex);
    for (auto* stmt = sqlite3_next_stmt(f.db().handle(), nullptr); stmt;
         stmt = sqlite3_next_stmt(f.db().handle(), stmt)) {
        const std::string sql = sqlite3_sql(stmt) ? sqlite3_sql(stmt) : "";
        retained_sql += sql + "\n";
        EXPECT_EQ(sqlite3_stmt_busy(stmt), 0) << sql;
        EXPECT_EQ(sql.find("wal-returning"), std::string::npos) << sql;
    }
    sqlite3_mutex_leave(mutex);
    RecordProperty("idle_cached_statements", retained_sql);
}

TEST(QueryStatementLifetime, FileWalCommittedBatchSurvivesLaterNestedRollback) {
    ObservedFixture f(true);
    bool injected = false;
    {
        RowHook hook([&](sqlite3_stmt* stmt) {
            if (!is_outer(stmt) || std::exchange(injected, true)) return;
            f.db().execute("INSERT INTO QueryProbe VALUES(1,'wal-committed',1)");
            f.db().begin_transaction();
            f.db().execute("INSERT INTO QueryProbe VALUES(2,'wal-rolled-back',2)");
            f.db().rollback();
            EXPECT_TRUE(f.ids.empty());
        });
        f.db().query("SELECT 1 /* outer */");
    }
    EXPECT_EQ(f.ids, (std::vector<std::string>{"wal-committed"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(f.db()), 1);
}

TEST(QueryStatementLifetime, MultiStatementErrorPreservesEarlierCommit) {
    ObservedFixture f;
    EXPECT_THROW(f.db().execute("INSERT INTO QueryProbe VALUES(1,'batch-committed',1); SELECT absent_function()"), db_error);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"batch-committed"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(f.db()), 1);
}

TEST(QueryStatementLifetime, DeferredCommitsPreserveIndividualObserverBatches) {
    ObservedFixture f;
    f.db().execute("INSERT INTO QueryProbe VALUES(1,'one',1); INSERT INTO QueryProbe VALUES(2,'two',2)");
    EXPECT_EQ(f.calls, 2u);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"one", "two"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK, SQLITE_OK}));
}

TEST(QueryStatementLifetime, ReturningDoesNotSealWhileItsWriteTransactionIsActive) {
    ObservedFixture f;
    bool visited = false;
    {
        RowHook hook([&](sqlite3_stmt* stmt) {
            const char* sql = sqlite3_sql(stmt);
            if (!sql || std::string(sql).find("RETURNING") == std::string::npos) return;
            visited = true;
            EXPECT_EQ(sqlite3_get_autocommit(f.db().handle()), 1);
            EXPECT_EQ(sqlite3_txn_state(f.db().handle(), nullptr), SQLITE_TXN_WRITE);
            f.db().query("SELECT 1");
            EXPECT_TRUE(f.ids.empty());
        });
        f.db().query("INSERT INTO QueryProbe VALUES(1,'returning',1) RETURNING id");
    }
    EXPECT_TRUE(visited);
    EXPECT_EQ(f.ids, (std::vector<std::string>{"returning"}));
    EXPECT_EQ(f.locks, (std::vector<int>{SQLITE_OK}));
}

TEST(QueryStatementLifetime, TwoCallbackConsumerRetainsCommittedSignalAcrossLaterRollback) {
    database db(":memory:");
    db.execute("CREATE TABLE QueryProbe(id INTEGER PRIMARY KEY,value INTEGER)");
    mark_writes(db);
    int settled = 0, rolled_back = 0;
    std::vector<int> locks;
    db.set_txn_hooks([&] { ++settled; locks.push_back(mutex_probe(db)); }, [&] { ++rolled_back; });
    bool first = true;
    {
        RowHook hook([&](sqlite3_stmt*) {
            if (!std::exchange(first, false)) return;
            db.execute("INSERT INTO QueryProbe VALUES(1,1)");
            db.begin_transaction();
            db.execute("INSERT INTO QueryProbe VALUES(2,2)");
            db.rollback();
            EXPECT_EQ(settled, 0);
        });
        db.query("SELECT 1");
    }
    EXPECT_EQ(settled, 1);
    EXPECT_EQ(rolled_back, 1);
    EXPECT_EQ(locks, (std::vector<int>{SQLITE_OK}));
    EXPECT_EQ(count(db), 1);
}

namespace {
std::string statements(int begin, int count) {
    std::string sql;
    for (int i = begin; i < begin + count; ++i)
        sql += "INSERT INTO QueryProbe VALUES(" + std::to_string(i) + ",'id-" + std::to_string(i) + "'," + std::to_string(i) + ");";
    return sql;
}
void more_than_drain_cap(bool disk, bool nested) {
    ObservedFixture f(disk);
    bool injected = false;
    if (nested) {
        RowHook hook([&](sqlite3_stmt* stmt) {
            if (!is_outer(stmt) || std::exchange(injected, true)) return;
            for (int i = 1; i <= 65; ++i) f.db().execute(statements(i, 1));
            EXPECT_EQ(f.calls, 0u);
        });
        f.db().query("SELECT 1 /* outer */");
        EXPECT_TRUE(injected);
    } else f.db().execute(statements(1,65));
    EXPECT_EQ(count(f.db()), 65);
    EXPECT_EQ(f.calls, 65u) << "all committed batches must deliver before the writer returns";
    EXPECT_EQ(f.ids.size(), 65u);
    for (size_t i=0; i<f.ids.size(); ++i) EXPECT_EQ(f.ids[i], "id-" + std::to_string(i+1));
    EXPECT_TRUE(std::all_of(f.locks.begin(), f.locks.end(), [](int n) { return n == SQLITE_OK; }));
    // Reads cannot repair a lost pending signal. Record that state before any write.
    f.db().query("SELECT 1");
    EXPECT_EQ(f.calls, 65u) << "committed batch remained stranded across a subsequent read";
    f.db().execute(statements(66,1));
    EXPECT_EQ(f.calls, 66u) << "an unrelated write eventually drains any stranded work";
    EXPECT_EQ(f.ids.size(), 66u);
}
void throwing_first_batch(bool disk) {
    ObservedFixture f(disk);
    f.owner->remove_table_observer("QueryProbe",f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const std::vector<lattice_db::change_event>& events) {
        ++f.calls;
        f.locks.push_back(mutex_probe(f.db()));
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
        if (f.calls == 1) throw std::runtime_error("first observer batch failure");
    });
    try { f.db().execute(statements(1,3)); FAIL() << "first observer error must surface"; }
    catch (const std::runtime_error& e) { EXPECT_STREQ(e.what(), "first observer batch failure"); }
    EXPECT_EQ(count(f.db()),3);
    EXPECT_EQ(f.calls,3u) << "later already-committed batches must not be stranded by an earlier callback error";
    EXPECT_EQ(f.ids, (std::vector<std::string>{"id-1","id-2","id-3"}));
    EXPECT_TRUE(std::all_of(f.locks.begin(),f.locks.end(),[](int n){ return n==SQLITE_OK; }));
    f.db().query("SELECT 1");
    EXPECT_EQ(f.calls,3u);
    f.db().execute(statements(4,1));
    EXPECT_EQ(f.calls,4u);
}
}
TEST(QueryDrainQualification, Memory65CommittedStatementsDeliver) { more_than_drain_cap(false,false); }
TEST(QueryDrainQualification, File65CommittedStatementsDeliver) { more_than_drain_cap(true,false); }
TEST(QueryDrainQualification, Memory65NestedCommittedStatementsDeliver) { more_than_drain_cap(false,true); }
TEST(QueryDrainQualification, File65NestedCommittedStatementsDeliver) { more_than_drain_cap(true,true); }
TEST(QueryDrainQualification, MemoryThrowDoesNotStrandLaterCommittedBatches) { throwing_first_batch(false); }
TEST(QueryDrainQualification, FileThrowDoesNotStrandLaterCommittedBatches) { throwing_first_batch(true); }

TEST(QueryDrainQualification, ObserverCreates65BatchesInOneFeedbackRound) {
    ObservedFixture f;
    f.owner->remove_table_observer("QueryProbe", f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const auto& events) {
        ++f.calls;
        f.locks.push_back(mutex_probe(f.db()));
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
        if (f.calls == 1) f.db().execute(statements(2,65));
    });
    f.db().execute(statements(1,1));
    EXPECT_EQ(count(f.db()),66);
    EXPECT_EQ(f.calls,66u);
    ASSERT_EQ(f.ids.size(),66u);
    for (size_t i=0; i<f.ids.size(); ++i) EXPECT_EQ(f.ids[i],"id-"+std::to_string(i+1));
    EXPECT_TRUE(std::all_of(f.locks.begin(),f.locks.end(),[](int n){ return n==SQLITE_OK; }));
}

TEST(QueryDrainQualification, WriteOnEveryFireRemainsBoundedAndCanRecover) {
    ObservedFixture f;
    f.owner->remove_table_observer("QueryProbe", f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const auto& events) {
        ++f.calls;
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
        // No artificial callback limit: the production drain must bound itself.
        f.db().execute(statements(static_cast<int>(f.calls+1),1));
    });
    f.db().execute(statements(1,1));
    EXPECT_EQ(f.calls,64u);
    EXPECT_EQ(count(f.db()),65);
    f.owner->remove_table_observer("QueryProbe", f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const auto& events) {
        ++f.calls;
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
    });
    f.db().execute(statements(66,1));
    EXPECT_EQ(f.calls,66u);
    ASSERT_EQ(f.ids.size(),66u);
    for (size_t i=0; i<f.ids.size(); ++i) EXPECT_EQ(f.ids[i],"id-"+std::to_string(i+1));
}

TEST(QueryDrainQualification, FirstObserverErrorSurvivesLaterObserverErrors) {
    ObservedFixture f;
    f.owner->remove_table_observer("QueryProbe", f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const auto& events) {
        ++f.calls;
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
        throw std::runtime_error("observer-error-"+std::to_string(f.calls));
    });
    try { f.db().execute(statements(1,3)); FAIL(); }
    catch(const std::runtime_error& e) { EXPECT_STREQ(e.what(),"observer-error-1"); }
    EXPECT_EQ(f.calls,3u);
    EXPECT_EQ(f.ids,(std::vector<std::string>{"id-1","id-2","id-3"}));
}

TEST(QueryDrainQualification, SqlErrorRemainsPrimaryAfterDrainingAllCommittedBatches) {
    ObservedFixture f;
    f.owner->remove_table_observer("QueryProbe", f.observer);
    f.observer = f.owner->add_table_observer("QueryProbe", [&](const auto& events) {
        ++f.calls;
        for (const auto& event : events) f.ids.push_back(std::get<3>(event));
        if (f.calls == 1) throw std::runtime_error("secondary observer failure");
    });
    try { f.db().execute(statements(1,3)+"SELECT nonexistent_qualification_function()"); FAIL(); }
    catch(const db_error& e) { EXPECT_NE(std::string(e.what()).find("no such function"),std::string::npos); }
    EXPECT_EQ(f.calls,3u);
    EXPECT_EQ(f.ids,(std::vector<std::string>{"id-1","id-2","id-3"}));
    EXPECT_EQ(count(f.db()),3);
}

TEST(QueryDrainQualification, CrudErrorsRetainPrepareAndStepPrefixes) {
    database db(":memory:");
    auto prefix = [&](auto&& operation, const std::string& expected) {
        try { operation(); ADD_FAILURE() << "expected " << expected; }
        catch(const db_error& e) { EXPECT_EQ(std::string(e.what()).find(expected),0u) << e.what(); }
    };
    prefix([&]{db.insert("Absent",{{"value",int64_t(1)}});},"Failed to prepare insert: ");
    prefix([&]{db.update("Absent",1,{{"value",int64_t(1)}});},"Failed to prepare update: ");
    prefix([&]{db.remove("Absent",1);},"Failed to prepare delete: ");
    db.execute("CREATE TABLE CrudProbe(id INTEGER PRIMARY KEY,value INTEGER UNIQUE); INSERT INTO CrudProbe VALUES(1,1); INSERT INTO CrudProbe VALUES(2,2)");
    prefix([&]{db.insert("CrudProbe",{{"id",int64_t(3)},{"value",int64_t(1)}});},"Insert failed: ");
    prefix([&]{db.insert("CrudProbe",{{"id",int64_t(2)},{"value",int64_t(1)}},{"id"});},"Insert failed: ");
    prefix([&]{db.update("CrudProbe",2,{{"value",int64_t(1)}});},"Update failed: ");
    db.execute("CREATE TRIGGER prevent_delete BEFORE DELETE ON CrudProbe BEGIN SELECT RAISE(FAIL,'blocked delete'); END");
    prefix([&]{db.remove("CrudProbe",1);},"Delete failed: ");
}
