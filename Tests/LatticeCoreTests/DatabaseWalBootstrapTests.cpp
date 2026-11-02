#include "TestHelpers.hpp"
#include <array>
#include <chrono>
#include <fstream>
#include <memory>

#if !defined(__EMSCRIPTEN__)
namespace lattice {
struct database_wal_test_access {
    static void enable(database& db) { db.enable_wal_mode(); }
};
}

namespace {
using namespace std::chrono_literals;
using clock_type = std::chrono::steady_clock;

struct receipt {
    std::mutex mutex;
    std::condition_variable cv;
    bool received = false;
    void signal() {
        std::lock_guard lock(mutex);
        received = true;
        cv.notify_all();
    }
    bool wait(std::chrono::milliseconds budget = 5s) {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, budget, [&] { return received; });
    }
};

void raw_execute(sqlite3* db, const char* sql) {
    char* error = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
    std::unique_ptr<char, decltype(&sqlite3_free)> owned(error, sqlite3_free);
    if (rc != SQLITE_OK) throw std::runtime_error(error ? error : sqlite3_errmsg(db));
}

// A real rollback-journal SHARED reader prevents the WAL transition. This is
// deliberately distinct from the constructor stress below: the helper tests
// have a causal trace receipt and deterministic release/cancellation ordering.
struct rollback_reader {
    TempDB file{"wal_bootstrap"};
    lattice::database owner;
    std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)> holder{nullptr, sqlite3_close_v2};
    explicit rollback_reader(int budget) : owner(file.str(), lattice::database::open_mode::read_write, budget) {
        owner.execute("CREATE TABLE wal_open_rows(value INTEGER)");
        owner.execute("PRAGMA journal_mode = DELETE");
        sqlite3* raw = nullptr;
        const int rc = sqlite3_open_v2(file.str().c_str(), &raw,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nullptr);
        holder.reset(raw);
        if (rc != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(raw));
        raw_execute(raw, "BEGIN; SELECT count(*) FROM wal_open_rows");
    }
    bool held() const { return sqlite3_get_autocommit(holder.get()) == 0; }
    void release() { raw_execute(holder.get(), "ROLLBACK"); }
};

struct wal_trace {
    sqlite3* db;
    receipt entered;
    explicit wal_trace(lattice::database& owner) : db(owner.handle()) {
        const int rc = sqlite3_trace_v2(db, SQLITE_TRACE_STMT,
            [](unsigned, void* context, void* statement, void*) -> int {
                const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
                if (sql && std::strcmp(sql, "PRAGMA journal_mode = WAL") == 0)
                    static_cast<wal_trace*>(context)->entered.signal();
                return 0;
            }, this);
        if (rc != SQLITE_OK) throw std::runtime_error("could not install WAL trace");
    }
    ~wal_trace() { sqlite3_trace_v2(db, 0, nullptr, nullptr); }
};

std::string exception_message(std::exception_ptr error) {
    if (!error) return {};
    try { std::rethrow_exception(error); }
    catch (const std::exception& caught) { return caught.what(); }
    catch (...) { return "non-std exception"; }
}

int statement_timeout(lattice::database& db) {
    return static_cast<int>(std::get<int64_t>(db.query("PRAGMA busy_timeout").at(0).at("timeout")));
}

void constructor_budget_failure(int budget) {
    rollback_reader fixture(2000);
    std::string error;
    const auto start = clock_type::now();
    try { lattice::database contender(fixture.file.str(), lattice::database::open_mode::read_write, budget); }
    catch (const lattice::db_error& caught) { error = caught.what(); }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::now() - start);
    EXPECT_NE(error.find("PRAGMA journal_mode = WAL"), std::string::npos) << error;
    EXPECT_NE(error.find("database is locked"), std::string::npos) << error;
    EXPECT_TRUE(fixture.held());
    EXPECT_LT(elapsed, 1500ms) << "budget=" << budget << " elapsed_ms=" << elapsed.count() << " error=" << error;
    if (budget > 0) EXPECT_GE(elapsed, std::chrono::milliseconds(budget / 2));
    fixture.release();
    // A failed constructor must release every connection and failed statement.
    lattice::database retry(fixture.file.str(), lattice::database::open_mode::read_write, 700);
    retry.execute("INSERT INTO wal_open_rows VALUES(7)");
    EXPECT_EQ(retry.query("SELECT value FROM wal_open_rows").size(), 1u);
    EXPECT_EQ(statement_timeout(retry), 700);
}
}

TEST(DatabaseWalBootstrap, HeldReaderReleaseAllowsHelperAndRestoresTimeout) {
    rollback_reader fixture(3000);
    wal_trace trace(fixture.owner);
    receipt done;
    std::exception_ptr failure;
    std::thread worker([&] {
        try { lattice::database_wal_test_access::enable(fixture.owner); }
        catch (...) { failure = std::current_exception(); }
        done.signal();
    });
    const bool attempted = trace.entered.wait();
    const bool finished_while_held = done.wait(75ms);
    const bool preserved = fixture.held();
    fixture.release();
    worker.join();
    ASSERT_TRUE(attempted);
    EXPECT_FALSE(finished_while_held);
    EXPECT_TRUE(preserved);
    ASSERT_FALSE(failure) << exception_message(failure);
    EXPECT_EQ(statement_timeout(fixture.owner), 3000);
    const auto mode = fixture.owner.query("PRAGMA journal_mode");
    EXPECT_EQ(std::get<std::string>(mode.at(0).at("journal_mode")), "wal");
    fixture.owner.execute("INSERT INTO wal_open_rows VALUES(9)");
}

TEST(DatabaseWalBootstrap, ConstructorExhaustsOneConfiguredBudgetAndReleasesLocks) {
    constructor_budget_failure(80);
}

TEST(DatabaseWalBootstrap, ZeroBudgetConstructorFailsImmediatelyAndReleasesLocks) {
    constructor_budget_failure(0);
}

TEST(DatabaseWalBootstrap, BoundCancellationSettlesHelperWithoutChangingOtherReader) {
    rollback_reader fixture(3000);
    auto cancellation = std::make_shared<std::atomic<bool>>(false);
    fixture.owner.bind_cancellation_token(cancellation);
    wal_trace trace(fixture.owner);
    receipt done;
    std::exception_ptr failure;
    std::thread worker([&] {
        try { lattice::database_wal_test_access::enable(fixture.owner); }
        catch (...) { failure = std::current_exception(); }
        done.signal();
    });
    const bool attempted = trace.entered.wait();
    const bool finished_before_cancel = done.wait(30ms);
    const auto cancelled_at = clock_type::now();
    cancellation->store(true, std::memory_order_release);
    worker.join();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::now() - cancelled_at);
    const bool preserved = fixture.held();
    fixture.release();
    EXPECT_TRUE(attempted);
    EXPECT_FALSE(finished_before_cancel);
    EXPECT_EQ(exception_message(failure), "database operation cancelled");
    EXPECT_LT(elapsed, 1500ms) << "cancel-to-settle_ms=" << elapsed.count();
    EXPECT_TRUE(preserved);
    lattice::database unrelated(fixture.file.str(), lattice::database::open_mode::read_write, 700);
    unrelated.execute("INSERT INTO wal_open_rows VALUES(11)");
    EXPECT_EQ(statement_timeout(unrelated), 700);
}

TEST(DatabaseWalBootstrap, NonBusyCorruptFileErrorIsNotRetried) {
    TempDB file{"wal_nonbusy"};
    { std::ofstream out(file.str()); out << "This is not a SQLite database"; }
    const auto start = clock_type::now();
    std::string error;
    try { lattice::database invalid(file.str(), lattice::database::open_mode::read_write, 3000); }
    catch (const lattice::db_error& caught) { error = caught.what(); }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::now() - start);
    EXPECT_NE(error.find("not a database"), std::string::npos) << error;
    EXPECT_LT(elapsed, 1500ms) << "elapsed_ms=" << elapsed.count() << " error=" << error;
}

TEST(DatabaseWalBootstrap, MemoryAndReadOnlyOpenKeepTheirJournalBehavior) {
    lattice::database memory(":memory:", lattice::database::open_mode::read_write, 0);
    EXPECT_EQ(std::get<std::string>(memory.query("PRAGMA journal_mode").at(0).at("journal_mode")), "memory");
    rollback_reader fixture(2000);
    lattice::database reader(fixture.file.str(), lattice::database::open_mode::read_only, 0);
    EXPECT_EQ(std::get<std::string>(reader.query("PRAGMA journal_mode").at(0).at("journal_mode")), "delete");
    EXPECT_TRUE(fixture.held());
    fixture.release();
}

TEST(DatabaseWalBootstrap, FreshConcurrentConstructorsWithDifferentBudgetsBothSucceed) {
    for (int round = 0; round < 100; ++round) {
        TempDB file{"wal_dual_open"};
        std::array<std::unique_ptr<lattice::database>, 2> handles;
        std::array<std::exception_ptr, 2> failures{};
        std::array<int64_t, 2> elapsed_us{};
        std::array<std::thread, 2> workers;
        std::mutex mutex;
        std::condition_variable cv;
        int ready = 0;
        bool go = false;
        for (int side = 0; side < 2; ++side) workers[side] = std::thread([&, side] {
            {
                std::unique_lock lock(mutex);
                ++ready;
                cv.notify_all();
                cv.wait(lock, [&] { return go; });
            }
            const auto start = clock_type::now();
            try {
                handles[side] = std::make_unique<lattice::database>(file.str(),
                    lattice::database::open_mode::read_write, side == 0 ? 30000 : 2000);
            } catch (...) { failures[side] = std::current_exception(); }
            elapsed_us[side] = std::chrono::duration_cast<std::chrono::microseconds>(clock_type::now() - start).count();
        });
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [&] { return ready == 2; });
            go = true;
        }
        cv.notify_all();
        for (auto& worker : workers) worker.join();
        // Both owners stay alive through both results, preventing an early
        // close/checkpoint from turning this into unrelated sequential opens.
        for (int side = 0; side < 2; ++side) {
            ASSERT_FALSE(failures[side]) << "round=" << round << " side=" << side
                << " elapsed_us=" << elapsed_us[side] << " error=" << exception_message(failures[side]);
            ASSERT_NE(handles[side], nullptr);
            EXPECT_EQ(statement_timeout(*handles[side]), side == 0 ? 30000 : 2000);
        }
    }
}
#endif
