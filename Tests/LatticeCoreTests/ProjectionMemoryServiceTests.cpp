#include "TestHelpers.hpp"
#include <lattice/projection.hpp>
#include <future>
#include <cstdio>

// Public native memory capture service regression tests. The earlier
// explicitly unsupported foundation checkpoint remains preserved separately.
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <csignal>
#include <unistd.h>
#endif
namespace {
using namespace lattice;
using namespace std::chrono_literals;
int32_t code(projection_status status) { return static_cast<int32_t>(status); }
std::string unique_memory_suffix() {
    static std::atomic<uint64_t> next{0};
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
        "_" + std::to_string(next.fetch_add(1));
}

struct MemoryProjectionFixture {
    lattice_db db;
    explicit MemoryProjectionFixture(const std::string& path = ":memory:") : db(path) {
        db.db().execute("CREATE TABLE IF NOT EXISTS MemoryItem(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL, "
                        "title TEXT, category TEXT, n INTEGER, body TEXT)");
    }
    void add(int64_t id, const std::string& title, int64_t n = 1, const std::string& category = "same") {
        db.db().execute("INSERT INTO MemoryItem VALUES(?,?,?,?,?,?)",
            {id, fake_uuid(static_cast<int>(id)), title, category, n, std::string(64 * 1024, 'b')});
    }
    projection_query query(std::vector<std::string> columns = {"id", "title"}) {
        projection_query query;
        query.table = "MemoryItem"; query.columns = std::move(columns);
        query.order_by = "id ASC"; query.order_columns = {"id"};
        query.max_rows = 100; query.max_copied_bytes = 1024 * 1024; query.timeout_ms = 3000;
        return query;
    }
};

bool released(projection_read_operation& operation) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    auto* retained = new std::shared_ptr<std::promise<bool>>(promise);
    if (!operation.when_released(retained, [](void* value) {
        std::unique_ptr<std::shared_ptr<std::promise<bool>>> owned(
            static_cast<std::shared_ptr<std::promise<bool>>*>(value));
        (**owned).set_value(true);
    })) { delete retained; return false; }
    return future.wait_for(3s) == std::future_status::ready && future.get();
}
}

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
void first_pull_captures_and_releases_writer() {
    MemoryProjectionFixture fixture;
    fixture.add(1, "one"); fixture.add(2, "two");
    auto query = fixture.query({"title", "id", "title"});
    query.max_copied_bytes = 28; // Two (3 + 8 + 3)-byte rows, no unselected body.
    auto operation = fixture.db.start_projection(query);
    auto first = operation.next_batch(1);
    ASSERT_EQ(first.status_code(), code(projection_status::batch)) << first.error_message();
    ASSERT_EQ(first.row_count(), 1); EXPECT_EQ(first.cumulative_rows(), 1);
    EXPECT_EQ(first.cumulative_copied_bytes(), 14);
    EXPECT_EQ(std::get<std::string>(first.value(0, 0)), "one");
    EXPECT_EQ(std::get<std::string>(first.value(0, 2)), "one");
    auto writer = std::async(std::launch::async, [&] {
        fixture.db.db().execute("UPDATE MemoryItem SET title='changed' WHERE id=2");
    });
    const auto progress = writer.wait_for(1s);
    if (progress != std::future_status::ready) operation.close();
    ASSERT_EQ(progress, std::future_status::ready);
    writer.get();
    auto second = operation.next_batch(1);
    ASSERT_EQ(second.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(second.value(0, 0)), "two");
    EXPECT_EQ(second.cumulative_rows(), 2); EXPECT_EQ(second.cumulative_copied_bytes(), 28);
    auto end = operation.next_batch(1);
    EXPECT_EQ(end.status_code(), code(projection_status::done)); EXPECT_EQ(end.row_count(), 0);
    EXPECT_FALSE(operation.has_resources());
    EXPECT_EQ(std::get<std::string>(fixture.db.db().query("SELECT title FROM MemoryItem WHERE id=2")[0].at("title")), "changed");
}

} // namespace
TEST(ProjectionMemoryService, FirstPullCapturesSelectedValuesAndReleasesWriterBeforePause) {
    // A broken borrower may leave the writer/future destructor blocked. A fresh
    // executable child bounds that entire failure path without detaching it.
    struct RestoreDeathTestStyle {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~RestoreDeathTestStyle() { ::testing::FLAGS_gtest_death_test_style = std::move(previous); }
    } restore_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({
        std::signal(SIGALRM, SIG_DFL);
        sigset_t alarms;
        if (sigemptyset(&alarms) != 0 || sigaddset(&alarms, SIGALRM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &alarms, nullptr) != 0) _exit(2);
        alarm(5);
        first_pull_captures_and_releases_writer();
        const bool passed = !::testing::Test::HasFailure();
        if (passed) std::fputs("memory_first_pull_writer_released\n", stderr);
        _exit(passed ? 0 : 1);
    }, ::testing::ExitedWithCode(0), "memory_first_pull_writer_released");
}
#else
TEST(ProjectionMemoryService, FirstPullCapturesSelectedValuesAndReleasesWriterBeforePause) {
    GTEST_SKIP() << "Writer-lock regression requires native POSIX bounded child execution";
}
#endif

TEST(ProjectionMemoryService, PlainMemoryIsIsolatedAndNamedMemorySharesCommittedRows) {
    MemoryProjectionFixture first, second;
    first.add(1, "first"); second.add(1, "second");
    auto a = first.db.start_projection(first.query()).next_batch(10);
    auto b = second.db.start_projection(second.query()).next_batch(10);
    ASSERT_EQ(a.row_count(), 1); ASSERT_EQ(b.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(a.value(0, 1)), "first");
    EXPECT_EQ(std::get<std::string>(b.value(0, 1)), "second");
    const auto name = "file:projection_shared_" + unique_memory_suffix() + "?mode=memory&cache=shared";
    MemoryProjectionFixture shared_a(name), shared_b(name);
    shared_a.add(1, "shared");
    auto shared = shared_b.db.start_projection(shared_b.query()).next_batch(10);
    ASSERT_EQ(shared.status_code(), code(projection_status::done)) << shared.error_message();
    ASSERT_EQ(shared.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(shared.value(0, 1)), "shared");
}

TEST(ProjectionMemoryService, MixedAttachmentsPreserveEqualIdentityRowsInBothDirections) {
    for (bool memory_parent : {false, true}) {
        TempDB file("projection_mixed_memory");
        const auto memory = "file:projection_arm_" + unique_memory_suffix() + "?mode=memory&cache=shared";
        MemoryProjectionFixture parent(memory_parent ? memory : file.str());
        TempDB other_file("projection_mixed_file_arm");
        const auto other_memory = "file:projection_other_" + unique_memory_suffix() + "?mode=memory&cache=shared";
        MemoryProjectionFixture arm(memory_parent ? other_file.str() : other_memory);
        parent.add(1, "main"); arm.add(1, "arm"); // Same id and fake globalId.
        parent.db.attach(arm.db);
        auto query = parent.query(); query.order_by = "title ASC"; query.order_columns = {"title"};
        auto batch = parent.db.start_projection(query).next_batch(10);
        ASSERT_EQ(batch.status_code(), code(projection_status::done)) << batch.error_message();
        ASSERT_EQ(batch.row_count(), 2);
        EXPECT_EQ(std::get<std::string>(batch.value(0, 1)), "arm");
        EXPECT_EQ(std::get<std::string>(batch.value(1, 1)), "main");
        EXPECT_EQ(std::get<int64_t>(batch.value(0, 0)), std::get<int64_t>(batch.value(1, 0)));
    }
}

TEST(ProjectionMemoryService, JsonCollectionGroupDistinctAndBoundsUseExistingSemantics) {
    MemoryProjectionFixture fixture;
    fixture.add(1, "same", 9, "a"); fixture.add(2, "same", 9, "a"); fixture.add(3, "outside", 10, "b");
    fixture.db.db().execute("CREATE VIRTUAL TABLE _MemoryItem_location_rtree USING rtree(id,minLat,maxLat,minLon,maxLon)");
    fixture.db.db().execute("INSERT INTO _MemoryItem_location_rtree VALUES(1,0,0,0,0),(2,0,0,0,0),(3,9,9,9,9)");
    auto query = fixture.query({"title", "n"});
    query.where_clause = "id IN (SELECT value FROM json_each(?))";
    query.parameters = {std::string("[1,2,3]")};
    query.group_by = "category"; query.distinct_by = "title";
    query.order_by = "title ASC"; query.order_columns = {"title"};
    query.bounds = projection_bounds{"location", -1, 1, -1, 1};
    auto batch = fixture.db.start_projection(query).next_batch(10);
    ASSERT_EQ(batch.status_code(), code(projection_status::done)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "same");
    EXPECT_EQ(std::get<int64_t>(batch.value(0, 1)), 9);
}

TEST(ProjectionMemoryService, ZeroLimitAndCompleteCaptureBudgetsNeverYieldPartialSuccess) {
    MemoryProjectionFixture fixture; fixture.add(1, "one"); fixture.add(2, "two");
    auto query = fixture.query(); query.limit = 0; query.max_rows = 0;
    auto empty = fixture.db.start_projection(query).next_batch(1);
    EXPECT_EQ(empty.status_code(), code(projection_status::done)); EXPECT_EQ(empty.row_count(), 0);
    query = fixture.query(); query.max_rows = 1;
    auto rows = fixture.db.start_projection(query).next_batch(1);
    EXPECT_EQ(rows.status_code(), code(projection_status::row_budget_exceeded)); EXPECT_EQ(rows.row_count(), 0);
    query = fixture.query(); query.max_copied_bytes = 21; // Two rows need22.
    auto bytes = fixture.db.start_projection(query).next_batch(1);
    EXPECT_EQ(bytes.status_code(), code(projection_status::byte_budget_exceeded)); EXPECT_EQ(bytes.row_count(), 0);
    query = fixture.query(); query.max_capture_bytes = 64;
    auto backing = fixture.db.start_projection(query).next_batch(1);
    EXPECT_EQ(backing.status_code(), code(projection_status::capture_budget_exceeded)); EXPECT_EQ(backing.row_count(), 0);
    EXPECT_EQ(fixture.db.projection_resources_outstanding(), 0u);
    EXPECT_NO_THROW(fixture.db.db().execute("UPDATE MemoryItem SET n=n+1"));
}

TEST(ProjectionMemoryService, RetainedBatchReservationSurvivesCompletion) {
    MemoryProjectionFixture fixture; fixture.add(1, "one");
    auto query = fixture.query(); query.max_capture_bytes = 64 * 1024 * 1024;
    auto first = fixture.db.start_projection(query);
    auto retained = first.next_batch(10);
    ASSERT_EQ(retained.status_code(), code(projection_status::done)) << retained.error_message();
    EXPECT_FALSE(first.has_resources());
    // A consumer-owned chunk still charges the original parent's account.
    auto blocked = fixture.db.start_projection(query).next_batch(10);
    EXPECT_EQ(blocked.status_code(), code(projection_status::admission_rejected));
    retained = projection_read_batch{};
    auto reclaimed = fixture.db.start_projection(query).next_batch(10);
    EXPECT_EQ(reclaimed.status_code(), code(projection_status::done)) << reclaimed.error_message();
}

TEST(ProjectionMemoryService, BufferedCancelAcknowledgesReleaseAndLateCancelCannotInterruptWrite) {
    MemoryProjectionFixture fixture; fixture.add(1, "one"); fixture.add(2, "two");
    auto operation = fixture.db.start_projection(fixture.query());
    auto retained = operation.next_batch(1);
    ASSERT_EQ(retained.status_code(), code(projection_status::batch)) << retained.error_message();
    operation.cancel();
    ASSERT_TRUE(released(operation));
    EXPECT_FALSE(operation.has_resources());
    EXPECT_EQ(operation.next_batch(1).status_code(), code(projection_status::cancelled));
    operation.cancel();
    EXPECT_NO_THROW(fixture.db.db().execute("UPDATE MemoryItem SET title='after' WHERE id=1"));
    EXPECT_EQ(std::get<std::string>(retained.value(0, 1)), "one");
}

TEST(ProjectionMemoryService, OpenTransactionAndEscapedHandleFailWithoutChangingWriter) {
    MemoryProjectionFixture fixture; fixture.add(1, "one");
    fixture.db.begin_transaction();
    fixture.db.db().execute("UPDATE MemoryItem SET title='uncommitted'");
    auto refused = fixture.db.start_projection(fixture.query()).next_batch(1);
    EXPECT_EQ(refused.status_code(), code(projection_status::admission_rejected));
    EXPECT_TRUE(fixture.db.db().is_in_transaction());
    fixture.db.commit();
    auto committed = fixture.db.start_projection(fixture.query()).next_batch(10);
    ASSERT_EQ(committed.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(committed.value(0, 1)), "uncommitted");
    (void)fixture.db.db().handle();
    auto escaped = fixture.db.start_projection(fixture.query()).next_batch(1);
    EXPECT_EQ(escaped.status_code(), code(projection_status::unsupported));
    EXPECT_NO_THROW(fixture.db.db().execute("UPDATE MemoryItem SET title='escaped write works'"));
}

TEST(ProjectionMemoryService, ParentCloseReapsUndeliveredCaptureButRetainedValuesRemainValid) {
    MemoryProjectionFixture fixture; fixture.add(1, "one"); fixture.add(2, "two");
    auto operation = fixture.db.start_projection(fixture.query());
    auto retained = operation.next_batch(1);
    ASSERT_EQ(retained.status_code(), code(projection_status::batch)) << retained.error_message();
    fixture.db.close();
    ASSERT_TRUE(released(operation));
    EXPECT_FALSE(operation.has_resources());
    EXPECT_EQ(operation.next_batch(1).status_code(), code(projection_status::snapshot_expired));
    EXPECT_EQ(std::get<std::string>(retained.value(0, 1)), "one");
}
