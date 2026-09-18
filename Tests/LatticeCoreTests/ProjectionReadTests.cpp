#include "TestHelpers.hpp"
#include <lattice/projection.hpp>
#include <chrono>
#include <functional>
#include <limits>

namespace {
using namespace lattice;
using namespace std::chrono_literals;

int32_t status(projection_status value) { return static_cast<int32_t>(value); }

struct ProjectionFixture {
    TempDB path;
    lattice_db db;

    explicit ProjectionFixture(const std::string& name) : path(name), db(path.str()) {
        db.db().execute("CREATE TABLE Fixture (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL, "
                        "title TEXT, n INTEGER, body TEXT, payload BLOB)");
    }

    void add(int64_t id, const std::string& title, int64_t number = 7,
             const std::string& global_id = "") {
        db.db().execute("INSERT INTO Fixture VALUES (?, ?, ?, ?, ?, ?)",
            {id, global_id.empty() ? fake_uuid(static_cast<int>(id)) : global_id,
             title, number, std::string(64 * 1024, 'b'), std::vector<uint8_t>(64 * 1024, 0x5a)});
    }

    projection_query query(std::vector<std::string> columns = {"id"}) const {
        projection_query result;
        result.table = "Fixture";
        result.columns = std::move(columns);
        result.order_by = "id ASC";
        result.max_rows = 100;
        result.max_copied_bytes = 1024 * 1024;
        result.timeout_ms = 3000;
        return result;
    }
};

bool wait_until(const std::function<bool()>& predicate) {
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= end) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

// Acknowledgement must observe released resources and allow reentrant state
// inspection: invoking it while holding the operation mutex would deadlock.
struct ReleaseProbe : std::enable_shared_from_this<ReleaseProbe> {
    projection_read_operation operation;
    std::atomic<int> calls{0};
    std::atomic<bool> saw_resources{false};

    bool register_callback() {
        auto* retained = new std::shared_ptr<ReleaseProbe>(shared_from_this());
        if (operation.when_released(retained, released)) return true;
        delete retained;
        return false;
    }

    static void released(void* context) {
        // The callback owns this retain even if a failing assertion ends the
        // test before the watchdog acknowledges cleanup.
        std::unique_ptr<std::shared_ptr<ReleaseProbe>> retained(
            static_cast<std::shared_ptr<ReleaseProbe>*>(context));
        auto& probe = **retained;
        probe.saw_resources.store(probe.operation.has_resources());
        probe.calls.fetch_add(1);
    }
};
} // namespace

TEST(ProjectionRead, SelectedScalarsOmitLargeBodyAndBlobAndPreserveOrder) {
    ProjectionFixture fixture("projection_columns");
    fixture.add(1, "one");
    auto query = fixture.query({"title", "id", "n"});
    query.max_rows = 1;
    query.max_copied_bytes = 19; // "one" plus two 8-byte integers.
    auto operation = fixture.db.start_projection(query);
    auto batch = operation.next_batch(1);
    ASSERT_EQ(batch.status_code(), status(projection_status::batch)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 1);
    ASSERT_EQ(batch.column_count(), 3);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "one");
    EXPECT_EQ(std::get<int64_t>(batch.value(0, 1)), 1);
    EXPECT_EQ(std::get<int64_t>(batch.value(0, 2)), 7);
    EXPECT_EQ(batch.cumulative_copied_bytes(), 19);
    EXPECT_EQ(batch.cumulative_rows(), 1);
    EXPECT_TRUE(operation.has_resources());
    // A full batch does not step ahead. Completion is discovered on demand.
    auto completion = operation.next_batch(1);
    EXPECT_EQ(completion.status_code(), status(projection_status::done));
    EXPECT_EQ(completion.row_count(), 0);
    EXPECT_EQ(completion.cumulative_rows(), 1);
    EXPECT_EQ(completion.cumulative_copied_bytes(), 19);
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, BoundValuesAndSQLLimitHaveExactCompletionSemantics) {
    ProjectionFixture fixture("projection_bound_filter");
    fixture.add(1, "don't interpolate ?");
    fixture.add(2, "different");
    auto query = fixture.query({"title"});
    query.where_clause = "title = ?";
    query.parameters = {std::string("don't interpolate ?")};
    query.limit = 1;
    query.max_rows = 1;
    auto operation = fixture.db.start_projection(query);
    auto batch = operation.next_batch(1);
    ASSERT_EQ(batch.status_code(), status(projection_status::batch)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "don't interpolate ?");
    auto completion = operation.next_batch(1);
    EXPECT_EQ(completion.status_code(), status(projection_status::done));
    EXPECT_EQ(completion.row_count(), 0);
    EXPECT_EQ(completion.cumulative_rows(), 1);
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, EmptyAndFailedHandlesAcknowledgeReleaseImmediately) {
    auto empty = std::make_shared<ReleaseProbe>();
    EXPECT_TRUE(empty->operation.is_terminal());
    EXPECT_FALSE(empty->operation.has_resources());
    EXPECT_EQ(empty->operation.next_batch(1).status_code(), status(projection_status::invalid_request));
    ASSERT_TRUE(empty->register_callback());
    EXPECT_EQ(empty->calls.load(), 1);
    EXPECT_FALSE(empty->saw_resources.load());

    ProjectionFixture fixture("projection_failed_ack");
    auto invalid_query = fixture.query();
    invalid_query.columns.clear();
    auto invalid = std::make_shared<ReleaseProbe>();
    invalid->operation = fixture.db.start_projection(invalid_query);
    EXPECT_TRUE(invalid->operation.is_terminal());
    EXPECT_FALSE(invalid->operation.has_resources());
    ASSERT_TRUE(invalid->register_callback());
    EXPECT_EQ(invalid->calls.load(), 1);
    EXPECT_FALSE(invalid->saw_resources.load());
    EXPECT_EQ(invalid->operation.next_batch(1).status_code(), status(projection_status::invalid_request));
    EXPECT_EQ(invalid->calls.load(), 1);

    fixture.db.close();
    auto closed = std::make_shared<ReleaseProbe>();
    closed->operation = fixture.db.start_projection(fixture.query());
    EXPECT_TRUE(closed->operation.is_terminal());
    EXPECT_FALSE(closed->operation.has_resources());
    ASSERT_TRUE(closed->register_callback());
    EXPECT_EQ(closed->calls.load(), 1);
    EXPECT_FALSE(closed->saw_resources.load());
    EXPECT_EQ(closed->operation.next_batch(1).status_code(), status(projection_status::snapshot_expired));
    EXPECT_EQ(closed->calls.load(), 1);
}

TEST(ProjectionRead, RowAndByteBudgetsRejectInsteadOfSilentlyTruncating) {
    ProjectionFixture fixture("projection_budgets");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto rows = fixture.query();
    rows.max_rows = 1;
    auto row_operation = fixture.db.start_projection(rows);
    auto first = row_operation.next_batch(1);
    auto terminal = first.status_code() == status(projection_status::batch)
        ? row_operation.next_batch(1) : first;
    EXPECT_EQ(terminal.status_code(), status(projection_status::row_budget_exceeded));
    EXPECT_LE(terminal.cumulative_rows(), 1);
    EXPECT_FALSE(row_operation.has_resources());

    auto bytes = fixture.query({"body"});
    bytes.max_copied_bytes = 8;
    auto byte_operation = fixture.db.start_projection(bytes);
    auto oversized = byte_operation.next_batch(1);
    EXPECT_EQ(oversized.status_code(), status(projection_status::byte_budget_exceeded));
    EXPECT_EQ(oversized.row_count(), 0);
    EXPECT_EQ(oversized.cumulative_rows(), 0);
    EXPECT_EQ(oversized.cumulative_copied_bytes(), 0);
    EXPECT_FALSE(byte_operation.has_resources());
}

TEST(ProjectionRead, NullAndEmptyCellsRemainDistinct) {
    ProjectionFixture fixture("projection_nulls");
    fixture.db.db().execute("INSERT INTO Fixture VALUES (1, 'null-row', NULL, NULL, '', X'')");
    auto operation = fixture.db.start_projection(fixture.query({"title", "n", "body", "payload"}));
    auto batch = operation.next_batch(10);
    ASSERT_EQ(batch.status_code(), status(projection_status::done));
    ASSERT_EQ(batch.row_count(), 1);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(batch.value(0, 0)));
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(batch.value(0, 1)));
    EXPECT_EQ(std::get<std::string>(batch.value(0, 2)), "");
    EXPECT_TRUE(std::get<std::vector<uint8_t>>(batch.value(0, 3)).empty());
    EXPECT_EQ(batch.cumulative_copied_bytes(), 0);
}

TEST(ProjectionRead, BatchesRetainCommittedSnapshotAcrossMutation) {
    ProjectionFixture fixture("projection_snapshot");
    fixture.add(1, "one"); fixture.add(2, "two"); fixture.add(3, "three");
    auto operation = fixture.db.start_projection(fixture.query({"id", "title"}));
    auto first = operation.next_batch(1);
    ASSERT_EQ(first.status_code(), status(projection_status::batch));
    ASSERT_EQ(first.row_count(), 1);
    EXPECT_TRUE(operation.has_resources());
    // Use a second connection: the cursor must hold a committed WAL snapshot.
    lattice_db writer(fixture.path.str());
    writer.db().execute("UPDATE Fixture SET title = 'changed' WHERE id = 2");
    writer.db().execute("DELETE FROM Fixture WHERE id = 3");
    writer.db().execute("INSERT INTO Fixture(id, globalId, title, n) VALUES(4, 'later', 'four', 4)");
    auto rest = operation.next_batch(10);
    ASSERT_EQ(rest.status_code(), status(projection_status::done)) << rest.error_message();
    ASSERT_EQ(rest.row_count(), 2);
    EXPECT_EQ(std::get<int64_t>(rest.value(0, 0)), 2);
    EXPECT_EQ(std::get<std::string>(rest.value(0, 1)), "two");
    EXPECT_EQ(std::get<int64_t>(rest.value(1, 0)), 3);
    EXPECT_EQ(std::get<std::string>(rest.value(1, 1)), "three");
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, AttachedRowsWithEqualLocalIDsAndUUIDsAreNotCollapsed) {
    ProjectionFixture main("projection_main"), arm("projection_arm");
    main.add(1, "main", 10, "same-uuid");
    arm.add(1, "arm", 20, "same-uuid");
    main.db.attach(arm.db);
    auto query = main.query({"id", "globalId", "title"});
    query.order_by = "title ASC";
    auto operation = main.db.start_projection(query);
    auto batch = operation.next_batch(10);
    ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 2);
    EXPECT_EQ(std::get<int64_t>(batch.value(0, 0)), 1);
    EXPECT_EQ(std::get<int64_t>(batch.value(1, 0)), 1);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 1)), "same-uuid");
    EXPECT_EQ(std::get<std::string>(batch.value(1, 1)), "same-uuid");
    EXPECT_EQ(std::get<std::string>(batch.value(0, 2)), "arm");
    EXPECT_EQ(std::get<std::string>(batch.value(1, 2)), "main");
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, IncompleteBoundsAndInvalidFieldsFailExplicitly) {
    ProjectionFixture fixture("projection_unsupported");
    fixture.add(1, "one");
    auto incomplete = fixture.query();
    incomplete.has_bounds = true;
    auto operation = fixture.db.start_projection(incomplete);
    EXPECT_EQ(operation.next_batch(1).status_code(), status(projection_status::invalid_request));
    EXPECT_FALSE(operation.has_resources());
    auto missing = fixture.db.start_projection(fixture.query({"missing"}));
    EXPECT_EQ(missing.next_batch(1).status_code(), status(projection_status::schema_changed));
    EXPECT_FALSE(missing.has_resources());
    lattice_db memory;
    auto unsupported = memory.start_projection(fixture.query());
    EXPECT_EQ(unsupported.next_batch(1).status_code(), status(projection_status::unsupported));
    EXPECT_FALSE(unsupported.has_resources());
}

TEST(ProjectionRead, GroupDistinctAndNestedShapesMatchStoredValueQueries) {
    ProjectionFixture fixture("projection_group_shapes");
    fixture.add(1, "a", 1); fixture.add(2, "a", 1);
    fixture.add(3, "b", 2); fixture.add(4, "b", 2);
    fixture.add(5, "c", 3); fixture.add(6, "c", 3);
    for (int shape = 0; shape != 3; ++shape) {
        auto query = fixture.query({"n", "title", "n"});
        query.where_clause = "n >= ?";
        query.parameters = {int64_t(1)};
        query.order_by = "title DESC, id ASC";
        query.order_columns = {"title", "id"};
        if (shape != 1) query.group_by = "title";
        if (shape != 0) query.distinct_by = "n";
        query.offset = 1; query.limit = 2;
        query.max_rows = 2; query.max_copied_bytes = 34;
        auto reference = fixture.db.query_rows("Fixture", query.where_clause, query.order_by,
            query.limit, query.offset,
            query.group_by.empty() ? std::nullopt : std::optional<std::string>(query.group_by),
            query.distinct_by.empty() ? std::nullopt : std::optional<std::string>(query.distinct_by), query.parameters);
        auto operation = fixture.db.start_projection(query);
        auto batch = operation.next_batch(10);
        ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
        ASSERT_EQ(batch.row_count(), reference.size());
        ASSERT_EQ(batch.column_count(), 3);
        for (int64_t row = 0; row != batch.row_count(); ++row) {
            EXPECT_EQ(batch.value(row, 0), reference[row].at("n"));
            EXPECT_EQ(batch.value(row, 1), reference[row].at("title"));
            EXPECT_EQ(batch.value(row, 2), reference[row].at("n"));
        }
        EXPECT_EQ(batch.cumulative_copied_bytes(), 34);
        EXPECT_FALSE(operation.has_resources());
    }
}

TEST(ProjectionRead, NestedGroupingRequiresValidExplicitOrderDependencies) {
    ProjectionFixture fixture("projection_order_dependencies");
    fixture.add(1, "one");
    auto query = fixture.query({"title"});
    query.group_by = "title"; query.distinct_by = "n";
    auto no_metadata = fixture.db.start_projection(query);
    EXPECT_EQ(no_metadata.next_batch(10).status_code(), status(projection_status::invalid_request));
    query.order_columns = {"missing"};
    auto missing = fixture.db.start_projection(query);
    EXPECT_EQ(missing.next_batch(10).status_code(), status(projection_status::schema_changed));
    query.order_columns = {"title"}; // id is still absent from narrowed inner row.
    auto incomplete = fixture.db.start_projection(query);
    EXPECT_EQ(incomplete.next_batch(10).status_code(), status(projection_status::database_failure));
    query.order_columns = {"id"};
    query.group_by = "missing";
    auto bad_group = fixture.db.start_projection(query);
    EXPECT_EQ(bad_group.next_batch(10).status_code(), status(projection_status::schema_changed));
}

namespace {
void create_projection_bounds(ProjectionFixture& fixture, bool list = false) {
    fixture.db.db().execute("CREATE VIRTUAL TABLE _Fixture_location_rtree USING rtree(id,minLat,maxLat,minLon,maxLon)");
    if (list) fixture.db.db().execute("CREATE TABLE _Fixture_location(id INTEGER PRIMARY KEY,parent_id TEXT)");
}
void add_projection_bounds(ProjectionFixture& fixture, int64_t id, double lat, double lon,
                           const std::string& parent = "") {
    fixture.db.db().execute("INSERT INTO _Fixture_location_rtree VALUES(?,?,?,?,?)", {id, lat, lat, lon, lon});
    if (!parent.empty()) fixture.db.db().execute("INSERT INTO _Fixture_location VALUES(?,?)", {id, parent});
}
projection_query bounded_projection(ProjectionFixture& fixture, std::vector<std::string> columns = {"id"}) {
    auto query = fixture.query(std::move(columns));
    query.bounds = projection_bounds{"location", -1, 1, -1, 1};
    query.order_by = "Fixture.id ASC";
    query.order_columns = {"id"};
    return query;
}
} // namespace

TEST(ProjectionRead, BoundsPreservePredicateBindingOrderLimitAndFinitePrecision) {
    ProjectionFixture fixture("projection_bounds_parameters");
    fixture.add(1, "keep", 1); fixture.add(2, "keep", 2); fixture.add(3, "exclude", 3);
    create_projection_bounds(fixture);
    add_projection_bounds(fixture, 1, 0, 0);
    add_projection_bounds(fixture, 2, 0.5, 0.5);
    add_projection_bounds(fixture, 3, 0, 0);
    auto query = bounded_projection(fixture, {"title", "n"});
    query.where_clause = "title = ? AND n >= ?";
    query.parameters = {std::string("keep"), int64_t(1)};
    query.offset = 1; query.limit = 1;
    query.max_copied_bytes = 12;
    auto operation = fixture.db.start_projection(query);
    auto batch = operation.next_batch(10);
    ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 1);
    EXPECT_EQ(std::get<int64_t>(batch.value(0, 1)), 2);
    EXPECT_EQ(batch.cumulative_copied_bytes(), 12);

    // Existing decimal-string interpolation rounded this negative max to zero,
    // incorrectly accepting the row at latitude zero. Bound REAL preserves it.
    query = bounded_projection(fixture);
    query.bounds->max_lat = -0.0000001;
    auto precise = fixture.db.start_projection(query);
    auto empty = precise.next_batch(10);
    EXPECT_EQ(empty.status_code(), status(projection_status::done));
    EXPECT_EQ(empty.row_count(), 0);
}

TEST(ProjectionRead, BoundsListsDeduplicateModelsWithoutCollapsingEqualProjectedValues) {
    ProjectionFixture fixture("projection_bounds_list");
    fixture.add(1, "same"); fixture.add(2, "same");
    create_projection_bounds(fixture, true);
    add_projection_bounds(fixture, 1, 0, 0, fake_uuid(1));
    add_projection_bounds(fixture, 2, 0.5, 0.5, fake_uuid(1));
    add_projection_bounds(fixture, 3, 0, 0, fake_uuid(2));
    auto operation = fixture.db.start_projection(bounded_projection(fixture, {"title"}));
    auto batch = operation.next_batch(10);
    ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 2);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "same");
    EXPECT_EQ(std::get<std::string>(batch.value(1, 0)), "same");
}

TEST(ProjectionRead, BoundsDistinctAndGroupComposeBeforeOrderingAndOffset) {
    ProjectionFixture fixture("projection_bounds_combinations");
    create_projection_bounds(fixture, true);
    for (int64_t id = 1; id <= 6; ++id) {
        fixture.add(id, id <= 2 ? "a" : id <= 4 ? "b" : "c", (id + 1) / 2);
        add_projection_bounds(fixture, id * 2, 0, 0, fake_uuid(static_cast<int>(id)));
        add_projection_bounds(fixture, id * 2 + 1, 0.5, 0.5, fake_uuid(static_cast<int>(id)));
    }
    for (int shape = 0; shape != 3; ++shape) {
        auto query = bounded_projection(fixture, {"title", "n"});
        if (shape != 1) query.group_by = "title";
        if (shape != 0) query.distinct_by = "n";
        query.order_by = "title DESC, Fixture.id ASC";
        query.order_columns = {"title", "id"};
        query.offset = 1; query.limit = 1; query.max_copied_bytes = 9;
        auto operation = fixture.db.start_projection(query);
        auto batch = operation.next_batch(10);
        ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
        ASSERT_EQ(batch.row_count(), 1);
        EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "b");
        EXPECT_EQ(std::get<int64_t>(batch.value(0, 1)), 2);
        EXPECT_EQ(batch.cumulative_copied_bytes(), 9);
    }
}

TEST(ProjectionRead, AttachedBoundsUsePhysicalRtreesAndPreserveEqualIdentityRoutes) {
    ProjectionFixture main("projection_bbox_main"), arm("projection_bbox_arm");
    main.add(1, "main", 1, "same-id"); arm.add(1, "arm", 1, "same-id");
    create_projection_bounds(main); create_projection_bounds(arm, true);
    add_projection_bounds(main, 1, 10, 10);
    add_projection_bounds(arm, 1, 0, 0, "same-id");
    add_projection_bounds(arm, 2, 0.5, 0.5, "same-id");
    main.db.attach(arm.db);
    auto query = bounded_projection(main, {"title"});
    query.order_by = "title ASC"; query.order_columns = {"title"};
    auto operation = main.db.start_projection(query);
    auto batch = operation.next_batch(10);
    ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    ASSERT_EQ(batch.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "arm");
    main.db.db().execute("UPDATE main._Fixture_location_rtree SET minLat=0,maxLat=0,minLon=0,maxLon=0 WHERE id=1");
    auto both = main.db.start_projection(query);
    auto both_rows = both.next_batch(10);
    ASSERT_EQ(both_rows.status_code(), status(projection_status::done)) << both_rows.error_message();
    ASSERT_EQ(both_rows.row_count(), 2);
    EXPECT_EQ(std::get<std::string>(both_rows.value(0, 0)), "arm");
    EXPECT_EQ(std::get<std::string>(both_rows.value(1, 0)), "main");
}

TEST(ProjectionRead, BoundsValidateCoordinatesAndEveryPhysicalIndex) {
    ProjectionFixture fixture("projection_bbox_invalid");
    fixture.add(1, "one");
    auto query = bounded_projection(fixture);
    for (int kind = 0; kind != 3; ++kind) {
        auto invalid = query;
        if (kind == 0) invalid.bounds->max_lat = std::numeric_limits<double>::infinity();
        if (kind == 1) invalid.bounds->min_lat = 2;
        if (kind == 2) invalid.bounds->column.clear();
        auto operation = fixture.db.start_projection(invalid);
        EXPECT_EQ(operation.next_batch(10).status_code(), status(projection_status::invalid_request));
        EXPECT_FALSE(operation.has_resources());
    }
    auto missing = fixture.db.start_projection(query);
    EXPECT_EQ(missing.next_batch(10).status_code(), status(projection_status::schema_changed));
    EXPECT_FALSE(missing.has_resources());
    create_projection_bounds(fixture);
    ProjectionFixture arm("projection_bbox_missing_arm"); arm.add(1, "arm");
    fixture.db.attach(arm.db);
    auto missing_arm = fixture.db.start_projection(query);
    EXPECT_EQ(missing_arm.next_batch(10).status_code(), status(projection_status::schema_changed));
}

TEST(ProjectionRead, AttachedOnlyBoundsUseTheAttachedSchemaForScalarAndListStorage) {
    for (bool list : {false, true}) {
        TempDB receiver_path("projection_bbox_attached_only");
        lattice_db receiver(receiver_path.str());
        ProjectionFixture arm("projection_bbox_only_arm");
        arm.add(1, "attached");
        create_projection_bounds(arm, list);
        add_projection_bounds(arm, 1, 0, 0, list ? fake_uuid(1) : "");
        receiver.attach(arm.db);
        auto query = bounded_projection(arm, {"title"});
        auto operation = receiver.start_projection(query);
        auto batch = operation.next_batch(10);
        ASSERT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
        ASSERT_EQ(batch.row_count(), 1);
        EXPECT_EQ(std::get<std::string>(batch.value(0, 0)), "attached");
        EXPECT_FALSE(operation.has_resources());
    }
}

TEST(ProjectionRead, BoundedGroupedCursorKeepsSnapshotBudgetsAndCancellation) {
    ProjectionFixture fixture("projection_bbox_snapshot");
    fixture.add(1, "a", 1); fixture.add(2, "b", 2);
    create_projection_bounds(fixture);
    add_projection_bounds(fixture, 1, 0, 0); add_projection_bounds(fixture, 2, 0, 0);
    auto query = bounded_projection(fixture, {"title"});
    query.group_by = "title"; query.distinct_by = "n";
    query.max_copied_bytes = 2;
    auto operation = fixture.db.start_projection(query);
    auto first = operation.next_batch(1);
    ASSERT_EQ(first.status_code(), status(projection_status::batch)) << first.error_message();
    lattice_db writer(fixture.path.str());
    writer.db().execute("UPDATE _Fixture_location_rtree SET minLat=10,maxLat=10,minLon=10,maxLon=10 WHERE id=2");
    auto rest = operation.next_batch(10);
    ASSERT_EQ(rest.status_code(), status(projection_status::done)) << rest.error_message();
    ASSERT_EQ(rest.row_count(), 1);
    EXPECT_EQ(std::get<std::string>(rest.value(0, 0)), "b");
    EXPECT_EQ(rest.cumulative_copied_bytes(), 2);
    query.max_copied_bytes = 0;
    auto bytes = fixture.db.start_projection(query);
    EXPECT_EQ(bytes.next_batch(10).status_code(), status(projection_status::byte_budget_exceeded));
    query.max_copied_bytes = 2; query.max_rows = 0;
    auto rows = fixture.db.start_projection(query);
    EXPECT_EQ(rows.next_batch(10).status_code(), status(projection_status::row_budget_exceeded));
    query.max_rows = 2;
    auto cancelled = fixture.db.start_projection(query);
    cancelled.cancel();
    EXPECT_EQ(cancelled.next_batch(10).status_code(), status(projection_status::cancelled));
    EXPECT_FALSE(cancelled.has_resources());
}

TEST(ProjectionRead, IdleCancellationAcknowledgesReleaseWithoutAnotherNext) {
    ProjectionFixture fixture("projection_idle_cancel");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto probe = std::make_shared<ReleaseProbe>();
    probe->operation = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(probe->operation.next_batch(1).status_code(), status(projection_status::batch));
    ASSERT_TRUE(probe->operation.has_resources());
    ASSERT_TRUE(probe->register_callback());
    probe->operation.cancel();
    probe->operation.cancel();
    probe->operation.close();
    ASSERT_TRUE(wait_until([&] { return probe->calls.load() == 1; }));
    EXPECT_FALSE(probe->saw_resources.load());
    EXPECT_FALSE(probe->operation.has_resources());
    EXPECT_EQ(probe->operation.next_batch(1).status_code(), status(projection_status::cancelled));
    EXPECT_EQ(probe->calls.load(), 1);
}

TEST(ProjectionRead, DeadlineReapsIdleCursorAndCompletionRegistrationIsImmediate) {
    ProjectionFixture fixture("projection_idle_deadline");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto query = fixture.query();
    query.timeout_ms = 100;
    auto operation = fixture.db.start_projection(query);
    ASSERT_EQ(operation.next_batch(1).status_code(), status(projection_status::batch));
    ASSERT_TRUE(wait_until([&] { return operation.is_terminal() && !operation.has_resources(); }));
    EXPECT_EQ(operation.next_batch(1).status_code(), status(projection_status::deadline_exceeded));
    auto probe = std::make_shared<ReleaseProbe>();
    probe->operation = operation;
    ASSERT_TRUE(probe->register_callback());
    EXPECT_EQ(probe->calls.load(), 1);
    EXPECT_FALSE(probe->saw_resources.load());
}

TEST(ProjectionRead, CancellingRetiredOperationCannotInterruptANewLease) {
    ProjectionFixture fixture("projection_late_cancel");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto old = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(old.next_batch(10).status_code(), status(projection_status::done));
    EXPECT_FALSE(old.has_resources());
    auto current = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(current.next_batch(1).status_code(), status(projection_status::batch));
    old.cancel(); old.close();
    auto last = current.next_batch(10);
    EXPECT_EQ(last.status_code(), status(projection_status::done)) << last.error_message();
    ASSERT_EQ(last.row_count(), 1);
    EXPECT_EQ(std::get<int64_t>(last.value(0, 0)), 2);
}

TEST(ProjectionRead, ActiveLeaseAdmissionIsBoundedAndRestoredAfterRelease) {
    ProjectionFixture fixture("projection_admission");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto first = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(first.next_batch(1).status_code(), status(projection_status::batch));
    auto second = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(second.next_batch(1).status_code(), status(projection_status::batch));
    auto rejected = fixture.db.start_projection(fixture.query());
    EXPECT_EQ(rejected.next_batch(1).status_code(), status(projection_status::admission_rejected));
    EXPECT_FALSE(rejected.has_resources());
    first.close();
    ASSERT_TRUE(wait_until([&] { return !first.has_resources(); }));
    auto replacement = fixture.db.start_projection(fixture.query());
    EXPECT_EQ(replacement.next_batch(10).status_code(), status(projection_status::done));
    second.close();
    ASSERT_TRUE(wait_until([&] { return !second.has_resources(); }));
}

TEST(ProjectionRead, ParentRetirementAndCloseReleaseOutstandingOperations) {
    ProjectionFixture fixture("projection_parent_lifecycle");
    fixture.add(1, "one"); fixture.add(2, "two");
    auto retired = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(retired.next_batch(1).status_code(), status(projection_status::batch));
    fixture.db.retire_all_read_generations();
    ASSERT_TRUE(wait_until([&] { return !retired.has_resources(); }));
    EXPECT_EQ(retired.next_batch(1).status_code(), status(projection_status::snapshot_expired));
    auto closing = fixture.db.start_projection(fixture.query());
    ASSERT_EQ(closing.next_batch(1).status_code(), status(projection_status::batch));
    fixture.db.close();
    EXPECT_FALSE(closing.has_resources());
    EXPECT_EQ(closing.next_batch(1).status_code(), status(projection_status::snapshot_expired));
}

TEST(ProjectionRead, AdmissionRejectsMetadataOverflowAndBoundsPendingHandles) {
    ProjectionFixture fixture("projection_admission");
    fixture.add(1, "one");
    auto overflow = fixture.query({"id", "title", "n"});
    overflow.max_rows = std::numeric_limits<int64_t>::max();
    auto invalid = fixture.db.start_projection(overflow);
    EXPECT_EQ(invalid.next_batch(1).status_code(), status(projection_status::invalid_request));
    EXPECT_FALSE(invalid.has_resources());

    std::vector<projection_read_operation> pending;
    for (int i = 0; i < 64; ++i) pending.push_back(fixture.db.start_projection(fixture.query()));
    auto rejected = fixture.db.start_projection(fixture.query());
    EXPECT_EQ(rejected.next_batch(1).status_code(), status(projection_status::admission_rejected));
    EXPECT_FALSE(rejected.has_resources());
    for (const auto& operation : pending) operation.close();
}

TEST(ProjectionRead, LongRepresentableDeadlineIsHonoredAndOverflowIsRejected) {
    ProjectionFixture fixture("projection_timeout_range");
    fixture.add(1, "one");
    auto query = fixture.query();
    query.timeout_ms = 48LL * 60 * 60 * 1000;
    auto operation = fixture.db.start_projection(query);
    EXPECT_EQ(operation.next_batch(2).status_code(), status(projection_status::done));
    EXPECT_FALSE(operation.has_resources());
    query.timeout_ms = std::numeric_limits<int64_t>::max();
    auto overflow = fixture.db.start_projection(query);
    EXPECT_EQ(overflow.next_batch(1).status_code(), status(projection_status::invalid_request));
    EXPECT_FALSE(overflow.has_resources());
}

namespace lattice {
// Source-only qualification seam: no runtime pause hook is installed in WAL
// callbacks. This exposes just the reader protocol phases for interleavings.
struct projection_pressure_test_access {
    static unsigned before_increment(lattice_db& db) { return db.projection_pressure_slot_.load(); }
    static bool finish_delayed_reader(lattice_db& db, unsigned slot) {
        db.projection_pressure_readers_[slot].fetch_add(1);
        bool valid = false;
        if (slot == db.projection_pressure_slot_.load()) {
            const auto* map = db.projection_pressure_maps_[slot].load();
            valid = map && map->find("main") != map->end();
        }
        db.projection_pressure_readers_[slot].fetch_sub(1);
        return valid;
    }
    static std::vector<std::shared_ptr<projection_pressure_source>> sources(lattice_db& db) {
        return db.projection_pressure_sources();
    }
};
} // namespace lattice

namespace {
std::shared_ptr<database_read_control> test_read_control() {
    auto control = std::make_shared<database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + 3s;
    return control;
}
std::string projection_file_uri(const std::string& path) {
    static constexpr char digits[] = "0123456789ABCDEF";
    std::string uri = "file:";
    for (unsigned char c : path) {
        if (c == '%' || c == '?' || c == '#' || c == ' ') {
            uri += '%'; uri += digits[c >> 4]; uri += digits[c & 15];
        } else uri += static_cast<char>(c);
    }
    return uri + "?cache=private";
}
} // namespace

TEST(ProjectionRead, AttachedStoreRetirementFindsForeignParentAndPreservesUnrelatedStore) {
    ProjectionFixture parent("projection_cross_parent"), arm("projection_cross_arm"), other("projection_unrelated");
    parent.add(1, "parent"); arm.add(2, "arm");
    other.add(1, "other-one"); other.add(2, "other-two");
    parent.db.attach(arm.db);
    auto attached = parent.db.start_projection(parent.query());
    ASSERT_EQ(attached.next_batch(1).status_code(), status(projection_status::batch));
    auto unrelated = other.db.start_projection(other.query());
    ASSERT_EQ(unrelated.next_batch(1).status_code(), status(projection_status::batch));
    EXPECT_EQ(arm.db.local_read_generations_outstanding(), 0u);
    EXPECT_EQ(arm.db.read_generations_outstanding(), 1u);
    EXPECT_EQ(parent.db.read_generations_outstanding(), 1u);
    arm.db.retire_all_read_generations();
    ASSERT_TRUE(wait_until([&] { return !attached.has_resources(); }));
    EXPECT_EQ(attached.next_batch(1).status_code(), status(projection_status::snapshot_expired));
    EXPECT_EQ(arm.db.read_generations_outstanding(), 0u);
    EXPECT_FALSE(unrelated.is_terminal());
    EXPECT_EQ(unrelated.next_batch(10).status_code(), status(projection_status::done));
}

TEST(ProjectionRead, FileURISymlinkAndDuplicateAttachmentUseOnePhysicalIdentity) {
    ProjectionFixture parent("projection_alias_parent"), arm("projection alias % arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    TempDB symlink("projection_alias_link");
    std::error_code error;
    std::filesystem::create_symlink(arm.path.path, symlink.path, error);
    ASSERT_FALSE(error) << error.message();
    lattice_db uri{configuration(projection_file_uri(arm.path.str()))};
    lattice_db link{configuration(symlink.str())};
    auto expected = arm.db.db().physical_identity();
    auto from_uri = uri.db().physical_identity();
    auto from_link = link.db().physical_identity();
    ASSERT_TRUE(expected && from_uri && from_link);
    EXPECT_TRUE(*expected == *from_uri);
    EXPECT_TRUE(*expected == *from_link);
    parent.db.attach(arm.db);
    parent.db.attach(link);
    auto operation = parent.db.start_projection(parent.query());
    ASSERT_EQ(operation.next_batch(1).status_code(), status(projection_status::batch));
    EXPECT_EQ(uri.read_generations_outstanding(), 1u) << "duplicate physical arms count once";
    uri.request_generation_advance();
    ASSERT_TRUE(wait_until([&] { return !operation.has_resources(); }));
    EXPECT_EQ(operation.next_batch(1).status_code(), status(projection_status::snapshot_expired));
    auto second = parent.db.start_projection(parent.query());
    ASSERT_EQ(second.next_batch(1).status_code(), status(projection_status::batch));
    link.retire_all_read_generations();
    ASSERT_TRUE(wait_until([&] { return !second.has_resources(); }));
    EXPECT_EQ(second.next_batch(1).status_code(), status(projection_status::snapshot_expired));
}

TEST(ProjectionRead, BusyAttachedStoreTruncateSignalsReleaseForLaterRetry) {
    ProjectionFixture parent("projection_checkpoint_parent"), arm("projection_checkpoint_arm");
    parent.add(1, "parent"); arm.add(2, "arm"); parent.db.attach(arm.db);
    auto operation = parent.db.start_projection(parent.query());
    ASSERT_EQ(operation.next_batch(1).status_code(), status(projection_status::batch));
    arm.db.db().execute("UPDATE Fixture SET body = ? WHERE id = 2", {std::string(96 * 1024, 'c')});
    const auto first = arm.db.db().wal_checkpoint(true, 25);
    EXPECT_NE(first.busy, 0) << "the first attempt reports its actual BUSY result";
    ASSERT_TRUE(wait_until([&] { return !operation.has_resources(); }));
    EXPECT_EQ(operation.next_batch(1).status_code(), status(projection_status::snapshot_expired));
    EXPECT_EQ(arm.db.db().wal_checkpoint(true, 250).busy, 0);
}

TEST(ProjectionRead, AdmissionTicketRejectsRetirementAndActivePressureInterleavings) {
    ProjectionFixture fixture("projection_epoch_admission");
    auto identity = fixture.db.db().physical_identity();
    ASSERT_TRUE(identity);
    auto stale = capture_projection_stores({identity});
    retire_projection_store(identity);
    EXPECT_EQ(stale.publish(UINT64_MAX, test_read_control()), projection_status::snapshot_expired);
    auto pressure = make_projection_pressure_source(identity);
    ASSERT_TRUE(pressure);
    auto before_pressure = capture_projection_stores({identity});
    pressure->raise();
    const auto first_raise = pressure->raised.load();
    EXPECT_EQ(before_pressure.publish(UINT64_MAX - 1, test_read_control()), projection_status::admission_rejected);
    pressure->raise();
    pressure->acknowledge(first_raise);
    auto still_busy = capture_projection_stores({identity});
    EXPECT_EQ(still_busy.publish(UINT64_MAX - 2, test_read_control()), projection_status::admission_rejected);
    pressure->active.store(false);
    auto after_close = capture_projection_stores({identity});
    auto control = test_read_control();
    EXPECT_EQ(after_close.publish(UINT64_MAX - 3, control), projection_status::batch);
    EXPECT_EQ(projection_store_readers(identity), 1u);
    after_close.release();
    EXPECT_EQ(projection_store_readers(identity), 0u);
}

TEST(ProjectionRead, PressurePendingAfterRetirementPreventsNewForeignParentPin) {
    ProjectionFixture parent("projection_pressure_parent"), arm("projection_pressure_arm");
    parent.add(1, "parent"); arm.add(2, "arm"); parent.db.attach(arm.db);
    auto identity = arm.db.db().physical_identity();
    ASSERT_TRUE(identity);
    // Deterministically hold the registry's outstanding count after retirement;
    // no SQLite resource is fabricated, and release is explicit below.
    auto blocker = capture_projection_stores({identity});
    auto control = test_read_control();
    ASSERT_EQ(blocker.publish(UINT64_MAX - 10, control), projection_status::batch);
    arm.db.set_wal_keeper_eviction_threshold_bytes(1);
    arm.db.db().execute("UPDATE Fixture SET n = n + 1 WHERE id = 2");
    ASSERT_TRUE(arm.db.wal_eviction_pending());
    arm.db.run_read_pool_maintenance_all_instances();
    EXPECT_EQ(control->stop_code.load(), status(projection_status::snapshot_expired));
    EXPECT_TRUE(arm.db.wal_eviction_pending());
    auto rejected = parent.db.start_projection(parent.query());
    EXPECT_EQ(rejected.next_batch(1).status_code(), status(projection_status::admission_rejected));
    EXPECT_FALSE(rejected.has_resources());
    blocker.release();
    arm.db.run_read_pool_maintenance_all_instances();
    EXPECT_FALSE(arm.db.wal_eviction_pending());
    auto admitted = parent.db.start_projection(parent.query());
    EXPECT_EQ(admitted.next_batch(10).status_code(), status(projection_status::done));
}

TEST(ProjectionRead, MetadataMutexWaitUsesOriginalDeadlineAndNeverInterruptsWriter) {
    ProjectionFixture fixture("projection_metadata_deadline");
    fixture.add(1, "one");
    std::atomic<bool> held{false}, release{false};
    auto* mutex = sqlite3_db_mutex(fixture.db.db().handle());
    std::thread holder([&] {
        sqlite3_mutex_enter(mutex);
        held.store(true);
        while (!release.load()) std::this_thread::yield();
        sqlite3_mutex_leave(mutex);
    });
    const bool ready = wait_until([&] { return held.load(); });
    auto query = fixture.query(); query.timeout_ms = 10;
    auto operation = fixture.db.start_projection(query);
    auto terminal = operation.next_batch(1);
    release.store(true); holder.join();
    ASSERT_TRUE(ready);
    EXPECT_EQ(terminal.status_code(), status(projection_status::deadline_exceeded));
    EXPECT_FALSE(operation.has_resources());
    EXPECT_NO_THROW(fixture.db.db().execute("UPDATE Fixture SET n = n + 1"));
}

TEST(ProjectionRead, DelayedSnapshotReaderAcrossTwoPublicationsAndShutdownIsSafe) {
    ProjectionFixture parent("projection_snapshot_parent"), arm("projection_snapshot_arm");
    const auto stale_slot = projection_pressure_test_access::before_increment(parent.db);
    parent.db.attach(arm.db);  // First publication.
    parent.db.detach(arm.db); // Second publication, reuses the original slot.
    EXPECT_TRUE(projection_pressure_test_access::finish_delayed_reader(parent.db, stale_slot));
    auto sources = projection_pressure_test_access::sources(parent.db);
    ASSERT_EQ(sources.size(), 1u);
    auto identity = parent.db.db().physical_identity();
    sources.front()->raise();
    auto blocked = capture_projection_stores({identity});
    EXPECT_EQ(blocked.publish(UINT64_MAX - 20, test_read_control()), projection_status::admission_rejected);
    parent.db.close();
    EXPECT_FALSE(sources.front()->active.load());
    auto admitted = capture_projection_stores({identity});
    auto control = test_read_control();
    EXPECT_EQ(admitted.publish(UINT64_MAX - 21, control), projection_status::batch);
    admitted.release();
}

TEST(ProjectionRead, PressureSnapshotsSurviveConcurrentHookWritesAndTopologyChanges) {
    ProjectionFixture parent("projection_snapshot_stress"), arm("projection_snapshot_stress_arm");
    parent.add(1, "one"); arm.add(2, "two");
    parent.db.set_wal_keeper_eviction_threshold_bytes(1);
    std::atomic<bool> stop{false}, failed{false};
    const auto* diagnostic_flag = std::getenv("LATTICE_PROJECTION_STRESS_DIAGNOSTICS");
    const bool diagnostics = diagnostic_flag && std::strcmp(diagnostic_flag, "1") == 0;
    std::atomic<int> iteration{-1}, phase{0};
    std::atomic<uint64_t> writer_attempts{0}, writer_completed{0}, phase_started_ns{0};
    std::atomic<bool> writer_in_flight{false};
    const auto monotonic_ns = [] {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    };
    const auto mark_phase = [&](int next) {
        if (diagnostics) { phase_started_ns.store(monotonic_ns()); phase.store(next); }
    };
    // Diagnostic thread owns no database handle. Fixed scalar state and at
    // most 32 records; no logging, waits or yields enter either tested loop.
    // Phases: 0=starting, 1=attach, 2=attached, 3=detach, 4=detached,
    // 5=writer join, 6=joined. Fields are independent diagnostic snapshots.
    std::jthread diagnostic_thread;
    if (diagnostics) {
        phase_started_ns.store(monotonic_ns());
        diagnostic_thread = std::jthread([&](std::stop_token token) {
            for (unsigned sample = 0; sample != 32 && !token.stop_requested(); ++sample) {
                const auto now = monotonic_ns();
                const auto started = phase_started_ns.load();
                std::fprintf(stderr,
                    "[projection-stress] sample=%u uptime_ns=%llu iteration=%d phase=%d phase_age_ms=%llu writer_attempts=%llu writer_completed=%llu writer_in_flight=%d\n",
                    sample, static_cast<unsigned long long>(now), iteration.load(), phase.load(),
                    static_cast<unsigned long long>(now >= started ? (now - started) / 1000000 : 0),
                    static_cast<unsigned long long>(writer_attempts.load()),
                    static_cast<unsigned long long>(writer_completed.load()), writer_in_flight.load() ? 1 : 0);
                std::fflush(stderr);
                for (int tick = 0; tick != 10 && !token.stop_requested(); ++tick)
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }
    std::thread writer([&] {
        try {
            while (!stop.load()) {
                if (diagnostics) { writer_attempts.fetch_add(1); writer_in_flight.store(true); }
                parent.db.db().execute("UPDATE main.Fixture SET n = n + 1 WHERE id = 1");
                if (diagnostics) { writer_completed.fetch_add(1); writer_in_flight.store(false); }
            }
        } catch (...) {
            if (diagnostics) writer_in_flight.store(false);
            failed.store(true);
        }
    });
    try {
        for (int i = 0; i < 32; ++i) {
            if (diagnostics) iteration.store(i);
            mark_phase(1); parent.db.attach(arm.db); mark_phase(2);
            mark_phase(3); parent.db.detach(arm.db); mark_phase(4);
        }
    } catch (...) { failed.store(true); }
    stop.store(true); mark_phase(5); writer.join(); mark_phase(6);
    EXPECT_FALSE(failed.load());
    parent.db.close();
    for (const auto& source : projection_pressure_test_access::sources(parent.db)) EXPECT_FALSE(source->active.load());
    // Qualification requires native ASan/TSan; source review is not a race test.
}

TEST(ProjectionRead, DetectedFileReplacementRefusesIdentityAndDoesNotRetireSuccessor) {
    TempDB original("projection_identity_original"), replacement("projection_identity_replacement"), moved("projection_identity_moved");
    database first(original.str());
    first.execute("CREATE TABLE Fixture(id INTEGER)");
    { database second(replacement.str()); second.execute("CREATE TABLE Fixture(id INTEGER)"); }
    auto old_identity = first.physical_identity();
    ASSERT_TRUE(old_identity);
    auto old_ticket = capture_projection_stores({old_identity});
    auto old_control = test_read_control();
    ASSERT_EQ(old_ticket.publish(UINT64_MAX - 30, old_control), projection_status::batch);
    std::filesystem::rename(original.path, moved.path);
    std::filesystem::rename(replacement.path, original.path);
    const auto detected = first.physical_identity("main", {}, true);
    std::shared_ptr<const physical_store_identity> successor_identity;
    {
        database successor(original.str(), database::open_mode::read_only);
        successor_identity = successor.physical_identity();
    }
    // Restore filenames before any assertion/destructor can run SQL on first.
    std::filesystem::rename(original.path, replacement.path);
    std::filesystem::rename(moved.path, original.path);
    EXPECT_FALSE(detected);
    ASSERT_TRUE(successor_identity);
    EXPECT_FALSE(*old_identity == *successor_identity);
    auto successor_ticket = capture_projection_stores({successor_identity});
    auto successor_control = test_read_control();
    ASSERT_EQ(successor_ticket.publish(UINT64_MAX - 31, successor_control), projection_status::batch);
    retire_projection_store(old_identity);
    EXPECT_EQ(old_control->stop_code.load(), status(projection_status::snapshot_expired));
    EXPECT_EQ(successor_control->stop_code.load(), 0);
    old_ticket.release(); successor_ticket.release();
}

TEST(ProjectionRead, WriterMaintenanceTerminatesOldHandlesAndReopensLocalAndAttachedReads) {
    ProjectionFixture local("projection_reopen_local"), parent("projection_reopen_parent"), arm("projection_reopen_arm");
    local.add(1, "one"); local.add(2, "two");
    parent.add(1, "parent"); arm.add(2, "arm"); parent.db.attach(arm.db);
    for (auto* fixture : {&local, &parent}) {
        auto old = fixture->db.start_projection(fixture->query());
        ASSERT_EQ(old.next_batch(1).status_code(), status(projection_status::batch));
        fixture->db.close_write_db();
        EXPECT_FALSE(old.has_resources());
        EXPECT_EQ(old.next_batch(1).status_code(), status(projection_status::snapshot_expired));
        auto paused = fixture->db.start_projection(fixture->query());
        EXPECT_EQ(paused.next_batch(1).status_code(), status(projection_status::snapshot_expired));
        EXPECT_FALSE(paused.has_resources());
        fixture->db.reopen_write_db();
        auto fresh = fixture->db.start_projection(fixture->query());
        const auto rows = fresh.next_batch(10);
        ASSERT_EQ(rows.status_code(), status(projection_status::done)) << rows.error_message();
        EXPECT_EQ(rows.row_count(), 2);
        EXPECT_FALSE(fresh.has_resources());
        EXPECT_EQ(old.next_batch(1).status_code(), status(projection_status::snapshot_expired));
        for (const auto& source : projection_pressure_test_access::sources(fixture->db))
            EXPECT_TRUE(source->active.load());
    }
    EXPECT_EQ(projection_pressure_test_access::sources(parent.db).size(), 2u);
}

TEST(ProjectionRead, RejectedAttachIdentityStatementInvalidatesPartialTopology) {
    ProjectionFixture parent("projection_attach_capture_failure"), arm("projection_attach_capture_arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    struct Authorizer {
        sqlite3* handle;
        bool attached = false;
        bool rejected = false;
        ~Authorizer() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    } authorizer{parent.db.db().handle()};
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle,
        [](void* raw, int action, const char*, const char*, const char*, const char*) noexcept {
            auto& state = *static_cast<Authorizer*>(raw);
            if (action == SQLITE_ATTACH) state.attached = true;
            if (action == SQLITE_SELECT && state.attached) {
                state.rejected = true;
                return SQLITE_DENY;
            }
            return SQLITE_OK;
        }, &authorizer), SQLITE_OK);
    EXPECT_THROW(parent.db.attach(arm.db), db_error);
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle, nullptr, nullptr), SQLITE_OK);
    EXPECT_TRUE(authorizer.attached);
    EXPECT_TRUE(authorizer.rejected);
    const auto alias = arm.path.path.filename().replace_extension().string();
    const auto databases = parent.db.db().query("PRAGMA database_list");
    bool writer_attached = false;
    for (const auto& row : databases)
        writer_attached |= std::get<std::string>(row.at("name")) == alias;
    EXPECT_TRUE(writer_attached) << "the original ATTACH succeeded before capture failed";
    auto operation = parent.db.start_projection(parent.query());
    EXPECT_EQ(operation.next_batch(10).status_code(), status(projection_status::snapshot_expired));
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, ReaderAttachFailureCannotPublishCapturedWriterIdentity) {
    ProjectionFixture parent("projection_attach_reader_failure"), arm("projection_attach_reader_arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    struct Authorizer {
        sqlite3* handle;
        bool rejected = false;
        ~Authorizer() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    } authorizer{parent.db.read_db().handle()};
    ASSERT_NE(authorizer.handle, parent.db.db().handle());
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle,
        [](void* raw, int action, const char*, const char*, const char*, const char*) noexcept {
            if (action == SQLITE_ATTACH) {
                static_cast<Authorizer*>(raw)->rejected = true;
                return SQLITE_DENY;
            }
            return SQLITE_OK;
        }, &authorizer), SQLITE_OK);
    EXPECT_THROW(parent.db.attach(arm.db), db_error);
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle, nullptr, nullptr), SQLITE_OK);
    EXPECT_TRUE(authorizer.rejected);
    const auto alias = arm.path.path.filename().replace_extension().string();
    const auto writer_identity = parent.db.db().physical_identity(alias, {}, true);
    const auto source_identity = arm.db.db().physical_identity("main", {}, true);
    ASSERT_TRUE(writer_identity && source_identity);
    EXPECT_TRUE(*writer_identity == *source_identity);
    auto operation = parent.db.start_projection(parent.query());
    EXPECT_EQ(operation.next_batch(10).status_code(), status(projection_status::snapshot_expired));
    EXPECT_FALSE(operation.has_resources());
}

TEST(ProjectionRead, AttachmentMetadataSeesUncommittedWriterSchema) {
    ProjectionFixture parent("projection_metadata_uncommitted"), arm("projection_metadata_uncommitted_arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    transaction pending(parent.db.db());
    parent.db.db().execute("ALTER TABLE main.Fixture ADD COLUMN _source TEXT");
    bool rejected_reserved = false;
    try { parent.db.attach(arm.db); }
    catch (const std::runtime_error& error) {
        rejected_reserved = std::string(error.what()).find("reserved routing column '_source'") != std::string::npos;
    }
    EXPECT_TRUE(rejected_reserved) << "preflight must see writer-local uncommitted schema";
    const auto alias = arm.path.path.filename().replace_extension().string();
    for (const auto& row : parent.db.db().query("PRAGMA database_list"))
        EXPECT_NE(std::get<std::string>(row.at("name")), alias);
    pending.rollback();
    parent.db.attach(arm.db);
    auto operation = parent.db.start_projection(parent.query());
    const auto batch = operation.next_batch(10);
    EXPECT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    EXPECT_EQ(batch.row_count(), 2u);
}

TEST(ProjectionRead, AttachmentMetadataValidatesQuotedModelBeforeSideEffects) {
    ProjectionFixture parent("projection_metadata_quoted"), arm("projection_metadata_quoted_arm");
    parent.db.db().execute("CREATE TABLE \"odd \"\"model'雪\" (_source TEXT)");
    bool rejected_reserved = false;
    try { parent.db.attach(arm.db); }
    catch (const std::runtime_error& error) {
        rejected_reserved = std::string(error.what()).find("reserved routing column '_source'") != std::string::npos;
    }
    EXPECT_TRUE(rejected_reserved) << "metadata validation must parse the literal model name";
    const auto alias = arm.path.path.filename().replace_extension().string();
    for (const auto& row : parent.db.db().query("PRAGMA database_list"))
        EXPECT_NE(std::get<std::string>(row.at("name")), alias);
}

TEST(ProjectionRead, AttachmentMetadataIgnoresShadowPragmaFunctionNames) {
    ProjectionFixture parent("projection_metadata_shadow"), arm("projection_metadata_shadow_arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    parent.db.db().execute("CREATE TEMP TABLE pragma_table_info (id INTEGER)");
    // Keep the literal underscore exclusion; this is not a model table.
    parent.db.db().execute("CREATE TABLE _metadata_hidden (_source TEXT)");
    parent.db.attach(arm.db);
    auto operation = parent.db.start_projection(parent.query());
    const auto batch = operation.next_batch(10);
    EXPECT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    EXPECT_EQ(batch.row_count(), 2u);
}

TEST(ProjectionRead, AttachmentMetadataFailureBeforeAttachLeavesTopologyUsable) {
    ProjectionFixture parent("projection_metadata_error"), arm("projection_metadata_error_arm");
    parent.add(1, "parent"); arm.add(2, "arm");
    struct Authorizer {
        sqlite3* handle;
        bool rejected = false;
        ~Authorizer() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    } authorizer{parent.db.db().handle()};
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle,
        [](void* raw, int action, const char* argument, const char*, const char*, const char*) noexcept {
            if (action == SQLITE_PRAGMA && argument && std::strcmp(argument, "table_info") == 0) {
                static_cast<Authorizer*>(raw)->rejected = true;
                return SQLITE_DENY;
            }
            return SQLITE_OK;
        }, &authorizer), SQLITE_OK);
    EXPECT_THROW(parent.db.attach(arm.db), db_error);
    ASSERT_EQ(sqlite3_set_authorizer(authorizer.handle, nullptr, nullptr), SQLITE_OK);
    EXPECT_TRUE(authorizer.rejected);
    const auto alias = arm.path.path.filename().replace_extension().string();
    for (const auto& row : parent.db.db().query("PRAGMA database_list"))
        EXPECT_NE(std::get<std::string>(row.at("name")), alias);
    auto still_local = parent.db.start_projection(parent.query());
    EXPECT_EQ(still_local.next_batch(10).status_code(), status(projection_status::done));
    parent.db.attach(arm.db);
    auto attached = parent.db.start_projection(parent.query());
    const auto batch = attached.next_batch(10);
    EXPECT_EQ(batch.status_code(), status(projection_status::done)) << batch.error_message();
    EXPECT_EQ(batch.row_count(), 2u);
}
