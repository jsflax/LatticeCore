#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <lattice/spatial_query.hpp>
#include <limits>

namespace {
using namespace lattice;

struct SpatialBridgeDB {
    TempDB path;
    lattice::swift_lattice core;
    std::string table, geo, sidecar;
    bool list;
    SpatialBridgeDB(const std::string& label, bool is_list = false,
                    std::string model = "SpatialItem", std::string column = "location")
        : path(label), core(swift_configuration(path.str()), SchemaVector{}),
          table(std::move(model)), geo(std::move(column)), sidecar("_" + table + "_" + geo), list(is_list) {
        core.db().execute("CREATE TABLE " + q(table) +
            " (id INTEGER PRIMARY KEY,globalId TEXT NOT NULL,name TEXT,category INTEGER)");
        core.db().execute("CREATE VIRTUAL TABLE " + q(sidecar + "_rtree") +
            " USING rtree(id,minLat,maxLat,minLon,maxLon)");
        if (list) core.db().execute("CREATE TABLE " + q(sidecar) + " (id INTEGER PRIMARY KEY,parent_id TEXT)");
    }
    static std::string q(const std::string& name) { return spatial_quoted_identifier(name); }
    void add(int64_t id, const std::string& name, int64_t category = 1,
             double coordinate = 0, const std::string& global_id = "") {
        const auto gid = global_id.empty() ? fake_uuid(static_cast<int>(id)) : global_id;
        core.db().execute("INSERT INTO " + q(table) + " VALUES(?,?,?,?)", {id, gid, name, category});
        bound(id, coordinate, list ? gid : "");
    }
    void bound(int64_t id, double coordinate, const std::string& parent = "") {
        core.db().execute("INSERT INTO " + q(sidecar + "_rtree") + " VALUES(?,?,?,?,?)",
            {id, coordinate, coordinate, coordinate, coordinate});
        if (list) core.db().execute("INSERT INTO " + q(sidecar) + " VALUES(?,?)", {id, parent});
    }
    std::vector<managed<swift_dynamic_object>> rows(OptionalString group = std::nullopt,
        OptionalString distinct = std::nullopt, OptionalInt64 limit = std::nullopt, OptionalInt64 offset = std::nullopt) {
        return core.objects_within_bbox_shape(table, geo, -1, 1, -1, 1, std::nullopt,
            q(table) + ".name ASC," + q(table) + ".id ASC", limit, offset, group, distinct);
    }
    int64_t count(OptionalString group = std::nullopt, OptionalString distinct = std::nullopt) {
        return core.count_within_bbox_shape(table, geo, -1, 1, -1, 1, std::nullopt, group, distinct);
    }
};

std::string name_of(const managed<swift_dynamic_object>& row) {
    // Query rows retain their immutable selected image separately from the
    // intentionally unhydrated mutable/live source values.
    if (!row.query_row_image_) {
        ADD_FAILURE() << "Spatial query row is missing its immutable query image";
        return {};
    }
    const auto found = row.query_row_image_->find("name");
    if (found == row.query_row_image_->end()) {
        ADD_FAILURE() << "Spatial query image is missing selected column 'name'";
        return {};
    }
    const auto* value = std::get_if<std::string>(&found->second);
    if (!value) {
        ADD_FAILURE() << "Spatial query image column 'name' is not text (variant index "
                      << found->second.index() << ")";
        return {};
    }
    return *value;
}
} // namespace

TEST(SpatialShapeBridge, OldSignaturesRetainFullBoundPrecisionAndRejectInvalidBounds) {
    SpatialBridgeDB fixture("spatial_bridge_precision");
    fixture.add(1, "zero");
    auto precise = fixture.core.objects_within_bbox(fixture.table, fixture.geo, -1, -0.0000001, -1, 1);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_TRUE(precise.empty());
    EXPECT_EQ(fixture.core.count_within_bbox(fixture.table, fixture.geo, -1, -0.0000001, -1, 1), 0);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_EQ(fixture.core.objects_within_bbox(fixture.table, fixture.geo, -1, 1, -1, 1).size(), 1);
    EXPECT_EQ(fixture.core.count_within_bbox(fixture.table, fixture.geo, -1, 1, -1, 1), 1);
    EXPECT_TRUE(fixture.core.objects_within_bbox(fixture.table, fixture.geo,
        -1, std::numeric_limits<double>::infinity(), -1, 1).empty());
    EXPECT_FALSE(last_bridge_error().empty());
}

TEST(SpatialShapeBridge, GroupDistinctCountsAndPaginationUseTheSameRelation) {
    SpatialBridgeDB fixture("spatial_bridge_shapes", true);
    for (int64_t id = 1; id <= 6; ++id) {
        fixture.add(id, id <= 2 ? "a" : id <= 4 ? "b" : "c", (id + 1) / 2);
        fixture.bound(id + 100, 0.5, fake_uuid(static_cast<int>(id)));
    }
    fixture.add(7, "outside", 4, 10);
    for (int shape = 0; shape != 4; ++shape) {
        OptionalString group = (shape & 1) ? OptionalString("name") : std::nullopt;
        OptionalString distinct = (shape & 2) ? OptionalString("category") : std::nullopt;
        const int64_t expected = shape == 0 ? 6 : 3;
        auto rows = fixture.rows(group, distinct);
        ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        EXPECT_EQ(rows.size(), expected);
        EXPECT_EQ(fixture.count(group, distinct), expected);
        EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        auto page = fixture.rows(group, distinct, 1, shape == 0 ? 2 : 1);
        ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        ASSERT_EQ(page.size(), 1);
        EXPECT_EQ(name_of(page.front()), "b");
        auto tail = fixture.rows(group, distinct, std::nullopt, 1);
        EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        EXPECT_EQ(tail.size(), expected - 1); // Offset without limit uses LIMIT -1.
    }
}

TEST(SpatialShapeBridge, LiteralPredicateAndQualifiedOrderSurviveBothGroupingLevels) {
    SpatialBridgeDB fixture("spatial_bridge_filter");
    fixture.add(1, "a", 1); fixture.add(2, "b", 2); fixture.add(3, "c", 3);
    auto rows = fixture.core.objects_within_bbox_shape(fixture.table, fixture.geo, -1, 1, -1, 1,
        std::string("category >= 2"), std::string("SpatialItem.id DESC"), 1, std::nullopt,
        std::string("name"), std::string("category"));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(name_of(rows.front()), "c");
    EXPECT_EQ(fixture.core.count_within_bbox_shape(fixture.table, fixture.geo, -1, 1, -1, 1,
        std::string("category >= 2"), std::string("name"), std::string("category")), 2);
    auto qualified_group = fixture.core.objects_within_bbox(fixture.table, fixture.geo, -1, 1, -1, 1,
        std::nullopt, std::string("SpatialItem.id ASC"), std::nullopt, std::nullopt,
        std::string("SpatialItem.category"));
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_EQ(qualified_group.size(), 3);
}

TEST(SpatialShapeBridge, ScalarAndListAttachedReplicasNeverBorrowOtherStoreMatches) {
    for (bool list : {false, true}) {
        SpatialBridgeDB main("spatial_bridge_main", list), arm("spatial_bridge_arm", list);
        main.add(1, "main", 1, 10, "same-uuid");
        arm.add(1, "arm", 1, 0, "same-uuid");
        if (list) arm.bound(2, 0.5, "same-uuid");
        ASSERT_TRUE(main.core.attach(arm.core));
        auto rows = main.rows();
        ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        ASSERT_EQ(rows.size(), 1);
        EXPECT_EQ(name_of(rows.front()), "arm");
        EXPECT_EQ(main.count(), 1);
        main.core.db().execute("UPDATE main." + SpatialBridgeDB::q(main.sidecar + "_rtree") +
            " SET minLat=0,maxLat=0,minLon=0,maxLon=0 WHERE id=1");
        rows = main.rows();
        ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
        ASSERT_EQ(rows.size(), 2);
        EXPECT_EQ(name_of(rows[0]), "arm");
        EXPECT_EQ(name_of(rows[1]), "main");
        EXPECT_EQ(main.count(), 2); // Equal id/globalId still represent two routes.
        EXPECT_EQ(main.count(std::nullopt, std::string("category")), 1);
    }
}

TEST(SpatialShapeBridge, QuotedModelGeoAndAttachmentAliasesAreEscaped) {
    SpatialBridgeDB local("spatial_bridge_names", true, "odd \"model", "geo \"field");
    local.add(1, "quoted");
    auto local_rows = local.rows(std::string("name"), std::string("category"));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(local_rows.size(), 1);
    EXPECT_EQ(name_of(local_rows.front()), "quoted");
    EXPECT_EQ(local.count(std::string("name"), std::string("category")), 1);
    // Existing ATTACH model discovery has a separate unquoted-table limitation;
    // exercise its supported table name while stressing the quoted alias here.
    SpatialBridgeDB main("spatial_bridge_alias_main"), arm("spatial_bridge_\"arm'quote");
    main.add(1, "main", 1, 10); arm.add(1, "arm");
    ASSERT_TRUE(main.core.attach(arm.core));
    auto rows = main.rows();
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(name_of(rows.front()), "arm");
    EXPECT_EQ(main.count(), 1);
}

TEST(SpatialShapeBridge, PreAttachKeeperUsesItsOwnTopologyAndCurrentReadsUseAttachedArms) {
    SpatialBridgeDB main("spatial_bridge_keeper_main"), arm("spatial_bridge_keeper_arm", true);
    main.add(1, "main"); arm.add(1, "arm");
    const auto generation = main.core.acquire_read_generation();
    ASSERT_NE(generation, 0);
    struct release_generation {
        lattice::swift_lattice& owner; uint64_t generation;
        ~release_generation() { owner.release_read_generation(generation); }
    } release{main.core, generation};
    ASSERT_TRUE(main.core.attach(arm.core));
    auto old = main.core.objects_within_bbox_shape_at(generation, main.table, main.geo, -1, 1, -1, 1,
        std::nullopt, std::string("SpatialItem.id ASC"), std::nullopt, std::nullopt,
        std::string("name"), std::string("category"));
    ASSERT_FALSE(main.core.last_generation_read_stale()) << last_bridge_error();
    ASSERT_EQ(old.size(), 1);
    EXPECT_EQ(name_of(old.front()), "main");
    auto old_wrapper = main.core.objects_within_bbox_at(generation, main.table, main.geo, -1, 1, -1, 1);
    EXPECT_FALSE(main.core.last_generation_read_stale());
    ASSERT_EQ(old_wrapper.size(), 1);
    EXPECT_EQ(name_of(old_wrapper.front()), "main");
    auto current = main.rows();
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(current.size(), 2);
    EXPECT_EQ(main.count(), 2);
    EXPECT_EQ(name_of(current[0]), "arm"); EXPECT_EQ(name_of(current[1]), "main");
}

TEST(SpatialShapeBridge, AttachedOnlyModelAndRetiredKeeperFailWithoutWrongConnectionFallback) {
    TempDB path("spatial_bridge_empty_main");
    lattice::swift_lattice receiver(swift_configuration(path.str()), SchemaVector{});
    SpatialBridgeDB arm("spatial_bridge_only_arm", true);
    arm.add(1, "arm");
    const auto generation = receiver.acquire_read_generation();
    ASSERT_NE(generation, 0);
    ASSERT_TRUE(receiver.attach(arm.core));
    auto rows = receiver.objects_within_bbox_shape(arm.table, arm.geo, -1, 1, -1, 1);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(receiver.count_within_bbox_shape(arm.table, arm.geo, -1, 1, -1, 1), 1);
    auto old = receiver.objects_within_bbox_shape_at(generation, arm.table, arm.geo, -1, 1, -1, 1);
    EXPECT_TRUE(old.empty());
    EXPECT_TRUE(receiver.last_generation_read_stale());
    receiver.release_read_generation(generation);
    receiver.retire_all_read_generations();
    auto stale = receiver.objects_within_bbox_shape_at(generation, arm.table, arm.geo, -1, 1, -1, 1);
    EXPECT_TRUE(stale.empty());
    EXPECT_TRUE(receiver.last_generation_read_stale());
}

TEST(SpatialShapeBridge, AttachedWriteObserverUsesPhysicalSchemaForGlobalID) {
    SpatialBridgeDB main("spatial_hook_main"), arm("spatial_hook_\"arm'quote");
    main.add(1, "main-before", 1, 0, "main-source-uuid");
    arm.add(1, "arm-before", 1, 0, "attached-source-uuid");
    ASSERT_TRUE(main.core.attach(arm.core));

    // Both physical stores have row id 1; the union's first row belongs to
    // main. The hook must use SQLite's db_name, including its quoted alias.
    struct Receipt {
        std::mutex mutex;
        std::vector<lattice_db::change_event> events;
    };
    auto receipt = std::make_shared<Receipt>();
    const auto token = static_cast<lattice_db&>(main.core).add_table_observer(main.table,
        [receipt](const std::vector<lattice_db::change_event>& changes) {
            std::lock_guard<std::mutex> lock(receipt->mutex);
            receipt->events.insert(receipt->events.end(), changes.begin(), changes.end());
        });
    struct RemoveObserver {
        lattice::swift_lattice& owner;
        const std::string& table;
        lattice_db::observer_id token;
        ~RemoveObserver() { owner.remove_table_observer(table, token); }
    } remove{main.core, main.table, token};

    const auto alias = arm.path.path.filename().replace_extension().string();
    main.core.db().execute("UPDATE " + SpatialBridgeDB::q(alias) + "." +
        SpatialBridgeDB::q(main.table) + " SET name = ? WHERE id = 1",
        {std::string("arm-after")});
    std::vector<lattice_db::change_event> delivered;
    {
        std::lock_guard<std::mutex> lock(receipt->mutex);
        delivered = receipt->events;
    }
    ASSERT_EQ(delivered.size(), 1u);
    EXPECT_EQ(std::get<0>(delivered.front()), main.table);
    EXPECT_EQ(std::get<1>(delivered.front()), "UPDATE");
    EXPECT_EQ(std::get<2>(delivered.front()), 1);
    EXPECT_EQ(std::get<3>(delivered.front()), "attached-source-uuid");

    const auto local = main.core.db().query("SELECT name FROM main." +
        SpatialBridgeDB::q(main.table) + " WHERE id = 1");
    const auto attached = main.core.db().query("SELECT name FROM " +
        SpatialBridgeDB::q(alias) + "." + SpatialBridgeDB::q(main.table) + " WHERE id = 1");
    ASSERT_EQ(local.size(), 1u);
    ASSERT_EQ(attached.size(), 1u);
    EXPECT_EQ(std::get<std::string>(local.front().at("name")), "main-before");
    EXPECT_EQ(std::get<std::string>(attached.front().at("name")), "arm-after");
}
