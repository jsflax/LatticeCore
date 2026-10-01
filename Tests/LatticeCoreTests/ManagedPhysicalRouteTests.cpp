#include "TestHelpers.hpp"
#include "ManagedAttachmentTestSupport.hpp"
#include <lattice.hpp>
#include <geo_bounds.hpp>

namespace {
using namespace lattice;

SchemaVector route_schemas() {
    swift_schema_entry entry;
    entry.table_name = "ManagedRouteItem";
    for (const auto& name : {"name", "optionalText"}) {
        property_descriptor p;
        p.name = name;
        p.type = column_type::text;
        p.nullable = true;
        entry.properties[name] = p;
    }
    property_descriptor count;
    count.name = "count";
    count.type = column_type::integer;
    entry.properties[count.name] = count;
    property_descriptor bytes;
    bytes.name = "bytes";
    bytes.type = column_type::blob;
    bytes.nullable = true;
    entry.properties[bytes.name] = bytes;
    property_descriptor location;
    location.name = "location";
    location.type = column_type::real;
    location.is_geo_bounds = true;
    entry.properties[location.name] = location;
    auto regions = location;
    regions.name = "regions";
    regions.kind = property_kind::list;
    regions.nullable = true;
    entry.properties[regions.name] = regions;
    return {entry};
}

std::unique_ptr<swift_lattice_ref> route_ref(const std::string& path) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), route_schemas()));
#else
    return std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), route_schemas()));
#endif
}

std::unique_ptr<geo_bounds_list_ref> region_ref(const dynamic_object_ref& object) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<geo_bounds_list_ref>(object.get_geo_bounds_list("regions"));
#else
    return std::make_unique<geo_bounds_list_ref>(object.get_geo_bounds_list("regions"));
#endif
}

std::unique_ptr<dynamic_object_ref> add_route_item(swift_lattice_ref& owner,
                                                 const std::string& name, double coordinate) {
    swift_dynamic_object source;
    source.table_name = "ManagedRouteItem";
    source.properties = route_schemas()[0].properties;
    source.values["name"] = name;
    source.values["count"] = int64_t(1);
    source.values["optionalText"] = std::string("present");
    source.values["bytes"] = std::vector<uint8_t>{1};
    source.set_geo_bounds("location", geo_bounds(coordinate, coordinate, coordinate, coordinate));
    auto object = std::make_unique<dynamic_object_ref>(source);
    // Both physical stores deliberately contain the same primary key AND UUID.
    owner.get()->add_preserving_global_id(*object->get(), fake_uuid(1));
    return object;
}

int64_t physical_count(lattice::swift_lattice& owner, const std::string& table) {
    return std::get<int64_t>(owner.db().query("SELECT COUNT(*) AS n FROM " + table)[0].at("n"));
}
} // namespace

TEST(ManagedAttachmentLifetime, AliasReplacementRejectsLiveScalarsAndHeldFieldCopies) {
    TempDB main_path("scalar_lifetime_main"), arm_path("scalar_lifetime_\"arm");
    auto main = route_ref(main_path.str()), arm = route_ref(arm_path.str());
    main->get()->stop_audit_maintenance();
    arm->get()->stop_audit_maintenance();
    auto local = add_route_item(*main, "local", 60);
    auto original = add_route_item(*arm, "original", 2);
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    auto values = main->get()->objects("ManagedRouteItem", std::string("name = 'original'"));
    ASSERT_EQ(values.size(), 1u);
    dynamic_object_ref held(values[0]);
    auto count = managed_attachment_test_access::field<int64_t>(held, "count");
    auto optional = managed_attachment_test_access::field<std::optional<std::string>>(held, "optionalText");
    auto bytes = managed_attachment_test_access::field<std::vector<uint8_t>>(held, "bytes");
    auto bounds = managed_attachment_test_access::field<geo_bounds>(held, "location");
    auto optional_bounds = managed_attachment_test_access::field<std::optional<geo_bounds>>(held, "location");
    EXPECT_EQ(count.detach(), 1);
    EXPECT_TRUE(optional.has_value());
    EXPECT_EQ(bytes.detach(), (std::vector<uint8_t>{1}));
    EXPECT_DOUBLE_EQ(bounds.detach().min_lat, 2);
    EXPECT_TRUE(optional_bounds.detach().has_value());
    held.enable_row_cache();
    ASSERT_TRUE(main->get()->detach(*arm->get()));
    struct Directory {
        std::filesystem::path path;
        explicit Directory(std::filesystem::path value) : path(std::move(value)) {
            std::filesystem::create_directory(path);
        }
        ~Directory() { std::filesystem::remove_all(path); }
    } directory(arm_path.str() + "-replacement");
    auto replacement = route_ref((directory.path / arm_path.path.filename()).string());
    auto remote = add_route_item(*replacement, "replacement", 9);
    remote->set_int("count", 71);
    ASSERT_TRUE(main->get()->attach(*replacement->get()));
    ASSERT_EQ(held.managed_primary_key(), remote->managed_primary_key());
    // Same UUID and row id do not revive the old attachment generation.
    EXPECT_EQ(held.get_string("name"), "original") << "explicit cache hits remain snapshots";
    EXPECT_THROW(held.get()->get_int("count + 0"), db_error) << "cache misses remain guarded live reads";
    held.refresh_row_cache();
    EXPECT_EQ(held.get_string("name"), "original") << "refresh must not import replacement values";
    held.disable_row_cache();
    EXPECT_THROW(held.get()->get_int("count"), db_error);
    EXPECT_THROW(held.get()->get_string("name"), db_error);
    EXPECT_THROW(held.get()->has_value("optionalText"), db_error);
    EXPECT_THROW(held.get()->set_int("count", 99), db_error);
    EXPECT_THROW(held.get()->set_nil("optionalText"), db_error);
    EXPECT_THROW(held.get()->increment_int_field("count", 1), db_error);
    EXPECT_THROW(count.detach(), db_error);
    EXPECT_THROW(count = int64_t(99), db_error);
    EXPECT_THROW(optional.has_value(), db_error);
    EXPECT_THROW(optional.set_nil(), db_error);
    EXPECT_THROW(bytes.detach(), db_error);
    bytes.is_vector_column = true;
    const auto tables_before = physical_count(*main->get(), "main.sqlite_master");
    EXPECT_THROW(bytes.set_value(std::vector<uint8_t>{0, 0, 0, 0}), db_error);
    EXPECT_EQ(physical_count(*main->get(), "main.sqlite_master"), tables_before)
        << "stale vector route must fail before sidecar creation";
    EXPECT_THROW(bounds.detach(), db_error);
    EXPECT_THROW(optional_bounds.detach(), db_error);
    auto fresh_values = main->get()->objects("ManagedRouteItem", std::string("name = 'replacement'"));
    ASSERT_EQ(fresh_values.size(), 1u);
    dynamic_object_ref fresh(fresh_values[0]);
    EXPECT_EQ(fresh.get_int("count"), 71);
    fresh.set_int("count", 72);
    fresh.set_nil("optionalText");
    fresh.set_data("bytes", {3, 4});
    EXPECT_EQ(remote->get_int("count"), 72);
    EXPECT_FALSE(remote->has_value("optionalText"));
    EXPECT_EQ(remote->get_data("bytes"), (std::vector<uint8_t>{3, 4}));
    EXPECT_EQ(original->get_int("count"), 1);
    EXPECT_EQ(local->get_int("count"), 1);
    local->set_int("count", 5);
    EXPECT_EQ(local->get_int("count"), 5) << "pre-attach main fields stay live";
    ASSERT_TRUE(main->get()->detach(*replacement->get()));
}

TEST(ManagedAttachmentLifetime, SameFileReattachAndWriterReopenNeverReviveFields) {
    TempDB main_path("scalar_samefile_main"), arm_path("scalar_samefile_arm");
    auto main = route_ref(main_path.str()), arm = route_ref(arm_path.str());
    main->get()->stop_audit_maintenance();
    arm->get()->stop_audit_maintenance();
    auto original = add_route_item(*arm, "arm", 2);
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    auto values = main->get()->objects("ManagedRouteItem");
    ASSERT_EQ(values.size(), 1u);
    dynamic_object_ref held(values[0]);
    auto copy = managed_attachment_test_access::field<int64_t>(held, "count");
    ASSERT_TRUE(main->get()->detach(*arm->get()));
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    EXPECT_THROW(copy.detach(), db_error);
    EXPECT_THROW(held.get()->get_int("count"), db_error);
    auto fresh_values = main->get()->objects("ManagedRouteItem");
    ASSERT_EQ(fresh_values.size(), 1u);
    dynamic_object_ref fresh(fresh_values[0]);
    auto fresh_copy = managed_attachment_test_access::field<int64_t>(fresh, "count");
    EXPECT_EQ(fresh_copy.detach(), 1);
    const auto old_writer = fresh_copy.attachment_writer;
    main->get()->reopen_write_db();
    EXPECT_TRUE(old_writer.expired()) << "held models/fields must not retain a retired SQLite connection";
    EXPECT_THROW(fresh_copy.detach(), db_error);
    auto reopened_values = main->get()->objects("ManagedRouteItem");
    ASSERT_EQ(reopened_values.size(), 1u);
    dynamic_object_ref reopened(reopened_values[0]);
    EXPECT_EQ(reopened.get_int("count"), 1);
    reopened.set_int("count", 4);
    EXPECT_EQ(original->get_int("count"), 4);
    auto retired = managed_attachment_test_access::field<int64_t>(reopened, "count");
    main->get()->close_write_db();
    EXPECT_TRUE(retired.attachment_writer.expired());
    EXPECT_THROW(retired.detach(), db_error);
    main->get()->reopen_write_db();
    auto restored_values = main->get()->objects("ManagedRouteItem");
    ASSERT_EQ(restored_values.size(), 1u);
    dynamic_object_ref restored(restored_values[0]);
    EXPECT_EQ(restored.get_int("count"), 4)
        << "close_write/reopen must republish bindings from authoritative topology";
    ASSERT_TRUE(main->get()->detach(*arm->get()));
}

TEST(ManagedPhysicalRoute, SQLRoutePreservesExplicitSchemasAndQuotesSidecars) {
    EXPECT_EQ(managed_table_sql("Item"), "main.\"Item\"");
    EXPECT_EQ(managed_table_sql("aux.Item"), "aux.\"Item\"");
    EXPECT_EQ(managed_table_sql("\"a.\"\"b\".Item"), "\"a.\"\"b\".\"Item\"");
    EXPECT_EQ(managed_table_sql("main.\"Item\""), "main.\"Item\"");
    EXPECT_EQ(managed_sidecar_sql("\"a.\"\"b\".Item", "regions"),
              "\"a.\"\"b\".\"_Item_regions\"");
    EXPECT_EQ(managed_sidecar_sql("main.\"odd \"\"model\"", "regions"),
              "main.\"_odd \"\"model_regions\"");
}

TEST(ManagedPhysicalRoute, PreAttachScalarGeoAndHeldListStayOnMain) {
    TempDB main_path("managed_route_main"), arm_path("managed_route_arm");
    auto main = route_ref(main_path.str()), arm = route_ref(arm_path.str());
    auto local = add_route_item(*main, "local", 60);
    auto remote = add_route_item(*arm, "attached", 2);
    ASSERT_EQ(local->managed_primary_key(), remote->managed_primary_key());
    auto held_list = region_ref(*local);
    held_list->push_back(geo_bounds(60, 60, 60, 60));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    auto queried = main->get()->objects("ManagedRouteItem");
    ASSERT_EQ(queried.size(), 1u);
    dynamic_object_ref held_query(queried[0]);
    ASSERT_TRUE(main->get()->attach(*arm->get()));

    // The old SQL target really is the TEMP UNION view after ATTACH.
    EXPECT_THROW(main->get()->db().update("ManagedRouteItem", local->managed_primary_key(),
        {{"count", int64_t(99)}}), db_error);
    local->set_geo_bounds("location", 0, 0, 0, 0);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    held_query.set_string("name", "local-after");
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    held_query.set_int("count", 7);
    held_query.increment_int_field("count", 3);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    held_query.set_nil("optionalText");
    held_query.set_data("bytes", {4, 5});
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    held_list->push_back(geo_bounds(0, 0, 0, 0));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();

    EXPECT_EQ(local->get_string("name"), "local-after");
    EXPECT_EQ(local->get_int("count"), 10);
    EXPECT_EQ(local->get_data("bytes"), (std::vector<uint8_t>{4, 5}));
    EXPECT_FALSE(local->has_value("optionalText"));
    EXPECT_EQ(local->get_geo_bounds("location").min_lat, 0);
    EXPECT_EQ(remote->get_string("name"), "attached");
    EXPECT_EQ(remote->get_int("count"), 1);
    EXPECT_EQ(remote->get_geo_bounds("location").min_lat, 2);
    EXPECT_EQ(physical_count(*main->get(), "main._ManagedRouteItem_regions"), 2);
    EXPECT_EQ(physical_count(*arm->get(), "main._ManagedRouteItem_regions"), 0);
    EXPECT_EQ(main->get()->count_within_bbox("ManagedRouteItem", "location", -1, 1, -1, 1), 1);
    EXPECT_EQ(main->get()->count_within_bbox("ManagedRouteItem", "regions", -1, 1, -1, 1), 1);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
}

TEST(ManagedPhysicalRoute, AttachedGeoAndListWritesUseQuotedPhysicalSchema) {
    TempDB main_path("managed_route_receiver"), arm_path("managed_route_\"arm'quote");
    auto main = route_ref(main_path.str()), arm = route_ref(arm_path.str());
    auto local = add_route_item(*main, "local", 60);
    auto remote = add_route_item(*arm, "attached", 60);
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    const auto expected_alias = managed_quote_identifier(arm_path.path.filename().replace_extension().string());
    const auto source_labels = main->get()->db().query("SELECT name,_source FROM ManagedRouteItem ORDER BY name");
    ASSERT_EQ(source_labels.size(), 2u);
    EXPECT_EQ(std::get<std::string>(source_labels[0].at("name")), "attached");
    EXPECT_EQ(std::get<std::string>(source_labels[0].at("_source")), expected_alias);
    EXPECT_EQ(std::get<std::string>(source_labels[1].at("_source")), "main");
    auto rows = main->get()->objects("ManagedRouteItem", std::string("name = 'attached'"));
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(managed_route(rows[0].table_name()).schema_sql, expected_alias);
    EXPECT_EQ(managed_route(rows[0].table_name()).table, "ManagedRouteItem");
    dynamic_object_ref attached(rows[0]);
    attached.set_geo_bounds("location", geo_bounds(0, 0, 0, 0));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    attached.set_int("count", 9);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    auto regions = region_ref(attached);
    regions->push_back(geo_bounds(0, 0, 0, 0));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    regions->set(0, geo_bounds(30, 30, 30, 30));
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_EQ(local->get_geo_bounds("location").min_lat, 60);
    EXPECT_EQ(remote->get_geo_bounds("location").min_lat, 0);
    EXPECT_EQ(remote->get_int("count"), 9);
    EXPECT_EQ(physical_count(*main->get(), "main._ManagedRouteItem_regions"), 0);
    EXPECT_EQ(physical_count(*arm->get(), "main._ManagedRouteItem_regions"), 1);
    EXPECT_EQ(main->get()->count_within_bbox("ManagedRouteItem", "regions", 0, 2, 0, 2), 0);
    EXPECT_EQ(main->get()->count_within_bbox("ManagedRouteItem", "regions", 29, 31, 29, 31), 1);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    regions->erase(0);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_EQ(physical_count(*arm->get(), "main._ManagedRouteItem_regions"), 0);
}

TEST(ManagedPhysicalRoute, TriggerFreeListRowUpdatesOnlyItsPhysicalRTree) {
    database db(":memory:");
    const auto attached_schema = managed_quote_identifier("arm.\"quoted");
    db.execute("ATTACH DATABASE ':memory:' AS " + attached_schema);
    for (const auto& schema : {std::string("main"), attached_schema}) {
        db.execute("CREATE TABLE " + schema + ".regions "
                   "(id INTEGER PRIMARY KEY,minLat REAL,maxLat REAL,minLon REAL,maxLon REAL)");
        db.execute("CREATE VIRTUAL TABLE " + schema + ".regions_rtree USING rtree(id,minLat,maxLat,minLon,maxLon)");
        db.execute("INSERT INTO " + schema + ".regions VALUES(1,0,0,0,0)");
        db.execute("INSERT INTO " + schema + ".regions_rtree VALUES(1,0,0,0,0)");
    }
    const auto coordinate = [&](const std::string& schema, const std::string& table) {
        return std::get<double>(db.query("SELECT minLat FROM " + schema + '.' + table + " WHERE id = 1")[0].at("minLat"));
    };
    managed<geo_bounds*> local, attached;
    local.bind_to_list_row(&db, nullptr, "main.\"regions\"", "main.\"regions_rtree\"", 1, "parent");
    attached.bind_to_list_row(&db, nullptr, attached_schema + ".\"regions\"",
                              attached_schema + ".\"regions_rtree\"", 1, "parent");
    local = geo_bounds(10, 10, 10, 10);
    EXPECT_EQ(coordinate("main", "regions"), 10);
    EXPECT_EQ(coordinate("main", "regions_rtree"), 10);
    EXPECT_EQ(coordinate(attached_schema, "regions_rtree"), 0);
    attached = geo_bounds(30, 30, 30, 30);
    EXPECT_EQ(coordinate(attached_schema, "regions"), 30);
    EXPECT_EQ(coordinate(attached_schema, "regions_rtree"), 30);
    EXPECT_EQ(coordinate("main", "regions_rtree"), 10);
    // Preserve the historical optional-index contract, too.
    db.execute("DROP TABLE " + attached_schema + ".regions_rtree");
    EXPECT_NO_THROW(attached = geo_bounds(40, 40, 40, 40));
    EXPECT_EQ(coordinate(attached_schema, "regions"), 40);
}

TEST(ManagedPhysicalRoute, GeoBridgeReportsPrepareAndTriggerFailuresWithoutThrowing) {
    TempDB path("managed_route_errors");
    auto owner = route_ref(path.str());
    auto object = add_route_item(*owner, "local", 60);
    EXPECT_NO_THROW(object->set_geo_bounds("missing", 0, 0, 0, 0));
    EXPECT_NE(last_bridge_error().find("no such column"), std::string::npos);
    EXPECT_NO_THROW(object->set_geo_bounds("missing", geo_bounds(0, 0, 0, 0)));
    EXPECT_NE(last_bridge_error().find("no such column"), std::string::npos);
    auto regions = region_ref(*object);
    owner->get()->db().execute("CREATE TRIGGER reject_region BEFORE INSERT ON _ManagedRouteItem_regions "
                              "BEGIN SELECT RAISE(ABORT, 'deliberate region failure'); END");
    EXPECT_NO_THROW(regions->push_back(geo_bounds(0, 0, 0, 0)));
    EXPECT_NE(last_bridge_error().find("deliberate region failure"), std::string::npos);
    EXPECT_EQ(physical_count(*owner->get(), "main._ManagedRouteItem_regions"), 0);
    object->set_geo_bounds("location", 1, 1, 1, 1);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
}

TEST(ManagedPhysicalRoute, SealedBoundaryHandlesNonStandardExceptions) {
    static_assert(noexcept(record_bridge_error("failure")));
    EXPECT_NO_THROW(sealed([] { throw 7; }));
    EXPECT_EQ(last_bridge_error(), "Unknown C++ bridge exception");
    EXPECT_EQ(sealed([]() -> int { throw 7; }), 0);
    EXPECT_EQ(last_bridge_error(), "Unknown C++ bridge exception");
    EXPECT_EQ(sealed([] { return 12; }), 12);
    EXPECT_TRUE(last_bridge_error().empty());
}

TEST(ManagedPhysicalRoute, ClosedStateIsSharedAcrossWrappersForFileAndMemory) {
    TempDB path("managed_route_close");
    for (const auto& store : {path.str(), std::string(":memory:")}) {
        auto owner = route_ref(store);
        auto object = add_route_item(*owner, "local", 0);
#if LATTICE_HAS_FRT
        auto sibling = std::unique_ptr<swift_lattice_ref>(object->getLattice());
#else
        auto sibling = std::make_unique<swift_lattice_ref>(object->getLattice());
#endif
        ASSERT_NE(sibling.get(), nullptr);
        ASSERT_TRUE(sibling->valid());
        ASSERT_EQ(owner->get(), sibling->get());
        EXPECT_FALSE(owner->is_closed());
        EXPECT_FALSE(sibling->is_closed());
        sibling->close();
        EXPECT_TRUE(owner->is_closed());
        EXPECT_TRUE(sibling->is_closed());
        owner->close();
        EXPECT_TRUE(sibling->is_closed());
    }
}
