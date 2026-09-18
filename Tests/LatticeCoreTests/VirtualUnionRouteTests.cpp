#include "TestHelpers.hpp"
#include <lattice.hpp>

namespace {
using namespace lattice;
SchemaVector virtual_schemas(std::initializer_list<const char*> tables) {
    SchemaVector result;
    for (auto table : tables) {
        swift_schema_entry entry; entry.table_name = table;
        for (const auto& name : {"name", "country"}) {
            property_descriptor p; p.name = name; p.type = column_type::text;
            entry.properties[p.name] = p;
        }
        property_descriptor rank; rank.name = "rank"; rank.type = column_type::integer;
        entry.properties[rank.name] = rank;
        result.push_back(entry);
    }
    return result;
}
std::unique_ptr<swift_lattice_ref> virtual_owner(const std::string& path, const SchemaVector& schemas) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), schemas));
#else
    return std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), schemas));
#endif
}
std::unique_ptr<dynamic_object_ref> virtual_item(swift_lattice_ref& owner, const char* table,
                                                const std::string& name, const std::string& country,
                                                int64_t rank, const std::string& global_id) {
    swift_dynamic_object source; source.table_name = table;
    source.properties = virtual_schemas({table})[0].properties;
    source.values["name"] = name; source.values["country"] = country; source.values["rank"] = rank;
    auto object = std::make_unique<dynamic_object_ref>(source);
    owner.get()->add_preserving_global_id(*object->get(), global_id);
    EXPECT_EQ(object->get()->lattice.get(), owner.get());
    EXPECT_NE(object->managed_primary_key(), 0);
    return object;
}
}

TEST(VirtualUnionRoute, AttachedOnlyModelRetainsIdentityBoundPredicateAndLiveWrites) {
    TempDB main_path("virtual_restaurant"), arm_path("virtual_\"museum'arm");
    auto main = virtual_owner(main_path.str(), virtual_schemas({"UnionRestaurant"}));
    auto arm = virtual_owner(arm_path.str(), virtual_schemas({"UnionMuseum"}));
    auto restaurant = virtual_item(*main, "UnionRestaurant", "Restaurant", "US", 1, fake_uuid(1));
    auto museum = virtual_item(*arm, "UnionMuseum", "Louvre", "France", 2, fake_uuid(2));
    ASSERT_EQ(restaurant->managed_primary_key(), museum->managed_primary_key());
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    const auto alias = managed_quote_identifier(arm_path.path.stem().string());

    const auto raw = main->get()->query_union_rows({"UnionRestaurant", "UnionMuseum"},
        std::string("country = ?"), std::string("rank ASC"), std::nullopt, std::nullopt,
        {std::string("France")});
    ASSERT_EQ(raw.size(), 1u);
    EXPECT_EQ(std::get<std::string>(raw[0].at("_type")), "UnionMuseum");
    EXPECT_EQ(std::get<std::string>(raw[0].at("_source")), alias);
    EXPECT_GT(std::get<int64_t>(raw[0].at("_lattice_attach_token")), 0);
    auto rows = main->get()->union_objects({"UnionRestaurant", "UnionMuseum"},
        std::string("country = ?"), std::string("rank ASC"), std::nullopt, std::nullopt,
        {std::string("France")});
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    ASSERT_EQ(rows.size(), 1u);
    dynamic_object_ref hydrated(rows[0]);
    ASSERT_EQ(hydrated.get()->lattice.get(), main->get());
    EXPECT_EQ(managed_route(rows[0].table_name()).schema_sql, alias);
    EXPECT_EQ(hydrated.get_model_table_name(), "UnionMuseum");
    EXPECT_EQ(hydrated.get()->get_model_table_name(), "UnionMuseum");
    EXPECT_EQ(hydrated.get_table_name(), alias + ".UnionMuseum");
    EXPECT_EQ(museum->get_model_table_name(), "UnionMuseum");
    EXPECT_EQ(hydrated.get_int("id"), museum->get_int("id"));
    EXPECT_EQ(hydrated.get_string("globalId"), museum->get_string("globalId"));
    EXPECT_EQ(hydrated.get_string("name"), "Louvre");
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    hydrated.set_string("name", "Louvre updated"); hydrated.set_int("rank", 9);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    database fresh_arm(arm_path.str());
    const auto persisted = fresh_arm.query("SELECT name,rank FROM UnionMuseum WHERE id=1");
    ASSERT_EQ(persisted.size(), 1u);
    EXPECT_EQ(std::get<std::string>(persisted[0].at("name")), "Louvre updated");
    EXPECT_EQ(std::get<int64_t>(persisted[0].at("rank")), 9);
    EXPECT_EQ(restaurant->get_string("name"), "Restaurant");
    EXPECT_EQ(restaurant->get_int("rank"), 1);
}

TEST(VirtualUnionRoute, MixedMainAndRoutedArmsKeepReplicasSeparateWithPagination) {
    TempDB main_path("virtual_replicas_main"), arm_path("virtual_replicas_arm");
    auto main = virtual_owner(main_path.str(), virtual_schemas({"UnionRestaurant", "UnionMuseum"}));
    auto arm = virtual_owner(arm_path.str(), virtual_schemas({"UnionMuseum"}));
    auto restaurant = virtual_item(*main, "UnionRestaurant", "Restaurant", "France", 0, fake_uuid(1));
    auto local = virtual_item(*main, "UnionMuseum", "Local museum", "France", 1, fake_uuid(2));
    auto remote = virtual_item(*arm, "UnionMuseum", "Remote museum", "France", 2, fake_uuid(2));
    ASSERT_EQ(local->managed_primary_key(), remote->managed_primary_key());
    ASSERT_TRUE(main->get()->attach(*arm->get()));
    const auto raw = main->get()->query_union_rows({"UnionRestaurant", "UnionMuseum"},
        std::string("country = ?"), std::string("rank ASC"), std::nullopt, std::nullopt,
        {std::string("France")});
    ASSERT_EQ(raw.size(), 3u);
    EXPECT_EQ(std::get<std::string>(raw[0].at("_source")), "main");
    EXPECT_EQ(std::get<int64_t>(raw[0].at("_lattice_attach_token")), 0);
    EXPECT_EQ(std::get<std::string>(raw[1].at("_source")), "main");
    EXPECT_EQ(std::get<int64_t>(raw[1].at("_lattice_attach_token")), 0);
    EXPECT_EQ(std::get<std::string>(raw[2].at("_source")), managed_quote_identifier(arm_path.path.stem().string()));
    EXPECT_GT(std::get<int64_t>(raw[2].at("_lattice_attach_token")), 0);
    auto rows = main->get()->union_objects({"UnionRestaurant", "UnionMuseum"},
        std::string("country = ?"), std::string("rank ASC"), std::nullopt, std::nullopt,
        {std::string("France")});
    ASSERT_EQ(rows.size(), 3u);
    dynamic_object_ref local_hydrated(rows[1]), remote_hydrated(rows[2]);
    dynamic_object_ref restaurant_hydrated(rows[0]);
    EXPECT_EQ(restaurant_hydrated.get_model_table_name(), "UnionRestaurant");
    EXPECT_EQ(restaurant_hydrated.get_table_name(), "main.UnionRestaurant");
    EXPECT_EQ(local_hydrated.get_model_table_name(), "UnionMuseum");
    EXPECT_EQ(remote_hydrated.get_model_table_name(), "UnionMuseum");
    EXPECT_EQ(local_hydrated.get_table_name(), "main.UnionMuseum");
    EXPECT_NE(local_hydrated.get_table_name(), remote_hydrated.get_table_name());
    EXPECT_EQ(local_hydrated.get_int("id"), remote_hydrated.get_int("id"));
    EXPECT_EQ(local_hydrated.get_string("globalId"), remote_hydrated.get_string("globalId"));
    EXPECT_EQ(local_hydrated.get_string("name"), "Local museum");
    EXPECT_EQ(remote_hydrated.get_string("name"), "Remote museum");
    remote_hydrated.set_string("name", "Remote only"); remote_hydrated.set_int("rank", 20);
    ASSERT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    EXPECT_EQ(local->get_string("name"), "Local museum"); EXPECT_EQ(local->get_int("rank"), 1);
    EXPECT_EQ(remote->get_string("name"), "Remote only"); EXPECT_EQ(remote->get_int("rank"), 20);
    auto page = main->get()->union_objects({"UnionRestaurant", "UnionMuseum"},
        std::string("country = ?"), std::string("rank ASC"), int64_t(1), int64_t(1),
        {std::string("France")});
    ASSERT_EQ(page.size(), 1u);
    dynamic_object_ref middle(page[0]);
    EXPECT_EQ(middle.get_string("name"), "Local museum");
    EXPECT_EQ(managed_route(page[0].table_name()).schema_sql, "main");
    EXPECT_EQ(restaurant->get_int("rank"), 0);
}

TEST(VirtualUnionRoute, OrdinaryRegisteredMainArmIgnoresUnroutedTempShadow) {
    TempDB path("virtual_main_shadow");
    auto owner = virtual_owner(path.str(), virtual_schemas({"UnionRestaurant"}));
    auto original = virtual_item(*owner, "UnionRestaurant", "Physical main", "US", 7, fake_uuid(1));
    // A deterministic shadow proves that an unrouted registered-model arm
    // selects physical main. It does not grant arbitrary-view query support.
    owner->get()->read_db().execute("CREATE TEMP VIEW UnionRestaurant AS SELECT id,globalId,"
        "name||'-shadow' AS name,country,rank+1000 AS rank FROM main.UnionRestaurant");
    auto shadow = owner->get()->read_db().query("SELECT name FROM UnionRestaurant");
    ASSERT_EQ(shadow.size(), 1u);
    EXPECT_EQ(std::get<std::string>(shadow[0].at("name")), "Physical main-shadow");
    const auto raw = owner->get()->query_union_rows({"UnionRestaurant"});
    ASSERT_EQ(raw.size(), 1u);
    EXPECT_EQ(std::get<std::string>(raw[0].at("_type")), "UnionRestaurant");
    EXPECT_EQ(std::get<std::string>(raw[0].at("name")), "Physical main");
    EXPECT_EQ(std::get<int64_t>(raw[0].at("rank")), 7);
    EXPECT_EQ(raw[0].count("_source"), 0u);
    EXPECT_EQ(raw[0].count("_lattice_attach_token"), 0u);
    EXPECT_EQ(original->get_int("rank"), 7);
}

TEST(VirtualUnionRoute, UnmanagedLogicalNamePreservesDotsWithoutRouteParsing) {
    swift_dynamic_object source;
    source.table_name = "literal.model.name";
    dynamic_object object(source);
    dynamic_object_ref reference(source);
    EXPECT_EQ(object.get_model_table_name(), "literal.model.name");
    EXPECT_EQ(reference.get_model_table_name(), "literal.model.name");
    EXPECT_EQ(reference.get_table_name(), "literal.model.name");
}
