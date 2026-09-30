#include <gtest/gtest.h>
#include <dynamic_object.hpp>
#include <list.hpp>

namespace {
using namespace lattice;

using Properties = std::unordered_map<std::string, property_descriptor>;

Properties construction_properties() {
    Properties properties;
    const auto add = [&](const char* name, column_type type,
                         property_kind kind = property_kind::primitive,
                         bool nullable = false, bool geo = false) {
        property_descriptor p;
        p.name = name;
        p.type = type;
        p.kind = kind;
        p.nullable = nullable;
        p.is_geo_bounds = geo;
        properties[name] = p;
    };
    add("count", column_type::integer);
    add("score", column_type::real);
    add("title", column_type::text);
    add("bytes", column_type::blob);
    add("optional", column_type::text, property_kind::primitive, true);
    add("bounds", column_type::real, property_kind::primitive, false, true);
    add("children", column_type::integer, property_kind::list, true);
    add("virtualChildren", column_type::integer, property_kind::virtual_list, true);
    add("virtualChild", column_type::integer, property_kind::virtual_link, true);
    return properties;
}

std::shared_ptr<dynamic_object> construct(const Properties& props, bool direct) {
    if (direct) {
#if LATTICE_HAS_FRT
        std::unique_ptr<dynamic_object_ref> ref(dynamic_object_ref::create_unmanaged("Constructed", props));
        return ref->shared();
#else
        return dynamic_object_ref::create_unmanaged("Constructed", props).shared();
#endif
    }
    // Keep the old Swift path's two const-reference copies in the oracle.
    swift_dynamic_object unmanaged("Constructed", props);
    dynamic_object object(unmanaged);
#if LATTICE_HAS_FRT
    std::unique_ptr<dynamic_object_ref> ref(dynamic_object_ref::wrap(object));
    return ref->shared();
#else
    return dynamic_object_ref::wrap(object).shared();
#endif
}

std::unique_ptr<link_list_ref> children(const dynamic_object& object, const std::string& name) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<link_list_ref>(object.get_link_list(name));
#else
    return std::make_unique<link_list_ref>(object.get_link_list(name));
#endif
}
}

TEST(UnmanagedConstruction, DirectFactoryPreservesDefaultsAndUnmanagedIdentity) {
    const auto props = construction_properties();
    auto direct = construct(props, true);
    auto legacy = construct(props, false);
    EXPECT_EQ(direct->get_table_name(), legacy->get_table_name());
    EXPECT_EQ(direct->get_model_table_name(), "Constructed");
    EXPECT_EQ(direct->managed_primary_key(), 0);
    EXPECT_FALSE(direct->lattice);
    EXPECT_FALSE(direct->is_row_cache_enabled());
    EXPECT_FALSE(direct->has_query_row_image());
    for (const auto* key : {"count", "score", "title", "bytes", "optional", "bounds",
                            "bounds_minLat", "bounds_maxLat", "bounds_minLon", "bounds_maxLon"}) {
        EXPECT_EQ(direct->has_value(key), legacy->has_value(key)) << key;
    }
    for (const auto* key : {"count", "score", "title", "bytes",
                            "bounds_minLat", "bounds_maxLat", "bounds_minLon", "bounds_maxLon"}) {
        ASSERT_TRUE(direct->has_value(key)) << key;
    }
    EXPECT_EQ(direct->get_int("count"), 0);
    EXPECT_DOUBLE_EQ(direct->get_double("score"), 0.0);
    EXPECT_EQ(direct->get_string("title"), "");
    EXPECT_TRUE(direct->get_data("bytes").empty());
    EXPECT_FALSE(direct->has_value("optional"));
    EXPECT_FALSE(direct->has_value("bounds"));
    for (const auto* key : {"bounds_minLat", "bounds_maxLat", "bounds_minLon", "bounds_maxLon"}) {
        EXPECT_DOUBLE_EQ(direct->get_double(key), 0.0);
    }
    for (const auto* key : {"children", "virtualChildren", "virtualChild"}) {
        EXPECT_EQ(children(*direct, key)->size(), 0u);
        EXPECT_EQ(children(*legacy, key)->size(), 0u);
    }
}

TEST(UnmanagedConstruction, FreshInstancesOwnTheirDefaultsAndSchema) {
    auto props = construction_properties();
    auto first = construct(props, true);
    auto second = construct(props, true);
    props.clear(); // The object must own its descriptor after the call returns.
    first->set_int("count", 23);
    first->set_string("title", "changed");
    first->set_data("bytes", std::vector<uint8_t>{1, 2, 3});
    first->set_double("bounds_minLat", 4.5);
    // JSON enumerates the owned schema; defaults alone would not prove that
    // the descriptor map survived its caller's destruction.
    const auto json = first->to_json(0);
    EXPECT_NE(json.find("\"count\":23"), std::string::npos);
    EXPECT_NE(json.find("\"title\":\"changed\""), std::string::npos);
    EXPECT_EQ(second->get_int("count"), 0);
    EXPECT_EQ(second->get_string("title"), "");
    EXPECT_TRUE(second->get_data("bytes").empty());
    EXPECT_DOUBLE_EQ(second->get_double("bounds_minLat"), 0.0);

    swift_dynamic_object child("Child", Properties{});
    // The raw swift_dynamic_object overload is a no-op for unmanaged lists.
    // Append an owning dynamic_object_ref on both bridge ownership paths.
#if LATTICE_HAS_FRT
    std::unique_ptr<dynamic_object_ref> child_ref(dynamic_object_ref::wrap(dynamic_object(child)));
#else
    const auto child_ref = dynamic_object_ref::wrap(dynamic_object(child));
#endif
    for (const auto* key : {"children", "virtualChildren", "virtualChild"}) {
        auto first_list = children(*first, key);
#if LATTICE_HAS_FRT
        first_list->push_back(*child_ref);
#else
        first_list->push_back(child_ref);
#endif
        EXPECT_EQ(first_list->size(), 1u);
        EXPECT_EQ(children(*second, key)->size(), 0u);
    }
}
