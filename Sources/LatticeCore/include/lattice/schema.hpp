#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include <string>
#include <vector>
#include <typeinfo>
#include <unordered_map>
#include <functional>
#include <tuple>
#include <array>

namespace lattice {

// Forward declarations
template<typename T, typename> struct managed;
class database;

// Type trait to get column_type from C++ type
template<typename T>
struct type_to_column;

template<> struct type_to_column<int64_t> {
    static constexpr column_type value = column_type::integer;
};
template<> struct type_to_column<int> {
    static constexpr column_type value = column_type::integer;
};
template<> struct type_to_column<bool> {
    static constexpr column_type value = column_type::integer;
};
template<> struct type_to_column<double> {
    static constexpr column_type value = column_type::real;
};
template<> struct type_to_column<float> {
    static constexpr column_type value = column_type::real;
};
template<> struct type_to_column<std::string> {
    static constexpr column_type value = column_type::text;
};
template<> struct type_to_column<std::vector<uint8_t>> {
    static constexpr column_type value = column_type::blob;
};
template<> struct type_to_column<timestamp_t> {
    static constexpr column_type value = column_type::real;
};
template<> struct type_to_column<uuid_t> {
    static constexpr column_type value = column_type::text;
};

// Optional handling
template<typename T>
struct type_to_column<std::optional<T>> {
    static constexpr column_type value = type_to_column<T>::value;
};

// Primitive vector handling (stored as JSON TEXT)
template<typename T>
struct type_to_column<std::vector<T>> {
    static constexpr column_type value = column_type::text;  // JSON encoded
};

// Type trait to detect if a type is a link (pointer to model)
template<typename T>
struct is_link_type : std::false_type {};

template<typename T>
struct is_link_type<T*> : std::true_type {};

template<typename T>
struct is_link_type<std::vector<T*>> : std::true_type {};

// Property kind - distinguishes primitives from relationships
enum class property_kind {
    primitive,   // stored directly in table (string, int, double, bool, etc.)
    link,        // single object reference (nullable foreign key)
    list         // collection via join table
};

// Property descriptor (runtime info about a property)
struct property_descriptor {
    std::string name;
    column_type type;                           // SQL type (for primitives)
    property_kind kind = property_kind::primitive;
    std::string target_table;                   // for link/list: the referenced table
    std::string link_table;                     // for list: the join table name
    bool nullable = false;
    bool is_vector = false;                     // true if this is a vector for similarity search
    bool is_geo_bounds = false;                 // true if this is a geo_bounds for R*Tree spatial indexing
    bool is_full_text = false;                  // true if this is a full-text search column (FTS5)
    bool is_indexed = false;                    // true if this property should have a non-unique index
};

// Type trait to detect geo_bounds types (single value)
template<typename T>
struct is_geo_bounds_type : std::false_type {};

template<>
struct is_geo_bounds_type<geo_bounds> : std::true_type {};

template<>
struct is_geo_bounds_type<std::optional<geo_bounds>> : std::true_type {};

// Type trait to detect geo_bounds list types (vector)
template<typename T>
struct is_geo_bounds_list_type : std::false_type {};

template<>
struct is_geo_bounds_list_type<std::vector<geo_bounds>> : std::true_type {};

// Schema info for a model type
struct model_schema {
    std::string table_name;
    std::vector<property_descriptor> properties;
};

// Global schema registry
class schema_registry {
public:
    static schema_registry& instance();

    void register_model(const std::type_info& type, model_schema schema);
    const model_schema* get_schema(const std::type_info& type) const;
    const model_schema* get_schema(const std::string& table_name) const;

    std::vector<const model_schema*> all_schemas() const;

private:
    schema_registry() = default;
    std::unordered_map<std::string, model_schema> schemas_by_name_;
    std::unordered_map<std::string, std::string> type_to_table_;
};

// Helper to register schema at static init time
template<typename T>
struct schema_registrar {
    explicit schema_registrar(model_schema schema) {
        schema_registry::instance().register_model(typeid(T), std::move(schema));
    }
};

} // namespace lattice

// ============================================================================
// LATTICE_SCHEMA Macro System (inspired by realm-cpp)
//
// Usage:
//   struct Trip {
//       std::string name;
//       int days;
//   };
//   LATTICE_SCHEMA(Trip, name, days)
// ============================================================================

// FOR_EACH variadic macro helpers (realm-cpp style - recursive concatenation)
#define LFE_0(WHAT, cls)
#define LFE_1(WHAT, cls, X) WHAT(cls, X)
#define LFE_2(WHAT, cls, X, ...) WHAT(cls, X) LFE_1(WHAT, cls, __VA_ARGS__)
#define LFE_3(WHAT, cls, X, ...) WHAT(cls, X) LFE_2(WHAT, cls, __VA_ARGS__)
#define LFE_4(WHAT, cls, X, ...) WHAT(cls, X) LFE_3(WHAT, cls, __VA_ARGS__)
#define LFE_5(WHAT, cls, X, ...) WHAT(cls, X) LFE_4(WHAT, cls, __VA_ARGS__)
#define LFE_6(WHAT, cls, X, ...) WHAT(cls, X) LFE_5(WHAT, cls, __VA_ARGS__)
#define LFE_7(WHAT, cls, X, ...) WHAT(cls, X) LFE_6(WHAT, cls, __VA_ARGS__)
#define LFE_8(WHAT, cls, X, ...) WHAT(cls, X) LFE_7(WHAT, cls, __VA_ARGS__)
#define LFE_9(WHAT, cls, X, ...) WHAT(cls, X) LFE_8(WHAT, cls, __VA_ARGS__)
#define LFE_10(WHAT, cls, X, ...) WHAT(cls, X) LFE_9(WHAT, cls, __VA_ARGS__)
#define LFE_11(WHAT, cls, X, ...) WHAT(cls, X) LFE_10(WHAT, cls, __VA_ARGS__)
#define LFE_12(WHAT, cls, X, ...) WHAT(cls, X) LFE_11(WHAT, cls, __VA_ARGS__)
#define LFE_13(WHAT, cls, X, ...) WHAT(cls, X) LFE_12(WHAT, cls, __VA_ARGS__)
#define LFE_14(WHAT, cls, X, ...) WHAT(cls, X) LFE_13(WHAT, cls, __VA_ARGS__)
#define LFE_15(WHAT, cls, X, ...) WHAT(cls, X) LFE_14(WHAT, cls, __VA_ARGS__)
#define LFE_16(WHAT, cls, X, ...) WHAT(cls, X) LFE_15(WHAT, cls, __VA_ARGS__)

#define L_GET_MACRO(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, NAME, ...) NAME

#define L_FOR_EACH(action, cls, ...) \
    L_GET_MACRO(_0, __VA_ARGS__, \
        LFE_16, LFE_15, LFE_14, LFE_13, LFE_12, LFE_11, LFE_10, LFE_9, \
        LFE_8, LFE_7, LFE_6, LFE_5, LFE_4, LFE_3, LFE_2, LFE_1, LFE_0)(action, cls, __VA_ARGS__)

// Individual action macros (each includes its own terminator)
#define LATTICE_DECLARE_MANAGED(cls, prop) \
    ::lattice::managed<decltype(cls::prop)> prop;

// Bind property to parent - each managed<> type extracts what it needs
#define LATTICE_BIND_PROP(cls, prop) \
    prop.bind_to_parent(this, #prop);

// Helper to set unmanaged value, skipping links
namespace lattice { namespace detail {
    template<typename OriginalType, typename ManagedType, typename SrcType>
    void set_unmanaged_if_not_link(ManagedType& dst, const SrcType& src) {
        if constexpr (!is_link_type<OriginalType>::value) {
            dst.unmanaged_value = src;
        }
    }
}}

#define LATTICE_SET_UNMANAGED(cls, prop) \
    ::lattice::detail::set_unmanaged_if_not_link<decltype(cls::prop)>(this->prop, src.prop);

// Helper to collect values, skipping links and geo_bounds lists, handling single geo_bounds specially
namespace lattice { namespace detail {
    template<typename OriginalType, typename ManagedType>
    void collect_if_not_link(std::vector<std::pair<std::string, column_value_t>>& out,
                             const char* name, const ManagedType& prop) {
        if constexpr (is_link_type<OriginalType>::value) {
            // Skip links - they're handled separately
        } else if constexpr (is_geo_bounds_list_type<OriginalType>::value) {
            // Skip geo_bounds lists - they're stored in separate tables
        } else if constexpr (is_geo_bounds_type<OriginalType>::value) {
            // geo_bounds expands to 4 columns
            std::string base = name;
            if constexpr (is_optional<OriginalType>::value) {
                if (prop.unmanaged_value.has_value()) {
                    out.push_back({base + "_minLat", prop.unmanaged_value->min_lat});
                    out.push_back({base + "_maxLat", prop.unmanaged_value->max_lat});
                    out.push_back({base + "_minLon", prop.unmanaged_value->min_lon});
                    out.push_back({base + "_maxLon", prop.unmanaged_value->max_lon});
                } else {
                    out.push_back({base + "_minLat", nullptr});
                    out.push_back({base + "_maxLat", nullptr});
                    out.push_back({base + "_minLon", nullptr});
                    out.push_back({base + "_maxLon", nullptr});
                }
            } else {
                out.push_back({base + "_minLat", prop.unmanaged_value.min_lat});
                out.push_back({base + "_maxLat", prop.unmanaged_value.max_lat});
                out.push_back({base + "_minLon", prop.unmanaged_value.min_lon});
                out.push_back({base + "_maxLon", prop.unmanaged_value.max_lon});
            }
        } else {
            out.push_back({name, get_prop_value(prop.unmanaged_value)});
        }
    }
}}

#define LATTICE_COLLECT_VALUE(cls, prop) \
    ::lattice::detail::collect_if_not_link<decltype(cls::prop)>(result, #prop, this->prop);

// Helper to collect geo_bounds lists for persistence
namespace lattice { namespace detail {
    template<typename OriginalType, typename ManagedType>
    void collect_geo_bounds_list(std::vector<std::pair<std::string, std::vector<geo_bounds>>>& out,
                                 const char* name, const ManagedType& prop) {
        if constexpr (is_geo_bounds_list_type<OriginalType>::value) {
            out.push_back({name, prop.unmanaged_value});
        }
    }
}}

#define LATTICE_COLLECT_GEO_BOUNDS_LIST(cls, prop) \
    ::lattice::detail::collect_geo_bounds_list<decltype(cls::prop)>(result, #prop, this->prop);

// Type trait to get the target type from a link type
template<typename T>
struct link_target_type { using type = T; };

template<typename T>
struct link_target_type<T*> { using type = T; };

template<typename T>
struct link_target_type<std::vector<T*>> { using type = T; };

// Helper to add property descriptors for all property types
namespace lattice { namespace detail {
    template<typename PropType>
    void add_property_descriptor(std::vector<property_descriptor>& out, const char* name) {
        property_descriptor desc;
        desc.name = name;

        if constexpr (is_geo_bounds_list_type<PropType>::value) {
            // Geographic bounds list property (stored in separate table with R*Tree)
            desc.kind = property_kind::list;
            desc.type = column_type::real;  // Underlying column type for geo values
            desc.is_geo_bounds = true;
            desc.nullable = true;  // Lists are always nullable
        } else if constexpr (is_geo_bounds_type<PropType>::value) {
            // Geographic bounds property (expands to 4 columns + R*Tree)
            desc.kind = property_kind::primitive;
            desc.type = column_type::real;  // Underlying column type
            desc.is_geo_bounds = true;
            desc.nullable = is_optional<PropType>::value;
        } else if constexpr (is_link_type<PropType>::value) {
            // Link or list property
            using target_t = typename link_target_type<PropType>::type;

            // Check if it's a list (vector of pointers) or single link (pointer)
            if constexpr (std::is_same_v<PropType, std::vector<target_t*>>) {
                desc.kind = property_kind::list;
                desc.type = column_type::integer;  // Not directly stored, but set a default
                desc.nullable = true;  // Lists don't create columns, but mark nullable for safety
            } else {
                desc.kind = property_kind::link;
                desc.type = column_type::integer;  // Foreign key is integer
                desc.nullable = true;  // Link foreign keys are always nullable
            }
            // Note: target_table will be set by the schema registration system
            // once we have the target type's table name
        } else {
            // Primitive property
            desc.kind = property_kind::primitive;
            desc.type = type_to_column<typename unwrap_optional<PropType>::type>::value;
            desc.nullable = is_optional<PropType>::value;
        }

        out.push_back(std::move(desc));
    }
}}

#define LATTICE_PROP_DESC(cls, prop) \
    ::lattice::detail::add_property_descriptor<decltype(cls::prop)>(props, #prop);

// Main schema registration macro
#define LATTICE_SCHEMA(cls, ...) \
    template<> \
    struct lattice::managed<cls> : ::lattice::model_base { \
        /* Property members */ \
        L_FOR_EACH(LATTICE_DECLARE_MANAGED, cls, __VA_ARGS__) \
        \
        /* Default constructor */ \
        managed() = default; \
        \
        /* Construct from unmanaged object */ \
        explicit managed(const cls& src) { \
            L_FOR_EACH(LATTICE_SET_UNMANAGED, cls, __VA_ARGS__) \
        } \
        \
        /* Bind all properties to database */ \
        void bind_to_db() { \
            L_FOR_EACH(LATTICE_BIND_PROP, cls, __VA_ARGS__) \
        } \
        \
        /* Get unmanaged values for insert (skips link properties) */ \
        std::vector<std::pair<std::string, ::lattice::column_value_t>> collect_values() const { \
            std::vector<std::pair<std::string, ::lattice::column_value_t>> result; \
            L_FOR_EACH(LATTICE_COLLECT_VALUE, cls, __VA_ARGS__) \
            return result; \
        } \
        \
        /* Get geo_bounds list values for insert */ \
        std::vector<std::pair<std::string, std::vector<::lattice::geo_bounds>>> collect_geo_bounds_lists() const { \
            std::vector<std::pair<std::string, std::vector<::lattice::geo_bounds>>> result; \
            L_FOR_EACH(LATTICE_COLLECT_GEO_BOUNDS_LIST, cls, __VA_ARGS__) \
            return result; \
        } \
        \
        /* Schema metadata (skips link properties) */ \
        static const ::lattice::model_schema& schema() { \
            static ::lattice::model_schema s = []() { \
                std::vector<::lattice::property_descriptor> props; \
                L_FOR_EACH(LATTICE_PROP_DESC, cls, __VA_ARGS__) \
                return ::lattice::model_schema{#cls, props}; \
            }(); \
            return s; \
        } \
        \
        /* Object-level observation with typed object_change<cls> */ \
        using change_callback_t = std::function<void(::lattice::object_change<cls>&)>; \
        ::lattice::notification_token observe(change_callback_t callback) { \
            return observe_base([this, callback](bool is_deleted, const std::vector<std::string>& changed_props) { \
                ::lattice::object_change<cls> change; \
                change.object = this; \
                change.is_deleted = is_deleted; \
                for (const auto& prop_name : changed_props) { \
                    ::lattice::property_change<cls> prop_change; \
                    prop_change.name = prop_name; \
                    change.property_changes.push_back(std::move(prop_change)); \
                } \
                callback(change); \
            }); \
        } \
    }; \
    static ::lattice::schema_registrar<cls> _lattice_registrar_##cls { \
        ::lattice::managed<cls>::schema() \
    }

#endif // __cplusplus
