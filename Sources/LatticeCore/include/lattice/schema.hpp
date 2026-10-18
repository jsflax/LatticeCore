#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include "detail/observer_delivery_values.hpp"
#include <string>
#include <vector>
#include <typeinfo>
#include <unordered_map>
#include <functional>
#include <tuple>
#include <array>
#include <optional>
#include <string_view>
#include <map>
#include <stdexcept>
#include <memory>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <iterator>
#include <new>

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
    primitive,    // stored directly in table (string, int, double, bool, etc.)
    link,         // single object reference (nullable foreign key)
    list,         // collection via join table
    virtual_list, // polymorphic collection via discriminated join table
    virtual_link, // polymorphic single link via discriminated join table
    union_type    // owned union table reference (parent stores globalId as TEXT)
};

// Describes one associated value within a union case.
struct union_case_value {
    std::string param_name;    // label; empty for single unlabeled values
    column_type type;          // SQL column type (text for links/embedded, native for primitives)
    bool is_link = false;      // true = globalId ref to another model table
    std::string link_target;   // target table name if is_link (e.g., "Dog")
};

// Describes one case of a union enum.
// Bare cases (no associated values) have an empty `values` vector.
struct union_case {
    std::string case_name;
    std::vector<union_case_value> values;
};

// Describes a full union type — used to create and manage the union's internal table.
struct union_descriptor {
    std::string union_table_name;  // e.g., "_FeedItem"
    std::vector<union_case> cases;
};

// Property descriptor (runtime info about a property)
struct property_descriptor {
    std::string name;
    column_type type;                           // SQL type (for primitives)
    property_kind kind = property_kind::primitive;
    std::string target_table;                   // for link/list: referenced table; for union_type: union table name
    std::string link_table;                     // for list: the join table name
    bool nullable = false;
    bool is_vector = false;                     // true if this is a vector for similarity search
    bool is_geo_bounds = false;                 // true if this is a geo_bounds for R*Tree spatial indexing
    bool is_full_text = false;                  // true if this is a full-text search column (FTS5)
    bool is_indexed = false;                    // true if this property should have a non-unique index
    bool is_unique = false;                     // true if this property should have a unique constraint
    std::string column_name;                    // custom column name (empty = use field name)
    bool is_union = false;                      // true when kind == union_type
    union_descriptor union_desc;                // populated when is_union == true
    // Appended LAST (1.5.0) so Swift's imported memberwise init keeps its
    // existing argument order and only gains a trailing `no_history:`.
    bool no_history = false;                    // true: UPDATE audit rows record that the column changed, not its value
                                                // (a streamed/growing column — sync late-binds the live value)
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

namespace detail {
struct schema_generation_v2;
struct schema_registry_generation_test_access;
class schema_read_scope_v2;
std::uint64_t schema_registry_process_v2()noexcept;
using schema_model_map_v2=std::map<std::string,std::shared_ptr<const model_schema>,std::less<>>;
}
class schema_handle_v2;
class database;
class lattice_db;
class model_base;

// A retained immutable declaration generation. Iterators and raw .get() DATA
// are borrowed only while this snapshot/its handle remains alive and current
// in its creator process. No implicit raw-pointer or owning legacy conversion.
class schema_snapshot_v2 {
    friend class schema_registry;
    friend class schema_handle_v2;
    friend struct detail::schema_registry_generation_test_access;
    using shared=std::shared_ptr<const detail::schema_generation_v2>;
    const std::uint64_t creator_;
    alignas(shared) unsigned char shared_storage_[sizeof(shared)];
    shared& shared_()noexcept;
    const shared& shared_()const noexcept;
    void require_creator_()const;
    void adopt_(shared&&);
public:
    class iterator {
        friend class schema_snapshot_v2;
        const detail::schema_generation_v2* generation_=nullptr;
        std::uint64_t creator_=detail::schema_registry_process_v2();
        detail::schema_model_map_v2::const_iterator at_{};
        iterator(const detail::schema_generation_v2*,detail::schema_model_map_v2::const_iterator);
    public:
        using iterator_category=std::forward_iterator_tag;
        using value_type=const model_schema*;
        using difference_type=std::ptrdiff_t;
        using pointer=const model_schema**;
        using reference=const model_schema*;
        iterator()=default;
        const model_schema* operator*()const;
        iterator& operator++();
        iterator operator++(int){if(creator_!=detail::schema_registry_process_v2()||!generation_)throw std::runtime_error("inherited or empty schema iterator");auto copy=*this;++*this;return copy;}
        bool operator==(const iterator&)const;
    };
    schema_snapshot_v2()noexcept;
    schema_snapshot_v2(const schema_snapshot_v2&)noexcept;
    schema_snapshot_v2(schema_snapshot_v2&&)noexcept;
    schema_snapshot_v2& operator=(const schema_snapshot_v2&);
    schema_snapshot_v2& operator=(schema_snapshot_v2&&);
    ~schema_snapshot_v2()noexcept;
    bool valid()const noexcept;
    std::size_t size()const;
    bool empty()const{return size()==0;}
    std::uint64_t generation()const;
    iterator begin()const;
    iterator end()const;
    schema_handle_v2 get_schema(const std::type_info&)const;
    schema_handle_v2 get_schema(std::string_view)const;
};
class schema_handle_v2 {
    friend class schema_snapshot_v2;
    schema_snapshot_v2 generation_;
    const model_schema* model_=nullptr; // immutable DATA owned by generation_
    schema_handle_v2(const schema_snapshot_v2& generation,const model_schema* model)
        :generation_(generation),model_(model){}
public:
    schema_handle_v2()=default;
    bool valid()const noexcept{return model_&&generation_.valid();}
    explicit operator bool()const noexcept{return valid();}
    const model_schema* get()const noexcept{return valid()?model_:nullptr;}
    const model_schema* operator->()const;
    const model_schema& operator*()const{return *operator->();}
    bool operator==(const schema_handle_v2& other)const noexcept{return get()==other.get();}
    bool operator==(std::nullptr_t)const noexcept{return !valid();}
};

// Runtime registration replaces whole immutable values. Publication is only a
// generation comparison/ownership transfer; all allocation and disposal occur
// outside that gate. Public return types are deliberately versioned retained
// DATA; callers must rebuild and keep their snapshot/handle through each use.
class schema_registry {
    friend struct detail::schema_registry_generation_test_access;
    friend class detail::schema_read_scope_v2;
    struct publication_probe {
        void(*after_candidate)(void*,schema_registry&)=nullptr;
        void* context=nullptr;
    };
    struct fields {
        std::mutex gate;
        std::shared_ptr<const detail::schema_generation_v2> current;
        std::atomic<const publication_probe*> observation{nullptr};
    };
    class publication_hold {
        const std::uint64_t creator_;
        fields* actual_=nullptr;
    public:
        explicit publication_hold(const schema_registry&,bool try_only=false);
        bool entered()const noexcept{return actual_!=nullptr;}
        ~publication_hold()noexcept;
        publication_hold(const publication_hold&)=delete;
        publication_hold& operator=(const publication_hold&)=delete;
    };
    const std::uint64_t creator_;
    alignas(fields) unsigned char fields_storage_[sizeof(fields)];
    fields& fields_()const noexcept;
    void require_creator_()const;
    static std::optional<schema_snapshot_v2> try_entry_snapshot_();
    schema_registry()noexcept;
    ~schema_registry()noexcept;
    schema_registry(const schema_registry&)=delete;
    schema_registry& operator=(const schema_registry&)=delete;
public:
    static schema_registry& instance();
    void register_model(const std::type_info& type,model_schema schema);
    schema_snapshot_v2 all_schemas()const;
    std::optional<schema_snapshot_v2> bounded_schemas(std::size_t maximum)const;
    schema_handle_v2 get_schema(const std::type_info& type)const;
    schema_handle_v2 get_schema(std::string_view table_name)const;
};

namespace detail {
// Internal lexical DATA lease. Only actual Core SQL/maintenance entry and the
// typed observation path install it. Nested entry borrows the enclosing view;
// no registry publication gate is acquired while a Core Native frame is live.
// A coherent synchronous operation (including its reentrant callbacks) sees
// one generation; the next outer entry sees the then-current publication.
class schema_read_scope_v2 {
    friend class ::lattice::database;
    friend class ::lattice::lattice_db;
    friend class ::lattice::model_base;
    friend class managed_route_scope;
    friend struct schema_registry_generation_test_access;
    const std::uint64_t creator_=schema_registry_process_v2();
    schema_snapshot_v2 view_;
    schema_read_scope_v2* previous_=nullptr;
    bool entered_=false,publication_busy_=false;
    static thread_local schema_read_scope_v2* current_;
    explicit schema_read_scope_v2(bool may_capture,bool try_only=false);
    bool publication_busy()const noexcept{return publication_busy_;}
public:
    ~schema_read_scope_v2()noexcept;
    schema_read_scope_v2(const schema_read_scope_v2&)=delete;
    schema_read_scope_v2& operator=(const schema_read_scope_v2&)=delete;
    static schema_handle_v2 lookup(std::string_view);
};
}

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

#define LATTICE_DELIVERY_ADOPT_PROP(cls, prop) \
    prop.row_id = actual_row;

#define LATTICE_DELIVERY_MATERIALIZE_PROP(cls, prop) \
    ::lattice::detail::observer_delivery_value_detail::materialize<decltype(cls::prop)>(this->prop, row, #prop, \
        std::array<std::string_view,4>{#prop "_minLat",#prop "_maxLat",#prop "_minLon",#prop "_maxLon"});

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

// Immutable producer DATA: builtin member pointers and literal names, never
// a collector function selected by a same-spelled callable on a custom model.
#define LATTICE_DELIVERY_MEMBER(cls, prop) \
    ::lattice::detail::observer_delivery_member{std::string_view{#prop}, &cls::prop, \
        std::array<std::string_view,4>{#prop "_minLat",#prop "_maxLat",#prop "_minLon",#prop "_maxLon"}},

#define LATTICE_DELIVERY_OUTPUT_MEMBER(cls, prop) \
    ::lattice::detail::observer_delivery_output_member<::lattice::managed<cls>,decltype(cls::prop)>{ \
        std::string_view{#prop}, &::lattice::managed<cls>::prop, \
        std::array<std::string_view,4>{#prop "_minLat",#prop "_maxLat",#prop "_minLon",#prop "_maxLon"}},

// Main schema registration macro
#define LATTICE_SCHEMA(cls, ...) \
    template<> \
    struct lattice::managed<cls> : ::lattice::model_base { \
        inline static constexpr std::string_view observer_delivery_table = #cls; \
        inline static constexpr auto observer_delivery_members = std::tuple{ \
            L_FOR_EACH(LATTICE_DELIVERY_MEMBER, cls, __VA_ARGS__) \
        }; \
        /* Property members */ \
        L_FOR_EACH(LATTICE_DECLARE_MANAGED, cls, __VA_ARGS__) \
        inline static constexpr auto observer_delivery_outputs = std::tuple{ \
            L_FOR_EACH(LATTICE_DELIVERY_OUTPUT_MEMBER, cls, __VA_ARGS__) \
        }; \
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
        /* Closed immutable row image, after credit and before BEGIN. */ \
        void observer_delivery_materialize(::lattice::detail::observer_delivery_values::row_view row) { \
            L_FOR_EACH(LATTICE_DELIVERY_MATERIALIZE_PROP, cls, __VA_ARGS__) \
        } \
        \
        /* Strings/property custody are prepared before the owned SQL unit. */ \
        void observer_delivery_adopt_row(::lattice::primary_key_t actual_row) noexcept { \
            this->id_ = actual_row; \
            L_FOR_EACH(LATTICE_DELIVERY_ADOPT_PROP, cls, __VA_ARGS__) \
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
    template<> struct lattice::detail::observer_delivery_generated_schema<cls> : std::true_type {}; \
    static ::lattice::schema_registrar<cls> _lattice_registrar_##cls { \
        ::lattice::managed<cls>::schema() \
    }

#endif // __cplusplus
