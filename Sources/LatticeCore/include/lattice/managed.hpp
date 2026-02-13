#pragma once

#ifdef __cplusplus

#include "types.hpp"
#include "db.hpp"
#include "observation.hpp"
#include "../nlohmann/json.hpp"
#include <memory>
#include <string>
#include <ostream>
#include <unordered_map>
#include <map>
#include <vector>
#include <functional>
#include <mutex>
#include "schema.hpp"

// ============================================================================
// nlohmann::json ADL serialization for lattice types
// ============================================================================

namespace lattice {

// uuid_t serialization - stores as string (ADL works since uuid_t is in lattice namespace)
inline void to_json(nlohmann::json& j, const uuid_t& u) {
    j = u.to_string();
}

inline void from_json(const nlohmann::json& j, uuid_t& u) {
    if (j.is_string()) {
        u = uuid_t::from_string(j.get<std::string>());
    }
}

} // namespace lattice

// timestamp_t is std::chrono::time_point, so ADL won't find functions in lattice namespace.
// Use nlohmann's adl_serializer specialization instead.
namespace nlohmann {
template <>
struct adl_serializer<lattice::timestamp_t> {
    static void to_json(json& j, const lattice::timestamp_t& t) {
        auto duration = t.time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        j = static_cast<double>(millis) / 1000.0;
    }

    static void from_json(const json& j, lattice::timestamp_t& t) {
        if (j.is_number()) {
            double seconds = j.get<double>();
            auto millis = static_cast<int64_t>(seconds * 1000.0);
            t = lattice::timestamp_t(std::chrono::milliseconds(millis));
        }
    }
};
} // namespace nlohmann

#if __has_include(<swift/bridging>)
#include <swift/bridging>
#define CONFORMS_TO_MANAGED \
SWIFT_CONFORMS_TO_PROTOCOL(LatticeSwiftModule.CxxManagedType)
#define CONFORMS_TO_OPTIONAL_MANAGED \
SWIFT_CONFORMS_TO_PROTOCOL(LatticeSwiftModule.OptionalProtocol) \
SWIFT_CONFORMS_TO_PROTOCOL(LatticeSwiftModule.CxxManagedType)
#else
#define CONFORMS_TO_MANAGED
#define CONFORMS_TO_OPTIONAL_MANAGED
#ifndef SWIFT_NAME
#define SWIFT_NAME(_name)
#endif
#ifndef SWIFT_COMPUTED_PROPERTY
#define SWIFT_COMPUTED_PROPERTY
#endif
#endif


namespace lattice {

// Forward declarations
class lattice_db;
class model_base;

// Primary template - never instantiated directly
// Specializations exist for primitives, models, links (T*), and lists (std::vector<T*>)
template<typename T, typename = void>
struct managed;

// ============================================================================
// managed_base - Base for property wrappers (holds DB binding info)
// ============================================================================

struct managed_base {
    database* db = nullptr;
    lattice_db* lattice = nullptr;
    std::string table_name;
    std::string column_name;
    primary_key_t row_id = 0;
    bool is_vector_column = false;  // True if this column is a vector for similarity search

    bool is_bound() const { return db != nullptr && row_id != 0; }

    void assign(database* d, lattice_db* l, const std::string& table,
                const std::string& col, primary_key_t id) {
        db = d;
        lattice = l;
        table_name = table;
        column_name = col;
        row_id = id;
    }

    // Called by LATTICE_BIND_PROP macro - implementation below after model_base
    void bind_to_parent(model_base* parent, const char* prop_name);
};

// ============================================================================
// model_base - Base for model objects (holds object identity)
// ============================================================================

// ============================================================================
// object_observer_registry - External storage for object observers
// Allows model_base to remain copyable
// ============================================================================

class object_observer_registry {
public:
    using object_observer_t = std::function<void(bool is_deleted, const std::vector<std::string>& changed_props)>;
    using observer_key_t = std::pair<std::string, global_id_t>;  // (table_name, global_id)

    static object_observer_registry& instance() {
        static object_observer_registry registry;
        return registry;
    }

    uint64_t add_observer(const observer_key_t& key, object_observer_t callback) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto id = next_id_++;
        observers_[key][id] = std::move(callback);
        return id;
    }

    void remove_observer(const observer_key_t& key, uint64_t observer_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = observers_.find(key);
        if (it != observers_.end()) {
            it->second.erase(observer_id);
            if (it->second.empty()) {
                observers_.erase(it);
            }
        }
    }

    void notify(const observer_key_t& key, bool is_deleted, const std::vector<std::string>& changed_props) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = observers_.find(key);
        if (it != observers_.end()) {
            for (auto& [id, callback] : it->second) {
                callback(is_deleted, changed_props);
            }
        }
    }

    void notify_deleted(const observer_key_t& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = observers_.find(key);
        if (it != observers_.end()) {
            for (auto& [id, callback] : it->second) {
                callback(true, {});
            }
            observers_.erase(it);
        }
    }

private:
    object_observer_registry() = default;
    std::mutex mutex_;
    std::map<observer_key_t, std::unordered_map<uint64_t, object_observer_t>> observers_;
    uint64_t next_id_ = 0;
};

class model_base {
public:
    virtual ~model_base() = default;

    bool is_managed() const { return db_ != nullptr && id_ != 0; }
    bool is_valid() const { return is_managed(); }
    primary_key_t id() const { return id_; }
    const global_id_t& global_id() const { return global_id_; }
    const std::string& table_name() const { return table_name_; }

    // Type-erased property access (for Swift C++ interop)
    virtual column_value_t get_value(const std::string& name) const {
        auto it = unmanaged_values_.find(name);
        return it != unmanaged_values_.end() ? it->second : column_value_t{nullptr};
    }

    virtual void set_value(const std::string& name, const column_value_t& value) {
        if (is_managed()) {
            db_->update(table_name_, id_, {{name, value}});
        }
        unmanaged_values_[name] = value;
        // Notify observers of property change
        notify_property_change(name);
    }

    void set_schema(const std::string& table,
                    std::vector<std::string> prop_names,
                    std::unordered_map<std::string, column_type> prop_types) {
        table_name_ = table;
        property_names_ = std::move(prop_names);
        property_types_ = std::move(prop_types);
    }

    std::vector<std::string> property_names() const { return property_names_; }

    column_type property_type(const std::string& name) const {
        auto it = property_types_.find(name);
        return it != property_types_.end() ? it->second : column_type::text;
    }

    bool has_property(const std::string& name) const {
        return property_types_.find(name) != property_types_.end();
    }

    // ========================================================================
    // Object-level observation (type-erased)
    // ========================================================================

    using object_observer_t = object_observer_registry::object_observer_t;

    /// Register an observer for this object. Returns token that unregisters on destruction.
    notification_token observe_base(object_observer_t callback) {
        if (!is_managed() || global_id_.empty()) {
            // Can't observe unmanaged objects
            return notification_token();
        }
        auto key = std::make_pair(table_name_, global_id_);
        auto observer_id = object_observer_registry::instance().add_observer(key, std::move(callback));
        return notification_token([key, observer_id]() {
            object_observer_registry::instance().remove_observer(key, observer_id);
        });
    }

    /// Notify observers of a property change
    void notify_property_change(const std::string& property_name) {
        if (is_managed() && !global_id_.empty()) {
            auto key = std::make_pair(table_name_, global_id_);
            object_observer_registry::instance().notify(key, false, {property_name});
        }
    }

    /// Notify observers of deletion
    void notify_deleted() {
        if (is_managed() && !global_id_.empty()) {
            auto key = std::make_pair(table_name_, global_id_);
            object_observer_registry::instance().notify_deleted(key);
        }
    }

protected:
    friend class lattice_db;
    friend struct managed_base;  // For bind_to_parent access
    template<typename U, typename> friend struct managed;
    template<typename U> friend class query;

    database* db_ = nullptr;
    lattice_db* lattice_ = nullptr;
    std::string table_name_;
    primary_key_t id_ = 0;
    global_id_t global_id_;

    std::vector<std::string> property_names_;
    std::unordered_map<std::string, column_type> property_types_;
    mutable std::unordered_map<std::string, column_value_t> unmanaged_values_;
};

// Implementation of managed_base::bind_to_parent (needs model_base definition)
inline void managed_base::bind_to_parent(model_base* parent, const char* prop_name) {
    db = parent->db_;
    lattice = parent->lattice_;
    table_name = parent->table_name_;
    column_name = prop_name;
    row_id = parent->id_;
}

// ============================================================================
// managed<T> specializations for primitive types
// ============================================================================

template<>
struct CONFORMS_TO_MANAGED managed<int64_t> : managed_base {
    using SwiftType = int64_t;
    using OptionalType = managed<std::optional<int64_t>>;
    
    int64_t unmanaged_value = 0;

    managed() = default;
    managed(int64_t v) : unmanaged_value(v) {
    }

    managed& operator=(int64_t v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, v}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] int64_t detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    return std::get<int64_t>(it->second);
                }
            }
        }
        return unmanaged_value;
    }

    operator int64_t() const SWIFT_NAME(get()) {
        return this->detach();
    }
};

// MARK: managed<int>
template<>
struct CONFORMS_TO_MANAGED managed<int> : managed_base {
    using SwiftType = int;
    using OptionalType = managed<std::optional<int>>;

    int unmanaged_value = 0;
    
    managed() = default;
    managed(int v) : unmanaged_value(v) {}

    managed& operator=(int v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, static_cast<int64_t>(v)}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] int detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    return static_cast<int>(std::get<int64_t>(it->second));
                }
            }
        }
        return unmanaged_value;
    }

    operator int() const SWIFT_NAME(get()) { return detach(); }
};

// MARK: managed<double>
template<>
struct CONFORMS_TO_MANAGED managed<double> : managed_base {
    using SwiftType = double;
    using OptionalType = managed<std::optional<double>>;
    
    double unmanaged_value;
    
    managed() = default;
    managed(double v) : unmanaged_value(v) {}
    
    managed& operator=(double v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, v}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] double detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<double>(it->second)) {
                    return std::get<double>(it->second);
                }
            }
        }
        return unmanaged_value;
    }

    operator double() const SWIFT_NAME(get()) { return detach(); }
};

// managed<float>
template<>
struct CONFORMS_TO_MANAGED managed<float> : managed_base {
    using SwiftType = float;
    using OptionalType = managed<std::optional<float>>;
    
    float unmanaged_value = 0.0f;

    managed() = default;
    managed(float v) : unmanaged_value(v) {}

    managed& operator=(float v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, static_cast<double>(v)}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] float detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<double>(it->second)) {
                    return static_cast<float>(std::get<double>(it->second));
                }
            }
        }
        return unmanaged_value;
    }

    operator float() const SWIFT_NAME(get()) { return detach(); }
};

// MARK: managed<bool>
template<>
struct CONFORMS_TO_MANAGED managed<bool> : managed_base {
    using SwiftType = bool;
    using OptionalType = managed<std::optional<bool>>;
    
    bool unmanaged_value = false;

    managed() = default;
    managed(bool v) : unmanaged_value(v) {}

    managed& operator=(bool v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, static_cast<int64_t>(v ? 1 : 0)}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] bool detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    return std::get<int64_t>(it->second) != 0;
                }
            }
        }
        return unmanaged_value;
    }

    operator bool() const SWIFT_NAME(get()) { return detach(); }
};

// MARK: managed<std::string>
template<>
struct CONFORMS_TO_MANAGED managed<std::string> : managed_base {
    using OptionalType = managed<std::optional<std::string>>;
    using SwiftType = std::string;
    
    mutable std::string unmanaged_value;
    mutable std::string cached_value;

    managed() = default;
//    managed(const std::string& v) : unmanaged_value(v) {}
//    managed(const char* v) : unmanaged_value(v) {}

    bool empty() const { return detach().empty(); }
    size_t size() const { return detach().size(); }

    const char* c_str() const {
        cached_value = detach();
        return cached_value.c_str();
    }

    bool operator==(const std::string& other) const { return detach() == other; }
    bool operator==(const char* other) const { return detach() == other; }
    bool operator!=(const std::string& other) const { return detach() != other; }

    friend std::ostream& operator<<(std::ostream& os, const managed<std::string>& m) {
        return os << m.detach();
    }
    
    managed& operator=(std::string v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, v}});
        }
        unmanaged_value = v;
        return *this;
    }
    
    operator std::string() const SWIFT_NAME(get()) { return detach(); }
    
    [[nodiscard]] std::string detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
                    return std::get<std::string>(it->second);
                }
            }
        }
        return unmanaged_value;
    }
};

// MARK: managed<timestamp_t>
template<>
struct CONFORMS_TO_MANAGED managed<timestamp_t> : managed_base {
    using OptionalType = managed<std::optional<timestamp_t>>;
    using SwiftType = timestamp_t;
    
    timestamp_t unmanaged_value{};

    managed() = default;
    managed(timestamp_t v) : unmanaged_value(v) {}

    managed& operator=(timestamp_t v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, detail::to_column_value(v)}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] timestamp_t detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && std::holds_alternative<double>(it->second)) {
                    return detail::from_column_value<timestamp_t>(it->second);
                }
            }
        }
        return unmanaged_value;
    }

    operator timestamp_t() const SWIFT_NAME(get()) { return detach(); }

    static timestamp_t now() { return std::chrono::system_clock::now(); }
};

// MARK: managed<uuid_t>
template<>
struct CONFORMS_TO_MANAGED managed<uuid_t> : managed_base {
    using OptionalType = managed<std::optional<uuid_t>>;
    using SwiftType = uuid_t;
    
    uuid_t unmanaged_value{};

    managed() = default;
    managed(const uuid_t& v) : unmanaged_value(v) {}

    managed& operator=(const uuid_t& v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, detail::to_column_value(v)}});
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] uuid_t detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end() && !std::holds_alternative<std::nullptr_t>(it->second)) {
                    return detail::from_column_value<uuid_t>(it->second);
                }
            }
        }
        return unmanaged_value;
    }

    operator uuid_t() const SWIFT_NAME(get()) { return detach(); }

    // Convenience: convert to string
    std::string to_string() const { return detach().to_string(); }

    // Check if nil
    bool is_nil() const { return detach().is_nil(); }

    // Generate new UUID
    static uuid_t generate() { return uuid_t::generate(); }
};

// MARK: managed<geo_bounds>
template<>
struct CONFORMS_TO_MANAGED managed<geo_bounds> : managed_base {
    using SwiftType = geo_bounds;
    using OptionalType = managed<std::optional<geo_bounds>>;

    geo_bounds unmanaged_value{};
    std::string rtree_table_;  // e.g., "_Place_location_rtree"

    managed() = default;
    managed(const geo_bounds& v) : unmanaged_value(v) {}
    managed(double minLat, double maxLat, double minLon, double maxLon)
        : unmanaged_value(minLat, maxLat, minLon, maxLon) {}

    // Override bind_to_parent to set up R*Tree table name
    void bind_to_parent(model_base* parent, const char* prop_name) {
        managed_base::bind_to_parent(parent, prop_name);
        rtree_table_ = "_" + parent->table_name_ + "_" + prop_name + "_rtree";
    }

    // For dynamic objects with property_descriptor
    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        rtree_table_ = "_" + parent->table_name_ + "_" + p.name + "_rtree";
    }

    // Column names for the 4 components (main table)
    std::string col_min_lat() const { return column_name + "_minLat"; }
    std::string col_max_lat() const { return column_name + "_maxLat"; }
    std::string col_min_lon() const { return column_name + "_minLon"; }
    std::string col_max_lon() const { return column_name + "_maxLon"; }

    // R*Tree table name
    const std::string& rtree_table() const { return rtree_table_; }

    managed& operator=(const geo_bounds& v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(table_name, row_id, {
                {col_min_lat(), v.min_lat},
                {col_max_lat(), v.max_lat},
                {col_min_lon(), v.min_lon},
                {col_max_lon(), v.max_lon}
            });
            // R*Tree stays in sync via triggers
        }
        unmanaged_value = v;
        return *this;
    }

    [[nodiscard]] geo_bounds detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + col_min_lat() + ", " + col_max_lat() + ", " +
                col_min_lon() + ", " + col_max_lon() +
                " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                const auto& row = rows[0];
                auto get_double = [&](const std::string& col) -> double {
                    auto it = row.find(col);
                    if (it != row.end() && std::holds_alternative<double>(it->second)) {
                        return std::get<double>(it->second);
                    }
                    return 0.0;
                };
                return geo_bounds(
                    get_double(col_min_lat()),
                    get_double(col_max_lat()),
                    get_double(col_min_lon()),
                    get_double(col_max_lon())
                );
            }
        }
        return unmanaged_value;
    }

    operator geo_bounds() const SWIFT_NAME(get()) { return detach(); }

    // Convenience accessors
    double min_lat() const { return detach().min_lat; }
    double max_lat() const { return detach().max_lat; }
    double min_lon() const { return detach().min_lon; }
    double max_lon() const { return detach().max_lon; }

    double center_lat() const { return detach().center_lat(); }
    double center_lon() const { return detach().center_lon(); }

    bool is_point() const { return detach().is_point(); }

    bool contains(double lat, double lon) const { return detach().contains(lat, lon); }
    bool intersects(const geo_bounds& other) const { return detach().intersects(other); }
};

// MARK: managed<std::optional<geo_bounds>>
template<>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<std::optional<geo_bounds>> : managed_base {
    using SwiftType = std::optional<geo_bounds>;
    using OptionalType = managed<std::optional<geo_bounds>>;
    using Wrapped = geo_bounds;

    std::optional<geo_bounds> unmanaged_value;
    std::string rtree_table_;

    managed() = default;
    managed(const geo_bounds& v) : unmanaged_value(v) {}
    managed(std::nullopt_t) : unmanaged_value(std::nullopt) {}

    // Override bind_to_parent to set up R*Tree table name
    void bind_to_parent(model_base* parent, const char* prop_name) {
        managed_base::bind_to_parent(parent, prop_name);
        rtree_table_ = "_" + parent->table_name_ + "_" + prop_name + "_rtree";
    }

    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        rtree_table_ = "_" + parent->table_name_ + "_" + p.name + "_rtree";
    }

    // Column names
    std::string col_min_lat() const { return column_name + "_minLat"; }
    std::string col_max_lat() const { return column_name + "_maxLat"; }
    std::string col_min_lon() const { return column_name + "_minLon"; }
    std::string col_max_lon() const { return column_name + "_maxLon"; }

    const std::string& rtree_table() const { return rtree_table_; }

    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return *this->detach();
    }
    void setPointee(Wrapped wrapped) SWIFT_COMPUTED_PROPERTY {
        this->operator=(std::optional(wrapped));
    }

    managed& operator=(std::optional<geo_bounds> v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            if (v) {
                db->update(table_name, row_id, {
                    {col_min_lat(), v->min_lat},
                    {col_max_lat(), v->max_lat},
                    {col_min_lon(), v->min_lon},
                    {col_max_lon(), v->max_lon}
                });
            } else {
                db->update(table_name, row_id, {
                    {col_min_lat(), nullptr},
                    {col_max_lat(), nullptr},
                    {col_min_lon(), nullptr},
                    {col_max_lon(), nullptr}
                });
            }
        }
        unmanaged_value = v;
        return *this;
    }

    managed& operator=(std::nullopt_t) {
        if (is_bound()) {
            db->update(table_name, row_id, {
                {col_min_lat(), nullptr},
                {col_max_lat(), nullptr},
                {col_min_lon(), nullptr},
                {col_max_lon(), nullptr}
            });
        }
        unmanaged_value = std::nullopt;
        return *this;
    }

    void set_nil() {
        *this = std::nullopt;
    }

    [[nodiscard]] std::optional<geo_bounds> detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + col_min_lat() + ", " + col_max_lat() + ", " +
                col_min_lon() + ", " + col_max_lon() +
                " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                const auto& row = rows[0];
                auto it = row.find(col_min_lat());
                // If any column is NULL, return nullopt
                if (it != row.end() && std::holds_alternative<std::nullptr_t>(it->second)) {
                    return std::nullopt;
                }
                auto get_double = [&](const std::string& col) -> double {
                    auto it2 = row.find(col);
                    if (it2 != row.end() && std::holds_alternative<double>(it2->second)) {
                        return std::get<double>(it2->second);
                    }
                    return 0.0;
                };
                return geo_bounds(
                    get_double(col_min_lat()),
                    get_double(col_max_lat()),
                    get_double(col_min_lon()),
                    get_double(col_max_lon())
                );
            }
        }
        return unmanaged_value;
    }

    operator std::optional<geo_bounds>() const SWIFT_NAME(get()) { return detach(); }

    bool has_value() const SWIFT_NAME(hasValue()) {
        return detach().has_value();
    }

    geo_bounds value() const SWIFT_NAME(value()) { return *detach(); }

    explicit operator bool() const { return has_value(); }
};

// ============================================================================
// MARK: managed<std::vector<geo_bounds>> - Geo bounds list (stored in separate table with R*Tree)
// ============================================================================

class lattice_db;  // Forward declaration

template<>
struct CONFORMS_TO_MANAGED managed<std::vector<geo_bounds>> : managed_base {
    using SwiftType = std::vector<geo_bounds>;
    using OptionalType = managed<std::vector<geo_bounds>>;

    mutable std::vector<geo_bounds> unmanaged_value;
    mutable std::vector<geo_bounds> cached_objects_;
    mutable bool is_loaded_ = false;
    std::string list_table_;    // e.g., "_Place_regions"
    std::string rtree_table_;   // e.g., "_Place_regions_rtree"
    std::string parent_global_id_;  // globalId of parent object

    managed() = default;
    managed(const std::vector<geo_bounds>& val) : unmanaged_value(val) {}
    managed(std::vector<geo_bounds>&& val) : unmanaged_value(std::move(val)) {}

    // Bind to parent - sets up list table name
    void bind_to_parent(model_base* parent, const char* prop_name) {
        managed_base::bind_to_parent(parent, prop_name);
        list_table_ = "_" + parent->table_name_ + "_" + std::string(prop_name);
        rtree_table_ = list_table_ + "_rtree";
        parent_global_id_ = parent->global_id_;
    }

    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        list_table_ = "_" + parent->table_name_ + "_" + p.name;
        rtree_table_ = list_table_ + "_rtree";
        parent_global_id_ = parent->global_id_;
    }

    const std::string& list_table() const { return list_table_; }
    const std::string& rtree_table() const { return rtree_table_; }

    // Load from database
    void load_if_needed() const {
        if (is_loaded_ || !is_bound()) return;

        cached_objects_.clear();
        std::string sql = "SELECT minLat, maxLat, minLon, maxLon FROM " + list_table_ +
                          " WHERE parent_id = ? ORDER BY id";
        auto rows = db->query(sql, {parent_global_id_});

        for (const auto& row : rows) {
            auto get_double = [&](const std::string& col) -> double {
                auto it = row.find(col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    return std::get<double>(it->second);
                }
                return 0.0;
            };
            cached_objects_.emplace_back(
                get_double("minLat"),
                get_double("maxLat"),
                get_double("minLon"),
                get_double("maxLon")
            );
        }
        is_loaded_ = true;
    }

    // Size
    size_t size() const {
        if (!is_bound()) return unmanaged_value.size();
        load_if_needed();
        return cached_objects_.size();
    }

    bool empty() const { return size() == 0; }

    // Read-only access
    [[nodiscard]] std::vector<geo_bounds> detach() const {
        if (!is_bound()) return unmanaged_value;
        load_if_needed();
        return cached_objects_;
    }

    operator std::vector<geo_bounds>() const SWIFT_NAME(get()) { return detach(); }

    // Index access
    const geo_bounds& operator[](size_t index) const {
        load_if_needed();
        return is_bound() ? cached_objects_[index] : unmanaged_value[index];
    }

    // Iterators
    using iterator = typename std::vector<geo_bounds>::iterator;
    using const_iterator = typename std::vector<geo_bounds>::const_iterator;

    const_iterator begin() const {
        load_if_needed();
        return is_bound() ? cached_objects_.begin() : unmanaged_value.begin();
    }

    const_iterator end() const {
        load_if_needed();
        return is_bound() ? cached_objects_.end() : unmanaged_value.end();
    }

    // Add a geo_bounds to the list
    void push_back(const geo_bounds& bounds);

    // Remove a geo_bounds by index
    void erase(size_t index);

    // Clear all geo_bounds from the list
    void clear();

    // Assign entire list
    managed& operator=(const std::vector<geo_bounds>& val) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            clear();
            for (const auto& b : val) {
                push_back(b);
            }
        }
        unmanaged_value = val;
        return *this;
    }
};

// ============================================================================
// MARK: managed<geo_bounds*> - A single geo_bounds stored as a row in a list table
// This is different from managed<geo_bounds> which stores in 4 columns of the parent table.
// This version wraps a geo_bounds that lives in a separate list table row.
// ============================================================================

template<>
struct CONFORMS_TO_MANAGED managed<geo_bounds*> : managed_base {
    using SwiftType = geo_bounds*;
    using OptionalType = managed<geo_bounds*>;

    mutable geo_bounds cached_value_;
    mutable bool is_loaded_ = false;
    std::string list_table_;      // e.g., "_Place_regions"
    std::string rtree_table_;     // e.g., "_Place_regions_rtree"
    int64_t list_row_id_ = 0;     // Row ID within the list table
    std::string parent_global_id_;

    managed() = default;
    managed(const geo_bounds& v) : cached_value_(v), is_loaded_(true) {}

    // Bind to a specific row in a list table
    void bind_to_list_row(database* db_ptr, lattice_db* lattice_ptr,
                          const std::string& list_table, const std::string& rtree_table,
                          int64_t list_row_id, const std::string& parent_gid) {
        db = db_ptr;
        lattice = lattice_ptr;
        list_table_ = list_table;
        rtree_table_ = rtree_table;
        list_row_id_ = list_row_id;
        parent_global_id_ = parent_gid;
    }

    bool is_bound() const { return db != nullptr && list_row_id_ != 0; }

    void load_if_needed() const {
        if (is_loaded_ || !is_bound()) return;

        std::string sql = "SELECT minLat, maxLat, minLon, maxLon FROM " + list_table_ +
                          " WHERE id = ?";
        auto rows = db->query(sql, {list_row_id_});

        if (!rows.empty()) {
            const auto& row = rows[0];
            auto get_double = [&](const std::string& col) -> double {
                auto it = row.find(col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    return std::get<double>(it->second);
                }
                return 0.0;
            };
            cached_value_ = geo_bounds(
                get_double("minLat"),
                get_double("maxLat"),
                get_double("minLon"),
                get_double("maxLon")
            );
        }
        is_loaded_ = true;
    }

    [[nodiscard]] geo_bounds detach() const {
        load_if_needed();
        return cached_value_;
    }

    operator geo_bounds() const SWIFT_NAME(get()) { return detach(); }

    // Assignment updates the database row
    managed& operator=(const geo_bounds& v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            db->update(list_table_, list_row_id_, {
                {"minLat", v.min_lat},
                {"maxLat", v.max_lat},
                {"minLon", v.min_lon},
                {"maxLon", v.max_lon}
            });
            // Update R*Tree if it exists
            if (db->table_exists(rtree_table_)) {
                db->execute("UPDATE " + rtree_table_ + " SET minLat = ?, maxLat = ?, minLon = ?, maxLon = ? WHERE id = ?",
                    {v.min_lat, v.max_lat, v.min_lon, v.max_lon, list_row_id_});
            }
        }
        cached_value_ = v;
        is_loaded_ = true;
        return *this;
    }

    // Accessors for geo_bounds fields
    double min_lat() const { return detach().min_lat; }
    double max_lat() const { return detach().max_lat; }
    double min_lon() const { return detach().min_lon; }
    double max_lon() const { return detach().max_lon; }

    // Convenience methods from geo_bounds
    double center_lat() const { return detach().center_lat(); }
    double center_lon() const { return detach().center_lon(); }
    double lat_span() const { return detach().lat_span(); }
    double lon_span() const { return detach().lon_span(); }
    bool is_point() const { return detach().is_point(); }
    bool contains(double lat, double lon) const { return detach().contains(lat, lon); }
    bool intersects(const geo_bounds& other) const { return detach().intersects(other); }
};

// ============================================================================
// MARK: managed<std::vector<geo_bounds*>> - Geo bounds list with pointer semantics
// Similar to managed<std::vector<T*>> for models, but for geo_bounds.
// Each geo_bounds in the list is stored as a row in a separate table with R*Tree index.
// ============================================================================

template<>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<std::vector<geo_bounds*>> : managed_base {
    using OptionalType = managed<std::vector<geo_bounds*>>;
    using SwiftType = std::vector<geo_bounds*>;
    using Wrapped = std::vector<geo_bounds*>;

    mutable std::vector<std::shared_ptr<managed<geo_bounds*>>> cached_objects_;
    mutable bool loaded_ = false;
    std::string parent_global_id_;
    std::string list_table_;    // e.g., "_Place_regions"
    std::string rtree_table_;   // e.g., "_Place_regions_rtree"

    // Unmanaged storage for objects not yet bound
    std::vector<geo_bounds> unmanaged_value;

    managed() = default;
    managed(const managed&) = default;
    managed(managed&&) = default;
    managed(const std::vector<geo_bounds>& val) : unmanaged_value(val) {}

    // OptionalProtocol requirements - lists always "have value" (even if empty)
    bool hasValue() const SWIFT_NAME(hasValue()) { return true; }
    std::vector<geo_bounds*> value() const SWIFT_NAME(value()) { return std::vector<geo_bounds*>{}; }

    operator std::vector<geo_bounds*>() const SWIFT_NAME(get()) {
        return std::vector<geo_bounds*>{};
    }

    managed& operator=(std::vector<geo_bounds*> v) SWIFT_NAME(set(_:)) {
        return *this;
    }

    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return value();
    }
    void setPointee(Wrapped wrapped) SWIFT_COMPUTED_PROPERTY {
        this->operator=(wrapped);
    }

    // Bind to parent - sets up list table name
    void bind_to_parent(model_base* parent, const char* prop_name) {
        managed_base::bind_to_parent(parent, prop_name);
        list_table_ = "_" + parent->table_name_ + "_" + std::string(prop_name);
        rtree_table_ = list_table_ + "_rtree";
        parent_global_id_ = parent->global_id_;
    }

    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        list_table_ = "_" + parent->table_name_ + "_" + p.name;
        rtree_table_ = list_table_ + "_rtree";
        parent_global_id_ = parent->global_id_;
    }

    const std::string& list_table() const { return list_table_; }
    const std::string& rtree_table() const { return rtree_table_; }

    // Size
    size_t size() const;
    bool empty() const { return size() == 0; }

    // Modifiers - declarations, implementations in lattice.hpp
    void push_back(const geo_bounds& bounds);
    void erase(size_t index);
    void clear();

    // Get a copy of all geo_bounds
    [[nodiscard]] std::vector<geo_bounds> detach() const {
        if (!is_bound()) return unmanaged_value;
        load_if_needed();
        std::vector<geo_bounds> result;
        result.reserve(cached_objects_.size());
        for (const auto& obj : cached_objects_) {
            result.push_back(obj->detach());
        }
        return result;
    }

    // Proxy class for subscript assignment
    class element_proxy {
    public:
        element_proxy(managed<std::vector<geo_bounds*>>* list, size_t index)
            : list_(list), index_(index) {}

        // Read access - convert to managed<geo_bounds*>&
        operator managed<geo_bounds*>&() const;

        // Pointer access to underlying geo_bounds
        managed<geo_bounds*>* operator->() const;

        // Assignment from geo_bounds value
        element_proxy& operator=(const geo_bounds& bounds);

    private:
        managed<std::vector<geo_bounds*>>* list_;
        size_t index_;
    };

    // Iterator support
    struct iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = managed<geo_bounds*>;
        using difference_type = std::ptrdiff_t;
        using pointer = managed<geo_bounds*>*;
        using reference = managed<geo_bounds*>&;

        iterator(typename std::vector<std::shared_ptr<managed<geo_bounds*>>>::iterator it) : it_(it) {}
        iterator(const iterator&) = default;
        iterator& operator=(const iterator&) = default;

        reference operator*() const { return **it_; }
        pointer operator->() const { return it_->get(); }
        iterator& operator++() { ++it_; return *this; }
        iterator operator++(int) { iterator tmp = *this; ++it_; return tmp; }

        friend bool operator==(const iterator& a, const iterator& b) {
            return a.it_ == b.it_;
        }
        friend bool operator!=(const iterator& a, const iterator& b) {
            return !(a == b);
        }

    private:
        typename std::vector<std::shared_ptr<managed<geo_bounds*>>>::iterator it_;
    };

    iterator begin();
    iterator end();

    // Subscript access with assignment support
    element_proxy operator[](size_t index);

    // Const subscript access (read-only, returns copy)
    geo_bounds operator[](size_t index) const;

private:
    friend class element_proxy;
    void load_if_needed() const;
};

template <typename T>
struct is_vector {
    static bool const value = false;
};

template<class T>
struct is_vector<std::vector<T> > {
  static bool const value = true;
};

// MARK: managed<std::optional<T>>
template<typename T>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<std::optional<T>> : managed_base {
    using SwiftType = std::optional<T>;
    using OptionalType = managed<std::optional<T>>;  // Self - already optional
    using Wrapped = T;
    
    std::optional<T> unmanaged_value;

    managed() = default;
    managed(const T& v) : unmanaged_value(v) {}
    managed(std::nullopt_t) : unmanaged_value(std::nullopt) {}
    managed(Wrapped&& wrapped) {
        throw std::runtime_error("not supported");
    }
    
    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return *this->detach();
    }
    void setPointee(Wrapped wrapped) SWIFT_COMPUTED_PROPERTY {
        this->operator=(std::optional(wrapped));
    }
    managed& operator=(std::optional<T> v) SWIFT_NAME(set(_:)) {
        if (is_bound()) {
            if (v) {
                if constexpr (is_vector<T>::value) {
                    
                } else {
                    column_value_t cv = detail::to_column_value(*v);
                    db->update(table_name, row_id, {{column_name, cv}});
                }
            } else {
                db->update(table_name, row_id, {{column_name, nullptr}});
            }
        } else {
            unmanaged_value = v;
        }
        return *this;
    }

    managed& operator=(std::nullopt_t) {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, nullptr}});
        } else {
            unmanaged_value = std::nullopt;
        }
        return *this;
    }

    void set_nil() {
        if (is_bound()) {
            db->update(table_name, row_id, {{column_name, nullptr}});
        } else {
            unmanaged_value = std::nullopt;
        }
    }
    
    [[nodiscard]] std::optional<T> detach() const {
        if (is_bound()) {
            auto rows = db->query(
                "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end()) {
                    if (std::holds_alternative<std::nullptr_t>(it->second)) {
                        return std::nullopt;
                    }
                    if constexpr (is_vector<T>::value) {
                        managed<T> m;
                        m.assign(this->db, lattice, table_name, column_name, row_id);
                        return m;
                    } else {
                        return detail::from_column_value<T>(it->second);
                    }
                }
            }
        }
        return unmanaged_value;
    }

    operator std::optional<T>() const SWIFT_NAME(get()) { return detach(); }

    bool has_value() const SWIFT_NAME(hasValue()) {
        if (is_bound()) {
            auto rows = db->query("SELECT " + column_name + " FROM " + table_name + " WHERE id = ?",
                                  {row_id});
            if (!rows.empty()) {
                auto it = rows[0].find(column_name);
                if (it != rows[0].end()) {
                    if (std::holds_alternative<std::nullptr_t>(it->second)) {
                        return false;
                    }
                    return true;
                }
            }
        }
        
        return false;
    }
    
    T value() const SWIFT_NAME(value()) { return *detach(); }
    
    explicit operator bool() const { return has_value(); }
};

// ============================================================================
// Type traits for detecting model types (has schema)
// ============================================================================

template<typename T>
struct is_model {
private:
    template<typename U>
    static auto test(int) -> decltype(managed<U>::schema(), std::true_type{});
    template<typename>
    static std::false_type test(...);
public:
    static constexpr bool value = decltype(test<T>(0))::value;
};

// Type trait for primitive types (stored directly, not as links)
template<typename T>
struct is_primitive : std::bool_constant<
    std::is_same_v<T, std::string> ||
    std::is_same_v<T, int64_t> ||
    std::is_same_v<T, int> ||
    std::is_same_v<T, double> ||
    std::is_same_v<T, float> ||
    std::is_same_v<T, bool> ||
    std::is_same_v<T, lattice::uuid_t> ||
    std::is_same_v<T, lattice::timestamp_t> ||
    std::is_same_v<T, lattice::geo_bounds>
> {};

// ============================================================================
// managed<std::vector<T>> - Primitive array (stored as JSON TEXT)
// For primitive types like std::vector<std::string>, std::vector<int>, etc.
// ============================================================================
// MARK: managed<std::vector<T>>

template<typename T>
struct CONFORMS_TO_MANAGED managed<std::vector<T>, std::enable_if_t<is_primitive<T>::value>> : managed_base {
    using OptionalType = managed<std::optional<std::vector<T>>>;
    using SwiftType = std::vector<T>;
    
    std::vector<T> unmanaged_value;

    managed() = default;
    managed(const std::vector<T>& val) : unmanaged_value(val) {}
    managed(std::vector<T>&& val) : unmanaged_value(std::move(val)) {}

    // Read from database (JSON decode)
    std::vector<T> detach() const {
        if (!is_bound()) return unmanaged_value;

        std::string sql = "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?";
        auto rows = db->query(sql, {row_id});
        if (rows.empty()) return unmanaged_value;

        auto it = rows[0].find(column_name);
        if (it == rows[0].end()) return unmanaged_value;

        if (auto* json_str = std::get_if<std::string>(&it->second)) {
            if (json_str->empty()) return {};
            try {
                auto j = nlohmann::json::parse(*json_str);
                return j.template get<std::vector<T>>();
            } catch (...) {
                return unmanaged_value;
            }
        }
        return unmanaged_value;
    }

    // Write to database (JSON encode)
    void set_value(const std::vector<T>& val) {
        unmanaged_value = val;
        if (!is_bound()) return;

        std::string json_str = nlohmann::json(val).dump();
        std::string sql = "UPDATE " + table_name + " SET " + column_name + " = ? WHERE id = ?";
        db->execute(sql, {json_str, row_id});
    }

    // Assignment operator
    managed& operator=(const std::vector<T>& val) SWIFT_NAME(set(_:)) {
        set_value(val);
        return *this;
    }

    // Conversion to vector
    operator std::vector<T>() const SWIFT_NAME(get()) { return detach(); }

    // Collection-like interface
    size_t size() const { return detach().size(); }
    bool empty() const { return detach().empty(); }
    T operator[](size_t idx) const { return detach()[idx]; }

    void push_back(const T& val) {
        auto vec = detach();
        vec.push_back(val);
        set_value(vec);
    }

    void clear() {
        set_value({});
    }
};

// ============================================================================
// managed<std::vector<uint8_t>> - Binary data (stored as BLOB)
// Special case for byte arrays - stored directly as BLOB, not JSON
// ============================================================================

template<>
struct CONFORMS_TO_MANAGED managed<std::vector<uint8_t>> : managed_base {
    using SwiftType = std::vector<uint8_t>;
    using OptionalType = managed<std::optional<std::vector<uint8_t>>>;
    
    std::vector<uint8_t> unmanaged_value;

    managed() = default;
    managed(const std::vector<uint8_t>& val) : unmanaged_value(val) {}
    managed(std::vector<uint8_t>&& val) : unmanaged_value(std::move(val)) {}

    // Read from database (BLOB)
    std::vector<uint8_t> detach() const {
        if (!is_bound()) return unmanaged_value;

        std::string sql = "SELECT " + column_name + " FROM " + table_name + " WHERE id = ?";
        auto rows = db->query(sql, {row_id});
        if (rows.empty()) return unmanaged_value;

        auto it = rows[0].find(column_name);
        if (it == rows[0].end()) return unmanaged_value;

        if (auto* blob = std::get_if<std::vector<uint8_t>>(&it->second)) {
            return *blob;
        }
        return unmanaged_value;
    }

    // Write to database (BLOB)
    // Note: ensure_vec0_for_blob is defined in lattice.hpp after lattice_db is complete
    void set_value(const std::vector<uint8_t>& val);

    // Implementation helper - defined in lattice.hpp
    static void ensure_vec0_for_blob(lattice_db* lattice, const std::string& table,
                                     const std::string& column, const std::vector<uint8_t>& val);

    // Assignment operator
    managed& operator=(const std::vector<uint8_t>& val) SWIFT_NAME(set(_:)) {
        set_value(val);
        return *this;
    }

    // Conversion to vector
    operator std::vector<uint8_t>() const SWIFT_NAME(get()) { return detach(); }

    // Collection-like interface
    size_t size() const { return detach().size(); }
    bool empty() const { return detach().empty(); }
    uint8_t operator[](size_t idx) const { return detach()[idx]; }

    void push_back(uint8_t val) {
        auto vec = detach();
        vec.push_back(val);
        set_value(vec);
    }

    void clear() {
        set_value({});
    }

    // Get raw data pointer (useful for Swift interop)
    const uint8_t* data() const {
        return unmanaged_value.data();
    }
};

// ============================================================================
// managed<T*> - To-one relationship (link to another model)
// Declared here, implemented in lattice.hpp after lattice_db is defined
// ============================================================================
// MARK: Link

template<typename T>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<T*, std::enable_if_t<is_model<T>::value>> : managed_base {
    using OptionalType = managed<T*>;
    using SwiftType = std::optional<managed<T>>;
    using Wrapped = managed<T>;
    
    mutable std::shared_ptr<managed<T>> cached_object_;
    mutable bool loaded_ = false;
    global_id_t parent_global_id_;
    std::string link_table_;
    std::string target_table_;  // For dynamic objects where schema().table_name is empty

    // Dummy unmanaged_value for macro compatibility (never used)
    std::nullptr_t unmanaged_value = nullptr;

    managed() = default;
    managed(const managed&) = default;
    managed(Wrapped&& wrapped) {
        throw std::runtime_error("not supported");
    };
    
    // Get effective target table name (instance override or static schema)
    std::string get_target_table() const {
        if (!target_table_.empty()) return target_table_;
        return managed<T>::schema().table_name;
    }

    // Override bind_to_parent to set up link table info
    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        parent_global_id_ = parent->global_id_;
        // Compute link table name: _ParentTable_ChildTable_propName
        link_table_ = "_" + parent->table_name_ + "_" +
                      p.target_table + "_" + p.name;
        target_table_ = p.target_table;
    }

    // Overload for LATTICE_SCHEMA macro (takes prop name string)
    void bind_to_parent(model_base* parent, const char* prop_name) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = prop_name;
        row_id = parent->id_;
        parent_global_id_ = parent->global_id_;
        // Compute link table from static schema
        link_table_ = "_" + parent->table_name_ + "_" + managed<T>::schema().table_name + "_" + prop_name;
        target_table_ = managed<T>::schema().table_name;
    }

    bool has_value() const SWIFT_NAME(hasValue());
    Wrapped value() const SWIFT_NAME(value()) {
        return *get_value();
    }
    explicit operator bool() const { return has_value(); }

    managed<T>* get_value() const;
    
    operator SwiftType() const SWIFT_NAME(get()) {
        if (get_value()) {
            return *get_value();
        } else {
            return SwiftType();
        }
    }
    managed& set_value(SwiftType s) SWIFT_NAME(set(_:)) {
        if (!s) {
            return this->operator=(nullptr);
        } else {
            return this->operator=(&*s);
        }
    }
    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return *get_value();
    }
    void setPointee(managed<T> wrapped) {
        this->operator=(&wrapped);
    }
    
    managed<T>* operator->() const { return get_value(); }
    managed<T>& operator*() const { return *get_value(); }

    // Assignment operators - implemented in lattice.hpp
    managed& operator=(const T& obj);      // owner.pet = Pet{"Fido", 30.0}
    managed& operator=(managed<T>* obj);   // owner.pet = &some_managed_pet
    managed& operator=(managed<T*> obj);   // owner.pet = some_managed_link
    managed& operator=(std::nullptr_t);    // owner.pet = nullptr

private:
    std::optional<global_id_t> get_linked_id() const;
    void set_link(const global_id_t& child_global_id);
    void clear_link();
    void load_if_needed() const;
};

// ============================================================================
// managed<std::vector<T*>> - To-many relationship (list of models)
// Declared here, implemented in lattice.hpp after lattice_db is defined
// ============================================================================
// MARK: Link List
template<typename T>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>> : managed_base {
    using OptionalType = managed<std::vector<T*>>;  // Self - lists aren't optional
    using SwiftType = std::vector<T*>;
    using Wrapped = std::vector<T*>;

    mutable std::vector<std::shared_ptr<managed<T>>> cached_objects_;
    mutable bool loaded_ = false;
    global_id_t parent_global_id_;
    std::string link_table_;
    std::string target_table_;
    
    // Dummy unmanaged_value for macro compatibility (never used)
    std::nullptr_t unmanaged_value = nullptr;

    managed() = default;
    managed(const managed&) = default;
    managed(Wrapped&& wrapped) {
        throw std::runtime_error("not supported");
    }
    managed(managed&&) = default;
    
    // OptionalProtocol requirements - lists always "have value" (even if empty)
    bool hasValue() const SWIFT_NAME(hasValue()) { return true; }
    std::vector<T*> value() const SWIFT_NAME(value()) { return std::vector<T*>{}; }

    operator std::vector<T*>() const SWIFT_NAME(get()) {
        return std::vector<T*>{};
    }

    managed& operator=(std::vector<T*> v) SWIFT_NAME(set(_:)) {
        return *this;
    }
    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return value();
    }
    void setPointee(Wrapped wrapped) SWIFT_COMPUTED_PROPERTY {
        this->operator=(wrapped);
    }
    // Override bind_to_parent to set up link table info
    void bind_to_parent(model_base* parent, const property_descriptor& p) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = p.name;
        row_id = parent->id_;
        parent_global_id_ = parent->global_id_;
        // Compute link table name: _ParentTable_ChildTable_propName
        link_table_ = "_" + parent->table_name_ + "_" +
                      p.target_table + "_" + p.name;
        target_table_ = p.target_table;
    }

    // Overload for LATTICE_SCHEMA macro (takes prop name string)
    void bind_to_parent(model_base* parent, const char* prop_name) {
        db = parent->db_;
        lattice = parent->lattice_;
        table_name = parent->table_name_;
        column_name = prop_name;
        row_id = parent->id_;
        parent_global_id_ = parent->global_id_;
        // Compute link table from static schema
        link_table_ = "_" + parent->table_name_ + "_" + managed<T>::schema().table_name + "_" + prop_name;
        target_table_ = managed<T>::schema().table_name;
    }

    size_t size() const;
    bool empty() const { return size() == 0; }

    // Add items - implemented in lattice.hpp
    void push_back(const T& obj);       // trip.destinations.push_back(Destination{...})
    void push_back(managed<T>* obj);
    void erase(managed<T>* obj);
    void clear();

    // Proxy class for subscript assignment
    class element_proxy {
    public:
        element_proxy(managed<std::vector<T*>>* list, size_t index)
            : list_(list), index_(index) {}

        // Read access - convert to managed<T>&
        operator managed<T>&() const;

        // Pointer access
        managed<T>* operator->() const;

        // Assignment from unmanaged object
        element_proxy& operator=(const T& obj);

        // Assignment from managed pointer
        element_proxy& operator=(managed<T>* obj);

    private:
        managed<std::vector<T*>>* list_;
        size_t index_;
    };

    // Iterator support (C++ only - Swift uses subscript access)
    struct iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = managed<T>;
        using difference_type = std::ptrdiff_t;
        using pointer = managed<T>*;
        using reference = managed<T>&;

        iterator(typename std::vector<std::shared_ptr<managed<T>>>::iterator it) : it_(it) {}
        iterator(const iterator&) = default;
        iterator& operator=(const iterator&) = default;

        reference operator*() const { return **it_; }
        pointer operator->() const { return it_->get(); }
        iterator& operator++() { ++it_; return *this; }
        iterator operator++(int) { iterator tmp = *this; ++it_; return tmp; }

        friend bool operator==(const iterator& a, const iterator& b) {
            return a.it_ == b.it_;
        }
        friend bool operator!=(const iterator& a, const iterator& b) {
            return !(a == b);
        }

    private:
        typename std::vector<std::shared_ptr<managed<T>>>::iterator it_;
    };

    iterator begin();
    iterator end();

    // Subscript access with assignment support
    element_proxy operator[](size_t index);

    // Const subscript access (read-only)
    const managed<T>& operator[](size_t index) const;

private:
    friend class element_proxy;
    std::vector<global_id_t> get_linked_ids() const;
    void add_link(const global_id_t& child_global_id);
    void replace_link_at(size_t index, const global_id_t& new_child_global_id);
    void load_if_needed() const;
};

} // namespace lattice

#endif // __cplusplus
