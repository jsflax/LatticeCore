#ifndef managed_object_hpp
#define managed_object_hpp

#include <array>
#include <bridging.hpp>
#include <string>
#include <unmanaged_object.hpp>
#include <LatticeCpp.hpp>


// ============================================================================
// managed<swift_dynamic_object> - Managed wrapper for dynamic objects
// ============================================================================

namespace lattice {

struct swift_lattice_ref;

#define get_managed_field_fn(type) \
template <> \
const managed<type> get_managed_field(const std::string& name) const { \
managed<type> m; \
m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_); \
return m; \
} \
template <> \
const managed<std::optional<type>> get_managed_field(const std::string& name) const { \
managed<std::optional<type>> m; \
m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_); \
return m; \
} \
template <> \
const managed<std::vector<type>> get_managed_field(const std::string& name) const { \
managed<std::vector<type>> m; \
m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_); \
return m; \
}


template<>
struct CONFORMS_TO_OPTIONAL_MANAGED managed<swift_dynamic_object> : model_base {
    using SwiftType = managed<swift_dynamic_object>;
    using OptionalType = managed<swift_dynamic_object*>;
    using Wrapped = managed<swift_dynamic_object>;
    
    swift_dynamic_object source;
    std::unordered_map<std::string, property_descriptor> properties_;
    std::unordered_map<std::string, managed_base> fields;  // Managed property wrappers for read/write
    
    managed() = default;
    managed(managed&&) = default;
    managed(const managed&) = default;
    managed& operator=(const managed&) = default;
    managed& operator=(managed&&) = default;
    
    explicit managed(const swift_dynamic_object& src) : source(src) {
        table_name_ = src.table_name;
        properties_ = src.properties;
        unmanaged_values_ = src.values;
        source = src;
    }
    
    bool has_value() const SWIFT_NAME(hasValue()) { return is_managed(); }
    
    bool has_value(const std::string& name) const;
    
    Wrapped value() const SWIFT_NAME(value()) {
        //        return source;
        return *this;
    }
    
    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return *this;
        //        return source;
    }
    void setPointee(Wrapped src) SWIFT_COMPUTED_PROPERTY {
        LOG_DEBUG("managed_object", "SETTING POINTEE");
        source = src.source;
        source.setPointee(src.source);
        table_name_ = src.table_name_;
        properties_ = src.properties_;
        unmanaged_values_ = src.source.values;
        
    }
    
    managed<swift_dynamic_object*> as_link() const {
        auto link = managed<swift_dynamic_object*>();
        managed<swift_dynamic_object>* _this = std::remove_const_t<managed<swift_dynamic_object>*>(this);
        link = _this;
        return link;
    }
    
    // Bind to database after insert
    void bind_to_db() {
        // Properties are accessed via get_value/set_value from model_base
    }
    
    // Collect values for insert (only primitives, not links/lists)
    std::vector<std::pair<std::string, column_value_t>> collect_values() const {
        std::vector<std::pair<std::string, column_value_t>> result;
        for (const auto& [name, desc] : properties_) {
            if (desc.kind == property_kind::primitive) {
                if (desc.is_geo_bounds) {
                    // geo_bounds expands to 4 columns
                    std::array<std::string, 4> suffixes = {"_minLat", "_maxLat", "_minLon", "_maxLon"};
                    for (const auto& suffix : suffixes) {
                        std::string col_name = name + suffix;
                        auto it = unmanaged_values_.find(col_name);
                        if (it != unmanaged_values_.end()) {
                            result.emplace_back(col_name, it->second);
                        }
                    }
                } else {
                    auto it = unmanaged_values_.find(name);
                    if (it != unmanaged_values_.end()) {
                        result.emplace_back(name, it->second);
                    }
                }
            }
        }
        return result;
    }
    
    swift_dynamic_object detach() const;
    
    // Required by lattice_db::add/remove - returns schema from instance
    // For dynamic objects, schema comes from the source object
    static const model_schema& schema() {
        // This is a placeholder - actual schema is per-instance
        // lattice_db needs to be updated to handle dynamic schemas
        static model_schema empty_schema{"", {}};
        return empty_schema;
    }
    
    // Get the schema for this instance
    model_schema instance_schema() const {
        if (this->is_valid()) {
            std::vector<property_descriptor> props;
            props.reserve(properties_.size());
            for (const auto& [name, desc] : properties_) {
                props.push_back(desc);
            }
            return model_schema{table_name_, props};
        } else {
            return source.instance_schema();
        }
    }
    
    SwiftType get() const {
        return {};
    }
    managed& set_swift_value(SwiftType v) SWIFT_NAME(set(_:)) {
        return *this;
    }
    // Get a property descriptor by name
    const property_descriptor* get_property(const std::string& name) const {
        auto it = properties_.find(name);
        return it != properties_.end() ? &it->second : nullptr;
    }
    
    template <typename T>
    const managed<T> get_managed_field(const std::string& name) const;
    
    // String
    get_managed_field_fn(std::string)
    get_managed_field_fn(int64_t)
    get_managed_field_fn(float)
    get_managed_field_fn(double)
    get_managed_field_fn(bool)
    get_managed_field_fn(timestamp_t)
    get_managed_field_fn(uuid_t)
    
    // Data (BLOB)
    template <>
    const managed<std::vector<uint8_t>> get_managed_field(const std::string& name) const {
        managed<std::vector<uint8_t>> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        // Check if this is a vector column for similarity search
        auto it = properties_.find(name);
        if (it != properties_.end() && it->second.is_vector) {
            m.is_vector_column = true;
        }
        return m;
    }
    template <>
    const managed<std::optional<std::vector<uint8_t>>> get_managed_field(const std::string& name) const {
        managed<std::optional<std::vector<uint8_t>>> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        // Check if this is a vector column for similarity search
        auto it = properties_.find(name);
        if (it != properties_.end() && it->second.is_vector) {
            m.is_vector_column = true;
        }
        return m;
    }
    
    // Link (to-one relationship)
    template <>
    const managed<swift_dynamic_object*> get_managed_field(const std::string& name) const {
        managed<swift_dynamic_object*> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        const property_descriptor& property = properties_.at(name);
        auto base = static_cast<model_base>(*this);
        m.bind_to_parent(&base, property);
        return m;
    }
    
    template <>
    const managed<swift_dynamic_object> get_managed_field(const std::string& name) const {
        managed<swift_dynamic_object*> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        auto base = static_cast<model_base>(*this);
        const property_descriptor& property = properties_.at(name);
        m.bind_to_parent(&base, property);
        return *m.get_value();
    }
    
    // Int list
    template <>
    const managed<std::vector<int>> get_managed_field(const std::string& name) const {
        managed<std::vector<int>> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        return m;
    }
    
    // Link list (to-many relationship)
    template <>
    const managed<std::vector<swift_dynamic_object*>> get_managed_field(const std::string& name) const {
        managed<std::vector<swift_dynamic_object*>> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        auto base = static_cast<model_base>(*this);
        const property_descriptor& property = properties_.at(name);
        m.bind_to_parent(&base, property);
        return m;
    }

    // geo_bounds (stored as 4 columns + R*Tree)
    template <>
    const managed<geo_bounds> get_managed_field(const std::string& name) const {
        managed<geo_bounds> m;
        m.db = this->db_;
        m.lattice = this->lattice_;
        m.table_name = this->table_name_;
        m.column_name = name;
        m.row_id = this->id_;
        m.rtree_table_ = "_" + this->table_name_ + "_" + name + "_rtree";
        return m;
    }

    template <>
    const managed<std::optional<geo_bounds>> get_managed_field(const std::string& name) const {
        managed<std::optional<geo_bounds>> m;
        m.db = this->db_;
        m.lattice = this->lattice_;
        m.table_name = this->table_name_;
        m.column_name = name;
        m.row_id = this->id_;
        m.rtree_table_ = "_" + this->table_name_ + "_" + name + "_rtree";
        return m;
    }

    // geo_bounds list (stored in separate table with R*Tree)
    template <>
    const managed<std::vector<geo_bounds>> get_managed_field(const std::string& name) const {
        managed<std::vector<geo_bounds>> m;
        m.db = this->db_;
        m.lattice = this->lattice_;
        m.table_name = this->table_name_;
        m.column_name = name;
        m.row_id = this->id_;
        m.list_table_ = "_" + this->table_name_ + "_" + name;
        m.rtree_table_ = m.list_table_ + "_rtree";
        m.parent_global_id_ = this->global_id_;
        return m;
    }

    // Convenience accessors that delegate to model_base
    void set_string(const std::string& name, const std::string& value) {
        set_value(name, column_value_t{value});
    }
    void set_int(const std::string& name, int64_t value) {
        set_value(name, column_value_t{value});
    }
    void set_double(const std::string& name, double value) {
        set_value(name, column_value_t{value});
    }
    
    void set_nil(const std::string& name);
    
    std::string get_string(const std::string& name) const {
        managed<std::string> managed;
        managed.assign(this->db_,
                       this->lattice_,
                       this->table_name_,
                       name, this->id_);
        return managed.detach();
    }
    
    int64_t get_int(const std::string& name) const {
        managed<int64_t> managed;
        managed.assign(this->db_,
                       this->lattice_,
                       this->table_name_,
                       name, this->id_);
        return managed.detach();
    }
    
    double get_double(const std::string& name) const {
        managed<double> managed;
        managed.assign(this->db_,
                       this->lattice_,
                       this->table_name_,
                       name, this->id_);
        return managed.detach();
    }
    
    bool get_bool(const std::string& name) const {
        managed<bool> managed;
        managed.assign(this->db_,
                       this->lattice_,
                       this->table_name_,
                       name, this->id_);
        return managed.detach();
    }

    // geo_bounds accessors
    geo_bounds get_geo_bounds(const std::string& name) const {
        auto m = get_managed_field<geo_bounds>(name);
        return m.detach();
    }

    void set_geo_bounds(const std::string& name, const geo_bounds& value) {
        auto m = get_managed_field<geo_bounds>(name);
        const_cast<managed<geo_bounds>&>(m) = value;
    }

    void set_geo_bounds(const std::string& name, double minLat, double maxLat, double minLon, double maxLon) {
        set_geo_bounds(name, geo_bounds(minLat, maxLat, minLon, maxLon));
    }

    bool has_geo_bounds(const std::string& name) const {
        // Check if any of the geo columns has a value
        auto m = get_managed_field<std::optional<geo_bounds>>(name);
        return m.has_value();
    }

    // geo_bounds list accessors
    std::vector<geo_bounds> get_geo_bounds_list(const std::string& name) const {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        return m.detach();
    }

    size_t geo_bounds_list_size(const std::string& name) const {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        return m.size();
    }

    geo_bounds get_geo_bounds_at(const std::string& name, size_t index) const {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        return m[index];
    }

    void add_geo_bounds(const std::string& name, const geo_bounds& value) {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        const_cast<managed<std::vector<geo_bounds>>&>(m).push_back(value);
    }

    void clear_geo_bounds_list(const std::string& name) {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        const_cast<managed<std::vector<geo_bounds>>&>(m).clear();
    }

    void remove_geo_bounds_at(const std::string& name, size_t index) {
        auto m = get_managed_field<std::vector<geo_bounds>>(name);
        const_cast<managed<std::vector<geo_bounds>>&>(m).erase(index);
    }

    template <typename T>
    void assign_managed(std::string name,
                        managed<T>* managed) const {
        managed->assign(this->db_,
                        this->lattice_,
                        this->table_name_,
                        name, this->id_);
    }
    
    swift_lattice_ref* lattice_ref() const;
    
    friend struct dynamic_object;
    friend struct link_list;
};
}

#endif /* Header_h */
