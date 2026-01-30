#ifndef unmanaged_object_hpp
#define unmanaged_object_hpp

#ifdef __cplusplus

#include <bridging.hpp>
#include <string>
#include <unordered_map>
#include <LatticeCpp.hpp>

namespace lattice {

struct dynamic_object;
struct link_list;

struct SWIFT_CONFORMS_TO_PROTOCOL(LatticeSwiftModule.OptionalProtocol) swift_dynamic_object {
    using Wrapped = swift_dynamic_object;
    
    std::string table_name;
    std::unordered_map<std::string, property_descriptor> properties;
    std::unordered_map<std::string, column_value_t> values;
    std::unordered_map<std::string, std::shared_ptr<dynamic_object>> link_values;
    std::unordered_map<std::string, std::shared_ptr<link_list>> list_values;
    std::unordered_map<std::string, std::vector<geo_bounds>> geo_bounds_lists;
    
    swift_dynamic_object() = default;

    swift_dynamic_object(const std::string& table,
                         const std::unordered_map<std::string, property_descriptor>& props);

    swift_dynamic_object(const swift_dynamic_object&) = default;
    swift_dynamic_object& operator=(const swift_dynamic_object&) = default;
    swift_dynamic_object(swift_dynamic_object&&) = default;
    
    bool has_value() const SWIFT_NAME(hasValue()) {
        return table_name != "";
    }
    
    Wrapped value() const SWIFT_NAME(value()) {
        return *this;
    }
    
    // Get a property descriptor by name (O(1) lookup)
    const property_descriptor* get_property(const std::string& name) const {
        auto it = properties.find(name);
        return it != properties.end() ? &it->second : nullptr;
    }

    // Get a property value
    column_value_t get(const std::string& name) const {
        auto it = values.find(name);
        return it != values.end() ? it->second : column_value_t{nullptr};
    }

    // Set a property value
    void set(const std::string& name, const column_value_t& value) {
        values[name] = value;
    }

    // Convenience setters for Swift
    void set_string(const std::string& name, const std::string& value) {
        values[name] = value;
    }
    void set_int(const std::string& name, int64_t value) {
        values[name] = value;
    }
    void set_double(const std::string& name, double value) {
        values[name] = value;
    }
    void set_bool(const std::string& name, bool value) {
        values[name] = value ? 1LL : 0LL;
    }
    void set_blob(const std::string& name, const std::vector<uint8_t>& value) {
        values[name] = value;
    }
    void set_nil(const std::string& name) {
        if (link_values.count(name)) {
            link_values.erase(name);
        } else {
            values[name] = nullptr;
        }
    }
    
    // Convenience getters for Swift
    std::string get_string(const std::string& name) const {
        auto v = get(name);
        if (auto* s = std::get_if<std::string>(&v)) return *s;
        return "";
    }
    int64_t get_int(const std::string& name) const {
        auto v = get(name);
        if (auto* i = std::get_if<int64_t>(&v)) return *i;
        return 0;
    }
    double get_double(const std::string& name) const {
        auto v = get(name);
        if (auto* d = std::get_if<double>(&v)) return *d;
        return 0.0;
    }
    bool get_bool(const std::string& name) const {
        return get_int(name) != 0;
    }
    std::vector<uint8_t> get_blob(const std::string& name) const {
        auto v = get(name);
        if (auto* b = std::get_if<std::vector<uint8_t>>(&v)) return *b;
        return {};
    }

    // geo_bounds support - stored as 4 columns: name_minLat, name_maxLat, name_minLon, name_maxLon
    geo_bounds get_geo_bounds(const std::string& name) const {
        return geo_bounds(
            get_double(name + "_minLat"),
            get_double(name + "_maxLat"),
            get_double(name + "_minLon"),
            get_double(name + "_maxLon")
        );
    }

    void set_geo_bounds(const std::string& name, const geo_bounds& value) {
        set_double(name + "_minLat", value.min_lat);
        set_double(name + "_maxLat", value.max_lat);
        set_double(name + "_minLon", value.min_lon);
        set_double(name + "_maxLon", value.max_lon);
    }

    void set_geo_bounds(const std::string& name, double minLat, double maxLat, double minLon, double maxLon) {
        set_double(name + "_minLat", minLat);
        set_double(name + "_maxLat", maxLat);
        set_double(name + "_minLon", minLon);
        set_double(name + "_maxLon", maxLon);
    }

    // Check if geo_bounds has a value (any of the 4 columns is non-null)
    bool has_geo_bounds(const std::string& name) const {
        return has_value(name + "_minLat");
    }

    // geo_bounds list support (stored in separate table when managed, in-memory when unmanaged)
    std::vector<geo_bounds> get_geo_bounds_list(const std::string& name) const {
        auto it = geo_bounds_lists.find(name);
        return it != geo_bounds_lists.end() ? it->second : std::vector<geo_bounds>{};
    }

    size_t geo_bounds_list_size(const std::string& name) const {
        auto it = geo_bounds_lists.find(name);
        return it != geo_bounds_lists.end() ? it->second.size() : 0;
    }

    geo_bounds get_geo_bounds_at(const std::string& name, size_t index) const {
        auto it = geo_bounds_lists.find(name);
        if (it != geo_bounds_lists.end() && index < it->second.size()) {
            return it->second[index];
        }
        return geo_bounds{};
    }

    void add_geo_bounds_to_list(const std::string& name, const geo_bounds& value) {
        geo_bounds_lists[name].push_back(value);
    }

    void set_geo_bounds_list(const std::string& name, const std::vector<geo_bounds>& list) {
        geo_bounds_lists[name] = list;
    }

    void clear_geo_bounds_list(const std::string& name) {
        geo_bounds_lists[name].clear();
    }

    void remove_geo_bounds_at(const std::string& name, size_t index) {
        auto it = geo_bounds_lists.find(name);
        if (it != geo_bounds_lists.end() && index < it->second.size()) {
            it->second.erase(it->second.begin() + index);
        }
    }

    std::shared_ptr<link_list> get_link_list(const std::string& name) const;
    
    bool has_value(const std::string& name) const {
        auto v = get(name);
        auto count = link_values.count(name);
        return !std::get_if<std::nullptr_t>(&v) || count > 0;
    }
    
    // Get the schema for this instance
    model_schema instance_schema() const {
        std::vector<property_descriptor> props;
        props.reserve(properties.size());
        for (const auto& [name, desc] : properties) {
            props.push_back(desc);
        }
        return model_schema{table_name, props};
    }
    
    Wrapped getPointee() const SWIFT_COMPUTED_PROPERTY {
        return *this;
    }
    void setPointee(Wrapped wrapped) SWIFT_COMPUTED_PROPERTY {
        this->table_name = wrapped.table_name;
        this->properties = wrapped.properties;
        this->values = wrapped.values;
//        this->operator=(wrapped);
    }
};

}

#endif // __cplusplus

#endif /* unmanaged_object_hpp */
