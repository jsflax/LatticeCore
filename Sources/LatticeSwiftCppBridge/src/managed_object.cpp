#include <managed_object.hpp>

namespace lattice {

void managed<swift_dynamic_object>::set_nil(const std::string& name) {
    if (!properties_.count(name)) {
        return;
    }
    const auto& property_desc = this->properties_.at(name);
    
    if (property_desc.kind == property_kind::link) {
        managed<swift_dynamic_object *> m;
        m.assign(this->db_,
                 this->lattice_,
                 this->table_name_,
                 name, this->id_);
        auto base = static_cast<model_base>(*this);
        m.bind_to_parent(&base, property_desc);
        m = nullptr;
    } else {
        managed<std::optional<std::string>> m;
        m.assign(this->db_,
                 this->lattice_,
                 this->table_name_,
                 name, this->id_);
        m.set_nil();
    }
}

swift_dynamic_object managed<swift_dynamic_object>::detach() const {
    swift_dynamic_object s(table_name_, properties_);

    // Copy id and globalId from model_base
    s.set_int("id", this->id_);
    s.set_string("globalId", this->global_id_);

    // Iterate over all properties and read their values from the database
    for (const auto& [name, desc] : properties_) {
        if (name == "id" || name == "globalId") continue;  // Already handled

        if (desc.kind == property_kind::primitive) {
            if (desc.is_geo_bounds) {
                // Read geo_bounds (4 columns)
                if (has_geo_bounds(name)) {
                    geo_bounds bounds = get_geo_bounds(name);
                    s.set_geo_bounds(name, bounds);
                }
            } else {
                // Read primitive value based on type
                switch (desc.type) {
                    case column_type::integer:
                        if (desc.nullable) {
                            managed<std::optional<int64_t>> m;
                            m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
                            if (m.has_value()) {
                                s.set_int(name, m.detach().value());
                            }
                        } else {
                            s.set_int(name, get_int(name));
                        }
                        break;
                    case column_type::real:
                        if (desc.nullable) {
                            managed<std::optional<double>> m;
                            m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
                            if (m.has_value()) {
                                s.set_double(name, m.detach().value());
                            }
                        } else {
                            s.set_double(name, get_double(name));
                        }
                        break;
                    case column_type::text:
                        if (desc.nullable) {
                            managed<std::optional<std::string>> m;
                            m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
                            if (m.has_value()) {
                                s.set_string(name, m.detach().value());
                            }
                        } else {
                            s.set_string(name, get_string(name));
                        }
                        break;
                    case column_type::blob: {
                        managed<std::optional<std::vector<uint8_t>>> m;
                        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
                        if (m.has_value()) {
                            s.set_blob(name, m.detach().value());
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        } else if (desc.kind == property_kind::list && desc.is_geo_bounds) {
            // Read geo_bounds list
            std::vector<geo_bounds> bounds_list = get_geo_bounds_list(name);
            s.set_geo_bounds_list(name, bounds_list);
        }
        // Skip link and link_list properties for now - they're handled separately
    }

    return s;
}

}
