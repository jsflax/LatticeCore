#include <stdio.h>
#include <unmanaged_object.hpp>
#include <list.hpp>

namespace lattice {

swift_dynamic_object::swift_dynamic_object(const std::string& table,
                     const std::unordered_map<std::string, property_descriptor>& props)
    : table_name(table), properties(props) {
        for (auto& [name, desc] : props) {
            if (!desc.nullable && desc.kind == property_kind::primitive) {
                if (desc.is_geo_bounds) {
                    // A single geo_bounds is stored as four expanded REAL
                    // columns — seed those, never the raw property name.
                    // Seeding `values[name]` here put a key with no matching
                    // table column into the value map; any path that trusted
                    // the map's keys as column names then built an INSERT
                    // against a column that doesn't exist.
                    values[name + "_minLat"] = 0.0;
                    values[name + "_maxLat"] = 0.0;
                    values[name + "_minLon"] = 0.0;
                    values[name + "_maxLon"] = 0.0;
                    continue;
                }
                switch (desc.type) {
                    case column_type::integer:
                        values[name] = int64_t(0);
                        break;
                    case column_type::real:
                        values[name] = 0.0;
                        break;
                    case column_type::text:
                        values[name] = std::string("");
                        break;
                    case column_type::blob:
                        values[name] = std::vector<uint8_t>{};
                        break;
                }
            } else if (desc.kind == property_kind::list
                    || desc.kind == property_kind::virtual_list
                    || desc.kind == property_kind::virtual_link) {
                list_values[name] = std::make_shared<link_list>();
            }
        }
    }

std::shared_ptr<link_list> swift_dynamic_object::get_link_list(const std::string &name) const {
    return list_values.at(name);
}

}
