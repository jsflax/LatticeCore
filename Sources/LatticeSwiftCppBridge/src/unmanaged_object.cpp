#include <stdio.h>
#include <unmanaged_object.hpp>
#include <list.hpp>

namespace lattice {

swift_dynamic_object::swift_dynamic_object(const std::string& table,
                     const std::unordered_map<std::string, property_descriptor>& props)
    : table_name(table), properties(props) {
        for (auto& [name, desc] : props) {
            if (!desc.nullable && desc.kind == property_kind::primitive) {
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
            } else if (desc.kind == property_kind::list) {
                list_values[name] = std::make_shared<link_list>();
            }
        }
    }

std::shared_ptr<link_list> swift_dynamic_object::get_link_list(const std::string &name) const {
    return list_values.at(name);
}

}
