#include "lattice/schema.hpp"

namespace lattice {

schema_registry& schema_registry::instance() {
    static schema_registry registry;
    return registry;
}

void schema_registry::register_model(const std::type_info& type, model_schema schema) {
    type_to_table_[type.name()] = schema.table_name;
    schemas_by_name_[schema.table_name] = std::move(schema);
}

const model_schema* schema_registry::get_schema(const std::type_info& type) const {
    auto it = type_to_table_.find(type.name());
    if (it == type_to_table_.end()) {
        return nullptr;
    }
    return get_schema(it->second);
}

const model_schema* schema_registry::get_schema(const std::string& table_name) const {
    auto it = schemas_by_name_.find(table_name);
    if (it == schemas_by_name_.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<const model_schema*> schema_registry::all_schemas() const {
    std::vector<const model_schema*> result;
    result.reserve(schemas_by_name_.size());
    for (const auto& [_, schema] : schemas_by_name_) {
        result.push_back(&schema);
    }
    return result;
}

} // namespace lattice
