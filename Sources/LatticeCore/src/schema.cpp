#include "lattice/schema.hpp"
#include "lattice/recovery_schema.hpp"

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

std::optional<std::vector<const model_schema*>> schema_registry::bounded_schemas(size_t maximum)const {
    if(schemas_by_name_.size()>maximum)return std::nullopt;
    return all_schemas();
}

} // namespace lattice

namespace lattice::detail {
bool recovery_owner_schema::charge(size_t size) {
    if(!valid_||size>max_bytes-bytes_){valid_=false;return false;}
    bytes_+=size;return true;
}
bool recovery_owner_schema::field(const std::string& value) {
    if(value.size()>65536){valid_=false;return false;}
    return charge(value.size()+16);
}
bool recovery_owner_schema::property(const property_descriptor& p) {
    if(properties_==max_properties){valid_=false;return false;}
    ++properties_;
    if(!charge(128)||!field(p.name)||!field(p.target_table)||!field(p.link_table)||
       !field(p.column_name)||!field(p.union_desc.union_table_name)||
       !admit_count(p.union_desc.cases.size(),64,32))return false;
    for(const auto& c:p.union_desc.cases) {
        if(!field(c.case_name)||!admit_count(c.values.size(),32,32))return false;
        for(const auto& v:c.values)if(!field(v.param_name)||!field(v.link_target))return false;
    }
    return true;
}
bool recovery_owner_schema::admit_count(size_t count,size_t cap,size_t overhead) {
    if(count>cap||overhead>max_bytes||(overhead&&count>max_bytes/overhead)){valid_=false;return false;}
    return charge(count*overhead);
}
bool recovery_owner_schema::admit_field(const std::string& value){return field(value);}
bool recovery_owner_schema::admit_property(const property_descriptor& value){return property(value);}
bool recovery_owner_schema::admit_model(const model_schema& value) {
    if(models.size()>=max_models||models.count(value.table_name)||value.properties.size()>256){valid_=false;return false;}
    if(!field(value.table_name))return false;
    for(const auto& p:value.properties)if(!property(p))return false;
    models.emplace(value.table_name,value);return true;
}
recovery_owner_schema recovery_owner_schema::capture_native() {
    recovery_owner_schema result;
    const auto registered=schema_registry::instance().bounded_schemas(max_models);
    if(!registered){result.refuse();return result;}
    for(const auto* model:*registered)if(!result.admit_model(*model))break;
    return result;
}
const model_schema* recovery_owner_schema::find(const std::string& name)const noexcept {
    const auto found=models.find(name);return found==models.end()?nullptr:&found->second;
}
}
