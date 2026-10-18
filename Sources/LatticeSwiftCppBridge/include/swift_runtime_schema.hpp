#pragma once
#include <lattice/schema.hpp>
#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <lattice/detail/core_model_registry_storage.hpp>

namespace lattice::detail {
// Runtime DATA is distinct from caller-owned SchemaVector/SwiftSchema input.
// Every retained character, bucket, node and nested vector uses the actual
// prospective allocator. No allocator retains an Open, account or scope.
using swift_runtime_text=core_model_registry_text;
template<class T>using swift_runtime_vector=std::vector<T,core_model_registry_allocator<T>>;
inline swift_runtime_text swift_runtime_text_from_(std::string_view text){return {text.data(),text.size()};}
// An owning normal-object boundary, NEVER a pending-startup fallback. This
// compiled entry refuses an active debit scope before its first allocation.
inline std::string swift_runtime_ordinary_text_(std::string_view text){return core_model_registry_legacy_text_(text);}
void require_swift_runtime_ordinary_copy_();

struct swift_runtime_union_value {
    swift_runtime_text param_name,link_target;
    column_type type=column_type::text;bool is_link=false;
    explicit swift_runtime_union_value(const union_case_value& source)
        :param_name(swift_runtime_text_from_(source.param_name)),link_target(swift_runtime_text_from_(source.link_target)),
         type(source.type),is_link(source.is_link){}
};
struct swift_runtime_union_case {
    swift_runtime_text case_name;swift_runtime_vector<swift_runtime_union_value> values;
    explicit swift_runtime_union_case(const union_case& source):case_name(swift_runtime_text_from_(source.case_name)){
        values.reserve(source.values.size());for(const auto& value:source.values)values.emplace_back(value);
    }
};
struct swift_runtime_union_descriptor {
    swift_runtime_text union_table_name;swift_runtime_vector<swift_runtime_union_case> cases;
    swift_runtime_union_descriptor()=default;
    explicit swift_runtime_union_descriptor(const union_descriptor& source):union_table_name(swift_runtime_text_from_(source.union_table_name)){
        cases.reserve(source.cases.size());for(const auto& item:source.cases)cases.emplace_back(item);
    }
    swift_runtime_union_descriptor& operator=(const union_descriptor& source){
        swift_runtime_union_descriptor replacement(source);swap(replacement);return *this;
    }
    void swap(swift_runtime_union_descriptor& other)noexcept{union_table_name.swap(other.union_table_name);cases.swap(other.cases);}
    union_descriptor ordinary_copy()const {
        require_swift_runtime_ordinary_copy_();union_descriptor result;
        result.union_table_name=swift_runtime_ordinary_text_(union_table_name);result.cases.reserve(cases.size());
        for(const auto& item:cases){union_case target;target.case_name=swift_runtime_ordinary_text_(item.case_name);target.values.reserve(item.values.size());
            for(const auto& value:item.values)target.values.push_back({swift_runtime_ordinary_text_(value.param_name),value.type,value.is_link,swift_runtime_ordinary_text_(value.link_target)});
            result.cases.push_back(std::move(target));}return result;
    }
};
struct swift_runtime_property_descriptor {
    swift_runtime_text name,target_table,link_table,column_name;
    column_type type=column_type::text;property_kind kind=property_kind::primitive;
    bool nullable=false,is_vector=false,is_geo_bounds=false,is_full_text=false,is_indexed=false,is_unique=false,is_union=false,no_history=false;
    swift_runtime_union_descriptor union_desc;
    swift_runtime_property_descriptor()=default;
    explicit swift_runtime_property_descriptor(const property_descriptor& source)
        :name(swift_runtime_text_from_(source.name)),target_table(swift_runtime_text_from_(source.target_table)),
         link_table(swift_runtime_text_from_(source.link_table)),column_name(swift_runtime_text_from_(source.column_name)),
         type(source.type),kind(source.kind),nullable(source.nullable),is_vector(source.is_vector),is_geo_bounds(source.is_geo_bounds),
         is_full_text(source.is_full_text),is_indexed(source.is_indexed),is_unique(source.is_unique),is_union(source.is_union),
         no_history(source.no_history),union_desc(source.union_desc){}
    property_descriptor ordinary_copy()const {
        require_swift_runtime_ordinary_copy_();property_descriptor result{};
        result.name=swift_runtime_ordinary_text_(name);result.target_table=swift_runtime_ordinary_text_(target_table);
        result.link_table=swift_runtime_ordinary_text_(link_table);result.column_name=swift_runtime_ordinary_text_(column_name);
        result.type=type;result.kind=kind;result.nullable=nullable;result.is_vector=is_vector;result.is_geo_bounds=is_geo_bounds;
        result.is_full_text=is_full_text;result.is_indexed=is_indexed;result.is_unique=is_unique;result.is_union=is_union;
        result.no_history=no_history;result.union_desc=union_desc.ordinary_copy();return result;
    }
};
struct swift_runtime_constraint {
    swift_runtime_vector<swift_runtime_text> columns;bool allows_upsert=false;
    template<class Input>explicit swift_runtime_constraint(const Input& source):allows_upsert(source.allows_upsert){
        columns.reserve(source.columns.size());for(const auto& text:source.columns)columns.push_back(swift_runtime_text_from_(text));
    }
};
template<class Value>class swift_runtime_map:public core_model_registry_map<Value> {
    using base=core_model_registry_map<Value>;
public:
    using base::base;
    Value& operator[](std::string_view name){return base::operator[](swift_runtime_text_from_(name));}
    Value& at(std::string_view name){auto found=this->find(name);if(found==this->end())throw std::out_of_range("runtime schema key absent");return found->second;}
    const Value& at(std::string_view name)const{auto found=this->find(name);if(found==this->end())throw std::out_of_range("runtime schema key absent");return found->second;}
};
class swift_runtime_schema:public swift_runtime_map<swift_runtime_property_descriptor> {
public:
    swift_runtime_schema()=default;
    template<class Input>explicit swift_runtime_schema(const Input& source){assign(source);}
    template<class Input>swift_runtime_schema& operator=(const Input& source){assign(source);return *this;}
    template<class Input>void assign(const Input& source){swift_runtime_schema replacement;replacement.reserve(source.size());
        for(const auto& [name,property]:source)replacement.emplace(swift_runtime_text_from_(name),swift_runtime_property_descriptor(property));
        this->swap(replacement);}
    std::unordered_map<std::string,property_descriptor> ordinary_copy()const {
        require_swift_runtime_ordinary_copy_();std::unordered_map<std::string,property_descriptor> result;result.reserve(this->size());
        for(const auto& [name,property]:*this)result.emplace(swift_runtime_ordinary_text_(name),property.ordinary_copy());return result;
    }
};
class swift_runtime_constraints:public swift_runtime_vector<swift_runtime_constraint> {
public:
    swift_runtime_constraints()=default;
    template<class Input>explicit swift_runtime_constraints(const Input& source){assign(source);}
    template<class Input>swift_runtime_constraints& operator=(const Input& source){assign(source);return *this;}
    template<class Input>void assign(const Input& source){swift_runtime_constraints replacement;replacement.reserve(source.size());
        for(const auto& constraint:source)replacement.emplace_back(constraint);this->swap(replacement);}
};
struct swift_runtime_registry_stage {
    swift_runtime_map<swift_runtime_schema> schemas;
    swift_runtime_map<swift_runtime_constraints> constraints;
    swift_runtime_map<swift_runtime_union_descriptor> unions;
};
}
namespace lattice {
// Versioned borrowed aliases. Input SwiftSchema/ConstraintVector are unchanged.
using RuntimeSwiftSchema=detail::swift_runtime_schema;
using RuntimeConstraintVector=detail::swift_runtime_constraints;
// Fixed owned DATA for Clang/Swift consumers. Lookup alone touches the real
// owner. A copied summary retains no descriptor, name buffer, owner or account.
// This bound applies only to the new summary API; runtime catalogs/getters keep
// their complete values. Oversize names produce an entirely invalid summary.
class swift_property_info {
    friend class swift_lattice;
    std::array<std::uint8_t,256> name_{};
    std::uint16_t name_size_=0;
    std::int32_t kind_=-1;
    std::size_t union_case_count_=0;
    bool valid_=false,nullable_=false,is_vector_=false,is_geo_bounds_=false,
         is_full_text_=false,is_indexed_=false,is_unique_=false,is_union_=false,no_history_=false;
    explicit swift_property_info(const detail::swift_runtime_property_descriptor& value)noexcept {
        if(value.name.empty()||value.name.size()>name_.size())return;
        // Prove the complete extent before copying any exposed byte or flag.
        for(std::size_t index=0;index<value.name.size();++index)
            name_[index]=static_cast<std::uint8_t>(value.name[index]);
        name_size_=static_cast<std::uint16_t>(value.name.size());
        kind_=static_cast<std::int32_t>(value.kind);union_case_count_=value.union_desc.cases.size();
        nullable_=value.nullable;is_vector_=value.is_vector;is_geo_bounds_=value.is_geo_bounds;
        is_full_text_=value.is_full_text;is_indexed_=value.is_indexed;is_unique_=value.is_unique;
        is_union_=value.is_union;no_history_=value.no_history;valid_=true;
    }
public:
    swift_property_info()noexcept=default;
    swift_property_info(const swift_property_info&)noexcept=default;
    swift_property_info& operator=(const swift_property_info&)noexcept=default;
    bool valid()const noexcept{return valid_;}
    std::size_t name_size()const noexcept{return valid_?name_size_:0;}
    // Byte values 0..255; -1 means invalid summary or an out-of-range index.
    std::int32_t name_byte(std::size_t index)const noexcept {
        return valid_&&index<name_size_?static_cast<std::int32_t>(name_[index]):-1;
    }
    std::int32_t kind_value()const noexcept{return valid_?kind_:-1;}
    bool nullable()const noexcept{return valid_&&nullable_;}
    bool is_vector()const noexcept{return valid_&&is_vector_;}
    bool is_geo_bounds()const noexcept{return valid_&&is_geo_bounds_;}
    bool is_full_text()const noexcept{return valid_&&is_full_text_;}
    bool is_indexed()const noexcept{return valid_&&is_indexed_;}
    bool is_unique()const noexcept{return valid_&&is_unique_;}
    bool no_history()const noexcept{return valid_&&no_history_;}
    bool is_union()const noexcept{return valid_&&is_union_;}
    std::size_t union_case_count()const noexcept{return valid_?union_case_count_:0;}
};
}
