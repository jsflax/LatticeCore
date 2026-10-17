#pragma once
#include <lattice/ordinary_program.hpp>
#include <lattice/ordinary_geo_program.hpp>
#include <lattice/ordinary_sync.hpp>
#include <lattice/recovery_schema.hpp>
#include <stdexcept>
#include <string_view>

namespace lattice::detail {
// Declaration-derived logical storage. This is a plan, not evidence that an
// existing database has these columns/programs, nor an export/install grant.
// The actual capture/apply caller must validate every object in its retained
// main view, including derived module shadows, before reading or changing rows.
enum class ordinary_projection_role { model, link, virtual_link, union_value, geographic_list };
struct ordinary_projection_column {
    std::string name;
    column_type type=column_type::text;
    bool nullable=true;
    bool no_history=false;
    // Union's existing audit generator emits the quoted SQL identifier as
    // the JSON field name. Preserve that original instead of rewriting IDs.
    std::string audit_name;
    bool operator==(const ordinary_projection_column&)const=default;
};
struct ordinary_projection_table {
    std::string name,create_sql;
    ordinary_projection_role role=ordinary_projection_role::model;
    bool local_id=true;
    std::vector<ordinary_projection_column> columns;
    std::vector<std::string> audit_programs,other_programs,index_programs;
    // Each variant is one complete set emitted by a real normal setup path.
    // Validation must never accept arbitrary mixing between these sets.
    std::vector<std::vector<std::string>> alternate_audit_programs;
    std::vector<std::string> shadow_columns;
};
enum class ordinary_derived_kind { full_text, vector, geographic };
struct ordinary_projection_derived {
    std::string model,property,table,create_sql;
    ordinary_derived_kind kind;
    std::vector<std::string> programs;
};
struct ordinary_schema_projection {
    std::vector<std::string> models;
    std::map<std::string,ordinary_projection_table> tables;
    std::vector<ordinary_projection_derived> derived;
    size_t logical_bytes=0,columns=0;
};
struct ordinary_identifier_less {
    bool operator()(std::string_view a,std::string_view b)const noexcept {
        const auto fold=[](unsigned char c){return c>='A'&&c<='Z'?static_cast<unsigned char>(c+('a'-'A')):c;};
        for(size_t i=0;i<std::min(a.size(),b.size());++i) {
            const auto x=fold(static_cast<unsigned char>(a[i])),y=fold(static_cast<unsigned char>(b[i]));
            if(x!=y)return x<y;
        }
        return a.size()<b.size();
    }
};

// SwiftSchema is an unordered_map. Its traversal order is not part of the
// ordinary logical schema or wire column identity. Borrow only these immutable
// declaration pointers for this synchronous call; never change the retained
// owner catalog or its independent continuous-producer descriptor/programs.
template<class Charge>
inline std::vector<const property_descriptor*> ordinary_projection_property_order(
    const model_schema& model,bool swift,const Charge& charge) {
    if(model.properties.size()>ordinary_sync_limits::properties)
        throw std::runtime_error("ordinary projection property order bound exceeded");
    charge(model.properties.size()*sizeof(const property_descriptor*));
    std::vector<const property_descriptor*> properties;properties.reserve(model.properties.size());
    for(const auto& property:model.properties)properties.push_back(&property);
    if(swift)std::sort(properties.begin(),properties.end(),[](const auto* left,const auto* right){return left->name<right->name;});
    return properties;
}

inline ordinary_schema_projection describe_ordinary_projection(const recovery_owner_schema& catalog,
                                                               const std::vector<std::string>& requested) {
    using caps=ordinary_sync_limits;
    const auto require=[](bool ok,const char* message){if(!ok)throw std::runtime_error(message);};
    require(catalog.valid()&&requested.size()<=caps::models,"ordinary projection catalog unavailable");
    ordinary_schema_projection result;
    const auto charge=[&](size_t n){
        constexpr size_t limit=16*1024*1024;
        require(n<=limit-result.logical_bytes,"ordinary projection program bound exceeded");result.logical_bytes+=n;
    };
    const auto identifier=[&](const std::string& s){
        require(!s.empty()&&s.size()<=256,"ordinary projection identifier bound exceeded");
        for(size_t i=0;i<s.size();++i){const auto c=static_cast<unsigned char>(s[i]);
            require((c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='_'||(i&&c>='0'&&c<='9'),"ordinary projection identifier is not generated SQL");}
    };
    if(requested.empty())for(const auto& [name,unused]:catalog.models){identifier(name);charge(sizeof(std::string)+name.size());result.models.push_back(name);}
    else for(const auto& name:requested){identifier(name);charge(sizeof(std::string)+name.size());result.models.push_back(name);}
    require(!result.models.empty()&&result.models.size()<=caps::models,"ordinary projection root scope bound exceeded");
    std::sort(result.models.begin(),result.models.end());
    require(std::adjacent_find(result.models.begin(),result.models.end())==result.models.end(),"duplicate ordinary projection model");
    const auto selected=[&](const std::string& name){return std::binary_search(result.models.begin(),result.models.end(),name);};
    const auto program=[&](std::vector<std::string>& programs,const std::string& sql){charge(sizeof(std::string)+sql.size());programs.push_back(sql);};
    const auto column=[&](ordinary_projection_table& table,const std::string& name,column_type type,bool nullable,bool no_history=false,const std::string& alias=std::string{}){
        identifier(name);require(table.columns.size()<caps::columns_per_table&&result.columns<caps::projection_columns,"ordinary projection column bound exceeded");
        require(std::none_of(table.columns.begin(),table.columns.end(),[&](const auto& c){return c.name==name;}),"ordinary projection column collision");
        charge(sizeof(ordinary_projection_column)+name.size()+(alias.empty()?name.size():alias.size()));++result.columns;
        table.columns.push_back({name,type,nullable,no_history,alias.empty()?name:alias});
    };
    const auto insert=[&](ordinary_projection_table table)->ordinary_projection_table&{
        identifier(table.name);require(!result.tables.count(table.name),"ordinary projection table collision");
        require(result.tables.size()<caps::projection_tables,"ordinary projection table bound exceeded");
        charge(sizeof(ordinary_projection_table)+2*table.name.size()+table.create_sql.size());
        auto position=result.tables.emplace(table.name,std::move(table));return position.first->second;
    };
    // Known inbound/outbound declared references must remain within this cut.
    // Polymorphic rhs_type values need a separate actual-row scope check.
    // A union table can be shared by several declared parents. Its rows form
    // one projected table, so selecting any parent requires every such parent
    // even though ordinary TEXT references are not SQL foreign keys.
    std::map<std::string,bool,ordinary_identifier_less> union_selection;
    for(const auto& [name,model]:catalog.models)for(const auto& p:model.properties) {
        if(p.kind==property_kind::union_type||p.is_union) {
            const auto& target=p.union_desc.union_table_name;identifier(target);
            const auto found=union_selection.find(target);
            if(found==union_selection.end()) {
                charge(sizeof(std::pair<const std::string,bool>)+target.size());
                union_selection.emplace(target,selected(name));
            } else require(found->second==selected(name),"ordinary projection requires every shared union parent");
        }
        if(!p.target_table.empty()&&p.kind!=property_kind::union_type)
            require(selected(name)==selected(p.target_table),"ordinary projection requires a closed relationship scope");
        for(const auto& c:p.union_desc.cases)for(const auto& v:c.values)if(v.is_link)
            require(selected(name)==selected(v.link_target),"ordinary projection requires a closed union relationship scope");
    }
    for(const auto& name:result.models) {
        const auto* declaration=catalog.find(name);require(declaration&&name.front()!='_',"ordinary projection root declaration missing");
        model_schema stored;stored.table_name=name;
        for(const auto* property:ordinary_projection_property_order(*declaration,catalog.swift_models.count(name),charge)) {
            const auto& p=*property;
            identifier(p.name);
            if(catalog.swift_models.count(name)) {
                if(p.kind==property_kind::union_type){auto q=p;q.type=column_type::text;q.nullable=true;stored.properties.push_back(std::move(q));}
                else if(p.kind==property_kind::primitive||(p.kind==property_kind::list&&p.is_geo_bounds))stored.properties.push_back(p);
            } else stored.properties.push_back(p);
        }
        const auto generated=ordinary_model_program(stored);
        ordinary_projection_table table;table.name=name;table.create_sql=generated.create_table;
        column(table,"globalId",column_type::text,false);
        std::set<std::string> no_history;
        for(const auto& p:stored.properties) {
            if(p.kind==property_kind::list)continue;
            if(p.is_geo_bounds)for(const auto* suffix:{"_minLat","_maxLat","_minLon","_maxLon"})column(table,p.name+suffix,column_type::real,true);
            else {
                column(table,p.name,p.type,p.nullable,p.kind==property_kind::primitive&&p.no_history);
                if(p.kind==property_kind::primitive&&p.no_history)no_history.insert(p.name);
            }
        }
        ordinary_model_audit_programs(name,generated.columns,no_history,{},[&](const std::string& sql){program(table.audit_programs,sql);});
        // Ordinary recreate_model_table_triggers currently emits only the
        // primitive properties, unlike initial creation's non-list columns.
        // Preserve both actual complete shapes when inspecting migrated files.
        std::vector<std::pair<std::string,column_type>> recreated_columns;
        for(const auto& p:stored.properties)if(p.kind==property_kind::primitive) {
            if(p.is_geo_bounds)for(const auto* suffix:{"_minLat","_maxLat","_minLon","_maxLon"})recreated_columns.emplace_back(p.name+suffix,column_type::real);
            else recreated_columns.emplace_back(p.name,p.type);
        }
        std::vector<std::string> recreated;
        ordinary_model_audit_programs(name,recreated_columns,no_history,{},[&](const auto& sql){program(recreated,sql);});
        if(recreated!=table.audit_programs){charge(sizeof(recreated));table.alternate_audit_programs.push_back(std::move(recreated));}
        insert(std::move(table));
    }
    for(const auto& name:result.models) {
        const auto& model=*catalog.find(name);auto& parent=result.tables.at(name);
        for(const auto* property:ordinary_projection_property_order(model,catalog.swift_models.count(name),charge)) {
            const auto& p=*property;
            if(p.kind==property_kind::union_type||p.is_union) {
                const auto& target=p.union_desc.union_table_name;identifier(target);
                for(const auto& c:p.union_desc.cases){identifier(c.case_name);for(const auto& v:c.values)if(!v.param_name.empty())identifier(v.param_name);}
                const auto generated=ordinary_union_program(target,p.union_desc);
                if(auto found=result.tables.find(target);found!=result.tables.end())
                    require(found->second.role==ordinary_projection_role::union_value&&found->second.create_sql==generated.create_table,"ordinary projection shared union differs");
                else {
                    ordinary_projection_table table;table.name=target;table.create_sql=generated.create_table;table.role=ordinary_projection_role::union_value;
                    column(table,"globalId",column_type::text,false);
                    for(const auto& [field,type]:generated.columns)column(table,field=="\"case\""?"case":field,type,field!="\"case\"",false,field);
                    ordinary_model_audit_programs(target,generated.columns,{},{},[&](const auto& sql){program(table.audit_programs,sql);});
                    program(table.index_programs,ordinary_union_case_index_program(target));insert(std::move(table));
                }
                program(parent.other_programs,ordinary_union_cascade_program(name,p.name,target));
            } else if(p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list||
                     ((p.kind==property_kind::link||p.kind==property_kind::list)&&!p.is_geo_bounds&&!p.target_table.empty())) {
                const bool polymorphic=p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list;
                const auto target="_"+name+"_"+(polymorphic?std::string{}:p.target_table+"_")+p.name;
                ordinary_projection_table table;table.name=target;table.local_id=false;
                table.role=polymorphic?ordinary_projection_role::virtual_link:ordinary_projection_role::link;
                table.create_sql=polymorphic?ordinary_virtual_link_table_program(target):ordinary_link_table_program(target);
                column(table,"globalId",column_type::text,false);column(table,"lhs",column_type::text,false);column(table,"rhs",column_type::text,false);
                if(polymorphic){column(table,"rhs_type",column_type::text,false);ordinary_virtual_link_audit_programs(target,[&](const auto& sql){program(table.audit_programs,sql);});}
                else ordinary_link_audit_programs(target,{},[&](const auto& sql){program(table.audit_programs,sql);});
                insert(std::move(table));
            } else if(p.is_geo_bounds&&p.kind==property_kind::list) {
                const auto generated=ordinary_geo_list_program("_"+name+"_"+p.name);
                ordinary_projection_table table;table.name=generated.table;table.create_sql=generated.create_table;table.role=ordinary_projection_role::geographic_list;
                column(table,"globalId",column_type::text,false);column(table,"parent_id",column_type::text,false);
                for(const auto* field:{"minLat","maxLat","minLon","maxLon"})column(table,field,column_type::real,false);
                for(const auto& trigger:generated.audit_triggers)program(table.audit_programs,trigger.sql);
                for(const auto& trigger:generated.rtree_triggers)program(table.other_programs,trigger.sql);
                program(table.index_programs,generated.parent_index);insert(std::move(table));
                result.derived.push_back({generated.table,p.name,generated.rtree_table,generated.rtree_create_table,ordinary_derived_kind::geographic,{}});
            }
            if(p.is_geo_bounds&&p.kind!=property_kind::list) {
                const auto generated=ordinary_rtree_program(name,p.name);
                for(const auto& trigger:generated.triggers)program(parent.other_programs,trigger.sql);
                result.derived.push_back({name,p.name,generated.table,generated.create_table,ordinary_derived_kind::geographic,{}});
            }
            if(p.is_full_text) {
                const auto generated=fts5_porter_program(name,p.name);
                for(const auto& trigger:generated.triggers)program(parent.other_programs,trigger.sql);
                result.derived.push_back({name,p.name,generated.table,generated.create_table,ordinary_derived_kind::full_text,{}});
            }
            if(p.is_vector) {
                const auto generated=vec0_program(name,p.name);
                // Dimensions and optional index presence require actual module
                // metadata/value checks in the retained view. No guessed DDL.
                result.derived.push_back({name,p.name,generated.table,{},ordinary_derived_kind::vector,{}});
                for(const auto& sql:generated.sql)program(result.derived.back().programs,sql);
            }
        }
        if(catalog.swift_models.count(name)) {
            for(const auto* property:ordinary_projection_property_order(model,true,charge)) {
                const auto& p=*property;
                if(p.is_indexed&&(p.kind==property_kind::primitive||p.kind==property_kind::union_type||
                    (p.kind==property_kind::list&&p.is_geo_bounds)))
                    program(parent.index_programs,ordinary_property_index_program(name,p.name));
            }
            const auto constraints=catalog.swift_constraints.find(name);require(constraints!=catalog.swift_constraints.end(),"ordinary projection Swift constraints missing");
            for(size_t i=0;i<constraints->second.size();++i) {
                const auto& constraint=constraints->second[i];if(constraint.columns.empty())continue;
                std::vector<std::string> resolved;
                for(const auto& field:constraint.columns) {
                    identifier(field);const auto found=std::find_if(model.properties.begin(),model.properties.end(),[&](const auto& p){return p.name==field;});
                    if(found!=model.properties.end()&&found->kind==property_kind::link&&!found->target_table.empty()) {
                        const auto shadow=field+"__link_gid",link="_"+name+"_"+found->target_table+"_"+field;identifier(shadow);
                        if(std::find(parent.shadow_columns.begin(),parent.shadow_columns.end(),shadow)==parent.shadow_columns.end()) {
                            charge(sizeof(std::string)+shadow.size());parent.shadow_columns.push_back(shadow);
                            auto& table=result.tables.at(link);
                            ordinary_link_shadow_programs(name,link,shadow,[&](const auto& sql){program(table.other_programs,sql);});
                        }
                        resolved.push_back(shadow);
                    } else {
                        require(std::any_of(parent.columns.begin(),parent.columns.end(),[&](const auto& c){return c.name==field;}),"ordinary projection unique column unavailable");resolved.push_back(field);
                    }
                }
                program(parent.index_programs,ordinary_unique_index_program(name,i,resolved));
            }
        } else {
            size_t ordinal=0;
            for(const auto& p:model.properties)if(p.is_unique){const auto i=ordinal++;if(p.kind==property_kind::primitive&&!p.is_geo_bounds)program(parent.index_programs,ordinary_unique_index_program(name,i,{p.name}));}
        }
    }
    for(const auto& d:result.derived){charge(sizeof(d)+d.model.size()+d.property.size()+d.table.size()+d.create_sql.size());identifier(d.table);}
    return result;
}
} // namespace lattice::detail
