#pragma once
#include "ordinary_initialization_storage.hpp"
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
    ordinary_initialization_text name;
    column_type type=column_type::text;
    bool nullable=true;
    bool no_history=false;
    // Union's existing audit generator emits the quoted SQL identifier as
    // the JSON field name. Preserve that original instead of rewriting IDs.
    ordinary_initialization_text audit_name;
    ordinary_projection_column()=default;
    ordinary_projection_column(std::string_view column,column_type storage=column_type::text,
        bool allows_null=true,bool excludes_history=false,std::string_view audit={})
        :name(column.data(),column.size()),type(storage),nullable(allows_null),no_history(excludes_history),
         audit_name(audit.empty()?"":audit.data(),audit.size()){}
    bool operator==(const ordinary_projection_column&)const=default;
};
struct ordinary_projection_table {
    ordinary_initialization_text name,create_sql;
    ordinary_projection_role role=ordinary_projection_role::model;
    bool local_id=true;
    ordinary_initialization_vector<ordinary_projection_column> columns;
    ordinary_initialization_vector<ordinary_initialization_text> audit_programs,other_programs,index_programs;
    // Each variant is one complete set emitted by a real normal setup path.
    // Validation must never accept arbitrary mixing between these sets.
    ordinary_initialization_vector<ordinary_initialization_vector<ordinary_initialization_text>> alternate_audit_programs;
    ordinary_initialization_vector<ordinary_initialization_text> shadow_columns;
};
enum class ordinary_derived_kind { full_text, vector, geographic };
struct ordinary_projection_derived {
    ordinary_initialization_text model,property,table,create_sql;
    ordinary_derived_kind kind;
    ordinary_initialization_vector<ordinary_initialization_text> programs;
};
struct ordinary_schema_projection {
    ordinary_initialization_vector<ordinary_initialization_text> models;
    ordinary_initialization_text_map<ordinary_projection_table> tables;
    ordinary_initialization_vector<ordinary_projection_derived> derived;
    size_t logical_bytes=0,columns=0;
};

struct ordinary_projection_model_storage {
    ordinary_initialization_text create_table;
    ordinary_initialization_vector<std::pair<ordinary_initialization_text,column_type>> columns;
};
struct ordinary_projection_trigger_storage {
    ordinary_initialization_text name,owner_table,sql;
};
struct ordinary_projection_fts_storage {
    ordinary_initialization_text table,create_table;
    std::array<ordinary_projection_trigger_storage,3> triggers;
    ordinary_initialization_text populate;
};
struct ordinary_projection_vec_storage {
    ordinary_initialization_text table,owner_table;
    std::array<ordinary_initialization_text,4> sql,names;
};
struct ordinary_projection_rtree_storage {
    ordinary_initialization_text table,create_table;
    std::array<ordinary_projection_trigger_storage,3> triggers;
};
struct ordinary_projection_geo_list_storage {
    ordinary_initialization_text table,create_table,parent_index_name,parent_index,rtree_table,rtree_create_table;
    std::array<ordinary_projection_trigger_storage,3> rtree_triggers;
    std::array<ordinary_projection_trigger_storage,2> audit_triggers;
};
struct ordinary_projection_property_storage {
    ordinary_initialization_text name;
    column_type type;
    property_kind kind;
    bool nullable,is_geo_bounds,no_history;
};
struct ordinary_projection_model_view {
    ordinary_initialization_text table_name;
    ordinary_initialization_vector<ordinary_projection_property_storage> properties;
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
inline ordinary_initialization_vector<const property_descriptor*> ordinary_projection_property_order(
    const model_schema& model,bool swift,const Charge& charge) {
    if(model.properties.size()>ordinary_sync_limits::properties)
        throw std::runtime_error("ordinary projection property order bound exceeded");
    charge(model.properties.size()*sizeof(const property_descriptor*));
    ordinary_initialization_vector<const property_descriptor*> properties;properties.reserve(model.properties.size());
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
    ordinary_initialization_map<std::string_view,const model_schema*> declarations;
    ordinary_initialization_set<std::string_view> swift_names;
    ordinary_initialization_map<std::string_view,const std::vector<recovery_constraint>*> constraints_by_name;
    for(const auto& [name,model]:catalog.models)declarations.emplace(std::string_view(name),&model);
    for(const auto& name:catalog.swift_models)swift_names.emplace(name);
    for(const auto& [name,constraints]:catalog.swift_constraints)constraints_by_name.emplace(std::string_view(name),&constraints);
    const auto declaration_for=[&](std::string_view name)->const model_schema* {
        const auto found=declarations.find(name);return found==declarations.end()?nullptr:found->second;
    };
    const auto charge=[&](size_t n){
        constexpr size_t limit=16*1024*1024;
        require(n<=limit-result.logical_bytes,"ordinary projection program bound exceeded");result.logical_bytes+=n;
    };
    const auto identifier=[&](std::string_view s){
        require(!s.empty()&&s.size()<=256,"ordinary projection identifier bound exceeded");
        for(size_t i=0;i<s.size();++i){const auto c=static_cast<unsigned char>(s[i]);
            require((c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='_'||(i&&c>='0'&&c<='9'),"ordinary projection identifier is not generated SQL");}
    };
    if(requested.empty())for(const auto& [name,unused]:catalog.models){identifier(name);charge(sizeof(std::string)+name.size());result.models.emplace_back(name.data(),name.size());}
    else for(const auto& name:requested){identifier(name);charge(sizeof(std::string)+name.size());result.models.emplace_back(name.data(),name.size());}
    require(!result.models.empty()&&result.models.size()<=caps::models,"ordinary projection root scope bound exceeded");
    std::sort(result.models.begin(),result.models.end());
    require(std::adjacent_find(result.models.begin(),result.models.end())==result.models.end(),"duplicate ordinary projection model");
    const auto selected=[&](std::string_view name){return std::binary_search(result.models.begin(),result.models.end(),name,ordinary_initialization_text_less{});};
    const auto program=[&](auto& programs,std::string_view sql){charge(sizeof(std::string)+sql.size());programs.emplace_back(sql.data(),sql.size());};
    const auto column=[&](ordinary_projection_table& table,std::string_view name,column_type type,bool nullable,bool no_history=false,std::string_view alias={}){
        identifier(name);require(table.columns.size()<caps::columns_per_table&&result.columns<caps::projection_columns,"ordinary projection column bound exceeded");
        require(std::none_of(table.columns.begin(),table.columns.end(),[&](const auto& c){return c.name==name;}),"ordinary projection column collision");
        charge(sizeof(ordinary_projection_column)+name.size()+(alias.empty()?name.size():alias.size()));++result.columns;
        table.columns.push_back({ordinary_initialization_text(name.data(),name.size()),type,nullable,no_history,ordinary_initialization_text((alias.empty()?name:alias).data(),(alias.empty()?name:alias).size())});
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
    ordinary_initialization_map<ordinary_initialization_text,bool,ordinary_identifier_less> union_selection;
    for(const auto& [name,model]:catalog.models)for(const auto& p:model.properties) {
        if(p.kind==property_kind::union_type||p.is_union) {
            const ordinary_initialization_text target(p.union_desc.union_table_name.data(),p.union_desc.union_table_name.size());identifier(target);
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
        const auto* declaration=declaration_for(std::string_view(name));require(declaration&&name.front()!='_',"ordinary projection root declaration missing");
        ordinary_projection_model_view stored;stored.table_name=name;
        for(const auto* property:ordinary_projection_property_order(*declaration,swift_names.count(std::string_view(name)),charge)) {
            const auto& p=*property;
            identifier(p.name);
            const auto append=[&](column_type type,bool nullable){stored.properties.push_back({
                ordinary_initialization_text(p.name.data(),p.name.size()),type,p.kind,nullable,p.is_geo_bounds,p.no_history});};
            if(swift_names.count(std::string_view(name))) {
                if(p.kind==property_kind::union_type)append(column_type::text,true);
                else if(p.kind==property_kind::primitive||(p.kind==property_kind::list&&p.is_geo_bounds))append(p.type,p.nullable);
            } else append(p.type,p.nullable);
        }
        const auto generated=ordinary_model_program_storage<ordinary_initialization_text,ordinary_projection_model_storage>(stored);
        ordinary_projection_table table;table.name=name;table.create_sql=generated.create_table;
        column(table,"globalId",column_type::text,false);
        ordinary_initialization_text_set no_history;
        for(const auto& p:stored.properties) {
            if(p.kind==property_kind::list)continue;
            if(p.is_geo_bounds)for(const auto* suffix:{"_minLat","_maxLat","_minLon","_maxLon"})column(table,p.name+suffix,column_type::real,true);
            else {
                column(table,p.name,p.type,p.nullable,p.kind==property_kind::primitive&&p.no_history);
                if(p.kind==property_kind::primitive&&p.no_history)no_history.insert(p.name);
            }
        }
        ordinary_model_audit_programs_storage(name,generated.columns,no_history,ordinary_initialization_text{},[&](const auto& sql){program(table.audit_programs,sql);});
        // Ordinary recreate_model_table_triggers currently emits only the
        // primitive properties, unlike initial creation's non-list columns.
        // Preserve both actual complete shapes when inspecting migrated files.
        ordinary_initialization_vector<std::pair<ordinary_initialization_text,column_type>> recreated_columns;
        for(const auto& p:stored.properties)if(p.kind==property_kind::primitive) {
            if(p.is_geo_bounds)for(const auto* suffix:{"_minLat","_maxLat","_minLon","_maxLon"})recreated_columns.emplace_back(p.name+suffix,column_type::real);
            else recreated_columns.emplace_back(p.name,p.type);
        }
        ordinary_initialization_vector<ordinary_initialization_text> recreated;
        ordinary_model_audit_programs_storage(name,recreated_columns,no_history,ordinary_initialization_text{},[&](const auto& sql){program(recreated,sql);});
        if(recreated!=table.audit_programs){charge(sizeof(recreated));table.alternate_audit_programs.push_back(std::move(recreated));}
        insert(std::move(table));
    }
    for(const auto& name:result.models) {
        const auto& model=*declaration_for(std::string_view(name));auto& parent=result.tables.at(name);
        for(const auto* property:ordinary_projection_property_order(model,swift_names.count(std::string_view(name)),charge)) {
            const auto& p=*property;
            if(p.kind==property_kind::union_type||p.is_union) {
                const ordinary_initialization_text target(p.union_desc.union_table_name.data(),p.union_desc.union_table_name.size());identifier(target);
                for(const auto& c:p.union_desc.cases){identifier(c.case_name);for(const auto& v:c.values)if(!v.param_name.empty())identifier(v.param_name);}
                const auto generated=ordinary_union_program_storage<ordinary_initialization_text,ordinary_projection_model_storage>(target,p.union_desc);
                if(auto found=result.tables.find(target);found!=result.tables.end())
                    require(found->second.role==ordinary_projection_role::union_value&&found->second.create_sql==generated.create_table,"ordinary projection shared union differs");
                else {
                    ordinary_projection_table table;table.name=target;table.create_sql=generated.create_table;table.role=ordinary_projection_role::union_value;
                    column(table,"globalId",column_type::text,false);
                    for(const auto& [field,type]:generated.columns)column(table,field=="\"case\""?"case":field,type,field!="\"case\"",false,field);
                    ordinary_model_audit_programs_storage(target,generated.columns,ordinary_initialization_text_set{},ordinary_initialization_text{},[&](const auto& sql){program(table.audit_programs,sql);});
                    program(table.index_programs,ordinary_union_case_index_program_storage(target));insert(std::move(table));
                }
                program(parent.other_programs,ordinary_union_cascade_program_storage(name,ordinary_initialization_text(p.name.data(),p.name.size()),target));
            } else if(p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list||
                     ((p.kind==property_kind::link||p.kind==property_kind::list)&&!p.is_geo_bounds&&!p.target_table.empty())) {
                const bool polymorphic=p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list;
                const auto target="_"+name+"_"+(polymorphic?ordinary_initialization_text{}:ordinary_initialization_text(p.target_table.data(),p.target_table.size())+"_")+ordinary_initialization_text(p.name.data(),p.name.size());
                ordinary_projection_table table;table.name=target;table.local_id=false;
                table.role=polymorphic?ordinary_projection_role::virtual_link:ordinary_projection_role::link;
                table.create_sql=polymorphic?ordinary_virtual_link_table_program_storage(target):ordinary_link_table_program_storage(target);
                column(table,"globalId",column_type::text,false);column(table,"lhs",column_type::text,false);column(table,"rhs",column_type::text,false);
                if(polymorphic){column(table,"rhs_type",column_type::text,false);ordinary_virtual_link_audit_programs_storage(target,[&](const auto& sql){program(table.audit_programs,sql);});}
                else ordinary_link_audit_programs_storage(target,ordinary_initialization_text{},[&](const auto& sql){program(table.audit_programs,sql);});
                insert(std::move(table));
            } else if(p.is_geo_bounds&&p.kind==property_kind::list) {
                const auto generated=ordinary_geo_list_program_storage<ordinary_initialization_text,ordinary_projection_geo_list_storage>("_"+name+"_"+ordinary_initialization_text(p.name.data(),p.name.size()));
                ordinary_projection_table table;table.name=generated.table;table.create_sql=generated.create_table;table.role=ordinary_projection_role::geographic_list;
                column(table,"globalId",column_type::text,false);column(table,"parent_id",column_type::text,false);
                for(const auto* field:{"minLat","maxLat","minLon","maxLon"})column(table,field,column_type::real,false);
                for(const auto& trigger:generated.audit_triggers)program(table.audit_programs,trigger.sql);
                for(const auto& trigger:generated.rtree_triggers)program(table.other_programs,trigger.sql);
                program(table.index_programs,generated.parent_index);insert(std::move(table));
                result.derived.push_back({generated.table,ordinary_initialization_text(p.name.data(),p.name.size()),generated.rtree_table,generated.rtree_create_table,ordinary_derived_kind::geographic,{}});
            }
            if(p.is_geo_bounds&&p.kind!=property_kind::list) {
                const auto generated=ordinary_rtree_program_storage<ordinary_initialization_text,ordinary_projection_rtree_storage>(name,ordinary_initialization_text(p.name.data(),p.name.size()));
                for(const auto& trigger:generated.triggers)program(parent.other_programs,trigger.sql);
                result.derived.push_back({name,ordinary_initialization_text(p.name.data(),p.name.size()),generated.table,generated.create_table,ordinary_derived_kind::geographic,{}});
            }
            if(p.is_full_text) {
                const auto generated=fts5_porter_program_storage<ordinary_initialization_text,ordinary_projection_fts_storage>(name,ordinary_initialization_text(p.name.data(),p.name.size()));
                for(const auto& trigger:generated.triggers)program(parent.other_programs,trigger.sql);
                result.derived.push_back({name,ordinary_initialization_text(p.name.data(),p.name.size()),generated.table,generated.create_table,ordinary_derived_kind::full_text,{}});
            }
            if(p.is_vector) {
                const auto generated=vec0_program_storage<ordinary_initialization_text,ordinary_projection_vec_storage>(name,ordinary_initialization_text(p.name.data(),p.name.size()));
                // Dimensions and optional index presence require actual module
                // metadata/value checks in the retained view. No guessed DDL.
                result.derived.push_back({name,ordinary_initialization_text(p.name.data(),p.name.size()),generated.table,{},ordinary_derived_kind::vector,{}});
                for(const auto& sql:generated.sql)program(result.derived.back().programs,sql);
            }
        }
        if(swift_names.count(std::string_view(name))) {
            for(const auto* property:ordinary_projection_property_order(model,true,charge)) {
                const auto& p=*property;
                if(p.is_indexed&&(p.kind==property_kind::primitive||p.kind==property_kind::union_type||
                    (p.kind==property_kind::list&&p.is_geo_bounds)))
                    program(parent.index_programs,ordinary_property_index_program_storage(name,ordinary_initialization_text(p.name.data(),p.name.size())));
            }
            const auto constraints=constraints_by_name.find(std::string_view(name));require(constraints!=constraints_by_name.end(),"ordinary projection Swift constraints missing");
            for(size_t i=0;i<constraints->second->size();++i) {
                const auto& constraint=(*constraints->second)[i];if(constraint.columns.empty())continue;
                ordinary_initialization_vector<ordinary_initialization_text> resolved;
                for(const auto& field:constraint.columns) {
                    identifier(field);const auto found=std::find_if(model.properties.begin(),model.properties.end(),[&](const auto& p){return p.name==field;});
                    if(found!=model.properties.end()&&found->kind==property_kind::link&&!found->target_table.empty()) {
                        const auto shadow=ordinary_initialization_text(field.data(),field.size())+"__link_gid",link="_"+name+"_"+ordinary_initialization_text(found->target_table.data(),found->target_table.size())+"_"+ordinary_initialization_text(field.data(),field.size());identifier(shadow);
                        if(std::find(parent.shadow_columns.begin(),parent.shadow_columns.end(),shadow)==parent.shadow_columns.end()) {
                            charge(sizeof(std::string)+shadow.size());parent.shadow_columns.push_back(shadow);
                            auto& table=result.tables.at(link);
                            ordinary_link_shadow_programs_storage(name,link,shadow,[&](const auto& sql){program(table.other_programs,sql);});
                        }
                        resolved.push_back(shadow);
                    } else {
                        require(std::any_of(parent.columns.begin(),parent.columns.end(),[&](const auto& c){return c.name==field;}),"ordinary projection unique column unavailable");resolved.emplace_back(field.data(),field.size());
                    }
                }
                program(parent.index_programs,ordinary_unique_index_program_storage(name,i,resolved));
            }
        } else {
            size_t ordinal=0;
            for(const auto& p:model.properties)if(p.is_unique){const auto i=ordinal++;if(p.kind==property_kind::primitive&&!p.is_geo_bounds){
                ordinary_initialization_vector<ordinary_initialization_text> fields;
                fields.emplace_back(p.name.data(),p.name.size());program(parent.index_programs,ordinary_unique_index_program_storage(name,i,fields));}}
        }
    }
    for(const auto& d:result.derived){charge(sizeof(d)+d.model.size()+d.property.size()+d.table.size()+d.create_sql.size());identifier(d.table);}
    return result;
}

// This literal observes ALL durable MAIN objects, including SQLite's own
// sqlite_sequence. It selects no freshness or creation authority by itself.
inline constexpr char ordinary_fresh_catalog_presence_program_[]="SELECT name FROM main.sqlite_schema LIMIT 1";

// Pure, synchronous traversal of ONE full immutable compiled catalog. The
// caller owns the real first-start/WRITE admission and typed operator checks.
// Alternate audit bundles are historical admission DATA, never fresh output.
template<class Program,class Shadow>
inline void ordinary_fresh_catalog_programs_(const ordinary_schema_projection& projection,
    const Program& program,const Shadow& shadow) {
    for(const auto& [name,table]:projection.tables)program(std::string_view(table.create_sql));
    for(const auto& [name,table]:projection.tables)for(const auto& column:table.shadow_columns)
        shadow(std::string_view(name),std::string_view(column));
    for(const auto& derived:projection.derived)if(!derived.create_sql.empty())program(std::string_view(derived.create_sql));
    for(const auto& [name,table]:projection.tables) {
        for(const auto& sql:table.index_programs)program(std::string_view(sql));
        for(const auto& sql:table.audit_programs)program(std::string_view(sql));
        for(const auto& sql:table.other_programs)program(std::string_view(sql));
    }
    for(const auto& derived:projection.derived)for(const auto& sql:derived.programs)program(std::string_view(sql));
}

inline ordinary_initialization_text_map<ordinary_initialization_text> ordinary_fresh_catalog_internal_metadata_(
    const recovery_owner_schema& catalog,const ordinary_schema_projection& projection) {
    ordinary_initialization_text_map<ordinary_initialization_text_set> parents;
    for(const auto& [name,model]:catalog.models)for(const auto& property:model.properties) {
        ordinary_initialization_text table,parent(name.data(),name.size());
        if(property.kind==property_kind::union_type||property.is_union){table.assign(property.union_desc.union_table_name);parent+=':';parent.append(property.name);}
        else if(property.kind==property_kind::virtual_link||property.kind==property_kind::virtual_list){table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());if(catalog.swift_models.count(name)){parent+=':';parent.append(property.name);}}
        else if((property.kind==property_kind::link||property.kind==property_kind::list)&&!property.is_geo_bounds&&!property.target_table.empty()){
            table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.target_table.data(),property.target_table.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());if(catalog.swift_models.count(name)){parent+=':';parent.append(property.name);}}
        else if(property.kind==property_kind::list&&property.is_geo_bounds)table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());
        if(!table.empty()&&projection.tables.count(std::string_view(table)))parents[table].insert(std::move(parent));
    }
    ordinary_initialization_text_map<ordinary_initialization_text> result;
    for(const auto& [name,table]:projection.tables)if(table.role!=ordinary_projection_role::model) {
        const auto found=parents.find(std::string_view(name));
        if(found==parents.end()||found->second.empty())throw std::runtime_error("fresh internal table lacks its full captured parent corpus");
        ordinary_initialization_text value;for(const auto& parent:found->second){if(!value.empty())value+=';';value+=parent;}
        result.emplace(ordinary_initialization_text("internal_table:")+name,std::move(value));
    }
    return result;
}
} // namespace lattice::detail
