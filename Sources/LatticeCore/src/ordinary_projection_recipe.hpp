#pragma once
#include "ordinary_schema_projection.hpp"
#include "ordinary_export_fingerprint.hpp"
#include "ordinary_export.hpp"
#include <sstream>
// Exact logical descriptor/fingerprint producer only. No query/row/current grant.
namespace lattice::detail {
struct ordinary_projection_recipe final {
    template<class Budget,class Serialize>
    static ordinary_export_fingerprint begin(const recovery_owner_schema& catalog,const ordinary_schema_projection& projection,
        const Serialize& serialize,ordinary_export_frame& frame,Budget& retained) {
        if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("startup cannot enter generic projection adapter");
        const auto append=[](std::string& out,const std::string& value,Budget& b) {
            const auto count=std::to_string(value.size());b.add(count.size()+1+value.size());out+=count;out+=':';out+=value;
        };
        ordinary_export_fingerprint schema;schema.bytes(std::string("ordinary-logical-projection-v2"));
        schema.number(projection.models.size());
        for(const auto& model_name:projection.models) {
            const auto name=ordinary_initialization_legacy_text(model_name);
            retained.add(sizeof(std::string)+name.size());frame.models.push_back(name);append(frame.scope,name,retained);
            schema.bytes(name);schema.number(catalog.swift_models.count(name));
            const auto& model=*catalog.find(name);schema.number(model.properties.size());
            for(const auto* property:ordinary_projection_property_order(model,catalog.swift_models.count(name),[&](size_t bytes){retained.add(bytes);})) {
                const auto& p=*property;
                std::ostringstream out;serialize(out,p);const auto encoded=out.str();retained.add(encoded.size());schema.bytes(encoded);
                // kind==union_type is meaningful even in catalogs whose
                // independent is_union convenience flag was not set.
                schema.bytes(p.union_desc.union_table_name);schema.number(p.union_desc.cases.size());
                for(const auto& c:p.union_desc.cases){schema.bytes(c.case_name);schema.number(c.values.size());for(const auto& v:c.values){schema.bytes(v.param_name);schema.number(static_cast<uint64_t>(v.type));schema.number(v.is_link);schema.bytes(v.link_target);}}
            }
            const auto constraints=catalog.swift_constraints.find(name);
            schema.number(constraints==catalog.swift_constraints.end()?0:constraints->second.size());
            if(constraints!=catalog.swift_constraints.end())for(const auto& c:constraints->second){schema.number(c.allows_upsert);schema.number(c.columns.size());for(const auto& field:c.columns)schema.bytes(field);}
        }
        schema.number(projection.tables.size());return schema;
    }
    template<class Budget>
    static ordinary_export_table table(ordinary_export_fingerprint& schema,std::string_view borrowed_name,
        const ordinary_projection_table& logical,Budget& retained) {
    if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("startup cannot enter generic projection adapter");
        const auto name=ordinary_initialization_legacy_text(borrowed_name);
            schema.bytes(name);schema.number(static_cast<uint64_t>(logical.role));schema.number(logical.local_id);schema.bytes(logical.create_sql);
            ordinary_export_table table;retained.add(sizeof(table)+name.size());table.name=name;table.local_id=logical.local_id;
            schema.number(logical.columns.size());
            for(const auto& column:logical.columns) {
                retained.add(2*sizeof(std::string)+sizeof(column_type)+column.name.size()+column.audit_name.size());
                table.columns.push_back(ordinary_initialization_legacy_text(column.name));table.audit_names.push_back(ordinary_initialization_legacy_text(column.audit_name));table.types.push_back(column.type);
                schema.bytes(column.name);schema.bytes(column.audit_name);schema.number(static_cast<uint64_t>(column.type));schema.number(column.nullable);schema.number(column.no_history);
            }
            for(const auto* programs:{&logical.audit_programs,&logical.other_programs,&logical.index_programs}){schema.number(programs->size());for(const auto& sql:*programs)schema.bytes(sql);}
            schema.number(logical.alternate_audit_programs.size());for(const auto& variant:logical.alternate_audit_programs){schema.number(variant.size());for(const auto& sql:variant)schema.bytes(sql);}
            schema.number(logical.shadow_columns.size());for(const auto& c:logical.shadow_columns)schema.bytes(c);
        return table;
    }
    template<class Budget>
    static void finish(ordinary_export_fingerprint& schema,const ordinary_schema_projection& projection,
        ordinary_export_frame& frame,Budget& retained) {
        if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("startup cannot enter generic projection adapter");
        schema.number(projection.derived.size());for(const auto& d:projection.derived){schema.bytes(d.model);schema.bytes(d.property);schema.bytes(d.table);schema.number(static_cast<uint64_t>(d.kind));schema.bytes(d.create_sql);schema.number(d.programs.size());for(const auto& sql:d.programs)schema.bytes(sql);}
        // Physical ALTER spelling, actual column order and installed derived
        // options are validated locally. They are not logical schema identity.
        frame.schema="ordinary-logical-projection-v2:"+schema.finish();retained.add(frame.schema.size());
    }
};
}
