#pragma once
#include "observer_delivery_meter.hpp"
#include "ordinary_schema_validation.hpp"
#include <lattice/recovery_schema.hpp>

namespace lattice::detail {
// Read-only actual-schema/profile observation, never enrollment or SQL grant.
// A detected versioned MAIN ledger requires the complete generated guard
// bundle; absence preserves the original strict validator. No trust boolean,
// supplied digest, arbitrary SQL alternative or per-trigger mixing is accepted.
struct observer_delivery_schema {
    static std::vector<observer_delivery_meter_table> tables(const recovery_owner_schema&);
    template<class Text>static observer_delivery_meter_vector<Text,observer_delivery_meter_table_storage<Text>> tables_storage_(const ordinary_schema_projection& projection) {
        const auto refuse=[](const char* message){throw ordinary_schema_validation_error(message);};
        if(projection.tables.size()>=observer_delivery_meter::table_ceiling)
            refuse("delivery schema complete catalog table bound");
        observer_delivery_meter_vector<Text,observer_delivery_meter_table_storage<Text>> result;result.reserve(projection.tables.size());
        for(const auto& [name,table]:projection.tables) {
            observer_delivery_meter_table_storage<Text> actual;actual.name=Text(name.data(),name.size());
            if(table.local_id)actual.columns.push_back({"id"});
            for(const auto& column:table.columns)actual.columns.push_back({Text(column.name.data(),column.name.size())});
            for(const auto& shadow:table.shadow_columns)actual.columns.push_back({Text(shadow.data(),shadow.size())});
            if(actual.columns.empty()||actual.columns.size()>observer_delivery_meter::column_ceiling)
                refuse("delivery schema complete physical column bound");
            result.push_back(std::move(actual));
        }
        // The actual Core original buffers the physical AuditLog INSERT as its
        // own delivery. Model guards precharge BEFORE JSON production; these
        // additional guards charge the actual stored audit value and direct audit
        // writes as well. UPDATE/DELETE are conservatively charged although the
        // current update hook publishes only INSERTs for this internal table.
        observer_delivery_meter_table_storage<Text> audit; audit.name="AuditLog";
        for(const char* name:{"id","globalId","tableName","operation","rowId","globalRowId",
            "changedFields","changedFieldsNames","isFromRemote","isSynchronized","timestamp","synthesized"})
            audit.columns.push_back({name});
        result.push_back(std::move(audit));
        return result;
    }
    // Pure compiled DATA alternatives for the actual model-prefix producer.
    // They issue no SQL/Open/profile permission and accept no observed SQL.
    static ordinary_schema_projection model_meter_variant_(const ordinary_schema_projection& model,
        const ordinary_schema_projection& normal) {
        auto result=normal;
        ordinary_projection_table ledger;ledger.name=ordinary_initialization_text(observer_delivery_meter::ledger_name());
        ledger.create_sql=ordinary_initialization_text(observer_delivery_meter::ledger_ddl());
        ledger.role=ordinary_projection_role::link;ledger.local_id=false;
        if(!result.tables.emplace(ledger.name,std::move(ledger)).second)
            throw ordinary_schema_validation_error("model meter variant already has a ledger declaration");
        const auto programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(tables_storage_<ordinary_initialization_text>(model));
        for(const auto& program:programs) {
            const auto target=result.tables.find(std::string_view(program.table));
            if(target==result.tables.end())throw ordinary_schema_validation_error("model meter guard owner left actual compiled model scope");
            target->second.other_programs.push_back(program.sql);
        }
        return result;
    }
    static void require_model_meter_objects_(const ordinary_schema_projection& model,const ordinary_schema_preimage& actual) {
        const ordinary_identifier_less less;
        const auto same=[&](std::string_view a,std::string_view b){return !less(a,b)&&!less(b,a);};
        constexpr std::string_view prefix="_lattice_observer_delivery_";
        const auto reserved=[&](std::string_view name){return name.size()>=prefix.size()&&same(name.substr(0,prefix.size()),prefix);};
        const auto programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(tables_storage_<ordinary_initialization_text>(model));
        using program=observer_delivery_meter_program_storage<ordinary_initialization_text>;
        ordinary_initialization_text_map<const program*> expected;
        for(const auto& emitted:programs)
            if(!expected.emplace(emitted.name,&emitted).second)throw ordinary_schema_validation_error("compiled model meter guard collision");
        ordinary_initialization_text_set seen;bool ledger=false;
        for(const auto& object:actual.main_objects) {
            if(!reserved(object.name)&&!reserved(object.owner))continue;
            if(same(object.name,observer_delivery_meter::ledger_name())) {
                if(ledger||std::string_view(object.name)!=observer_delivery_meter::ledger_name()||object.type!="table"||
                   std::string_view(object.owner)!=observer_delivery_meter::ledger_name()||std::string_view(object.sql)!=observer_delivery_meter::ledger_ddl())
                    throw ordinary_schema_validation_error("existing model meter ledger identity/program differs");
                ledger=true;continue;
            }
            const auto found=expected.find(std::string_view(object.name));
            if(found==expected.end()||object.type!="trigger"||object.name!=found->second->name||
               object.owner!=found->second->table||object.sql!=found->second->sql||!seen.emplace(object.name.data(),object.name.size()).second)
                throw ordinary_schema_validation_error("existing model meter guard identity/owner/program differs");
        }
        for(const auto& object:actual.temp_objects)
            if(reserved(object.name)||reserved(object.owner))throw ordinary_schema_validation_error("TEMP existing model meter family refused");
        if(!ledger||seen.size()!=expected.size())throw ordinary_schema_validation_error("existing model meter family is incomplete");
    }
    // This is DATA classification of an already captured private proof, not
    // an SQL grant. Ordinary construction keeps its original exact table-count
    // contract and its existing ordered shadow-subset checks at the caller.
    // The extra ledger is admitted only as the whole regenerated alternative.
    enum class model_stage_family { ordinary_construction, complete_meter };
    static model_stage_family model_stage_scope_(const ordinary_schema_projection& model,
        const ordinary_schema_projection& normal,const ordinary_schema_projection& selected,
        const ordinary_schema_preimage& actual) {
        if(selected.tables.size()==normal.tables.size())return model_stage_family::ordinary_construction;
        const auto expected=model_meter_variant_(model,normal);
        const auto table_equal=[](const ordinary_projection_table& a,const ordinary_projection_table& b)noexcept {
            return a.name==b.name&&a.create_sql==b.create_sql&&a.role==b.role&&a.local_id==b.local_id&&
                a.columns==b.columns&&a.audit_programs==b.audit_programs&&a.other_programs==b.other_programs&&
                a.index_programs==b.index_programs&&a.alternate_audit_programs==b.alternate_audit_programs&&
                a.shadow_columns==b.shadow_columns;
        };
        const auto equal=[&] {
            if(selected.models!=expected.models||selected.logical_bytes!=expected.logical_bytes||
               selected.columns!=expected.columns||selected.tables.size()!=expected.tables.size()||
               selected.derived.size()!=expected.derived.size())return false;
            auto a=selected.tables.begin(),b=expected.tables.begin();
            for(;a!=selected.tables.end();++a,++b)
                if(a->first!=b->first||!table_equal(a->second,b->second))return false;
            for(std::size_t i=0;i<selected.derived.size();++i) {
                const auto& a=selected.derived[i];const auto& b=expected.derived[i];
                if(a.model!=b.model||a.property!=b.property||a.table!=b.table||a.create_sql!=b.create_sql||
                   a.kind!=b.kind||a.programs!=b.programs)return false;
            }
            return true;
        };
        if(!equal()||!actual.absent_tables.empty())
            throw ordinary_schema_validation_error("model physical stage is not the complete compiled guarded family");
        require_model_meter_objects_(model,actual);
        return model_stage_family::complete_meter;
    }
    static std::vector<observer_delivery_meter_program> programs(const recovery_owner_schema&);
    static ordinary_schema_metadata validate(const recovery_owner_schema&,
        const ordinary_schema_projection&,const ordinary_metadata_query&,
        const ordinary_metadata_limits& = {});
    // Uses the existing pre-copy bounded retained-view metadata adapter. The
    // caller must already own its actual main transaction/serialized handle;
    // this performs no BEGIN, DDL, callback, enrollment or marker repair.
    static void validate_database(database&,const recovery_owner_schema&);
};
}
