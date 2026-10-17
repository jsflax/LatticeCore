#include "observer_delivery_schema.hpp"
#include <algorithm>
#include <set>

namespace lattice::detail {
namespace {
[[noreturn]] void refuse(const char* reason){throw ordinary_schema_validation_error(reason);}
const column_value_t& cell(const database::row_t& row,const char* name) {
    const auto found=row.find(name);if(found==row.end())refuse("delivery schema missing actual result field");return found->second;
}
bool text_is(const database::row_t& row,const char* name,std::string_view expected) {
    const auto* value=std::get_if<std::string>(&cell(row,name));return value&&*value==expected;
}
bool integer_is(const database::row_t& row,const char* name,int64_t expected) {
    const auto* value=std::get_if<int64_t>(&cell(row,name));return value&&*value==expected;
}
bool nonnegative_integer(const database::row_t& row,const char* name) {
    const auto* value=std::get_if<int64_t>(&cell(row,name));return value&&*value>=0;
}
}
std::vector<observer_delivery_meter_table> observer_delivery_schema::tables(const recovery_owner_schema& catalog) {
    // Complete retained catalog fixes global ordinal guard identities even for
    // a later closed subset capture. Reordering a requested subset cannot mint
    // another approved name/bundle. All validation inputs remain bounded.
    const auto projection=describe_ordinary_projection(catalog,{});
    if(projection.tables.size()>=observer_delivery_meter::table_ceiling)
        refuse("delivery schema complete catalog table bound");
    std::vector<observer_delivery_meter_table> result;result.reserve(projection.tables.size());
    for(const auto& [name,table]:projection.tables) {
        observer_delivery_meter_table actual;actual.name=name;
        if(table.local_id)actual.columns.push_back({"id"});
        for(const auto& column:table.columns)actual.columns.push_back({column.name});
        for(const auto& shadow:table.shadow_columns)actual.columns.push_back({shadow});
        if(actual.columns.empty()||actual.columns.size()>observer_delivery_meter::column_ceiling)
            refuse("delivery schema complete physical column bound");
        result.push_back(std::move(actual));
    }
    // The actual Core original buffers the physical AuditLog INSERT as its
    // own delivery. Model guards precharge BEFORE JSON production; these
    // additional guards charge the actual stored audit value and direct audit
    // writes as well. UPDATE/DELETE are conservatively charged although the
    // current update hook publishes only INSERTs for this internal table.
    observer_delivery_meter_table audit; audit.name="AuditLog";
    for(const char* name:{"id","globalId","tableName","operation","rowId","globalRowId",
        "changedFields","changedFieldsNames","isFromRemote","isSynchronized","timestamp","synthesized"})
        audit.columns.push_back({name});
    result.push_back(std::move(audit));
    return result;
}
std::vector<observer_delivery_meter_program> observer_delivery_schema::programs(const recovery_owner_schema& catalog) {
    return observer_delivery_meter::programs(tables(catalog));
}
ordinary_schema_metadata observer_delivery_schema::validate(const recovery_owner_schema& catalog,
    const ordinary_schema_projection& selected,const ordinary_metadata_query& query,const ordinary_metadata_limits& limits) {
    if(!query)refuse("delivery schema requires real bounded retained-view adapter");
    // sqlite_schema-only classification never instantiates a user view/module.
    // The exact new ledger DDL is checked BEFORE any ledger row is evaluated.
    const auto ledger=query("SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema WHERE name='_lattice_observer_delivery_meter_v1' COLLATE NOCASE LIMIT 2",{},2);
    if(ledger.empty())return validate_ordinary_schema(selected,query,limits);
    if(ledger.size()!=1||!text_is(ledger[0],"type","table")||
       !text_is(ledger[0],"name",observer_delivery_meter::ledger_name())||
       !text_is(ledger[0],"owner",observer_delivery_meter::ledger_name())||
       !text_is(ledger[0],"sql",observer_delivery_meter::ledger_ddl()))
        refuse("delivery schema exact versioned MAIN ledger differs");
    if(!query("SELECT 1 AS present FROM temp.sqlite_schema WHERE name='_lattice_observer_delivery_meter_v1' COLLATE NOCASE OR tbl_name='_lattice_observer_delivery_meter_v1' COLLATE NOCASE LIMIT 1",{},1).empty())
        refuse("delivery schema TEMP ledger alias or program refused");
    if(!query("SELECT 1 AS present FROM main.sqlite_schema WHERE tbl_name='_lattice_observer_delivery_meter_v1' COLLATE NOCASE AND type IN ('trigger','index') LIMIT 1",{},1).empty())
        refuse("delivery schema ledger has an unapproved program/index");
    // No untrusted TEXT/BLOB payload is copied by these shape projections.
    const auto rows=query("SELECT id,typeof(token) AS token_type,length(token) AS token_bytes,CASE WHEN typeof(events)='integer' THEN events END AS events,CASE WHEN typeof(bytes)='integer' THEN bytes END AS bytes FROM main._lattice_observer_delivery_meter_v1 LIMIT 2",{},2);
    if(rows.size()!=1||!integer_is(rows[0],"id",1)||!text_is(rows[0],"token_type","blob")||
       !integer_is(rows[0],"token_bytes",32)||!nonnegative_integer(rows[0],"events")||!nonnegative_integer(rows[0],"bytes"))
        refuse("delivery schema actual ledger row shape differs");
    auto guarded=selected;const auto complete=programs(catalog);
    std::map<std::string,size_t> appended;
    for(const auto& program:complete) {
        const auto found=guarded.tables.find(program.table);if(found==guarded.tables.end())continue;
        found->second.other_programs.push_back(program.sql);++appended[program.table];
    }
    for(const auto& [name,table]:guarded.tables)
        if(appended[name]!=3)refuse("delivery schema selected table lacks one complete approved guard bundle");
    // Existing exact table/index/audit/module/TEMP/ownership closure remains
    // the final comparator. Additional guard programs are regenerated from the
    // same retained catalog, never accepted as arbitrary observed SQL. The
    // logical caller projection is not mutated or relabeled as a new receipt.
    return validate_ordinary_schema(guarded,query,limits);
}
}
