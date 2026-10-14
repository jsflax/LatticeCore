#include "ordinary_export.hpp"
#include "ordinary_target_staging_io.hpp"
#include "ordinary_export_storage_access.hpp"
#include "ordinary_history_receive.hpp"
#include "ordinary_history_wire.hpp"
#include "ordinary_resource_pool.hpp"
#include "ordinary_history_stage.hpp"
#include "ordinary_history_storage_recipe.hpp"
#include "ordinary_history_record_key_recipe.hpp"
#include "ordinary_export_fingerprint.hpp"
#include "ordinary_wire_test_hooks.hpp"
#include "ordinary_schema_validation.hpp"
#include "ordinary_projection_recipe.hpp"
#include "ordinary_row_recipe.hpp"
#include "ordinary_internal_schema.hpp"
#include "ordinary_vector_integrity.hpp"
#include "ordinary_v2_property.hpp"
#include "ordinary_v2_audit_json.hpp"
#include "ordinary_v2_original.hpp"
#include "ordinary_original_descriptor_codec.hpp"
#include "recovery_writer_access.hpp"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <exception>
#include <bit>
#include "vendor/picosha2/picosha2.h"
#include <limits>
#include <set>
#include <string_view>
#include <nlohmann/json.hpp>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace lattice::detail {
sqlite3* ordinary_export_access::borrowed_storage_handle(database& writer) noexcept {
    return writer.internal_handle();
}
std::shared_ptr<void>& ordinary_export_access::history_capacity_connection_witness(database& writer) {
    if(!writer.retirement_||writer.retirement_->ordinary_history_creator_pid_!=database_retirement_state::ordinary_history_process()||writer.retirement_->state()!=database_retirement_state::phase::live)
        throw db_error("history capacity connection retired");
    return writer.retirement_->ordinary_history_capacity_witness_;
}
uint64_t ordinary_export_access::history_capacity_connection_epoch(database& writer) {
    (void)history_capacity_connection_witness(writer);
    const auto epoch=writer.retirement_->ordinary_history_mutation_epoch_.load(std::memory_order_acquire);
    if(epoch==std::numeric_limits<uint64_t>::max())throw db_error("history connection mutation sequence exhausted");
    return epoch;
}
namespace {
constexpr size_t maximum_models=16, maximum_columns=256, maximum_rows=4096;
constexpr size_t maximum_cells=131072, maximum_bytes=16*1024*1024;
struct export_limits { size_t models,columns,rows,cells,bytes; };
constexpr export_limits legacy_limits{maximum_models,maximum_columns,maximum_rows,maximum_cells,maximum_bytes};
constexpr export_limits wire_limits{256,2050,8192,2097152,128*1024*1024};
constexpr size_t maximum_profiles=16;
constexpr const char* source_table="_lattice_ordered_source_v1";
constexpr const char* source_ddl="CREATE TABLE _lattice_ordered_source_v1 ("
    "scope TEXT PRIMARY KEY,source TEXT NOT NULL,epoch INTEGER NOT NULL CHECK(epoch>0),"
    "cut INTEGER NOT NULL CHECK(cut>=0),device TEXT NOT NULL,inode TEXT NOT NULL,schema TEXT NOT NULL)";
thread_local sqlite3* mutation_owner=nullptr;
struct mutation_scope {
    sqlite3* previous;
    explicit mutation_scope(sqlite3* handle):previous(mutation_owner){mutation_owner=handle;}
    ~mutation_scope(){mutation_owner=previous;}
};
[[noreturn]] void refuse(const char* reason){throw db_error(std::string("ordinary owned export: ")+reason);}
std::string quote(const std::string& name) {
    return ordinary_row_recipe::quote(name);
}

struct budget {
    const export_limits& limits;
    size_t bytes=0,cells=0,rows=0,workspace_bytes=0;
    explicit budget(const export_limits& bounds=legacy_limits):limits(bounds){}
    budget(const budget&)=delete;
    budget& operator=(const budget&)=delete;
    void add(size_t n){if(n>limits.bytes-bytes-workspace_bytes)refuse("captured logical byte bound exceeded");bytes+=n;}
    void cell(){if(cells==limits.cells)refuse("captured cell bound exceeded");++cells;}
    struct workspace_ticket {
        budget& owner;size_t charge;
        workspace_ticket(budget& value,size_t amount)noexcept:owner(value),charge(amount){}
        workspace_ticket(const workspace_ticket&)=delete;
        ~workspace_ticket(){owner.workspace_bytes-=charge;}
    };
    // Only the synchronous private parser holds this ticket. The budget
    // outlives that call, and parser DOMs are destroyed before its ticket.
    std::shared_ptr<void> acquire_workspace(size_t n) {
        constexpr size_t overhead=sizeof(workspace_ticket)+4*sizeof(void*);
        if(n>ordinary_sync_limits::parser_workspace_bytes-overhead)refuse("audit parser workspace bound exceeded");
        const auto charge=n+overhead;
        if(charge>limits.bytes-bytes-workspace_bytes)refuse("captured logical workspace bound exceeded");
        workspace_bytes+=charge;
        try{return std::make_shared<workspace_ticket>(*this,charge);}
        catch(...){workspace_bytes-=charge;throw;}
    }
};
using values=std::vector<column_value_t>;
using rows=std::vector<values>;
// The owning WRITE pins values between the byte check and retained copy. No
// caller statement or pointer escapes; column byte counts precede allocation.
rows read(sqlite3* handle,const std::string& sql,const values& parameters,
          size_t limit,budget& retained) {
    database::record_statement();sqlite3_stmt* raw=nullptr;
    const int prepared=sqlite3_prepare_v2(handle,sql.c_str(),-1,&raw,nullptr);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,&sqlite3_finalize);
    if(prepared!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw))refuse("capture requires a read-only prepared statement");
    if(sqlite3_bind_parameter_count(raw)!=static_cast<int>(parameters.size()))refuse("capture parameter shape changed");
    for(size_t n=0;n<parameters.size();++n) {
        const auto& value=parameters[n];int rc=SQLITE_MISUSE;
        if(const auto* v=std::get_if<std::string>(&value))rc=sqlite3_bind_text(raw,static_cast<int>(n+1),v->data(),static_cast<int>(v->size()),SQLITE_STATIC);
        else if(const auto* v=std::get_if<int64_t>(&value))rc=sqlite3_bind_int64(raw,static_cast<int>(n+1),*v);
        else refuse("unsupported internal capture parameter");
        if(rc!=SQLITE_OK)refuse("capture parameter binding failed");
    }
    const int columns=sqlite3_column_count(raw);
    if(columns<1||columns>static_cast<int>(retained.limits.columns+2))refuse("capture column bound exceeded");
    rows result;int rc;
    while((rc=sqlite3_step(raw))==SQLITE_ROW) {
        if(result.size()==limit)refuse("capture row bound exceeded");
        retained.add(sizeof(values)+static_cast<size_t>(columns)*sizeof(column_value_t));
        for(int c=0;c<columns;++c)retained.cell();
        values row;row.reserve(static_cast<size_t>(columns));
        for(int c=0;c<columns;++c) {
            switch(sqlite3_column_type(raw,c)) {
                case SQLITE_NULL:row.emplace_back(nullptr);break;
                case SQLITE_INTEGER:row.emplace_back(static_cast<int64_t>(sqlite3_column_int64(raw,c)));break;
                case SQLITE_FLOAT:{const double v=sqlite3_column_double(raw,c);if(!std::isfinite(v))refuse("nonfinite captured number");row.emplace_back(v);break;}
                case SQLITE_TEXT:{
                    const int n=sqlite3_column_bytes(raw,c);if(n<0)refuse("negative captured text length");retained.add(static_cast<size_t>(n));
                    const auto* p=sqlite3_column_text(raw,c);if(!p&&n)refuse("captured text unavailable");
                    row.emplace_back(std::string(reinterpret_cast<const char*>(p?p:reinterpret_cast<const unsigned char*>("")),static_cast<size_t>(n)));break;}
                case SQLITE_BLOB:{
                    const int n=sqlite3_column_bytes(raw,c);if(n<0)refuse("negative captured blob length");retained.add(static_cast<size_t>(n));
                    const auto* p=static_cast<const uint8_t*>(sqlite3_column_blob(raw,c));if(!p&&n)refuse("captured blob unavailable");
                    std::vector<uint8_t> value;if(n)value.assign(p,p+n);row.emplace_back(std::move(value));break;}
                default:refuse("unknown captured storage class");
            }
        }
        result.push_back(std::move(row));
    }
    if(rc!=SQLITE_DONE)refuse("capture statement did not finish");
    return result;
}
// Closed metadata adapter. The caller keeps its actual main read/WRITE view
// and serialized handle for the entire validation. Every retained key, cell
// and TEXT/BLOB byte is charged before copying; no generic database callback,
// model value reader or application code is used here.
std::vector<database::row_t> read_metadata(sqlite3* handle,const std::string& sql,
    const values& parameters,size_t limit,budget& retained) {
    if(!handle||sqlite3_txn_state(handle,"main")==SQLITE_TXN_NONE||
       sql.empty()||sql.size()>256*1024||parameters.size()>16||!limit||limit>262144)
        refuse("metadata adapter requires a bounded retained main view");
    database::record_statement();sqlite3_stmt* raw=nullptr;const char* tail=nullptr;
    const int prepared=sqlite3_prepare_v2(handle,sql.data(),static_cast<int>(sql.size()),&raw,&tail);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,&sqlite3_finalize);
    if(prepared!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw)||
       tail!=sql.data()+sql.size()||sqlite3_bind_parameter_count(raw)!=static_cast<int>(parameters.size()))
        refuse("metadata adapter statement shape changed");
    for(size_t n=0;n<parameters.size();++n) {
        int rc=SQLITE_MISUSE;
        if(const auto* value=std::get_if<std::string>(&parameters[n])) {
            if(value->size()>256)refuse("metadata parameter bound exceeded");
            rc=sqlite3_bind_text(raw,static_cast<int>(n+1),value->data(),static_cast<int>(value->size()),SQLITE_STATIC);
        } else if(const auto* value=std::get_if<int64_t>(&parameters[n]))rc=sqlite3_bind_int64(raw,static_cast<int>(n+1),*value);
        if(rc!=SQLITE_OK)refuse("metadata parameter binding failed");
    }
    const int count=sqlite3_column_count(raw);if(count<1||count>16)refuse("metadata column bound exceeded");
    retained.add(static_cast<size_t>(count)*sizeof(std::string));std::vector<std::string> keys;keys.reserve(static_cast<size_t>(count));
    for(int c=0;c<count;++c) {
        const char* key=sqlite3_column_name(raw,c);if(!key)refuse("metadata column name unavailable");
        size_t length=0;while(length<=256&&key[length])++length;
        if(!length||length>256)refuse("metadata column name bound exceeded");retained.add(length);
        keys.emplace_back(key,length);
        if(std::find(keys.begin(),keys.end()-1,keys.back())!=keys.end()-1)refuse("metadata duplicate column name");
    }
    std::vector<database::row_t> result;int rc;
    while((rc=sqlite3_step(raw))==SQLITE_ROW) {
        if(result.size()==limit)refuse("metadata row bound exceeded before copy");
        retained.add(sizeof(database::row_t));database::row_t row;
        for(int c=0;c<count;++c) {
            retained.cell();retained.add(sizeof(database::row_t::value_type)+keys[static_cast<size_t>(c)].size());
            column_value_t value=nullptr;
            switch(sqlite3_column_type(raw,c)) {
                case SQLITE_NULL:break;
                case SQLITE_INTEGER:value=static_cast<int64_t>(sqlite3_column_int64(raw,c));break;
                case SQLITE_FLOAT:{const auto number=sqlite3_column_double(raw,c);if(!std::isfinite(number))refuse("nonfinite metadata number");value=number;break;}
                case SQLITE_TEXT:{
                    const int bytes=sqlite3_column_bytes(raw,c);if(bytes<0)refuse("negative metadata text length");retained.add(static_cast<size_t>(bytes));
                    const auto* data=sqlite3_column_text(raw,c);if(!data&&bytes)refuse("metadata text unavailable");
                    value=bytes?std::string(reinterpret_cast<const char*>(data),static_cast<size_t>(bytes)):std::string{};break;}
                case SQLITE_BLOB:{
                    const int bytes=sqlite3_column_bytes(raw,c);if(bytes<0)refuse("negative metadata blob length");retained.add(static_cast<size_t>(bytes));
                    const auto* data=static_cast<const uint8_t*>(sqlite3_column_blob(raw,c));if(!data&&bytes)refuse("metadata blob unavailable");
                    value=bytes?std::vector<uint8_t>(data,data+bytes):std::vector<uint8_t>{};break;}
                default:refuse("metadata storage class unavailable");
            }
            row.emplace(keys[static_cast<size_t>(c)],std::move(value));
        }
        result.push_back(std::move(row));
    }
    if(rc!=SQLITE_DONE)refuse("metadata query did not finish");
    return result;
}
const std::string& text(const column_value_t& v){const auto* p=std::get_if<std::string>(&v);if(!p)refuse("expected exact text storage");return *p;}
int64_t integer(const column_value_t& v){const auto* p=std::get_if<int64_t>(&v);if(!p)refuse("expected exact integer storage");return *p;}
bool canonical_uuid(const std::string& value) {
    if(value.size()!=36)return false;
    for(size_t n=0;n<value.size();++n) {
        if(n==8||n==13||n==18||n==23){if(value[n]!='-')return false;}
        else if(!((value[n]>='0'&&value[n]<='9')||(value[n]>='a'&&value[n]<='f')))return false;
    }
    return true;
}
void append(std::string& out,const std::string& value,budget& b) {
    const auto count=std::to_string(value.size());b.add(count.size()+1+value.size());out+=count;out+=':';out+=value;
}
std::string guard_ddl(const std::string& operation) {
    return "CREATE TRIGGER _lattice_ordered_source_"+operation+"_v1 BEFORE "+operation+
        " ON _lattice_ordered_source_v1 BEGIN SELECT lattice_ordered_export_owned_v1(); END";
}
bool inspect_storage(sqlite3* handle,budget& b) {
    auto objects=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE name LIKE '_lattice_ordered_source%' OR tbl_name=? ORDER BY name",{std::string(source_table)},6,b);
    if(objects.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected{
        {source_table,{"table",source_ddl}},
        {"sqlite_autoindex__lattice_ordered_source_v1_1",{"index",nullptr}}};
    for(const auto* op:{"INSERT","UPDATE","DELETE"})expected.emplace(std::string("_lattice_ordered_source_")+op+"_v1",std::make_pair(std::string("trigger"),column_value_t{guard_ddl(op)}));
    if(objects.size()!=expected.size())refuse("source storage/guard inventory changed");
    for(const auto& object:objects) {
        const auto found=expected.find(text(object[1]));
        if(found==expected.end()||text(object[0])!=found->second.first||object[2]!=found->second.second)
            refuse("source storage/guard definition changed");
    }
    return true;
}
void register_mutation_guard(sqlite3* handle) {
    const int rc=sqlite3_create_function_v2(handle,"lattice_ordered_export_owned_v1",0,SQLITE_UTF8,
        nullptr,[](sqlite3_context* context,int,sqlite3_value**) noexcept {
            if(mutation_owner==sqlite3_context_db_handle(context))sqlite3_result_int(context,1);
            else sqlite3_result_error(context,"ordered source mutation requires its owned export turn",-1);
        },nullptr,nullptr,nullptr);
    if(rc!=SQLITE_OK)refuse("source mutation guard registration failed");
}
void validate_property_json(const nlohmann::json& value) {
    using json=nlohmann::json;
    const auto exact_integer=[](const json& v){return v.is_number_integer()&&(!v.is_number_unsigned()||v.get<uint64_t>()<=static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));};
    if(value.is_null()||value.is_string()||value.is_boolean()||exact_integer(value))return;
    if(value.is_number_float()){if(!std::isfinite(value.get<double>()))refuse("nonfinite audit value");return;}
    if(!value.is_object()||value.empty()||value.size()>2||!value.contains("kind")||!exact_integer(value.at("kind")))refuse("unsupported audit property encoding");
    const auto kind=value.at("kind").get<int64_t>();
    if(kind<0||kind>7)refuse("unknown audit property kind");
    if(kind==static_cast<int64_t>(any_property_kind::null_kind)) {
        if(value.contains("value")&&!value.at("value").is_null())refuse("invalid null audit property");
        return;
    }
    if(!value.contains("value"))refuse("missing typed audit value");const auto& v=value.at("value");
    switch(static_cast<any_property_kind>(kind)) {
        case any_property_kind::int_kind:case any_property_kind::int64_kind:if(!exact_integer(v))refuse("invalid integer audit property");break;
        case any_property_kind::string_kind:if(!v.is_string())refuse("invalid text audit property");break;
        case any_property_kind::date_kind:case any_property_kind::float_kind:case any_property_kind::double_kind:
            if(!v.is_number()||!std::isfinite(v.get<double>())||
               (kind==static_cast<int64_t>(any_property_kind::float_kind)&&!std::isfinite(static_cast<float>(v.get<double>()))))
                refuse("invalid numeric audit property");break;
        case any_property_kind::data_kind:{
            if(!v.is_string())refuse("invalid blob audit property");const auto& hex=v.get_ref<const std::string&>();
            if(hex.size()%2)refuse("odd audit blob encoding");
            for(char c:hex)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))refuse("invalid audit blob encoding");break;}
        default:refuse("unknown audit property kind");
    }
}
void capture_projected_tables(sqlite3*,const recovery_owner_schema&,
    const std::function<void(std::ostringstream&,const property_descriptor&)>&,
    const std::vector<std::string>&,ordinary_export_frame&,budget&,bool,
    ordinary_schema_projection*,ordinary_schema_metadata*);
void capture_tables(sqlite3* handle,const recovery_owner_schema& catalog,
    const std::function<void(std::ostringstream&,const property_descriptor&)>& serialize,
    const std::function<std::vector<std::string>(const model_schema&)>& generated,
    const std::vector<std::string>& names,ordinary_export_frame& captured,budget& retained,bool include_values=true,
    ordinary_schema_projection* projection=nullptr,ordinary_schema_metadata* metadata=nullptr) {
    if(captured.wire_profile) {
        capture_projected_tables(handle,catalog,serialize,names,captured,retained,include_values,projection,metadata);
        return;
    }
    for(const auto& name:names)retained.add(sizeof(std::string)+name.size());
    captured.models=names;
    values shadow_names{std::string("AuditLog"),std::string("_SyncControl")};std::string shadow_parameters="?,?";
    for(const auto& name:names){shadow_names.emplace_back(name);shadow_parameters+=",?";}
    if(!read(handle,"SELECT 1 FROM temp.sqlite_master WHERE type='trigger' OR name IN ("+shadow_parameters+") OR name GLOB '_lattice_ordered_*' LIMIT 1",shadow_names,1,retained).empty())
        refuse("TEMP program or object shadows the owned main capture");
    // This first scope has no foreign-key or relationship closure support.
    // A conservative SQL-token presence refusal may reject quoted user text;
    // it does not pretend to be a general SQLite schema parser.
    if(!read(handle,"SELECT 1 FROM main.sqlite_master WHERE type='table' AND instr(upper(sql),'REFERENCES')>0 LIMIT 1",{},1,retained).empty())refuse("foreign-key closure requires later ordered capture support");
    for(const auto& [unused,model]:catalog.models)for(const auto& property:model.properties) {
        if(std::binary_search(names.begin(),names.end(),property.target_table))refuse("relationship closure requires later ordered capture support");
        for(const auto& item:property.union_desc.cases)for(const auto& value:item.values)
            if(value.is_link&&std::binary_search(names.begin(),names.end(),value.link_target))refuse("union closure requires later ordered capture support");
    }
    // The scope includes full model declarations and actual model programs.
    // This slice refuses relationships/virtual sidecars before allocation.
    for(const auto& name:names) {
        const auto* declaration=catalog.find(name);
        if(!declaration||catalog.swift_models.count(name)||name.front()=='_'||declaration->properties.size()>retained.limits.columns)refuse("scope declaration unavailable");
        ordinary_export_table table;table.name=name;table.columns.push_back("globalId");table.types.push_back(column_type::text);
        append(captured.scope,name,retained);append(captured.schema,name,retained);
        for(const auto& property:declaration->properties) {
            if(property.kind!=property_kind::primitive||property.is_vector||property.is_geo_bounds||property.is_union||property.is_full_text||(!property.column_name.empty()&&property.column_name!=property.name))
                refuse("model shape requires later owned export support");
            (void)quote(property.name);table.columns.push_back(property.name);table.types.push_back(property.type);
            std::ostringstream descriptor;serialize(descriptor,property);
            append(captured.schema,descriptor.str(),retained);
        }
        auto programs=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE tbl_name=? ORDER BY type,name",{name},retained.limits.columns*4,retained);
        auto generated_sql=generated(*declaration);std::set<std::string> expected_triggers;
        for(auto& sql:generated_sql) {
            retained.add(sql.size());const std::string prefix="CREATE TRIGGER IF NOT EXISTS ";
            if(sql.compare(0,prefix.size(),prefix)!=0)refuse("unexpected generated model program");
            sql.replace(0,prefix.size(),"CREATE TRIGGER ");expected_triggers.insert(std::move(sql));
        }
        bool ordinary_table=false;size_t triggers=0;
        for(const auto& program:programs) {
            const auto kind=text(program[0]);append(captured.schema,kind,retained);append(captured.schema,text(program[1]),retained);
            if(const auto* sql=std::get_if<std::string>(&program[2])) {
                append(captured.schema,*sql,retained);
                if(kind=="trigger"){++triggers;if(!expected_triggers.count(*sql))refuse("model has an unowned trigger program");}
                if(kind=="table"&&sql->find("VIRTUAL")==std::string::npos)ordinary_table=true;
            } else if(!std::holds_alternative<std::nullptr_t>(program[2]))refuse("invalid schema SQL storage");
            else append(captured.schema,"",retained);
        }
        if(triggers!=expected_triggers.size())refuse("generated model trigger inventory changed");
        if(!ordinary_table)refuse("scope is not an actual ordinary model table");
        const auto shape=read(handle,"PRAGMA main.table_xinfo("+quote(name)+")",{},retained.limits.columns+2,retained);
        if(shape.size()!=table.columns.size()+1)refuse("actual model column inventory differs");
        std::set<std::string> seen_columns;
        for(const auto& column:shape) {
            if(column.size()!=7||integer(column[6])!=0||!seen_columns.insert(text(column[1])).second)refuse("hidden or duplicate model column");
            if(text(column[1])=="id"){if(text(column[2])!="INTEGER"||integer(column[5])!=1)refuse("model local identity shape differs");continue;}
            const auto expected=std::find(table.columns.begin(),table.columns.end(),text(column[1]));
            if(expected==table.columns.end()||integer(column[5])!=0)refuse("unknown model column");
            const auto type=table.types[static_cast<size_t>(expected-table.columns.begin())];
            const std::string sql_type=type==column_type::integer?"INTEGER":type==column_type::real?"REAL":type==column_type::text?"TEXT":"BLOB";
            if(text(column[2])!=sql_type)refuse("model column type differs");
            const auto position=static_cast<size_t>(expected-table.columns.begin());
            if(position&&integer(column[3])!=int64_t(!declaration->properties[position-1].nullable))refuse("model nullability differs from declaration");
        }
        if(!include_values){captured.tables.push_back(std::move(table));continue;}
        std::string selected;for(const auto& column:table.columns){if(!selected.empty())selected+=',';selected+=quote(column);}
        table.rows=read(handle,"SELECT "+selected+" FROM main."+quote(name)+" ORDER BY CAST(globalId AS BLOB)",{},retained.limits.rows-captured.rows,retained);
        std::set<std::string> identities;
        for(const auto& row:table.rows) {
            const auto& identity=text(row[0]);if(identity.empty()||identity.size()>4096||!identities.insert(identity).second)refuse("invalid captured model identity");
            for(size_t c=1;c<row.size();++c) {
                if(std::holds_alternative<std::nullptr_t>(row[c]))continue;
                const auto type=table.types[c];
                if((type==column_type::text&&!std::holds_alternative<std::string>(row[c]))||
                   (type==column_type::integer&&!std::holds_alternative<int64_t>(row[c]))||
                   (type==column_type::blob&&!std::holds_alternative<std::vector<uint8_t>>(row[c]))||
                   (type==column_type::real&&!std::holds_alternative<double>(row[c])&&!std::holds_alternative<int64_t>(row[c])))
                    refuse("captured model storage disagrees with declaration");
            }
        }
        captured.rows+=table.rows.size();captured.tables.push_back(std::move(table));
    }
}
ordinary_audit_original_descriptor capture_original_descriptor(const audit_log_entry&,
    const recovery_owner_schema&,budget&);
void hydrate_source_upload(sqlite3*,const ordinary_export_frame&,audit_log_entry&,budget&);
bool ingress_storage(sqlite3*,budget&);
bool validate_source_upload_capture(sqlite3*,const ordinary_export_frame&,budget&);
void hydrate_canonical_original(ordinary_wire_original_turn&,const std::string&,audit_log_entry&,const recovery_owner_schema&,budget&,bool);
void capture_entries(sqlite3* handle,ordinary_export_frame& frame,const recovery_owner_schema& catalog,budget& retained,
    ordinary_wire_original_turn* canonical=nullptr) {
    const bool source_ingress=frame.wire_profile&&validate_source_upload_capture(handle,frame,retained);
    std::string placeholders;values names;
    for(const auto& table:frame.tables){if(!placeholders.empty())placeholders+=',';placeholders+='?';names.emplace_back(table.name);}
    auto audit=read(handle,"SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,CAST(timestamp AS TEXT),isFromRemote,isSynchronized,synthesized FROM main.AuditLog WHERE tableName IN ("+placeholders+") ORDER BY id",names,retained.limits.rows,retained);
    int64_t prior=0;std::set<std::string> originals;
    std::set<std::string,ordinary_identifier_less> wire_originals;
    for(const auto& row:audit) {
        retained.add(sizeof(audit_log_entry));
        for(size_t index:{size_t{1},size_t{2},size_t{3},size_t{5},size_t{6},size_t{7},size_t{8}})retained.add(text(row[index]).size());
        audit_log_entry entry;entry.id=integer(row[0]);entry.global_id=text(row[1]);entry.table_name=text(row[2]);entry.operation=text(row[3]);entry.row_id=integer(row[4]);entry.global_row_id=text(row[5]);entry.timestamp=text(row[8]);
        if(entry.id<=prior||entry.global_id.empty()||entry.global_id.size()>4096||entry.global_row_id.empty()||entry.global_row_id.size()>4096)refuse("audit identity/order changed");prior=entry.id;
        if(frame.wire_profile) {
            if(entry.global_id.find('\0')!=std::string::npos||entry.global_row_id.find('\0')!=std::string::npos||!wire_originals.insert(entry.global_id).second)
                refuse("invalid or duplicate SQLite audit identity");
        } else if(!originals.insert(entry.global_id).second)refuse("audit identity/order changed");
        if(entry.operation!="INSERT"&&entry.operation!="UPDATE"&&entry.operation!="DELETE")refuse("unknown captured audit operation");
        for(size_t index:{size_t{9},size_t{10},size_t{11}})if(integer(row[index])!=0&&integer(row[index])!=1)refuse("invalid captured audit flag");
        entry.is_from_remote=integer(row[9])!=0;entry.is_synchronized=integer(row[10])!=0;entry.synthesized=integer(row[11])!=0;
        const auto table=std::find_if(frame.tables.begin(),frame.tables.end(),[&](const auto& t){return t.name==entry.table_name;});
        if(table==frame.tables.end())refuse("audit scope changed");
        const auto& audit_names=table->audit_names.empty()?table->columns:table->audit_names;
        const auto in_scope=[&](const std::string& field){return std::find(audit_names.begin(),audit_names.end(),field)!=audit_names.end();};
        if(frame.wire_profile) {
            auto parsed=parse_ordinary_v2_audit_values(text(row[6]),text(row[7]),
                [&](size_t amount){retained.add(amount);},
                [&](size_t amount){return retained.acquire_workspace(amount);});
            for(const auto& [name,value]:parsed.fields)if(!in_scope(name))refuse("unknown audit field");
            for(const auto& name:parsed.names)if(!in_scope(name))refuse("unknown changed field name");
            entry.changed_fields=std::move(parsed.fields);entry.changed_fields_names=std::move(parsed.names);
        } else {
            const auto parse=[&retained](const std::string& source) {
                size_t events=0;
                return nlohmann::json::parse(source,[&](int depth,nlohmann::json::parse_event_t,nlohmann::json&){
                    if(depth>4||++events>retained.limits.columns*16)refuse("audit JSON shape bound exceeded");return true;});
            };
            const auto fields=parse(text(row[6])),changed=parse(text(row[7]));
            if(!fields.is_object()||fields.size()>retained.limits.columns+1||!changed.is_array()||changed.size()>retained.limits.columns+1)refuse("audit field shape changed");
            for(const auto& item:fields.items()){if(!in_scope(item.key()))refuse("unknown audit field");validate_property_json(item.value());retained.add(sizeof(any_property)+item.key().size());}
            for(const auto& name:changed)if(!name.is_null()&&(!name.is_string()||!in_scope(name.get_ref<const std::string&>())))refuse("unknown changed field name");
            entry.changed_fields=audit_log_entry::parse_changed_fields(text(row[6]));
            entry.changed_fields_names=audit_log_entry::parse_changed_fields_names(text(row[7]));
            for(const auto& [name,value]:entry.changed_fields) {
                if(const auto* p=std::get_if<std::string>(&value.value))retained.add(p->size());
                if(const auto* p=std::get_if<std::vector<uint8_t>>(&value.value))retained.add(p->size());
            }
            for(const auto& name:entry.changed_fields_names)retained.add(sizeof(std::string)+name.size());
            if(entry.changed_fields.size()!=fields.size())refuse("captured audit property decoding failed");
        }
        if(frame.wire_profile) {
            if(source_ingress)hydrate_source_upload(handle,frame,entry,retained);
            if(canonical)hydrate_canonical_original(*canonical,frame.scope,entry,catalog,retained,false);
            frame.originals.push_back(capture_original_descriptor(entry,catalog,retained));
        }
        // Fill no_history from the already captured complete cut, never a
        // later database query or a mutable caller's representation.
        const auto* declaration=catalog.find(table->name);
        if(declaration)for(const auto& property:declaration->properties)if(property.no_history&&(!frame.wire_profile||property.kind==property_kind::primitive)&&entry.operation=="UPDATE"&&
            std::find(entry.changed_fields_names.begin(),entry.changed_fields_names.end(),property.name)!=entry.changed_fields_names.end()) {
            const auto column=std::find(table->columns.begin(),table->columns.end(),property.name)-table->columns.begin();
            if(static_cast<size_t>(column)==table->columns.size())refuse("late-bound audit property lost its actual column");
            const auto live=std::find_if(table->rows.begin(),table->rows.end(),[&](const auto& value){
                const auto& id=text(value[0]);return frame.wire_profile?
                    !ordinary_identifier_less{}(id,entry.global_row_id)&&!ordinary_identifier_less{}(entry.global_row_id,id):id==entry.global_row_id;
            });
            if(live==table->rows.end()) {
                entry.changed_fields.erase(property.name);
                entry.changed_fields_names.erase(std::remove(entry.changed_fields_names.begin(),entry.changed_fields_names.end(),property.name),entry.changed_fields_names.end());
            } else {
                const auto& value=(*live)[static_cast<size_t>(column)];retained.add(sizeof(any_property));
                if(const auto* p=std::get_if<std::string>(&value))retained.add(p->size());
                if(const auto* p=std::get_if<std::vector<uint8_t>>(&value))retained.add(p->size());
                entry.changed_fields[property.name]=any_property::from_column_value(value);
            }
        }
        frame.entries.push_back(std::move(entry));
    }
    if(source_ingress&&!validate_source_upload_capture(handle,frame,retained))refuse("source ingress disappeared in capture");
}
// Stable binary scalar encoding preserves SQLite storage classes and every byte.
// The size pass charges before the retained allocation/copy; it is not a heap
// allocator accounting claim. No decoder can turn these bytes into authority.
uint8_t scalar_tag(const column_value_t& value) {return ordinary_export_scalar_tag(value);}
size_t encoded_size(const values& row,size_t byte_limit) {
    size_t n=8;
    for(const auto& value:row) {
        size_t extra=9;
        if(const auto* p=std::get_if<std::string>(&value))extra+=p->size();
        if(const auto* p=std::get_if<std::vector<uint8_t>>(&value))extra+=p->size();
        if(extra>byte_limit-n)refuse("encoded row bound exceeded");n+=extra;
    }
    return n;
}
std::vector<uint8_t> encode(const values& row,budget& retained) {
    const auto size=encoded_size(row,retained.limits.bytes);retained.add(size);
    std::vector<uint8_t> out;out.reserve(size);
    const auto number=[&](uint64_t value){for(unsigned i=0;i<8;++i)out.push_back(static_cast<uint8_t>(value>>(56-8*i)));};
    number(row.size());
    for(const auto& value:row) {
        out.push_back(scalar_tag(value));
        if(std::holds_alternative<std::nullptr_t>(value))number(0);
        else if(const auto* p=std::get_if<int64_t>(&value))number(static_cast<uint64_t>(*p));
        else if(const auto* p=std::get_if<double>(&value))number(std::bit_cast<uint64_t>(*p));
        else if(const auto* p=std::get_if<std::string>(&value)){number(p->size());out.insert(out.end(),p->begin(),p->end());}
        else {const auto& blob=std::get<std::vector<uint8_t>>(value);number(blob.size());out.insert(out.end(),blob.begin(),blob.end());}
    }
    return out;
}
using fingerprint=ordinary_export_fingerprint;
// Descriptor identity is independent of projected no_history values and of
// receiver-local ordering/ACK facts. It does not reuse the protected producer
// identity domain or issue a producer registration.
bool original_late_bound(const audit_log_entry& entry,const std::string& name,
    const ordinary_audit_original_descriptor& original,const recovery_owner_schema& catalog) {
    if(entry.operation!="UPDATE"||std::find(original.names.begin(),original.names.end(),name)==original.names.end())return false;
    const auto* model=catalog.find(entry.table_name);if(!model)return false;
    const auto property=std::find_if(model->properties.begin(),model->properties.end(),[&](const auto& p){return p.name==name;});
    return property!=model->properties.end()&&property->no_history&&property->kind==property_kind::primitive;
}
std::string original_semantic_digest(const audit_log_entry& entry,
    const ordinary_audit_original_descriptor& original,const recovery_owner_schema& catalog) {
    fingerprint hash;hash.bytes(std::string("ordinary-audit-original-semantics-v2"));
    hash.bytes(entry.global_id);hash.bytes(entry.table_name);hash.bytes(entry.operation);
    hash.bytes(entry.global_row_id);hash.bytes(entry.timestamp);hash.number(entry.synthesized);
    hash.number(original.fields.size());
    for(const auto& name:original.fields) {
        hash.bytes(name);const bool late=original_late_bound(entry,name,original,catalog);hash.number(late);
        if(!late) {
            const auto value=entry.changed_fields.find(name);if(value==entry.changed_fields.end())refuse("ordinary original lost immutable field");
            hash.number(static_cast<uint64_t>(value->second.kind));hash.scalar(value->second.value);
        }
    }
    hash.number(original.names.size());for(const auto& name:original.names)hash.bytes(name);
    return hash.finish();
}
std::string bind_original_digest(const ordinary_export_frame& frame,const std::string& semantic) {
    fingerprint hash;hash.bytes(std::string("ordinary-audit-source-original-v2"));
    hash.bytes(frame.source);hash.number(static_cast<uint64_t>(frame.epoch));hash.bytes(frame.scope);hash.bytes(frame.schema);hash.bytes(semantic);
    return hash.finish();
}
ordinary_audit_original_descriptor capture_original_descriptor(const audit_log_entry& entry,
    const recovery_owner_schema& catalog,budget& retained) {
    retained.add(sizeof(ordinary_audit_original_descriptor)+(entry.changed_fields.size()+entry.changed_fields_names.size())*sizeof(std::string)+64);
    ordinary_audit_original_descriptor result;result.fields.reserve(entry.changed_fields.size());result.names.reserve(entry.changed_fields_names.size());
    for(const auto& [name,value]:entry.changed_fields){retained.add(name.size());result.fields.push_back(name);}
    std::sort(result.fields.begin(),result.fields.end());
    for(const auto& name:entry.changed_fields_names){retained.add(name.size());result.names.push_back(name);}
    if(entry.upload_original) {
        retained.add((entry.upload_original->fields.size()+entry.upload_original->names.size())*sizeof(std::string));
        for(const auto& name:entry.upload_original->fields)retained.add(name.size());
        for(const auto& name:entry.upload_original->names)retained.add(name.size());
        result.fields=entry.upload_original->fields;result.names=entry.upload_original->names;
    }
    // Bound to the actual durable source only after its identity is validated
    // within this same WRITE. Internal before-images never publish this form.
    result.digest=original_semantic_digest(entry,result,catalog);return result;
}
void validate_original_descriptors(const ordinary_export_frame& frame,const recovery_owner_schema& catalog,budget& retained,
    const std::vector<ordinary_export_table>* local_tables=nullptr) {
    const auto workspace=retained.acquire_workspace(256);
    using row_index=std::map<std::string_view,const values*,ordinary_identifier_less>;
    std::map<std::string_view,row_index> late_rows;
    if(frame.originals.size()!=frame.entries.size())refuse("ordinary original descriptor count differs");
    for(size_t n=0;n<frame.entries.size();++n) {
        const auto& entry=frame.entries[n];const auto& original=frame.originals[n];
        const auto table=std::find_if(frame.tables.begin(),frame.tables.end(),[&](const auto& t){return t.name==entry.table_name;});
        if(table==frame.tables.end())refuse("ordinary original table missing");
        const auto& actual=local_tables?*local_tables:frame.tables;
        const auto observed=std::find_if(actual.begin(),actual.end(),[&](const auto& t){return t.name==entry.table_name;});
        if(observed==actual.end())refuse("ordinary original actual table missing");
        const auto& known=observed->audit_names.empty()?observed->columns:observed->audit_names;
        const auto allowed=[&](const std::string& name){return std::find(known.begin(),known.end(),name)!=known.end();};
        if(original.fields.size()>wire_limits.columns||original.names.size()>wire_limits.columns)refuse("ordinary original descriptor name bound");
        for(size_t i=0;i<original.fields.size();++i)if(!allowed(original.fields[i])||(i&&original.fields[i-1]>=original.fields[i]))refuse("ordinary original raw field shape differs");
        for(size_t i=0;i<original.names.size();++i)if(!allowed(original.names[i])||std::find(original.names.begin(),original.names.begin()+i,original.names[i])!=original.names.begin()+i)refuse("ordinary original raw names differ");
        // Ordinary originals need no model lookup. For no_history entries,
        // build one borrowed index per participating table instead of scanning
        // all current rows for every historical UPDATE.
        const values* live=nullptr;
        if(std::any_of(original.names.begin(),original.names.end(),[&](const auto& name){return original_late_bound(entry,name,original,catalog);})) {
            auto index=late_rows.find(table->name);
            if(index==late_rows.end()) {
                retained.add(sizeof(decltype(late_rows)::value_type));index=late_rows.emplace(table->name,row_index{}).first;
                for(const auto& row:table->rows){retained.add(sizeof(row_index::value_type));if(!index->second.emplace(text(row[0]),&row).second)refuse("ordinary original projection has duplicate target");}
            }
            const auto found=index->second.find(entry.global_row_id);if(found!=index->second.end())live=found->second;
        }
        const bool absent=!live;size_t position=0;
        for(const auto& name:original.names) {
            const bool late=original_late_bound(entry,name,original,catalog);
            if(late&&absent){if(entry.changed_fields.count(name))refuse("absent no_history target retained projected field");continue;}
            if(position==entry.changed_fields_names.size()||entry.changed_fields_names[position++]!=name)refuse("ordinary projected names differ from original");
            if(late) {
                const auto column=std::find(table->columns.begin(),table->columns.end(),name);
                const auto field=entry.changed_fields.find(name);
                if(column==table->columns.end()||field==entry.changed_fields.end())refuse("ordinary no_history projection missing");
                const auto& expected=(*live)[static_cast<size_t>(column-table->columns.begin())];
                const auto kind=std::holds_alternative<std::nullptr_t>(expected)?any_property_kind::null_kind:
                    std::holds_alternative<int64_t>(expected)?any_property_kind::int64_kind:
                    std::holds_alternative<double>(expected)?any_property_kind::double_kind:
                    std::holds_alternative<std::string>(expected)?any_property_kind::string_kind:any_property_kind::data_kind;
                if(field->second.kind!=kind||field->second.value!=expected)refuse("ordinary no_history projection differs from current cut");
            }
        }
        if(position!=entry.changed_fields_names.size())refuse("ordinary projection gained changed name");
        for(const auto& name:original.fields)if(!entry.changed_fields.count(name)&&!(absent&&original_late_bound(entry,name,original,catalog)))refuse("ordinary projection lost original field");
        for(const auto& [name,value]:entry.changed_fields)if(!std::binary_search(original.fields.begin(),original.fields.end(),name)&&!original_late_bound(entry,name,original,catalog))refuse("ordinary projection gained immutable field");
        if(bind_original_digest(frame,original_semantic_digest(entry,original,catalog))!=original.digest)refuse("ordinary source original digest differs");
    }
}
using projected_row_index=std::map<std::string_view,const values*,ordinary_identifier_less>;
void validate_projected_values(const ordinary_export_frame& frame,const ordinary_schema_projection& projection,
    const ordinary_schema_metadata& metadata,const recovery_owner_schema& catalog,budget& retained) {
    if(frame.tables.size()!=projection.tables.size())refuse("logical table projection differs");
    size_t position=0,total=0;
    for(const auto& [name,logical]:projection.tables) {
        const auto& table=frame.tables[position++];
        if(table.name!=name||table.columns.size()!=logical.columns.size()||table.types.size()!=logical.columns.size())refuse("logical column projection differs");
        const auto& physical=metadata.tables.at(name);std::vector<bool> required;
        retained.add(logical.columns.size()*sizeof(bool));required.reserve(logical.columns.size());
        for(size_t c=0;c<logical.columns.size();++c) {
            const auto& declared=logical.columns[c];
            if(table.columns[c]!=declared.name||table.types[c]!=declared.type)refuse("logical column descriptor differs");
            const auto actual=std::find_if(physical.columns.begin(),physical.columns.end(),[&](const auto& v){return v.name==declared.name;});
            if(actual==physical.columns.end())refuse("actual column metadata missing");
            required.push_back(actual->not_null!=0);
        }
        projected_row_index identities;
        for(const auto& row:table.rows) {
            if(total==retained.limits.rows||row.size()!=logical.columns.size())refuse("logical row shape/bound differs");++total;
            const auto& id=text(row[0]);
            if(id.empty()||id.size()>4096||id.find('\0')!=std::string::npos)refuse("invalid logical row identity");
            retained.add(sizeof(projected_row_index::value_type));
            if(!identities.emplace(id,&row).second)refuse("duplicate SQLite global identity");
            ordinary_row_recipe::validate_cells(row,table.types,required);
        }
    }
    if(total!=frame.rows)refuse("logical row count differs");
    for(const auto& name:projection.models)for(const auto& p:catalog.find(name)->properties) {
        const bool poly=p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list;
        const bool link=poly||((p.kind==property_kind::link||p.kind==property_kind::list)&&!p.is_geo_bounds&&!p.target_table.empty());
        if(!link)continue;
        const auto junction="_"+name+"_"+(poly?std::string{}:p.target_table+"_")+p.name;
        const auto table=std::find_if(frame.tables.begin(),frame.tables.end(),[&](const auto& t){return t.name==junction;});
        if(table==frame.tables.end())refuse("logical relationship table missing");
        std::set<std::string_view,ordinary_identifier_less> parents;
        for(const auto& row:table->rows) {
            for(size_t c:{size_t{1},size_t{2}}) {
                const auto& id=text(row[c]);if(id.empty()||id.size()>4096||id.find('\0')!=std::string::npos)refuse("relationship identity bound exceeded");
            }
            if(p.kind==property_kind::link||p.kind==property_kind::virtual_link) {
                retained.add(sizeof(std::string_view));
                if(!parents.insert(text(row[1])).second)refuse("to-one relationship has multiple targets");
            }
            if(poly&&!std::binary_search(projection.models.begin(),projection.models.end(),text(row[3])))
                refuse("polymorphic relationship leaves the actual declared scope");
        }
    }
}
void capture_projected_tables(sqlite3* handle,const recovery_owner_schema& catalog,
    const std::function<void(std::ostringstream&,const property_descriptor&)>& serialize,
    const std::vector<std::string>& names,ordinary_export_frame& frame,budget& retained,bool include_values,
    ordinary_schema_projection* retained_projection,ordinary_schema_metadata* retained_metadata) {
    try {
        auto projection=describe_ordinary_projection(catalog,names);retained.add(projection.logical_bytes);
        const ordinary_metadata_query query=[&](const auto& sql,const auto& args,size_t limit){return read_metadata(handle,sql,args,limit,retained);};
        auto metadata=validate_ordinary_schema(projection,query);
        // Both validators run synchronously on this exact owned SQLite view,
        // before any source-cut or receiver-ledger mutation. Module validation
        // must precede the internal helper validator's broader FK inspection.
        const auto internal=validate_ordinary_internal_schema(catalog,projection,metadata,query);
        retained.add(metadata.copied_bytes);retained.add(internal.copied_bytes);
        auto schema=ordinary_projection_recipe::begin(catalog,projection,serialize,frame,retained);
        for(const auto& [name,logical]:projection.tables) {
            auto table=ordinary_projection_recipe::table(schema,name,logical,retained);
            if(include_values) {
                std::string columns;for(const auto& c:table.columns){if(!columns.empty())columns+=',';columns+=quote(c);}
                table.rows=read(handle,"SELECT "+columns+" FROM main."+quote(name)+" ORDER BY CAST(globalId AS BLOB)",{},retained.limits.rows-frame.rows,retained);
                frame.rows+=table.rows.size();
            }
            frame.tables.push_back(std::move(table));
        }
        ordinary_projection_recipe::finish(schema,projection,frame,retained);
        if(include_values)validate_projected_values(frame,projection,metadata,catalog,retained);
        if(retained_projection)*retained_projection=std::move(projection);
        if(retained_metadata)*retained_metadata=std::move(metadata);
    } catch(const db_error&) {throw;}
      catch(const std::runtime_error& error) {throw db_error(std::string("ordinary projected schema: ")+error.what());}
}
void materialize_projected(database& writer,const ordinary_export_frame& before,const ordinary_export_frame& incoming,
    const ordinary_schema_projection& projection,const ordinary_schema_metadata& metadata,
    const recovery_owner_schema& catalog,budget& retained) {
    auto* handle=ordinary_export_storage_access::borrow(writer);
    // Derive absent vector dimensions only from actual incoming float bytes.
    // Existing local module options/dimensions remain locally authoritative.
    std::map<std::string,size_t> vector_dimensions;
    for(const auto& derived:metadata.derived)if(derived.kind==ordinary_derived_kind::vector) {
        const auto table=std::find_if(incoming.tables.begin(),incoming.tables.end(),[&](const auto& t){return t.name==derived.model;});
        if(table==incoming.tables.end())refuse("derived model projection missing");
        const auto field=std::find(table->columns.begin(),table->columns.end(),derived.property);
        if(field==table->columns.end())refuse("derived vector property missing");
        const auto column=static_cast<size_t>(field-table->columns.begin());size_t dimensions=derived.dimensions;
        for(const auto& row:table->rows) {
            if(std::holds_alternative<std::nullptr_t>(row[column]))continue;
            const auto* bytes=std::get_if<std::vector<uint8_t>>(&row[column]);if(!bytes)refuse("derived vector requires actual blob storage");
            if(bytes->empty())continue;
            if(bytes->size()%sizeof(float)||bytes->size()/sizeof(float)>8192)refuse("derived vector dimension bound exceeded");
            const auto size=bytes->size()/sizeof(float);if(!dimensions)dimensions=size;
            if(size!=dimensions)refuse("derived vector dimensions differ from actual module");
            for(size_t offset=0;offset<bytes->size();offset+=sizeof(float)) {
                float number;std::memcpy(&number,bytes->data()+offset,sizeof(number));
                if(!std::isfinite(number))refuse("derived vector contains nonfinite float");
            }
        }
        retained.add(sizeof(std::pair<const std::string,size_t>)+derived.table.size());vector_dimensions.emplace(derived.table,dimensions);
    }
    retained.add(2*incoming.tables.size()*sizeof(projected_row_index));
    std::vector<projected_row_index> prior(incoming.tables.size()),next(incoming.tables.size());
    retained.add((before.rows+incoming.rows)*sizeof(projected_row_index::value_type));
    for(size_t t=0;t<incoming.tables.size();++t) {
        for(const auto& row:before.tables[t].rows)prior[t].emplace(text(row[0]),&row);
        for(const auto& row:incoming.tables[t].rows)next[t].emplace(text(row[0]),&row);
    }
    // All programs come from actual metadata already checked against the
    // closed generated programs in this owned WRITE. Suspend them atomically:
    // union cascades must not destroy a surviving union's local ID, and stale
    // derived content must not turn an old FTS delete into a failed repair.
    // SQL rollback restores these objects if any mutation or check fails.
    for(const auto& object:metadata.objects)if(object.type=="trigger")
        writer.execute("DROP TRIGGER main."+quote(object.name));
    for(const auto& index:metadata.unique_indexes)writer.execute("DROP INDEX main."+quote(index.name));
    // Complete removals precede all insertions. Junction primary keys cannot
    // be suspended, so changed junction rows leave before their replacements.
    for(size_t t=0;t<incoming.tables.size();++t) {
        const auto& table=before.tables[t];
        for(const auto& row:table.rows) {
            const auto found=next[t].find(text(row[0]));
            if(found==next[t].end()||(!table.local_id&&*found->second!=row))
                writer.execute("DELETE FROM main."+quote(table.name)+" WHERE globalId=?",{row[0]});
        }
    }
    for(size_t t=0;t<incoming.tables.size();++t) {
        const auto& table=incoming.tables[t];const auto& local=before.tables[t];
        const auto fields=ordinary_row_recipe::fields(table.columns);
        const auto& columns=fields.columns;const auto& arguments=fields.arguments;const auto& assignments=fields.assignments;
        for(const auto& row:table.rows) {
            const auto existing=prior[t].find(text(row[0]));
            if(existing!=prior[t].end()&&*existing->second==row)continue;
            if(existing==prior[t].end()||!local.local_id)
                writer.execute(ordinary_row_recipe::insert(table.name,fields),row);
            else {
                // Match the old SQL identity, including NOCASE, and preserve
                // its local integer ID while installing exact new spelling.
                values args;args.reserve(row.size()+1);retained.add((row.size()+1)*sizeof(column_value_t));
                for(const auto& value:row){if(const auto* p=std::get_if<std::string>(&value))retained.add(p->size());if(const auto* p=std::get_if<std::vector<uint8_t>>(&value))retained.add(p->size());args.push_back(value);}
                retained.add(text((*existing->second)[0]).size());args.push_back((*existing->second)[0]);
                writer.execute("UPDATE main."+quote(table.name)+" SET "+assignments+" WHERE globalId=?",args);
                if(writer.changes()!=1)refuse("logical surviving row update was ignored");
            }
        }
    }
    // Link shadows are local derived values, never incoming row authority.
    for(const auto& name:projection.models)for(const auto& shadow:projection.tables.at(name).shadow_columns) {
        const auto& model=*catalog.find(name);
        const auto property=std::find_if(model.properties.begin(),model.properties.end(),[&](const auto& p){return p.kind==property_kind::link&&p.name+"__link_gid"==shadow;});
        if(property==model.properties.end())refuse("validated link shadow declaration missing");
        const auto link="_"+name+"_"+property->target_table+"_"+property->name;
        writer.execute("UPDATE main."+quote(name)+" SET "+quote(shadow)+"=(SELECT rhs FROM main."+quote(link)+" WHERE lhs="+quote(name)+".globalId COLLATE NOCASE)");
    }
    for(const auto& derived:metadata.derived) {
        const auto table="main."+quote(derived.table),model="main."+quote(derived.model);
        if(derived.kind==ordinary_derived_kind::full_text) {
            writer.execute("INSERT INTO "+table+'('+quote(derived.table)+") VALUES('rebuild')");
            writer.execute("INSERT INTO "+table+'('+quote(derived.table)+",rank) VALUES('integrity-check',1)");
        } else if(derived.kind==ordinary_derived_kind::vector) {
            const auto dimensions=vector_dimensions.at(derived.table);
            if(!derived.present&&!dimensions)continue;
            // DELETE/full-scan only walks vec0's rowid map. It cannot clear
            // orphan validity slots that KNN reads directly. Recreate the
            // validated module, including its exact local IVF options, so
            // all shadow data starts empty in this same rollbackable turn.
            if(derived.present) {
                writer.execute("DROP TABLE "+table);
                writer.execute(derived.create_sql);
            } else writer.execute(vec0_create_table_program(derived.table,static_cast<int>(dimensions),0,0));
            const auto column=quote(derived.property),predicate=column+" IS NOT NULL AND length("+column+")>0";
            writer.execute("INSERT INTO "+table+"(global_id,embedding) SELECT globalId,"+column+" FROM "+model+" WHERE "+predicate);
            const auto expected=read(handle,"SELECT globalId,"+column+" FROM "+model+" WHERE "+predicate+" ORDER BY CAST(globalId AS BLOB)",{},retained.limits.rows,retained);
            const auto actual=read(handle,"SELECT global_id,embedding FROM "+table+" ORDER BY CAST(global_id AS BLOB)",{},retained.limits.rows,retained);
            if(actual!=expected)refuse("rebuilt vector content differs from installed rows");
            if(!derived.present)for(const auto& sql:vec0_program(derived.model,derived.property).sql)writer.execute(sql);
        } else {
            const auto& logical=projection.tables.at(derived.model);
            const auto prefix=logical.role==ordinary_projection_role::geographic_list?std::string{}:derived.property+"_";
            const std::array<std::string,4> fields{prefix+"minLat",prefix+"maxLat",prefix+"minLon",prefix+"maxLon"};
            std::string columns;for(const auto& field:fields){columns+=',';columns+=quote(field);}
            const auto predicate=logical.role==ordinary_projection_role::geographic_list?std::string{}:" WHERE "+quote(fields[0])+" IS NOT NULL";
            writer.execute("DELETE FROM "+table);
            writer.execute("INSERT INTO "+table+"(id,minLat,maxLat,minLon,maxLon) SELECT id"+columns+" FROM "+model+predicate);
            const auto count=read(handle,"SELECT (SELECT COUNT(*) FROM "+table+"),(SELECT COUNT(*) FROM "+model+predicate+")",{},1,retained);
            if(count.size()!=1||count[0][0]!=count[0][1])refuse("rebuilt geographic row count differs");
            const auto check=read(handle,"SELECT rtreecheck('main',?)",{derived.table},1,retained);
            if(check.size()!=1||text(check[0][0])!="ok")refuse("rebuilt geographic index integrity differs");
        }
    }
    for(const auto& object:metadata.objects)if(object.type=="trigger")writer.execute(object.sql);
    // Explicit generated uniqueness is restored after every swap and link
    // shadow is installed. PK/globalId autoindexes were never suspended.
    for(const auto& index:metadata.unique_indexes)writer.execute(index.create_sql);
}
std::string entry_fingerprint(const audit_log_entry& entry){
    fingerprint hash;hash.bytes(std::string(entry.original_identity?"ordinary-origin-identity-v2":"ordinary-origin-v1"));hash.entry(entry);
    if(entry.original_identity)hash.original_identity(entry);return hash.finish();
}
std::string frame_fingerprint(const ordinary_export_frame& frame) {
    fingerprint hash;hash.bytes(std::string(frame.wire_profile?"ordinary-complete-cut-v2":"ordinary-complete-cut-v1"));hash.bytes(frame.source);hash.bytes(frame.scope);hash.bytes(frame.schema);
    hash.number(frame.epoch);hash.number(frame.cut);hash.bytes(frame.device);hash.bytes(frame.inode);hash.number(frame.tables.size());
    for(const auto& table:frame.tables) {
        hash.bytes(table.name);hash.number(table.columns.size());for(const auto& column:table.columns)hash.bytes(column);
        hash.number(table.rows.size());for(const auto& row:table.rows){hash.number(row.size());for(const auto& value:row)hash.scalar(value);}
    }
    hash.number(frame.entries.size());for(const auto& entry:frame.entries) {
        hash.entry(entry);
        if(frame.wire_profile) {
            // A replay receipt covers every decoded original field in the
            // cut, including source-local bookkeeping and passive identity.
            // This is deliberately stricter than cross-peer effect equality.
            hash.number(static_cast<uint64_t>(entry.id));hash.number(static_cast<uint64_t>(entry.row_id));
            hash.number(entry.is_from_remote);hash.number(entry.is_synchronized);hash.original_identity(entry);
        }
    }
    if(frame.wire_profile) {
        hash.number(frame.originals.size());
        for(const auto& original:frame.originals){hash.number(original.fields.size());for(const auto& name:original.fields)hash.bytes(name);hash.number(original.names.size());for(const auto& name:original.names)hash.bytes(name);hash.bytes(original.digest);}
    }
    return hash.finish();
}
std::vector<uint8_t> encode_entry(const audit_log_entry& entry,budget& retained,bool wire_profile=false) {
    if(wire_profile)return encode_ordinary_v2_retained_entry(entry,[&](size_t amount){retained.add(amount);});
    if(entry.original_identity)refuse("legacy retained envelope cannot discard protected identity");
    // Copy accounting precedes the bounded scalar staging buffer as well as
    // the final encoded payload. Archive local IDs/flags with original values.
    size_t copied=entry.global_id.size()+entry.table_name.size()+entry.operation.size()+entry.global_row_id.size()+entry.timestamp.size();
    for(const auto& [name,value]:entry.changed_fields){copied+=name.size();if(const auto* p=std::get_if<std::string>(&value.value))copied+=p->size();if(const auto* p=std::get_if<std::vector<uint8_t>>(&value.value))copied+=p->size();}
    for(const auto& name:entry.changed_fields_names)copied+=name.size();
    retained.add(copied+(12+entry.changed_fields.size()*3+entry.changed_fields_names.size())*sizeof(column_value_t));
    values row{entry.id,entry.global_id,entry.table_name,entry.operation,entry.row_id,entry.global_row_id,entry.timestamp,
        int64_t(entry.is_from_remote),int64_t(entry.is_synchronized),int64_t(entry.synthesized),int64_t(entry.changed_fields.size())};
    std::vector<const changed_fields_map::value_type*> fields;fields.reserve(entry.changed_fields.size());
    for(const auto& field:entry.changed_fields)fields.push_back(&field);
    std::sort(fields.begin(),fields.end(),[](const auto* a,const auto* b){return a->first<b->first;});
    for(const auto* field:fields){row.emplace_back(field->first);row.emplace_back(static_cast<int64_t>(field->second.kind));row.push_back(field->second.value);}
    row.emplace_back(static_cast<int64_t>(entry.changed_fields_names.size()));for(const auto& name:entry.changed_fields_names)row.emplace_back(name);
    return encode(row,retained);
}
const std::map<std::string,std::string>& receiver_definitions() {
    static const std::map<std::string,std::string> value{
    {"_lattice_ordered_receiver_v1","CREATE TABLE _lattice_ordered_receiver_v1 (scope TEXT PRIMARY KEY,source TEXT NOT NULL,epoch INTEGER NOT NULL,cut INTEGER NOT NULL,digest TEXT NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,schema TEXT NOT NULL,baseline TEXT NOT NULL)"},
    {"_lattice_ordered_receiver_baseline_v1","CREATE TABLE _lattice_ordered_receiver_baseline_v1 (scope TEXT NOT NULL,model TEXT NOT NULL,gid TEXT NOT NULL,value BLOB NOT NULL,PRIMARY KEY(scope,model,gid))"},
    {"_lattice_ordered_receiver_cut_v1","CREATE TABLE _lattice_ordered_receiver_cut_v1 (scope TEXT NOT NULL,cut INTEGER NOT NULL,digest TEXT NOT NULL,ids BLOB NOT NULL,PRIMARY KEY(scope,cut))"},
    {"_lattice_ordered_receiver_conflict_v1","CREATE TABLE _lattice_ordered_receiver_conflict_v1 (scope TEXT NOT NULL,kind TEXT NOT NULL,identity TEXT NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL,cut INTEGER NOT NULL,PRIMARY KEY(scope,kind,identity,digest))"}
};
    return value;
}
std::string receiver_guard(const std::string& table,const std::string& operation) {
    return "CREATE TRIGGER "+table+'_'+operation+" BEFORE "+operation+" ON "+table+" BEGIN SELECT lattice_ordered_export_owned_v1(); END";
}
constexpr const char* wire_table="_lattice_ordered_wire_v2";
constexpr const char* wire_ddl="CREATE TABLE _lattice_ordered_wire_v2 (scope TEXT PRIMARY KEY,channel TEXT NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,schema TEXT NOT NULL,cut INTEGER NOT NULL,digest TEXT NOT NULL)";
constexpr const char* wire_original_table="_lattice_ordered_wire_original_v2";
constexpr const char* wire_original_archive_table="_lattice_ordered_wire_original_archive_v2";
const std::map<std::string,std::string>& wire_definitions() {
    static const std::map<std::string,std::string> definitions{
        {wire_table,wire_ddl},
        {wire_original_table,"CREATE TABLE _lattice_ordered_wire_original_v2 (scope TEXT NOT NULL,gid TEXT NOT NULL COLLATE NOCASE,codec INTEGER NOT NULL,envelope BLOB NOT NULL,descriptor_codec INTEGER NOT NULL,descriptor BLOB NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL,cut INTEGER NOT NULL,digest TEXT NOT NULL,schema TEXT NOT NULL,audit_id INTEGER NOT NULL,projection TEXT NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(scope,gid))"},
        {wire_original_archive_table,"CREATE TABLE _lattice_ordered_wire_original_archive_v2 (scope TEXT NOT NULL,gid TEXT NOT NULL COLLATE NOCASE,prior_seal TEXT NOT NULL,reason TEXT NOT NULL,record BLOB NOT NULL,audit BLOB NOT NULL,states BLOB NOT NULL,slots BLOB NOT NULL,bindings BLOB NOT NULL,helper BLOB NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(scope,gid,seal))"}};
    return definitions;
}
bool wire_storage(sqlite3* handle,budget& retained) {
    const auto objects=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_wire*' OR tbl_name GLOB '_lattice_ordered_wire*' ORDER BY name",{},16,retained);
    if(objects.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:wire_definitions()) {
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t{ddl}));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t{nullptr}));
        for(const auto* operation:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+operation,std::make_pair(std::string("trigger"),column_value_t{receiver_guard(name,operation)}));
    }
    if(objects.size()!=expected.size())refuse("wire binding guard inventory changed");
    for(const auto& object:objects){const auto found=expected.find(text(object[1]));if(found==expected.end()||text(object[0])!=found->second.first||object[2]!=found->second.second)refuse("wire binding definition changed");}
    return true;
}
std::optional<values> wire_binding(sqlite3* handle,const std::string& scope,budget& retained) {
    if(!wire_storage(handle,retained))return std::nullopt;
    const auto found=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema,cut,digest FROM main._lattice_ordered_wire_v2 WHERE scope=?",{scope},1,retained);
    if(found.empty())return std::nullopt;return found[0];
}
const export_limits& helper_limits(sqlite3* handle) {
    budget inspected;
    return wire_storage(handle,inspected)?wire_limits:legacy_limits;
}
void store_wire_binding(database& writer,const values& binding,budget& retained) {
    writer.execute("INSERT INTO main._lattice_ordered_wire_v2 VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET cut=excluded.cut,digest=excluded.digest",binding);
    if(writer.changes()!=1||wire_binding(ordinary_export_storage_access::borrow(writer),text(binding[0]),retained)!=std::optional<values>{binding})refuse("wire source/frontier publication changed");
}
bool receiver_storage(sqlite3* handle,budget& retained) {
    const auto actual=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_receiver*' OR tbl_name GLOB '_lattice_ordered_receiver*' ORDER BY name",{},21,retained);
    if(actual.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:receiver_definitions()) {
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t{ddl}));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t{nullptr}));
        for(const auto* operation:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+operation,std::make_pair(std::string("trigger"),column_value_t{receiver_guard(name,operation)}));
    }
    if(actual.size()!=expected.size())refuse("receiver storage/guard inventory changed");
    for(const auto& object:actual){const auto found=expected.find(text(object[1]));if(found==expected.end()||text(object[0])!=found->second.first||object[2]!=found->second.second)refuse("receiver storage/guard definition changed");}
    return true;
}
std::vector<std::string> scope_names(const std::string& scope,size_t model_limit=maximum_models) {
    std::vector<std::string> result;size_t start=0;
    while(start<scope.size()) {
        if(result.size()==model_limit)refuse("stored scope exceeds bound");
        const auto colon=scope.find(':',start);if(colon==std::string::npos||colon==start||colon-start>3||scope[start]=='0')refuse("stored scope length invalid");
        size_t length=0;for(size_t i=start;i<colon;++i){if(scope[i]<'0'||scope[i]>'9')refuse("stored scope length invalid");length=length*10+static_cast<size_t>(scope[i]-'0');}
        if(length==0||length>256||length>scope.size()-colon-1)refuse("stored scope value invalid");
        result.emplace_back(scope.substr(colon+1,length));(void)quote(result.back());start=colon+1+length;
        if(result.size()>1&&result[result.size()-2]>=result.back())refuse("stored scope is not canonical");
    }
    if(result.empty())refuse("stored scope empty");return result;
}
void bound_storage(sqlite3* handle,const std::string& table,const std::string& payload,budget& retained) {
    const auto counts=read(handle,"SELECT COUNT(*),COALESCE(SUM(length("+payload+")),0) FROM main."+table,{},1,retained);
    const auto count=integer(counts.at(0).at(0)),bytes=integer(counts.at(0).at(1));
    if(count<0||count>static_cast<int64_t>(retained.limits.rows)||bytes<0||bytes>static_cast<int64_t>(retained.limits.bytes))refuse("durable receiver storage capacity exceeded");
}

// These objects are separate from the strict issued-cut receiver. Merely
// having a receiver or a JSON source field never admits the provisional route.
const std::map<std::string,std::string>& helper_definitions() {
    static const std::map<std::string,std::string> definitions{
        {"_lattice_ordered_helper_v1","CREATE TABLE _lattice_ordered_helper_v1 (scope TEXT PRIMARY KEY,channel TEXT NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,schema TEXT NOT NULL)"},
        {"_lattice_ordered_helper_entry_v1","CREATE TABLE _lattice_ordered_helper_entry_v1 (scope TEXT NOT NULL,gid TEXT NOT NULL,effect TEXT NOT NULL,payload BLOB NOT NULL,canonical INTEGER NOT NULL CHECK(canonical IN (0,1)),seal TEXT NOT NULL,PRIMARY KEY(scope,gid,effect))"}};
    return definitions;
}
bool helper_storage(sqlite3* handle,budget& retained) {
    const auto actual=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_helper*' OR tbl_name GLOB '_lattice_ordered_helper*' ORDER BY name",{},11,retained);
    if(actual.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:helper_definitions()) {
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t{ddl}));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t{nullptr}));
        for(const auto* operation:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+operation,std::make_pair(std::string("trigger"),column_value_t{receiver_guard(name,operation)}));
    }
    if(actual.size()!=expected.size())refuse("helper storage/guard inventory changed");
    for(const auto& object:actual){const auto found=expected.find(text(object[1]));if(found==expected.end()||text(object[0])!=found->second.first||object[2]!=found->second.second)refuse("helper storage/guard definition changed");}
    return true;
}
std::string effect_fingerprint(const audit_log_entry& entry) {
    // This is an applied-operation witness, never a source/cut identity.
    // Legacy uploads omit REAL audit timestamps and the receiver supplies its
    // own arrival time; local row IDs and ACK flags likewise differ by peer.
    fingerprint hash;hash.bytes(std::string("ordinary-provisional-effect-v1"));hash.entry(entry,false);return hash.finish();
}
std::string helper_entry_seal(const std::string& scope,const std::string& gid,const std::string& effect,const column_value_t& payload,int64_t canonical) {
    if((canonical!=0&&canonical!=1)||effect.size()!=64||!std::holds_alternative<std::vector<uint8_t>>(payload))refuse("helper entry storage shape changed");
    fingerprint hash;hash.bytes(std::string("ordinary-helper-retained-entry-v1"));hash.bytes(scope);hash.bytes(gid);hash.bytes(effect);hash.scalar(payload);hash.number(canonical);return hash.finish();
}
void validate_helper_entry(const audit_log_entry& entry,const ordinary_export_frame& image,budget& retained) {
    retained.add(sizeof(audit_log_entry));
    for(const auto* value:{&entry.global_id,&entry.table_name,&entry.operation,&entry.global_row_id,&entry.timestamp}){if(value->size()>4096)refuse("provisional key bound exceeded");retained.add(value->size());}
    if(entry.global_id.empty()||entry.global_row_id.empty()||(entry.operation!="INSERT"&&entry.operation!="UPDATE"&&entry.operation!="DELETE"))refuse("invalid provisional identity or operation");
    const auto table=std::find_if(image.tables.begin(),image.tables.end(),[&](const auto& value){return value.name==entry.table_name;});
    if(table==image.tables.end())refuse("provisional operation is outside its issued helper scope");
    const auto& audit_names=table->audit_names.empty()?table->columns:table->audit_names;
    const auto& limits=image.wire_profile?wire_limits:legacy_limits;
    if(entry.changed_fields.size()>limits.columns+1||entry.changed_fields_names.size()>limits.columns+1)refuse("provisional field bound exceeded");
    for(const auto& [name,property]:entry.changed_fields) {
        if(std::find(audit_names.begin(),audit_names.end(),name)==audit_names.end())refuse("unknown provisional field");
        retained.add(name.size()+sizeof(any_property));
        if(const auto* value=std::get_if<std::string>(&property.value))retained.add(value->size());
        if(const auto* value=std::get_if<std::vector<uint8_t>>(&property.value))retained.add(value->size());
        if(const auto* value=std::get_if<double>(&property.value);value&&!std::isfinite(*value))refuse("nonfinite provisional value");
        const auto kind=static_cast<int>(property.kind);if(kind<0||kind>7)refuse("unknown provisional property kind");
        if(property.kind!=any_property_kind::null_kind&&std::holds_alternative<std::nullptr_t>(property.to_column_value()))refuse("invalid provisional property representation");
    }
    for(const auto& name:entry.changed_fields_names){if(std::find(audit_names.begin(),audit_names.end(),name)==audit_names.end())refuse("unknown provisional changed field");retained.add(sizeof(std::string)+name.size());}
}
values helper_binding(sqlite3* handle,const std::string& scope,budget& retained) {
    const auto bindings=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 WHERE scope=?",{scope},1,retained);
    if(bindings.size()!=1)refuse("helper scope is not enrolled");return bindings[0];
}
void verify_helper_binding(database& writer,const values& binding,const ordinary_export_frame& image,budget& retained) {
    auto* handle=ordinary_export_storage_access::borrow(writer);
    if(!receiver_storage(handle,retained))refuse("helper lost its issued receiver");
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("helper physical identity unavailable");
    if(text(binding[0])!=image.scope||text(binding[4])!=std::to_string(physical->device)||text(binding[5])!=std::to_string(physical->inode)||text(binding[6])!=image.schema||!canonical_uuid(text(binding[2]))||integer(binding[3])!=1||text(binding[1]).size()>4096)refuse("helper source/schema/physical binding changed");
    const auto current=read(handle,"SELECT source,epoch,device,inode,schema,cut,digest,baseline FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{image.scope},1,retained);
    if(current.size()!=1||current[0][0]!=binding[2]||current[0][1]!=binding[3]||current[0][2]!=binding[4]||current[0][3]!=binding[5]||current[0][4]!=binding[6]||integer(current[0][5])<1)refuse("helper no longer has its actual source frontier");
    const auto receipt=read(handle,"SELECT digest FROM main._lattice_ordered_receiver_cut_v1 WHERE scope=? AND cut=?",{image.scope,integer(current[0][5])},1,retained);
    if(receipt.size()!=1||receipt[0][0]!=current[0][6])refuse("helper lost its committed source cut receipt");
    const auto baseline=read(handle,"SELECT model,gid,value FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=? ORDER BY model,gid",{image.scope},retained.limits.rows,retained);
    fingerprint hash;hash.bytes(image.scope);hash.number(baseline.size());for(const auto& row:baseline)for(const auto& cell:row)hash.scalar(cell);
    if(text(current[0][7])!=hash.finish())refuse("helper lost its exact committed baseline");
    if(const auto wire=wire_binding(handle,image.scope,retained)) {
        if(*wire!=values{image.scope,binding[1],binding[2],binding[3],binding[4],binding[5],binding[6],current[0][5],current[0][6]})refuse("helper lost its negotiated source/frontier binding");
    }
}
struct helper_capacity {int64_t rows,bytes;bool operator==(const helper_capacity&) const=default;};
helper_capacity helper_usage(sqlite3* handle,budget& retained) {
    const auto usage=read(handle,"SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM main._lattice_ordered_helper_entry_v1",{},1,retained);
    const helper_capacity result{integer(usage[0][0]),integer(usage[0][1])};
    if(result.rows<0||result.rows>static_cast<int64_t>(retained.limits.rows)||result.bytes<0||result.bytes>static_cast<int64_t>(retained.limits.bytes))refuse("helper retained capacity changed");return result;
}
void retain_helper_entry(database& writer,const std::string& scope,const audit_log_entry& entry,bool canonical,budget& retained,helper_capacity& usage,bool wire_profile) {
    auto* handle=ordinary_export_storage_access::borrow(writer);const auto effect=effect_fingerprint(entry);
    auto existing=read(handle,"SELECT payload,canonical,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? AND gid=? AND effect=?",{scope,entry.global_id,effect},1,retained);
    if(!existing.empty()) {
        if(text(existing[0][2])!=helper_entry_seal(scope,entry.global_id,effect,existing[0][0],integer(existing[0][1])))refuse("helper retained payload/classification changed");
        if(canonical&&integer(existing[0][1])==0) {
            const auto seal=helper_entry_seal(scope,entry.global_id,effect,existing[0][0],1);
            writer.execute("UPDATE main._lattice_ordered_helper_entry_v1 SET canonical=1,seal=? WHERE scope=? AND gid=? AND effect=?",{seal,scope,entry.global_id,effect});
            if(writer.changes()!=1)refuse("canonical helper confirmation was ignored");
            const auto confirmed=read(handle,"SELECT payload,canonical,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? AND gid=? AND effect=?",{scope,entry.global_id,effect},1,retained);
            if(confirmed.size()!=1||confirmed[0]!=values{existing[0][0],int64_t{1},seal})refuse("canonical helper confirmation postimage changed");
        }
        return; // Retain the first actual applied payload, including its flags.
    }
    const auto payload=encode_entry(entry,retained,wire_profile);
    if(usage.rows==static_cast<int64_t>(retained.limits.rows)||payload.size()>retained.limits.bytes-static_cast<size_t>(usage.bytes))refuse("unresolved helper entry capacity exhausted");
    const auto seal=helper_entry_seal(scope,entry.global_id,effect,payload,int64_t(canonical));
    writer.execute("INSERT INTO main._lattice_ordered_helper_entry_v1 VALUES(?,?,?,?,?,?)",{scope,entry.global_id,effect,payload,int64_t(canonical),seal});
    if(writer.changes()!=1)refuse("helper applied payload retention was ignored");
    ++usage.rows;usage.bytes+=static_cast<int64_t>(payload.size());
    existing=read(handle,"SELECT payload,canonical,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? AND gid=? AND effect=?",{scope,entry.global_id,effect},1,retained);
    if(existing.size()!=1||existing[0]!=values{payload,int64_t(canonical),seal})refuse("helper applied payload postimage changed");
}
// The helpers below store observations only. Their constructor is private to
// the already-owning export lane; neither decoded rows nor matching checksums
// establish source policy, channel fencing or a transaction's ownership.
const char* wire_original_columns="scope,gid,codec,envelope,descriptor_codec,descriptor,source,epoch,cut,digest,schema,audit_id,projection,seal";
void original_view(database& writer,bool write) {
    auto* h=ordinary_export_storage_access::borrow(writer);
    if(!h||sqlite3_txn_state(h,"main")==SQLITE_TXN_NONE||(write&&sqlite3_txn_state(h,"main")!=SQLITE_TXN_WRITE))refuse("canonical original requires its retained main view");
}
void original_copy_charge(const values& row,budget& b) {
    b.add(sizeof(values)+row.size()*sizeof(column_value_t));
    for(const auto& v:row) {
        if(const auto* s=std::get_if<std::string>(&v))b.add(s->size());
        if(const auto* s=std::get_if<std::vector<uint8_t>>(&v))b.add(s->size());
    }
}
void original_key_charge(const std::string& scope,const std::string& gid,budget& b) {
    if(scope.empty()||scope.size()>256*(256+4)||gid.empty()||gid.size()>4096||gid.find('\0')!=std::string::npos)refuse("canonical original key bound");
    b.add(scope.size()+gid.size()+2*sizeof(column_value_t));
}
void original_digest(const std::string& digest) {
    if(digest.size()!=64||!std::all_of(digest.begin(),digest.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))refuse("canonical original digest spelling");
}
std::string original_seal(const char* domain,const values& row,size_t count,budget& b) {
    b.add(64);fingerprint h;h.bytes(std::string_view(domain));h.number(count);
    for(size_t i=0;i<count;++i)h.scalar(row.at(i));return h.finish();
}
values original_audit(database& writer,const std::string& gid,budget& b) {
    const auto found=read(ordinary_export_storage_access::borrow(writer),"SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,synthesized,isFromRemote,isSynchronized FROM main.AuditLog WHERE globalId=?",{gid},1,b);
    if(found.empty())return {};
    const auto& row=found[0];
    if(integer(row[0])<=0||text(row[1]).empty()||text(row[2]).empty()||text(row[5]).empty())refuse("canonical original audit association shape");
    (void)integer(row[4]);
    if(text(row[3])!="INSERT"&&text(row[3])!="UPDATE"&&text(row[3])!="DELETE")refuse("canonical original audit operation");
    for(size_t i:{size_t{9},size_t{10},size_t{11}})if(integer(row[i])!=0&&integer(row[i])!=1)refuse("canonical original audit flag");
    for(size_t i:{size_t{6},size_t{7}})if(!std::holds_alternative<std::string>(row[i])&&!std::holds_alternative<std::nullptr_t>(row[i]))refuse("canonical original audit JSON storage");
    if(!std::holds_alternative<double>(row[8])&&!std::holds_alternative<int64_t>(row[8]))refuse("canonical original audit timestamp storage");
    original_copy_charge(row,b);return row;
}
std::vector<uint8_t> original_rows(const rows& input,budget& b) {
    b.add(sizeof(values)+input.size()*sizeof(column_value_t));values encoded;encoded.reserve(input.size());
    for(const auto& row:input)encoded.emplace_back(encode(row,b));
    return encode(encoded,b);
}
// This cache belongs to one already-owned database view. It observes only
// the shared context; every record seal and actual AuditLog association is
// still checked separately. Final verification is required before publication.
struct original_scope_context {
    values binding;
    rows receipts;
    bool operator==(const original_scope_context&)const=default;
};
class original_context_cache {
    database& writer_;
    budget& retained_;
    bool checked_=false;
    int64_t schema_version_=0;
    std::shared_ptr<const physical_store_identity> physical_;
    std::map<std::string,original_scope_context> scopes_;
    int64_t schema_version() {
        const auto rows=read(ordinary_export_storage_access::borrow(writer_),"PRAGMA main.schema_version",{},1,retained_);
        if(rows.size()!=1)refuse("canonical original schema version unavailable");
        return integer(rows[0][0]);
    }
    void guards() {
        if(!receiver_storage(ordinary_export_storage_access::borrow(writer_),retained_)||
           !helper_storage(ordinary_export_storage_access::borrow(writer_),retained_)||
           !wire_storage(ordinary_export_storage_access::borrow(writer_),retained_))
            refuse("canonical original receiver/helper/wire guards missing");
    }
    original_scope_context observe(const std::string& scope) {
        original_key_charge(scope,"context",retained_);
        // One charged scope copy is borrowed by all four synchronous reads.
        const values parameters{scope};
        auto* h=ordinary_export_storage_access::borrow(writer_);
        auto binding=read(h,"SELECT scope,channel,source,epoch,device,inode,schema,cut,digest FROM main._lattice_ordered_wire_v2 WHERE scope=?",parameters,1,retained_);
        if(binding.size()!=1)refuse("canonical original scope is not enrolled");
        const auto& row=binding[0];
        const auto route=read(h,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 WHERE scope=?",parameters,1,retained_);
        if(route.size()!=1)refuse("canonical original helper/source binding missing");
        for(size_t n=0;n<7;++n)if(route[0][n]!=row[n])
            refuse("canonical original helper/source binding differs");
        if(!physical_||text(row[4])!=std::to_string(physical_->device)||text(row[5])!=std::to_string(physical_->inode))
            refuse("canonical original physical association changed");
        const auto frontier=read(h,"SELECT source,epoch,cut,digest,device,inode,schema FROM main._lattice_ordered_receiver_v1 WHERE scope=?",parameters,1,retained_);
        if(frontier.size()!=1)refuse("canonical original receiver frontier missing");
        constexpr std::array<size_t,7> frontier_columns{2,3,7,8,4,5,6};
        for(size_t n=0;n<frontier_columns.size();++n)if(frontier[0][n]!=row[frontier_columns[n]])
            refuse("canonical original receiver frontier differs");
        // The production replay ledger permits at most 64 rows across all
        // scopes. This read has the same finite ceiling and retains no IDs.
        auto receipts=read(h,"SELECT cut,digest FROM main._lattice_ordered_receiver_cut_v1 WHERE scope=? ORDER BY cut",parameters,64,retained_);
        const auto current=integer(row[7]);
        bool current_found=false;int64_t previous=0;
        for(const auto& receipt:receipts) {
            const auto cut=integer(receipt[0]);original_digest(text(receipt[1]));
            if(cut<=previous||cut>current)refuse("canonical original receipt order/frontier differs");
            previous=cut;
            if(cut==current){if(receipt[1]!=row[8])refuse("canonical original current frontier receipt changed");current_found=true;}
        }
        if(!current_found)refuse("canonical original current frontier receipt missing");
        return {std::move(binding[0]),std::move(receipts)};
    }
public:
    original_context_cache(database& writer,budget& retained):writer_(writer),retained_(retained){}
    void check() {
        original_view(writer_,false);
        if(!checked_) {
            schema_version_=schema_version();guards();
            physical_=writer_.physical_identity("main",{},true);
            if(!physical_)refuse("canonical original physical identity unavailable");
            if(schema_version()!=schema_version_)refuse("canonical original schema changed during admission");
            checked_=true;
        } else if(schema_version()!=schema_version_)
            refuse("canonical original schema changed within its owned turn");
    }
    const original_scope_context& scope(const std::string& name) {
        check();
        const auto found=scopes_.find(name);if(found!=scopes_.end())return found->second;
        if(scopes_.size()==maximum_profiles)refuse("canonical original context scope bound");
        original_key_charge(name,"retained-context",retained_);
        retained_.add(sizeof(original_scope_context)+sizeof(std::string)+4*sizeof(void*));
        auto context=observe(name);
        return scopes_.emplace(name,std::move(context)).first->second;
    }
    void validate(const ordinary_wire_original_stamp& stamp,bool installation) {
        const auto& observed=scope(stamp.scope);
        if(!canonical_uuid(stamp.source)||stamp.epoch<=0||stamp.cut<=0)refuse("canonical original source coordinate");
        constexpr std::string_view domain="ordinary-logical-projection-v2:";
        if(stamp.schema.size()!=domain.size()+64||!std::string_view(stamp.schema).starts_with(domain)||
           !std::all_of(stamp.schema.begin()+static_cast<ptrdiff_t>(domain.size()),stamp.schema.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))
            refuse("canonical original schema domain changed");
        original_digest(stamp.digest);
        const auto& binding=observed.binding;
        if(text(binding[2])!=stamp.source||integer(binding[3])!=stamp.epoch||text(binding[6])!=stamp.schema||integer(binding[7])<stamp.cut)
            refuse("canonical original has no accepted wire source/frontier");
        const auto receipt=std::lower_bound(observed.receipts.begin(),observed.receipts.end(),stamp.cut,
            [](const values& row,int64_t cut){return integer(row[0])<cut;});
        const bool present=receipt!=observed.receipts.end()&&integer((*receipt)[0])==stamp.cut;
        // Installation requires the actual accepted receipt; a guarded older
        // mapping may outlive replay retention but cannot grant new authority.
        if((installation&&!present)||(present&&text((*receipt)[1])!=stamp.digest))
            refuse("canonical original cut receipt missing or changed");
    }
    void finish() {
        if(!checked_)return;
        check();guards();
        const auto physical=writer_.physical_identity("main",{},true);
        if(!physical||physical->device!=physical_->device||physical->inode!=physical_->inode)
            refuse("canonical original physical identity changed before completion");
        for(const auto& [name,context]:scopes_)if(observe(name)!=context)
            refuse("canonical original context changed before completion");
        if(schema_version()!=schema_version_)refuse("canonical original schema changed before completion");
    }
};
values original_record(database& writer,const std::string& scope,const std::string& gid,budget& b,original_context_cache& context) {
    original_view(writer,false);original_key_charge(scope,gid,b);
    context.check();
    const auto result=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+wire_original_columns+" FROM main._lattice_ordered_wire_original_v2 WHERE scope=? AND gid=?",{scope,gid},1,b);
    if(result.empty())return {};
    const auto& row=result[0];
    if(text(row[0])!=scope||text(row[1])!=gid||integer(row[2])!=1||integer(row[11])<=0||text(row[13])!=original_seal("ordinary-wire-current-record-v2",row,13,b))refuse("canonical original record seal/key changed");
    if(integer(row[4])<0||integer(row[4])>1)refuse("ordinary descriptor codec unknown");
    const auto* descriptor=std::get_if<std::vector<uint8_t>>(&row[5]);
    if(!descriptor||descriptor->size()>ordinary_sync_limits::frame_bytes||(integer(row[4])==0)!=descriptor->empty())refuse("ordinary descriptor bytes unavailable or malformed");
    original_digest(text(row[12]));
    if(!std::holds_alternative<std::vector<uint8_t>>(row[3]))refuse("canonical original envelope storage");
    original_copy_charge(row,b);return row;
}
ordinary_wire_original_record original_decode(database& writer,const values& row,budget& b,bool verify_audit,original_context_cache& context) {
    original_copy_charge(row,b);ordinary_wire_original_record out;
    out.stamp={text(row[0]),text(row[6]),text(row[10]),text(row[9]),integer(row[7]),integer(row[8])};
    context.validate(out.stamp,false);
    out.gid=text(row[1]);out.codec=integer(row[2]);out.envelope=std::get<std::vector<uint8_t>>(row[3]);out.descriptor_codec=integer(row[4]);out.descriptor=std::get<std::vector<uint8_t>>(row[5]);out.local_audit_id=integer(row[11]);out.projection_seal=text(row[12]);out.record_seal=text(row[13]);
    out.local_view=decode_ordinary_v2_retained_entry(out.envelope,[&](size_t n){b.add(n);});
    if(out.local_view.global_id!=out.gid)refuse("canonical original envelope GID differs");
    if(verify_audit) {
        original_key_charge(out.stamp.scope,out.gid,b);const auto audit=original_audit(writer,out.gid,b);
        if(audit.empty()||integer(audit[0])!=out.local_audit_id||text(audit[1])!=out.gid||original_seal("ordinary-wire-local-semantic-projection-v2",audit,11,b)!=out.projection_seal)refuse("canonical original AuditLog association changed");
        out.local_view.id=integer(audit[0]);out.local_view.row_id=integer(audit[4]);out.local_view.is_from_remote=integer(audit[10]);out.local_view.is_synchronized=integer(audit[11]);
    }
    return out;
}
struct original_usage {int64_t rows=0,bytes=0;bool operator==(const original_usage&)const=default;};
original_usage original_capacity(database& writer,bool archive,budget& b) {
    const auto found=read(ordinary_export_storage_access::borrow(writer),archive?
        "SELECT COUNT(*),COALESCE(SUM(length(record)+length(audit)+length(states)+length(slots)+length(bindings)+length(helper)),0) FROM main._lattice_ordered_wire_original_archive_v2":
        "SELECT COUNT(*),COALESCE(SUM(length(envelope)+length(descriptor)),0) FROM main._lattice_ordered_wire_original_v2",{},1,b);
    original_usage out{integer(found[0][0]),integer(found[0][1])};
    if(out.rows<0||out.rows>static_cast<int64_t>(b.limits.rows)||out.bytes<0||out.bytes>static_cast<int64_t>(b.limits.bytes))refuse("canonical original durable capacity changed");return out;
}
}

struct ordinary_wire_original_turn::state {
    database& writer;budget& retained;original_context_cache context;
    std::optional<original_usage> current_usage,archive_usage,delivery_usage;bool finished=false;
    original_usage& capacity(bool archive){auto& value=archive?archive_usage:current_usage;if(!value)value=original_capacity(writer,archive,retained);return *value;}
    void usable(){if(finished)refuse("canonical original turn already finished");original_view(writer,false);}
    state(database& w,budget& b):writer(w),retained(b),context(w,b){original_view(w,false);}
};
std::optional<ordinary_wire_original_record> ordinary_wire_original_turn::current(const std::string& scope,const std::string& gid) {
    auto& s=*state_;s.usable();const auto row=original_record(s.writer,scope,gid,s.retained,s.context);
    if(row.empty())return std::nullopt;return original_decode(s.writer,row,s.retained,true,s.context);
}
namespace {
std::string archive_original_facts(database& writer,const std::string& scope,const std::string& gid,
    const std::string& reason,const values& prior,budget& b,original_usage& usage) {
    const auto audit=original_audit(writer,gid,b);
    if(audit.empty()&&prior.empty())refuse("canonical original archive has no observed prior fact");
    const auto states=audit.empty()?rows{}:read(ordinary_export_storage_access::borrow(writer),"SELECT audit_entry_id,sync_id,is_synchronized FROM main._lattice_sync_state WHERE audit_entry_id=? ORDER BY sync_id",{integer(audit[0])},b.limits.rows,b);
    for(const auto& row:states)if(integer(row[0])!=integer(audit[0])||text(row[1]).empty()||text(row[1]).size()>4096||(integer(row[2])!=0&&integer(row[2])!=1))refuse("canonical original channel state malformed");
    // Preserve actual durable slot columns/rows. This is not a live-channel
    // enrollment fence and contains no invented session-generation field.
    const auto slot_schema=read(ordinary_export_storage_access::borrow(writer),"SELECT type,sql FROM main.sqlite_master WHERE name='_lattice_replication_slots'",{},1,b);
    rows slot_facts;
    if(!slot_schema.empty()) {
        if(text(slot_schema[0][0])!="table")refuse("replication slot observation is not a table");
        const auto columns=read(ordinary_export_storage_access::borrow(writer),"PRAGMA main.table_info('_lattice_replication_slots')",{},8,b);
        b.add(6*(sizeof(std::string)+4*sizeof(void*)+32));
        const std::set<std::string> allowed{"sync_id","confirmed_audit_id","last_received_event_id","last_active_at","upload_floor","is_observer"};
        std::set<std::string> actual;values names;
        b.add(sizeof(values)+columns.size()*sizeof(column_value_t));
        for(const auto& column:columns){const auto& name=text(column[1]);if(!allowed.count(name))refuse("unrecognized replication slot observation column");b.add(2*name.size()+sizeof(std::string)+4*sizeof(void*));if(!actual.insert(name).second)refuse("duplicate replication slot observation column");names.emplace_back(name);}
        if(!actual.count("sync_id")||!actual.count("confirmed_audit_id")||!actual.count("last_active_at"))refuse("replication slot observation lacks actual identity");
        b.add(sizeof(values)*2);original_copy_charge(slot_schema[0],b);slot_facts.push_back(slot_schema[0]);slot_facts.push_back(std::move(names));
        const auto observed=read(ordinary_export_storage_access::borrow(writer),"SELECT * FROM main._lattice_replication_slots ORDER BY sync_id",{},b.limits.rows,b);
        for(const auto& row:observed){original_copy_charge(row,b);slot_facts.push_back(row);}
    }
    const auto bindings=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,channel,source,epoch,device,inode,schema,cut,digest FROM main._lattice_ordered_wire_v2 ORDER BY scope",{},maximum_profiles,b);
    const auto helper=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,gid,effect,payload,canonical,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? AND gid=? COLLATE NOCASE ORDER BY gid,effect",{scope,gid},b.limits.rows,b);
    for(const auto& row:helper)if(text(row[5])!=helper_entry_seal(text(row[0]),text(row[1]),text(row[2]),row[3],integer(row[4])))refuse("canonical archive first-effect custody changed");
    auto record_bytes=encode(prior,b),audit_bytes=encode(audit,b),state_bytes=original_rows(states,b),slot_bytes=original_rows(slot_facts,b),binding_bytes=original_rows(bindings,b),helper_bytes=original_rows(helper,b);
    size_t added=0;for(const auto* blob:{&record_bytes,&audit_bytes,&state_bytes,&slot_bytes,&binding_bytes,&helper_bytes}){if(blob->size()>b.limits.bytes-added)refuse("canonical archive payload bound");added+=blob->size();}
    b.add(sizeof(values)+11*sizeof(column_value_t)+scope.size()+gid.size()+reason.size()+128);
    values row;row.reserve(11);row.emplace_back(scope);row.emplace_back(gid);row.emplace_back(prior.empty()?std::string{}:text(prior[13]));row.emplace_back(reason);
    row.emplace_back(std::move(record_bytes));row.emplace_back(std::move(audit_bytes));row.emplace_back(std::move(state_bytes));row.emplace_back(std::move(slot_bytes));row.emplace_back(std::move(binding_bytes));row.emplace_back(std::move(helper_bytes));
    row.emplace_back(original_seal("ordinary-wire-prior-observation-v2",row,10,b));
    original_key_charge(scope,gid,b);
    const auto found=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,gid,prior_seal,reason,record,audit,states,slots,bindings,helper,seal FROM main._lattice_ordered_wire_original_archive_v2 WHERE scope=? AND gid=? AND seal=?",{scope,gid,row[10]},1,b);
    if(!found.empty()){if(found[0]!=row)refuse("canonical archive digest collision or corruption");b.add(64);return text(row[10]);}
    if(usage.rows==static_cast<int64_t>(b.limits.rows)||added>b.limits.bytes-static_cast<size_t>(usage.bytes))refuse("canonical original archive capacity exhausted");
    register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    writer.execute("INSERT INTO main._lattice_ordered_wire_original_archive_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?)",row);
    if(writer.changes()!=1)refuse("canonical original archive insertion ignored");
    const auto post=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,gid,prior_seal,reason,record,audit,states,slots,bindings,helper,seal FROM main._lattice_ordered_wire_original_archive_v2 WHERE scope=? AND gid=? AND seal=?",{scope,gid,row[10]},1,b);
    if(post.size()!=1||post[0]!=row)refuse("canonical original archive postimage differs");
    usage=original_usage{usage.rows+1,usage.bytes+static_cast<int64_t>(added)};
    b.add(64);return text(row[10]);
}
}
std::string ordinary_wire_original_turn::archive(const std::string& scope,const std::string& gid,const std::string& reason) {
    auto& s=*state_;s.usable();auto& b=s.retained;auto& writer=s.writer;original_view(writer,true);
    if(reason.empty()||reason.size()>64||reason.find('\0')!=std::string::npos)refuse("canonical original archive reason bound");
    original_key_charge(scope,gid,b);
    const auto& binding=s.context.scope(scope).binding;
    original_copy_charge(binding,b); // Charge the temporary owning stamp before its strings copy.
    s.context.validate({text(binding[0]),text(binding[2]),text(binding[6]),text(binding[8]),integer(binding[3]),integer(binding[7])},true);
    auto prior=original_record(writer,scope,gid,b,s.context);
    if(!prior.empty())(void)original_decode(writer,prior,b,true,s.context);
    auto& usage=s.capacity(true);
    return archive_original_facts(writer,scope,gid,reason,prior,b,usage);
}

void ordinary_wire_original_turn::store(const ordinary_wire_original_stamp& stamp,const audit_log_entry& entry,
    int64_t descriptor_codec,const std::vector<uint8_t>& descriptor,int64_t local_audit_id) {
    auto& s=*state_;s.usable();auto& b=s.retained;auto& writer=s.writer;original_view(writer,true);s.context.validate(stamp,true);
    if(local_audit_id<=0||descriptor_codec<0||descriptor_codec>1||descriptor.size()>ordinary_sync_limits::frame_bytes||((descriptor_codec==0)!=descriptor.empty()))refuse("canonical original descriptor/local ID invalid");
    auto prior=original_record(writer,stamp.scope,entry.global_id,b,s.context);
    const auto usage=s.capacity(false);
    const auto prior_bytes=prior.empty()?size_t{0}:std::get<std::vector<uint8_t>>(prior[3]).size()+std::get<std::vector<uint8_t>>(prior[5]).size();
    if(prior_bytes>static_cast<size_t>(usage.bytes)||(prior.empty()&&usage.rows==static_cast<int64_t>(b.limits.rows)))refuse("canonical original current row capacity exhausted before copy");
    const auto available=b.limits.bytes-(static_cast<size_t>(usage.bytes)-prior_bytes);
    if(descriptor.size()>available)refuse("canonical descriptor capacity exhausted before copy");
    auto envelope=encode_ordinary_v2_retained_entry(entry,[&](size_t amount){
        if(amount<sizeof(std::vector<uint8_t>)||amount-sizeof(std::vector<uint8_t>)>available-descriptor.size())refuse("canonical envelope capacity exhausted before copy");b.add(amount);
    });
    original_key_charge(stamp.scope,entry.global_id,b);const auto audit=original_audit(writer,entry.global_id,b);
    if(audit.empty()||integer(audit[0])!=local_audit_id||text(audit[1])!=entry.global_id||text(audit[2])!=entry.table_name||text(audit[3])!=entry.operation||text(audit[5])!=entry.global_row_id||integer(audit[9])!=int64_t(entry.synthesized))refuse("canonical original semantic association not staged");
    const auto projection=original_seal("ordinary-wire-local-semantic-projection-v2",audit,11,b);
    b.add(sizeof(values)+14*sizeof(column_value_t)+stamp.scope.size()+entry.global_id.size()+descriptor.size()+stamp.source.size()+stamp.schema.size()+stamp.digest.size()+128);
    values row;row.reserve(14);row.emplace_back(stamp.scope);row.emplace_back(entry.global_id);row.emplace_back(int64_t{1});row.emplace_back(std::move(envelope));row.emplace_back(descriptor_codec);row.emplace_back(descriptor);
    row.emplace_back(stamp.source);row.emplace_back(stamp.epoch);row.emplace_back(stamp.cut);row.emplace_back(stamp.digest);row.emplace_back(stamp.schema);row.emplace_back(local_audit_id);row.emplace_back(projection);row.emplace_back(original_seal("ordinary-wire-current-record-v2",row,13,b));
    if(!prior.empty()) {
        if(integer(prior[8])>stamp.cut)refuse("canonical original observation would regress");
        // Re-observing identical custody in a newer accepted cut does not
        // replace the original observation or consume another archive slot.
        // Envelope/descriptor, local association and source domain stay exact;
        // only the unrelated cut/digest and resulting record seal may differ.
        bool unchanged=true;
        for(size_t i:{size_t{0},size_t{1},size_t{2},size_t{3},size_t{4},size_t{5},size_t{6},size_t{7},size_t{10},size_t{11},size_t{12}})
            if(prior[i]!=row[i])unchanged=false;
        if(unchanged){(void)original_decode(writer,prior,b,true,s.context);return;}
        // The caller archives before changing the actual AuditLog projection.
        // Current reads intentionally fail if that association already moved;
        // replacement uses sealed prior record bytes only after archival proof.
        const auto old_bytes=encode(prior,b);
        const auto archives=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,gid,prior_seal,reason,record,audit,states,slots,bindings,helper,seal FROM main._lattice_ordered_wire_original_archive_v2 WHERE scope=? AND gid=? AND prior_seal=?",{stamp.scope,entry.global_id,prior[13]},b.limits.rows,b);
        bool retained=false;for(const auto& saved:archives){if(text(saved[10])!=original_seal("ordinary-wire-prior-observation-v2",saved,10,b))refuse("prior canonical archive seal changed");const auto* bytes=std::get_if<std::vector<uint8_t>>(&saved[4]);if(!bytes)refuse("canonical archive record storage changed");if(*bytes==old_bytes)retained=true;}
        if(!retained)refuse("canonical replacement lacks exact prior observation archive");
    }
    const auto next_bytes=std::get<std::vector<uint8_t>>(row[3]).size()+descriptor.size();
    if(prior_bytes>static_cast<size_t>(usage.bytes)||next_bytes>b.limits.bytes-(static_cast<size_t>(usage.bytes)-prior_bytes)||(prior.empty()&&usage.rows==static_cast<int64_t>(b.limits.rows)))refuse("canonical original current capacity exhausted");
    register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    writer.execute("INSERT INTO main._lattice_ordered_wire_original_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scope,gid) DO UPDATE SET codec=excluded.codec,envelope=excluded.envelope,descriptor_codec=excluded.descriptor_codec,descriptor=excluded.descriptor,source=excluded.source,epoch=excluded.epoch,cut=excluded.cut,digest=excluded.digest,schema=excluded.schema,audit_id=excluded.audit_id,projection=excluded.projection,seal=excluded.seal",row);
    if(writer.changes()!=1||original_record(writer,stamp.scope,entry.global_id,b,s.context)!=row)refuse("canonical original current postimage differs");
    s.current_usage=original_usage{usage.rows+(prior.empty()?1:0),usage.bytes-static_cast<int64_t>(prior_bytes)+static_cast<int64_t>(next_bytes)};
}
namespace {
ordinary_audit_original_descriptor canonical_descriptor(const ordinary_wire_original_record& record,
    const recovery_owner_schema& catalog,budget& b) {
    if(record.descriptor_codec!=1)refuse("canonical original membership is unknown; bootstrap required");
    auto descriptor=decode_ordinary_original_descriptor(record.descriptor,[&](size_t n){b.add(n);});
    const auto semantic=original_semantic_digest(record.local_view,descriptor,catalog);
    fingerprint bound;bound.bytes(std::string("ordinary-audit-source-original-v2"));bound.bytes(record.stamp.source);
    bound.number(static_cast<uint64_t>(record.stamp.epoch));bound.bytes(record.stamp.scope);bound.bytes(record.stamp.schema);bound.bytes(semantic);
    if(descriptor.digest!=bound.finish())refuse("canonical original descriptor lost its actual source binding");
    return descriptor;
}
void hydrate_canonical_original(ordinary_wire_original_turn& turn,const std::string& scope,
    audit_log_entry& entry,const recovery_owner_schema& catalog,budget& b,bool require_known) {
    auto saved=turn.current(scope,entry.global_id);if(!saved)return;
    if(saved->descriptor_codec==0&&!require_known)return; // internal foundation snapshot only; never exact upload
    if(saved->local_view.id!=entry.id||saved->local_view.table_name!=entry.table_name||saved->local_view.global_row_id!=entry.global_row_id)
        refuse("canonical upload changed its local association");
    auto descriptor=canonical_descriptor(*saved,catalog,b);
    b.add(sizeof(ordinary_upload_original)+4*sizeof(void*));auto observed=std::make_shared<ordinary_upload_original>();
    observed->fields=std::move(descriptor.fields);observed->names=std::move(descriptor.names);
    // Hydration restores immutable raw membership before a fresh same-view
    // NoHistory projection. It does not rewrite the stored canonical envelope.
    b.add(observed->names.size()*sizeof(std::string));for(const auto& name:observed->names)b.add(name.size());
    saved->local_view.changed_fields_names=observed->names;saved->local_view.upload_original=std::move(observed);
    b.add(saved->record_seal.size());saved->local_view.ordinary_delivery_version=saved->record_seal;
    entry=std::move(saved->local_view);
}
}

#include "ordinary_delivery_storage.inc"

void ordinary_wire_original_turn::finish() {
    auto& s=*state_;s.usable();
    if(s.current_usage&&original_capacity(s.writer,false,s.retained)!=*s.current_usage)refuse("canonical current final capacity changed");
    if(s.archive_usage&&original_capacity(s.writer,true,s.retained)!=*s.archive_usage)refuse("canonical archive final capacity changed");
    if(s.delivery_usage&&delivery_usage(ordinary_export_storage_access::borrow(s.writer),s.retained)!=*s.delivery_usage)refuse("delivery final accounting changed");
    s.context.finish();
    s.finished=true;
}
recovery_install_result ordinary_export_access::with_original_store_for_test(lattice_db& owner,const std::function<void(ordinary_wire_original_turn&)>& body) {
    return recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        if(recovery_writer_access::active_writer(owner)!=&writer)refuse("test store lost actual writer");
        budget retained{wire_limits};ordinary_wire_original_turn::state state{writer,retained};ordinary_wire_original_turn turn{state};body(turn);turn.finish();
    });
}

void require_ordered_receiver_absent(database& writer) {
    // This is deliberately uncached: enrollment is online in this foundation.
    // A previous ordinary absence is not a durable downgrade grant. The full
    // incremental transport will replace this refusal, not bypass it.
    if(!writer.query("SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_receiver*' OR tbl_name GLOB '_lattice_ordered_receiver*' OR name GLOB '_lattice_ordered_wire*' OR tbl_name GLOB '_lattice_ordered_wire*' OR name GLOB '_lattice_ordered_prepare_*' OR tbl_name GLOB '_lattice_ordered_prepare_*' OR name GLOB '_lattice_ordered_accepted_*' OR tbl_name GLOB '_lattice_ordered_accepted_*' LIMIT 1").empty())
        refuse("ordered receiver requires issued cuts; legacy export/apply is unavailable");
}

ordinary_export_frame ordinary_export_access::snapshot(lattice_db& owner,database& writer,const std::vector<std::string>& names,bool include_values) {
    if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_)refuse("helper owner catalog unavailable");
    auto* hook=writer.lattice_update_hook_context_.get();
    if(!hook||hook->owner!=&owner||hook->connection!=ordinary_export_storage_access::borrow(writer))refuse("helper requires its actual writer owner");
    budget binding_budget;std::string scope;for(const auto& name:names)append(scope,name,binding_budget);
    // A protected negotiated binding selects v2's finite local profile. It
    // grants nothing: the actual schema and exact source/frontier/baseline
    // are still checked by this capture and verify_helper_binding below.
    const bool wire=wire_binding(ordinary_export_storage_access::borrow(writer),scope,binding_budget).has_value();
    for(const auto& name:names){const auto* model=owner.recovery_schemas_.find(name);if(!model)refuse("helper model missing");if(!wire)for(const auto& property:model->properties)if(property.no_history)refuse("helper no_history requires a negotiated incremental route");}
    ordinary_export_frame result;result.wire_profile=wire;budget retained{wire?wire_limits:legacy_limits};
    capture_tables(ordinary_export_storage_access::borrow(writer),owner.recovery_schemas_,[](auto& stream,const auto& property){lattice_db::serialize_property_for_fingerprint(stream,property);},[&](const model_schema& declaration){
        std::vector<std::pair<std::string,column_type>> columns;std::set<std::string> no_history;
        for(const auto& property:declaration.properties)columns.emplace_back(property.name,property.type);
        std::vector<std::string> programs;owner.create_model_table_triggers(declaration.table_name,columns,no_history,{},&programs);return programs;
    },names,result,retained,include_values);
    if(include_values) {
        if(wire){ordinary_wire_original_turn::state state{writer,retained};ordinary_wire_original_turn turn{state};
            capture_entries(ordinary_export_storage_access::borrow(writer),result,owner.recovery_schemas_,retained,&turn);turn.finish();}
        else capture_entries(ordinary_export_storage_access::borrow(writer),result,owner.recovery_schemas_,retained);
    }
    return result;
}
void ordinary_export_access::preserve(lattice_db& owner,database& writer) {
    auto* handle=ordinary_export_storage_access::borrow(writer);
    budget delivery_budget{wire_limits};
    if(delivery_storage(handle,delivery_budget)&&!read(handle,"SELECT 1 FROM main._lattice_ordered_delivery_v1 WHERE repair=1 LIMIT 1",{},1,delivery_budget).empty())
        refuse("canonical delivery repair retains actual audit history");
    if(writer.query("SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_receiver*' OR name GLOB '_lattice_ordered_helper*' OR name GLOB '_lattice_ordered_wire*' OR tbl_name GLOB '_lattice_ordered_receiver*' OR tbl_name GLOB '_lattice_ordered_helper*' OR tbl_name GLOB '_lattice_ordered_wire*' LIMIT 1").empty())return;
    budget retained{helper_limits(handle)};
    if(!helper_storage(handle,retained))refuse("strict or damaged ordered receiver refuses legacy ACK/history mutation");
    if(sqlite3_get_autocommit(handle))refuse("helper preservation requires the actual owned WRITE");
    const auto bindings=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 ORDER BY scope",{},maximum_profiles,retained);
    register_mutation_guard(handle);mutation_scope permit(handle);
    auto usage=helper_usage(handle,retained);
    for(const auto& binding:bindings) {
        const auto image=snapshot(owner,writer,scope_names(text(binding[0]),retained.limits.models));verify_helper_binding(writer,binding,image,retained);
        for(const auto& entry:image.entries){validate_helper_entry(entry,image,retained);retain_helper_entry(writer,image.scope,entry,false,retained,usage,image.wire_profile);}
    }
    if(helper_usage(handle,retained)!=usage)refuse("helper preservation accounting changed");
}
void preserve_ordered_helper_history(lattice_db& owner,database& writer){ordinary_export_access::preserve(owner,writer);}
ordinary_provisional_delivery::ordinary_provisional_delivery(lattice_db& owner,std::shared_ptr<database> writer,const std::vector<audit_log_entry>& input,const std::optional<std::string>& channel):owner_(&owner),writer_(std::move(writer)) {
    budget retained{helper_limits(ordinary_export_storage_access::borrow(*writer_))};
    if(input.size()>retained.limits.rows)refuse("provisional delivery count bound exceeded");
    if(channel){if(channel->empty()||channel->size()>4096)refuse("provisional channel bound exceeded");retained.add(channel->size());channel_=channel;}
    // Reserve the complete retained input charge before copying caller memory.
    for(const auto& entry:input) {
        if(entry.changed_fields.size()>retained.limits.columns+1||entry.changed_fields_names.size()>retained.limits.columns+1)refuse("provisional copy field bound exceeded");
        retained.add(sizeof(audit_log_entry)+entry.global_id.size()+entry.table_name.size()+entry.operation.size()+entry.global_row_id.size()+entry.timestamp.size());
        for(const auto& [name,p]:entry.changed_fields){retained.add(sizeof(any_property)+name.size());if(const auto* value=std::get_if<std::string>(&p.value))retained.add(value->size());if(const auto* value=std::get_if<std::vector<uint8_t>>(&p.value))retained.add(value->size());}
        for(const auto& name:entry.changed_fields_names)retained.add(sizeof(std::string)+name.size());
        if(entry.original_identity)refuse("protected original identity is not a provisional helper grant");
    }
    entries_=input;
}
bool ordinary_provisional_delivery::already_applied(const audit_log_entry& entry) const {
    const auto found=applied_.find(entry.global_id);return found!=applied_.end()&&found->second==effect_fingerprint(entry);
}
void ordinary_export_access::admit(ordinary_provisional_delivery& delivery,lattice_db& owner,database& writer) {
    if(delivery.owner_!=&owner||delivery.writer_.get()!=&writer||sqlite3_get_autocommit(ordinary_export_storage_access::borrow(writer)))refuse("provisional delivery lost its actual owned WRITE");
    {std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||owner.db_!=delivery.writer_)refuse("provisional delivery owner changed its writer");}
    auto* handle=ordinary_export_storage_access::borrow(writer);budget retained{helper_limits(handle)};
    if(!helper_storage(handle,retained))refuse("provisional delivery lost its issued helper route");
    const auto bindings=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 ORDER BY scope",{},maximum_profiles,retained);
    const values* selected=nullptr;
    for(const auto& binding:bindings) {
        if(text(binding[1])!=delivery.channel_.value_or(""))continue;
        const auto selection=snapshot(owner,writer,scope_names(text(binding[0]),retained.limits.models),false);bool covers=true;
        for(const auto& entry:delivery.entries_)if(std::none_of(selection.tables.begin(),selection.tables.end(),[&](const auto& t){return t.name==entry.table_name;}))covers=false;
        if(covers){if(selected)refuse("provisional helper scope is ambiguous");selected=&binding;}
    }
    if(!selected)refuse("provisional delivery has no matching source scope/channel");
    const auto image=snapshot(owner,writer,scope_names(text((*selected)[0]),retained.limits.models));verify_helper_binding(writer,*selected,image,retained);
    if(!image.wire_profile&&delivery.entries_.size()>legacy_limits.rows)refuse("legacy provisional delivery count bound exceeded");
    auto turn=*selected;const auto frontier=read(handle,"SELECT cut,digest FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{image.scope},1,retained);
    if(frontier.size()!=1)refuse("provisional source frontier missing");turn.insert(turn.end(),frontier[0].begin(),frontier[0].end());
    if(delivery.binding_.empty())delivery.binding_=turn;else if(delivery.binding_!=turn)refuse("issued cut changed between provisional chunks");
    register_mutation_guard(handle);mutation_scope permit(handle);
    auto usage=helper_usage(handle,retained);
    for(const auto& entry:image.entries){validate_helper_entry(entry,image,retained);retain_helper_entry(writer,image.scope,entry,false,retained,usage,image.wire_profile);}
    if(helper_usage(handle,retained)!=usage)refuse("helper admission accounting changed");
    const auto known=read(handle,"SELECT gid,effect,payload,canonical,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? ORDER BY gid,effect",{image.scope},retained.limits.rows,retained);
    delivery.applied_.clear();
    for(const auto& row:known){if(text(row[4])!=helper_entry_seal(image.scope,text(row[0]),text(row[1]),row[2],integer(row[3])))refuse("provisional applied witness changed");const auto [position,inserted]=delivery.applied_.emplace(text(row[0]),text(row[1]));if(!inserted&&position->second!=text(row[1]))position->second.clear();}
    std::map<std::string,std::string> incoming;
    for(const auto& entry:delivery.entries_) {
        validate_helper_entry(entry,image,retained);const auto effect=effect_fingerprint(entry);
        const auto existing=delivery.applied_.find(entry.global_id);
        if(existing!=delivery.applied_.end()&&existing->second!=effect)refuse("provisional original identity changed its applied effect");
        const auto [position,inserted]=incoming.emplace(entry.global_id,effect);if(!inserted&&position->second!=effect)refuse("provisional delivery repeats a conflicting identity");
    }
}
void ordinary_export_access::finish(ordinary_provisional_delivery& delivery,database& writer,const std::vector<std::string>& accepted,size_t begin) {
    if(delivery.binding_.empty()||delivery.writer_.get()!=&writer||sqlite3_get_autocommit(ordinary_export_storage_access::borrow(writer)))refuse("provisional completion has no admitted turn");
    auto* handle=ordinary_export_storage_access::borrow(writer);budget retained{helper_limits(handle)};register_mutation_guard(handle);mutation_scope permit(handle);
    if(!helper_storage(handle,retained))refuse("provisional completion lost its helper guards");
    auto current=helper_binding(handle,text(delivery.binding_[0]),retained);
    const auto frontier=read(handle,"SELECT cut,digest FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{current[0]},1,retained);
    if(frontier.size()!=1)refuse("provisional completion lost its source frontier");current.insert(current.end(),frontier[0].begin(),frontier[0].end());
    if(current!=delivery.binding_)refuse("source binding changed before provisional commit");
    const bool wire_profile=wire_binding(handle,text(delivery.binding_[0]),retained).has_value();
    auto usage=helper_usage(handle,retained);
    for(size_t n=begin;n<accepted.size();++n) {
        const auto entry=std::find_if(delivery.entries_.begin(),delivery.entries_.end(),[&](const auto& value){return value.global_id==accepted[n];});
        if(entry==delivery.entries_.end())refuse("provisional ACK has no actual admitted input");
        retain_helper_entry(writer,text(delivery.binding_[0]),*entry,false,retained,usage,wire_profile);
    }
    if(helper_usage(handle,retained)!=usage)refuse("helper completion accounting changed");
    // No canonical frontier, receipt or classification is changed here.
}
std::optional<std::vector<std::string>> ordinary_export_access::provisional(lattice_db& owner,const std::vector<audit_log_entry>& entries,const std::optional<std::string>& channel) {
    std::shared_ptr<database> writer;
    {std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||!owner.db_)refuse("provisional owner closed");writer=owner.db_;}
    budget retained;if(!helper_storage(ordinary_export_storage_access::borrow(*writer),retained))return std::nullopt;
    ordinary_provisional_delivery delivery(owner,std::move(writer),entries,channel);
    return owner.apply_remote_changes_impl_(delivery.entries_,channel,nullptr,&delivery);
}
std::vector<audit_log_entry> ordinary_export_access::upload_query(database& writer,bool only_unsynced,
    const std::function<std::vector<audit_log_entry>()>& body,bool exact_originals) {
    // The caller retains this exact physical wrapper (and its parent, if any)
    // through this synchronous call. No borrowed pointer/callback escapes.
    std::vector<audit_log_entry> result;std::exception_ptr failure;bool delivery_owned=false;
    try {
        auto* handle=ordinary_export_storage_access::borrow(writer);auto* mutex=handle?sqlite3_db_mutex(handle):nullptr;
#ifndef __EMSCRIPTEN__
        if(!mutex)refuse("upload requires a serialized connection");
#endif
        if(!handle||!body||database::engine_query_scope::active_for(writer))refuse("upload query reentry or unavailable writer");
        if(sqlite3_mutex_try(mutex)!=SQLITE_OK)refuse("upload writer busy");
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        if(writer.is_closed()||writer.channel_reset_unsettled_.load(std::memory_order_acquire)||
           database::update_hook_scope::active_for(handle))refuse("upload writer closed or unsettled");
        auto* hook=writer.lattice_update_hook_context_.get();
        if(hook&&(hook->connection!=handle||hook->sync_chunk))refuse("upload refuses an active foreign engine turn");
        auto* owner=hook?hook->owner:nullptr;
        if(owner) {
            const auto thread=owner->txn_owner_thread_.load(std::memory_order_acquire);
            if(owner->closed_.load()||(thread!=std::thread::id{}&&thread!=std::this_thread::get_id()))
                refuse("upload refuses foreign transaction custody");
        }
        for(auto* statement=sqlite3_next_stmt(handle,nullptr);statement;statement=sqlite3_next_stmt(handle,statement))
            if(sqlite3_stmt_busy(statement)&&!sqlite3_stmt_readonly(statement))refuse("upload cannot reenter an active mutation");
        const int autocommit=sqlite3_get_autocommit(handle);
        const auto changes=sqlite3_total_changes64(handle);
        // COUNT always produces one ROW, including genuine absence. Retaining
        // that main-schema read cursor pins the actual SQLite snapshot across
        // validation, enumeration and late binding. No transaction is begun,
        // upgraded or settled here, including a caller's explicit READ/WRITE.
        static constexpr const char* presence_sql="SELECT COUNT(*) FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_receiver*' OR tbl_name GLOB '_lattice_ordered_receiver*' OR name GLOB '_lattice_ordered_helper*' OR tbl_name GLOB '_lattice_ordered_helper*' OR name GLOB '_lattice_ordered_wire*' OR tbl_name GLOB '_lattice_ordered_wire*'";
        database::engine_query_scope pin(writer,true);delivery_owned=true;sqlite3_stmt* raw=nullptr;
        database::record_statement();const int prepared=sqlite3_prepare_v2(handle,presence_sql,-1,&raw,nullptr);pin.adopt(raw);
        if(prepared!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw)||sqlite3_step(raw)!=SQLITE_ROW||
           sqlite3_column_type(raw,0)!=SQLITE_INTEGER)refuse("upload could not pin its actual main read view");
        const auto presence=sqlite3_column_int64(raw,0);
        const int transaction=sqlite3_txn_state(handle,"main");
        if(presence<0||(transaction!=SQLITE_TXN_READ&&transaction!=SQLITE_TXN_WRITE))refuse("upload main read view unavailable");
        std::set<std::string> covered_tables;std::map<std::string,std::string> table_scopes;
        const auto validate=[&] {
            budget retained{helper_limits(handle)};covered_tables.clear();table_scopes.clear();
            if(!helper_storage(handle,retained))refuse("ordered receiver requires issued cuts; legacy export/apply is unavailable");
            if(!only_unsynced)refuse("helper receiver cannot impersonate its canonical server export");
            if(!owner)refuse("helper upload requires its actual writer owner");
            auto bindings=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 ORDER BY scope",{},maximum_profiles,retained);
            if(bindings.empty())refuse("helper upload has no source scope");
            for(const auto& binding:bindings) {
                // Validate catalog and actual programs without rescanning model
                // values/AuditLog. Durable frontier/receipt/baseline validation
                // remains exact; this is not a cached metadata authority.
                const auto image=snapshot(*owner,writer,scope_names(text(binding[0]),retained.limits.models),false);
                verify_helper_binding(writer,binding,image,retained);
                for(const auto& table:image.tables){retained.add(3*sizeof(std::string)+2*table.name.size()+text(binding[0]).size()+4*sizeof(void*));covered_tables.insert(table.name);
                    if(!table_scopes.emplace(table.name,text(binding[0])).second)refuse("overlapping upload scope");}
            }
            return bindings;
        };
        const auto bindings=presence?validate():rows{};
        result=body();
        if(exact_originals) {
            for(auto& entry:result)entry.ordinary_delivery_version="0";
            if(!owner||!owner->recovery_schemas_.valid())refuse("exact upload needs actual owner catalog");
            budget retained{wire_limits};
            if(presence) {
                ordinary_wire_original_turn::state state{writer,retained};ordinary_wire_original_turn turn{state};
                for(auto& entry:result){const auto scope=table_scopes.find(entry.table_name);if(scope==table_scopes.end())refuse("exact upload escaped actual source scope");
                    hydrate_canonical_original(turn,scope->second,entry,owner->recovery_schemas_,retained,true);}
                turn.finish();
            }
            if(ingress_storage(handle,retained)) {
                const auto scopes=read(handle,"SELECT scope FROM main._lattice_ordered_ingress_v2 ORDER BY scope",{},maximum_profiles,retained);
                const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("exact source upload physical identity unavailable");
                for(const auto& selected:scopes) {
                    ordinary_export_frame image;image.wire_profile=true;image.device=std::to_string(physical->device);image.inode=std::to_string(physical->inode);
                    capture_projected_tables(handle,owner->recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},scope_names(text(selected[0]),wire_limits.models),image,retained,false,nullptr,nullptr);
                    if(!validate_source_upload_capture(handle,image,retained))refuse("exact source upload ingress disappeared");
                    for(auto& entry:result)if(std::any_of(image.tables.begin(),image.tables.end(),[&](const auto& table){return table.name==entry.table_name;}))hydrate_source_upload(handle,image,entry,retained);
                    if(!validate_source_upload_capture(handle,image,retained))refuse("exact source upload ingress changed");
                }
                const auto final_physical=writer.physical_identity("main",{},true);if(!final_physical||*physical!=*final_physical)refuse("exact source upload physical identity changed");
            }
            for(const auto& entry:result)if(!entry.upload_original)refuse("remote original membership unknown; explicit bootstrap required");
            // Reproject actual NoHistory UPDATE columns even when hydration
            // restored an earlier non-null projected value. The legacy helper
            // fills absent values only and converts query errors into absence;
            // neither behavior establishes this required-mode observation.
            // The real pinned view remains live and read failures refuse it.
            for(auto& entry:result) {
                if(entry.operation!="UPDATE")continue;
                const auto* model=owner->recovery_schemas_.find(entry.table_name);if(!model)continue;
                std::vector<std::string_view> late;retained.add(model->properties.size()*sizeof(std::string_view));late.reserve(model->properties.size());
                for(const auto& property:model->properties)if(property.no_history&&property.kind==property_kind::primitive&&
                    std::find(entry.upload_original->names.begin(),entry.upload_original->names.end(),property.name)!=entry.upload_original->names.end())late.push_back(property.name);
                if(late.empty())continue;
                size_t select_bytes=64+2*entry.table_name.size();for(const auto name:late)select_bytes+=2*name.size()+4;
                retained.add(select_bytes);std::string selected;selected.reserve(select_bytes);
                for(const auto name:late){if(!selected.empty())selected+=',';selected+=quote(std::string(name));}
                const auto live=read(handle,"SELECT "+selected+" FROM main."+quote(entry.table_name)+" WHERE globalId=?",{entry.global_row_id},1,retained);
                for(size_t n=0;n<late.size();++n) {
                    retained.add(late[n].size()+1+sizeof(any_property)+4*sizeof(void*));const std::string name(late[n]);
                    if(live.empty()) {
                        entry.changed_fields.erase(name);
                        entry.changed_fields_names.erase(std::remove(entry.changed_fields_names.begin(),entry.changed_fields_names.end(),name),entry.changed_fields_names.end());
                    } else {
                        const auto& value=live[0][n];
                        if(const auto* text=std::get_if<std::string>(&value))retained.add(text->size());
                        if(const auto* bytes=std::get_if<std::vector<uint8_t>>(&value))retained.add(bytes->size());
                        entry.changed_fields[name]=any_property::from_column_value(value);
                    }
                }
            }
        }
        // These are refusal checks, not proof of snapshot or transaction
        // ownership. Trusted engine callbacks must never settle/reopen the
        // borrowed caller turn, even when a read-only COMMIT has no hook.
        if(writer.is_closed()||(owner&&owner->closed_.load())||ordinary_export_storage_access::borrow(writer)!=handle||
           writer.lattice_update_hook_context_.get()!=hook||sqlite3_get_autocommit(handle)!=autocommit||
           sqlite3_txn_state(handle,"main")!=transaction||sqlite3_total_changes64(handle)!=changes||
           !sqlite3_stmt_busy(raw))refuse("upload read view changed during enumeration");
        budget final_budget;const auto final_presence=read(handle,presence_sql,{},1,final_budget);
        if(final_presence.size()!=1||integer(final_presence[0][0])!=presence)refuse("upload enrollment changed during enumeration");
        if(presence) {
            if(validate()!=bindings)refuse("upload source binding changed during enumeration");
            for(const auto& entry:result) {
                if(!covered_tables.count(entry.table_name))refuse("provisional upload escaped its issued scope");
            }
        }
        // pin is finalized before release unlocks. Pending observer delivery
        // stays owned by the writer until the explicit off-lock tail below.
    } catch(...) {failure=std::current_exception();}
    if(delivery_owned)try {writer.drain_if_settled();}catch(...) {if(!failure)failure=std::current_exception();}
    if(failure)std::rethrow_exception(failure);
    return result;
}

server_export_batch ordinary_export_access::events(database& writer,const std::optional<std::string>& checkpoint) {
    auto value=std::make_shared<ordinary_export_batch>();
    auto* hook=writer.lattice_update_hook_context_.get();
    if(hook&&hook->owner&&hook->connection==ordinary_export_storage_access::borrow(writer))value->complete=capture_impl(*hook->owner,{},&writer,true);
    if(!value->complete){value->entries=events_after_provisional(writer,checkpoint);return server_export_batch{std::move(value)};}
    const auto& all=value->complete->entries();size_t start=0;
    if(checkpoint){const auto position=std::find_if(all.begin(),all.end(),[&](const auto& entry){return entry.global_id==*checkpoint;});if(position!=all.end())start=static_cast<size_t>(position-all.begin())+1;}
    const bool wire_profile=value->complete->frame_->wire_profile;budget retained{wire_profile?wire_limits:legacy_limits};
    for(size_t n=start;n<all.size();++n)(void)encode_entry(all[n],retained,wire_profile); // Charge copied payloads before the retained view allocation.
    value->entries.assign(all.begin()+static_cast<ptrdiff_t>(start),all.end());return server_export_batch{std::move(value)};
}
std::vector<std::string> ordinary_export_access::apply_helper(lattice_db& owner,const server_export_batch& batch,const std::optional<std::string>& channel) {
    const auto value=batch.value_;if(!value)refuse("moved server helper batch");
    if(!value->complete)return channel?apply_remote_changes_for(owner,value->entries,*channel):apply_remote_changes(owner,value->entries);
    const auto all=apply_impl(owner,*value->complete,channel,true);if(all.empty()&&!value->complete->entries().empty())return {};
    std::vector<std::string> accepted;accepted.reserve(value->entries.size());for(const auto& entry:value->entries)accepted.push_back(entry.global_id);return accepted;
}

owned_server_export ordinary_export_access::capture(lattice_db& owner,const std::vector<std::string>& requested) {
    auto result=capture_impl(owner,requested,nullptr,false);if(!result)refuse("explicit capture was not issued");return std::move(*result);
}
owned_server_export ordinary_export_access::capture_wire(lattice_db& owner,const std::vector<std::string>& requested) {
    if(!owner.recovery_schemas_.valid()||requested.size()>wire_limits.models)refuse("wire capture owner catalog or scope unavailable");
    if(!requested.empty()) {
        auto result=capture_impl(owner,requested,nullptr,false,true);
        if(!result)refuse("wire capture was not issued");return std::move(*result);
    }
    std::vector<std::string> names;
    names.reserve(owner.recovery_schemas_.models.size());
    for(const auto& [name,unused]:owner.recovery_schemas_.models){(void)quote(name);names.push_back(name);}
    auto result=capture_impl(owner,names,nullptr,false,true);
    if(!result)refuse("wire capture was not issued");return std::move(*result);
}
std::vector<std::string> ordinary_export_access::observation_tables(lattice_db& owner,const std::vector<std::string>& requested) {
    if(!owner.recovery_schemas_.valid()||requested.size()>recovery_owner_schema::max_models)
        refuse("observation catalog or scope unavailable");
    std::vector<std::string> names;
    if(requested.empty()) {
        names.reserve(owner.recovery_schemas_.models.size());
        for(const auto& [name,unused]:owner.recovery_schemas_.models){(void)quote(name);names.push_back(name);}
    } else {
        names.reserve(requested.size());
        for(const auto& name:requested){(void)quote(name);names.push_back(name);}
    }
    if(names.empty())refuse("observation requires actual declared models");
    std::sort(names.begin(),names.end());
    if(std::adjacent_find(names.begin(),names.end())!=names.end())refuse("duplicate observation model");
    std::shared_ptr<database> writer;
    {std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||!owner.db_)refuse("observation owner closed");writer=owner.db_;}
    std::vector<std::string> result;std::exception_ptr failure;bool delivery_owned=false;
    try {
        auto* handle=ordinary_export_storage_access::borrow(*writer);auto* mutex=handle?sqlite3_db_mutex(handle):nullptr;
#ifndef __EMSCRIPTEN__
        if(!mutex)refuse("observation requires a serialized connection");
#endif
        if(!handle||database::engine_query_scope::active_for(*writer))refuse("observation query reentry or unavailable writer");
        if(sqlite3_mutex_try(mutex)!=SQLITE_OK)refuse("observation writer busy");
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        if(writer->is_closed()||writer->channel_reset_unsettled_.load(std::memory_order_acquire)||
           database::update_hook_scope::active_for(handle))refuse("observation writer closed or unsettled");
        auto* hook=writer->lattice_update_hook_context_.get();
        const auto thread=owner.txn_owner_thread_.load(std::memory_order_acquire);
        if(!hook||hook->owner!=&owner||hook->connection!=handle||hook->sync_chunk||owner.closed_.load()||
           (thread!=std::thread::id{}&&thread!=std::this_thread::get_id()))refuse("observation refuses foreign transaction custody");
        for(auto* statement=sqlite3_next_stmt(handle,nullptr);statement;statement=sqlite3_next_stmt(handle,statement))
            if(sqlite3_stmt_busy(statement)&&!sqlite3_stmt_readonly(statement))refuse("observation cannot reenter an active mutation");
        const int autocommit=sqlite3_get_autocommit(handle);const auto changes=sqlite3_total_changes64(handle);
        // Retain a real main-schema ROW across every metadata lookup. This
        // does not begin, upgrade, commit or roll back the caller's turn.
        database::engine_query_scope pin(*writer,true);delivery_owned=true;sqlite3_stmt* raw=nullptr;
        database::record_statement();const int prepared=sqlite3_prepare_v2(handle,"SELECT COUNT(*) FROM main.sqlite_master",-1,&raw,nullptr);pin.adopt(raw);
        if(prepared!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw)||sqlite3_step(raw)!=SQLITE_ROW||
           sqlite3_column_type(raw,0)!=SQLITE_INTEGER)refuse("observation could not pin its actual main read view");
        const int transaction=sqlite3_txn_state(handle,"main");
        if(transaction!=SQLITE_TXN_READ&&transaction!=SQLITE_TXN_WRITE)refuse("observation main read view unavailable");
        budget retained{wire_limits};
        try {
            const auto projection=describe_ordinary_projection(owner.recovery_schemas_,names);
            retained.add(projection.logical_bytes);
            // Metadata only: main values, AuditLog and source/receiver ledgers
            // are not read or mutated. The validator preflights owned module
            // definitions before targeted metadata can connect those modules.
            (void)validate_ordinary_schema(projection,[&](const auto& sql,const auto& args,size_t limit){
                return read_metadata(handle,sql,args,limit,retained);
            });
            retained.add(projection.tables.size()*sizeof(std::string));result.reserve(projection.tables.size());
            for(const auto& [name,unused]:projection.tables){retained.add(name.size());result.push_back(name);}
        } catch(const std::runtime_error& error) {
            throw db_error(std::string("ordinary observation schema: ")+error.what());
        }
        if(writer->is_closed()||owner.closed_.load()||ordinary_export_storage_access::borrow(*writer)!=handle||
           writer->lattice_update_hook_context_.get()!=hook||sqlite3_get_autocommit(handle)!=autocommit||
           sqlite3_txn_state(handle,"main")!=transaction||sqlite3_total_changes64(handle)!=changes||!sqlite3_stmt_busy(raw))
            refuse("observation read view changed during selection");
    } catch(...) {failure=std::current_exception();}
    if(delivery_owned)try {writer->drain_if_settled();}catch(...) {if(!failure)failure=std::current_exception();}
    if(failure)std::rethrow_exception(failure);
    return result;
}
#include "ordinary_source_upload_storage.inc"

recovery_owner_schema ordinary_export_access::managed_clone_catalog(lattice_db& parent,const configuration& config) {
    if(parent.is_closed() || !parent.recovery_schemas_.valid() || !config.ordinary_context ||
        config.ordinary_context!=parent.config_.ordinary_context || config.path!=parent.config_.path ||
        config.is_sync_enabled() || config.is_ipc_enabled() || config.is_in_memory() ||
        config.read_only!=parent.config_.read_only || config.audit_retention_seconds!=0)
        refuse("managed attachment clone requires its actual parent configuration");
    const auto settled=recovery_writer_access::ordinary_owned_write(parent,[&](database& writer){
        const auto profile=parent.ordinary_managed_profile_.load(std::memory_order_acquire);
        if(!profile)refuse("managed attachment clone parent is not enrolled");
        profile->validate(parent,writer);
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    if(parent.is_closed())refuse("managed attachment clone parent closed during admission");
    // Copied from actual immutable native declarations, never caller schemas.
    return parent.recovery_schemas_;
}
void ordinary_export_access::validate_managed_reference(lattice_db& owner) {
    if(!owner.config_.ordinary_context||owner.is_closed())refuse("managed reference actual owner unavailable");
    // Actual creator-process verification precedes the writer/engine leaves.
    // Cohort/physical/catalog checks run once in the owned view below; do not
    // repeat the full image/record census before entering that same turn.
    // A copied ordinary-context field alone has no profile to validate below.
    const bool historical=owner.config_.ordinary_context->is_managed_history_view();
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        const auto retained=owner.ordinary_managed_profile_.load(std::memory_order_acquire);
        if(historical){
            if(!retained)refuse("managed reference actual owner is not enrolled");
            retained->validate(owner,writer);
        }else{
            // Existing normal primary startup has only its actual ordinary
            // open capability. Retain that behavior without promoting it to
            // a historical profile or allowing attachment/source authority.
            if(retained)refuse("normal ordinary reference has unexpected history profile");
            const auto physical=writer.physical_identity("main",{},true);
            if(!physical)refuse("normal ordinary reference physical owner unavailable");
            owner.config_.ordinary_context->validate_physical(physical->device,physical->inode);
        }
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    if(owner.is_closed())refuse("managed reference actual owner closed during validation");
}
void ordinary_export_access::admit_managed_clone(lattice_db& parent,lattice_db& child) {
    if(&parent==&child || parent.is_closed() || child.is_closed() ||
        child.config_.ordinary_context!=parent.config_.ordinary_context ||
        child.config_.path!=parent.config_.path || child.config_.is_sync_enabled() || child.config_.is_ipc_enabled() ||
        child.ordinary_managed_profile_.load(std::memory_order_acquire))
        refuse("managed attachment clone publication refused");
    std::shared_ptr<const ordinary_managed_profile> retained;
    const auto settled=recovery_writer_access::ordinary_owned_write(parent,[&](database& writer){
        retained=parent.ordinary_managed_profile_.load(std::memory_order_acquire);
        if(!retained)refuse("managed attachment clone lost enrolled parent");
        retained->validate(parent,writer);
        const auto expected=writer.physical_identity("main",{},true);
        const auto actual=child.db_->physical_identity("main",{},true);
        if(!expected||!actual||*expected!=*actual)refuse("managed attachment clone opened a different physical file");
        if(child.read_db_){const auto reader=child.read_db_->physical_identity("main",{},true);
            if(!reader||*reader!=*expected)refuse("managed attachment clone reader opened a different physical file");}
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    if(parent.is_closed()||child.is_closed())refuse("managed attachment clone owner closed before publication");
    child.ordinary_managed_profile_.store(std::move(retained),std::memory_order_release);
}
void ordinary_export_access::note_history_uncaptured(lattice_db& owner) {
    owner.require_creator_process();
    // Native admission itself is now reserved by each actual factory. This
    // compatibility entry is not a taint flag, current-cut or close grant.
}
bool ordinary_export_access::fresh_origin(const lattice_db& owner,const physical_store_identity& physical) {
    return owner.ordinary_schema_initialized_here_||
        (owner.ordinary_child_origin_&&owner.ordinary_child_origin_->physical_==physical);
}
std::unique_ptr<lattice_db> ordinary_export_access::child(lattice_db& parent,const configuration& config,
    std::shared_ptr<ordinary_history_child_connections>* history_custody) {
    if(parent.is_closed()||!parent.recovery_schemas_.valid())refuse("ordinary child parent/catalog unavailable");
    // The immutable catalog is the actual parent's declaration snapshot. The
    // unexposed child does not synthesize Swift declarations from physical SQL.
    std::unique_ptr<lattice_db> child;
    if(history_custody){
        auto* actual=new ordinary_history_child_connections();
        auto custody=std::shared_ptr<ordinary_history_child_connections>(actual,&ordinary_history_child_connections::dispose);
        *history_custody=custody; // Real prefix owns every record BEFORE first native constructor entry.
        child.reset(new lattice_db(config,true,parent.recovery_schemas_,{},custody));
        child->require_creator_process();
        child->ordinary_history_child_connections_.store(std::move(custody),std::memory_order_seq_cst);
    }else child.reset(new lattice_db(config,true,parent.recovery_schemas_));
    // The real unpublished child exists; no parent write or SQLite ownership
    // leaf is held. The copied test rendezvous cannot escape this borrow.
    auto child_opened=ordinary_wire_test_hooks::after_child_open;
    if(child_opened)child_opened(*child);
    std::shared_ptr<const ordinary_child_origin> origin;
    std::shared_ptr<const ordinary_managed_profile> managed;
    std::shared_ptr<const ordinary_managed_receiver_preservation> preserved;
    const auto settled=recovery_writer_access::ordinary_owned_write(parent,[&](database& writer){
        const auto physical=writer.physical_identity("main",{},true);
        const auto child_physical=child->db_->physical_identity("main",{},true);
        if(!physical||!child_physical||*physical!=*child_physical||parent.is_closed())refuse("ordinary child no longer shares its actual live parent image");
        if(fresh_origin(parent,*physical))origin=std::shared_ptr<const ordinary_child_origin>(new ordinary_child_origin(*physical));
        managed=parent.ordinary_managed_profile_.load(std::memory_order_acquire);
        if(managed)managed->validate(parent,writer);
        preserved=parent.ordinary_receiver_preservation_.snapshot().lock();
        if(preserved){if(config.ordinary_context!=parent.config_.ordinary_context||config.path!=parent.config_.path||config.is_sync_enabled()||config.is_ipc_enabled()||config.read_only||config.audit_retention_seconds!=0)refuse("restart receiver child configuration differs");validate_receiver_clone_admission(*preserved,parent,writer);}
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    if(parent.is_closed())refuse("ordinary child parent closed during admission delivery");
    // Publish after verified own COMMIT and off-lock delivery. A historical
    // parent has no origin receipt; its child can only use a previously proved
    // durable binding. This receipt never grants source or remote authority.
    child->ordinary_child_origin_=std::move(origin);
    child->ordinary_managed_profile_.store(std::move(managed),std::memory_order_release);
    if(preserved){validate_receiver_clone_publication(*preserved,parent,*child);child->ordinary_receiver_preservation_.publish(preserved);}
    return child;
}
std::optional<owned_server_export> ordinary_export_access::capture_impl(lattice_db& owner,const std::vector<std::string>& requested,database* expected,bool automatic,bool wire) {
    const auto& limits=wire?wire_limits:legacy_limits;
    if((requested.empty()&&!automatic)||requested.size()>limits.models)refuse("model scope bound exceeded");
    // Copy caller-selected names before any callback or transaction can run.
    std::vector<std::string> names;names.reserve(requested.size());
    for(const auto& name:requested){(void)quote(name);names.push_back(name);}
    std::sort(names.begin(),names.end());
    if(std::adjacent_find(names.begin(),names.end())!=names.end())refuse("duplicate scope model");
    auto captured=std::make_shared<ordinary_export_frame>();captured->wire_profile=wire;
    bool issued=false;
    const auto settlement=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer) {
        if(expected&&expected!=&writer)refuse("helper no longer owns its borrowed physical writer");
        require_recovery_local_producer_maintenance_absent(writer);
        require_continuous_legacy_export_absent(writer);
        require_ordered_receiver_absent(writer);
        if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_)
            refuse("owner catalog or ordinary route unavailable");
        auto* handle=ordinary_export_storage_access::borrow(writer);budget retained{limits};
        require_managed_profile_if_present(owner,writer);
        const bool enrolled=inspect_storage(handle,retained);
        if(automatic) {
            if(!enrolled&&!owner.ordinary_schema_initialized_here_)return; // Explicitly provisional historical helper.
            const auto profiles=enrolled?read(handle,"SELECT scope FROM main._lattice_ordered_source_v1 ORDER BY scope",{},maximum_profiles,retained):rows{};
            if(profiles.size()>1)refuse("automatic helper requires one explicit source scope");
            if(!profiles.empty())names=scope_names(text(profiles[0][0]));
            else {
                const auto models=read(handle,"SELECT DISTINCT tableName FROM main.AuditLog ORDER BY tableName",{},maximum_models,retained);
                for(const auto& model:models)names.push_back(text(model[0]));
            }
            bool supported=!names.empty();
            for(const auto& name:names) {
                const auto* model=owner.recovery_schemas_.find(name);
                if(!model||name.empty()||name.front()=='_'||owner.recovery_schemas_.swift_models.count(name)||model->properties.size()>maximum_columns){supported=false;break;}
                for(const auto& p:model->properties)if(p.kind!=property_kind::primitive||p.is_vector||p.is_geo_bounds||p.is_union||p.is_full_text||p.no_history||(!p.column_name.empty()&&p.column_name!=p.name))supported=false;
            }
            if(!supported){if(enrolled)refuse("enrolled helper requires explicit model/schema transition");return;}
            const auto actual=read(handle,"SELECT DISTINCT tableName FROM main.AuditLog ORDER BY tableName",{},maximum_models,retained);
            for(const auto& model:actual)if(!std::binary_search(names.begin(),names.end(),text(model[0])))refuse("helper history gained a model outside its issued scope");
        }
        const auto physical=writer.physical_identity("main",{},true);
        if(!physical){if(automatic&&!enrolled&&owner.config_.is_in_memory())return;refuse("current filesystem identity unavailable");}
        captured->device=std::to_string(physical->device);captured->inode=std::to_string(physical->inode);
        capture_tables(handle,owner.recovery_schemas_,[](auto& stream,const auto& property){
            lattice_db::serialize_property_for_fingerprint(stream,property);
        },[&](const model_schema& declaration){
            std::vector<std::pair<std::string,column_type>> columns;std::set<std::string> no_history;
            for(const auto& property:declaration.properties){columns.emplace_back(property.name,property.type);if(property.no_history)no_history.insert(property.name);}
            std::vector<std::string> programs;owner.create_model_table_triggers(declaration.table_name,columns,no_history,{},&programs);return programs;
        },names,*captured,retained);
        capture_entries(handle,*captured,owner.recovery_schemas_,retained);
        // Exact source issuance may not promote normalized legacy remote rows.
        // Hydrated ingress (and the composed canonical mapping path) must supply
        // validated raw membership. Internal receiver before/after snapshots
        // remain separate; historical unknown-custody bootstrap is unfinished.
        if(wire)for(const auto& entry:captured->entries)
            if(entry.is_from_remote&&!entry.upload_original)
                refuse("remote source original custody is unknown; explicit bootstrap required");
        register_mutation_guard(handle);
        mutation_scope permit(handle);
        if(!enrolled) {
            if(!fresh_origin(owner,*physical)||owner.ordinary_export_enrolled_here_.load(std::memory_order_acquire))refuse("historical origin requires actual managed admission");
            writer.execute(source_ddl);
            for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(guard_ddl(op));
            if(!inspect_storage(handle,retained))refuse("source storage did not install");
        }
        const auto profiles=read(handle,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 ORDER BY scope",{},maximum_profiles,retained);
        const values* current=nullptr;
        for(const auto& profile:profiles) {
            if(text(profile[4])!=captured->device||text(profile[5])!=captured->inode||!canonical_uuid(text(profile[1]))||integer(profile[2])!=1||integer(profile[3])<0)
                refuse("durable source identity changed");
            if(text(profile[0])==captured->scope)current=&profile;
        }
        if(current) {
            if(text((*current)[6])!=captured->schema)refuse("source schema changed; explicit migration required");
            captured->source=text((*current)[1]);captured->epoch=integer((*current)[2]);
            const auto prior=integer((*current)[3]);if(prior==std::numeric_limits<int64_t>::max())refuse("source cut exhausted");captured->cut=prior+1;
            writer.execute("UPDATE main._lattice_ordered_source_v1 SET cut=? WHERE scope=? AND cut=?",{captured->cut,captured->scope,prior});
        } else {
            if(profiles.size()==maximum_profiles)refuse("durable scope capacity exhausted");
            captured->source=uuid_t::generate().to_string();captured->epoch=1;captured->cut=1;
            writer.execute("INSERT INTO main._lattice_ordered_source_v1 VALUES(?,?,?,?,?,?,?)",
                {captured->scope,captured->source,captured->epoch,captured->cut,captured->device,captured->inode,captured->schema});
        }
        if(writer.changes()!=1)refuse("source cut publication was ignored");
        const auto stored=read(handle,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{captured->scope},1,retained);
        const values expected{captured->scope,captured->source,captured->epoch,captured->cut,captured->device,captured->inode,captured->schema};
        if(stored.size()!=1||stored[0]!=expected)refuse("source cut postimage changed");
        if(wire) {
            for(auto& original:captured->originals){retained.add(64);original.digest=bind_original_digest(*captured,original.digest);}
            validate_original_descriptors(*captured,owner.recovery_schemas_,retained);
        }
        issued=true;
        // No model mutation, audit ACK, cursor advance or receipt is inferred.
        // The owned helper must return from its actual COMMIT before this frame
        // can become a public immutable value. Failure discards this candidate.
    },[&]{if(issued)owner.ordinary_export_enrolled_here_.store(true,std::memory_order_release);});
    if(settlement.state!=recovery_install_state::committed||settlement.primary_error||settlement.cleanup_error||settlement.postcommit_error||settlement.notification_error)
        throw ordinary_order_write_error(settlement);
    if(!issued)return std::nullopt;return owned_server_export{std::move(captured)};
}
#include "ordinary_history_stage.inc"
#include "ordinary_managed_profile.inc"

void ordinary_export_access::validate_checked_open_admission(const ordinary_context& context,
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const std::string& path) {
    ordinary_require_creator(context);
    if(!context||!context->matches_managed_open_admission(admission,path))
        refuse("checked existing open admission differs from actual retained context");
}

void ordinary_export_access::claim_managed_source_open(
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const std::string& path) {
    if(!admission)refuse("managed source opening admission unavailable");
    admission->claim_opening_until(path,ordinary_managed_child_admission::opening_role::source,
        std::chrono::steady_clock::now()+std::chrono::seconds(10));
}
void ordinary_export_access::remember_managed_open_failure(
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const std::string& path,std::exception_ptr error) noexcept {
    if(admission)admission->remember_opening_failure(path,std::move(error));
}
void ordinary_export_access::claim_managed_group_open(const ordinary_context& context,
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const std::string& path,
    std::uint64_t device,std::uint64_t inode,const recovery_owner_schema& catalog,ordinary_launch::deadline end) {
    validate_checked_open_admission(context,admission,path);
    if(!admission||!catalog.valid()||catalog.swift_models!=std::set<std::string>{"Edge","GroupProjectMap","Memory"}||
       catalog.swift_fingerprint.empty())refuse("dormant group requires its exact native and Swift root catalog");
    const auto member=admission->ipc_member_binding(path,device,inode,end).first;
    if(!member.starts_with("sync/group-")||!member.ends_with(".sqlite")||member.size()!=54)
        refuse("dormant group requires an actual registered GROUP main");
    auto group=member.substr(11,36);for(auto& c:group)if(c>='A'&&c<='F')c+=('a'-'A');
    if(!canonical_uuid(group))refuse("dormant group registered GROUP spelling refused");
    admission->claim_opening_until(path,ordinary_managed_child_admission::opening_role::group_receiver,end);
}
void ordinary_export_access::validate_managed_group_open(const ordinary_context& context,
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const std::string& path,
    std::uint64_t device,std::uint64_t inode,ordinary_launch::deadline end) {
    validate_checked_open_admission(context,admission,path);
    if(!admission)refuse("dormant group original admission unavailable");
    admission->require_group_opening_until(path,end);
    context->validate_managed_attachment(admission,path,device,inode,end);
}
void ordinary_export_access::validate_checked_managed_schema(lattice_db& owner,checked_reference_phase phase,
    const std::shared_ptr<const ordinary_managed_child_admission>& bounded_admission,ordinary_launch::deadline end) {
    owner.require_creator_process();
    const auto context=owner.config_.ordinary_context;
    if(!context||owner.is_closed()||!owner.recovery_schemas_.valid()||owner.config_.target_schema_version<=0)
        refuse("checked schema actual owner unavailable");
    ordinary_require_creator(context);
    if(!bounded_admission)ordinary_before_open(owner.config_.path,context);
    const bool historical=context->is_managed_history_view();
    const auto profile=owner.ordinary_managed_profile_.load(std::memory_order_acquire);
    if(phase==checked_reference_phase::enrolled&&historical&&!profile)
        refuse("checked historical reference lacks actual post-COMMIT enrollment");
    if(!historical&&profile)refuse("checked normal reference has unexpected history profile");
    if(bounded_admission&&(!historical||phase!=checked_reference_phase::before_enrollment||profile))
        refuse("dormant group rejects any published producer profile");
    std::shared_ptr<database> writer;uint64_t revision;
    {std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load()||!owner.db_)refuse("checked schema owner closed");
        writer=owner.db_;revision=owner.connection_revision_;}
    const auto physical=writer->physical_identity("main",{},true);
    if(!physical)refuse("checked schema actual physical file unavailable");
    if(bounded_admission)validate_managed_group_open(context,bounded_admission,owner.config_.path,physical->device,physical->inode,end);
    else context->validate_physical(physical->device,physical->inode);
    std::string generation;
    if(profile){const auto& state=profile->impl_;
        if(!historical||!state||!state->admission||state->path!=owner.config_.path||*physical!=state->physical||
           !context->matches_managed_open_admission(state->admission,state->path))
            refuse("checked history retained profile differs from actual owner");
        // Actual admission/current generation checks run off native leaves.
        generation=state->admission->enrollment_binding(state->path,physical->device,physical->inode);
    }
    // Actual existing shared four-export capacity is charged before the
    // closed metadata capture allocates; validation issues no source/cut.
    ordinary_resource_reservation<false> metadata_capacity;
    const auto core=owner.compute_core_fingerprint_key();
    const auto swift=owner.recovery_schemas_.swift_fingerprint;
    if(swift.empty())refuse("checked schema Swift fingerprint unavailable");
    std::vector<std::string> names;for(const auto& [name,unused]:owner.recovery_schemas_.models)names.push_back(name);
    std::exception_ptr primary,cleanup;bool rollback_uncertain=false;
    try {
        database::maintenance_scope::admission admission;
        database::maintenance_scope turn(*writer,&admission);
        if(admission!=database::maintenance_scope::admission::ready)refuse("checked schema native writer busy");
        auto* handle=borrowed_storage_handle(*writer);
        if(!handle||writer->is_closed()||owner.is_closed()||writer->channel_reset_unsettled_.load()||
           sqlite3_get_autocommit(handle)==0)refuse("checked schema refuses caller transaction or retired writer");
        const auto changes=sqlite3_total_changes64(handle);
        bool begun=false;
        try {
            if(bounded_admission&&std::chrono::steady_clock::now()>=end)refuse("dormant group original deadline expired before READ");
            writer->execute("BEGIN");begun=true;
            budget retained{wire_limits};
            // BEGIN is deferred: pin the actual main btree before entering the
            // closed metadata adapter, which requires an already owned READ.
            (void)read(handle,"SELECT 1 FROM main.sqlite_master LIMIT 1",{},1,retained);
            if(sqlite3_txn_state(handle,"main")!=SQLITE_TXN_READ)
                refuse("checked schema main READ was not pinned");
            const auto version=read_metadata(handle,"SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS version FROM main._lattice_meta WHERE key='schema_version' COLLATE BINARY LIMIT 2",{},2,retained);
            if(version.size()!=1||!std::holds_alternative<std::string>(version[0].at("version"))||
               std::get<std::string>(version[0].at("version"))!=std::to_string(owner.config_.target_schema_version))
                refuse("checked schema actual positive version differs; no migration");
            const auto cookie_rows=read_metadata(handle,"PRAGMA main.schema_version",{},1,retained);
            if(cookie_rows.size()!=1||!std::holds_alternative<int64_t>(cookie_rows[0].at("schema_version")))
                refuse("checked schema actual cookie unavailable");
            const auto cookie=std::get<int64_t>(cookie_rows[0].at("schema_version"));
            if(cookie<0||cookie>INT32_MAX)refuse("checked schema canonical cookie refused");
            for(const auto* key:{&core,&swift}){
                const auto marker=read_metadata(handle,"SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS cookie FROM main._lattice_meta WHERE key=? COLLATE BINARY LIMIT 2",{*key},2,retained);
                if(marker.size()!=1||!std::holds_alternative<std::string>(marker[0].at("cookie"))||
                   std::get<std::string>(marker[0].at("cookie"))!=std::to_string(cookie))
                    refuse("checked schema fingerprint/cookie differs; no repair");
            }
            ordinary_export_frame observed;observed.wire_profile=true;
            capture_projected_tables(handle,owner.recovery_schemas_,[](auto& out,const auto& property){
                lattice_db::serialize_property_for_fingerprint(out,property);},names,observed,retained,false,nullptr,nullptr);
            if(profile){const auto& state=profile->impl_;
                if(!inspect_storage(handle,retained)||!managed_profile_storage(handle,retained)||
                   managed_profile_row(*writer,text(state->record[0]),retained)!=state->record||
                   generation!=text(state->record[6]))refuse("checked retained history enrollment changed");
                const auto binding=read(handle,"SELECT source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{state->record[0]},1,retained);
                if(binding.size()!=1||text(binding[0][0])!=text(state->record[1])||integer(binding[0][1])!=integer(state->record[2])||
                   integer(binding[0][2])<0||binding[0][3]!=state->record[4]||binding[0][4]!=state->record[5]||binding[0][5]!=state->record[3]||
                   observed.schema!=text(state->record[3])||observed.scope!=text(state->record[0]))
                    refuse("checked retained history source/schema changed");
            }
            const auto actual=writer->physical_identity("main",{},true);
            if(!actual||*actual!=*physical||owner.closed_.load()||writer->is_closed()||
               sqlite3_total_changes64(handle)!=changes||sqlite3_get_autocommit(handle)!=0||
               sqlite3_txn_state(handle,"main")!=SQLITE_TXN_READ)
                refuse("checked schema actual READ lifetime changed");
            writer->execute("ROLLBACK");begun=false;
            if(sqlite3_get_autocommit(handle)==0)refuse("checked schema READ did not settle");
        }catch(...){
            primary=std::current_exception();
            if(begun||sqlite3_get_autocommit(handle)==0)try{writer->execute("ROLLBACK");}
                catch(...){cleanup=std::current_exception();rollback_uncertain=true;}
            if(sqlite3_get_autocommit(handle)==0)rollback_uncertain=true;
        }
    }catch(...){if(!primary)primary=std::current_exception();}
    // No context/journal operation or connection-publication leaf surrounds SQL.
    if(rollback_uncertain){
        try{writer->close();}catch(...){if(!cleanup)cleanup=std::current_exception();}
        owner.closed_.store(true,std::memory_order_release);
    }
    if(primary)throw ordinary_checked_open_error(primary,cleanup,context,database_retirement_access::retain(*writer));
    {std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load()||owner.db_!=writer||owner.connection_revision_!=revision)
            refuse("checked schema actual writer publication changed");}
    if(!bounded_admission)ordinary_before_open(owner.config_.path,context);
    const auto after=writer->physical_identity("main",{},true);
    if(!after||*after!=*physical)refuse("checked schema actual physical file changed after READ");
    if(bounded_admission)validate_managed_group_open(context,bounded_admission,owner.config_.path,after->device,after->inode,end);
    else context->validate_physical(after->device,after->inode);
    if(profile){const auto& state=profile->impl_;
        if(state->admission->enrollment_binding(state->path,after->device,after->inode)!=generation||
           owner.ordinary_managed_profile_.load(std::memory_order_acquire)!=profile)
            refuse("checked schema actual generation/profile changed after READ");}
}
// No receiver context or source UUID is synthesized by this existing server READ.
void ordinary_export_access::validate_checked_mounted_server_schema(lattice_db& owner,
    const physical_store_identity& expected) {
    owner.require_creator_process();
    if(owner.config_.ordinary_context||owner.config_.is_in_memory()||owner.config_.read_only||
       owner.config_.is_sync_enabled()||owner.config_.is_ipc_enabled()||owner.config_.audit_retention_seconds!=0||
       owner.config_.managed_product_credentials||owner.recovery_continuous_||owner.administrative_owner_||
       owner.is_closed()||!owner.recovery_schemas_.valid()||owner.recovery_schemas_.swift_models.empty()||
       owner.recovery_schemas_.swift_fingerprint.empty()||owner.config_.target_schema_version<=0)
        refuse("checked mounted server actual no-context catalog owner required");
    std::shared_ptr<database> writer;uint64_t revision;
    {std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load()||!owner.db_)refuse("checked mounted server owner closed");
        writer=owner.db_;revision=owner.connection_revision_;}
    const auto physical=writer->physical_identity("main",{},true);
    if(!physical||*physical!=expected||physical->filename!=expected.filename)
        refuse("checked mounted server physical file changed");
    ordinary_resource_reservation<false> metadata_capacity;
    const auto core=owner.compute_core_fingerprint_key();
    const auto swift=owner.recovery_schemas_.swift_fingerprint;
    budget retained{wire_limits};
    retained.add(sizeof(std::vector<std::string>)+owner.recovery_schemas_.models.size()*sizeof(std::string));
    std::vector<std::string> names;names.reserve(owner.recovery_schemas_.models.size());
    for(const auto& [name,unused]:owner.recovery_schemas_.models){retained.add(name.size());names.push_back(name);}
    std::exception_ptr primary,cleanup;bool rollback_uncertain=false;
    try {
        database::maintenance_scope::admission admission;
        database::maintenance_scope turn(*writer,&admission);
        if(admission!=database::maintenance_scope::admission::ready)
            refuse("checked mounted server native writer busy");
        auto* handle=borrowed_storage_handle(*writer);
        if(!handle||writer->is_closed()||owner.is_closed()||writer->channel_reset_unsettled_.load()||
           sqlite3_get_autocommit(handle)==0)
            refuse("checked mounted server refuses caller transaction or retired writer");
        const auto changes=sqlite3_total_changes64(handle);bool begun=false;
        try {
            writer->execute("BEGIN");begun=true;
            (void)read(handle,"SELECT 1 FROM main.sqlite_master LIMIT 1",{},1,retained);
            if(sqlite3_txn_state(handle,"main")!=SQLITE_TXN_READ)
                refuse("checked mounted server main READ unavailable");
            require_recovery_local_producer_maintenance_absent(*writer);
            require_continuous_legacy_export_absent(*writer);require_ordered_receiver_absent(*writer);
            const auto version=read_metadata(handle,"SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS version FROM main._lattice_meta WHERE key='schema_version' COLLATE BINARY LIMIT 2",{},2,retained);
            if(version.size()!=1||!std::holds_alternative<std::string>(version[0].at("version"))||
               std::get<std::string>(version[0].at("version"))!=std::to_string(owner.config_.target_schema_version))
                refuse("checked mounted server positive version differs; no migration");
            const auto cookie_rows=read_metadata(handle,"PRAGMA main.schema_version",{},1,retained);
            if(cookie_rows.size()!=1||!std::holds_alternative<int64_t>(cookie_rows[0].at("schema_version")))
                refuse("checked mounted server actual cookie unavailable");
            const auto cookie=std::get<int64_t>(cookie_rows[0].at("schema_version"));
            if(cookie<0||cookie>INT32_MAX)refuse("checked mounted server canonical cookie refused");
            for(const auto* key:{&core,&swift}){
                const auto marker=read_metadata(handle,"SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS cookie FROM main._lattice_meta WHERE key=? COLLATE BINARY LIMIT 2",{*key},2,retained);
                if(marker.size()!=1||!std::holds_alternative<std::string>(marker[0].at("cookie"))||
                   std::get<std::string>(marker[0].at("cookie"))!=std::to_string(cookie))
                    refuse("checked mounted server fingerprint/cookie differs; no repair");
            }
            ordinary_export_frame observed;observed.wire_profile=true;
            capture_projected_tables(handle,owner.recovery_schemas_,[](auto& out,const auto& property){
                lattice_db::serialize_property_for_fingerprint(out,property);},names,observed,retained,false,nullptr,nullptr);
            const auto actual=writer->physical_identity("main",{},true);
            if(!actual||*actual!=expected||actual->filename!=expected.filename||owner.closed_.load()||writer->is_closed()||
               sqlite3_total_changes64(handle)!=changes||sqlite3_get_autocommit(handle)!=0||
               sqlite3_txn_state(handle,"main")!=SQLITE_TXN_READ)
                refuse("checked mounted server actual READ lifetime changed");
            writer->execute("ROLLBACK");begun=false;
            if(sqlite3_get_autocommit(handle)==0)refuse("checked mounted server READ did not settle");
        }catch(...){
            primary=std::current_exception();
            if(begun||sqlite3_get_autocommit(handle)==0)try{writer->execute("ROLLBACK");}
                catch(...){cleanup=std::current_exception();rollback_uncertain=true;}
            if(sqlite3_get_autocommit(handle)==0)rollback_uncertain=true;
        }
    }catch(...){if(!primary)primary=std::current_exception();}
    if(rollback_uncertain){
        try{writer->close();}catch(...){if(!cleanup)cleanup=std::current_exception();}
        owner.closed_.store(true,std::memory_order_release);
    }
    if(primary)throw ordinary_checked_open_error(primary,cleanup,{},database_retirement_access::retain(*writer));
    {std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load()||owner.db_!=writer||owner.connection_revision_!=revision)
            refuse("checked mounted server writer publication changed");}
    owner.require_creator_process();
    const auto after=writer->physical_identity("main",{},true);
    if(!after||*after!=expected||after->filename!=expected.filename)
        refuse("checked mounted server actual physical file changed after READ");
}
#include "ordinary_managed_profile_pair.inc"
#include "ordinary_managed_product_policy.inc"
#include "ordinary_managed_wss_binding.inc"
#include "ordinary_managed_peer_owner.inc"
#include "ordinary_managed_contribution_ledger.inc"
#include "ordinary_managed_peer_retry.inc"
#include "ordinary_managed_upstream_capture.inc"
#include "ordinary_managed_peer_request.inc"
#include "ordinary_managed_peer_delivery.inc"
#include "ordinary_history_owner.inc"
#include "ordinary_history_capture.inc"
#include "ordinary_history_receive_storage.inc"
#include "ordinary_managed_accepted_baseline.inc"
#include "ordinary_managed_baseline_paging.inc"
#include "ordinary_managed_receiver_preservation.inc"
#include "ordinary_history_final_install.inc"
#include "ordinary_history_capacity.inc"
#include "ordinary_receiver_restart_access.inc"

std::vector<std::string> ordinary_export_access::apply(lattice_db& owner,const owned_server_export& issued) {
    return apply_impl(owner,issued,std::nullopt,false);
}
std::vector<std::string> ordinary_export_access::apply_session(lattice_db& owner,const owned_server_export& issued,const std::string& channel,ordinary_managed_wss_acceptance* observed) {
    ordinary_managed_wss_acceptance candidate;
    if(observed){
        const auto frame=issued.frame_;if(!frame)refuse("moved managed WSS cut");
        if(frame->scope.size()>ordinary_sync_limits::models*260||frame->source.size()>36)refuse("managed WSS cut identity bound");
        candidate={frame->scope,frame->source,picosha2::hash256_hex_string(frame->schema),frame_fingerprint(*frame),frame->epoch,frame->cut};
    }
    auto result=apply_impl(owner,issued,std::nullopt,false,&channel,original_policy::authenticated_session);
    // apply_impl returns only after clean actual COMMIT + off-lock delivery.
    // Empty rows/originals are valid. No payload size becomes an authority test.
    if(observed)*observed=std::move(candidate);
    return result;
}
std::vector<std::string> ordinary_export_access::apply_impl(lattice_db& owner,const owned_server_export& issued,const std::optional<std::string>& channel,bool helper,const std::string* wire_channel,original_policy originals_policy) {
    const auto frame=issued.frame_;if(!frame)refuse("moved owned export");
    const bool install_originals=originals_policy==original_policy::authenticated_session;
    if(install_originals&&(!wire_channel||!frame->wire_profile))refuse("canonical original install needs actual session entry");
    const auto& limits=frame->wire_profile?wire_limits:legacy_limits;
    // Retain the exact issued immutable capture across synchronous observer
    // reentry. No caller DTO or wire field can supply this authority.
    const auto names=scope_names(frame->scope,limits.models);const auto digest=frame_fingerprint(*frame);
    std::vector<std::string> accepted;accepted.reserve(frame->entries.size());
    values ids;ids.reserve(frame->entries.size());budget preparation{limits};
    for(const auto& entry:frame->entries){preparation.add(2*entry.global_id.size()+sizeof(std::string)+sizeof(column_value_t));accepted.push_back(entry.global_id);ids.emplace_back(entry.global_id);}
    const auto encoded_ids=encode(ids,preparation);
    const auto settlement=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer) {
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);
        require_managed_profile_if_present(owner,writer);
        if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_)
            refuse("owner catalog or ordinary receive route unavailable");
        auto* handle=ordinary_export_storage_access::borrow(writer);budget retained{limits};
        const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("receiver physical identity unavailable");
        const std::string device=std::to_string(physical->device),inode=std::to_string(physical->inode);
        ordinary_export_frame before;before.wire_profile=frame->wire_profile;
        ordinary_schema_projection projection;ordinary_schema_metadata metadata;
        capture_tables(handle,owner.recovery_schemas_,[](auto& stream,const auto& property){lattice_db::serialize_property_for_fingerprint(stream,property);},[&](const model_schema& declaration){
            std::vector<std::pair<std::string,column_type>> columns;std::set<std::string> no_history;
            for(const auto& property:declaration.properties){columns.emplace_back(property.name,property.type);if(property.no_history)no_history.insert(property.name);}
            std::vector<std::string> programs;owner.create_model_table_triggers(declaration.table_name,columns,no_history,{},&programs);return programs;
        },names,before,retained,true,frame->wire_profile?&projection:nullptr,frame->wire_profile?&metadata:nullptr);
        if(before.scope!=frame->scope||before.schema!=frame->schema)refuse("receiver schema differs from issued complete cut");
        if(before.tables.size()!=frame->tables.size())refuse("received table projection differs");
        for(size_t t=0;t<before.tables.size();++t)if(before.tables[t].name!=frame->tables[t].name||before.tables[t].columns!=frame->tables[t].columns||before.tables[t].types!=frame->tables[t].types)refuse("received column projection differs from actual schema");
        if(frame->wire_profile)validate_projected_values(*frame,projection,metadata,owner.recovery_schemas_,retained);
        register_mutation_guard(handle);mutation_scope permit(handle);
        if(!receiver_storage(handle,retained)) {
            if(!fresh_origin(owner,*physical)||owner.ordinary_receive_enrolled_here_.load(std::memory_order_acquire))refuse("historical receiver requires actual managed admission");
            for(const auto& [name,ddl]:receiver_definitions()){writer.execute(ddl);for(const auto* operation:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,operation));}
            if(!receiver_storage(handle,retained))refuse("receiver storage did not install");
        }
        const auto profiles=read(handle,"SELECT scope,source,epoch,cut,digest,device,inode,schema,baseline FROM main._lattice_ordered_receiver_v1 ORDER BY scope",{},maximum_profiles,retained);
        const values* prior=nullptr;
        for(const auto& profile:profiles) {
            if(!canonical_uuid(text(profile[1]))||integer(profile[2])!=1||integer(profile[3])<1||text(profile[4]).size()!=64||text(profile[5])!=device||text(profile[6])!=inode)
                refuse("receiver durable binding changed");
            const auto other=scope_names(text(profile[0]),limits.models);
            if(text(profile[0])==frame->scope)prior=&profile;
            else for(const auto& name:names)if(std::binary_search(other.begin(),other.end(),name))refuse("overlapping receiver scopes require explicit rebind");
        }
        bool has_helper=helper_storage(handle,retained);std::optional<values> helper_route;
        if(has_helper) {
            const auto routes=read(handle,"SELECT scope,channel,source,epoch,device,inode,schema FROM main._lattice_ordered_helper_v1 WHERE scope=?",{frame->scope},1,retained);
            if(!routes.empty())helper_route=routes[0];
        }
        if(helper&&channel&&(channel->empty()||channel->size()>4096))refuse("helper receive channel bound exceeded");
        if(helper&&prior&&!helper_route) {
            // An explicit foundation receiver cannot be implicitly promoted
            // by a later vector-like helper value. Preserve its old refusal.
            accepted.clear();return;
        }
        if(wire_channel&&prior&&!helper_route)refuse("existing local-only receiver has no proved remote channel");
        if(helper_route) {
            if(!prior||*helper_route!=values{frame->scope,text((*helper_route)[1]),frame->source,frame->epoch,device,inode,frame->schema})refuse("helper binding differs from issued source");
            if(helper&&text((*helper_route)[1])!=channel.value_or(""))refuse("helper channel change requires explicit bootstrap");
        } else if(helper||wire_channel) {
            if(!wire_channel)for(const auto& name:names){const auto* model=owner.recovery_schemas_.find(name);for(const auto& property:model->properties)if(property.no_history)refuse("helper no_history requires a negotiated incremental route");}
            if(!has_helper)for(const auto& [name,ddl]:helper_definitions()){writer.execute(ddl);for(const auto* operation:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,operation));}
            if(!helper_storage(handle,retained))refuse("helper storage did not install");
            helper_route=values{frame->scope,wire_channel?*wire_channel:channel.value_or(""),frame->source,frame->epoch,device,inode,frame->schema};
            writer.execute("INSERT INTO main._lattice_ordered_helper_v1 VALUES(?,?,?,?,?,?,?)",*helper_route);
            if(writer.changes()!=1||helper_binding(handle,frame->scope,retained)!=*helper_route)refuse("helper route publication was ignored");
        }
        const auto baselines=read(handle,"SELECT model,gid,value FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=? ORDER BY model,gid",{frame->scope},retained.limits.rows,retained);
        fingerprint prior_baseline;prior_baseline.bytes(frame->scope);prior_baseline.number(baselines.size());
        for(const auto& value:baselines){for(const auto& cell:value)prior_baseline.scalar(cell);}
        if(prior) {
            if(text((*prior)[8])!=prior_baseline.finish())refuse("stored complete baseline changed");
        } else if(!baselines.empty())refuse("baseline exists without receiver authority");
        // This additive v2 record is a validated migration of the existing
        // source/frontier, never a replacement epoch or metadata-derived grant.
        // An existing v1 receiver needs its actual matching helper channel to
        // prove continuity; an explicit local-only enrollment cannot silently
        // become a remote source. Fresh enrollment still requires constructor
        // custody above and the private authenticated session caller.
        auto wire=wire_binding(handle,frame->scope,retained);
        if(wire) {
            if(!prior||text((*wire)[1]).empty()||text((*wire)[1]).size()>4096||
               *wire!=values{frame->scope,(*wire)[1],(*prior)[1],(*prior)[2],device,inode,(*prior)[7],(*prior)[3],(*prior)[4]})refuse("wire binding lost its exact source/frontier");
            if(wire_channel&&(*wire)[1]!=column_value_t(*wire_channel))refuse("wire channel change requires conflict-preserving bootstrap");
        } else if(wire_channel) {
            if(wire_channel->empty()||wire_channel->size()>4096)refuse("wire channel bound");
            if(prior&&(!helper_route||(*helper_route)[1]!=column_value_t(*wire_channel)))refuse("v1 remote channel continuity is unproved");
            if(!wire_storage(handle,retained)) {
                for(const auto& [name,ddl]:wire_definitions()){writer.execute(ddl);for(const auto* operation:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,operation));}
                if(!wire_storage(handle,retained))refuse("wire binding did not install");
            }
            wire=values{frame->scope,*wire_channel,frame->source,frame->epoch,device,inode,frame->schema,prior?(*prior)[3]:column_value_t(int64_t{0}),prior?(*prior)[4]:column_value_t(std::string{})};
        }
        if(prior) {
            if(text((*prior)[1])!=frame->source||integer((*prior)[2])!=frame->epoch||text((*prior)[7])!=frame->schema)
                refuse("receiver source/schema/epoch change requires explicit bootstrap");
            const auto current_cut=integer((*prior)[3]);
            const auto current_receipt=read(handle,"SELECT digest FROM main._lattice_ordered_receiver_cut_v1 WHERE scope=? AND cut=?",{frame->scope,current_cut},1,retained);
            if(current_receipt.size()!=1||current_receipt[0][0]!=(*prior)[4])refuse("current cut receipt is missing or changed");
            if(frame->cut<=current_cut) {
                const auto receipt=read(handle,"SELECT digest,ids FROM main._lattice_ordered_receiver_cut_v1 WHERE scope=? AND cut=?",{frame->scope,frame->cut},1,retained);
                if(receipt.size()!=1||text(receipt[0][0])!=digest||receipt[0][1]!=column_value_t(encoded_ids))refuse("old cut is unobserved, changed or expired; explicit bootstrap required");
                if(frame->cut==current_cut&&text((*prior)[4])!=digest)refuse("current frontier differs from its receipt");
                if(install_originals) {
                    ordinary_wire_original_turn::state replay_state{writer,retained};ordinary_wire_original_turn replay{replay_state};
                    for(const auto& entry:frame->entries){const auto current=replay.current(frame->scope,entry.global_id);
                        if(!current)refuse("canonical replay lost original custody");(void)canonical_descriptor(*current,owner.recovery_schemas_,retained);}
                    replay.finish();
                }
                if(wire)store_wire_binding(writer,*wire,retained);
                return; // Actual receipt replay ACKs identities, never replays row SQL.
            }
        } else if(profiles.size()==maximum_profiles)refuse("receiver scope capacity exhausted");
        if(frame->wire_profile)validate_original_descriptors(*frame,owner.recovery_schemas_,retained,&before.tables);
        if(install_originals&&prior&&wire&&!wire_binding(handle,frame->scope,retained)) {
            // Actual validated v1 continuity is already proved above. Stage
            // its unchanged old frontier for the old archive turn; the new
            // frontier remains unpublished until materialization succeeds.
            store_wire_binding(writer,*wire,retained);
        }
        struct canonical_install {std::vector<uint8_t> descriptor;bool rewrite=false;};
        std::vector<canonical_install> canonical;
        if(install_originals) {
            install_delivery_storage(writer,retained);
            retained.add(frame->entries.size()*sizeof(canonical_install));canonical.resize(frame->entries.size());
            ordinary_wire_original_turn::state old_state{writer,retained};ordinary_wire_original_turn old_turn{old_state};
            // This phase observes the old frontier only. The turn finishes
            // before materialization can rebuild schema programs or publish
            // the new frontier; nothing is cached across that transition.
            capture_entries(handle,before,owner.recovery_schemas_,retained,prior?&old_turn:nullptr);
            std::optional<original_usage> initial_archives;
            using target_index=std::set<std::string_view,ordinary_identifier_less>;
            retained.add(frame->tables.size()*sizeof(target_index)+frame->rows*(sizeof(std::string_view)+4*sizeof(void*)));
            std::vector<target_index> targets(frame->tables.size());
            for(size_t t=0;t<frame->tables.size();++t)for(const auto& row:frame->tables[t].rows)targets[t].insert(text(row[0]));
            for(size_t n=0;n<frame->entries.size();++n) {
                const auto& entry=frame->entries[n];auto& plan=canonical[n];
                plan.descriptor=encode_ordinary_original_descriptor(frame->originals.at(n),[&](size_t bytes){retained.add(bytes);});
                std::optional<ordinary_wire_original_record> current;if(prior)current=old_turn.current(frame->scope,entry.global_id);
                const auto actual=original_audit(writer,entry.global_id,retained);
                bool replace=!actual.empty();
                if(current) {
                    const auto descriptor=canonical_descriptor(*current,owner.recovery_schemas_,retained);
                    if(current->stamp.source!=frame->source||current->stamp.epoch!=frame->epoch||current->stamp.schema!=frame->schema||
                       descriptor.fields!=frame->originals[n].fields||descriptor.names!=frame->originals[n].names||descriptor.digest!=frame->originals[n].digest||
                       current->local_view.original_identity!=entry.original_identity)
                        refuse("accepted canonical original changed immutable identity; bootstrap required");
                    const auto envelope=encode_ordinary_v2_retained_entry(entry,[&](size_t bytes){retained.add(bytes);});
                    replace=current->envelope!=envelope||current->descriptor!=plan.descriptor;
                    const auto table=std::lower_bound(before.tables.begin(),before.tables.end(),entry.table_name,[](const auto& t,const auto& name){return t.name<name;});
                    if(table==before.tables.end()||table->name!=entry.table_name)refuse("canonical target lost schema");
                    const size_t t=static_cast<size_t>(table-before.tables.begin());
                    const bool target_present=targets.at(t).count(entry.global_row_id)!=0;
                    const auto target=table->local_id&&target_present?read(handle,"SELECT id FROM main."+quote(entry.table_name)+" WHERE globalId=?",{entry.global_row_id},1,retained):rows{};
                    if(table->local_id&&target_present&&target.empty())replace=true; // a new local target ID will be allocated
                    else if(current->local_view.row_id!=(target.empty()?int64_t{0}:integer(target[0][0])))replace=true;
                    if(replace)(void)old_turn.archive(frame->scope,entry.global_id,"canonical-observation-replaced");
                } else if(!actual.empty()) {
                    if(prior)(void)old_turn.archive(frame->scope,entry.global_id,integer(actual[10])?"prior-remote-original-unknown":"prior-local-original-observed");
                    else {
                        // No prior accepted frontier exists. Preserve raw
                        // observed audit/channel/slot facts, explicitly with
                        // no canonical record or invented cut receipt.
                        if(!read(handle,"SELECT 1 FROM main._lattice_ordered_wire_original_v2 WHERE scope=? AND gid=?",{frame->scope,entry.global_id},1,retained).empty())refuse("unbound canonical original already exists");
                        if(!initial_archives)initial_archives=original_capacity(writer,true,retained);
                        (void)archive_original_facts(writer,frame->scope,entry.global_id,integer(actual[10])?"first-cut-remote-original-unknown":"first-cut-local-original-observed",{},retained,*initial_archives);
                    }
                }
                plan.rewrite=replace;
            }
            if(initial_archives&&original_capacity(writer,true,retained)!=*initial_archives)refuse("first-cut archive accounting changed");
            old_turn.finish();
        } else capture_entries(handle,before,owner.recovery_schemas_,retained);
        helper_capacity helper_space{};if(helper_route)helper_space=helper_usage(handle,retained);
        if(helper_route)for(const auto& entry:before.entries){validate_helper_entry(entry,before,retained);retain_helper_entry(writer,frame->scope,entry,false,retained,helper_space,frame->wire_profile);}
        bound_storage(handle,"_lattice_ordered_receiver_baseline_v1","value",retained);
        bound_storage(handle,"_lattice_ordered_receiver_conflict_v1","payload",retained);
        const auto conflict_usage=read(handle,"SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM main._lattice_ordered_receiver_conflict_v1",{},1,retained);
        int64_t conflict_rows=integer(conflict_usage.at(0).at(0)),conflict_bytes=integer(conflict_usage.at(0).at(1));
        const auto archive=[&](const std::string& kind,const std::string& identity,const std::vector<uint8_t>& payload) {
            fingerprint hash;hash.bytes(payload);const auto payload_hash=hash.finish();
            auto stored=read(handle,"SELECT payload FROM main._lattice_ordered_receiver_conflict_v1 WHERE scope=? AND kind=? AND identity=? AND digest=?",{frame->scope,kind,identity,payload_hash},1,retained);
            if(stored.empty()) {
                if(conflict_rows==static_cast<int64_t>(retained.limits.rows)||payload.size()>retained.limits.bytes-static_cast<size_t>(conflict_bytes))refuse("unresolved conflict capacity exhausted");
                writer.execute("INSERT INTO main._lattice_ordered_receiver_conflict_v1(scope,kind,identity,digest,payload,cut) VALUES(?,?,?,?,?,?)",{frame->scope,kind,identity,payload_hash,payload,frame->cut});
                if(writer.changes()!=1)refuse("conflict custody insertion was ignored");
                ++conflict_rows;conflict_bytes+=static_cast<int64_t>(payload.size());
                stored=read(handle,"SELECT payload FROM main._lattice_ordered_receiver_conflict_v1 WHERE scope=? AND kind=? AND identity=? AND digest=?",{frame->scope,kind,identity,payload_hash},1,retained);
            }
            if(stored.size()!=1||stored[0][0]!=column_value_t(payload))refuse("conflict custody did not persist exactly");
        };
        std::map<std::string,std::string> confirmed;
        for(const auto& entry:frame->entries){retained.add(entry.global_id.size()+64);confirmed.emplace(entry.global_id,entry_fingerprint(entry));}
        for(const auto& entry:before.entries)if(!entry.is_from_remote&&!entry.is_synchronized) {
            const auto known=confirmed.find(entry.global_id);
            if(known==confirmed.end()||known->second!=entry_fingerprint(entry))archive("audit",entry.global_id,encode_entry(entry,retained,frame->wire_profile));
        }
        if(helper_route) {
            // The provisional ledger survives ordinary ACK collapse, pruning
            // and subsequent sibling overwrites. It is not a server receipt.
            const auto pending=read(handle,"SELECT gid,effect,payload,seal FROM main._lattice_ordered_helper_entry_v1 WHERE scope=? AND canonical=0 ORDER BY gid,effect",{frame->scope},retained.limits.rows,retained);
            std::map<std::string,std::string> issued_effects;for(const auto& entry:frame->entries)issued_effects.emplace(entry.global_id,effect_fingerprint(entry));
            for(const auto& entry:pending){if(text(entry[3])!=helper_entry_seal(frame->scope,text(entry[0]),text(entry[1]),entry[2],0))refuse("pending helper payload changed");const auto found=issued_effects.find(text(entry[0]));if(found==issued_effects.end()||found->second!=text(entry[1])){const auto* payload=std::get_if<std::vector<uint8_t>>(&entry[2]);if(!payload)refuse("pending helper payload storage changed");archive("provisional",text(entry[0]),*payload);}}
        }
        struct row_less {
            bool nocase;
            bool operator()(std::string_view a,std::string_view b)const {return nocase?ordinary_identifier_less{}(a,b):a<b;}
        };
        using row_index=std::map<std::string_view,const values*,row_less>;
        const row_index empty_index{row_less{frame->wire_profile}};
        std::vector<row_index> old_index(frame->tables.size(),empty_index),incoming_index(frame->tables.size(),empty_index);
        using baseline_key=std::pair<std::string_view,std::string_view>;
        const auto baseline_less=[wire=frame->wire_profile](const baseline_key& a,const baseline_key& b){
            if(a.first!=b.first)return a.first<b.first;return wire?ordinary_identifier_less{}(a.second,b.second):a.second<b.second;
        };
        std::map<baseline_key,const values*,decltype(baseline_less)> baseline_index{baseline_less};
        retained.add((before.rows+frame->rows+baselines.size())*sizeof(std::pair<std::string_view,const values*>));
        for(size_t t=0;t<frame->tables.size();++t){for(const auto& row:before.tables[t].rows)old_index[t].emplace(text(row[0]),&row);for(const auto& row:frame->tables[t].rows)incoming_index[t].emplace(text(row[0]),&row);}
        for(const auto& row:baselines)baseline_index.emplace(std::make_pair(std::string_view(text(row[0])),std::string_view(text(row[1]))),&row);
        // Preserve divergent current values unless they equal the last exact
        // installed baseline. Absence of audit history never proves acceptance.
        for(size_t t=0;t<before.tables.size();++t)for(const auto& row:before.tables[t].rows) {
            const auto& table=before.tables[t];
            const auto match=incoming_index[t].find(text(row[0]));
            if(match!=incoming_index[t].end()&&*match->second==row)continue;
            const auto encoded=encode(row,retained);
            const auto prior_row=baseline_index.find({table.name,text(row[0])});
            if(prior_row!=baseline_index.end()&&(*prior_row->second)[2]==column_value_t(encoded))continue;
            column_value_t previous_value=nullptr,incoming_value=nullptr;
            if(prior_row!=baseline_index.end())previous_value=(*prior_row->second)[2];
            if(match!=incoming_index[t].end())incoming_value=encode(*match->second,retained);
            archive("row",std::to_string(table.name.size())+':'+table.name+text(row[0]),
                encode({table.name,row[0],previous_value,encoded,incoming_value},retained));
        }
        for(const auto& baseline:baselines) {
            const auto table=std::find_if(before.tables.begin(),before.tables.end(),[&](const auto& value){return value.name==text(baseline[0]);});
            if(table==before.tables.end())refuse("baseline has a model outside its bound scope");
            const auto position=static_cast<size_t>(table-before.tables.begin());
            if(old_index[position].count(text(baseline[1])))continue;
            const auto restored=incoming_index[position].find(text(baseline[1]));
            if(restored==incoming_index[position].end())continue;
            archive("absence",std::to_string(table->name.size())+':'+table->name+text(baseline[1]),
                encode({table->name,baseline[1],baseline[2],nullptr,encode(*restored->second,retained)},retained));
        }
        const auto conflicts_after=read(handle,"SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM main._lattice_ordered_receiver_conflict_v1",{},1,retained);
        if(conflicts_after.at(0)!=values{conflict_rows,conflict_bytes})refuse("conflict accounting changed before materialization");
        const auto flags=read(handle,"SELECT id,disabled FROM main._SyncControl ORDER BY id",{},2,retained);
        if(flags.size()!=1||integer(flags[0][0])!=1||(integer(flags[0][1])!=0&&integer(flags[0][1])!=1))refuse("ordinary audit flag shape changed");
        const auto previous=integer(flags[0][1]);
        const bool previous_applying=owner.applying_remote_changes_.exchange(true,std::memory_order_acq_rel);
        struct restore_applying {std::atomic<bool>& flag;bool previous;~restore_applying(){flag.store(previous,std::memory_order_release);}} applying{owner.applying_remote_changes_,previous_applying};
        writer.execute("UPDATE main._SyncControl SET disabled=1 WHERE id=1");
        if(writer.changes()!=1||read(handle,"SELECT disabled FROM main._SyncControl WHERE id=1",{},1,retained).at(0).at(0)!=column_value_t(int64_t{1}))refuse("audit suppression was refused");
        if(frame->wire_profile)materialize_projected(writer,before,*frame,projection,metadata,owner.recovery_schemas_,retained);
        else for(size_t t=0;t<frame->tables.size();++t) {
            const auto& table=frame->tables[t];const auto& old=before.tables[t];
            for(const auto& row:old.rows)if(!incoming_index[t].count(text(row[0])))
                writer.execute("DELETE FROM main."+quote(table.name)+" WHERE globalId=?",{row[0]});
            for(const auto& row:table.rows) {
                const auto existing=old_index[t].find(text(row[0]));
                if(existing!=old_index[t].end()&&*existing->second==row)continue;
                std::string columns,parameters;for(const auto& name:table.columns){if(!columns.empty()){columns+=',';parameters+=',';}columns+=quote(name);parameters+='?';}
                if(existing==old_index[t].end())writer.execute("INSERT INTO main."+quote(table.name)+'('+columns+") VALUES("+parameters+')',row);
                else {
                    std::string update;values parameters;parameters.reserve(row.size());
                    for(size_t c=1;c<row.size();++c){if(!update.empty())update+=',';update+=quote(table.columns[c])+"=?";parameters.push_back(row[c]);}
                    parameters.push_back(row[0]);writer.execute("UPDATE main."+quote(table.name)+" SET "+update+" WHERE globalId=?",parameters);
                }
            }
        }
        if(helper_route) {
            const auto& receiving=text((*helper_route)[1]);
            for(size_t original_index=0;original_index<frame->entries.size();++original_index) {
                const auto& entry=frame->entries[original_index];
                validate_helper_entry(entry,before,retained);
                const auto existing=read(handle,"SELECT id FROM main.AuditLog WHERE globalId=?",{entry.global_id},1,retained);
                if(existing.empty()) {
                    const auto table=std::find_if(before.tables.begin(),before.tables.end(),[&](const auto& t){return t.name==entry.table_name;});
                    if(table==before.tables.end())refuse("canonical original lost its actual table projection");
                    const auto local=table->local_id?read(handle,"SELECT id FROM main."+quote(entry.table_name)+" WHERE globalId=?",{entry.global_row_id},1,retained):rows{};
                    const auto row_id=local.empty()?int64_t{0}:integer(local[0][0]);
                    // The retained envelope preserves the exact timestamp;
                    // the actual local REAL column uses the same conversion
                    // as committed ordinary source intake. Legacy unchanged.
                    const column_value_t physical_timestamp=install_originals?column_value_t(ordinary_upload_timestamp_to_epoch(entry.timestamp)):column_value_t(entry.timestamp);
                    writer.execute("INSERT INTO main.AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized) VALUES(?,?,?,?,?,?,?,CAST(? AS REAL),1,?,?)",
                        {entry.global_id,entry.table_name,entry.operation,row_id,entry.global_row_id,entry.changed_fields_to_json(),entry.changed_fields_names_to_json(),physical_timestamp,int64_t(receiving.empty()),int64_t(entry.synthesized)});
                    if(writer.changes()!=1)refuse("canonical helper audit identity insert was ignored");
                    const auto inserted=read(handle,"SELECT id FROM main.AuditLog WHERE globalId=?",{entry.global_id},1,retained);if(inserted.size()!=1)refuse("canonical helper audit identity unavailable");
                    if(!receiving.empty()) {
                        writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1",{integer(inserted[0][0]),receiving});
                        if(writer.changes()!=1)refuse("canonical helper receiving state was ignored");
                    }
                }
                if(install_originals) {
                    if(!existing.empty()&&canonical[original_index].rewrite) {
                        const auto table=std::find_if(before.tables.begin(),before.tables.end(),[&](const auto& t){return t.name==entry.table_name;});
                        if(table==before.tables.end())refuse("canonical replacement lost its table");
                        const auto target=table->local_id?read(handle,"SELECT id FROM main."+quote(entry.table_name)+" WHERE globalId=?",{entry.global_row_id},1,retained):rows{};
                        const auto row_id=target.empty()?int64_t{0}:integer(target[0][0]);
                        const auto physical_timestamp=ordinary_upload_timestamp_to_epoch(entry.timestamp);
                        writer.execute("UPDATE main.AuditLog SET globalId=?,tableName=?,operation=?,rowId=?,globalRowId=?,changedFields=?,changedFieldsNames=?,timestamp=?,isFromRemote=1,synthesized=? WHERE id=?",
                            {entry.global_id,entry.table_name,entry.operation,row_id,entry.global_row_id,entry.changed_fields_to_json(),entry.changed_fields_names_to_json(),physical_timestamp,int64_t(entry.synthesized),integer(existing[0][0])});
                        if(writer.changes()!=1)refuse("canonical original replacement was ignored");
                        const auto observed=original_audit(writer,entry.global_id,retained);
                        if(observed.size()!=12||integer(observed[0])!=integer(existing[0][0])||text(observed[1])!=entry.global_id||text(observed[2])!=entry.table_name||text(observed[3])!=entry.operation||integer(observed[4])!=row_id||text(observed[5])!=entry.global_row_id||observed[6]!=column_value_t(entry.changed_fields_to_json())||observed[7]!=column_value_t(entry.changed_fields_names_to_json())||observed[8]!=column_value_t(physical_timestamp)||integer(observed[9])!=int64_t(entry.synthesized)||integer(observed[10])!=1)
                            refuse("canonical original replacement postimage differs");
                    }
                    const auto staged=original_audit(writer,entry.global_id,retained);
                    if(staged.empty())refuse("canonical original has no staged local identity");
                    if(!receiving.empty()) {
                        writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1",{integer(staged[0]),receiving});
                        const auto state=read(handle,"SELECT is_synchronized FROM main._lattice_sync_state WHERE audit_entry_id=? AND sync_id=?",{integer(staged[0]),receiving},1,retained);
                        if(writer.changes()!=1||state.size()!=1||state[0][0]!=column_value_t(int64_t{1}))refuse("canonical receiving obligation differs");
                    }
                    // Other channels' old ACK facts are neither erased nor
                    // promoted to a replacement receipt. Their generation and
                    // negotiated repair integration is an explicit next gate.
                }
                retain_helper_entry(writer,frame->scope,entry,true,retained,helper_space,frame->wire_profile);
            }
        }
        writer.execute("UPDATE main._SyncControl SET disabled=? WHERE id=1",{previous});
        if(writer.changes()!=1||read(handle,"SELECT disabled FROM main._SyncControl WHERE id=1",{},1,retained).at(0).at(0)!=column_value_t(previous))refuse("audit flag restoration was refused");
        ordinary_export_frame after;after.wire_profile=frame->wire_profile;
        ordinary_schema_projection after_projection;ordinary_schema_metadata after_metadata;
        capture_tables(handle,owner.recovery_schemas_,[](auto& stream,const auto& property){lattice_db::serialize_property_for_fingerprint(stream,property);},[&](const model_schema& declaration){
            std::vector<std::pair<std::string,column_type>> columns;std::set<std::string> no_history;
            for(const auto& property:declaration.properties){columns.emplace_back(property.name,property.type);if(property.no_history)no_history.insert(property.name);}
            std::vector<std::string> programs;owner.create_model_table_triggers(declaration.table_name,columns,no_history,{},&programs);return programs;
        },names,after,retained,true,&after_projection,&after_metadata);
        if(after.schema!=frame->schema||after.tables.size()!=frame->tables.size())refuse("receiver schema changed during materialization");
        for(size_t t=0;t<after.tables.size();++t)if(after.tables[t].rows!=frame->tables[t].rows)refuse("receiver complete postimage differs");
        if(frame->wire_profile) {
            const ordinary_metadata_query query=[&](const std::string& sql,const values& parameters,size_t limit) {
                if(sqlite3_txn_state(handle,"main")!=SQLITE_TXN_WRITE)refuse("derived postimage lost its owned WRITE");
                return read_metadata(handle,sql,parameters,limit,retained);
            };
            for(const auto& derived:after_metadata.derived)if(derived.kind==ordinary_derived_kind::vector&&derived.present) {
                // Validate physical maps/valid slots as well as the module's
                // visible rows, using newly observed metadata after reset.
                // Training restoration is a separate unfinished operation;
                // this draft currently rebuilds IVF in its untrained state.
                (void)validate_ordinary_vector_integrity(derived,query,false,
                    ordinary_vector_integrity_limits{retained.limits.rows,retained.limits.bytes});
            }
        }
        writer.execute("DELETE FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=?",{frame->scope});
        for(const auto& table:frame->tables)for(const auto& row:table.rows)writer.execute("INSERT INTO main._lattice_ordered_receiver_baseline_v1 VALUES(?,?,?,?)",{frame->scope,table.name,row[0],encode(row,retained)});
        bound_storage(handle,"_lattice_ordered_receiver_baseline_v1","value",retained);
        const auto installed_baseline=read(handle,"SELECT model,gid,value FROM main._lattice_ordered_receiver_baseline_v1 WHERE scope=? ORDER BY model,gid",{frame->scope},retained.limits.rows,retained);
        fingerprint baseline_hash;baseline_hash.bytes(frame->scope);baseline_hash.number(frame->rows);size_t baseline_position=0;
        for(const auto& table:frame->tables)for(const auto& row:table.rows) {
            const values expected{table.name,row[0],encode(row,retained)};
            if(baseline_position==installed_baseline.size()||installed_baseline[baseline_position++]!=expected)refuse("complete baseline persistence changed");
            for(const auto& cell:expected)baseline_hash.scalar(cell);
        }
        if(baseline_position!=installed_baseline.size())refuse("complete baseline gained unissued rows");
        const auto baseline_digest=baseline_hash.finish();
        writer.execute("INSERT INTO main._lattice_ordered_receiver_cut_v1 VALUES(?,?,?,?)",{frame->scope,frame->cut,digest,encoded_ids});
        // A finite replay window. Expired delivery is an explicit refusal, never
        // invented ACK or stale row materialization. No conflict is evicted.
        writer.execute("INSERT INTO main._lattice_ordered_receiver_v1 VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET cut=excluded.cut,digest=excluded.digest,baseline=excluded.baseline",{frame->scope,frame->source,frame->epoch,frame->cut,digest,device,inode,frame->schema,baseline_digest});
        if(writer.changes()!=1)refuse("receiver frontier publication was ignored");
        writer.execute("DELETE FROM main._lattice_ordered_receiver_cut_v1 AS cut WHERE NOT EXISTS (SELECT 1 FROM main._lattice_ordered_receiver_v1 AS current WHERE current.scope=cut.scope AND current.cut=cut.cut) AND rowid NOT IN (SELECT rowid FROM main._lattice_ordered_receiver_cut_v1 ORDER BY rowid DESC LIMIT 48)");
        bound_storage(handle,"_lattice_ordered_receiver_cut_v1","ids",retained);
        if(integer(read(handle,"SELECT COUNT(*) FROM main._lattice_ordered_receiver_cut_v1",{},1,retained).at(0).at(0))>64)refuse("cut receipt count bound exceeded");
        const auto final=read(handle,"SELECT scope,source,epoch,cut,digest,device,inode,schema,baseline FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{frame->scope},1,retained);
        if(final.size()!=1||final[0]!=values{frame->scope,frame->source,frame->epoch,frame->cut,digest,device,inode,frame->schema,baseline_digest})refuse("receiver frontier changed after materialization");
        const auto receipt=read(handle,"SELECT digest,ids FROM main._lattice_ordered_receiver_cut_v1 WHERE scope=? AND cut=?",{frame->scope,frame->cut},1,retained);
        if(receipt.size()!=1||receipt[0]!=values{digest,encoded_ids})refuse("receiver cut receipt changed before commit");
        if(wire){(*wire)[7]=frame->cut;(*wire)[8]=digest;store_wire_binding(writer,*wire,retained);}
        if(install_originals) {
            // Fresh context after actual receipt/frontier publication. The
            // old turn is already finished and cannot observe this frontier.
            ordinary_wire_original_turn::state current_state{writer,retained};ordinary_wire_original_turn current{current_state};
            retained.add(frame->scope.size()+frame->source.size()+frame->schema.size()+digest.size()+sizeof(ordinary_wire_original_stamp));
            const ordinary_wire_original_stamp stamp{frame->scope,frame->source,frame->schema,digest,frame->epoch,frame->cut};
            for(size_t n=0;n<frame->entries.size();++n){const auto audit=original_audit(writer,frame->entries[n].global_id,retained);
                if(audit.empty())refuse("accepted canonical original lost its staged audit row");current.store(stamp,frame->entries[n],1,canonical[n].descriptor,integer(audit[0]));
                current.bind_delivery(frame->scope,frame->entries[n].global_id,*wire_channel);}
            current.finish();
        }
        if(helper_route){if(helper_binding(handle,frame->scope,retained)!=*helper_route)refuse("helper route changed before commit");verify_helper_binding(writer,*helper_route,after,retained);if(helper_usage(handle,retained)!=helper_space)refuse("canonical helper accounting changed");}
    },[&]{owner.ordinary_receive_enrolled_here_.store(true,std::memory_order_release);});
    if(settlement.state!=recovery_install_state::committed||settlement.primary_error||settlement.cleanup_error||settlement.postcommit_error||settlement.notification_error)
        throw ordinary_order_write_error(settlement);
    return accepted;
}
}

namespace lattice {
server_export_batch::server_export_batch(std::shared_ptr<const detail::ordinary_export_batch> value):value_(std::move(value)){}
bool server_export_batch::ordered()const{if(!value_)throw db_error("moved server export batch");return value_->complete.has_value();}
const std::vector<audit_log_entry>& server_export_batch::entries()const{if(!value_)throw db_error("moved server export batch");return value_->entries;}
owned_server_export::owned_server_export(std::shared_ptr<const detail::ordinary_export_frame> frame):frame_(std::move(frame)){}
const std::string& owned_server_export::source_id()const{if(!frame_)throw db_error("moved owned export");return frame_->source;}
int64_t owned_server_export::epoch()const{if(!frame_)throw db_error("moved owned export");return frame_->epoch;}
int64_t owned_server_export::cut()const{if(!frame_)throw db_error("moved owned export");return frame_->cut;}
size_t owned_server_export::row_count()const{if(!frame_)throw db_error("moved owned export");return frame_->rows;}
const std::vector<audit_log_entry>& owned_server_export::entries()const{if(!frame_)throw db_error("moved owned export");return frame_->entries;}
owned_server_export capture_owned_server_export(lattice_db& owner,const std::vector<std::string>& models){return detail::ordinary_export_access::capture(owner,models);}
std::vector<std::string> apply_owned_server_export(lattice_db& owner,const owned_server_export& issued){return detail::ordinary_export_access::apply(owner,issued);}
}

namespace lattice {
ordinary_history_page::ordinary_history_page(std::shared_ptr<const detail::ordinary_history_page_state> state):state_(std::move(state)){}
std::span<const uint8_t> ordinary_history_page::bytes()const {if(!state_)throw db_error("moved history page");detail::history_process(*state_->owner);return state_->bytes;}
ordinary_history_export::ordinary_history_export(std::shared_ptr<detail::ordinary_history_export_state> state):state_(std::move(state)){}
const std::string& ordinary_history_export::stage_id()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.identity.stage;}
const std::string& ordinary_history_export::source_id()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.identity.source;}
int64_t ordinary_history_export::epoch()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.identity.epoch;}
int64_t ordinary_history_export::cut()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.identity.cut;}
uint64_t ordinary_history_export::frame_count()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.totals.pages+1;}
uint64_t ordinary_history_export::durable_bytes()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->record.totals.bytes;}
ordinary_history_page ordinary_history_export::page(uint64_t index)const{return ordinary_history_page{detail::ordinary_export_access::history_page(state_,index)};}
std::optional<ordinary_history_prior_source_observation> ordinary_history_export::prior_mounted_source()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->prior;}
const std::string& ordinary_history_export::mounted_source_relation_observation()const{if(!state_)throw db_error("moved history export");detail::history_process(*state_);return state_->mounted_relation;}
ordinary_mounted_history_source::ordinary_mounted_history_source(std::shared_ptr<detail::ordinary_mounted_history_source_state> state):state_(std::move(state)){}
ordinary_mounted_history_source ordinary_mounted_history_source::issue(std::shared_ptr<lattice_db> owner,const std::vector<std::string>& mounted,const ordinary_history_limits& limits){return detail::ordinary_export_access::register_mounted_history(std::move(owner),mounted,limits);}
ordinary_history_export ordinary_mounted_history_source::capture(const std::vector<std::string>& selected)const{return detail::ordinary_export_access::capture_mounted_history(state_,selected);}
ordinary_history_export ordinary_mounted_history_source::capture_with_lineage_observation(const std::vector<std::string>& selected)const{return detail::ordinary_export_access::capture_mounted_history(state_,selected,true);}
ordinary_history_export capture_ordinary_history_export(lattice_db& owner,const std::vector<std::string>& models,const ordinary_history_limits& limits){return detail::ordinary_export_access::capture_history(owner,models,limits);}
ordinary_history_export resume_ordinary_history_export(lattice_db& owner,const std::string& stage,const ordinary_history_limits& limits){return detail::ordinary_export_access::resume_history(owner,stage,limits);}
}
