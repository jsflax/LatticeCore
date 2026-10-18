#pragma once
#include "observer_delivery_domain.hpp"
#include <lattice/detail/observer_delivery_values.hpp>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <set>
#include <algorithm>
#include <charconv>
#include <stdexcept>

struct sqlite3_value;

namespace lattice::detail {
// Private SQL meter building block. Admission/connection/Native original and
// delivery-tail wiring are separate required parts of this staged vertical.
// No mutable DTO, path spelling, callback or caller-supplied SQL grants credit.
using observer_delivery_meter_budget=observer_delivery_values::write_limits;
template<class Text,class Value>using observer_delivery_meter_vector=std::vector<Value,typename std::allocator_traits<typename Text::allocator_type>::template rebind_alloc<Value>>;
template<class Text>struct observer_delivery_meter_column_storage {Text name;};
template<class Text>struct observer_delivery_meter_table_storage {
    Text name;
    observer_delivery_meter_vector<Text,observer_delivery_meter_column_storage<Text>> columns;
};
template<class Text>struct observer_delivery_meter_program_storage {Text name,table,sql;};
using observer_delivery_meter_column=observer_delivery_meter_column_storage<std::string>;
using observer_delivery_meter_table=observer_delivery_meter_table_storage<std::string>;
using observer_delivery_meter_program=observer_delivery_meter_program_storage<std::string>;

class observer_delivery_meter {
public:
    static constexpr uint64_t integer_ceiling = UINT64_C(0x7fffffffffffffff);
    static constexpr size_t table_ceiling = 256, column_ceiling = 128;
    static constexpr size_t catalog_name_bytes = 65536, program_byte_ceiling = 1048576;
    // Includes both the declared structured-delivery liability and the actual
    // declaration's staging/prepare/bind workspace. Overflow never becomes a
    // double/REAL or a smaller admission. Fanout/capture weights must already
    // have been checked by the bounded registration/envelope producer.
    static observer_delivery_domain::units admission_charge(observer_delivery_meter_budget);
    // Conservative UTF8 JSON string representation without building JSON.
    // BLOB length/hex cost is distinct; integers/reals have fixed safe bounds.
    static uint64_t json_string_bytes(std::string_view);
    // Strict UTF8 profile; no conversion/allocation is used to validate input.
    // NUL is valid TEXT data and remains byte-counted by json_string_bytes.
    static bool valid_utf8(std::string_view)noexcept;
    static uint64_t row_bytes(std::string_view table,
        std::span<const std::string_view> column_names,
        std::span<sqlite3_value* const> values);
    // Canonical complete MAIN BEFORE programs. They meter before an AFTER
    // audit producer can allocate JSON. All identifier bytes are quoted;
    // duplicates/virtual/unadmitted catalog entries must fail before this API.
    static std::vector<observer_delivery_meter_program> programs(
        const std::vector<observer_delivery_meter_table>&);
    // ONE pure library generator owns both normal and pending-startup bytes.
    // Every dynamic node/character uses Text's real rebound allocator. No
    // legacy conversion, profile flag, SQL issuer or retained account exists.
    template<class Text>static observer_delivery_meter_vector<Text,observer_delivery_meter_program_storage<Text>> programs_storage_(
        const observer_delivery_meter_vector<Text,observer_delivery_meter_table_storage<Text>>& input) {
        const auto refuse=[](const char* message)->void{throw std::length_error(message);};
        const auto fold_identifier=[](std::string_view value){Text result(value.data(),value.size());for(auto& c:result)if(c>='A'&&c<='Z')c+=('a'-'A');return result;};
        const auto identifier=[&](std::string_view value) {
            if(value.empty()||value.size()>255||value.find('\0')!=std::string_view::npos||!valid_utf8(value))refuse("observer delivery meter identifier bound");
            Text result="\"";for(const char c:value){if(c=='\"')result+='\"';result+=c;}result+='\"';return result;
        };
        const auto literal=[](std::string_view value){Text result="'";for(const char c:value){if(c=='\'')result+='\'';result+=c;}result+='\'';return result;};
        const auto decimal=[&](size_t value){char buffer[3*sizeof(size_t)+1]{};const auto actual=std::to_chars(buffer,buffer+sizeof(buffer),value);if(actual.ec!=std::errc{})refuse("observer delivery meter ordinal conversion");return Text(buffer,actual.ptr);};
        const auto meter_table=ledger_name();
        if(input.empty()||input.size()>table_ceiling)refuse("observer delivery meter catalog bound");
        observer_delivery_meter_vector<Text,const observer_delivery_meter_table_storage<Text>*> tables;tables.reserve(input.size());
        std::set<Text,std::less<>,typename std::allocator_traits<typename Text::allocator_type>::template rebind_alloc<Text>> unique;
        size_t name_bytes=0;
        for(const auto& table:input) {
            (void)identifier(table.name);
            if(table.name.size()>catalog_name_bytes-name_bytes)refuse("observer delivery meter catalog name bytes");
            name_bytes+=table.name.size();
            const auto folded=fold_identifier(table.name);
            if(folded==meter_table||folded.starts_with("sqlite_")||
               folded.starts_with("_lattice_observer_delivery_")||!unique.insert(folded).second||
               table.columns.empty()||table.columns.size()>column_ceiling)
                refuse("observer delivery meter invalid catalog member");
            std::set<Text,std::less<>,typename std::allocator_traits<typename Text::allocator_type>::template rebind_alloc<Text>> names;
            for(const auto& column:table.columns){(void)identifier(column.name);
                if(column.name.size()>catalog_name_bytes-name_bytes)refuse("observer delivery meter catalog name bytes");
                name_bytes+=column.name.size();
                if(!names.insert(fold_identifier(column.name)).second)refuse("observer delivery meter duplicate column");}
            tables.push_back(&table);
        }
        std::sort(tables.begin(),tables.end(),[](const auto* a,const auto* b){return a->name<b->name;});
        observer_delivery_meter_vector<Text,observer_delivery_meter_program_storage<Text>> result;result.reserve(3*tables.size());
        size_t program_bytes=0;
        for(size_t index=0;index<tables.size();++index)for(const char* op:{"INSERT","UPDATE","DELETE"}) {
            const auto& table=*tables[index];const char* source=std::string_view(op)=="DELETE"?"OLD":"NEW";
            const Text name="_lattice_observer_delivery_before_v1_"+decimal(index)+"_"+op;
            const auto row_for=[&](const char* version){
                // At most121 arguments: works with SQLite's historical127-arg
                // function ceiling as well as current systems. Multiple chunks
                // conservatively charge their fixed envelope overhead separately.
                Text row;
                for(size_t start=0;start<table.columns.size();start+=60) {
                    Text chunk="lattice_observer_delivery_row_bytes_v1("+literal(table.name);
                    const auto end=std::min(start+60,table.columns.size());
                    for(size_t column=start;column<end;++column)
                        chunk+=","+literal(table.columns[column].name)+","+version+Text(".")+identifier(table.columns[column].name);
                    chunk+=")";
                    row=row.empty()?std::move(chunk):"lattice_observer_delivery_add_bytes_v1("+row+","+chunk+")";
                }
                return row;
            };
            // UPDATE audit production may retain both old and new values. Charge
            // both before it can build that JSON; a changed-only audit is smaller.
            const Text row=std::string_view(op)=="UPDATE"
                ?"lattice_observer_delivery_add_bytes_v1("+row_for("OLD")+","+row_for("NEW")+")"
                :row_for(source);
            const Text current="lattice_observer_delivery_token_v1()";
            const Text previous_count="CASE WHEN token="+current+" THEN events ELSE 0 END";
            const Text previous_bytes="CASE WHEN token="+current+" THEN bytes ELSE 0 END";
            Text sql="CREATE TRIGGER "+identifier(name)+" BEFORE "+op+" ON "+identifier(table.name)+" BEGIN ";
            sql+="SELECT CASE WHEN lattice_observer_delivery_permit_v1() IS NOT 1 THEN RAISE(ABORT,'observer delivery declaration required') END; ";
            sql+="UPDATE "+identifier(meter_table)+" SET token="+current+",events=("+previous_count+")+1,bytes=("+previous_bytes+")+"+row;
            // Subtraction before addition prevents SQLite's integer-to-REAL
            // promotion. Corrupt type/state fails WHERE, then actual changes()!=1.
            sql+=" WHERE id=1 AND typeof(token)='blob' AND typeof(events)='integer' AND events>=0 AND typeof(bytes)='integer' AND bytes>=0";
            sql+=" AND ("+previous_count+")<lattice_observer_delivery_event_limit_v1()";
            sql+=" AND "+row+"<=lattice_observer_delivery_byte_limit_v1()";
            sql+=" AND ("+previous_bytes+")<=lattice_observer_delivery_byte_limit_v1()-"+row+"; ";
            sql+="SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'observer delivery declaration exceeded or corrupt') END; END";
            if(sql.size()>program_byte_ceiling-program_bytes)refuse("observer delivery meter complete program bytes");
            program_bytes+=sql.size();
            result.push_back({name,table.name,std::move(sql)});
        }
        return result;
    }
    static std::string_view ledger_name() noexcept;
    static std::string_view ledger_ddl() noexcept;
    static std::string_view initialization_seed_()noexcept;
    static std::string_view initialization_marker_cas_()noexcept;
};
}
