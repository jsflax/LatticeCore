#include "observer_delivery_meter.hpp"
#include <sqlite3.h>
#include <algorithm>
#include <cmath>
#include <limits>
#include <set>
#include <stdexcept>

namespace lattice::detail {
namespace {
constexpr const char* meter_table = "_lattice_observer_delivery_meter_v1";
constexpr const char* meter_ddl =
    "CREATE TABLE _lattice_observer_delivery_meter_v1 ("
    "id INTEGER PRIMARY KEY CHECK(id=1),token BLOB NOT NULL,"
    "events INTEGER NOT NULL CHECK(typeof(events)='integer' AND events>=0),"
    "bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>=0))";
[[noreturn]] void refuse(const char* reason) { throw std::length_error(reason); }
uint64_t plus(uint64_t left,uint64_t right) {
    if(left>observer_delivery_meter::integer_ceiling ||
       right>observer_delivery_meter::integer_ceiling-left)
        refuse("observer delivery meter integer overflow");
    return left+right;
}
uint64_t twice(uint64_t value) { return plus(value,value); }
uint64_t value_bytes(sqlite3_value* value) {
    if(!value)refuse("observer delivery meter absent SQL value");
    switch(sqlite3_value_type(value)) {
    case SQLITE_NULL:return 4;
    case SQLITE_INTEGER:return 21; // signed decimal, including INT64_MIN
    case SQLITE_FLOAT:
        if(!std::isfinite(sqlite3_value_double(value)))refuse("observer delivery meter nonfinite REAL");
        return 32; // exceeds the approved finite JSON spelling
    case SQLITE_TEXT:{
        const auto* text=sqlite3_value_text(value);
        const int size=sqlite3_value_bytes(value); // UTF8 bytes, never TEXT length
        if(size<0||(!text&&size))refuse("observer delivery meter UTF8 unavailable");
        if(!text&&sqlite3_value_type(value)!=SQLITE_NULL)
            refuse("observer delivery meter UTF8 conversion failed");
        const std::string_view actual{reinterpret_cast<const char*>(text),static_cast<size_t>(size)};
        if(!observer_delivery_meter::valid_utf8(actual))refuse("observer delivery meter invalid UTF8 TEXT");
        return observer_delivery_meter::json_string_bytes(actual);
    }
    case SQLITE_BLOB:{
        const int size=sqlite3_value_bytes(value);
        if(size<0)refuse("observer delivery meter BLOB unavailable");
        // Quoted hexadecimal encoding; its length is twice the actual bytes.
        return plus(2,twice(static_cast<uint64_t>(size)));
    }
    default:refuse("observer delivery meter unknown SQL type");
    }
}
}

observer_delivery_domain::units observer_delivery_meter::admission_charge(observer_delivery_meter_budget value) {
    if(!value.events||!value.event_bytes||!value.workspace_bytes||value.events>integer_ceiling)
        refuse("observer delivery meter empty or oversized declaration");
    return {value.events,plus(value.event_bytes,value.workspace_bytes)};
}
uint64_t observer_delivery_meter::json_string_bytes(std::string_view value) {
    uint64_t total=2;
    for(const unsigned char c:value) {
        // U+0000..001f may be emitted as six-byte \u00XX; short escapes only
        // reduce the actual output. UTF8 multibyte bytes remain byte-counted.
        total=plus(total,c<0x20?6:(c=='\"'||c=='\\')?2:1);
    }
    return total;
}
bool observer_delivery_meter::valid_utf8(std::string_view value)noexcept {
    size_t index=0;
    while(index<value.size()) {
        const auto first=static_cast<unsigned char>(value[index++]);
        if(first<0x80)continue;
        size_t count=0;uint32_t scalar=0,minimum=0;
        if(first>=0xc2&&first<=0xdf){count=1;scalar=first&0x1f;minimum=0x80;}
        else if(first>=0xe0&&first<=0xef){count=2;scalar=first&0x0f;minimum=0x800;}
        else if(first>=0xf0&&first<=0xf4){count=3;scalar=first&7;minimum=0x10000;}
        else return false;
        if(count>value.size()-index)return false;
        for(size_t part=0;part<count;++part) {
            const auto next=static_cast<unsigned char>(value[index++]);
            if((next&0xc0)!=0x80)return false;
            scalar=(scalar<<6)|(next&0x3f);
        }
        if(scalar<minimum||scalar>0x10ffff||(scalar>=0xd800&&scalar<=0xdfff))return false;
    }
    return true;
}
uint64_t observer_delivery_meter::row_bytes(std::string_view table,
    std::span<const std::string_view> names,std::span<sqlite3_value* const> values) {
    if(names.size()!=values.size()||names.size()>column_ceiling)
        refuse("observer delivery meter row width differs");
    // Envelope/op/id/commas/braces and numeric row identity are charged even
    // for empty values. This is structured-data liability, not sizeof(fn).
    uint64_t total=plus(128,json_string_bytes(table));
    for(size_t i=0;i<names.size();++i)
        total=plus(total,plus(plus(json_string_bytes(names[i]),2),value_bytes(values[i])));
    return total;
}
std::string_view observer_delivery_meter::ledger_name() noexcept {return meter_table;}
std::string_view observer_delivery_meter::ledger_ddl() noexcept {return meter_ddl;}
std::string_view observer_delivery_meter::initialization_seed_()noexcept {
    return "INSERT INTO main._lattice_observer_delivery_meter_v1(id,token,events,bytes) VALUES(1,zeroblob(32),0,0)";
}
std::string_view observer_delivery_meter::initialization_marker_cas_()noexcept {
    return "UPDATE main._lattice_meta SET value=? WHERE key=? AND value=?";
}

std::vector<observer_delivery_meter_program> observer_delivery_meter::programs(
    const std::vector<observer_delivery_meter_table>& input) {
    return programs_storage_<std::string>(input);
}
}
