#include "ordinary_v2_original.hpp"
#include <lattice/ordinary_sync.hpp>
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <limits>

namespace lattice::detail {
namespace {
using caps=ordinary_sync_limits;
enum class envelope { original, local_retained };
constexpr std::array<uint8_t,8> original_magic{'L','O','V','2',0,0,0,1};
constexpr std::array<uint8_t,8> retained_magic{'L','R','V','2',0,0,0,1};
const auto& magic_for(envelope format)noexcept{return format==envelope::original?original_magic:retained_magic;}
bool matches(std::span<const uint8_t> bytes,envelope format)noexcept{const auto& magic=magic_for(format);return bytes.size()>=magic.size()&&std::equal(magic.begin(),magic.end(),bytes.begin());}
constexpr size_t names_limit=caps::columns_per_table;
[[noreturn]] void fail(const char* why){throw db_error(std::string("ordinary v2 original: ")+why);}
struct size_counter {
    size_t value=0,limit=caps::frame_bytes;
    void add(size_t n){if(n>limit-value)fail("payload or retained value byte bound");value+=n;}
};
void key(std::string_view s,size_t maximum,bool empty=false){if((!empty&&s.empty())||s.size()>maximum||s.find('\0')!=std::string_view::npos)fail("key bound or embedded NUL");}
void operation(std::string_view s){if(s!="INSERT"&&s!="UPDATE"&&s!="DELETE")fail("operation differs");}
void digest(std::string_view s){if(s.size()!=64||!std::all_of(s.begin(),s.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))fail("identity digest spelling");}
void unique(std::array<std::string_view,names_limit>& names,size_t count){
    std::sort(names.begin(),names.begin()+static_cast<ptrdiff_t>(count));
    if(std::adjacent_find(names.begin(),names.begin()+static_cast<ptrdiff_t>(count))!=names.begin()+static_cast<ptrdiff_t>(count))fail("duplicate property name");
}
void name_sequence(const std::vector<std::string>& names,size_counter& size){
    if(names.size()>names_limit)fail("name count bound");size.add(4);std::array<std::string_view,names_limit> seen{};
    for(size_t n=0;n<names.size();++n){key(names[n],256);size.add(4+names[n].size());seen[n]=names[n];}unique(seen,names.size());
}
size_t property_size(const any_property& property){
    switch(property.kind){
        case any_property_kind::null_kind:if(!std::holds_alternative<std::nullptr_t>(property.value))fail("null kind mismatch");return 1;
        case any_property_kind::int_kind:case any_property_kind::int64_kind:if(!std::holds_alternative<int64_t>(property.value))fail("integer kind mismatch");return 9;
        case any_property_kind::float_kind:case any_property_kind::double_kind:case any_property_kind::date_kind:{const auto* p=std::get_if<double>(&property.value);if(!p||!std::isfinite(*p))fail("real kind mismatch");return 9;}
        case any_property_kind::string_kind:{const auto* p=std::get_if<std::string>(&property.value);if(!p||p->size()>caps::scalar_bytes)fail("string kind/bound mismatch");return 5+p->size();}
        case any_property_kind::data_kind:{const auto* p=std::get_if<std::vector<uint8_t>>(&property.value);if(!p||p->size()>caps::scalar_bytes/2)fail("data kind/bound mismatch");return 5+p->size();}
    }
    fail("unknown property kind");
}
size_t encoded_size(const audit_log_entry& entry,envelope format){
    const auto& magic=magic_for(format);size_counter size;size.add(magic.size()+17);
    if(format==envelope::original&&entry.id<=0)fail("positive original id required");key(entry.global_id,4096);key(entry.table_name,256);operation(entry.operation);key(entry.global_row_id,4096);key(entry.timestamp,caps::scalar_bytes,true);
    for(const auto* value:{&entry.global_id,&entry.table_name,&entry.operation,&entry.global_row_id,&entry.timestamp})size.add(4+value->size());
    if(entry.changed_fields.size()>names_limit)fail("field count bound");size.add(4);
    for(const auto& [name,value]:entry.changed_fields){key(name,256);size.add(4+name.size());size.add(property_size(value));}
    name_sequence(entry.changed_fields_names,size);size.add(1);
    if(entry.original_identity){const auto& id=*entry.original_identity;if(id.version!=1)fail("identity version");digest(id.digest);size.add(8+4+id.digest.size());name_sequence(id.changed_fields_names,size);}
    return size.value;
}
struct writer {
    std::vector<uint8_t>& out;
    void number(uint64_t n,unsigned width){for(unsigned i=0;i<width;++i)out.push_back(static_cast<uint8_t>(n>>(8*(width-1-i))));}
    void string(std::string_view s){number(s.size(),4);out.insert(out.end(),s.begin(),s.end());}
    void names(const std::vector<std::string>& values){number(values.size(),4);for(const auto& value:values)string(value);}
    void property(const any_property& p){number(static_cast<uint8_t>(p.kind),1);
        if(const auto* value=std::get_if<int64_t>(&p.value))number(std::bit_cast<uint64_t>(*value),8);
        else if(const auto* value=std::get_if<double>(&p.value))number(std::bit_cast<uint64_t>(*value),8);
        else if(const auto* value=std::get_if<std::string>(&p.value))string(*value);
        else if(const auto* value=std::get_if<std::vector<uint8_t>>(&p.value)){number(value->size(),4);out.insert(out.end(),value->begin(),value->end());}
    }
};
struct reader {
    std::span<const uint8_t> data;size_t offset=0;
    std::span<const uint8_t> take(size_t size){if(size>data.size()-offset)fail("truncated payload");const auto result=data.subspan(offset,size);offset+=size;return result;}
    uint64_t number(unsigned width){const auto bytes=take(width);uint64_t result=0;for(const auto byte:bytes)result=(result<<8)|byte;return result;}
    std::string_view string(size_t maximum){const auto size=number(4);if(size>maximum)fail("scalar length bound");const auto bytes=take(static_cast<size_t>(size));return {reinterpret_cast<const char*>(bytes.data()),bytes.size()};}
    size_t count(){const auto n=number(4);if(n>names_limit)fail("name/field count bound");return static_cast<size_t>(n);}
};
void read_header(reader& input,envelope format){if(!matches(input.data,format))fail("unknown payload version");input.take(magic_for(format).size());}
void inspect_names(reader& input,size_counter& retained){
    const auto count=input.count();retained.add(count*sizeof(std::string));std::array<std::string_view,names_limit> names{};
    for(size_t n=0;n<count;++n){names[n]=input.string(256);key(names[n],256);retained.add(names[n].size()+1);}unique(names,count);
}
void inspect_property(reader& input,size_counter& retained){
    const auto kind=input.number(1);if(kind>7)fail("unknown property kind");
    switch(static_cast<any_property_kind>(kind)){
        case any_property_kind::null_kind:break;
        case any_property_kind::int_kind:case any_property_kind::int64_kind:input.number(8);break;
        case any_property_kind::float_kind:case any_property_kind::double_kind:case any_property_kind::date_kind:if(!std::isfinite(std::bit_cast<double>(input.number(8))))fail("nonfinite real");break;
        case any_property_kind::string_kind:{const auto text=input.string(caps::scalar_bytes);retained.add(text.size()+1);break;}
        case any_property_kind::data_kind:{const auto bytes=input.string(caps::scalar_bytes/2);retained.add(bytes.size());break;}
    }
}
size_t inspect(std::span<const uint8_t> bytes,envelope format){
    if(bytes.empty()||bytes.size()>caps::frame_bytes)fail("payload byte bound");reader input{bytes};read_header(input,format);size_counter retained;retained.limit=caps::decoded_receive_bytes;retained.add(sizeof(audit_log_entry));
    const auto id=std::bit_cast<int64_t>(input.number(8));if(format==envelope::original&&id<=0)fail("positive original id required");input.number(8);if(input.number(1)>7)fail("unknown flags");
    for(size_t n=0;n<5;++n){const auto value=input.string(n==1?256:n==2?16:n==4?caps::scalar_bytes:4096);key(value,n==1?256:n==2?16:n==4?caps::scalar_bytes:4096,n==4);if(n==2)operation(value);retained.add(value.size()+1);}
    const auto fields=input.count();retained.add(fields*(sizeof(changed_fields_map::value_type)+4*sizeof(void*)));std::array<std::string_view,names_limit> names{};
    for(size_t n=0;n<fields;++n){names[n]=input.string(256);key(names[n],256);retained.add(names[n].size()+1);inspect_property(input,retained);}unique(names,fields);inspect_names(input,retained);
    const auto identity=input.number(1);if(identity>1)fail("identity presence flag");
    if(identity){if(input.number(8)!=1)fail("identity version");inspect_names(input,retained);const auto value=input.string(64);digest(value);retained.add(value.size()+1);}
    if(input.offset!=bytes.size())fail("trailing payload bytes");return retained.value;
}
std::string copy(std::string_view value){return std::string(value.data(),value.size());}
std::vector<std::string> read_names(reader& input){const auto count=input.count();std::vector<std::string> values;values.reserve(count);for(size_t n=0;n<count;++n)values.push_back(copy(input.string(256)));return values;}
any_property read_property(reader& input){
    any_property result;result.kind=static_cast<any_property_kind>(input.number(1));
    switch(result.kind){
        case any_property_kind::null_kind:break;
        case any_property_kind::int_kind:case any_property_kind::int64_kind:result.value=std::bit_cast<int64_t>(input.number(8));break;
        case any_property_kind::float_kind:case any_property_kind::double_kind:case any_property_kind::date_kind:result.value=std::bit_cast<double>(input.number(8));break;
        case any_property_kind::string_kind:result.value=copy(input.string(caps::scalar_bytes));break;
        case any_property_kind::data_kind:{const auto value=input.string(caps::scalar_bytes/2);const auto* data=reinterpret_cast<const uint8_t*>(value.data());result.value=std::vector<uint8_t>(data,data+value.size());break;}
    }
    return result;
}
std::vector<uint8_t> encode_entry(const audit_log_entry& entry,const ordinary_v2_retained_charge& charge,envelope format){
    if(!charge)fail("retained admission callback required");const auto& magic=magic_for(format);const auto size=encoded_size(entry,format);charge(sizeof(std::vector<uint8_t>)+size);
    std::vector<uint8_t> out;out.reserve(size);out.insert(out.end(),magic.begin(),magic.end());writer write{out};write.number(std::bit_cast<uint64_t>(entry.id),8);write.number(std::bit_cast<uint64_t>(entry.row_id),8);
    write.number(uint8_t(entry.is_from_remote)|(uint8_t(entry.is_synchronized)<<1)|(uint8_t(entry.synthesized)<<2),1);
    for(const auto* value:{&entry.global_id,&entry.table_name,&entry.operation,&entry.global_row_id,&entry.timestamp})write.string(*value);
    std::array<const changed_fields_map::value_type*,names_limit> fields{};size_t count=0;for(const auto& field:entry.changed_fields)fields[count++]=&field;
    std::sort(fields.begin(),fields.begin()+static_cast<ptrdiff_t>(count),[](const auto* a,const auto* b){return a->first<b->first;});write.number(count,4);
    for(size_t n=0;n<count;++n){write.string(fields[n]->first);write.property(fields[n]->second);}write.names(entry.changed_fields_names);write.number(bool(entry.original_identity),1);
    if(entry.original_identity){write.number(static_cast<uint64_t>(entry.original_identity->version),8);write.names(entry.original_identity->changed_fields_names);write.string(entry.original_identity->digest);}
    if(out.size()!=size)fail("encoded size differs");return out;
}
audit_log_entry decode_entry(std::span<const uint8_t> bytes,const ordinary_v2_retained_charge& charge,envelope format){
    if(!charge)fail("retained admission callback required");const auto retained=inspect(bytes,format);charge(retained);
    reader input{bytes};read_header(input,format);audit_log_entry entry;entry.id=std::bit_cast<int64_t>(input.number(8));entry.row_id=std::bit_cast<int64_t>(input.number(8));const auto flags=input.number(1);entry.is_from_remote=flags&1;entry.is_synchronized=flags&2;entry.synthesized=flags&4;
    entry.global_id=copy(input.string(4096));entry.table_name=copy(input.string(256));entry.operation=copy(input.string(16));entry.global_row_id=copy(input.string(4096));entry.timestamp=copy(input.string(caps::scalar_bytes));
    const auto fields=input.count();entry.changed_fields.reserve(fields);
    for(size_t n=0;n<fields;++n){auto name=copy(input.string(256));auto value=read_property(input);entry.changed_fields.emplace(std::move(name),std::move(value));}
    entry.changed_fields_names=read_names(input);if(input.number(1)){audit_original_identity identity;identity.version=std::bit_cast<int64_t>(input.number(8));identity.changed_fields_names=read_names(input);identity.digest=copy(input.string(64));entry.original_identity=std::move(identity);}
    return entry;
}

}
bool is_ordinary_v2_original(std::span<const uint8_t> bytes)noexcept{return matches(bytes,envelope::original);}
std::vector<uint8_t> encode_ordinary_v2_original(const audit_log_entry& entry,const ordinary_v2_retained_charge& charge){return encode_entry(entry,charge,envelope::original);}
audit_log_entry decode_ordinary_v2_original(std::span<const uint8_t> bytes,const ordinary_v2_retained_charge& charge){return decode_entry(bytes,charge,envelope::original);}
bool is_ordinary_v2_retained_entry(std::span<const uint8_t> bytes)noexcept{return matches(bytes,envelope::local_retained);}
std::vector<uint8_t> encode_ordinary_v2_retained_entry(const audit_log_entry& entry,const ordinary_v2_retained_charge& charge){return encode_entry(entry,charge,envelope::local_retained);}
audit_log_entry decode_ordinary_v2_retained_entry(std::span<const uint8_t> bytes,const ordinary_v2_retained_charge& charge){return decode_entry(bytes,charge,envelope::local_retained);}
}
