#pragma once
#include "ordinary_export.hpp"
#include "ordinary_timestamp.hpp"
#include <lattice/ordinary_history.hpp>
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <cmath>
#include <bit>
#include <string_view>

namespace lattice::detail::history_wire {
// Pure bounded serialization; these values cannot issue source/session grants.
// The actual exporter reserves the shared native slot before calling here.
using caps=ordinary_sync_limits;
[[noreturn]] inline void fail(){throw db_error("ordinary paged history wire bound or shape");}
inline void add(std::string& out,std::string_view bytes){if(bytes.size()>caps::frame_bytes-out.size())fail();out.append(bytes);}
inline void quote(std::string& out,std::string_view bytes){
    if(bytes.size()>caps::scalar_bytes)fail();size_t bound=2;
    for(unsigned char c:bytes){const size_t n=c<32?6:(c=='"'||c=='\\'?2:1);if(n>caps::frame_bytes-bound)fail();bound+=n;}
    if(bound>caps::frame_bytes-out.size())fail();add(out,nlohmann::json(std::string(bytes)).dump());
}
inline void count(std::string& out,uint64_t n){quote(out,std::to_string(n));}
inline std::string digest(std::string_view bytes){return picosha2::hash256_hex_string(bytes.begin(),bytes.end());}
inline void scalar(std::string& out,const column_value_t& value){
    add(out,"[");
    if(std::holds_alternative<std::nullptr_t>(value))add(out,"\"null\"");
    else if(const auto* n=std::get_if<int64_t>(&value)){add(out,"\"integer\",");quote(out,std::to_string(*n));}
    else if(const auto* n=std::get_if<double>(&value)){if(!std::isfinite(*n))fail();add(out,"\"real\",");add(out,nlohmann::json(*n).dump());}
    else if(const auto* s=std::get_if<std::string>(&value)){add(out,"\"text\",");quote(out,*s);}
    else {const auto& data=std::get<std::vector<uint8_t>>(value);if(data.size()>caps::scalar_bytes/2||out.size()>caps::frame_bytes-16||data.size()>(caps::frame_bytes-out.size()-16)/2)fail();
        add(out,"\"blob\",\"");static constexpr char hex[]="0123456789abcdef";for(auto v:data){const char pair[]{hex[v>>4],hex[v&15]};add(out,std::string_view(pair,2));}add(out,"\"");}
    add(out,"]");
}
inline std::string prefix(const ordinary_export_frame& image,const std::string& stage,const char* kind){
    std::string out;add(out,"{\"latticeOrdinarySync\":{\"version\":2,\"profile\":\"paged-history-v1\",\"kind\":");quote(out,kind);
    add(out,",\"source\":");quote(out,image.source);add(out,",\"epoch\":");count(out,image.epoch);add(out,",\"cut\":");count(out,image.cut);add(out,",\"stage\":");quote(out,stage);return out;
}
inline std::string row(const std::string& table,const std::vector<column_value_t>& cells){
    if(cells.empty()||cells.size()>caps::columns_per_table)fail();std::string out;add(out,"{\"row\":{\"table\":");quote(out,table);add(out,",\"values\":[");
    for(size_t n=0;n<cells.size();++n){if(n)add(out,",");scalar(out,cells[n]);}add(out,"]}}");return out;
}
inline std::string unknown(const std::vector<column_value_t>& observed){
    // Exact typed AuditLog observations, including malformed raw JSON or age,
    // are retained as unknown. They are never decoded into accepted originals.
    if(observed.size()!=12)fail();std::string out;add(out,"{\"unknownOriginal\":{\"provenance\":\"legacy-observed\",\"audit\":[");
    for(size_t n=0;n<observed.size();++n){if(n)add(out,",");
        if(const auto* value=std::get_if<std::string>(&observed[n])) {
            // Unknown legacy TEXT is observed bytes, including embedded NUL
            // or malformed UTF-8. Never parse/re-encode it as an original.
            if(value->size()>caps::scalar_bytes||out.size()>caps::frame_bytes-20||value->size()>(caps::frame_bytes-out.size()-20)/2)fail();
            add(out,"[\"textBytes\",\"");static constexpr char hex[]="0123456789abcdef";
            for(unsigned char byte:*value){const char pair[]{hex[byte>>4],hex[byte&15]};add(out,std::string_view(pair,2));}add(out,"\"]");
        } else if(const auto* value=std::get_if<double>(&observed[n]);value&&!std::isfinite(*value)){
            // Only unknown observed history accepts this bit-preserving kind.
            // Model values and known originals continue to require finite REAL.
            const auto bits=std::bit_cast<uint64_t>(*value);static constexpr char hex[]="0123456789abcdef";
            add(out,"[\"realBits\",\"");for(int shift=60;shift>=0;shift-=4){const char digit=hex[(bits>>shift)&15];add(out,std::string_view(&digit,1));}add(out,"\"]");
        }else scalar(out,observed[n]);}add(out,"]}}");return out;
}
inline std::string known(const audit_log_entry& entry,const ordinary_audit_original_descriptor& original){
    (void)normalize_required_ordinary_timestamp(entry.timestamp);
    if(entry.changed_fields.size()>caps::columns_per_table||entry.changed_fields_names.size()>caps::columns_per_table||original.fields.size()>caps::columns_per_table||original.names.size()>caps::columns_per_table||original.digest.size()!=64)fail();
    // Charge a conservative encoded bound before to_json allocates its DOM.
    size_t bytes=2048;const auto charge=[&](size_t n){if(n>caps::frame_bytes-bytes)fail();bytes+=n;};
    const auto string=[&](const std::string& s){if(s.size()>caps::scalar_bytes||s.size()>(caps::frame_bytes-bytes)/6)fail();charge(s.size()*6+2);};
    for(const auto* s:{&entry.global_id,&entry.table_name,&entry.operation,&entry.global_row_id,&entry.timestamp})string(*s);
    for(const auto& [name,p]:entry.changed_fields){string(name);charge(128);if(const auto* s=std::get_if<std::string>(&p.value))string(*s);if(const auto* b=std::get_if<std::vector<uint8_t>>(&p.value)){if(b->size()>caps::scalar_bytes/2||b->size()>(caps::frame_bytes-bytes)/2)fail();charge(b->size()*2);}}
    for(const auto* names:{&entry.changed_fields_names,&original.fields,&original.names})for(const auto& name:*names)string(name);
    if(entry.original_identity){for(const auto& name:entry.original_identity->changed_fields_names)string(name);string(entry.original_identity->digest);}
    std::string out;add(out,"{\"original\":");add(out,entry.to_json());add(out,",\"originalDescriptor\":{\"version\":1,\"fields\":[");
    for(size_t n=0;n<original.fields.size();++n){if(n)add(out,",");quote(out,original.fields[n]);}add(out,"],\"names\":[");
    for(size_t n=0;n<original.names.size();++n){if(n)add(out,",");quote(out,original.names[n]);}add(out,"],\"digest\":");quote(out,original.digest);add(out,"}}");return out;
}
inline bool workspace(size_t bytes,size_t nodes){return bytes<=caps::parser_workspace_bytes/caps::parser_wire_copies&&nodes<=(caps::parser_workspace_bytes-bytes*caps::parser_wire_copies)/caps::parser_node_logical_bytes;}
}
