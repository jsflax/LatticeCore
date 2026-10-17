#pragma once
#include "ordinary_export.hpp"
#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <set>
#include <span>
#include <string_view>
#include <vector>
#include <cstdint>

namespace lattice::detail {
// Passive local storage only. Decoding a descriptor never grants a source,
// accepted cut, or producer identity; the owning turn verifies those facts.
namespace ordinary_descriptor_codec_detail {
using charge=std::function<void(size_t)>;
[[noreturn]] inline void fail(){throw db_error("ordinary original descriptor codec bound or shape");}
inline void digest(const std::string& s){if(s.size()!=64||!std::all_of(s.begin(),s.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))fail();}
inline size_t sequence_size(const std::vector<std::string>& values,bool sorted,const charge& retain){
    if(values.size()>2050)fail();size_t bytes=8;std::set<std::string_view> seen;
    for(size_t n=0;n<values.size();++n){const auto& v=values[n];if(v.empty()||v.size()>256||v.find('\0')!=std::string::npos||(sorted&&n&&values[n-1]>=v))fail();
        retain(sizeof(std::string_view)+4*sizeof(void*));if(!seen.emplace(v).second)fail();bytes+=8+v.size();}return bytes;
}
}
inline std::vector<uint8_t> encode_ordinary_original_descriptor(const ordinary_audit_original_descriptor& value,
    const ordinary_descriptor_codec_detail::charge& charge) {
    using namespace ordinary_descriptor_codec_detail;digest(value.digest);
    const auto bytes=4+sequence_size(value.fields,true,charge)+sequence_size(value.names,false,charge)+64;
    if(bytes>4*1024*1024)fail();charge(sizeof(std::vector<uint8_t>)+bytes);
    std::vector<uint8_t> out;out.reserve(bytes);for(char c:std::string_view("LOD2"))out.push_back(static_cast<uint8_t>(c));
    const auto number=[&](size_t n){for(int i=7;i>=0;--i)out.push_back(static_cast<uint8_t>(uint64_t(n)>>(8*i)));};
    for(const auto* sequence:{&value.fields,&value.names}){number(sequence->size());for(const auto& name:*sequence){number(name.size());out.insert(out.end(),name.begin(),name.end());}}
    out.insert(out.end(),value.digest.begin(),value.digest.end());return out;
}
inline ordinary_audit_original_descriptor decode_ordinary_original_descriptor(std::span<const uint8_t> raw,
    const ordinary_descriptor_codec_detail::charge& charge) {
    using namespace ordinary_descriptor_codec_detail;
    if(raw.size()<84||raw.size()>4*1024*1024||std::memcmp(raw.data(),"LOD2",4))fail();size_t at=4;
    const auto number=[&]()->uint64_t{if(raw.size()-at<8)fail();uint64_t n=0;for(size_t i=0;i<8;++i)n=(n<<8)|raw[at++];return n;};
    charge(sizeof(ordinary_audit_original_descriptor));ordinary_audit_original_descriptor result;
    for(auto* sequence:{&result.fields,&result.names}){const auto count=number();if(count>2050)fail();charge(static_cast<size_t>(count)*sizeof(std::string));sequence->reserve(static_cast<size_t>(count));
        for(size_t n=0;n<count;++n){const auto bytes=number();if(!bytes||bytes>256||bytes>raw.size()-at)fail();charge(static_cast<size_t>(bytes)+1);
            sequence->emplace_back(reinterpret_cast<const char*>(raw.data()+at),static_cast<size_t>(bytes));at+=static_cast<size_t>(bytes);}}
    if(raw.size()-at!=64)fail();charge(65);result.digest.assign(reinterpret_cast<const char*>(raw.data()+at),64);
    (void)sequence_size(result.fields,true,charge);(void)sequence_size(result.names,false,charge);digest(result.digest);return result;
}
}
