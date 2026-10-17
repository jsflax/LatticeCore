#pragma once
#include <lattice/sync.hpp>
#include <nlohmann/json.hpp>
#include <cmath>
#include <limits>

namespace lattice::detail {
// Allocation-free validation and copied scalar-byte charge for the exact same
// representation accepted below. Container/name charges belong to the caller.
inline size_t inspect_ordinary_v2_property(const nlohmann::json& input,size_t maximum_scalar_bytes) {
    using json=nlohmann::json;
    const auto fail=[](const char* why)->void{throw db_error(std::string("ordinary v2 property: ")+why);};
    const auto integer=[&](const json& value){if(!value.is_number_integer()||(value.is_number_unsigned()&&value.get<uint64_t>()>uint64_t(std::numeric_limits<int64_t>::max())))fail("exact signed integer required");};
    const auto real=[&](const json& value){if(!value.is_number()||!std::isfinite(value.get<double>()))fail("finite numeric value required");};
    const auto string=[&](const json& value)->const std::string&{if(!value.is_string())fail("string required");const auto& text=value.get_ref<const std::string&>();if(text.size()>maximum_scalar_bytes)fail("scalar byte bound");return text;};
    if(input.is_null()||input.is_boolean())return 0;
    if(input.is_number_integer()){integer(input);return 0;}
    if(input.is_number_float()){real(input);return 0;}
    if(input.is_string())return string(input).size()+1;
    if(!input.is_object()||!input.contains("kind")||input.size()!=1+size_t(input.contains("value")))fail("typed envelope differs");
    integer(input["kind"]);const auto kind=input["kind"].get<int64_t>();if(kind<0||kind>7)fail("unknown kind");
    if(kind==4){if(input.contains("value")&&!input["value"].is_null())fail("null kind differs");return 0;}
    if(!input.contains("value"))fail("typed value missing");const auto& value=input["value"];
    if(kind==0||kind==1){integer(value);return 0;}
    if(kind==3||kind==5||kind==7){real(value);return 0;}
    const auto& text=string(value);if(kind==2)return text.size()+1;
    if(text.size()%2)fail("odd hex data");for(char c:text)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))fail("nonhex data");return text.size()/2;
}
// Value conversion only: this does not admit a source, descriptor or original.
// The caller owns the bounded JSON tree and charges retained allocations before
// invoking this helper. Network ingress additionally requires the exact typed
// kind/value envelope; actual SQL audit capture also uses raw JSON primitives.
inline any_property decode_ordinary_v2_property(const nlohmann::json& input,size_t maximum_scalar_bytes) {
    (void)inspect_ordinary_v2_property(input,maximum_scalar_bytes);
    using json=nlohmann::json;
    const auto fail=[](const char* reason)->void{throw db_error(std::string("ordinary v2 property: ")+reason);};
    const auto integer=[&](const json& value)->int64_t{
        if(!value.is_number_integer()||(value.is_number_unsigned()&&value.get<uint64_t>()>uint64_t(std::numeric_limits<int64_t>::max())))fail("exact signed integer required");
        return value.get<int64_t>();
    };
    const auto real=[&](const json& value)->double{
        if(!value.is_number()||!std::isfinite(value.get<double>()))fail("finite numeric value required");return value.get<double>();
    };
    const auto string=[&](const json& value)->const std::string&{
        if(!value.is_string())fail("string required");const auto& text=value.get_ref<const std::string&>();
        if(text.size()>maximum_scalar_bytes)fail("scalar byte bound");return text;
    };
    if(input.is_null())return any_property(nullptr);
    if(input.is_boolean())return any_property(input.get<bool>()?1:0);
    if(input.is_number_integer())return any_property(integer(input));
    if(input.is_number_float())return any_property(real(input));
    if(input.is_string())return any_property(string(input));
    if(!input.is_object()||!input.contains("kind")||input.size()!=1+size_t(input.contains("value")))fail("typed envelope differs");
    const auto tag=integer(input["kind"]);if(tag<0||tag>7)fail("unknown kind");
    any_property result;result.kind=static_cast<any_property_kind>(tag);
    if(result.kind==any_property_kind::null_kind){if(input.contains("value")&&!input["value"].is_null())fail("null kind differs");return result;}
    if(!input.contains("value"))fail("typed value missing");const auto& value=input["value"];
    switch(result.kind){
        case any_property_kind::null_kind:break;
        case any_property_kind::int_kind:case any_property_kind::int64_kind:result.value=integer(value);break;
        case any_property_kind::float_kind:case any_property_kind::double_kind:case any_property_kind::date_kind:result.value=real(value);break;
        case any_property_kind::string_kind:result.value=string(value);break;
        case any_property_kind::data_kind:{
            const auto& text=string(value);if(text.size()%2)fail("odd hex data");
            const auto nib=[](char c)->int{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;if(c>='A'&&c<='F')return c-'A'+10;return -1;};
            for(char c:text)if(nib(c)<0)fail("nonhex data");
            std::vector<uint8_t> data;data.reserve(text.size()/2);
            for(size_t n=0;n<text.size();n+=2)data.push_back(uint8_t(nib(text[n])*16+nib(text[n+1])));
            result.value=std::move(data);break;
        }
    }
    return result;
}
}
