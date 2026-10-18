#pragma once
#include <string_view>

namespace lattice::detail::resource_memory_route {
// Exact supported SQLite memory/temporary selector only. This is no file,
// root, SQL or lease authority. The caller must prove actual URI enablement.
inline int hex(char c)noexcept {
    if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;
    if(c>='A'&&c<='F')return c-'A'+10;return -1;
}
inline bool valid_component(std::string_view raw)noexcept {
    for(std::size_t i=0;i<raw.size();++i){
        const auto c=raw[i];if(!c)return false;
        if(c=='%'){
            if(i+2>=raw.size()||hex(raw[i+1])<0||hex(raw[i+2])<0||
               (hex(raw[i+1])==0&&hex(raw[i+2])==0))return false;i+=2;
        }
    }
    return true;
}
inline bool decoded_equals(std::string_view raw,std::string_view expected)noexcept {
    std::size_t at=0;
    for(std::size_t i=0;i<raw.size();++i){
        unsigned char c=static_cast<unsigned char>(raw[i]);
        if(c=='%'){
            if(i+2>=raw.size()||hex(raw[i+1])<0||hex(raw[i+2])<0)return false;
            c=static_cast<unsigned char>((hex(raw[i+1])<<4)|hex(raw[i+2]));i+=2;
        }
        if(!c||at==expected.size()||c!=static_cast<unsigned char>(expected[at++]))return false;
    }
    return at==expected.size();
}
inline bool exempt(std::string_view path,bool uri_enabled)noexcept {
    if(path.empty()||path==":memory:")return true; // empty is disposable TEMP, not pure RAM
    if(!uri_enabled||!path.starts_with("file:")||path.find('\0')!=std::string_view::npos||
       path.find('#')!=std::string_view::npos)return false;
    path.remove_prefix(5);if(path.starts_with("//"))return false; // unsupported authority grammar
    const auto q=path.find('?');const auto name=path.substr(0,q);
    if(!valid_component(name))return false;
    const bool anonymous=decoded_equals(name,":memory:");
    if(q==std::string_view::npos)return anonymous;
    auto query=path.substr(q+1);if(query.empty())return false;
    bool mode=false,cache=false;
    for(;;){
        const auto amp=query.find('&');const auto pair=query.substr(0,amp);const auto eq=pair.find('=');
        if(eq==std::string_view::npos||!eq)return false;
        const auto key=pair.substr(0,eq),value=pair.substr(eq+1);
        if(decoded_equals(key,"mode")){
            if(mode||!decoded_equals(value,"memory"))return false;mode=true;
        }else if(decoded_equals(key,"cache")){
            if(cache||(!decoded_equals(value,"shared")&&!decoded_equals(value,"private")))return false;cache=true;
        }else return false; // no VFS/immutable/nolock/unknown policy exemption
        if(amp==std::string_view::npos)break;query.remove_prefix(amp+1);if(query.empty())return false;
    }
    return anonymous||mode;
}
} // namespace lattice::detail::resource_memory_route
