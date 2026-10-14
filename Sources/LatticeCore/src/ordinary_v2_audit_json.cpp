#include "ordinary_v2_audit_json.hpp"
#include "ordinary_v2_property.hpp"
#include <lattice/ordinary_sync.hpp>
#include <algorithm>
#include <array>
#include <set>

namespace lattice::detail {
namespace {
using json=nlohmann::json;using caps=ordinary_sync_limits;
[[noreturn]] void fail(const char* why){throw db_error(std::string("ordinary v2 audit JSON: ")+why);}
bool space(char c){return c==' '||c=='\t'||c=='\n'||c=='\r';}
unsigned hex(char c){if(c>='0'&&c<='9')return unsigned(c-'0');if(c>='a'&&c<='f')return unsigned(c-'a'+10);if(c>='A'&&c<='F')return unsigned(c-'A'+10);fail("invalid Unicode escape");}
// Allocation-free lexical bounds precede SAX's own string/key allocations.
// Full grammar/UTF8 validity and decoded-key duplication are checked by SAX.
size_t lexical_nodes(std::string_view raw,bool names){
    size_t nodes=0,depth=0,at=0;const auto node=[&]{if(++nodes>caps::parser_nodes)fail("node bound");};
    while(at<raw.size()){
        const auto c=raw[at];if(space(c)||c==','||c==':'){++at;continue;}
        if(c=='{'||c=='['){node();if(++depth>4)fail("depth bound");++at;continue;}
        if(c=='}'||c==']'){if(!depth)fail("unmatched close");--depth;++at;continue;}
        if(c=='"'){
            node();++at;size_t bytes=0;bool closed=false;
            while(at<raw.size()){
                const auto value=static_cast<unsigned char>(raw[at++]);if(value=='"'){closed=true;break;}if(value<0x20)fail("unescaped string control");
                size_t copied=1;if(value=='\\'){
                    if(at==raw.size())fail("truncated escape");const auto escape=raw[at++];
                    if(escape=='u'){
                        const auto code=[&](){if(raw.size()-at<4)fail("truncated Unicode escape");unsigned result=0;for(unsigned n=0;n<4;++n)result=(result<<4)|hex(raw[at++]);return result;};
                        const auto first=code();if(first>=0xd800&&first<=0xdbff){if(raw.size()-at<6||raw[at]!='\\'||raw[at+1]!='u')fail("missing low surrogate");at+=2;const auto second=code();if(second<0xdc00||second>0xdfff)fail("invalid low surrogate");copied=4;}
                        else if(first>=0xdc00&&first<=0xdfff)fail("unpaired low surrogate");else copied=first<=0x7f?1:first<=0x7ff?2:3;
                    } else if(escape!='"'&&escape!='\\'&&escape!='/'&&escape!='b'&&escape!='f'&&escape!='n'&&escape!='r'&&escape!='t')fail("invalid escape");
                }
                if(copied>caps::scalar_bytes-bytes)fail("scalar byte bound");bytes+=copied;
            }
            if(!closed)fail("unterminated string");auto next=at;while(next<raw.size()&&space(raw[next]))++next;
            if((names||(next<raw.size()&&raw[next]==':'))&&bytes>256)fail("name byte bound");continue;
        }
        node();while(at<raw.size()&&!space(raw[at])&&raw[at]!=','&&raw[at]!=']'&&raw[at]!='}'&&raw[at]!=':'&&raw[at]!='['&&raw[at]!='{'&&raw[at]!='"')++at;
    }
    if(depth)fail("unclosed container");return nodes;
}
struct sax final:nlohmann::json_sax<json>{
    struct level{bool object;std::set<std::string> keys;};std::vector<level> stack;size_t nodes=0;
    bool node(){return ++nodes<=caps::parser_nodes;}
    bool null()override{return node();}bool boolean(bool)override{return node();}
    bool number_integer(number_integer_t)override{return node();}bool number_unsigned(number_unsigned_t)override{return node();}
    bool number_float(number_float_t value,const string_t&)override{return std::isfinite(value)&&node();}
    bool string(string_t& value)override{return value.size()<=caps::scalar_bytes&&node();}bool binary(binary_t&)override{return false;}
    bool start(bool object){if(stack.size()>=4||!node())return false;stack.push_back({object,{}});return true;}
    bool start_object(size_t)override{return start(true);}bool start_array(size_t)override{return start(false);}
    bool key(string_t& value)override{return !stack.empty()&&stack.back().object&&value.size()<=256&&node()&&stack.back().keys.insert(value).second;}
    bool end_object()override{if(stack.empty()||!stack.back().object)return false;stack.pop_back();return true;}
    bool end_array()override{if(stack.empty()||stack.back().object)return false;stack.pop_back();return true;}
    bool parse_error(size_t,const std::string&,const nlohmann::detail::exception&)override{return false;}
};
void validate_json(std::string_view raw,size_t expected_nodes){sax check;if(!json::sax_parse(raw.begin(),raw.end(),&check)||check.nodes!=expected_nodes)fail("invalid or duplicate-key JSON");}
void name(std::string_view value){if(value.empty()||value.size()>256||value.find('\0')!=std::string_view::npos)fail("property name bound or NUL");}
}
ordinary_v2_audit_values parse_ordinary_v2_audit_values(std::string_view raw_fields,std::string_view raw_names,
    const ordinary_v2_retained_charge& charge,const ordinary_v2_workspace_acquire& acquire){
    if(!charge||!acquire)fail("actual retained/workspace admission required");
    if(raw_fields.empty()||raw_names.empty()||raw_fields.size()>caps::frame_bytes||raw_names.size()>caps::frame_bytes-raw_fields.size())fail("input byte bound");
    const auto field_nodes=lexical_nodes(raw_fields,false),name_nodes=lexical_nodes(raw_names,true);if(name_nodes>caps::parser_nodes-field_nodes)fail("combined node bound");
    const auto wire_bytes=raw_fields.size()+raw_names.size(),nodes=field_nodes+name_nodes;
    const auto copies=wire_bytes*caps::parser_wire_copies;
    if(copies>caps::parser_workspace_bytes||nodes>(caps::parser_workspace_bytes-copies)/caps::parser_node_logical_bytes)fail("workspace byte bound");
    // Declared before every parser/DOM object, destroyed after all of them.
    auto workspace=acquire(copies+nodes*caps::parser_node_logical_bytes);if(!workspace)fail("workspace ticket unavailable");
    ordinary_v2_audit_values result;
    {
        validate_json(raw_fields,field_nodes);validate_json(raw_names,name_nodes);
        const auto fields=json::parse(raw_fields.begin(),raw_fields.end()),names=json::parse(raw_names.begin(),raw_names.end());
        if(!fields.is_object()||fields.size()>caps::columns_per_table||!names.is_array()||names.size()>caps::columns_per_table)fail("field/name shape bound");
        size_t retained=sizeof(result);const auto add=[&](size_t amount){if(amount>caps::decoded_receive_bytes-retained)fail("retained byte bound");retained+=amount;};
        add(fields.size()*(sizeof(changed_fields_map::value_type)+4*sizeof(void*)));
        for(auto item=fields.begin();item!=fields.end();++item){name(item.key());add(item.key().size()+1);add(inspect_ordinary_v2_property(item.value(),caps::scalar_bytes));}
        std::array<std::string_view,caps::columns_per_table> seen{};size_t count=0;
        for(const auto& value:names){if(value.is_null())continue;if(!value.is_string())fail("changed name type");const auto& text=value.get_ref<const std::string&>();name(text);seen[count++]=text;add(sizeof(std::string)+text.size()+1);}
        std::sort(seen.begin(),seen.begin()+static_cast<ptrdiff_t>(count));if(std::adjacent_find(seen.begin(),seen.begin()+static_cast<ptrdiff_t>(count))!=seen.begin()+static_cast<ptrdiff_t>(count))fail("duplicate changed name");
        charge(retained);result.fields.reserve(fields.size());result.names.reserve(count);
        for(auto item=fields.begin();item!=fields.end();++item)result.fields.emplace(item.key(),decode_ordinary_v2_property(item.value(),caps::scalar_bytes));
        for(const auto& value:names)if(value.is_string())result.names.push_back(value.get<std::string>());
    }
    return result;
}
}
