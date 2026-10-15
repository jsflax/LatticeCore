#include "recovery_receipt_coverage.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <string_view>

namespace lattice::detail {
namespace {
[[noreturn]] void refuse(const char* why){throw db_error(why);}
void bounded(const std::string& s,size_t cap){if(s.empty()||s.size()>cap||s.find('\0')!=std::string::npos)refuse("receipt coverage identity outside bound");}
void uuid(const std::string& s){
    if(s.size()!=36)refuse("receipt coverage UUID shape");
    for(size_t i=0;i<s.size();++i)if(i==8||i==13||i==18||i==23){if(s[i]!='-')refuse("receipt coverage UUID shape");}
    else if(!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f')))refuse("receipt coverage UUID must be normalized");
}
void digest(const std::string& s){if(s.size()!=64)refuse("receipt identity digest shape");for(char c:s)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')))refuse("receipt identity digest shape");}
std::string normalized_uuid(std::string s){for(char& c:s)if(c>='A'&&c<='F')c+=('a'-'A');uuid(s);return s;}
class encoder {
    picosha2::hash256_one_by_one hash_;
    size_t bytes_=0;
public:
    void raw(std::string_view v){
        constexpr size_t cap=4194304;
        if(v.size()>cap-bytes_)refuse("original identity encoding exceeds finite byte bound");bytes_+=v.size();
        while(!v.empty()){const auto n=std::min<size_t>(4096,v.size());hash_.process(v.begin(),v.begin()+n);v.remove_prefix(n);}
    }
    void u(uint64_t v){char b[8];for(unsigned i=0;i<8;++i)b[7-i]=static_cast<char>(v>>(8*i));raw({b,8});}
    void s(std::string_view v){u(v.size());raw(v);}
    void scalar(const any_property& p,column_type type){
        if(std::holds_alternative<std::nullptr_t>(p.value)){u(0);return;}
        // SQLite declared types normalize equivalent wire scalar spellings;
        // BLOB generated hex and actual bytes have one immutable encoding.
        if(type==column_type::integer){auto v=std::get_if<int64_t>(&p.value);if(!v)refuse("original identity integer mismatch");u(1);u(static_cast<uint64_t>(*v));return;}
        if(type==column_type::real){double v;if(auto n=std::get_if<double>(&p.value))v=*n;else if(auto n=std::get_if<int64_t>(&p.value))v=static_cast<double>(*n);else refuse("original identity real mismatch");
            if(!std::isfinite(v))refuse("original identity nonfinite real");if(v==0)v=0;u(2);u(std::bit_cast<uint64_t>(v));return;}
        if(type==column_type::text){auto v=std::get_if<std::string>(&p.value);if(!v)refuse("original identity text mismatch");u(3);s(*v);return;}
        u(4);
        if(auto v=std::get_if<std::vector<uint8_t>>(&p.value)){u(v->size());if(!v->empty())raw({reinterpret_cast<const char*>(v->data()),v->size()});return;}
        auto hex=std::get_if<std::string>(&p.value);if(!hex||hex->size()%2)refuse("original identity BLOB mismatch");u(hex->size()/2);
        auto nib=[](char c)->unsigned{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;if(c>='A'&&c<='F')return c-'A'+10;refuse("original identity malformed BLOB hex");};
        for(size_t i=0;i<hex->size();i+=2){const char v=static_cast<char>((nib((*hex)[i])<<4)|nib((*hex)[i+1]));raw({&v,1});}
    }
    std::string finish(){hash_.finish();return picosha2::get_hash_hex_string(hash_);}
};
}
void recovery_producer_registration::validate()const{bounded(registration_id,256);uuid(incarnation);}
void canonical_coverage_profile::validate()const{
    uuid(cohort_id);if(revision<=0||namespaces.empty()||namespaces.size()>64)refuse("receipt cohort bounds");
    std::set<std::string> seen;for(const auto& n:namespaces){bounded(n,256);if(!seen.insert(n).second)refuse("duplicate receipt cohort namespace");}
    if(maximum_origins<8192||maximum_origins>65536||maximum_origin_bytes<8388608||maximum_origin_bytes>67108864||
       maximum_cells<maximum_origins*static_cast<int64_t>(namespaces.size())||maximum_cells>4194304||
       maximum_cell_bytes<maximum_cells*384||maximum_cell_bytes>1610612736)refuse("receipt cohort durable capacity cannot cover admitted originals");
}
void recovery_receipt_binding::validate()const{producer.validate();uuid(cohort_id);if(cohort_revision<=0||operation_codec!=1)refuse("unsupported receipt coverage binding");}
size_t original_identity_bytes(const audit_original_identity& x){
    if(x.version!=1||x.changed_fields_names.size()>32)refuse("original identity metadata bounds");digest(x.digest);
    size_t bytes=80;std::set<std::string> seen;
    for(const auto& n:x.changed_fields_names){bounded(n,64);if(!seen.insert(n).second)refuse("original identity duplicate field");bytes+=8+n.size();}
    return bytes;
}
audit_original_identity make_original_identity(const audit_log_entry& e,
    const std::unordered_map<std::string,column_type>& columns,const std::set<std::string>& no_history,
    const std::string& schema,const recovery_producer_registration& producer,const std::vector<std::string>* original_names){
    producer.validate();digest(schema);bounded(e.table_name,64);bounded(e.timestamp,64);
    if(e.synthesized||e.changed_fields.size()>32||e.changed_fields_names.size()>32||
       (e.operation!="INSERT"&&e.operation!="UPDATE"&&e.operation!="DELETE"))refuse("original identity requires generated operation");
    const auto& supplied=original_names?*original_names:e.changed_fields_names;
    if(supplied.size()>32)refuse("original identity original names exceed bound");
    std::set<std::string> names,projected;
    for(const auto& n:supplied){bounded(n,64);if(n=="id"||n=="globalId"||!columns.count(n)||!names.insert(n).second)refuse("original identity original field differs from schema");}
    for(const auto& n:e.changed_fields_names)if(!names.count(n)||!projected.insert(n).second||!e.changed_fields.count(n))refuse("original identity projection adds or duplicates field");
    for(const auto& [n,v]:e.changed_fields)if(!columns.count(n)||n=="id"||n=="globalId")refuse("original identity unknown payload field");
    encoder h;h.s("lattice.original-operation.v1");h.s(schema);h.s(producer.registration_id);h.s(producer.incarnation);
    h.s(normalized_uuid(e.global_id));h.s(e.table_name);h.s(normalized_uuid(e.global_row_id));h.s(e.operation);h.s(e.timestamp);h.u(names.size());
    for(const auto& n:names){h.s(n);if(e.operation=="UPDATE"&&no_history.count(n)){h.u(1);continue;}
        if(!projected.count(n))refuse("original identity ordinary original value omitted");h.u(0);h.scalar(e.changed_fields.at(n),columns.at(n));}
    return {1,{names.begin(),names.end()},h.finish()};
}
void verify_original_identity(const audit_log_entry& e,const std::unordered_map<std::string,column_type>& columns,
    const std::set<std::string>& no_history,const std::string& schema,const recovery_producer_registration& producer){
    if(!e.original_identity)refuse("registered producer original identity missing");(void)original_identity_bytes(*e.original_identity);
    const auto actual=make_original_identity(e,columns,no_history,schema,producer,&e.original_identity->changed_fields_names);
    if(actual!=*e.original_identity)refuse("registered producer immutable original identity differs");
}
}
