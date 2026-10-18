#include "ordinary_schema_validation.hpp"
#include "ordinary_initialization_storage.hpp"
#include <lattice/derived_program.hpp>
#include <lattice/ordinary_schema_recipe.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <sqlite-vec.h>
#include <algorithm>
#include <limits>
#include <set>
#include <mutex>
#include <charconv>
#include <initializer_list>

namespace lattice::detail {
// Source-private passive access to the two complete compiled recipes only.
// This is neither a SQL executor nor a schema/profile permission issuer.
struct ordinary_schema_validation_recipe_access final {
    static const char* audit_pending()noexcept{return ordinary_schema_recipe::audit_pending_index();}
    static const char* sync_state_pending()noexcept{return ordinary_schema_recipe::sync_state_pending();}
};
using preimage_owned_text=std::basic_string<char,std::char_traits<char>,ordinary_initialization_allocator<char>>;
struct ordinary_preimage_statement::state final {
    const kind purpose;
    const size_t row_limit,result_byte_limit,parameter_bytes;
    preimage_owned_text sql;
    std::vector<preimage_owned_text,ordinary_initialization_allocator<preimage_owned_text>> parameters;
    // Compatibility copies never participate in an actual Core initialization
    // request. Keep generic getters source-compatible and synchronized; their
    // first use irreversibly excludes this statement from Native issuance.
    mutable std::mutex legacy_mutex;
    mutable bool native_bound=false,legacy_used=false;
    mutable std::optional<std::string> legacy_sql;
    mutable std::optional<std::vector<column_value_t>> legacy_parameters;
    state(kind id,size_t rows,size_t bytes,size_t params)noexcept
        :purpose(id),row_limit(rows),result_byte_limit(bytes),parameter_bytes(params){}
};
bool ordinary_preimage_statement::creator_current()const noexcept {
    return creator_==std::uint64_t(::getpid());
}
void ordinary_preimage_statement::require_current_()const {
    if(!creator_current()||!state_)throw ordinary_schema_validation_error("inherited preimage statement");
}
ordinary_preimage_statement::~ordinary_preimage_statement()noexcept {
    if(creator_current())delete state_;
}
void ordinary_preimage_statement::destroy_(const ordinary_preimage_statement* value)noexcept {
    if(value&&value->creator_current())delete value;
}
ordinary_preimage_statement::kind ordinary_preimage_statement::kind_id()const {
    require_current_();return state_->purpose;
}
void ordinary_preimage_statement::enter_native_()const {
    require_current_();std::lock_guard<std::mutex> lock(state_->legacy_mutex);
    if(state_->legacy_used)
        throw ordinary_schema_validation_error("legacy preimage copies cannot enter initialization");
    state_->native_bound=true;
}
const std::string& ordinary_preimage_statement::sql()const {
    require_current_();std::lock_guard<std::mutex> lock(state_->legacy_mutex);
    if(state_->native_bound)throw ordinary_schema_validation_error("Native preimage SQL requires its borrowed view");
    state_->legacy_used=true; // Monotonic BEFORE an allocation attempt can throw.
    if(!state_->legacy_sql)state_->legacy_sql.emplace(state_->sql.data(),state_->sql.size());
    return *state_->legacy_sql;
}
const std::vector<column_value_t>& ordinary_preimage_statement::parameters()const {
    require_current_();std::lock_guard<std::mutex> lock(state_->legacy_mutex);
    if(state_->native_bound)throw ordinary_schema_validation_error("Native preimage parameters require borrowed views");
    state_->legacy_used=true; // Includes failed vector/string construction.
    if(!state_->legacy_parameters){
        std::vector<column_value_t> copy;copy.reserve(state_->parameters.size());
        for(const auto& parameter:state_->parameters)copy.emplace_back(std::string(parameter.data(),parameter.size()));
        state_->legacy_parameters.emplace(std::move(copy));
    }
    return *state_->legacy_parameters;
}
std::string_view ordinary_preimage_statement::sql_view()const {
    require_current_();return {state_->sql.data(),state_->sql.size()};
}
size_t ordinary_preimage_statement::parameter_count()const {require_current_();return state_->parameters.size();}
std::string_view ordinary_preimage_statement::parameter_text(size_t index)const {
    require_current_();
    if(index>=state_->parameters.size())throw ordinary_schema_validation_error("preimage parameter index differs");
    const auto& value=state_->parameters[index];return {value.data(),value.size()};
}
bool ordinary_preimage_statement::vector_connect_read_(const char* table,const char* column,const char* schema)const noexcept {
    if(!creator_current()||!state_||state_->purpose!=kind::module_vector_columns||
       state_->parameters.size()!=1||!table||!column||!schema||sqlite3_stricmp(schema,"main")!=0)
        return false;
    const auto& target=state_->parameters.front();
    if(target.empty()||target.size()>251)return false; // actual inventory names including _info are at most256
    const auto fold=[](unsigned char c)noexcept{return c>='A'&&c<='Z'?c+('a'-'A'):c;};
    std::size_t index=0;
    for(;index<target.size();++index)
        if(!table[index]||fold(static_cast<unsigned char>(table[index]))!=fold(static_cast<unsigned char>(target[index])))return false;
    constexpr char suffix[]="_info";
    for(std::size_t part=0;part<5;++part,++index)
        if(!table[index]||fold(static_cast<unsigned char>(table[index]))!=static_cast<unsigned char>(suffix[part]))return false;
    return !table[index]&&(sqlite3_stricmp(column,"key")==0||sqlite3_stricmp(column,"value")==0);
}
size_t ordinary_preimage_statement::row_limit()const {require_current_();return state_->row_limit;}
size_t ordinary_preimage_statement::result_byte_limit()const {require_current_();return state_->result_byte_limit;}
size_t ordinary_preimage_statement::parameter_bytes()const {require_current_();return state_->parameter_bytes;}
std::shared_ptr<const ordinary_preimage_statement>
ordinary_initialization_schema_validation::final_statement_(final_read selected) {
    using kind=ordinary_preimage_statement::kind;
    kind id=kind::unissued;const char* sql=nullptr;size_t rows=0,bytes=0;
    switch(selected) {
    case final_read::meter_namespace:id=kind::main_inventory;sql="SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema ORDER BY name COLLATE BINARY LIMIT 65537";rows=65536;bytes=32*1024*1024;break;
    case final_read::sync_control_row:id=kind::final_sync_control_row;sql="SELECT id,disabled FROM main._SyncControl LIMIT 2";rows=2;bytes=1024;break;
    case final_read::meter_row:id=kind::final_meter_row;sql="SELECT id,typeof(token) AS token_type,length(token) AS token_bytes,CASE WHEN typeof(events)='integer' THEN events END AS events,CASE WHEN typeof(bytes)='integer' THEN bytes END AS bytes FROM main._lattice_observer_delivery_meter_v1 LIMIT 2";rows=2;bytes=2048;break;
    case final_read::metadata_rows:id=kind::final_metadata_rows;sql="SELECT key,value FROM main._lattice_meta WHERE key='schema_version' OR key='lattice_swift_schema' OR substr(key,1,19)='schema_fingerprint:' OR substr(key,1,14)='trigger_flags:' OR substr(key,1,15)='internal_table:' ORDER BY key COLLATE BINARY LIMIT 65537";rows=65536;bytes=32*1024*1024;break;
    case final_read::encoding:id=kind::final_encoding;sql="PRAGMA main.encoding";rows=1;bytes=256;break;
    case final_read::recursive_triggers:id=kind::final_recursive_triggers;sql="PRAGMA recursive_triggers";rows=1;bytes=256;break;
    case final_read::foreign_keys:id=kind::final_foreign_keys;sql="PRAGMA foreign_keys";rows=1;bytes=256;break;
    case final_read::databases:id=kind::final_databases;sql="PRAGMA database_list";rows=32;bytes=8192;break;
    }
    if(!sql||id==kind::unissued)throw ordinary_schema_validation_error("unissued final metadata recipe");
    const auto creator=std::uint64_t(::getpid());
    ordinary_initialization_storage_scope::charge(native_retirement_storage<std::shared_ptr<const ordinary_preimage_statement>>::allocation_bytes());
    native_retirement_storage<std::shared_ptr<const ordinary_preimage_statement>> retained;
    if(creator!=std::uint64_t(::getpid()))throw ordinary_schema_validation_error("inherited final metadata custody");
    ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement::state));
    auto* held=new ordinary_preimage_statement::state(id,rows,bytes,0);
    if(creator!=std::uint64_t(::getpid()))throw ordinary_schema_validation_error("inherited final metadata allocation");
    ordinary_preimage_statement* wrapper=nullptr;bool owned=false;
    try {
        ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement));
        wrapper=new ordinary_preimage_statement(creator,held);owned=true;
        if(creator!=std::uint64_t(::getpid()))throw ordinary_schema_validation_error("inherited final metadata wrapper");
        held->sql.assign(sql);
        if(creator!=std::uint64_t(::getpid()))throw ordinary_schema_validation_error("inherited final metadata SQL copy");
        const auto* entrusted=wrapper;wrapper=nullptr;
        retained.get<0>()=std::shared_ptr<const ordinary_preimage_statement>(entrusted,
            &ordinary_preimage_statement::destroy_,ordinary_initialization_allocator<ordinary_preimage_statement>{});
        if(creator!=std::uint64_t(::getpid()))throw ordinary_schema_validation_error("inherited final metadata publication");
        return std::move(retained.get<0>());
    }catch(...) {if(creator==std::uint64_t(::getpid())){if(wrapper)delete wrapper;else if(!owned)delete held;}throw;}
}
namespace {
using preimage_kind=ordinary_preimage_statement::kind;
// The closed producer takes borrowed synchronous inputs. In particular a
// zero/one parameter list never first allocates a standard variant/vector.
using preimage_reader_query=ordinary_initialization_function_ref<std::vector<database::row_t>(preimage_kind,
    std::string_view,std::initializer_list<std::string_view>,size_t,size_t)>;
using token_text=std::basic_string<char,std::char_traits<char>,ordinary_initialization_allocator<char>>;
// All token characters and vector growth use the actual prospective allocator.
// These synchronous token sequences never outlive their owning SQL/value data.
// Unlike a token-count multiplier, growth/copy debits follow the implementation's
// real char and vector allocation requests, including transient old+new storage.
class tokens final : public std::vector<token_text,ordinary_initialization_allocator<token_text>> {
    using base=std::vector<token_text,ordinary_initialization_allocator<token_text>>;
public:
    tokens()=default;
    tokens(const tokens&)=default;tokens(tokens&&)noexcept=default;
    tokens& operator=(const tokens&)=default;tokens& operator=(tokens&&)noexcept=default;
    tokens(std::initializer_list<std::string_view> values) {
        this->reserve(values.size());
        for(const auto value:values)this->emplace_back(value.data(),value.size());
    }
    template<class Iterator>tokens(Iterator first,Iterator last):base(first,last){}
    friend bool operator==(const tokens& left,const tokens& right){return static_cast<const base&>(left)==static_cast<const base&>(right);}
    using base::push_back;
    void push_back(std::string_view value){this->emplace_back(value.data(),value.size());}
    void push_back(const char* value){push_back(std::string_view(value));}
};
// Every new owning scalar/parser value uses actual allocator requests.
ordinary_initialization_text own(std::string_view value) {
    return ordinary_initialization_text(value.empty()?"":value.data(),value.size());
}
ordinary_initialization_text decimal(size_t value) {
    char digits[std::numeric_limits<size_t>::digits10+1];
    const auto converted=std::to_chars(digits,digits+sizeof(digits),value);
    if(converted.ec!=std::errc{})throw ordinary_schema_validation_error("ordinary metadata decimal conversion");
    return ordinary_initialization_text(digits,converted.ptr);
}

void need(bool ok,const char* message) { if(!ok)throw ordinary_schema_validation_error(message); }
ordinary_initialization_text fold(std::string_view source){auto s=own(source);for(char& c:s)if(c>='A'&&c<='Z')c+=32;return s;}
bool word(char c){return(c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='_'||(c>='0'&&c<='9');}
void identifier(std::string_view s){need(!s.empty()&&s.size()<=256,"ordinary metadata identifier bound");
    need(!(s[0]>='0'&&s[0]<='9'),"ordinary metadata identifier start");for(char c:s)need(word(c),"ordinary metadata identifier spelling");}
ordinary_initialization_text ordinary_identifier_sql_(std::string_view s){identifier(s);return "\""+own(s)+"\"";}
const column_value_t& cell(const database::row_t& r,const char* key){auto i=r.find(key);need(i!=r.end(),"ordinary metadata missing result field");return i->second;}
int64_t number(const database::row_t& r,const char* key){const auto& v=cell(r,key);need(std::holds_alternative<int64_t>(v),"ordinary metadata noninteger");return std::get<int64_t>(v);}
std::string_view text_view(const database::row_t& r,const char* key){const auto& v=cell(r,key);std::string_view s;
    if(auto p=std::get_if<std::string>(&v))s=*p;
    else if(auto p=std::get_if<std::vector<uint8_t>>(&v)){if(!p->empty())s={reinterpret_cast<const char*>(p->data()),p->size()};}
    else need(false,"ordinary metadata nontext");need(s.find('\0')==std::string_view::npos,"ordinary metadata NUL");return s;}
ordinary_initialization_text text(const database::row_t& r,const char* key){return own(text_view(r,key));}
// Retain actual Request TEXT by move. BLOB spellings remain accepted through
// the same byte/NUL checks, with new characters on the actual allocator.
ordinary_inventory_text take_inventory_text(database::row_t& row,const char* key) {
    auto found=row.find(key);need(found!=row.end(),"ordinary metadata missing result field");
    if(auto* value=std::get_if<std::string>(&found->second)) {
        need(value->find('\0')==std::string::npos,"ordinary metadata NUL");
        return std::move(*value);
    }
    return text(row,key);
}
bool is_null(const database::row_t& r,const char* key){return std::holds_alternative<std::nullptr_t>(cell(r,key));}
ordinary_object_metadata take_inventory_object(database::row_t& row) {
    // Preserve original left-to-right field/type/NUL checks, and observe the
    // nullable SQL cell before transferring it. Returned strings own storage.
    return {take_inventory_text(row,"type"),take_inventory_text(row,"name"),
        take_inventory_text(row,"owner"),is_null(row,"sql")?ordinary_inventory_text{}:take_inventory_text(row,"sql")};
}

size_t size_of(const column_value_t& v){if(auto p=std::get_if<std::string>(&v))return p->size();if(auto p=std::get_if<std::vector<uint8_t>>(&v))return p->size();return 8;}
// Generated decimal LIMIT suffixes use their real allocator request. The
// fixed stack conversion cannot allocate and preserves the old decimal SQL.
ordinary_initialization_text limit_sql(std::string_view prefix,size_t limit) {
    char digits[std::numeric_limits<size_t>::digits10+1];
    const auto converted=std::to_chars(digits,digits+sizeof(digits),limit);
    need(converted.ec==std::errc{},"ordinary metadata LIMIT conversion");
    ordinary_initialization_text sql(prefix.data(),prefix.size());
    sql.append(digits,converted.ptr);return sql;
}
struct reader {
    const ordinary_metadata_query& query;const ordinary_metadata_limits& limits;size_t rows=0,bytes=0;
    const preimage_reader_query* issued_query=nullptr;
    std::vector<database::row_t> get(std::string_view sql,std::initializer_list<std::string_view> args,size_t cap,preimage_kind kind=preimage_kind::unissued){
        need(cap&&cap<=limits.rows&&sql.size()<=limits.sql_bytes,"ordinary metadata query bounds");
        const auto remaining=limits.rows-rows;need(remaining>0,"ordinary metadata aggregate row bound");
        const auto admitted=std::min(cap,remaining);
        std::vector<database::row_t> result;
        if(issued_query)result=(*issued_query)(kind,sql,args,admitted,limits.bytes-bytes);
        else {
            // Preserve the original ordinary callback API outside startup.
            // Refuse BEFORE any standard owning input allocation under the
            // actual pending account; it has only the typed borrowed route.
            auto legacy_sql=ordinary_initialization_legacy_text(sql);
            std::vector<column_value_t> legacy_args;legacy_args.reserve(args.size());
            for(const auto value:args)legacy_args.emplace_back(ordinary_initialization_legacy_text(value));
            result=query(legacy_sql,legacy_args,admitted);
        }
        need(result.size()<=admitted,"ordinary metadata callback overrun");
        need(result.size()<=limits.rows-rows,"ordinary metadata aggregate row bound");
        for(const auto& r:result){need(r.size()<=16,"ordinary metadata field bound");for(const auto& [key,value]:r){
            const auto n=size_of(value);need(n<=limits.bytes-bytes,"ordinary metadata aggregate byte bound");bytes+=n;
            need(key.size()<=256&&key.size()<=limits.bytes-bytes,"ordinary metadata key bound");bytes+=key.size();}}
        rows+=result.size();return result;
    }
    int64_t cookie(const char* schema,preimage_kind kind=preimage_kind::unissued){
        ordinary_initialization_text sql="PRAGMA ";sql.append(schema);sql.append(".schema_version");
        auto r=get(sql,{},1,kind);need(r.size()==1,"ordinary retained schema cookie missing");return number(r[0],"schema_version");}
};
// Closed SQL grammar: comments, unquoted non-ASCII names, parameters and any
// unrecognized token refuse. String bytes/case remain exact; identifier quotes
// and ASCII case are incidental. This is not a general SQL canonicalizer.
tokens lex(std::string_view s){tokens out;for(size_t i=0;i<s.size();){const char c=s[i];
    if(c==' '||c=='\t'||c=='\r'||c=='\n'){++i;continue;}
    if(c=='\''||c=='"'||c=='`'){const char q=c;++i;token_text x;bool closed=false;while(i<s.size()){
        const char a=s[i++];if(a==q){if(i<s.size()&&s[i]==q){x+=q;++i;}else{closed=true;break;}}else x+=a;}
        need(closed,"ordinary SQL unterminated quote");if(q=='\'')out.push_back("'"+x+"'");else {identifier(x);for(char& a:x)if(a>='A'&&a<='Z')a+=32;out.push_back(std::move(x));}continue;}
    if(word(c)){auto start=i++;while(i<s.size()&&(word(s[i])||(s[i]=='.'&&s[start]>='0'&&s[start]<='9')))++i;
        token_text value(s.data()+start,i-start);for(char& a:value)if(a>='A'&&a<='Z')a+=32;out.push_back(std::move(value));continue;}
    // Generated union cascades spell inequality exactly !=. A bare ! is not SQL.
    if(c=='!'&&i+1<s.size()&&s[i+1]=='='){out.push_back("!=");i+=2;continue;}
    need(std::string_view("(),;.=+-*%|[]<>").find(c)!=std::string_view::npos,"ordinary SQL token refused");
    need(!(c=='-'&&i+1<s.size()&&s[i+1]=='-'),"ordinary SQL comment refused");
    if(c=='|'&&i+1<s.size()&&s[i+1]=='|'){out.push_back("||");i+=2;}else {out.emplace_back(1,c);++i;}}
    if(!out.empty()&&out.back()==";")out.pop_back();return out;
}
bool contains(const tokens& v,const tokens& pattern){return std::search(v.begin(),v.end(),pattern.begin(),pattern.end())!=v.end();}
void remove_if(tokens& v){if(v.size()<3||v[0]!="create")return;const size_t i=(v[1]=="unique"||v[1]=="virtual")?3:2;if(i+2<v.size()&&v[i]=="if"&&v[i+1]=="not"&&v[i+2]=="exists")v.erase(v.begin()+i,v.begin()+i+3);}
ordinary_initialization_text object_name(std::string_view sql,std::string_view kind){auto t=lex(sql);remove_if(t);size_t at=1;if(at<t.size()&&t[at]=="unique")++at;
    need(t.size()>at+1&&t[0]=="create"&&std::string_view(t[at])==std::string_view(kind),"ordinary generated object prefix");return own(t[at+1]);}
bool same_program(std::string_view a,std::string_view b){auto x=lex(a),y=lex(b);remove_if(x);remove_if(y);return x==y;}
// Recover ordering DATA only. Even a successful extraction must subsequently
// reproduce one complete approved typed bundle through unchanged same_program.
// Never interpret arbitrary SQL expressions or search column permutations.
std::optional<ordinary_initialization_vector<ordinary_initialization_text>> audit_insert_order(std::string_view sql){
    const auto t=lex(sql);std::optional<ordinary_initialization_vector<ordinary_initialization_text>> result;
    for(size_t i=0;i+1<t.size();++i)if(t[i]=="json_array"&&t[i+1]=="("){
        if(result)return {};ordinary_initialization_vector<ordinary_initialization_text> order;size_t at=i+2;
        for(;;){
            if(at>=t.size()||order.size()>=ordinary_sync_limits::columns_per_table)return {};
            const auto& value=t[at++];
            if(value.size()<3||value.front()!='\''||value.back()!='\'')return {};
            order.push_back(own(std::string_view(value).substr(1,value.size()-2)));
            if(at>=t.size())return {};
            if(t[at]==")")break;
            if(t[at++]!=",")return {};
        }
        result=std::move(order);
    }
    return result;
}
struct table_ddl {ordinary_initialization_text_map<tokens> columns;ordinary_initialization_vector<ordinary_initialization_text> order;ordinary_initialization_vector<tokens> constraints;bool without_rowid=false;};
table_ddl parse_table(std::string_view sql,std::string_view name,bool initialization_unique=false){auto t=lex(sql);remove_if(t);size_t i=2;
    need(t.size()>4&&t[0]=="create"&&t[1]=="table","ordinary table declaration kind");if(i+2<t.size()&&t[i]=="main"&&t[i+1]==".")i+=2;
    need(i+1<t.size()&&std::string_view(t[i++])==fold(name)&&t[i++]=="(","ordinary table declaration identity");table_ddl d;tokens part;size_t depth=0;bool ended=false;
    const auto finish=[&]{need(!part.empty(),"ordinary empty table clause");if(part[0]=="check"||part[0]=="primary"||(initialization_unique&&part[0]=="unique"))d.constraints.push_back(part);
        else{identifier(part[0]);need(d.columns.emplace(own(part[0]),part).second,"ordinary duplicate table column");d.order.push_back(own(part[0]));}part.clear();};
    for(;i<t.size();++i){if(t[i]=="(" ){++depth;part.push_back(t[i]);}
        else if(t[i]==")"){if(depth){--depth;part.push_back(t[i]);}else{finish();++i;ended=true;break;}}
        else if(t[i]==","&&!depth)finish();else part.push_back(t[i]);}
    need(ended,"ordinary table declaration close");if(i<t.size()){need(i+2==t.size()&&t[i]=="without"&&t[i+1]=="rowid","ordinary table suffix refused");d.without_rowid=true;}
    return d;
}
struct index_spec {ordinary_initialization_text name,origin,sql;ordinary_initialization_vector<ordinary_initialization_text> keys;bool unique=true,partial=false;};
ordinary_initialization_vector<ordinary_initialization_text> key_list(const tokens& t,size_t at){need(at<t.size()&&t[at++]=="(","ordinary index key open");ordinary_initialization_vector<ordinary_initialization_text> keys;
    while(at<t.size()&&t[at]!=")"){identifier(t[at]);keys.push_back(own(t[at++]));if(at<t.size()&&t[at]==","){++at;continue;}break;}
    need(!keys.empty()&&at+1==t.size()&&t[at]==")","ordinary index expression/options refused");return keys;}
ordinary_initialization_vector<index_spec> implicit_indexes(const table_ddl& d,std::string_view table,bool initialization_order=false){ordinary_initialization_vector<index_spec> out;size_t ordinal=0;
    for(const auto& n:d.order){const auto& c=d.columns.at(n);if(contains(c,{"unique"})||contains(c,{"primary","key"})){
        if(contains(c,{"primary","key"})&&c.size()>1&&c[1]=="integer"&&!d.without_rowid)continue;
        out.push_back({"sqlite_autoindex_"+own(table)+"_"+decimal(++ordinal),contains(c,{"primary","key"})?"pk":"u",{}, {n},true});}}
    if(initialization_order){
        // Preserve actual table-constraint declaration order. Reordering
        // UNIQUE before/after PRIMARY changes SQLite's autoindex ordinals.
        for(const auto& c:d.constraints){
            if(c[0]=="primary"){need(c.size()>2&&c[1]=="key","ordinary primary key syntax");out.push_back({"sqlite_autoindex_"+own(table)+"_"+decimal(++ordinal),"pk",{},key_list(c,2),true});}
            else if(c[0]=="unique")out.push_back({"sqlite_autoindex_"+own(table)+"_"+decimal(++ordinal),"u",{},key_list(c,1),true});
        }
    }else{
        // Old generic metadata reader retains its original grammar/order.
        for(const auto& c:d.constraints)if(c[0]=="primary"){need(c.size()>2&&c[1]=="key","ordinary primary key syntax");out.push_back({"sqlite_autoindex_"+own(table)+"_"+decimal(++ordinal),"pk",{},key_list(c,2),true});}
    }
    return out;
}
enum class initialization_partial_recipe {none,audit_pending,sync_state_pending};
index_spec explicit_index(std::string_view sql,std::string_view table,
    initialization_partial_recipe approved=initialization_partial_recipe::none){auto t=lex(sql);remove_if(t);size_t i=1;bool unique=false;
    if(i<t.size()&&t[i]=="unique"){unique=true;++i;}need(t.size()>i+4&&t[0]=="create"&&t[i++]=="index","ordinary explicit index syntax");
    const auto name=t[i++];identifier(name);need(t[i++]=="on"&&std::string_view(t[i++])==fold(table),"ordinary explicit index owner");
    bool partial=false;const auto where=std::find(t.begin()+i,t.end(),"where");
    if(where!=t.end()){
        // Only these two complete compiled recipes, selected by the typed
        // initialization reader, have a partial-index grammar. Neither an
        // observed WHERE nor copied predicate/keys can issue another recipe.
        // The old generic/export reader retains its no-partial default.
        const bool audit=approved==initialization_partial_recipe::audit_pending&&
            table=="AuditLog"&&sql==ordinary_schema_validation_recipe_access::audit_pending()&&
            tokens(where,t.end())==tokens{"where","issynchronized","=","0"};
        const bool sync=approved==initialization_partial_recipe::sync_state_pending&&
            table=="_lattice_sync_state"&&sql==ordinary_schema_validation_recipe_access::sync_state_pending()&&
            tokens(where,t.end())==tokens{"where","is_synchronized","=","0"};
        need(audit||sync,"ordinary unissued partial index recipe");
        t.erase(where,t.end());partial=true;
    }
    return{own(name),"c",own(sql),key_list(t,i),unique,partial};}
std::optional<tokens> default_tokens(const tokens& c){auto i=std::find(c.begin(),c.end(),"default");if(i==c.end())return {};return tokens(i+1,c.end());}
tokens unparenthesized(tokens t){for(;;){if(t.size()<2||t.front()!="("||t.back()!=")")return t;size_t depth=0;bool wraps=true;for(size_t i=0;i<t.size();++i){if(t[i]=="(")++depth;else if(t[i]==")"){need(depth>0,"ordinary default parentheses");--depth;if(!depth&&i+1<t.size()){wraps=false;break;}}}if(!wraps)return t;t.erase(t.begin());t.pop_back();}}
std::string_view type_of(const tokens& c){if(c.size()<2||c[1]=="primary"||c[1]=="not"||c[1]=="unique")return {};return c[1];}
// Only classify an unrelated table's declaration prefix. Its remaining SQL
// need not belong to our generated grammar; it confers no owned-table grant.
// Refuse an ambiguous prefix instead of instantiating an unknown module.
bool virtual_declaration(std::string_view sql){size_t at=0;const auto next=[&]{while(at<sql.size()&&(sql[at]==' '||sql[at]=='\n'||sql[at]=='\t'||sql[at]=='\r'))++at;const auto start=at;while(at<sql.size()&&word(sql[at]))++at;need(at>start,"ordinary unrelated declaration prefix refused");return fold(sql.substr(start,at-start));};
    need(next()=="create","ordinary unrelated declaration not CREATE");const auto kind=next();need(kind=="table"||kind=="virtual","ordinary unrelated table prefix refused");return kind=="virtual";
}
using inventory_map=ordinary_initialization_text_map<ordinary_object_metadata>;
using inventory_names=ordinary_initialization_text_set;
struct ordinary_object_metadata_view {std::string_view name,owner,sql;};
struct validator {
    reader read;ordinary_schema_metadata result;inventory_map all;inventory_names allowed,owned;
    ordinary_initialization_vector<ordinary_initialization_text> families;
    std::vector<database::row_t> temp_inventory;
    bool initialization_prefix=false; // private typed construction only; strict readers default false
    const ordinary_object_metadata& fact(std::string_view name,std::string_view type,std::string_view owner){auto i=all.find(fold(name));
        need(i!=all.end()&&i->second.type==type&&fold(i->second.owner)==fold(owner),"ordinary required metadata object missing/owner mismatch");return i->second;}
    void keep(const ordinary_object_metadata& f){need(allowed.insert(fold(f.name)).second,"ordinary overlapping object ownership");
        // Only the ordinary metadata API returns this duplicate list. Both
        // typed preimage workers assemble main_objects from all after complete
        // ownership/schema validation and never read result.objects.
        if(!read.issued_query)result.objects.push_back(f);}
    void inventory(){auto r=read.get(limit_sql("SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema ORDER BY name COLLATE BINARY LIMIT ",read.limits.objects+1),{},read.limits.objects,preimage_kind::main_inventory);
        for(auto& row:r){auto f=take_inventory_object(row);
            need(f.name.size()<=256&&f.owner.size()<=256&&f.sql.size()<=read.limits.sql_bytes,"ordinary schema object size");
            need(all.emplace(fold(f.name),std::move(f)).second,"ordinary case-folded schema object collision");}}
    ordinary_initialization_vector<ordinary_column_metadata> columns(std::string_view table){auto r=read.get(limit_sql("SELECT cid,name,type,\"notnull\" AS nn,dflt_value AS def,pk,hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid LIMIT ",ordinary_sync_limits::columns_per_table+2),{table},ordinary_sync_limits::columns_per_table+1,preimage_kind::table_columns);
        ordinary_initialization_vector<ordinary_column_metadata> out;ordinary_initialization_text_set names;
        for(const auto& row:r){ordinary_column_metadata c{text(row,"name"),fold(text(row,"type")),number(row,"cid"),number(row,"nn"),number(row,"pk"),number(row,"hidden"),{}};
            need(c.cid==int64_t(out.size())&&names.insert(fold(c.name)).second,"ordinary column identity/order collision");
            if(!is_null(row,"def"))c.default_sql=text(row,"def");out.push_back(std::move(c));}return out;}
    void foreign_keys(std::string_view table){need(read.get("SELECT id FROM pragma_foreign_key_list(?, 'main') LIMIT 1",{table},1,preimage_kind::table_foreign_keys).empty(),"ordinary owned foreign key refused");}
    void indexes(std::string_view table,const table_ddl& ddl,const ordinary_initialization_vector<ordinary_column_metadata>& cols,const auto& explicit_sql,bool expose){
        auto specs=implicit_indexes(ddl,table,bool(read.issued_query));
        for(const auto& sql:explicit_sql) {
            auto approved=initialization_partial_recipe::none;
            if(read.issued_query) {
                if(table=="AuditLog"&&sql==ordinary_schema_validation_recipe_access::audit_pending())
                    approved=initialization_partial_recipe::audit_pending;
                else if(table=="_lattice_sync_state"&&sql==ordinary_schema_validation_recipe_access::sync_state_pending())
                    approved=initialization_partial_recipe::sync_state_pending;
            }
            specs.push_back(explicit_index(sql,table,approved));
        }
        ordinary_initialization_text_map<index_spec> expected;for(auto& s:specs)need(expected.emplace(fold(s.name),std::move(s)).second,"ordinary index name collision");
        auto list=read.get(limit_sql("SELECT name,\"unique\" AS uniq,origin,partial FROM pragma_index_list(?, 'main') LIMIT ",expected.size()+1),{table},std::max<size_t>(expected.size(),1),preimage_kind::index_list);
        need(initialization_prefix?list.size()<=expected.size():list.size()==expected.size(),"ordinary missing/extra index");ordinary_initialization_text_set seen;
        for(const auto& row:list){const auto name=text(row,"name");auto e=expected.find(fold(name));need(e!=expected.end()&&seen.insert(e->first).second,"ordinary unknown index");const auto& s=e->second;
            need(number(row,"uniq")==int64_t(s.unique)&&text(row,"origin")==s.origin&&number(row,"partial")==int64_t(s.partial),"ordinary index constraint mismatch");
            if(!(ddl.without_rowid&&s.origin=="pk")){const auto& f=fact(name,"index",table);need(s.origin=="c"?same_program(f.sql,s.sql):f.sql.empty(),"ordinary index program mismatch");keep(f);}
            else need(!all.count(fold(name)),"ordinary without-rowid index object mismatch");
            ordinary_initialization_vector<ordinary_initialization_text> order=s.keys;if(ddl.without_rowid){
                if(!read.issued_query||s.origin=="pk"){
                    // The actual WITHOUT ROWID primary index stores payload
                    // columns too. Keep old generic-reader behavior unchanged.
                    for(const auto& c:cols)if(std::find(order.begin(),order.end(),fold(c.name))==order.end())order.push_back(fold(c.name));
                }else{
                    // A secondary index carries only its key plus missing
                    // PRIMARY KEY columns, never nonkey table payload.
                    const auto primary=std::find_if(expected.begin(),expected.end(),[](const auto& x){return x.second.origin=="pk";});
                    need(primary!=expected.end(),"ordinary WITHOUT ROWID primary index missing");
                    for(const auto& key:primary->second.keys)if(std::find(order.begin(),order.end(),key)==order.end())order.push_back(key);
                }
            }else order.push_back({});
            auto detail=read.get(limit_sql("SELECT seqno,cid,name,\"desc\" AS descending,coll,\"key\" AS iskey FROM pragma_index_xinfo(?, 'main') ORDER BY seqno LIMIT ",order.size()+1),{name},order.size(),preimage_kind::index_columns);
            need(detail.size()==order.size(),"ordinary index column count");
            for(size_t i=0;i<order.size();++i){int64_t cid=-1;std::string_view coll="binary";if(!order[i].empty()){
                auto c=std::find_if(cols.begin(),cols.end(),[&](const auto& x){return fold(x.name)==order[i];});need(c!=cols.end(),"ordinary index missing field");cid=c->cid;
                if(contains(ddl.columns.at(order[i]),{"collate","nocase"}))coll="nocase";}
                const auto& d=detail[i];need(number(d,"seqno")==int64_t(i)&&number(d,"cid")==cid&&number(d,"descending")==0&&fold(text(d,"coll"))==coll&&number(d,"iskey")==int64_t(i<s.keys.size())&&(order[i].empty()?is_null(d,"name"):fold(text(d,"name"))==order[i]),"ordinary index key/collation mismatch");}
            if(expose&&s.origin=="c"&&s.unique){const auto& f=fact(name,"index",table);ordinary_initialization_vector<ordinary_initialization_text> names;
                for(const auto& key:s.keys){auto c=std::find_if(cols.begin(),cols.end(),[&](const auto& x){return fold(x.name)==key;});need(c!=cols.end(),"ordinary unique field missing");names.push_back(c->name);}
                result.unique_indexes.push_back({own(f.name),own(table),own(f.sql),std::move(names)});}
        }
        if(initialization_prefix)for(const auto& [name,spec]:expected)
            need(spec.origin=="c"||seen.count(name),"ordinary construction implicit index missing");
    }
    void ordinary_table(std::string_view table,std::string_view expected_sql,const ordinary_initialization_text_set& migratable,const auto& shadow_columns,const auto& index_sql,bool expose=false){
        const auto& f=fact(table,"table",table);auto actual=parse_table(f.sql,table,bool(read.issued_query)),expected=parse_table(expected_sql,table,bool(read.issued_query));
        for(const auto& name:shadow_columns){auto n=fold(name);
            need(expected.columns.emplace(n,tokens{n,"text"}).second,"ordinary shadow column collision");expected.order.push_back(n);}
        need(actual.without_rowid==expected.without_rowid&&actual.constraints==expected.constraints&&actual.columns.size()==expected.columns.size(),"ordinary table constraints/column count mismatch");
        for(const auto& [name,e]:expected.columns){auto a=actual.columns.find(name);need(a!=actual.columns.end(),"ordinary required column absent");if(a->second==e)continue;
            need(migratable.count(name)&&e.size()>=2,"ordinary column definition mismatch");const auto type=own(e[1]);
            need(type=="integer"||type=="real"||type=="text"||type=="blob","ordinary migrated column type");
            const tokens plain{name,type},nonnull{name,type,"not","null"};auto with_default=plain;with_default.push_back("default");
            if(type=="integer")with_default.push_back("0");else if(type=="real")with_default.push_back("0.0");else if(type=="text")with_default.push_back("''");else{with_default.push_back("x");with_default.push_back("''");}
            need(a->second==plain||a->second==nonnull||a->second==with_default,"ordinary ungenerated migration constraint/default");}
        // The closed CREATE grammar fixes ordinary/STRICT/WITHOUT ROWID shape.
        // Targeted xinfo and indexes cross-check its actual engine metadata.
        auto cols=columns(table);need(cols.size()==actual.columns.size(),"ordinary pragma/DDL column count mismatch");
        ordinary_initialization_text_map<int64_t> primary;for(const auto& c:actual.constraints)if(c[0]=="primary"){auto keys=key_list(c,2);for(size_t i=0;i<keys.size();++i)primary[keys[i]]=int64_t(i+1);}
        for(const auto& [n,c]:actual.columns)if(contains(c,{"primary","key"}))need(primary.emplace(n,1).second,"ordinary duplicate primary key");
        for(const auto& c:cols){auto a=actual.columns.find(fold(c.name));need(a!=actual.columns.end()&&fold(c.name)==actual.order[size_t(c.cid)],"ordinary pragma/DDL column order");const auto& clause=a->second;
            need(c.type==type_of(clause)&&c.hidden==0&&c.primary_key==primary[fold(c.name)]&&c.not_null==int64_t(contains(clause,{"not","null"})||(actual.without_rowid&&c.primary_key)),"ordinary pragma/DDL column flags");
            const auto d=default_tokens(clause);need(bool(d)==bool(c.default_sql),"ordinary pragma/DDL default presence");if(d)need(unparenthesized(*d)==unparenthesized(lex(*c.default_sql)),"ordinary pragma/DDL default mismatch");}
        foreign_keys(table);keep(f);owned.insert(fold(table));indexes(table,actual,cols,index_sql,expose);result.tables.emplace(table,ordinary_table_metadata{own(table),std::move(cols)});
    }
    std::optional<ordinary_initialization_vector<ordinary_initialization_text>> recovered_audits(const ordinary_projection_table& table,
            const auto& approved){
        if((table.role!=ordinary_projection_role::model&&table.role!=ordinary_projection_role::union_value)||approved.size()!=3)return {};
        const auto insert_name=fold("Audit"+own(table.name)+"Insert");
        const auto input=all.find(insert_name);
        if(input==all.end()||input->second.type!="trigger"||fold(input->second.owner)!=fold(table.name))return {};
        std::optional<std::string_view> template_insert;size_t capacity=0;
        for(const auto& sql:approved){
            if(sql.size()>16*1024*1024-capacity)return {};capacity+=sql.size();
            if(object_name(sql,"trigger")==insert_name){if(template_insert)return {};template_insert=std::string_view(sql);}
        }
        if(!template_insert)return {};
        const auto expected=audit_insert_order(*template_insert),actual=audit_insert_order(input->second.sql);
        if(!expected||!actual||expected->size()!=actual->size())return {};
        ordinary_initialization_text_set allowed(expected->begin(),expected->end()),seen(actual->begin(),actual->end());
        if(allowed.size()!=expected->size()||seen.size()!=actual->size()||seen!=allowed)return {};
        ordinary_initialization_text_map<const ordinary_projection_column*> declared;
        for(const auto& column:table.columns)
            if(!declared.emplace(column.audit_name,&column).second)return {};
        const auto columns=[&](const auto& order){
            ordinary_initialization_vector<std::pair<ordinary_initialization_text,column_type>> out;out.reserve(order.size());
            for(const auto& name:order){const auto found=declared.find(name);if(found==declared.end())return decltype(out){};
                out.emplace_back(own(name),found->second->type);}
            return out;
        };
        const auto original=columns(*expected),ordered=columns(*actual);
        if(original.size()!=expected->size()||ordered.size()!=actual->size())return {};
        ordinary_initialization_text_set no_history;
        for(const auto& name:allowed)if(declared.at(name)->no_history)no_history.insert(name);
        // First prove this approved variant is the SAME protected generator,
        // not a custom SQL template that could confer a different authority.
        size_t index=0;bool exact=true;
        ordinary_model_audit_programs_storage<ordinary_initialization_text>(own(table.name),original,no_history,{},[&](const ordinary_initialization_text& sql){
            if(index>=approved.size()||!same_program(approved[index],sql))exact=false;++index;
        });
        if(!exact||index!=approved.size())return {};
        // One transient three-program candidate; a permutation preserves the
        // aggregate byte size of its already admitted approved generator.
        ordinary_initialization_vector<ordinary_initialization_text> candidate;size_t bytes=0;
        ordinary_model_audit_programs_storage<ordinary_initialization_text>(own(table.name),ordered,no_history,{},[&](const ordinary_initialization_text& sql){
            need(candidate.size()<3&&sql.size()<=read.limits.sql_bytes&&sql.size()<=capacity-bytes,
                "ordinary recovered audit recipe bound");bytes+=sql.size();candidate.push_back(sql);
        });
        if(candidate.size()!=3)return {};return candidate;
    }
    void audits(const ordinary_projection_table& table){
        const auto matches=[&](const auto& bundle){for(const auto& sql:bundle){auto name=object_name(sql,"trigger");auto i=all.find(name);
            if(i==all.end()&&initialization_prefix)continue;
            if(i==all.end()||i->second.type!="trigger"||fold(i->second.owner)!=fold(table.name)||!same_program(i->second.sql,sql))return false;}
            ordinary_initialization_text_set names;for(const auto& sql:bundle)names.insert(object_name(sql,"trigger"));
            for(const auto& sql:table.audit_programs){const auto name=object_name(sql,"trigger");if(!names.count(name)&&all.count(name))return false;}
            for(const auto& alternate:table.alternate_audit_programs)for(const auto& sql:alternate){const auto name=object_name(sql,"trigger");if(!names.count(name)&&all.count(name))return false;}
            return true;};
        ordinary_initialization_vector<std::string_view> chosen;
        const auto select=[&](const auto& bundle){chosen.clear();chosen.reserve(bundle.size());for(const auto& sql:bundle)chosen.emplace_back(sql.data(),sql.size());};
        bool selected=matches(table.audit_programs);if(selected)select(table.audit_programs);
        if(!selected)for(const auto& alternate:table.alternate_audit_programs)if(matches(alternate)){select(alternate);selected=true;break;}
        ordinary_initialization_vector<ordinary_initialization_text> recovered;
        if(!selected){
            const auto try_order=[&](const auto& approved){auto value=recovered_audits(table,approved);
                if(value&&matches(*value)){recovered=std::move(*value);select(recovered);selected=true;}};
            try_order(table.audit_programs);
            if(!selected)for(const auto& alternate:table.alternate_audit_programs){try_order(alternate);if(selected)break;}
        }
        need(selected,"ordinary complete audit program bundle mismatch");for(const auto sql:chosen)trigger(sql,table.name);
    }
    void trigger(std::string_view sql,std::string_view owner){const auto name=object_name(sql,"trigger");
        if(initialization_prefix&&!all.count(name))return;
        const auto& f=fact(name,"trigger",owner);need(same_program(f.sql,sql),"ordinary generated trigger mismatch");keep(f);}
    void module_columns(std::string_view table,const ordinary_initialization_vector<ordinary_column_metadata>& expected){auto actual=columns(table);need(actual.size()==expected.size(),"ordinary module column count");
        for(size_t i=0;i<actual.size();++i){const auto& a=actual[i];const auto& e=expected[i];need(a.name==e.name&&a.type==e.type&&a.not_null==e.not_null&&a.primary_key==e.primary_key&&a.hidden==e.hidden&&!a.default_sql,"ordinary module column definition");}
        need(read.get("SELECT id FROM pragma_foreign_key_list(?, 'main') LIMIT 1",{table},1,preimage_kind::table_foreign_keys).empty(),"ordinary module foreign key");}
    void shadow(std::string_view name,std::string_view body,const ordinary_initialization_vector<ordinary_initialization_text>& indexes={}){
        ordinary_table(name,"CREATE TABLE "+ordinary_identifier_sql_(name)+own(body),ordinary_initialization_text_set{},ordinary_initialization_vector<ordinary_initialization_text>{},indexes);}
    void module(const ordinary_projection_derived& d);
    void close_ownership(){
        for(const auto& [name,f]:all){bool reserved=owned.count(fold(f.owner));for(const auto& family:families)reserved=reserved||name==family||name.starts_with(family+"_")||fold(f.owner).starts_with(family+"_");
            need(!reserved||allowed.count(name),"ordinary unknown owned program/shadow");}
        const auto& temps=temp_inventory;
        for(const auto& r:temps)need(!owned.count(fold(text(r,"owner")))&&!owned.count(fold(text(r,"name"))),"ordinary TEMP object overlaps owned tables");
        // Inbound foreign-key actions can mutate another table when an owned
        // parent changes. Inspect every ordinary table, including TEMP; no
        // application rows are read. Virtual-table SQL is never instantiated
        // merely to inspect an unrelated custom module.
        for(const auto& [name,f]:all)if(f.type=="table"&&!owned.count(name)&&!f.sql.empty()){
            if(virtual_declaration(f.sql))continue;
            auto refs=read.get(limit_sql("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'main') LIMIT ",read.limits.objects+1),{f.name},read.limits.objects,preimage_kind::table_foreign_keys);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary inbound foreign key outside projection");}
        for(const auto& r:temps)if(text(r,"type")=="table"){
            const auto n=text(r,"name");if(!is_null(r,"sql")&&virtual_declaration(text(r,"sql")))continue;auto refs=read.get(limit_sql("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'temp') LIMIT ",read.limits.objects+1),{n},read.limits.objects,preimage_kind::temp_table_foreign_keys);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary TEMP inbound foreign key");}
    }
    void close_read_ownership(const ordinary_initialization_text_set& plain_tables){
        for(const auto& [name,f]:all){bool reserved=owned.count(fold(f.owner));for(const auto& family:families)reserved=reserved||name==family||name.starts_with(family+"_")||fold(f.owner).starts_with(family+"_");
            // SQLite does not execute table triggers for SELECT. Preserve
            // their actual bounded DATA without treating a familiar name or
            // body as validated authority. Module-shadow families still went
            // through strict pre-connect checks above; only already-proved
            // ordinary application/internal tables can take this branch.
            const bool deferred_trigger=f.type=="trigger"&&plain_tables.count(fold(f.owner));
            need(!reserved||allowed.count(name)||deferred_trigger,"ordinary unknown owned program/shadow");}
        const auto& temps=temp_inventory;
        for(const auto& r:temps)need(!owned.count(fold(text(r,"owner")))&&!owned.count(fold(text(r,"name"))),"ordinary TEMP object overlaps owned tables");
        // Inbound foreign-key actions can mutate another table when an owned
        // parent changes. Inspect every ordinary table, including TEMP; no
        // application rows are read. Virtual-table SQL is never instantiated
        // merely to inspect an unrelated custom module.
        for(const auto& [name,f]:all)if(f.type=="table"&&!owned.count(name)&&!f.sql.empty()){
            if(virtual_declaration(f.sql))continue;
            auto refs=read.get(limit_sql("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'main') LIMIT ",read.limits.objects+1),{f.name},read.limits.objects,preimage_kind::table_foreign_keys);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary inbound foreign key outside projection");}
        for(const auto& r:temps)if(text(r,"type")=="table"){
            const auto n=text(r,"name");if(!is_null(r,"sql")&&virtual_declaration(text(r,"sql")))continue;auto refs=read.get(limit_sql("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'temp') LIMIT ",read.limits.objects+1),{n},read.limits.objects,preimage_kind::temp_table_foreign_keys);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary TEMP inbound foreign key");}
    }
};
size_t positive(std::string_view s,size_t maximum){need(!s.empty()&&s.size()<=8,"ordinary module numeric bound");size_t n=0;for(char c:s){need(c>='0'&&c<='9',"ordinary module numeric syntax");need(n<=(maximum-size_t(c-'0'))/10,"ordinary module numeric overflow");n=n*10+size_t(c-'0');}need(n&&n<=maximum&&decimal(n)==s,"ordinary module numeric range");return n;}
ordinary_column_metadata module_column(ordinary_initialization_text name,ordinary_initialization_text type={},int64_t nn=0,int64_t pk=0,int64_t hidden=0){return{std::move(name),std::move(type),0,nn,pk,hidden,{}};}
ordinary_derived_metadata module_declaration(const ordinary_projection_derived& d,std::string_view sql){
    ordinary_derived_metadata m{d.kind,own(d.model),own(d.property),own(d.table),own(sql),true,0,0,0};
    if(d.kind==ordinary_derived_kind::vector){
        const auto t=lex(sql);auto i=std::find(t.begin(),t.end(),"float");need(i!=t.end()&&t.end()-i>=4&&i[1]=="["&&i[3]=="]","ordinary vector dimension declaration");m.dimensions=positive(i[2],8192);
        const auto nlist=std::find(t.begin(),t.end(),"nlist");if(nlist!=t.end()){need(t.end()-nlist>=3&&nlist[1]=="=","ordinary IVF nlist syntax");m.ivf_nlist=positive(nlist[2],65536);}
        const auto nprobe=std::find(t.begin(),t.end(),"nprobe");if(nprobe!=t.end()){need(t.end()-nprobe>=3&&nprobe[1]=="=","ordinary IVF nprobe syntax");m.ivf_nprobe=positive(nprobe[2],65536);need(m.ivf_nlist&&m.ivf_nprobe<=m.ivf_nlist,"ordinary IVF probe bound");}
        need(same_program(sql,vec0_create_table_program_storage<ordinary_initialization_text>(own(d.table),int(m.dimensions),int(m.ivf_nlist),int(m.ivf_nprobe))),"ordinary vector module/options not generated");
    }else need((d.kind==ordinary_derived_kind::full_text||d.kind==ordinary_derived_kind::geographic)&&same_program(sql,d.create_sql),"ordinary derived module/options not generated");
    return m;
}
struct module_shadow {ordinary_initialization_text name,body;ordinary_initialization_vector<ordinary_initialization_text> indexes;};
ordinary_initialization_vector<module_shadow> module_shadows(const ordinary_derived_metadata& m){const auto& t=m.table;
    if(m.kind==ordinary_derived_kind::geographic)return {
        // rtreeSqlInit uses untyped payloads, unlike its introductory comment.
        {t+"_node","(nodeno INTEGER PRIMARY KEY,data)"},
        {t+"_parent","(nodeno INTEGER PRIMARY KEY,parentnode)"},
        {t+"_rowid","(rowid INTEGER PRIMARY KEY,nodeno)"}};
    if(m.kind==ordinary_derived_kind::full_text)return {
        {t+"_data","(id INTEGER PRIMARY KEY,block BLOB)"},
        {t+"_idx","(segid,term,pgno,PRIMARY KEY(segid,term)) WITHOUT ROWID"},
        {t+"_docsize","(id INTEGER PRIMARY KEY,sz BLOB)"},
        {t+"_config","(k PRIMARY KEY,v) WITHOUT ROWID"}};
    ordinary_initialization_vector<module_shadow> out{
        {t+"_info","(key text primary key,value any)"},
        {t+"_chunks","(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,size INTEGER NOT NULL,validity BLOB NOT NULL,rowids BLOB NOT NULL)"},
        {t+"_rowids","(rowid INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,chunk_id INTEGER,chunk_offset INTEGER)"}};
    // Module-owned names derive from the pinned module implementation.
    // No separate SQLite TF_Shadow flag observation is claimed.
    if(!m.ivf_nlist)out.push_back({t+"_vector_chunks00","(rowid PRIMARY KEY,vectors BLOB NOT NULL)"});
    else{
        out.push_back({t+"_ivf_centroids00","(centroid_id INTEGER PRIMARY KEY,centroid BLOB NOT NULL)"});
        out.push_back({t+"_ivf_cells00","(centroid_id INTEGER NOT NULL,n_vectors INTEGER NOT NULL DEFAULT 0,validity BLOB NOT NULL,rowids BLOB NOT NULL,vectors BLOB NOT NULL)",{
            "CREATE INDEX "+ordinary_identifier_sql_(t+"_ivf_cells00_centroid")+" ON "+ordinary_identifier_sql_(t+"_ivf_cells00")+" (centroid_id)"}});
        out.push_back({t+"_ivf_rowid_map00","(rowid INTEGER PRIMARY KEY,cell_id INTEGER NOT NULL,slot INTEGER NOT NULL)"});}
    return out;
}
// Some module xConnect implementations query their own configuration tables.
// Their exact table/index SQL must be closed BEFORE a connecting PRAGMA can
// select a custom collation or expression index. No SQL is executed here.
struct expected_module_trigger {std::string_view owner;std::string_view sql;};
void preflight_module_schema(validator& v,const ordinary_derived_metadata& m,const ordinary_initialization_text_map<expected_module_trigger>& programs){
    ordinary_initialization_text_set permitted{fold(m.table)};
    for(const auto& s:module_shadows(m)){
        const auto& f=v.fact(s.name,"table",s.name);const auto a=parse_table(f.sql,s.name),e=parse_table("CREATE TABLE "+ordinary_identifier_sql_(s.name)+s.body,s.name);
        need(a.columns==e.columns&&a.order==e.order&&a.constraints==e.constraints&&a.without_rowid==e.without_rowid,"ordinary module shadow pre-connect declaration mismatch");permitted.insert(fold(s.name));
        auto indexes=implicit_indexes(e,s.name);for(const auto& sql:s.indexes)indexes.push_back(explicit_index(sql,s.name));
        for(const auto& index:indexes){if(e.without_rowid&&index.origin=="pk"){need(!v.all.count(fold(index.name)),"ordinary pre-connect without-rowid index object");continue;}
            const auto& f=v.fact(index.name,"index",s.name);need(index.origin=="c"?same_program(f.sql,index.sql):f.sql.empty(),"ordinary module shadow pre-connect index mismatch");permitted.insert(fold(f.name));}
    }
    const auto prefix=fold(m.table)+"_";
    for(const auto& [name,expected]:programs)if(name.starts_with(prefix)){
        if(v.initialization_prefix&&!v.all.count(name))continue;
        const auto& actual=v.fact(name,"trigger",expected.owner);
        need(same_program(actual.sql,expected.sql),"ordinary module trigger pre-connect program mismatch");}
    for(const auto& [name,f]:v.all)if(name.starts_with(prefix)||fold(f.owner)==fold(m.table)||fold(f.owner).starts_with(prefix)){
        const auto expected=programs.find(name);
        const bool generated_trigger=f.type=="trigger"&&expected!=programs.end()&&
            fold(f.owner)==fold(expected->second.owner)&&same_program(f.sql,expected->second.sql);
        need(permitted.count(name)||generated_trigger,"ordinary unknown module family before connection");}
}
void validator::module(const ordinary_projection_derived& d){
    const auto found=all.find(fold(d.table));if(found==all.end()){need(d.kind==ordinary_derived_kind::vector,"ordinary required derived table missing");result.derived.push_back({d.kind,own(d.model),own(d.property),own(d.table),{},false,0,0,0});return;}
    const auto& f=fact(d.table,"table",d.table);auto m=module_declaration(d,f.sql);
    if(d.kind==ordinary_derived_kind::vector)
        module_columns(d.table,{module_column("global_id",{},1,1),module_column("embedding"),module_column(own(d.table),{},0,0,1),module_column("distance",{},0,0,1),module_column("k",{},0,0,1)});
    else if(d.kind==ordinary_derived_kind::full_text)
        module_columns(d.table,{module_column(own(d.property)),module_column(own(d.table),{},0,0,1),module_column("rank",{},0,0,1)});
    else module_columns(d.table,{module_column("id","int"),module_column("minLat","real"),module_column("maxLat","real"),module_column("minLon","real"),module_column("maxLon","real")});
    keep(f);owned.insert(fold(d.table));for(const auto& sql:d.programs)trigger(sql,d.model);
    const auto& t=d.table;
    for(const auto& spec:module_shadows(m))shadow(spec.name,spec.body,spec.indexes);
    if(d.kind==ordinary_derived_kind::full_text){
        auto config=read.get("SELECT k,v FROM main."+ordinary_identifier_sql_(t+"_config")+" LIMIT 2",{},1);
        need(config.size()==1&&text(config[0],"k")=="version"&&number(config[0],"v")==4,"ordinary FTS format/config mismatch");
    }else if(d.kind==ordinary_derived_kind::vector){
        auto info=read.get("SELECT key,value FROM main."+ordinary_identifier_sql_(t+"_info")+" LIMIT "+decimal(m.ivf_nlist?6:5),{},m.ivf_nlist?5:4);
        need(info.size()==(m.ivf_nlist?5:4),"ordinary vector info count");ordinary_initialization_text_set keys;
        for(const auto& row:info){const auto key=text(row,"key");need(keys.insert(key).second,"ordinary duplicate vector info key");
            if(key=="CREATE_VERSION")need(text(row,"value")==SQLITE_VEC_VERSION,"ordinary vector format version");
            else if(key=="CREATE_VERSION_MAJOR")need(number(row,"value")==SQLITE_VEC_VERSION_MAJOR,"ordinary vector major version");
            else if(key=="CREATE_VERSION_MINOR")need(number(row,"value")==SQLITE_VEC_VERSION_MINOR,"ordinary vector minor version");
            else if(key=="CREATE_VERSION_PATCH")need(number(row,"value")==SQLITE_VEC_VERSION_PATCH,"ordinary vector patch version");
            else{need(m.ivf_nlist&&key=="ivf_trained_0","ordinary unknown vector info");const auto value=number(row,"value");need(value==0||value==1,"ordinary IVF trained metadata");}}
        (void)fact("sqlite_sequence","table","sqlite_sequence");
    }
    result.derived.push_back(std::move(m));
}
} // namespace

ordinary_initialization_text ordinary_initialization_schema_validation::compiled_object_(std::string_view sql,std::string_view kind) {
    need(kind=="trigger"||kind=="index","unissued compiled object kind");return object_name(sql,kind);
}
ordinary_initialization_vector<ordinary_initialization_text> ordinary_initialization_schema_validation::compiled_implicit_indexes_(std::string_view sql,std::string_view table) {
    ordinary_initialization_vector<ordinary_initialization_text> names;
    for(const auto& spec:implicit_indexes(parse_table(sql,table,true),table,true))names.push_back(spec.name);
    return names;
}
ordinary_schema_preimage validate_ordinary_preexisting_schema(const ordinary_schema_projection& p,
    const ordinary_preimage_query& issued_query,const ordinary_metadata_limits& limits){
    return ordinary_initialization_schema_validation::validate_(p,issued_query,limits,
        ordinary_initialization_schema_validation::view::preexisting);
}
ordinary_schema_preimage ordinary_initialization_schema_validation::validate_(const ordinary_schema_projection& p,
    const ordinary_initialization_preimage_query& issued_query,const ordinary_metadata_limits& limits,view operation,ordinary_schema_projection* selected){
    need(operation==view::construction_initial?selected==&p:selected==nullptr,"ordinary construction selection must be its actual private projection copy");
    need(issued_query&&limits.objects&&limits.objects<=131072&&limits.rows&&limits.rows<=1048576&&
         limits.bytes&&limits.bytes<=64*1024*1024&&limits.sql_bytes&&limits.sql_bytes<=1024*1024&&limits.sql_bytes<=limits.bytes,
         "ordinary metadata limits refused");
    need(p.models.size()<=ordinary_sync_limits::models&&!p.tables.empty()&&p.tables.size()<=ordinary_sync_limits::projection_tables+16&&p.derived.size()<=ordinary_sync_limits::properties,
         "ordinary projection shape bound");
    // Validate the whole supplied description before the first query. Its
    // caller provenance is trusted, but malformed/colliding descriptions must
    // not acquire an accidental second ownership interpretation.
    ordinary_initialization_text_set table_names,program_names;size_t declared_bytes=0,columns=0;
    const auto charge=[&](std::string_view s){need(s.size()<=limits.sql_bytes&&s.size()<=16*1024*1024-declared_bytes,"ordinary projection admission bound");declared_bytes+=s.size();};
    const auto add_program=[&](std::string_view sql,std::string_view kind){charge(sql);need(program_names.insert(object_name(sql,kind)).second,"ordinary projection program collision");};
    for(const auto& [name,t]:p.tables){identifier(name);need(t.name==name&&table_names.insert(fold(name)).second,"ordinary projection table identity collision");charge(t.create_sql);
        need(t.columns.size()<=ordinary_sync_limits::columns_per_table&&t.columns.size()<=ordinary_sync_limits::projection_columns-columns,"ordinary projection columns bound");columns+=t.columns.size();
        ordinary_initialization_text_set names;if(t.local_id)names.insert("id");for(const auto& c:t.columns){identifier(c.name);need(names.insert(fold(c.name)).second,"ordinary projection column collision");}
        for(const auto& n:t.shadow_columns){identifier(n);need(names.insert(fold(n)).second,"ordinary projection shadow collision");}
        ordinary_initialization_text_set audit_names;
        for(const auto& sql:t.audit_programs){add_program(sql,"trigger");audit_names.insert(object_name(sql,"trigger"));}
        need(t.alternate_audit_programs.size()<=1,"ordinary audit variant bound");
        for(const auto& alternate:t.alternate_audit_programs){ordinary_initialization_text_set names;for(const auto& sql:alternate){charge(sql);const auto n=object_name(sql,"trigger");need(names.insert(n).second,"ordinary alternate audit collision");
            if(!audit_names.count(n)){need(program_names.insert(n).second,"ordinary alternate program collision");audit_names.insert(n);}}}
        for(const auto& sql:t.other_programs)add_program(sql,"trigger");for(const auto& sql:t.index_programs)add_program(sql,"index");}
    for(const auto& name:p.models){identifier(name);need(p.tables.count(name)&&p.tables.at(name).role==ordinary_projection_role::model&&name[0]!='_'&&fold(name)!="auditlog"&&fold(name)!="sqlite_sequence","ordinary projection root mismatch");}
    ordinary_initialization_text_set derived_names;
    for(const auto& d:p.derived){identifier(d.table);identifier(d.model);identifier(d.property);need(p.tables.count(d.model)&&table_names.insert(fold(d.table)).second&&derived_names.insert(fold(d.table)).second,"ordinary derived ownership collision");charge(d.create_sql);for(const auto& sql:d.programs)add_program(sql,"trigger");}
    for(const auto& n:program_names)need(!table_names.count(n),"ordinary table/program collision");
    for(const auto& family:derived_names)for(const auto& table:table_names)need(table==family||!table.starts_with(family+"_"),"ordinary derived family/table collision");
    // References remain inside this synchronous call's immutable description.
    // A familiar trigger name alone is never permission to connect a module.
    ordinary_initialization_text_map<expected_module_trigger> module_programs;
    for(const auto& [name,t]:p.tables)for(const auto& sql:t.other_programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{name,sql}).second,"ordinary module trigger collision");
    for(const auto& d:p.derived)for(const auto& sql:d.programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{d.model,sql}).second,"ordinary module trigger collision");
    // This is the sole typed issuer. Its inputs are only the fixed metadata
    // producers below/inside validator, after the entire projection was
    // validated above; the caller never supplies SQL to this adapter.
    const ordinary_metadata_query unused_query;
    const auto query=[&](preimage_kind kind,std::string_view sql,
            std::initializer_list<std::string_view> args,size_t row_limit,size_t result_bytes) {
        need(kind!=preimage_kind::unissued&&args.size()<=ordinary_preimage_statement::parameter_limit(),
            "ordinary preimage unissued purpose/parameter count");
        size_t parameter_bytes=0;
        for(const auto value:args){
            need(value.find('\0')==std::string_view::npos&&
                 value.size()<=ordinary_preimage_statement::parameter_byte_limit()-parameter_bytes,
                 "ordinary preimage parameter type/bytes");parameter_bytes+=value.size();}
        need(result_bytes>0,"ordinary preimage result bytes exhausted");
        const auto creator=std::uint64_t(::getpid());
        ordinary_initialization_storage_scope::charge(native_retirement_storage<
            std::shared_ptr<const ordinary_preimage_statement>>::allocation_bytes());
        native_retirement_storage<std::shared_ptr<const ordinary_preimage_statement>> actual;
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement custody");
        ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement::state));
        auto* held=new ordinary_preimage_statement::state(kind,row_limit,result_bytes,parameter_bytes);
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement state allocation");
        ordinary_preimage_statement* wrapper=nullptr;bool held_owned_by_wrapper=false;
        try {
            ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement));
            wrapper=new ordinary_preimage_statement(creator,held);held_owned_by_wrapper=true;
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement allocation");
            held->sql.assign(sql.data(),sql.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement SQL copy");
            held->parameters.reserve(args.size());
            for(const auto value:args)held->parameters.emplace_back(value.data(),value.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement parameter copy");
            const auto* entrusted=wrapper;wrapper=nullptr;
            actual.get<0>()=std::shared_ptr<const ordinary_preimage_statement>(entrusted,
                &ordinary_preimage_statement::destroy_,
                ordinary_initialization_allocator<ordinary_preimage_statement>{});
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement publication");
        } catch (...) {
            if(creator==std::uint64_t(::getpid())){
                if(wrapper)delete wrapper;else if(!held_owned_by_wrapper)delete held;
                // Failed shared_ptr publication already used its deleter.
            }
            throw;
        }
        return issued_query(actual.get<0>());
    };
    const preimage_reader_query issued(query);
    validator v{{unused_query,limits},{},{},{},{},{}};v.read.issued_query=&issued;
    v.initialization_prefix=operation==view::construction_prefix||operation==view::construction_initial;
    v.result.schema_cookie=v.read.cookie("main",preimage_kind::main_cookie);
    v.result.temp_schema_cookie=v.read.cookie("temp",preimage_kind::temp_cookie);
    v.inventory();
    v.temp_inventory=v.read.get(limit_sql("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT ",limits.objects+1),{},limits.objects,preimage_kind::temp_inventory);
    // These exact unqualified eponymous metadata producers are library
    // namespace, not application schema objects. Close both actual schemas
    // BEFORE any TVF producer can resolve/connect a persisted object instead.
    // This is a new preimage contract only; the old validator is unchanged.
    const auto metadata_producer_name=[](std::string_view name)noexcept {
        return name=="pragma_table_xinfo"||name=="pragma_foreign_key_list"||
               name=="pragma_index_list"||name=="pragma_index_xinfo"||
               name=="pragma_database_list";
    };
    for(const auto& [name,unused]:v.all)need(!metadata_producer_name(name),
        "ordinary preimage MAIN metadata producer namespace collision");
    // Complete MAIN and TEMP inventory precedes every ordinary table PRAGMA
    // and any possible selected-module connection. Never use table_list.
    const auto selected_name=[&](std::string_view raw){const auto name=fold(raw);
        if(table_names.count(name)||program_names.count(name))return true;
        for(const auto& family:derived_names)if(name.starts_with(family+"_"))return true;
        for(const auto& model:p.models)if(name.starts_with("_"+fold(model)+"_"))return true;
        return false;};
    ordinary_schema_preimage result;
    ordinary_initialization_text_set temp_names;
    for(const auto& row:v.temp_inventory){(void)text_view(row,"type");
        const auto name=text_view(row,"name"),owner=text_view(row,"owner"),sql=is_null(row,"sql")?std::string_view{}:text_view(row,"sql");
        const auto& f=ordinary_object_metadata_view{name,owner,sql};
        need(f.name.size()<=256&&f.owner.size()<=256&&f.sql.size()<=limits.sql_bytes&&
             temp_names.insert(fold(f.name)).second,"ordinary preimage TEMP object size/collision");
        need(!metadata_producer_name(fold(f.name)),
            "ordinary preimage TEMP metadata producer namespace collision");
        need(!selected_name(f.name)&&!selected_name(f.owner),"ordinary selected TEMP metadata shadow refused before connection");
        }
    // A missing module must have no surviving shadow or named trigger. An
    // existing module must have the complete exact pinned pre-connect family.
    ordinary_initialization_vector<ordinary_derived_metadata> modules;
    for(const auto& d:p.derived){const auto found=v.all.find(fold(d.table));
        if(found==v.all.end()){result.absent_tables.push_back(own(d.table));continue;}
        (void)v.fact(d.model,"table",d.model);
        const auto& f=v.fact(d.table,"table",d.table);
        auto described=module_declaration(d,f.sql);
        preflight_module_schema(v,described,module_programs);modules.push_back(std::move(described));}
    // The unchanged actual ordinary/shadow column/index/FK cross-checks are
    // safe only after the declarations above exclude modules/views/expressions.
    // No module_columns(), module(), configuration row, or application row is
    // evaluated by this preimage procedure.
    for(const auto& [name,t]:p.tables){
        if(!v.all.count(fold(name))){result.absent_tables.push_back(own(name));continue;}
        ordinary_initialization_text_set migratable;
        if(t.role==ordinary_projection_role::model)for(const auto& c:t.columns)
            if(fold(c.name)!="globalid")migratable.insert(fold(c.name));
        if(operation==view::construction_initial&&!t.shadow_columns.empty()) {
            need(selected&&t.role==ordinary_projection_role::model,"initial derived-column owner is not a selected model");
            const auto actual=parse_table(v.fact(name,"table",name).sql,name,true);
            size_t present=0;for(const auto& column:t.shadow_columns)present+=actual.columns.count(fold(column));
            // Two closed original producer images only: ordinary model CREATE
            // before every constraint shadow, or the exact complete current
            // set. A partly present preexisting set needs genuine historical
            // declarations and is never relabeled a typed in-flight stage.
            need(present==0||present==t.shadow_columns.size(),"unsupported preexisting partial derived-column stage");
            if(present==0)selected->tables.at(name).shadow_columns.clear();
        }
        v.ordinary_table(name,t.create_sql,migratable,t.shadow_columns,t.index_programs,true);
        v.audits(t);for(const auto& sql:t.other_programs)v.trigger(sql,name);}
    for(const auto& m:modules){v.keep(v.fact(m.table,"table",m.table));v.owned.insert(fold(m.table));
        const auto d=std::find_if(p.derived.begin(),p.derived.end(),[&](const auto& x){return x.table==m.table;});
        need(d!=p.derived.end(),"ordinary preimage module identity");
        for(const auto& sql:d->programs)v.trigger(sql,d->model);
        for(const auto& spec:module_shadows(m))v.shadow(spec.name,spec.body,spec.indexes);}
    // Reserve ABSENT selected names as well. Orphan programs, aliases, indexes
    // and old/custom sidecars cannot gain authority merely because their
    // expected parent/table has not yet been created by this startup.
    v.owned.insert(table_names.begin(),table_names.end());
    for(const auto& d:p.derived)v.families.push_back(fold(d.table));
    for(const auto& name:p.models)v.families.push_back("_"+fold(name));
    for(const auto& name:program_names)need(!v.all.count(name)||v.allowed.count(name),
        "ordinary preimage orphan or unapproved named program");
    v.close_ownership();
    need(v.read.cookie("main",preimage_kind::main_cookie)==v.result.schema_cookie&&v.read.cookie("temp",preimage_kind::temp_cookie)==v.result.temp_schema_cookie,
        "ordinary retained preimage schema view changed");
    result.schema_cookie=v.result.schema_cookie;result.temp_schema_cookie=v.result.temp_schema_cookie;
    result.inspected_rows=v.read.rows;result.copied_bytes=v.read.bytes;
    for(auto& row:v.temp_inventory)result.temp_objects.push_back(take_inventory_object(row));
    for(auto& [name,metadata]:v.result.tables){
        const auto& object=v.fact(name,"table",name);
        // The same actual xinfo/index/FK checks above validated every column
        // and WITHOUT ROWID PK layout against this actual closed declaration.
        const auto actual=parse_table(object.sql,name,true);
        result.plain_shapes.push_back({own(name),std::uint64_t(metadata.columns.size()),actual.without_rowid,std::move(metadata.columns)});
    }
    for(auto& [name,f]:v.all){if(f.type=="trigger"&&v.allowed.count(name))
            result.verified_trigger_origins.push_back(result.main_objects.size());
        result.main_objects.push_back(std::move(f));}
    std::sort(result.absent_tables.begin(),result.absent_tables.end());
    need(std::adjacent_find(result.absent_tables.begin(),result.absent_tables.end())==result.absent_tables.end(),
        "ordinary preimage absent identity collision");
    if(operation==view::required_present)
        need(result.absent_tables.empty(),"ordinary completed family still has absent required tables/modules");
    return result;
}

ordinary_schema_preimage inspect_ordinary_preexisting_read_tables(const ordinary_schema_projection& p,
    const ordinary_preimage_query& query,const ordinary_metadata_limits& limits){
    return ordinary_initialization_schema_validation::read_tables_(p,query,limits);
}

ordinary_schema_preimage ordinary_initialization_schema_validation::read_tables_(const ordinary_schema_projection& p,
    const ordinary_initialization_preimage_query& issued_query,const ordinary_metadata_limits& limits){
    need(issued_query&&limits.objects&&limits.objects<=131072&&limits.rows&&limits.rows<=1048576&&
         limits.bytes&&limits.bytes<=64*1024*1024&&limits.sql_bytes&&limits.sql_bytes<=1024*1024&&limits.sql_bytes<=limits.bytes,
         "ordinary metadata limits refused");
    need(p.models.size()<=ordinary_sync_limits::models&&!p.tables.empty()&&p.tables.size()<=ordinary_sync_limits::projection_tables+16&&p.derived.size()<=ordinary_sync_limits::properties,
         "ordinary projection shape bound");
    // Validate the whole supplied description before the first query. Its
    // caller provenance is trusted, but malformed/colliding descriptions must
    // not acquire an accidental second ownership interpretation.
    ordinary_initialization_text_set table_names,program_names;size_t declared_bytes=0,columns=0;
    const auto charge=[&](std::string_view s){need(s.size()<=limits.sql_bytes&&s.size()<=16*1024*1024-declared_bytes,"ordinary projection admission bound");declared_bytes+=s.size();};
    const auto add_program=[&](std::string_view sql,std::string_view kind){charge(sql);need(program_names.insert(object_name(sql,kind)).second,"ordinary projection program collision");};
    for(const auto& [name,t]:p.tables){identifier(name);need(t.name==name&&table_names.insert(fold(name)).second,"ordinary projection table identity collision");charge(t.create_sql);
        need(t.columns.size()<=ordinary_sync_limits::columns_per_table&&t.columns.size()<=ordinary_sync_limits::projection_columns-columns,"ordinary projection columns bound");columns+=t.columns.size();
        ordinary_initialization_text_set names;if(t.local_id)names.insert("id");for(const auto& c:t.columns){identifier(c.name);need(names.insert(fold(c.name)).second,"ordinary projection column collision");}
        for(const auto& n:t.shadow_columns){identifier(n);need(names.insert(fold(n)).second,"ordinary projection shadow collision");}
        ordinary_initialization_text_set audit_names;
        for(const auto& sql:t.audit_programs){add_program(sql,"trigger");audit_names.insert(object_name(sql,"trigger"));}
        need(t.alternate_audit_programs.size()<=1,"ordinary audit variant bound");
        for(const auto& alternate:t.alternate_audit_programs){ordinary_initialization_text_set names;for(const auto& sql:alternate){charge(sql);const auto n=object_name(sql,"trigger");need(names.insert(n).second,"ordinary alternate audit collision");
            if(!audit_names.count(n)){need(program_names.insert(n).second,"ordinary alternate program collision");audit_names.insert(n);}}}
        for(const auto& sql:t.other_programs)add_program(sql,"trigger");for(const auto& sql:t.index_programs)add_program(sql,"index");}
    for(const auto& name:p.models){identifier(name);need(p.tables.count(name)&&p.tables.at(name).role==ordinary_projection_role::model&&name[0]!='_'&&fold(name)!="auditlog"&&fold(name)!="sqlite_sequence","ordinary projection root mismatch");}
    ordinary_initialization_text_set derived_names;
    for(const auto& d:p.derived){identifier(d.table);identifier(d.model);identifier(d.property);need(p.tables.count(d.model)&&table_names.insert(fold(d.table)).second&&derived_names.insert(fold(d.table)).second,"ordinary derived ownership collision");charge(d.create_sql);for(const auto& sql:d.programs)add_program(sql,"trigger");}
    for(const auto& n:program_names)need(!table_names.count(n),"ordinary table/program collision");
    for(const auto& family:derived_names)for(const auto& table:table_names)need(table==family||!table.starts_with(family+"_"),"ordinary derived family/table collision");
    // References remain inside this synchronous call's immutable description.
    // A familiar trigger name alone is never permission to connect a module.
    ordinary_initialization_text_map<expected_module_trigger> module_programs;
    for(const auto& [name,t]:p.tables)for(const auto& sql:t.other_programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{name,sql}).second,"ordinary module trigger collision");
    for(const auto& d:p.derived)for(const auto& sql:d.programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{d.model,sql}).second,"ordinary module trigger collision");
    // This is the sole typed issuer. Its inputs are only the fixed metadata
    // producers below/inside validator, after the entire projection was
    // validated above; the caller never supplies SQL to this adapter.
    const ordinary_metadata_query unused_query;
    const auto query=[&](preimage_kind kind,std::string_view sql,
            std::initializer_list<std::string_view> args,size_t row_limit,size_t result_bytes) {
        need(kind!=preimage_kind::unissued&&args.size()<=ordinary_preimage_statement::parameter_limit(),
            "ordinary preimage unissued purpose/parameter count");
        size_t parameter_bytes=0;
        for(const auto value:args){
            need(value.find('\0')==std::string_view::npos&&
                 value.size()<=ordinary_preimage_statement::parameter_byte_limit()-parameter_bytes,
                 "ordinary preimage parameter type/bytes");parameter_bytes+=value.size();}
        need(result_bytes>0,"ordinary preimage result bytes exhausted");
        const auto creator=std::uint64_t(::getpid());
        ordinary_initialization_storage_scope::charge(native_retirement_storage<
            std::shared_ptr<const ordinary_preimage_statement>>::allocation_bytes());
        native_retirement_storage<std::shared_ptr<const ordinary_preimage_statement>> actual;
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement custody");
        ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement::state));
        auto* held=new ordinary_preimage_statement::state(kind,row_limit,result_bytes,parameter_bytes);
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement state allocation");
        ordinary_preimage_statement* wrapper=nullptr;bool held_owned_by_wrapper=false;
        try {
            ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement));
            wrapper=new ordinary_preimage_statement(creator,held);held_owned_by_wrapper=true;
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement allocation");
            held->sql.assign(sql.data(),sql.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement SQL copy");
            held->parameters.reserve(args.size());
            for(const auto value:args)held->parameters.emplace_back(value.data(),value.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement parameter copy");
            const auto* entrusted=wrapper;wrapper=nullptr;
            actual.get<0>()=std::shared_ptr<const ordinary_preimage_statement>(entrusted,
                &ordinary_preimage_statement::destroy_,
                ordinary_initialization_allocator<ordinary_preimage_statement>{});
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement publication");
        } catch (...) {
            if(creator==std::uint64_t(::getpid())){
                if(wrapper)delete wrapper;else if(!held_owned_by_wrapper)delete held;
                // Failed shared_ptr publication already used its deleter.
            }
            throw;
        }
        return issued_query(actual.get<0>());
    };
    const preimage_reader_query issued(query);
    validator v{{unused_query,limits},{},{},{},{},{}};v.read.issued_query=&issued;
    v.result.schema_cookie=v.read.cookie("main",preimage_kind::main_cookie);
    v.result.temp_schema_cookie=v.read.cookie("temp",preimage_kind::temp_cookie);
    v.inventory();
    v.temp_inventory=v.read.get(limit_sql("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT ",limits.objects+1),{},limits.objects,preimage_kind::temp_inventory);
    // These exact unqualified eponymous metadata producers are library
    // namespace, not application schema objects. Close both actual schemas
    // BEFORE any TVF producer can resolve/connect a persisted object instead.
    // This is a new preimage contract only; the old validator is unchanged.
    const auto metadata_producer_name=[](std::string_view name)noexcept {
        return name=="pragma_table_xinfo"||name=="pragma_foreign_key_list"||
               name=="pragma_index_list"||name=="pragma_index_xinfo"||
               name=="pragma_database_list";
    };
    for(const auto& [name,unused]:v.all)need(!metadata_producer_name(name),
        "ordinary preimage MAIN metadata producer namespace collision");
    // Complete MAIN and TEMP inventory precedes every ordinary table PRAGMA
    // and any possible selected-module connection. Never use table_list.
    const auto selected_name=[&](std::string_view raw){const auto name=fold(raw);
        if(table_names.count(name)||program_names.count(name))return true;
        for(const auto& family:derived_names)if(name.starts_with(family+"_"))return true;
        for(const auto& model:p.models)if(name.starts_with("_"+fold(model)+"_"))return true;
        return false;};
    ordinary_schema_preimage result;
    ordinary_initialization_text_set temp_names;
    for(const auto& row:v.temp_inventory){(void)text_view(row,"type");
        const auto name=text_view(row,"name"),owner=text_view(row,"owner"),sql=is_null(row,"sql")?std::string_view{}:text_view(row,"sql");
        const auto& f=ordinary_object_metadata_view{name,owner,sql};
        need(f.name.size()<=256&&f.owner.size()<=256&&f.sql.size()<=limits.sql_bytes&&
             temp_names.insert(fold(f.name)).second,"ordinary preimage TEMP object size/collision");
        need(!metadata_producer_name(fold(f.name)),
            "ordinary preimage TEMP metadata producer namespace collision");
        need(!selected_name(f.name)&&!selected_name(f.owner),"ordinary selected TEMP metadata shadow refused before connection");
        }
    // A missing module must have no surviving shadow or named trigger. An
    // existing module must have the complete exact pinned pre-connect family.
    ordinary_initialization_vector<ordinary_derived_metadata> modules;
    for(const auto& d:p.derived){const auto found=v.all.find(fold(d.table));
        if(found==v.all.end()){result.absent_tables.push_back(own(d.table));continue;}
        (void)v.fact(d.model,"table",d.model);
        const auto& f=v.fact(d.table,"table",d.table);
        auto described=module_declaration(d,f.sql);
        preflight_module_schema(v,described,module_programs);modules.push_back(std::move(described));}
    // The unchanged actual ordinary/shadow column/index/FK cross-checks are
    // safe only after the declarations above exclude modules/views/expressions.
    // No module_columns(), module(), configuration row, or application row is
    // evaluated by this preimage procedure.
    for(const auto& [name,t]:p.tables){
        if(!v.all.count(fold(name))){result.absent_tables.push_back(own(name));continue;}
        ordinary_initialization_text_set migratable;
        if(t.role==ordinary_projection_role::model)for(const auto& c:t.columns)
            if(fold(c.name)!="globalid")migratable.insert(fold(c.name));
        v.ordinary_table(name,t.create_sql,migratable,t.shadow_columns,t.index_programs,true);
        // Read phase never validates/adopts this table's trigger bundle.
        // B's existing complete enrolled-program validation runs later, still
        // in this SAME snapshot, before any policy/facility publication.
    }
    for(const auto& m:modules){v.keep(v.fact(m.table,"table",m.table));v.owned.insert(fold(m.table));
        const auto d=std::find_if(p.derived.begin(),p.derived.end(),[&](const auto& x){return x.table==m.table;});
        need(d!=p.derived.end(),"ordinary preimage module identity");
        for(const auto& sql:d->programs)v.trigger(sql,d->model);
        for(const auto& spec:module_shadows(m))v.shadow(spec.name,spec.body,spec.indexes);}
    // Reserve ABSENT selected names as well. Orphan programs, aliases, indexes
    // and old/custom sidecars cannot gain authority merely because their
    // expected parent/table has not yet been created by this startup.
    v.owned.insert(table_names.begin(),table_names.end());
    for(const auto& d:p.derived)v.families.push_back(fold(d.table));
    for(const auto& name:p.models)v.families.push_back("_"+fold(name));
    ordinary_initialization_text_set plain_tables;
    for(const auto& [name,unused]:p.tables)if(v.result.tables.count(name))plain_tables.insert(fold(name));
    for(const auto& name:program_names){
        const auto found=v.all.find(name);
        const bool deferred_trigger=found!=v.all.end()&&found->second.type=="trigger"&&
            plain_tables.count(fold(found->second.owner));
        need(found==v.all.end()||v.allowed.count(name)||deferred_trigger,
            "ordinary preimage orphan or unapproved named program");
    }
    v.close_read_ownership(plain_tables);
    need(v.read.cookie("main",preimage_kind::main_cookie)==v.result.schema_cookie&&v.read.cookie("temp",preimage_kind::temp_cookie)==v.result.temp_schema_cookie,
        "ordinary retained preimage schema view changed");
    result.schema_cookie=v.result.schema_cookie;result.temp_schema_cookie=v.result.temp_schema_cookie;
    result.inspected_rows=v.read.rows;result.copied_bytes=v.read.bytes;
    for(auto& row:v.temp_inventory)result.temp_objects.push_back(take_inventory_object(row));
    for(const auto& [name,metadata]:v.result.tables){
        const auto& object=v.fact(name,"table",name);
        // The same actual xinfo/index/FK checks above validated every column
        // and WITHOUT ROWID PK layout against this actual closed declaration.
        const auto actual=parse_table(object.sql,name,true);
        result.plain_shapes.push_back({own(name),std::uint64_t(metadata.columns.size()),actual.without_rowid});
    }
    // Even a module trigger checked for safe pre-connect ownership supplies
    // no origin authority through this SELECT-only result type/entry point.
    for(auto& [name,f]:v.all)result.main_objects.push_back(std::move(f));
    std::sort(result.absent_tables.begin(),result.absent_tables.end());
    need(std::adjacent_find(result.absent_tables.begin(),result.absent_tables.end())==result.absent_tables.end(),
        "ordinary preimage absent identity collision");
    return result;
}

ordinary_schema_metadata ordinary_initialization_schema_validation::complete_(const ordinary_schema_projection& projection,
    const ordinary_initialization_preimage_query& query,const ordinary_metadata_limits& limits) {
    const ordinary_metadata_query no_generic_fallback;
    return complete_body_(projection,no_generic_fallback,limits,&query);
}
ordinary_schema_metadata validate_ordinary_schema(const ordinary_schema_projection& p,
    const ordinary_metadata_query& query,const ordinary_metadata_limits& limits){
    return ordinary_initialization_schema_validation::complete_body_(p,query,limits,nullptr);
}
ordinary_schema_metadata ordinary_initialization_schema_validation::complete_body_(const ordinary_schema_projection& p,
    const ordinary_metadata_query& query,const ordinary_metadata_limits& limits,const ordinary_initialization_preimage_query* issued_query){
    need((query||(issued_query&&*issued_query))&&limits.objects&&limits.objects<=131072&&limits.rows&&limits.rows<=1048576&&
         limits.bytes&&limits.bytes<=64*1024*1024&&limits.sql_bytes&&limits.sql_bytes<=1024*1024&&limits.sql_bytes<=limits.bytes,
         "ordinary metadata limits refused");
    need((issued_query||!p.models.empty())&&p.models.size()<=ordinary_sync_limits::models&&!p.tables.empty()&&p.tables.size()<=ordinary_sync_limits::projection_tables+(issued_query?16:0)&&p.derived.size()<=ordinary_sync_limits::properties,
         "ordinary projection shape bound");
    // Validate the whole supplied description before the first query. Its
    // caller provenance is trusted, but malformed/colliding descriptions must
    // not acquire an accidental second ownership interpretation.
    ordinary_initialization_text_set table_names,program_names;size_t declared_bytes=0,columns=0;
    const auto charge=[&](std::string_view s){need(s.size()<=limits.sql_bytes&&s.size()<=16*1024*1024-declared_bytes,"ordinary projection admission bound");declared_bytes+=s.size();};
    const auto add_program=[&](std::string_view sql,std::string_view kind){charge(sql);need(program_names.insert(object_name(sql,kind)).second,"ordinary projection program collision");};
    for(const auto& [name,t]:p.tables){identifier(name);need(t.name==name&&table_names.insert(fold(name)).second,"ordinary projection table identity collision");charge(t.create_sql);
        need(t.columns.size()<=ordinary_sync_limits::columns_per_table&&t.columns.size()<=ordinary_sync_limits::projection_columns-columns,"ordinary projection columns bound");columns+=t.columns.size();
        ordinary_initialization_text_set names;if(t.local_id)names.insert("id");for(const auto& c:t.columns){identifier(c.name);need(names.insert(fold(c.name)).second,"ordinary projection column collision");}
        for(const auto& n:t.shadow_columns){identifier(n);need(names.insert(fold(n)).second,"ordinary projection shadow collision");}
        ordinary_initialization_text_set audit_names;
        for(const auto& sql:t.audit_programs){add_program(sql,"trigger");audit_names.insert(object_name(sql,"trigger"));}
        need(t.alternate_audit_programs.size()<=1,"ordinary audit variant bound");
        for(const auto& alternate:t.alternate_audit_programs){ordinary_initialization_text_set names;for(const auto& sql:alternate){charge(sql);const auto n=object_name(sql,"trigger");need(names.insert(n).second,"ordinary alternate audit collision");
            if(!audit_names.count(n)){need(program_names.insert(n).second,"ordinary alternate program collision");audit_names.insert(n);}}}
        for(const auto& sql:t.other_programs)add_program(sql,"trigger");for(const auto& sql:t.index_programs)add_program(sql,"index");}
    for(const auto& name:p.models){identifier(name);need(p.tables.count(name)&&p.tables.at(name).role==ordinary_projection_role::model&&name[0]!='_'&&fold(name)!="auditlog"&&fold(name)!="sqlite_sequence","ordinary projection root mismatch");}
    ordinary_initialization_text_set derived_names;
    for(const auto& d:p.derived){identifier(d.table);identifier(d.model);identifier(d.property);need(p.tables.count(d.model)&&table_names.insert(fold(d.table)).second&&derived_names.insert(fold(d.table)).second,"ordinary derived ownership collision");charge(d.create_sql);for(const auto& sql:d.programs)add_program(sql,"trigger");}
    for(const auto& n:program_names)need(!table_names.count(n),"ordinary table/program collision");
    for(const auto& family:derived_names)for(const auto& table:table_names)need(table==family||!table.starts_with(family+"_"),"ordinary derived family/table collision");
    // References remain inside this synchronous call's immutable description.
    // A familiar trigger name alone is never permission to connect a module.
    ordinary_initialization_text_map<expected_module_trigger> module_programs;
    for(const auto& [name,t]:p.tables)for(const auto& sql:t.other_programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{name,sql}).second,"ordinary module trigger collision");
    for(const auto& d:p.derived)for(const auto& sql:d.programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{d.model,sql}).second,"ordinary module trigger collision");
    // Populated only AFTER this exact validator has closed a selected Vec
    // declaration and all actual shadows/indexes/programs in this SAME view.
    // A catalog name or observed SQL by itself cannot issue the connect kind.
    ordinary_initialization_text_set preflight_vectors;
    const auto closed=[&](preimage_kind id,std::string_view sql,std::initializer_list<std::string_view> args,size_t row_limit,size_t result_bytes) {
        need(issued_query&&*issued_query,"complete metadata lacks its actual typed adapter");
        if(id==preimage_kind::table_columns&&args.size()==1&&
           sql==std::string_view(limit_sql("SELECT cid,name,type,\"notnull\" AS nn,dflt_value AS def,pk,hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid LIMIT ",ordinary_sync_limits::columns_per_table+2))&&
           preflight_vectors.count(*args.begin()))
            id=preimage_kind::module_vector_columns;
        if(id==preimage_kind::unissued) {
            if(sql=="PRAGMA main.schema_version")id=preimage_kind::main_cookie;
            else if(sql=="PRAGMA temp.schema_version")id=preimage_kind::temp_cookie;
            else if(sql==std::string_view(limit_sql("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT ",limits.objects+1)))id=preimage_kind::temp_inventory;
            else {
                // Only these actual compiled selected-module emitters can
                // issue application shadow-row metadata. Observed schema SQL
                // and arbitrary caller strings never reach this producer.
                for(const auto& module:p.derived) {
                    if(module.kind==ordinary_derived_kind::full_text&&sql==std::string_view("SELECT k,v FROM main."+ordinary_identifier_sql_(module.table+"_config")+" LIMIT 2")&&row_limit<=1)
                        id=preimage_kind::module_fts_config;
                    if(module.kind==ordinary_derived_kind::vector&&
                       (sql==std::string_view("SELECT key,value FROM main."+ordinary_identifier_sql_(module.table+"_info")+" LIMIT 5")||
                        sql==std::string_view("SELECT key,value FROM main."+ordinary_identifier_sql_(module.table+"_info")+" LIMIT 6"))&&row_limit<=5)
                        id=preimage_kind::module_vector_info;
                }
            }
        }
        need(id!=preimage_kind::unissued&&args.size()<=ordinary_preimage_statement::parameter_limit()&&result_bytes,
            "unissued complete metadata purpose/count/bound");
        size_t parameter_bytes=0;
        for(const auto value:args){need(value.find('\0')==std::string_view::npos&&
            value.size()<=ordinary_preimage_statement::parameter_byte_limit()-parameter_bytes,"complete metadata parameter bytes");parameter_bytes+=value.size();}
        const auto creator=std::uint64_t(::getpid());
        ordinary_initialization_storage_scope::charge(native_retirement_storage<
            std::shared_ptr<const ordinary_preimage_statement>>::allocation_bytes());
        native_retirement_storage<std::shared_ptr<const ordinary_preimage_statement>> actual;
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement custody");
        ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement::state));
        auto* held=new ordinary_preimage_statement::state(id,row_limit,result_bytes,parameter_bytes);
        need(creator==std::uint64_t(::getpid()),"inherited preimage statement state allocation");
        ordinary_preimage_statement* wrapper=nullptr;bool held_owned_by_wrapper=false;
        try {
            ordinary_initialization_storage_scope::charge(sizeof(ordinary_preimage_statement));
            wrapper=new ordinary_preimage_statement(creator,held);held_owned_by_wrapper=true;
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement allocation");
            held->sql.assign(sql.data(),sql.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement SQL copy");
            held->parameters.reserve(args.size());
            for(const auto value:args)held->parameters.emplace_back(value.data(),value.size());
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement parameter copy");
            const auto* entrusted=wrapper;wrapper=nullptr;
            actual.get<0>()=std::shared_ptr<const ordinary_preimage_statement>(entrusted,
                &ordinary_preimage_statement::destroy_,
                ordinary_initialization_allocator<ordinary_preimage_statement>{});
            need(creator==std::uint64_t(::getpid()),"inherited preimage statement publication");
        } catch (...) {
            if(creator==std::uint64_t(::getpid())){
                if(wrapper)delete wrapper;else if(!held_owned_by_wrapper)delete held;
                // Failed shared_ptr publication already used its deleter.
            }
            throw;
        }
        return (*issued_query)(actual.get<0>());
    };
    const preimage_reader_query typed(closed);
    validator v{{query,limits},{},{},{},{},{}};if(issued_query)v.read.issued_query=&typed;
    v.result.schema_cookie=v.read.cookie("main");v.result.temp_schema_cookie=v.read.cookie("temp");v.inventory();
    v.temp_inventory=v.read.get(limit_sql("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT ",limits.objects+1),{},limits.objects);
    // Targeted main-schema xinfo/index/FK queries do not initialize unrelated
    // views or virtual tables. In particular, never use table_list: SQLite's
    // initializer connects every main view/module before its name filter.
    // Close selected TEMP aliases before a selected module can read shadows.
    const auto selected_name=[&](std::string_view raw){const auto name=fold(raw);
        if(table_names.count(name)||program_names.count(name))return true;
        for(const auto& family:derived_names)if(name.starts_with(family+"_"))return true;
        for(const auto& model:p.models)if(name.starts_with("_"+fold(model)+"_"))return true;
        return false;};
    for(const auto& r:v.temp_inventory)need(!selected_name(text(r,"name"))&&!selected_name(text(r,"owner")),"ordinary selected TEMP metadata shadow refused before connection");
    // Preflight every selected module and all of its shadow/index declarations
    // before any selected xConnect. Unrelated modules remain inspection-only
    // sqlite_schema entries and never become projected rows or authority.
    for(const auto& d:p.derived){const auto found=v.all.find(fold(d.table));if(found==v.all.end()){
            need(d.kind==ordinary_derived_kind::vector,"ordinary required derived table missing");continue;}
        const auto& f=v.fact(d.table,"table",d.table);const auto described=module_declaration(d,f.sql);preflight_module_schema(v,described,module_programs);
        if(d.kind==ordinary_derived_kind::vector)preflight_vectors.emplace(d.table);}

    for(const auto& d:p.derived)v.families.push_back(fold(d.table));
    // Every generated sidecar of a selected model lives in this namespace.
    // Unknown old/custom sidecars must not silently disappear from a snapshot.
    for(const auto& name:p.models)v.families.push_back("_"+fold(name));
    for(const auto& [name,t]:p.tables){ordinary_initialization_text_set migratable;
        if(t.role==ordinary_projection_role::model)for(const auto& c:t.columns)if(fold(c.name)!="globalid")migratable.insert(fold(c.name));
        v.ordinary_table(name,t.create_sql,migratable,t.shadow_columns,t.index_programs,true);
        v.audits(t);for(const auto& sql:t.other_programs)v.trigger(sql,name);}
    for(const auto& d:p.derived)v.module(d);
    v.close_ownership();need(v.read.cookie("main")==v.result.schema_cookie&&v.read.cookie("temp")==v.result.temp_schema_cookie,"ordinary retained schema view changed");
    v.result.inspected_rows=v.read.rows;v.result.copied_bytes=v.read.bytes;return std::move(v.result);
}
} // namespace lattice::detail
