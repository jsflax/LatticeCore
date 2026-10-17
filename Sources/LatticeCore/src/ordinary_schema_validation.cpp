#include "ordinary_schema_validation.hpp"
#include <lattice/derived_program.hpp>
#include <sqlite-vec.h>
#include <algorithm>
#include <limits>
#include <set>

namespace lattice::detail {
namespace {
using tokens=std::vector<std::string>;
void need(bool ok,const char* message) { if(!ok)throw ordinary_schema_validation_error(message); }
std::string fold(std::string s){for(char& c:s)if(c>='A'&&c<='Z')c+=32;return s;}
bool word(char c){return(c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='_'||(c>='0'&&c<='9');}
void identifier(const std::string& s){need(!s.empty()&&s.size()<=256,"ordinary metadata identifier bound");
    need(!(s[0]>='0'&&s[0]<='9'),"ordinary metadata identifier start");for(char c:s)need(word(c),"ordinary metadata identifier spelling");}
std::string quoted(const std::string& s){identifier(s);return "\""+s+"\"";}
const column_value_t& cell(const database::row_t& r,const char* key){auto i=r.find(key);need(i!=r.end(),"ordinary metadata missing result field");return i->second;}
int64_t number(const database::row_t& r,const char* key){const auto& v=cell(r,key);need(std::holds_alternative<int64_t>(v),"ordinary metadata noninteger");return std::get<int64_t>(v);}
std::string text(const database::row_t& r,const char* key){const auto& v=cell(r,key);std::string s;
    if(auto p=std::get_if<std::string>(&v))s=*p;else if(auto p=std::get_if<std::vector<uint8_t>>(&v)){if(!p->empty())s.assign(reinterpret_cast<const char*>(p->data()),p->size());}
    else need(false,"ordinary metadata nontext");need(s.find('\0')==std::string::npos,"ordinary metadata NUL");return s;}
bool is_null(const database::row_t& r,const char* key){return std::holds_alternative<std::nullptr_t>(cell(r,key));}
size_t size_of(const column_value_t& v){if(auto p=std::get_if<std::string>(&v))return p->size();if(auto p=std::get_if<std::vector<uint8_t>>(&v))return p->size();return 8;}
struct reader {
    const ordinary_metadata_query& query;const ordinary_metadata_limits& limits;size_t rows=0,bytes=0;
    std::vector<database::row_t> get(const std::string& sql,const std::vector<column_value_t>& args,size_t cap){
        need(cap&&cap<=limits.rows&&sql.size()<=limits.sql_bytes,"ordinary metadata query bounds");
        const auto remaining=limits.rows-rows;need(remaining>0,"ordinary metadata aggregate row bound");
        const auto admitted=std::min(cap,remaining);auto result=query(sql,args,admitted);need(result.size()<=admitted,"ordinary metadata callback overrun");
        need(result.size()<=limits.rows-rows,"ordinary metadata aggregate row bound");
        for(const auto& r:result){need(r.size()<=16,"ordinary metadata field bound");for(const auto& [key,value]:r){
            const auto n=size_of(value);need(n<=limits.bytes-bytes,"ordinary metadata aggregate byte bound");bytes+=n;
            need(key.size()<=256&&key.size()<=limits.bytes-bytes,"ordinary metadata key bound");bytes+=key.size();}}
        rows+=result.size();return result;
    }
    int64_t cookie(const char* schema){auto r=get(std::string("PRAGMA ")+schema+".schema_version",{},1);need(r.size()==1,"ordinary retained schema cookie missing");return number(r[0],"schema_version");}
};
// Closed SQL grammar: comments, unquoted non-ASCII names, parameters and any
// unrecognized token refuse. String bytes/case remain exact; identifier quotes
// and ASCII case are incidental. This is not a general SQL canonicalizer.
tokens lex(const std::string& s){tokens out;for(size_t i=0;i<s.size();){const char c=s[i];
    if(c==' '||c=='\t'||c=='\r'||c=='\n'){++i;continue;}
    if(c=='\''||c=='"'||c=='`'){const char q=c;++i;std::string x;bool closed=false;while(i<s.size()){
        const char a=s[i++];if(a==q){if(i<s.size()&&s[i]==q){x+=q;++i;}else{closed=true;break;}}else x+=a;}
        need(closed,"ordinary SQL unterminated quote");if(q=='\'')out.push_back("'"+x+"'");else {identifier(x);out.push_back(fold(x));}continue;}
    if(word(c)){auto start=i++;while(i<s.size()&&(word(s[i])||(s[i]=='.'&&s[start]>='0'&&s[start]<='9')))++i;out.push_back(fold(s.substr(start,i-start)));continue;}
    // Generated union cascades spell inequality exactly !=. A bare ! is not SQL.
    if(c=='!'&&i+1<s.size()&&s[i+1]=='='){out.push_back("!=");i+=2;continue;}
    need(std::string("(),;.=+-*%|[]<>").find(c)!=std::string::npos,"ordinary SQL token refused");
    need(!(c=='-'&&i+1<s.size()&&s[i+1]=='-'),"ordinary SQL comment refused");
    if(c=='|'&&i+1<s.size()&&s[i+1]=='|'){out.push_back("||");i+=2;}else {out.emplace_back(1,c);++i;}}
    if(!out.empty()&&out.back()==";")out.pop_back();return out;
}
bool contains(const tokens& v,const tokens& pattern){return std::search(v.begin(),v.end(),pattern.begin(),pattern.end())!=v.end();}
void remove_if(tokens& v){if(v.size()<3||v[0]!="create")return;const size_t i=(v[1]=="unique"||v[1]=="virtual")?3:2;if(i+2<v.size()&&v[i]=="if"&&v[i+1]=="not"&&v[i+2]=="exists")v.erase(v.begin()+i,v.begin()+i+3);}
std::string object_name(const std::string& sql,const std::string& kind){auto t=lex(sql);remove_if(t);size_t at=1;if(at<t.size()&&t[at]=="unique")++at;
    need(t.size()>at+1&&t[0]=="create"&&t[at]==kind,"ordinary generated object prefix");return t[at+1];}
bool same_program(const std::string& a,const std::string& b){auto x=lex(a),y=lex(b);remove_if(x);remove_if(y);return x==y;}
// Recover ordering DATA only. Even a successful extraction must subsequently
// reproduce one complete approved typed bundle through unchanged same_program.
// Never interpret arbitrary SQL expressions or search column permutations.
std::optional<std::vector<std::string>> audit_insert_order(const std::string& sql){
    const auto t=lex(sql);std::optional<std::vector<std::string>> result;
    for(size_t i=0;i+1<t.size();++i)if(t[i]=="json_array"&&t[i+1]=="("){
        if(result)return {};std::vector<std::string> order;size_t at=i+2;
        for(;;){
            if(at>=t.size()||order.size()>=ordinary_sync_limits::columns_per_table)return {};
            const auto& value=t[at++];
            if(value.size()<3||value.front()!='\''||value.back()!='\'')return {};
            order.push_back(value.substr(1,value.size()-2));
            if(at>=t.size())return {};
            if(t[at]==")")break;
            if(t[at++]!=",")return {};
        }
        result=std::move(order);
    }
    return result;
}
struct table_ddl {std::map<std::string,tokens> columns;std::vector<std::string> order;std::vector<tokens> constraints;bool without_rowid=false;};
table_ddl parse_table(const std::string& sql,const std::string& name){auto t=lex(sql);remove_if(t);size_t i=2;
    need(t.size()>4&&t[0]=="create"&&t[1]=="table","ordinary table declaration kind");if(i+2<t.size()&&t[i]=="main"&&t[i+1]==".")i+=2;
    need(i+1<t.size()&&t[i++]==fold(name)&&t[i++]=="(","ordinary table declaration identity");table_ddl d;tokens part;size_t depth=0;bool ended=false;
    const auto finish=[&]{need(!part.empty(),"ordinary empty table clause");if(part[0]=="check"||part[0]=="primary")d.constraints.push_back(part);
        else{identifier(part[0]);need(d.columns.emplace(part[0],part).second,"ordinary duplicate table column");d.order.push_back(part[0]);}part.clear();};
    for(;i<t.size();++i){if(t[i]=="(" ){++depth;part.push_back(t[i]);}
        else if(t[i]==")"){if(depth){--depth;part.push_back(t[i]);}else{finish();++i;ended=true;break;}}
        else if(t[i]==","&&!depth)finish();else part.push_back(t[i]);}
    need(ended,"ordinary table declaration close");if(i<t.size()){need(i+2==t.size()&&t[i]=="without"&&t[i+1]=="rowid","ordinary table suffix refused");d.without_rowid=true;}
    return d;
}
struct index_spec {std::string name,origin,sql;std::vector<std::string> keys;bool unique=true;};
std::vector<std::string> key_list(const tokens& t,size_t at){need(at<t.size()&&t[at++]=="(","ordinary index key open");std::vector<std::string> keys;
    while(at<t.size()&&t[at]!=")"){identifier(t[at]);keys.push_back(t[at++]);if(at<t.size()&&t[at]==","){++at;continue;}break;}
    need(!keys.empty()&&at+1==t.size()&&t[at]==")","ordinary index expression/options refused");return keys;}
std::vector<index_spec> implicit_indexes(const table_ddl& d,const std::string& table){std::vector<index_spec> out;size_t ordinal=0;
    for(const auto& n:d.order){const auto& c=d.columns.at(n);if(contains(c,{"unique"})||contains(c,{"primary","key"})){
        if(contains(c,{"primary","key"})&&c.size()>1&&c[1]=="integer"&&!d.without_rowid)continue;
        out.push_back({"sqlite_autoindex_"+table+"_"+std::to_string(++ordinal),contains(c,{"primary","key"})?"pk":"u",{}, {n},true});}}
    for(const auto& c:d.constraints)if(c[0]=="primary"){need(c.size()>2&&c[1]=="key","ordinary primary key syntax");out.push_back({"sqlite_autoindex_"+table+"_"+std::to_string(++ordinal),"pk",{},key_list(c,2),true});}
    return out;
}
index_spec explicit_index(const std::string& sql,const std::string& table){auto t=lex(sql);remove_if(t);size_t i=1;bool unique=false;
    if(i<t.size()&&t[i]=="unique"){unique=true;++i;}need(t.size()>i+4&&t[0]=="create"&&t[i++]=="index","ordinary explicit index syntax");
    const auto name=t[i++];identifier(name);need(t[i++]=="on"&&t[i++]==fold(table),"ordinary explicit index owner");return{name,"c",sql,key_list(t,i),unique};}
std::optional<tokens> default_tokens(const tokens& c){auto i=std::find(c.begin(),c.end(),"default");if(i==c.end())return {};return tokens(i+1,c.end());}
tokens unparenthesized(tokens t){for(;;){if(t.size()<2||t.front()!="("||t.back()!=")")return t;size_t depth=0;bool wraps=true;for(size_t i=0;i<t.size();++i){if(t[i]=="(")++depth;else if(t[i]==")"){need(depth>0,"ordinary default parentheses");--depth;if(!depth&&i+1<t.size()){wraps=false;break;}}}if(!wraps)return t;t.erase(t.begin());t.pop_back();}}
std::string type_of(const tokens& c){if(c.size()<2||c[1]=="primary"||c[1]=="not"||c[1]=="unique")return {};return c[1];}
// Only classify an unrelated table's declaration prefix. Its remaining SQL
// need not belong to our generated grammar; it confers no owned-table grant.
// Refuse an ambiguous prefix instead of instantiating an unknown module.
bool virtual_declaration(const std::string& sql){size_t at=0;const auto next=[&]{while(at<sql.size()&&(sql[at]==' '||sql[at]=='\n'||sql[at]=='\t'||sql[at]=='\r'))++at;const auto start=at;while(at<sql.size()&&word(sql[at]))++at;need(at>start,"ordinary unrelated declaration prefix refused");return fold(sql.substr(start,at-start));};
    need(next()=="create","ordinary unrelated declaration not CREATE");const auto kind=next();need(kind=="table"||kind=="virtual","ordinary unrelated table prefix refused");return kind=="virtual";
}
struct validator {
    reader read;ordinary_schema_metadata result;std::map<std::string,ordinary_object_metadata> all;std::set<std::string> allowed,owned;
    std::vector<std::string> families;
    std::vector<database::row_t> temp_inventory;
    const ordinary_object_metadata& fact(const std::string& name,const std::string& type,const std::string& owner){auto i=all.find(fold(name));
        need(i!=all.end()&&i->second.type==type&&fold(i->second.owner)==fold(owner),"ordinary required metadata object missing/owner mismatch");return i->second;}
    void keep(const ordinary_object_metadata& f){need(allowed.insert(fold(f.name)).second,"ordinary overlapping object ownership");result.objects.push_back(f);}
    void inventory(){auto r=read.get("SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema ORDER BY name COLLATE BINARY LIMIT "+std::to_string(read.limits.objects+1),{},read.limits.objects);
        for(const auto& row:r){ordinary_object_metadata f{text(row,"type"),text(row,"name"),text(row,"owner"),is_null(row,"sql")?std::string{}:text(row,"sql")};
            need(f.name.size()<=256&&f.owner.size()<=256&&f.sql.size()<=read.limits.sql_bytes,"ordinary schema object size");
            need(all.emplace(fold(f.name),std::move(f)).second,"ordinary case-folded schema object collision");}}
    std::vector<ordinary_column_metadata> columns(const std::string& table){auto r=read.get("SELECT cid,name,type,\"notnull\" AS nn,dflt_value AS def,pk,hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid LIMIT "+std::to_string(ordinary_sync_limits::columns_per_table+2),{table},ordinary_sync_limits::columns_per_table+1);
        std::vector<ordinary_column_metadata> out;std::set<std::string> names;
        for(const auto& row:r){ordinary_column_metadata c{text(row,"name"),fold(text(row,"type")),number(row,"cid"),number(row,"nn"),number(row,"pk"),number(row,"hidden"),{}};
            need(c.cid==int64_t(out.size())&&names.insert(fold(c.name)).second,"ordinary column identity/order collision");
            if(!is_null(row,"def"))c.default_sql=text(row,"def");out.push_back(std::move(c));}return out;}
    void foreign_keys(const std::string& table){need(read.get("SELECT id FROM pragma_foreign_key_list(?, 'main') LIMIT 1",{table},1).empty(),"ordinary owned foreign key refused");}
    void indexes(const std::string& table,const table_ddl& ddl,const std::vector<ordinary_column_metadata>& cols,const std::vector<std::string>& explicit_sql,bool expose){
        auto specs=implicit_indexes(ddl,table);for(const auto& sql:explicit_sql)specs.push_back(explicit_index(sql,table));
        std::map<std::string,index_spec> expected;for(auto& s:specs)need(expected.emplace(fold(s.name),std::move(s)).second,"ordinary index name collision");
        auto list=read.get("SELECT name,\"unique\" AS uniq,origin,partial FROM pragma_index_list(?, 'main') LIMIT "+std::to_string(expected.size()+1),{table},std::max<size_t>(expected.size(),1));
        need(list.size()==expected.size(),"ordinary missing/extra index");std::set<std::string> seen;
        for(const auto& row:list){const auto name=text(row,"name");auto e=expected.find(fold(name));need(e!=expected.end()&&seen.insert(e->first).second,"ordinary unknown index");const auto& s=e->second;
            need(number(row,"uniq")==int64_t(s.unique)&&text(row,"origin")==s.origin&&number(row,"partial")==0,"ordinary index constraint mismatch");
            if(!(ddl.without_rowid&&s.origin=="pk")){const auto& f=fact(name,"index",table);need(s.origin=="c"?same_program(f.sql,s.sql):f.sql.empty(),"ordinary index program mismatch");keep(f);}
            else need(!all.count(fold(name)),"ordinary without-rowid index object mismatch");
            std::vector<std::string> order=s.keys;if(ddl.without_rowid){for(const auto& c:cols)if(std::find(order.begin(),order.end(),fold(c.name))==order.end())order.push_back(fold(c.name));}else order.push_back({});
            auto detail=read.get("SELECT seqno,cid,name,\"desc\" AS descending,coll,\"key\" AS iskey FROM pragma_index_xinfo(?, 'main') ORDER BY seqno LIMIT "+std::to_string(order.size()+1),{name},order.size());
            need(detail.size()==order.size(),"ordinary index column count");
            for(size_t i=0;i<order.size();++i){int64_t cid=-1;std::string coll="binary";if(!order[i].empty()){
                auto c=std::find_if(cols.begin(),cols.end(),[&](const auto& x){return fold(x.name)==order[i];});need(c!=cols.end(),"ordinary index missing field");cid=c->cid;
                if(contains(ddl.columns.at(order[i]),{"collate","nocase"}))coll="nocase";}
                const auto& d=detail[i];need(number(d,"seqno")==int64_t(i)&&number(d,"cid")==cid&&number(d,"descending")==0&&fold(text(d,"coll"))==coll&&number(d,"iskey")==int64_t(i<s.keys.size())&&(order[i].empty()?is_null(d,"name"):fold(text(d,"name"))==order[i]),"ordinary index key/collation mismatch");}
            if(expose&&s.origin=="c"&&s.unique){const auto& f=fact(name,"index",table);std::vector<std::string> names;
                for(const auto& key:s.keys){auto c=std::find_if(cols.begin(),cols.end(),[&](const auto& x){return fold(x.name)==key;});need(c!=cols.end(),"ordinary unique field missing");names.push_back(c->name);}
                result.unique_indexes.push_back({f.name,table,f.sql,std::move(names)});}
        }
    }
    void ordinary_table(const std::string& table,const std::string& expected_sql,const std::set<std::string>& migratable,const std::vector<std::string>& shadow_columns,const std::vector<std::string>& index_sql,bool expose=false){
        const auto& f=fact(table,"table",table);auto actual=parse_table(f.sql,table),expected=parse_table(expected_sql,table);
        for(const auto& name:shadow_columns){auto n=fold(name);need(expected.columns.emplace(n,tokens{n,"text"}).second,"ordinary shadow column collision");expected.order.push_back(n);}
        need(actual.without_rowid==expected.without_rowid&&actual.constraints==expected.constraints&&actual.columns.size()==expected.columns.size(),"ordinary table constraints/column count mismatch");
        for(const auto& [name,e]:expected.columns){auto a=actual.columns.find(name);need(a!=actual.columns.end(),"ordinary required column absent");if(a->second==e)continue;
            need(migratable.count(name)&&e.size()>=2,"ordinary column definition mismatch");const auto type=e[1];
            need(type=="integer"||type=="real"||type=="text"||type=="blob","ordinary migrated column type");
            const tokens plain{name,type},nonnull{name,type,"not","null"};auto with_default=plain;with_default.push_back("default");
            if(type=="integer")with_default.push_back("0");else if(type=="real")with_default.push_back("0.0");else if(type=="text")with_default.push_back("''");else{with_default.push_back("x");with_default.push_back("''");}
            need(a->second==plain||a->second==nonnull||a->second==with_default,"ordinary ungenerated migration constraint/default");}
        // The closed CREATE grammar fixes ordinary/STRICT/WITHOUT ROWID shape.
        // Targeted xinfo and indexes cross-check its actual engine metadata.
        auto cols=columns(table);need(cols.size()==actual.columns.size(),"ordinary pragma/DDL column count mismatch");
        std::map<std::string,int64_t> primary;for(const auto& c:actual.constraints)if(c[0]=="primary"){auto keys=key_list(c,2);for(size_t i=0;i<keys.size();++i)primary[keys[i]]=int64_t(i+1);}
        for(const auto& [n,c]:actual.columns)if(contains(c,{"primary","key"}))need(primary.emplace(n,1).second,"ordinary duplicate primary key");
        for(const auto& c:cols){auto a=actual.columns.find(fold(c.name));need(a!=actual.columns.end()&&fold(c.name)==actual.order[size_t(c.cid)],"ordinary pragma/DDL column order");const auto& clause=a->second;
            need(c.type==type_of(clause)&&c.hidden==0&&c.primary_key==primary[fold(c.name)]&&c.not_null==int64_t(contains(clause,{"not","null"})||(actual.without_rowid&&c.primary_key)),"ordinary pragma/DDL column flags");
            const auto d=default_tokens(clause);need(bool(d)==bool(c.default_sql),"ordinary pragma/DDL default presence");if(d)need(unparenthesized(*d)==unparenthesized(lex(*c.default_sql)),"ordinary pragma/DDL default mismatch");}
        foreign_keys(table);keep(f);owned.insert(fold(table));indexes(table,actual,cols,index_sql,expose);result.tables.emplace(table,ordinary_table_metadata{table,std::move(cols)});
    }
    std::optional<std::vector<std::string>> recovered_audits(const ordinary_projection_table& table,
            const std::vector<std::string>& approved){
        if((table.role!=ordinary_projection_role::model&&table.role!=ordinary_projection_role::union_value)||approved.size()!=3)return {};
        const auto insert_name=fold("Audit"+table.name+"Insert");
        const auto input=all.find(insert_name);
        if(input==all.end()||input->second.type!="trigger"||fold(input->second.owner)!=fold(table.name))return {};
        const std::string* template_insert=nullptr;size_t capacity=0;
        for(const auto& sql:approved){
            if(sql.size()>16*1024*1024-capacity)return {};capacity+=sql.size();
            if(object_name(sql,"trigger")==insert_name){if(template_insert)return {};template_insert=&sql;}
        }
        if(!template_insert)return {};
        const auto expected=audit_insert_order(*template_insert),actual=audit_insert_order(input->second.sql);
        if(!expected||!actual||expected->size()!=actual->size())return {};
        std::set<std::string> allowed(expected->begin(),expected->end()),seen(actual->begin(),actual->end());
        if(allowed.size()!=expected->size()||seen.size()!=actual->size()||seen!=allowed)return {};
        std::map<std::string,const ordinary_projection_column*> declared;
        for(const auto& column:table.columns)
            if(!declared.emplace(column.audit_name,&column).second)return {};
        const auto columns=[&](const std::vector<std::string>& order){
            std::vector<std::pair<std::string,column_type>> out;out.reserve(order.size());
            for(const auto& name:order){const auto found=declared.find(name);if(found==declared.end())return decltype(out){};
                out.emplace_back(name,found->second->type);}
            return out;
        };
        const auto original=columns(*expected),ordered=columns(*actual);
        if(original.size()!=expected->size()||ordered.size()!=actual->size())return {};
        std::set<std::string> no_history;
        for(const auto& name:allowed)if(declared.at(name)->no_history)no_history.insert(name);
        // First prove this approved variant is the SAME protected generator,
        // not a custom SQL template that could confer a different authority.
        size_t index=0;bool exact=true;
        ordinary_model_audit_programs(table.name,original,no_history,{},[&](const std::string& sql){
            if(index>=approved.size()||!same_program(approved[index],sql))exact=false;++index;
        });
        if(!exact||index!=approved.size())return {};
        // One transient three-program candidate; a permutation preserves the
        // aggregate byte size of its already admitted approved generator.
        std::vector<std::string> candidate;size_t bytes=0;
        ordinary_model_audit_programs(table.name,ordered,no_history,{},[&](const std::string& sql){
            need(candidate.size()<3&&sql.size()<=read.limits.sql_bytes&&sql.size()<=capacity-bytes,
                "ordinary recovered audit recipe bound");bytes+=sql.size();candidate.push_back(sql);
        });
        if(candidate.size()!=3)return {};return candidate;
    }
    void audits(const ordinary_projection_table& table){
        const auto matches=[&](const std::vector<std::string>& bundle){for(const auto& sql:bundle){auto name=object_name(sql,"trigger");auto i=all.find(name);
            if(i==all.end()||i->second.type!="trigger"||fold(i->second.owner)!=fold(table.name)||!same_program(i->second.sql,sql))return false;}
            std::set<std::string> names;for(const auto& sql:bundle)names.insert(object_name(sql,"trigger"));
            for(const auto& sql:table.audit_programs){const auto name=object_name(sql,"trigger");if(!names.count(name)&&all.count(name))return false;}
            for(const auto& alternate:table.alternate_audit_programs)for(const auto& sql:alternate){const auto name=object_name(sql,"trigger");if(!names.count(name)&&all.count(name))return false;}
            return true;};
        const std::vector<std::string>* chosen=matches(table.audit_programs)?&table.audit_programs:nullptr;
        if(!chosen)for(const auto& alternate:table.alternate_audit_programs)if(matches(alternate)){chosen=&alternate;break;}
        std::vector<std::string> recovered;
        if(!chosen){
            const auto try_order=[&](const std::vector<std::string>& approved){auto value=recovered_audits(table,approved);
                if(value&&matches(*value)){recovered=std::move(*value);chosen=&recovered;}};
            try_order(table.audit_programs);
            if(!chosen)for(const auto& alternate:table.alternate_audit_programs){try_order(alternate);if(chosen)break;}
        }
        need(chosen,"ordinary complete audit program bundle mismatch");for(const auto& sql:*chosen)trigger(sql,table.name);
    }
    void trigger(const std::string& sql,const std::string& owner){const auto name=object_name(sql,"trigger");const auto& f=fact(name,"trigger",owner);need(same_program(f.sql,sql),"ordinary generated trigger mismatch");keep(f);}
    void module_columns(const std::string& table,const std::vector<ordinary_column_metadata>& expected){auto actual=columns(table);need(actual.size()==expected.size(),"ordinary module column count");
        for(size_t i=0;i<actual.size();++i){const auto& a=actual[i];const auto& e=expected[i];need(a.name==e.name&&a.type==e.type&&a.not_null==e.not_null&&a.primary_key==e.primary_key&&a.hidden==e.hidden&&!a.default_sql,"ordinary module column definition");}
        need(read.get("SELECT id FROM pragma_foreign_key_list(?, 'main') LIMIT 1",{table},1).empty(),"ordinary module foreign key");}
    void shadow(const std::string& name,const std::string& body,const std::vector<std::string>& indexes={}){
        ordinary_table(name,"CREATE TABLE "+quoted(name)+body,{}, {},indexes);}
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
            auto refs=read.get("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'main') LIMIT "+std::to_string(read.limits.objects+1),{f.name},read.limits.objects);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary inbound foreign key outside projection");}
        for(const auto& r:temps)if(text(r,"type")=="table"){
            const auto n=text(r,"name");if(!is_null(r,"sql")&&virtual_declaration(text(r,"sql")))continue;auto refs=read.get("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'temp') LIMIT "+std::to_string(read.limits.objects+1),{n},read.limits.objects);
            for(const auto& row:refs)need(!owned.count(fold(text(row,"target"))),"ordinary TEMP inbound foreign key");}
    }
};
size_t positive(const std::string& s,size_t maximum){need(!s.empty()&&s.size()<=8,"ordinary module numeric bound");size_t n=0;for(char c:s){need(c>='0'&&c<='9',"ordinary module numeric syntax");need(n<=(maximum-size_t(c-'0'))/10,"ordinary module numeric overflow");n=n*10+size_t(c-'0');}need(n&&n<=maximum&&std::to_string(n)==s,"ordinary module numeric range");return n;}
ordinary_column_metadata module_column(std::string name,std::string type={},int64_t nn=0,int64_t pk=0,int64_t hidden=0){return{std::move(name),std::move(type),0,nn,pk,hidden,{}};}
ordinary_derived_metadata module_declaration(const ordinary_projection_derived& d,const std::string& sql){
    ordinary_derived_metadata m{d.kind,d.model,d.property,d.table,sql,true,0,0,0};
    if(d.kind==ordinary_derived_kind::vector){
        const auto t=lex(sql);auto i=std::find(t.begin(),t.end(),"float");need(i!=t.end()&&t.end()-i>=4&&i[1]=="["&&i[3]=="]","ordinary vector dimension declaration");m.dimensions=positive(i[2],8192);
        const auto nlist=std::find(t.begin(),t.end(),"nlist");if(nlist!=t.end()){need(t.end()-nlist>=3&&nlist[1]=="=","ordinary IVF nlist syntax");m.ivf_nlist=positive(nlist[2],65536);}
        const auto nprobe=std::find(t.begin(),t.end(),"nprobe");if(nprobe!=t.end()){need(t.end()-nprobe>=3&&nprobe[1]=="=","ordinary IVF nprobe syntax");m.ivf_nprobe=positive(nprobe[2],65536);need(m.ivf_nlist&&m.ivf_nprobe<=m.ivf_nlist,"ordinary IVF probe bound");}
        need(same_program(sql,vec0_create_table_program(d.table,int(m.dimensions),int(m.ivf_nlist),int(m.ivf_nprobe))),"ordinary vector module/options not generated");
    }else need((d.kind==ordinary_derived_kind::full_text||d.kind==ordinary_derived_kind::geographic)&&same_program(sql,d.create_sql),"ordinary derived module/options not generated");
    return m;
}
struct module_shadow {std::string name,body;std::vector<std::string> indexes;};
std::vector<module_shadow> module_shadows(const ordinary_derived_metadata& m){const auto& t=m.table;
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
    std::vector<module_shadow> out{
        {t+"_info","(key text primary key,value any)"},
        {t+"_chunks","(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,size INTEGER NOT NULL,validity BLOB NOT NULL,rowids BLOB NOT NULL)"},
        {t+"_rowids","(rowid INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,chunk_id INTEGER,chunk_offset INTEGER)"}};
    // Module-owned names derive from the pinned module implementation.
    // No separate SQLite TF_Shadow flag observation is claimed.
    if(!m.ivf_nlist)out.push_back({t+"_vector_chunks00","(rowid PRIMARY KEY,vectors BLOB NOT NULL)"});
    else{
        out.push_back({t+"_ivf_centroids00","(centroid_id INTEGER PRIMARY KEY,centroid BLOB NOT NULL)"});
        out.push_back({t+"_ivf_cells00","(centroid_id INTEGER NOT NULL,n_vectors INTEGER NOT NULL DEFAULT 0,validity BLOB NOT NULL,rowids BLOB NOT NULL,vectors BLOB NOT NULL)",{
            "CREATE INDEX "+quoted(t+"_ivf_cells00_centroid")+" ON "+quoted(t+"_ivf_cells00")+" (centroid_id)"}});
        out.push_back({t+"_ivf_rowid_map00","(rowid INTEGER PRIMARY KEY,cell_id INTEGER NOT NULL,slot INTEGER NOT NULL)"});}
    return out;
}
// Some module xConnect implementations query their own configuration tables.
// Their exact table/index SQL must be closed BEFORE a connecting PRAGMA can
// select a custom collation or expression index. No SQL is executed here.
struct expected_module_trigger {const std::string& owner;const std::string& sql;};
void preflight_module_schema(validator& v,const ordinary_derived_metadata& m,const std::map<std::string,expected_module_trigger>& programs){
    std::set<std::string> permitted{fold(m.table)};
    for(const auto& s:module_shadows(m)){
        const auto& f=v.fact(s.name,"table",s.name);const auto a=parse_table(f.sql,s.name),e=parse_table("CREATE TABLE "+quoted(s.name)+s.body,s.name);
        need(a.columns==e.columns&&a.order==e.order&&a.constraints==e.constraints&&a.without_rowid==e.without_rowid,"ordinary module shadow pre-connect declaration mismatch");permitted.insert(fold(s.name));
        auto indexes=implicit_indexes(e,s.name);for(const auto& sql:s.indexes)indexes.push_back(explicit_index(sql,s.name));
        for(const auto& index:indexes){if(e.without_rowid&&index.origin=="pk"){need(!v.all.count(fold(index.name)),"ordinary pre-connect without-rowid index object");continue;}
            const auto& f=v.fact(index.name,"index",s.name);need(index.origin=="c"?same_program(f.sql,index.sql):f.sql.empty(),"ordinary module shadow pre-connect index mismatch");permitted.insert(fold(f.name));}
    }
    const auto prefix=fold(m.table)+"_";
    for(const auto& [name,expected]:programs)if(name.starts_with(prefix)){
        const auto& actual=v.fact(name,"trigger",expected.owner);
        need(same_program(actual.sql,expected.sql),"ordinary module trigger pre-connect program mismatch");}
    for(const auto& [name,f]:v.all)if(name.starts_with(prefix)||fold(f.owner)==fold(m.table)||fold(f.owner).starts_with(prefix)){
        const auto expected=programs.find(name);
        const bool generated_trigger=f.type=="trigger"&&expected!=programs.end()&&
            fold(f.owner)==fold(expected->second.owner)&&same_program(f.sql,expected->second.sql);
        need(permitted.count(name)||generated_trigger,"ordinary unknown module family before connection");}
}
void validator::module(const ordinary_projection_derived& d){
    const auto found=all.find(fold(d.table));if(found==all.end()){need(d.kind==ordinary_derived_kind::vector,"ordinary required derived table missing");result.derived.push_back({d.kind,d.model,d.property,d.table,{},false,0,0,0});return;}
    const auto& f=fact(d.table,"table",d.table);auto m=module_declaration(d,f.sql);
    if(d.kind==ordinary_derived_kind::vector)
        module_columns(d.table,{module_column("global_id",{},1,1),module_column("embedding"),module_column(d.table,{},0,0,1),module_column("distance",{},0,0,1),module_column("k",{},0,0,1)});
    else if(d.kind==ordinary_derived_kind::full_text)
        module_columns(d.table,{module_column(d.property),module_column(d.table,{},0,0,1),module_column("rank",{},0,0,1)});
    else module_columns(d.table,{module_column("id","int"),module_column("minLat","real"),module_column("maxLat","real"),module_column("minLon","real"),module_column("maxLon","real")});
    keep(f);owned.insert(fold(d.table));for(const auto& sql:d.programs)trigger(sql,d.model);
    const auto& t=d.table;
    for(const auto& spec:module_shadows(m))shadow(spec.name,spec.body,spec.indexes);
    if(d.kind==ordinary_derived_kind::full_text){
        auto config=read.get("SELECT k,v FROM main."+quoted(t+"_config")+" LIMIT 2",{},1);
        need(config.size()==1&&text(config[0],"k")=="version"&&number(config[0],"v")==4,"ordinary FTS format/config mismatch");
    }else if(d.kind==ordinary_derived_kind::vector){
        auto info=read.get("SELECT key,value FROM main."+quoted(t+"_info")+" LIMIT "+std::to_string(m.ivf_nlist?6:5),{},m.ivf_nlist?5:4);
        need(info.size()==(m.ivf_nlist?5:4),"ordinary vector info count");std::set<std::string> keys;
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

ordinary_schema_metadata validate_ordinary_schema(const ordinary_schema_projection& p,
    const ordinary_metadata_query& query,const ordinary_metadata_limits& limits){
    need(query&&limits.objects&&limits.objects<=131072&&limits.rows&&limits.rows<=1048576&&
         limits.bytes&&limits.bytes<=64*1024*1024&&limits.sql_bytes&&limits.sql_bytes<=1024*1024&&limits.sql_bytes<=limits.bytes,
         "ordinary metadata limits refused");
    need(!p.models.empty()&&p.models.size()<=ordinary_sync_limits::models&&!p.tables.empty()&&p.tables.size()<=ordinary_sync_limits::projection_tables&&p.derived.size()<=ordinary_sync_limits::properties,
         "ordinary projection shape bound");
    // Validate the whole supplied description before the first query. Its
    // caller provenance is trusted, but malformed/colliding descriptions must
    // not acquire an accidental second ownership interpretation.
    std::set<std::string> table_names,program_names;size_t declared_bytes=0,columns=0;
    const auto charge=[&](const std::string& s){need(s.size()<=limits.sql_bytes&&s.size()<=16*1024*1024-declared_bytes,"ordinary projection admission bound");declared_bytes+=s.size();};
    const auto add_program=[&](const std::string& sql,const std::string& kind){charge(sql);need(program_names.insert(object_name(sql,kind)).second,"ordinary projection program collision");};
    for(const auto& [name,t]:p.tables){identifier(name);need(t.name==name&&table_names.insert(fold(name)).second,"ordinary projection table identity collision");charge(t.create_sql);
        need(t.columns.size()<=ordinary_sync_limits::columns_per_table&&t.columns.size()<=ordinary_sync_limits::projection_columns-columns,"ordinary projection columns bound");columns+=t.columns.size();
        std::set<std::string> names;if(t.local_id)names.insert("id");for(const auto& c:t.columns){identifier(c.name);need(names.insert(fold(c.name)).second,"ordinary projection column collision");}
        for(const auto& n:t.shadow_columns){identifier(n);need(names.insert(fold(n)).second,"ordinary projection shadow collision");}
        std::set<std::string> audit_names;
        for(const auto& sql:t.audit_programs){add_program(sql,"trigger");audit_names.insert(object_name(sql,"trigger"));}
        need(t.alternate_audit_programs.size()<=1,"ordinary audit variant bound");
        for(const auto& alternate:t.alternate_audit_programs){std::set<std::string> names;for(const auto& sql:alternate){charge(sql);const auto n=object_name(sql,"trigger");need(names.insert(n).second,"ordinary alternate audit collision");
            if(!audit_names.count(n)){need(program_names.insert(n).second,"ordinary alternate program collision");audit_names.insert(n);}}}
        for(const auto& sql:t.other_programs)add_program(sql,"trigger");for(const auto& sql:t.index_programs)add_program(sql,"index");}
    for(const auto& name:p.models){identifier(name);need(p.tables.count(name)&&p.tables.at(name).role==ordinary_projection_role::model&&name[0]!='_'&&fold(name)!="auditlog"&&fold(name)!="sqlite_sequence","ordinary projection root mismatch");}
    std::set<std::string> derived_names;
    for(const auto& d:p.derived){identifier(d.table);identifier(d.model);identifier(d.property);need(p.tables.count(d.model)&&table_names.insert(fold(d.table)).second&&derived_names.insert(fold(d.table)).second,"ordinary derived ownership collision");charge(d.create_sql);for(const auto& sql:d.programs)add_program(sql,"trigger");}
    for(const auto& n:program_names)need(!table_names.count(n),"ordinary table/program collision");
    for(const auto& family:derived_names)for(const auto& table:table_names)need(table==family||!table.starts_with(family+"_"),"ordinary derived family/table collision");
    // References remain inside this synchronous call's immutable description.
    // A familiar trigger name alone is never permission to connect a module.
    std::map<std::string,expected_module_trigger> module_programs;
    for(const auto& [name,t]:p.tables)for(const auto& sql:t.other_programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{name,sql}).second,"ordinary module trigger collision");
    for(const auto& d:p.derived)for(const auto& sql:d.programs)
        need(module_programs.emplace(object_name(sql,"trigger"),expected_module_trigger{d.model,sql}).second,"ordinary module trigger collision");
    validator v{{query,limits},{},{},{},{},{}};v.result.schema_cookie=v.read.cookie("main");v.result.temp_schema_cookie=v.read.cookie("temp");v.inventory();
    v.temp_inventory=v.read.get("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT "+std::to_string(limits.objects+1),{},limits.objects);
    // Targeted main-schema xinfo/index/FK queries do not initialize unrelated
    // views or virtual tables. In particular, never use table_list: SQLite's
    // initializer connects every main view/module before its name filter.
    // Close selected TEMP aliases before a selected module can read shadows.
    const auto selected_name=[&](const std::string& raw){const auto name=fold(raw);
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
        const auto& f=v.fact(d.table,"table",d.table);const auto described=module_declaration(d,f.sql);preflight_module_schema(v,described,module_programs);}

    for(const auto& d:p.derived)v.families.push_back(fold(d.table));
    // Every generated sidecar of a selected model lives in this namespace.
    // Unknown old/custom sidecars must not silently disappear from a snapshot.
    for(const auto& name:p.models)v.families.push_back("_"+fold(name));
    for(const auto& [name,t]:p.tables){std::set<std::string> migratable;
        if(t.role==ordinary_projection_role::model)for(const auto& c:t.columns)if(fold(c.name)!="globalid")migratable.insert(fold(c.name));
        v.ordinary_table(name,t.create_sql,migratable,t.shadow_columns,t.index_programs,true);
        v.audits(t);for(const auto& sql:t.other_programs)v.trigger(sql,name);}
    for(const auto& d:p.derived)v.module(d);
    v.close_ownership();need(v.read.cookie("main")==v.result.schema_cookie&&v.read.cookie("temp")==v.result.temp_schema_cookie,"ordinary retained schema view changed");
    v.result.inspected_rows=v.read.rows;v.result.copied_bytes=v.read.bytes;return std::move(v.result);
}
} // namespace lattice::detail
