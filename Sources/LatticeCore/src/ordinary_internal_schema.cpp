#include "ordinary_internal_schema.hpp"
#include <lattice/ordinary_internal_program.hpp>
#include <algorithm>
#include <set>

namespace lattice::detail {
namespace {
using row=database::row_t;
using tokens=std::vector<std::string>;
void require(bool ok,const char* reason){if(!ok)throw ordinary_schema_validation_error(reason);}
std::string fold(std::string s){for(auto& c:s)if(c>='A'&&c<='Z')c+=32;return s;}
bool word(char c){return(c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_';}
void identifier(const std::string& s){require(!s.empty()&&s.size()<=256&&!(s[0]>='0'&&s[0]<='9'),"ordinary internal identifier bound");for(auto c:s)require(word(c),"ordinary internal identifier syntax");}
const column_value_t& cell(const row& r,const char* key){auto i=r.find(key);require(i!=r.end(),"ordinary internal missing metadata field");return i->second;}
int64_t number(const row& r,const char* key){const auto* v=std::get_if<int64_t>(&cell(r,key));require(v,"ordinary internal noninteger metadata");return *v;}
const std::string& text(const row& r,const char* key){const auto* v=std::get_if<std::string>(&cell(r,key));require(v&&v->find('\0')==std::string::npos,"ordinary internal nontext metadata");return *v;}
bool null(const row& r,const char* key){return std::holds_alternative<std::nullptr_t>(cell(r,key));}
size_t bytes(const column_value_t& v){if(auto p=std::get_if<std::string>(&v))return p->size();if(auto p=std::get_if<std::vector<uint8_t>>(&v))return p->size();return sizeof(v);}
// Closed comparison grammar, not a SQL parser for arbitrary application code.
// Preserve string literal bytes/case; ignore only whitespace, identifier quotes,
// ASCII keyword/name case, a final semicolon and CREATE IF NOT EXISTS.
tokens lex(const std::string& s){tokens out;for(size_t i=0;i<s.size();){const auto c=s[i];
    if(c==' '||c=='\t'||c=='\r'||c=='\n'){++i;continue;}
    if(c=='\''||c=='"'||c=='`'){++i;std::string value;bool closed=false;while(i<s.size()){
        const auto next=s[i++];if(next==c){if(i<s.size()&&s[i]==c){value+=c;++i;}else{closed=true;break;}}else value+=next;}
        require(closed,"ordinary internal unterminated SQL quote");if(c=='\'')out.push_back("'"+value+"'");else{identifier(value);out.push_back(fold(value));}continue;}
    if(word(c)){const auto start=i++;while(i<s.size()&&(word(s[i])||(s[i]=='.'&&s[start]>='0'&&s[start]<='9')))++i;out.push_back(fold(s.substr(start,i-start)));continue;}
    require(std::string("(),;.=+-*%|").find(c)!=std::string::npos&&!(c=='-'&&i+1<s.size()&&s[i+1]=='-'),"ordinary internal SQL token refused");
    if(c=='|'&&i+1<s.size()&&s[i+1]=='|'){out.push_back("||");i+=2;}else{out.emplace_back(1,c);++i;}}
    if(!out.empty()&&out.back()==";")out.pop_back();return out;
}
tokens program(const std::string& sql){auto t=lex(sql);if(t.size()>4&&t[0]=="create"){
    const size_t at=t[1]=="unique"?3:2;if(at+2<t.size()&&t[at]=="if"&&t[at+1]=="not"&&t[at+2]=="exists")t.erase(t.begin()+at,t.begin()+at+3);}return t;}
tokens expression(const std::string& s){auto t=lex(s);for(;;){if(t.size()<2||t.front()!="("||t.back()!=")")return t;size_t depth=0;bool whole=true;
    for(size_t i=0;i<t.size();++i){if(t[i]=="(")++depth;else if(t[i]==")"){require(depth,"ordinary internal default parentheses");if(!--depth&&i+1<t.size()){whole=false;break;}}}if(!whole)return t;t.erase(t.begin());t.pop_back();}}
// Classify only the stored declaration prefix; never instantiate an unrelated
// virtual table or parse/execute its arbitrary module arguments.
bool virtual_table(const std::string& sql){size_t at=0;const auto next=[&]{while(at<sql.size()&&(sql[at]==' '||sql[at]=='\t'||sql[at]=='\r'||sql[at]=='\n'))++at;
    const auto start=at;while(at<sql.size()&&word(sql[at]))++at;require(at>start,"ordinary internal unqualified table prefix");return fold(sql.substr(start,at-start));};
    require(next()=="create","ordinary internal unqualified table declaration");const auto kind=next();require(kind=="table"||kind=="virtual","ordinary internal unqualified table kind");return kind=="virtual";}
struct reader {
    const ordinary_metadata_query& query;const ordinary_metadata_limits& limits;size_t rows=0,copied=0;
    std::vector<row> get(const std::string& sql,const std::vector<column_value_t>& args,size_t cap){
        require(cap&&cap<=limits.rows&&sql.size()<=limits.sql_bytes&&rows<limits.rows,"ordinary internal query bound");cap=std::min(cap,limits.rows-rows);
        auto result=query(sql,args,cap);require(result.size()<=cap,"ordinary internal adapter row overrun");
        for(const auto& r:result){require(r.size()<=16,"ordinary internal metadata field bound");for(const auto& [k,v]:r){const auto n=bytes(v);
            require(n<=limits.bytes-copied,"ordinary internal metadata byte bound");copied+=n;require(k.size()<=256&&k.size()<=limits.bytes-copied,"ordinary internal metadata key bound");copied+=k.size();}}
        rows+=result.size();return result;
    }
    int64_t cookie(const char* schema){auto r=get(std::string("PRAGMA ")+schema+".schema_version",{},1);require(r.size()==1,"ordinary internal schema cookie missing");return number(r[0],"schema_version");}
};
struct index_spec {std::string name,origin,sql;std::vector<std::string> columns;bool unique=false,partial=false;};
struct column_spec {std::string name,type;int64_t nn=0,pk=0;std::optional<std::string> value;std::string collation="binary";};
struct table_spec {std::string name,sql;std::vector<column_spec> columns;std::vector<index_spec> indexes;};
std::vector<table_spec> specifications(){
    // Default text is taken from the shared exact CREATE template, not a
    // second approximation of the UUID expression.
    const auto ddl=ordinary_audit_table_program();const std::string marker="DEFAULT ",closing="\n                )";
    const auto first=ddl.find(marker+"("),last=ddl.find(closing+",",first);
    require(first!=std::string::npos&&last!=std::string::npos,"ordinary internal generated UUID default missing");
    const auto uuid=ddl.substr(first+marker.size(),last+closing.size()-(first+marker.size()));
    table_spec audit{"AuditLog",ddl,{{"id","integer",0,1},{"globalId","text",0,0,uuid,"nocase"},
        {"tableName","text"},{"operation","text"},{"rowId","integer"},{"globalRowId","text"},{"changedFields","text"},{"changedFieldsNames","text"},
        {"isFromRemote","integer",0,0,"0"},{"isSynchronized","integer",0,0,"0"},{"timestamp","real",0,0,"unixepoch('subsec')"},{"synthesized","integer",0,0,"0"}},
        {{"sqlite_autoindex_AuditLog_1","u",{}, {"globalId"},true,false},
         {"idx_audit_log_pending_sync","c",ordinary_audit_pending_index_program(),{"isSynchronized"},false,true},
         {"idx_audit_log_change_lookup","c",ordinary_audit_lookup_index_program(),{"tableName","rowId","operation"},false,false}}};
    table_spec control{"_SyncControl",ordinary_sync_control_table_program(),{{"id","integer",0,1},{"disabled","integer",1,0,"0"}}, {}};
    table_spec state{"_lattice_sync_state",ordinary_sync_state_table_program(),{{"audit_entry_id","integer",1,1},{"sync_id","text",1,2},{"is_synchronized","integer",1,0,"0"}},
        {{"sqlite_autoindex__lattice_sync_state_1","pk",{}, {"audit_entry_id","sync_id"},true,false},
         {"idx_sync_state_pending","c",ordinary_sync_state_pending_index_program(),{"sync_id","is_synchronized"},false,true}}};
    table_spec meta{"_lattice_meta",ordinary_meta_table_program(),{{"key","text",0,1},{"value","text",1}},
        {{"sqlite_autoindex__lattice_meta_1","pk",{}, {"key"},true,false}}};
    return {std::move(audit),std::move(control),std::move(state),std::move(meta)};
}
struct validator {
    reader read;ordinary_internal_schema_metadata result;std::map<std::string,ordinary_object_metadata> all;std::set<std::string> expected,owned;
    const ordinary_object_metadata& object(const std::string& name,const char* kind,const std::string& owner){auto i=all.find(fold(name));require(i!=all.end()&&i->second.type==kind&&i->second.name==name&&i->second.owner==owner,"ordinary internal object identity differs");
        require(expected.insert(i->first).second,"ordinary internal overlapping object");result.objects.push_back(i->second);return i->second;}
    void inventory(){auto rows=read.get("SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema ORDER BY name COLLATE BINARY LIMIT "+std::to_string(read.limits.objects+1),{},read.limits.objects);
        for(const auto& r:rows){ordinary_object_metadata f{text(r,"type"),text(r,"name"),text(r,"owner"),null(r,"sql")?std::string{}:text(r,"sql")};
            require(f.name.size()<=256&&f.owner.size()<=256&&f.sql.size()<=read.limits.sql_bytes,"ordinary internal schema object bound");require(all.emplace(fold(f.name),std::move(f)).second,"ordinary internal schema name collision");}}
    void table(const table_spec& spec){owned.insert(fold(spec.name));const auto& f=object(spec.name,"table",spec.name);require(program(f.sql)==program(spec.sql),"ordinary internal table program/default mismatch");
        auto cols=read.get("SELECT cid,name,type,\"notnull\" AS nn,dflt_value AS def,pk,hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid LIMIT "+std::to_string(spec.columns.size()+1),{spec.name},spec.columns.size());
        require(cols.size()==spec.columns.size(),"ordinary internal column count mismatch");ordinary_table_metadata observed{spec.name,{}};
        for(size_t i=0;i<cols.size();++i){const auto& r=cols[i];const auto& c=spec.columns[i];require(number(r,"cid")==int64_t(i)&&text(r,"name")==c.name&&fold(text(r,"type"))==c.type&&number(r,"nn")==c.nn&&number(r,"pk")==c.pk&&number(r,"hidden")==0,"ordinary internal column flags mismatch");
            require(null(r,"def")==!c.value,"ordinary internal default presence mismatch");ordinary_column_metadata value{c.name,c.type,int64_t(i),c.nn,c.pk,0,{}};
            if(c.value){require(expression(text(r,"def"))==expression(*c.value),"ordinary internal default expression mismatch");value.default_sql=text(r,"def");}observed.columns.push_back(std::move(value));}
        result.tables.emplace(spec.name,std::move(observed));
        require(read.get("SELECT id FROM pragma_foreign_key_list(?, 'main') LIMIT 1",{spec.name},1).empty(),"ordinary internal foreign key refused");
        auto indexes=read.get("SELECT name,\"unique\" AS uniq,origin,partial FROM pragma_index_list(?, 'main') LIMIT "+std::to_string(spec.indexes.size()+1),{spec.name},std::max<size_t>(1,spec.indexes.size()));
        require(indexes.size()==spec.indexes.size(),"ordinary internal index count mismatch");std::set<std::string> seen;
        for(const auto& r:indexes){const auto& n=text(r,"name");auto it=std::find_if(spec.indexes.begin(),spec.indexes.end(),[&](const auto& x){return x.name==n;});require(it!=spec.indexes.end()&&seen.insert(n).second,"ordinary internal unknown index");const auto& s=*it;
            require(number(r,"uniq")==int64_t(s.unique)&&number(r,"partial")==int64_t(s.partial)&&text(r,"origin")==s.origin,"ordinary internal index flags mismatch");
            const auto& found=object(n,"index",spec.name);require(s.sql.empty()?found.sql.empty():program(found.sql)==program(s.sql),"ordinary internal index program/predicate mismatch");
            const auto count=s.columns.size()+1;auto details=read.get("SELECT seqno,cid,name,\"desc\" AS descending,coll,\"key\" AS iskey FROM pragma_index_xinfo(?, 'main') ORDER BY seqno LIMIT "+std::to_string(count+1),{n},count);
            require(details.size()==count,"ordinary internal index key count mismatch");
            for(size_t i=0;i<count;++i){const auto& d=details[i];const bool key=i<s.columns.size();int64_t cid=-1;std::string coll="binary";
                if(key){auto c=std::find_if(spec.columns.begin(),spec.columns.end(),[&](const auto& x){return x.name==s.columns[i];});require(c!=spec.columns.end(),"ordinary internal generated index column missing");cid=c-spec.columns.begin();coll=c->collation;}
                require(number(d,"seqno")==int64_t(i)&&number(d,"cid")==cid&&number(d,"descending")==0&&number(d,"iskey")==int64_t(key)&&fold(text(d,"coll"))==coll&&(key?text(d,"name")==s.columns[i]:null(d,"name")),"ordinary internal index keys/collation mismatch");}
        }
    }
    void close(){for(const auto& [name,f]:all)if(owned.count(name)||owned.count(fold(f.owner)))require(expected.count(name),"ordinary internal extra owned trigger/program");
        auto temp=read.get("SELECT type,name,tbl_name AS owner,sql FROM temp.sqlite_schema LIMIT "+std::to_string(read.limits.objects+1),{},read.limits.objects);
        for(const auto& r:temp)require(!owned.count(fold(text(r,"name")))&&!owned.count(fold(text(r,"owner"))),"ordinary internal TEMP shadow/trigger refused");
        // B's selected-module/shadow preflight is a required call-order
        // condition. No metadata invocation for unrelated main/TEMP modules.
        for(const auto& [name,f]:all)if(f.type=="table"&&!owned.count(name)){
            if(virtual_table(f.sql))continue;
            auto rows=read.get("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'main') LIMIT "+std::to_string(read.limits.objects+1),{f.name},read.limits.objects);
            for(const auto& r:rows)require(!owned.count(fold(text(r,"target"))),"ordinary internal inbound foreign key refused");}
        for(const auto& t:temp)if(text(t,"type")=="table"){
            require(!null(t,"sql"),"ordinary internal TEMP table declaration missing");if(virtual_table(text(t,"sql")))continue;
            auto rows=read.get("SELECT \"table\" AS target FROM pragma_foreign_key_list(?, 'temp') LIMIT "+std::to_string(read.limits.objects+1),{text(t,"name")},read.limits.objects);
            for(const auto& r:rows)require(!owned.count(fold(text(r,"target"))),"ordinary internal TEMP inbound foreign key refused");}
    }
};
std::set<std::string> parents(const std::string& value){std::set<std::string> out;if(value.empty())return out;size_t begin=0;
    for(;;){const auto end=value.find(';',begin);const auto length=(end==std::string::npos?value.size():end)-begin;
        require(length&&length<=513&&out.size()<ordinary_sync_limits::projection_tables,"ordinary internal parent membership bound");
        const auto part=value.substr(begin,length);const auto colon=part.find(':');identifier(part.substr(0,colon));if(colon!=std::string::npos)identifier(part.substr(colon+1));
        require(out.insert(part).second,"ordinary internal duplicate parent membership");if(end==std::string::npos)break;begin=end+1;}return out;}
void memberships(validator& v,const recovery_owner_schema& catalog,const ordinary_schema_projection& projection){
    std::map<std::string,std::set<std::string>> expected;std::set<std::string> selected,selected_folded;for(const auto& n:projection.models){selected.insert(n);selected_folded.insert(fold(n));}
    for(const auto& [name,model]:catalog.models)for(const auto& p:model.properties){std::string table,parent=name;
        if(p.kind==property_kind::union_type||p.is_union){table=p.union_desc.union_table_name;parent+=':'+p.name;}
        else if(p.kind==property_kind::virtual_link||p.kind==property_kind::virtual_list){table="_"+name+"_"+p.name;if(catalog.swift_models.count(name))parent+=':'+p.name;}
        else if((p.kind==property_kind::link||p.kind==property_kind::list)&&!p.is_geo_bounds&&!p.target_table.empty()){table="_"+name+"_"+p.target_table+"_"+p.name;if(catalog.swift_models.count(name))parent+=':'+p.name;}
        else if(p.kind==property_kind::list&&p.is_geo_bounds)table="_"+name+"_"+p.name;
        if(table.empty()||!projection.tables.count(table))continue;require(selected.count(name),"ordinary internal unselected declared shared parent");identifier(table);(void)parents(parent);expected[table].insert(parent);
    }
    for(const auto& [name,t]:projection.tables)if(t.role!=ordinary_projection_role::model)require(expected.count(name),"ordinary internal sidecar declaration lacks parent");
    auto rows=v.read.get("SELECT key,value FROM main._lattice_meta WHERE substr(key,1,15)='internal_table:' ORDER BY key COLLATE BINARY LIMIT "+std::to_string(v.read.limits.objects+1),{},v.read.limits.objects);
    std::set<std::string> seen;
    for(const auto& r:rows){const auto& key=text(r,"key");require(key.starts_with("internal_table:")&&key.size()<=271,"ordinary internal metadata key prefix/bound");const auto name=key.substr(15);identifier(name);const auto& raw=text(r,"value");const auto actual=parents(raw);
        auto e=expected.find(name);bool relevant=e!=expected.end();for(const auto& parent:actual)if(selected_folded.count(fold(parent.substr(0,parent.find(':')))))relevant=true;
        for(const auto& [wanted,unused]:expected)if(fold(wanted)==fold(name))relevant=true;
        if(!relevant)continue;require(e!=expected.end()&&e->second==actual&&seen.insert(name).second,"ordinary internal parent membership differs from declaration");v.result.internal_parents.emplace(name,raw);}
    require(seen.size()==expected.size(),"ordinary internal declared membership missing");
}
} // namespace
ordinary_internal_schema_metadata validate_ordinary_internal_schema(const recovery_owner_schema& catalog,const ordinary_schema_projection& projection,
    const ordinary_schema_metadata& inspected,const ordinary_metadata_query& query,const ordinary_metadata_limits& limits){
    require(query&&catalog.valid()&&!projection.models.empty()&&limits.objects&&limits.objects<=65536&&limits.rows&&limits.rows<=262144&&limits.bytes&&limits.bytes<=32*1024*1024&&limits.sql_bytes&&limits.sql_bytes<=256*1024,"ordinary internal validation bounds/input missing");
    validator v{{query,limits},{},{},{},{}};v.result.schema_cookie=v.read.cookie("main");v.result.temp_schema_cookie=v.read.cookie("temp");
    require(v.result.schema_cookie==inspected.schema_cookie&&v.result.temp_schema_cookie==inspected.temp_schema_cookie,"ordinary internal view differs from preflight");
    auto databases=v.read.get("PRAGMA database_list",{},3);require(databases.size()<=2,"ordinary internal attached schema refused");for(const auto& r:databases)require(text(r,"name")=="main"||text(r,"name")=="temp","ordinary internal attached schema refused");
    v.inventory();for(const auto& s:specifications())v.table(s);v.close();memberships(v,catalog,projection);
    require(v.read.cookie("main")==v.result.schema_cookie&&v.read.cookie("temp")==v.result.temp_schema_cookie,"ordinary internal retained view changed");
    v.result.inspected_rows=v.read.rows;v.result.copied_bytes=v.read.copied;return std::move(v.result);
}
} // namespace lattice::detail
