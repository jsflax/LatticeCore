#include "scoped_recovery_install.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cmath>
#include <climits>
#include <limits>
#include <map>
#include <set>
#include <string_view>

namespace lattice::detail {
namespace {
using values = sync_recovery::row_values;
using key = recovery_row_key;
using blob = std::vector<uint8_t>;
using json = nlohmann::json;
[[noreturn]] void refuse(const char* message) { throw std::runtime_error(message); }
void require(bool condition,const char* message) { if (!condition) refuse(message); }
std::string quote(const std::string& s) {
    require(!s.empty() && s.find('\0')==std::string::npos,"recovery invalid schema identifier");
    std::string result="\"";
    for (char c:s) { result+=c; if(c=='"') result+=c; }
    return result+'"';
}
std::string fold(std::string s) {
    for (auto& c:s) if(c>='A'&&c<='Z') c+=32;
    return s;
}
key folded(key k) { k.global_id=fold(std::move(k.global_id)); return k; }
uint64_t scalar_bytes(const recovery_scalar& v) {
    if (auto* p=std::get_if<std::string>(&v)) return p->size();
    if (auto* p=std::get_if<blob>(&v)) return p->size();
    return std::holds_alternative<std::nullptr_t>(v)?1:8;
}
struct work_budget {
    const scoped_recovery_limits& limits;
    uint64_t fields=0,bytes=0;
    void charge(uint64_t n) {
        require(fields<limits.fields && n<=limits.field_bytes && bytes<=limits.logical_bytes &&
            n<=limits.logical_bytes-bytes,"recovery work scalar budget exceeded before copy");
        ++fields; bytes+=n;
    }
    void identity(const std::string& s) { require(!s.empty(),"recovery empty identity"); charge(s.size()); }
    void value(const recovery_scalar& v) {
        if(auto* d=std::get_if<double>(&v)) require(std::isfinite(*d),"recovery nonfinite value");
        charge(scalar_bytes(v));
    }
};
struct stmt {
    sqlite3_stmt* p=nullptr;
    explicit stmt(sqlite3* db,const std::string& sql) {
        database::record_statement();
        const int rc=sqlite3_prepare_v2(db,sql.c_str(),-1,&p,nullptr);
        if(rc!=SQLITE_OK || !p) { sqlite3_finalize(p); p=nullptr; refuse("recovery SQLite prepare failed"); }
    }
    stmt(const stmt&)=delete;
    ~stmt() { sqlite3_finalize(p); }
    void value(int at,const recovery_scalar& v) {
        int rc=SQLITE_MISUSE;
        if(std::holds_alternative<std::nullptr_t>(v)) rc=sqlite3_bind_null(p,at);
        else if(auto* n=std::get_if<int64_t>(&v)) rc=sqlite3_bind_int64(p,at,*n);
        else if(auto* n=std::get_if<double>(&v)) rc=sqlite3_bind_double(p,at,*n);
        else if(auto* s=std::get_if<std::string>(&v)) {
            require(s->size()<=static_cast<size_t>(INT_MAX),"recovery TEXT bind too large");
            rc=sqlite3_bind_text(p,at,s->data(),static_cast<int>(s->size()),SQLITE_TRANSIENT);
        } else if(auto* b=std::get_if<blob>(&v)) {
            require(b->size()<=static_cast<size_t>(INT_MAX),"recovery BLOB bind too large");
            rc=sqlite3_bind_blob(p,at,b->empty()?static_cast<const void*>(""):b->data(),static_cast<int>(b->size()),SQLITE_TRANSIENT);
        }
        require(rc==SQLITE_OK,"recovery SQLite bind failed");
    }
    void text(int at,const std::string& s) { value(at,s); }
    void integer(int at,int64_t n) { value(at,n); }
    bool next() {
        const int rc=sqlite3_step(p);
        if(rc==SQLITE_ROW) return true;
        require(rc==SQLITE_DONE,"recovery SQLite step failed"); return false;
    }
    void done() { require(!next(),"recovery write unexpectedly returned a row"); }
    int64_t number(int at) {
        require(sqlite3_column_type(p,at)==SQLITE_INTEGER,"recovery corrupt metadata INTEGER");
        return sqlite3_column_int64(p,at);
    }
    std::string string(int at,uint64_t limit) {
        require(sqlite3_column_type(p,at)==SQLITE_TEXT,"recovery corrupt metadata TEXT");
        const int size=sqlite3_column_bytes(p,at);
        require(size>=0 && static_cast<uint64_t>(size)<=limit,"recovery metadata string too large before copy");
        const auto* bytes=sqlite3_column_text(p,at);
        require(bytes!=nullptr,"recovery metadata TEXT unavailable");
        return {reinterpret_cast<const char*>(bytes),static_cast<size_t>(size)};
    }
};
void execute(sqlite3* db,const std::string& sql) { stmt s(db,sql);s.done(); }
void changed(sqlite3* db) { require(sqlite3_changes(db)==1,"recovery metadata/effect write was ignored"); }
int64_t checked_add(int64_t a,uint64_t b,int64_t maximum) {
    require(a>=0 && maximum>=a && b<=static_cast<uint64_t>(maximum-a),"recovery durable capacity exceeded");
    return a+static_cast<int64_t>(b);
}
struct member { std::string channel; key target; };
struct metadata {
    sqlite3* db; const scoped_recovery_limits& limits;
    int64_t channels=0,members=0,bytes=0;
    std::map<std::string,std::string> declarations;
    std::map<key,member> ownership; // NOCASE identity matches Core UUID keys
    explicit metadata(sqlite3* handle,const scoped_recovery_limits& l):db(handle),limits(l) {
        stmt count(db,"SELECT COUNT(*) FROM main.sqlite_schema WHERE type='table' AND name IN "
            "('_lattice_recovery_scope_config','_lattice_recovery_scope','_lattice_recovery_member')");
        require(count.next(),"recovery metadata table count missing"); const auto n=count.number(0);
        require(!count.next() && (n==0 || n==3),"recovery partial membership schema");
        if(n==0) {
            execute(db,"CREATE TABLE main._lattice_recovery_scope_config(id INTEGER PRIMARY KEY CHECK(id=1),"
                "version INTEGER NOT NULL,channel_limit INTEGER NOT NULL,member_limit INTEGER NOT NULL,byte_limit INTEGER NOT NULL,"
                "channels INTEGER NOT NULL,members INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID");
            execute(db,"CREATE TABLE main._lattice_recovery_scope(channel TEXT PRIMARY KEY,declaration TEXT NOT NULL) WITHOUT ROWID");
            execute(db,"CREATE TABLE main._lattice_recovery_member(table_name TEXT NOT NULL,global_id TEXT COLLATE NOCASE NOT NULL,"
                "channel TEXT NOT NULL,PRIMARY KEY(table_name,global_id)) WITHOUT ROWID");
            stmt insert(db,"INSERT INTO main._lattice_recovery_scope_config VALUES(1,1,?,?,?,0,0,0)");
            insert.integer(1,l.channels);insert.integer(2,l.members);insert.integer(3,l.metadata_bytes);insert.done();changed(db);
        }
        audit();
    }
    void audit() {
        stmt config(db,"SELECT id,version,channel_limit,member_limit,byte_limit,channels,members,bytes "
            "FROM main._lattice_recovery_scope_config LIMIT 2");
        require(config.next() && config.number(0)==1 && config.number(1)==1 &&
            config.number(2)==limits.channels && config.number(3)==limits.members && config.number(4)==limits.metadata_bytes,
            "recovery membership version/limits mismatch");
        const auto expected_channels=config.number(5),expected_members=config.number(6),expected_bytes=config.number(7);
        require(!config.next(),"recovery duplicate membership configuration");
        declarations.clear();ownership.clear();channels=members=bytes=0;
        stmt scopes(db,"SELECT channel,declaration FROM main._lattice_recovery_scope ORDER BY channel");
        while(scopes.next()) {
            channels=checked_add(channels,1,limits.channels);
            auto channel=scopes.string(0,std::min<uint64_t>(limits.field_bytes,static_cast<uint64_t>(limits.metadata_bytes-bytes)));
            bytes=checked_add(bytes,channel.size(),limits.metadata_bytes);
            auto declaration=scopes.string(1,static_cast<uint64_t>(limits.metadata_bytes-bytes));
            require(!channel.empty() && !declaration.empty(),"recovery empty scope metadata");
            bytes=checked_add(bytes,declaration.size(),limits.metadata_bytes);
            require(declarations.emplace(std::move(channel),std::move(declaration)).second,"recovery duplicate scope");
        }
        stmt rows(db,"SELECT table_name,global_id,channel FROM main._lattice_recovery_member ORDER BY table_name,global_id");
        while(rows.next()) {
            members=checked_add(members,1,limits.members);
            auto bounded_string=[&](int at) {
                auto value=rows.string(at,std::min<uint64_t>(limits.field_bytes,static_cast<uint64_t>(limits.metadata_bytes-bytes)));
                bytes=checked_add(bytes,value.size(),limits.metadata_bytes);return value;
            };
            member m; m.channel=bounded_string(2);m.target.table=bounded_string(0);m.target.global_id=bounded_string(1);
            require(!m.target.table.empty()&&!m.target.global_id.empty()&&declarations.count(m.channel),"recovery orphan/empty membership");
            const auto normalized=folded(m.target);
            require(ownership.emplace(normalized,std::move(m)).second,"recovery overlapping membership");
        }
        require(channels==expected_channels&&members==expected_members&&bytes==expected_bytes,"recovery membership counter drift");
    }
    void replace(const std::string& channel,const std::string& declaration,const std::set<key>& desired) {
        const auto old_channels=channels,old_members=members,old_bytes=bytes;
        if(auto it=declarations.find(channel);it!=declarations.end()) {
            require(it->second==declaration,"recovery scope declaration replacement unsupported");
        } else {
            channels=checked_add(channels,1,limits.channels);
            bytes=checked_add(bytes,channel.size(),limits.metadata_bytes);
            bytes=checked_add(bytes,declaration.size(),limits.metadata_bytes);
            stmt add(db,"INSERT INTO main._lattice_recovery_scope VALUES(?,?)");add.text(1,channel);add.text(2,declaration);add.done();changed(db);
        }
        for(auto it=ownership.begin();it!=ownership.end();) {
            if(it->second.channel!=channel) {++it;continue;}
            const auto& m=it->second;
            require(bytes>=static_cast<int64_t>(m.channel.size()+m.target.table.size()+m.target.global_id.size())&&members>0,"recovery counter underflow");
            bytes-=static_cast<int64_t>(m.channel.size()+m.target.table.size()+m.target.global_id.size());--members;
            stmt del(db,"DELETE FROM main._lattice_recovery_member WHERE table_name=? AND global_id=? AND channel=?");
            del.text(1,m.target.table);del.text(2,m.target.global_id);del.text(3,channel);del.done();changed(db);
            it=ownership.erase(it);
        }
        for(const auto& k:desired) {
            require(!ownership.count(folded(k)),"recovery target overlaps another channel");
            members=checked_add(members,1,limits.members);
            bytes=checked_add(bytes,channel.size(),limits.metadata_bytes);
            bytes=checked_add(bytes,k.table.size(),limits.metadata_bytes);
            bytes=checked_add(bytes,k.global_id.size(),limits.metadata_bytes);
            stmt add(db,"INSERT INTO main._lattice_recovery_member VALUES(?,?,?)");
            add.text(1,k.table);add.text(2,k.global_id);add.text(3,channel);add.done();changed(db);
        }
        stmt update(db,"UPDATE main._lattice_recovery_scope_config SET channels=?,members=?,bytes=? "
            "WHERE id=1 AND channels=? AND members=? AND bytes=?");
        update.integer(1,channels);update.integer(2,members);update.integer(3,bytes);
        update.integer(4,old_channels);update.integer(5,old_members);update.integer(6,old_bytes);update.done();changed(db);
        audit(); // once per install, never per pending identity
        require(declarations.at(channel)==declaration,"recovery declaration write changed by trigger");
        std::set<key> actual;
        for(const auto& [_,m]:ownership) if(m.channel==channel) actual.insert(m.target);
        require(actual==desired,"recovery membership write changed by trigger");
    }
};
const recovery_outbox_column& column(const recovery_outbox_table& table,const std::string& name) {
    auto it=std::find_if(table.columns.begin(),table.columns.end(),[&](const auto& c){return c.name==name;});
    require(it!=table.columns.end(),"recovery unknown model column");return *it;
}
void valid_value(const recovery_outbox_column& c,const recovery_scalar& v) {
    if(std::holds_alternative<std::nullptr_t>(v)) {
        require(!c.not_null && c.name!="globalId" && c.name!="id","recovery required value is NULL");return;
    }
    const auto type=fold(c.declared_type);
    require((type=="integer"&&std::holds_alternative<int64_t>(v)) ||
        (type=="real"&&(std::holds_alternative<int64_t>(v)||std::holds_alternative<double>(v))) ||
        (type=="text"&&std::holds_alternative<std::string>(v)) ||
        (type=="blob"&&std::holds_alternative<blob>(v)),"recovery value violates actual column type");
    if(auto* d=std::get_if<double>(&v)) require(std::isfinite(*d),"recovery nonfinite row value");
}
void validate_row(const recovery_outbox_table& table,const key& k,const values& row) {
    const size_t expected=table.columns.size()-(table.kind==recovery_outbox_table_kind::model?1:0);
    require(row.size()==expected && !row.count("id"),"recovery incomplete final row or remote local PK");
    for(const auto& c:table.columns) {
        if(c.name=="id") continue;
        auto it=row.find(c.name);require(it!=row.end(),"recovery missing final column");valid_value(c,it->second);
    }
    require(std::get<std::string>(row.at("globalId"))==k.global_id,"recovery global identity disagreement");
}
values captured_values(const recovery_outbox_capture& capture,const recovery_outbox_current_row& row) {
    values result;
    const auto& table=capture.tables.at(row.table_index);
    require(row.values.size()==table.columns.size(),"recovery incomplete owned current row");
    for(size_t i=0;i<table.columns.size();++i) if(table.columns[i].name!="id")
        result.emplace(table.columns[i].name,row.values[i]);
    require(std::get<std::string>(result.at("globalId"))==row.lookup_global_id,
        "recovery case-alias current identity is unsupported");
    return result;
}
blob decode_hex(const std::string& text) {
    require(text.size()%2==0,"recovery malformed original BLOB hex");
    auto digit=[](char c)->int {if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;if(c>='A'&&c<='F')return c-'A'+10;return -1;};
    blob result;result.reserve(text.size()/2);
    for(size_t i=0;i<text.size();i+=2) {
        const int a=digit(text[i]),b=digit(text[i+1]);require(a>=0&&b>=0,"recovery malformed original BLOB hex");
        result.push_back(static_cast<uint8_t>(16*a+b));
    }
    return result;
}
json strict_json(const std::string& input) {
    std::map<int,std::set<std::string>> keys;
    return json::parse(input,[&](int depth,json::parse_event_t event,json& value) {
        require(depth<=3,"recovery original payload nesting unsupported");
        if(event==json::parse_event_t::object_start) keys[depth+1].clear();
        if(event==json::parse_event_t::key)
            require(keys[depth].insert(value.get<std::string>()).second,"recovery duplicate original payload key");
        return true;
    });
}
recovery_scalar original_scalar(const json& input,const recovery_outbox_column& c) {
    const json* v=&input;std::optional<int64_t> kind;
    if(input.is_object()) {
        require(input.size()==2&&input.contains("kind")&&input.contains("value")&&input.at("kind").is_number_integer(),
            "recovery malformed original property wrapper");
        kind=input.at("kind").get<int64_t>();require(*kind>=0&&*kind<=7,"recovery unknown original property kind");v=&input.at("value");
    }
    recovery_scalar result;
    if(v->is_null()) result=nullptr;
    else if(v->is_number_unsigned()) {
        const auto n=v->get<uint64_t>();require(n<=static_cast<uint64_t>(INT64_MAX),"recovery original integer overflow");result=static_cast<int64_t>(n);
    } else if(v->is_number_integer()) result=v->get<int64_t>();
    else if(v->is_number_float()) result=v->get<double>();
    else if(v->is_string()) {
        auto s=v->get<std::string>();
        if((kind&&*kind==6)||(!kind&&fold(c.declared_type)=="blob")) result=decode_hex(s);
        else result=std::move(s);
    } else refuse("recovery unsupported original scalar");
    if(kind) {
        const bool match=((*kind==0||*kind==1)&&std::holds_alternative<int64_t>(result)) ||
            (*kind==2&&std::holds_alternative<std::string>(result)) ||
            ((*kind==3||*kind==5||*kind==7)&&(std::holds_alternative<double>(result)||std::holds_alternative<int64_t>(result))) ||
            (*kind==4&&std::holds_alternative<std::nullptr_t>(result)) ||
            (*kind==6&&std::holds_alternative<blob>(result));
        require(match,"recovery original property kind/value mismatch");
    }
    if(fold(c.declared_type)=="real"&&std::holds_alternative<int64_t>(result))
        result=static_cast<double>(std::get<int64_t>(result));
    valid_value(c,result);return result;
}
std::set<std::string> no_history(const recovery_outbox_table& t) {
    std::set<std::string> result;
    if(!t.trigger_flags) return result;
    for(size_t at=0;at<t.trigger_flags->size();) {
        auto end=t.trigger_flags->find(',',at);
        result.insert(t.trigger_flags->substr(at,end==std::string::npos?end:end-at));
        if(end==std::string::npos)break;at=end+1;
    }
    return result;
}
struct planned_row {
    std::optional<values> before,after;
    std::optional<int64_t> local_id;
    bool unresolved=false;
};
void replay(const recovery_outbox_audit& audit,const recovery_outbox_table& table,
            planned_row& row,work_budget& budget) {
    const auto names=strict_json(audit.changed_names_json);
    require(names.is_array(),"recovery changed names are not an array");
    if(names.size()==1&&names[0].is_string()&&names[0].get<std::string>()=="__lattice_filter_removal")
        return; // strict producer shape already validated by outbox capture
    if(audit.operation=="DELETE") {row.after.reset();return;}
    if(audit.operation=="UPDATE"&&!row.after) return; // existing UPDATE does not resurrect
    if(audit.operation=="INSERT"&&audit.synthesized&&row.after) return;
    auto fields=strict_json(audit.changed_fields_json);
    require(fields.is_object(),"recovery original fields are not an object");
    if(!row.after) row.after=values{{"globalId",audit.global_row_id}};
    const auto latest=no_history(table);
    for(const auto& changed_name:names) {
        if(changed_name.is_null())continue;
        require(changed_name.is_string(),"recovery invalid changed field name");
        const auto name=changed_name.get<std::string>();const auto& c=column(table,name);
        const auto it=fields.find(name);require(it!=fields.end(),"recovery missing changed field value");
        if(name=="id") continue; // immutable local PK is never replayed from another origin
        if(name=="globalId") {
            require(original_scalar(*it,c)==recovery_scalar(audit.global_row_id),"recovery original global identity mismatch");continue;
        }
        recovery_scalar v;
        if(latest.count(name)) {
            if(!row.before) continue; // same missing-NoHistory guard as existing replay
            v=row.before->at(name);
        } else v=original_scalar(*it,c);
        budget.charge(name.size());budget.value(v);(*row.after)[name]=std::move(v);
    }
}
std::string declaration(const scoped_recovery_request& r,uint64_t maximum) {
    // Versioned length-exact encoding; bound BEFORE copying any field. This
    // avoids constructing an unbounded escaped JSON mirror of the schema.
    std::string result;
    auto number=[&](uint64_t n) {
        require(result.size()<=maximum && maximum-result.size()>=8,"recovery declaration byte budget exceeded");
        for(int shift=56;shift>=0;shift-=8)result.push_back(static_cast<char>((n>>shift)&255));
    };
    auto text=[&](const std::string& value) {
        number(value.size());require(value.size()<=maximum-result.size(),"recovery declaration byte budget exceeded");
        result.append(value);
    };
    number(1);number(r.model_tables.size());for(const auto& name:r.model_tables)text(name);
    number(r.scoped_link_tables.size());for(const auto& name:r.scoped_link_tables)text(name);
    number(r.relations.size());for(const auto& link:r.relations){text(link.table);text(link.lhs_model);text(link.rhs_model);}
    return result;
}
void check_relation_metadata(sqlite3* db,const scoped_recovery_request& request,
    const std::map<std::string,recovery_outbox_table>& schemas,const scoped_recovery_limits& limits) {
    std::map<std::string,recovery_relation> catalog;
    for(const auto& relation:request.relations) {
        require(catalog.emplace(relation.table,relation).second,"recovery duplicate relation descriptor");
        const auto& table=schemas.at(relation.table);
        require(table.kind==recovery_outbox_table_kind::link&&table.internal_parent,
            "recovery relation must be an ordinary complete link table");
        // The known parent must match the explicit lhs descriptor. A shared
        // polymorphic/multiple-parent metadata value is outside this slice.
        const auto& parent=*table.internal_parent;
        require(parent==relation.lhs_model || parent.rfind(relation.lhs_model+":",0)==0,
            "recovery relation lhs descriptor contradicts durable metadata");
        require(parent.find(';')==std::string::npos,"recovery shared link ownership unsupported");
    }
    // Validate durable parent-side coverage without guessing an RHS type from
    // a link table's spelling. The complete RHS catalog remains a trusted
    // actual-registration/controller precondition, not a proof from these rows.
    for(const auto& model:request.model_tables) {
        stmt links(db,"SELECT key,value FROM main._lattice_meta WHERE substr(key,1,15)='internal_table:' "
            "AND (value=?1 OR substr(value,1,length(?1)+1)=?1||':' OR instr(';'||value||';',';'||?1||':')>0)");
        links.text(1,model);uint64_t count=0;
        while(links.next()) {
            require(count++<limits.capture.tables,"recovery relation metadata count limit exceeded");
            const auto name=links.string(0,limits.field_bytes);
            const auto parent=links.string(1,limits.field_bytes);
            require(name.size()>15&&catalog.count(name.substr(15))&&parent.find(';')==std::string::npos,
                "recovery unknown or unsupported relation closure");
        }
    }
}
void install_body(lattice_db& owner,database& writer,const scoped_recovery_request& request,
                  const scoped_recovery_limits& limits) {
    auto* db=writer.handle();work_budget budget{limits};metadata durable(db,limits);
    require(request.identity.mode==receive_install_mode::full,"recovery model installer supports full scope only");
    require(request.model_tables.size()<=limits.capture.tables&&request.relations.size()<=limits.capture.tables&&
        request.scoped_link_tables.size()<=limits.capture.tables&&request.full_rows.size()<=limits.targets&&
        request.initial_row_grants.size()<=limits.targets&&request.pending.size()<=limits.receipts,
        "recovery input collection limit exceeded");
    std::set<std::string> admitted_tables,models,scoped_links;
    std::map<std::string,std::vector<std::string>> requested;
    for(const auto& name:request.model_tables) {
        budget.identity(name);quote(name);
        require(models.insert(name).second&&admitted_tables.insert(name).second,"recovery duplicate model declaration");
        if(const auto* registered=schema_registry::instance().get_schema(name)) {
            for(const auto& property:registered->properties)
                require(!property.is_geo_bounds && !property.is_union && !property.is_vector &&
                    property.kind!=property_kind::union_type && property.kind!=property_kind::virtual_link &&
                    property.kind!=property_kind::virtual_list,"recovery registered non-scalar model dependency unsupported");
        }
        requested[name];
    }
    for(const auto& relation:request.relations) {
        budget.identity(relation.table);budget.identity(relation.lhs_model);budget.identity(relation.rhs_model);
        quote(relation.table);quote(relation.lhs_model);quote(relation.rhs_model);
        require(!models.count(relation.table),"recovery relation/model alias");requested[relation.table];
    }
    for(const auto& name:request.scoped_link_tables) {
        budget.identity(name);
        require(requested.count(name)&&!models.count(name)&&scoped_links.insert(name).second&&admitted_tables.insert(name).second,
            "recovery scoped link lacks unique descriptor");
    }
    const auto declared=declaration(request,static_cast<uint64_t>(limits.metadata_bytes));
    require(declared.size()<=static_cast<uint64_t>(limits.metadata_bytes),"recovery declaration exceeds durable byte limit");
    if(auto old=durable.declarations.find(request.binding.channel);old!=durable.declarations.end()) {
        require(request.identity.expected_revision>0,"recovery membership exists without installed receiver state");
        require(old->second==declared,"recovery scope declaration changed");
    }
    else require(request.identity.expected_revision==0,"recovery installed scope membership is missing");
    std::map<key,key> targets; // folded->original; refusal instead of implicit case canonicalization
    auto target=[&](const key& k) {
        budget.identity(k.table);budget.identity(k.global_id);
        require(k.global_id.find('\0')==std::string::npos,"recovery NUL global row identity is unsupported by NOCASE keys");
        require(admitted_tables.count(k.table),"recovery row target is outside declared scope tables");
        const auto normalized=folded(k);
        auto found=targets.find(normalized);
        if(found!=targets.end()) require(found->second==k,"recovery case-alias input target");
        else {require(targets.size()<limits.targets,"recovery target union limit exceeded");targets.emplace(normalized,k);}
        if(auto own=durable.ownership.find(normalized);own!=durable.ownership.end())
            require(own->second.channel==request.binding.channel&&own->second.target==k,"recovery target owned by another scope or spelling");
    };
    for(const auto& [_,m]:durable.ownership)if(m.channel==request.binding.channel)target(m.target);
    std::set<key> granted;
    for(const auto& k:request.initial_row_grants) {
        require(request.identity.expected_revision==0,"recovery initial adoption after installation is unsupported");
        target(k);require(granted.insert(k).second,"recovery duplicate initial row grant");
    }
    std::map<std::string,const recovery_pending_grant*> receipts;
    for(const auto& grant:request.pending) {
        budget.identity(grant.audit_global_id);
        require(grant.audit_global_id.find('\0')==std::string::npos,"recovery NUL audit identity is unsupported by NOCASE keys");
        target(grant.target);granted.insert(grant.target);
        require(receipts.emplace(grant.audit_global_id,&grant).second,"recovery duplicate pending identity grant");
        require(grant.outcome==recovery_pending_outcome::committed_effect||grant.outcome==recovery_pending_outcome::committed_noop||
            grant.outcome==recovery_pending_outcome::not_committed,"recovery unknown or policy-only pending outcome");
    }
    std::map<key,const recovery_full_row*> full;
    for(const auto& row:request.full_rows) {
        target(row.key);require(full.emplace(row.key,&row).second,"recovery duplicate full row");
        require(row.values.size()<=limits.capture.columns_per_table,"recovery full row field count exceeded");
        for(const auto& [name,value]:row.values){budget.identity(name);budget.value(value);}
    }
    for(const auto& [_,k]:targets)requested[k.table].push_back(k.global_id);
    auto current=capture_recovery_rows(owner,requested,limits.capture);
    std::map<std::string,recovery_outbox_table> schemas;
    for(auto& table:current.tables) {
        require(table.kind!=recovery_outbox_table_kind::polymorphic_link,"recovery polymorphic links unsupported");
        require((models.count(table.name)!=0)==(table.kind==recovery_outbox_table_kind::model),"recovery declared model/link kind mismatch");
        schemas.emplace(table.name,table);
    }
    check_relation_metadata(db,request,schemas,limits);
    std::map<key,planned_row> plan;
    for(const auto& row:current.current_rows) {
        key k{current.tables.at(row.table_index).name,row.lookup_global_id};
        planned_row p;p.local_id=row.local_row_id;
        if(row.present) {
            p.before=captured_values(current,row);
            require(durable.ownership.count(folded(k))||granted.count(k),"recovery existing unowned row needs explicit grant");
        }
        if(auto input=full.find(k);input!=full.end()) {
            validate_row(schemas.at(k.table),k,input->second->values);
            p.after=input->second->values;
            for(auto& [name,value]:*p.after)if(fold(column(schemas.at(k.table),name).declared_type)=="real"&&std::holds_alternative<int64_t>(value))
                value=static_cast<double>(std::get<int64_t>(value));
        }
        require(plan.emplace(k,std::move(p)).second,"recovery duplicate captured target");
    }
    require(plan.size()==targets.size(),"recovery target capture incomplete");
    std::vector<key> filter;filter.reserve(targets.size());
    for(const auto& [_,k]:targets)filter.push_back(k);
    auto pending=capture_pending_outbox_for_targets(owner,request.binding.channel,filter,limits.capture);
    std::vector<const recovery_outbox_audit*> accepted;
    std::set<std::string> classified;
    for(const auto& audit:pending.audit) {
        const key k{audit.table_name,audit.global_row_id};
        auto receipt=receipts.find(audit.global_id);
        require(receipt!=receipts.end()&&receipt->second->target==k,"recovery pending identity lacks exact target-bound outcome");
        require(classified.insert(audit.global_id).second,"recovery duplicate original AuditLog identity");
        if(receipt->second->outcome==recovery_pending_outcome::not_committed) {
            auto& row=plan.at(k);row.unresolved=true;replay(audit,schemas.at(k.table),row,budget);
        } else accepted.push_back(&audit);
    }
    // An explicit grant may refer to a receipt already settled locally, but
    // cannot acquire an unrelated row without an actual original audit record.
    for(const auto& grant:request.pending)if(!classified.count(grant.audit_global_id)) {
        stmt audit(db,"SELECT a.tableName,a.globalRowId,ss.is_synchronized FROM main.AuditLog a "
            "LEFT JOIN main._lattice_sync_state ss ON ss.audit_entry_id=a.id AND ss.sync_id=?1 WHERE a.globalId=?2 LIMIT 2");
        audit.text(1,request.binding.channel);audit.text(2,grant.audit_global_id);
        require(audit.next()&&audit.string(0,limits.field_bytes)==grant.target.table&&
            audit.string(1,limits.field_bytes)==grant.target.global_id&&audit.number(2)==1&&!audit.next(),
            "recovery pending target grant has no retained original channel receipt");
        require(grant.outcome!=recovery_pending_outcome::not_committed,"recovery not-committed outcome contradicts settled local receipt");
    }
    std::set<key> membership;
    for(const auto& [k,row]:plan) {
        if(row.after){validate_row(schemas.at(k.table),k,*row.after);membership.insert(k);}
        else if(row.unresolved)membership.insert(k);
    }
    // Final link endpoint closure is explicit and checked before any model
    // effect. This slice does not permit references into another scope.
    std::map<std::string,recovery_relation> relations;
    for(const auto& relation:request.relations)relations.emplace(relation.table,relation);
    std::map<std::string,std::set<std::pair<std::string,std::string>>> pairs;
    for(const auto& [k,row]:plan)if(row.after&&scoped_links.count(k.table)) {
        const auto& relation=relations.at(k.table);
        const auto lhs=std::get<std::string>(row.after->at("lhs"));const auto rhs=std::get<std::string>(row.after->at("rhs"));
        require(!lhs.empty()&&!rhs.empty(),"recovery empty link endpoint");
        for(const auto& endpoint:{key{relation.lhs_model,lhs},key{relation.rhs_model,rhs}}) {
            const auto found=plan.find(endpoint);
            require(found!=plan.end()&&found->second.after,"recovery link endpoint is absent or outside this scope");
        }
        require(pairs[k.table].emplace(lhs,rhs).second,"recovery duplicate link endpoint pair");
        stmt collision(db,"SELECT globalId FROM main."+quote(k.table)+" WHERE lhs=? AND rhs=? LIMIT 2");
        collision.text(1,lhs);collision.text(2,rhs);
        while(collision.next()) {
            key existing{k.table,collision.string(0,limits.field_bytes)};
            auto found=plan.find(existing);
            require(existing==k || (found!=plan.end()&&!found->second.after),"recovery link pair overlaps a preserved row");
        }
    }
    for(const auto& [k,row]:plan)if(row.before&&!row.after&&models.count(k.table)) {
        for(const auto& relation:request.relations) {
            for(const auto& end:{std::pair{relation.lhs_model,std::string("lhs")},std::pair{relation.rhs_model,std::string("rhs")}}) {
                if(end.first!=k.table)continue;
                stmt references(db,"SELECT globalId FROM main."+quote(relation.table)+" WHERE "+quote(end.second)+"=? COLLATE NOCASE");
                references.text(1,k.global_id);uint64_t n=0;
                while(references.next()) {
                    require(n++<limits.targets,"recovery reference check limit exceeded");
                    key link{relation.table,references.string(0,limits.field_bytes)};
                    auto it=plan.find(link);
                    require(it!=plan.end()&&!it->second.after,"recovery deletion would orphan a preserved outside-scope link");
                }
            }
        }
    }
    // Scope counters/capacity are evaluated before model effects; savepoint and
    // the trusted outer install transaction roll metadata back on any failure.
    durable.replace(request.binding.channel,declared,membership);
    int64_t disabled;
    {
        stmt flag(db,"SELECT disabled FROM main._SyncControl WHERE id=1 LIMIT 2");
        require(flag.next(),"recovery sync control missing");disabled=flag.number(0);
        require((disabled==0||disabled==1)&&!flag.next(),"recovery corrupt sync control");
    }
    execute(db,"UPDATE main._SyncControl SET disabled=1 WHERE id=1");changed(db);
    {stmt flag(db,"SELECT disabled FROM main._SyncControl WHERE id=1");require(flag.next()&&flag.number(0)==1&&!flag.next(),"recovery audit suppression failed");}
    auto remove=[&](const key& k) {
        stmt sql(db,"DELETE FROM main."+quote(k.table)+" WHERE globalId=?");sql.text(1,k.global_id);sql.done();changed(db);
    };
    // Remove doomed links before models; surviving model rows use UPDATE and
    // retain their physical PK. Final pair changes use DELETE/INSERT for links
    // only (their rowid is internal, never a public model identity).
    for(const auto& [k,row]:plan)if(scoped_links.count(k.table)&&row.before&&row.before!=row.after)remove(k);
    for(const auto& [k,row]:plan)if(models.count(k.table)&&row.before&&!row.after)remove(k);
    auto write=[&](const key& k,const planned_row& row) {
        if(!row.after||row.before==row.after)return;
        const bool update=models.count(k.table)&&row.before.has_value();
        std::string sql=update?"UPDATE main."+quote(k.table)+" SET ":"INSERT INTO main."+quote(k.table)+"(";
        int index=0;
        for(const auto& [name,_]:*row.after){if(index++)sql+=",";sql+=quote(name);if(update)sql+="=?";}
        if(update)sql+=" WHERE globalId=?";
        else {sql+=") VALUES(";for(int i=0;i<index;++i){if(i)sql+=",";sql+="?";}sql+=")";}
        stmt effect(db,sql);int at=1;for(const auto& [_,v]:*row.after)effect.value(at++,v);
        if(update)effect.text(at,k.global_id);effect.done();changed(db);
    };
    for(const auto& [k,row]:plan)if(models.count(k.table))write(k,row);
    for(const auto& [k,row]:plan)if(scoped_links.count(k.table))write(k,row);
    // Positive postcondition before publication. A user trigger/IGNORE cannot
    // silently claim the planned final state or replace a surviving local PK.
    auto actual=capture_recovery_rows(owner,requested,limits.capture);
    for(const auto& row:actual.current_rows) {
        key k{actual.tables.at(row.table_index).name,row.lookup_global_id};const auto& expected=plan.at(k);
        require(row.present==expected.after.has_value(),"recovery final row presence mismatch");
        if(row.present) {
            require(captured_values(actual,row)==*expected.after,"recovery final row value mismatch");
            if(models.count(k.table)&&expected.before)require(row.local_row_id==expected.local_id,"recovery surviving local PK changed");
        }
    }
    {
        stmt restore(db,"UPDATE main._SyncControl SET disabled=? WHERE id=1 AND disabled=1");restore.integer(1,disabled);restore.done();changed(db);
        stmt flag(db,"SELECT disabled FROM main._SyncControl WHERE id=1");require(flag.next()&&flag.number(0)==disabled&&!flag.next(),"recovery audit suppression restore failed");
    }
    for(const auto* audit:accepted) {
        stmt settle(db,"INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) "
            "ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1");
        settle.integer(1,audit->id);settle.text(2,request.binding.channel);settle.done();changed(db);
        stmt read(db,"SELECT is_synchronized FROM main._lattice_sync_state WHERE audit_entry_id=? AND sync_id=? LIMIT 2");
        read.integer(1,audit->id);read.text(2,request.binding.channel);
        require(read.next()&&read.number(0)==1&&!read.next(),"recovery scoped obligation settlement failed");
    }
}
} // namespace

scoped_recovery_result install_scoped_recovery(std::shared_ptr<lattice_db> owner,
    const scoped_recovery_request& request,const scoped_recovery_limits& limits) {
    scoped_recovery_result result;
    result.transaction=recovery_writer_access::install(owner,[&](database& writer) {
        require(limits.channels>0&&limits.members>=0&&limits.metadata_bytes>0&&limits.targets>0&&
            limits.receipts>0&&limits.fields>0&&limits.field_bytes>0&&limits.field_bytes<=static_cast<uint64_t>(INT_MAX)&&
            limits.logical_bytes>0,"recovery invalid explicit limits");
        receive_install_store state(owner,limits.installations);state.initialize();state.bind(request.binding);
        result.installation=state.apply_if_new(request.binding,request.identity,request.supersede,[&](database& actual) {
            require(&actual==&writer&&recovery_writer_access::active_writer(*owner)==&writer,"recovery physical writer changed");
            install_body(*owner,writer,request,limits);
        });
    });
    if(result.transaction.state!=recovery_install_state::committed)result.installation.reset();
    return result;
}
} // namespace lattice::detail
