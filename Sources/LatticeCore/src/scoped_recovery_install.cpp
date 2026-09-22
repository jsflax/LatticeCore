#include "scoped_recovery_install.hpp"
#include "canonical_scoped_install.hpp"
#include "recovery_witness.hpp"
#include "canonical_writer_adapter.hpp"
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
struct identities {
    bool uuid;
    explicit identities(recovery_identity_mode mode) : uuid(mode==recovery_identity_mode::uuid) {
        require(mode==recovery_identity_mode::exact_string || mode==recovery_identity_mode::uuid,
            "recovery invalid identity mode");
    }
    std::string id(const std::string& value) const {
        return uuid ? canonical_writer_adapter::uuid_key(value) : value;
    }
    key normalized(const key& value) const {return {value.table,id(value.global_id)};}
    bool equal(const std::string& one,const std::string& two) const {return id(one)==id(two);}
    std::string collation() const {return uuid ? " COLLATE NOCASE" : "";}
};
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
// Only the explicitly selected UUID mode calls these. A complete unique
// NOCASE key makes each normalized globalId lookup indexed and unambiguous.
// Partial, expression, multi-column or binary-only keys cannot establish it.
void require_uuid_key(sqlite3* db,const std::string& table,work_budget& budget) {
    stmt indexes(db,"PRAGMA main.index_list("+quote(table)+")");
    bool found=false;
    while(indexes.next()) {
        budget.charge(8);
        const auto unique=indexes.number(2),partial=indexes.number(4);
        require((unique==0||unique==1)&&(partial==0||partial==1),"recovery corrupt index flags");
        if(!unique||partial) continue;
        const auto name=indexes.string(1,budget.limits.field_bytes);budget.identity(name);
        stmt columns(db,"PRAGMA main.index_xinfo("+quote(name)+")");
        size_t count=0;bool match=true;
        while(columns.next()) {
            budget.charge(8);const auto used=columns.number(5);
            require(used==0||used==1,"recovery corrupt index key flag");
            if(!used)continue;
            ++count;
            if(columns.number(1)<0 || sqlite3_column_type(columns.p,2)!=SQLITE_TEXT ||
                sqlite3_column_type(columns.p,4)!=SQLITE_TEXT) {match=false;continue;}
            const auto column=columns.string(2,budget.limits.field_bytes);
            const auto collation=columns.string(4,budget.limits.field_bytes);
            budget.identity(column);budget.identity(collation);
            match=match && column=="globalId" && fold(collation)=="nocase";
        }
        found=found || (count==1&&match);
    }
    require(found,"recovery UUID identity requires a complete NOCASE unique key");
}
std::string local_uuid_spelling(sqlite3* db,const key& canonical,work_budget& budget) {
    stmt row(db,"SELECT globalId FROM main."+quote(canonical.table)+" WHERE globalId=? COLLATE NOCASE LIMIT 2");
    row.text(1,canonical.global_id);
    if(!row.next())return canonical.global_id;
    auto actual=row.string(0,36);budget.identity(actual);
    require(canonical_writer_adapter::uuid_key(actual)==canonical.global_id&&!row.next(),
        "recovery ambiguous or corrupt local UUID identity");
    return actual;
}
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
void validate_row(const recovery_outbox_table& table,const key& k,const values& row,const identities& ids) {
    const size_t expected=table.columns.size()-(table.kind==recovery_outbox_table_kind::model?1:0);
    require(row.size()==expected && !row.count("id"),"recovery incomplete final row or remote local PK");
    for(const auto& c:table.columns) {
        if(c.name=="id") continue;
        auto it=row.find(c.name);require(it!=row.end(),"recovery missing final column");valid_value(c,it->second);
    }
    require(ids.equal(std::get<std::string>(row.at("globalId")),k.global_id),"recovery global identity disagreement");
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
    bool base_replaced=true, preserve_membership=false;
};
void replay(const recovery_outbox_audit& audit,const recovery_outbox_table& table,
            planned_row& row,work_budget& budget,const identities& ids) {
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
            const auto original=original_scalar(*it,c);
            require(ids.uuid ? ids.equal(std::get<std::string>(original),audit.global_row_id) :
                original==recovery_scalar(audit.global_row_id),"recovery original global identity mismatch");continue;
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
    number(r.identity_mode==recovery_identity_mode::uuid?2:1);number(r.model_tables.size());for(const auto& name:r.model_tables)text(name);
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
void verify_planned_rows(lattice_db& owner,const std::map<std::string,std::vector<std::string>>& requested,
    const std::map<key,planned_row>& plan,const std::set<std::string>& models,
    const scoped_recovery_limits& limits,const identities& ids) {
    auto actual=capture_recovery_rows(owner,requested,limits.capture);
    for(const auto& row:actual.current_rows) {
        key k=ids.normalized({actual.tables.at(row.table_index).name,row.lookup_global_id});const auto& expected=plan.at(k);
        require(row.present==expected.after.has_value(),"recovery final row presence mismatch");
        if(row.present) {
            require(captured_values(actual,row)==*expected.after,"recovery final row value mismatch");
            if(models.count(k.table)&&expected.before)require(row.local_row_id==expected.local_id,"recovery surviving local PK changed");
        }
    }
}
struct installed_plan {
    // Bounded actual original identities from positive Q, including entries
    // already settled before this attempt. No negative/fresh-write inference.
    struct receipt { int64_t audit_id; std::string original_id; };
    std::map<std::string,std::vector<std::string>> requested;
    std::map<key,planned_row> rows;
    std::set<std::string> models;
    std::set<key> membership;
    std::string channel,declaration;
    int64_t disabled=0;
    std::vector<receipt> receipts;
    std::optional<recovery_witness> witness;
    void verify(lattice_db& owner,database& writer,const scoped_recovery_limits& limits,const identities& ids) const {
        verify_planned_rows(owner,requested,rows,models,limits,ids);
        auto* db=recovery_writer_access::active_handle(owner,writer);
        metadata actual(db,limits);
        require(actual.declarations.count(channel) && actual.declarations.at(channel)==declaration,
            "canonical final scope declaration changed");
        std::set<key> members;
        for(const auto& [_,member]:actual.ownership)if(member.channel==channel)members.insert(member.target);
        require(members==membership,"canonical final scope membership changed");
        // Receiver completion and journal settlement can run triggers after the
        // model installer's own checks. Revalidate these owned outputs only
        // after those writes; no repair or broader whole-database claim.
        require(receipts.size()<=limits.receipts,"canonical final receipt count exceeded");
        for(const auto& expected:receipts) {
            stmt read(db,"SELECT ss.is_synchronized FROM main.AuditLog a "
                "JOIN main._lattice_sync_state ss ON ss.audit_entry_id=a.id AND ss.sync_id=?1 "
                "WHERE a.id=?2 AND a.globalId=?3 COLLATE NOCASE LIMIT 2");
            read.text(1,channel);read.integer(2,expected.audit_id);read.text(3,expected.original_id);
            require(read.next()&&read.number(0)==1&&!read.next(),"canonical final scoped receipt changed");
        }
        {
            stmt flag(db,"SELECT disabled FROM main._SyncControl WHERE id=1 LIMIT 2");
            require(flag.next()&&flag.number(0)==disabled&&!flag.next(),"canonical final sync control changed");
        }
        require(witness.has_value() && read_recovery_witness(writer)==witness,
            "canonical final recovery witness changed");
    }
};
void validate_install_limits(const scoped_recovery_limits& limits) {
    require(limits.channels>0&&limits.members>=0&&limits.metadata_bytes>0&&limits.targets>0&&
        limits.receipts>0&&limits.fields>0&&limits.field_bytes>0&&limits.field_bytes<=static_cast<uint64_t>(INT_MAX)&&
        limits.logical_bytes>0,"recovery invalid explicit limits");
}
void install_body(lattice_db& owner,database& writer,const scoped_recovery_request& request,
                  const scoped_recovery_limits& limits,
                  const std::vector<recovery_row_image>* explicit_images,
                  installed_plan* final_plan=nullptr) {
    const identities ids(request.identity_mode);
    auto* db=recovery_writer_access::active_handle(owner,writer);work_budget budget{limits};metadata durable(db,limits);
    const bool delta=request.identity.mode==receive_install_mode::delta;
    require(explicit_images || request.identity.mode==receive_install_mode::full,"recovery model installer supports full scope only");
    require(!explicit_images || request.full_rows.empty(),"recovery explicit images cannot mix with legacy full rows");
    require(request.model_tables.size()<=limits.capture.tables&&request.relations.size()<=limits.capture.tables&&
        request.scoped_link_tables.size()<=limits.capture.tables&&
        (explicit_images?explicit_images->size():request.full_rows.size())<=limits.targets&&
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
    std::map<key,key> targets; // folded -> exact key or explicit canonical UUID key
    auto target=[&](const key& input) {
        budget.identity(input.table);budget.identity(input.global_id);
        const key k=ids.normalized(input);
        require(k.global_id.find('\0')==std::string::npos,"recovery NUL global row identity is unsupported by NOCASE keys");
        require(admitted_tables.count(k.table),"recovery row target is outside declared scope tables");
        const auto normalized=folded(k);
        auto found=targets.find(normalized);
        if(found!=targets.end()) require(found->second==k,"recovery case-alias input target");
        else {require(targets.size()<limits.targets,"recovery target union limit exceeded");targets.emplace(normalized,k);}
        if(auto own=durable.ownership.find(normalized);own!=durable.ownership.end())
            require(own->second.channel==request.binding.channel&&ids.normalized(own->second.target)==k,"recovery target owned by another scope or spelling");
        return k;
    };
    for(const auto& [_,m]:durable.ownership)if(m.channel==request.binding.channel)target(m.target);
    std::set<key> granted;
    for(const auto& k:request.initial_row_grants) {
        require(request.identity.expected_revision==0,"recovery initial adoption after installation is unsupported");
        require(granted.insert(target(k)).second,"recovery duplicate initial row grant");
    }
    std::map<std::string,const recovery_pending_grant*> receipts;
    for(const auto& grant:request.pending) {
        budget.identity(grant.audit_global_id);
        require(grant.audit_global_id.find('\0')==std::string::npos,"recovery NUL audit identity is unsupported by NOCASE keys");
        granted.insert(target(grant.target));
        require(receipts.emplace(ids.id(grant.audit_global_id),&grant).second,"recovery duplicate pending identity grant");
        require(grant.outcome==recovery_pending_outcome::committed_effect||grant.outcome==recovery_pending_outcome::committed_noop||
            grant.outcome==recovery_pending_outcome::not_committed,"recovery unknown or policy-only pending outcome");
    }
    // Non-owning views into immutable caller input. Never normalize or rewrite
    // the source row payload while constructing the separate local plan.
    std::map<key,const values*> images;
    auto image=[&](const key& k,const values* row) {
        require(images.emplace(target(k),row).second,explicit_images?
            "recovery duplicate row image":"recovery duplicate full row");
        if(!row)return;
        require(row->size()<=limits.capture.columns_per_table,"recovery full row field count exceeded");
        for(const auto& [name,value]:*row){budget.identity(name);budget.value(value);}
    };
    if(explicit_images)for(const auto& row:*explicit_images)image(row.key,row.present?&*row.present:nullptr);
    else for(const auto& row:request.full_rows)image(row.key,&row.values);
    if(delta)for(const auto& grant:request.pending)
        require(grant.outcome==recovery_pending_outcome::not_committed || images.count(ids.normalized(grant.target)),
            "recovery committed delta outcome requires explicit rebase image");
    if(ids.uuid) {
        for(const auto& [table,_]:requested)require_uuid_key(db,table,budget);
        require_uuid_key(db,"AuditLog",budget);
    }
    for(const auto& [_,k]:targets)
        requested[k.table].push_back(ids.uuid?local_uuid_spelling(db,k,budget):k.global_id);
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
        key k=ids.normalized({current.tables.at(row.table_index).name,row.lookup_global_id});
        planned_row p;p.local_id=row.local_row_id;
        if(row.present) {
            p.before=captured_values(current,row);
            require(durable.ownership.count(folded(k))||granted.count(k),"recovery existing unowned row needs explicit grant");
        }
        const auto input=images.find(k);
        p.base_replaced=!delta || input!=images.end();
        p.preserve_membership=delta && input==images.end() && durable.ownership.count(folded(k));
        if(!p.base_replaced)p.after=p.before;
        if(input!=images.end() && input->second) {
            validate_row(schemas.at(k.table),k,*input->second,ids);
            p.after=*input->second;
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
        const key k=ids.normalized({audit.table_name,audit.global_row_id});
        auto receipt=receipts.find(ids.id(audit.global_id));
        require(receipt!=receipts.end()&&ids.normalized(receipt->second->target)==k,"recovery pending identity lacks exact target-bound outcome");
        require(classified.insert(ids.id(audit.global_id)).second,"recovery duplicate original AuditLog identity");
        if(receipt->second->outcome==recovery_pending_outcome::not_committed) {
            auto& row=plan.at(k);row.unresolved=true;
            if(row.base_replaced)replay(audit,schemas.at(k.table),row,budget,ids);
        } else accepted.push_back(&audit);
    }
    // An explicit grant may refer to a receipt already settled locally, but
    // cannot acquire an unrelated row without an actual original audit record.
    for(const auto& grant:request.pending)if(!classified.count(ids.id(grant.audit_global_id))) {
        stmt audit(db,"SELECT a.tableName,a.globalRowId,ss.is_synchronized FROM main.AuditLog a "
            "LEFT JOIN main._lattice_sync_state ss ON ss.audit_entry_id=a.id AND ss.sync_id=?1 WHERE a.globalId=?2"+ids.collation()+" LIMIT 2");
        audit.text(1,request.binding.channel);audit.text(2,grant.audit_global_id);
        require(audit.next()&&audit.string(0,limits.field_bytes)==grant.target.table&&
            ids.equal(audit.string(1,limits.field_bytes),grant.target.global_id)&&audit.number(2)==1&&!audit.next(),
            "recovery pending target grant has no retained original channel receipt");
        require(grant.outcome!=recovery_pending_outcome::not_committed,"recovery not-committed outcome contradicts settled local receipt");
    }
    // Work on owned copies only. A surviving local spelling is authoritative
    // for storage, not a rewrite of the authenticated source values/digests.
    if(ids.uuid) for(auto& [k,row]:plan) {
        if(row.after&&row.before)(*row.after)["globalId"]=row.before->at("globalId");
    }
    std::set<key> membership;
    for(const auto& [k,row]:plan) {
        if(row.after) {
            validate_row(schemas.at(k.table),k,*row.after,ids);
            membership.insert({k.table,ids.uuid?std::get<std::string>(row.after->at("globalId")):k.global_id});
        } else if(row.preserve_membership) {
            // An omitted row may be locally absent (e.g. pending DELETE).
            // Preserve the original member spelling as well as ownership.
            membership.insert(durable.ownership.at(folded(k)).target);
        } else if(row.unresolved)membership.insert({k.table,
            ids.uuid&&row.before?std::get<std::string>(row.before->at("globalId")):k.global_id});
    }
    // Final link endpoint closure is explicit and checked before any model
    // effect. This slice does not permit references into another scope.
    std::map<std::string,recovery_relation> relations;
    for(const auto& relation:request.relations)relations.emplace(relation.table,relation);
    std::map<std::string,std::set<std::pair<std::string,std::string>>> pairs;
    for(auto& [k,row]:plan)if(row.after&&scoped_links.count(k.table)) {
        const auto& relation=relations.at(k.table);
        if(ids.uuid && row.base_replaced) for(const auto& endpoint:{std::pair{relation.lhs_model,std::string("lhs")},
                                                std::pair{relation.rhs_model,std::string("rhs")}}) {
            const auto target=ids.normalized({endpoint.first,std::get<std::string>(row.after->at(endpoint.second))});
            const auto found=plan.find(target);
            require(found!=plan.end()&&found->second.after,"recovery link endpoint is absent or outside this scope");
            (*row.after)[endpoint.second]=found->second.after->at("globalId");
        }
        const auto lhs=std::get<std::string>(row.after->at("lhs"));const auto rhs=std::get<std::string>(row.after->at("rhs"));
        require(!lhs.empty()&&!rhs.empty(),"recovery empty link endpoint");
        for(const auto& endpoint:{key{relation.lhs_model,lhs},key{relation.rhs_model,rhs}}) {
            const auto found=plan.find(ids.normalized(endpoint));
            require(found!=plan.end()&&found->second.after,"recovery link endpoint is absent or outside this scope");
        }
        require(pairs[k.table].emplace(delta?ids.id(lhs):lhs,delta?ids.id(rhs):rhs).second,
            "recovery duplicate link endpoint pair");
        stmt collision(db,"SELECT globalId FROM main."+quote(k.table)+" WHERE lhs=?"+ids.collation()+
            " AND rhs=?"+ids.collation()+(ids.uuid?"":" LIMIT 2"));
        collision.text(1,lhs);collision.text(2,rhs);
        uint64_t collisions=0;
        while(collision.next()) {
            if(ids.uuid)require(collisions++<limits.targets,"recovery link collision check limit exceeded");
            key existing=ids.normalized({k.table,collision.string(0,limits.field_bytes)});
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
                    key link=ids.normalized({relation.table,references.string(0,limits.field_bytes)});
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
        stmt sql(db,"DELETE FROM main."+quote(k.table)+" WHERE globalId=?"+ids.collation());sql.text(1,k.global_id);sql.done();changed(db);
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
        if(update)sql+=" WHERE globalId=?"+ids.collation();
        else {sql+=") VALUES(";for(int i=0;i<index;++i){if(i)sql+=",";sql+="?";}sql+=")";}
        stmt effect(db,sql);int at=1;for(const auto& [_,v]:*row.after)effect.value(at++,v);
        if(update)effect.text(at,k.global_id);effect.done();changed(db);
    };
    for(const auto& [k,row]:plan)if(models.count(k.table))write(k,row);
    for(const auto& [k,row]:plan)if(scoped_links.count(k.table))write(k,row);
    // Positive postcondition before publication. A user trigger/IGNORE cannot
    // silently claim the planned final state or replace a surviving local PK.
    if(ids.uuid) {
        for(auto& [_,keys]:requested)keys.clear();
        for(const auto& [k,row]:plan) {
            const auto* value=row.after?&*row.after:row.before?&*row.before:nullptr;
            requested[k.table].push_back(value?std::get<std::string>(value->at("globalId")):k.global_id);
        }
    }
    verify_planned_rows(owner,requested,plan,models,limits,ids);
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
    if(final_plan)*final_plan={std::move(requested),std::move(plan),std::move(models),std::move(membership),
        request.binding.channel,declared,disabled,{},{}};
}
scoped_recovery_result install_images(std::shared_ptr<lattice_db> owner,
    const scoped_recovery_request& request,const scoped_recovery_limits& limits,
    const std::vector<recovery_row_image>* explicit_images) {
    scoped_recovery_result result;
    result.transaction=recovery_writer_access::install(owner,[&](database& writer) {
        validate_install_limits(limits);
        // Admission remains inside the original retained-owner frame, including
        // exact retries. Reject two competing row inputs before state work.
        require(!explicit_images || request.full_rows.empty(),"recovery explicit images cannot mix with legacy full rows");
        receive_install_store state(owner,limits.installations);state.initialize();state.bind(request.binding);
        result.installation=state.apply_if_new(request.binding,request.identity,request.supersede,[&](database& actual) {
            require(&actual==&writer&&recovery_writer_access::active_writer(*owner)==&writer,"recovery physical writer changed");
            install_body(*owner,writer,request,limits,explicit_images);
            // Only a newly applied installation reaches this body. Its witness
            // commits/rolls back with model, membership and installed receipt;
            // exact retries and generic writer-access reads do not advance it.
            bump_recovery_witness(*owner);
        });
    });
    if(result.transaction.state!=recovery_install_state::committed)result.installation.reset();
    return result;
}
} // namespace

scoped_recovery_result install_scoped_recovery(std::shared_ptr<lattice_db> owner,
    const scoped_recovery_request& request,const scoped_recovery_limits& limits) {
    return install_images(std::move(owner),request,limits,nullptr);
}
scoped_recovery_result install_scoped_recovery_images(std::shared_ptr<lattice_db> owner,
    const scoped_recovery_request& request,const std::vector<recovery_row_image>& images,
    const scoped_recovery_limits& limits) {
    return install_images(std::move(owner),request,limits,&images);
}

scoped_recovery_result install_staged_canonical_range(const canonical_install_admission& grant) {
    namespace cr=canonical_range;
    scoped_recovery_result result;
    const auto& limits=grant.limits_.install;
    result.transaction=recovery_writer_access::install(grant.owner_,[&](database& writer) {
        validate_install_limits(limits);
        require(grant.journal_revision_>0 && !grant.coverage_id_.empty() &&
            grant.coverage_id_.size()<=limits.field_bytes,"canonical installation lacks bound coverage admission");
        require(grant.journal_.channel==grant.attempt_.channel &&
            grant.profile_.binding.channel==grant.attempt_.channel,"canonical admission channel differs");
        receive_install_store state(grant.owner_,limits.installations);state.initialize();
        canonical_range_staging staged(grant.owner_,limits.installations,grant.limits_.codec,grant.limits_.staging);
        staged.initialize();
        recovery_obligation_store journal(grant.owner_,grant.limits_.obligations,limits.installations);journal.initialize();
        const cr::frame ending{grant.attempt_,grant.route_,cr::end{grant.manifest_digest_}};
        const auto verified=staged.verify_end(ending);
        require(verified.content_verified && verified.state.frozen_request.request_digest==grant.request_digest_ &&
            verified.installation_binding==grant.profile_.binding,"canonical admission differs from retained stage");
        auto current_journal=journal.read(grant.journal_.channel);
        require(current_journal && current_journal->address==grant.journal_ && current_journal->profile==grant.profile_,
            "canonical admission differs from current journal binding");
        const auto& identity=verified.installation_identity;
        if(grant.receive_guard_) {
            require(grant.receive_guard_->channel==grant.attempt_.channel,"canonical receive guard channel differs");
            const auto installed=state.read(grant.attempt_.channel);
            if(installed && installed->last_installed==std::optional<receive_install_identity>{identity})
                receive_delivery_guard_access::verify_canonical_completed(*grant.owner_,writer,*grant.receive_guard_);
            else receive_delivery_guard_access::verify_owned(*grant.owner_,writer,*grant.receive_guard_);
        } else require(receive_delivery_guard_access::read_owned(*grant.owner_,writer,grant.attempt_.channel).state!=
            receive_guard_state::canonical_installed,"canonical channel requires bound receive completion admission");
        std::optional<recovery_obligation_snapshot> before;
        std::optional<installed_plan> final_plan;
        std::vector<recovery_obligation_receipt_claim> positives;
        std::vector<installed_plan::receipt> positive_receipts;
        std::map<std::string,recovery_obligation_entry> retained;
        std::vector<std::string> content_pages,receipt_pages;
        const auto journal_usage=journal.usage();
        auto same_stage=[&] {
            const auto again=staged.resume(grant.attempt_,grant.manifest_digest_,grant.route_);
            require(again.content_verified && again.route_generation==verified.route_generation &&
                again.state==verified.state && again.installation_identity==identity &&
                again.installation_binding==verified.installation_binding,"canonical retained stage changed during install");
            // M/C/E do not bind partition boundaries for an equal page count.
            // Each canonical encoded page must still be the page we consumed.
            require(content_pages.size()==verified.state.offer.counts.content_pages &&
                receipt_pages.size()==verified.state.offer.counts.receipt_pages,"canonical consumed page inventory incomplete");
            for(size_t n=0;n<content_pages.size();++n)
                require(std::get<cr::content_page>(staged.read_verified_page(grant.attempt_,grant.manifest_digest_,grant.route_,cr::stream_kind::content,n)).digest==content_pages[n],
                    "canonical consumed content page changed");
            for(size_t n=0;n<receipt_pages.size();++n)
                require(std::get<cr::receipt_page>(staged.read_verified_page(grant.attempt_,grant.manifest_digest_,grant.route_,cr::stream_kind::receipts,n)).digest==receipt_pages[n],
                    "canonical consumed receipt page changed");
        };
        result.installation=state.apply_if_new(verified.installation_binding,identity,grant.supersede_,[&](database& actual) {
            require(&actual==&writer && recovery_writer_access::active_writer(*grant.owner_)==&writer,
                "canonical physical writer changed");
            before=journal.snapshot_for_install(grant.journal_,identity.sequence);
            require(before->scope.revision==grant.journal_revision_ && before->scope.profile==grant.profile_,
                "canonical final journal revision differs from admission");
            work_budget budget{limits};const identities ids(recovery_identity_mode::uuid);
            const auto& request=verified.state.frozen_request;
            require(request.receipts.size()<=limits.receipts && before->entries.size()<=limits.receipts &&
                verified.state.offer.counts.identities<=limits.targets &&
                verified.state.offer.counts.receipts<=limits.receipts,"canonical aggregate collection limit exceeded");
            std::map<std::string,const cr::receipt_request*> requested;
            for(const auto& q:request.receipts) {
                budget.identity(q.original_id);
                require(q.namespace_id==std::optional<std::string>{grant.profile_.receipt_namespace},
                    "canonical requested namespace is not admitted");
                // One original has one actual target. Broader rebase requests
                // remain a protocol feature; this first assembler refuses them.
                require(q.targets.size()==1,"canonical assembler requires one exact requested target");
                budget.identity(q.targets[0].table);budget.identity(q.targets[0].id);
                const auto original=ids.id(q.original_id);
                require(requested.emplace(original,&q).second,"canonical request has UUID alias originals");
                auto entry=journal.find(grant.journal_,original);
                require(entry && entry->canonical_target_id==ids.id(q.targets[0].id) &&
                    entry->record.table==q.targets[0].table,"canonical request lacks actual journal original/target");
                retained.emplace(original,std::move(*entry));
            }
            for(const auto& entry:before->entries)
                require(requested.count(entry.canonical_original_id),"canonical final obligation is absent from frozen Q; reconciliation required");
            require(grant.contract_.model_tables.size()<=limits.capture.tables &&
                grant.contract_.relations.size()<=limits.capture.tables && grant.contract_.scoped_link_tables.size()<=limits.capture.tables &&
                grant.contract_.initial_row_grants.size()<=limits.targets,"canonical scope collection limit exceeded");
            for(const auto& name:grant.contract_.model_tables)budget.identity(name);
            for(const auto& relation:grant.contract_.relations){budget.identity(relation.table);budget.identity(relation.lhs_model);budget.identity(relation.rhs_model);}
            for(const auto& name:grant.contract_.scoped_link_tables)budget.identity(name);
            for(const auto& row:grant.contract_.initial_row_grants){budget.identity(row.table);budget.identity(row.global_id);}
            scoped_recovery_request context;
            context.binding=verified.installation_binding;context.identity=identity;context.supersede=grant.supersede_;
            context.model_tables=grant.contract_.model_tables;context.relations=grant.contract_.relations;
            context.scoped_link_tables=grant.contract_.scoped_link_tables;
            context.initial_row_grants=grant.contract_.initial_row_grants;context.identity_mode=recovery_identity_mode::uuid;
            std::vector<recovery_row_image> images;
            images.reserve(static_cast<size_t>(verified.state.offer.counts.identities));
            std::set<key> unique_images;
            for(uint64_t n=0;n<verified.state.offer.counts.content_pages;++n) {
                auto page=std::get<cr::content_page>(staged.read_verified_page(grant.attempt_,grant.manifest_digest_,grant.route_,cr::stream_kind::content,n));
                budget.identity(page.digest);content_pages.push_back(page.digest);
                for(const auto& item:page.items) {
                    budget.identity(item.key.table);budget.identity(item.key.id);
                    require(images.size()<limits.targets,"canonical image aggregate count exceeded");
                    const key k{item.key.table,item.key.id};
                    require(unique_images.insert(ids.normalized(k)).second,"canonical content has UUID alias identities");
                    recovery_row_image image{k,std::nullopt};
                    if(const auto* present=std::get_if<cr::present>(&item.value)) {
                        auto cap=grant.limits_.codec.values;
                        cap.fields=std::min<uint64_t>(cap.fields,(limits.fields-budget.fields)/2);
                        cap.name_bytes=std::min<uint64_t>(cap.name_bytes,limits.field_bytes);
                        cap.value_bytes=std::min<uint64_t>(cap.value_bytes,limits.field_bytes);
                        cap.decoded_bytes=std::min<uint64_t>(cap.decoded_bytes,limits.logical_bytes-budget.bytes);
                        image.present=sync_recovery::decode_values(present->payload,cap);
                        for(const auto& [name,value]:*image.present){budget.identity(name);budget.value(value);}
                    }
                    images.push_back(std::move(image));
                }
            }
            std::set<std::string> seen;
            for(uint64_t n=0;n<verified.state.offer.counts.receipt_pages;++n) {
                auto page=std::get<cr::receipt_page>(staged.read_verified_page(grant.attempt_,grant.manifest_digest_,grant.route_,cr::stream_kind::receipts,n));
                budget.identity(page.digest);receipt_pages.push_back(page.digest);
                for(const auto& item:page.items) {
                    budget.identity(item.original_id);
                    const auto original=ids.id(item.original_id);
                    require(requested.count(original) && seen.size()<limits.receipts && seen.insert(original).second,
                        "canonical receipt has missing or duplicate requested original");
                    const auto& entry=retained.at(original);
                    recovery_pending_outcome outcome;
                    if(const auto* positive=std::get_if<cr::committed>(&item.value)) {
                        require(positive->namespace_id==grant.profile_.receipt_namespace && positive->coverage_id==grant.coverage_id_ &&
                            positive->position<=static_cast<uint64_t>(identity.head) && positive->accepted_target &&
                            positive->accepted_target->table==entry.record.table &&
                            ids.id(positive->accepted_target->id)==entry.canonical_target_id && positive->outcome!=cr::decision::policy,
                            "canonical positive receipt lacks bound source coverage/target");
                        outcome=positive->outcome==cr::decision::applied?recovery_pending_outcome::committed_effect:recovery_pending_outcome::committed_noop;
                        recovery_obligation_receipt_claim claim{entry.canonical_original_id,grant.profile_.receipt_namespace,
                            static_cast<int64_t>(positive->position),positive->outcome==cr::decision::applied?
                                recovery_obligation_outcome::applied:recovery_obligation_outcome::no_op};
                        require(!entry.acknowledged || entry.acknowledged==claim,"canonical positive contradicts retained first ACK");
                        if(entry.stage!=recovery_obligation_stage::settled)positives.push_back(std::move(claim));
                        require(positive_receipts.size()<limits.receipts && entry.record.audit_id>0,
                            "canonical positive actual receipt bound exceeded");
                        positive_receipts.push_back({entry.record.audit_id,entry.canonical_original_id});
                    } else if(const auto* negative=std::get_if<cr::not_committed>(&item.value)) {
                        require(negative->namespace_id==grant.profile_.receipt_namespace && negative->coverage_id==grant.coverage_id_ &&
                            entry.stage==recovery_obligation_stage::open && !entry.acknowledged,
                            "canonical negative lacks coverage or contradicts retained receipt");
                        outcome=recovery_pending_outcome::not_committed;
                    } else refuse("canonical unknown receipt requires reconciliation");
                    budget.identity(entry.record.original_id);budget.identity(entry.record.table);budget.identity(entry.record.target_id);
                    context.pending.push_back({entry.record.original_id,{entry.record.table,entry.record.target_id},outcome});
                }
            }
            require(seen.size()==requested.size(),"canonical receipt stream omitted a requested original");
            final_plan.emplace();
            install_body(*grant.owner_,writer,context,limits,&images,&*final_plan);
            same_stage();
            const auto unchanged=journal.snapshot_for_install(grant.journal_,identity.sequence);
            require(unchanged.scope==before->scope && unchanged.entries==before->entries,
                "canonical journal changed during model effects");
            final_plan->receipts=std::move(positive_receipts);
            final_plan->witness=bump_recovery_witness(*grant.owner_);
        });
        // Capture actual receiver completion before journal/guard bookkeeping.
        // Neither metadata write may silently rewrite its identity or counters.
        const auto completed_receiver=grant.receive_guard_?state.read(grant.attempt_.channel):std::nullopt;
        const auto completed_usage=grant.receive_guard_?std::optional<receive_install_usage>{state.usage()}:std::nullopt;
        std::optional<receive_guard_snapshot> completed_guard;
        if(result.installation->disposition==receive_install_disposition::installed) {
            require(before.has_value(),"canonical new installation lost its journal snapshot");
            // Actual receiver completion must precede journal settlement; both
            // still live inside the retained outer transaction, never two commits.
            const auto expected=journal.settle_install(grant.journal_,before->scope.revision,identity,positives);
            if(grant.receive_guard_)
                completed_guard=receive_delivery_guard_access::complete_canonical(*grant.owner_,writer,*grant.receive_guard_);
            same_stage();journal.audit();
            require(journal.read(grant.journal_.channel)==std::optional<recovery_obligation_scope>{expected},
                "canonical journal completion postimage changed");
            auto expected_usage=journal_usage;
            const auto old_size=before->scope.installed_manifest.size();
            const auto new_size=expected.installed_manifest.size();
            require(expected_usage.encoded_bytes>=static_cast<int64_t>(old_size),"canonical journal charge underflow");
            expected_usage.encoded_bytes-=static_cast<int64_t>(old_size);
            require(new_size<=static_cast<uint64_t>(INT64_MAX-expected_usage.encoded_bytes),"canonical journal charge overflow");
            expected_usage.encoded_bytes+=static_cast<int64_t>(new_size);
            require(journal.usage()==expected_usage,"canonical journal usage changed beyond settlement");
            for(const auto& positive:positives) {
                auto& entry=retained.at(canonical_writer_adapter::uuid_key(positive.original_id));
                entry.acknowledged=positive;entry.acknowledged->original_id=entry.canonical_original_id;
                entry.stage=recovery_obligation_stage::settled;entry.settled_install_sequence=identity.sequence;
            }
            for(const auto& [original,entry]:retained)
                require(journal.find(grant.journal_,original)==std::optional<recovery_obligation_entry>{entry},
                    "canonical journal final original postimage changed");
            require(final_plan.has_value(),"canonical final model plan missing");
            final_plan->verify(*grant.owner_,writer,limits,identities(recovery_identity_mode::uuid));
        }
        if(grant.receive_guard_) {
            require(completed_receiver && completed_receiver->binding==verified.installation_binding &&
                completed_receiver->last_installed==std::optional<receive_install_identity>{identity},
                "canonical guard completion lacks actual receiver installation");
            state.audit();
            require(state.read(grant.attempt_.channel)==completed_receiver && state.usage()==*completed_usage,
                "canonical guard completion changed receiver postimage");
            if(completed_guard)receive_delivery_guard_access::verify_owned(*grant.owner_,writer,*completed_guard);
            else receive_delivery_guard_access::verify_canonical_completed(*grant.owner_,writer,*grant.receive_guard_);
        }
    });
    if(result.transaction.state!=recovery_install_state::committed)result.installation.reset();
    return result;
}
scoped_recovery_result inspect_committed_canonical_range(const canonical_install_admission& grant,
    const receive_install_identity& identity,const canonical_range::request& request,
    const canonical_range::manifest& manifest) {
    scoped_recovery_result result;
    const auto& limits=grant.limits_.install;
    result.transaction=recovery_writer_access::install(grant.owner_,[&](database& writer) {
        validate_install_limits(limits);
        require(grant.journal_revision_>0 && !grant.coverage_id_.empty() &&
            grant.coverage_id_.size()<=limits.field_bytes,"canonical inspection lacks bound admission");
        require(grant.receive_guard_.has_value() && grant.receive_guard_->channel==grant.attempt_.channel &&
            grant.journal_.channel==grant.attempt_.channel && grant.profile_.binding.channel==grant.attempt_.channel,
            "canonical inspection channel or guard admission differs");
        // Q includes receiver/channel incarnations and attempt UUID. Validate
        // complete frozen framing before using stored digests as retry evidence.
        const auto described=describe_canonical_range(grant.attempt_,request,manifest,grant.limits_.codec,grant.route_);
        require(described.installation_binding==grant.profile_.binding && described.installation_identity==identity &&
            request.request_digest==grant.request_digest_ && manifest.manifest_digest==grant.manifest_digest_,
            "canonical inspection framing differs from exact retained receiver identity");
        // Audit existing stores only. A missing result must not initialize an
        // empty receiver/journal or recreate discarded staging as a side effect.
        receive_install_store state(grant.owner_,limits.installations);state.audit();
        const auto current=state.read(grant.attempt_.channel);
        require(current && current->binding==grant.profile_.binding && current->last_installed &&
            *current->last_installed==identity && identity.sequence>0 &&
            static_cast<uint64_t>(identity.sequence)==grant.attempt_.sequence &&
            identity.request_digest==grant.request_digest_ && identity.manifest_digest==grant.manifest_digest_,
            "canonical inspection lacks exact retained receiver identity");
        recovery_obligation_store journal(grant.owner_,grant.limits_.obligations,limits.installations);journal.audit();
        const auto scope=journal.read(grant.attempt_.channel);
        require(scope && scope->address==grant.journal_ && scope->profile==grant.profile_ &&
            scope->revision>=grant.journal_revision_ && scope->installed_sequence==identity.sequence &&
            scope->installed_revision==current->revision && scope->installed_head==identity.head &&
            scope->installed_manifest==identity.manifest_digest,
            "canonical inspection differs from current journal installation");
        const auto guard=receive_delivery_guard_access::read_owned(*grant.owner_,writer,grant.attempt_.channel);
        require(guard.present && !guard.legacy_origin && !guard.capacity_refused &&
            guard.state==receive_guard_state::canonical_installed,
            "canonical inspection lacks current modern installed guard");
        auto expected=*grant.receive_guard_;
        // Global counters can grow on unrelated channels between observation
        // and retry; the exact target incarnation/generation still must match.
        expected.store_incarnation=guard.store_incarnation;
        expected.store_channels=guard.store_channels;
        expected.store_channel_bytes=guard.store_channel_bytes;
        if(expected==guard)receive_delivery_guard_access::verify_owned(*grant.owner_,writer,guard);
        else receive_delivery_guard_access::verify_canonical_completed(*grant.owner_,writer,*grant.receive_guard_);
        result.installation=receive_install_receipt{receive_install_disposition::already_installed,current->revision,identity.head};
    });
    if(result.transaction.state!=recovery_install_state::committed)result.installation.reset();
    return result;
}
} // namespace lattice::detail
