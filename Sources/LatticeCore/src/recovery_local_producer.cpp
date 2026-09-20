#include "recovery_local_producer.hpp"
#include "vendor/picosha2/picosha2.h"
#include <array>
#include <cstring>
#include <map>
#include <set>
#include <utility>

namespace lattice::detail {
namespace recovery_local_producer_test_hooks {
thread_local const authorizer_fault* fault=nullptr;
thread_local void (*after_inventory)()=nullptr;
}
namespace {
using blob=std::vector<uint8_t>;
constexpr size_t max_models=16, max_relations=32, max_columns=32, max_sql=262144;
constexpr size_t max_manifest=1048576, max_receipt=65536, max_existing=4096;
// Independent discovery ceilings, never populated from stored limits. This
// private slice refuses larger enrolled profiles instead of unbounded opening.
constexpr recovery_obligation_producer_discovery_limits discovery_caps{
    {16,100000,4096,67108864},{16,4096,1048576},{16,100000,4096,1048576,67108864}};
constexpr const char* template_version="lattice-local-original-v1/uuid-nocase/schema-v1";
[[noreturn]] void refuse(const char* s) { throw db_error(s); }
bool identifier(const std::string& s) {
    if(s.empty() || s.size()>64 || (s[0]>='0'&&s[0]<='9')) return false;
    for(unsigned char c:s) if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_')) return false;
    return true;
}
std::string literal(const std::string& s) {
    static constexpr char h[]="0123456789abcdef"; std::string out="X'";
    for(unsigned char c:s) {out+=h[c>>4];out+=h[c&15];} return out+"'";
}
int64_t integer(const database::row_t& r,const char* n) {
    const auto i=r.find(n); if(i==r.end()||!std::holds_alternative<int64_t>(i->second)) refuse("local producer invalid integer descriptor");
    return std::get<int64_t>(i->second);
}
std::string text(const database::row_t& r,const char* n) {
    const auto i=r.find(n); if(i==r.end()||!std::holds_alternative<std::string>(i->second)) refuse("local producer invalid bounded descriptor");
    return std::get<std::string>(i->second);
}
bool uuid(const unsigned char* p,int n,char* out) noexcept {
    if(!p||n!=36) return false;
    for(int i=0;i<36;++i) {
        unsigned char c=p[i];
        if(i==8||i==13||i==18||i==23) {if(c!='-')return false;}
        else {if(c>='A'&&c<='F')c+=32;if(!((c>='a'&&c<='f')||(c>='0'&&c<='9')))return false;}
        out[i]=static_cast<char>(c);
    }
    return true;
}
void uuid_sql(sqlite3_context* c,int n,sqlite3_value** v) noexcept {
    char result[36];
    if(n!=1 || sqlite3_value_type(v[0])!=SQLITE_TEXT || sqlite3_value_bytes(v[0])!=36 ||
       !uuid(sqlite3_value_text(v[0]),36,result)) {sqlite3_result_null(c);return;}
    sqlite3_result_blob(c,result,36,SQLITE_TRANSIENT);
}
void append(blob& to,const void* bytes,size_t count) {
    if(to.size()>max_manifest || count>max_manifest-to.size())refuse("local producer manifest budget exceeded");
    if(count) {const auto* p=static_cast<const uint8_t*>(bytes);to.insert(to.end(),p,p+count);}
}
void number(blob& to,uint64_t n) {
    uint8_t b[8];for(int i=7;i>=0;--i){b[i]=static_cast<uint8_t>(n);n>>=8;}append(to,b,8);
}
void field(blob& to,const std::string& s) {number(to,s.size());append(to,s.data(),s.size());}
void field(blob& to,const blob& s) {number(to,s.size());append(to,s.data(),s.size());}
struct reader {
    const blob& bytes; size_t at=0;
    uint64_t number() {
        if(at>bytes.size() || 8>bytes.size()-at)refuse("local producer truncated manifest");
        uint64_t out=0;for(int i=0;i<8;++i)out=(out<<8)|bytes[at++];return out;
    }
    std::string field(size_t limit) {
        auto n=number();if(n>limit || n>bytes.size()-at)refuse("local producer invalid manifest field");
        std::string out(reinterpret_cast<const char*>(bytes.data()+at),static_cast<size_t>(n));at+=n;return out;
    }
};
recovery_local_producer_grant grant_from(const recovery_obligation_producer_profile& p) {
    if(p.grant_manifest.size()>max_manifest)refuse("local producer oversized stored grant");
    reader in{p.grant_manifest};
    if(in.field(128)!=template_version)refuse("local producer unsupported template revision");
    auto receipt=in.field(max_receipt); if(receipt.empty())refuse("local producer missing incoming grant claim");
    recovery_local_producer_grant out;out.address={p.contribution.binding.channel,p.contribution_incarnation,0};
    out.incoming_grant_receipt={receipt.begin(),receipt.end()};
    auto count=in.number();if(!count||count>max_models)refuse("local producer model inventory bound");
    for(size_t i=0;i<count;++i)out.models.push_back(in.field(64));
    // Remaining descriptors are regenerated from the same registered/durable
    // schema and compared as complete bytes, not trusted as parser authority.
    return out;
}
std::string normalize_sql(std::string s) {
    const std::string prefix="CREATE TRIGGER IF NOT EXISTS ";
    if(s.compare(0,prefix.size(),prefix)==0)s.replace(0,prefix.size(),"CREATE TRIGGER ");
    return s;
}
std::string program_name(const std::string& sql) {
    auto s=normalize_sql(sql);const std::string prefix="CREATE TRIGGER ";
    if(s.compare(0,prefix.size(),prefix)!=0)refuse("local producer unexpected generator program");
    auto e=s.find(' ',prefix.size());if(e==std::string::npos)refuse("local producer malformed program name");
    return s.substr(prefix.size(),e-prefix.size());
}
using programs=std::map<std::string,std::string>;
programs actual_programs(database& db,const std::string& table) {
    const auto budget=db.query("SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(name AS BLOB))+length(CAST(sql AS BLOB))),0) AS bytes FROM (SELECT name,sql FROM main.sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 17)",{table});
    if(integer(budget.at(0),"n")>16||integer(budget.at(0),"bytes")>static_cast<int64_t>(max_manifest))refuse("local producer trigger bytes exceed allocation bound");
    const auto rows=db.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 17",{table});
    if(rows.size()>16)refuse("local producer trigger inventory bound");
    programs out;for(const auto& r:rows)out.emplace(text(r,"name"),normalize_sql(text(r,"sql")));return out;
}
programs retention_programs() {
    // SQL-only guards protect connections opened before enrollment too. No
    // connection UDF or sync-disabled setting can bypass retained originals.
    const std::string predicate=" WHEN EXISTS(SELECT 1 FROM _lattice_obligation_producer_profile LIMIT 1) BEGIN SELECT RAISE(ABORT,'local producer durable audit retention fence'); END";
    const std::string remove="_lattice_local_producer_AuditLog_DELETE";
    const std::string update="_lattice_local_producer_AuditLog_UPDATE";
    const std::string insert="_lattice_local_producer_AuditLog_INSERT";
    std::string unchanged;
    for(const char* column:{"id","globalId","tableName","operation","rowId","globalRowId","changedFields","changedFieldsNames","isFromRemote","timestamp","synthesized"}) {
        if(!unchanged.empty())unchanged+=" AND ";
        const std::string name(column);
        // BINARY overrides globalId's NOCASE collation and compares full TEXT
        // lengths, including NUL. typeof prevents equal-value type changes.
        unchanged+="(typeof(OLD."+name+")=typeof(NEW."+name+") AND OLD."+name+" IS NEW."+name+" COLLATE BINARY)";
    }
    return {{remove,"CREATE TRIGGER "+remove+" BEFORE DELETE ON AuditLog"+predicate},
        {insert,"CREATE TRIGGER "+insert+" BEFORE INSERT ON AuditLog WHEN EXISTS(SELECT 1 FROM _lattice_obligation_producer_profile LIMIT 1) AND (EXISTS(SELECT 1 FROM AuditLog WHERE id=NEW.id) OR EXISTS(SELECT 1 FROM AuditLog WHERE globalId=NEW.globalId)) BEGIN SELECT RAISE(ABORT,'local producer durable audit retention fence'); END"},
        {update,"CREATE TRIGGER "+update+" BEFORE UPDATE ON AuditLog WHEN EXISTS(SELECT 1 FROM _lattice_obligation_producer_profile LIMIT 1) AND NOT ("+unchanged+") BEGIN SELECT RAISE(ABORT,'local producer durable audit retention fence'); END"}};
}
// This reads only fixed schema names and EXISTS, never stored payload bytes.
// Call under the actual writer transaction for maintenance mutation admission.
bool durable_producer_present(database& db) {
    const auto family=db.query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
    if(family.empty())return false;
    if(family.size()!=3)refuse("local producer incomplete durable family");
    return !db.query("SELECT 1 FROM main._lattice_obligation_producer_profile LIMIT 1").empty();
}
// Read-only discovery deliberately avoids database::query(): its settled drain
// may deliver the very legacy notification whose callback is asking this
// question. All returned values below are scalars or exact, bounded schema
// literals; no stored manifest, row payload, or unbounded metadata is copied.
struct discovery_statement {
    sqlite3_stmt* value=nullptr;
    explicit discovery_statement(sqlite3* db,const char* sql) {
        database::record_statement();
        if(sqlite3_prepare_v2(db,sql,-1,&value,nullptr)!=SQLITE_OK) {
            sqlite3_finalize(value);value=nullptr;
            refuse("export discovery could not prepare read-only metadata");
        }
        if(!value||!sqlite3_stmt_readonly(value)) {
            sqlite3_finalize(value);value=nullptr;
            refuse("export discovery requires read-only metadata statements");
        }
    }
    ~discovery_statement(){sqlite3_finalize(value);}
    discovery_statement(const discovery_statement&)=delete;
    discovery_statement& operator=(const discovery_statement&)=delete;
    bool row() {
        const int rc=sqlite3_step(value);
        if(rc==SQLITE_ROW)return true;
        if(rc!=SQLITE_DONE)refuse("export discovery could not read current metadata");
        return false;
    }
    bool text_is(int column,const char* expected) const {
        if(sqlite3_column_type(value,column)!=SQLITE_TEXT)return false;
        const auto size=std::strlen(expected);
        if(sqlite3_column_bytes(value,column)!=static_cast<int>(size))return false;
        const auto* bytes=sqlite3_column_text(value,column);
        return bytes&&std::memcmp(bytes,expected,size)==0;
    }
    int64_t number(int column) const {
        if(sqlite3_column_type(value,column)!=SQLITE_INTEGER)
            refuse("export discovery has malformed integer metadata");
        return sqlite3_column_int64(value,column);
    }
};
// This classifier recognizes exactly producer storage v1, without obtaining a
// storage writer capability. Keep these bounded read-only definitions equal to
// recovery_obligation_store.cpp's producer_definitions (SQLite omits main.).
constexpr std::pair<const char*,const char*> discovery_definitions[]={
 {"_lattice_obligation_producer_store","CREATE TABLE _lattice_obligation_producer_store(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,max_profiles INTEGER NOT NULL,max_stamps INTEGER NOT NULL,max_field INTEGER NOT NULL,max_manifest INTEGER NOT NULL,max_bytes INTEGER NOT NULL,profiles INTEGER NOT NULL,stamps INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID"},
 {"_lattice_obligation_producer_profile","CREATE TABLE _lattice_obligation_producer_profile(channel BLOB PRIMARY KEY,incarnation INTEGER NOT NULL UNIQUE,program_revision INTEGER NOT NULL,program_digest BLOB NOT NULL,manifest BLOB NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID"},
 {"_lattice_obligation_producer_stamp","CREATE TABLE _lattice_obligation_producer_stamp(channel BLOB NOT NULL,original BLOB NOT NULL,incarnation INTEGER NOT NULL,program_revision INTEGER NOT NULL,audit_id INTEGER NOT NULL,record_sequence INTEGER NOT NULL UNIQUE,generation INTEGER NOT NULL,scope_revision INTEGER NOT NULL,base_scopes INTEGER NOT NULL,base_records INTEGER NOT NULL,base_bytes INTEGER NOT NULL,base_incarnation INTEGER NOT NULL,base_export INTEGER NOT NULL,producer_profiles INTEGER NOT NULL,producer_stamps INTEGER NOT NULL,producer_bytes INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(channel,original),UNIQUE(channel,audit_id)) WITHOUT ROWID"}
};
bool discovery_family_present(sqlite3* db,bool admitted_profiles) {
    discovery_statement family(db,"SELECT type,name,sql FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
    unsigned seen=0;
    while(family.row()) {
        unsigned match=0;
        for(unsigned i=0;i<3;++i)if(family.text_is(1,discovery_definitions[i].first)) {
            match=1u<<i;
            if(!family.text_is(0,"table")||!family.text_is(2,discovery_definitions[i].second))
                refuse("export discovery producer schema differs");
            break;
        }
        if(!match||(seen&match))refuse("export discovery duplicate or unknown producer schema");
        seen|=match;
    }
    if(!seen) {
        if(admitted_profiles)refuse("export admitted producer family disappeared");
        return false;
    }
    if(seen!=7)refuse("export discovery incomplete durable producer family");
    discovery_statement config(db,"SELECT id,version,max_profiles,max_stamps,max_field,max_manifest,max_bytes,profiles,stamps,bytes FROM main._lattice_obligation_producer_store LIMIT 2");
    if(!config.row())refuse("export discovery missing producer metadata");
    std::array<int64_t,10> values{};
    for(int i=0;i<10;++i)values[i]=config.number(i);
    if(config.row()||values[0]!=1||values[1]!=1)
        refuse("export discovery unsupported producer metadata");
    const auto& cap=discovery_caps.producers;
    if(values[2]<0||values[2]>cap.profiles||values[3]<0||values[3]>cap.stamps||
       values[4]<=0||values[4]>cap.field_bytes||values[5]<0||values[5]>cap.manifest_bytes||
       values[6]<0||values[6]>cap.encoded_bytes||values[7]<0||values[7]>values[2]||
       values[8]<0||values[8]>values[3]||values[9]<0||values[9]>values[6])
        refuse("export discovery producer limits or counters are invalid");
    discovery_statement profiles(db,"SELECT COUNT(*) FROM (SELECT 1 FROM main._lattice_obligation_producer_profile LIMIT 17)");
    if(!profiles.row())refuse("export discovery missing profile count");
    const auto count=profiles.number(0);
    if(profiles.row()||count!=values[7]||count>cap.profiles)
        refuse("export discovery producer profile count differs");
    if(count)return true; // Classification only; strict preparation rereads admission.
    if(admitted_profiles)refuse("export admitted producer profiles disappeared");
    discovery_statement stamps(db,"SELECT 1 FROM main._lattice_obligation_producer_stamp LIMIT 1");
    if(values[8]!=0||values[9]!=0||stamps.row())
        refuse("export discovery dormant producer family is not empty");
    return false;
}
void validate_retention_programs(database& db,bool required) {
    const auto actual=actual_programs(db,"AuditLog");
    if(actual.empty()&&!required)return;
    if(actual!=retention_programs())refuse("local producer durable audit retention program mismatch");
}
std::string require(const std::string& condition) {
    return " SELECT CASE WHEN ("+condition+") THEN 1 ELSE RAISE(ABORT,'local producer phase/program refused') END;";
}
bool equal_blob(sqlite3_value* value,const std::string& expected) noexcept {
    return sqlite3_value_type(value)==SQLITE_BLOB && sqlite3_value_bytes(value)==static_cast<int>(expected.size()) &&
        (!expected.size() || (sqlite3_value_blob(value) && std::memcmp(sqlite3_value_blob(value),expected.data(),expected.size())==0));
}
void validate_limits(const recovery_obligation_producer_discovery_limits& x) {
    const auto fit=[](int64_t n,int64_t cap){return n>0&&n<=cap;};
    if(!fit(x.obligations.scopes,discovery_caps.obligations.scopes)||!fit(x.obligations.records,discovery_caps.obligations.records)||
       !fit(x.obligations.field_bytes,discovery_caps.obligations.field_bytes)||!fit(x.obligations.encoded_bytes,discovery_caps.obligations.encoded_bytes)||
       !fit(x.installations.channels,discovery_caps.installations.channels)||!fit(x.installations.field_bytes,discovery_caps.installations.field_bytes)||
       !fit(x.installations.encoded_bytes,discovery_caps.installations.encoded_bytes)||
       !fit(x.producers.profiles,discovery_caps.producers.profiles)||!fit(x.producers.stamps,discovery_caps.producers.stamps)||
       !fit(x.producers.field_bytes,discovery_caps.producers.field_bytes)||!fit(x.producers.manifest_bytes,discovery_caps.producers.manifest_bytes)||
       !fit(x.producers.encoded_bytes,discovery_caps.producers.encoded_bytes))refuse("local producer explicit limits exceed independent discovery caps");
}
bool same_ascii(const char* a,const char* b) noexcept {
    if(!a||!b)return false;
    for(;*a&&*b;++a,++b) {unsigned char x=*a,y=*b;if(x>='A'&&x<='Z')x+=32;if(y>='A'&&y<='Z')y+=32;if(x!=y)return false;}
    return *a==*b;
}
struct table_plan {
    std::string name,ddl; bool link=false;
    std::vector<std::pair<std::string,column_type>> columns;
    std::set<std::string> no_history;
    programs ordinary, enrolled;
};
}

struct recovery_local_producer_adapter::descriptor {
    blob manifest; std::string digest;
    std::map<std::string,table_plan> tables;
};
struct recovery_local_producer_adapter::management {
    sqlite3* connection; management* previous;
    explicit management(sqlite3* c):connection(c),previous(management_){management_=this;}
    ~management(){management_=previous;}
};
thread_local recovery_local_producer_adapter::management* recovery_local_producer_adapter::management_=nullptr;
struct recovery_local_producer_adapter::context {
    struct profile {recovery_obligation_producer_profile stored; descriptor schema;};
    sqlite3* connection=nullptr; lattice_db* owner=nullptr; // identity, never lifetime ownership
    std::shared_ptr<instance_guard> lifetime;
    recovery_obligation_producer_discovery_limits admitted_limits{};
    // Revocation is monotonic. Settlement only changes configuration status,
    // never writes true over a concurrent close/raw-escape revocation.
    std::shared_ptr<std::atomic<bool>> active=std::make_shared<std::atomic<bool>>(true);
    std::atomic<int> status{0}; // 0 pending, 1 committed, 2 rolled back
    std::shared_ptr<context> previous;
    std::vector<profile> profiles;
    const context* effective() const noexcept {
        const auto state=status.load(std::memory_order_acquire);
        if(state==2)return previous ? previous->effective() : nullptr;
        return this;
    }
    static void guard(sqlite3_context* sql,int n,sqlite3_value** v) noexcept {
        auto& root=**static_cast<std::shared_ptr<context>*>(sqlite3_user_data(sql));
        const auto* effective=root.effective();
        if(!effective || effective->status.load(std::memory_order_acquire)!=1){sqlite3_result_int(sql,0);return;}
        const auto& c=*effective;
        bool ok=n==6 && sqlite3_context_db_handle(sql)==c.connection &&
            sqlite3_value_type(v[1])==SQLITE_INTEGER && sqlite3_value_type(v[2])==SQLITE_INTEGER && sqlite3_value_type(v[5])==SQLITE_INTEGER;
        const auto phase=ok?sqlite3_value_int64(v[5]):0;
        const bool install=ok&&recovery_writer_access::active_install_for(c.owner,c.connection);
        ok=ok&&((phase==1 && !install && root.active->load(std::memory_order_acquire) && c.active->load(std::memory_order_acquire) && c.lifetime->alive.load(std::memory_order_acquire)) ||
                (phase==2 && install));
        bool found=false;
        for(const auto& p:c.profiles) {
            if(!ok || !equal_blob(v[0],p.stored.contribution.binding.channel) ||
               sqlite3_value_int64(v[1])!=p.stored.contribution_incarnation || sqlite3_value_int64(v[2])!=p.stored.program_revision ||
               !equal_blob(v[3],p.stored.program_digest))continue;
            for(const auto& pair:p.schema.tables)if(equal_blob(v[4],pair.first)){found=true;break;}
            if(found)break;
        }
        sqlite3_result_int(sql,found?1:0);
    }
    static int authorize(void* raw,int action,const char* one,const char* two,const char* schema,const char* origin) noexcept {
        auto& root=*static_cast<context*>(raw);
        const auto normal=[&]() noexcept -> int {
        for(auto* m=management_;m;m=m->previous)if(m->connection==root.connection)return SQLITE_OK;
        const auto* effective=root.effective();
        if(!effective || effective->profiles.empty())return SQLITE_OK;
        const auto& c=*effective;
        switch(action) {
        case SQLITE_ATTACH:case SQLITE_DETACH:case SQLITE_ALTER_TABLE:
        case SQLITE_CREATE_TABLE:case SQLITE_CREATE_TEMP_TABLE:case SQLITE_CREATE_TRIGGER:case SQLITE_CREATE_TEMP_TRIGGER:
        case SQLITE_DROP_TABLE:case SQLITE_DROP_TEMP_TABLE:case SQLITE_DROP_TRIGGER:case SQLITE_DROP_TEMP_TRIGGER:
        case SQLITE_CREATE_INDEX:case SQLITE_DROP_INDEX:case SQLITE_CREATE_TEMP_INDEX:case SQLITE_DROP_TEMP_INDEX:
        case SQLITE_CREATE_VIEW:case SQLITE_DROP_VIEW:case SQLITE_CREATE_TEMP_VIEW:case SQLITE_DROP_TEMP_VIEW:
        case SQLITE_CREATE_VTABLE:case SQLITE_DROP_VTABLE: return SQLITE_DENY;
        default:break;
        }
        if(action==SQLITE_PRAGMA && two && (same_ascii(one,"recursive_triggers")||same_ascii(one,"writable_schema")||same_ascii(one,"schema_version")))return SQLITE_DENY;
        // Retention/reset integration is not active. Preserve original bodies
        // conservatively across ALL channels instead of losing a pinned body
        // and discovering the damage during a later recovery read.
        if(one && same_ascii(one,"AuditLog") && (action==SQLITE_DELETE ||
           (action==SQLITE_UPDATE && !same_ascii(two,"isSynchronized"))))return SQLITE_DENY;
        if(one && action==SQLITE_DELETE && (same_ascii(one,"_lattice_sync_state") ||
           same_ascii(one,"_lattice_sync_set")||same_ascii(one,"_lattice_replication_slots")))return SQLITE_DENY;
        if(one && action==SQLITE_UPDATE && same_ascii(one,"_lattice_replication_slots") &&
           (same_ascii(two,"confirmed_audit_id")||same_ascii(two,"upload_floor")))return SQLITE_DENY;
        if((action==SQLITE_INSERT||action==SQLITE_UPDATE||action==SQLITE_DELETE) && one &&
           std::strncmp(one,"_lattice_obligation_producer_",sizeof("_lattice_obligation_producer_")-1)==0) {
            if(!schema||std::strcmp(schema,"main")||!origin)return SQLITE_DENY;
            if(std::strcmp(one,"_lattice_obligation_producer_profile")==0)return SQLITE_DENY;
            for(const auto& p:c.profiles)for(const auto& t:p.schema.tables)
                for(const auto& program:t.second.ordinary)if(program.first==origin)return SQLITE_OK;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
        };
        const int admission=normal();
        if(admission!=SQLITE_OK)return admission;
        const auto* fault=recovery_local_producer_test_hooks::fault;
        if(fault && fault->owner==root.owner && fault->restrict_action) {
            const int value=fault->restrict_action(action,one,two,origin);
            if(value==SQLITE_DENY||value==SQLITE_IGNORE)return value;
        }
        return SQLITE_OK;
    }
};

std::shared_ptr<database> recovery_local_producer_adapter::retained_writer_for_test(lattice_db& owner) {
    std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);return owner.db_;
}
recovery_local_producer_adapter::descriptor recovery_local_producer_adapter::describe(
    lattice_db& owner,database& db,const recovery_local_producer_grant& grant,bool initial_inventory) {
    if(grant.models.empty()||grant.models.size()>max_models||grant.incoming_grant_receipt.empty()||grant.incoming_grant_receipt.size()>max_receipt)
        refuse("local producer requires bounded explicit whole-model incoming grant");
    for(const auto& name:grant.models)if(!identifier(name)||name[0]=='_')refuse("local producer unsupported model identifier");
    const std::set<std::string> models(grant.models.begin(),grant.models.end());
    if(models.size()!=grant.models.size())refuse("local producer duplicate model grant");
    descriptor d;
    field(d.manifest,std::string(template_version));field(d.manifest,grant.incoming_grant_receipt);number(d.manifest,models.size());
    for(const auto& name:models) {
        if(!identifier(name)||name[0]=='_')refuse("local producer unsupported model identifier");field(d.manifest,name);
        auto* schema=schema_registry::instance().get_schema(name);
        if(!schema || schema->properties.empty() || schema->properties.size()>max_columns)refuse("local producer unknown/oversized complete schema");
        table_plan t;t.name=name;
        for(const auto& p:schema->properties) {
            if(!identifier(p.name)||p.is_vector||p.is_geo_bounds||p.is_full_text||p.is_union||
               (!p.column_name.empty()&&p.column_name!=p.name)||
               (p.kind!=property_kind::primitive&&p.kind!=property_kind::link&&p.kind!=property_kind::list))
                refuse("local producer unsupported complete schema property");
            if(p.kind!=property_kind::list)t.columns.emplace_back(p.name,p.type);
            if(p.no_history)t.no_history.insert(p.name);
        }
        if(t.columns.empty())refuse("local producer model has no generated audit program");
        d.tables.emplace(name,std::move(t));
    }
    const auto schemas=schema_registry::instance().all_schemas();
    if(schemas.size()>256)refuse("local producer registered schema inventory bound");
    for(const auto* s:schemas)for(const auto& p:s->properties) {
        if(p.kind!=property_kind::link&&p.kind!=property_kind::list)continue;
        if(!models.count(s->table_name)&&!models.count(p.target_table))continue;
        if(p.is_geo_bounds||p.target_table.empty()||!models.count(s->table_name)||!models.count(p.target_table))
            refuse("local producer incomplete regular-link incoming/outgoing closure");
        table_plan t;t.name="_"+s->table_name+"_"+p.target_table+"_"+p.name;t.link=true;
        if(!identifier(t.name))refuse("local producer oversized relation");d.tables.emplace(t.name,std::move(t));
    }
    if(d.tables.size()>max_relations)refuse("local producer physical relation budget");
    // Fixed retention-program bytes participate in the descriptor digest;
    // their trigger names/definitions are independently checked at admission.
    for(const auto& [name,sql]:retention_programs()){field(d.manifest,name);field(d.manifest,sql);}
    size_t existing=0;
    for(auto& [name,t]:d.tables) {
        auto ddl=db.query("SELECT CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='table' AND name=?",{name});
        if(ddl.size()!=1)refuse("local producer missing durable model/link relation");t.ddl=text(ddl[0],"sql");
        if(t.ddl.find("globalId TEXT UNIQUE COLLATE NOCASE")==std::string::npos || t.ddl.find("CREATE VIRTUAL")!=std::string::npos || t.ddl.find("WITHOUT ROWID")!=std::string::npos)
            refuse("local producer unsupported durable UUID relation");
        const auto columns=db.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END AS name,CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END AS type,CASE WHEN typeof(hidden)='integer' THEN hidden END AS hidden FROM pragma_table_xinfo(?) LIMIT 35",{name});
        if(columns.size()>34)refuse("local producer durable column count");
        std::map<std::string,std::string> actual,want;
        for(const auto& c:columns) {if(integer(c,"hidden")!=0)refuse("local producer hidden/generated columns");actual.emplace(text(c,"name"),text(c,"type"));}
        if(t.link)want={{"lhs","TEXT"},{"rhs","TEXT"},{"globalId","TEXT"}};
        else {
            want={{"id","INTEGER"},{"globalId","TEXT"}};
            for(const auto& [column,type]:t.columns)want.emplace(column,type==column_type::integer?"INTEGER":type==column_type::real?"REAL":type==column_type::blob?"BLOB":"TEXT");
        }
        if(actual!=want)refuse("local producer durable/registered schema disagreement");
        // Only first enrollment scans the bounded initial identity inventory.
        // Admitted programs guard later UUID writes. Reconstructing their exact
        // schema/program descriptor must not add a lower row-growth ceiling.
        // Arbitrary external/raw schema or UDF tampering is unsupported.
        if(initial_inventory) {
            const auto rows=db.query("SELECT CASE WHEN typeof(globalId)='text' AND length(CAST(globalId AS BLOB))=36 THEN globalId END AS gid FROM main."+name+" LIMIT "+std::to_string(max_existing-existing+1));
            if(rows.size()>max_existing-existing)refuse("local producer initial UUID inventory bound");existing+=rows.size();
            for(const auto& row:rows) {auto id=text(row,"gid");char out[36];if(!uuid(reinterpret_cast<const unsigned char*>(id.data()),id.size(),out))refuse("local producer non-UUID existing identity");}
        }
        std::vector<std::string> sql;
        if(t.link)owner.create_link_table_triggers(name,{},&sql);
        else owner.create_model_table_triggers(name,t.columns,t.no_history,{},&sql);
        for(const auto& s:sql)t.ordinary.emplace(program_name(s),normalize_sql(s));
        field(d.manifest,name);number(d.manifest,t.link?1:0);field(d.manifest,t.ddl);
        for(const auto& [n,s]:t.ordinary){field(d.manifest,n);field(d.manifest,s);}
        const auto index_budget=db.query("SELECT COUNT(*) AS n,COALESCE(SUM(length(CAST(name AS BLOB))+COALESCE(length(CAST(sql AS BLOB)),0)),0) AS bytes FROM (SELECT name,sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? LIMIT 33)",{name});
        if(integer(index_budget.at(0),"n")>32||integer(index_budget.at(0),"bytes")>static_cast<int64_t>(max_manifest-d.manifest.size()))refuse("local producer index bytes exceed allocation bound");
        const auto indexes=db.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN sql IS NULL THEN '' WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? ORDER BY name LIMIT 33",{name});
        if(indexes.size()>32)refuse("local producer index inventory bound");number(d.manifest,indexes.size());
        for(const auto& i:indexes){field(d.manifest,text(i,"name"));field(d.manifest,text(i,"sql"));}
    }
    d.digest=picosha2::hash256_hex_string(d.manifest);return d;
}

void recovery_local_producer_adapter::validate_custody(lattice_db& owner,database& writer) {
    if(owner.config_.read_only||owner.config_.is_sync_enabled()||owner.config_.is_ipc_enabled()||owner.config_.audit_retention_seconds!=0)
        refuse("local producer qualification refuses legacy transports/automatic retention/read-only owner");
    if(writer.raw_handle_escaped_.load(std::memory_order_acquire))refuse("local producer raw handle escaped; engine hook custody unavailable");
    if(writer.canonical_callback_custody_ || writer.table_exists("_lattice_canonical_coverage"))
        refuse("local producer source-canonical coattachment is unqualified");
    const auto dbs=writer.query("SELECT name FROM pragma_database_list LIMIT 3");
    if(dbs.size()>2)refuse("local producer attached databases are unqualified");
    for(const auto& r:dbs)if(text(r,"name")!="main"&&text(r,"name")!="temp")refuse("local producer attached databases are unqualified");
    if(!writer.query("SELECT 1 FROM temp.sqlite_master LIMIT 1").empty())refuse("local producer temporary schema is unqualified");
    validate_retention_programs(writer,durable_producer_present(writer));
    if(!writer.query("SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND substr(tbl_name,1,20)='_lattice_obligation_' LIMIT 1").empty())
        refuse("local producer obligation metadata triggers are unqualified");
}
void recovery_local_producer_adapter::register_context(database& writer,const std::shared_ptr<context>& c) {
    auto* held=new std::shared_ptr<context>(c);
    if(sqlite3_create_function_v2(c->connection,"lattice_recovery_producer_guard_v1",6,SQLITE_UTF8,held,context::guard,nullptr,nullptr,
        [](void* p){delete static_cast<std::shared_ptr<context>*>(p);})!=SQLITE_OK)refuse("local producer guard registration failed");
    if(sqlite3_create_function_v2(c->connection,"lattice_recovery_producer_uuid_v1",1,SQLITE_UTF8|SQLITE_DETERMINISTIC,nullptr,uuid_sql,nullptr,nullptr,nullptr)!=SQLITE_OK)
        refuse("local producer UUID registration failed");
    // Authorizer userdata has the same physical lifetime as the registered UDF;
    // database close/replacement revokes admission before releasing its copy.
    std::atomic_store(&writer.local_producer_callback_custody_,std::static_pointer_cast<void>(c));
    std::atomic_store(&writer.local_producer_write_allowed_,c->active);
    if(sqlite3_set_authorizer(c->connection,context::authorize,c.get())!=SQLITE_OK)refuse("local producer authorizer registration failed");
}

void recovery_local_producer_adapter::compile_programs(lattice_db& owner,descriptor& d,
    const recovery_obligation_producer_program& program) {
    const auto& p=program.profile();size_t generated=0;
    for(auto& [name,t]:d.tables) {
        const auto tail=program.emit_tail(name,t.link,max_sql);
        if(tail.size()>max_sql)refuse("local producer tail exceeds independent SQL budget");
        for(const auto& entry:t.ordinary)if(entry.second.size()>max_sql-tail.size())refuse("local producer complete program bound");
        std::vector<std::string> emitted;
        if(t.link)owner.create_link_table_triggers(name,tail,&emitted);
        else owner.create_model_table_triggers(name,t.columns,t.no_history,tail,&emitted);
        const auto guard=[&](int phase) {
            return "lattice_recovery_producer_guard_v1("+literal(p.contribution.binding.channel)+","+
                std::to_string(p.contribution_incarnation)+","+std::to_string(p.program_revision)+","+
                literal(p.program_digest)+","+literal(name)+","+std::to_string(phase)+")=1";
        };
        for(const auto* operation:{"INSERT","UPDATE","DELETE"}) {
            const std::string op(operation), key=op=="DELETE"?"OLD.globalId":"NEW.globalId";
            std::string check="((sync_disabled()=0 AND "+guard(1)+") OR (sync_disabled()=1 AND "+guard(2)+"))";
            check+=" AND lattice_recovery_producer_uuid_v1("+key+") IS NOT NULL";
            if(op=="UPDATE") {
                check+=" AND CAST(OLD.globalId AS BLOB) IS CAST(NEW.globalId AS BLOB)";
                if(t.link)check+=" AND ("+guard(2)+" OR (OLD.lhs IS NEW.lhs AND OLD.rhs IS NEW.rhs))";
                else check+=" AND OLD.id IS NEW.id";
            }
            emitted.push_back("CREATE TRIGGER _lattice_local_producer_"+name+"_"+op+" BEFORE "+op+" ON "+name+" BEGIN"+require(check)+" END");
        }
        for(auto& sql:emitted) {
            if(sql.size()>max_sql || generated>max_manifest || sql.size()>max_manifest-generated)refuse("local producer generated program aggregate bound");
            generated+=sql.size();const auto name=program_name(sql);t.enrolled.emplace(name,normalize_sql(std::move(sql)));
        }
    }
}

recovery_install_result recovery_local_producer_adapter::enroll_for_qualification(
    std::shared_ptr<lattice_db> owner,const recovery_local_producer_grant& grant,
    const recovery_obligation_producer_discovery_limits& limits) {
    std::shared_ptr<context> candidate;
    auto result=recovery_writer_access::install_impl(owner,[&](database& db) {
        validate_limits(limits);validate_custody(*owner,db);
        management changing(db.internal_handle());
        auto prior=std::static_pointer_cast<context>(std::atomic_load(&db.local_producer_callback_custody_));
        while(prior && prior->status.load(std::memory_order_acquire)==2)prior=prior->previous;
        candidate=std::make_shared<context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;candidate->previous=prior;candidate->admitted_limits=limits;
        recovery_obligation_store obligations(owner,limits.obligations,limits.installations);
        obligations.audit();const auto scope=obligations.read(grant.address.channel);
        if(!scope || scope->address!=grant.address)refuse("local producer stale or unbound contribution address");
        recovery_obligation_producer_store storage(owner,limits.obligations,limits.installations,limits.producers);
        storage.initialize();
        auto schema=describe(*owner,db,grant,true);
        recovery_obligation_producer_profile profile{scope->profile,scope->address.incarnation,1,schema.digest,schema.manifest};
        const auto existing=storage.profiles();
        for(const auto& p:existing) {
            if(p.contribution.binding.channel==profile.contribution.binding.channel)refuse("local producer contribution already enrolled");
            auto old=describe(*owner,db,grant_from(p),false);
            if(old.manifest!=p.grant_manifest||old.digest!=p.program_digest||p.program_revision!=1)refuse("local producer changed existing grant/schema");
            compile_programs(*owner,old,recovery_obligation_producer_store::compile(p,limits.obligations,limits.producers));
            for(const auto& [name,t]:old.tables) {
                if(schema.tables.count(name))refuse("local producer overlapping whole-model contribution grants");
                if(actual_programs(db,name)!=t.enrolled)refuse("local producer changed existing generated program");
            }
            candidate->profiles.push_back({p,std::move(old)});
        }
        for(const auto& [name,t]:schema.tables)if(actual_programs(db,name)!=t.ordinary)refuse("local producer missing/extra/non-generated pre-enrollment trigger");
        const auto program=storage.enroll(profile);
        compile_programs(*owner,schema,program);
        candidate->profiles.push_back({profile,std::move(schema)});
        // Non-writing guards installed before persistent programs. Pending
        // configuration cannot authorize effects until the owned COMMIT.
        register_context(db,candidate);
        db.execute("PRAGMA recursive_triggers=ON");
        if(integer(db.query("PRAGMA recursive_triggers").at(0),"recursive_triggers")!=1)refuse("local producer REPLACE qualification requires recursive triggers");
        if(actual_programs(db,"AuditLog").empty())for(const auto& [name,sql]:retention_programs())db.execute(sql);
        validate_retention_programs(db,true);
        for(const auto& [name,t]:candidate->profiles.back().schema.tables) {
            for(const auto& old:t.ordinary)db.execute("DROP TRIGGER "+old.first);
            for(const auto& installed:t.enrolled)db.execute(installed.second);
        }
        owner->store_fingerprint_marker(owner->compute_core_fingerprint_key());
    },[&] {
        // Monotonic revocation flag is untouched. A concurrent logical close
        // still fences phase 1; pre-admitted phase 2 uses its retained frame.
        candidate->status.store(1,std::memory_order_release);candidate->previous.reset();
    });
    if(candidate && result.state!=recovery_install_state::committed)candidate->status.store(2,std::memory_order_release);
    return result;
}

recovery_install_result recovery_local_producer_adapter::retire_for_qualification(
    std::shared_ptr<lattice_db> owner,const recovery_obligation_address& address,
    const recovery_obligation_producer_discovery_limits& limits) {
    std::shared_ptr<context> candidate;
    auto result=recovery_writer_access::install_impl(owner,[&](database& db) {
        validate_limits(limits);validate_custody(*owner,db);management changing(db.internal_handle());
        auto prior=std::static_pointer_cast<context>(std::atomic_load(&db.local_producer_callback_custody_));
        while(prior && prior->status.load(std::memory_order_acquire)==2)prior=prior->previous;
        if(!prior || prior->status.load(std::memory_order_acquire)!=1)refuse("local producer retirement requires admitted connection");
        candidate=std::make_shared<context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;candidate->previous=prior;candidate->admitted_limits=limits;
        const context::profile* removed=nullptr;
        for(const auto& p:prior->profiles) {
            for(const auto& [name,t]:p.schema.tables)if(actual_programs(db,name)!=t.enrolled)refuse("local producer retirement program mismatch");
            if(p.stored.contribution.binding.channel==address.channel)removed=&p;
            else candidate->profiles.push_back(p);
        }
        if(!removed||removed->stored.contribution_incarnation!=address.incarnation)refuse("local producer retirement incarnation mismatch");
        recovery_obligation_producer_store storage(owner,limits.obligations,limits.installations,limits.producers);
        storage.retire_contribution(address,removed->stored);
        register_context(db,candidate);
        for(const auto& [name,t]:removed->schema.tables) {
            for(const auto& installed:t.enrolled)db.execute("DROP TRIGGER "+installed.first);
            for(const auto& ordinary:t.ordinary)db.execute(ordinary.second);
        }
        owner->store_fingerprint_marker(owner->compute_core_fingerprint_key());
    },[&]{candidate->status.store(1,std::memory_order_release);candidate->previous.reset();});
    if(candidate && result.state!=recovery_install_state::committed)candidate->status.store(2,std::memory_order_release);
    return result;
}

std::shared_ptr<recovery_local_producer_adapter::context> recovery_local_producer_adapter::bootstrap(
    lattice_db& owner,const std::shared_ptr<database>& writer) {
    std::shared_ptr<context> c;
    recovery_obligation_producer_store::bootstrap_profiles(writer,discovery_caps,
        [&](database& view,const recovery_obligation_producer_inventory& inventory) {
            if(!inventory.initialized)return;
            // Storage-only initialization can predate first enrollment. Empty
            // families allow absent guards or the exact retained dormant bundle;
            // partial/replaced programs always refuse, active profiles need all.
            validate_retention_programs(view,!inventory.profiles.empty());
            if(inventory.profiles.empty())return;
            validate_custody(owner,view);
            if(recovery_local_producer_test_hooks::after_inventory)recovery_local_producer_test_hooks::after_inventory();
            c=std::make_shared<context>();c->connection=view.internal_handle();c->owner=&owner;c->lifetime=owner.guard_;
            c->admitted_limits={inventory.stored_obligation_limits,inventory.stored_installation_limits,inventory.stored_producer_limits};
            std::set<std::string> relations;
            for(const auto& p:inventory.profiles) {
                auto d=describe(owner,view,grant_from(p),false);
                if(p.program_revision!=1||d.manifest!=p.grant_manifest||d.digest!=p.program_digest)refuse("local producer bootstrap descriptor/program revision mismatch");
                compile_programs(owner,d,recovery_obligation_producer_store::compile(p,inventory.stored_obligation_limits,inventory.stored_producer_limits));
                for(const auto& [name,t]:d.tables) {
                    if(!relations.insert(name).second)refuse("local producer bootstrap overlapping grants");
                    if(actual_programs(view,name)!=t.enrolled)refuse("local producer bootstrap generated program mismatch");
                }
                c->profiles.push_back({p,std::move(d)});
            }
        });
    if(!c)return {};
    // Inventory, descriptor and exact trigger bytes were read in ONE owned
    // snapshot. A later sibling retirement is still a separate lifetime event;
    // final owned profile discovery rereads storage and refuses stale custody.
    database::maintenance_scope maintenance(*writer);
    register_context(*writer,c);
    {management changing(c->connection);writer->execute("PRAGMA recursive_triggers=ON");}
    if(integer(writer->query("PRAGMA recursive_triggers").at(0),"recursive_triggers")!=1)refuse("local producer recursive-trigger setup failed");
    return c;
}

void recovery_local_producer_adapter::publish(lattice_db& owner,database& db) noexcept {
    auto c=std::static_pointer_cast<context>(std::atomic_load(&db.local_producer_callback_custody_));
    if(!c)return;
    const auto* hook=db.lattice_update_hook_context_.get();
    if(c->owner==&owner && c->connection==db.internal_handle() && hook && hook->owner==&owner && hook->connection==c->connection &&
       owner.guard_->alive.load(std::memory_order_acquire) && !db.raw_handle_escaped_.load(std::memory_order_acquire))
        c->status.store(1,std::memory_order_release);
    else c->active->store(false,std::memory_order_release);
}
bool prepare_recovery_local_producer(lattice_db& owner,const std::shared_ptr<database>& writer) {
    return static_cast<bool>(recovery_local_producer_adapter::bootstrap(owner,writer));
}
void publish_recovery_local_producer(lattice_db& owner,database& writer) noexcept {
    recovery_local_producer_adapter::publish(owner,writer);
}
bool preserve_recovery_local_producer_relation(database& db,const std::string& name) {
    const auto c=std::static_pointer_cast<recovery_local_producer_adapter::context>(std::atomic_load(&db.local_producer_callback_custody_));
    if(!c)return false;
    const auto* effective=c->effective();if(!effective)return false;
    for(const auto& p:effective->profiles)if(p.schema.tables.count(name)) {
        if(!c->active->load(std::memory_order_acquire))refuse("local producer relation admission revoked");
        return true;
    }
    return false;
}
void require_recovery_local_producer_maintenance_absent(database& db) {
    const auto c=std::static_pointer_cast<recovery_local_producer_adapter::context>(std::atomic_load(&db.local_producer_callback_custody_));
    const auto* effective=c?c->effective():nullptr;
    if((effective && !effective->profiles.empty())||durable_producer_present(db))refuse("local producer retention/history/reset integration is not active");
}
std::vector<recovery_obligation_producer_profile> recovery_local_producer_adapter::profiles_for_owned_write(
    std::shared_ptr<lattice_db> owner,const recovery_obligation_producer_discovery_limits& limits) {
    validate_limits(limits);
    if(!owner)refuse("local producer discovery requires retained owner");
    auto* writer=recovery_writer_access::active_writer(*owner);
    if(!writer)refuse("local producer discovery requires actual owned WRITE");
    const auto c=std::static_pointer_cast<context>(std::atomic_load(&writer->local_producer_callback_custody_));
    // An owner may have opened BEFORE a sibling enrolled. Missing local
    // callback custody is not proof of durable profile absence.
    const auto family=writer->query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
    if(family.empty()) {
        if(c && c->effective() && !c->effective()->profiles.empty())refuse("local producer admitted family disappeared");
        return {};
    }
    if(family.size()!=3)refuse("local producer discovery found incomplete durable family");
    recovery_obligation_producer_store storage(owner,limits.obligations,limits.installations,limits.producers);
    auto stored=storage.profiles();
    if(!c) {
        if(!stored.empty())refuse("local producer durable profiles lack physical admission");
        return {};
    }
    const auto* effective=c->effective();
    if(!effective || effective->status.load(std::memory_order_acquire)!=1 || !c->active->load(std::memory_order_acquire) ||
       effective->owner!=owner.get()||effective->connection!=writer->internal_handle()||owner->is_closed()||
       writer->raw_handle_escaped_.load(std::memory_order_acquire))refuse("local producer discovery lacks current physical admission");
    if(stored.size()!=effective->profiles.size())refuse("local producer stored/admitted inventory mismatch");
    for(const auto& p:stored) {
        bool found=false;for(const auto& admitted:effective->profiles)if(p==admitted.stored){found=true;break;}
        if(!found)refuse("local producer stored/admitted profile mismatch");
    }
    return stored;
}
bool recovery_local_producer_adapter::export_protection_required(std::shared_ptr<lattice_db> owner) {
    if(!owner)refuse("export discovery requires a retained owner");
    std::shared_ptr<database> writer;
    {
        std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
        if(owner->closed_.load())refuse("export discovery owner is closed");
        writer=owner->db_;
    }
    if(!writer)refuse("export discovery has no published writer");
    auto* db=writer->internal_handle();auto* mutex=db?sqlite3_db_mutex(db):nullptr;
#ifndef __EMSCRIPTEN__
    if(!mutex)refuse("export discovery requires a serialized connection");
#endif
    if(sqlite3_mutex_try(mutex)!=SQLITE_OK)refuse("export discovery writer is busy");
    struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
    const auto validate_owner=[&] {
        const auto* hook=writer->lattice_update_hook_context_.get();
        std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
        if(!db||writer->is_closed()||owner->closed_.load()||owner->db_!=writer||
           !hook||hook->owner!=owner.get()||hook->connection!=db||
           writer->channel_reset_unsettled_.load(std::memory_order_acquire)||
           database::update_hook_scope::active_for(db))
            refuse("export discovery captured writer is unavailable");
    };
    validate_owner();
    // A preexisting read snapshot may predate sibling enrollment. Only an
    // idle connection or the actual current owned WRITE can classify absence.
    const bool idle=sqlite3_get_autocommit(db)!=0&&sqlite3_txn_state(db,"main")==SQLITE_TXN_NONE;
    if(!idle&&(sqlite3_txn_state(db,"main")!=SQLITE_TXN_WRITE||
               recovery_writer_access::active_writer(*owner)!=writer.get()))
        refuse("export discovery requires a fresh view or actual owned WRITE");
    const auto root=std::static_pointer_cast<context>(std::atomic_load(&writer->local_producer_callback_custody_));
    const auto* admitted=root?root->effective():nullptr;
    const bool has_admitted=admitted&&!admitted->profiles.empty();
    // Leave this VM at ROW until every dependent metadata read has finished.
    // It pins one fresh main snapshot without beginning/settling a transaction
    // or entering the notification-draining database query funnel.
    discovery_statement anchor(db,"SELECT 1 FROM main.sqlite_schema LIMIT 1");
    const bool schema_present=anchor.row();
    if(!schema_present&&has_admitted)refuse("export admitted producer schema disappeared");
    const bool result=schema_present&&discovery_family_present(db,has_admitted);
    validate_owner();
    return result;
}
recovery_local_export_inventory recovery_local_producer_adapter::export_inventory_for_owned_write(std::shared_ptr<lattice_db> owner) {
    if(!owner)refuse("export inventory requires retained owner");
    auto* writer=recovery_writer_access::active_writer(*owner);
    if(!writer)refuse("export inventory requires actual owned WRITE");
    const auto context_root=std::static_pointer_cast<context>(std::atomic_load(&writer->local_producer_callback_custody_));
    const auto* admitted=context_root?context_root->effective():nullptr;
    const auto family=writer->query("SELECT name FROM main.sqlite_schema WHERE name IN ('_lattice_obligation_producer_store','_lattice_obligation_producer_profile','_lattice_obligation_producer_stamp') LIMIT 4");
    if(family.empty()) {
        if(admitted&&!admitted->profiles.empty())refuse("export admitted producer family disappeared");
        return {};
    }
    if(family.size()!=3)refuse("export incomplete durable producer family");
    const auto count=writer->query("SELECT COUNT(*) AS n FROM (SELECT 1 FROM main._lattice_obligation_producer_profile LIMIT 17)");
    const auto n=integer(count.at(0),"n");
    if(n<0||n>16)refuse("export producer inventory bound");
    if(n==0 && (!admitted||admitted->profiles.empty()))return {};
    if(!admitted || admitted->status.load(std::memory_order_acquire)!=1 ||
       !context_root->active->load(std::memory_order_acquire)||!admitted->active->load(std::memory_order_acquire)||
       !admitted->lifetime->alive.load(std::memory_order_acquire)||owner->is_closed()||
       admitted->owner!=owner.get()||admitted->connection!=writer->internal_handle()||
       writer->raw_handle_escaped_.load(std::memory_order_acquire)||static_cast<size_t>(n)!=admitted->profiles.size())
        refuse("export producer inventory lacks current physical admission");
    validate_limits(admitted->admitted_limits);
    recovery_local_export_inventory result;result.limits=admitted->admitted_limits;
    recovery_obligation_store journal(owner,result.limits.obligations,result.limits.installations);
    for(const auto& profile:admitted->profiles) {
        const auto& p=profile.stored;
        // Full manifest/program validation belongs to enrollment/bootstrap.
        // Engine-owned profile mutation is forbidden while admitted. This hot
        // addressed check deliberately neither copies nor compares its bytes.
        const auto rows=writer->query("SELECT 1 AS ok FROM main._lattice_obligation_producer_profile WHERE channel=? AND typeof(channel)='blob' AND typeof(incarnation)='integer' AND incarnation=? AND typeof(program_revision)='integer' AND program_revision=? AND typeof(program_digest)='blob' AND program_digest=? AND typeof(manifest)='blob' AND length(manifest)=? LIMIT 2",
            {blob(p.contribution.binding.channel.begin(),p.contribution.binding.channel.end()),p.contribution_incarnation,p.program_revision,
             blob(p.program_digest.begin(),p.program_digest.end()),static_cast<int64_t>(p.grant_manifest.size())});
        const auto scope=journal.read(p.contribution.binding.channel);
        if(rows.size()!=1||!scope||scope->profile!=p.contribution||scope->address.incarnation!=p.contribution_incarnation)
            refuse("export durable producer binding changed");
        recovery_local_export_scope item;item.contribution=*scope;item.program_revision=p.program_revision;item.program_digest=p.program_digest;
        for(const auto& [name,table]:profile.schema.tables)
            item.tables.push_back({name,table.columns,table.no_history,table.link});
        result.scopes.push_back(std::move(item));
    }
    return result;
}
} // namespace lattice::detail
