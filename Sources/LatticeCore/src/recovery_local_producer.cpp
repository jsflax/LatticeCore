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
        candidate=std::make_shared<context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;candidate->previous=prior;
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
        candidate=std::make_shared<context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;candidate->previous=prior;
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
} // namespace lattice::detail
