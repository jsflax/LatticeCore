#include "canonical_writer_adapter.hpp"
#include "recovery_writer_access.hpp"
#include "vendor/picosha2/picosha2.h"
#include <atomic>
#include <array>
#include <cstring>
#include <map>
#include <set>
#include <utility>
#include <cmath>
#include <limits>
#include <filesystem>
#include <cerrno>
#if defined(__APPLE__) || defined(__linux__)
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace lattice::detail {
namespace {
constexpr size_t max_tables=16, max_columns=32, max_existing_rows=4096, max_sql=262144;
using blob=std::vector<uint8_t>;
[[noreturn]] void refuse(const char* why) { throw db_error(why); }
blob bytes(const std::string& s) { return {s.begin(),s.end()}; }
bool identifier(const std::string& s) {
    if(s.empty() || s.size()>64) return false;
    for(unsigned char c:s) if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_')) return false;
    return !(s[0]>='0'&&s[0]<='9');
}
std::string literal(const std::string& s) { // bounded opaque bytes, no SQL interpolation
    static constexpr char hex[]="0123456789abcdef";
    std::string out="X'"; for(unsigned char c:s) {out+=hex[c>>4];out+=hex[c&15];} return out+"'";
}
bool uuid(const unsigned char* p,int n,char* out) noexcept {
    if(!p || n!=36) return false;
    for(int i=0;i<36;++i) {
        unsigned char c=p[i];
        if(i==8||i==13||i==18||i==23) {if(c!='-')return false;}
        else {if(c>='A'&&c<='F')c=static_cast<unsigned char>(c+32); if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')))return false;}
        out[i]=static_cast<char>(c);
    }
    return true;
}
void uuid_sql(sqlite3_context* ctx,int,sqlite3_value** args) noexcept {
    char out[36];
    if(sqlite3_value_type(args[0])!=SQLITE_TEXT || sqlite3_value_bytes(args[0])!=36 ||
       !uuid(sqlite3_value_text(args[0]),sqlite3_value_bytes(args[0]),out)) {
        sqlite3_result_null(ctx); return;
    }
    sqlite3_result_blob(ctx,out,36,SQLITE_TRANSIENT);
}
int64_t integer(const database::row_t& r,const char* k) {
    auto it=r.find(k);if(it==r.end()||!std::holds_alternative<int64_t>(it->second))refuse("canonical invalid integer metadata");
    return std::get<int64_t>(it->second);
}
std::string string(const database::row_t& r,const char* k) {
    auto it=r.find(k);if(it==r.end()||!std::holds_alternative<std::string>(it->second))refuse("canonical invalid bounded metadata");
    return std::get<std::string>(it->second);
}
bool same_ascii(const char* one,const char* two) noexcept {
    if(!one||!two)return false;
    for(;*one&&*two;++one,++two) {
        auto a=static_cast<unsigned char>(*one), b=static_cast<unsigned char>(*two);
        if(a>='A'&&a<='Z')a+=32; if(b>='A'&&b<='Z')b+=32;
        if(a!=b)return false;
    }
    return *one==*two;
}
std::string normalized(std::string s) {
    const std::string prefix="CREATE TRIGGER IF NOT EXISTS ";
    if(s.starts_with(prefix))s.replace(0,prefix.size(),"CREATE TRIGGER ");
    return s;
}
std::string demand(const std::string& condition, const std::string& entry_guard={}) {
    if(!entry_guard.empty())return " SELECT lattice_canonical_require_v1(("+condition+") AND "+entry_guard+");";
    return " SELECT CASE WHEN ("+condition+") THEN 1 ELSE RAISE(ABORT,'canonical marker admission refused') END;";
}
std::string namespace_condition(const canonical_namespace_entry& entry,bool local) {
    return "(SELECT COUNT(*) FROM _lattice_canonical_namespace WHERE namespace_id="+literal(entry.namespace_id)+
        " AND typeof(namespace_id)='blob' AND coverage_id="+literal(entry.coverage_id)+
        " AND typeof(coverage_id)='blob' AND typeof(revision)='integer' AND revision="+std::to_string(entry.revision)+
        " AND typeof(status)='integer' AND status=1 AND typeof(is_local)='integer' AND is_local="+std::to_string(local?1:0)+")=1";
}
std::string namespace_inventory_condition(const canonical_namespace_profile& p) {
    auto out="(SELECT COUNT(*) FROM _lattice_canonical_namespace)="+std::to_string(p.entries.size());
    for(const auto& entry:p.entries)out+=" AND "+namespace_condition(entry,entry.namespace_id==p.local_namespace);
    return out;
}
std::string guard(const canonical_writer_profile& p, const std::string& entry_guard={},const canonical_namespace_profile* namespaces=nullptr) {
    const auto check=[&](const std::string& condition){return demand(condition,entry_guard);};
    const auto limits=" AND typeof(max_markers)='integer' AND typeof(max_marker_bytes)='integer'"
        " AND typeof(max_receipts)='integer' AND typeof(max_receipt_bytes)='integer'"
        " AND typeof(max_batch)='integer' AND typeof(max_identity)='integer' AND typeof(max_operation)='integer'"
        " AND max_markers="+std::to_string(p.limits.markers)+
        " AND max_marker_bytes="+std::to_string(p.limits.marker_bytes)+" AND max_receipts="+std::to_string(p.limits.receipts)+
        " AND max_receipt_bytes="+std::to_string(p.limits.receipt_bytes)+" AND max_batch="+std::to_string(p.limits.batch_identities)+
        " AND max_identity="+std::to_string(p.limits.identity_bytes)+" AND max_operation="+std::to_string(p.limits.operation_bytes);
    return check("lattice_canonical_guard_v1("+literal(p.binding.source)+","+literal(p.binding.epoch)+","+
        literal(p.binding.scope)+","+literal(p.binding.schema)+")=1")+
        check("(SELECT COUNT(*) FROM _lattice_canonical_store WHERE id=1 AND version="+std::to_string(namespaces?2:1)+" AND source="+
        literal(p.binding.source)+" AND epoch="+literal(p.binding.epoch)+" AND scope="+literal(p.binding.scope)+
        " AND schema_id="+literal(p.binding.schema)+" AND typeof(head)='integer' AND typeof(floor)='integer' AND floor>=0 AND head>=floor"
        " AND typeof(markers)='integer' AND markers BETWEEN 0 AND max_markers"
        " AND typeof(marker_bytes)='integer' AND marker_bytes BETWEEN 0 AND max_marker_bytes"
        " AND typeof(receipts)='integer' AND receipts BETWEEN 0 AND max_receipts"
        " AND typeof(receipt_bytes)='integer' AND receipt_bytes BETWEEN 0 AND max_receipt_bytes"+limits+")=1");
}
// One bounded SQL kernel used for every marker and genuine local receipt. It
// runs inside the originating statement, never opens a savepoint/calls C++ SQL
// from a callback. Metadata is WITHOUT ROWID: last_insert_rowid remains the
// generated AuditLog identity until its receipt tail finishes.
std::string mutation(const canonical_writer_profile& p,const std::string& table,
                     const std::string& identity,const std::string& original={},
                     int64_t outcome=1, const std::string& entry_guard={},
                     const canonical_namespace_profile* namespaces=nullptr,const std::string& imported_namespace={}) {
    const auto check=[&](const std::string& condition){return demand(condition,entry_guard);};
    const auto runtime=entry_guard.empty()?std::string{}:" AND "+entry_guard;
    const auto relation=literal(table), key="lattice_canonical_uuid_v1("+identity+")";
    const auto where="relation="+relation+" AND identity="+key;
    const auto charge=std::to_string(24+table.size()+36);
    auto sql=guard(p,entry_guard,namespaces)+check(key+" IS NOT NULL");
    if(original.empty()) {
        const auto fresh="(NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+"))";
        sql+=check("NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+
          " AND (typeof(position)!='integer' OR position<=(SELECT floor FROM _lattice_canonical_store) OR position>(SELECT head FROM _lattice_canonical_store) OR typeof(charge)!='integer' OR charge!="+charge+"))");
        sql+=" UPDATE _lattice_canonical_store SET head=head+1,markers=markers+"+fresh+",marker_bytes=marker_bytes+"+fresh+"*"+charge+
          " WHERE id=1 AND head<9223372036854775807 AND "+fresh+"<=max_markers-markers AND "+fresh+"*"+charge+"<=max_marker_bytes-marker_bytes;";
        sql+=check("changes()=1");
        sql+=" UPDATE _lattice_canonical_touch SET position=(SELECT head FROM _lattice_canonical_store) WHERE "+where+";";
        sql+=" INSERT INTO _lattice_canonical_touch(relation,identity,position,charge) SELECT "+relation+","+key+",head,"+charge+
          " FROM _lattice_canonical_store WHERE id=1 AND NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+");";
        sql+=check("(SELECT COUNT(*) FROM _lattice_canonical_touch WHERE "+where+" AND position=(SELECT head FROM _lattice_canonical_store) AND charge="+charge+")=1");
    } else {
        const auto op="lattice_canonical_uuid_v1("+original+")";
        const auto receipt_namespace=namespaces?(entry_guard.empty()?namespaces->local_namespace:imported_namespace):std::string{};
        if(namespaces) {
            bool found=false;for(const auto& entry:namespaces->entries)if(entry.namespace_id==receipt_namespace) {
                sql+=check(namespace_condition(entry,entry.namespace_id==namespaces->local_namespace));found=true;
            }
            if(!found)refuse("canonical receipt namespace absent from admitted profile");
        }
        const auto receipt_charge=std::to_string(32+36+table.size()+36+receipt_namespace.size());
        // Generated local UUIDs cannot dedup AFTER a second model effect. A
        // collision refuses that whole statement instead of keeping the effect.
        sql+=check(op+" IS NOT NULL AND NOT EXISTS(SELECT 1 FROM _lattice_canonical_receipt WHERE original_id="+op+")");
        sql+=" UPDATE _lattice_canonical_store SET head=head+1,receipts=receipts+1,receipt_bytes=receipt_bytes+"+receipt_charge+
          " WHERE id=1 AND head<9223372036854775807 AND receipts<max_receipts AND "+receipt_charge+"<=max_receipt_bytes-receipt_bytes"+runtime+";";
        sql+=check("changes()=1");
        sql+=" INSERT INTO _lattice_canonical_receipt(original_id,position,outcome,relation,identity,charge"+std::string(namespaces?",namespace_id":"")+") SELECT "+op+",head,"+std::to_string(outcome)+","+
          relation+","+key+","+receipt_charge+(namespaces?","+literal(receipt_namespace):"")+" FROM _lattice_canonical_store WHERE id=1"+runtime+";";
        sql+=check("changes()=1")+check("(SELECT COUNT(*) FROM _lattice_canonical_receipt WHERE original_id="+op+
          " AND position=(SELECT head FROM _lattice_canonical_store) AND outcome="+std::to_string(outcome)+" AND relation="+relation+" AND identity="+key+" AND charge="+receipt_charge+(namespaces?" AND namespace_id="+literal(receipt_namespace):"")+")=1");
    }
    return sql;
}
struct table_plan {std::string name,table_sql;bool link=false;std::vector<std::pair<std::string,column_type>> columns;std::set<std::string> no_history;};
struct program {std::string name,sql;};
std::string program_name(const std::string& sql) {
    auto s=normalized(sql);const std::string prefix="CREATE TRIGGER ";
    if(!s.starts_with(prefix))refuse("canonical unexpected generated trigger");
    auto end=s.find(' ',prefix.size());if(end==std::string::npos)refuse("canonical missing trigger name");return s.substr(prefix.size(),end-prefix.size());
}
std::map<std::string,std::string> triggers(database& db,const std::string& table) {
    auto rows=db.query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 17",{table});
    if(rows.size()>16)refuse("canonical too many table triggers");
    std::map<std::string,std::string> out;for(const auto& row:rows)out.emplace(string(row,"name"),normalized(string(row,"sql")));return out;
}
}

struct canonical_writer_adapter::context {
    sqlite3* connection=nullptr;
    canonical_store_binding binding;
    std::shared_ptr<std::atomic<bool>> active=std::make_shared<std::atomic<bool>>(false);
    std::set<std::string> programs, relations;
    lattice_db* owner=nullptr; // identity only; upstream delivery holds the strong owner
    canonical_writer_profile profile;
    std::optional<canonical_namespace_profile> namespaces;
    const canonical_namespace_profile* namespace_profile()const {return namespaces?&*namespaces:nullptr;}
    // Built once from the exact table/index/program bytes validated at attach.
    // Immutable after publication; capture never accepts replacement scope.
    std::vector<sync_recovery::source_relation> source_relations;
    std::map<std::string,std::map<std::pair<std::string,std::string>,std::string>> source_objects;
    std::string source_manifest, source_descriptor_digest;
    std::optional<canonical_upstream_limits> upstream;
    std::map<std::string,std::unordered_map<std::string,column_type>> schemas;
    std::map<std::string,std::set<std::string>> no_history;
    std::weak_ptr<retention_session> retention;
    static void admit_retention(sqlite3_context*,int,sqlite3_value**) noexcept;
    static bool authorize_retention(context&,int,const char*,const char*) noexcept;
    static void require(sqlite3_context* sql,int count,sqlite3_value** values) noexcept {
        if(count!=1 || sqlite3_value_type(values[0])!=SQLITE_INTEGER || sqlite3_value_int(values[0])!=1)
            sqlite3_result_error(sql,"canonical upstream condition refused",-1);
        else sqlite3_result_int(sql,1);
    }
    static void admit_entry(sqlite3_context* sql,int count,sqlite3_value** values) noexcept {
        auto* d=canonical_upstream_delivery::current_;
        bool ok=d && d->entry_ && d->context_->active->load(std::memory_order_acquire) &&
            sqlite3_context_db_handle(sql)==d->context_->connection && count==3;
        const std::string* fields[3]={ok?&d->original_:nullptr,ok?&d->entry_->table_name:nullptr,ok?&d->target_:nullptr};
        for(int i=0;ok&&i<3;++i) {
            const auto* data=sqlite3_value_blob(values[i]);
            ok=sqlite3_value_type(values[i])==SQLITE_BLOB && data &&
                sqlite3_value_bytes(values[i])==static_cast<int>(fields[i]->size()) &&
                std::memcmp(data,fields[i]->data(),fields[i]->size())==0;
        }
        if(!ok)sqlite3_result_error(sql,"canonical upstream entry expired or mismatched",-1);
        else sqlite3_result_int(sql,1);
    }
    static void admit(sqlite3_context* sql,int count,sqlite3_value** values) noexcept {
        auto& self=**static_cast<std::shared_ptr<context>*>(sqlite3_user_data(sql));
        bool ok=count==4 && self.active->load(std::memory_order_acquire) && sqlite3_context_db_handle(sql)==self.connection;
        const std::array<const std::string*,4> fields{&self.binding.source,&self.binding.epoch,&self.binding.scope,&self.binding.schema};
        for(int i=0;ok&&i<4;++i) {
            const auto* data=sqlite3_value_blob(values[i]);
            ok=sqlite3_value_type(values[i])==SQLITE_BLOB && data &&
                sqlite3_value_bytes(values[i])==static_cast<int>(fields[i]->size()) &&
                std::memcmp(data,fields[i]->data(),fields[i]->size())==0;
        }
        sqlite3_result_int(sql,ok?1:0);
    }
    static int authorize(void* opaque,int action,const char* one,const char* two,const char* schema,const char* origin) noexcept {
        auto& self=*static_cast<context*>(opaque);
        const auto normal=[&]() noexcept -> int {
        // Context is connection-owned; no SQLite/SQL, allocations or callbacks.
        if(action==SQLITE_ATTACH || action==SQLITE_DETACH || action==SQLITE_ALTER_TABLE ||
           action==SQLITE_CREATE_TABLE || action==SQLITE_CREATE_TEMP_TABLE || action==SQLITE_CREATE_TRIGGER ||
           action==SQLITE_CREATE_TEMP_TRIGGER || action==SQLITE_DROP_TABLE || action==SQLITE_DROP_TEMP_TABLE ||
           action==SQLITE_DROP_TRIGGER || action==SQLITE_DROP_TEMP_TRIGGER || action==SQLITE_CREATE_INDEX ||
           action==SQLITE_DROP_INDEX || action==SQLITE_CREATE_TEMP_INDEX || action==SQLITE_DROP_TEMP_INDEX ||
           action==SQLITE_CREATE_VIEW || action==SQLITE_DROP_VIEW || action==SQLITE_CREATE_TEMP_VIEW || action==SQLITE_DROP_TEMP_VIEW ||
           action==SQLITE_CREATE_VTABLE || action==SQLITE_DROP_VTABLE) return SQLITE_DENY;
        if(action==SQLITE_PRAGMA && two && (same_ascii(one,"recursive_triggers") ||
           same_ascii(one,"writable_schema") || same_ascii(one,"schema_version")))return SQLITE_DENY;
        if((action==SQLITE_INSERT||action==SQLITE_UPDATE||action==SQLITE_DELETE) && one &&
           std::strncmp(one,"_lattice_canonical_",19)==0) {
            if(!self.active->load(std::memory_order_acquire)||!schema||std::strcmp(schema,"main"))return SQLITE_DENY;
            if(!origin) {
                if(authorize_retention(self,action,one,two))return SQLITE_OK;
                auto* d=canonical_upstream_delivery::current_;
                const bool phase=d && d->entry_ && d->finalizing_ && d->context_.get()==&self;
                if(phase && action==SQLITE_INSERT && std::strcmp(one,"_lattice_canonical_receipt")==0)return SQLITE_OK;
                if(phase && action==SQLITE_UPDATE && std::strcmp(one,"_lattice_canonical_store")==0 && two &&
                    (std::strcmp(two,"head")==0 || std::strcmp(two,"receipts")==0 || std::strcmp(two,"receipt_bytes")==0))return SQLITE_OK;
                return SQLITE_DENY;
            }
            // Heterogeneous lookup avoids allocation from SQLite C frames.
            for(const auto& name:self.programs)if(name==origin)return SQLITE_OK;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
        };
        const int admitted=normal();
        if(admitted!=SQLITE_OK)return admitted;
        const auto* fault=canonical_upstream_test_hooks::fault;
        if(self.upstream && fault && fault->connection==self.connection && fault->restrict_action) {
            const int restricted=fault->restrict_action(action,one,two,origin);
            if(restricted==SQLITE_DENY || restricted==SQLITE_IGNORE)return restricted;
        }
        const auto* retention_fault=canonical_retention_test_hooks::fault;
        if(!self.retention.expired() && retention_fault && retention_fault->connection==self.connection && retention_fault->restrict_action) {
            const int restricted=retention_fault->restrict_action(action,one,two,origin);
            if(restricted==SQLITE_DENY || restricted==SQLITE_IGNORE)return restricted;
        }
        return SQLITE_OK;
    }
};
#include "canonical_transfer_retention.inc"
std::string canonical_writer_adapter::uuid_key(const std::string& value) {
    if(value.size()!=36)refuse("canonical requires UUID identity");
    char out[36];if(!uuid(reinterpret_cast<const unsigned char*>(value.data()),static_cast<int>(value.size()),out))refuse("canonical requires UUID identity");return {out,36};
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach(lattice_db& owner,const canonical_writer_profile& profile) {
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(owner,profile));
}
thread_local const std::function<void(lattice_db&)>* canonical_writer_adapter::namespace_before_write_test_hook_=nullptr;
canonical_writer_adapter::~canonical_writer_adapter() {
    if(context_) {
        context_->active->store(false,std::memory_order_release);
    }
}
canonical_writer_adapter::canonical_writer_adapter(lattice_db& owner,const canonical_writer_profile& p,
    const canonical_upstream_limits* upstream,const canonical_retention_limits* retention,
    const canonical_namespace_profile* namespaces) {
    const auto& catalog=owner.recovery_schemas_;
    if(!catalog.valid())refuse("canonical owner schema catalog outside bounds or ambiguous");
    // The attachment owns its setup transaction. It cannot attach during caller
    // work, on an active synchronizer, or claim adoption of another connection.
    if((p.upstream_requested && !upstream) || owner.config_.is_sync_enabled() || owner.config_.is_ipc_enabled())
        refuse("canonical Slice A refuses upstream/transport activation");
    if(owner.is_closed() || owner.config_.read_only || owner.db_->is_closed() || owner.db_->is_in_transaction())
        refuse("canonical attachment requires an idle live writer");
    if(p.models.empty() || p.models.size()>max_tables || p.limits.identity_bytes<64 || p.limits.operation_bytes<36)
        refuse("canonical unsupported scope/identity budget");
    for(const auto& name:p.models)if(!identifier(name))refuse("canonical invalid bounded model name");
    canonical_change_store store(owner,p.binding,p.limits,namespaces); // Validates before copying/registration.
    writer_=owner.db_;
    {
        auto* mutex=sqlite3_db_mutex(writer_->internal_handle());sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        if(writer_->canonical_custody_bootstrap_ || writer_->raw_handle_escaped_.load(std::memory_order_acquire) ||
           writer_->txn_hooks_external_ || writer_->read_control_ ||
           std::atomic_load(&writer_->local_producer_callback_custody_))
            refuse("canonical attachment requires unescaped engine connection policy");
        for(auto* statement=sqlite3_next_stmt(writer_->internal_handle(),nullptr);statement;
            statement=sqlite3_next_stmt(writer_->internal_handle(),statement))
            if(sqlite3_stmt_busy(statement))refuse("canonical attachment requires no active statements");
        writer_->canonical_custody_bootstrap_=true;
    }
    // Public raw/hook/capture entry cannot replace policy between initial
    // admission and context publication, including callbacks during COMMIT.
    const auto end_bootstrap=[](database* writer) {
        auto* mutex=sqlite3_db_mutex(writer->internal_handle());sqlite3_mutex_enter(mutex);
        writer->canonical_custody_bootstrap_=false;sqlite3_mutex_leave(mutex);
    };
    const std::unique_ptr<database,decltype(end_bootstrap)> bootstrap(writer_.get(),end_bootstrap);
    if(writer_->canonical_callback_custody_) {
        auto old=std::static_pointer_cast<context>(writer_->canonical_callback_custody_);
        if(old->active->load(std::memory_order_acquire))refuse("canonical writer already attached");
    }
    // No external callback may coexist on this private profile's writer.
    // The same attachment's revoked callback has connection-owned custody.
    context_=std::make_shared<context>();context_->connection=writer_->internal_handle();context_->binding=p.binding;
    context_->owner=&owner;context_->profile=p;
    if(namespaces)context_->namespaces=*namespaces;
    if(upstream)context_->upstream=*upstream;
    // An inert primitive ledger is not evidence of prior owned acceptance.
    // V2 first enrollment requires no canonical metadata at all; only the
    // complete exact retained profile may reopen below. No receipts are adopted.
    const bool namespaced_reopen=namespaces && writer_->table_exists("_lattice_canonical_coverage");
    if(namespaces && !namespaced_reopen &&
       !writer_->query("SELECT 1 FROM main.sqlite_master WHERE substr(name,1,19)='_lattice_canonical_' LIMIT 1").empty())
        refuse("canonical namespaced enrollment refuses preexisting unadmitted metadata");
    if(retention)prepare_retention(owner,*retention);
    const auto register_shared=[&] {
        auto* held=new std::shared_ptr<context>(context_);
        if(sqlite3_create_function_v2(context_->connection,"lattice_canonical_guard_v1",4,SQLITE_UTF8,held,
              context::admit,nullptr,nullptr,[](void* x){delete static_cast<std::shared_ptr<context>*>(x);})!=SQLITE_OK)
            refuse("canonical guard registration failed"); // SQLite owns/destructs userdata on failure.
        if(sqlite3_create_function_v2(context_->connection,"lattice_canonical_uuid_v1",1,SQLITE_UTF8|SQLITE_DETERMINISTIC,
              nullptr,uuid_sql,nullptr,nullptr,nullptr)!=SQLITE_OK)refuse("canonical UUID registration failed");
        if(upstream && (sqlite3_create_function_v2(context_->connection,"lattice_canonical_require_v1",1,SQLITE_UTF8,
                nullptr,context::require,nullptr,nullptr,nullptr)!=SQLITE_OK ||
            sqlite3_create_function_v2(context_->connection,"lattice_canonical_entry_v1",3,SQLITE_UTF8,
                nullptr,context::admit_entry,nullptr,nullptr,nullptr)!=SQLITE_OK))
            refuse("canonical upstream guard registration failed");
    };
    bool began=false;
    try {
        if(writer_->canonical_callback_custody_)sqlite3_set_authorizer(writer_->internal_handle(),nullptr,nullptr);
        register_shared();
        if(retention) {
            auto* held=new std::shared_ptr<context>(context_);
            if(sqlite3_create_function_v2(context_->connection,"lattice_canonical_retention_guard_v1",0,SQLITE_UTF8,
                held,context::admit_retention,nullptr,nullptr,[](void* value){delete static_cast<std::shared_ptr<context>*>(value);})!=SQLITE_OK)
                refuse("canonical retention guard registration failed");
        }
        writer_->execute("PRAGMA recursive_triggers=ON");
        if(integer(writer_->query("PRAGMA recursive_triggers").at(0),"recursive_triggers")!=1)
            refuse("canonical REPLACE coverage needs recursive triggers");
        if(namespaces && namespace_before_write_test_hook_)(*namespace_before_write_test_hook_)(owner);
        owner.begin_transaction();began=true;
        if(namespaces) {
            // Preflight is only a refusal optimization. BEGIN IMMEDIATE now
            // excludes sibling writes: make the no-adoption decision here,
            // before initialize() can audit an inert primitive ledger or any
            // coverage/retention enrollment can write or clean up attempts.
            if(recovery_writer_access::active_writer(owner)!=writer_.get())
                refuse("canonical namespaced enrollment requires the actual owned WRITE");
            if(writer_->table_exists("_lattice_canonical_coverage")!=namespaced_reopen ||
               writer_->table_exists("_lattice_canonical_retention")!=namespaced_reopen)
                refuse("canonical namespaced enrollment profile changed before owned WRITE");
            if(!namespaced_reopen &&
               !writer_->query("SELECT 1 FROM main.sqlite_master WHERE substr(name,1,19)='_lattice_canonical_' LIMIT 1").empty())
                refuse("canonical namespaced enrollment refuses preexisting unadmitted metadata");
            if(namespaced_reopen)retention_inventory_matches(*writer_,p,namespaces);
        }
        const auto databases=writer_->query("SELECT name FROM pragma_database_list LIMIT 3");
        for(const auto& row:databases) {
            const auto name=string(row,"name");
            if(name!="main"&&name!="temp")refuse("canonical attached schemas are unqualified");
        }
        if(databases.size()>2 || !writer_->query("SELECT 1 FROM temp.sqlite_master LIMIT 1").empty())
            refuse("canonical temporary schema objects are unqualified");
        // Only the generated audit INSERT followed immediately by our tail may
        // attest an original. Another AuditLog trigger could change that row or
        // ignore the INSERT, so the whole source profile refuses it.
        if(!writer_->query("SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND tbl_name='AuditLog' LIMIT 1").empty())
            refuse("canonical AuditLog has unapproved triggers");
        store.initialize();
        if(!retention && !writer_->query("SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND substr(tbl_name,1,19)='_lattice_canonical_' LIMIT 1").empty())
            refuse("canonical metadata has unapproved triggers");
        std::map<std::string,table_plan> tables;
        std::set<std::string> models(p.models.begin(),p.models.end());
        if(models.size()!=p.models.size())refuse("canonical duplicate scoped model");
        for(const auto& name:models) {
            if(!identifier(name) || name[0]=='_')refuse("canonical unsupported model name");
            const auto* schema=catalog.find(name);
            if(!schema || schema->properties.empty() || schema->properties.size()>max_columns)
                refuse("canonical unknown/oversized scalar model schema");
            table_plan plan;plan.name=name;
            for(const auto& prop:schema->properties) {
                // Actual virtual source relations are outside this fixed
                // profile. Derived vector/FTS/geo indexes of ordinary models
                // are a distinct, required follow-up qualification (Engram).
                if(!identifier(prop.name)||prop.is_vector||prop.is_geo_bounds||prop.is_full_text||prop.is_union||
                   (!prop.column_name.empty()&&prop.column_name!=prop.name)||
                   (prop.kind!=property_kind::primitive&&prop.kind!=property_kind::link&&prop.kind!=property_kind::list))
                    refuse("canonical unsupported complete-table property");
                const bool swift=catalog.swift_models.count(name);
                const bool physical=swift?prop.kind==property_kind::primitive:prop.kind!=property_kind::list;
                if(physical)plan.columns.emplace_back(prop.name,prop.type);
                if(prop.no_history&&(!swift||physical))plan.no_history.insert(prop.name);
            }
            tables.emplace(name,std::move(plan));
        }
        // Complete connected regular-link closure, including incoming links.
        for(const auto& [schema_name,schema]:catalog.models)for(const auto& prop:schema.properties) {
            if(prop.kind!=property_kind::link&&prop.kind!=property_kind::list)continue;
            if(!models.count(schema.table_name)&&!models.count(prop.target_table))continue;
            if(prop.is_geo_bounds || prop.target_table.empty() || !models.count(schema.table_name)||!models.count(prop.target_table))
                refuse("canonical incomplete/unsupported relationship closure");
            const auto name="_"+schema.table_name+"_"+prop.target_table+"_"+prop.name;
            if(!identifier(name))refuse("canonical oversized relation name");
            table_plan link;link.name=name;link.link=true;tables.emplace(name,std::move(link));
            context_->relations.insert(name);
        }
        if(tables.size()>max_tables)refuse("canonical too many physical scope tables");
        std::string scoped;
        for(const auto& [name,table]:tables) {if(!scoped.empty())scoped+=",";scoped+=literal(name);}
        if(!writer_->query("SELECT 1 FROM main._lattice_canonical_touch WHERE relation NOT IN ("+scoped+
            ") OR lattice_canonical_uuid_v1(CAST(identity AS TEXT)) IS NOT identity LIMIT 1").empty() ||
           !writer_->query("SELECT 1 FROM main._lattice_canonical_receipt WHERE lattice_canonical_uuid_v1(CAST(original_id AS TEXT)) IS NOT original_id"
            " OR (relation IS NOT NULL AND (relation NOT IN ("+scoped+") OR lattice_canonical_uuid_v1(CAST(identity AS TEXT)) IS NOT identity)) LIMIT 1").empty())
            refuse("canonical prior keys do not match fixed UUID/scope profile");
        std::vector<program> originals,installed;
        std::string manifest="canonical-fixed-local-v1\nuuid-ascii-nocase-v1\n";
        if(namespaces) {
            manifest+="canonical-receipt-v2-fixed-64-256\nlocal:"+literal(namespaces->local_namespace)+"\n";
            for(const auto& entry:namespaces->entries)manifest+=literal(entry.namespace_id)+":"+literal(entry.coverage_id)+":"+std::to_string(entry.revision)+"\n";
        }
        if(!catalog.swift_digest.empty())manifest+="swift-owner-schema-v1:"+catalog.swift_digest+"\n";
        size_t existing=0;
        for(auto& [name,table]:tables) {
            auto ddl=writer_->query("SELECT CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='table' AND name=?",{name});
            if(ddl.size()!=1)refuse("canonical missing table");
            table.table_sql=string(ddl[0],"sql");
            context_->source_relations.push_back({name,table.link?sync_recovery::relation_kind::link:
                sync_recovery::relation_kind::model,true});
            context_->source_objects[name].emplace(std::pair{"table",name},table.table_sql);
            if(table.table_sql.find("globalId TEXT UNIQUE COLLATE NOCASE")==std::string::npos ||
               table.table_sql.find("CREATE VIRTUAL")!=std::string::npos || table.table_sql.find("WITHOUT ROWID")!=std::string::npos)
                refuse("canonical unsupported identity/table shape");
            auto cols=writer_->query("SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END AS name,CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END AS type,hidden FROM pragma_table_xinfo(?) ORDER BY cid LIMIT 35",{name});
            std::map<std::string,std::string> got;
            for(const auto& row:cols) {
                if(integer(row,"hidden")!=0)refuse("canonical generated/hidden source column");
                got.emplace(string(row,"name"),string(row,"type"));
            }
            std::map<std::string,std::string> want;
            if(table.link)want={{"lhs","TEXT"},{"rhs","TEXT"},{"globalId","TEXT"}};
            else {
                want={{"id","INTEGER"},{"globalId","TEXT"}};
                for(const auto& [column,type]:table.columns) {
                    const auto t=type==column_type::integer?"INTEGER":type==column_type::real?"REAL":type==column_type::blob?"BLOB":"TEXT";
                    want.emplace(column,t);
                }
            }
            if(got!=want)refuse("canonical durable columns differ from complete descriptor");
            if(catalog.swift_models.count(name)) {
                std::vector<std::pair<std::string,column_type>> ordered;
                for(const auto& c:cols) {
                    const auto column=string(c,"name");
                    for(const auto& declared:table.columns)if(declared.first==column){ordered.push_back(declared);break;}
                }
                if(ordered.size()!=table.columns.size())refuse("canonical incomplete physical column order");
                table.columns=std::move(ordered);
            }
            for(const auto& [column,type]:got)context_->schemas[name].emplace(column,
                type=="INTEGER"?column_type::integer:type=="REAL"?column_type::real:type=="BLOB"?column_type::blob:column_type::text);
            context_->no_history[name]=table.no_history;
            auto rows=writer_->query("SELECT CASE WHEN typeof(globalId)='text' AND length(CAST(globalId AS BLOB))=36 THEN globalId END AS gid FROM main."+name+" LIMIT 4097");
            if(rows.size()>max_existing_rows-existing)refuse("canonical bounded initial identity scan exceeded");
            existing+=rows.size();
            for(const auto& row:rows)(void)uuid_key(string(row,"gid"));
            std::vector<std::string> old_sql,new_sql;
            const auto target="(SELECT globalRowId FROM AuditLog WHERE id=last_insert_rowid())";
            const auto original="(SELECT globalId FROM AuditLog WHERE id=last_insert_rowid())";
            const auto tail=demand("changes()=1")+
                demand("(SELECT COUNT(*) FROM AuditLog WHERE id=last_insert_rowid() AND tableName='"+name+
                    "' AND typeof(globalRowId)='text' AND isFromRemote=0 AND synthesized=0)=1")+
                mutation(p,name,target,original,1,{},namespaces);
            if(table.link) {
                owner.create_link_table_triggers(name,{},&old_sql);
                owner.create_link_table_triggers(name,tail,&new_sql);
            } else {
                owner.create_model_table_triggers(name,table.columns,table.no_history,{},&old_sql);
                owner.create_model_table_triggers(name,table.columns,table.no_history,tail,&new_sql);
            }
            for(const auto& sql:old_sql)originals.push_back({program_name(sql),sql});
            for(const auto& sql:new_sql)installed.push_back({program_name(sql),sql});
            for(const auto* event:{"INSERT","UPDATE","DELETE"}) {
                const auto trigger="_lattice_canonical_"+name+"_"+event;
                const auto identity=std::string(event)=="DELETE"?"OLD.globalId":"NEW.globalId";
                installed.push_back({trigger,"CREATE TRIGGER "+trigger+" AFTER "+event+" ON "+name+" BEGIN"+mutation(p,name,identity,{},1,{},namespaces)+" END"});
            }
            const auto identity_guard="_lattice_canonical_"+name+"_identity";
            auto same="CAST(OLD.globalId AS BLOB) IS CAST(NEW.globalId AS BLOB)";
            installed.push_back({identity_guard,"CREATE TRIGGER "+identity_guard+" BEFORE UPDATE ON "+name+" BEGIN"+
                guard(p,{},namespaces)+demand(same+(table.link?std::string{}:" AND OLD.id IS NEW.id"))+" END"});
            const auto indexes=writer_->query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN sql IS NULL THEN '' WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? ORDER BY name LIMIT 33",{name});
            if(indexes.size()>32)refuse("canonical too many source indexes");
            std::string definitions=name+"\n"+table.table_sql+"\n";
            for(const auto& index:indexes) {
                const auto index_name=string(index,"name"), index_sql=string(index,"sql");
                if(index_name.size()>128 || index_sql.size()>max_sql || definitions.size()>max_sql-index_sql.size())
                    refuse("canonical index descriptor budget exceeded");
                definitions+=index_name+"\n"+index_sql+"\n";
                context_->source_objects[name].emplace(std::pair{std::string("index"),index_name},index_sql);
            }
            if(definitions.size()>max_sql || manifest.size()>max_sql-definitions.size())refuse("canonical descriptor budget exceeded");
            manifest+=definitions;
        }
        for(const auto& program:installed) {
            if(program.sql.size()>max_sql || manifest.size()>max_sql-program.sql.size())refuse("canonical generated SQL budget exceeded");
            manifest+=normalized(program.sql)+"\n";
            context_->programs.insert(program.name);
            for(const auto& [name,table]:tables)if(program.sql.find(" ON "+name+" ")!=std::string::npos)
                context_->source_objects[name].emplace(std::pair{std::string("trigger"),program.name},normalized(program.sql));
        }
        if(manifest.size()>max_sql)refuse("canonical manifest budget exceeded");
        context_->source_manifest=manifest;
        context_->source_descriptor_digest=picosha2::hash256_hex_string(manifest);
        const bool reopen=namespaces?namespaced_reopen:writer_->table_exists("_lattice_canonical_coverage");
        if(reopen) {
            const auto shape=writer_->query("SELECT wr FROM pragma_table_list WHERE schema='main' AND name='_lattice_canonical_coverage'");
            if(shape.size()!=1 || integer(shape[0],"wr")!=1)refuse("canonical coverage must be WITHOUT ROWID");
            auto prior=writer_->query("SELECT id,CASE WHEN typeof(manifest)='blob' AND length(manifest)<=262144 THEN manifest END AS manifest FROM main._lattice_canonical_coverage LIMIT 2");
            if(prior.size()!=1 || integer(prior[0],"id")!=1 || !std::holds_alternative<blob>(prior[0].at("manifest")) || std::get<blob>(prior[0].at("manifest"))!=bytes(manifest))
                refuse("canonical coverage manifest differs; no rebind/repair");
        }
        const auto& expected=reopen?installed:originals;
        for(const auto& [name,table]:tables) {
            std::map<std::string,std::string> want;
            const auto actual=triggers(*writer_,name);
            for(const auto& program:expected)if(program.sql.find(" ON "+name+" ")!=std::string::npos)want.emplace(program.name,normalized(program.sql));
            if(actual!=want)refuse("canonical missing/extra/non-generated trigger; scope refused");
        }
        if(!reopen) {
            for(const auto& program:originals)writer_->execute("DROP TRIGGER "+program.name);
            for(const auto& program:installed)writer_->execute(program.sql);
            writer_->execute("CREATE TABLE main._lattice_canonical_coverage(id INTEGER PRIMARY KEY CHECK(id=1),manifest BLOB NOT NULL) WITHOUT ROWID");
            writer_->execute("INSERT INTO main._lattice_canonical_coverage VALUES(1,?)",{bytes(manifest)});
        }
        if(retention)enroll_retention(owner,reopen);
        store.audit();
        owner.commit();began=false;
        auto* mutex=sqlite3_db_mutex(context_->connection);sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        writer_->canonical_trigger_only_=true;
        writer_->canonical_callback_custody_=context_;
        writer_->canonical_write_allowed_=context_->active;
        context_->active->store(true,std::memory_order_release);
        if(sqlite3_set_authorizer(context_->connection,context::authorize,context_.get())!=SQLITE_OK)
            refuse("canonical writer authorizer registration failed");
    } catch(...) {
        context_->active->store(false,std::memory_order_release);
        const auto primary=std::current_exception();
        std::exception_ptr cleanup;
        if(began)try {owner.rollback();}catch(...) {cleanup=std::current_exception();}
        // Keep callback userdata alive and failed persistent attachments closed.
        auto* mutex=sqlite3_db_mutex(context_->connection);sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        writer_->canonical_callback_custody_=context_;
        writer_->canonical_write_allowed_=context_->active;
        sqlite3_set_authorizer(context_->connection,context::authorize,context_.get());
        if(cleanup)throw canonical_store_error(canonical_store_error_code::cleanup_failed,
            "canonical attachment rollback failed; writer remains refused",primary,cleanup);
        std::rethrow_exception(primary);
    }
}
sync_recovery::owned_canonical_capture canonical_writer_adapter::capture_recovery_owned(
    std::shared_ptr<lattice_db> owner,const canonical_store_binding& binding,std::optional<int64_t> base,
    const std::vector<sync_recovery::canonical_capture_request>& requests,
    const sync_recovery::canonical_capture_limits& limits) {
    if(retention_)refuse("canonical retention profile requires capture through a committed reservation");
    return capture_recovery_impl(std::move(owner),binding,base,requests,limits,{},{},{});
}
sync_recovery::owned_canonical_capture canonical_writer_adapter::capture_recovery_impl(
    std::shared_ptr<lattice_db> owner,const canonical_store_binding& binding,std::optional<int64_t> base,
    const std::vector<sync_recovery::canonical_capture_request>& requests,
    const sync_recovery::canonical_capture_limits& limits,
    const std::function<void(size_t,uint64_t)>& after_batch,
    const std::function<void()>& before_decision,const std::function<void()>& after_decision,
    const std::function<void(uint64_t)>& verify_retention_generation,
    const canonical_namespace_admission* namespace_admission) {
    // No access to this after these copies: qualification callbacks may retire
    // the wrapper. They cannot release our actual owner/writer/context custody.
    auto state=context_;auto writer=writer_;
    const auto held_admission=namespace_admission?std::optional<canonical_namespace_admission>(*namespace_admission):std::nullopt;
    namespace_admission=held_admission?&*held_admission:nullptr;
    if(!owner || !state || state->owner!=owner.get() || binding!=state->binding)
        refuse("canonical source requires this admitted owner and exact binding");
    if(bool(state->namespaces)!=bool(namespace_admission))refuse("canonical capture requires exact receipt profile admission");
    if(namespace_admission) {
        if(!limits.requests || limits.requests>4096 || requests.size()>limits.requests)
            refuse("canonical namespaced request count exceeds finite capture policy");
        validate_namespace_admission(owner,writer,state,*namespace_admission);
        for(const auto& request:requests)if(request.namespace_id!=std::optional<std::string>(namespace_admission->namespace_.namespace_id))
            refuse("canonical capture request namespace differs from admission");
    }
    uint64_t revision;bool admitted;
    {
        std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
        admitted=!owner->closed_.load() && owner->db_==writer && state->active->load(std::memory_order_acquire);
        revision=owner->connection_revision_;
    }
    if(!admitted)refuse("canonical source attachment is retired");
    const auto decide=[&] {
        // Final validity linearizes at the active load while publication is
        // locked. No SQL, callback, allocation or teardown occurs under it.
        bool valid;
        {
            std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
            valid=!owner->closed_.load() && owner->db_==writer && owner->connection_revision_==revision &&
                !writer->is_closed() && matches_connection(*writer,state->connection) &&
                state->active->load(std::memory_order_acquire);
        }
        if(!valid)refuse("canonical source owner, writer or attachment changed");
        if(namespace_admission)validate_namespace_admission(owner,writer,state,*namespace_admission);
    };
    const auto verify_generation=[&](uint64_t generation) {
        decide(); // after the actual keeper pin, not an earlier guessed view
        if(verify_retention_generation)verify_retention_generation(generation);
        const auto query=[&](const std::string& sql,const std::vector<column_value_t>& args={}) {
            auto result=owner->query_at_generation(generation,sql,args);
            if(!result)refuse("canonical source descriptor view retired");return std::move(*result);
        };
        if(state->namespaces) {
            const auto catalog=query("SELECT "+namespace_inventory_condition(*state->namespaces)+" AS exact");
            if(catalog.size()!=1 || integer(catalog[0],"exact")!=1)refuse("canonical source namespace catalog changed");
        }
        const auto coverage=query("SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,typeof(manifest)='blob' AND length(manifest)<=262144 AND manifest=? AS exact "
            "FROM main._lattice_canonical_coverage LIMIT 2",{bytes(state->source_manifest)});
        if(coverage.size()!=1 || integer(coverage[0],"id")!=1 || integer(coverage[0],"exact")!=1)
            refuse("canonical source coverage descriptor changed");
        size_t copied=0;
        for(const auto& [table,expected]:state->source_objects) {
            // Inspect lengths before copying DDL; aggregate allocation is
            // bounded independently of corrupt stored metadata lengths.
            const auto shape=query("SELECT type,CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,"
                "CASE WHEN sql IS NULL THEN 0 WHEN typeof(sql)='text' THEN length(CAST(sql AS BLOB)) ELSE -1 END AS n "
                "FROM main.sqlite_master WHERE tbl_name=? AND type IN ('table','index','trigger') ORDER BY type,name LIMIT 51",{table});
            if(shape.size()!=expected.size() || shape.size()>50)refuse("canonical source object inventory changed");
            for(const auto& row:shape) {
                const auto key=std::pair{string(row,"type"),string(row,"name")};const auto n=integer(row,"n");
                if(!expected.count(key) || n<0 || static_cast<uint64_t>(n)>max_sql-copied)
                    refuse("canonical source descriptor shape or byte budget changed");
                copied+=static_cast<size_t>(n);
            }
            const auto actual=query("SELECT type,name,COALESCE(sql,'') AS sql FROM main.sqlite_master "
                "WHERE tbl_name=? AND type IN ('table','index','trigger') ORDER BY type,name LIMIT 51",{table});
            std::map<std::pair<std::string,std::string>,std::string> objects;
            for(const auto& row:actual)objects.emplace(std::pair{string(row,"type"),string(row,"name")},normalized(string(row,"sql")));
            if(objects!=expected)refuse("canonical source schema or generated programs changed");
        }
    };
    auto result=sync_recovery::canonical_source_session_access::capture(*owner,state->binding,state->source_relations,
        base,requests,limits,verify_generation,after_batch,state->namespace_profile());
    result.descriptor_digest=state->source_descriptor_digest;
    if(before_decision)before_decision();
    decide();
    // No mutable validity reread after this decision: later close cannot
    // retroactively change the decided, unsealed facts. There is no send here.
    if(after_decision)after_decision();
    return result;
}
void require_canonical_relation(database& db,const std::string& name) {
    if(!db.canonical_callback_custody_)refuse("canonical relation has no admitted callback custody");
    const auto state=std::static_pointer_cast<canonical_writer_adapter::context>(db.canonical_callback_custody_);
    if(!state->active->load(std::memory_order_acquire)||!state->relations.count(name))
        refuse("canonical relation is outside the complete admitted scope");
}

namespace {
void bounded_add(size_t& used,size_t amount,size_t limit) {
    if(used>limit || amount>limit-used)refuse("canonical upstream logical byte budget exceeded");
    used+=amount;
}
size_t scalar_bytes(const any_property::value_type& value) {
    return std::visit([](const auto& v)->size_t {
        using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,blob>)return v.size();
        else return 8;
    },value);
}
struct checked_statement {
    sqlite3_stmt* value=nullptr;
    ~checked_statement(){if(value)sqlite3_finalize(value);}
    void finish() {
        auto* statement=std::exchange(value,nullptr);
        if(sqlite3_finalize(statement)!=SQLITE_OK)refuse("canonical upstream statement finalization failed");
    }
};
void bind_checked(sqlite3_stmt* stmt,int index,const column_value_t& value) {
    const int rc=std::visit([&](const auto& v)->int {
        using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,std::nullptr_t>)return sqlite3_bind_null(stmt,index);
        else if constexpr(std::is_same_v<T,int64_t>)return sqlite3_bind_int64(stmt,index,v);
        else if constexpr(std::is_same_v<T,double>)return sqlite3_bind_double(stmt,index,v);
        else if constexpr(std::is_same_v<T,std::string>)return sqlite3_bind_text64(stmt,index,v.data(),v.size(),SQLITE_TRANSIENT,SQLITE_UTF8);
        else return v.empty()?sqlite3_bind_zeroblob(stmt,index,0):sqlite3_bind_blob64(stmt,index,v.data(),v.size(),SQLITE_TRANSIENT);
    },value);
    if(rc!=SQLITE_OK)refuse("canonical upstream parameter binding failed");
}
}
thread_local canonical_upstream_delivery* canonical_upstream_delivery::current_=nullptr;
thread_local const canonical_upstream_test_hooks::authorizer_fault* canonical_upstream_test_hooks::fault=nullptr;
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach_upstream_for_qualification(
    std::shared_ptr<lattice_db> owner,const canonical_writer_profile& p,canonical_upstream_limits limits) {
    if(!owner || !p.upstream_requested || !limits.entries || !limits.field_bytes || !limits.delivery_bytes ||
        limits.field_bytes>limits.delivery_bytes || limits.delivery_bytes>static_cast<size_t>(std::numeric_limits<int>::max()/8))
        refuse("canonical upstream qualification requires explicit bounded profile and retained owner");
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(*owner,p,&limits));
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach_retained_upstream_for_qualification(
    std::shared_ptr<lattice_db> owner,const canonical_writer_profile& p,
    canonical_upstream_limits upstream,canonical_retention_limits retention) {
    if(!owner || !p.upstream_requested || !upstream.entries || !upstream.field_bytes || !upstream.delivery_bytes ||
        upstream.field_bytes>upstream.delivery_bytes || upstream.delivery_bytes>static_cast<size_t>(std::numeric_limits<int>::max()/8))
        refuse("canonical retained upstream requires explicit bounded profile and retained owner");
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(*owner,p,&upstream,&retention));
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach_namespaced_upstream_for_qualification(
    std::shared_ptr<lattice_db> owner,const canonical_namespaced_writer_profile& p,
    canonical_upstream_limits upstream,canonical_retention_limits retention) {
    if(!owner || !p.writer.upstream_requested || !upstream.entries || upstream.field_bytes<256 || !upstream.delivery_bytes ||
       upstream.field_bytes>upstream.delivery_bytes || upstream.delivery_bytes>static_cast<size_t>(std::numeric_limits<int>::max()/8))
        refuse("canonical namespaced qualification requires bounded retained upstream owner");
    p.namespaces.validate();
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(*owner,p.writer,&upstream,&retention,&p.namespaces));
}
void canonical_writer_adapter::validate_namespace_admission(const std::shared_ptr<lattice_db>& owner,
    const std::shared_ptr<database>& writer,const std::shared_ptr<context>& state,const canonical_namespace_admission& admission) {
    if(!owner || !state || !state->namespaces || owner!=admission.owner_ || writer!=admission.writer_ ||
       admission.context_.get()!=state.get() || state->owner!=owner.get())refuse("canonical namespace admission belongs to another actual owner");
    bool found=false;for(const auto& entry:state->namespaces->entries)if(entry==admission.namespace_)found=true;
    if(!found || admission.replica_.empty())refuse("canonical namespace admission provenance differs");
    std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
    if(owner->closed_.load() || owner->db_!=writer || owner->connection_revision_!=admission.revision_ ||
       !state->active->load(std::memory_order_acquire) || writer->is_closed() || !matches_connection(*writer,state->connection))
        refuse("canonical namespace admission physical session retired");
}
canonical_namespace_admission canonical_writer_adapter::admit_namespace_for_qualification(std::shared_ptr<lattice_db> owner,
    const std::string& namespace_id,const std::string& replica_id) {
    auto state=context_;auto writer=writer_;
    if(!owner || !state || !state->namespaces || state->owner!=owner.get() || namespace_id.empty() || namespace_id.size()>256 ||
        replica_id.empty() || replica_id.size()>256)refuse("canonical namespace qualification requires bounded actual profile");
    canonical_namespace_admission result;bool found=false;
    for(const auto& entry:state->namespaces->entries)if(entry.namespace_id==namespace_id){result.namespace_=entry;found=true;}
    if(!found)refuse("canonical namespace is not enrolled");
    result.owner_=owner;result.writer_=writer;result.context_=state;result.replica_=replica_id;
    {std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);result.revision_=owner->connection_revision_;}
    validate_namespace_admission(owner,writer,state,result);
    // Qualification issuer only: no assertion of network/application identity.
    return result;
}
bool canonical_writer_adapter::matches_connection(const database& writer,sqlite3* handle) noexcept {
    return writer.internal_handle()==handle;
}
std::vector<std::string> canonical_writer_adapter::apply_upstream_owned(std::shared_ptr<lattice_db> owner,
    const std::vector<audit_log_entry>& entries,const std::optional<std::string>& receiving_channel) {
    return apply_upstream_impl(std::move(owner),entries,receiving_channel,nullptr);
}
std::vector<std::string> canonical_writer_adapter::apply_upstream_namespaced_owned(std::shared_ptr<lattice_db> owner,
    const canonical_namespace_admission& admission,const std::vector<audit_log_entry>& entries,
    const std::optional<std::string>& receiving_channel) {
    return apply_upstream_impl(std::move(owner),entries,receiving_channel,&admission);
}
std::vector<std::string> canonical_writer_adapter::apply_upstream_impl(std::shared_ptr<lattice_db> owner,
    const std::vector<audit_log_entry>& entries,const std::optional<std::string>& receiving_channel,
    const canonical_namespace_admission* admission) {
    // Copy all adapter custody before any SQL/callback. The caller may revoke
    // and destroy this wrapper during a later observer without invalidating the
    // delivery's context or retained actual owner.
    auto state=context_;auto writer=writer_;
    if(!owner || !state || !state->upstream || state->owner!=owner.get())
        refuse("canonical upstream requires this attachment's retained actual owner");
    if(bool(state->namespaces)!=bool(admission))refuse("canonical upstream requires exact receipt profile admission");
    if(admission)validate_namespace_admission(owner,writer,state,*admission);
    uint64_t revision;
    {
        std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
        if(owner->closed_.load() || owner->db_!=writer || !state->active->load(std::memory_order_acquire))
            refuse("canonical upstream attachment is retired");
        revision=owner->connection_revision_;
    }
    canonical_upstream_delivery delivery(owner,std::move(writer),std::move(state),revision,admission);
    delivery.validate_envelope(entries,receiving_channel);
    return owner->apply_remote_changes_impl_(entries,receiving_channel,&delivery);
}
canonical_upstream_delivery::canonical_upstream_delivery(std::shared_ptr<lattice_db> owner,
    std::shared_ptr<database> writer,std::shared_ptr<canonical_writer_adapter::context> context,uint64_t revision,
    const canonical_namespace_admission* admission)
    :owner_(std::move(owner)),writer_(std::move(writer)),context_(std::move(context)),revision_(revision) {
    if(admission)namespace_admission_=*admission;
}
void canonical_upstream_delivery::validate_chunk(lattice_db& owner,database& writer) const {
    // Called only after the owning loop acquired its gate/SQLite maintenance
    // locks and validated physical hook and publication revision. No raw handle
    // from a caller can construct this capability.
    if(&owner!=owner_.get() || &writer!=writer_.get() || !context_->upstream ||
        !context_->active->load(std::memory_order_acquire) || writer.is_closed() ||
        !canonical_writer_adapter::matches_connection(writer,context_->connection))
        refuse("canonical upstream chunk capability is retired or mismatched");
    // The apply loop holds publication here and already checks revision_.
    // Never recursively acquire that nonrecursive mutex from this callback.
    if(namespace_admission_ && (namespace_admission_->owner_!=owner_ || namespace_admission_->writer_!=writer_ ||
       namespace_admission_->context_.get()!=context_.get() || namespace_admission_->revision_!=revision_))
        refuse("canonical namespace chunk physical admission differs");
}
void canonical_upstream_delivery::validate_envelope(const std::vector<audit_log_entry>& entries,
    const std::optional<std::string>& channel) const {
    const auto limits=*context_->upstream;
    if(entries.size()>limits.entries)refuse("canonical upstream entry count budget exceeded");
    size_t used=0;
    if(channel) {
        if(channel->empty() || channel->size()>64 || channel->find('\0')!=std::string::npos)
            refuse("canonical upstream invalid bounded receiving channel");
        bounded_add(used,channel->size(),limits.delivery_bytes);
    }
    for(const auto& e:entries) {
        if(!identifier(e.table_name) || !context_->schemas.count(e.table_name) ||
            e.operation.size()>16 || e.timestamp.size()>64 || e.global_id.size()!=36 || e.global_row_id.size()!=36 ||
            e.changed_fields.size()>max_columns || e.changed_fields_names.size()>max_columns)
            refuse("canonical upstream invalid bounded identity envelope");
        (void)canonical_writer_adapter::uuid_key(e.global_id);
        (void)canonical_writer_adapter::uuid_key(e.global_row_id);
        for(const auto* value:{&e.table_name,&e.operation,&e.timestamp,&e.global_id,&e.global_row_id})
            bounded_add(used,value->size(),limits.delivery_bytes);
        for(const auto& name:e.changed_fields_names) {
            if(name.size()>64)refuse("canonical upstream field name budget exceeded");
            bounded_add(used,name.size(),limits.delivery_bytes);
        }
        for(const auto& [name,value]:e.changed_fields) {
            if(name.size()>64)refuse("canonical upstream field name budget exceeded");
            const auto size=scalar_bytes(value.value);
            if(size>limits.field_bytes)refuse("canonical upstream field byte budget exceeded");
            bounded_add(used,name.size(),limits.delivery_bytes);bounded_add(used,size,limits.delivery_bytes);
        }
    }
}
const std::unordered_map<std::string,column_type>& canonical_upstream_delivery::schema(const std::string& table) const {
    auto it=context_->schemas.find(table);if(it==context_->schemas.end())refuse("canonical upstream unknown fixed relation");return it->second;
}
void canonical_upstream_delivery::execute(const std::string& sql,const std::vector<column_value_t>& params) {
    if(sql.empty() || sql.size()>max_sql || params.size()>2*max_columns+16)
        refuse("canonical upstream SQL/parameter budget exceeded");
    checked_statement statement;const char* tail=nullptr;
    database::record_statement();
    if(sqlite3_prepare_v2(context_->connection,sql.data(),static_cast<int>(sql.size()),&statement.value,&tail)!=SQLITE_OK || !statement.value)
        refuse("canonical upstream statement preparation failed");
    for(const char* p=tail;p<sql.data()+sql.size();++p)if(*p!=' '&&*p!='\t'&&*p!='\r'&&*p!='\n')
        refuse("canonical upstream executor requires one statement");
    if(sqlite3_bind_parameter_count(statement.value)!=static_cast<int>(params.size()))
        refuse("canonical upstream parameter count mismatch");
    // Serialized AuditLog JSON can be six times the admitted raw scalar bytes.
    // Check before SQLite copies it; the input envelope was checked before JSON
    // serialization. This is a logical byte bound, not a total RSS guarantee.
    size_t used=0;const size_t limit=context_->upstream->delivery_bytes*8;
    for(size_t i=0;i<params.size();++i) {
        const size_t size=std::visit([](const auto& v)->size_t {using T=std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,blob>)return v.size();else return 8;},params[i]);
        bounded_add(used,size,limit);bind_checked(statement.value,static_cast<int>(i+1),params[i]);
    }
    if(sqlite3_step(statement.value)!=SQLITE_DONE)refuse("canonical upstream statement execution failed");
    statement.finish();
}
std::vector<database::row_t> canonical_upstream_delivery::query(const std::string& sql,const std::vector<column_value_t>& params) {
    if(sql.empty() || sql.size()>max_sql || params.size()>2*max_columns+16)
        refuse("canonical upstream query budget exceeded");
    checked_statement statement;const char* tail=nullptr;
    database::record_statement();
    if(sqlite3_prepare_v2(context_->connection,sql.data(),static_cast<int>(sql.size()),&statement.value,&tail)!=SQLITE_OK || !statement.value)
        refuse("canonical upstream query preparation failed");
    for(const char* p=tail;p<sql.data()+sql.size();++p)if(*p!=' '&&*p!='\t'&&*p!='\r'&&*p!='\n')
        refuse("canonical upstream query requires one statement");
    if(!sqlite3_stmt_readonly(statement.value) || sqlite3_bind_parameter_count(statement.value)!=static_cast<int>(params.size()))
        refuse("canonical upstream query is not a bounded read");
    size_t used=0;const size_t budget=context_->upstream->delivery_bytes*8;
    for(size_t i=0;i<params.size();++i) {
        const size_t size=std::visit([](const auto& v)->size_t {using T=std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,blob>)return v.size();else return 8;},params[i]);
        bounded_add(used,size,budget);bind_checked(statement.value,static_cast<int>(i+1),params[i]);
    }
    const int columns=sqlite3_column_count(statement.value);
    if(columns<1 || columns>static_cast<int>(max_columns))refuse("canonical upstream query column budget exceeded");
    std::vector<database::row_t> rows;
    for(;;) {
        const int rc=sqlite3_step(statement.value);
        if(rc==SQLITE_DONE)break;
        if(rc!=SQLITE_ROW)refuse("canonical upstream query execution failed");
        if(rows.size()>=64)refuse("canonical upstream query row budget exceeded");
        database::row_t row;
        for(int i=0;i<columns;++i) {
            const char* label=sqlite3_column_name(statement.value,i);
            if(!label || std::strlen(label)>64)refuse("canonical upstream query name budget exceeded");
            bounded_add(used,std::strlen(label),budget);
            column_value_t value=nullptr;
            switch(sqlite3_column_type(statement.value,i)) {
            case SQLITE_NULL:break;
            case SQLITE_INTEGER:value=static_cast<int64_t>(sqlite3_column_int64(statement.value,i));bounded_add(used,8,budget);break;
            case SQLITE_FLOAT:value=sqlite3_column_double(statement.value,i);bounded_add(used,8,budget);break;
            case SQLITE_TEXT:case SQLITE_BLOB: {
                const bool text=sqlite3_column_type(statement.value,i)==SQLITE_TEXT;
                // SQLite metadata reports length before copying the value into
                // C++ storage; each addressed value and the entire read are capped.
                const int bytes=sqlite3_column_bytes(statement.value,i);
                if(bytes<0 || static_cast<size_t>(bytes)>std::max<size_t>(128,context_->upstream->field_bytes))
                    refuse("canonical upstream query field byte budget exceeded");
                bounded_add(used,static_cast<size_t>(bytes),budget);
                const void* data=text?static_cast<const void*>(sqlite3_column_text(statement.value,i)):sqlite3_column_blob(statement.value,i);
                if(!data && (text||bytes))refuse("canonical upstream query value conversion failed");
                if(text)value=std::string(static_cast<const char*>(data),static_cast<size_t>(bytes));
                else if(bytes)value=blob(static_cast<const uint8_t*>(data),static_cast<const uint8_t*>(data)+bytes);
                else value=blob{};
                break;
            }
            default:refuse("canonical upstream unsupported query value");
            }
            if(!row.emplace(label,std::move(value)).second)refuse("canonical upstream duplicate result column");
        }
        rows.push_back(std::move(row));
    }
    statement.finish();return rows;
}
std::string canonical_upstream_delivery::entry_guard() const {
    return "lattice_canonical_entry_v1("+literal(original_)+","+literal(entry_->table_name)+","+literal(target_)+")=1";
}
void canonical_upstream_delivery::script(const std::string& sql) {
    if(sql.size()>max_sql)refuse("canonical upstream kernel budget exceeded");
    const char* at=sql.data();const char* end=at+sql.size();
    while(at<end) {
        checked_statement statement;const char* next=nullptr;
        database::record_statement();
        if(sqlite3_prepare_v2(context_->connection,at,static_cast<int>(end-at),&statement.value,&next)!=SQLITE_OK || next<=at)
            refuse("canonical upstream kernel preparation failed");
        at=next;if(!statement.value)continue;
        int rc;do {rc=sqlite3_step(statement.value);}while(rc==SQLITE_ROW);
        if(rc!=SQLITE_DONE)refuse("canonical upstream kernel condition or mutation failed");
        statement.finish();
    }
}
void canonical_upstream_delivery::begin_entry(const audit_log_entry& entry) {
    if(entry_ || current_==this || !context_->active->load(std::memory_order_acquire))
        refuse("canonical upstream entry capability unavailable");
    // The loop, not autocommit, establishes actual ownership. This check only
    // detects lost transaction state after that admission.
    if(sqlite3_get_autocommit(context_->connection)!=0 || sqlite3_txn_state(context_->connection,"main")!=SQLITE_TXN_WRITE)
        refuse("canonical upstream owned entry transaction was lost");
    original_=canonical_writer_adapter::uuid_key(entry.global_id);
    target_=canonical_writer_adapter::uuid_key(entry.global_row_id);
    entry_=&entry;previous_=current_;current_=this;
    try {
        if(namespace_admission_)canonical_writer_adapter::validate_namespace_admission(owner_,writer_,context_,*namespace_admission_);
        script(guard(context_->profile,entry_guard(),context_->namespace_profile()));
        if(namespace_admission_)script(demand(namespace_condition(namespace_admission_->namespace_,
            namespace_admission_->namespace_.namespace_id==context_->namespaces->local_namespace),entry_guard()));
    }
    catch(...) {end_entry();throw;}
}
void canonical_upstream_delivery::end_entry() noexcept {
    current_=previous_;previous_=nullptr;entry_=nullptr;finalizing_=false;
    original_.clear();target_.clear();
}
bool canonical_upstream_delivery::entry_scope::duplicate() const {
    auto& d=delivery_;
    const auto rows=d.query("SELECT position,outcome,charge,"
        "CASE WHEN typeof(relation)='blob' AND length(relation)<=64 THEN relation END AS relation,"
        "CASE WHEN typeof(identity)='blob' AND length(identity)=36 THEN identity END AS identity "
        +std::string(d.namespace_admission_?",CASE WHEN typeof(namespace_id)='blob' AND length(namespace_id) BETWEEN 1 AND 256 THEN namespace_id END AS namespace_id ":"")+
        "FROM main._lattice_canonical_receipt WHERE original_id=?",{bytes(d.original_)});
    if(rows.empty())return false;
    if(rows.size()!=1)refuse("canonical upstream duplicate receipt shape");
    const auto& r=rows[0];
    if(d.namespace_admission_ && (!std::holds_alternative<blob>(r.at("namespace_id")) ||
       std::get<blob>(r.at("namespace_id"))!=bytes(d.namespace_admission_->namespace_.namespace_id)))
        refuse("canonical original ID belongs to a different namespace");
    const auto position=integer(r,"position"),outcome=integer(r,"outcome");
    const auto head=d.query("SELECT head FROM main._lattice_canonical_store WHERE id=1");
    if(head.size()!=1 || position<1 || position>integer(head[0],"head") || outcome<1 || outcome>3 ||
        integer(r,"charge")!=static_cast<int64_t>(32+36+d.entry_->table_name.size()+36+(d.namespace_admission_?d.namespace_admission_->namespace_.namespace_id.size():0)) ||
        !std::holds_alternative<blob>(r.at("relation")) || std::get<blob>(r.at("relation"))!=bytes(d.entry_->table_name) ||
        !std::holds_alternative<blob>(r.at("identity")) || std::get<blob>(r.at("identity"))!=bytes(d.target_))
        refuse("canonical upstream retained receipt corrupt or different target");
    return true; // First retained outcome wins; replacement payload is never interpreted.
}
void canonical_upstream_delivery::entry_scope::validate_payload() const {
    const auto& d=delivery_;const auto& e=*d.entry_;const auto& columns=d.schema(e.table_name);
    if(e.operation!="INSERT"&&e.operation!="UPDATE"&&e.operation!="DELETE")refuse("canonical upstream unsupported operation");
    if(e.timestamp.find('\0')!=std::string::npos)refuse("canonical upstream malformed timestamp bytes");
    if(e.synthesized && e.operation!="INSERT")refuse("canonical upstream unsupported synthesized operation");
    std::set<std::string> names;
    for(const auto& name:e.changed_fields_names) {
        if(name=="id"||name=="globalId"||!columns.count(name)||!names.insert(name).second)
            refuse("canonical upstream unknown/identity/duplicate changed field");
        if(!e.changed_fields.count(name))refuse("canonical upstream missing changed value");
    }
    for(const auto& [name,value]:e.changed_fields) {
        if(!columns.count(name)||name=="id"||name=="globalId")refuse("canonical upstream unknown payload field");
        // Unchanged fields may carry the generated audit's null placeholders.
        if(!names.count(name))continue;
        const auto type=columns.at(name);bool valid=false;
        switch(value.kind) {
        case any_property_kind::null_kind: valid=std::holds_alternative<std::nullptr_t>(value.value);break;
        case any_property_kind::int_kind:case any_property_kind::int64_kind:
            valid=std::holds_alternative<int64_t>(value.value)&&(type==column_type::integer||type==column_type::real);break;
        case any_property_kind::float_kind:case any_property_kind::double_kind:case any_property_kind::date_kind:
            valid=std::holds_alternative<double>(value.value)&&std::isfinite(std::get<double>(value.value))&&type==column_type::real;break;
        case any_property_kind::string_kind:
            valid=std::holds_alternative<std::string>(value.value)&&type==column_type::text;
            if(std::holds_alternative<std::string>(value.value)&&type==column_type::blob) {
                const auto& hex=std::get<std::string>(value.value);valid=hex.size()%2==0;
                for(char c:hex)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))valid=false;
            }
            break;
        case any_property_kind::data_kind:
            valid=std::holds_alternative<blob>(value.value)&&type==column_type::blob;
            if(std::holds_alternative<std::string>(value.value)&&type==column_type::blob) {
                const auto& hex=std::get<std::string>(value.value);valid=hex.size()%2==0;
                for(char c:hex)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))valid=false;
            }
            break;
        }
        if(!valid)refuse("canonical upstream malformed or incompatible typed value");
        if(e.operation!="DELETE" && value.is_null() && d.context_->no_history.at(e.table_name).count(name))
            refuse("canonical upstream unresolved NoHistory value");
        if(d.context_->relations.count(e.table_name) && (name=="lhs"||name=="rhs") && !value.is_null()) {
            if(!std::holds_alternative<std::string>(value.value))refuse("canonical upstream invalid link target");
            (void)canonical_writer_adapter::uuid_key(std::get<std::string>(value.value));
        }
    }
}
void canonical_upstream_delivery::entry_scope::accept(canonical_receipt_outcome outcome) {
    auto& d=delivery_;
    if(outcome!=canonical_receipt_outcome::applied && outcome!=canonical_receipt_outcome::no_op)
        refuse("canonical upstream cannot create a policy-only acceptance");
    if(d.finalizing_)refuse("canonical upstream duplicate finalizer");
    const auto before=d.query("SELECT head,receipts,receipt_bytes FROM main._lattice_canonical_store WHERE id=1");
    if(before.size()!=1)refuse("canonical upstream finalizer state missing");
    const int64_t head=integer(before[0],"head"),receipts=integer(before[0],"receipts"),receipt_bytes=integer(before[0],"receipt_bytes");
    const int64_t charge=static_cast<int64_t>(32+36+d.entry_->table_name.size()+36+(d.namespace_admission_?d.namespace_admission_->namespace_.namespace_id.size():0));
    if(head==INT64_MAX || receipts==INT64_MAX || receipt_bytes>INT64_MAX-charge)
        refuse("canonical upstream finalizer sequence/counter exhausted");
    d.finalizing_=true;
    try {
        d.script(mutation(d.context_->profile,d.entry_->table_name,"CAST("+literal(d.target_)+" AS TEXT)",
            "CAST("+literal(d.original_)+" AS TEXT)",static_cast<int64_t>(outcome),d.entry_guard(),d.context_->namespace_profile(),
            d.namespace_admission_?d.namespace_admission_->namespace_.namespace_id:std::string{}));
        const auto after=d.query("SELECT head,receipts,receipt_bytes FROM main._lattice_canonical_store WHERE id=1");
        if(after.size()!=1 || integer(after[0],"head")!=head+1 || integer(after[0],"receipts")!=receipts+1 ||
            integer(after[0],"receipt_bytes")!=receipt_bytes+charge)
            refuse("canonical upstream finalizer counter write was ignored");
        d.finalizing_=false;
    } catch(...) {d.finalizing_=false;throw;}
}

} // namespace lattice::detail
