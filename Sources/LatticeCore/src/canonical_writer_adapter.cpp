#include "canonical_writer_adapter.hpp"
#include "recovery_writer_access.hpp"
#include <atomic>
#include <array>
#include <cstring>
#include <map>
#include <set>
#include <utility>

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
std::string demand(const std::string& condition) {
    return " SELECT CASE WHEN ("+condition+") THEN 1 ELSE RAISE(ABORT,'canonical marker admission refused') END;";
}
std::string guard(const canonical_writer_profile& p) {
    const auto limits=" AND typeof(max_markers)='integer' AND typeof(max_marker_bytes)='integer'"
        " AND typeof(max_receipts)='integer' AND typeof(max_receipt_bytes)='integer'"
        " AND typeof(max_batch)='integer' AND typeof(max_identity)='integer' AND typeof(max_operation)='integer'"
        " AND max_markers="+std::to_string(p.limits.markers)+
        " AND max_marker_bytes="+std::to_string(p.limits.marker_bytes)+" AND max_receipts="+std::to_string(p.limits.receipts)+
        " AND max_receipt_bytes="+std::to_string(p.limits.receipt_bytes)+" AND max_batch="+std::to_string(p.limits.batch_identities)+
        " AND max_identity="+std::to_string(p.limits.identity_bytes)+" AND max_operation="+std::to_string(p.limits.operation_bytes);
    return demand("lattice_canonical_guard_v1("+literal(p.binding.source)+","+literal(p.binding.epoch)+","+
        literal(p.binding.scope)+","+literal(p.binding.schema)+")=1")+
        demand("(SELECT COUNT(*) FROM _lattice_canonical_store WHERE id=1 AND version=1 AND source="+
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
                     const std::string& identity,const std::string& original={}) {
    const auto relation=literal(table), key="lattice_canonical_uuid_v1("+identity+")";
    const auto where="relation="+relation+" AND identity="+key;
    const auto charge=std::to_string(24+table.size()+36);
    auto sql=guard(p)+demand(key+" IS NOT NULL");
    if(original.empty()) {
        const auto fresh="(NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+"))";
        sql+=demand("NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+
          " AND (typeof(position)!='integer' OR position<=(SELECT floor FROM _lattice_canonical_store) OR position>(SELECT head FROM _lattice_canonical_store) OR typeof(charge)!='integer' OR charge!="+charge+"))");
        sql+=" UPDATE _lattice_canonical_store SET head=head+1,markers=markers+"+fresh+",marker_bytes=marker_bytes+"+fresh+"*"+charge+
          " WHERE id=1 AND head<9223372036854775807 AND "+fresh+"<=max_markers-markers AND "+fresh+"*"+charge+"<=max_marker_bytes-marker_bytes;";
        sql+=demand("changes()=1");
        sql+=" UPDATE _lattice_canonical_touch SET position=(SELECT head FROM _lattice_canonical_store) WHERE "+where+";";
        sql+=" INSERT INTO _lattice_canonical_touch(relation,identity,position,charge) SELECT "+relation+","+key+",head,"+charge+
          " FROM _lattice_canonical_store WHERE id=1 AND NOT EXISTS(SELECT 1 FROM _lattice_canonical_touch WHERE "+where+");";
        sql+=demand("(SELECT COUNT(*) FROM _lattice_canonical_touch WHERE "+where+" AND position=(SELECT head FROM _lattice_canonical_store) AND charge="+charge+")=1");
    } else {
        const auto op="lattice_canonical_uuid_v1("+original+")";
        const auto receipt_charge=std::to_string(32+36+table.size()+36);
        // Generated local UUIDs cannot dedup AFTER a second model effect. A
        // collision refuses that whole statement instead of keeping the effect.
        sql+=demand(op+" IS NOT NULL AND NOT EXISTS(SELECT 1 FROM _lattice_canonical_receipt WHERE original_id="+op+")");
        sql+=" UPDATE _lattice_canonical_store SET head=head+1,receipts=receipts+1,receipt_bytes=receipt_bytes+"+receipt_charge+
          " WHERE id=1 AND head<9223372036854775807 AND receipts<max_receipts AND "+receipt_charge+"<=max_receipt_bytes-receipt_bytes;";
        sql+=demand("changes()=1");
        sql+=" INSERT INTO _lattice_canonical_receipt(original_id,position,outcome,relation,identity,charge) SELECT "+op+",head,1,"+
          relation+","+key+","+receipt_charge+" FROM _lattice_canonical_store WHERE id=1;";
        sql+=demand("changes()=1")+demand("(SELECT COUNT(*) FROM _lattice_canonical_receipt WHERE original_id="+op+
          " AND position=(SELECT head FROM _lattice_canonical_store) AND outcome=1 AND relation="+relation+" AND identity="+key+" AND charge="+receipt_charge+")=1");
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
            if(!self.active->load(std::memory_order_acquire)||!schema||std::strcmp(schema,"main")||!origin)return SQLITE_DENY;
            // Heterogeneous lookup avoids allocation from SQLite C frames.
            for(const auto& name:self.programs)if(name==origin)return SQLITE_OK;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
};
std::string canonical_writer_adapter::uuid_key(const std::string& value) {
    if(value.size()!=36)refuse("canonical requires UUID identity");
    char out[36];if(!uuid(reinterpret_cast<const unsigned char*>(value.data()),static_cast<int>(value.size()),out))refuse("canonical requires UUID identity");return {out,36};
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach(lattice_db& owner,const canonical_writer_profile& profile) {
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(owner,profile));
}
canonical_writer_adapter::~canonical_writer_adapter() {
    if(context_) {
        context_->active->store(false,std::memory_order_release);
    }
}
canonical_writer_adapter::canonical_writer_adapter(lattice_db& owner,const canonical_writer_profile& p) {
    // The attachment owns its setup transaction. It cannot attach during caller
    // work, on an active synchronizer, or claim adoption of another connection.
    if(p.upstream_requested || owner.config_.is_sync_enabled() || owner.config_.is_ipc_enabled())
        refuse("canonical Slice A refuses upstream/transport activation");
    if(owner.is_closed() || owner.config_.read_only || owner.db_->is_closed() || owner.db_->is_in_transaction())
        refuse("canonical attachment requires an idle live writer");
    if(p.models.empty() || p.models.size()>max_tables || p.limits.identity_bytes<64 || p.limits.operation_bytes<36)
        refuse("canonical unsupported scope/identity budget");
    for(const auto& name:p.models)if(!identifier(name))refuse("canonical invalid bounded model name");
    canonical_change_store store(owner,p.binding,p.limits); // Validates before copying/registration.
    writer_=owner.db_;
    if(writer_->canonical_callback_custody_) {
        auto old=std::static_pointer_cast<context>(writer_->canonical_callback_custody_);
        if(old->active->load(std::memory_order_acquire))refuse("canonical writer already attached");
    }
    // No external callback may coexist on this private profile's writer.
    // The same attachment's revoked callback has connection-owned custody.
    context_=std::make_shared<context>();context_->connection=writer_->internal_handle();context_->binding=p.binding;
    const auto register_shared=[&] {
        auto* held=new std::shared_ptr<context>(context_);
        if(sqlite3_create_function_v2(context_->connection,"lattice_canonical_guard_v1",4,SQLITE_UTF8,held,
              context::admit,nullptr,nullptr,[](void* x){delete static_cast<std::shared_ptr<context>*>(x);})!=SQLITE_OK)
            refuse("canonical guard registration failed"); // SQLite owns/destructs userdata on failure.
        if(sqlite3_create_function_v2(context_->connection,"lattice_canonical_uuid_v1",1,SQLITE_UTF8|SQLITE_DETERMINISTIC,
              nullptr,uuid_sql,nullptr,nullptr,nullptr)!=SQLITE_OK)refuse("canonical UUID registration failed");
    };
    bool began=false;
    try {
        if(writer_->canonical_callback_custody_)sqlite3_set_authorizer(writer_->internal_handle(),nullptr,nullptr);
        register_shared();
        writer_->execute("PRAGMA recursive_triggers=ON");
        if(integer(writer_->query("PRAGMA recursive_triggers").at(0),"recursive_triggers")!=1)
            refuse("canonical REPLACE coverage needs recursive triggers");
        owner.begin_transaction();began=true;
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
        if(!writer_->query("SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND substr(tbl_name,1,19)='_lattice_canonical_' LIMIT 1").empty())
            refuse("canonical metadata has unapproved triggers");
        std::map<std::string,table_plan> tables;
        std::set<std::string> models(p.models.begin(),p.models.end());
        if(models.size()!=p.models.size())refuse("canonical duplicate scoped model");
        for(const auto& name:models) {
            if(!identifier(name) || name[0]=='_')refuse("canonical unsupported model name");
            auto* schema=schema_registry::instance().get_schema(name);
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
                if(prop.kind!=property_kind::list)plan.columns.emplace_back(prop.name,prop.type);
                if(prop.no_history)plan.no_history.insert(prop.name);
            }
            tables.emplace(name,std::move(plan));
        }
        // Complete connected regular-link closure, including incoming links.
        const auto all_schemas=schema_registry::instance().all_schemas();
        if(all_schemas.size()>256)refuse("canonical schema inventory budget exceeded");
        for(auto* schema:all_schemas)for(const auto& prop:schema->properties) {
            if(prop.kind!=property_kind::link&&prop.kind!=property_kind::list)continue;
            if(!models.count(schema->table_name)&&!models.count(prop.target_table))continue;
            if(prop.is_geo_bounds || prop.target_table.empty() || !models.count(schema->table_name)||!models.count(prop.target_table))
                refuse("canonical incomplete/unsupported relationship closure");
            const auto name="_"+schema->table_name+"_"+prop.target_table+"_"+prop.name;
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
        size_t existing=0;
        for(auto& [name,table]:tables) {
            auto ddl=writer_->query("SELECT CASE WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='table' AND name=?",{name});
            if(ddl.size()!=1)refuse("canonical missing table");
            table.table_sql=string(ddl[0],"sql");
            if(table.table_sql.find("globalId TEXT UNIQUE COLLATE NOCASE")==std::string::npos ||
               table.table_sql.find("CREATE VIRTUAL")!=std::string::npos || table.table_sql.find("WITHOUT ROWID")!=std::string::npos)
                refuse("canonical unsupported identity/table shape");
            auto cols=writer_->query("SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END AS name,CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END AS type,hidden FROM pragma_table_xinfo(?) LIMIT 35",{name});
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
                mutation(p,name,target,original);
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
                installed.push_back({trigger,"CREATE TRIGGER "+trigger+" AFTER "+event+" ON "+name+" BEGIN"+mutation(p,name,identity)+" END"});
            }
            const auto identity_guard="_lattice_canonical_"+name+"_identity";
            auto same="CAST(OLD.globalId AS BLOB) IS CAST(NEW.globalId AS BLOB)";
            installed.push_back({identity_guard,"CREATE TRIGGER "+identity_guard+" BEFORE UPDATE ON "+name+" BEGIN"+
                guard(p)+demand(same+(table.link?std::string{}:" AND OLD.id IS NEW.id"))+" END"});
            const auto indexes=writer_->query("SELECT CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN sql IS NULL THEN '' WHEN length(CAST(sql AS BLOB))<=262144 THEN sql END AS sql FROM main.sqlite_master WHERE type='index' AND tbl_name=? ORDER BY name LIMIT 33",{name});
            if(indexes.size()>32)refuse("canonical too many source indexes");
            std::string definitions=name+"\n"+table.table_sql+"\n";
            for(const auto& index:indexes) {
                const auto index_name=string(index,"name"), index_sql=string(index,"sql");
                if(index_name.size()>128 || index_sql.size()>max_sql || definitions.size()>max_sql-index_sql.size())
                    refuse("canonical index descriptor budget exceeded");
                definitions+=index_name+"\n"+index_sql+"\n";
            }
            if(definitions.size()>max_sql || manifest.size()>max_sql-definitions.size())refuse("canonical descriptor budget exceeded");
            manifest+=definitions;
        }
        for(const auto& program:installed) {
            if(program.sql.size()>max_sql || manifest.size()>max_sql-program.sql.size())refuse("canonical generated SQL budget exceeded");
            manifest+=normalized(program.sql)+"\n";
            context_->programs.insert(program.name);
        }
        if(manifest.size()>max_sql)refuse("canonical manifest budget exceeded");
        const bool reopen=writer_->table_exists("_lattice_canonical_coverage");
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
        store.audit();
        owner.commit();began=false;
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
        writer_->canonical_callback_custody_=context_;
        writer_->canonical_write_allowed_=context_->active;
        sqlite3_set_authorizer(context_->connection,context::authorize,context_.get());
        if(cleanup)throw canonical_store_error(canonical_store_error_code::cleanup_failed,
            "canonical attachment rollback failed; writer remains refused",primary,cleanup);
        std::rethrow_exception(primary);
    }
}
void require_canonical_relation(database& db,const std::string& name) {
    if(!db.canonical_callback_custody_)refuse("canonical relation has no admitted callback custody");
    const auto state=std::static_pointer_cast<canonical_writer_adapter::context>(db.canonical_callback_custody_);
    if(!state->active->load(std::memory_order_acquire)||!state->relations.count(name))
        refuse("canonical relation is outside the complete admitted scope");
}
} // namespace lattice::detail
