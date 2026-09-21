#include "recovery_export_adapter.hpp"
#include <cmath>
#include <algorithm>
#include <array>
#include <utility>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>

namespace lattice::detail {
namespace recovery_export_test_hooks {
thread_local void (*before_claim_commit)()=nullptr;
thread_local void (*after_claim_commit)()=nullptr;
}
namespace {
[[noreturn]] void refuse(const char* message){throw db_error(message);}
struct statement {
    sqlite3_stmt* p=nullptr;
    statement(sqlite3* db,const std::string& sql){
        database::record_statement();const int rc=sqlite3_prepare_v2(db,sql.c_str(),-1,&p,nullptr);
        if(rc!=SQLITE_OK||!p||!sqlite3_stmt_readonly(p)){sqlite3_finalize(p);p=nullptr;refuse("export bounded read preparation failed");}
    }
    ~statement(){sqlite3_finalize(p);}
    statement(const statement&)=delete;
    void integer(int at,int64_t value){if(sqlite3_bind_int64(p,at,value)!=SQLITE_OK)refuse("export integer binding failed");}
    void text(int at,const std::string& value){
        if(value.size()>static_cast<size_t>(std::numeric_limits<int>::max())||
           sqlite3_bind_text(p,at,value.data(),static_cast<int>(value.size()),SQLITE_STATIC)!=SQLITE_OK)refuse("export text binding failed");
    }
    bool next(){const int rc=sqlite3_step(p);if(rc==SQLITE_ROW)return true;if(rc==SQLITE_DONE)return false;refuse("export bounded read failed");}
};
struct budget {
    const recovery_export_limits& limits;size_t used=0;
    void charge(size_t n){if(n>limits.field_bytes||n>limits.raw_bytes-used)refuse("export raw byte budget exceeded before copy");used+=n;}
};
int64_t integer(statement& s,int at){if(sqlite3_column_type(s.p,at)!=SQLITE_INTEGER)refuse("export expected INTEGER");return sqlite3_column_int64(s.p,at);}
std::string text(statement& s,int at,budget& b){
    if(sqlite3_column_type(s.p,at)!=SQLITE_TEXT)refuse("export expected TEXT");
    const int n=sqlite3_column_bytes(s.p,at);if(n<0)refuse("export invalid text length");b.charge(static_cast<size_t>(n));
    const auto* p=sqlite3_column_text(s.p,at);if(!p)refuse("export unavailable text");return {reinterpret_cast<const char*>(p),static_cast<size_t>(n)};
}
column_value_t scalar(statement& s,int at,budget& b){
    switch(sqlite3_column_type(s.p,at)){
    case SQLITE_NULL:b.charge(0);return nullptr;
    case SQLITE_INTEGER:b.charge(8);return static_cast<int64_t>(sqlite3_column_int64(s.p,at));
    case SQLITE_FLOAT:{b.charge(8);const double v=sqlite3_column_double(s.p,at);if(!std::isfinite(v))refuse("export nonfinite scalar");return v;}
    case SQLITE_TEXT:return text(s,at,b);
    case SQLITE_BLOB:{const int n=sqlite3_column_bytes(s.p,at);if(n<0)refuse("export invalid blob length");b.charge(static_cast<size_t>(n));if(!n)return std::vector<uint8_t>{};
        const auto* p=static_cast<const uint8_t*>(sqlite3_column_blob(s.p,at));if(!p)refuse("export unavailable blob");return std::vector<uint8_t>(p,p+n);}
    default:refuse("export unsupported scalar");
    }
}
std::string quote_identifier(const std::string& name){
    if(name.empty()||name.size()>64)refuse("export invalid admitted identifier");
    for(unsigned char c:name)if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_'))refuse("export invalid admitted identifier");
    return "\""+name+"\"";
}
void limits_ok(const recovery_export_limits& l,size_t count,const std::vector<int64_t>& in_flight){
    if(!l.entries||l.entries>1000||!count||count>l.entries||in_flight.size()>2000||
       !l.field_bytes||l.field_bytes>1048576||l.raw_bytes<l.field_bytes||l.raw_bytes>4194304||
       l.wire_bytes<16||l.wire_bytes>8388608||!l.coverage_candidates||l.coverage_candidates>4096)refuse("export independent limits exceeded");
    for(const auto id:in_flight)if(id<=0)refuse("export synthetic in-flight original unsupported");
}
struct raw_audit {
    audit_log_entry entry;std::string fields,names;
    column_value_t persisted_timestamp;
};
std::string wire_timestamp(const column_value_t& value,budget& b){
    // Real generated AuditLog rows use numeric epoch seconds. The wire field
    // remains text, including older textual originals, but conversion must
    // neither discard fractional seconds nor invent a replacement timestamp.
    if(const auto* legacy=std::get_if<std::string>(&value))return *legacy;
    std::string encoded;
    if(const auto* seconds=std::get_if<int64_t>(&value))encoded=std::to_string(*seconds);
    else if(const auto* seconds=std::get_if<double>(&value)){
        std::ostringstream stream;stream.imbue(std::locale::classic());
        stream<<std::setprecision(std::numeric_limits<double>::max_digits10)<<*seconds;
        if(!stream)refuse("export timestamp encoding failed");encoded=stream.str();
    }else refuse("export timestamp is not numeric or text");
    b.charge(encoded.size());return encoded;
}
raw_audit read_audit(sqlite3* db,int64_t id,budget& b){
    statement s(db,"SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized FROM main.AuditLog WHERE id=? LIMIT 2");s.integer(1,id);
    if(!s.next())refuse("export original disappeared");raw_audit r;auto& e=r.entry;
    e.id=integer(s,0);e.global_id=text(s,1,b);e.table_name=text(s,2,b);e.operation=text(s,3,b);e.row_id=integer(s,4);e.global_row_id=text(s,5,b);
    r.fields=text(s,6,b);r.names=text(s,7,b);r.persisted_timestamp=scalar(s,8,b);
    e.timestamp=wire_timestamp(r.persisted_timestamp,b);
    const auto remote=integer(s,9),synced=integer(s,10),synthesized=integer(s,11);
    if(e.id!=id||id<=0||e.global_id.size()!=36||e.global_row_id.size()!=36||e.table_name.empty()||
       (e.operation!="INSERT"&&e.operation!="UPDATE"&&e.operation!="DELETE")||e.row_id<0||remote!=0||synthesized!=0||(synced!=0&&synced!=1))
        refuse("export requires local persisted original shape");
    e.is_synchronized=synced!=0;
    if(s.next())refuse("export ambiguous original");return r;
}
bool same_original(const raw_audit& a,const raw_audit& b){
    const auto& x=a.entry;const auto& y=b.entry;
    return x.id==y.id&&x.global_id==y.global_id&&x.table_name==y.table_name&&x.operation==y.operation&&x.row_id==y.row_id&&
        x.global_row_id==y.global_row_id&&x.timestamp==y.timestamp&&a.persisted_timestamp==b.persisted_timestamp&&
        x.is_synchronized==y.is_synchronized&&a.fields==b.fields&&a.names==b.names;
}
bool has_column(const recovery_local_export_table& table,const std::string& name){
    if(table.regular_link)return name=="lhs"||name=="rhs";
    for(const auto& c:table.columns)if(c.first==name)return true;
    return false;
}
void decode_generated(sqlite3* db,raw_audit& row,const recovery_local_export_table& table,budget& b,
    bool retained_later_delete=false){
    auto& e=row.entry;
    {statement valid(db,"SELECT json_valid(?1),json_valid(?2),CASE WHEN json_valid(?1) THEN json_type(?1) END,CASE WHEN json_valid(?2) THEN json_type(?2) END");
     valid.text(1,row.fields);valid.text(2,row.names);
     if(!valid.next()||integer(valid,0)!=1||integer(valid,1)!=1||text(valid,2,b)!="object"||text(valid,3,b)!="array")refuse("export malformed generated JSON");}
    std::set<std::string> keys,changed;
    {statement fields(db,"SELECT key,type,atom FROM json_each(?) LIMIT 33");fields.text(1,row.fields);
     while(fields.next()){
        if(keys.size()>=32)refuse("export generated field count exceeded");const auto key=text(fields,0,b);const auto type=text(fields,1,b);
        if(!has_column(table,key)||!keys.insert(key).second||type=="object"||type=="array"||type=="true"||type=="false")refuse("export unsupported or duplicate generated field");
        e.changed_fields.emplace(key,any_property::from_column_value(scalar(fields,2,b)));
     }}
    {statement names(db,"SELECT type,atom FROM json_each(?) LIMIT 33");names.text(1,row.names);size_t count=0;
     while(names.next()){
        if(++count>32)refuse("export generated name count exceeded");const auto type=text(names,0,b);if(type=="null")continue;
        if(type!="text")refuse("export generated name is not text");const auto name=text(names,1,b);
        if(!keys.count(name)||!changed.insert(name).second)refuse("export missing or duplicate generated name");e.changed_fields_names.push_back(name);
     }}
    if(table.regular_link&&e.operation=="UPDATE")refuse("export unsupported link update");
    if(e.operation=="UPDATE")for(const auto& column:table.no_history){
        if(!changed.count(column))continue;
        statement current(db,"SELECT "+quote_identifier(column)+" FROM main."+quote_identifier(table.name)+" WHERE globalId=? LIMIT 2");current.text(1,e.global_row_id);
        if(!current.next()){
            if(!retained_later_delete)refuse("export NoHistory current row is absent");
            // Only the wire projection changes. The original names/fields and
            // generated stamp remain intact and are verified again precommit.
            e.changed_fields.erase(column);
            e.changed_fields_names.erase(std::remove(e.changed_fields_names.begin(),e.changed_fields_names.end(),column),e.changed_fields_names.end());
            continue;
        }
        e.changed_fields[column]=any_property::from_column_value(scalar(current,0,b));
        if(current.next())refuse("export NoHistory target is ambiguous");
    }
}
size_t wire_bound(const audit_log_entry& e,size_t cap){
    size_t n=512;auto add=[&](size_t bytes,size_t multiplier=1){if(n>cap||bytes>(cap-n)/multiplier)refuse("export wire budget exceeded before serialization");n+=bytes*multiplier;};
    for(const auto* s:{&e.global_id,&e.table_name,&e.operation,&e.global_row_id,&e.timestamp})add(s->size(),6);
    for(const auto& name:e.changed_fields_names){add(name.size(),6);add(4);}
    for(const auto& [name,p]:e.changed_fields){add(name.size(),6);add(80);
        if(const auto* s=std::get_if<std::string>(&p.value))add(s->size(),6);
        else if(const auto* bytes=std::get_if<std::vector<uint8_t>>(&p.value))add(bytes->size(),2);
    }
    if(n>cap)refuse("export wire budget exceeded before serialization");return n;
}
void check_scopes(const recovery_local_export_inventory& inventory,const std::vector<recovery_local_export_scope>& expected){
    if(inventory.scopes.size()!=expected.size())refuse("export scope inventory changed");
    for(size_t i=0;i<expected.size();++i){const auto& a=inventory.scopes[i];const auto& b=expected[i];
        if(a.contribution.address!=b.contribution.address||a.contribution.profile!=b.contribution.profile||
           a.contribution.mode!=recovery_obligation_mode::recording||a.program_digest!=b.program_digest||a.program_revision!=b.program_revision)
            refuse("export contribution generation or program changed");
    }
}
void check_claims(recovery_obligation_store& journal,const std::vector<recovery_obligation_export_ticket>& claims,const std::vector<audit_log_entry>& entries){
    for(const auto& claim:claims)for(const auto& id:claim.canonical_original_ids){
        const auto found=journal.find(claim.address,id);if(!found||!found->first_export_claim||found->stage!=recovery_obligation_stage::open)refuse("export final claim disappeared");
        const audit_log_entry* actual=nullptr;for(const auto& e:entries)if(e.id==found->record.audit_id){actual=&e;break;}
        if(!actual||actual->global_id!=found->record.original_id||actual->table_name!=found->record.table||actual->global_row_id!=found->record.target_id)
            refuse("export final claimed identity changed");
    }
}
std::string small_text(statement& s,int at,size_t limit){
    if(sqlite3_column_type(s.p,at)!=SQLITE_TEXT)refuse("export coverage expected TEXT");
    const int n=sqlite3_column_bytes(s.p,at);
    if(n<0||static_cast<size_t>(n)>limit)refuse("export coverage text exceeds bound before copy");
    const auto* value=sqlite3_column_text(s.p,at);
    if(!value)refuse("export coverage unavailable text");
    return {reinterpret_cast<const char*>(value),static_cast<size_t>(n)};
}
std::string compact_index_sql(std::string sql){
    std::string result;result.reserve(sql.size());
    for(unsigned char c:sql){
        if(c==' '||c=='\t'||c=='\n'||c=='\r')continue;
        result+=static_cast<char>(c>='A'&&c<='Z'?c-'A'+'a':c);
    }
    return result;
}
void coverage_indexes(sqlite3* db){
    for(const auto& index:std::array<std::pair<const char*,const char*>,2>{{
        {"idx_sync_state_pending","CREATE INDEX idx_sync_state_pending ON _lattice_sync_state(sync_id, is_synchronized) WHERE is_synchronized = 0"},
        {"idx_audit_log_pending_sync","CREATE INDEX idx_audit_log_pending_sync ON AuditLog(isSynchronized) WHERE isSynchronized = 0"}}}){
        statement query(db,"SELECT sql FROM main.sqlite_schema WHERE type='index' AND name=? LIMIT 2");
        const std::string name(index.first);query.text(1,name);
        if(!query.next())refuse("export coverage required pending index differs");
        const auto actual=compact_index_sql(small_text(query,0,512));const auto expected=compact_index_sql(index.second);
        const auto guarded=std::string("createindexifnotexists")+expected.substr(sizeof("createindex")-1);
        if((actual!=expected&&actual!=guarded)||query.next())
            refuse("export coverage required pending index differs");
    }
}
// This is an addressed stamp check, not a repeated full profile/manifest read.
// Full schema/profile audit and exclusive metadata custody remain prerequisites.
void coverage_stamp(sqlite3* db,const recovery_local_export_inventory& inventory,
    const recovery_local_export_scope& scope,const recovery_obligation_entry& entry){
    statement query(db,"SELECT t.incarnation,t.program_revision,t.audit_id,t.record_sequence,t.generation,t.scope_revision,"
        "t.base_scopes,t.base_records,t.base_bytes,t.base_incarnation,t.base_export,t.producer_profiles,t.producer_stamps,t.producer_bytes,t.bytes,"
        "g.incarnation,g.export_sequence,u.stamps,u.bytes,u.profiles "
        "FROM main._lattice_obligation_producer_stamp t CROSS JOIN main._lattice_obligation_store g CROSS JOIN main._lattice_obligation_producer_store u "
        "WHERE t.channel=CAST(? AS BLOB) AND t.original=CAST(? AS BLOB) AND g.id=1 AND u.id=1 LIMIT 2");
    query.text(1,scope.contribution.address.channel);query.text(2,entry.canonical_original_id);
    if(!query.next())refuse("export coverage pending original lacks generated stamp");
    std::array<int64_t,20> value{};
    for(size_t i=0;i<value.size();++i)value[i]=integer(query,static_cast<int>(i));
    if(query.next())refuse("export coverage ambiguous generated stamp");
    const auto& ol=inventory.limits.obligations;const auto& pl=inventory.limits.producers;
    const auto& address=scope.contribution.address;
    const int64_t stamp_bytes=15*8+36+static_cast<int64_t>(address.channel.size());
    const int64_t entry_bytes=8*8+static_cast<int64_t>(address.channel.size()+entry.record.original_id.size()+
        entry.record.table.size()+entry.record.target_id.size()+entry.canonical_original_id.size()+entry.canonical_target_id.size());
    if(value[0]!=address.incarnation||value[1]!=scope.program_revision||value[2]!=entry.record.audit_id||value[3]!=entry.sequence||
       value[4]<=0||value[4]>address.generation||value[5]<=0||value[5]>scope.contribution.revision||
       value[6]<=0||value[6]>ol.scopes||value[7]<=0||value[7]>ol.records||value[8]<entry_bytes||value[8]>ol.encoded_bytes||
       value[9]<address.incarnation||value[9]>value[15]||value[10]<0||value[10]>value[16]||
       value[11]<=0||value[11]>pl.profiles||value[12]<=0||value[12]>pl.stamps||value[13]<stamp_bytes||value[13]>pl.encoded_bytes||
       value[14]!=stamp_bytes||value[17]<1||value[17]>pl.stamps||value[18]<stamp_bytes||value[18]>pl.encoded_bytes||
       value[19]!=static_cast<int64_t>(inventory.scopes.size()))
        refuse("export coverage generated stamp contradicts current obligation");
}
std::vector<int64_t> covered_pending(sqlite3* db,const std::string& channel,
    const recovery_local_export_inventory& inventory,recovery_obligation_store& journal,size_t cap){
    coverage_indexes(db);
    std::vector<int64_t> explicit_ids,global_ids;
    const auto collect=[&](statement& query,std::vector<int64_t>& ids){
        while(query.next()){
            // Count raw candidates BEFORE joins/state exclusions. LIMIT on a
            // missing-entry anti-join would not bound the work it skips.
            if(ids.size()==cap)refuse("protected export coverage budget exceeded");
            const auto id=integer(query,0);if(id<=0)refuse("export coverage invalid audit identity");ids.push_back(id);
        }
    };
    {statement query(db,"SELECT CASE WHEN typeof(audit_entry_id)='integer' THEN audit_entry_id END "
        "FROM main._lattice_sync_state INDEXED BY idx_sync_state_pending WHERE sync_id=? AND is_synchronized=0 LIMIT ?");
     query.text(1,channel);query.integer(2,static_cast<int64_t>(cap+1));collect(query,explicit_ids);}
    {statement query(db,"SELECT id FROM main.AuditLog INDEXED BY idx_audit_log_pending_sync WHERE isSynchronized=0 LIMIT ?");
     query.integer(1,static_cast<int64_t>(cap+1));collect(query,global_ids);}
    std::set<int64_t> pending(explicit_ids.begin(),explicit_ids.end());
    for(const auto id:global_ids){
        statement state(db,"SELECT is_synchronized FROM main._lattice_sync_state WHERE audit_entry_id=? AND sync_id=? LIMIT 2");
        state.integer(1,id);state.text(2,channel);
        if(!state.next()){pending.insert(id);continue;}
        const auto value=integer(state,0);if((value!=0&&value!=1)||state.next())refuse("export coverage invalid channel state");
        // Explicit zero is already in stream A; one resolves this route.
        if(value==0&&!pending.count(id))refuse("export coverage explicit pending inventory changed");
    }
    std::map<std::string,size_t> tables;
    for(size_t i=0;i<inventory.scopes.size();++i)for(const auto& table:inventory.scopes[i].tables)
        if(!tables.emplace(table.name,i).second)refuse("export coverage ambiguous admitted table");
    for(const auto id:pending){
        statement audit(db,"SELECT id,globalId,tableName,globalRowId,rowId,isFromRemote,synthesized,isSynchronized "
            "FROM main.AuditLog WHERE id=? LIMIT 2");audit.integer(1,id);
        if(!audit.next())refuse("export coverage pending audit disappeared");
        const auto actual_id=integer(audit,0);const auto original=small_text(audit,1,36);
        const auto table=small_text(audit,2,64);const auto target=small_text(audit,3,36);
        const auto row_id=integer(audit,4),remote=integer(audit,5),synthetic=integer(audit,6),synchronized=integer(audit,7);
        if(actual_id!=id||original.size()!=36||target.size()!=36||row_id<0||remote!=0||synthetic!=0||
           (synchronized!=0&&synchronized!=1)||audit.next())refuse("export coverage unsupported pending original");
        const auto found=tables.find(table);if(found==tables.end())refuse("export coverage pending original has no admitted contribution");
        const auto& scope=inventory.scopes[found->second];const auto entry=journal.find(scope.contribution.address,original);
        if(!entry||entry->record.audit_id!=id||entry->record.original_id!=original||entry->record.table!=table||
           entry->record.target_id!=target||entry->record.origin!=recovery_obligation_origin::local_candidate||
           entry->stage!=recovery_obligation_stage::open)
            refuse("export coverage pending original lacks current open obligation");
        coverage_stamp(db,inventory,scope,*entry);
    }
    return {pending.begin(),pending.end()};
}
// Unlike pending coverage, this selector does not exclude any addressed row.
// The integer PK range + LIMIT bounds materialization to the requested page;
// larger retained history is paged, not silently treated as unsupported/empty.
std::vector<int64_t> history_page(sqlite3* db,int64_t after,size_t count){
    statement query(db,"SELECT id FROM main.AuditLog WHERE id>? ORDER BY id LIMIT ?");
    query.integer(1,after);query.integer(2,static_cast<int64_t>(count));
    std::vector<int64_t> ids;int64_t previous=after;
    while(query.next()){
        const auto id=integer(query,0);
        if(ids.size()==count||id<=previous)refuse("export history invalid ordered identity");
        ids.push_back(id);previous=id;
    }
    return ids;
}
void history_original(sqlite3* db,const recovery_local_export_inventory& inventory,
    const recovery_local_export_scope& scope,recovery_obligation_store& journal,const audit_log_entry& row){
    // find() checks normalized UUID identity and the actual persisted original;
    // comparisons preserve its exact AuditLog spelling/body rather than rewrite.
    const auto entry=journal.find(scope.contribution.address,row.global_id);
    if(!entry||entry->record.audit_id!=row.id||entry->record.original_id!=row.global_id||
       entry->record.table!=row.table_name||entry->record.target_id!=row.global_row_id||
       entry->record.origin!=recovery_obligation_origin::local_candidate||entry->stage!=recovery_obligation_stage::open)
        refuse("export history original lacks current open obligation");
    coverage_stamp(db,inventory,scope,*entry);
}
} // namespace

committed_export_frame::committed_export_frame(committed_export_frame&& other) noexcept {
    *this=std::move(other);
}
committed_export_frame& committed_export_frame::operator=(committed_export_frame&& other) noexcept {
    if(this==&other)return *this;
    owner_=std::move(other.owner_);claims_=std::move(other.claims_);scopes_=std::move(other.scopes_);
    limits_=other.limits_;entries_=std::move(other.entries_);message_=std::move(other.message_);
    physical_generation_=std::exchange(other.physical_generation_,0);
    consumed_=std::exchange(other.consumed_,true);
    return *this;
}

void recovery_export_adapter::require_committed(const recovery_install_result& result){
    if(result.state==recovery_install_state::committed)return;
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    refuse("export owned operation did not commit");
}
bool recovery_export_adapter::protected_store(std::shared_ptr<lattice_db> owner){
    return recovery_local_producer_adapter::export_protection_required(std::move(owner));
}
std::optional<bool> recovery_export_adapter::try_protected_store(std::shared_ptr<lattice_db> owner){
    try{return recovery_local_producer_adapter::export_protection_required(std::move(owner));}
    catch(const export_discovery_busy&){return std::nullopt;}
}
std::optional<recovery_export_preparation> recovery_export_adapter::try_prepare_pending(std::shared_ptr<lattice_db> owner,
    const std::string& sync_id,uint64_t generation,size_t count,const std::vector<int64_t>& in_flight,
    bool filtered,const recovery_export_limits& limits){
    bool busy=false;
    auto prepared=prepare(std::move(owner),sync_id,generation,count,in_flight,filtered,limits,std::nullopt,&busy);
    if(busy)return std::nullopt;
    return prepared;
}
recovery_export_preparation recovery_export_adapter::prepare_pending(std::shared_ptr<lattice_db> owner,const std::string& sync_id,
    uint64_t generation,size_t count,const std::vector<int64_t>& in_flight,bool filtered,const recovery_export_limits& limits){
    return prepare(std::move(owner),sync_id,generation,count,in_flight,filtered,limits,std::nullopt);
}
recovery_export_preparation recovery_export_adapter::prepare_history_page(std::shared_ptr<lattice_db> owner,
    uint64_t generation,int64_t after,size_t count,const recovery_export_limits& limits){
    if(after<0)refuse("export history cursor must be a resolved nonnegative PK");
    return prepare(std::move(owner),{},generation,count,{},false,limits,after);
}
recovery_export_preparation recovery_export_adapter::prepare_retained_page(std::shared_ptr<lattice_db> owner,
    uint64_t generation,int64_t after,size_t count,const recovery_export_limits& limits){
    if(after<0)refuse("export retained cursor must be a resolved nonnegative PK");
    return prepare(std::move(owner),{},generation,count,{},false,limits,after,nullptr,true);
}
recovery_export_preparation recovery_export_adapter::prepare(std::shared_ptr<lattice_db> owner,const std::string& sync_id,
    uint64_t generation,size_t count,const std::vector<int64_t>& in_flight,bool filtered,const recovery_export_limits& limits,
    std::optional<int64_t> history_after,bool* discovery_busy,bool retained_delete_page){
    recovery_export_preparation output;
    // Catch only this first no-effect classifier. A busy exception arising
    // later from reentrant work must never replay a claim or mutation stage.
    try {if(!recovery_local_producer_adapter::export_protection_required(owner))return output;}
    catch(const export_discovery_busy&){
        if(!discovery_busy)throw;
        *discovery_busy=true;return output;
    }
    committed_export_frame frame;frame.owner_=owner;frame.physical_generation_=generation;
    const auto result=recovery_writer_access::install(owner,[&](database& writer){
        auto inventory=recovery_local_producer_adapter::export_inventory_for_owned_write(owner);if(inventory.scopes.empty())return;
        limits_ok(limits,count,in_flight);if(!history_after&&(sync_id.empty()||sync_id.size()>4096))refuse("export invalid route channel");
        output.protected_store=true;if(filtered)refuse("protected export refuses legacy filter synthesis");
        for(const auto& scope:inventory.scopes)if(scope.contribution.mode!=recovery_obligation_mode::recording)refuse("protected export contribution is frozen or installed");
        frame.scopes_=inventory.scopes;frame.limits_=inventory.limits;
        auto* db=recovery_writer_access::active_handle(*owner,writer);
        recovery_obligation_store journal(owner,inventory.limits.obligations,inventory.limits.installations);
        std::vector<int64_t> pending,ids;
        if(history_after)ids=history_page(db,*history_after,count);
        else {
            pending=covered_pending(db,sync_id,inventory,journal,limits.coverage_candidates);
            const std::set<int64_t> sending(in_flight.begin(),in_flight.end());
            for(const auto id:pending)if(!sending.count(id)){ids.push_back(id);if(ids.size()==count)break;}
        }
        budget raw{limits};std::vector<raw_audit> originals;std::vector<std::vector<std::string>> by_scope(inventory.scopes.size());
        // Validate the entire finite page's provenance before any later row
        // can justify an earlier UPDATE projection. No caller-provided flag,
        // operation text alone or unselected DELETE is generated evidence.
        std::vector<const recovery_local_export_table*> tables;
        std::vector<size_t> scope_indexes;
        for(const auto id:ids){auto row=read_audit(db,id,raw);const recovery_local_export_table* table=nullptr;size_t scope_index=0;
            for(size_t i=0;i<inventory.scopes.size();++i)for(const auto& t:inventory.scopes[i].tables)if(t.name==row.entry.table_name){if(table)refuse("export ambiguous contribution table");table=&t;scope_index=i;}
            if(!table)refuse("export original has no admitted whole-model contribution");
            if(history_after)history_original(db,inventory,inventory.scopes[scope_index],journal,row.entry);
            tables.push_back(table);scope_indexes.push_back(scope_index);originals.push_back(std::move(row));
        }
        const auto later_delete=[&](size_t index){
            if(!retained_delete_page)return false;
            const auto& current=originals[index].entry;
            if(current.operation!="UPDATE")return false;
            for(size_t next=index+1;next<originals.size();++next){const auto& candidate=originals[next].entry;
                if(candidate.operation=="DELETE"&&candidate.id>current.id&&candidate.table_name==current.table_name&&
                   candidate.global_row_id==current.global_row_id)return true;
            }
            return false;
        };
        std::string encoded="{\"auditLog\":[";
        for(size_t i=0;i<originals.size();++i){auto& row=originals[i];
            decode_generated(db,row,*tables[i],raw,later_delete(i));wire_bound(row.entry,limits.wire_bytes-encoded.size()-2);
            const auto json=row.entry.to_json();if(json.size()+3>limits.wire_bytes-encoded.size())refuse("export encoded frame exceeds budget");
            if(!frame.entries_.empty())encoded+=',';encoded+=json;by_scope[scope_indexes[i]].push_back(row.entry.global_id);frame.entries_.push_back(row.entry);
        }
        encoded+="]}";if(frame.entries_.empty())return;
        std::vector<std::pair<recovery_obligation_address,recovery_obligation_entry>> expected_entries;
        std::vector<recovery_obligation_scope> expected_scopes;
        for(size_t i=0;i<by_scope.size();++i)if(!by_scope[i].empty()){
            const auto& address=inventory.scopes[i].contribution.address;
            frame.claims_.push_back(journal.claim_export(address,by_scope[i]));
            for(const auto& id:by_scope[i]){const auto entry=journal.find(address,id);if(!entry)refuse("export claimed original missing");expected_entries.emplace_back(address,*entry);}
            const auto scope=journal.read(address.channel);if(!scope)refuse("export claimed scope missing");expected_scopes.push_back(*scope);
        }
        if(recovery_export_test_hooks::before_claim_commit)recovery_export_test_hooks::before_claim_commit();
        const auto final_inventory=recovery_local_producer_adapter::export_inventory_for_owned_write(owner);
        check_scopes(final_inventory,frame.scopes_);check_claims(journal,frame.claims_,frame.entries_);
        for(const auto& [address,expected]:expected_entries)if(journal.find(address,expected.record.original_id)!=std::optional<recovery_obligation_entry>(expected))refuse("export cross-contribution final entry changed");
        for(const auto& expected:expected_scopes)if(journal.read(expected.address.channel)!=std::optional<recovery_obligation_scope>(expected))refuse("export cross-contribution final scope changed");
        budget verify{limits};
        for(size_t i=0;i<originals.size();++i){const auto& before=originals[i];auto after=read_audit(db,before.entry.id,verify);
            if(!same_original(before,after))refuse("export original changed after claims");
            if(retained_delete_page){
                // Recheck both actual row absence/value and exact projection
                // after reentrant claim hooks; read errors never prove absence.
                decode_generated(db,after,*tables[i],verify,later_delete(i));
                const auto& projected=frame.entries_[i];
                if(after.entry.changed_fields_names!=projected.changed_fields_names||after.entry.changed_fields.size()!=projected.changed_fields.size())
                    refuse("export retained projection changed after claims");
                for(const auto& [name,value]:after.entry.changed_fields){const auto found=projected.changed_fields.find(name);
                    if(found==projected.changed_fields.end()||found->second.kind!=value.kind||found->second.value!=value.value)
                        refuse("export retained projection changed after claims");
                }
            }
        }
        if(history_after){
            if(history_page(db,*history_after,count)!=ids)refuse("export history page changed during claims");
            for(const auto& scope:final_inventory.scopes)for(const auto& table:scope.tables)
                for(const auto& entry:frame.entries_)if(entry.table_name==table.name)
                    history_original(db,final_inventory,scope,journal,entry);
        }else if(covered_pending(db,sync_id,final_inventory,journal,limits.coverage_candidates)!=pending)
            refuse("export coverage pending inventory changed during claims");
        frame.message_=transport_message::from_binary({encoded.begin(),encoded.end()});
    });
    require_committed(result);
    if(!frame.entries_.empty()){
        if(recovery_export_test_hooks::after_claim_commit)recovery_export_test_hooks::after_claim_commit();
        output.frame=std::move(frame);
    }
    return output;
}
void recovery_export_adapter::acknowledge_legacy(std::shared_ptr<lattice_db> owner,const std::string& channel,const std::vector<std::string>& ids){
    if(channel.empty()||channel.size()>4096||ids.size()>2000)refuse("export legacy ACK bounds exceeded");
    const auto result=recovery_writer_access::install(owner,[&](database& writer){
        const auto inventory=recovery_local_producer_adapter::export_inventory_for_owned_write(owner);
        if(inventory.scopes.empty())refuse("export legacy ACK lost protected inventory");
        recovery_obligation_store journal(owner,inventory.limits.obligations,inventory.limits.installations);
        for(const auto& scope:inventory.scopes)if(scope.contribution.mode!=recovery_obligation_mode::recording)refuse("export legacy ACK belongs to frozen generation");
        std::set<std::string> unique;
        for(const auto& id:ids){
            if(id.size()!=36||!unique.insert(id).second)refuse("export legacy ACK invalid or duplicate original");
            std::optional<recovery_obligation_entry> entry;
            for(const auto& scope:inventory.scopes){const auto found=journal.find(scope.contribution.address,id);if(found){if(entry)refuse("export ambiguous ACK contribution");entry=found;}}
            if(!entry||!entry->first_export_claim||entry->stage!=recovery_obligation_stage::open)refuse("export legacy ACK has no retained claimed original");
            writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1",{entry->record.audit_id,channel});
            const auto rows=writer.query("SELECT 1 AS ok FROM main._lattice_sync_state WHERE audit_entry_id=? AND sync_id=? AND typeof(is_synchronized)='integer' AND is_synchronized=1 LIMIT 2",{entry->record.audit_id,channel});
            if(rows.size()!=1)refuse("export legacy ACK bookkeeping was ignored");
        }
    });
    require_committed(result);
}
void recovery_export_adapter::validate_server_limits(const recovery_export_limits& limits){limits_ok(limits,limits.entries,{});}
void recovery_export_adapter::revalidate_claimed_frame(const committed_export_frame& frame){
    const auto result=recovery_writer_access::install(frame.owner_,[&](database&){
        check_scopes(recovery_local_producer_adapter::export_inventory_for_owned_write(frame.owner_),frame.scopes_);
        recovery_obligation_store journal(frame.owner_,frame.limits_.obligations,frame.limits_.installations);check_claims(journal,frame.claims_,frame.entries_);
    });
    recovery_export_adapter::require_committed(result);
}
recovery_export_route::recovery_export_route(std::shared_ptr<sync_transport> transport,std::shared_ptr<sync_callback_lifetime> lifetime):transport_(std::move(transport)),lifetime_(std::move(lifetime)){}
void recovery_export_route::prepare_protected(uint64_t generation){
#ifdef __EMSCRIPTEN__
    refuse("protected export retirement is not available in the browser graph");
#else
    if(!lifetime_->can_begin_protected(generation))refuse("protected export requires a fresh physical endpoint");
    // Worker launch and capacity reservation happen before any route/handler
    // publication. The reserved slot owns the transport throughout callbacks.
    auto reservation=sync_retirement_lane::instance()->reserve(transport_,lifetime_);
    std::lock_guard<std::mutex> lock(mutex_);
    if(retired_||retirement_)refuse("protected export route already published or retired");
    lifetime_->begin_connect(generation,true);
    retirement_.emplace(std::move(reservation));retirement_->publish();protected_=true;
#endif
}
bool recovery_export_route::retire_protected(std::thread pacer)noexcept{
#ifndef __EMSCRIPTEN__
    std::optional<sync_retirement_lane::reservation> reservation;
    {std::lock_guard<std::mutex> lock(mutex_);if(!retirement_){if(pacer.joinable())std::terminate();return protected_;}open_=false;retired_=true;reservation.emplace(std::move(*retirement_));retirement_.reset();}
    lifetime_->end_protected_attempt();reservation->retire(std::move(pacer));return true;
#else
    return false;
#endif
}
void recovery_export_route::publish(uint64_t generation,bool open) noexcept{std::lock_guard<std::mutex> lock(mutex_);if(!retired_){generation_=generation;open_=open;}}
void recovery_export_route::retire() noexcept{if(retire_protected())return;std::lock_guard<std::mutex> lock(mutex_);retired_=true;open_=false;}
bool recovery_export_route::current(uint64_t generation) noexcept{std::lock_guard<std::mutex> lock(mutex_);return !retired_&&open_&&generation_==generation&&lifetime_->current(generation);}
bool recovery_export_route::handoff(committed_export_frame frame){
    if(frame.consumed_||!frame.owner_||frame.entries_.empty()||frame.claims_.empty())refuse("export permit already consumed or missing custody");frame.consumed_=true;
    if(!current(frame.physical_generation_)||frame.owner_->is_closed())return false;
    recovery_export_adapter::revalidate_claimed_frame(frame);
    if(frame.owner_->is_closed()||!lifetime_->protected_current(frame.physical_generation_))return false;
    std::shared_ptr<sync_transport> transport;
    {std::lock_guard<std::mutex> lock(mutex_);if(retired_||!open_||generation_!=frame.physical_generation_)return false;transport=transport_;}
    transport->send(frame.message_);return true;
}
} // namespace lattice::detail
