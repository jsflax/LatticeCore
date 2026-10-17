// Included only by ordinary_export.cpp: shares its real bounded read adapter,
// mutation guard and exact writer transaction. No standalone issuer/SQL layer.
namespace {
constexpr const char* ingress_binding_ddl="CREATE TABLE _lattice_ordered_ingress_v2 (scope TEXT PRIMARY KEY,device TEXT NOT NULL,inode TEXT NOT NULL,schema TEXT NOT NULL,seal TEXT NOT NULL)";
constexpr const char* ingress_entry_ddl="CREATE TABLE _lattice_ordered_ingress_entry_v2 (scope TEXT NOT NULL,gid TEXT COLLATE NOCASE NOT NULL,envelope BLOB NOT NULL,observation BLOB NOT NULL,audit_id INTEGER NOT NULL CHECK(audit_id>0),projection TEXT NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(scope,gid))";
std::string ingress_guard(const std::string& table,const std::string& op){return "CREATE TRIGGER "+table+"_"+op+" BEFORE "+op+" ON "+table+" BEGIN SELECT lattice_ordered_export_owned_v1(); END";}
bool ingress_storage(sqlite3* h,budget& b) {
    const auto objects=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_ingress*' OR tbl_name GLOB '_lattice_ordered_ingress*' ORDER BY name",{},11,b);
    if(objects.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [table,ddl]:{std::pair{"_lattice_ordered_ingress_v2",ingress_binding_ddl},std::pair{"_lattice_ordered_ingress_entry_v2",ingress_entry_ddl}}) {
        expected.emplace(table,std::make_pair("table",column_value_t{ddl}));
        expected.emplace(std::string("sqlite_autoindex_")+table+"_1",std::make_pair("index",column_value_t{nullptr}));
        for(const auto* op:{"INSERT","UPDATE","DELETE"})expected.emplace(std::string(table)+"_"+op,std::make_pair("trigger",column_value_t{ingress_guard(table,op)}));
    }
    if(objects.size()!=expected.size())refuse("ingress guard inventory changed");
    for(const auto& row:objects){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("ingress object definition changed");}return true;
}
const char* ingress_columns="scope,gid,envelope,observation,audit_id,projection,seal";
struct ingress_capacity {int64_t rows=0,bytes=0;bool operator==(const ingress_capacity&)const=default;};
ingress_capacity ingress_usage(sqlite3* h,budget& b) {
    const auto count=read(h,"SELECT COUNT(*),COALESCE(SUM(length(envelope)+length(observation)+length(CAST(scope AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(projection AS BLOB))+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_ingress_entry_v2",{},1,b);
    const auto n=integer(count[0][0]),used=integer(count[0][1]);
    if(n<0||n>8192||used<0||used>128*1024*1024)refuse("ingress durable capacity exhausted");
    return {n,used};
}
// Caller-local ordering, ACK flags and late-bound NoHistory observations do
// not rename an original. Exact first input bytes remain in the guarded row.
// The late-field mask is derived from the actual catalog during this WRITE.
std::string ingress_original_key(const audit_log_entry& entry,const ordinary_upload_original& original,
    const std::vector<std::string>& late_fields,budget& b) {
    fingerprint hash;hash.bytes(std::string("ordinary-ingress-immutable-original-v2"));
    hash.bytes(entry.global_id);hash.bytes(entry.table_name);hash.bytes(entry.operation);
    hash.bytes(entry.global_row_id);hash.bytes(entry.timestamp);hash.number(entry.synthesized);
    hash.number(original.fields.size());
    for(const auto& name:original.fields){hash.bytes(name);const bool late=std::binary_search(late_fields.begin(),late_fields.end(),name);hash.number(late);
        if(!late){const auto found=entry.changed_fields.find(name);if(found==entry.changed_fields.end())refuse("ingress original lost immutable field");hash.number(static_cast<uint64_t>(found->second.kind));hash.scalar(found->second.value);}}
    hash.number(original.names.size());for(const auto& name:original.names)hash.bytes(name);
    hash.original_identity(entry);b.add(64);return hash.finish();
}

values ingress_binding(sqlite3* h,const ordinary_export_frame& frame,budget& b) {
    const auto found=read(h,"SELECT scope,device,inode,schema,seal FROM main._lattice_ordered_ingress_v2 WHERE scope=?",{frame.scope},1,b);
    if(found.size()!=1)refuse("ingress scope unavailable");const auto& row=found[0];
    if(row[0]!=column_value_t(frame.scope)||row[1]!=column_value_t(frame.device)||row[2]!=column_value_t(frame.inode)||row[3]!=column_value_t(frame.schema)||text(row[4])!=original_seal("ordinary-ingress-binding-v2",row,4,b))refuse("ingress physical/schema association changed");original_copy_charge(row,b);return row;
}
const std::vector<uint8_t>& ingress_blob(const column_value_t& v){const auto* bytes=std::get_if<std::vector<uint8_t>>(&v);if(!bytes||bytes->empty()||bytes->size()>ordinary_sync_limits::frame_bytes)refuse("ingress blob bound");return *bytes;}
void ingress_record(sqlite3* h,const values& row,budget& b) {
    if(row.size()!=7||text(row[6])!=original_seal("ordinary-ingress-original-v2",row,6,b))refuse("ingress original record changed");
    const auto audit=read(h,"SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,synthesized,isFromRemote,isSynchronized FROM main.AuditLog WHERE globalId=?",{text(row[1])},1,b);
    if(audit.size()!=1||integer(audit[0][0])!=integer(row[4])||text(row[5])!=original_seal("ordinary-ingress-audit-v2",audit[0],11,b))refuse("ingress local AuditLog association changed or disappeared");
}
bool validate_source_upload_capture(sqlite3* h,const ordinary_export_frame& frame,budget& b) {
    if(!ingress_storage(h,b))return false;(void)ingress_binding(h,frame,b);ingress_usage(h,b);
    // Reclamation may never silently discard an unresolved intake original.
    // Missing local association refuses a cut; no historical fact is invented.
    const auto orphan=read(h,"SELECT 1 FROM main._lattice_ordered_ingress_entry_v2 i LEFT JOIN main.AuditLog a ON a.globalId=i.gid AND a.id=i.audit_id WHERE i.scope=? AND a.id IS NULL LIMIT 1",{frame.scope},1,b);
    if(!orphan.empty())refuse("source history lost retained ingress association");return true;
}
void hydrate_source_upload(sqlite3* h,const ordinary_export_frame& frame,audit_log_entry& entry,budget& b) {
    // Inventory/context was checked once by capture_entries in this same owned
    // view. No observer delivery or caller callback can run between these reads.
    const auto rows=read(h,std::string("SELECT ")+ingress_columns+" FROM main._lattice_ordered_ingress_entry_v2 WHERE scope=? AND gid=?",{frame.scope,entry.global_id},1,b);
    if(rows.empty())return;const auto& row=rows[0];ingress_record(h,row,b);
    auto saved=decode_ordinary_v2_retained_entry(ingress_blob(row[2]),[&](size_t n){b.add(n);});
    saved.upload_original=decode_ordinary_upload_observation(ingress_blob(row[3]),[&](size_t n){b.add(n);});
    if(saved.global_id!=entry.global_id||saved.table_name!=entry.table_name||saved.global_row_id!=entry.global_row_id)refuse("ingress semantic/local association differs");
    saved.id=entry.id;saved.row_id=entry.row_id;saved.is_from_remote=entry.is_from_remote;saved.is_synchronized=entry.is_synchronized;
    saved.ordinary_delivery_version=std::move(entry.ordinary_delivery_version);
    // Restore observed membership before this source's own late binding. It
    // was schema-validated at committed intake; only actual no_history UPDATE
    // keys could have been absent from the received projected payload.
    b.add(saved.upload_original->names.size()*sizeof(std::string));for(const auto& name:saved.upload_original->names)b.add(name.size());
    saved.changed_fields_names=saved.upload_original->names;
    entry=std::move(saved);
}
void validate_upload_membership(const audit_log_entry& entry,const ordinary_export_frame& frame,const recovery_owner_schema& catalog,budget& b) {
    validate_helper_entry(entry,frame,b);if(!entry.upload_original)refuse("upload lacks pre-late-binding observation");
    (void)ordinary_upload_timestamp_to_epoch(entry.timestamp); // before any required source apply
    const auto table=std::find_if(frame.tables.begin(),frame.tables.end(),[&](const auto& v){return v.name==entry.table_name;});
    if(table==frame.tables.end())refuse("upload table outside local projection");
    const auto& observed=*entry.upload_original;
    const auto late=[&](const std::string& name){
        if(entry.operation!="UPDATE"||std::find(observed.names.begin(),observed.names.end(),name)==observed.names.end())return false;
        const auto* model=catalog.find(entry.table_name);if(!model)return false;
        const auto property=std::find_if(model->properties.begin(),model->properties.end(),[&](const auto& p){return p.name==name;});
        return property!=model->properties.end()&&property->no_history&&property->kind==property_kind::primitive;};
    const auto& names=table->audit_names.empty()?table->columns:table->audit_names;
    for(const auto* sequence:{&observed.fields,&observed.names})for(const auto& name:*sequence)
        if(std::find(names.begin(),names.end(),name)==names.end())refuse("upload original name outside actual schema");
    for(const auto& [name,value]:entry.changed_fields) {
        if(!std::binary_search(observed.fields.begin(),observed.fields.end(),name)&&!late(name))refuse("upload gained an unobserved field");
        const auto column=std::find(names.begin(),names.end(),name);if(column==names.end())refuse("upload field outside actual projection");
        const auto type=table->types.at(static_cast<size_t>(column-names.begin()));const auto& v=value.value;
        const bool nullable=std::holds_alternative<std::nullptr_t>(v);
        const auto* hex=type==column_type::blob?std::get_if<std::string>(&v):nullptr;
        const bool audit_hex=hex&&hex->size()%2==0&&std::all_of(hex->begin(),hex->end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F');});
        if(!nullable&&!((type==column_type::integer&&std::holds_alternative<int64_t>(v))||(type==column_type::real&&(std::holds_alternative<double>(v)||std::holds_alternative<int64_t>(v)))||(type==column_type::text&&std::holds_alternative<std::string>(v))||(type==column_type::blob&&(std::holds_alternative<std::vector<uint8_t>>(v)||audit_hex))))refuse("upload value disagrees with actual schema");
    }
    for(const auto& name:observed.fields)
        if(!entry.changed_fields.count(name)&&!late(name))refuse("upload lost an immutable field");
    size_t current=0;
    for(const auto& name:observed.names){if(current<entry.changed_fields_names.size()&&entry.changed_fields_names[current]==name)++current;
        else if(!late(name))refuse("upload changed immutable name/order");}
    if(current!=entry.changed_fields_names.size())refuse("upload gained or reordered a name");
}
}
struct ordinary_source_upload_delivery::state {
    lattice_db* owner;std::shared_ptr<database> writer;
    ordinary_source_upload_batch input;std::vector<std::string> models;
    budget retained{wire_limits};ordinary_export_frame context;values binding,source_profile;bool source_enrolled=false;
    struct original_check {std::vector<std::string> late_fields;std::string immutable,expected_seal;};
    std::vector<original_check> checks;ingress_capacity capacity;bool capacity_dirty=true;
    original_check& check(const audit_log_entry& entry){
        const auto found=std::find_if(input.entries.begin(),input.entries.end(),[&](const auto& v){return &v==&entry;});
        if(found==input.entries.end()||checks.size()!=input.entries.size())refuse("ingress entry outside retained input");return checks.at(static_cast<size_t>(found-input.entries.begin()));}
    state(lattice_db& o,std::shared_ptr<database> w,ordinary_source_upload_batch&& i,std::vector<std::string>&& m)
        :owner(&o),writer(std::move(w)),input(std::move(i)),models(std::move(m)){retained.add(input.logical_bytes);}
};
ordinary_source_upload_delivery::ordinary_source_upload_delivery(lattice_db& owner,std::shared_ptr<database> writer,ordinary_source_upload_batch&& input,std::vector<std::string> models)
    :state_(std::make_unique<state>(owner,std::move(writer),std::move(input),std::move(models))){}
ordinary_source_upload_delivery::~ordinary_source_upload_delivery()=default;
const std::vector<audit_log_entry>& ordinary_source_upload_delivery::entries()const{return state_->input.entries;}
std::vector<std::string> ordinary_export_access::receive_upload(lattice_db& owner,std::string_view raw,const std::vector<std::string>& requested) {
    auto decoded=decode_ordinary_source_upload(raw); // Before any legacy parser or SQL.
    if(requested.size()>ordinary_sync_limits::models)refuse("upload model count bound");
    std::vector<std::string> models;models.reserve(requested.size());for(const auto& name:requested){(void)quote(name);models.push_back(name);}
    std::sort(models.begin(),models.end());if(std::adjacent_find(models.begin(),models.end())!=models.end())refuse("duplicate upload model");
    std::shared_ptr<database> writer;{std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||!owner.db_)refuse("source upload owner closed");writer=owner.db_;}
    ordinary_source_upload_delivery delivery(owner,std::move(writer),std::move(decoded),std::move(models));
    return owner.apply_remote_changes_impl_(delivery.entries(),std::nullopt,nullptr,nullptr,&delivery);
}
ordinary_upload_acknowledgment_result ordinary_export_access::receive_upload_with_acknowledgment(lattice_db& owner,std::string_view raw,const std::vector<std::string>& requested) {
    auto decoded=decode_ordinary_source_upload(raw,true); // Before any legacy parser or SQL.
    // Output may coexist with the full decoded intake until actual return.
    // Give its immutable payload a separate real slot before any commit, so
    // the input lease does not undercharge this overlap or Swift value copies.
    auto result_reservation=reserve_ordinary_upload_result();
    if(requested.size()>ordinary_sync_limits::models)refuse("upload model count bound");
    std::vector<std::string> models;models.reserve(requested.size());for(const auto& name:requested){(void)quote(name);models.push_back(name);}
    std::sort(models.begin(),models.end());if(std::adjacent_find(models.begin(),models.end())!=models.end())refuse("duplicate upload model");
    std::shared_ptr<database> writer;{std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||!owner.db_)refuse("source upload owner closed");writer=owner.db_;}
    ordinary_source_upload_delivery delivery(owner,std::move(writer),std::move(decoded),std::move(models));
    auto accepted=owner.apply_remote_changes_impl_(delivery.entries(),std::nullopt,nullptr,nullptr,&delivery);
    const auto bytes=encode_ordinary_upload_ack(delivery.entries(),accepted);
    std::vector<uint8_t> encoded(bytes.begin(),bytes.end());
    return ordinary_upload_acknowledgment_result(std::move(result_reservation),
        std::move(accepted),std::move(encoded));
}
void ordinary_export_access::admit(ordinary_source_upload_delivery& delivery,lattice_db& owner,database& writer) {
    auto& s=*delivery.state_;auto& b=s.retained;auto* h=ordinary_export_storage_access::borrow(writer);
    if(s.owner!=&owner||s.writer.get()!=&writer||sqlite3_txn_state(h,"main")!=SQLITE_TXN_WRITE)refuse("source upload lost actual writer");
    {std::lock_guard lock(owner.connection_ownership_mutex_);if(owner.closed_.load()||owner.db_!=s.writer)refuse("source upload owner changed");}
    require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);require_ordered_receiver_absent(writer);
    if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_)refuse("source upload ordinary catalog unavailable");
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("source upload physical identity unavailable");
    ordinary_export_frame frame;frame.wire_profile=true;frame.device=std::to_string(physical->device);frame.inode=std::to_string(physical->inode);
    capture_projected_tables(h,owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},s.models,frame,b,false,nullptr,nullptr);
    const bool source=inspect_storage(h,b);values profile;
    if(source) {
        const auto existing=read(h,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{frame.scope},1,b);
        if(existing.size()!=1||text(existing[0][0])!=frame.scope||!canonical_uuid(text(existing[0][1]))||integer(existing[0][2])!=1||integer(existing[0][3])<0||text(existing[0][4])!=frame.device||text(existing[0][5])!=frame.inode||text(existing[0][6])!=frame.schema)refuse("source upload has no matching durable source scope");
        original_copy_charge(existing[0],b);profile=existing[0];
    } else if(!fresh_origin(owner,*physical)||owner.ordinary_export_enrolled_here_.load(std::memory_order_acquire))refuse("historical source upload requires managed admission");
    if(!s.binding.empty()&&(s.source_enrolled!=source||s.source_profile!=profile))refuse("source scope changed between upload chunks");
    s.source_enrolled=source;s.source_profile=std::move(profile);
    register_mutation_guard(h);mutation_scope permit(h);
    if(!ingress_storage(h,b)) {
        writer.execute(ingress_binding_ddl);writer.execute(ingress_entry_ddl);
        for(const auto* table:{"_lattice_ordered_ingress_v2","_lattice_ordered_ingress_entry_v2"})for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(ingress_guard(table,op));
        if(!ingress_storage(h,b))refuse("ingress install failed");
    }
    auto known=read(h,"SELECT scope FROM main._lattice_ordered_ingress_v2 WHERE scope=?",{frame.scope},1,b);
    if(known.empty()) {
        const auto count=read(h,"SELECT COUNT(*) FROM main._lattice_ordered_ingress_v2",{},1,b);if(integer(count[0][0])>=16)refuse("ingress scope capacity");
        values value{frame.scope,frame.device,frame.inode,frame.schema};value.emplace_back(original_seal("ordinary-ingress-binding-v2",value,4,b));
        writer.execute("INSERT INTO main._lattice_ordered_ingress_v2 VALUES(?,?,?,?,?)",value);if(writer.changes()!=1)refuse("ingress binding ignored");
    }
    auto binding=ingress_binding(h,frame,b);if(!s.binding.empty()&&s.binding!=binding)refuse("source upload changed between chunks");
    // Re-derived at every actual chunk admission, never cached across an
    // observer turn, rollback, schema change or source publication boundary.
    b.add(s.input.entries.size()*sizeof(ordinary_source_upload_delivery::state::original_check));
    std::vector<ordinary_source_upload_delivery::state::original_check> checks;checks.reserve(s.input.entries.size());
    for(const auto& entry:s.input.entries){validate_upload_membership(entry,frame,owner.recovery_schemas_,b);
        ordinary_source_upload_delivery::state::original_check check;const auto& observed=*entry.upload_original;
        const auto* model=owner.recovery_schemas_.find(entry.table_name);
        for(const auto& name:observed.fields)if(entry.operation=="UPDATE"&&model&&std::find(observed.names.begin(),observed.names.end(),name)!=observed.names.end()){
            const auto property=std::find_if(model->properties.begin(),model->properties.end(),[&](const auto& p){return p.name==name;});
            if(property!=model->properties.end()&&property->no_history&&property->kind==property_kind::primitive){b.add(sizeof(std::string)+name.size());check.late_fields.push_back(name);}}
        check.immutable=ingress_original_key(entry,observed,check.late_fields,b);checks.push_back(std::move(check));}
    s.checks=std::move(checks);s.context=std::move(frame);s.binding=std::move(binding);
    s.capacity=ingress_usage(h,b);s.capacity_dirty=false;
}
bool ordinary_source_upload_delivery::duplicate(database& writer,const audit_log_entry& entry) {
    auto& s=*state_;auto& b=s.retained;if(&writer!=s.writer.get()||s.binding.empty())refuse("unadmitted source duplicate check");
    auto& check=s.check(entry);
    const auto found=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+ingress_columns+" FROM main._lattice_ordered_ingress_entry_v2 WHERE scope=? AND gid=?",{s.context.scope,entry.global_id},1,b);
    if(found.empty())return false;const auto& row=found[0];ingress_record(ordinary_export_storage_access::borrow(writer),row,b);
    auto saved=decode_ordinary_v2_retained_entry(ingress_blob(row[2]),[&](size_t n){b.add(n);});
    saved.upload_original=decode_ordinary_upload_observation(ingress_blob(row[3]),[&](size_t n){b.add(n);});
    if(saved.upload_original->fields!=entry.upload_original->fields||saved.upload_original->names!=entry.upload_original->names||
        ingress_original_key(saved,*saved.upload_original,check.late_fields,b)!=check.immutable)refuse("duplicate upload changed its immutable original");
    b.add(text(row[6]).size());check.expected_seal=text(row[6]);
    // This ACK means the same original was actually applied before. A retry
    // never replays a newer projected NoHistory value over current source rows.
    // A new edit must carry its own original ID and pass normal actual apply.
    return true;
}
void ordinary_source_upload_delivery::rolled_back_entry(const audit_log_entry& entry)noexcept {
    // Called only after both ROLLBACK TO and RELEASE succeeded. Accounting is
    // invalidated, never decremented from an assumption about partial effects.
    auto& s=*state_;s.capacity_dirty=true;
    for(size_t n=0;n<s.input.entries.size()&&n<s.checks.size();++n)if(&s.input.entries[n]==&entry){s.checks[n].expected_seal.clear();break;}
}

void ordinary_source_upload_delivery::inserted(database& writer,const audit_log_entry& entry) {
    auto& s=*state_;auto& b=s.retained;if(&writer!=s.writer.get()||s.binding.empty()||sqlite3_txn_state(ordinary_export_storage_access::borrow(writer),"main")!=SQLITE_TXN_WRITE)refuse("source original insertion lost owning savepoint");
    auto audit=original_audit(writer,entry.global_id,b);if(audit.empty()||text(audit[1])!=entry.global_id||text(audit[2])!=entry.table_name||text(audit[5])!=entry.global_row_id)refuse("upload has no actual inserted AuditLog row");
    auto envelope=encode_ordinary_v2_retained_entry(entry,[&](size_t n){b.add(n);});
    auto observation=encode_ordinary_upload_observation(*entry.upload_original,[&](size_t n){b.add(n);});
    const auto durable_bytes=envelope.size()+observation.size()+s.context.scope.size()+entry.global_id.size()+128;
    b.add(7*sizeof(column_value_t)+s.context.scope.size()+entry.global_id.size());
    values value{s.context.scope,entry.global_id,std::move(envelope),std::move(observation),integer(audit[0]),original_seal("ordinary-ingress-audit-v2",audit,11,b)};
    value.emplace_back(original_seal("ordinary-ingress-original-v2",value,6,b));
    if(s.capacity_dirty){s.capacity=ingress_usage(ordinary_export_storage_access::borrow(writer),b);s.capacity_dirty=false;}
    if(s.capacity.rows==8192||durable_bytes>128*1024*1024-size_t(s.capacity.bytes))refuse("ingress durable capacity exhausted");
    mutation_scope permit(ordinary_export_storage_access::borrow(writer));writer.execute("INSERT INTO main._lattice_ordered_ingress_entry_v2 VALUES(?,?,?,?,?,?,?)",value);
    if(writer.changes()!=1)refuse("ingress original insertion ignored");
    const auto after=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+ingress_columns+" FROM main._lattice_ordered_ingress_entry_v2 WHERE scope=? AND gid=?",{s.context.scope,entry.global_id},1,b);
    if(after.size()!=1||after[0]!=value)refuse("ingress insertion postimage differs");
    ++s.capacity.rows;s.capacity.bytes+=static_cast<int64_t>(durable_bytes);
    auto& check=s.check(entry);b.add(text(value[6]).size());check.expected_seal=text(value[6]);
}
void ordinary_source_upload_delivery::finish(database& writer,const std::vector<std::string>& ids,size_t begin) {
    auto& s=*state_;auto& b=s.retained;if(&writer!=s.writer.get()||sqlite3_txn_state(ordinary_export_storage_access::borrow(writer),"main")!=SQLITE_TXN_WRITE)refuse("source intake finish lost actual WRITE");
    const auto physical=writer.physical_identity("main",{},true);
    if(!physical||std::to_string(physical->device)!=s.context.device||std::to_string(physical->inode)!=s.context.inode)refuse("source physical identity changed before commit");
    if(inspect_storage(ordinary_export_storage_access::borrow(writer),b)!=s.source_enrolled)refuse("source inventory changed before upload commit");
    if(s.source_enrolled){const auto profile=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{s.context.scope},1,b);if(profile.size()!=1||profile[0]!=s.source_profile)refuse("source frontier changed before upload commit");}
    if(!ingress_storage(ordinary_export_storage_access::borrow(writer),b)||ingress_binding(ordinary_export_storage_access::borrow(writer),s.context,b)!=s.binding)refuse("ingress context changed before commit");
    for(size_t n=begin;n<ids.size();++n){const auto entry=std::find_if(s.input.entries.begin(),s.input.entries.end(),[&](const auto& v){return v.global_id==ids[n];});
        if(entry==s.input.entries.end())refuse("source ACK outside retained input");const auto& check=s.check(*entry);
        const auto rows=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+ingress_columns+" FROM main._lattice_ordered_ingress_entry_v2 WHERE scope=? AND gid=?",{s.context.scope,entry->global_id},1,b);
        if(check.expected_seal.empty()||rows.size()!=1||text(rows[0][6])!=check.expected_seal)refuse("source ACK lacks same-chunk original custody");
        ingress_record(ordinary_export_storage_access::borrow(writer),rows[0],b);}
    const auto actual=ingress_usage(ordinary_export_storage_access::borrow(writer),b);
    if(!s.capacity_dirty&&actual!=s.capacity)refuse("ingress durable accounting changed before commit");
    s.capacity=actual;s.capacity_dirty=false;
}
