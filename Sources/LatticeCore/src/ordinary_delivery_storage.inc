// Included by ordinary_export.cpp after the actual original-turn definitions.
// The record seal is a durable version correlation, not a decoded source grant.
namespace {
constexpr const char* delivery_table="_lattice_ordered_delivery_v1";
constexpr const char* delivery_ddl="CREATE TABLE _lattice_ordered_delivery_v1 (scope TEXT NOT NULL,gid TEXT NOT NULL COLLATE NOCASE,channel TEXT NOT NULL,version TEXT NOT NULL,handled INTEGER NOT NULL CHECK(handled IN(0,1)),repair INTEGER NOT NULL CHECK(repair IN(0,1)),seal TEXT NOT NULL,PRIMARY KEY(scope,gid,channel))";
constexpr const char* delivery_pending_ddl="CREATE INDEX _lattice_ordered_delivery_v1_pending ON _lattice_ordered_delivery_v1(channel,repair,gid) WHERE repair=1";
constexpr size_t delivery_channels=64,delivery_rows=8192*delivery_channels,delivery_bytes=128*1024*1024;
std::string delivery_guard(const std::string& op){return "CREATE TRIGGER _lattice_ordered_delivery_v1_"+op+" BEFORE "+op+" ON _lattice_ordered_delivery_v1 BEGIN SELECT lattice_ordered_export_owned_v1(); END";}
bool delivery_storage(sqlite3* h,budget& b) {
    const auto objects=read(h,"SELECT type,name,tbl_name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_delivery*' OR tbl_name GLOB '_lattice_ordered_delivery*' ORDER BY name",{},7,b);
    if(objects.empty())return false;
    std::map<std::string,values> expected;
    expected.emplace(delivery_table,values{"table",delivery_table,delivery_table,delivery_ddl});
    expected.emplace("sqlite_autoindex__lattice_ordered_delivery_v1_1",values{"index","sqlite_autoindex__lattice_ordered_delivery_v1_1",delivery_table,nullptr});
    expected.emplace("_lattice_ordered_delivery_v1_pending",values{"index","_lattice_ordered_delivery_v1_pending",delivery_table,delivery_pending_ddl});
    for(const auto* op:{"INSERT","UPDATE","DELETE"}){const std::string name=std::string(delivery_table)+'_'+op;expected.emplace(name,values{"trigger",name,delivery_table,delivery_guard(op)});}
    if(objects.size()!=expected.size())refuse("delivery obligation inventory changed");
    for(const auto& row:objects){const auto found=expected.find(text(row[1]));if(found==expected.end()||row!=found->second)refuse("delivery obligation definition changed");}
    if(!read(h,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_delivery*' OR tbl_name GLOB '_lattice_ordered_delivery*' LIMIT 1",{},1,b).empty())refuse("delivery TEMP shadow or program");
    return true;
}
original_usage delivery_usage(sqlite3* h,budget& b) {
    const auto rows=read(h,"SELECT COUNT(*),COALESCE(SUM(length(CAST(scope AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(channel AS BLOB))+length(CAST(version AS BLOB))+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_delivery_v1",{},1,b);
    original_usage result{integer(rows[0][0]),integer(rows[0][1])};
    if(result.rows<0||uint64_t(result.rows)>delivery_rows||result.bytes<0||uint64_t(result.bytes)>delivery_bytes)refuse("delivery durable bound");return result;
}
void install_delivery_storage(database& writer,budget& b) {
    original_view(writer,true);register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    if(!delivery_storage(ordinary_export_storage_access::borrow(writer),b)){
        writer.execute(delivery_ddl);writer.execute(delivery_pending_ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(delivery_guard(op));
        if(!delivery_storage(ordinary_export_storage_access::borrow(writer),b))refuse("delivery obligation installation failed");
    }
}
const char* delivery_columns="scope,gid,channel,version,handled,repair,seal";
void delivery_parameter_charge(std::initializer_list<std::string_view> strings,budget& b,size_t copies=1) {
    // Fixed conservative staging charge precedes each owning values/key/result
    // construction. The raw row reader separately charges actual returned data.
    for(size_t n=0;n<copies;++n){b.add(sizeof(values)+7*sizeof(column_value_t));for(const auto value:strings)b.add(value.size()+1);}
}
size_t delivery_size(const values& row){size_t total=0;for(size_t n:{size_t(0),size_t(1),size_t(2),size_t(3),size_t(6)})total+=text(row[n]).size();return total;}
void verify_delivery_row(const values& row,budget& b) {
    if(row.size()!=7||text(row[0]).empty()||text(row[1]).empty()||text(row[1]).size()>4096||text(row[2]).empty()||text(row[2]).size()>4096||text(row[3]).size()!=64||!ordinary_delivery_version_valid(text(row[3]))||
       (integer(row[4])!=0&&integer(row[4])!=1)||(integer(row[5])!=0&&integer(row[5])!=1)||text(row[6])!=original_seal("ordinary-delivery-obligation-v1",row,6,b))refuse("delivery obligation row changed");
}
void put_delivery(database& writer,values row,const values* before,original_usage& usage,budget& b) {
    row.emplace_back(original_seal("ordinary-delivery-obligation-v1",row,6,b));
    const auto old=before?delivery_size(*before):0,added=delivery_size(row);
    if(old>size_t(usage.bytes)||added>delivery_bytes-(size_t(usage.bytes)-old)||(!before&&size_t(usage.rows)==delivery_rows))refuse("delivery obligation capacity exhausted");
    register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    writer.execute("INSERT INTO main._lattice_ordered_delivery_v1 VALUES(?,?,?,?,?,?,?) ON CONFLICT(scope,gid,channel) DO UPDATE SET version=excluded.version,handled=excluded.handled,repair=excluded.repair,seal=excluded.seal",row);
    delivery_parameter_charge({text(row[0]),text(row[1]),text(row[2])},b);
    const auto observed=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+delivery_columns+" FROM main._lattice_ordered_delivery_v1 WHERE scope=? AND gid=? AND channel=?",{row[0],row[1],row[2]},1,b);
    if(writer.changes()!=1||observed.size()!=1||observed[0]!=row)refuse("delivery obligation postimage differs");
    usage.rows+=before?0:1;usage.bytes+=static_cast<int64_t>(added)-static_cast<int64_t>(old);
}
}
void ordinary_wire_original_turn::bind_delivery(const std::string& scope,const std::string& gid,const std::string& receiving) {
    auto& s=*state_;s.usable();original_view(s.writer,true);auto& b=s.retained;auto* h=ordinary_export_storage_access::borrow(s.writer);
    if(receiving.empty()||receiving.size()>4096)refuse("delivery receiving channel bound");
    if(!s.delivery_usage){if(!delivery_storage(h,b))refuse("delivery obligations unavailable");s.delivery_usage=::lattice::detail::delivery_usage(h,b);}
    const auto current=original_record(s.writer,scope,gid,b,s.context);if(current.empty())refuse("delivery has no actual canonical original");
    (void)original_decode(s.writer,current,b,true,s.context); // exact physical/binding/receipt/local association
    const auto& binding=s.context.scope(scope).binding;
    if(binding[1]!=column_value_t(receiving))refuse("delivery channel differs from actual accepted source");
    delivery_parameter_charge({scope,gid,receiving},b,2);
    const auto existing=read(h,std::string("SELECT ")+delivery_columns+" FROM main._lattice_ordered_delivery_v1 WHERE scope=? AND gid=? ORDER BY channel",{scope,gid},delivery_channels,b);
    std::map<std::string,const values*> prior;
    for(const auto& row:existing){verify_delivery_row(row,b);b.add(sizeof(std::string)+text(row[2]).size()+4*sizeof(void*));prior.emplace(text(row[2]),&row);}
    const auto channels=read(h,"SELECT sync_id FROM main._lattice_replication_slots UNION SELECT sync_id FROM main._lattice_sync_state WHERE audit_entry_id=? UNION SELECT channel FROM main._lattice_ordered_delivery_v1 WHERE scope=? AND gid=? UNION SELECT ? ORDER BY 1",{current[11],scope,gid,receiving},delivery_channels,b);
    for(const auto& channel:channels){const auto& name=text(channel[0]);if(name.empty()||name.size()>4096)refuse("retained delivery channel bound");const auto old=prior.find(name);const values* row=old==prior.end()?nullptr:old->second;
        // Only the real installed cut clears repair for its receiving route.
        // A prior ACK from any other channel is not a receipt for replacement.
        const bool source=name==receiving,same=row&&(*row)[3]==current[13];
        const int64_t handled=source?1:(same?integer((*row)[4]):0),repair=source?0:(same?integer((*row)[5]):1);
        delivery_parameter_charge({scope,gid,name,text(current[13])},b);
        values next{scope,gid,name,current[13],handled,repair};
        if(!row||!same||integer((*row)[4])!=handled||integer((*row)[5])!=repair)put_delivery(s.writer,std::move(next),row,*s.delivery_usage,b);
        delivery_parameter_charge({name},b,2);
        s.writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,?) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=excluded.is_synchronized",{current[11],name,handled});
        if(s.writer.changes()!=1)refuse("delivery channel projection ignored");
        if(repair){
            // A new canonical version can reuse an AuditLog id below an old
            // floor. Regress only its actual channel under the same owned WRITE.
            // Later ordinary floor updates refuse while any channel debt remains.
            s.writer.execute("UPDATE main._lattice_replication_slots SET upload_floor=MIN(upload_floor,?) WHERE sync_id=?",{integer(current[11])-1,name});
        }
    }
    s.writer.execute("UPDATE main.AuditLog SET isSynchronized=0 WHERE id=?",{current[11]});
    if(s.writer.changes()!=1)refuse("delivery global collapse reset ignored");
}

bool ordinary_export_access::legacy_settlement_allowed(database& writer) {
    // Negative fence only, not a decoded authority grant. An upgraded store
    // cannot settle any of its repair obligations through GID-only callbacks.
    budget b{wire_limits};const auto present=read(ordinary_export_storage_access::borrow(writer),"SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_delivery*' OR tbl_name GLOB '_lattice_ordered_delivery*' OR name='_lattice_ordered_wire_original_v2' LIMIT 1",{},1,b);
    if(present.empty())return true;
    // A missing/corrupt delivery table cannot revive GID-only settlement while
    // guarded canonical mappings still exist. This check only denies authority.
    (void)delivery_storage(ordinary_export_storage_access::borrow(writer),b);return false;
}
std::vector<ordinary_delivery_outcome> ordinary_export_access::acknowledge_upload(lattice_db& owner,const std::string& channel,const std::vector<ordinary_upload_ack>& acknowledgments) {
    if(channel.empty()||channel.size()>4096||acknowledgments.empty()||acknowledgments.size()>ordinary_sync_limits::records_per_page)refuse("versioned acknowledgment bounds");
    budget b{wire_limits};b.add(acknowledgments.size()*sizeof(ordinary_delivery_outcome));
    std::vector<ordinary_delivery_outcome> outcomes;outcomes.reserve(acknowledgments.size());
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        auto* h=ordinary_export_storage_access::borrow(writer);original_view(writer,true);
        const auto has_mapping=!read(h,"SELECT 1 FROM main.sqlite_master WHERE name='_lattice_ordered_wire_original_v2' LIMIT 1",{},1,b).empty();
        const bool has_delivery=delivery_storage(h,b);std::optional<original_usage> usage;
        if(has_delivery)usage=delivery_usage(h,b);
        original_context_cache context(writer,b);
        if(has_mapping||has_delivery)context.check();
        for(const auto& ack:acknowledgments){if(ack.gid.empty()||ack.gid.size()>4096||!ordinary_delivery_version_valid(ack.version))refuse("versioned acknowledgment identity");
            delivery_parameter_charge({ack.gid,ack.version,channel},b,5);
            const auto mapping=has_mapping?read(h,"SELECT scope FROM main._lattice_ordered_wire_original_v2 WHERE gid=?",{ack.gid},1,b):rows{};
            const auto audit=original_audit(writer,ack.gid,b);if(audit.empty())continue;
            if(mapping.empty()){
                if(ack.version!="0")continue;
                writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1",{audit[0],channel});
                if(writer.changes()!=1)refuse("version-zero acknowledgment ignored");
                const auto remaining=read(h,"SELECT COUNT(*) FROM main._lattice_replication_slots r WHERE NOT EXISTS(SELECT 1 FROM main._lattice_sync_state s WHERE s.audit_entry_id=? AND s.sync_id=r.sync_id AND s.is_synchronized=1)",{audit[0]},1,b);
                if(integer(remaining[0][0])==0){writer.execute("UPDATE main.AuditLog SET isSynchronized=1 WHERE id=?",{audit[0]});writer.execute("DELETE FROM main._lattice_sync_state WHERE audit_entry_id=?",{audit[0]});}
                outcomes.push_back({ack.gid,ack.version,integer(audit[0]),true});continue;
            }
            const auto& scope=text(mapping[0][0]);const auto current=original_record(writer,scope,ack.gid,b,context);(void)original_decode(writer,current,b,true,context);
            if(text(current[13])!=ack.version)continue; // Late ACK for an older durable version.
            if(!has_delivery)refuse("canonical acknowledgment lacks delivery obligations");
            delivery_parameter_charge({scope,ack.gid,channel,ack.version},b,2);
            const auto rows=read(h,std::string("SELECT ")+delivery_columns+" FROM main._lattice_ordered_delivery_v1 WHERE scope=? AND gid=? AND channel=?",{scope,ack.gid,channel},1,b);
            if(rows.empty())refuse("unregistered canonical delivery channel");const auto& old=rows[0];verify_delivery_row(old,b);
            if(old[3]!=current[13])refuse("delivery obligation version is stale");
            if(integer(old[4])!=1)put_delivery(writer,{old[0],old[1],old[2],old[3],int64_t{1},old[5]},&old,*usage,b);
            // The ordinary upload request is handled; suppress its immediate
            // resend while the independent repair obligation remains durable.
            // This derived flag cannot settle the floor or collapse global sync.
            writer.execute("INSERT INTO main._lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=1",{current[11],channel});
            if(writer.changes()!=1)refuse("handled delivery projection ignored");
            // Handling immutable-original custody never clears canonical repair.
            outcomes.push_back({ack.gid,ack.version,integer(audit[0]),integer(old[5])==0});
        }
        context.finish();if(usage&&delivery_usage(h,b)!=*usage)refuse("delivery accounting changed before acknowledgment commit");
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    return outcomes;
}

bool ordinary_export_access::has_delivery_obligations(database& writer) {
    budget b{wire_limits};return delivery_storage(ordinary_export_storage_access::borrow(writer),b);
}

bool ordinary_export_access::delivery_floor_allowed(database& writer,const std::string& channel) {
    original_view(writer,false);budget b{wire_limits};
    if(!delivery_storage(ordinary_export_storage_access::borrow(writer),b)) {
        // A formerly upgraded scope cannot silently regain legacy floor grants
        // through loss of its delivery namespace.
        if(!read(ordinary_export_storage_access::borrow(writer),"SELECT 1 FROM main.sqlite_master WHERE name='_lattice_ordered_wire_original_v2' LIMIT 1",{},1,b).empty())
            refuse("canonical floor lacks delivery obligations");
        return true;
    }
    // Negative fence: a handled request is not a canonical application receipt.
    // Keep the prior floor until this channel's actual installed cuts clear all
    // retained repair debt. Indexed existence check is independent of backlog.
    delivery_parameter_charge({channel},b);
    return read(ordinary_export_storage_access::borrow(writer),"SELECT 1 FROM main._lattice_ordered_delivery_v1 WHERE channel=? AND repair=1 LIMIT 1",{channel},1,b).empty();
}
