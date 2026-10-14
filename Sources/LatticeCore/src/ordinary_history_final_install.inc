// Included after the complete native history stage, receive and local archive
// implementations. This installs a COMPLETE accepted per-lineage row version;
// it never selects a queried winner or rewrites old receipts/original bytes.
namespace {
constexpr const char* accepted_cut_table="_lattice_ordered_accepted_cut_v1";
constexpr const char* accepted_row_table="_lattice_ordered_accepted_row_v1";
const std::vector<std::pair<std::string,std::string>>& history_accepted_definitions(){
    static const std::vector<std::pair<std::string,std::string>> definitions{
        {accepted_cut_table,"CREATE TABLE _lattice_ordered_accepted_cut_v1 (stage TEXT PRIMARY KEY,channel TEXT NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL,cut INTEGER NOT NULL,scope TEXT NOT NULL,schema TEXT NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,generation TEXT NOT NULL,local_snapshot TEXT NOT NULL,rows INTEGER NOT NULL,known INTEGER NOT NULL,unknown INTEGER NOT NULL,baseline TEXT NOT NULL,content TEXT NOT NULL,seal TEXT NOT NULL,UNIQUE(channel,source,epoch,scope,cut))"},
        {accepted_row_table,"CREATE TABLE _lattice_ordered_accepted_row_v1 (stage TEXT NOT NULL,model TEXT NOT NULL,gid TEXT COLLATE NOCASE NOT NULL,page INTEGER NOT NULL,ordinal INTEGER NOT NULL,value BLOB NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(stage,model,gid),UNIQUE(stage,page,ordinal))"}};
    return definitions;
}
bool history_accepted_storage(sqlite3* h,budget& retained){
    const auto actual=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_accepted_*' OR tbl_name GLOB '_lattice_ordered_accepted_*' ORDER BY name",{},13,retained);
    if(actual.empty())return false;std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:history_accepted_definitions()){
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t(ddl)));
        for(int index:{1,2})expected.emplace("sqlite_autoindex_"+name+'_'+std::to_string(index),std::make_pair(std::string("index"),column_value_t(nullptr)));
        for(const char* op:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+op,std::make_pair(std::string("trigger"),column_value_t(receiver_guard(name,op))));
    }
    if(actual.size()!=expected.size())refuse("accepted history guard inventory changed");
    for(const auto& row:actual){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("accepted history program changed");}
    if(!read(h,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_accepted_*' OR tbl_name GLOB '_lattice_ordered_accepted_*' LIMIT 1",{},1,retained).empty())refuse("temporary accepted history shadow refused");return true;
}
uint64_t history_accepted_extra_bytes(database& writer,budget& retained){
    auto* h=ordinary_export_storage_access::borrow(writer);if(!history_accepted_storage(h,retained))return 0;
    const auto total=read(h,"SELECT (SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(channel AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(local_snapshot AS BLOB))+length(CAST(baseline AS BLOB))+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+40),0) FROM main._lattice_ordered_accepted_cut_v1)+(SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(model AS BLOB))+length(CAST(gid AS BLOB))+length(value)+length(CAST(seal AS BLOB))+16),0) FROM main._lattice_ordered_accepted_row_v1)",{},1,retained);
    const auto n=integer(total.at(0).at(0));if(n<0)refuse("accepted history quota invalid");return uint64_t(n);
}
values history_accepted_row(database& writer,const std::string& stage,budget& scratch){
    const auto rows=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,channel,source,epoch,cut,scope,schema,device,inode,generation,local_snapshot,rows,known,unknown,baseline,content,seal FROM main._lattice_ordered_accepted_cut_v1 WHERE stage=?",{stage},1,scratch);
    if(rows.size()!=1||text(rows[0][16])!=history_seal("ordinary-accepted-history-cut-v1",rows[0],16))refuse("accepted history complete cut missing or changed");return rows[0];
}
void history_accepted_schema(lattice_db& owner,database& writer,const ordinary_history_descriptor& header,
    const recovery_owner_schema& schema,const std::function<void(std::ostringstream&,const property_descriptor&)>& serialize){
    // The caller supplies only its actual immutable owner schema/serializer;
    // projection bytes cannot create generated table, array, link or nullable
    // semantics. This helper never reads optimistic values as server rows.
    (void)owner;ordinary_export_frame actual;actual.wire_profile=true;budget metadata{history_metadata_limits};
    capture_projected_tables(ordinary_export_storage_access::borrow(writer),schema,serialize,header.projection.models,actual,metadata,false,nullptr,nullptr);
    if(actual.scope!=header.projection.scope||actual.schema!=header.projection.schema||actual.tables.size()!=header.projection.tables.size())refuse("accepted history actual catalog differs");
    for(size_t n=0;n<actual.tables.size();++n)if(actual.tables[n].name!=header.projection.tables[n].name||actual.tables[n].columns!=header.projection.tables[n].columns||actual.tables[n].types!=header.projection.tables[n].types)refuse("accepted history actual projection differs");
}
// Revisit one real durable page at a time, recomputing the native parser,
// complete chain, record keys, exact counters and both source content/baseline.
// Callback is a private synchronous storage function, never a peer/user hook.
std::string history_accepted_pages(database& writer,const ordinary_history_descriptor& header,
    const ordinary_history_limits& limits,const std::string& channel,ordinary_launch::deadline end,
    const std::function<void(const values&)>& row_consumer){
    managed_pair_deadline(end);budget metadata{history_stage_limits};auto* h=ordinary_export_storage_access::borrow(writer);
    if(!history_storage(h,metadata)||!history_receive_storage(h,metadata))refuse("accepted history durable stage missing");
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("accepted history physical owner unavailable");
    const auto stage=history_stage_row(writer,header.stage,metadata),meta=history_receive_meta_row(writer,header.stage,metadata);
    const auto progress=history_receive_progress(stage,meta,header,limits,channel,std::to_string(physical->device),std::to_string(physical->inode));
    if(!progress.complete||progress.next_page!=header.pages)refuse("accepted history requires the whole actual same-cut stage");
    ordinary_history_progress checked;picosha2::hash256_one_by_one content;fingerprint baseline;baseline.bytes(std::string_view("ordinary-accepted-history-baseline-v1"));baseline.number(header.rows);uint64_t bytes=header.header.size();
    for(uint64_t n=0;n<header.pages;++n){managed_pair_deadline(end);budget scratch{history_row_limits};scratch.add(metadata.bytes);
        const auto stored=read(h,"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{header.stage,int64_t(n)},1,scratch);
        if(stored.size()!=1)refuse("accepted history page missing");const auto& p=stored[0];const auto* payload=std::get_if<std::vector<uint8_t>>(&p[4]);
        if(!payload||payload->empty()||payload->size()>limits.page_bytes||p[0]!=column_value_t(header.stage)||integer(p[1])!=int64_t(n)||text(p[8])!=history_seal("ordinary-paged-page-v1",p,8))refuse("accepted history retained page changed");
        const std::string_view raw(reinterpret_cast<const char*>(payload->data()),payload->size());auto decoded=decode_ordinary_history_page(raw,header,checked,limits);if(p[2]!=column_value_t(decoded.previous)||p[3]!=column_value_t(decoded.full_digest))refuse("accepted history chain differs");const auto before=checked;history_receive_verify_page(writer,header,decoded,checked,scratch);
        if(integer(p[5])!=int64_t(checked.rows-before.rows)||integer(p[6])!=int64_t(checked.known-before.known)||integer(p[7])!=int64_t(checked.unknown-before.unknown))refuse("accepted history page counters differ");
        // Exact known originals and unknown raw observations remain in their
        // real retained native page/key custody, separate from these row BLOBs.
        for(size_t ordinal=0;ordinal<decoded.records.size();++ordinal)if(const auto* row=std::get_if<ordinary_history_row>(&decoded.records[ordinal])){
            budget one{history_row_limits};one.add(scratch.bytes);const auto maximum=size_t(limits.page_bytes)+9*ordinary_sync_limits::projection_columns+8;
            (void)encoded_size(row->values,maximum);auto encoded=encode(row->values,one);
            values value{header.stage,header.projection.tables.at(row->table).name,text(row->values.at(0)),int64_t(n),int64_t(ordinal),std::move(encoded)};
            for(const auto& cell:value)baseline.scalar(cell);value.emplace_back(history_seal("ordinary-accepted-history-row-v1",value,6));row_consumer(value);
        }
        content.process(payload->begin(),payload->end());if(payload->size()>limits.durable_stage_bytes-bytes)refuse("accepted history retained byte total exceeds local profile");bytes+=payload->size();
    }
    content.finish();if(checked.rows!=header.rows||checked.known!=header.known||checked.unknown!=header.unknown||checked.next_page!=header.pages||checked.last_table!=progress.last_table||bytes!=uint64_t(integer(stage[15]))||picosha2::get_hash_hex_string(content)!=header.content_digest)refuse("accepted history complete content/counters differ");
    const auto counts=read(h,"SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_page_v1 WHERE stage=?),(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=?)",{header.stage,header.stage},1,metadata);
    if(counts[0]!=values{int64_t(header.pages),int64_t(header.rows+header.known+header.unknown)})refuse("accepted history extra page/key or missing filtered row");return baseline.finish();
}
void history_accepted_verify_inventory(database& writer,const ordinary_history_descriptor& header,const ordinary_receiver_preservation_record& local,
    const std::string& channel,const ordinary_history_limits& limits,ordinary_launch::deadline end,const values& expected){
    budget scratch{history_stage_limits};auto* h=ordinary_export_storage_access::borrow(writer);if(!history_accepted_storage(h,scratch))refuse("accepted history inventory missing");
    const auto actual=history_accepted_row(writer,header.stage,scratch);const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("accepted history physical identity absent");
    if(actual!=expected||actual[1]!=column_value_t(channel)||actual[2]!=column_value_t(header.projection.source)||integer(actual[3])!=header.projection.epoch||integer(actual[4])!=header.projection.cut||actual[5]!=column_value_t(header.projection.scope)||actual[6]!=column_value_t(header.projection.schema)||actual[7]!=column_value_t(std::to_string(physical->device))||actual[8]!=column_value_t(std::to_string(physical->inode))||actual[9]!=column_value_t(local.generation)||actual[10]!=column_value_t(local.snapshot)||integer(actual[11])!=int64_t(header.rows)||integer(actual[12])!=int64_t(header.known)||integer(actual[13])!=int64_t(header.unknown)||actual[15]!=column_value_t(header.content_digest))refuse("accepted history retained lineage/local custody association differs");
    preservation_verify(writer,local,limits,end,false);
    const auto digest=history_accepted_pages(writer,header,limits,channel,end,[&](const values& wanted){budget one{history_row_limits};const auto& payload=std::get<std::vector<uint8_t>>(wanted[5]);one.add(2*payload.size()+1024);
        const auto rows=read(h,"SELECT stage,model,gid,page,ordinal,value,seal FROM main._lattice_ordered_accepted_row_v1 WHERE stage=? AND page=? AND ordinal=?",{wanted[0],wanted[3],wanted[4]},1,one);if(rows.size()!=1||rows[0]!=wanted)refuse("accepted history row differs from actual staged source row");});
    if(actual[14]!=column_value_t(digest))refuse("accepted history baseline digest differs");const auto count=read(h,"SELECT COUNT(*) FROM main._lattice_ordered_accepted_row_v1 WHERE stage=?",{header.stage},1,scratch);if(integer(count[0][0])!=int64_t(header.rows))refuse("accepted history extra or missing baseline row");managed_pair_deadline(end);
}
void history_accepted_verify(database& writer,const ordinary_history_descriptor& header,const ordinary_receiver_preservation_record& local,
    const std::string& channel,const ordinary_history_limits& limits,ordinary_launch::deadline end,const values& expected){
    history_accepted_verify_inventory(writer,header,local,channel,limits,end,expected);
    budget scratch{history_stage_limits};(void)history_capacity_observe(writer,limits,scratch,true,end);managed_pair_deadline(end);
}

values history_install_first_lineage(database& writer,const ordinary_history_descriptor& header,const ordinary_receiver_preservation_record& local,
    const std::string& channel,const ordinary_history_limits& limits,ordinary_launch::deadline end){
    original_view(writer,true);managed_pair_deadline(end);budget scratch{history_stage_limits};auto* h=ordinary_export_storage_access::borrow(writer);
    preservation_verify(writer,local,limits,end);
    // An actual old ordered receipt requires genuine matched server transition.
    // Absence is observed in this pinned view, never asserted by a caller flag.
    if(receiver_storage(h,scratch)&&!read(h,"SELECT 1 FROM main._lattice_ordered_receiver_v1 LIMIT 1",{},1,scratch).empty())refuse("ordered old source binding requires genuine server scope transition");
    if(history_accepted_storage(h,scratch)){
        const auto prior=read(h,"SELECT stage FROM main._lattice_ordered_accepted_cut_v1 ORDER BY stage",{},65,scratch);
        if(!prior.empty()){
            if(prior.size()!=1||prior[0][0]!=column_value_t(header.stage))refuse("accepted lineage successor requires genuine continuity/rebind");
            auto existing=history_accepted_row(writer,header.stage,scratch);history_accepted_verify(writer,header,local,channel,limits,end,existing);return existing;
        }
    }
    register_mutation_guard(h);mutation_scope permit(h);
    if(!history_accepted_storage(h,scratch)){for(const auto& [name,ddl]:history_accepted_definitions()){writer.execute(ddl);for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}}
    if(!history_accepted_storage(h,scratch))refuse("accepted history migration did not install");history_capacity_turn capacity(writer,limits,scratch,true,end);auto shared=capacity.bytes();
    const auto reserve=[&](uint64_t amount){capacity.reserve(amount);if(amount>limits.durable_stage_bytes-shared)refuse("accepted history combined quota refused before write");shared+=amount;};
    const auto baseline=history_accepted_pages(writer,header,limits,channel,end,[&](const values& row){const auto& payload=std::get<std::vector<uint8_t>>(row[5]);reserve(text(row[0]).size()+text(row[1]).size()+text(row[2]).size()+payload.size()+64+16);
        writer.execute("INSERT INTO main._lattice_ordered_accepted_row_v1 VALUES(?,?,?,?,?,?,?)",row);budget one{history_row_limits};one.add(2*payload.size()+1024);const auto actual=read(h,"SELECT stage,model,gid,page,ordinal,value,seal FROM main._lattice_ordered_accepted_row_v1 WHERE stage=? AND page=? AND ordinal=?",{row[0],row[3],row[4]},1,one);if(writer.changes()!=1||actual.size()!=1||actual[0]!=row)refuse("accepted history row postimage differs");capacity.insert(history_capacity_class::accepted_row,actual[0]);if(capacity.bytes()!=shared)refuse("accepted row issued cost differs");});
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("accepted history actual physical store missing");
    values cut{header.stage,channel,header.projection.source,header.projection.epoch,header.projection.cut,header.projection.scope,header.projection.schema,std::to_string(physical->device),std::to_string(physical->inode),local.generation,local.snapshot,int64_t(header.rows),int64_t(header.known),int64_t(header.unknown),baseline,header.content_digest};cut.emplace_back(history_seal("ordinary-accepted-history-cut-v1",cut,16));
    uint64_t cost=40;for(const auto& cell:cut)if(const auto* value=std::get_if<std::string>(&cell))cost+=value->size();reserve(cost);
    writer.execute("INSERT INTO main._lattice_ordered_accepted_cut_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",cut);
    if(writer.changes()!=1||history_accepted_row(writer,header.stage,scratch)!=cut)refuse("accepted history complete cut postimage/quota differs");
    capacity.insert(history_capacity_class::accepted_cut,cut);if(capacity.bytes()!=shared)refuse("accepted cut issued cost differs");capacity.finish(scratch,true);
    // This local_snapshot is unresolved local-edit/deletion/conflict custody,
    // including unknown legacy originals. It is NEVER marked synced, accepted
    // as server rows, or discarded because a source row is missing/filtered.
    history_accepted_verify(writer,header,local,channel,limits,end,cut);return cut;
}
}
struct ordinary_history_final_install_guard::retained {
    const uint64_t creator=history_creator();
    lattice_db* child=nullptr;
    std::shared_ptr<ordinary_history_receive_state> state;
    std::shared_ptr<const ordinary_managed_receiver_preservation> restart;
    values accepted;
};
void ordinary_history_final_install_guard::dispose::operator()(retained* value)const noexcept{
    if(!value||value->creator!=history_creator())return;
    value->state.reset();if(value->creator!=history_creator())return;
    value->restart.reset();if(value->creator!=history_creator())return;
    delete value;
}
ordinary_history_final_install_guard::ordinary_history_final_install_guard(lattice_db& child,std::shared_ptr<ordinary_history_receive_state> state):held_(new retained){
    if(!state||!state->restart_||!state->current()||!state->progress_.complete)refuse("history final guard requires actual admitted complete episode");
    held_->child=&child;held_->restart=state->restart_;held_->state=std::move(state);
}
ordinary_history_final_install_guard::~ordinary_history_final_install_guard()=default;
void ordinary_history_final_install_guard::validate(lattice_db& child,database& writer)const{
    ordinary_export_access::validate_history_final_guard(*this,child,writer);
}
void ordinary_export_access::validate_receiver_clone_admission(const ordinary_managed_receiver_preservation& cap,lattice_db& parent,database& writer){
    if(!cap.state_||cap.state_->creator!=history_creator()||std::chrono::steady_clock::now()>=cap.state_->admission_end)refuse("restart receiver new-child admission expired");
    validate_receiver_preservation(cap,parent,writer,cap.state_->admission_end);
}
void ordinary_export_access::validate_receiver_clone_publication(const ordinary_managed_receiver_preservation& cap,lattice_db& parent,lattice_db& child){
    const auto& state=cap.state_;if(!state||state->creator!=history_creator()||state->owner.get()!=&parent||&parent==&child||parent.is_closed()||child.is_closed()||std::chrono::steady_clock::now()>=state->admission_end||parent.config_.ordinary_context!=child.config_.ordinary_context||parent.config_.path!=child.config_.path)refuse("restart receiver child lost actual current parent");
    const auto physical=child.db_->physical_identity("main",{},true);if(!physical||*physical!=state->physical)refuse("restart receiver child actual file differs");
    parent.config_.ordinary_context->validate_managed_attachment(state->admission,state->path,physical->device,physical->inode,state->admission_end);
}
void ordinary_export_access::admit_history_restart(lattice_db& child,database& writer,ordinary_history_receive_state& episode){
    const auto cap=child.ordinary_receiver_preservation_.snapshot().lock();if(!cap)return; // Exact passive/default history path.
    const auto& local=cap->state_;if(!local||local->creator!=history_creator()||!local->owner||local->owner.get()==&child||!episode.current()||episode.episode_end_==std::chrono::steady_clock::time_point::max())refuse("restart history actual child/episode admission absent");
    validate_receiver_clone_publication(*cap,*local->owner,child);
    validate_receiver_preservation(*cap,*local->owner,writer,local->admission_end);
    bool prior=false;if(!local->episode_claimed.compare_exchange_strong(prior,true,std::memory_order_acq_rel))refuse("restart receiver episode already consumed; genuine new restart required");
    // This actual native HEADER body consumes admission once. Subsequent page
    // and final settlement use the ORIGINAL native300s episode, not the30s new
    // admission cutoff. A failed header/attempt cannot reset or renew this cap.
    local->episode_end_millis.store(std::chrono::duration_cast<std::chrono::milliseconds>(episode.episode_end_.time_since_epoch()).count(),std::memory_order_release);
    episode.restart_=cap;
}
void ordinary_export_access::validate_history_final_guard(const ordinary_history_final_install_guard& guard,lattice_db& child,database& writer){
    const auto& held=guard.held_;if(!held||held->creator!=history_creator()||held->child!=&child||!held->state||!held->restart||held->state->restart_!=held->restart||!held->state->current()||!held->state->progress_.complete||child.is_closed())refuse("history final actual episode retired");
    const auto& local=held->restart->state_;if(!local||local->creator!=history_creator()||!local->owner||local->owner->is_closed()||local->owner.get()==&child||!child.config_.ordinary_context||child.config_.ordinary_context!=local->owner->config_.ordinary_context||child.config_.path!=local->path||child.ordinary_receiver_preservation_.snapshot().lock()!=held->restart)refuse("history final exact restart child association changed");
    const auto end=held->state->episode_end_;managed_pair_deadline(end);const auto physical=writer.physical_identity("main",{},true);if(!physical||*physical!=local->physical)refuse("history final actual physical store changed");
    if(local->admission->enrollment_binding_until(local->path,physical->device,physical->inode,end)!=local->record.generation)refuse("history final managed generation retired");
    child.config_.ordinary_context->validate_managed_attachment(local->admission,local->path,physical->device,physical->inode,end);
    // Full bounded recheck occurs AFTER engine flush, BEFORE its owner_finalizing
    // policy. This can veto while ownership is intact; it cannot guarantee that
    // a later logical close or unsupported already-observed COMMIT rolls back.
    history_accepted_verify(writer,held->state->descriptor_,local->record,held->state->channel_,held->state->limits_,end,held->accepted);
    if(!held->state->current()||child.is_closed()||local->owner->is_closed())refuse("history final current owner changed during validation");
}
void ordinary_export_access::install_history_accepted(lattice_db& child,const std::shared_ptr<ordinary_history_receive_state>& episode){
    ordinary_history_final_install_guard guard(child,episode);const auto& local=guard.held_->restart->state_;
    const auto settled=recovery_writer_access::ordinary_owned_history_write(child,[&](database& writer){
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);
        if(!episode->current()||!local||local->creator!=history_creator()||!local->owner||local->owner->is_closed()||child.is_closed())refuse("accepted history native owner retired before body");
        history_accepted_schema(child,writer,episode->descriptor_,child.recovery_schemas_,[](std::ostringstream& out,const property_descriptor& property){lattice_db::serialize_property_for_fingerprint(out,property);});
        guard.held_->accepted=history_install_first_lineage(writer,episode->descriptor_,local->record,episode->channel_,episode->limits_,episode->episode_end_);
    },guard);
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    // Accepted durable custody remains even if publication is now stale. No
    // rollback, RESULT, ACK, source prune, queried winner or old-scope receipt
    // can be inferred from this post-COMMIT currentness check or passive progress.
    if(!episode->current()||child.is_closed()||local->owner->is_closed())refuse("committed accepted history is no longer current for publication");
    const auto end=episode->episode_end_;if(local->admission->enrollment_binding_until(local->path,local->physical.device,local->physical.inode,end)!=local->record.generation)refuse("committed accepted history generation is stale");
}
void ordinary_export_access::install_history_accepted_for_test(lattice_db& owner,const std::string& stage,const std::string& snapshot){
    // Storage-only genuine SQL fixture, not an issuer of a managed admission,
    // physical endpoint, source transition, final guard or production receipt.
    ordinary_resource_reservation<false> charge;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){budget scratch{history_stage_limits};const auto row=history_stage_row(writer,stage,scratch);const auto& raw=std::get<std::vector<uint8_t>>(row[10]);auto header=decode_ordinary_history_descriptor(std::string_view(reinterpret_cast<const char*>(raw.data()),raw.size()),{});const auto meta=history_receive_meta_row(writer,stage,scratch);const auto local=preservation_decode(preservation_header(writer,snapshot,scratch),{});
        history_accepted_schema(owner,writer,header,owner.recovery_schemas_,[](std::ostringstream& out,const property_descriptor& p){lattice_db::serialize_property_for_fingerprint(out,p);});(void)history_install_first_lineage(writer,header,local,text(meta[1]),{},end);
    });if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
}
void ordinary_export_access::verify_history_accepted_for_test(lattice_db& owner,const std::string& stage){
    ordinary_resource_reservation<false> charge;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){budget scratch{history_stage_limits};const auto accepted=history_accepted_row(writer,stage,scratch),row=history_stage_row(writer,stage,scratch);const auto& raw=std::get<std::vector<uint8_t>>(row[10]);auto header=decode_ordinary_history_descriptor(std::string_view(reinterpret_cast<const char*>(raw.data()),raw.size()),{});const auto local=preservation_decode(preservation_header(writer,text(accepted[10]),scratch),{});history_accepted_verify(writer,header,local,text(accepted[1]),{},end,accepted);
    });if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
}
