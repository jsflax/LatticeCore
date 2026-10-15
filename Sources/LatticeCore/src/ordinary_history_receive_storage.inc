// Private passive staging. Included after history source helpers so both roles
// use one guarded inventory and one durable quota. These records never enroll
// a receiver or widen its canonical/original profile.
namespace {
constexpr const char* history_receive_meta="_lattice_ordered_history_receive_v1";
constexpr const char* history_receive_key="_lattice_ordered_history_receive_key_v1";
const std::vector<std::pair<std::string,std::string>>& history_receive_definitions(){
    static const std::vector<std::pair<std::string,std::string>> definitions{
        {history_receive_meta,ordinary_history_storage_recipe::meta},
        {history_receive_key,ordinary_history_storage_recipe::key}};
    return definitions;
}
bool history_receive_storage(sqlite3* h,budget& retained){
    const auto actual=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_history_receive_*' OR name='_lattice_ordered_history_receive_v1' OR tbl_name GLOB '_lattice_ordered_history_receive_*' OR tbl_name='_lattice_ordered_history_receive_v1' ORDER BY name",{},16,retained);
    if(actual.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:history_receive_definitions()){
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t(ddl)));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t(nullptr)));
        if(name==history_receive_key)expected.emplace("sqlite_autoindex_"+name+"_2",std::make_pair(std::string("index"),column_value_t(nullptr)));
        for(const auto* op:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+op,std::make_pair(std::string("trigger"),column_value_t(receiver_guard(name,op))));
    }
    if(actual.size()!=expected.size())refuse("history receive schema inventory differs");
    for(const auto& row:actual){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("history receive schema or guard differs");}
    return true;
}
uint64_t history_receive_extra_bytes(database& writer,budget& retained){
    if(!history_receive_storage(ordinary_export_storage_access::borrow(writer),retained))return 0;
    const auto rows=read(ordinary_export_storage_access::borrow(writer),"SELECT (SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(channel AS BLOB))+length(CAST(seal AS BLOB))+48),0) FROM main._lattice_ordered_history_receive_v1)+(SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(model AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(seal AS BLOB))+24),0) FROM main._lattice_ordered_history_receive_key_v1)",{},1,retained);
    const auto total=integer(rows.at(0).at(0));if(total<0)refuse("history receive quota is invalid");return uint64_t(total);
}
values history_receive_meta_row(database& writer,const std::string& stage,budget& retained){
    const auto rows=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,channel,records_limit,pages_limit,page_limit,records_per_page,bytes_limit,last_table,seal FROM main._lattice_ordered_history_receive_v1 WHERE stage=?",{stage},1,retained);
    if(rows.size()!=1||text(rows[0][8])!=history_seal("ordinary-history-receive-meta-v1",rows[0],8))refuse("history receive metadata missing or changed");return rows[0];
}
ordinary_history_progress history_receive_progress(const values& stage,const values& meta,const ordinary_history_descriptor& header,const ordinary_history_limits& limits,const std::string& channel,const std::string& device,const std::string& inode){
    const auto& f=header.projection;
    if(stage.size()!=18||stage[0]!=column_value_t(header.stage)||integer(stage[1])!=2||stage[2]!=column_value_t(f.source)||integer(stage[3])!=f.epoch||integer(stage[4])!=f.cut||stage[5]!=column_value_t(f.scope)||stage[6]!=column_value_t(f.schema)||stage[7]!=column_value_t(device)||stage[8]!=column_value_t(inode)||stage[16]!=column_value_t(header.content_digest))refuse("history receive stage association changed");
    const auto* raw=std::get_if<std::vector<uint8_t>>(&stage[10]);if(!raw||raw->size()!=header.header.size()||!std::equal(raw->begin(),raw->end(),header.header.begin()))refuse("history receive descriptor changed");
    if(meta[0]!=stage[0]||meta[1]!=column_value_t(channel)||integer(meta[2])!=int64_t(limits.records)||integer(meta[3])!=int64_t(limits.pages)||integer(meta[4])!=limits.page_bytes||integer(meta[5])!=limits.records_per_page||integer(meta[6])!=int64_t(limits.durable_stage_bytes))refuse("history receive channel or local profile changed");
    for(size_t n=11;n<=15;++n)if(integer(stage[n])<0)refuse("history receive negative progress");
    ordinary_history_progress p;p.rows=uint64_t(integer(stage[11]));p.known=uint64_t(integer(stage[12]));p.unknown=uint64_t(integer(stage[13]));p.next_page=uint64_t(integer(stage[14]));p.last_table=integer(meta[7]);p.complete=integer(stage[9])==1;
    if((integer(stage[9])!=0&&!p.complete)||p.rows>header.rows||p.known>header.known||p.unknown>header.unknown||p.next_page>header.pages||p.last_table < -1||p.last_table>=int64_t(f.tables.size())||p.complete!=(p.next_page==header.pages)||uint64_t(integer(stage[15]))>limits.durable_stage_bytes)refuse("history receive progress bound changed");
    if(p.complete&&(p.rows!=header.rows||p.known!=header.known||p.unknown!=header.unknown))refuse("history complete stage has missing records");return p;
}
// Per-record keys are passive pointers into exact retained page bytes. SQL
// NOCASE matches actual model/AuditLog identity. No historical receipt or
// accepted original is manufactured by this index.
values history_receive_record_key(const ordinary_history_descriptor& header,const ordinary_history_decoded_page& page,size_t at,ordinary_history_progress& next){
    return ordinary_history_record_key_recipe::key(header,page,at,next);
}
void history_receive_verify_page(database& writer,const ordinary_history_descriptor& header,const ordinary_history_decoded_page& decoded,ordinary_history_progress& progress,budget& scratch){
    const auto keys=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,role,model,gid,page,ordinal,seal FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=? AND page=? ORDER BY ordinal",{header.stage,int64_t(decoded.index)},64,scratch);
    if(keys.size()!=decoded.records.size())refuse("history page key count changed");
    for(size_t n=0;n<keys.size();++n){scratch.add(2*(header.stage.size()+256+4096)+7*sizeof(column_value_t)+128);if(keys[n]!=history_receive_record_key(header,decoded,n,progress))refuse("history page key changed");}
    ++progress.next_page;progress.previous=decoded.full_digest;
}
}

ordinary_history_progress ordinary_export_access::stage_history(lattice_db& owner,const std::shared_ptr<ordinary_history_receive_operation>& operation){
    if(!operation||!operation->state_)refuse("history staging requires its private admitted operation");auto& state=*operation->state_;const auto& header=state.descriptor_;const auto& frame=header.projection;const auto& limits=state.limits_;
    history_limits(limits);const bool had_page=operation->page_.has_value();ordinary_history_progress committed_progress;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);
        if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_||!state.current())refuse("history staging lost actual ordinary session");
        auto* h=ordinary_export_storage_access::borrow(writer);const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("history receiving physical identity unavailable");const auto device=std::to_string(physical->device),inode=std::to_string(physical->inode);
        // Validate only the actual local schema. Do not read/retain the model
        // rows, enroll receiver metadata, or obtain a producer capability.
        {ordinary_export_frame actual;actual.wire_profile=true;budget metadata{history_metadata_limits};
        capture_projected_tables(h,owner.recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},frame.models,actual,metadata,false,nullptr,nullptr);
        if(actual.scope!=frame.scope||actual.schema!=frame.schema||actual.tables.size()!=frame.tables.size())refuse("history schema differs from actual local projection");
        for(size_t n=0;n<actual.tables.size();++n)if(actual.tables[n].name!=frame.tables[n].name||actual.tables[n].columns!=frame.tables[n].columns||actual.tables[n].types!=frame.tables[n].types)refuse("history column projection differs");}
        if(!operation->page_&&!state.restart_)admit_history_restart(owner,writer,state);
        // Existing bindings are continuity constraints, never first authority.
        // No absence branch inserts any source/receiver/helper row here.
        budget binding_budget{history_stage_limits};if(receiver_storage(h,binding_budget)){
            const auto binding=read(h,"SELECT source,epoch,device,inode,schema FROM main._lattice_ordered_receiver_v1 WHERE scope=?",{frame.scope},1,binding_budget);
            if(!binding.empty()&&binding[0]!=values{frame.source,frame.epoch,device,inode,frame.schema})refuse("history source change requires explicit bootstrap");
        }
        register_mutation_guard(h);mutation_scope permit(h);budget scratch{history_stage_limits};
        if(!history_storage(h,scratch)){
            if(history_receive_storage(h,scratch))refuse("history receiving inventory lost shared stage tables");
            for(const auto& [name,ddl]:history_definitions()){writer.execute(ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}
        }
        if(!history_receive_storage(h,scratch)){
            for(const auto& [name,ddl]:history_receive_definitions()){writer.execute(ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}
        }
        if(!history_storage(h,scratch)||!history_receive_storage(h,scratch))refuse("history receiving inventory did not install");
        history_capacity_turn capacity(writer,limits,scratch,!had_page);auto shared=capacity.bytes();
        const auto reserve=[&](uint64_t n){if(n>limits.durable_stage_bytes-shared)refuse("history shared durable quota refused before write");shared+=n;};
        const auto exists=read(h,"SELECT 1 FROM main._lattice_ordered_history_stage_v1 WHERE stage=?",{header.stage},1,scratch);
        if(exists.empty()){
            if(operation->page_)refuse("history page has no durable header");const auto count=read(h,"SELECT COUNT(*) FROM main._lattice_ordered_history_stage_v1",{},1,scratch);if(integer(count[0][0])>=64)refuse("history retained stage capacity");
            const ordinary_history_stage_identity identity{header.stage,frame.source,frame.scope,frame.schema,device,inode,frame.epoch,frame.cut,true};
            reserve(history_stage_cost(identity)+header.header.size()+header.content_digest.size()+header.stage.size()+state.channel_.size()+48+64);
            scratch.add(3*header.header.size()+1024);values row{header.stage,int64_t{2},frame.source,frame.epoch,frame.cut,frame.scope,frame.schema,device,inode,int64_t(header.pages==0),std::vector<uint8_t>(header.header.begin(),header.header.end()),int64_t{0},int64_t{0},int64_t{0},int64_t{0},int64_t(header.header.size()),header.content_digest};row.emplace_back(history_seal("ordinary-paged-stage-v1",row,17));
            writer.execute("INSERT INTO main._lattice_ordered_history_stage_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row);if(writer.changes()!=1)refuse("history initial stage write ignored");
            values meta{header.stage,state.channel_,int64_t(limits.records),int64_t(limits.pages),int64_t(limits.page_bytes),int64_t(limits.records_per_page),int64_t(limits.durable_stage_bytes),int64_t{-1}};meta.emplace_back(history_seal("ordinary-history-receive-meta-v1",meta,8));writer.execute("INSERT INTO main._lattice_ordered_history_receive_v1 VALUES(?,?,?,?,?,?,?,?,?)",meta);if(writer.changes()!=1)refuse("history initial metadata write ignored");
            if(history_stage_row(writer,header.stage,scratch)!=row||history_receive_meta_row(writer,header.stage,scratch)!=meta)refuse("history initial receiving postimage differs");capacity.insert(history_capacity_class::stage,row);capacity.insert(history_capacity_class::receive,meta);
        }
        auto row=history_stage_row(writer,header.stage,scratch);auto meta=history_receive_meta_row(writer,header.stage,scratch);auto progress=history_receive_progress(row,meta,header,limits,state.channel_,device,inode);
        if(progress.next_page){const auto previous=read(h,"SELECT digest FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{header.stage,int64_t(progress.next_page-1)},1,scratch);if(previous.size()!=1)refuse("history prior page missing");progress.previous=text(previous[0][0]);}
        if(operation->page_){
            const auto& page=*operation->page_;if(progress.complete||page.index!=progress.next_page||page.previous!=progress.previous||progress.next_page!=state.progress_.next_page||progress.rows!=state.progress_.rows||progress.known!=state.progress_.known||progress.unknown!=state.progress_.unknown||progress.last_table!=state.progress_.last_table)refuse("history durable progress differs from admitted page");
            auto next=progress;uint64_t rows=0,known=0,unknown=0;
            // Index and raw page writes are in this same actual WRITE. Any
            // duplicate/capacity/COMMIT refusal rolls back every one of them.
            for(size_t n=0;n<page.records.size();++n){scratch.add(2*(header.stage.size()+256+4096)+7*sizeof(column_value_t)+128);auto key=history_receive_record_key(header,page,n,next);reserve(header.stage.size()+text(key[2]).size()+text(key[3]).size()+24+64);writer.execute("INSERT INTO main._lattice_ordered_history_receive_key_v1 VALUES(?,?,?,?,?,?,?)",key);if(writer.changes()!=1)refuse("history key write ignored");capacity.insert(history_capacity_class::key,key);}
            rows=next.rows-progress.rows;known=next.known-progress.known;unknown=next.unknown-progress.unknown;
            reserve(header.stage.size()+page.previous.size()+page.full_digest.size()+page.raw.size()+32+64);budget page_scratch{history_stage_limits};page_scratch.add(scratch.bytes);page_scratch.add(3*page.raw.size()+1024);
            values stored{header.stage,int64_t(page.index),page.previous,page.full_digest,std::vector<uint8_t>(page.raw.begin(),page.raw.end()),int64_t(rows),int64_t(known),int64_t(unknown)};stored.emplace_back(history_seal("ordinary-paged-page-v1",stored,8));writer.execute("INSERT INTO main._lattice_ordered_history_page_v1 VALUES(?,?,?,?,?,?,?,?,?)",stored);
            const auto verified=read(h,"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{header.stage,int64_t(page.index)},1,page_scratch);if(writer.changes()!=1||verified.size()!=1||verified[0]!=stored)refuse("history staged page postimage differs");capacity.insert(history_capacity_class::page,stored);
            ordinary_history_progress indexed=progress;history_receive_verify_page(writer,header,page,indexed,page_scratch);next.next_page=indexed.next_page;next.previous=indexed.previous;next.complete=next.next_page==header.pages;
            const auto before_row=row,before_meta=meta;row[9]=int64_t(next.complete);row[11]=int64_t(next.rows);row[12]=int64_t(next.known);row[13]=int64_t(next.unknown);row[14]=int64_t(next.next_page);row[15]=integer(row[15])+int64_t(page.raw.size());row[17]=history_seal("ordinary-paged-stage-v1",row,17);meta[7]=next.last_table;meta[8]=history_seal("ordinary-history-receive-meta-v1",meta,8);
            writer.execute("UPDATE main._lattice_ordered_history_stage_v1 SET state=?,rows=?,known=?,unknown=?,pages=?,bytes=?,seal=? WHERE stage=?",{row[9],row[11],row[12],row[13],row[14],row[15],row[17],row[0]});if(writer.changes()!=1)refuse("history stage progress write ignored");writer.execute("UPDATE main._lattice_ordered_history_receive_v1 SET last_table=?,seal=? WHERE stage=?",{meta[7],meta[8],meta[0]});if(writer.changes()!=1)refuse("history metadata progress write ignored");
            if(history_stage_row(writer,header.stage,page_scratch)!=row||history_receive_meta_row(writer,header.stage,page_scratch)!=meta)refuse("history durable progress write differs");capacity.replace(history_capacity_class::stage,before_row,row);capacity.replace(history_capacity_class::receive,before_meta,meta);progress=next;
        }
        // Header resume and final page verify the full retained chain with one
        // page at a time. No all-history set or materialized postimage exists.
        if(!had_page||progress.complete){
            // The just-written input has no further consumer. Release its
            // decoded containers before overlapping resume parser workspace.
            operation->page_.reset();
            ordinary_history_progress checked;picosha2::hash256_one_by_one content;uint64_t bytes=header.header.size();
            for(uint64_t n=0;n<progress.next_page;++n){budget page_budget{history_stage_limits};page_budget.add(scratch.bytes);const auto pages=read(h,"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{header.stage,int64_t(n)},1,page_budget);if(pages.size()!=1)refuse("history resume page missing");const auto& p=pages[0];const auto* payload=std::get_if<std::vector<uint8_t>>(&p[4]);if(!payload||payload->size()>limits.page_bytes||text(p[8])!=history_seal("ordinary-paged-page-v1",p,8))refuse("history resume page changed");
                const std::string_view raw(reinterpret_cast<const char*>(payload->data()),payload->size());auto decoded=decode_ordinary_history_page(raw,header,checked,limits);if(p[2]!=column_value_t(decoded.previous)||p[3]!=column_value_t(decoded.full_digest))refuse("history resume page chain differs");const auto before=checked;history_receive_verify_page(writer,header,decoded,checked,page_budget);if(integer(p[5])!=int64_t(checked.rows-before.rows)||integer(p[6])!=int64_t(checked.known-before.known)||integer(p[7])!=int64_t(checked.unknown-before.unknown))refuse("history page counters changed");content.process(payload->begin(),payload->end());bytes+=payload->size();
            }
            if(checked.next_page!=progress.next_page||checked.rows!=progress.rows||checked.known!=progress.known||checked.unknown!=progress.unknown||checked.last_table!=progress.last_table||bytes!=uint64_t(integer(row[15])))refuse("history resume progress differs from retained pages");
            budget counts_budget{history_stage_limits};const auto counts=read(h,"SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_page_v1 WHERE stage=?),(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=?)",{header.stage,header.stage},1,counts_budget);if(counts[0]!=values{int64_t(progress.next_page),int64_t(progress.rows+progress.known+progress.unknown)})refuse("history staged extra page or key");
            if(progress.complete){content.finish();if(picosha2::get_hash_hex_string(content)!=header.content_digest)refuse("history complete content digest differs");}
            progress.previous=checked.previous;
        }
        budget finish{history_stage_limits};finish.add(scratch.bytes);if(capacity.bytes()!=shared||!history_storage(h,finish)||!history_receive_storage(h,finish))refuse("history receiving quota/inventory changed before commit");capacity.finish(finish,!had_page||progress.complete);
        const auto after=writer.physical_identity("main",{},true);if(!after||after->device!=physical->device||after->inode!=physical->inode||!state.current())refuse("history stage lost physical session before commit");committed_progress=progress;
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    return committed_progress;
}
