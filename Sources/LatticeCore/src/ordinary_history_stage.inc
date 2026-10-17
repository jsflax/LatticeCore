// Included only by ordinary_export.cpp, inside lattice::detail. This uses the
// same closed owned-WRITE guard and bounded SQL readers as source issuance.
namespace {
constexpr export_limits history_metadata_limits{256,2050,8192,2097152,ordinary_history_limits::metadata_bytes};
constexpr export_limits history_row_limits{256,2050,8192,2097152,ordinary_history_limits::row_scratch_bytes};
constexpr export_limits history_stage_limits{256,2050,8192,2097152,ordinary_history_limits::stage_scratch_bytes};
constexpr const char* history_stage_table="_lattice_ordered_history_stage_v1";
constexpr const char* history_page_table="_lattice_ordered_history_page_v1";
const std::vector<std::pair<std::string,std::string>>& history_definitions(){
    static const std::vector<std::pair<std::string,std::string>> definitions{
        {history_stage_table,ordinary_history_storage_recipe::stage},
        {history_page_table,ordinary_history_storage_recipe::page}};
    return definitions;
}
void history_limits(const ordinary_history_limits& limits){
    // The local policy is configurable within a finite implementation ceiling.
    // Neither the peer nor decoded header can request a larger allocation.
    if(!limits.durable_stage_bytes||limits.durable_stage_bytes>uint64_t{8}*1024*1024*1024||
       !limits.records||limits.records>uint64_t{16}*1024*1024||!limits.pages||limits.pages>262144||
       !limits.records_per_page||limits.records_per_page>64||limits.page_bytes<2048||limits.page_bytes>ordinary_sync_limits::frame_bytes)
        refuse("paged history local resource profile bound");
}
bool history_receive_storage(sqlite3*,budget&);
uint64_t history_receive_extra_bytes(database&,budget&);
uint64_t receiver_preservation_extra_bytes(database&,budget&);
bool history_accepted_storage(sqlite3*,budget&);
const std::vector<std::pair<std::string,std::string>>& history_accepted_definitions();
bool history_storage(sqlite3* h,budget& retained){
    const auto actual=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE (name GLOB '_lattice_ordered_history_*' OR tbl_name GLOB '_lattice_ordered_history_*') AND name NOT GLOB '_lattice_ordered_history_receive*' AND tbl_name NOT GLOB '_lattice_ordered_history_receive*' ORDER BY name",{},16,retained);
    if(actual.empty()){if(history_receive_storage(h,retained))refuse("history receiving inventory lost shared storage");return false;}
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:history_definitions()){
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t(ddl)));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t(nullptr)));
        for(const auto* op:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+op,std::make_pair(std::string("trigger"),column_value_t(receiver_guard(name,op))));
    }
    if(actual.size()!=expected.size())refuse("paged history schema inventory differs");
    for(const auto& row:actual){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("paged history schema or guard differs");}
    (void)history_receive_storage(h,retained);return true;
}
uint64_t history_stage_cost(const ordinary_history_stage_identity& id){
    return id.stage.size()+id.source.size()+id.scope.size()+id.schema.size()+id.device.size()+id.inode.size()+80+64; // seal, initially empty header/content
}
std::string history_seal(const char* domain,const values& row,size_t count){return ordinary_history_storage_seal(domain,row,count);}
values history_stage_row(database& writer,const std::string& stage,budget& retained){
    const auto rows=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,role,source,epoch,cut,scope,schema,device,inode,state,header,rows,known,unknown,pages,bytes,content,seal FROM main._lattice_ordered_history_stage_v1 WHERE stage=?",{stage},1,retained);
    if(rows.size()!=1)refuse("paged history stage unavailable");const auto& row=rows[0];
    if(text(row[0])!=stage||text(row[17])!=history_seal("ordinary-paged-stage-v1",row,17))refuse("paged history stage seal differs");return row;
}
}
#include "ordinary_history_capacity_declarations.inc"
namespace {
uint64_t history_shared_bytes(database& writer,const ordinary_history_limits& limits,budget& retained){return history_capacity_observe(writer,limits,retained);}
}
struct ordinary_history_stage_writer::state {
    database& writer;ordinary_history_stage_identity identity;ordinary_history_limits limits;
    ordinary_history_stage_totals totals;std::unique_ptr<history_capacity_turn> capacity;uint64_t shared=0;std::string previous;bool finished=false,failed=false;
    state(database& w,ordinary_history_stage_identity i,const ordinary_history_limits& l):writer(w),identity(std::move(i)),limits(l){}
    void available(){original_view(writer,true);if(finished||failed)refuse("paged history writer is settled or failed");}
    void reserve(uint64_t amount){if(amount>limits.durable_stage_bytes-shared)refuse("paged history shared durable quota refused before write");}
};
ordinary_history_stage_writer::ordinary_history_stage_writer(database& writer,const ordinary_history_stage_identity& identity,const ordinary_history_limits& limits){
    original_view(writer,true);history_limits(limits);
    if(identity.receive)refuse("paged receive staging requires its authenticated integration");
    if(!canonical_uuid(identity.stage)||!canonical_uuid(identity.source)||identity.epoch<=0||identity.cut<=0||identity.scope.empty()||identity.scope.size()>256*(256+4)||identity.schema.empty()||identity.schema.size()>ordinary_sync_limits::schema_bytes)
        refuse("paged history actual source identity bound");
    const auto physical=writer.physical_identity("main",{},true);if(!physical||identity.device!=std::to_string(physical->device)||identity.inode!=std::to_string(physical->inode))refuse("paged history source physical identity changed");
    budget retained{history_stage_limits};if(!inspect_storage(ordinary_export_storage_access::borrow(writer),retained))refuse("paged history has no issued source storage");
    const auto binding=read(ordinary_export_storage_access::borrow(writer),"SELECT source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{identity.scope},1,retained);
    if(binding.size()!=1||binding[0]!=values{identity.source,identity.epoch,identity.cut,identity.device,identity.inode,identity.schema})refuse("paged history lost its actual captured source coordinate");
    register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    if(!history_storage(ordinary_export_storage_access::borrow(writer),retained)){
        for(const auto& [name,ddl]:history_definitions()){writer.execute(ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}
        if(!history_storage(ordinary_export_storage_access::borrow(writer),retained))refuse("paged history storage did not install");
    }
    const auto active=read(ordinary_export_storage_access::borrow(writer),"SELECT COUNT(*) FROM main._lattice_ordered_history_stage_v1",{},1,retained);
    if(integer(active[0][0])>=64)refuse("paged history retained stage count exhausted");
    auto state=std::make_unique<ordinary_history_stage_writer::state>(writer,identity,limits);state->capacity=std::make_unique<history_capacity_turn>(writer,limits,retained,true);state->shared=state->capacity->bytes();state->reserve(history_stage_cost(identity));
    values row{identity.stage,int64_t{1},identity.source,identity.epoch,identity.cut,identity.scope,identity.schema,identity.device,identity.inode,int64_t{0},std::vector<uint8_t>{},int64_t{0},int64_t{0},int64_t{0},int64_t{0},int64_t{0},std::string{}};
    row.emplace_back(history_seal("ordinary-paged-stage-v1",row,17));writer.execute("INSERT INTO main._lattice_ordered_history_stage_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row);
    if(writer.changes()!=1||history_stage_row(writer,identity.stage,retained)!=row)refuse("paged history initial stage postimage differs");
    state->shared+=history_stage_cost(identity);state->capacity->insert(history_capacity_class::stage,row);if(state->capacity->bytes()!=state->shared)refuse("paged history initial shared accounting differs");state_=std::move(state);
}
ordinary_history_stage_writer::~ordinary_history_stage_writer()=default;
void ordinary_history_stage_writer::append(std::string_view page,std::string_view previous,std::string_view digest,uint64_t rows,uint64_t known,uint64_t unknown){
    auto& s=*state_;s.available();
    try {
        if(page.empty()||page.size()>s.limits.page_bytes||s.totals.pages==s.limits.pages||rows>s.limits.records_per_page||known>s.limits.records_per_page-rows||unknown>s.limits.records_per_page-rows-known||(rows+known+unknown)==0)
            refuse("paged history page bound");
        const auto records=rows+known+unknown,total=s.totals.rows+s.totals.known+s.totals.unknown;
        if(records>s.limits.records-total||previous!=s.previous)refuse("paged history count or page chain differs");
        const auto valid=[](std::string_view v){return v.size()==64&&std::all_of(v.begin(),v.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');});};
        if(!valid(digest)||digest!=picosha2::hash256_hex_string(page.begin(),page.end())||(!previous.empty()&&!valid(previous)))refuse("paged history digest spelling");
        const auto cost=s.identity.stage.size()+previous.size()+digest.size()+page.size()+64+32;s.reserve(cost);
        // This page's scratch and SQL copies die before the next append. Its
        // budget is intentionally not a cumulative whole-history charge.
        budget scratch{history_stage_limits};scratch.add(3*page.size()+1024);
        values row{s.identity.stage,static_cast<int64_t>(s.totals.pages),std::string(previous),std::string(digest),std::vector<uint8_t>(page.begin(),page.end()),static_cast<int64_t>(rows),static_cast<int64_t>(known),static_cast<int64_t>(unknown)};
        row.emplace_back(history_seal("ordinary-paged-page-v1",row,8));
        // The private constructor installed this SAME connection's guard before
        // streaming. Re-registering it while the source cursor is on ROW fails.
        mutation_scope permit(ordinary_export_storage_access::borrow(s.writer));
        s.writer.execute("INSERT INTO main._lattice_ordered_history_page_v1 VALUES(?,?,?,?,?,?,?,?,?)",row);
        const auto actual=read(ordinary_export_storage_access::borrow(s.writer),"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{s.identity.stage,static_cast<int64_t>(s.totals.pages)},1,scratch);
        if(s.writer.changes()!=1||actual.size()!=1||actual[0]!=row)refuse("paged history page postimage differs");
        s.capacity->insert(history_capacity_class::page,row);s.shared+=cost;s.totals.bytes+=page.size();s.totals.rows+=rows;s.totals.known+=known;s.totals.unknown+=unknown;++s.totals.pages;s.previous=digest;
    }catch(...){s.failed=true;throw;}
}
ordinary_history_stage_totals ordinary_history_stage_writer::finish(std::string_view header,std::string_view content){
    auto& s=*state_;s.available();
    try {
        if(header.empty()||header.size()>s.limits.page_bytes||content.size()!=64||!std::all_of(content.begin(),content.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))refuse("paged history final header bound");
        s.reserve(header.size()+content.size());budget retained{history_stage_limits};retained.add(3*header.size()+1024);
        const auto counts=read(ordinary_export_storage_access::borrow(s.writer),"SELECT COUNT(*),COALESCE(SUM(rows),0),COALESCE(SUM(known),0),COALESCE(SUM(unknown),0),COALESCE(SUM(length(payload)),0),COALESCE(MIN(page),0),COALESCE(MAX(page),-1) FROM main._lattice_ordered_history_page_v1 WHERE stage=?",{s.identity.stage},1,retained);
        if(counts.size()!=1||counts[0]!=values{static_cast<int64_t>(s.totals.pages),static_cast<int64_t>(s.totals.rows),static_cast<int64_t>(s.totals.known),static_cast<int64_t>(s.totals.unknown),static_cast<int64_t>(s.totals.bytes),int64_t{0},s.totals.pages?static_cast<int64_t>(s.totals.pages-1):int64_t{-1}})refuse("paged history final page counts differ");
        auto row=history_stage_row(s.writer,s.identity.stage,retained);const auto before=row;row[9]=int64_t{1};row[10]=std::vector<uint8_t>(header.begin(),header.end());row[11]=static_cast<int64_t>(s.totals.rows);row[12]=static_cast<int64_t>(s.totals.known);row[13]=static_cast<int64_t>(s.totals.unknown);row[14]=static_cast<int64_t>(s.totals.pages);row[15]=static_cast<int64_t>(s.totals.bytes+header.size());row[16]=std::string(content);row[17]=history_seal("ordinary-paged-stage-v1",row,17);
        register_mutation_guard(ordinary_export_storage_access::borrow(s.writer));mutation_scope permit(ordinary_export_storage_access::borrow(s.writer));
        s.writer.execute("UPDATE main._lattice_ordered_history_stage_v1 SET state=?,header=?,rows=?,known=?,unknown=?,pages=?,bytes=?,content=?,seal=? WHERE stage=? AND state=0",{row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[0]});
        if(s.writer.changes()!=1||history_stage_row(s.writer,s.identity.stage,retained)!=row||!history_storage(ordinary_export_storage_access::borrow(s.writer),retained))refuse("paged history sealed header postimage differs");
        s.capacity->replace(history_capacity_class::stage,before,row);s.shared+=header.size()+content.size();if(s.capacity->bytes()!=s.shared)refuse("paged history final shared quota differs");s.capacity->finish(retained,true);
        const auto physical=s.writer.physical_identity("main",{},true);if(!physical||s.identity.device!=std::to_string(physical->device)||s.identity.inode!=std::to_string(physical->inode))refuse("paged history physical identity changed before source commit");
        const auto binding=read(ordinary_export_storage_access::borrow(s.writer),"SELECT source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{s.identity.scope},1,retained);
        if(binding.size()!=1||binding[0]!=values{s.identity.source,s.identity.epoch,s.identity.cut,s.identity.device,s.identity.inode,s.identity.schema})refuse("paged history source changed before own commit");
        s.totals.bytes+=header.size();s.finished=true;return s.totals;
    }catch(...){s.failed=true;throw;}
}
