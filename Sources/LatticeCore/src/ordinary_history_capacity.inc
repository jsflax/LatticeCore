// Included after all eight owning schemas, preservation and accepted inventory verifiers.
// Counters are atomically derived from actual successful native mutations.
// Checksums detect malformed custody; they are not authentication/issuance.
namespace {
constexpr const char* history_capacity_table="_lattice_ordered_capacity_v1";
constexpr const char* history_capacity_ddl="CREATE TABLE _lattice_ordered_capacity_v1 (id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL CHECK(revision>0),stages INTEGER NOT NULL CHECK(stages>=0),stage_bytes INTEGER NOT NULL CHECK(stage_bytes>=0),pages INTEGER NOT NULL CHECK(pages>=0),page_bytes INTEGER NOT NULL CHECK(page_bytes>=0),receives INTEGER NOT NULL CHECK(receives>=0),receive_bytes INTEGER NOT NULL CHECK(receive_bytes>=0),keys INTEGER NOT NULL CHECK(keys>=0),key_bytes INTEGER NOT NULL CHECK(key_bytes>=0),snapshots INTEGER NOT NULL CHECK(snapshots>=0),snapshot_bytes INTEGER NOT NULL CHECK(snapshot_bytes>=0),records INTEGER NOT NULL CHECK(records>=0),record_bytes INTEGER NOT NULL CHECK(record_bytes>=0),seal TEXT NOT NULL)";
// Fourteen actual fixed INTEGER cells + the exact 64-byte seal. This new
// durable logical payload shares the original pool; SQLite/WAL is additional.
constexpr uint64_t history_capacity_overhead=14*8+64;
constexpr uint64_t history_capacity_eight_overhead=18*8+64;
constexpr const char* history_capacity_eight_ddl="CREATE TABLE _lattice_ordered_capacity_v1 (id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL CHECK(revision>0),stages INTEGER NOT NULL CHECK(stages>=0),stage_bytes INTEGER NOT NULL CHECK(stage_bytes>=0),pages INTEGER NOT NULL CHECK(pages>=0),page_bytes INTEGER NOT NULL CHECK(page_bytes>=0),receives INTEGER NOT NULL CHECK(receives>=0),receive_bytes INTEGER NOT NULL CHECK(receive_bytes>=0),keys INTEGER NOT NULL CHECK(keys>=0),key_bytes INTEGER NOT NULL CHECK(key_bytes>=0),snapshots INTEGER NOT NULL CHECK(snapshots>=0),snapshot_bytes INTEGER NOT NULL CHECK(snapshot_bytes>=0),records INTEGER NOT NULL CHECK(records>=0),record_bytes INTEGER NOT NULL CHECK(record_bytes>=0),accepted_cuts INTEGER NOT NULL CHECK(accepted_cuts>=0),accepted_cut_bytes INTEGER NOT NULL CHECK(accepted_cut_bytes>=0),accepted_rows INTEGER NOT NULL CHECK(accepted_rows>=0),accepted_row_bytes INTEGER NOT NULL CHECK(accepted_row_bytes>=0),seal TEXT NOT NULL)";
const char* history_capacity_domain(size_t cells){if(cells==15)return "ordinary-history-capacity-v1";if(cells==19)return "ordinary-history-capacity-v2";refuse("history capacity exact row shape");}

uint64_t history_capacity_creator()noexcept {
#if defined(__APPLE__) || defined(__linux__)
    return static_cast<uint64_t>(::getpid());
#else
    return 0;
#endif
}
void history_capacity_deadline(ordinary_launch::deadline end){
    if(std::chrono::steady_clock::now()>=end)refuse("history capacity owning deadline expired");
}
struct history_capacity_dispose {void operator()(history_capacity_witness* p)const noexcept {
    if(p&&p->creator==history_capacity_creator())delete p;
}};
int64_t history_capacity_pragma(sqlite3* h,const char* sql,budget& scratch){
    const auto rows=read(h,sql,{},1,scratch);
    if(rows.size()!=1||rows[0].size()!=1||integer(rows[0][0])<0)refuse("history writer version unavailable");
    return integer(rows[0][0]);
}
int history_capacity_storage(sqlite3* h,budget& scratch){
    const auto temp=read(h,"SELECT name FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_capacity*' OR tbl_name GLOB '_lattice_ordered_capacity*'",{},1,scratch);
    if(!temp.empty())refuse("history capacity temporary shadow");
    const auto rows=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_capacity*' OR tbl_name GLOB '_lattice_ordered_capacity*' ORDER BY name",{},5,scratch);
    if(rows.empty())return 0;
    const auto table=std::find_if(rows.begin(),rows.end(),[](const values& row){return row[1]==column_value_t(history_capacity_table)&&row[0]==column_value_t(std::string("table"));});
    if(table==rows.end())refuse("history capacity table inventory lost");
    const auto& ddl=text((*table)[2]);const int format=ddl==history_capacity_ddl?1:(ddl==history_capacity_eight_ddl?2:0);
    if(!format)refuse("history capacity schema version differs");
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    expected.emplace(history_capacity_table,std::make_pair(std::string("table"),column_value_t(ddl)));
    for(const auto* op:{"INSERT","UPDATE","DELETE"})expected.emplace(std::string(history_capacity_table)+'_'+op,std::make_pair(std::string("trigger"),column_value_t(receiver_guard(history_capacity_table,op))));
    if(rows.size()!=expected.size())refuse("history capacity exact program inventory differs");
    for(const auto& row:rows){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("history capacity program or guard changed");}
    return format;
}
values history_capacity_read(sqlite3* h,budget& scratch){
    const auto format=history_capacity_storage(h,scratch);if(!format)refuse("history capacity row has no exact storage");
    const auto extra=format==2?std::string(",accepted_cuts,accepted_cut_bytes,accepted_rows,accepted_row_bytes"):std::string{};
    const auto rows=read(h,"SELECT id,revision,stages,stage_bytes,pages,page_bytes,receives,receive_bytes,keys,key_bytes,snapshots,snapshot_bytes,records,record_bytes"+extra+",seal FROM main._lattice_ordered_capacity_v1 ORDER BY id",{},2,scratch);
    const size_t cells=format==2?19:15,seal=cells-1;
    if(rows.size()!=1||rows[0].size()!=cells||integer(rows[0][0])!=1||integer(rows[0][1])<=0||text(rows[0][seal])!=history_seal(history_capacity_domain(cells),rows[0],seal))refuse("history capacity row missing or malformed");
    for(size_t n=2;n<seal;++n)if(integer(rows[0][n])<0)refuse("history capacity negative counters");return rows[0];
}
uint64_t history_capacity_total(const values& row){
    (void)history_capacity_domain(row.size());uint64_t total=row.size()==19?history_capacity_eight_overhead:history_capacity_overhead;
    for(size_t n=3;n+1<row.size();n+=2){const auto amount=uint64_t(integer(row[n]));if(amount>std::numeric_limits<uint64_t>::max()-total)refuse("history capacity sum overflow");total+=amount;}
    return total;
}
uint64_t history_capacity_cost(history_capacity_class kind,const values& row){
    uint64_t total=0,fixed=0;
    const auto add=[&](size_t n,bool blob=false){uint64_t amount;
        if(blob){const auto* value=std::get_if<std::vector<uint8_t>>(&row.at(n));if(!value)refuse("history actual accounting blob type");amount=value->size();}
        else amount=text(row.at(n)).size();
        if(amount>std::numeric_limits<uint64_t>::max()-total)refuse("history actual accounting scalar overflow");total+=amount;};
    switch(kind){
        case history_capacity_class::stage:if(row.size()!=18)refuse("history stage accounting shape");for(size_t n:std::array<size_t,8>{0,2,5,6,7,8,16,17})add(n);add(10,true);fixed=80;break;
        case history_capacity_class::page:if(row.size()!=9)refuse("history page accounting shape");for(size_t n:std::array<size_t,4>{0,2,3,8})add(n);add(4,true);fixed=32;break;
        case history_capacity_class::receive:if(row.size()!=9)refuse("history receive accounting shape");for(size_t n:std::array<size_t,3>{0,1,8})add(n);fixed=48;break;
        case history_capacity_class::key:if(row.size()!=7)refuse("history key accounting shape");for(size_t n:std::array<size_t,4>{0,2,3,6})add(n);fixed=24;break;
        case history_capacity_class::snapshot:if(row.size()!=13)refuse("preservation header accounting shape");for(size_t n:std::array<size_t,8>{0,1,2,3,4,5,11,12})add(n);add(6,true);fixed=32;break;
        case history_capacity_class::record:if(row.size()!=10)refuse("preservation record accounting shape");for(size_t n:std::array<size_t,4>{0,3,8,9})add(n);add(7,true);fixed=40;break;
        case history_capacity_class::accepted_cut:if(row.size()!=17)refuse("accepted cut accounting shape");for(size_t n:std::array<size_t,12>{0,1,2,5,6,7,8,9,10,14,15,16})add(n);fixed=40;break;
        case history_capacity_class::accepted_row:if(row.size()!=7)refuse("accepted row accounting shape");for(size_t n:std::array<size_t,4>{0,1,2,6})add(n);add(5,true);fixed=16;break;
        default:refuse("history accounting custody class unavailable");
    }
    if(fixed>std::numeric_limits<uint64_t>::max()-total)refuse("history actual accounting fixed overflow");return total+fixed;
}
struct history_capacity_class_query {const char* table;const char* cost;const char* invalid;uint64_t count_limit;};
// The implementation ceiling bounds audit work for old accepted local profiles;
// it never substitutes for the current source/receiver's original local limits.
const std::array<history_capacity_class_query,8>& history_capacity_queries(){
    static const std::array<history_capacity_class_query,8> q{{
        {history_stage_table,"length(CAST(stage AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(header)+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+80","typeof(stage)!='text' OR typeof(source)!='text' OR typeof(scope)!='text' OR typeof(schema)!='text' OR typeof(device)!='text' OR typeof(inode)!='text' OR typeof(header)!='blob' OR typeof(content)!='text' OR typeof(seal)!='text' OR length(CAST(stage AS BLOB))!=36 OR length(CAST(source AS BLOB))!=36 OR length(CAST(scope AS BLOB))>66560 OR length(CAST(schema AS BLOB))>1048576 OR length(CAST(device AS BLOB))>32 OR length(CAST(inode AS BLOB))>32 OR length(header)>4194304 OR length(CAST(content AS BLOB))>64 OR length(CAST(seal AS BLOB))!=64",64},
        {history_page_table,"length(CAST(stage AS BLOB))+length(CAST(previous AS BLOB))+length(CAST(digest AS BLOB))+length(payload)+length(CAST(seal AS BLOB))+32","typeof(stage)!='text' OR typeof(previous)!='text' OR typeof(digest)!='text' OR typeof(payload)!='blob' OR typeof(seal)!='text' OR length(CAST(stage AS BLOB))!=36 OR length(CAST(previous AS BLOB))>64 OR length(CAST(digest AS BLOB))!=64 OR length(payload)=0 OR length(payload)>4194304 OR length(CAST(seal AS BLOB))!=64",64ULL*262144},
        {history_receive_meta,"length(CAST(stage AS BLOB))+length(CAST(channel AS BLOB))+length(CAST(seal AS BLOB))+48","typeof(stage)!='text' OR typeof(channel)!='text' OR typeof(seal)!='text' OR length(CAST(stage AS BLOB))!=36 OR length(CAST(channel AS BLOB))=0 OR length(CAST(channel AS BLOB))>4096 OR length(CAST(seal AS BLOB))!=64",64},
        {history_receive_key,"length(CAST(stage AS BLOB))+length(CAST(model AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(seal AS BLOB))+24","typeof(stage)!='text' OR typeof(model)!='text' OR typeof(gid)!='text' OR typeof(seal)!='text' OR length(CAST(stage AS BLOB))!=36 OR length(CAST(model AS BLOB))>256 OR length(CAST(gid AS BLOB))>4096 OR length(CAST(seal AS BLOB))!=64",64ULL*16777216},
        {preservation_snapshot,"length(CAST(snapshot AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(catalog)+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+32","typeof(snapshot)!='text' OR typeof(generation)!='text' OR typeof(scope)!='text' OR typeof(schema)!='text' OR typeof(device)!='text' OR typeof(inode)!='text' OR typeof(catalog)!='blob' OR typeof(content)!='text' OR typeof(seal)!='text' OR length(CAST(snapshot AS BLOB))!=36 OR length(CAST(generation AS BLOB))=0 OR length(CAST(generation AS BLOB))>128 OR length(CAST(scope AS BLOB))>66560 OR length(CAST(schema AS BLOB))>1048576 OR length(CAST(device AS BLOB))>32 OR length(CAST(inode AS BLOB))>32 OR length(catalog)=0 OR length(catalog)>4194304 OR length(CAST(content AS BLOB))>64 OR length(CAST(seal AS BLOB))!=64",64},
        {preservation_record,"length(CAST(snapshot AS BLOB))+length(CAST(model AS BLOB))+length(CAST(previous AS BLOB))+length(CAST(seal AS BLOB))+length(payload)+40","typeof(snapshot)!='text' OR typeof(model)!='text' OR typeof(previous)!='text' OR typeof(seal)!='text' OR typeof(payload)!='blob' OR length(CAST(snapshot AS BLOB))!=36 OR length(CAST(model AS BLOB))>256 OR length(CAST(previous AS BLOB))>64 OR length(CAST(seal AS BLOB))!=64 OR length(payload)=0 OR length(payload)>4194304",64ULL*16777216}
        ,{accepted_cut_table,"length(CAST(stage AS BLOB))+length(CAST(channel AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(local_snapshot AS BLOB))+length(CAST(baseline AS BLOB))+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+40","typeof(stage)!='text' OR typeof(channel)!='text' OR typeof(source)!='text' OR typeof(scope)!='text' OR typeof(schema)!='text' OR typeof(device)!='text' OR typeof(inode)!='text' OR typeof(generation)!='text' OR typeof(local_snapshot)!='text' OR typeof(baseline)!='text' OR typeof(content)!='text' OR typeof(seal)!='text' OR typeof(epoch)!='integer' OR typeof(cut)!='integer' OR typeof(rows)!='integer' OR typeof(known)!='integer' OR typeof(unknown)!='integer' OR epoch<=0 OR cut<=0 OR rows<0 OR known<0 OR unknown<0 OR length(CAST(stage AS BLOB))!=36 OR length(CAST(source AS BLOB))!=36 OR length(CAST(channel AS BLOB))=0 OR length(CAST(channel AS BLOB))>4096 OR length(CAST(scope AS BLOB))=0 OR length(CAST(scope AS BLOB))>66560 OR length(CAST(schema AS BLOB))=0 OR length(CAST(schema AS BLOB))>1048576 OR length(CAST(device AS BLOB))>32 OR length(CAST(inode AS BLOB))>32 OR length(CAST(generation AS BLOB))=0 OR length(CAST(generation AS BLOB))>128 OR length(CAST(local_snapshot AS BLOB))!=36 OR length(CAST(baseline AS BLOB))!=64 OR length(CAST(content AS BLOB))!=64 OR length(CAST(seal AS BLOB))!=64",64}
        ,{accepted_row_table,"length(CAST(stage AS BLOB))+length(CAST(model AS BLOB))+length(CAST(gid AS BLOB))+length(value)+length(CAST(seal AS BLOB))+16","typeof(stage)!='text' OR typeof(model)!='text' OR typeof(gid)!='text' OR typeof(page)!='integer' OR typeof(ordinal)!='integer' OR typeof(value)!='blob' OR typeof(seal)!='text' OR length(CAST(stage AS BLOB))!=36 OR length(CAST(model AS BLOB))=0 OR length(CAST(model AS BLOB))>256 OR length(CAST(gid AS BLOB))=0 OR length(CAST(gid AS BLOB))>4096 OR page<0 OR page>=262144 OR ordinal<0 OR ordinal>=64 OR length(value)=0 OR length(value)>4784136 OR length(CAST(seal AS BLOB))!=64",64ULL*16777216}
    }};return q;
}
ordinary_history_limits history_capacity_audit_limits(){ordinary_history_limits bounds;bounds.durable_stage_bytes=uint64_t{8}*1024*1024*1024;bounds.records=uint64_t{16}*1024*1024;bounds.pages=262144;bounds.page_bytes=ordinary_sync_limits::frame_bytes;return bounds;}
void history_capacity_audit_history(database& writer,const values& usage,ordinary_launch::deadline end){
    auto* h=ordinary_export_storage_access::borrow(writer);budget ids_budget{history_stage_limits};
    const auto ids=read(h,"SELECT stage FROM main._lattice_ordered_history_stage_v1 ORDER BY stage",{},64,ids_budget);
    uint64_t seen_pages=0,seen_keys=0,seen_receives=0;const auto ceiling=history_capacity_audit_limits();
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("history capacity audit physical owner unavailable");
    for(const auto& id:ids){history_capacity_deadline(end);budget one{history_stage_limits};auto stage=history_stage_row(writer,text(id[0]),one);
        const auto role=integer(stage[1]);if((role!=1&&role!=2)||!canonical_uuid(text(stage[0]))||!canonical_uuid(text(stage[2]))||integer(stage[3])<=0||integer(stage[4])<=0||stage[7]!=column_value_t(std::to_string(physical->device))||stage[8]!=column_value_t(std::to_string(physical->inode)))refuse("history capacity audit stage identity");
        for(size_t n:std::array<size_t,6>{9,11,12,13,14,15})if(integer(stage[n])<0)refuse("history capacity audit stage scalar");
        if(integer(stage[9])>1||uint64_t(integer(stage[14]))>ceiling.pages||uint64_t(integer(stage[15]))>ceiling.durable_stage_bytes)refuse("history capacity audit stage count");
        const auto& header_blob=std::get<std::vector<uint8_t>>(stage[10]);if(header_blob.empty())refuse("history capacity audit unsealed source capture");
        const auto descriptor=decode_ordinary_history_descriptor(std::string_view(reinterpret_cast<const char*>(header_blob.data()),header_blob.size()),ceiling);
        const auto& projection=descriptor.projection;
        if(stage[0]!=column_value_t(descriptor.stage)||stage[2]!=column_value_t(projection.source)||integer(stage[3])!=projection.epoch||integer(stage[4])!=projection.cut||stage[5]!=column_value_t(projection.scope)||stage[6]!=column_value_t(projection.schema)||stage[16]!=column_value_t(descriptor.content_digest))refuse("history capacity audit header association");
        ordinary_history_progress expected;values meta;ordinary_history_limits profile=ceiling;
        if(role==1){if(integer(stage[9])!=1||stage[7]!=column_value_t(projection.device)||stage[8]!=column_value_t(projection.inode))refuse("history capacity audit source not complete");}
        else {budget metadata{history_stage_limits};meta=history_receive_meta_row(writer,text(stage[0]),metadata);++seen_receives;
            for(size_t n=2;n<=6;++n)if(integer(meta[n])<=0)refuse("history capacity audit invalid receive profile");
            if(uint64_t(integer(meta[4]))>ordinary_sync_limits::frame_bytes||integer(meta[5])>64)refuse("history capacity audit receive integer narrowing");
            profile.records=uint64_t(integer(meta[2]));profile.pages=uint64_t(integer(meta[3]));profile.page_bytes=uint32_t(integer(meta[4]));profile.records_per_page=uint32_t(integer(meta[5]));profile.durable_stage_bytes=uint64_t(integer(meta[6]));history_limits(profile);
            expected=history_receive_progress(stage,meta,descriptor,profile,text(meta[1]),std::to_string(physical->device),std::to_string(physical->inode));
        }
        ordinary_history_progress checked;picosha2::hash256_one_by_one content;uint64_t bytes=header_blob.size();
        for(uint64_t at=0;at<uint64_t(integer(stage[14]));++at){history_capacity_deadline(end);budget page_budget{history_stage_limits};const auto rows=read(h,"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{stage[0],int64_t(at)},1,page_budget);
            if(rows.size()!=1)refuse("history capacity audit page missing");const auto& row=rows[0];const auto& payload=std::get<std::vector<uint8_t>>(row[4]);
            if(text(row[8])!=history_seal("ordinary-paged-page-v1",row,8))refuse("history capacity audit page seal");
            const auto decoded=decode_ordinary_history_page(std::string_view(reinterpret_cast<const char*>(payload.data()),payload.size()),descriptor,checked,profile);
            if(row[2]!=column_value_t(decoded.previous)||row[3]!=column_value_t(decoded.full_digest))refuse("history capacity audit page chain");const auto before=checked;
            if(role==2){history_receive_verify_page(writer,descriptor,decoded,checked,page_budget);seen_keys+=decoded.records.size();}
            else {for(size_t n=0;n<decoded.records.size();++n)(void)history_receive_record_key(descriptor,decoded,n,checked);++checked.next_page;checked.previous=decoded.full_digest;}
            if(integer(row[5])!=int64_t(checked.rows-before.rows)||integer(row[6])!=int64_t(checked.known-before.known)||integer(row[7])!=int64_t(checked.unknown-before.unknown))refuse("history capacity audit page counters");
            content.process(payload.begin(),payload.end());bytes+=payload.size();++seen_pages;
        }
        if(checked.rows!=uint64_t(integer(stage[11]))||checked.known!=uint64_t(integer(stage[12]))||checked.unknown!=uint64_t(integer(stage[13]))||bytes!=uint64_t(integer(stage[15])))refuse("history capacity audit manifest counts");
        if(role==2&&(expected.next_page!=checked.next_page||expected.last_table!=checked.last_table))refuse("history capacity audit key phase");
        if(integer(stage[9])==1){content.finish();if(checked.next_page!=descriptor.pages||checked.rows!=descriptor.rows||checked.known!=descriptor.known||checked.unknown!=descriptor.unknown||picosha2::get_hash_hex_string(content)!=descriptor.content_digest)refuse("history capacity audit completed cut");}
    }
    if(ids.size()!=uint64_t(integer(usage[2]))||seen_pages!=uint64_t(integer(usage[4]))||seen_receives!=uint64_t(integer(usage[6]))||seen_keys!=uint64_t(integer(usage[8])))refuse("history capacity audit orphan page/meta/key");
}
// Complete accepted custody is checked against retained native pages and
// its exact local archive, not against optimistic queried rows or DTO flags.
void history_capacity_audit_accepted(database& writer,const values& usage,ordinary_launch::deadline end){
    auto* h=ordinary_export_storage_access::borrow(writer);budget ids_budget{history_stage_limits};
    const auto ids=read(h,"SELECT stage FROM main._lattice_ordered_accepted_cut_v1 ORDER BY stage",{},64,ids_budget);uint64_t rows=0;
    const auto ceiling=history_capacity_audit_limits();
    for(const auto& id:ids){history_capacity_deadline(end);budget one{history_stage_limits};
        const auto accepted=history_accepted_row(writer,text(id[0]),one),stage=history_stage_row(writer,text(id[0]),one),meta=history_receive_meta_row(writer,text(id[0]),one);
        if(integer(stage[1])!=2||integer(stage[9])!=1)refuse("history capacity accepted cut has no complete receiver stage");
        const auto& raw=std::get<std::vector<uint8_t>>(stage[10]);const auto header=decode_ordinary_history_descriptor(std::string_view(reinterpret_cast<const char*>(raw.data()),raw.size()),ceiling);
        for(size_t n=2;n<=6;++n)if(integer(meta[n])<=0)refuse("history capacity accepted receive profile unavailable");
        if(uint64_t(integer(meta[4]))>ordinary_sync_limits::frame_bytes||integer(meta[5])>64)refuse("history capacity accepted profile narrowing");
        auto profile=ceiling;profile.records=uint64_t(integer(meta[2]));profile.pages=uint64_t(integer(meta[3]));profile.page_bytes=uint32_t(integer(meta[4]));profile.records_per_page=uint32_t(integer(meta[5]));profile.durable_stage_bytes=uint64_t(integer(meta[6]));history_limits(profile);
        const auto local=preservation_decode(preservation_header(writer,text(accepted[10]),one),profile);
        history_accepted_verify_inventory(writer,header,local,text(accepted[1]),profile,end,accepted);
        if(header.rows>std::numeric_limits<uint64_t>::max()-rows)refuse("history capacity accepted row count overflow");rows+=header.rows;
    }
    if(ids.size()!=uint64_t(integer(usage[14]))||rows!=uint64_t(integer(usage[16])))refuse("history capacity orphan accepted cut/row custody");
}
values history_capacity_audit(database& writer,const ordinary_history_limits& limits,budget& scratch,ordinary_launch::deadline end){
    original_view(writer,true);history_capacity_deadline(end);auto* h=ordinary_export_storage_access::borrow(writer);
    const bool has_history=history_storage(h,scratch),has_receive=history_receive_storage(h,scratch),has_preservation=preservation_storage(h,scratch),has_accepted=history_accepted_storage(h,scratch);
    if(!has_history&&(has_receive||has_preservation||has_accepted))refuse("history capacity shared stage schema lost");
    if(has_accepted&&(!has_receive||!has_preservation))refuse("history capacity accepted dependency schema lost");
    values usage{int64_t{1},int64_t{1}};const auto& queries=history_capacity_queries();
    for(size_t n=0;n<(has_accepted?8:6);++n){history_capacity_deadline(end);const bool present=n<2?has_history:(n<4?has_receive:(n<6?has_preservation:has_accepted));if(!present){usage.emplace_back(int64_t{0});usage.emplace_back(int64_t{0});continue;}
        const auto& q=queries[n];budget aggregate{history_stage_limits};const auto rows=read(h,"SELECT COUNT(*),COALESCE(SUM("+std::string(q.cost)+"),0),COALESCE(SUM(CASE WHEN "+q.invalid+" THEN 1 ELSE 0 END),0) FROM main."+q.table,{},1,aggregate);
        if(rows.size()!=1||integer(rows[0][0])<0||uint64_t(integer(rows[0][0]))>q.count_limit||integer(rows[0][1])<0||integer(rows[0][2])!=0)refuse("history capacity audit bounded storage class");usage.push_back(rows[0][0]);usage.push_back(rows[0][1]);
    }
    usage.emplace_back(history_seal(history_capacity_domain(usage.size()+1),usage,usage.size()));if(history_capacity_total(usage)>limits.durable_stage_bytes)refuse("history capacity full inventory exceeds shared durable quota");
    if(has_accepted&&history_accepted_extra_bytes(writer,scratch)!=uint64_t(integer(usage[15]))+uint64_t(integer(usage[17])))refuse("history accepted cost hook differs from eight-class inventory");
    // Aggregated type/size/count bounds precede any retained payload. Existing
    // exact native decoders validate one page/chunk at a time; no all-cut set.
    if(has_history)history_capacity_audit_history(writer,usage,end);
    if(has_preservation){budget ids{history_stage_limits};const auto snapshots=read(h,"SELECT snapshot FROM main._lattice_ordered_prepare_snapshot_v1 ORDER BY snapshot",{},64,ids);uint64_t records=0;const auto ceiling=history_capacity_audit_limits();
        for(const auto& id:snapshots){history_capacity_deadline(end);budget one{history_stage_limits};const auto row=preservation_header(writer,text(id[0]),one);const auto record=preservation_decode(row,ceiling);preservation_verify(writer,record,ceiling,end,false);records+=record.records;}
        if(snapshots.size()!=uint64_t(integer(usage[10]))||records!=uint64_t(integer(usage[12])))refuse("history capacity audit orphan preservation custody");
    }
    if(has_accepted)history_capacity_audit_accepted(writer,usage,end);
    history_capacity_deadline(end);return usage;
}
bool history_capacity_usage_equal(const values& left,const values& right){if(left.size()!=right.size())return false;for(size_t n=2;n+1<left.size();++n)if(left[n]!=right[n])return false;return true;}
history_capacity_turn::history_capacity_turn(database& w,const ordinary_history_limits& l,budget& scratch,bool full,ordinary_launch::deadline deadline)
    :writer(w),limits(l),end(deadline){
    // PID/sequence custody is checked before this helper first touches SQLite.
    (void)ordinary_export_storage_access::history_capacity_epoch(writer);
    original_view(writer,true);history_limits(limits);history_capacity_deadline(end);auto* h=ordinary_export_storage_access::borrow(writer);
    // Raw escape prevents a continuity fast grant; the existing managed owner
    // restrictions still apply independently. Real fault-injection fixtures
    // may use an escaped handle only through complete actual inventory audits.
    auto& cell=ordinary_export_storage_access::history_capacity_cell(writer);const auto held=std::static_pointer_cast<history_capacity_witness>(cell);
    if(held&&held->creator!=history_capacity_creator())refuse("history inherited capacity witness");
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("history capacity physical owner unavailable");device=std::to_string(physical->device);inode=std::to_string(physical->inode);
    const auto format=history_capacity_storage(h,scratch);const bool has_accepted=history_accepted_storage(h,scratch);
    if(format==2&&!has_accepted)refuse("history eight-class ledger lost accepted storage");
    if(!format||(format==1&&has_accepted)){
        // One singleton remains. The exact six schema migrates only inside this
        // real owned WRITE after bounded inventory proof, never by caller flag.
        values old;if(format)old=history_capacity_read(h,scratch);
        auto actual=history_capacity_audit(writer,limits,scratch,end);
        if(format){for(size_t n=2;n<14;++n)if(old[n]!=actual[n])refuse("history six-class migration refuses existing counter drift");
            if(integer(old[1])==std::numeric_limits<int64_t>::max())refuse("history capacity migration revision exhausted");
            actual[1]=integer(old[1])+1;actual.back()=history_seal(history_capacity_domain(actual.size()),actual,actual.size()-1);}
        register_mutation_guard(h);mutation_scope permit(h);
        if(format)writer.execute("DROP TABLE main._lattice_ordered_capacity_v1"); // Ledger only; no custody is retired.
        writer.execute(actual.size()==19?history_capacity_eight_ddl:history_capacity_ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(history_capacity_table,op));
        std::string insert="INSERT INTO main._lattice_ordered_capacity_v1 VALUES(";for(size_t n=0;n<actual.size();++n){if(n)insert+=',';insert+='?';}insert+=')';writer.execute(insert,actual);
        if(writer.changes()!=1||!history_capacity_storage(h,scratch)||history_capacity_read(h,scratch)!=actual)refuse("history capacity migration postimage differs");
    }
    epoch=ordinary_export_storage_access::history_capacity_epoch(writer);
    prior=history_capacity_read(h,scratch);next=prior;changes=sqlite3_total_changes64(h);if(changes<0)refuse("history writer actual mutation count unavailable");
    data_version=history_capacity_pragma(h,"PRAGMA main.data_version",scratch);schema_version=history_capacity_pragma(h,"PRAGMA main.schema_version",scratch);
    const auto retirement=database_retirement_access::retain(writer);const bool current=held&&held->epoch==epoch&&retirement&&!retirement->raw_handle_escaped()&&held->changes==changes&&held->data_version==data_version&&held->schema_version==schema_version&&std::string_view(held->seal.data(),held->seal.size())==text(prior.back())&&held->device==physical->device&&held->inode==physical->inode;
    if(full||!current){const auto actual=history_capacity_audit(writer,limits,scratch,end);if(!history_capacity_usage_equal(actual,prior))refuse("history durable counter drift refuses admission");}
    if(history_capacity_total(prior)>limits.durable_stage_bytes)refuse("history retained shared capacity exhausted");
    if(ordinary_export_storage_access::history_capacity_epoch(writer)!=epoch||sqlite3_total_changes64(h)!=changes||history_capacity_pragma(h,"PRAGMA main.data_version",scratch)!=data_version||history_capacity_pragma(h,"PRAGMA main.schema_version",scratch)!=schema_version||history_capacity_read(h,scratch)!=prior)refuse("history writer changed during capacity admission");
}
uint64_t history_capacity_turn::bytes()const{return history_capacity_total(next);}
void history_capacity_turn::reserve(uint64_t amount)const {const auto total=bytes();if(total>limits.durable_stage_bytes||amount>limits.durable_stage_bytes-total)refuse("history shared durable quota refused before actual write");}
void history_capacity_turn::insert(history_capacity_class kind,const values& row){
    if(finished)refuse("history capacity turn already settled");const size_t at=2+2*size_t(kind);if(at+1>=next.size()-1)refuse("history capacity class not installed");const auto amount=history_capacity_cost(kind,row);reserve(amount);
    if(integer(next[at])==std::numeric_limits<int64_t>::max()||amount>uint64_t(std::numeric_limits<int64_t>::max()-integer(next[at+1])))refuse("history issued counter overflow");
    next[at]=integer(next[at])+1;next[at+1]=integer(next[at+1])+int64_t(amount);++mutations;
}
void history_capacity_turn::replace(history_capacity_class kind,const values& before,const values& after){
    if(finished)refuse("history capacity turn already settled");const size_t at=3+2*size_t(kind);if(at>=next.size()-1)refuse("history capacity class not installed");const auto old=history_capacity_cost(kind,before),now=history_capacity_cost(kind,after);
    if(old>uint64_t(integer(next[at])))refuse("history issued replacement underflow");if(now>old)reserve(now-old);
    const auto kept=uint64_t(integer(next[at]))-old;if(now>uint64_t(std::numeric_limits<int64_t>::max())-kept)refuse("history issued replacement overflow");next[at]=int64_t(kept+now);++mutations;
}
void history_capacity_turn::finish(budget& scratch,bool full){
    if(finished)refuse("history capacity turn finished twice");history_capacity_deadline(end);auto* h=ordinary_export_storage_access::borrow(writer);
    const auto current_epoch=ordinary_export_storage_access::history_capacity_epoch(writer);
    if(current_epoch<epoch||current_epoch-epoch!=mutations)refuse("history unrecognized program mutation inside owning capacity turn");
    const auto now=sqlite3_total_changes64(h);if(now<changes||uint64_t(now-changes)!=mutations)refuse("history unrecognized mutation inside owning capacity turn");
    if(!history_storage(h,scratch)||!history_capacity_storage(h,scratch)||(prior.size()==19&&!history_accepted_storage(h,scratch))||history_capacity_read(h,scratch)!=prior)refuse("history capacity owning prior changed");
    if(full){const auto actual=history_capacity_audit(writer,limits,scratch,end);if(!history_capacity_usage_equal(actual,next))refuse("history issued deltas differ from actual full inventory");}
    if(mutations){if(integer(prior[1])==std::numeric_limits<int64_t>::max())refuse("history capacity revision exhausted");next[1]=integer(prior[1])+1;next.back()=history_seal(history_capacity_domain(next.size()),next,next.size()-1);
        register_mutation_guard(h);mutation_scope permit(h);values params(next.begin()+1,next.end());params.push_back(prior[1]);params.push_back(prior.back());
        const auto extra=next.size()==19?std::string(",accepted_cuts=?,accepted_cut_bytes=?,accepted_rows=?,accepted_row_bytes=?"):std::string{};
        writer.execute("UPDATE main._lattice_ordered_capacity_v1 SET revision=?,stages=?,stage_bytes=?,pages=?,page_bytes=?,receives=?,receive_bytes=?,keys=?,key_bytes=?,snapshots=?,snapshot_bytes=?,records=?,record_bytes=?"+extra+",seal=? WHERE id=1 AND revision=? AND seal=?",params);
        if(writer.changes()!=1||history_capacity_read(h,scratch)!=next)refuse("history capacity own counter postimage differs");
    }
    const auto final_epoch=ordinary_export_storage_access::history_capacity_epoch(writer);
    if(final_epoch<epoch||final_epoch-epoch!=mutations+(mutations?1:0))refuse("history final program mutation sequence changed");
    const auto final=sqlite3_total_changes64(h);const auto version=history_capacity_pragma(h,"PRAGMA main.data_version",scratch),schema=history_capacity_pragma(h,"PRAGMA main.schema_version",scratch);const auto physical=writer.physical_identity("main",{},true);
    if(final<changes||uint64_t(final-changes)!=mutations+(mutations?1:0)||version!=data_version||schema!=schema_version||!physical||device!=std::to_string(physical->device)||inode!=std::to_string(physical->inode)||history_capacity_read(h,scratch)!=next)refuse("history capacity final owning witness changed");
    auto& cell=ordinary_export_storage_access::history_capacity_cell(writer);const auto retirement=database_retirement_access::retain(writer);
    if(retirement&&!retirement->raw_handle_escaped()){
        scratch.add(history_capacity_retained_payload);std::shared_ptr<history_capacity_witness> held(new history_capacity_witness,history_capacity_dispose{});held->epoch=final_epoch;held->changes=final;held->data_version=version;held->schema_version=schema;const auto& seal=text(next.back());if(seal.size()!=held->seal.size())refuse("history capacity fixed seal spelling");std::copy(seal.begin(),seal.end(),held->seal.begin());held->device=physical->device;held->inode=physical->inode;cell=std::move(held);
    }else cell.reset();
    // This is prospective continuity only. A denied/rolled-back COMMIT changes
    // the durable seal or mutation witness and forces the next bounded audit.
    // No success/receipt/publication is inferred until existing engine return.
    finished=true;
}
uint64_t history_capacity_observe(database& writer,const ordinary_history_limits& limits,budget& scratch,bool full,ordinary_launch::deadline end){history_capacity_turn turn(writer,limits,scratch,full,end);const auto total=turn.bytes();turn.finish(scratch);return total;}
}
uint64_t ordinary_export_access::history_capacity_for_test(lattice_db& owner,const ordinary_history_limits& limits,bool full){
    // Refuse an inherited connection before the native owning entry/admission.
    if(owner.is_closed())throw db_error("history capacity observer owner closed");
    (void)history_capacity_connection_epoch(owner.db());
    uint64_t observed=0;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){budget scratch{history_stage_limits};observed=history_capacity_observe(writer,limits,scratch,full);});
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    return observed; // Observation only, no grant/accepted cut/receipt factory.
}
