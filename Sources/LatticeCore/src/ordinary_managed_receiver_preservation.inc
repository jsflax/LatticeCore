// Included only by ordinary_export.cpp. This is LOCAL observation preservation,
// not accepted server history installation. No existing model/audit/receipt row
// is changed, reinterpreted, synchronized or retracted by this transaction.
namespace {
constexpr const char* preservation_snapshot="_lattice_ordered_prepare_snapshot_v1";
constexpr const char* preservation_record="_lattice_ordered_prepare_record_v1";
const std::vector<std::pair<std::string,std::string>>& preservation_definitions(){
    static const std::vector<std::pair<std::string,std::string>> definitions{
        {preservation_snapshot,"CREATE TABLE _lattice_ordered_prepare_snapshot_v1 (snapshot TEXT PRIMARY KEY,generation TEXT NOT NULL,scope TEXT NOT NULL,schema TEXT NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,catalog BLOB NOT NULL,state INTEGER NOT NULL CHECK(state IN (0,1)),rows INTEGER NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,content TEXT NOT NULL,seal TEXT NOT NULL)"},
        {preservation_record,"CREATE TABLE _lattice_ordered_prepare_record_v1 (snapshot TEXT NOT NULL,ordinal INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN (1,2)),model TEXT NOT NULL,row_index INTEGER NOT NULL,part INTEGER NOT NULL,is_last INTEGER NOT NULL CHECK(is_last IN (0,1)),payload BLOB NOT NULL,previous TEXT NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(snapshot,ordinal))"}};
    return definitions;
}
bool preservation_storage(sqlite3* h,budget& retained){
    const auto actual=read(h,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_prepare_*' OR tbl_name GLOB '_lattice_ordered_prepare_*' ORDER BY name",{},11,retained);
    if(actual.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected;
    for(const auto& [name,ddl]:preservation_definitions()){
        expected.emplace(name,std::make_pair(std::string("table"),column_value_t(ddl)));
        expected.emplace("sqlite_autoindex_"+name+"_1",std::make_pair(std::string("index"),column_value_t(nullptr)));
        for(const char* op:{"INSERT","UPDATE","DELETE"})expected.emplace(name+'_'+op,std::make_pair(std::string("trigger"),column_value_t(receiver_guard(name,op))));
    }
    if(actual.size()!=expected.size())refuse("receiver preservation inventory changed");
    for(const auto& row:actual){const auto found=expected.find(text(row[1]));if(found==expected.end()||row[0]!=column_value_t(found->second.first)||row[2]!=found->second.second)refuse("receiver preservation program changed");}
    if(!read(h,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_prepare_*' OR tbl_name GLOB '_lattice_ordered_prepare_*' LIMIT 1",{},1,retained).empty())refuse("temporary receiver preservation shadow refused");
    return true;
}
uint64_t receiver_preservation_extra_bytes(database& writer,budget& retained){
    if(!preservation_storage(ordinary_export_storage_access::borrow(writer),retained))return 0;
    const auto rows=read(ordinary_export_storage_access::borrow(writer),
        "SELECT (SELECT COALESCE(SUM(length(CAST(snapshot AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(catalog)+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+32),0) FROM main._lattice_ordered_prepare_snapshot_v1)+(SELECT COALESCE(SUM(length(CAST(snapshot AS BLOB))+length(CAST(model AS BLOB))+length(payload)+length(CAST(previous AS BLOB))+length(CAST(seal AS BLOB))+40),0) FROM main._lattice_ordered_prepare_record_v1)",{},1,retained);
    const auto bytes=integer(rows.at(0).at(0));if(bytes<0)refuse("receiver preservation byte accounting invalid");return uint64_t(bytes);
}
struct preservation_table {std::string name;std::vector<std::string> columns;int64_t kind=0;uint64_t records=0;std::string observed_ddl;};
// Only a fixed known custody name or an actual native-validated generated
// model table enters this cursor. Hidden/generated/virtual columns cannot
// execute application expressions. No caller SQL, filter or complete flag.
std::vector<std::string> preservation_observed_columns(sqlite3* h,const std::string& name,budget& metadata,std::string& ddl){
    const auto table=read(h,"SELECT sql FROM main.sqlite_master WHERE type='table' AND name=? AND tbl_name=?",{name,name},1,metadata);
    if(table.size()!=1)refuse("receiver preservation known table disappeared");ddl=text(table[0][0]);
    size_t at=0;const auto token=[&](){while(at<ddl.size()&&(ddl[at]==' '||ddl[at]=='\n'||ddl[at]=='\r'||ddl[at]=='\t'))++at;const auto start=at;while(at<ddl.size()&&((ddl[at]>='a'&&ddl[at]<='z')||(ddl[at]>='A'&&ddl[at]<='Z')))++at;auto word=ddl.substr(start,at-start);for(auto& c:word)if(c>='A'&&c<='Z')c+=32;return word;};
    if(token()!="create"||token()!="table")refuse("receiver preservation virtual or unqualified table refused");
    const auto observed=read(h,"SELECT cid,name,hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid",{name},2051,metadata);
    if(observed.empty())refuse("receiver preservation column list empty");std::vector<std::string> columns;columns.reserve(observed.size());
    for(size_t n=0;n<observed.size();++n){if(integer(observed[n][0])!=int64_t(n)||integer(observed[n][2])!=0)refuse("receiver preservation generated or hidden column refused");auto column=text(observed[n][1]);(void)quote(column);columns.push_back(std::move(column));}
    return columns;
}
uint64_t preservation_count(sqlite3* h,const std::string& table,budget& metadata){
    const auto count=read(h,"SELECT COUNT(*) FROM main."+quote(table),{},1,metadata);const auto n=integer(count.at(0).at(0));if(n<0)refuse("receiver preservation negative row count");return uint64_t(n);
}
struct preservation_cursor {
    sqlite3_stmt* statement=nullptr;
    preservation_cursor(sqlite3* h,const preservation_table& table){
        std::string columns;for(const auto& name:table.columns){if(!columns.empty())columns+=',';columns+=quote(name);}
        const auto sql="SELECT "+columns+" FROM main."+quote(table.name);const char* tail=nullptr;
        const auto rc=sqlite3_prepare_v2(h,sql.c_str(),int(sql.size()),&statement,&tail);
        if(rc!=SQLITE_OK||!statement||!tail||*tail||!sqlite3_stmt_readonly(statement)||sqlite3_bind_parameter_count(statement)||sqlite3_column_count(statement)!=int(table.columns.size())){if(statement)sqlite3_finalize(statement);statement=nullptr;refuse("receiver preservation cursor prepare refused");}
    }
    ~preservation_cursor(){if(statement)sqlite3_finalize(statement);}
    preservation_cursor(const preservation_cursor&)=delete;
    bool next(const std::function<void(std::vector<uint8_t>&&,uint64_t,bool)>& emit){
        const auto rc=sqlite3_step(statement);if(rc==SQLITE_DONE)return false;if(rc!=SQLITE_ROW)refuse("receiver preservation cursor did not finish");
        const auto columns=sqlite3_column_count(statement);size_t encoded=8;
        for(int n=0;n<columns;++n){size_t extra=9;const auto type=sqlite3_column_type(statement,n);if(type==SQLITE_TEXT||type==SQLITE_BLOB){const auto bytes=sqlite3_column_bytes(statement,n);if(bytes<0)refuse("receiver preservation scalar length invalid");extra+=size_t(bytes);}
            else if(type!=SQLITE_NULL&&type!=SQLITE_INTEGER&&type!=SQLITE_FLOAT)refuse("receiver preservation scalar class invalid");
            if(extra>wire_limits.bytes-encoded)refuse("receiver preservation logical row exceeds finite bound");encoded+=extra;}
        // The SQLite scalar remains BORROWED from this exact owned statement.
        // Only one4MiB chunk is copied; multi-blob originals do not require a
        // second whole encoded row or a lower per-row acceptance limit.
        std::vector<uint8_t> buffer;buffer.reserve(ordinary_history_limits::record_buffer_bytes);uint64_t part=0;
        const auto bytes=[&](const uint8_t* input,size_t count){while(count){if(buffer.size()==ordinary_history_limits::record_buffer_bytes){emit(std::move(buffer),part++,false);buffer=std::vector<uint8_t>{};buffer.reserve(ordinary_history_limits::record_buffer_bytes);}
            const auto n=std::min(count,ordinary_history_limits::record_buffer_bytes-buffer.size());buffer.insert(buffer.end(),input,input+n);input+=n;count-=n;}};
        const auto number=[&](uint64_t n){std::array<uint8_t,8> raw{};for(unsigned i=0;i<8;++i)raw[i]=uint8_t(n>>(56-8*i));bytes(raw.data(),raw.size());};
        number(uint64_t(columns));
        for(int n=0;n<columns;++n){const auto type=sqlite3_column_type(statement,n);const uint8_t tag=type==SQLITE_NULL?0:type==SQLITE_INTEGER?1:type==SQLITE_FLOAT?2:type==SQLITE_TEXT?3:4;bytes(&tag,1);
            if(tag==0)number(0);else if(tag==1)number(uint64_t(sqlite3_column_int64(statement,n)));else if(tag==2)number(std::bit_cast<uint64_t>(sqlite3_column_double(statement,n)));
            else{const auto count=sqlite3_column_bytes(statement,n);const auto* input=tag==3?sqlite3_column_text(statement,n):static_cast<const uint8_t*>(sqlite3_column_blob(statement,n));if(!input&&count)refuse("receiver preservation borrowed scalar unavailable");number(uint64_t(count));if(count)bytes(input,size_t(count));}}
        if(buffer.empty())refuse("receiver preservation final row chunk unavailable");emit(std::move(buffer),part,true);return true;
    }

};
values preservation_header(database& writer,const std::string& snapshot,budget& scratch){
    const auto rows=read(ordinary_export_storage_access::borrow(writer),"SELECT snapshot,generation,scope,schema,device,inode,catalog,state,rows,records,bytes,content,seal FROM main._lattice_ordered_prepare_snapshot_v1 WHERE snapshot=?",{snapshot},1,scratch);
    if(rows.size()!=1||text(rows[0][12])!=history_seal("ordinary-receiver-local-preservation-v1",rows[0],12))refuse("receiver preservation header missing or changed");return rows[0];
}
ordinary_receiver_preservation_record preservation_decode(const values& row,const ordinary_history_limits& limits){
    if(row.size()!=13||!canonical_uuid(text(row[0]))||integer(row[7])!=1||integer(row[8])<0||integer(row[9])<0||integer(row[10])<0||uint64_t(integer(row[8]))>limits.records||uint64_t(integer(row[9]))>limits.records||uint64_t(integer(row[10]))>limits.durable_stage_bytes)refuse("receiver preservation header incomplete or exceeds limits");
    const auto* catalog=std::get_if<std::vector<uint8_t>>(&row[6]);if(!catalog||catalog->empty()||catalog->size()>ordinary_history_limits::record_buffer_bytes)refuse("receiver preservation catalog bound");
    return {text(row[0]),text(row[1]),text(row[2]),text(row[3]),text(row[4]),text(row[5]),picosha2::hash256_hex_string(catalog->begin(),catalog->end()),text(row[11]),text(row[12]),uint64_t(integer(row[8])),uint64_t(integer(row[9])),uint64_t(integer(row[10]))};
}
std::vector<preservation_table> preservation_catalog_decode(const std::vector<uint8_t>& input,budget& metadata,const ordinary_history_limits& limits){
    size_t at=0;const auto number=[&](){if(input.size()-at<8)refuse("receiver preservation catalog truncated scalar");uint64_t n=0;for(unsigned i=0;i<8;++i)n=(n<<8)|input[at++];return n;};
    const auto count=number();if(!count||count>metadata.limits.cells)refuse("receiver preservation catalog scalar count bound");uint64_t seen=0;
    const auto integer_value=[&](){if(seen==count||at==input.size()||input[at++]!=1)refuse("receiver preservation catalog integer shape");++seen;const auto n=number();if(n>uint64_t(INT64_MAX))refuse("receiver preservation catalog integer range");return n;};
    const auto text_value=[&](){if(seen==count||at==input.size()||input[at++]!=3)refuse("receiver preservation catalog text shape");++seen;const auto n=number();if(n>input.size()-at)refuse("receiver preservation catalog text truncated");metadata.add(size_t(n)+sizeof(std::string));std::string out(reinterpret_cast<const char*>(input.data()+at),size_t(n));at+=size_t(n);return out;};
    std::vector<preservation_table> tables;uint64_t total=0;
    while(seen<count){if(tables.size()==ordinary_sync_limits::projection_tables+24)refuse("receiver preservation catalog table count bound");preservation_table table;table.kind=int64_t(integer_value());table.name=text_value();(void)quote(table.name);if(table.kind!=1&&table.kind!=2)refuse("receiver preservation catalog kind invalid");table.records=integer_value();if(table.records>limits.records-total)refuse("receiver preservation catalog row total bound");total+=table.records;
        const auto columns=integer_value();if(!columns||columns>2051)refuse("receiver preservation catalog column count bound");metadata.add(sizeof(preservation_table)+size_t(columns)*sizeof(std::string));table.columns.reserve(size_t(columns));for(uint64_t n=0;n<columns;++n){auto column=text_value();(void)quote(column);table.columns.push_back(std::move(column));}table.observed_ddl=text_value();tables.push_back(std::move(table));}
    if(at!=input.size())refuse("receiver preservation catalog trailing bytes");return tables;
}
// Verify a serialized row across bounded chunks without retaining the row or
// trusting a copied final/completeness flag. All scalar boundaries and exact
// column counts come from the native-issued retained catalog and codec.
struct preservation_row_validator {
    enum class phase { count,tag,number,payload,done } phase_=phase::count;
    uint64_t expected,columns=0,scalar=0,number_=0,remaining=0,total=0;unsigned digits=0;uint8_t tag_=0;
    explicit preservation_row_validator(uint64_t count):expected(count){}
    void finish_scalar(){++scalar;phase_=scalar==columns?phase::done:phase::tag;}
    void feed(const std::vector<uint8_t>& input){if(input.empty()||input.size()>ordinary_history_limits::record_buffer_bytes||input.size()>wire_limits.bytes-total)refuse("receiver preservation row/chunk byte bound");total+=input.size();size_t at=0;
        while(at<input.size()){if(phase_==phase::done)refuse("receiver preservation row has trailing bytes");
            if(phase_==phase::payload){const auto n=std::min<uint64_t>(remaining,input.size()-at);at+=size_t(n);remaining-=n;if(!remaining)finish_scalar();continue;}
            if(phase_==phase::tag){tag_=input[at++];if(tag_>4)refuse("receiver preservation scalar tag invalid");phase_=phase::number;digits=0;number_=0;continue;}
            number_=(number_<<8)|input[at++];if(++digits!=8)continue;
            if(phase_==phase::count){columns=number_;if(columns!=expected||!columns||columns>2051)refuse("receiver preservation row column count differs");phase_=phase::tag;}
            else if(tag_==0){if(number_)refuse("receiver preservation null encoding changed");finish_scalar();}
            else if(tag_==1||tag_==2)finish_scalar(); // Preserve INTEGER/REAL bits as observations.
            else{if(number_>wire_limits.bytes-total+input.size())refuse("receiver preservation scalar length bound");remaining=number_;if(remaining)phase_=phase::payload;else finish_scalar();}
        }
    }
    bool complete()const{return phase_==phase::done;}
};

void preservation_verify(database& writer,const ordinary_receiver_preservation_record& record,const ordinary_history_limits& limits,ordinary_launch::deadline end,bool shared_quota=true){
    managed_pair_deadline(end);original_view(writer,true);budget metadata{history_stage_limits};auto* h=ordinary_export_storage_access::borrow(writer);
    if(!preservation_storage(h,metadata)||!history_storage(h,metadata))refuse("receiver preservation inventory unavailable");
    const auto header=preservation_header(writer,record.snapshot,metadata);const auto actual=preservation_decode(header,limits);
    if(actual.snapshot!=record.snapshot||actual.generation!=record.generation||actual.scope!=record.scope||actual.schema!=record.schema||actual.device!=record.device||actual.inode!=record.inode||actual.catalog_digest!=record.catalog_digest||actual.content!=record.content||actual.seal!=record.seal||actual.rows!=record.rows||actual.records!=record.records||actual.bytes!=record.bytes)refuse("receiver preservation retained header changed");
    const auto physical=writer.physical_identity("main",{},true);if(!physical||record.device!=std::to_string(physical->device)||record.inode!=std::to_string(physical->inode))refuse("receiver preservation physical store replaced");
    const auto counts=read(h,"SELECT COUNT(*),COALESCE(MIN(ordinal),0),COALESCE(MAX(ordinal),-1) FROM main._lattice_ordered_prepare_record_v1 WHERE snapshot=?",{record.snapshot},1,metadata);
    if(counts[0]!=values{int64_t(record.records),int64_t{0},record.records?int64_t(record.records-1):int64_t{-1}})refuse("receiver preservation missing or extra record");
    const auto& raw_catalog=std::get<std::vector<uint8_t>>(header[6]);budget catalog_budget{history_metadata_limits};auto tables=preservation_catalog_decode(raw_catalog,catalog_budget,limits);uint64_t expected_rows=0;for(const auto& table:tables)expected_rows+=table.records;if(expected_rows!=record.rows)refuse("receiver preservation catalog/header row total differs");
    size_t table_index=0;uint64_t row_index=0,part=0,rows=0,bytes=0;std::string previous;std::optional<preservation_row_validator> row_validator;
    for(uint64_t n=0;n<record.records;++n){managed_pair_deadline(end);while(table_index<tables.size()&&row_index==tables[table_index].records){++table_index;row_index=0;}if(table_index==tables.size())refuse("receiver preservation extra row/chunk");const auto& table=tables[table_index];budget one{history_stage_limits};one.add(metadata.bytes+catalog_budget.bytes);
        const auto found=read(h,"SELECT snapshot,ordinal,kind,model,row_index,part,is_last,payload,previous,seal FROM main._lattice_ordered_prepare_record_v1 WHERE snapshot=? AND ordinal=?",{record.snapshot,int64_t(n)},1,one);
        if(found.size()!=1)refuse("receiver preservation indexed record missing");const auto& row=found[0];const auto* payload=std::get_if<std::vector<uint8_t>>(&row[7]);
        if(!payload||payload->empty()||payload->size()>ordinary_history_limits::record_buffer_bytes||row[0]!=column_value_t(record.snapshot)||integer(row[1])!=int64_t(n)||integer(row[2])!=table.kind||text(row[3])!=table.name||integer(row[4])!=int64_t(row_index)||integer(row[5])!=int64_t(part)||(integer(row[6])!=0&&integer(row[6])!=1)||text(row[8])!=previous||text(row[9])!=history_seal("ordinary-receiver-local-observation-v1",row,9))refuse("receiver preservation record chain changed");
        if(!row_validator)row_validator.emplace(table.columns.size());row_validator->feed(*payload);const bool last=integer(row[6])==1;if(last!=row_validator->complete())refuse("receiver preservation row chunk boundary differs");if(!last&&payload->size()!=ordinary_history_limits::record_buffer_bytes)refuse("receiver preservation intermediate chunk is not full");
        previous=text(row[9]);if(payload->size()>limits.durable_stage_bytes-bytes)refuse("receiver preservation record byte total exceeded");bytes+=payload->size();
        if(last){++row_index;++rows;part=0;row_validator.reset();}else if(++part>=wire_limits.bytes/ordinary_history_limits::record_buffer_bytes)refuse("receiver preservation row part count bound");
    }
    while(table_index<tables.size()&&row_index==tables[table_index].records){++table_index;row_index=0;}
    if(row_validator||table_index!=tables.size()||rows!=record.rows||previous!=record.content||bytes!=record.bytes)refuse("receiver preservation totals, row completion or chain changed");

    if(shared_quota)(void)history_shared_bytes(writer,limits,metadata);managed_pair_deadline(end);
}
}
struct ordinary_managed_receiver_preservation::state {
    std::shared_ptr<lattice_db> owner;
    std::shared_ptr<const ordinary_managed_child_admission> admission;
    const std::string path;
    const ordinary_receiver_preservation_record record;
    const ordinary_history_limits limits;
    const physical_store_identity physical;
    const ordinary_launch::deadline admission_end;
    mutable std::atomic<bool> episode_claimed{false};
    mutable std::atomic<int64_t> episode_end_millis{0};
    const uint64_t creator=history_creator();
    state(std::shared_ptr<lattice_db> o,std::shared_ptr<const ordinary_managed_child_admission> a,std::string p,ordinary_receiver_preservation_record r,ordinary_history_limits l,physical_store_identity actual,ordinary_launch::deadline until)
        :owner(std::move(o)),admission(std::move(a)),path(std::move(p)),record(std::move(r)),limits(l),physical(std::move(actual)),admission_end(until){}
    static void dispose(state* value)noexcept {
        if(!value||value->creator!=history_creator())return;
        value->owner.reset();if(value->creator!=history_creator())return;
        value->admission.reset();if(value->creator!=history_creator())return;
        delete value; // Only bounded plain metadata remains; no inherited foreign finalizer.
    }
};
ordinary_managed_receiver_preservation::~ordinary_managed_receiver_preservation()=default;
ordinary_receiver_preservation_record ordinary_export_access::preserve_receiver_storage(lattice_db& owner,database& writer,
    const std::vector<std::string>& names,const std::string& generation,const ordinary_history_limits& limits,ordinary_launch::deadline end){
    managed_pair_deadline(end);original_view(writer,true);history_limits(limits);require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);
    if(!owner.recovery_schemas_.valid()||owner.recovery_continuous_||owner.administrative_owner_||generation.empty()||generation.size()>128)refuse("receiver preservation actual catalog/generation unavailable");
    budget metadata{history_metadata_limits};ordinary_export_frame image;image.wire_profile=true;auto* h=ordinary_export_storage_access::borrow(writer);
    capture_projected_tables(h,owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},names,image,metadata,false,nullptr,nullptr);
    const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("receiver preservation physical identity unavailable");
    std::vector<preservation_table> tables;tables.reserve(image.tables.size()+24);
    for(const auto& table:image.tables){preservation_table t{table.name,table.columns,1,0,{}};if(table.local_id)t.columns.insert(t.columns.begin(),"id");t.records=preservation_count(h,t.name,metadata);tables.push_back(std::move(t));}
    const auto observed=[&](const std::string& name){preservation_table table;table.name=name;table.kind=2;table.columns=preservation_observed_columns(h,name,metadata,table.observed_ddl);table.records=preservation_count(h,name,metadata);tables.push_back(std::move(table));};
    // Native-validated current rows are distinct from these raw local custody
    // observations. Old source names/receipts are archived, never relabelled.
    for(const char* name:{"AuditLog","_lattice_sync_state","_SyncControl","_lattice_meta"})observed(name);
    for(const char* name:{"_lattice_sync_set","_lattice_replication_slots"})if(!read(h,"SELECT 1 FROM main.sqlite_master WHERE name=? AND type='table'",{std::string(name)},1,metadata).empty())observed(name);
    if(inspect_storage(h,metadata))observed(source_table);
    if(receiver_storage(h,metadata))for(const auto& [name,ddl]:receiver_definitions())observed(name);
    if(wire_storage(h,metadata))for(const auto& [name,ddl]:wire_definitions())observed(name);
    if(helper_storage(h,metadata))for(const auto& [name,ddl]:helper_definitions())observed(name);
    if(ingress_storage(h,metadata)){observed("_lattice_ordered_ingress_v2");observed("_lattice_ordered_ingress_entry_v2");}
    if(managed_profile_storage(h,metadata))observed(managed_profile_table);
    if(history_accepted_storage(h,metadata))for(const auto& [name,ddl]:history_accepted_definitions())observed(name);
    uint64_t rows=0;values catalog;
    for(const auto& table:tables){if(table.records>limits.records-rows)refuse("receiver preservation total record count bound");rows+=table.records;
        metadata.add(sizeof(preservation_table)+2*(table.name.size()+table.observed_ddl.size())+table.columns.size()*sizeof(std::string)+(5+table.columns.size())*sizeof(column_value_t));for(const auto& column:table.columns)metadata.add(2*column.size());
        catalog.emplace_back(table.kind);catalog.emplace_back(table.name);catalog.emplace_back(int64_t(table.records));catalog.emplace_back(int64_t(table.columns.size()));for(const auto& column:table.columns)catalog.emplace_back(column);catalog.emplace_back(table.observed_ddl);}
    if(encoded_size(catalog,ordinary_history_limits::record_buffer_bytes)>ordinary_history_limits::record_buffer_bytes)refuse("receiver preservation catalog exceeds finite buffer");
    const auto encoded_catalog=encode(catalog,metadata);
    register_mutation_guard(h);mutation_scope permit(h);budget initial{history_stage_limits};
    if(!history_storage(h,initial)){for(const auto& [name,ddl]:history_definitions()){writer.execute(ddl);for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}}
    if(!preservation_storage(h,initial)){for(const auto& [name,ddl]:preservation_definitions()){writer.execute(ddl);for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(name,op));}}
    if(!history_storage(h,initial)||!preservation_storage(h,initial))refuse("receiver preservation storage failed to install");
    const auto count=read(h,"SELECT COUNT(*) FROM main._lattice_ordered_prepare_snapshot_v1",{},1,initial);if(integer(count[0][0])>=64)refuse("receiver preservation snapshot count capacity exhausted");
    history_capacity_turn capacity(writer,limits,initial,true,end);uint64_t shared=capacity.bytes();
    const auto reserve=[&](uint64_t n){if(n>limits.durable_stage_bytes-shared)refuse("receiver preservation combined durable quota refused before write");shared+=n;};
    initial.add(2*encoded_catalog.size()+13*sizeof(column_value_t)+generation.size()+image.scope.size()+image.schema.size()+512);
    const auto snapshot=uuid_t::generate().to_string();values header{snapshot,generation,image.scope,image.schema,std::to_string(physical->device),std::to_string(physical->inode),encoded_catalog,int64_t{0},int64_t{0},int64_t{0},int64_t{0},std::string{}};
    header.emplace_back(history_seal("ordinary-receiver-local-preservation-v1",header,12));
    // Initial content is empty; final seal has the same 64-byte representation.
    reserve(snapshot.size()+generation.size()+image.scope.size()+image.schema.size()+text(header[4]).size()+text(header[5]).size()+encoded_catalog.size()+64+32);
    writer.execute("INSERT INTO main._lattice_ordered_prepare_snapshot_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",header);
    if(writer.changes()!=1||preservation_header(writer,snapshot,initial)!=header)refuse("receiver preservation initial header postimage differs");capacity.insert(history_capacity_class::snapshot,header);
    uint64_t ordinal=0,payload_bytes=0,seen_rows=0;std::string previous;
    for(const auto& table:tables){uint64_t seen=0;preservation_cursor cursor(h,table);
        for(;;){const bool present=cursor.next([&](std::vector<uint8_t>&& payload,uint64_t part,bool last){managed_pair_deadline(end);if(seen==table.records||seen_rows==rows||ordinal==limits.records)refuse("receiver preservation cursor exceeds retained row/chunk count");
            budget one{history_stage_limits};one.add(2*payload.size()+10*sizeof(column_value_t)+snapshot.size()+table.name.size()+192);
            values stored{snapshot,int64_t(ordinal),table.kind,table.name,int64_t(seen),int64_t(part),int64_t(last),std::move(payload),previous};stored.emplace_back(history_seal("ordinary-receiver-local-observation-v1",stored,9));
            const auto& raw=std::get<std::vector<uint8_t>>(stored[7]);reserve(snapshot.size()+table.name.size()+raw.size()+previous.size()+64+40);
            writer.execute("INSERT INTO main._lattice_ordered_prepare_record_v1 VALUES(?,?,?,?,?,?,?,?,?,?)",stored);
            const auto actual=read(h,"SELECT snapshot,ordinal,kind,model,row_index,part,is_last,payload,previous,seal FROM main._lattice_ordered_prepare_record_v1 WHERE snapshot=? AND ordinal=?",{snapshot,int64_t(ordinal)},1,one);
            if(writer.changes()!=1||actual.size()!=1||actual[0]!=stored)refuse("receiver preservation record postimage differs");capacity.insert(history_capacity_class::record,stored);previous=text(stored[9]);payload_bytes+=raw.size();++ordinal;
        });if(!present)break;++seen;++seen_rows;}
        if(seen!=table.records)refuse("receiver preservation local rows disappeared");
    }
    if(seen_rows!=rows)refuse("receiver preservation incomplete local cut");
    reserve(previous.size());const auto before_header=header;header[7]=int64_t{1};header[8]=int64_t(rows);header[9]=int64_t(ordinal);header[10]=int64_t(payload_bytes);header[11]=previous;header[12]=history_seal("ordinary-receiver-local-preservation-v1",header,12);
    writer.execute("UPDATE main._lattice_ordered_prepare_snapshot_v1 SET state=?,rows=?,records=?,bytes=?,content=?,seal=? WHERE snapshot=? AND state=0",{header[7],header[8],header[9],header[10],header[11],header[12],header[0]});
    if(writer.changes()!=1||preservation_header(writer,snapshot,initial)!=header)refuse("receiver preservation final postimage or quota differs");capacity.replace(history_capacity_class::snapshot,before_header,header);if(capacity.bytes()!=shared)refuse("preservation shared issued deltas differ");capacity.finish(initial,true);

    auto record=preservation_decode(header,limits);preservation_verify(writer,record,limits,end);
    const auto after=writer.physical_identity("main",{},true);if(!after||*after!=*physical)refuse("receiver preservation physical identity changed before body return");return record;
}
void ordinary_export_access::validate_receiver_preservation(const ordinary_managed_receiver_preservation& cap,lattice_db& owner,database& writer,ordinary_launch::deadline end){
    const auto& state=cap.state_;if(!state||state->creator!=history_creator()||state->owner.get()!=&owner||!state->admission||!owner.config_.ordinary_context||owner.is_closed()||lattice_db::resolve_path(owner.config_)!=state->path||std::chrono::steady_clock::now()>=end)refuse("receiver preservation actual owner unavailable");
    const auto physical=writer.physical_identity("main",{},true);if(!physical||state->record.device!=std::to_string(physical->device)||state->record.inode!=std::to_string(physical->inode))refuse("receiver preservation actual physical owner changed");
    if(state->admission->enrollment_binding_until(state->path,physical->device,physical->inode,end)!=state->record.generation)refuse("receiver preservation managed generation changed");
    owner.config_.ordinary_context->validate_managed_attachment(state->admission,state->path,physical->device,physical->inode,end);
    preservation_verify(writer,state->record,state->limits,end);
}
std::shared_ptr<const ordinary_managed_receiver_preservation> ordinary_export_access::preserve_managed_receiver(std::shared_ptr<lattice_db> owner,
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,ordinary_launch::deadline end,const ordinary_history_limits& limits){
    history_limits(limits);
    if(!owner||!admission||owner->is_closed()||owner->config_.read_only||owner->config_.is_in_memory()||!owner->config_.ordinary_context||owner->config_.audit_retention_seconds!=0||owner->config_.is_sync_enabled()||owner->config_.is_ipc_enabled()||std::chrono::steady_clock::now()>=end)refuse("receiver preservation requires actual quiescent managed early entry");
    const auto path=lattice_db::resolve_path(owner->config_);admission->claim_enrollment_until(path,end);
    try{
        ordinary_resource_reservation<false> charge; // Before native metadata, cursor or row copies.
        std::shared_ptr<const ordinary_managed_receiver_preservation> candidate;
        const auto settled=recovery_writer_access::ordinary_owned_write(*owner,[&](database& writer){
            if(owner->ordinary_managed_profile_.load(std::memory_order_acquire)||owner->recovery_schemas_.swift_models.empty()||owner->recovery_schemas_.swift_models.size()>wire_limits.models)refuse("receiver preservation published producer or actual mount unavailable");
            const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("receiver preservation managed physical identity unavailable");const auto generation=admission->enrollment_binding_until(path,physical->device,physical->inode,end);
            owner->config_.ordinary_context->validate_managed_attachment(admission,path,physical->device,physical->inode,end);
            std::vector<std::string> names;for(const auto& name:owner->recovery_schemas_.swift_models)names.push_back(name);std::sort(names.begin(),names.end());
            auto record=preserve_receiver_storage(*owner,writer,names,generation,limits,end);
            const auto retained=history_capacity_retained_payload+sizeof(ordinary_managed_receiver_preservation::state)+path.size()+physical->filename.size()+record.snapshot.size()+record.generation.size()+record.scope.size()+record.schema.size()+record.device.size()+record.inode.size()+record.catalog_digest.size()+record.content.size()+record.seal.size();
            if(path.size()>4096||physical->filename.size()>4096||retained>ordinary_history_limits::owner_bytes)refuse("receiver preservation retained owner metadata bound");
            auto state=std::shared_ptr<const ordinary_managed_receiver_preservation::state>(new ordinary_managed_receiver_preservation::state(owner,admission,path,std::move(record),limits,*physical,end),&ordinary_managed_receiver_preservation::state::dispose);
            candidate=std::shared_ptr<const ordinary_managed_receiver_preservation>(new ordinary_managed_receiver_preservation(std::move(state)));
            validate_receiver_preservation(*candidate,*owner,writer,end);
        });
        // Engine durability and current publication are different outcomes.
        // No universal rollback guarantee follows from logical close, a late
        // currentness change, notification failure or observed premature COMMIT.
        if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
        const auto& state=candidate->state_;if(state->creator!=history_creator()||owner->is_closed()||std::chrono::steady_clock::now()>=end)refuse("committed receiver preservation no longer publishable");
        if(admission->enrollment_binding_until(path,state->physical.device,state->physical.inode,end)!=state->record.generation)refuse("committed receiver preservation lost managed publication eligibility");
        // Publish only a weak association after actual own COMMIT/delivery.
        owner->ordinary_receiver_preservation_.publish(candidate);
        return candidate;
    }catch(...){admission->fail_enrollment();throw;}
}
std::string ordinary_export_access::preserve_receiver_for_test(lattice_db& owner,const std::vector<std::string>& models,const ordinary_history_limits& limits){
    // This real SQL/storage fixture has NO managed admission and returns only
    // an opaque selection string; it cannot issue the production capability.
    ordinary_resource_reservation<false> charge;ordinary_receiver_preservation_record record;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){record=preserve_receiver_storage(owner,writer,models,"test-observation-only-not-managed-authority",limits,std::chrono::steady_clock::now()+std::chrono::seconds(30));});
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);return record.snapshot;
}
void ordinary_export_access::verify_receiver_preservation_for_test(lattice_db& owner,const std::string& snapshot,const ordinary_history_limits& limits){
    ordinary_resource_reservation<false> charge;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){budget one{history_stage_limits};auto record=preservation_decode(preservation_header(writer,snapshot,one),limits);preservation_verify(writer,record,limits,std::chrono::steady_clock::now()+std::chrono::seconds(30));});
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
}
