// Included by ordinary_export.cpp after its actual source/custody adapters.
namespace {
// Stream a single prepared cursor. Only fixed engine-owned callbacks occur;
// there is no application callback, model hydration or network wait here.
// Copies are charged against a fresh scratch account per actual SQLite ROW.
void history_stream(sqlite3* h,const std::string& sql,const values& parameters,
    const std::function<void(const values&,budget&)>& consume) {
    database::record_statement();sqlite3_stmt* raw=nullptr;const char* tail=nullptr;
    const auto rc=sqlite3_prepare_v2(h,sql.data(),static_cast<int>(sql.size()),&raw,&tail);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> held(raw,&sqlite3_finalize);
    if(rc!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw)||tail!=sql.data()+sql.size()||sqlite3_bind_parameter_count(raw)!=static_cast<int>(parameters.size()))refuse("history cursor shape changed");
    for(size_t n=0;n<parameters.size();++n){const auto& value=text(parameters[n]);if(sqlite3_bind_text(raw,int(n+1),value.data(),int(value.size()),SQLITE_STATIC)!=SQLITE_OK)refuse("history cursor bind failed");}
    const int columns=sqlite3_column_count(raw);if(columns<1||columns>2050)refuse("history cursor column bound");
    int step;
    while((step=sqlite3_step(raw))==SQLITE_ROW){
        if(sqlite3_txn_state(h,"main")!=SQLITE_TXN_WRITE)refuse("history cursor lost its actual source WRITE");
        budget scratch{history_row_limits};scratch.add(sizeof(values)+size_t(columns)*sizeof(column_value_t));values row;row.reserve(size_t(columns));
        for(int c=0;c<columns;++c){scratch.cell();switch(sqlite3_column_type(raw,c)){
            case SQLITE_NULL:row.emplace_back(nullptr);break;
            case SQLITE_INTEGER:row.emplace_back(static_cast<int64_t>(sqlite3_column_int64(raw,c)));break;
            case SQLITE_FLOAT:row.emplace_back(sqlite3_column_double(raw,c));break; // unknown observations preserve nonfinite bits
            case SQLITE_TEXT:{const int n=sqlite3_column_bytes(raw,c);if(n<0||n>int(ordinary_sync_limits::scalar_bytes))refuse("history text scalar bound");scratch.add(size_t(n));const auto* p=sqlite3_column_text(raw,c);if(!p&&n)refuse("history text disappeared");row.emplace_back(std::string(reinterpret_cast<const char*>(p?p:reinterpret_cast<const unsigned char*>("")),size_t(n)));break;}
            case SQLITE_BLOB:{const int n=sqlite3_column_bytes(raw,c);if(n<0||n>int(ordinary_sync_limits::scalar_bytes/2))refuse("history blob scalar bound");scratch.add(size_t(n));const auto* p=static_cast<const uint8_t*>(sqlite3_column_blob(raw,c));if(!p&&n)refuse("history blob disappeared");row.emplace_back(n?std::vector<uint8_t>(p,p+n):std::vector<uint8_t>{});break;}
            default:refuse("history storage class unavailable");}}
        consume(row,scratch);
    }
    if(step!=SQLITE_DONE)refuse("history cursor failed before complete cut");
}
struct history_page_builder {
    const ordinary_export_frame& frame;const std::string& stage;ordinary_history_stage_writer& writer;const ordinary_history_limits& limits;
    std::string page,previous;uint64_t pages=0,rows=0,known=0,unknown=0;size_t nodes=128;
    picosha2::hash256_one_by_one content;
    void finish(){
        if(!rows&&!known&&!unknown)return;
        history_wire::add(page,"]");const auto checksum=history_wire::digest(page);
        history_wire::add(page,",\"digest\":");history_wire::quote(page,checksum);history_wire::add(page,"}}");
        const auto full=history_wire::digest(page);writer.append(page,previous,full,rows,known,unknown);
        content.process(page.begin(),page.end());previous=full;++pages;page.clear();rows=known=unknown=0;nodes=128;
    }
    void push(std::string_view record,size_t count,int role){
        if(record.size()>limits.page_bytes-1024||count>ordinary_sync_limits::parser_nodes-128||!history_wire::workspace(record.size()+1024,count+128))refuse("history record requires a larger-value profile");
        const auto records=rows+known+unknown;
        if(records&&(records==limits.records_per_page||record.size()+128>limits.page_bytes-page.size()||count>ordinary_sync_limits::parser_nodes-nodes||!history_wire::workspace(page.size()+record.size()+128,nodes+count)))finish();
        if(!rows&&!known&&!unknown){page=history_wire::prefix(frame,stage,"historyPage");history_wire::add(page,",\"index\":");history_wire::count(page,pages);history_wire::add(page,",\"previousDigest\":");history_wire::quote(page,previous);history_wire::add(page,",\"records\":[");}
        else history_wire::add(page,",");
        history_wire::add(page,record);nodes+=count;if(role==0)++rows;else if(role==1)++known;else ++unknown;
    }
};
std::string history_header(const ordinary_export_frame& frame,const std::string& stage,
    uint64_t rows,uint64_t known,uint64_t unknown,uint64_t pages,const std::string& content){
    using namespace history_wire;std::string out=prefix(frame,stage,"historyCut");
    add(out,",\"mode\":\"full\",\"base\":null,\"scope\":");::lattice::detail::history_wire::quote(out,frame.scope);add(out,",\"schema\":");::lattice::detail::history_wire::quote(out,digest(frame.schema));add(out,",\"catalog\":");::lattice::detail::history_wire::quote(out,frame.schema);
    add(out,",\"captureDevice\":");::lattice::detail::history_wire::quote(out,frame.device);add(out,",\"captureInode\":");::lattice::detail::history_wire::quote(out,frame.inode);
    add(out,",\"rows\":");count(out,rows);add(out,",\"knownOriginals\":");count(out,known);add(out,",\"unknownOriginals\":");count(out,unknown);add(out,",\"pages\":");count(out,pages);add(out,",\"contentDigest\":");::lattice::detail::history_wire::quote(out,content);
    add(out,",\"models\":[");for(size_t n=0;n<frame.models.size();++n){if(n)add(out,",");::lattice::detail::history_wire::quote(out,frame.models[n]);}add(out,"],\"tables\":[");size_t nodes=160+frame.models.size();
    for(size_t n=0;n<frame.tables.size();++n){const auto& t=frame.tables[n];if(n)add(out,",");add(out,"{\"name\":");::lattice::detail::history_wire::quote(out,t.name);add(out,",\"columns\":[");
        if(t.columns.size()>ordinary_sync_limits::columns_per_table||t.columns.size()!=t.types.size()||16+2*t.columns.size()>ordinary_sync_limits::parser_nodes-nodes)refuse("history descriptor node bound");nodes+=16+2*t.columns.size();
        for(size_t c=0;c<t.columns.size();++c){if(c)add(out,",");::lattice::detail::history_wire::quote(out,t.columns[c]);}add(out,"],\"types\":[");for(size_t c=0;c<t.types.size();++c){if(c)add(out,",");add(out,std::to_string(static_cast<int>(t.types[c])));}add(out,"]}");}
    add(out,"]");const auto checksum=digest(out);add(out,",\"digest\":");::lattice::detail::history_wire::quote(out,checksum);add(out,"}}");if(!workspace(out.size(),nodes))refuse("history descriptor workspace bound");return out;
}
// Passive same-WRITE source observations tied to this actual sealed cut. A
// predecessor frontier is not an old receiver receipt/content/result proof.
std::string history_mounted_relation(const ordinary_history_stage_record& record,
    const values* mounted_predecessor,const values* selected_predecessor) {
    using namespace history_wire;
    constexpr size_t relation_bytes=262144;
    std::string out="{\"version\":1,\"mountedPredecessor\":";
    const auto bounded=[&]{if(out.size()>relation_bytes)refuse("mounted relation observation byte bound");};
    const auto predecessor=[&](const values* old){
        if(!old){add(out,"null");return;}
        add(out,"{\"scope\":");::lattice::detail::history_wire::quote(out,text((*old)[0]));add(out,",\"source\":");::lattice::detail::history_wire::quote(out,text((*old)[1]));
        add(out,",\"epoch\":");count(out,uint64_t(integer((*old)[2])));
        add(out,",\"issuedFrontier\":");count(out,uint64_t(integer((*old)[3])));
        add(out,",\"captureDevice\":");::lattice::detail::history_wire::quote(out,text((*old)[4]));add(out,",\"captureInode\":");::lattice::detail::history_wire::quote(out,text((*old)[5]));
        add(out,",\"schema\":");::lattice::detail::history_wire::quote(out,digest(text((*old)[6])));add(out,"}");bounded();
    };
    predecessor(mounted_predecessor);add(out,",\"selectedPredecessor\":");predecessor(selected_predecessor);
    const auto& now=record.identity;
    const auto suffix=record.header.rfind(",\"digest\":\"");
    if(suffix==std::string::npos||record.header.size()!=suffix+78)refuse("mounted relation actual header seal unavailable");
    const auto header_digest=record.header.substr(suffix+11,64);
    if(header_digest!=digest(std::string_view(record.header).substr(0,suffix)))refuse("mounted relation actual header digest changed");
    add(out,",\"current\":{\"source\":");::lattice::detail::history_wire::quote(out,now.source);add(out,",\"epoch\":");count(out,uint64_t(now.epoch));
    add(out,",\"cut\":");count(out,uint64_t(now.cut));add(out,",\"stage\":");::lattice::detail::history_wire::quote(out,now.stage);
    add(out,",\"scope\":");::lattice::detail::history_wire::quote(out,now.scope);add(out,",\"schema\":");::lattice::detail::history_wire::quote(out,digest(now.schema));
    add(out,",\"captureDevice\":");::lattice::detail::history_wire::quote(out,now.device);add(out,",\"captureInode\":");::lattice::detail::history_wire::quote(out,now.inode);
    add(out,",\"headerDigest\":");::lattice::detail::history_wire::quote(out,header_digest);add(out,",\"contentDigest\":");::lattice::detail::history_wire::quote(out,record.content_digest);
    add(out,"}}");bounded();return out;
}

// Known originals are issued only from actual locally generated observations
// or an already validated ingress envelope. Unknown remote AuditLog history
// retains its exact typed observations without parsing or inventing identity.
std::optional<std::pair<audit_log_entry,ordinary_audit_original_descriptor>> history_original(
    database& writer,const ordinary_export_frame& image,const values& row,
    const recovery_owner_schema& catalog,bool ingress,budget& scratch) {
    if(row.size()!=13)refuse("history AuditLog projection changed");
    bool retained_known=false;
    if(ingress&&std::holds_alternative<std::string>(row[1]))retained_known=!read(ordinary_export_storage_access::borrow(writer),"SELECT 1 FROM main._lattice_ordered_ingress_entry_v2 WHERE scope=? AND gid=?",{image.scope,text(row[1])},1,scratch).empty();
    const auto unknown=[&]()->std::optional<std::pair<audit_log_entry,ordinary_audit_original_descriptor>>{if(retained_known)refuse("proved ingress original cannot be downgraded to unknown");return std::nullopt;};
    // No receipt is being issued for an unproved malformed observation. Its
    // full typed row can still be preserved as unknown historical evidence.
    for(size_t n:{size_t{0},size_t{4},size_t{9},size_t{10},size_t{11}})if(!std::holds_alternative<int64_t>(row[n]))return unknown();
    for(size_t n:{size_t{1},size_t{2},size_t{3},size_t{5}})if(!std::holds_alternative<std::string>(row[n]))return unknown();
    audit_log_entry entry;entry.id=integer(row[0]);entry.global_id=text(row[1]);entry.table_name=text(row[2]);entry.operation=text(row[3]);entry.row_id=integer(row[4]);entry.global_row_id=text(row[5]);
    if(entry.id<=0||entry.global_id.empty()||entry.global_id.size()>4096||entry.global_id.find('\0')!=std::string::npos||entry.global_row_id.empty()||entry.global_row_id.size()>4096||entry.global_row_id.find('\0')!=std::string::npos)return unknown();
    for(size_t n:{size_t{9},size_t{10},size_t{11}})if(integer(row[n])!=0&&integer(row[n])!=1)return unknown();
    entry.is_from_remote=integer(row[9]);entry.is_synchronized=integer(row[10]);entry.synthesized=integer(row[11]);
    const auto table=std::find_if(image.tables.begin(),image.tables.end(),[&](const auto& t){return t.name==entry.table_name;});if(table==image.tables.end())refuse("history audit scope changed");
    if(ingress)hydrate_source_upload(ordinary_export_storage_access::borrow(writer),image,entry,scratch);
    if(!entry.upload_original){
        if(entry.is_from_remote)return unknown();
        // Malformed legacy age/JSON remains unknown; capacity and actual SQL
        // errors below are not silently reclassified as successful history.
        if(!std::holds_alternative<std::string>(row[12]))return unknown();
        entry.timestamp=text(row[12]);
        try{(void)normalize_required_ordinary_timestamp(entry.timestamp);}catch(const db_error&){return unknown();}
        if(!std::holds_alternative<std::string>(row[6])||!std::holds_alternative<std::string>(row[7]))return unknown();
        bool capacity_failure=false;
        try {
            auto parsed=parse_ordinary_v2_audit_values(text(row[6]),text(row[7]),[&](size_t n){try{scratch.add(n);}catch(...){capacity_failure=true;throw;}},[&](size_t n){try{return scratch.acquire_workspace(n);}catch(...){capacity_failure=true;throw;}});
            entry.changed_fields=std::move(parsed.fields);entry.changed_fields_names=std::move(parsed.names);
        }catch(const db_error&){if(capacity_failure)throw;return unknown();}
    }
    const auto& fields=table->audit_names.empty()?table->columns:table->audit_names;
    for(const auto& [name,value]:entry.changed_fields)if(std::find(fields.begin(),fields.end(),name)==fields.end()){if(entry.upload_original)refuse("known ingress original name outside actual projection");return unknown();}
    for(const auto& name:entry.changed_fields_names)if(std::find(fields.begin(),fields.end(),name)==fields.end()){if(entry.upload_original)refuse("known ingress changed name outside actual projection");return unknown();}
    if(entry.operation!="INSERT"&&entry.operation!="UPDATE"&&entry.operation!="DELETE")return unknown();
    (void)normalize_required_ordinary_timestamp(entry.timestamp);
    for(const auto& [name,property]:entry.changed_fields){const auto field=std::find(fields.begin(),fields.end(),name);const auto type=table->types.at(size_t(field-fields.begin()));const auto& value=property.value;
        const auto* blob=type==column_type::blob?std::get_if<std::string>(&value):nullptr;const bool hex_blob=blob&&blob->size()%2==0&&std::all_of(blob->begin(),blob->end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F');});
        const bool valid=std::holds_alternative<std::nullptr_t>(value)||(type==column_type::text&&std::holds_alternative<std::string>(value))||(type==column_type::integer&&std::holds_alternative<int64_t>(value))||(type==column_type::blob&&(std::holds_alternative<std::vector<uint8_t>>(value)||hex_blob))||(type==column_type::real&&(std::holds_alternative<double>(value)||std::holds_alternative<int64_t>(value)));
        if(!valid){if(entry.upload_original)refuse("known ingress original type changed");return unknown();}
    }
    auto descriptor=capture_original_descriptor(entry,catalog,scratch);descriptor.digest=bind_original_digest(image,descriptor.digest);
    // Late binding uses a real point read in the same pinned source WRITE.
    // Raw membership and its digest are captured first and never overwritten.
    const auto* model=catalog.find(entry.table_name);
    if(model&&entry.operation=="UPDATE")for(const auto& property:model->properties)if(property.no_history&&property.kind==property_kind::primitive&&std::find(descriptor.names.begin(),descriptor.names.end(),property.name)!=descriptor.names.end()){
        const auto current=read(ordinary_export_storage_access::borrow(writer),"SELECT "+quote(property.name)+" FROM main."+quote(entry.table_name)+" WHERE globalId=?",{entry.global_row_id},1,scratch);
        if(current.empty()){entry.changed_fields.erase(property.name);entry.changed_fields_names.erase(std::remove(entry.changed_fields_names.begin(),entry.changed_fields_names.end(),property.name),entry.changed_fields_names.end());}
        else entry.changed_fields[property.name]=any_property::from_column_value(current[0][0]);
    }
    return std::pair{std::move(entry),std::move(descriptor)};
}
}

ordinary_mounted_history_source ordinary_export_access::register_mounted_history(std::shared_ptr<lattice_db> parent,
    const std::vector<std::string>& supplied,const ordinary_history_limits& limits){
    // Reserve common capacity before catalog copying, SQL or child allocation.
    ordinary_resource_reservation<false> reservation;history_limits(limits);
    if(!parent||parent->is_closed()||parent->config_.is_in_memory()||parent->config_.read_only||
        parent->config_.is_sync_enabled()||parent->config_.is_ipc_enabled()||parent->config_.ordinary_context||
        parent->recovery_continuous_||parent->administrative_owner_||!parent->recovery_schemas_.valid())
        refuse("mounted history requires actual ordinary server owner");
    std::vector<std::string> names;
    // This issuer is specifically the actual Swift mount boundary. Generic
    // native catalogs remain on their unchanged existing capture route.
    if(parent->recovery_schemas_.swift_models.empty()||parent->recovery_schemas_.swift_models.size()>wire_limits.models||supplied.size()!=parent->recovery_schemas_.swift_models.size())refuse("mounted history actual Swift catalog unavailable");
    for(const auto& name:parent->recovery_schemas_.swift_models){(void)quote(name);names.push_back(name);}
    auto requested=supplied;for(const auto& name:requested)(void)quote(name);std::sort(requested.begin(),requested.end());
    if(requested!=names)refuse("mounted history typed mount differs from actual catalog");
    auto state=std::shared_ptr<ordinary_mounted_history_source_state>(new ordinary_mounted_history_source_state(std::move(reservation),parent,limits,std::move(names)),&ordinary_mounted_history_source_state::dispose);
    const auto settled=recovery_writer_access::ordinary_owned_write(*parent,[&](database& writer){
        require_managed_profile_if_present(*parent,writer,&limits);
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);require_ordered_receiver_absent(writer);
        auto* h=ordinary_export_storage_access::borrow(writer);budget scratch{history_metadata_limits};
        const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("mounted history physical owner unavailable");
        ordinary_export_frame frame;frame.wire_profile=true;
        capture_projected_tables(h,parent->recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},state->mounted,frame,scratch,false,nullptr,nullptr);
        if(history_capacity_retained_payload+sizeof(ordinary_mounted_history_source_state)+sizeof(ordinary_history_refund_cell)+frame.schema.size()+frame.scope.size()+state->mounted.size()*sizeof(std::string)>ordinary_history_limits::owner_bytes-65536)refuse("mounted history owner retention bound");
        state->scope=std::move(frame.scope);state->schema=std::move(frame.schema);state->device=std::to_string(physical->device);state->inode=std::to_string(physical->inode);
        const bool existing=inspect_storage(h,scratch);register_mutation_guard(h);mutation_scope permission(h);
        if(!existing){
            // Historical server rows may be captured by this real mounted
            // source issuer. Surviving receiver/profile/original/stage custody
            // cannot be silently promoted after source loss.
            if(!read(h,"SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_*' OR tbl_name GLOB '_lattice_ordered_*' LIMIT 1",{},1,scratch).empty())refuse("mounted historical source has surviving ordered custody");
            writer.execute(source_ddl);for(const char* operation:{"INSERT","UPDATE","DELETE"})writer.execute(guard_ddl(operation));
        }
        if(!inspect_storage(h,scratch))refuse("mounted source guards unavailable");
        const auto rows=read(h,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 ORDER BY scope",{},maximum_profiles,scratch);
        for(const auto& row:rows){
            if(text(row[4])!=state->device||text(row[5])!=state->inode||!canonical_uuid(text(row[1]))||integer(row[2])!=1||integer(row[3])<0)refuse("mounted source physical continuity changed");
            if(text(row[0])==state->scope&&text(row[6])!=state->schema)refuse("mounted source prior full catalog changed");
        }
        // Source guard DDL changed the real cookie. The complete mounted catalog
        // above was validated in this SAME WRITE; refresh both canonical markers
        // before its original COMMIT so checked existing reopen remains honest.
        parent->store_recovery_fingerprints();
        // No full-mounted source row, UUID, epoch, frontier or receipt is minted
        // merely because six models were mounted. Selected capture issues its
        // own new lineage only inside the later complete native cut transaction.
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    if(parent->is_closed()||state->creator!=history_creator())refuse("mounted source retired before publication");
    parent->ordinary_export_enrolled_here_.store(true,std::memory_order_release);
    return ordinary_mounted_history_source{std::move(state)};
}

ordinary_history_export ordinary_export_access::capture_mounted_history(const std::shared_ptr<ordinary_mounted_history_source_state>& state,
    const std::vector<std::string>& names,bool observe_lineage){
    if(!state||state->creator!=history_creator()||state->owner->is_closed())refuse("mounted source is not current");
    // Declared BEFORE every selected/cut temporary; failed admission/creation
    // keeps capturing occupied through their complete unwind and destruction.
    struct completion {ordinary_mounted_history_source_state& value;bool admitted=false;
        ~completion(){if(admitted&&value.creator==history_creator()){std::lock_guard lock(value.leaf);value.capturing=false;}}} done{*state};
    if(names.empty()||names.size()>state->mounted.size())refuse("mounted selected scope bound");
    auto selected=names;for(const auto& name:selected)(void)quote(name);std::sort(selected.begin(),selected.end());
    if(std::adjacent_find(selected.begin(),selected.end())!=selected.end()||!std::includes(state->mounted.begin(),state->mounted.end(),selected.begin(),selected.end()))refuse("selected model outside actual mounted catalog");
    std::shared_ptr<ordinary_history_export_state> active;bool denied=false;
    {std::lock_guard lock(state->leaf);active=state->active.lock();denied=bool(active)||state->capturing||state->retained_cut||state->retained_construction;
        if(!denied){state->capturing=true;done.admitted=true;}}
    if(denied)refuse("mounted source already retains a cut"); // temporary is destroyed off-leaf
    auto cut=capture_history_impl(*state->owner,selected,state->limits,state,observe_lineage);
    if(state->creator!=history_creator()||state->owner->is_closed())refuse("mounted source retired after durable cut");
    {std::lock_guard lock(state->leaf);state->active=cut.state_;}
    return cut;
}
ordinary_history_export ordinary_export_access::capture_history(lattice_db& parent,
    const std::vector<std::string>& requested,const ordinary_history_limits& limits){return capture_history_impl(parent,requested,limits,{});}

ordinary_history_export ordinary_export_access::capture_history_impl(lattice_db& parent,
    const std::vector<std::string>& requested,const ordinary_history_limits& limits,const std::shared_ptr<ordinary_mounted_history_source_state>& mounted,bool observe_lineage) {
    std::optional<ordinary_resource_reservation<false>> reservation;if(!mounted)reservation.emplace();history_limits(limits);
    if(!parent.recovery_schemas_.valid()||requested.size()>wire_limits.models||parent.config_.is_in_memory()||parent.config_.read_only)refuse("history source requires actual writable file owner/catalog");
    std::vector<std::string> names;
    if(requested.empty()){for(const auto& [name,unused]:parent.recovery_schemas_.models){(void)quote(name);names.push_back(name);}}
    else for(const auto& name:requested){(void)quote(name);names.push_back(name);}
    if(names.empty()||names.size()>wire_limits.models)refuse("history source scope bound");std::sort(names.begin(),names.end());if(std::adjacent_find(names.begin(),names.end())!=names.end())refuse("history duplicate model");
    auto construction=history_retain_construction(std::move(reservation),mounted);
    configuration config=parent.config_;config.path=lattice_db::resolve_path(parent.config_);config.websocket_url.clear();config.authorization_token.clear();config.audit_retention_seconds=0;
    const auto creator=history_creator();
    // child() returns unique ownership. Release directly into creator-aware
    // deletion, so a forked copy never closes the parent's inherited child.
    auto* raw_child=child(parent,config,&construction->connections).release();
    if(creator!=history_creator())refuse("history child created across process boundary"); // inherited raw child is quarantined
    std::shared_ptr<lattice_db> owner(raw_child,[creator](lattice_db* value){if(creator==history_creator())delete value;});
    ordinary_export_frame frame;frame.wire_profile=true;ordinary_history_stage_record record;record.identity.stage=uuid_t::generate().to_string();
    std::optional<ordinary_history_prior_source_observation> prior;
    std::string mounted_relation;
    const auto settled=recovery_writer_access::ordinary_owned_write(*owner,[&](database& writer){
        require_managed_profile_if_present(*owner,writer,&limits);
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);require_ordered_receiver_absent(writer);
        if(owner->recovery_continuous_||owner->administrative_owner_)refuse("history source is not an ordinary issued owner");
        auto* h=ordinary_export_storage_access::borrow(writer);budget metadata_budget{history_metadata_limits};const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("history source physical identity unavailable");
        frame.device=std::to_string(physical->device);frame.inode=std::to_string(physical->inode);
        ordinary_schema_projection projection;ordinary_schema_metadata metadata;
        capture_projected_tables(h,owner->recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},names,frame,metadata_budget,false,&projection,&metadata);
        if(mounted){
            if(mounted->creator!=history_creator()||mounted->owner.get()!=&parent||parent.is_closed()||mounted->device!=frame.device||mounted->inode!=frame.inode)refuse("mounted cut actual owner changed");
            ordinary_export_frame full;full.wire_profile=true;
            capture_projected_tables(h,owner->recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},mounted->mounted,full,metadata_budget,false,nullptr,nullptr);
            if(full.scope!=mounted->scope||full.schema!=mounted->schema)refuse("mounted cut full catalog changed");
        }
        const bool enrolled=inspect_storage(h,metadata_budget);register_mutation_guard(h);mutation_scope permitted(h);
        if(mounted&&!enrolled)refuse("mounted cut lost guarded source storage");
        if(!enrolled){if(!read(h,"SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_history_*' OR tbl_name GLOB '_lattice_ordered_history_*' LIMIT 1",{},1,metadata_budget).empty())refuse("prior history stage lost source storage");if(!fresh_origin(*owner,*physical)||owner->ordinary_export_enrolled_here_.load(std::memory_order_acquire))refuse("historical source requires actual managed admission");writer.execute(source_ddl);for(const auto* op:{"INSERT","UPDATE","DELETE"})writer.execute(guard_ddl(op));if(!inspect_storage(h,metadata_budget))refuse("history source enrollment failed");}
        const auto profiles=read(h,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 ORDER BY scope",{},maximum_profiles,metadata_budget);const values* current=nullptr;const values* mounted_predecessor=nullptr;
        for(const auto& p:profiles){if(text(p[4])!=frame.device||text(p[5])!=frame.inode||!canonical_uuid(text(p[1]))||integer(p[2])!=1||integer(p[3])<0)refuse("history source identity changed");if(text(p[0])==frame.scope)current=&p;}
        if(mounted)for(const auto& p:profiles)if(text(p[0])==mounted->scope){
            if(text(p[6])!=mounted->schema)refuse("mounted old full source schema changed");
            mounted_predecessor=&p; // Before allocation/advance, including full==selected group mounts.
            if(mounted->scope!=frame.scope)prior=ordinary_history_prior_source_observation{text(p[0]),text(p[1]),history_wire::digest(text(p[6])),integer(p[2]),integer(p[3])};
        }
        if(current){if(text((*current)[6])!=frame.schema||integer((*current)[3])==INT64_MAX)refuse("history schema transition or exhausted cut");frame.source=text((*current)[1]);frame.epoch=integer((*current)[2]);frame.cut=integer((*current)[3])+1;writer.execute("UPDATE main._lattice_ordered_source_v1 SET cut=? WHERE scope=? AND cut=?",{frame.cut,frame.scope,frame.cut-1});}
        else {if(profiles.size()==maximum_profiles)refuse("history source scope capacity");frame.source=uuid_t::generate().to_string();frame.epoch=1;frame.cut=1;writer.execute("INSERT INTO main._lattice_ordered_source_v1 VALUES(?,?,?,?,?,?,?)",{frame.scope,frame.source,frame.epoch,frame.cut,frame.device,frame.inode,frame.schema});}
        if(writer.changes()!=1)refuse("history source cut allocation ignored");
        record.identity={record.identity.stage,frame.source,frame.scope,frame.schema,frame.device,frame.inode,frame.epoch,frame.cut,false};
        ordinary_history_stage_writer staged(writer,record.identity,limits);history_page_builder pages{frame,record.identity.stage,staged,limits};uint64_t rows=0,known=0,unknown=0;
        for(const auto& table:frame.tables){
            std::string columns;for(const auto& c:table.columns){if(!columns.empty())columns+=',';columns+=quote(c);}
            // Every logical table was proved against the actual generated
            // ordinary rowid-table program. A single rowid cursor needs no
            // growing offset or repeated full-history sort per output page.
            history_stream(h,"SELECT "+columns+" FROM main."+quote(table.name)+" ORDER BY rowid",{},[&](const values& row,budget& scratch){
                if(rows+known+unknown==limits.records)refuse("history total record quota");
                const auto& logical=projection.tables.at(table.name);const auto& actual=metadata.tables.at(table.name);if(row.size()!=logical.columns.size())refuse("history logical row shape");
                const auto& gid=text(row[0]);if(gid.empty()||gid.size()>4096||gid.find('\0')!=std::string::npos)refuse("history logical row identity");
                for(size_t c=0;c<row.size();++c){const auto physical_column=std::find_if(actual.columns.begin(),actual.columns.end(),[&](const auto& v){return v.name==logical.columns[c].name;});if(physical_column==actual.columns.end())refuse("history physical column missing");
                    if(std::holds_alternative<std::nullptr_t>(row[c])){if(c==0||physical_column->not_null)refuse("history row violates actual nullability");continue;}
                    const auto type=table.types[c];if((type==column_type::text&&!std::holds_alternative<std::string>(row[c]))||(type==column_type::integer&&!std::holds_alternative<int64_t>(row[c]))||(type==column_type::blob&&!std::holds_alternative<std::vector<uint8_t>>(row[c]))||(type==column_type::real&&!std::holds_alternative<double>(row[c])&&!std::holds_alternative<int64_t>(row[c])))refuse("history row storage differs from actual schema");}
                pages.push(history_wire::row(table.name,row),8+3*row.size(),0);++rows;
            });
        }
        const bool ingress=validate_source_upload_capture(h,frame,metadata_budget);
        std::string slots;values scope;for(const auto& table:frame.tables){if(!slots.empty())slots+=',';slots+='?';scope.emplace_back(table.name);}
        history_stream(h,"SELECT id,globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized,CAST(timestamp AS TEXT) FROM main.AuditLog WHERE tableName IN ("+slots+") ORDER BY id",scope,[&](const values& observed,budget& scratch){
            if(rows+known+unknown==limits.records)refuse("history total record quota");auto original=history_original(writer,frame,observed,owner->recovery_schemas_,ingress,scratch);
            if(original){const auto& e=original->first;const auto& d=original->second;pages.push(history_wire::known(e,d),160+8*e.changed_fields.size()+2*e.changed_fields_names.size()+2*d.fields.size()+2*d.names.size()+(e.original_identity?2*e.original_identity->changed_fields_names.size():0),1);++known;}
            else {scratch.add(sizeof(values)+12*sizeof(column_value_t));for(size_t c=0;c<12;++c){if(const auto* s=std::get_if<std::string>(&observed[c]))scratch.add(s->size());if(const auto* b=std::get_if<std::vector<uint8_t>>(&observed[c]))scratch.add(b->size());}values actual(observed.begin(),observed.begin()+12);pages.push(history_wire::unknown(actual),64,2);++unknown;}
        });
        if(ingress&&!validate_source_upload_capture(h,frame,metadata_budget))refuse("history ingress context changed");
        pages.finish();pages.content.finish();record.content_digest=picosha2::get_hash_hex_string(pages.content);
        record.header=history_header(frame,record.identity.stage,rows,known,unknown,pages.pages,record.content_digest);record.totals=staged.finish(record.header,record.content_digest);record.sealed=true;
        record.seal=text(history_stage_row(writer,record.identity.stage,metadata_budget)[17]);
        if(mounted&&observe_lineage){
            // Conservative fixed scratch BEFORE source relation rendering; no
            // rows or cut BLOB copies. Old profile pointers are this SAME WRITE's
            // bounded pre-advance observations, never a receiver's receipt.
            metadata_budget.add(4*1024*1024);
            mounted_relation=history_mounted_relation(record,mounted_predecessor,current);
        }
        // First stage/page/ledger creation can also change the cookie. Validate
        // the actual FULL catalog (not only this selected scope) in this WRITE,
        // then refresh its canonical markers before the original source COMMIT.
        ordinary_export_frame complete;complete.wire_profile=true;
        metadata_budget.add(sizeof(std::vector<std::string>)+owner->recovery_schemas_.models.size()*sizeof(std::string));
        std::vector<std::string> complete_names;complete_names.reserve(owner->recovery_schemas_.models.size());
        for(const auto& [name,unused]:owner->recovery_schemas_.models){metadata_budget.add(name.size());complete_names.push_back(name);}
        capture_projected_tables(h,owner->recovery_schemas_,[](auto& out,const auto& property){
            lattice_db::serialize_property_for_fingerprint(out,property);},complete_names,complete,metadata_budget,false,nullptr,nullptr);
        owner->store_recovery_fingerprints();
    },[&]{owner->ordinary_export_enrolled_here_.store(true,std::memory_order_release);});
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    // Refusal-only memory on the original actual parent. This cannot mint a
    // source if durable storage later disappears; source identity remains SQL.
    parent.ordinary_export_enrolled_here_.store(true,std::memory_order_release);
    if(creator!=history_creator())refuse("history capture crossed creator process");
    auto issued=ordinary_history_export{history_owned_state(*construction,mounted,std::move(owner),limits,std::move(record),construction->connections,std::move(prior),std::move(mounted_relation))};
    return issued; // Prefix transfer already preceded the actual control-block boundary.
}

ordinary_history_export ordinary_export_access::resume_history(lattice_db& parent,const std::string& selected,const ordinary_history_limits& limits){
    std::optional<ordinary_resource_reservation<false>> reservation;reservation.emplace();history_limits(limits);
    if(!canonical_uuid(selected)||parent.config_.is_in_memory()||parent.config_.read_only||!parent.recovery_schemas_.valid())refuse("history resume actual owner/selection unavailable");
    auto construction=history_retain_construction(std::move(reservation),{});
    configuration config=parent.config_;config.path=lattice_db::resolve_path(parent.config_);config.websocket_url.clear();config.authorization_token.clear();config.audit_retention_seconds=0;
    const auto creator=history_creator();
    // child() returns unique ownership. Release directly into creator-aware
    // deletion, so a forked copy never closes the parent's inherited child.
    auto* raw_child=child(parent,config,&construction->connections).release();
    if(creator!=history_creator())refuse("history child created across process boundary"); // inherited raw child is quarantined
    std::shared_ptr<lattice_db> owner(raw_child,[creator](lattice_db* value){if(creator==history_creator())delete value;});ordinary_history_stage_record record;
    const auto settled=recovery_writer_access::ordinary_owned_write(*owner,[&](database& writer){
        require_managed_profile_if_present(*owner,writer,&limits);
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);require_ordered_receiver_absent(writer);
        if(owner->recovery_continuous_||owner->administrative_owner_)refuse("history resume requires actual ordinary source route");
        budget metadata_budget{history_metadata_limits};if(!history_storage(ordinary_export_storage_access::borrow(writer),metadata_budget))refuse("history resume stage storage absent");
        const auto stored=history_stage_row(writer,selected,metadata_budget);metadata_budget.add(std::get<std::vector<uint8_t>>(stored[10]).size()+ordinary_sync_limits::schema_bytes+4096);
        record=history_decode_stage(stored,limits);history_source_context(writer,record,metadata_budget);(void)history_shared_bytes(writer,limits,metadata_budget);
        ordinary_export_frame actual;actual.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),owner->recovery_schemas_,[](auto& out,const auto& p){lattice_db::serialize_property_for_fingerprint(out,p);},scope_names(record.identity.scope,wire_limits.models),actual,metadata_budget,false,nullptr,nullptr);
        if(actual.scope!=record.identity.scope||actual.schema!=record.identity.schema)refuse("history resume actual generated schema changed");
        uint64_t bytes=record.header.size(),rows=0,known=0,unknown=0;std::string previous;picosha2::hash256_one_by_one content;
        // Indexed reads retain one actual page at a time. The same owning
        // transaction prevents page replacement between validation and issue.
        for(uint64_t n=0;n<record.totals.pages;++n){budget page_budget{history_stage_limits};
            const auto found=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{selected,int64_t(n)},1,page_budget);
            if(found.size()!=1)refuse("history restart found a missing page");const auto& page=found[0];const auto* payload=std::get_if<std::vector<uint8_t>>(&page[4]);
            if(!payload||payload->empty()||payload->size()>limits.page_bytes||text(page[2])!=previous||text(page[8])!=history_seal("ordinary-paged-page-v1",page,8)||text(page[3])!=picosha2::hash256_hex_string(payload->begin(),payload->end()))refuse("history restart page chain changed");
            const auto r=integer(page[5]),k=integer(page[6]),u=integer(page[7]);if(r<0||k<0||u<0||uint64_t(r)+uint64_t(k)+uint64_t(u)==0||uint64_t(r)+uint64_t(k)+uint64_t(u)>limits.records_per_page)refuse("history restart page count changed");
            if(uint64_t(r)>record.totals.rows-rows||uint64_t(k)>record.totals.known-known||uint64_t(u)>record.totals.unknown-unknown||payload->size()>record.totals.bytes-bytes)refuse("history restart total count changed");
            rows+=uint64_t(r);known+=uint64_t(k);unknown+=uint64_t(u);bytes+=payload->size();previous=text(page[3]);content.process(payload->begin(),payload->end());
        }
        const auto count=read(ordinary_export_storage_access::borrow(writer),"SELECT COUNT(*) FROM main._lattice_ordered_history_page_v1 WHERE stage=?",{selected},1,metadata_budget);
        content.finish();if(uint64_t(integer(count[0][0]))!=record.totals.pages||rows!=record.totals.rows||known!=record.totals.known||unknown!=record.totals.unknown||bytes!=record.totals.bytes||picosha2::get_hash_hex_string(content)!=record.content_digest)refuse("history restart complete manifest changed");
        // Reconstruct the header from the actual catalog and sealed coordinates,
        // not from trusting parsed stored JSON fields as source identity.
        actual.source=record.identity.source;actual.epoch=record.identity.epoch;actual.cut=record.identity.cut;actual.device=record.identity.device;actual.inode=record.identity.inode;
        if(history_header(actual,selected,rows,known,unknown,record.totals.pages,record.content_digest)!=record.header)refuse("history restart header changed");
        history_source_context(writer,record,metadata_budget);(void)history_shared_bytes(writer,limits,metadata_budget);
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    if(creator!=history_creator())refuse("history resume crossed creator process");
    auto issued=ordinary_history_export{history_owned_state(*construction,{},std::move(owner),limits,std::move(record),construction->connections)};
    return issued; // Prefix transfer already preceded the actual control-block boundary.
}
