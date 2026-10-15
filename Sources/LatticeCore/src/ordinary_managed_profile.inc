// Included only by ordinary_export.cpp. This is a private managed early-entry
// transaction, not a path/metadata/wire factory. All SQL is in the actual owned
// WRITE; the capability is published only after its clean COMMIT and delivery.
namespace {
constexpr const char* managed_profile_table="_lattice_ordered_managed_profile_v1";
constexpr const char* managed_profile_ddl="CREATE TABLE _lattice_ordered_managed_profile_v1 ("
    "scope TEXT PRIMARY KEY,source TEXT NOT NULL,epoch INTEGER NOT NULL CHECK(epoch>0),schema TEXT NOT NULL,"
    "device TEXT NOT NULL,inode TEXT NOT NULL,generation TEXT NOT NULL,durable_bytes INTEGER NOT NULL,"
    "records INTEGER NOT NULL,pages INTEGER NOT NULL,records_per_page INTEGER NOT NULL,page_bytes INTEGER NOT NULL,seal TEXT NOT NULL)";
bool managed_profile_storage(sqlite3* handle,budget& retained) {
    const auto objects=read(handle,"SELECT type,name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_managed_*' OR tbl_name=? ORDER BY name",{std::string(managed_profile_table)},6,retained);
    if(objects.empty())return false;
    std::map<std::string,std::pair<std::string,column_value_t>> expected{
        {managed_profile_table,{"table",managed_profile_ddl}},
        {"sqlite_autoindex__lattice_ordered_managed_profile_v1_1",{"index",nullptr}}};
    for(const char* op:{"INSERT","UPDATE","DELETE"})expected.emplace(std::string(managed_profile_table)+"_"+op,std::make_pair(std::string("trigger"),column_value_t{receiver_guard(managed_profile_table,op)}));
    if(objects.size()!=expected.size())refuse("managed profile guard inventory changed");
    for(const auto& row:objects){const auto at=expected.find(text(row[1]));
        if(at==expected.end()||text(row[0])!=at->second.first||row[2]!=at->second.second)refuse("managed profile program changed");}
    if(!read(handle,"SELECT 1 FROM temp.sqlite_master WHERE type='trigger' OR name GLOB '_lattice_ordered_*' LIMIT 1",{},1,retained).empty())refuse("managed profile temporary program refused");
    return true;
}
values managed_profile_row(database& writer,const std::string& scope,budget& retained) {
    auto found=read(ordinary_export_storage_access::borrow(writer),"SELECT scope,source,epoch,schema,device,inode,generation,durable_bytes,records,pages,records_per_page,page_bytes,seal FROM main._lattice_ordered_managed_profile_v1 WHERE scope=?",{scope},1,retained);
    if(found.size()!=1 || text(found[0][12])!=history_seal("ordinary-managed-profile-v1",found[0],12))refuse("managed profile record unavailable or changed");
    return std::move(found[0]);
}
}
struct ordinary_managed_profile::implementation {
    const std::shared_ptr<const ordinary_managed_child_admission> admission;
    const std::string path;
    const physical_store_identity physical;
    const values record;
    const ordinary_history_limits limits;
    implementation(std::shared_ptr<const ordinary_managed_child_admission> a,std::string p,
        physical_store_identity actual,values row,ordinary_history_limits l)
        :admission(std::move(a)),path(std::move(p)),physical(std::move(actual)),record(std::move(row)),limits(l){}
};
ordinary_managed_profile::ordinary_managed_profile(std::shared_ptr<const implementation> value):impl_(std::move(value)){}
ordinary_managed_profile::~ordinary_managed_profile()=default;
std::shared_ptr<const ordinary_managed_child_admission> ordinary_managed_profile::attachment_admission(
    const ordinary_context& view,const std::string& path,uint64_t device,uint64_t inode,ordinary_launch::deadline end)const {
    const auto& state=impl_;
    if(!state||!state->admission||!view||state->path!=path||state->physical.device!=device||state->physical.inode!=inode)
        refuse("managed attachment enrolled profile does not match physical view");
    view->validate_managed_attachment(state->admission,path,device,inode,end);
    return state->admission;
}
std::shared_ptr<ordinary_managed_cohort_link> ordinary_managed_profile::claim_cohort_link(
    lattice_db& owner,database& writer,ordinary_launch::deadline end)const {
    ordinary_export_access::validate_managed_profile_until(*this,owner,writer,end);
    if(!impl_||!impl_->admission)refuse("managed paired profile unavailable");
    return impl_->admission->claim_cohort_link(impl_->admission,end);
}
void ordinary_managed_profile::validate(lattice_db& owner,database& writer) const {
    ordinary_export_access::validate_managed_profile(*this,owner,writer);
}
void ordinary_export_access::validate_managed_profile(const ordinary_managed_profile& profile,lattice_db& owner,database& writer) {
    validate_managed_profile_impl(profile,owner,writer,std::nullopt);
}
void ordinary_export_access::validate_managed_profile_until(const ordinary_managed_profile& profile,lattice_db& owner,database& writer,
    ordinary_launch::deadline end) {
    validate_managed_profile_impl(profile,owner,writer,end);
}
void ordinary_export_access::validate_managed_profile_impl(const ordinary_managed_profile& profile,lattice_db& owner,database& writer,
    std::optional<ordinary_launch::deadline> end) {
    if(end&&std::chrono::steady_clock::now()>=*end)refuse("managed profile validation deadline expired");
    original_view(writer,true);const auto& state=profile.impl_;
    if(!state||!state->admission||owner.is_closed()||!owner.config_.ordinary_context||
        owner.config_.path!=state->path||!owner.recovery_schemas_.valid()||owner.config_.read_only||owner.config_.is_in_memory())
        refuse("managed profile actual owner unavailable");
    const auto physical=writer.physical_identity("main",{},true);
    if(!physical||*physical!=state->physical)refuse("managed profile physical owner changed");
    const auto generation=end?state->admission->enrollment_binding_until(state->path,physical->device,physical->inode,*end):
        state->admission->enrollment_binding(state->path,physical->device,physical->inode);
    budget retained{history_metadata_limits};auto* handle=ordinary_export_storage_access::borrow(writer);
    if(!inspect_storage(handle,retained)||!managed_profile_storage(handle,retained))refuse("managed profile storage disappeared");
    if(managed_profile_row(writer,text(state->record[0]),retained)!=state->record||generation!=text(state->record[6]))refuse("managed profile retained enrollment changed");
    const auto binding=read(handle,"SELECT source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{state->record[0]},1,retained);
    if(binding.size()!=1||text(binding[0][0])!=text(state->record[1])||integer(binding[0][1])!=integer(state->record[2])||
        integer(binding[0][2])<0||binding[0][3]!=state->record[4]||binding[0][4]!=state->record[5]||binding[0][5]!=state->record[3])
        refuse("managed profile source continuity changed");
    ordinary_export_frame observed;observed.wire_profile=true;
    capture_projected_tables(handle,owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
        scope_names(text(state->record[0]),wire_limits.models),observed,retained,false,nullptr,nullptr);
    if(observed.schema!=text(state->record[3])||observed.scope!=text(state->record[0]))refuse("managed profile current schema changed");
    if(end&&std::chrono::steady_clock::now()>=*end)refuse("managed profile validation deadline expired");
}
void ordinary_export_access::require_managed_profile_if_present(lattice_db& owner,database& writer,const ordinary_history_limits* requested) {
    original_view(writer,true);budget retained{history_metadata_limits};
    const bool persisted=managed_profile_storage(ordinary_export_storage_access::borrow(writer),retained);
    const auto profile=owner.ordinary_managed_profile_.load(std::memory_order_acquire);
    if(!persisted&&!profile)return; // Existing never-managed source contract.
    if(!persisted||!profile)refuse("managed history requires its actual retained enrollment");
    profile->validate(owner,writer);
    if(requested){const auto& admitted=profile->impl_->limits;
        if(requested->durable_stage_bytes!=admitted.durable_stage_bytes||requested->records!=admitted.records||
            requested->pages!=admitted.pages||requested->records_per_page!=admitted.records_per_page||
            requested->page_bytes!=admitted.page_bytes)refuse("history request differs from managed enrolled limits");}
}
std::shared_ptr<const ordinary_managed_profile> ordinary_export_access::enroll_managed_history(lattice_db& owner,
    const std::shared_ptr<const ordinary_managed_child_admission>& admission,const ordinary_history_limits& limits) {
    history_limits(limits);
    if(!admission||owner.is_closed()||owner.config_.read_only||owner.config_.is_in_memory()||
        !owner.config_.ordinary_context||owner.config_.audit_retention_seconds!=0||(owner.config_.is_sync_enabled()||owner.config_.is_ipc_enabled()))
        refuse("managed enrollment requires actual retained early-entry and quiescent owner");
    if(owner.ordinary_managed_profile_.load(std::memory_order_acquire))refuse("managed profile already published on owner");
    admission->claim_enrollment(owner.config_.path);
    // Any failure poisons this launch's admission; a new attempt needs real
    // managed restart/continuity issuance, not a reset of a caller flag.
    try {
    std::shared_ptr<const ordinary_managed_profile> candidate;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        require_recovery_local_producer_maintenance_absent(writer);require_continuous_legacy_export_absent(writer);
        // Existing complete-mirror receiver scopes cannot become independent
        // local producers. The separate lineage/partition adoption path must
        // preserve their upstream source and is intentionally not this issuer.
        require_ordered_receiver_absent(writer);
        if(owner.recovery_continuous_||owner.administrative_owner_||!owner.recovery_schemas_.valid())refuse("managed enrollment ordinary catalog unavailable");
        const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("managed enrollment physical owner unavailable");
        const auto path=lattice_db::resolve_path(owner.config_);
        const auto generation=admission->enrollment_binding(path,physical->device,physical->inode);
        budget retained{history_metadata_limits};ordinary_export_frame image;image.wire_profile=true;
        std::vector<std::string> names;retained.add(owner.recovery_schemas_.models.size()*sizeof(std::string));
        if(owner.recovery_schemas_.models.empty()||owner.recovery_schemas_.models.size()>wire_limits.models)refuse("managed enrollment catalog bound");
        for(const auto& [name,unused]:owner.recovery_schemas_.models){retained.add(name.size());names.push_back(name);}
        std::sort(names.begin(),names.end());
        auto* handle=ordinary_export_storage_access::borrow(writer);
        capture_projected_tables(handle,owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},names,image,retained,false,nullptr,nullptr);
        image.device=std::to_string(physical->device);image.inode=std::to_string(physical->inode);
        const bool source_exists=inspect_storage(handle,retained),profile_exists=managed_profile_storage(handle,retained);
        if(profile_exists&&!source_exists)refuse("managed profile lost prior source identity");
        if(!source_exists&&!read(handle,"SELECT 1 FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_*' OR tbl_name GLOB '_lattice_ordered_*' LIMIT 1",{},1,retained).empty())
            refuse("managed initial source has surviving ordered custody");
        register_mutation_guard(handle);mutation_scope permission(handle);
        if(!source_exists){writer.execute(source_ddl);for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(guard_ddl(op));}
        if(!inspect_storage(handle,retained))refuse("managed source installation failed");
        const auto prior=read(handle,"SELECT scope,source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 ORDER BY scope",{},maximum_profiles,retained);
        // A scope merge would change the historical producer promise. It needs
        // explicit migration, not a new UUID derived from a different catalog.
        if(prior.size()>1)refuse("managed historical source has multiple scopes");
        if(!prior.empty()){
            const auto& row=prior.front();
            if(text(row[0])!=image.scope||!canonical_uuid(text(row[1]))||integer(row[2])!=1||integer(row[3])<0||
                text(row[4])!=image.device||text(row[5])!=image.inode||text(row[6])!=image.schema)refuse("managed existing source continuity unavailable");
            image.source=text(row[1]);image.epoch=integer(row[2]);
        }else{
            if(profile_exists)refuse("managed profile source binding disappeared");
            image.source=uuid_t::generate().to_string();image.epoch=1;
            writer.execute("INSERT INTO main._lattice_ordered_source_v1 VALUES(?,?,?,?,?,?,?)",{image.scope,image.source,image.epoch,int64_t{0},image.device,image.inode,image.schema});
            if(writer.changes()!=1)refuse("managed source publication ignored");
        }
        retained.add(sizeof(values)+13*sizeof(column_value_t)+image.scope.size()+image.source.size()+
            image.schema.size()+image.device.size()+image.inode.size()+generation.size()+64);
        values record{image.scope,image.source,image.epoch,image.schema,image.device,image.inode,generation,
            static_cast<int64_t>(limits.durable_stage_bytes),static_cast<int64_t>(limits.records),static_cast<int64_t>(limits.pages),
            static_cast<int64_t>(limits.records_per_page),static_cast<int64_t>(limits.page_bytes)};
        record.emplace_back(history_seal("ordinary-managed-profile-v1",record,12));
        if(profile_exists){
            const auto count=read(handle,"SELECT COUNT(*) FROM main._lattice_ordered_managed_profile_v1",{},1,retained);
            if(count.size()!=1||integer(count[0][0])!=1)refuse("managed profile scope inventory changed");
        }
        if(!profile_exists){
            writer.execute(managed_profile_ddl);for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(managed_profile_table,op));
            writer.execute("INSERT INTO main._lattice_ordered_managed_profile_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",record);
            if(writer.changes()!=1)refuse("managed profile publication ignored");
        }
        if(!managed_profile_storage(handle,retained)||managed_profile_row(writer,image.scope,retained)!=record)refuse("managed profile limits or continuity changed");
        // Candidate is private until verified own COMMIT. Allocation failure
        // here rolls back both metadata changes; it cannot publish capability.
        retained.add(sizeof(ordinary_managed_profile::implementation)+sizeof(ordinary_managed_profile)+8*sizeof(void*)+path.size()+physical->filename.size());
        candidate=std::shared_ptr<const ordinary_managed_profile>(new ordinary_managed_profile(
            std::make_shared<const ordinary_managed_profile::implementation>(admission,path,*physical,std::move(record),limits)));
        candidate->validate(owner,writer);
        // Genuine enrollment DDL changes SQLite's cookie. Refresh the
            // complete declaration markers only after candidate validation,
            // inside this exact owned WRITE before its original COMMIT.
        owner.store_recovery_fingerprints();
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    if(!candidate||owner.is_closed())refuse("managed owner retired during enrollment delivery");
    admission->validate_open(owner.config_.path);
    owner.ordinary_managed_profile_.store(candidate,std::memory_order_release);
    owner.ordinary_export_enrolled_here_.store(true,std::memory_order_release);
    return candidate;
    } catch(...) {admission->fail_enrollment();throw;}
}
