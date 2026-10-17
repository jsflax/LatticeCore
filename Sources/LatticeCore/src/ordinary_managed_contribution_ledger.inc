// Include in ordinary_export.cpp after its actual original/outbox helpers and
// the post-COMMIT managed profile implementation. No public entrypoint.
// These durable observations do not materialize rows or issue ACK/repair facts.
namespace {
constexpr const char* ipc_observation_table="_lattice_ordered_ipc_observation_v1";
constexpr const char* ipc_observation_ddl="CREATE TABLE _lattice_ordered_ipc_observation_v1 ("
    "route TEXT NOT NULL,policy TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),"
    "generation TEXT NOT NULL,source_member TEXT NOT NULL,recipient_member TEXT NOT NULL,"
    "device TEXT NOT NULL,inode TEXT NOT NULL,source TEXT NOT NULL,epoch INTEGER NOT NULL CHECK(epoch>0),"
    "scope TEXT NOT NULL,schema TEXT NOT NULL,cut INTEGER NOT NULL CHECK(cut>0),"
    "model TEXT NOT NULL,row_gid TEXT NOT NULL COLLATE NOCASE,original_gid TEXT NOT NULL COLLATE NOCASE,"
    "envelope BLOB NOT NULL,descriptor BLOB NOT NULL,projection BLOB NOT NULL,"
    "digest TEXT NOT NULL,record_seal TEXT NOT NULL,seal TEXT PRIMARY KEY NOT NULL)";
constexpr const char* ipc_observation_index="CREATE INDEX _lattice_ordered_ipc_observation_v1_row ON "
    "_lattice_ordered_ipc_observation_v1(model,row_gid,source,epoch,scope,schema,cut)";
constexpr const char* ipc_observation_columns="route,policy,revision,generation,source_member,recipient_member,device,inode,source,epoch,scope,schema,cut,model,row_gid,original_gid,envelope,descriptor,projection,digest,record_seal,seal";
// Same finite durable envelope as existing channel delivery metadata. This is
// a stop-with-debt bound, not proof that admission/reclamation is fair or live.
constexpr std::size_t ipc_observation_rows=8192*64;
constexpr std::size_t ipc_observation_bytes=128*1024*1024;
void ipc_observation_parameter_charge(std::initializer_list<std::string_view> inputs,budget& b) {
    b.add(sizeof(values)+7*sizeof(column_value_t));
    for(const auto value:inputs)b.add(value.size()+1);
}

bool ipc_observation_storage(sqlite3* h,budget& b) {
    b.add(24*1024); // fixed SQL/program/map workspace, before allocation
    const auto objects=read(h,"SELECT type,name,tbl_name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_ipc_observation*' OR tbl_name GLOB '_lattice_ordered_ipc_observation*' ORDER BY name",{},8,b);
    if(objects.empty())return false;
    std::map<std::string,values> expected{
        {ipc_observation_table,{"table",ipc_observation_table,ipc_observation_table,ipc_observation_ddl}},
        {"sqlite_autoindex__lattice_ordered_ipc_observation_v1_1",{"index","sqlite_autoindex__lattice_ordered_ipc_observation_v1_1",ipc_observation_table,nullptr}},
        {"_lattice_ordered_ipc_observation_v1_row",{"index","_lattice_ordered_ipc_observation_v1_row",ipc_observation_table,ipc_observation_index}}};
    for(const char* op:{"INSERT","UPDATE","DELETE"}){
        const std::string name=std::string(ipc_observation_table)+'_'+op;
        expected.emplace(name,values{"trigger",name,ipc_observation_table,receiver_guard(ipc_observation_table,op)});
    }
    if(objects.size()!=expected.size())refuse("managed contribution observation inventory changed");
    for(const auto& row:objects){const auto at=expected.find(text(row[1]));if(at==expected.end()||at->second!=row)refuse("managed contribution observation program changed");}
    if(!read(h,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_ipc_observation*' OR tbl_name GLOB '_lattice_ordered_ipc_observation*' LIMIT 1",{},1,b).empty())
        refuse("managed contribution observation TEMP shadow/program");
    return true;
}
original_usage ipc_observation_usage(sqlite3* h,budget& b) {
    const auto found=read(h,"SELECT COUNT(*),COALESCE(SUM(length(CAST(route AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(source_member AS BLOB))+length(CAST(recipient_member AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(model AS BLOB))+length(CAST(row_gid AS BLOB))+length(CAST(original_gid AS BLOB))+length(envelope)+length(descriptor)+length(projection)+length(CAST(digest AS BLOB))+length(CAST(record_seal AS BLOB))+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_ipc_observation_v1",{},1,b);
    if(found.size()!=1)refuse("managed contribution observation usage missing");
    original_usage result{integer(found[0][0]),integer(found[0][1])};
    if(result.rows<0||std::uint64_t(result.rows)>ipc_observation_rows||result.bytes<0||std::uint64_t(result.bytes)>ipc_observation_bytes)
        refuse("managed contribution observation durable bound");
    return result;
}
void verify_ipc_observation(const values& row,budget& b) {
    if(row.size()!=22||integer(row[2])<=0||integer(row[9])<=0||integer(row[12])<=0||
        !std::holds_alternative<std::vector<std::uint8_t>>(row[16])||
        !std::holds_alternative<std::vector<std::uint8_t>>(row[17])||
        !std::holds_alternative<std::vector<std::uint8_t>>(row[18])||
        text(row[21])!=original_seal("ordinary-managed-contribution-observation-v1",row,21,b))
        refuse("managed contribution observation changed");
    for(const auto n:{0u,1u,3u,4u,5u,6u,7u,8u,10u,11u,13u,14u,15u,19u,20u,21u}){
        const auto& value=text(row[n]);if(value.empty()||value.find('\0')!=std::string::npos)refuse("managed contribution observation scalar");
    }
}
std::size_t ipc_observation_size(const values& row) {
    std::size_t result=0;
    for(const auto& value:row){
        const auto* s=std::get_if<std::string>(&value);const auto* bytes=std::get_if<std::vector<std::uint8_t>>(&value);
        const auto n=s?s->size():(bytes?bytes->size():0);
        if(n>ipc_observation_bytes-result)refuse("managed contribution observation payload bound");result+=n;
    }
    return result;
}
std::string ipc_route_policy(const ordinary_managed_ipc_route_description& description,budget& b) {
    // Production snapshot comes from the private issuer. The test seam supplies
    // passive facts only. This fingerprint authenticates neither source.
    (void)description.copy_charge();b.add(64);
    fingerprint h;h.bytes(std::string_view("ordinary-managed-ipc-policy-v1"));
    h.bytes(description.route_key);h.bytes(description.generation);h.bytes(description.source_member);h.bytes(description.recipient_member);
    h.number(description.policy_revision);h.bytes(description.channel);h.number(description.narrowing_emits_removals);
    h.number(description.models.size());for(const auto& model:description.models)h.bytes(model);
    h.number(description.filter.has_value());
    if(description.filter){h.number(description.filter->size());for(const auto& filter:*description.filter){
        h.bytes(filter.table_name);h.number(filter.where_clause.has_value());if(filter.where_clause)h.bytes(*filter.where_clause);
    }}
    return h.finish();
}
// Production metadata encodes only a compact exact association; canonical
// envelope/descriptor bytes stay durable once. The legacy passive fixture
// path retains its original aggregate frame limit and cannot select this tag.
enum class ipc_observation_encoding { passive_fixture, retained_original_association };
constexpr std::size_t ipc_association_bytes=16*1024;
void ipc_observation_input(const ordinary_managed_contribution_observation& input,budget& b,
    ipc_observation_encoding encoding=ipc_observation_encoding::passive_fixture) {
    if(!canonical_uuid(input.source)||input.epoch<=0||input.cut<=0)refuse("managed contribution observation source shape");
    original_key_charge(input.scope,input.original_gid,b);original_digest(input.digest);original_digest(input.record_seal);
    constexpr std::string_view domain="ordinary-logical-projection-v2:";
    if(input.schema.size()!=domain.size()+64||!std::string_view(input.schema).starts_with(domain)||
        !std::all_of(input.schema.begin()+static_cast<std::ptrdiff_t>(domain.size()),input.schema.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))
        refuse("managed contribution observation schema domain");
    if(input.table.empty()||input.table.size()>256||input.row_gid.empty()||input.row_gid.size()>4096||
        input.table.find('\0')!=std::string::npos||input.row_gid.find('\0')!=std::string::npos)
        refuse("managed contribution observation row key");
    std::size_t payload=0;
    for(const auto* value:{&input.envelope,&input.descriptor}) {
        if(value->empty()||value->size()>ordinary_sync_limits::frame_bytes-payload)
            refuse("managed contribution original frame bound");
        payload+=value->size();
    }
    const auto remaining=encoding==ipc_observation_encoding::retained_original_association
        ?ipc_association_bytes:ordinary_sync_limits::frame_bytes-payload;
    if(input.projection.empty()||input.projection.size()>remaining)refuse("managed contribution association bound");
    payload+=input.projection.size();
    // Actual source capture/decoded workspace owns input. Charge one complete
    // retained SQL values copy before copying it; the bounded raw reader also
    // charges all existing rows before any field copy.
    b.add(sizeof(values)+22*sizeof(column_value_t)+payload+input.source.size()+input.scope.size()+
        input.schema.size()+input.table.size()+input.row_gid.size()+input.original_gid.size()+input.digest.size()+input.record_seal.size()+128);
}
}

namespace {
// Shared passive persistence only. Callers separately own their real WRITE;
// production additionally validates its issued local route/profile around it.
values ipc_observation_row(const physical_store_identity& physical,const ordinary_managed_ipc_route_description& d,
    const ordinary_managed_contribution_observation& input,budget& retained) {
    const auto policy=ipc_route_policy(d,retained);
    // Fixed metadata copy budget precedes the only owning row construction.
    retained.add(d.route_key.size()+policy.size()+d.generation.size()+d.source_member.size()+d.recipient_member.size()+128);
    values row;row.reserve(22);
    const auto add=[&](const auto& value){row.emplace_back(value);};
    add(d.route_key);add(policy);add(d.policy_revision);add(d.generation);add(d.source_member);add(d.recipient_member);
    add(std::to_string(physical.device));add(std::to_string(physical.inode));
    add(input.source);add(input.epoch);add(input.scope);add(input.schema);add(input.cut);
    add(input.table);add(input.row_gid);add(input.original_gid);add(input.envelope);add(input.descriptor);add(input.projection);
    add(input.digest);add(input.record_seal);
    row.emplace_back(original_seal("ordinary-managed-contribution-observation-v1",row,21,retained));
    return row;
}
ordinary_managed_contribution_observation_result ipc_observation_record(database& writer,
    const ordinary_managed_ipc_route_description& d,const ordinary_managed_contribution_observation& input,
    budget& retained,ipc_observation_encoding encoding=ipc_observation_encoding::passive_fixture) {
    original_view(writer,true);ipc_observation_input(input,retained,encoding);
    auto* handle=ordinary_export_storage_access::borrow(writer);const auto physical=writer.physical_identity("main",{},true);
    if(!physical)refuse("managed contribution recipient physical identity missing");
    ordinary_managed_contribution_observation_result output;
    register_mutation_guard(handle);mutation_scope permit(handle);
    if(!ipc_observation_storage(handle,retained)){
        writer.execute(ipc_observation_ddl);writer.execute(ipc_observation_index);
        for(const char* op:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(ipc_observation_table,op));
        if(!ipc_observation_storage(handle,retained))refuse("managed contribution observation install failed");
    }
    const auto usage=ipc_observation_usage(handle,retained);
    auto row=ipc_observation_row(*physical,d,input,retained);
    ipc_observation_parameter_charge({text(row[21])},retained);
    const auto prior=read(handle,std::string("SELECT ")+ipc_observation_columns+" FROM main._lattice_ordered_ipc_observation_v1 WHERE seal=?",{row[21]},1,retained);
    if(!prior.empty()){
        verify_ipc_observation(prior[0],retained);if(prior[0]!=row)refuse("managed contribution observation seal collision");
        output.exact_replay=true;
    }else{
        const auto bytes=ipc_observation_size(row);
        if(std::uint64_t(usage.rows)==ipc_observation_rows||bytes>ipc_observation_bytes-static_cast<std::size_t>(usage.bytes))
            refuse("managed contribution observation capacity exhausted");
        writer.execute("INSERT INTO main._lattice_ordered_ipc_observation_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row);
        if(writer.changes()!=1)refuse("managed contribution observation insert ignored");
        ipc_observation_parameter_charge({text(row[21])},retained);
        const auto post=read(handle,std::string("SELECT ")+ipc_observation_columns+" FROM main._lattice_ordered_ipc_observation_v1 WHERE seal=?",{row[21]},1,retained);
        if(post.size()!=1||post[0]!=row)refuse("managed contribution observation postimage differs");
        const auto final_usage=ipc_observation_usage(handle,retained);
        if(final_usage.rows!=usage.rows+1||final_usage.bytes!=usage.bytes+static_cast<std::int64_t>(bytes))refuse("managed contribution observation accounting changed");
    }
    ipc_observation_parameter_charge({input.table,input.row_gid,input.source,input.scope,input.schema},retained);
    // This bounded existence query does not select or compare another
    // source's frontier and never discards any competing observation.
    output.other_lineage_observed=!read(handle,"SELECT 1 FROM main._lattice_ordered_ipc_observation_v1 WHERE model=? AND row_gid=? AND NOT(source=? AND epoch=? AND scope=? AND schema=?) LIMIT 1",{input.table,input.row_gid,input.source,input.epoch,input.scope,input.schema},1,retained).empty();
    // Recheck even an exact replay: no input payload or helper code may alter
    // the protected inventory or silently change accounting in this turn.
    if(!ipc_observation_storage(handle,retained))refuse("managed contribution observation inventory disappeared");
    retained.add(64);output.observation_seal=text(row[21]);
    return output;
}
}

ordinary_managed_contribution_observation_result ordinary_export_access::record_managed_contribution_observation(
    const ordinary_managed_ipc_local_route& route,const ordinary_managed_contribution_observation& input) {
    route.validate_lifetime();auto& owner=route.recipient_owner();const auto& d=route.description();
    budget retained{wire_limits};ordinary_managed_contribution_observation_result output;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        original_view(writer,true);route.validate_lifetime();route.recipient_profile().validate(owner,writer);validate_managed_product_policy(route,owner,writer);
        // Actual declaration-derived/generated table ownership is checked in
        // this view too. A route's root list alone is not a table whitelist.
        ordinary_export_frame observed;observed.wire_profile=true;
        capture_projected_tables(ordinary_export_storage_access::borrow(writer),owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
            d.models,observed,retained,false,nullptr,nullptr);
        if(std::none_of(observed.tables.begin(),observed.tables.end(),[&](const auto& table){return table.name==input.table;}))
            refuse("managed contribution observation model lies outside route");
        output=ipc_observation_record(writer,d,input,retained);
        route.validate_lifetime();route.recipient_profile().validate(owner,writer);validate_managed_product_policy(route,owner,writer);
    });
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)
        throw ordinary_order_write_error(settled);
    // Actual COMMIT of observations only. No model row, AuditLog, delivery bit,
    // source frontier, original ACK, or partition receipt has been changed.
    return output;
}

recovery_install_result ordinary_export_access::record_contribution_observations_for_test(lattice_db& owner,
    const ordinary_managed_ipc_route_description& d,
    const std::vector<ordinary_managed_contribution_observation>& inputs,
    std::vector<ordinary_managed_contribution_observation_result>& results) {
    // Real owned-WRITE storage testing only. No managed profile, route, role,
    // receipt or materialization capability is manufactured by this seam.
    results.clear();std::vector<ordinary_managed_contribution_observation_result> candidate;
    const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        if(recovery_writer_access::active_writer(owner)!=&writer)refuse("test observation lost actual writer");
        budget retained{wire_limits};
        if(inputs.size()>64)refuse("test observation count bound");
        retained.add(inputs.size()*sizeof(ordinary_managed_contribution_observation_result));candidate.reserve(inputs.size());
        for(const auto& input:inputs)candidate.push_back(ipc_observation_record(writer,d,input,retained));
    });
    if(settled.state==recovery_install_state::committed&&!settled.primary_error&&!settled.cleanup_error&&!settled.postcommit_error&&!settled.notification_error)
        results=std::move(candidate);
    return settled;
}
