// Include after guarded original/observation helpers, before peer_request.inc.
// Durable pending metadata references the existing exact current/archive
// original; it grants neither replay authorization nor visible-value choice.
namespace {
constexpr const char* peer_retry_table="_lattice_ordered_ipc_retry_v1";
constexpr const char* peer_retry_columns="identity,policy,scope,gid,original,recipient_device,recipient_inode,receipt,association,observed,seal";
constexpr const char* peer_retry_ddl="CREATE TABLE _lattice_ordered_ipc_retry_v1 (identity TEXT PRIMARY KEY NOT NULL,policy TEXT NOT NULL,scope TEXT NOT NULL,gid TEXT NOT NULL COLLATE NOCASE,original TEXT NOT NULL,recipient_device TEXT NOT NULL,recipient_inode TEXT NOT NULL,receipt TEXT NOT NULL,association BLOB NOT NULL,observed INTEGER NOT NULL CHECK(observed IN (0,1)),seal TEXT NOT NULL)";
constexpr const char* peer_retry_index="CREATE INDEX _lattice_ordered_ipc_retry_v1_original ON _lattice_ordered_ipc_retry_v1(scope,gid,original,policy,observed)";

bool peer_retry_storage(sqlite3* handle,budget& charged){
    charged.add(16*1024);
    const auto objects=read(handle,"SELECT type,name,tbl_name,sql FROM main.sqlite_master WHERE name GLOB '_lattice_ordered_ipc_retry*' OR tbl_name GLOB '_lattice_ordered_ipc_retry*' ORDER BY name",{},8,charged);
    if(!read(handle,"SELECT 1 FROM temp.sqlite_master WHERE name GLOB '_lattice_ordered_ipc_retry*' OR tbl_name GLOB '_lattice_ordered_ipc_retry*' LIMIT 1",{},1,charged).empty())
        refuse("managed peer retry TEMP shadow/program");
    if(objects.empty())return false;
    std::map<std::string,values> expected{
        {peer_retry_table,{"table",peer_retry_table,peer_retry_table,peer_retry_ddl}},
        {"sqlite_autoindex__lattice_ordered_ipc_retry_v1_1",{"index","sqlite_autoindex__lattice_ordered_ipc_retry_v1_1",peer_retry_table,nullptr}},
        {"_lattice_ordered_ipc_retry_v1_original",{"index","_lattice_ordered_ipc_retry_v1_original",peer_retry_table,peer_retry_index}}};
    for(const char* operation:{"INSERT","UPDATE","DELETE"}){
        const auto name=std::string(peer_retry_table)+'_'+operation;
        expected.emplace(name,values{"trigger",name,peer_retry_table,receiver_guard(peer_retry_table,operation)});
    }
    if(objects.size()!=expected.size())refuse("managed peer retry inventory changed");
    for(const auto& row:objects){const auto found=expected.find(text(row[1]));if(found==expected.end()||found->second!=row)refuse("managed peer retry program changed");}
    return true;
}
void peer_retry_verify(const values& row,budget& charged){
    if(row.size()!=11||(integer(row[9])!=0&&integer(row[9])!=1))refuse("managed peer retry row shape");
    for(const auto n:{0u,1u,4u,7u,10u})original_digest(text(row[n]));
    original_key_charge(text(row[2]),text(row[3]),charged);
    for(const auto n:{5u,6u}){
        const auto& value=text(row[n]);if(value.empty()||value.size()>20||(value.size()>1&&value[0]=='0')||!std::all_of(value.begin(),value.end(),[](char c){return c>='0'&&c<='9';}))
            refuse("managed peer retry physical spelling");
    }
    const auto* association=std::get_if<std::vector<std::uint8_t>>(&row[8]);
    if(!association||association->empty()||association->size()>ipc_association_bytes)refuse("managed peer retry association bound");
    // Charge before making the temporary immutable-key copy.
    original_copy_charge(row,charged);values key;key.reserve(6);
    for(std::size_t n=1;n<7;++n)key.push_back(row[n]);
    if(text(row[0])!=original_seal("managed-peer-obligation-v1",key,key.size(),charged))refuse("managed peer retry identity changed");
    if(text(row[10])!=original_seal("managed-peer-obligation-state-v1",row,10,charged))refuse("managed peer retry seal changed");
}
original_usage peer_retry_usage(sqlite3* handle,budget& charged){
    const auto found=read(handle,"SELECT COUNT(*),COALESCE(SUM(length(CAST(identity AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(gid AS BLOB))+length(CAST(original AS BLOB))+length(CAST(recipient_device AS BLOB))+length(CAST(recipient_inode AS BLOB))+length(CAST(receipt AS BLOB))+length(association)+length(CAST(seal AS BLOB))),0) FROM main._lattice_ordered_ipc_retry_v1",{},1,charged);
    if(found.size()!=1)refuse("managed peer retry usage unavailable");
    original_usage result{integer(found[0][0]),integer(found[0][1])};
    if(result.rows<0||result.rows>static_cast<std::int64_t>(ipc_observation_rows)||result.bytes<0||result.bytes>static_cast<std::int64_t>(ipc_observation_bytes))
        refuse("managed peer retry metadata bound");
    return result;
}
values peer_retry_read(database& writer,const std::string& identity,budget& charged){
    original_view(writer,true);original_digest(identity);
    if(!peer_retry_storage(ordinary_export_storage_access::borrow(writer),charged))refuse("managed peer durable obligation missing");
    original_key_charge(identity,identity,charged);
    const auto result=read(ordinary_export_storage_access::borrow(writer),std::string("SELECT ")+peer_retry_columns+" FROM main._lattice_ordered_ipc_retry_v1 WHERE identity=?",{identity},1,charged);
    if(result.size()!=1)refuse("managed peer durable obligation lost");peer_retry_verify(result[0],charged);
    original_copy_charge(result[0],charged);return result[0];
}
values peer_retry_record(database& writer,const std::string& policy,const values& mapping,
    const physical_store_identity& recipient,const std::string& receipt,const std::vector<std::uint8_t>& association,budget& charged){
    original_view(writer,true);original_digest(policy);original_digest(receipt);
    if(association.empty()||association.size()>ipc_association_bytes)refuse("managed peer pending association bound");
    if(mapping.size()!=14||text(mapping[13])!=original_seal("ordinary-wire-current-record-v2",mapping,13,charged))
        refuse("managed peer retry requires exact stored original");
    // No original bytes are duplicated into this metadata journal. Existing
    // canonical replacement preserves this exact record in its guarded archive.
    // Its original byte/row limits remain unchanged and are checked below.
    (void)original_capacity(writer,false,charged);(void)original_capacity(writer,true,charged);
    auto* handle=ordinary_export_storage_access::borrow(writer);register_mutation_guard(handle);mutation_scope permit(handle);
    if(!peer_retry_storage(handle,charged)){
        writer.execute(peer_retry_ddl);writer.execute(peer_retry_index);
        for(const char* operation:{"INSERT","UPDATE","DELETE"})writer.execute(receiver_guard(peer_retry_table,operation));
        if(!peer_retry_storage(handle,charged))refuse("managed peer retry install failed");
    }
    charged.add(sizeof(values)+11*sizeof(column_value_t)+association.size()+policy.size()+text(mapping[0]).size()+text(mapping[1]).size()+text(mapping[13]).size()+receipt.size()+256);
    values fields{policy,mapping[0],mapping[1],mapping[13],std::to_string(recipient.device),std::to_string(recipient.inode)};
    auto identity=original_seal("managed-peer-obligation-v1",fields,fields.size(),charged);
    original_copy_charge(fields,charged);values row;row.reserve(11);row.emplace_back(identity);
    for(auto& field:fields)row.push_back(std::move(field));row.emplace_back(receipt);row.emplace_back(association);row.emplace_back(std::int64_t{0});
    row.emplace_back(original_seal("managed-peer-obligation-state-v1",row,10,charged));
    const auto prior=read(handle,std::string("SELECT ")+peer_retry_columns+" FROM main._lattice_ordered_ipc_retry_v1 WHERE identity=?",{identity},1,charged);
    if(!prior.empty()){
        peer_retry_verify(prior[0],charged);
        for(std::size_t n=0;n<7;++n)if(prior[0][n]!=row[n])refuse("managed peer retry identity collision");
        original_copy_charge(prior[0],charged);return prior[0];
    }
    const auto usage=peer_retry_usage(handle,charged);const auto bytes=ipc_observation_size(row);
    if(usage.rows==static_cast<std::int64_t>(ipc_observation_rows)||bytes>ipc_observation_bytes-static_cast<std::size_t>(usage.bytes))
        refuse("managed peer retry capacity exhausted");
    writer.execute("INSERT INTO main._lattice_ordered_ipc_retry_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?)",row);
    if(writer.changes()!=1||peer_retry_read(writer,identity,charged)!=row)refuse("managed peer retry postimage changed");
    const auto final=peer_retry_usage(handle,charged);
    if(final.rows!=usage.rows+1||final.bytes!=usage.bytes+static_cast<std::int64_t>(bytes))refuse("managed peer retry accounting changed");
    return row;
}
void peer_retry_observed(database& writer,const std::string& identity,const std::string& actual_receipt,budget& charged){
    auto row=peer_retry_read(writer,identity,charged);original_digest(actual_receipt);
    if(text(row[7])!=actual_receipt)refuse("managed peer result is not exact durable observation");
    if(integer(row[9])==1)return;
    const auto usage=peer_retry_usage(ordinary_export_storage_access::borrow(writer),charged);
    row[9]=std::int64_t{1};row[10]=original_seal("managed-peer-obligation-state-v1",row,10,charged);
    register_mutation_guard(ordinary_export_storage_access::borrow(writer));mutation_scope permit(ordinary_export_storage_access::borrow(writer));
    original_key_charge(identity,actual_receipt,charged);
    writer.execute("UPDATE main._lattice_ordered_ipc_retry_v1 SET observed=1,seal=? WHERE identity=? AND observed=0",{row[10],identity});
    if(writer.changes()!=1||peer_retry_read(writer,identity,charged)!=row||peer_retry_usage(ordinary_export_storage_access::borrow(writer),charged)!=usage)
        refuse("managed peer durable result settlement changed");
}
}

// Private passive storage regression seam. These methods cannot construct a
// product/peer/request or authorize delivery. They use actual locally seeded
// original storage and actual owned SQL solely to test journal integrity.
std::string ordinary_export_access::record_managed_peer_retry_for_test(lattice_db& owner,const std::string& scope,
    const std::string& gid,const std::string& policy,const std::string& receipt,const std::vector<std::uint8_t>& association){
    ordinary_resource_reservation<false> held;budget charged{wire_limits};values row;
    const auto result=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        original_context_cache context(writer,charged);const auto mapping=original_record(writer,scope,gid,charged,context);
        if(mapping.empty())refuse("retry fixture has no actual stored original");
        const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("retry fixture physical unavailable");
        row=peer_retry_record(writer,policy,mapping,*physical,receipt,association,charged);context.finish();
    });managed_clean_turn(result);return text(row[0]);
}
void ordinary_export_access::settle_managed_peer_retry_for_test(lattice_db& owner,const std::string& identity,const std::string& receipt){
    budget charged{wire_limits};const auto result=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
        peer_retry_observed(writer,identity,receipt,charged);
    });managed_clean_turn(result);
}
