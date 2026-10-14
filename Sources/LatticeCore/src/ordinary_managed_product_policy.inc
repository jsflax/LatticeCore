// Include only in ordinary_export.cpp after actual metadata/original helpers.
// No network result or public DTO reaches this metadata/policy reader.
namespace {
constexpr export_limits managed_policy_limits{256,2050,8192,2097152,16*1024*1024};
std::string managed_sql_literal(const std::string& value,budget& retained) {
    if(value.size()>4096||value.find('\0')!=std::string::npos)refuse("managed product filter scalar bound");
    retained.add(2+2*value.size());std::string result="'";
    for(char c:value){result+=c;if(c=='\'')result+='\'';}result+='\'';return result;
}
std::string managed_group_spelling(std::string group){for(auto& c:group)if(c>='a'&&c<='f')c=static_cast<char>(c-('a'-'A'));return group;}
void managed_product_columns(const ordinary_schema_projection& projection,const char* table,
    std::initializer_list<std::pair<const char*,column_type>> fields) {
    const auto at=projection.tables.find(table);
    if(at==projection.tables.end()||at->second.role!=ordinary_projection_role::model)refuse("managed product root declaration missing");
    for(const auto& [name,type]:fields){const auto found=std::find_if(at->second.columns.begin(),at->second.columns.end(),[&](const auto& value){return value.name==name;});
        if(found==at->second.columns.end()||found->type!=type)refuse("managed product field declaration changed");}
}
rows managed_policy_rows(database& writer,budget& retained) {
    // Every returned byte is checked by read BEFORE retention, under the same
    // actual owned view as metadata validation. No query callback is supplied.
    return read(ordinary_export_storage_access::borrow(writer),"SELECT project,policy,exposedTeams FROM main.SyncConfig ORDER BY project COLLATE BINARY",{},4096,retained);
}
std::string managed_policy_seal(const rows& policies,budget& retained) {
    values cells;retained.add(sizeof(values)+policies.size()*3*sizeof(column_value_t));
    for(const auto& row:policies){if(row.size()!=3)refuse("managed policy row shape");
        for(const auto& cell:row){const auto& value=text(cell);retained.add(value.size());cells.emplace_back(value);}}
    return original_seal("managed-engram-current-policy-v1",cells,cells.size(),retained);
}
std::string managed_memory_predicate(const rows& policies,bool group_route,const std::string& group,budget& retained) {
    const auto exact_group=managed_group_spelling(group);std::vector<std::string> selected;
    for(const auto& row:policies){
        const auto& project=text(row[0]);const auto& policy=text(row[1]);bool include=policy=="sync";
        if(policy!="sync"&&policy!="local")refuse("managed unknown SyncConfig policy");
        if(group_route){
            const auto& encoded=text(row[2]);if(encoded.size()>65536)refuse("managed exposure set bound");
            auto workspace=retained.acquire_workspace(4096+encoded.size()*32);
            const auto decoded=nlohmann::json::parse(encoded);
            if(!decoded.is_array()||decoded.size()>256)refuse("managed exposure set shape");
            include=false;
            for(const auto& item:decoded){if(!item.is_string()||item.get_ref<const std::string&>().size()>4096)refuse("managed exposure member shape");
                if(item.get_ref<const std::string&>()==exact_group)include=true;}
        }
        if(include){retained.add(sizeof(std::string)+project.size());selected.push_back(project);}
    }
    // Source legacy Query.in([]) is match-none. Never broaden an empty set.
    if(selected.empty())return "0";
    std::string result="(\"isPrivate\" = 0 AND \"project\" IN (";
    for(size_t i=0;i<selected.size();++i){if(i)result+=",";result+=managed_sql_literal(selected[i],retained);if(result.size()>60000)refuse("managed exact predicate bound");}
    result+="))";return result;
}
void managed_clean_turn(const recovery_install_result& value){
    if(value.state!=recovery_install_state::committed||value.primary_error||value.cleanup_error||value.postcommit_error||value.notification_error)
        throw ordinary_order_write_error(value);
}
}

managed_engram_policy ordinary_export_access::capture_managed_engram_policy(
    const ordinary_managed_profile_pair& pair,managed_engram_direction direction,
    const std::string& principal,const std::string& group,ordinary_launch::deadline end) {
    pair.validate(end);
    const bool reverse=direction==managed_engram_direction::personal_to_hub||direction==managed_engram_direction::group_to_hub;
    const bool group_route=direction==managed_engram_direction::hub_to_group||direction==managed_engram_direction::group_to_hub;
    managed_engram_policy result;result.principal=principal;result.group=group;
    auto& source=reverse?*pair.recipient_owner():*pair.source_owner();
    auto& recipient=reverse?*pair.source_owner():*pair.recipient_owner();
    const auto& sp=reverse?pair.recipient_profile():pair.source_profile();
    const auto& rp=reverse?pair.source_profile():pair.recipient_profile();
    auto& route=result.route;
    route.generation=pair.generation();route.source_member=reverse?pair.recipient_member():pair.source_member();
    route.recipient_member=reverse?pair.source_member():pair.recipient_member();route.policy_revision=1;
    route.models=direction==managed_engram_direction::group_to_hub?std::vector<std::string>{"Memory"}:
        group_route?std::vector<std::string>{"Edge","Memory"}:std::vector<std::string>{"Edge","Memory","SyncConfig"};
    route.channel=group_route?"ipc:engram-group-"+managed_group_spelling(group):"ipc:engram-sync";
    route.narrowing_emits_removals=!group_route;
    result.full_source_partition=direction==managed_engram_direction::personal_to_hub;
    const auto capture=[&](lattice_db& owner,const ordinary_managed_profile& profile,bool source_side){
        if(std::chrono::steady_clock::now()>=end)refuse("managed product original deadline expired");
        const auto settled=recovery_writer_access::ordinary_owned_write(owner,[&](database& writer){
            profile.validate(owner,writer);budget retained{managed_policy_limits};
            ordinary_export_frame observed;observed.wire_profile=true;
            const auto declared=describe_ordinary_projection(owner.recovery_schemas_,route.models);
            managed_product_columns(declared,"Memory",{{"isPrivate",column_type::integer},{"project",column_type::text},{"authorUserId",column_type::text}});
            if(direction!=managed_engram_direction::group_to_hub)
                managed_product_columns(declared,"Edge",{{"sourceGlobalId",column_type::text},{"targetGlobalId",column_type::text}});
            capture_projected_tables(ordinary_export_storage_access::borrow(writer),owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
                route.models,observed,retained,false,nullptr,nullptr);
            auto& tables=source_side?result.source_tables:result.recipient_tables;
            for(const auto& table:observed.tables){retained.add(table.name.size()+sizeof(std::string));tables.push_back(table.name);}
            (source_side?result.source_schema:result.recipient_schema)=observed.schema;
            if(source_side){
                if(direction==managed_engram_direction::personal_to_hub)route.filter=std::nullopt;
                else if(direction==managed_engram_direction::group_to_hub)
                    route.filter=std::vector<sync_filter_entry>{{"Memory","\"authorUserId\" = "+managed_sql_literal(principal,retained)}};
                else {
                    // SyncConfig is a policy dependency even though group
                    // payload scope deliberately excludes this personal table.
                    ordinary_export_frame policy_schema;policy_schema.wire_profile=true;
                    const auto policy_declared=describe_ordinary_projection(owner.recovery_schemas_,{"SyncConfig"});
                    managed_product_columns(policy_declared,"SyncConfig",{{"project",column_type::text},{"policy",column_type::text},{"exposedTeams",column_type::text}});
                    capture_projected_tables(ordinary_export_storage_access::borrow(writer),owner.recovery_schemas_,[](auto& out,const auto& property){lattice_db::serialize_property_for_fingerprint(out,property);},
                        {"SyncConfig"},policy_schema,retained,false,nullptr,nullptr);
                    const auto policies=managed_policy_rows(writer,retained);
                    result.policy_seal=managed_policy_seal(policies,retained);
                    const auto predicate=managed_memory_predicate(policies,group_route,group,retained);
                    const auto sub="SELECT \"globalId\" FROM \"Memory\" WHERE "+predicate;
                    route.filter=std::vector<sync_filter_entry>{{"Memory",predicate},{"Edge","(\"sourceGlobalId\" IN ("+sub+") AND \"targetGlobalId\" IN ("+sub+"))"}};
                    if(!group_route)route.filter->push_back({"SyncConfig",std::nullopt});
                    if(managed_policy_rows(writer,retained)!=policies)refuse("managed product policy changed in owning view");
                }
            }
            profile.validate(owner,writer);
            if(std::chrono::steady_clock::now()>=end)refuse("managed policy original deadline expired");
        });managed_clean_turn(settled);
    };
    // Separate actual owned turns, never one owner's SQL under the other's leaf.
    capture(source,*sp,true);capture(recipient,*rp,false);
    if(result.source_schema!=result.recipient_schema||result.source_tables!=result.recipient_tables)
        refuse("managed product declared source/recipient projection differs");
    // Current Engram Memory/Edge/SyncConfig declarations are scalar roots.
    // Their derived FTS/vector objects are metadata-validated but never sent
    // as logical rows. Future real link/union/geo declarations require an
    // explicit parent-eligibility builder, not guessed underscore matching.
    for(const auto& table:result.source_tables)if(std::find(route.models.begin(),route.models.end(),table)==route.models.end())
        refuse("managed product generated logical table requires closed eligibility program");
    pair.validate(end);return result;
}

void ordinary_export_access::validate_managed_product_policy(const ordinary_managed_ipc_local_route& route,
    lattice_db& owner,database& writer) {
    route.validate_lifetime();const auto& actual=route.product_policy();
    // The policy decision linearizes in its real source-owned turn. A later
    // recipient turn consumes that issued decision, not a fresh hub row or a
    // caller filter. Explicit session revocation is rechecked at publication.
    if(&owner==&route.source_owner()&&!actual.policy_seal.empty()){
        budget retained{managed_policy_limits};
        if(managed_policy_seal(managed_policy_rows(writer,retained),retained)!=actual.policy_seal)
            refuse("managed product policy changed; fresh route required");
    }
}
