// Included only inside the genuine request/schema issuer. Pure declarations
// stay observational until the SAME target shell's actual Native read ending.
const std::shared_ptr<ordinary_group_target_schema_original::projection_program>&
ordinary_group_target_schema_original::prepare_projection(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    ordinary_export_access::prepare_receiver_group_target_projection(*this,connection,end);return require_projection(connection,end);
}
const std::shared_ptr<ordinary_group_target_schema_original::projection_program>&
ordinary_group_target_schema_original::require_projection(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    require_connection(connection,end);
    if(!state_->projection_preparation_attempted||!state_->projection||state_->projection->original.lock().get()!=this||
       !state_->projection->generation_entered||!state_->projection->generation_returned||state_->projection->first_failure||
       !state_->projection->plan||!state_->projection->logical)
        throw std::runtime_error("GROUP actual request-owned projection program unavailable");
    return state_->projection;
}
void ordinary_group_target_schema_original::validate_projection_read(const ordinary_target_staging_connection& connection,
    const ordinary_metadata_query& query,ordinary_launch::deadline end)const {
    ordinary_export_access::validate_receiver_group_target_projection(*this,connection,query,end);
}
void ordinary_export_access::prepare_receiver_group_target_projection(const ordinary_group_target_schema_original& original,
    const ordinary_target_staging_connection& connection,ordinary_launch::deadline end) {
    creator_check();original.require_connection(connection,end);const auto held=original.state_;
    require(!held->projection_preparation_attempted&&!held->projection,"GROUP actual projection original already attempted");
    const auto request=held->request.lock();require(request&&request->state_->target_schema.get()==&original,"GROUP projection actual schema request retired");
    held->projection_preparation_attempted=true;
    try {
        held->projection=std::make_shared<ordinary_group_target_schema_original::projection_program>(request->state_->target_schema);
        const auto out=held->projection; // SAME original holds output before post-current/diagnostics.
        original.require_connection(connection,end);out->generation_entered=true;
        out->plan=std::make_shared<ordinary_schema_projection>(describe_ordinary_projection(*held->declarations,{"Edge","GroupProjectMap","Memory"}));
        out->add(out->plan->logical_bytes);original.require_connection(connection,end);
        out->logical=std::make_shared<ordinary_export_frame>();out->logical->wire_profile=true;
        const auto serialize=[](std::ostringstream& stream,const property_descriptor& property){lattice_db::serialize_property_for_fingerprint(stream,property);};
        auto digest=ordinary_projection_recipe::begin(*held->declarations,*out->plan,serialize,*out->logical,*out);
        for(const auto& [name,table]:out->plan->tables)out->logical->tables.push_back(ordinary_projection_recipe::table(digest,name,table,*out));
        ordinary_projection_recipe::finish(digest,*out->plan,*out->logical,*out);out->generation_returned=true;
        original.require_connection(connection,end);
    }catch(...){if(held->projection&&!held->projection->first_failure)held->projection->first_failure=std::current_exception();if(!held->first_failure)held->first_failure=std::current_exception();throw;}
}
void ordinary_export_access::validate_receiver_group_target_projection(const ordinary_group_target_schema_original& original,
    const ordinary_target_staging_connection& connection,const ordinary_metadata_query& query,ordinary_launch::deadline end) {
    creator_check();const auto out=original.require_projection(connection,end);const auto held=original.state_;
    require(query&&!out->validation_entered&&!out->metadata&&!out->internal,"GROUP projection Native validation already attempted/unavailable");
    out->validation_entered=true;
    try {
        // Exact normal selected-module validation precedes internal validation.
        // The sole private DB caller holds a genuine SAME Native read transaction.
        out->metadata=std::make_shared<ordinary_schema_metadata>(validate_ordinary_schema(*out->plan,query));
        out->add(out->metadata->copied_bytes);original.require_connection(connection,end);
        out->internal=std::make_shared<ordinary_internal_schema_metadata>(validate_ordinary_internal_schema(*held->declarations,*out->plan,*out->metadata,query));
        out->add(out->internal->copied_bytes);out->validation_returned=true;original.require_connection(connection,end);
    }catch(...){if(!out->first_failure)out->first_failure=std::current_exception();if(!held->first_failure)held->first_failure=std::current_exception();throw;}
}
