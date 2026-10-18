// Included only in the actual request/schema producer namespace. The real
// Native import writer must consume this exact programme under its own role.
const std::shared_ptr<ordinary_group_target_schema_original::import_suspension_program>&
ordinary_group_target_schema_original::prepare_import_suspension(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    ordinary_export_access::prepare_receiver_group_target_import_suspension(*this,connection,end);return require_import_suspension(connection,end);
}
const std::shared_ptr<ordinary_group_target_schema_original::import_suspension_program>&
ordinary_group_target_schema_original::require_import_suspension(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    require_connection(connection,end);const auto out=state_->import_suspension;
    if(!state_->import_suspension_attempted||!out||out->original.lock().get()!=this||!out->generation_entered||
       !out->generation_returned||out->first_failure||out->projection!=state_->projection)
        throw std::runtime_error("GROUP import suspension actual programme unavailable");
    return state_->import_suspension;
}
void ordinary_export_access::prepare_receiver_group_target_import_suspension(
    const ordinary_group_target_schema_original& original,const ordinary_target_staging_connection& connection,ordinary_launch::deadline end) {
    creator_check();
    if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("startup cannot enter generic GROUP import suspension producer");
    original.require_connection(connection,end);const auto held=original.state_;const auto request=held->request.lock();
    require(request&&request->state_->target_schema.get()==&original,"GROUP import suspension actual request/schema retired");
    const auto receiving=std::atomic_load(&request->state_->group_receiver);
    require(receiving&&request->state_->staging_connection.get()==&connection,"GROUP import suspension actual receiving owner differs");
    connection.require_history_chain_ending(*receiving,end); // Actual same-owner native pass, not header/hash DATA.
    const auto projection=original.require_projection(connection,end);
    require(projection->validation_returned&&!projection->first_failure&&projection->metadata&&projection->internal,
        "GROUP import suspension actual physical projection ending missing");
    require(!held->import_suspension_attempted&&!held->import_suspension,"GROUP import suspension already attempted");
    held->import_suspension_attempted=true;
    try {
        held->import_suspension=std::make_shared<ordinary_group_target_schema_original::import_suspension_program>(request->state_->target_schema,projection);
        const auto out=held->import_suspension;original.require_connection(connection,end);out->generation_entered=true;
        using purpose=ordinary_group_target_schema_original::import_suspension_program::purpose;
        for(const auto& [name,table]:projection->plan->tables) {
            (void)table;
            out->append(purpose::require_empty,"SELECT COUNT(*) AS n FROM main."+ordinary_row_recipe::quote(name),name);
        }
        // Exact normal suspension order: all validated triggers, then explicit
        // generated unique indexes. PK/globalId autoindexes remain untouched.
        for(const auto& object:projection->metadata->objects)if(object.type=="trigger")
            out->append(purpose::drop_trigger,"DROP TRIGGER main."+ordinary_row_recipe::quote(object.name),object.name);
        for(const auto& index:projection->metadata->unique_indexes)
            out->append(purpose::drop_index,"DROP INDEX main."+ordinary_row_recipe::quote(index.name),index.name);
        for(const auto& object:projection->metadata->objects)if(object.type=="trigger")
            out->append(purpose::require_absent,"SELECT COUNT(*) AS n FROM main.sqlite_schema WHERE type='trigger' AND name=?",object.name);
        for(const auto& index:projection->metadata->unique_indexes)
            out->append(purpose::require_absent,"SELECT COUNT(*) AS n FROM main.sqlite_schema WHERE type='index' AND name=?",index.name);
        out->generation_returned=true;connection.require_history_chain_ending(*receiving,end);original.require_connection(connection,end);
        // Actual original metadata retains full restore SQL/identities. Nothing
        // is dropped until the genuine Native writer accepts this SAME input.
    }catch(...){if(held->import_suspension&&!held->import_suspension->first_failure)held->import_suspension->first_failure=std::current_exception();if(!held->first_failure)held->first_failure=std::current_exception();throw;}
}
