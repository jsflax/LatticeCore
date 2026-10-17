// Actual Native wrapper-cell producer; no public factory or ended Native read.
namespace lattice {
bool database::target_terminal_scope_root_context_snapshot_(const detail::ordinary_target_staging_connection& connection,
    detail::ordinary_target_scope_native_fact& out)const noexcept {
    try {
        const auto& state=target_staging_;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||!retirement_||
           retirement_->owning_wrapper_moved()||retirement_->raw_handle_escaped()||!state->retirement_requested||
           !state->close_returned||state->first_close_result!=SQLITE_OK||
           !state->terminal_scope_ending_attempted||!state->terminal_scope_ending_returned||
           state->terminal_scope_result!=detail::ordinary_target_staging_connection::terminal_scope_retirement_result::detached||
           !state->terminal_native_statics_attempted||!state->terminal_native_statics_allocation_returned||
           state->terminal_native_statics_allocation_exception_returned||state->terminal_native_statics_first_failure||
           !state->terminal_native_statics_original||!state->terminal_native_pages_original||!state->terminal_native_outer_original||
           !state->terminal_native_phase_original||!state->terminal_decoder_original)return false;
        out.state=state.get();out.retirement=retirement_.get();out.slot=state->slot;out.serial=state->serial;
        out.flags=detail::terminal_scope_fact_flags({state->initialization_attempted,state->native_open_attempted,state->native_open_returned,
            state->configured,state->retirement_requested,state->close_returned,state->fallback_returned,state->udf_registration_attempted,
            state->udf_registration_returned,state->udf_destroy_entered,state->udf_destroy_returned});
        out.open_rc=state->first_open_result;out.close_rc=state->first_close_result;out.fallback_rc=state->first_fallback_result;
        out.udf_rc=state->first_udf_registration_result;out.first_failure=state->first_failure;
        out.histories={state->terminal_decoder_original.get(),state->terminal_native_phase_original.get(),state->terminal_native_outer_original.get(),
            state->terminal_native_pages_original.get(),state->terminal_native_statics_original.get()};
        out.retained_bytes=sizeof(target_staging_state)+sizeof(target_staging_state::terminal_decoder_ending)+
            2*(sizeof(target_staging_state::terminal_decoder_ending::capture_record)+64)+
            sizeof(target_staging_state::terminal_native_phase_ending)+2*(sizeof(target_staging_state::terminal_native_phase_record)+64)+
            sizeof(target_staging_state::terminal_native_outer_ending)+2*(sizeof(target_staging_state::terminal_native_outer_record)+64)+
            sizeof(target_staging_state::terminal_native_page_ending)+2*(sizeof(target_staging_state::terminal_native_page_record)+64)+
            sizeof(target_staging_state::terminal_native_static_ending)+4*(sizeof(target_staging_state::terminal_native_static_record)+64);
        for(const auto& record:state->terminal_native_statics_original->records)if(record) {
            if(record->kind>=4||record->statements.capacity()>record->statement_slots)return false;
            const auto bytes=record->statements.capacity()*sizeof(target_staging_state::terminal_native_static_record::vm_fact);
            if(bytes>ordinary_history_limits::owner_bytes||out.retained_bytes>ordinary_history_limits::owner_bytes-bytes)return false;
            out.retained_bytes+=bytes;
        }
        return true; // Reads only retained state/first DATA, never an ended Native payload.
    }catch(...){return false;}
}
bool database::target_terminal_scope_root_context_matches_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_target_scope_native_fact& first)const noexcept {
    detail::ordinary_target_scope_native_fact actual;
    return target_terminal_scope_root_context_snapshot_(connection,actual)&&actual.state==first.state&&actual.retirement==first.retirement&&
        actual.slot==first.slot&&actual.serial==first.serial&&actual.flags==first.flags&&actual.open_rc==first.open_rc&&
        actual.close_rc==first.close_rc&&actual.fallback_rc==first.fallback_rc&&actual.udf_rc==first.udf_rc&&
        actual.first_failure==first.first_failure&&actual.histories==first.histories&&actual.retained_bytes==first.retained_bytes;
}
bool database::retire_target_terminal_scope_roots_(detail::ordinary_target_staging_connection& connection,
    detail::ordinary_group_target_receiving_owner& owner)noexcept {
    const auto& state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return false;
    detail::ordinary_target_scope_native_fact first;
    if(!target_terminal_scope_root_context_snapshot_(connection,first))return false;
    return connection.retire_terminal_scope_roots_from_native(owner,first,
        &state->constructor_scope,&state->schema_scope,&state->cleanup_scope);
}
}

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
namespace lattice {
detail::ordinary_target_scope_root_observation database::qualification_target_scope_root_observation_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto& state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return {};
    return connection.qualification_scope_root_observation_from_native(owner,
        &state->constructor_scope,&state->schema_scope,&state->cleanup_scope);
}
}
#endif
