// First Native correspondence only. All actual Native objects are already
// ended; neither a programme nor an ended scope/VM address is dereferenced.
namespace lattice {
bool database::target_terminal_programme_originals_known_(
    const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& owner,
    const detail::ordinary_group_target_schema_original& schema,
    const std::array<const void*,4>& originals,
    const std::array<std::exception_ptr,4>& failures)const noexcept {
    return target_terminal_programme_history_known_(connection,owner,&schema,originals,failures);
}
bool database::target_terminal_programme_history_known_(
    const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& owner,const void* schema_original,
    const std::array<const void*,4>& originals,
    const std::array<std::exception_ptr,4>& failures)const noexcept {
    try {
        const auto state=target_staging_;
        if(!schema_original||!state||!state->current_creator()||state->connection.lock().get()!=&connection||
           !target_terminal_native_static_history_known_(connection,owner))return false;
        const auto& statics=state->terminal_native_statics_original;
        const auto& outer=state->terminal_native_outer_original;
        const auto& phases=state->terminal_native_phase_original;
        if(!statics||!outer||!phases)return false;
        const auto same=[&](std::size_t native,std::size_t programme) {
            const auto& first=statics->records[native];
            if(!first)return !originals[programme]&&!failures[programme]; // Unissued DATA only.
            return originals[programme]&&first->schema_original==schema_original&&
                first->programme_original==originals[programme]&&first->first_programme_failure==failures[programme];
        };
        if(!same(0,0)||!same(2,1)||!same(3,2))return false;
        const auto& row=outer->records[1];
        if(!row)return !originals[3]&&!failures[3];
        // A row programme is complete before its actual Native object can
        // consume it. A failed/unpublished generator remains separate debt.
        if(!originals[3]||failures[3]||row->programme_original!=originals[3])return false;
        const auto& phase=phases->records[1];
        return !phase||phase->programme_original==originals[3];
    }catch(...){return false;}
}
}
