// Included after every actual target native phase is complete. This is a
// private evidence prerequisite, not deletion, admission or cleanup success.
// Checked SQLite close never substitutes for the separate VM/call/UDF facts.
namespace lattice {
bool database::target_staging_native_cleanup_known_(
    const detail::ordinary_target_staging_connection& connection)const noexcept {
    try {
        const auto state=target_staging_;
        // Once outer-root extraction is attempted, this LIVE/absence-tolerant
        // observer is permanently unavailable. Use the distinct first history.
        if(!state||state->terminal_native_outer_attempted||!state->current_creator()||state->connection.lock().get()!=&connection||
           !state->initialization_attempted||!state->native_open_attempted||!state->native_open_returned||
           !state->retirement_requested||!state->cleanup_attempted||!state->cleanup_admission_returned||
           !state->cleanup_admitted||!state->cleanup_scope||!state->close_returned||
           state->first_close_result!=SQLITE_OK||!state->cleanup_scope->checked_native_close_known(*this)||
           !retirement_||retirement_->owning_wrapper_moved()||retirement_->raw_handle_escaped())return false;
        const auto calls_known=[](const auto& calls,std::size_t count)noexcept {
            if(count>calls.size())return false;
            for(std::size_t at=0;at<count;++at)
                if(calls[at].entered&&!calls[at].returned)return false;
            return true;
        };
        const auto vm_known=[](const auto& statement,bool destruction_known)noexcept {
            if(statement.prepare_entered&&!statement.prepare_returned)return false;
            if(!statement.prepare_entered&&(statement.prepare_returned||statement.prepared_identity))return false;
            if(statement.held)return false;
            if(statement.prepared_identity)
                return statement.prepare_entered&&statement.prepare_returned&&statement.finalize_entered&&
                    statement.finalize_returned&&destruction_known&&(statement.finalize_rc&0xff)!=SQLITE_MISUSE&&
                    statement.finalized_identity==statement.prepared_identity;
            return !statement.finalize_entered&&!statement.finalized_identity;
        };
        const auto transaction_known=[](const auto& begin,const auto& commit,const auto& rollback,
            bool autocommit_returned,int autocommit)noexcept {
            return !begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||
                (commit.terminal_returned&&commit.terminal_rc==SQLITE_DONE)||
                (rollback.terminal_returned&&rollback.terminal_rc==SQLITE_DONE)||
                (autocommit_returned&&autocommit!=0);
        };
        // These are real returned constructor operations. A failed native
        // result remains failed; known return is not configured/work credit.
        if(state->native_open_attempted&&!state->native_open_returned)return false;
        if(state->configuration_attempts>state->configuration_calls.size())return false;
        for(std::size_t at=0;at<state->configuration_attempts;++at) {
            const auto& call=state->configuration_calls[at];
            if(!call.attempted||!call.returned)return false;
        }
        if(!calls_known(state->schema_calls,state->schema_call_count))return false;
        for(const auto& statement:state->schema_statements) {
            if((statement.prepare_attempted&&!statement.prepare_returned)||
               (statement.step_attempted&&!statement.step_returned)||statement.held)return false;
            if(!statement.prepare_attempted&&(statement.prepare_returned||statement.prepared_identity))return false;
            if(statement.prepared_identity) {
                if(!statement.prepare_attempted||!statement.prepare_returned||!statement.finalize_attempted||
                   !statement.finalize_returned||!statement.finalize_destruction_known||
                   (statement.finalize_rc&0xff)==SQLITE_MISUSE||
                   statement.finalized_identity!=statement.prepared_identity)return false;
            }else if(statement.finalize_attempted||statement.finalized_identity)return false;
        }
        if(state->ending_autocommit_attempted&&!state->ending_autocommit_returned)return false;
        const auto& schema_begin=state->schema_statements[0];
        const auto& schema_commit=state->schema_statements[8];
        const auto& schema_rollback=state->schema_statements[9];
        if(schema_begin.step_returned&&schema_begin.step_rc==SQLITE_DONE&&
           !(schema_commit.step_returned&&schema_commit.step_rc==SQLITE_DONE)&&
           !(schema_rollback.step_returned&&schema_rollback.step_rc==SQLITE_DONE)&&
           !(state->ending_autocommit_returned&&state->ending_autocommit!=0))return false;
        if(state->udf_registration_attempted&&(!state->udf_registration_returned||
           !state->udf_destroy_entered||!state->udf_destroy_returned))return false;
        if(state->userdata_original&&(!state->udf_destroy_entered||!state->udf_destroy_returned))return false;

        const auto catalog=state->catalog;
        if(catalog) {
            if(catalog->shell!=this||!calls_known(catalog->calls,catalog->call_count)||
               (catalog->module_entered&&!catalog->module_returned)||
               (catalog->ending_autocommit_entered&&!catalog->ending_autocommit_returned)||
               catalog->commit_index>=catalog->statements.size()||catalog->rollback_index>=catalog->statements.size())return false;
            for(const auto& statement:catalog->statements)
                if(!vm_known(statement,statement.finalize_destruction_known))return false;
            if(!transaction_known(catalog->statements[0],catalog->statements[catalog->commit_index],
               catalog->statements[catalog->rollback_index],catalog->ending_autocommit_returned,
               catalog->first_ending_autocommit))return false;
        }
        const auto header=state->history_header;
        if(header) {
            if(header->shell!=this||!calls_known(header->calls,header->call_count)||
               (header->guard_registration_entered&&!header->guard_registration_returned)||
               (header->userdata&&(!header->guard_destroy_entered||!header->guard_destroy_returned))||
               (header->ending_autocommit_entered&&!header->ending_autocommit_returned))return false;
            for(const auto& statement:header->statements)
                if(!vm_known(statement,statement.destruction_known))return false;
            if(!transaction_known(header->statements[header->begin],header->statements[header->commit],
               header->statements[header->rollback],header->ending_autocommit_returned,
               header->first_ending_autocommit))return false;
        }
        // Successful page replacement drops its prior original only after
        // actual guard/VM endings. Failure retains at most this current page
        // plus one predecessor; refuse a longer chain instead of traversing it.
        auto page=state->history_page;
        for(std::size_t retained=0;page;++retained,page=page->previous) {
            if(retained==2||page->shell!=this||!calls_known(page->calls,page->call_count)||
               (page->guard_registration_entered&&!page->guard_registration_returned)||
               (page->userdata&&(!page->guard_destroy_entered||!page->guard_destroy_returned))||
               (page->ending_autocommit_entered&&!page->ending_autocommit_returned)||
               (page->raw_append&&!page->raw_append->cleanup_known()))return false;
            for(const auto& statement:page->statements)
                if(!vm_known(statement,statement.destruction_known))return false;
            if(!transaction_known(page->statements[page->begin],page->statements[page->commit],
               page->statements[page->rollback],page->ending_autocommit_returned,
               page->first_ending_autocommit))return false;
        }
        const auto projection=state->history_projection;
        if(projection) {
            if(projection->shell!=this||!calls_known(projection->calls,projection->call_count)||
               !projection->cleanup_known())return false;
            for(const auto& statement:projection->statements)
                if(!vm_known(statement,statement.destruction_known))return false;
        }
        const auto chain=state->history_chain;
        if(chain) {
            if(chain->shell!=this)return false;
            const auto phase=chain->active;
            if(phase) {
                if(phase->shell!=this||!calls_known(phase->calls,phase->call_count)||
                   (phase->decoder_retirement_entered&&!phase->decoder_retirement_returned)||
                   !phase->cleanup_known())return false;
                for(const auto& statement:phase->statements)
                    if(!vm_known(statement,statement.destruction_known))return false;
            }
        }
        const auto suspension=state->import_suspension;
        if(suspension) {
            if(suspension->shell!=this||!calls_known(suspension->calls,suspension->call_count)||
               suspension->begin>=suspension->statements.size()||suspension->commit>=suspension->statements.size()||
               suspension->rollback>=suspension->statements.size()||!suspension->cleanup_known())return false;
            for(const auto& statement:suspension->statements)
                if(!vm_known(statement,statement.destruction_known))return false;
        }
        const auto rows=state->row_import;
        if(rows) {
            if(rows->shell!=this)return false;
            const auto phase=rows->active;
            if(phase) {
                if(phase->shell!=this||!calls_known(phase->calls,phase->call_count)||
                   (phase->decoder_retirement_entered&&!phase->decoder_retirement_returned)||
                   !phase->cleanup_known())return false;
                for(const auto& statement:phase->statements)
                    if(!vm_known(statement,statement.destruction_known))return false;
            }
        }
        return target_history_guard_destroyed_()&&target_row_import_cleanup_ending_()&&
            target_import_suspension_cleanup_ending_()&&target_history_chain_cleanup_ending_()&&
            target_history_projection_cleanup_ending_();
        // Actual native/VM/UDF/decode endings only. No first failure, sticky
        // uncertainty, payload, lease, native-file or historical scope is reset.
    }catch(...){return false;}
}
} // namespace lattice
