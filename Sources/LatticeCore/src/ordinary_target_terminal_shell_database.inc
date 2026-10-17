// Separate after-scope-root shell/member ending. Called only before extraction;
// after the actual shell reset, the caller consumes bound first tails and the
// actual owned-connection deleter/ledger return, never this live observer again.
namespace lattice {
database::terminal_database_shell_tail::terminal_database_shell_tail(const database* original)noexcept
    :process_(detail::database_native_process()),thread_(std::this_thread::get_id()),original_(original){}
database::terminal_database_shell_tail::~terminal_database_shell_tail()noexcept {
    if(process_==detail::database_native_process()&&observer_close_proved_&&observer_delivery_)
        observer_delivery_->owner_storage_ended();
    if(process_!=detail::database_native_process()||thread_!=std::this_thread::get_id()||!first_||
       first_->original!=original_||first_->kind!=8)return;
    first_->partial_constructor=!construction_returned_;first_->members_destroyed=true;
}
bool database::terminal_database_shell_tail::bind(const std::shared_ptr<detail::ordinary_target_scope_root_record>& first)noexcept {
    if(process_!=detail::database_native_process()||thread_!=std::this_thread::get_id()||!construction_returned_||first_||!first||
       first->original!=original_||first->kind!=8||!first->constructor_returned||first->binding_attempted)return false;
    first->binding_attempted=true;first_=first;first->binding_returned=true;return true;
}
bool database::bind_target_terminal_shell_destruction_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_target_scope_native_fact& first,
    const std::shared_ptr<detail::ordinary_target_scope_root_record>& native_record,
    const std::shared_ptr<detail::ordinary_target_scope_root_record>& shell_record,
    std::shared_ptr<detail::database_open_ledger>& ledger)noexcept {
    try {
        const auto& state=target_staging_;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||
           state.use_count()!=1||db_||!retirement_||retirement_->state()!=detail::database_retirement_state::phase::closed||
           retirement_->checked_close_result()!=SQLITE_OK||retirement_->raw_handle_escaped()||retirement_->owning_wrapper_moved()||
           !state->ledger||ledger||!native_record||!shell_record||
           native_record->original!=state.get()||native_record->kind!=7||!native_record->constructor_returned||
           shell_record->original!=this||shell_record->kind!=8||!shell_record->constructor_returned||
           !target_terminal_scope_root_context_matches_(connection,first))return false;
        const auto& native_tail=state->terminal_receiving_tail_;
        const auto& shell_tail=target_terminal_shell_tail_;
        if(native_tail.process_!=detail::ordinary_launch::current_process()||native_tail.thread_!=std::this_thread::get_id()||
           native_tail.original_!=state.get()||native_tail.kind_!=7||!native_tail.construction_returned_||native_tail.first_||
           shell_tail.process_!=detail::database_native_process()||shell_tail.thread_!=std::this_thread::get_id()||
           shell_tail.original_!=this||!shell_tail.construction_returned_||shell_tail.first_)return false;
        if(!state->ledger->seal_fixed_scope())return false;
        ledger=state->ledger; // Exact actual ledger held before binding or wrapper extraction.
        if(!state->terminal_receiving_tail_.bind(native_record))return false;
        return target_terminal_shell_tail_.bind(shell_record);
    }catch(...){return false;}
}
}
