// Included before the actual page definition. Only fixed Native result DATA,
// identities and <=64 digest bytes survive; no argument/call/payload graph does.
#include <algorithm>
#include <tuple>
namespace lattice {
struct database::target_staging_state::terminal_native_page_record {
    struct vm_fact {
        const void* original=nullptr;
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared=nullptr;sqlite3_stmt* finalized=nullptr;
        bool prepare_entered=false,prepare_returned=false,terminal_returned=false,first_step_returned=false;
        bool finalize_entered=false,finalize_returned=false,destruction_known=false;
        int prepare_rc=SQLITE_MISUSE,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot,kind;
    const std::uint64_t serial,generation,page_index;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const void* const cleanup_scope;
    const void* const page_original;
    const void* const previous_original;
    const void* const tail_original;
    const void* const scope_wrapper_original;
    const void* const append_original;
    const void* const calls_original;
    const void* const statements_original;
    const void* const stage_original;
    const void* const meta_original;
    const void* const progress_original;
    const void* const hash_original;
    const void* const userdata_original;
    const std::exception_ptr first_native_failure;
    const detail::ordinary_target_staging_connection::database_scope::terminal_page_scope_history scope_history;
    const bool constructor_returned,calls_returned,append_cleanup_known;
    const bool ending,left,completed,previous_destroy_joined,progress_complete;
    const bool guard_registration_entered,guard_registration_returned,guard_destroy_entered,guard_destroy_returned;
    const int first_guard_registration_rc;
    const bool ending_autocommit_entered,ending_autocommit_returned;
    const int first_ending_autocommit;
    const std::size_t call_count,completed_operations,guard_invocations,logical_bytes,active_statement;
    const std::size_t stage_cells,meta_cells,keys_count,previous_size;
    const std::uint64_t durable_bytes,next_page,rows,known,unknown,raw_offset,raw_length;
    const std::int64_t last_table;
    const std::array<vm_fact,144> statements;
    std::array<char,64> previous_digest{};
    bool binding_attempted=false,binding_returned=false;
    bool extraction_attempted=false,extraction_returned=false;
    bool release_attempted=false,release_returned=false;
    std::atomic<bool> members_destroyed{false},partial_constructor_destroyed{false};
    std::thread::id ending_thread;
    template<typename Original>
    terminal_native_page_record(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup,std::uint64_t gen,std::size_t selected,const Original& input)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),kind(selected),
         serial(actual->serial),generation(gen),page_index(input.terminal_page.page_index),state(actual),owner(input.owner),
         shell(&actual_shell),connection(&conn),cleanup_scope(cleanup),page_original(&input),previous_original(input.previous.get()),
         tail_original(&input.terminal_page),scope_wrapper_original(input.scope.get()),append_original(input.raw_append.get()),
         calls_original(&input.calls),statements_original(&input.statements),stage_original(&input.stage),meta_original(&input.meta),
         progress_original(&input.progress),hash_original(&input.content),userdata_original(input.userdata),
         first_native_failure(input.first_failure),
         scope_history(input.scope?input.scope->terminal_page_detached_history(actual_shell,conn,receiving,cleanup,gen):
             detail::ordinary_target_staging_connection::database_scope::terminal_page_scope_history{}),
         constructor_returned(input.terminal_page.construction_returned),
         calls_returned([&] {if(input.call_count>input.calls.size())return false;
             for(std::size_t at=0;at<input.call_count;++at)if(input.calls[at].entered&&!input.calls[at].returned)return false;
             return true;}()),
         append_cleanup_known(!input.raw_append||input.raw_append->cleanup_known()),
         ending(input.ending),left(input.left),completed(input.completed),previous_destroy_joined(input.previous_destroy_joined),
         progress_complete(input.progress.complete),guard_registration_entered(input.guard_registration_entered),
         guard_registration_returned(input.guard_registration_returned),guard_destroy_entered(input.guard_destroy_entered),
         guard_destroy_returned(input.guard_destroy_returned),first_guard_registration_rc(input.first_guard_registration_rc),
         ending_autocommit_entered(input.ending_autocommit_entered),ending_autocommit_returned(input.ending_autocommit_returned),
         first_ending_autocommit(input.first_ending_autocommit),call_count(input.call_count),completed_operations(input.completed_operations),
         guard_invocations(input.guard_invocations),logical_bytes(input.logical_bytes),active_statement(input.active_statement),
         stage_cells(input.stage.size()),meta_cells(input.meta.size()),keys_count(input.keys.size()),previous_size(input.progress.previous.size()),
         durable_bytes(input.durable_bytes),next_page(input.progress.next_page),rows(input.progress.rows),known(input.progress.known),
         unknown(input.progress.unknown),raw_offset(input.raw_span.first),raw_length(input.raw_span.second),last_table(input.progress.last_table),
         statements([&] {std::array<vm_fact,144> copy{};
             static_assert(std::tuple_size<decltype(input.statements)>::value==144);
             for(std::size_t at=0;at<copy.size();++at) {
                 const auto& vm=input.statements[at];copy[at]={&vm,vm.held,vm.prepared_identity,vm.finalized_identity,
                     vm.prepare_entered,vm.prepare_returned,vm.terminal_returned,vm.first_step_returned,
                     vm.finalize_entered,vm.finalize_returned,vm.destruction_known,vm.prepare_rc,vm.first_step_rc,vm.terminal_rc,vm.finalize_rc};
             }return copy;}()) {
        if(previous_size>previous_digest.size())throw db_error("terminal page fixed progress history bound");
        std::copy(input.progress.previous.begin(),input.progress.previous.end(),previous_digest.begin());
    }
    bool native_returns_known()const noexcept {
        if(!constructor_returned||!calls_returned||!append_cleanup_known||call_count>8448||
           previous_size>previous_digest.size()||(guard_registration_entered&&!guard_registration_returned)||
           (userdata_original&&(!guard_destroy_entered||!guard_destroy_returned))||
           (ending_autocommit_entered&&!ending_autocommit_returned))return false;
        if(scope_wrapper_original) {
            if(!scope_history.detached_known||!scope_history.scope_original||!scope_history.retirement_original||
               scope_history.active_callback_slot!=64||scope_history.active_callback_generation||
               scope_history.settling_callback_slot!=64||scope_history.settling_callback_generation)return false;
            if(scope_history.published) {
                if(!scope_history.left||!scope_history.generation||scope_history.generation>=generation||
                   !scope_history.ledger_original)return false;
            }else if(scope_history.generation||scope_history.ledger_original||scope_history.environment_original||
                scope_history.left||scope_history.leave_returned||scope_history.leave_joined)return false;
        }else if(scope_history.detached_known||scope_history.scope_original||scope_history.retirement_original||
            scope_history.ledger_original||scope_history.environment_original||scope_history.generation)return false;
        // A page published before its scope allocation can be a real failed
        // object. No scope must imply no issued Native call or userdata.
        if(!scope_wrapper_original&&(call_count||guard_registration_entered||userdata_original||ending_autocommit_entered))return false;
        for(const auto& vm:statements) {
            if(!vm.original||vm.held||(vm.prepare_entered&&!vm.prepare_returned)||
               (!vm.prepare_entered&&(vm.prepare_returned||vm.prepared)))return false;
            if(vm.prepared) {
                if(!vm.prepare_entered||!vm.prepare_returned||!vm.finalize_entered||!vm.finalize_returned||
                   !vm.destruction_known||(vm.finalize_rc&0xff)==SQLITE_MISUSE||vm.finalized!=vm.prepared)return false;
            }else if(vm.finalize_entered||vm.finalized)return false;
        }
        const auto& begin=statements[0];const auto& commit=statements[138];const auto& rollback=statements[139];
        return !begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||
            (commit.terminal_returned&&commit.terminal_rc==SQLITE_DONE)||
            (rollback.terminal_returned&&rollback.terminal_rc==SQLITE_DONE)||
            (ending_autocommit_returned&&first_ending_autocommit!=0);
        // Known failed results remain exact failed DATA, never work credit.
    }
};
struct database::target_staging_state::terminal_native_page_tail {
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial,page_index;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const void* const page_original;
    bool construction_returned=false,binding_attempted=false;
    std::shared_ptr<terminal_native_page_record> first;
    terminal_native_page_tail(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const void* original,std::uint64_t index)
        :creator(actual.target_staging_->creator),thread(actual.target_staging_->thread),end(actual.target_staging_->end),
         slot(actual.target_staging_->slot),serial(actual.target_staging_->serial),page_index(index),state(actual.target_staging_),
         owner(receiving),shell(&actual),page_original(original) {
        if(!actual.target_staging_->current_creator()||!receiving||!original)
            throw db_error("actual Native page constructor identity unavailable");
    }
    terminal_native_page_tail(const terminal_native_page_tail&)=delete;
    terminal_native_page_tail& operator=(const terminal_native_page_tail&)=delete;
    void constructor_returned()noexcept {
        if(creator==detail::database_native_process()&&thread==std::this_thread::get_id())construction_returned=true;
    }
    bool bind(const std::shared_ptr<terminal_native_page_record>& issued)noexcept {
        if(creator!=detail::database_native_process()||thread!=std::this_thread::get_id()||binding_attempted||first)return false;
        binding_attempted=true;
        if(!construction_returned||!issued||issued->creator!=creator||issued->thread!=thread||issued->end!=end||
           issued->slot!=slot||issued->serial!=serial||issued->page_index!=page_index||issued->shell!=shell||
           issued->page_original!=page_original||issued->tail_original!=this||issued->state.lock()!=state.lock()||
           issued->owner.lock()!=owner.lock()||!issued->native_returns_known())return false;
        issued->binding_attempted=true;first=issued;issued->binding_returned=true;return true;
    }
    ~terminal_native_page_tail()noexcept {
        if(creator!=detail::database_native_process()||!first)return;
        first->ending_thread=std::this_thread::get_id();
        if(construction_returned&&first->binding_returned)first->members_destroyed.store(true,std::memory_order_release);
        else first->partial_constructor_destroyed.store(true,std::memory_order_release);
        database::wake_target_terminal_native_page_(state,owner,first->connection);
        // Actual scope wrapper, previous link, RAW append reference, SQL/args,
        // key vectors, progress/hash, calls/VM bank have all died before this.
    }
};
} // namespace lattice
