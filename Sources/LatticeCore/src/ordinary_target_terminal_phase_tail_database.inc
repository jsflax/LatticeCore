// Complete staging state, before the two actual active phase definitions.
// Inline constructor/destructor observations allocate no normal-page receipt.
#include <atomic>
#include <array>
#include <memory>
#include <thread>
namespace lattice {
struct database::target_staging_state::terminal_native_phase_record {
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
    const void* const phase_original;
    const void* const tail_original;
    const void* const scope_wrapper_original;
    const void* const optional_original;
    const void* const raw_reader_original;
    const void* const programme_original;
    const std::exception_ptr phase_first_failure,outer_first_failure;
    const bool constructor_returned,decode_entered,decode_returned,decode_exception_returned;
    const bool ordinary_decoder_entered,ordinary_decoder_returned,phase_ending,phase_left;
    const bool snapshot_observed,ending_autocommit_returned;
    const int snapshot_state,ending_autocommit;
    const std::size_t native_call_count,statement_count;
    // Bounded scalar/pointer first VM history, not copied SQL/bindings/rows or
    // the large per-call/scratch arrays being destroyed with the actual phase.
    struct statement_history {
        const void* original=nullptr;
        const void* held=nullptr;
        const void* prepared=nullptr;
        const void* finalized=nullptr;
        bool prepare_entered=false,prepare_returned=false,readonly_returned=false,terminal_returned=false;
        bool finalize_entered=false,finalize_returned=false,destruction_known=false,first_step_returned=false;
        int prepare_rc=SQLITE_MISUSE,readonly_value=0,first_step_rc=SQLITE_MISUSE;
        int terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
        bool changes_returned=false,row_id_returned=false;
        int changes_value=0;
        sqlite3_int64 row_id_value=0;
    };
    std::array<statement_history,136> statements{};
    bool binding_attempted=false,binding_returned=false;
    bool extraction_attempted=false,extraction_returned=false;
    bool release_attempted=false,release_returned=false;
    std::atomic<bool> members_destroyed{false},partial_constructor_destroyed{false};
    std::thread::id ending_thread;
    template<typename Phase,typename Original>
    terminal_native_phase_record(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& actual_connection,const void* scope,std::uint64_t gen,
        std::size_t selected,const Phase& input,const Original& outer)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),kind(selected),
         serial(actual->serial),generation(gen),page_index(input.page_index),state(actual),owner(input.owner),
         shell(&actual_shell),connection(&actual_connection),cleanup_scope(scope),phase_original(&input),
         tail_original(&input.terminal_payload),scope_wrapper_original(input.scope.get()),optional_original(&input.decoded),
         raw_reader_original(input.raw_reader.get()),programme_original([&]() -> const void* {
             if constexpr(requires {input.row_program;})return input.row_program.get();
             else return input.program;
         }()),phase_first_failure(input.first_failure),outer_first_failure(outer.first_failure),
         constructor_returned(input.terminal_payload.construction_returned),decode_entered(input.decode_entered),
         decode_returned(input.decode_returned),decode_exception_returned(input.decode_exception_returned),
         ordinary_decoder_entered(input.decoder_retirement_entered),ordinary_decoder_returned(input.decoder_retirement_returned),
         phase_ending(input.ending),phase_left(input.left),snapshot_observed(input.snapshot_observed),
         ending_autocommit_returned(input.ending_autocommit_returned),snapshot_state(input.snapshot_state),
         ending_autocommit(input.ending_autocommit),native_call_count(input.call_count),statement_count(input.statements.size()) {
        if(statement_count>statements.size())throw db_error("terminal phase VM history exceeds actual fixed shape");
        for(std::size_t index=0;index<statement_count;++index) {
            const auto& source=input.statements[index];auto& saved=statements[index];
            saved.original=&source;saved.held=source.held;saved.prepared=source.prepared_identity;saved.finalized=source.finalized_identity;
            saved.prepare_entered=source.prepare_entered;saved.prepare_returned=source.prepare_returned;
            saved.readonly_returned=source.readonly_returned;saved.terminal_returned=source.terminal_returned;
            saved.finalize_entered=source.finalize_entered;saved.finalize_returned=source.finalize_returned;
            saved.destruction_known=source.destruction_known;saved.first_step_returned=source.first_step_returned;
            saved.prepare_rc=source.prepare_rc;saved.readonly_value=source.readonly_value;
            saved.first_step_rc=source.first_step_rc;saved.terminal_rc=source.terminal_rc;saved.finalize_rc=source.finalize_rc;
            if constexpr(requires {source.changes_returned;}) {
                saved.changes_returned=source.changes_returned;saved.row_id_returned=source.row_id_returned;
                saved.changes_value=source.changes_value;saved.row_id_value=source.row_id_value;
            }
        }
    }
};
struct database::target_staging_state::terminal_native_phase_tail {
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot,kind;
    const std::uint64_t serial;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const void* const phase_original;
    bool construction_returned=false,binding_attempted=false;
    std::shared_ptr<terminal_native_phase_record> first;
    terminal_native_phase_tail(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const void* original,std::size_t selected)
        :creator(actual.target_staging_->creator),thread(actual.target_staging_->thread),end(actual.target_staging_->end),
         slot(actual.target_staging_->slot),kind(selected),serial(actual.target_staging_->serial),
         state(actual.target_staging_),owner(receiving),shell(&actual),phase_original(original) {
        if(!actual.target_staging_->current_creator()||!receiving||!original||kind>1)
            throw db_error("actual Native phase constructor identity unavailable");
    }
    terminal_native_phase_tail(const terminal_native_phase_tail&)=delete;
    terminal_native_phase_tail& operator=(const terminal_native_phase_tail&)=delete;
    void constructor_returned()noexcept {
        if(creator==detail::database_native_process()&&thread==std::this_thread::get_id())
            construction_returned=true; // Called only at the real constructor's last operation.
    }
    bool bind(const std::shared_ptr<terminal_native_phase_record>& issued)noexcept {
        if(creator!=detail::database_native_process()||thread!=std::this_thread::get_id()||binding_attempted||first)
            return false;
        binding_attempted=true;
        if(!construction_returned||!issued||issued->creator!=creator||issued->thread!=thread||issued->end!=end||
           issued->slot!=slot||issued->serial!=serial||issued->kind!=kind||issued->shell!=shell||
           issued->phase_original!=phase_original||issued->tail_original!=this||issued->state.lock()!=state.lock()||
           issued->owner.lock()!=owner.lock()||!issued->constructor_returned)return false;
        issued->binding_attempted=true;first=issued;issued->binding_returned=true;
        return true;
    }
    ~terminal_native_phase_tail()noexcept {
        if(creator!=detail::database_native_process()||!first)return;
        first->ending_thread=std::this_thread::get_id();
        if(construction_returned&&first->binding_returned)
            first->members_destroyed.store(true,std::memory_order_release);
        else first->partial_constructor_destroyed.store(true,std::memory_order_release);
        // This first member runs AFTER every real phase member, including
        // scope wrapper/reader/programme references. It grants no shared-owner ending.
        database::wake_target_terminal_native_phase_(state,owner,first->connection);
    }
};
} // namespace lattice
