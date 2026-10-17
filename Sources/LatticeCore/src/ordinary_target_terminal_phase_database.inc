// Complete actual phases and decoder first receipts. Only two active phase
// objects are selected; outer Native/programme/RAW/scope owners remain held.
#include "../include/lattice/native_retirement_storage.hpp"
namespace lattice {
struct database::target_staging_state::terminal_native_phase_ending {
    using chain_phase=history_chain_native::phase;
    using row_phase=row_import_native::phase;
    using custody=detail::native_retirement_storage<std::shared_ptr<chain_phase>,std::shared_ptr<row_phase>>;
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial,generation;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const detail::ordinary_group_target_receiving_owner* const owner;
    const void* const scope;
    const std::shared_ptr<terminal_decoder_ending> decoder_original; // Small first history, no phase payload.
    std::array<const void*,2> outer_originals{},prepared_phases{};
    std::array<bool,2> record_allocation_attempted{},record_allocation_returned{},record_allocation_exception_returned{};
    std::array<std::shared_ptr<terminal_native_phase_record>,2> records{};
    custody held; // Actual empty typed ownership allocated before binding/extraction.
    bool preparation_returned=false,extraction_attempted=false,extraction_returned=false,returned=false;
    terminal_native_phase_result result=terminal_native_phase_result::retained_debt;
    std::exception_ptr first_failure;
    terminal_native_phase_ending(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup_scope,std::uint64_t gen)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),serial(actual->serial),
         generation(gen),shell(&actual_shell),connection(&conn),owner(&receiving),scope(cleanup_scope),
         decoder_original(actual->terminal_decoder_original) {}
};
void database::wake_target_terminal_native_phase_(const std::weak_ptr<target_staging_state>& weak_state,
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner>& weak_owner,
    const detail::ordinary_target_staging_connection* connection_original)noexcept {
    try {
        const auto state=weak_state.lock();
        if(!state||!state->current_creator())return;
        const auto connection=state->connection.lock();const auto receiving=weak_owner.lock();
        if(connection&&connection.get()==connection_original&&receiving&&receiving->terminal_inputs_ended(*connection))
            receiving->schedule_collection(); // Actual post-member-tail wake, outside all leaves.
    }catch(...){}
}
database::terminal_native_phase_result database::retire_target_terminal_native_phases_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving)noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)
        return terminal_native_phase_result::retained_debt;
    if(state->terminal_native_phase_attempted) {
        const auto& first=state->terminal_native_phase_original;
        if(!first||!first->returned)return terminal_native_phase_result::retained_debt;
        if(first->result==terminal_native_phase_result::not_held)return first->result; // Unissued DATA only.
        return target_terminal_native_phases_ended_(connection,receiving)?terminal_native_phase_result::phases_ended:
            terminal_native_phase_result::retained_debt;
    }
    const void* scope=nullptr;std::uint64_t generation=0;
    // Observe the complete live Native/VM/UDF/transaction/RAW/provider proof
    // BEFORE either phase root dies. Never use its absence-tolerant form afterward.
    if(!connection.terminal_native_decoder_context_known(receiving,scope,generation)||!scope||!generation||
       !target_staging_native_cleanup_known_(connection))return terminal_native_phase_result::retained_debt;
    const auto& decoder=state->terminal_decoder_original;
    if(!state->terminal_decoder_attempted||!state->terminal_decoder_allocation_returned||
       state->terminal_decoder_allocation_exception_returned||state->terminal_decoder_first_failure||!decoder||
       decoder->first_failure||!decoder->returned||!decoder->preparation_returned||!decoder->extraction_returned||
       decoder->creator!=state->creator||decoder->thread!=state->thread||decoder->end!=state->end||
       decoder->slot!=state->slot||decoder->serial!=state->serial||decoder->shell!=this||
       decoder->connection!=&connection||decoder->owner!=&receiving||decoder->scope!=scope||decoder->generation!=generation)
        return terminal_native_phase_result::retained_debt;
    if(decoder->result==terminal_decoder_payload_result::captures_ended) {
        if(!target_terminal_decoder_payloads_ended_(connection,receiving))return terminal_native_phase_result::retained_debt;
    }else if(decoder->result==terminal_decoder_payload_result::not_held) {
        if(decoder->held_values||decoder->held_charge_references||decoder->records[0]||decoder->records[1])
            return terminal_native_phase_result::retained_debt; // No decoded grant from DATA.
    }else return terminal_native_phase_result::retained_debt;
    const auto eligible=[&](const auto& phase,std::size_t kind) {
        if(!phase)return true; // No issued phase DATA; no ending issued for it.
        const auto& tail=phase->terminal_payload;
        return phase.use_count()==1&&phase->shell==this&&phase->owner.lock().get()==&receiving&&
            tail.creator==state->creator&&tail.thread==state->thread&&tail.end==state->end&&tail.slot==state->slot&&
            tail.serial==state->serial&&tail.kind==kind&&tail.shell==this&&tail.phase_original==phase.get()&&
            tail.state.lock()==state&&tail.owner.lock().get()==&receiving&&tail.construction_returned&&
            !tail.binding_attempted&&!tail.first&&!phase->decoded&&!phase->decoder_charge&&phase->cleanup_known();
    };
    if((state->history_chain&&!eligible(state->history_chain->active,0))||
       (state->row_import&&!eligible(state->row_import->active,1)))return terminal_native_phase_result::retained_debt;
    state->terminal_native_phase_attempted=true; // First bank allocation, never reset or retried.
    try {
        using ending=target_staging_state::terminal_native_phase_ending;
        using record=target_staging_state::terminal_native_phase_record;
        auto first=std::make_shared<ending>(state,*this,connection,receiving,scope,generation);
        state->terminal_native_phase_original=first;state->terminal_native_phase_allocation_returned=true;
        const auto prepare=[&]<std::size_t Index>(const auto& outer) {
            first->outer_originals[Index]=outer.get();if(!outer||!outer->active)return;
            const auto& phase=outer->active;first->prepared_phases[Index]=phase.get();
            first->record_allocation_attempted[Index]=true;
            try {
                first->records[Index]=std::make_shared<record>(state,*this,connection,scope,generation,Index,*phase,*outer);
                first->record_allocation_returned[Index]=true;
            }catch(...) {first->record_allocation_exception_returned[Index]=true;throw;}
        };
        prepare.template operator()<0>(state->history_chain);prepare.template operator()<1>(state->row_import);
        first->preparation_returned=true; // BOTH actual records and empty typed custody exist before arming/moves.
        const auto bind=[&]<std::size_t Index>(const auto& outer) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!outer||!outer->active||outer->active.get()!=receipt->phase_original||
               !outer->active->terminal_payload.bind(receipt))throw db_error("terminal phase original tail binding differs");
        };
        bind.template operator()<0>(state->history_chain);bind.template operator()<1>(state->row_import);
        first->extraction_attempted=true;
        const auto extract=[&]<std::size_t Index>(const auto& outer) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!outer||!outer->active||outer->active.get()!=receipt->phase_original||outer->active.use_count()!=1||
               !receipt->binding_returned||first->held.template get<Index>())
                throw db_error("terminal phase actual extraction identity changed");
            receipt->extraction_attempted=true;
            first->held.template get<Index>()=std::move(outer->active);
            receipt->extraction_returned=true;
        };
        extract.template operator()<0>(state->history_chain);extract.template operator()<1>(state->row_import);
        first->extraction_returned=true;
        const auto release=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];if(!receipt)return true;
            receipt->release_attempted=true;
            const bool returned=first->held.template release<Index>(); // Real phase destruction OUTSIDE every leaf.
            if(!state->current_creator()||!returned)return false;
            receipt->release_returned=true;
            return receipt->members_destroyed.load(std::memory_order_acquire)&&receipt->ending_thread==state->thread&&
                !receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        if(!release.template operator()<0>()||!release.template operator()<1>())return terminal_native_phase_result::retained_debt;
        if(!connection.terminal_receiver_input_history_matches(receiving,scope,generation))
            return terminal_native_phase_result::retained_debt;
        first->result=(first->records[0]||first->records[1])?terminal_native_phase_result::phases_ended:
            terminal_native_phase_result::not_held;
        first->returned=true;return first->result;
    }catch(...) {
        if(state->current_creator()) {
            if(!state->terminal_native_phase_allocation_returned)state->terminal_native_phase_allocation_exception_returned=true;
            if(!state->terminal_native_phase_first_failure)state->terminal_native_phase_first_failure=std::current_exception();
            if(state->terminal_native_phase_original&&!state->terminal_native_phase_original->first_failure)
                state->terminal_native_phase_original->first_failure=std::current_exception();
        }
        // Remaining actual source/custody and first faults remain held. No new
        // phase, constructor, decoded retirement, deadline, native join or refund.
        return terminal_native_phase_result::retained_debt;
    }
}
bool database::target_terminal_native_phases_ended_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_native_phase_original:nullptr;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||
           !state->terminal_native_phase_attempted||!state->terminal_native_phase_allocation_returned||
           state->terminal_native_phase_allocation_exception_returned||state->terminal_native_phase_first_failure||
           !first||first->creator!=state->creator||first->thread!=state->thread||first->end!=state->end||
           first->slot!=state->slot||first->serial!=state->serial||first->shell!=this||first->connection!=&connection||
           first->owner!=&receiving||!first->scope||!first->generation||!first->preparation_returned||
           !first->extraction_attempted||!first->extraction_returned||!first->returned||first->first_failure||
           first->result!=terminal_native_phase_result::phases_ended||(!first->records[0]&&!first->records[1])||
           !receiving.terminal_inputs_ended(connection)||
           !connection.terminal_receiver_input_history_matches(receiving,first->scope,first->generation))return false;
        // Distinct historical decoder receipt: actual capsule/source endings,
        // same phase addresses and scope/generation, without dereferencing dead phases.
        const auto& decoder=first->decoder_original;
        if(!decoder||decoder!=state->terminal_decoder_original||!state->terminal_decoder_attempted||
           !state->terminal_decoder_allocation_returned||state->terminal_decoder_allocation_exception_returned||
           state->terminal_decoder_first_failure||decoder->first_failure||!decoder->evidence_returned||
           !decoder->evidence_known||!decoder->preparation_returned||!decoder->extraction_attempted||
           !decoder->extraction_returned||!decoder->returned||decoder->creator!=state->creator||
           decoder->thread!=state->thread||decoder->end!=state->end||decoder->slot!=state->slot||
           decoder->serial!=state->serial||decoder->shell!=this||decoder->connection!=&connection||
           decoder->owner!=&receiving||decoder->scope!=first->scope||decoder->generation!=first->generation)return false;
        if(decoder->result==terminal_decoder_payload_result::not_held) {
            if(decoder->held_values||decoder->held_charge_references||decoder->records[0]||decoder->records[1])return false;
        }else if(decoder->result!=terminal_decoder_payload_result::captures_ended||
                 (!decoder->held_values&&!decoder->held_charge_references))return false;
        const auto known=[&]<std::size_t Index>(const auto& outer) {
            const auto& receipt=first->records[Index];const auto& decoded=decoder->records[Index];
            if(outer.get()!=first->outer_originals[Index])return false;
            if(!receipt)return !decoded&&!first->prepared_phases[Index]&&!first->record_allocation_attempted[Index]&&
                !first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index]&&(!outer||!outer->active);
            if(!outer||outer->active||!first->record_allocation_attempted[Index]||
               !first->record_allocation_returned[Index]||first->record_allocation_exception_returned[Index]||
               first->prepared_phases[Index]!=receipt->phase_original||receipt->creator!=state->creator||
               receipt->thread!=state->thread||receipt->end!=state->end||receipt->slot!=state->slot||
               receipt->serial!=state->serial||receipt->kind!=Index||receipt->generation!=first->generation||
               receipt->state.lock()!=state||receipt->owner.lock().get()!=&receiving||receipt->shell!=this||
               receipt->connection!=&connection||receipt->cleanup_scope!=first->scope||!receipt->phase_original||
               !receipt->tail_original||!receipt->optional_original||!receipt->constructor_returned||
               outer->first_failure!=receipt->outer_first_failure||!receipt->binding_attempted||!receipt->binding_returned||
               !receipt->extraction_attempted||!receipt->extraction_returned||!receipt->release_attempted||!receipt->release_returned||
               !receipt->members_destroyed.load(std::memory_order_acquire)||receipt->ending_thread!=state->thread||
               receipt->partial_constructor_destroyed.load(std::memory_order_acquire)||
               (receipt->decode_entered&&!receipt->decode_returned&&!receipt->decode_exception_returned)||
               (receipt->ordinary_decoder_entered&&!receipt->ordinary_decoder_returned)||
               receipt->statement_count!=(Index==0?8u:136u)||receipt->native_call_count>(Index==0?4352u:33024u))return false;
            if constexpr(requires {outer->program;})
                if(outer->program.get()!=receipt->programme_original)return false;
            for(std::size_t at=0;at<receipt->statement_count;++at) {
                const auto& vm=receipt->statements[at];
                if(!vm.original||vm.held||(vm.prepare_entered&&!vm.prepare_returned)||
                   (vm.prepared&&(!vm.finalize_entered||!vm.finalize_returned||!vm.destruction_known||
                     vm.prepared!=vm.finalized||(vm.finalize_rc&0xff)==SQLITE_MISUSE)))return false;
            }
            if(!decoded)return !decoder->record_allocation_attempted[Index]&&!decoder->record_allocation_returned[Index]&&
                !decoder->record_allocation_exception_returned[Index]&&!decoder->prepared_phases[Index]&&
                !decoder->prepared_optionals[Index]&&!decoder->prepared_charges[Index]; // Unissued DATA only.
            return decoder->record_allocation_attempted[Index]&&decoder->record_allocation_returned[Index]&&
                !decoder->record_allocation_exception_returned[Index]&&decoder->prepared_phases[Index]==receipt->phase_original&&
                decoder->prepared_optionals[Index]==receipt->optional_original&&decoder->prepared_charges[Index]==decoded->charge_original&&
                decoded->phase==receipt->phase_original&&decoded->phase_scope==receipt->scope_wrapper_original&&
                decoded->page_index==receipt->page_index&&decoded->optional_original==receipt->optional_original&&
                decoded->decode_entered==receipt->decode_entered&&decoded->decode_returned==receipt->decode_returned&&
                decoded->decode_exception_returned==receipt->decode_exception_returned&&
                decoded->ordinary_retirement_entered==receipt->ordinary_decoder_entered&&
                decoded->ordinary_retirement_returned==receipt->ordinary_decoder_returned&&
                decoded->creator==state->creator&&decoded->thread==state->thread&&decoded->end==state->end&&
                decoded->slot==state->slot&&decoded->serial==state->serial&&decoded->kind==Index&&
                decoded->state.lock()==state&&decoded->owner.lock().get()==&receiving&&decoded->shell==this&&
                decoded->connection==&connection&&decoded->cleanup_scope==first->scope&&decoded->generation==first->generation&&
                decoded->allocation_attempted&&decoded->allocation_returned&&!decoded->allocation_exception_returned&&
                decoded->construction_entered&&decoded->construction_returned&&decoded->capsule_original&&!decoded->first_failure&&
                decoded->transfer_attempted&&decoded->transfer_returned&&decoded->release_attempted&&decoded->release_returned&&
                (!decoded->held_decoded||(decoded->source_reset_attempted&&decoded->source_reset_returned))&&
                decoded->payload_members_destroyed.load(std::memory_order_acquire)&&decoded->ending_thread==state->thread&&
                !decoded->partial_constructor_destroyed.load(std::memory_order_acquire)&&
                (!decoded->held_charge||receiving.terminal_decoder_charge_identity_matches(connection,decoded->charge_original));
        };
        return known.template operator()<0>(state->history_chain)&&known.template operator()<1>(state->row_import);
        // Two actual phase objects only; no remaining Native, shared owner, files or credit join.
    }catch(...){return false;}
}
} // namespace lattice
