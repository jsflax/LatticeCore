// Two actual outer chain/row objects only. Static Native roots and shared
// programme/RAW/scope/charge/Conn-owner custody remain separate retained debt.
namespace lattice {
struct database::target_staging_state::terminal_native_outer_ending {
    using custody=detail::native_retirement_storage<std::shared_ptr<history_chain_native>,std::shared_ptr<row_import_native>>;
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial,generation;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const detail::ordinary_group_target_receiving_owner* const owner;
    const void* const scope;
    const std::shared_ptr<terminal_native_phase_ending> phase_original;
    std::array<const void*,2> prepared_roots{};
    std::array<bool,2> original_issuance_attempted{},record_allocation_attempted{},record_allocation_returned{},record_allocation_exception_returned{};
    std::array<std::shared_ptr<terminal_native_outer_record>,2> records{};
    custody held; // BOTH empty owning slots exist before arming or moving roots.
    bool preparation_returned=false,extraction_attempted=false,extraction_returned=false,returned=false;
    terminal_native_outer_result result=terminal_native_outer_result::retained_debt;
    std::exception_ptr first_failure;
    terminal_native_outer_ending(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup_scope,std::uint64_t gen)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),serial(actual->serial),
         generation(gen),shell(&actual_shell),connection(&conn),owner(&receiving),scope(cleanup_scope),
         phase_original(actual->terminal_native_phase_original) {}
};
void database::wake_target_terminal_native_outer_(const std::weak_ptr<target_staging_state>& weak_state,
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner>& weak_owner,
    const detail::ordinary_target_staging_connection* connection_original)noexcept {
    try {
        const auto state=weak_state.lock();
        if(!state||!state->current_creator())return;
        const auto connection=state->connection.lock();const auto receiving=weak_owner.lock();
        if(connection&&connection.get()==connection_original&&receiving&&receiving->terminal_inputs_ended(*connection))
            receiving->schedule_collection(); // Real post-member-tail wake, outside leaves.
    }catch(...){}
}

// Distinct historical phase/decoder proof. No chain/row/phase pointer is
// dereferenced here; those addresses are comparison DATA after actual death.
bool database::target_terminal_phase_history_after_outer_known_(
    const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto roots=state?state->terminal_native_outer_original:nullptr;
        const auto phase=roots?roots->phase_original:nullptr;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||!roots||
           !state->terminal_native_outer_attempted||!state->terminal_native_outer_allocation_returned||
           state->terminal_native_outer_allocation_exception_returned||state->terminal_native_outer_first_failure||
           roots->first_failure||!roots->preparation_returned||roots->creator!=state->creator||roots->thread!=state->thread||
           roots->end!=state->end||roots->slot!=state->slot||roots->serial!=state->serial||roots->shell!=this||
           roots->connection!=&connection||roots->owner!=&receiving||!roots->scope||!roots->generation||
           !receiving.terminal_inputs_ended(connection)||
           !connection.terminal_receiver_input_history_matches(receiving,roots->scope,roots->generation)||
           !state->terminal_native_phase_attempted||!state->terminal_native_phase_allocation_returned||
           state->terminal_native_phase_allocation_exception_returned||state->terminal_native_phase_first_failure||
           !phase||phase!=state->terminal_native_phase_original||phase->creator!=state->creator||phase->thread!=state->thread||
           phase->end!=state->end||phase->slot!=state->slot||phase->serial!=state->serial||phase->shell!=this||
           phase->connection!=&connection||phase->owner!=&receiving||phase->scope!=roots->scope||phase->generation!=roots->generation||
           !phase->preparation_returned||!phase->extraction_attempted||!phase->extraction_returned||
           !phase->returned||phase->first_failure||phase->outer_originals!=roots->prepared_roots)return false;
        const bool any_phase=bool(phase->records[0]||phase->records[1]);
        if(any_phase?(phase->result!=terminal_native_phase_result::phases_ended):
                     (phase->result!=terminal_native_phase_result::not_held))return false;
        const auto& decoder=phase->decoder_original;
        if(!decoder||decoder!=state->terminal_decoder_original||!state->terminal_decoder_attempted||
           !state->terminal_decoder_allocation_returned||state->terminal_decoder_allocation_exception_returned||
           state->terminal_decoder_first_failure||decoder->first_failure||!decoder->evidence_returned||
           !decoder->evidence_known||!decoder->preparation_returned||!decoder->extraction_attempted||
           !decoder->extraction_returned||!decoder->returned||decoder->creator!=state->creator||decoder->thread!=state->thread||
           decoder->end!=state->end||decoder->slot!=state->slot||decoder->serial!=state->serial||decoder->shell!=this||
           decoder->connection!=&connection||decoder->owner!=&receiving||decoder->scope!=roots->scope||decoder->generation!=roots->generation)return false;
        if(decoder->result==terminal_decoder_payload_result::not_held) {
            if(decoder->held_values||decoder->held_charge_references||decoder->records[0]||decoder->records[1])return false;
        }else if(decoder->result!=terminal_decoder_payload_result::captures_ended||
                 (!decoder->held_values&&!decoder->held_charge_references))return false;
        const auto known=[&]<std::size_t Index>() {
            const auto& outer=roots->records[Index];const auto& receipt=phase->records[Index];const auto& decoded=decoder->records[Index];
            if(!receipt)return !decoded&&!phase->prepared_phases[Index]&&!phase->record_allocation_attempted[Index]&&
                !phase->record_allocation_returned[Index]&&!phase->record_allocation_exception_returned[Index]&&
                !decoder->record_allocation_attempted[Index]&&!decoder->record_allocation_returned[Index]&&
                !decoder->record_allocation_exception_returned[Index]&&!decoder->prepared_phases[Index]&&
                !decoder->prepared_optionals[Index]&&!decoder->prepared_charges[Index]; // Unissued DATA, no phase ending grant.
            if(!outer||outer->outer_original!=roots->prepared_roots[Index]||
               !phase->record_allocation_attempted[Index]||!phase->record_allocation_returned[Index]||
               phase->record_allocation_exception_returned[Index]||phase->prepared_phases[Index]!=receipt->phase_original||
               receipt->creator!=state->creator||receipt->thread!=state->thread||receipt->end!=state->end||
               receipt->slot!=state->slot||receipt->serial!=state->serial||receipt->kind!=Index||receipt->generation!=roots->generation||
               receipt->state.lock()!=state||receipt->owner.lock().get()!=&receiving||receipt->shell!=this||
               receipt->connection!=&connection||receipt->cleanup_scope!=roots->scope||!receipt->phase_original||
               !receipt->tail_original||!receipt->optional_original||!receipt->constructor_returned||
               outer->first_native_failure!=receipt->outer_first_failure||!receipt->binding_attempted||!receipt->binding_returned||
               !receipt->extraction_attempted||!receipt->extraction_returned||!receipt->release_attempted||!receipt->release_returned||
               !receipt->members_destroyed.load(std::memory_order_acquire)||receipt->ending_thread!=state->thread||
               receipt->partial_constructor_destroyed.load(std::memory_order_acquire)||
               (receipt->decode_entered&&!receipt->decode_returned&&!receipt->decode_exception_returned)||
               (receipt->ordinary_decoder_entered&&!receipt->ordinary_decoder_returned)||
               receipt->statement_count!=(Index==0?8u:136u)||receipt->native_call_count>(Index==0?4352u:33024u))return false;
            if constexpr(Index==1)if(outer->programme_original!=receipt->programme_original)return false;
            for(std::size_t at=0;at<receipt->statement_count;++at) {
                const auto& vm=receipt->statements[at];
                if(!vm.original||vm.held||(vm.prepare_entered&&!vm.prepare_returned)||
                   (vm.prepared&&(!vm.finalize_entered||!vm.finalize_returned||!vm.destruction_known||
                     vm.prepared!=vm.finalized||(vm.finalize_rc&0xff)==SQLITE_MISUSE)))return false;
            }
            if(!decoded)return !decoder->record_allocation_attempted[Index]&&!decoder->record_allocation_returned[Index]&&
                !decoder->record_allocation_exception_returned[Index]&&!decoder->prepared_phases[Index]&&
                !decoder->prepared_optionals[Index]&&!decoder->prepared_charges[Index];
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
                decoded->connection==&connection&&decoded->cleanup_scope==roots->scope&&decoded->generation==roots->generation&&
                decoded->allocation_attempted&&decoded->allocation_returned&&!decoded->allocation_exception_returned&&
                decoded->construction_entered&&decoded->construction_returned&&decoded->capsule_original&&!decoded->first_failure&&
                decoded->transfer_attempted&&decoded->transfer_returned&&decoded->release_attempted&&decoded->release_returned&&
                (!decoded->held_decoded||(decoded->source_reset_attempted&&decoded->source_reset_returned))&&
                decoded->payload_members_destroyed.load(std::memory_order_acquire)&&decoded->ending_thread==state->thread&&
                !decoded->partial_constructor_destroyed.load(std::memory_order_acquire)&&
                (!decoded->held_charge||receiving.terminal_decoder_charge_identity_matches(connection,decoded->charge_original));
        };
        return known.template operator()<0>()&&known.template operator()<1>();
    }catch(...){return false;}
}

database::terminal_native_outer_result database::retire_target_terminal_native_outer_roots_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving)noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return terminal_native_outer_result::retained_debt;
    if(state->terminal_native_outer_attempted) {
        // NEVER call older live Native/phase/decoder observers after extraction,
        // including a partial/unknown first attempt. History is the only route.
        const auto& first=state->terminal_native_outer_original;
        if(!first||!first->returned)return terminal_native_outer_result::retained_debt;
        if(first->result==terminal_native_outer_result::not_held)return first->result;
        return target_terminal_native_outer_roots_ended_(connection,receiving)?terminal_native_outer_result::roots_ended:
            terminal_native_outer_result::retained_debt;
    }
    // The phase attempt seals off the older LIVE decoder observer even if
    // outer readiness later refuses. Phase's own attempted route is historical.
    if(!state->terminal_native_phase_attempted)
        (void)retire_target_terminal_decoder_payloads_(connection,receiving);
    (void)retire_target_terminal_native_phases_(connection,receiving);
    const void* scope=nullptr;std::uint64_t generation=0;
    if(!connection.terminal_native_decoder_context_known(receiving,scope,generation)||!scope||!generation||
       !target_staging_native_cleanup_known_(connection))return terminal_native_outer_result::retained_debt;
    const auto& phase=state->terminal_native_phase_original;
    if(!phase||!phase->returned||phase->first_failure||!phase->preparation_returned||!phase->extraction_returned||
       phase->scope!=scope||phase->generation!=generation)return terminal_native_outer_result::retained_debt;
    if(phase->result==terminal_native_phase_result::phases_ended) {
        if(!target_terminal_native_phases_ended_(connection,receiving))return terminal_native_outer_result::retained_debt;
    }else if(phase->result!=terminal_native_phase_result::not_held||phase->records[0]||phase->records[1])
        return terminal_native_outer_result::retained_debt;
    const auto eligible=[&](const auto& root,std::size_t kind) {
        if(!root)return true; // No materialized root DATA; no destruction grant.
        const auto& tail=root->terminal_outer;
        return root.use_count()==1&&root->shell==this&&root->owner.lock().get()==&receiving&&!root->active&&
            root->checked.previous.size()<=64&&tail.creator==state->creator&&tail.thread==state->thread&&
            tail.end==state->end&&tail.slot==state->slot&&tail.serial==state->serial&&tail.kind==kind&&tail.shell==this&&
            tail.outer_original==root.get()&&tail.state.lock()==state&&tail.owner.lock().get()==&receiving&&
            tail.construction_returned&&!tail.binding_attempted&&!tail.first;
    };
    if(!eligible(state->history_chain,0)||!eligible(state->row_import,1))return terminal_native_outer_result::retained_debt;
    state->terminal_native_outer_attempted=true; // Before first allocation; never reset/retry unknown effects.
    try {
        using ending=target_staging_state::terminal_native_outer_ending;using record=target_staging_state::terminal_native_outer_record;
        static_assert(sizeof(record)<=2048&&sizeof(ending)<=2048,"terminal outer history must remain a fixed small bank");
        auto first=std::make_shared<ending>(state,*this,connection,receiving,scope,generation);
        state->terminal_native_outer_original=first;state->terminal_native_outer_allocation_returned=true;
        first->original_issuance_attempted={state->history_chain_attempted,state->row_import_attempted};
        const auto prepare=[&]<std::size_t Index>(const auto& root) {
            first->prepared_roots[Index]=root.get();if(!root)return;
            if(!first->original_issuance_attempted[Index])throw db_error("terminal outer source issuance differs");
            first->record_allocation_attempted[Index]=true;
            try {first->records[Index]=std::make_shared<record>(state,*this,connection,scope,generation,Index,*root);
                first->record_allocation_returned[Index]=true;}
            catch(...) {first->record_allocation_exception_returned[Index]=true;throw;}
        };
        prepare.template operator()<0>(state->history_chain);prepare.template operator()<1>(state->row_import);
        first->preparation_returned=true;
        if(!target_terminal_phase_history_after_outer_known_(connection,receiving))
            throw db_error("terminal outer exact first phase history differs");
        const auto bind=[&]<std::size_t Index>(const auto& root) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!root||root.get()!=receipt->outer_original||!root->terminal_outer.bind(receipt))
                throw db_error("terminal outer original tail binding differs");
        };
        bind.template operator()<0>(state->history_chain);bind.template operator()<1>(state->row_import);
        first->extraction_attempted=true;
        const auto extract=[&]<std::size_t Index>(auto& root) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!root||root.get()!=receipt->outer_original||root.use_count()!=1||root->active||
               !receipt->binding_returned||first->held.template get<Index>())
                throw db_error("terminal outer actual extraction identity differs");
            receipt->extraction_attempted=true;first->held.template get<Index>()=std::move(root);receipt->extraction_returned=true;
        };
        extract.template operator()<0>(state->history_chain);extract.template operator()<1>(state->row_import);
        first->extraction_returned=true;
        const auto release=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];if(!receipt)return true;
            receipt->release_attempted=true;
            const bool returned=first->held.template release<Index>(); // Actual whole Native destruction OUTSIDE leaves.
            if(!state->current_creator()||!returned)return false;
            receipt->release_returned=true;
            return receipt->members_destroyed.load(std::memory_order_acquire)&&receipt->ending_thread==state->thread&&
                !receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        if(!release.template operator()<0>()||!release.template operator()<1>())return terminal_native_outer_result::retained_debt;
        if(!target_terminal_phase_history_after_outer_known_(connection,receiving))return terminal_native_outer_result::retained_debt;
        first->result=(first->records[0]||first->records[1])?terminal_native_outer_result::roots_ended:terminal_native_outer_result::not_held;
        first->returned=true;return first->result;
    }catch(...) {
        if(state->current_creator()) {
            if(!state->terminal_native_outer_allocation_returned)state->terminal_native_outer_allocation_exception_returned=true;
            if(!state->terminal_native_outer_first_failure)state->terminal_native_outer_first_failure=std::current_exception();
            if(state->terminal_native_outer_original&&!state->terminal_native_outer_original->first_failure)
                state->terminal_native_outer_original->first_failure=std::current_exception();
        }
        return terminal_native_outer_result::retained_debt; // SAME remaining roots/custody and first faults retained.
    }
}
bool database::target_terminal_native_outer_roots_ended_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_native_outer_original:nullptr;
        if(!state||!state->current_creator()||!first||!target_terminal_phase_history_after_outer_known_(connection,receiving)||
           !first->extraction_attempted||!first->extraction_returned||!first->returned||first->first_failure||
           first->result!=terminal_native_outer_result::roots_ended||(!first->records[0]&&!first->records[1])||
           first->held.template get<0>()||first->held.template get<1>()||state->history_chain||state->row_import)return false;
        const auto known=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];
            if(!receipt)return !first->prepared_roots[Index]&&!first->record_allocation_attempted[Index]&&
                !first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index];
            return first->original_issuance_attempted[Index]&&first->record_allocation_attempted[Index]&&
                first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index]&&
                first->prepared_roots[Index]==receipt->outer_original&&receipt->creator==state->creator&&receipt->thread==state->thread&&
                receipt->end==state->end&&receipt->slot==state->slot&&receipt->serial==state->serial&&receipt->kind==Index&&
                receipt->generation==first->generation&&receipt->state.lock()==state&&receipt->owner.lock().get()==&receiving&&
                receipt->shell==this&&receipt->connection==&connection&&receipt->cleanup_scope==first->scope&&
                receipt->outer_original&&receipt->tail_original&&receipt->stage_original&&receipt->meta_original&&
                receipt->progress_original&&receipt->hash_original&&receipt->constructor_returned&&receipt->previous_size<=64&&
                receipt->binding_attempted&&receipt->binding_returned&&receipt->extraction_attempted&&receipt->extraction_returned&&
                receipt->release_attempted&&receipt->release_returned&&receipt->members_destroyed.load(std::memory_order_acquire)&&
                receipt->ending_thread==state->thread&&!receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        return known.template operator()<0>()&&known.template operator()<1>();
        // Only these two actual Native roots. No static/shared owner/credit join.
    }catch(...){return false;}
}
} // namespace lattice
