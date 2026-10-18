// Actual catalog/header/projection/suspension Native object endings only.
// Shared RAW/scope/programme/owner charge and complete service/refund stay separate.
namespace lattice {
void database::wake_target_terminal_native_static_(const std::weak_ptr<target_staging_state>& weak_state,
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner>& weak_owner,
    const detail::ordinary_target_staging_connection* connection_original)noexcept {
    try {
        const auto state=weak_state.lock();if(!state||!state->current_creator())return;
        const auto connection=state->connection.lock();const auto receiving=weak_owner.lock();
        if(connection&&connection.get()==connection_original&&receiving&&receiving->terminal_inputs_ended(*connection))
            receiving->schedule_collection(); // Real post-member tail, outside leaves; no credit.
    }catch(...){}
}
database::terminal_native_statics_result database::retire_target_terminal_native_statics_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving)noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return terminal_native_statics_result::retained_debt;
    if(state->terminal_native_statics_attempted) {
        const auto& first=state->terminal_native_statics_original;
        if(!first||!first->returned||!target_terminal_native_static_history_known_(connection,receiving))
            return terminal_native_statics_result::retained_debt;
        return first->result; // Same actual first history; never reissue a partial effect.
    }
    (void)retire_target_terminal_native_pages_(connection,receiving);
    if(!target_terminal_native_page_history_known_(connection,receiving))return terminal_native_statics_result::retained_debt;
    const auto& pages=state->terminal_native_pages_original;
    using record=target_staging_state::terminal_native_static_record;
    const auto eligible=[&]<std::size_t Index>(const auto& root) {
        if(!root)return true;const auto& tail=root->terminal_static;
        if(root.use_count()!=1||root->shell!=this||tail.creator!=state->creator||tail.thread!=state->thread||
           tail.end!=state->end||tail.slot!=state->slot||tail.serial!=state->serial||tail.kind!=Index||tail.shell!=this||
           tail.object_original!=root.get()||!tail.schema_original||tail.state.lock()!=state||!tail.construction_returned||
           tail.binding_attempted||tail.first)return false;
        if constexpr(Index==0) {
            // Catalog was genuinely constructed from schema before header work;
            // do not manufacture an earlier receiving owner or require a weak
            // schema to survive real receiving-input destruction.
            if(!tail.owner.expired())return false;
            const auto original=root->original.lock();if(original&&original.get()!=tail.schema_original)return false;
        }else {
            if(root->owner.lock().get()!=&receiving||tail.owner.lock().get()!=&receiving)return false;
            const auto original=root->schema.lock();if(original&&original.get()!=tail.schema_original)return false;
        }
        return record::source_known(*root,*this,connection,receiving,pages->scope,pages->generation,Index);
    };
    if(!eligible.template operator()<0>(state->catalog)||!eligible.template operator()<1>(state->history_header)||
       !eligible.template operator()<2>(state->history_projection)||!eligible.template operator()<3>(state->import_suspension))
        return terminal_native_statics_result::retained_debt;
    state->terminal_native_statics_attempted=true; // Before first allocation; failed/unknown effects never rearm.
    try {
        using ending=target_staging_state::terminal_native_static_ending;
        static_assert(sizeof(record::vm_fact)<=128&&sizeof(record)<=2048&&sizeof(ending)<=2048,
            "terminal static history is bounded first Native DATA, not replacement payload");
        auto first=std::make_shared<ending>(state,*this,connection,receiving,pages->scope,pages->generation);
        state->terminal_native_statics_original=first;state->terminal_native_statics_allocation_returned=true;
        const auto prepare=[&]<std::size_t Index>(const auto& root) {
            first->prepared_roots[Index]=root.get();if(!root) {
                first->slot_results[Index]=terminal_native_statics_result::not_held;first->slot_returned[Index]=true;return;
            }
            if(!first->original_issuance_attempted[Index])throw db_error("terminal static Native source issuance differs");
            first->record_allocation_attempted[Index]=true;
            try {first->records[Index]=std::make_shared<record>(state,*this,connection,receiving,first->scope,first->generation,Index,*root);
                first->record_allocation_returned[Index]=true;}
            catch(...) {first->record_allocation_exception_returned[Index]=true;throw;}
            if(first->records[Index]->owner.lock().get()!=&receiving||!first->records[Index]->native_returns_known())
                throw db_error("terminal static Native first returned history differs");
        };
        prepare.template operator()<0>(state->catalog);prepare.template operator()<1>(state->history_header);
        prepare.template operator()<2>(state->history_projection);prepare.template operator()<3>(state->import_suspension);
        first->preparation_returned=true;
        if(!target_terminal_native_page_history_known_(connection,receiving)||first->pages_original!=state->terminal_native_pages_original||
           first->source_first_failure!=state->first_failure)throw db_error("terminal static Native predecessor history changed");
        const auto bind=[&]<std::size_t Index>(const auto& root) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!root||root.get()!=receipt->object_original||!root->terminal_static.bind(receipt))
                throw db_error("terminal static Native original tail binding differs");
        };
        bind.template operator()<0>(state->catalog);bind.template operator()<1>(state->history_header);
        bind.template operator()<2>(state->history_projection);bind.template operator()<3>(state->import_suspension);
        first->extraction_attempted=true;
        const auto extract=[&]<std::size_t Index>(auto& root) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!root||root.get()!=receipt->object_original||root.use_count()!=1||!receipt->binding_returned||first->held.template get<Index>())
                throw db_error("terminal static Native actual ownership differs");
            receipt->extraction_attempted=true;
            first->held.template get<Index>()=std::move(root);receipt->extraction_returned=true;
        };
        extract.template operator()<0>(state->catalog);extract.template operator()<1>(state->history_header);
        extract.template operator()<2>(state->history_projection);extract.template operator()<3>(state->import_suspension);
        first->extraction_returned=true;
        const auto release=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];
            if(!receipt)return first->slot_returned[Index]&&first->slot_results[Index]==terminal_native_statics_result::not_held;
            receipt->release_attempted=true;const bool returned=first->held.template release<Index>(); // Whole real object OUTSIDE leaves.
            if(!state->current_creator()||!returned)return false;
            receipt->release_returned=true;
            if(!receipt->members_destroyed.load(std::memory_order_acquire)||receipt->ending_thread!=state->thread||
                receipt->partial_constructor_destroyed.load(std::memory_order_acquire))return false;
            first->slot_results[Index]=terminal_native_statics_result::objects_ended;first->slot_returned[Index]=true;return true;
        };
        if(!release.template operator()<0>()||!release.template operator()<1>()||
           !release.template operator()<2>()||!release.template operator()<3>())return terminal_native_statics_result::retained_debt;
        if(!target_terminal_native_page_history_known_(connection,receiving))return terminal_native_statics_result::retained_debt;
        const bool any=bool(first->records[0]||first->records[1]||first->records[2]||first->records[3]);
        first->result=any?terminal_native_statics_result::objects_ended:terminal_native_statics_result::not_held;first->returned=true;
        return target_terminal_native_static_history_known_(connection,receiving)?first->result:terminal_native_statics_result::retained_debt;
    }catch(...) {
        if(state->current_creator()) {
            if(!state->terminal_native_statics_allocation_returned)state->terminal_native_statics_allocation_exception_returned=true;
            if(!state->terminal_native_statics_first_failure)state->terminal_native_statics_first_failure=std::current_exception();
            if(state->terminal_native_statics_original&&!state->terminal_native_statics_original->first_failure)
                state->terminal_native_statics_original->first_failure=std::current_exception();
        }
        return terminal_native_statics_result::retained_debt; // SAME originals/slots/errors; no retry/credit/refund.
    }
}
bool database::target_terminal_native_static_history_known_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_native_statics_original:nullptr;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||!first||
           !state->terminal_native_statics_attempted||!state->terminal_native_statics_allocation_returned||
           state->terminal_native_statics_allocation_exception_returned||state->terminal_native_statics_first_failure||
           !target_terminal_native_page_history_known_(connection,receiving)||first->pages_original!=state->terminal_native_pages_original||
           first->creator!=state->creator||first->thread!=state->thread||first->end!=state->end||first->slot!=state->slot||
           first->serial!=state->serial||first->shell!=this||first->connection!=&connection||first->owner!=&receiving||
           first->scope!=first->pages_original->scope||first->generation!=first->pages_original->generation||
           first->source_first_failure!=state->first_failure||first->original_issuance_attempted!=std::array<bool,4>{state->catalog_attempted,
               state->history_header_attempted,state->history_projection_attempted,state->import_suspension_attempted}||
           !first->preparation_returned||!first->extraction_attempted||!first->extraction_returned||!first->returned||first->first_failure||
           first->held.template get<0>()||first->held.template get<1>()||first->held.template get<2>()||first->held.template get<3>()||
           state->catalog||state->history_header||state->history_projection||state->import_suspension)return false;
        const bool any=bool(first->records[0]||first->records[1]||first->records[2]||first->records[3]);
        if(first->result!=(any?terminal_native_statics_result::objects_ended:terminal_native_statics_result::not_held))return false;
        const auto known=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];
            if(!first->slot_returned[Index]||first->slot_results[Index]!=(receipt?terminal_native_statics_result::objects_ended:
                terminal_native_statics_result::not_held))return false;
            if(!receipt)return !first->prepared_roots[Index]&&!first->record_allocation_attempted[Index]&&
                !first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index];
            return first->original_issuance_attempted[Index]&&first->record_allocation_attempted[Index]&&
                first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index]&&
                first->prepared_roots[Index]==receipt->object_original&&receipt->creator==state->creator&&receipt->thread==state->thread&&
                receipt->end==state->end&&receipt->slot==state->slot&&receipt->serial==state->serial&&receipt->kind==Index&&
                receipt->generation==first->generation&&receipt->state.lock()==state&&receipt->owner.lock().get()==&receiving&&
                receipt->shell==this&&receipt->connection==&connection&&receipt->cleanup_scope==first->scope&&receipt->object_original&&
                receipt->tail_original&&receipt->schema_original&&receipt->statements_original&&receipt->calls_original&&
                receipt->native_returns_known()&&receipt->binding_attempted&&receipt->binding_returned&&
                receipt->extraction_attempted&&receipt->extraction_returned&&receipt->release_attempted&&receipt->release_returned&&
                receipt->members_destroyed.load(std::memory_order_acquire)&&receipt->ending_thread==state->thread&&
                !receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        return known.template operator()<0>()&&known.template operator()<1>()&&known.template operator()<2>()&&known.template operator()<3>();
        // Only original immutable identities/results are read. No ended Native,
        // scope, VM, programme, SQL, row or RAW address is ever dereferenced.
    }catch(...){return false;}
}
bool database::target_terminal_native_statics_ended_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!target_terminal_native_static_history_known_(connection,receiving))return false;
    return state->terminal_native_statics_original->result==terminal_native_statics_result::objects_ended;
}
} // namespace lattice
