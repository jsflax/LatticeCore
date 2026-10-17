// Only the two actual current/previous page Native objects. Every other static,
// shared, RAW/file/scope/charge/shell/Conn/owner resource family stays separate.
namespace lattice {
struct database::target_staging_state::terminal_native_page_ending {
    using custody=detail::native_retirement_storage<std::shared_ptr<history_page_native>,std::shared_ptr<history_page_native>>;
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial,generation,issued_pages;
    const bool source_page_active;
    const std::exception_ptr source_first_failure;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const detail::ordinary_group_target_receiving_owner* const owner;
    const void* const scope;
    const std::shared_ptr<terminal_native_outer_ending> outer_original;
    std::array<const void*,2> prepared_roots{};
    std::array<bool,2> record_allocation_attempted{},record_allocation_returned{},record_allocation_exception_returned{};
    std::array<std::shared_ptr<terminal_native_page_record>,2> records{};
    custody held; // BOTH empty owning slots before any actual link is detached.
    bool preparation_returned=false,extraction_attempted=false,extraction_returned=false,returned=false;
    terminal_native_pages_result result=terminal_native_pages_result::retained_debt;
    std::exception_ptr first_failure;
    terminal_native_page_ending(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup,std::uint64_t gen)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),serial(actual->serial),generation(gen),
         issued_pages(actual->history_pages_issued),source_page_active(actual->history_page_active),source_first_failure(actual->first_failure),
         shell(&actual_shell),connection(&conn),owner(&receiving),scope(cleanup),outer_original(actual->terminal_native_outer_original) {}
};
void database::wake_target_terminal_native_page_(const std::weak_ptr<target_staging_state>& weak_state,
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner>& weak_owner,
    const detail::ordinary_target_staging_connection* connection_original)noexcept {
    try {
        const auto state=weak_state.lock();if(!state||!state->current_creator())return;
        const auto connection=state->connection.lock();const auto receiving=weak_owner.lock();
        if(connection&&connection.get()==connection_original&&receiving&&receiving->terminal_inputs_ended(*connection))
            receiving->schedule_collection(); // Actual page-member tail, outside every leaf.
    }catch(...){}
}
bool database::target_terminal_outer_history_before_pages_known_(
    const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_native_outer_original:nullptr;
        if(!state||!state->current_creator()||!first||!target_terminal_phase_history_after_outer_known_(connection,receiving)||
           !first->extraction_attempted||!first->extraction_returned||!first->returned||first->first_failure)return false;
        if(first->result==terminal_native_outer_result::roots_ended)
            return target_terminal_native_outer_roots_ended_(connection,receiving);
        if(first->result!=terminal_native_outer_result::not_held||state->history_chain||state->row_import||
           first->held.template get<0>()||first->held.template get<1>())return false;
        // No materialized outer roots is exact first DATA, not roots_ended.
        // Failed page staging can genuinely stop before either outer is issued.
        for(std::size_t at=0;at<2;++at)if(first->prepared_roots[at]||first->records[at]||
            first->record_allocation_attempted[at]||first->record_allocation_returned[at]||
            first->record_allocation_exception_returned[at])return false;
        return true;
    }catch(...){return false;}
}
database::terminal_native_pages_result database::retire_target_terminal_native_pages_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving)noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return terminal_native_pages_result::retained_debt;
    if(state->terminal_native_pages_attempted) {
        // Every later observation uses first history, including partial/failed
        // attempts. Never inspect page/VM/UDF/scope members after extraction.
        const auto& first=state->terminal_native_pages_original;
        if(!first||!first->returned||!target_terminal_native_page_history_known_(connection,receiving))
            return terminal_native_pages_result::retained_debt;
        return first->result; // Preserve exact not-held DATA separately from pages-ended.
    }
    (void)retire_target_terminal_native_outer_roots_(connection,receiving);
    if(!target_terminal_outer_history_before_pages_known_(connection,receiving))return terminal_native_pages_result::retained_debt;
    const auto& outer=state->terminal_native_outer_original;
    const auto eligible=[&](const auto& root) {
        if(!root)return true;
        const auto& tail=root->terminal_page;
        if(root.use_count()!=1||root->shell!=this||root->owner.lock().get()!=&receiving||
           root->progress.previous.size()>64||tail.creator!=state->creator||tail.thread!=state->thread||
           tail.end!=state->end||tail.slot!=state->slot||tail.serial!=state->serial||tail.shell!=this||
           tail.page_original!=root.get()||tail.state.lock()!=state||tail.owner.lock().get()!=&receiving||
           !tail.construction_returned||tail.binding_attempted||tail.first||tail.page_index==UINT64_MAX||
           tail.page_index>=state->history_pages_issued||root->progress.next_page!=tail.page_index+1||
           root->call_count>root->calls.size()||(root->guard_registration_entered&&!root->guard_registration_returned)||
           (root->userdata&&(!root->guard_destroy_entered||!root->guard_destroy_returned))||
           (root->ending_autocommit_entered&&!root->ending_autocommit_returned)||
           (root->raw_append&&!root->raw_append->cleanup_known())||
           (root->scope&&!root->scope->terminal_page_detached_history(*this,connection,receiving,outer->scope,outer->generation).detached_known))return false;
        if(!root->scope&&(root->call_count||root->guard_registration_entered||root->userdata||root->ending_autocommit_entered))return false;
        for(std::size_t at=0;at<root->call_count;++at)if(root->calls[at].entered&&!root->calls[at].returned)return false;
        for(const auto& vm:root->statements) {
            if(vm.held||(vm.prepare_entered&&!vm.prepare_returned)||
               (!vm.prepare_entered&&(vm.prepare_returned||vm.prepared_identity)))return false;
            if(vm.prepared_identity) {
                if(!vm.prepare_entered||!vm.prepare_returned||!vm.finalize_entered||!vm.finalize_returned||
                   !vm.destruction_known||(vm.finalize_rc&0xff)==SQLITE_MISUSE||vm.finalized_identity!=vm.prepared_identity)return false;
            }else if(vm.finalize_entered||vm.finalized_identity)return false;
        }
        const auto& begin=root->statements[root->begin];const auto& commit=root->statements[root->commit];
        const auto& rollback=root->statements[root->rollback];
        return !begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||
            (commit.terminal_returned&&commit.terminal_rc==SQLITE_DONE)||
            (rollback.terminal_returned&&rollback.terminal_rc==SQLITE_DONE)||
            (root->ending_autocommit_returned&&root->first_ending_autocommit!=0);
    };
    const auto& current=state->history_page; // Reference only: never mint a new owning alias.
    if(!eligible(current))return terminal_native_pages_result::retained_debt;
    if(current&&current->previous) {
        const auto& previous=current->previous;
        if(previous.get()==current.get()||previous->previous||!eligible(previous)||
           previous->terminal_page.page_index==UINT64_MAX||
           previous->terminal_page.page_index+1!=current->terminal_page.page_index)return terminal_native_pages_result::retained_debt;
    }
    state->terminal_native_pages_attempted=true; // Before all fallible storage/records; never reissue.
    try {
        using ending=target_staging_state::terminal_native_page_ending;using record=target_staging_state::terminal_native_page_record;
        static_assert(sizeof(record)<=16384&&sizeof(ending)<=2048,"terminal page history stays a fixed bounded Native result bank");
        auto first=std::make_shared<ending>(state,*this,connection,receiving,outer->scope,outer->generation);
        state->terminal_native_pages_original=first;state->terminal_native_pages_allocation_returned=true;
        const auto prepare=[&]<std::size_t Index>(const auto& root) {
            first->prepared_roots[Index]=root.get();if(!root)return;
            first->record_allocation_attempted[Index]=true;
            try {first->records[Index]=std::make_shared<record>(state,*this,connection,receiving,first->scope,first->generation,Index,*root);
                first->record_allocation_returned[Index]=true;}
            catch(...) {first->record_allocation_exception_returned[Index]=true;throw;}
            if(!first->records[Index]->native_returns_known())throw db_error("terminal page actual Native ending differs");
        };
        prepare.template operator()<0>(current);
        if(current)prepare.template operator()<1>(current->previous);
        first->preparation_returned=true;
        if(!target_terminal_outer_history_before_pages_known_(connection,receiving)||
           first->issued_pages!=state->history_pages_issued||first->source_page_active!=state->history_page_active||
           first->source_first_failure!=state->first_failure)throw db_error("terminal page first original changed before extraction");
        const auto bind=[&]<std::size_t Index>(const auto& root) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            if(!root||root.get()!=receipt->page_original||!root->terminal_page.bind(receipt))
                throw db_error("terminal page original tail binding differs");
        };
        bind.template operator()<0>(current);if(current)bind.template operator()<1>(current->previous);
        first->extraction_attempted=true;
        if(current) {
            if(current.get()!=first->prepared_roots[0]||current.use_count()!=1||
               current->previous.get()!=first->prepared_roots[1])throw db_error("terminal current page actual custody differs");
            if(current->previous) {
                const auto& receipt=first->records[1];
                if(current->previous.use_count()!=1||!receipt||!receipt->binding_returned||first->held.template get<1>())
                    throw db_error("terminal previous page actual custody differs");
                // Detach the actual link FIRST. Current's destruction cannot
                // recursively destroy previous outside its own controlled reset.
                receipt->extraction_attempted=true;
                first->held.template get<1>()=std::move(current->previous);receipt->extraction_returned=true;
            }
            const auto& receipt=first->records[0];
            if(!receipt||!receipt->binding_returned||first->held.template get<0>())throw db_error("terminal page current binding differs");
            receipt->extraction_attempted=true;
            first->held.template get<0>()=std::move(state->history_page);receipt->extraction_returned=true;
        }
        first->extraction_returned=true;
        const auto release=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];if(!receipt)return true;
            receipt->release_attempted=true;
            const bool returned=first->held.template release<Index>(); // Real whole page destruction, outside leaves.
            if(!state->current_creator()||!returned)return false;
            receipt->release_returned=true;
            return receipt->members_destroyed.load(std::memory_order_acquire)&&receipt->ending_thread==state->thread&&
                !receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        if(!release.template operator()<0>()||!release.template operator()<1>())return terminal_native_pages_result::retained_debt;
        if(!target_terminal_outer_history_before_pages_known_(connection,receiving))return terminal_native_pages_result::retained_debt;
        first->result=(first->records[0]||first->records[1])?terminal_native_pages_result::pages_ended:terminal_native_pages_result::not_held;
        first->returned=true;
        return target_terminal_native_page_history_known_(connection,receiving)?first->result:terminal_native_pages_result::retained_debt;
    }catch(...) {
        if(state->current_creator()) {
            if(!state->terminal_native_pages_allocation_returned)state->terminal_native_pages_allocation_exception_returned=true;
            if(!state->terminal_native_pages_first_failure)state->terminal_native_pages_first_failure=std::current_exception();
            if(state->terminal_native_pages_original&&!state->terminal_native_pages_original->first_failure)
                state->terminal_native_pages_original->first_failure=std::current_exception();
        }
        return terminal_native_pages_result::retained_debt; // Original roots/slots/first failures remain exact retained debt.
    }
}
bool database::target_terminal_native_page_history_known_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_native_pages_original:nullptr;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||!first||
           !state->terminal_native_pages_attempted||!state->terminal_native_pages_allocation_returned||
           state->terminal_native_pages_allocation_exception_returned||state->terminal_native_pages_first_failure||
           !target_terminal_outer_history_before_pages_known_(connection,receiving)||
           first->outer_original!=state->terminal_native_outer_original||first->creator!=state->creator||first->thread!=state->thread||
           first->end!=state->end||first->slot!=state->slot||first->serial!=state->serial||first->shell!=this||
           first->connection!=&connection||first->owner!=&receiving||first->scope!=first->outer_original->scope||
           first->generation!=first->outer_original->generation||first->issued_pages!=state->history_pages_issued||
           first->source_page_active!=state->history_page_active||first->source_first_failure!=state->first_failure||
           !first->preparation_returned||!first->extraction_attempted||!first->extraction_returned||!first->returned||first->first_failure||
           first->held.template get<0>()||first->held.template get<1>()||state->history_page)return false;
        if(first->result!=(first->records[0]?terminal_native_pages_result::pages_ended:terminal_native_pages_result::not_held)||
           (!first->records[0]&&first->records[1]))return false;
        const auto known=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];
            if(!receipt)return !first->prepared_roots[Index]&&!first->record_allocation_attempted[Index]&&
                !first->record_allocation_returned[Index]&&!first->record_allocation_exception_returned[Index];
            if(!first->record_allocation_attempted[Index]||!first->record_allocation_returned[Index]||
               first->record_allocation_exception_returned[Index]||first->prepared_roots[Index]!=receipt->page_original||
               receipt->creator!=state->creator||receipt->thread!=state->thread||receipt->end!=state->end||receipt->slot!=state->slot||
               receipt->serial!=state->serial||receipt->kind!=Index||receipt->generation!=first->generation||
               receipt->state.lock()!=state||receipt->owner.lock().get()!=&receiving||receipt->shell!=this||
               receipt->connection!=&connection||receipt->cleanup_scope!=first->scope||!receipt->page_original||!receipt->tail_original||
               !receipt->calls_original||!receipt->statements_original||!receipt->stage_original||!receipt->meta_original||
               !receipt->progress_original||!receipt->hash_original||receipt->page_index>=first->issued_pages||
               receipt->page_index==UINT64_MAX||receipt->next_page!=receipt->page_index+1||!receipt->native_returns_known()||
               !receipt->binding_attempted||!receipt->binding_returned||!receipt->extraction_attempted||!receipt->extraction_returned||
               !receipt->release_attempted||!receipt->release_returned||!receipt->members_destroyed.load(std::memory_order_acquire)||
               receipt->ending_thread!=state->thread||receipt->partial_constructor_destroyed.load(std::memory_order_acquire))return false;
            if constexpr(Index==0)return receipt->previous_original==first->prepared_roots[1];
            else return !receipt->previous_original&&receipt->page_index+1==first->records[0]->page_index;
        };
        return known.template operator()<0>()&&known.template operator()<1>();
        // No page/scope/VM/raw-append address is dereferenced after death.
        // RAW append/spool ownership, files, history charges and refunds remain separate.
    }catch(...){return false;}
}
bool database::target_terminal_native_pages_ended_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!target_terminal_native_page_history_known_(connection,receiving))return false;
    const auto& first=state->terminal_native_pages_original;
    return first&&first->result==terminal_native_pages_result::pages_ended&&bool(first->records[0]);
}
} // namespace lattice

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#include <cstring>
#include <typeinfo>
namespace lattice {
detail::ordinary_target_lifetime_observation database::qualification_target_lifetime_observation_(
    const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    using observation=detail::ordinary_target_lifetime_observation;
    observation out;
    try {
        const auto state=target_staging_;
        if(!state||!state->current_creator()||state->connection.lock().get()!=&connection)return out;
        out.creator_worker=true;out.inputs_ended=receiving.terminal_inputs_ended(connection);
        out.retirement_requested=state->retirement_requested;out.state_first_failure=bool(state->first_failure);
        if(state->first_failure) {
            // Rethrow the SAME saved first exception. An earlier registration,
            // BEGIN/postimage/currentness error must never look like injection.
            try {std::rethrow_exception(state->first_failure);}
            catch(const db_error& first) {
                out.state_first_failure_observed=true;
                out.state_first_failure_is_page2_injection=typeid(first)==typeid(db_error)&&
                    std::strcmp(first.what(),"injected receiving history page2 pre-predecessor-release failure")==0;
            }catch(...) {out.state_first_failure_observed=true;} // Known unrecognized type; never a matching injection.
        }
        out.history_page_active=state->history_page_active;out.history_pages_issued=state->history_pages_issued;
        const auto address=[](const void* value){return reinterpret_cast<std::uintptr_t>(value);};
        bool same_owner=true,bound=false;
        const auto source_identity=[&](const auto& source) {
            if(!source)return;
            if(source->owner.lock().get()!=&receiving)same_owner=false;else bound=true;
        };
        // Header remains an independent live static root in this source graph.
        // No proof of its death or shared schema/programme ending is issued.
        if(!state->terminal_native_statics_attempted)source_identity(state->history_header);
        const auto bank=[&](auto& output,bool attempted,bool allocated,bool allocation_exception,bool failure,const auto& first) {
            output.attempted=attempted;output.allocation_returned=allocated;
            output.allocation_exception_returned=allocation_exception;output.first_failure=failure;
            if(!first)return;
            if(first->owner!=&receiving)same_owner=false;else bound=true;
            output.first_failure=output.first_failure||bool(first->first_failure);
            output.preparation_returned=first->preparation_returned;output.extraction_attempted=first->extraction_attempted;
            output.extraction_returned=first->extraction_returned;output.returned=first->returned;
            output.result=static_cast<int>(first->result);
        };
        bank(out.decoder,state->terminal_decoder_attempted,state->terminal_decoder_allocation_returned,
            state->terminal_decoder_allocation_exception_returned,bool(state->terminal_decoder_first_failure),state->terminal_decoder_original);
        bank(out.phases,state->terminal_native_phase_attempted,state->terminal_native_phase_allocation_returned,
            state->terminal_native_phase_allocation_exception_returned,bool(state->terminal_native_phase_first_failure),state->terminal_native_phase_original);
        bank(out.outer,state->terminal_native_outer_attempted,state->terminal_native_outer_allocation_returned,
            state->terminal_native_outer_allocation_exception_returned,bool(state->terminal_native_outer_first_failure),state->terminal_native_outer_original);
        bank(out.pages,state->terminal_native_pages_attempted,state->terminal_native_pages_allocation_returned,
            state->terminal_native_pages_allocation_exception_returned,bool(state->terminal_native_pages_first_failure),state->terminal_native_pages_original);
        // Only the composed historical route is used. Never call old live
        // decoder/phase/Native predicates once their source extraction began.
        if(state->terminal_native_outer_attempted) {
            const bool known=target_terminal_outer_history_before_pages_known_(connection,receiving);
            out.outer.history_checked=out.phases.history_checked=out.decoder.history_checked=true;
            out.outer.history_known=out.phases.history_known=out.decoder.history_known=known;
        }
        if(state->terminal_native_pages_attempted) {
            out.pages.history_checked=true;out.pages.history_known=target_terminal_native_page_history_known_(connection,receiving);
        }
        const auto live=[&](auto& root,const auto& source) {
            if(!source)return;
            source_identity(source);root.materialized=root.source_materialized=true;
            root.original=root.source_original=address(source.get());root.owner_count_observed=true;root.owner_count=source.use_count();
            root.source_failure_observed=true;root.source_failure=bool(source->first_failure);
            if constexpr(requires {source->calls;source->call_count;}) {
                root.native_call_count=source->call_count;
                if(source->call_count>source->calls.size())root.outstanding_native_call=true;
                else for(std::size_t at=0;at<source->call_count;++at)
                    if(source->calls[at].entered&&!source->calls[at].returned)root.outstanding_native_call=true;
            }
        };
        const auto history_tail=[&](auto& root,const auto& receipt) {
            root.binding_attempted=receipt.binding_attempted;root.binding_returned=receipt.binding_returned;
            root.extraction_attempted=receipt.extraction_attempted;root.extraction_returned=receipt.extraction_returned;
            root.release_attempted=receipt.release_attempted;root.release_returned=receipt.release_returned;
            root.members_destroyed=receipt.members_destroyed.load(std::memory_order_acquire);
            root.partial_constructor_destroyed=receipt.partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        // Before each first attempt only its actual live sources are read.
        // After ANY attempt, including failed/partial ones, only first records.
        if(state->terminal_decoder_attempted) {
            const auto& first=state->terminal_decoder_original;
            if(first)for(std::size_t at=0;at<2;++at)if(const auto& record=first->records[at]) {
                auto& root=out.decoder.roots[at];root.original=address(record->capsule_original);root.source_original=address(record->phase);
                root.materialized=bool(record->capsule_original);root.source_materialized=bool(record->phase);
                root.constructor_observed=true;root.constructor_returned=record->construction_returned;root.page_index=record->page_index;
                root.extraction_attempted=record->transfer_attempted;root.extraction_returned=record->transfer_returned;
                root.source_reset_attempted=record->source_reset_attempted;root.source_reset_returned=record->source_reset_returned;
                root.release_attempted=record->release_attempted;root.release_returned=record->release_returned;
                root.members_destroyed=record->payload_members_destroyed.load(std::memory_order_acquire);
                root.partial_constructor_destroyed=record->partial_constructor_destroyed.load(std::memory_order_acquire);
            }
        }else if(!state->terminal_native_phase_attempted&&!state->terminal_native_outer_attempted) {
            const auto decoded_source=[&](auto& root,const auto& outer) {
                if(!outer||!outer->active)return;
                const auto& phase=outer->active;source_identity(phase);
                root.source_original=address(phase.get());root.source_materialized=true;root.page_index=phase->page_index;
                root.source_constructor_observed=true;root.source_constructor_returned=phase->terminal_payload.construction_returned;
                root.source_failure_observed=true;root.source_failure=bool(phase->first_failure);
            };
            decoded_source(out.decoder.roots[0],state->history_chain);decoded_source(out.decoder.roots[1],state->row_import);
        }
        if(state->terminal_native_phase_attempted) {
            const auto& first=state->terminal_native_phase_original;
            if(first)for(std::size_t at=0;at<2;++at)if(const auto& record=first->records[at]) {
                auto& root=out.phases.roots[at];root.original=root.source_original=address(record->phase_original);
                root.materialized=root.source_materialized=true;root.constructor_observed=root.source_constructor_observed=true;root.constructor_returned=root.source_constructor_returned=record->constructor_returned;
                root.page_index=record->page_index;root.native_call_count=record->native_call_count;
                root.source_failure_observed=true;root.source_failure=bool(record->phase_first_failure);
                history_tail(root,*record);
            }
        }else if(!state->terminal_native_outer_attempted) {
            const auto phase_source=[&](auto& root,const auto& outer) {
                if(!outer)return;
                const auto& phase=outer->active;live(root,phase);if(!phase)return;
                root.constructor_observed=root.source_constructor_observed=true;root.constructor_returned=root.source_constructor_returned=phase->terminal_payload.construction_returned;root.page_index=phase->page_index;
            };
            phase_source(out.phases.roots[0],state->history_chain);phase_source(out.phases.roots[1],state->row_import);
        }
        if(state->terminal_native_outer_attempted) {
            const auto& first=state->terminal_native_outer_original;
            if(first)for(std::size_t at=0;at<2;++at)if(const auto& record=first->records[at]) {
                auto& root=out.outer.roots[at];root.original=root.source_original=address(record->outer_original);
                root.materialized=root.source_materialized=true;root.constructor_observed=root.source_constructor_observed=true;root.constructor_returned=root.source_constructor_returned=record->constructor_returned;
                root.source_failure_observed=true;root.source_failure=bool(record->first_native_failure);history_tail(root,*record);
            }
        }else {
            const auto outer_source=[&](auto& root,const auto& source) {
                live(root,source);if(source) {root.constructor_observed=root.source_constructor_observed=true;
                    root.constructor_returned=root.source_constructor_returned=source->terminal_outer.construction_returned;}
            };
            outer_source(out.outer.roots[0],state->history_chain);outer_source(out.outer.roots[1],state->row_import);
        }
        if(state->terminal_native_pages_attempted) {
            const auto& first=state->terminal_native_pages_original;
            if(first)for(std::size_t at=0;at<2;++at)if(const auto& record=first->records[at]) {
                auto& root=out.pages.roots[at];root.original=root.source_original=address(record->page_original);
                root.previous_original=address(record->previous_original);root.materialized=root.source_materialized=true;
                root.constructor_observed=root.source_constructor_observed=true;root.constructor_returned=root.source_constructor_returned=record->constructor_returned;root.page_index=record->page_index;
                root.source_failure_observed=true;root.source_failure=bool(record->first_native_failure);root.native_call_count=record->call_count;
                root.native_returns_observed=true;root.native_returns_known=record->native_returns_known();history_tail(root,*record);
                const auto& scope=record->scope_history;root.scope_history_observed=bool(record->scope_wrapper_original)&&scope.detached_known;
                root.scope_published=scope.published;root.scope_left=scope.left;root.scope_failed=scope.failed;
                root.scope_leave_returned=scope.leave_returned;root.scope_leave_joined=scope.leave_joined;root.scope_detached=scope.detached_known;
            }
        }else {
            const auto page_source=[&](auto& root,const auto& source) {
                live(root,source);if(!source)return;
                root.constructor_observed=root.source_constructor_observed=true;root.constructor_returned=root.source_constructor_returned=source->terminal_page.construction_returned;
                root.page_index=source->terminal_page.page_index;root.previous_original=address(source->previous.get());
            };
            const auto& current=state->history_page;page_source(out.pages.roots[0],current);
            if(current) {
                const auto& previous=current->previous;page_source(out.pages.roots[1],previous);
                out.unexpected_predecessor=previous&&(previous.get()==current.get()||bool(previous->previous));
            }
        }
        // Static-family suffix. Existing family fields/order stay unchanged.
        // After ANY static attempt not even header identity is read from a live
        // state root; the same actual first records are the only source.
        if(state->terminal_native_statics_attempted) {
            const auto& first=state->terminal_native_statics_original;
            const bool known=target_terminal_native_static_history_known_(connection,receiving);
            if(first) {if(first->owner!=&receiving)same_owner=false;else bound=true;}
            for(std::size_t at=0;at<4;++at) {
                auto& family=out.static_native[at];family.attempted=true;family.history_checked=true;family.history_known=known;
                family.allocation_exception_returned=state->terminal_native_statics_allocation_exception_returned;
                family.first_failure=bool(state->terminal_native_statics_first_failure);
                if(!first)continue; // Unknown first bank never falls back to still-held source objects.
                family.allocation_returned=first->record_allocation_returned[at];
                family.allocation_exception_returned=family.allocation_exception_returned||first->record_allocation_exception_returned[at];
                family.first_failure=family.first_failure||bool(first->first_failure);
                family.preparation_returned=first->preparation_returned;family.returned=first->slot_returned[at];
                family.result=static_cast<int>(first->slot_results[at]);
                const auto& record=first->records[at];if(!record)continue;
                family.extraction_attempted=record->extraction_attempted;family.extraction_returned=record->extraction_returned;
                auto& root=family.roots[0];root.original=root.source_original=address(record->object_original);
                root.materialized=root.source_materialized=true;root.constructor_observed=root.source_constructor_observed=true;
                root.constructor_returned=root.source_constructor_returned=record->constructor_returned;
                root.source_failure_observed=true;root.source_failure=bool(record->first_native_failure);root.native_call_count=record->call_count;
                root.native_returns_observed=true;root.native_returns_known=record->native_returns_known();history_tail(root,*record);
                const auto& scope=record->scope_history.common;
                root.scope_history_observed=bool(record->scope_wrapper_original)&&scope.detached_known;
                root.scope_published=scope.published;root.scope_left=scope.left;root.scope_failed=scope.failed;
                root.scope_leave_returned=scope.leave_returned;root.scope_leave_joined=scope.leave_joined;root.scope_detached=scope.detached_known;
                out.static_programme_failure_observed[at]=bool(record->programme_original);
                out.static_programme_failure[at]=bool(record->first_programme_failure);
            }
        }else {
            const auto static_source=[&]<std::size_t Index>(const auto& source) {
                if(!source)return;auto& root=out.static_native[Index].roots[0];
                if constexpr(Index==0) {
                    // Catalog had no receiving owner at construction. Bind only
                    // the actual managed worker/Conn's existing weak self now.
                    if(receiving.terminal_retirement_wake_original(connection).lock().get()!=&receiving||
                       source->terminal_static.state.lock()!=state||source->terminal_static.shell!=this)same_owner=false;
                    else bound=true;
                }else source_identity(source);
                root.materialized=root.source_materialized=true;root.original=root.source_original=address(source.get());
                root.constructor_observed=root.source_constructor_observed=true;
                root.constructor_returned=root.source_constructor_returned=source->terminal_static.construction_returned;
                root.owner_count_observed=true;root.owner_count=source.use_count();root.source_failure_observed=true;root.source_failure=bool(source->first_failure);
                root.native_call_count=source->call_count;
                if(source->call_count>source->calls.size())root.outstanding_native_call=true;
                else for(std::size_t at=0;at<source->call_count;++at)
                    if(source->calls[at].entered&&!source->calls[at].returned)root.outstanding_native_call=true;
                if constexpr(requires {source->generated;}) {
                    out.static_programme_failure_observed[Index]=bool(source->generated);
                    if(source->generated)out.static_programme_failure[Index]=bool(source->generated->first_failure);
                }else if constexpr(requires {source->program;}) {
                    out.static_programme_failure_observed[Index]=bool(source->program);
                    if(source->program)out.static_programme_failure[Index]=bool(source->program->first_failure);
                }
            };
            static_source.template operator()<0>(state->catalog);static_source.template operator()<1>(state->history_header);
            static_source.template operator()<2>(state->history_projection);static_source.template operator()<3>(state->import_suspension);
        }
        if(!same_owner)return {};
        out.receiving_owner_bound=bound;return out;
    }catch(...){return {};}
}
} // namespace lattice
#endif
