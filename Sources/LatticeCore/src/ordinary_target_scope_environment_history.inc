// Scalar first-history and real member-tail prerequisite only. This does not
// retire/re-register any environment, VFS, provider, target, file or charge.
bool ordinary_target_staging_io::prepare_terminal_scope_environment_history(
    const ordinary_target_staging_connection& actual,const std::shared_ptr<environmental_phase>& phase,
    const std::shared_ptr<ordinary_resource_reservation<true>>& charge,
    std::shared_ptr<ordinary_target_scope_environment_record>& output)const noexcept {
    try {
        const auto data=state_;
        if(!data||!data->creator_current()||data->connection_identity!=&actual||data->connection.lock().get()!=&actual||
           !data->guard||!phase||!phase->creator_current()||phase->connection!=&actual||!charge||output)return false;
        const auto& tail=phase->terminal_scope_root_;
        if(tail.process_!=ordinary_launch::current_process()||tail.thread_!=std::this_thread::get_id()||tail.original_!=phase.get()||
           tail.kind_!=4||!tail.construction_returned_||tail.first_)return false;
        auto record=std::make_shared<ordinary_target_scope_environment_record>();
        record->ending=std::shared_ptr<ordinary_target_scope_root_record>(new ordinary_target_scope_root_record(
            phase.get(),4,tail.construction_returned_,charge));
        const auto guard=data->guard;if(!guard->current()||guard->original.lock()!=data)return false;
        {std::lock_guard lock(guard->mutex);
            const auto& retired=guard->terminal_registration;
            if(guard->callbacks||guard->environment||guard->registering||guard->registered||guard->retiring||!guard->retired||
               !guard->unregister_attempted||!guard->unregister_returned||guard->unregister_rc!=SQLITE_OK||guard->registration_custody||
               !retired.attempted||!retired.evidence_attempted||!retired.evidence_returned||!retired.evidence_known||
               !retired.native_entered||!retired.native_returned||retired.native_result!=SQLITE_OK||!retired.publication_attempted||
               !retired.publication_returned||!retired.custody_detachment_attempted||!retired.custody_detachment_returned||
               !retired.custody_release_attempted||!retired.custody_release_returned||!retired.returned||retired.first_failure||
               retired.connection_original!=&actual||retired.guard_original!=guard.get()||retired.vfs_original!=&guard->vfs||
               retired.registration_identity!=guard->registration_identity||retired.registration_identity!=data->policy_ordinal+1||
               phase->count>phase->calls.size())return false;
            const auto& ending=phase->terminal_ending;
            const bool ordinary=phase->settled&&phase->settlement_generation;
            const bool terminal=!phase->settled&&guard->terminal_environment==phase&&retired.environment_original==phase&&
                ending.attempted&&ending.evidence_returned&&ending.evidence_known&&ending.cleanup_generation==retired.generation&&
                ending.detachment_attempted&&ending.detachment_returned&&ending.detached&&ending.returned&&ending.joined;
            if(!ordinary&&!terminal)return false;
            record->original=phase.get();record->connection=&actual;record->generation=phase->generation;
            record->settlement_generation=phase->settlement_generation;record->cleanup_generation=ending.cleanup_generation;
            record->flags=terminal_scope_fact_flags({phase->settled,ending.attempted,ending.evidence_returned,ending.evidence_known,
                ending.detachment_attempted,ending.detachment_returned,ending.detached,ending.returned,ending.joined});
            record->count=phase->count;
            for(std::size_t at=0;at<phase->count;++at) {
                const auto& source=phase->calls[at];auto& first=record->calls[at];
                if(!source.attempted||source.kind<1||source.kind>4||((source.kind==1||source.kind==2)&&source.argument<0)||
                   ((source.kind==3||source.kind==4)&&source.argument!=0)||
                   (source.entered&&(!source.returned||!source.callback_generation))||(!source.entered&&source.returned))return false;
                first={source.kind,source.argument,source.result,source.callback_generation,
                    terminal_scope_fact_flags({source.attempted,source.entered,source.returned})};
            }}
        output=std::move(record);return true; // Allocation/snapshot only; tail binding is a separate allocation-free step.
    }catch(...){return false;}
}
bool ordinary_target_staging_io::bind_terminal_scope_environment_history(
    const ordinary_target_staging_connection& actual,const std::shared_ptr<environmental_phase>& phase,
    const std::shared_ptr<ordinary_target_scope_environment_record>& first)noexcept {
    try {
        const auto data=state_;
        if(!data||!data->creator_current()||data->connection_identity!=&actual||data->connection.lock().get()!=&actual||
           !data->guard||!phase||!phase->creator_current()||phase->connection!=&actual||!first||!first->ending||
           first->original!=phase.get()||first->connection!=&actual||first->generation!=phase->generation||
           first->count!=phase->count||first->count>phase->calls.size())return false;
        const auto guard=data->guard;if(!guard->current())return false;
        std::lock_guard lock(guard->mutex);
        if(guard->callbacks||guard->environment||guard->registering||guard->registered||guard->retiring||!guard->retired)return false;
        const auto& ending=phase->terminal_ending;
        if(first->settlement_generation!=phase->settlement_generation||first->cleanup_generation!=ending.cleanup_generation||
           first->flags!=terminal_scope_fact_flags({phase->settled,ending.attempted,ending.evidence_returned,ending.evidence_known,
               ending.detachment_attempted,ending.detachment_returned,ending.detached,ending.returned,ending.joined}))return false;
        for(std::size_t at=0;at<phase->count;++at) {
            const auto& source=phase->calls[at];const auto& saved=first->calls[at];
            if(saved.kind!=source.kind||saved.argument!=source.argument||saved.result!=source.result||saved.callback_generation!=source.callback_generation||
               saved.flags!=terminal_scope_fact_flags({source.attempted,source.entered,source.returned}))return false;
        }
        return phase->terminal_scope_root_.bind(first->ending);
    }catch(...){return false;}
}
