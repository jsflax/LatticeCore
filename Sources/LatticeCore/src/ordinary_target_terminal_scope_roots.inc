// Actual retained wrapper/state/call-bank payload endings. First call DATA is
// separate from objects; pending environment/resource endings remain explicit.
struct ordinary_target_staging_connection::state::terminal_scope_roots_ending final {
    static constexpr std::size_t maximum_resources=3*maximum_calls+4*maximum_journals*4+65+65+1;
    struct callback_fact {
        bool issued=false;unsigned kind=0;std::uint64_t generation=0,cookie=0,previous_generation=0,flags=0;
        const void* borrower=nullptr;std::size_t previous_slot=64;int body_result=SQLITE_MISUSE;
        ordinary_target_scope_resource_fact first_resource;
        bool negative_held=false,short_held=false;
        ordinary_target_scope_call_fact negative,short_read;
    };
    struct scope_fact {
        const void* original=nullptr;const void* ledger=nullptr;const void* environment=nullptr;const void* predecessor=nullptr;
        const void* retirement=nullptr;const void* schema=nullptr;
        std::uint64_t generation=0,flags=0,callback_flags=0,active_generation=0,settling_generation=0,settling_high_generation=0;
        std::size_t active_slot=64,settling_slot=64;
        std::size_t aliases=0,retired_count=0,schema_finalizations=0,configuration_count=0;
        int schema_begin_rc=0,schema_commit_rc=0,schema_rollback_rc=0,open_rc=0,close_rc=0;
        const void* native=nullptr;std::array<int,20> configurations{};
        std::array<callback_fact,64> callbacks{};
        std::array<ordinary_target_scope_resource_fact,maximum_journals> retired{};
    };
    struct ledger_fact {
        const void* original=nullptr;const void* issuer=nullptr;std::size_t aliases=0,count=0;
        std::array<ordinary_target_scope_call_fact,maximum_calls> calls{};
    };
    using custody=native_retirement_storage<std::unique_ptr<database_scope>,std::unique_ptr<database_scope>,std::unique_ptr<database_scope>,
        std::shared_ptr<database_scope::scope_state>,std::shared_ptr<database_scope::scope_state>,
        std::shared_ptr<database_scope::scope_state>,std::shared_ptr<database_scope::scope_state>,
        std::shared_ptr<call_bank>,std::shared_ptr<call_bank>,std::shared_ptr<call_bank>>;
    // SAME token first, destroyed after retained scalar banks and unknown real custody.
    native_retirement_storage<std::shared_ptr<ordinary_resource_reservation<true>>> charge;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const void* const connection_state;const ordinary_target_staging_connection* const connection;
    const ordinary_group_target_receiving_owner* const owner;
    const std::weak_ptr<ordinary_group_target_receiving_owner> owner_original;
    const ::lattice::database* const shell;const void* const target;const void* const io;
    const ordinary_launch::deadline end;
    const ordinary_target_scope_native_fact native;
    const ordinary_target_scope_predecessor_fact predecessor;
    const void* const charge_original;const std::size_t peak_bytes;
    custody objects;
    std::array<const void*,3> wrappers{};
    std::array<const void*,6> source_cells{},source_objects{};
    std::array<std::size_t,6> source_indices{};
    std::array<bool,6> source_attempted{},source_returned{};
    std::array<bool,4> ledger_source_attempted{},ledger_source_returned{};
    std::array<scope_fact,4> scopes{};std::size_t scope_count=0;
    std::array<ledger_fact,3> ledgers{};std::size_t ledger_count=0;
    std::array<std::shared_ptr<ordinary_target_scope_root_record>,10> records{};
    std::vector<std::shared_ptr<ordinary_target_scope_root_record>> resources;
    std::array<std::shared_ptr<ordinary_target_scope_environment_record>,3> environments{};std::size_t environment_count=0;
    std::array<bool,10> extraction_attempted{},extraction_returned{},reset_attempted{},reset_returned{};
    bool preparation_returned=false,returned=false,ending_observation_attempted=false,ending_observation_returned=false,ending_known=false;
    std::exception_ptr first_failure;
    terminal_scope_roots_ending(const state& actual,const ordinary_target_staging_connection& conn,ordinary_group_target_receiving_owner& receiving,
        const ordinary_target_scope_native_fact& native_first,const ordinary_target_scope_predecessor_fact& prior,
        const std::shared_ptr<ordinary_resource_reservation<true>>& token,std::size_t bytes,std::size_t files)
        :connection_state(&actual),connection(&conn),owner(&receiving),owner_original(receiving.terminal_retirement_wake_original(conn)),
         shell(actual.shell.get()),target(actual.target.get()),io(actual.io.get()),end(actual.end),native(native_first),predecessor(prior),
         charge_original(token.get()),peak_bytes(bytes),resources(files) {charge.template get<0>()=token;}
};
bool ordinary_target_staging_connection::retire_terminal_scope_roots(ordinary_group_target_receiving_owner& owner)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell)return false;
    if(native->scope_roots_attempted)return terminal_scope_roots_ended(owner);
    if(!terminal_schema_roots_ended(owner))return false;
    return native->shell->retire_target_terminal_scope_roots_(*this,owner);
}
bool ordinary_target_staging_connection::retire_terminal_scope_roots_from_native(ordinary_group_target_receiving_owner& owner,
    const ordinary_target_scope_native_fact& native_first,std::unique_ptr<database_scope>* constructor,
    std::unique_ptr<database_scope>* schema,std::unique_ptr<database_scope>* cleanup)noexcept {
    using ending=state::terminal_scope_roots_ending;
    const auto native=state_;
    if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||!native->io||!native->target)return false;
    if(native->scope_roots_attempted)return terminal_scope_roots_ended(owner);
    ordinary_target_scope_predecessor_fact predecessor;
    const auto token=owner.terminal_scope_charge(*this);
    if(!token||!terminal_scope_predecessor_snapshot(owner,predecessor)||!terminal_scope_history_binding_known(*native->shell)||
       !native->shell->target_terminal_scope_root_context_matches_(*this,native_first)||
       !constructor||!schema||!cleanup||constructor==schema||constructor==cleanup||schema==cleanup||
       !*constructor||!*schema||!*cleanup||!(*cleanup)->held_||!(*cleanup)->held_->cleanup||
       (*cleanup)->held_!=native->terminal_scope.scope_original||native->last_scope!=(*cleanup)->held_||
       !(*cleanup)->held_->cleanup_predecessor||native->scope_identity||!native->active_scope.expired()||
       native->settling_scope||native->settling_callback_phase||!native->io->callbacks_joined(*this))return false;
    std::shared_ptr<ending> first;
    try {
        // No local shared scope/ledger/resource owner is created by this census.
        std::array<std::unique_ptr<database_scope>*,3> wrappers{constructor,schema,cleanup};
        std::array<std::shared_ptr<database_scope::scope_state>*,6> cells{
            &(*constructor)->held_,&(*schema)->held_,&(*cleanup)->held_,&native->last_scope,
            &native->terminal_scope.scope_original,&(*cleanup)->held_->cleanup_predecessor};
        std::array<database_scope::scope_state*,4> scopes{};std::size_t scope_count=0;
        const auto add_scope=[&](database_scope::scope_state* value) {
            if(!value)return false;
            for(std::size_t at=0;at<scope_count;++at)if(scopes[at]==value)return true;
            if(scope_count==scopes.size())return false;scopes[scope_count++]=value;return true;
        };
        if(!add_scope((*cleanup)->held_.get()))return false; // Cleanup state really resets before predecessor/other states.
        for(const auto* cell:cells)if(!add_scope(cell->get()))return false;
        std::array<state::call_bank*,3> ledgers{};std::size_t ledger_count=0;
        std::array<const std::shared_ptr<ordinary_target_staging_io::environmental_phase>*,3> environments{};std::size_t environment_count=0;
        std::array<state::resource*,ending::maximum_resources> resources{};std::size_t resource_count=0;
        const auto resource=[&](const std::shared_ptr<state::resource>& value) {
            if(!value)return true;
            if(resource_count==resources.size())return false;resources[resource_count++]=value.get();return true;
        };
        const auto tail_current=[&](const auto& tail,const void* object,unsigned kind) {
            return tail.process_==ordinary_launch::current_process()&&tail.thread_==std::this_thread::get_id()&&
                tail.original_==object&&tail.kind_==kind&&tail.construction_returned_&&!tail.first_;
        };
        for(std::size_t at=0;at<3;++at)if(!tail_current((*wrappers[at])->terminal_scope_root_,wrappers[at]->get(),0))return false;
        for(std::size_t at=0;at<scope_count;++at) {
            const auto* scope=scopes[at];std::size_t aliases=0;
            for(const auto* cell:cells)if(cell->get()==scope)++aliases;
            const std::shared_ptr<database_scope::scope_state>* canonical=nullptr;
            for(const auto* cell:cells)if(cell->get()==scope){canonical=cell;break;}
            if(!canonical||canonical->use_count()!=static_cast<long>(aliases)||!tail_current(scope->terminal_scope_root_,scope,1)||
               scope->original.lock()!=native||scope->shell!=native->shell.get()||scope->retirement.get()!=native_first.retirement||
               (scope->schema?scope->schema_identity!=predecessor.schema:scope->schema_identity!=nullptr)||
               !scope->published||!scope->generation||scope->generation>native->scope_generation||!scope->ledger||!scope->environment||
               scope->active_callback_slot!=64||scope->active_callback_generation||scope->settling_callback_slot!=64||
               scope->settling_callback_generation||scope->settling_callback_high_generation||scope->retired_journal_count>scope->retired_journals.size()||
               scope->configuration_count>scope->configurations.size()||(!scope->cleanup&&(!scope->left||!scope->leave_returned)))return false;
            if(scope->cleanup_predecessor&&scope!=scopes[0])return false;
            bool found=false;for(std::size_t n=0;n<ledger_count;++n)if(ledgers[n]==scope->ledger.get())found=true;
            if(!found){if(ledger_count==ledgers.size())return false;ledgers[ledger_count++]=scope->ledger.get();}
            found=false;for(std::size_t n=0;n<environment_count;++n)if(environments[n]->get()==scope->environment.get())found=true;
            if(!found){if(environment_count==environments.size())return false;environments[environment_count++]=&scope->environment;}
            for(const auto& file:scope->retired_journals)if(!resource(file))return false;
            for(const auto& held:scope->callback_originals)if(held) {
                const auto& callback=*held;
                if(!callback.generation||!callback.body_returned||!callback.leave_attempted||!callback.leave_returned||
                   (callback.previous_slot==64?callback.previous_generation!=0:callback.previous_slot>=64||!callback.previous_generation||callback.previous_generation>=callback.generation)||
                   !resource(callback.first_resource))return false;
                const auto call_known=[&](const auto& call) {
                    return !call||(call->attempted&&call->entered&&call->returned&&call->callback_generation==callback.generation&&
                        call->callback_slot<64&&call->original==callback.first_resource&&resource(call->original));
                };
                if(!call_known(callback.first_negative)||!call_known(callback.first_short_read))return false;
            }
        }
        for(std::size_t at=0;at<ledger_count;++at) {
            const auto* bank=ledgers[at];std::size_t aliases=0;const std::shared_ptr<state::call_bank>* canonical=nullptr;
            for(std::size_t n=0;n<scope_count;++n)if(scopes[n]->ledger.get()==bank){++aliases;canonical=&scopes[n]->ledger;}
            if(!canonical||canonical->use_count()!=static_cast<long>(aliases)||!tail_current(bank->terminal_scope_root_,bank,2)||
               !bank->issuer||bank->count>bank->calls.size())return false;
            bool issuer=false;for(std::size_t n=0;n<scope_count;++n)if(bank->issuer==scopes[n])issuer=true;if(!issuer)return false;
            for(std::size_t n=0;n<bank->count;++n) {
                const auto& call=bank->calls[n];
                if(!call.attempted||!call.original||call.resource_index>=native->resources.size()||
                   state::resource_slot(call.original->cookie)!=call.resource_index||
                   (call.entered&&!call.returned)||(!call.entered&&call.returned)||!resource(call.original))return false;
            }
        }
        for(const auto& file:native->resources)if(!resource(file))return false;
        for(const auto& receipt:native->general_files.files)if(!resource(receipt.original))return false;
        if(!resource(native->abandoned_main.original))return false;
        // Bounded raw-pointer census: sorting does not allocate or create aliases.
        std::sort(resources.begin(),resources.begin()+resource_count,std::less<state::resource*>{});
        resource_count=static_cast<std::size_t>(std::unique(resources.begin(),resources.begin()+resource_count)-resources.begin());
        for(std::size_t at=0;at<resource_count;++at)if(!tail_current(resources[at]->terminal_scope_root_,resources[at],3))return false;
        // Real logical peak under the EXISTING 8MiB owner allowance. No new reservation, cap increase, allocator/RSS or ending grant.
        constexpr std::size_t allowance=ordinary_history_limits::owner_bytes;
        std::size_t peak=sizeof(ending)+sizeof(resources)+sizeof(scopes)+sizeof(ledgers)+sizeof(cells)+sizeof(state)+
            4*sizeof(database_scope::scope_state)+3*sizeof(state::call_bank)+3*sizeof(database_scope)+
            native->fixed_main_path.capacity()+sizeof(state::terminal_raw_objects_ending)+5*(sizeof(ordinary_target_raw_object_record)+64)+
            10*(sizeof(ordinary_target_scope_root_record)+64)+3*(sizeof(ordinary_target_scope_environment_record)+sizeof(ordinary_target_scope_root_record)+128);
        const auto add=[&](std::size_t bytes){if(bytes>allowance||peak>allowance-bytes)return false;peak+=bytes;return true;};
        if(!add(native_first.retained_bytes)||!add(predecessor.retained_bytes)||
           !add(resource_count*(sizeof(ordinary_target_scope_root_record)+sizeof(std::shared_ptr<ordinary_target_scope_root_record>)+64)))return false;
        native->scope_roots_attempted=true; // First selection before first allocation/binding/extraction; never reset.
        native->scope_roots_original=std::shared_ptr<ending>(new ending(*native,*this,owner,native_first,predecessor,token,peak,resource_count));
        native->scope_roots_allocation_returned=true;first=native->scope_roots_original;
        if(!first->objects.in_creator_process()||!first->charge.in_creator_process()||first->resources.capacity()!=resource_count)return false;
        first->scope_count=scope_count;first->ledger_count=ledger_count;first->environment_count=environment_count;
        for(std::size_t at=0;at<3;++at) {
            first->wrappers[at]=wrappers[at]->get();first->records[at]=std::make_shared<ordinary_target_scope_root_record>(
                first->wrappers[at],0,(*wrappers[at])->terminal_scope_root_.construction_returned_,token);
        }
        for(std::size_t at=0;at<6;++at) {
            first->source_cells[at]=cells[at];first->source_objects[at]=cells[at]->get();
            for(std::size_t n=0;n<scope_count;++n)if(scopes[n]==cells[at]->get())first->source_indices[at]=n;
        }
        for(std::size_t at=0;at<scope_count;++at) {
            const auto& source=*scopes[at];auto& saved=first->scopes[at];saved.original=&source;saved.ledger=source.ledger.get();
            saved.environment=source.environment.get();saved.predecessor=source.cleanup_predecessor.get();saved.retirement=source.retirement.get();
            saved.schema=source.schema_identity;saved.generation=source.generation;
            for(const auto* cell:cells)if(cell->get()==&source)++saved.aliases;
            saved.flags=terminal_scope_fact_flags({source.cleanup,source.schema,source.catalog,source.history_header,source.history_page,
                source.projection,source.history_chain,source.import_suspension,source.row_import,source.ending,source.published,source.left,source.failed,
                source.schema_begin_observed,source.schema_commit_observed,source.schema_rollback_observed,source.schema_finalization_failed,
                source.leave_returned,source.leave_joined,source.open_observed,source.close_observed,source.native_files_joined,source.abandonment_environment_joined});
            saved.callback_flags=terminal_scope_fact_flags({source.callback_credit_sealed});saved.retired_count=source.retired_journal_count;
            saved.active_slot=source.active_callback_slot;saved.settling_slot=source.settling_callback_slot;
            saved.active_generation=source.active_callback_generation;saved.settling_generation=source.settling_callback_generation;
            saved.settling_high_generation=source.settling_callback_high_generation;
            saved.schema_finalizations=source.schema_finalizations;saved.configuration_count=source.configuration_count;saved.configurations=source.configurations;
            saved.schema_begin_rc=source.schema_begin_rc;saved.schema_commit_rc=source.schema_commit_rc;saved.schema_rollback_rc=source.schema_rollback_rc;
            saved.open_rc=source.open_rc;saved.close_rc=source.close_rc;saved.native=source.native;
            for(std::size_t n=0;n<source.retired_journals.size();++n)if(source.retired_journals[n])saved.retired[n]=terminal_scope_resource_fact(*source.retired_journals[n]);
            for(std::size_t n=0;n<64;++n)if(const auto& held=source.callback_originals[n]) {
                const auto& value=*held;auto& copy=saved.callbacks[n];copy.issued=true;copy.kind=static_cast<unsigned>(value.kind);
                copy.generation=value.generation;copy.cookie=value.cookie;copy.previous_generation=value.previous_generation;
                copy.borrower=value.borrower;copy.previous_slot=value.previous_slot;copy.body_result=value.body_result;
                copy.flags=terminal_scope_fact_flags({value.body_returned,value.body_exception_returned,value.leave_attempted,value.leave_returned,value.leave_joined,value.retained_semantic});
                if(value.first_resource)copy.first_resource=terminal_scope_resource_fact(*value.first_resource);
                if(value.first_negative){copy.negative_held=true;copy.negative=terminal_scope_call_fact(*value.first_negative);}
                if(value.first_short_read){copy.short_held=true;copy.short_read=terminal_scope_call_fact(*value.first_short_read);}
            }
            first->records[3+at]=std::make_shared<ordinary_target_scope_root_record>(&source,1,source.terminal_scope_root_.construction_returned_,token);
        }
        for(std::size_t at=0;at<ledger_count;++at) {
            const auto& source=*ledgers[at];auto& saved=first->ledgers[at];saved.original=&source;saved.issuer=source.issuer;saved.count=source.count;
            for(std::size_t n=0;n<scope_count;++n)if(scopes[n]->ledger.get()==&source)++saved.aliases;
            for(std::size_t n=0;n<source.count;++n)saved.calls[n]=terminal_scope_call_fact(source.calls[n]);
            first->records[7+at]=std::make_shared<ordinary_target_scope_root_record>(&source,2,source.terminal_scope_root_.construction_returned_,token);
        }
        for(std::size_t at=0;at<resource_count;++at) {
            const auto& source=*resources[at];auto record=std::make_shared<ordinary_target_scope_root_record>(&source,3,source.terminal_scope_root_.construction_returned_,token);
            record->resource=terminal_scope_resource_fact(source);first->resources[at]=std::move(record);
        }
        for(std::size_t at=0;at<environment_count;++at)
            if(!native->io->prepare_terminal_scope_environment_history(*this,*environments[at],token,first->environments[at]))return false;
        // ALL typed custody/scalar records allocated before the FIRST binding or member move.
        if(!owner.terminal_scope_owner_matches(*this,first->charge_original)||!terminal_scope_predecessor_matches(owner,first->predecessor)||
           !native->shell->target_terminal_scope_root_context_matches_(*this,first->native))return false;
        for(std::size_t at=0;at<3;++at)if(!(*wrappers[at])->terminal_scope_root_.bind(first->records[at]))return false;
        for(std::size_t at=0;at<scope_count;++at)if(!scopes[at]->terminal_scope_root_.bind(first->records[3+at]))return false;
        for(std::size_t at=0;at<ledger_count;++at)if(!ledgers[at]->terminal_scope_root_.bind(first->records[7+at]))return false;
        for(std::size_t at=0;at<resource_count;++at)if(!resources[at]->terminal_scope_root_.bind(first->resources[at]))return false;
        for(std::size_t at=0;at<environment_count;++at)
            if(!native->io->bind_terminal_scope_environment_history(*this,*environments[at],first->environments[at]))return false;
        first->preparation_returned=true;
        const auto move_wrapper=[&]<std::size_t Index>() {
            first->extraction_attempted[Index]=true;
            static_assert(noexcept(first->objects.template get<Index>()=std::move(*wrappers[Index])));
            first->objects.template get<Index>()=std::move(*wrappers[Index]);first->extraction_returned[Index]=true;
        };
        move_wrapper.template operator()<0>();move_wrapper.template operator()<1>();move_wrapper.template operator()<2>();
        const auto move_scope=[&]<std::size_t Index>() {
            if(Index>=scope_count)return;
            first->extraction_attempted[3+Index]=true;bool moved=false;
            for(std::size_t n=0;n<6;++n)if(first->source_indices[n]==Index) {
                first->source_attempted[n]=true;
                if(!moved){first->objects.template get<3+Index>()=std::move(*cells[n]);moved=true;}else cells[n]->reset();
                first->source_returned[n]=true;
            }
            first->extraction_returned[3+Index]=moved;
        };
        move_scope.template operator()<0>();move_scope.template operator()<1>();move_scope.template operator()<2>();move_scope.template operator()<3>();
        const auto move_ledger=[&]<std::size_t Index>() {
            if(Index>=ledger_count)return;
            first->extraction_attempted[7+Index]=true;bool moved=false;
            for(std::size_t n=0;n<scope_count;++n)if(scopes[n]->ledger.get()==ledgers[Index]) {
                first->ledger_source_attempted[n]=true;
                if(!moved){first->objects.template get<7+Index>()=std::move(scopes[n]->ledger);moved=true;}else scopes[n]->ledger.reset();
                first->ledger_source_returned[n]=true;
            }
            first->extraction_returned[7+Index]=moved;
        };
        move_ledger.template operator()<0>();move_ledger.template operator()<1>();move_ledger.template operator()<2>();
        // Raw census pointers and source-cell pointers are never dereferenced again.
        const auto release=[&]<std::size_t Index>() {
            if(!native->creator_current()||!first->objects.in_creator_process())return false;
            if(!first->records[Index])return true; // Unissued slot DATA, no ending fact.
            first->reset_attempted[Index]=true;
            if(!first->objects.template release<Index>()||!native->creator_current())return false;
            first->reset_returned[Index]=true;return true;
        };
        if(!release.template operator()<0>()||!release.template operator()<1>()||!release.template operator()<2>()||
           !release.template operator()<3>()||!release.template operator()<4>()||!release.template operator()<5>()||
           !release.template operator()<6>()||!release.template operator()<7>()||!release.template operator()<8>()||
           !release.template operator()<9>())return false;
        first->ending_observation_attempted=true;first->ending_known=terminal_scope_roots_history_known(owner);
        first->ending_observation_returned=true;first->returned=true;
        const bool ended=terminal_scope_roots_ended(owner);
        if(ended)owner.schedule_collection(); // AFTER actual tails and reset returns, outside every leaf.
        return ended;
    }catch(...) {
        if(native->creator_current()) {
            if(native->scope_roots_attempted&&!native->scope_roots_original) {
                native->scope_roots_allocation_returned=true;native->scope_roots_allocation_exception_returned=true;
            }
            if(!native->scope_roots_first_failure)native->scope_roots_first_failure=std::current_exception();
            if(first&&!first->first_failure)first->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_scope_roots_history_known(const ordinary_group_target_receiving_owner& owner)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||!native->scope_roots_attempted||
           !native->scope_roots_allocation_returned||native->scope_roots_allocation_exception_returned||native->scope_roots_first_failure||
           !native->scope_roots_original)return false;
        const auto& first=*native->scope_roots_original;
        if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||first.connection_state!=native.get()||
           first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner||first.shell!=native->shell.get()||
           first.target!=native->target.get()||first.io!=native->io.get()||first.end!=native->end||!first.objects.in_creator_process()||
           !first.charge.in_creator_process()||!first.preparation_returned||first.first_failure||!first.scope_count||first.scope_count>4||
           !first.ledger_count||first.ledger_count>3||!first.environment_count||first.environment_count>3||first.peak_bytes>ordinary_history_limits::owner_bytes||
           !owner.terminal_scope_owner_matches(*this,first.charge_original)||!terminal_scope_predecessor_matches(owner,first.predecessor)||
           !native->shell->target_terminal_scope_root_context_matches_(*this,first.native))return false;
        for(std::size_t at=0;at<10;++at) {
            const bool issued=at<3||(at<7?at-3<first.scope_count:at-7<first.ledger_count);
            const auto& record=first.records[at];if(!issued){if(record)return false;continue;}
            const void* original=at<3?first.wrappers[at]:(at<7?first.scopes[at-3].original:first.ledgers[at-7].original);
            const unsigned kind=at<3?0:(at<7?1:2);
            if(!record||record->original!=original||record->kind!=kind||!record->constructor_returned||
               !record->binding_attempted||!record->binding_returned||!record->members_destroyed||record->partial_constructor||
               !first.extraction_attempted[at]||!first.extraction_returned[at]||!first.reset_attempted[at]||!first.reset_returned[at])return false;
        }
        for(std::size_t at=0;at<6;++at)if(!first.source_cells[at]||!first.source_objects[at]||!first.source_attempted[at]||
            !first.source_returned[at]||first.source_indices[at]>=first.scope_count||
            first.source_objects[at]!=first.scopes[first.source_indices[at]].original)return false;
        for(std::size_t at=0;at<first.scope_count;++at)
            if(!first.ledger_source_attempted[at]||!first.ledger_source_returned[at]||!first.scopes[at].aliases||
               first.scopes[at].active_slot!=64||first.scopes[at].settling_slot!=64||first.scopes[at].active_generation||
               first.scopes[at].settling_generation||first.scopes[at].settling_high_generation)return false;
        // These records preserve possible implicit descriptor/environment endings,
        // but are NOT required ended and do NOT supply their aggregate join.
        for(const auto& record:first.resources)if(!record||record->kind!=3||!record->constructor_returned||
            !record->binding_attempted||!record->binding_returned||record->original!=record->resource.original)return false;
        for(std::size_t at=0;at<first.environment_count;++at) {
            const auto& record=first.environments[at];
            if(!record||!record->ending||record->connection!=this||record->ending->kind!=4||
               record->ending->original!=record->original||!record->ending->constructor_returned||
               !record->ending->binding_attempted||!record->ending->binding_returned)return false;
        }
        return true; // Actual wrapper/state/call-bank endings ONLY; original charge stays live.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_scope_roots_ended(const ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto native=state_;if(!native||!native->creator_current()||!native->scope_roots_original)return false;
    const auto& first=*native->scope_roots_original;
    return first.returned&&first.ending_observation_attempted&&first.ending_observation_returned&&first.ending_known&&
        terminal_scope_roots_history_known(owner); // Unknown first whole-history result never repairs.
}
bool ordinary_target_staging_connection::terminal_scope_roots_shell_history_known(const ::lattice::database& actual)const noexcept {
    const auto native=state_;if(!native||!native->creator_current()||native->shell.get()!=&actual||!native->scope_roots_original)return false;
    const auto& first=*native->scope_roots_original;const auto owner=first.owner_original.lock();
    return owner&&owner.get()==first.owner&&terminal_scope_roots_ended(*owner);
}
bool ordinary_target_staging_connection::terminal_scope_root_identity_matches(const ordinary_group_target_receiving_owner& owner,
    const void* scope,std::uint64_t generation)const noexcept {
    const auto native=state_;if(!native||!native->creator_current()||native->facade.lock().get()!=this||!scope||!generation)return false;
    if(!native->scope_roots_attempted)return scope==native->terminal_scope.scope_original.get()&&generation==native->terminal_scope.generation;
    const auto& first=native->scope_roots_original;
    return first&&first->predecessor.scope==scope&&first->predecessor.generation==generation&&terminal_scope_roots_ended(owner);
}

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
ordinary_target_scope_root_observation ordinary_target_staging_connection::qualification_scope_root_observation(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    ordinary_target_scope_root_observation out;
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||
           owner.terminal_retirement_wake_original(*this).lock().get()!=&owner)return out;
        if(!native->scope_roots_attempted)return native->shell->qualification_target_scope_root_observation_(*this,owner);
        out.flags=3|(std::uint64_t{1}<<2)|(std::uint64_t{1}<<11);
        if(native->scope_roots_allocation_returned)out.flags|=std::uint64_t{1}<<3;
        if(native->scope_roots_allocation_exception_returned)out.flags|=std::uint64_t{1}<<4;
        if(native->scope_roots_first_failure)out.flags|=std::uint64_t{1}<<5;
        const auto& held=native->scope_roots_original;if(!held)return out;
        const auto& first=*held;
        if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||first.connection_state!=native.get()||
           first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner)return {};
        const auto set=[&](unsigned bit,bool fact){if(fact)out.flags|=std::uint64_t{1}<<bit;};
        set(5,bool(first.first_failure));set(6,first.preparation_returned);set(7,first.returned);
        set(8,first.ending_observation_attempted);set(9,first.ending_observation_returned);set(10,first.ending_known);
        set(12,terminal_scope_roots_ended(owner));out.peak_bytes=first.peak_bytes;out.scope_count=first.scope_count;out.ledger_count=first.ledger_count;
        out.resource_count=first.resources.size();out.environment_count=first.environment_count;
        for(std::size_t at=0;at<10;++at)if(const auto& record=first.records[at]) {
            auto& root=out.roots[at];root.original=reinterpret_cast<std::uintptr_t>(record->original);
            root.aliases=at<3?1:(at<7?first.scopes[at-3].aliases:first.ledgers[at-7].aliases);
            root.flags=terminal_scope_fact_flags({true,true,record->constructor_returned,true,record->binding_attempted,record->binding_returned,
                first.extraction_attempted[at],first.extraction_returned[at],first.reset_attempted[at],first.reset_returned[at],
                record->members_destroyed,record->partial_constructor});
        }
        for(std::size_t at=0;at<6;++at)out.sources[at]=(first.source_attempted[at]?1u:0u)|(first.source_returned[at]?2u:0u);
        for(std::size_t at=0;at<4;++at)out.ledger_sources[at]=(first.ledger_source_attempted[at]?1u:0u)|(first.ledger_source_returned[at]?2u:0u);
        return out; // After ANY attempt only actual retained first records; no live source fallback.
    }catch(...){return {};}
}
ordinary_target_scope_root_observation ordinary_target_staging_connection::qualification_scope_root_observation_from_native(
    const ordinary_group_target_receiving_owner& owner,const std::unique_ptr<database_scope>* constructor,
    const std::unique_ptr<database_scope>* schema,const std::unique_ptr<database_scope>* cleanup)const noexcept {
    ordinary_target_scope_root_observation out;
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->scope_roots_attempted||native->facade.lock().get()!=this||
           owner.terminal_retirement_wake_original(*this).lock().get()!=&owner||!constructor||!schema||!cleanup)return out;
        out.flags=3;
        const auto read=[&](const auto& tail,const void* original,unsigned kind,auto& root,std::uint64_t aliases) {
            if(tail.process_!=ordinary_launch::current_process()||tail.thread_!=std::this_thread::get_id()||
               tail.original_!=original||tail.kind_!=kind)return false;
            root.original=reinterpret_cast<std::uintptr_t>(original);root.aliases=aliases;
            root.flags=terminal_scope_fact_flags({true,true,tail.construction_returned_,true});return true;
        };
        std::array<const std::unique_ptr<database_scope>*,3> wrappers{constructor,schema,cleanup};
        std::array<const std::shared_ptr<database_scope::scope_state>*,6> cells{};
        for(std::size_t at=0;at<3;++at)if(*wrappers[at]) {
            if(!read((*wrappers[at])->terminal_scope_root_,wrappers[at]->get(),0,out.roots[at],1))return {};
            cells[at]=&(*wrappers[at])->held_;
        }
        cells[3]=&native->last_scope;cells[4]=&native->terminal_scope.scope_original;
        if(*cleanup&&(*cleanup)->held_)cells[5]=&(*cleanup)->held_->cleanup_predecessor;
        std::array<const std::shared_ptr<database_scope::scope_state>*,4> scopes{};std::size_t count=0;
        const auto add=[&](const auto* cell) {
            if(!cell||!*cell)return true;
            for(std::size_t at=0;at<count;++at)if(scopes[at]->get()==cell->get())return true;
            if(count==scopes.size())return false;scopes[count++]=cell;return true;
        };
        if(!add(cells[2]?cells[2]:cells[3]))return {};
        for(const auto* cell:cells)if(!add(cell))return {};
        std::array<const std::shared_ptr<state::call_bank>*,3> ledgers{};std::size_t banks=0;
        for(std::size_t at=0;at<count;++at) {
            const auto& value=*scopes[at];
            if(value->original.lock()!=native||value->shell!=native->shell.get()||
               !read(value->terminal_scope_root_,value.get(),1,out.roots[3+at],value.use_count()))return {};
            if(!value->ledger)continue;bool found=false;
            for(std::size_t n=0;n<banks;++n)if(ledgers[n]->get()==value->ledger.get())found=true;
            if(!found){if(banks==ledgers.size())return {};ledgers[banks++]=&value->ledger;}
        }
        for(std::size_t at=0;at<banks;++at)if(!read((*ledgers[at])->terminal_scope_root_,ledgers[at]->get(),2,
            out.roots[7+at],ledgers[at]->use_count()))return {};
        out.scope_count=count;out.ledger_count=banks;return out;
    }catch(...){return {};}
}
#endif
