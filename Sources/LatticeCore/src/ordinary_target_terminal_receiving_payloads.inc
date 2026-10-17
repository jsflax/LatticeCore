// Later terminal payload ending. This first bank is independent of the live
// shell/scope observers: those observers are consumed BEFORE any extraction.
// It does not return a MAIN/IO/target slot or the receiving reservation.
struct ordinary_target_staging_connection::state::terminal_receiving_ending final {
    static constexpr std::size_t maximum_sources=2*(maximum_journals+1)+1;
    using custody=native_retirement_storage<std::array<std::shared_ptr<resource>,maximum_sources>,
        std::array<std::shared_ptr<ordinary_target_staging_io::environmental_phase>,2>,
        std::shared_ptr<::lattice::database>,std::shared_ptr<const ordinary_group_target_receiving_owner>>;
    using charge_custody=native_retirement_storage<std::shared_ptr<ordinary_resource_reservation<true>>>;
    charge_custody charge; // Last real release.
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const void* const state_original;const ordinary_target_staging_connection* const connection;
    const ordinary_group_target_receiving_owner* const owner;
    const std::weak_ptr<ordinary_group_target_receiving_owner> owner_original;
    const void* const io;const void* const target;const void* const scope_history;
    const ::lattice::database* const shell;
    const void* const charge_original;std::size_t peak_bytes=0;
    const ordinary_target_scope_native_fact native_first;
    custody objects;
    std::array<const void*,maximum_sources> sources{},source_objects{};
    std::array<bool,maximum_sources> extraction_attempted{},extraction_returned{},reset_attempted{},reset_returned{};
    std::size_t source_count=0;
    std::array<std::shared_ptr<ordinary_target_scope_root_record>,maximum_journals+1> resources{};
    std::size_t resource_count=0;
    std::array<std::shared_ptr<ordinary_target_scope_environment_record>,3> environments{};
    std::size_t environment_count=0;
    std::array<const void*,2> environment_sources{},environment_objects{};
    std::array<bool,2> environment_extraction_attempted{},environment_extraction_returned{},environment_reset_attempted{},environment_reset_returned{};
    std::shared_ptr<ordinary_target_scope_root_record> native_record,shell_record;
    std::shared_ptr<database_open_ledger> ledger;
    bool preparation_returned=false,resources_ended=false,environment_sources_ended=false;
    bool io_payloads_ended=false,shell_binding_attempted=false,shell_binding_returned=false;
    bool shell_extraction_attempted=false,shell_extraction_returned=false,shell_reset_attempted=false,shell_reset_returned=false;
    bool shell_ending_observation_attempted=false,shell_ending_observation_returned=false,shell_ending_known=false;
    unsigned elected_reason=0;bool elected_failure_observed=false,elected_edge_not_issued_known=false;
    bool elected_edge_issued=false,elected_edge_extraction_attempted=false,elected_edge_extraction_returned=false;
    bool elected_edge_reset_attempted=false,elected_edge_reset_returned=false;
    bool returned=false;std::exception_ptr first_failure;
    terminal_receiving_ending(const state& actual,const ordinary_target_staging_connection& facade,
        ordinary_group_target_receiving_owner& receiving,const terminal_scope_roots_ending& prior,
        const std::shared_ptr<ordinary_resource_reservation<true>>& token)
        :state_original(&actual),connection(&facade),owner(&receiving),
         owner_original(receiving.terminal_retirement_wake_original(facade)),io(actual.io.get()),target(actual.target.get()),
         scope_history(&prior),shell(actual.shell.get()),charge_original(token.get()),native_first(prior.native) {
        charge.template get<0>()=token;
    }
};

bool ordinary_target_staging_connection::terminal_io_payload_context(const ordinary_target_staging_io& io,
    const ordinary_group_target_receiving_owner& owner,
    std::array<std::shared_ptr<ordinary_target_scope_environment_record>,3>& environments,
    std::size_t& count,const void*& context)const noexcept {
    // A caller must supply empty output custody. No caller-owned graph is reset.
    if(count||context)return false;
    for(const auto& value:environments)if(value)return false;
    try {
        const auto& native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||native->io.get()!=&io||
           !native->receiving_ending_attempted||!native->receiving_allocation_returned||
           native->receiving_allocation_exception_returned||native->receiving_first_failure||!native->receiving_ending_original)return false;
        const auto& first=*native->receiving_ending_original;
        if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||
           first.state_original!=native.get()||first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner||
           first.io!=&io||first.target!=native->target.get()||first.scope_history!=native->scope_roots_original.get()||
           !first.charge.in_creator_process()||!first.objects.in_creator_process()||!first.preparation_returned||first.first_failure||
           !first.resources_ended||!first.environment_sources_ended||!first.environment_count||first.environment_count>3||
           first.peak_bytes>ordinary_history_limits::owner_bytes||!native->receiving_allocations.sealed||
           first.peak_bytes!=native->receiving_allocations.total_bytes||native->receiving_allocations.charge!=first.charge_original||
           !owner.terminal_scope_owner_matches(*this,first.charge_original))return false;
        for(std::size_t at=0;at<first.source_count;++at)
            if(!first.sources[at]||!first.source_objects[at]||!first.extraction_attempted[at]||!first.extraction_returned[at]||
               !first.reset_attempted[at]||!first.reset_returned[at])return false;
        for(std::size_t at=0;at<first.resource_count;++at) {
            const auto& record=first.resources[at];
            if(!record||record->kind!=3||!record->constructor_returned||!record->binding_attempted||!record->binding_returned||
               !record->members_destroyed||record->partial_constructor)return false;
        }
        for(std::size_t at=0;at<2;++at)
            if(!first.environment_sources[at]||!first.environment_objects[at]||!first.environment_extraction_attempted[at]||
               !first.environment_extraction_returned[at]||!first.environment_reset_attempted[at]||!first.environment_reset_returned[at])return false;
        for(std::size_t at=0;at<first.environment_count;++at) {
            const auto& record=first.environments[at];
            if(!record||record->connection!=this||!record->ending||record->ending->original!=record->original||record->ending->kind!=4||
               !record->ending->constructor_returned||!record->ending->binding_attempted||!record->ending->binding_returned)return false;
        }
        environments=first.environments;count=first.environment_count;context=&first;return true;
    }catch(...){return false;}
}
std::size_t ordinary_target_staging_connection::terminal_io_predecessor_peak_bytes(const ordinary_target_staging_io& io,
    const ordinary_group_target_receiving_owner& owner,const void* context)const noexcept {
    const auto& native=state_;
    if(!native||!native->creator_current()||!native->receiving_ending_original)return 0;
    const auto& first=*native->receiving_ending_original;
    if(!context||context!=&first||first.connection!=this||first.owner!=&owner||first.io!=&io||
       first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||
       !first.preparation_returned||first.first_failure||!first.resources_ended||!first.environment_sources_ended)return 0;
    return first.peak_bytes;
}

bool ordinary_target_staging_connection::retire_terminal_receiving_payloads(ordinary_group_target_receiving_owner& owner)noexcept {
    using ending=state::terminal_receiving_ending;
    const auto& native=state_;
    if(!native||!native->creator_current()||native->facade.lock().get()!=this)return false;
    if(native->receiving_ending_attempted)return terminal_receiving_payloads_ended(owner);
    if(!native->shell||!native->io||!native->target||!terminal_scope_roots_ended(owner)||!native->scope_roots_original)return false;
    const auto& prior=*native->scope_roots_original;
    const auto token=owner.terminal_scope_charge(*this);
    if(!token||prior.owner!=&owner||prior.connection!=this||prior.native.state==nullptr||prior.native.serial==0||
       prior.environment_count==0||prior.environment_count>3||prior.resources.size()>state::terminal_scope_roots_ending::maximum_resources)return false;
    std::shared_ptr<ending> first;
    try {
        // No owning alias is made by this census. Every surviving resource must
        // be entirely accounted for by these actual Conn source cells.
        std::array<std::shared_ptr<state::resource>*,ending::maximum_sources> cells{};std::size_t count=0;
        const auto add=[&](std::shared_ptr<state::resource>& value) {if(value)cells[count++]=&value;};
        for(auto& value:native->resources)add(value);
        for(auto& value:native->general_files.files)add(value.original);
        add(native->abandoned_main.original);
        for(std::size_t at=0;at<count;++at) {
            const auto& source=*cells[at];std::size_t aliases=0;bool recorded=false;
            for(std::size_t n=0;n<count;++n)if(cells[n]->get()==source.get()) {
                if(source.owner_before(*cells[n])||cells[n]->owner_before(source))return false;++aliases;
            }
            if(source.use_count()!=static_cast<long>(aliases))return false;
            for(const auto& record:prior.resources)if(record&&record->original==source.get()) {
                if(recorded||record->kind!=3||!record->constructor_returned||!record->binding_returned||record->members_destroyed||
                   record->partial_constructor||source->terminal_scope_root_.first_!=record)return false;
                recorded=true;
            }
            if(!recorded)return false;
        }
        std::size_t survivors=0;
        for(const auto& record:prior.resources) {
            if(!record||record->kind!=3||!record->constructor_returned||!record->binding_attempted||!record->binding_returned||record->partial_constructor)return false;
            bool present=false;for(std::size_t at=0;at<count;++at)if(cells[at]->get()==record->original)present=true;
            if(record->members_destroyed) {if(present)return false;}
            else {if(!present||++survivors>state::maximum_journals+1)return false;}
        }
        std::array<std::shared_ptr<ordinary_target_staging_io::environmental_phase>*,2> environment_cells{
            &native->terminal_vfs.environment_original,&native->terminal_scope.environment_original};
        for(const auto* cell:environment_cells) {
            if(!*cell)return false;bool matched=false;
            for(std::size_t at=0;at<prior.environment_count;++at)if(prior.environments[at]&&prior.environments[at]->original==cell->get())matched=true;
            if(!matched)return false;
        }
        if(native->shell.use_count()!=1||native->main_fd!=-1||native->terminal_scope.scope_original||native->last_scope||
           native->settling_scope||native->settling_callback_phase||native->scope_identity||!native->active_scope.expired()||
           !native->io->callbacks_joined(*this))return false;
        const auto& election=native->terminal_abandonment;
        if(!election.attempted||!election.seal_attempted||!election.seal_returned||!election.sealed||election.shell_original!=native->shell.get())return false;
        const bool elected_edge=election.reason==state::terminal_reason::receiving_owner_stop;
        if(elected_edge?(election.owner_original.get()!=&owner||bool(election.failure_original)):
            (election.reason!=state::terminal_reason::database_failure||!election.failure_original||bool(election.owner_original)))return false;
        constexpr auto allowance=ordinary_history_limits::owner_bytes;
        // The inline database tail has no extra heap request. Include the real
        // shell fixed object in this later logical peak, not an estimated tail.
        constexpr auto shell_fixed_bytes=sizeof(::lattice::database)+sizeof(database_open_ledger);
        if(prior.peak_bytes>allowance||shell_fixed_bytes>allowance-prior.peak_bytes)return false;
        auto& allocations=native->receiving_allocations;
        native->receiving_ending_attempted=true; // One shot, including allocation or partial binding failure.
        if(!allocations.begin(prior.peak_bytes+shell_fixed_bytes,token.get())||
           !allocations.reserve_fields(1,ending::charge_custody::allocation_bytes())||
           !allocations.reserve_fields(2,ending::custody::allocation_bytes()))return false;
        native->receiving_ending_original=std::allocate_shared<ending>(
            ordinary_target_terminal_allocation::allocator<ending>{allocations,0},*native,*this,owner,prior,token);
        native->receiving_allocation_returned=true;first=native->receiving_ending_original;
        allocations.fields_constructor_returned(1);allocations.fields_constructor_returned(2);
        first->source_count=count;first->environment_count=prior.environment_count;first->environments=prior.environments;
        using record_charge=decltype(ordinary_target_scope_root_record::charge);
        if(!allocations.reserve_fields(4,record_charge::allocation_bytes()))return false;
        first->native_record=std::allocate_shared<ordinary_target_scope_root_record>(
            ordinary_target_terminal_allocation::allocator<ordinary_target_scope_root_record>{allocations,3},prior.native.state,7,true,token);
        allocations.fields_constructor_returned(4);
        if(!allocations.reserve_fields(6,record_charge::allocation_bytes()))return false;
        first->shell_record=std::allocate_shared<ordinary_target_scope_root_record>(
            ordinary_target_terminal_allocation::allocator<ordinary_target_scope_root_record>{allocations,5},native->shell.get(),8,true,token);
        allocations.fields_constructor_returned(6);
        if(!allocations.finish(7))return false;first->peak_bytes=allocations.total_bytes;
        // All records and typed storage exist before the FIRST extraction.
        if(!first->charge.in_creator_process()||!first->objects.in_creator_process()||
           !owner.terminal_scope_owner_matches(*this,first->charge_original)||!terminal_scope_roots_ended(owner))return false;
        for(std::size_t at=0;at<count;++at){first->sources[at]=cells[at];first->source_objects[at]=cells[at]->get();}
        for(const auto& record:prior.resources)if(!record->members_destroyed)first->resources[first->resource_count++]=record;
        for(std::size_t at=0;at<2;++at){first->environment_sources[at]=environment_cells[at];first->environment_objects[at]=environment_cells[at]->get();}
        first->elected_reason=static_cast<unsigned>(election.reason);first->elected_failure_observed=bool(election.failure_original);
        first->elected_edge_issued=elected_edge;
        first->elected_edge_not_issued_known=!elected_edge&&election.reason==state::terminal_reason::database_failure&&bool(election.failure_original);
        first->preparation_returned=true;
        for(std::size_t at=0;at<count;++at) {
            first->extraction_attempted[at]=true;first->objects.template get<0>()[at]=std::move(*cells[at]);first->extraction_returned[at]=true;
        }
        for(std::size_t at=0;at<count;++at) {
            if(!native->creator_current())return false;
            first->reset_attempted[at]=true;first->objects.template get<0>()[at].reset();
            if(!native->creator_current())return false;first->reset_returned[at]=true;
        }
        for(std::size_t at=0;at<first->resource_count;++at) {
            const auto& record=first->resources[at];
            if(!record->members_destroyed||record->partial_constructor)return false;
        }
        first->resources_ended=true;
        for(std::size_t at=0;at<2;++at) {
            first->environment_extraction_attempted[at]=true;first->objects.template get<1>()[at]=std::move(*environment_cells[at]);
            first->environment_extraction_returned[at]=true;
            first->environment_reset_attempted[at]=true;first->objects.template get<1>()[at].reset();
            if(!native->creator_current())return false;first->environment_reset_returned[at]=true;
        }
        first->environment_sources_ended=true;
        // Q's producer has its own immutable first selection, actual environment
        // tails and guard/budget tails; a false result cannot be overwritten here.
        if(!native->io->retire_terminal_payloads(*this,owner)||!native->io->terminal_payloads_ended(*this,owner))return false;
        first->io_payloads_ended=true;
        first->shell_binding_attempted=true;
        first->shell_binding_returned=native->shell->bind_target_terminal_shell_destruction_(
            *this,first->native_first,first->native_record,first->shell_record,first->ledger);
        if(!first->shell_binding_returned)return false;
        first->shell_extraction_attempted=true;first->objects.template get<2>()=std::move(native->shell);first->shell_extraction_returned=true;
        first->shell_reset_attempted=true;first->objects.template get<2>().reset();
        if(!native->creator_current())return false;first->shell_reset_returned=true;
        first->shell_ending_observation_attempted=true;
        first->shell_ending_known=first->native_record->members_destroyed&&!first->native_record->partial_constructor&&
            first->shell_record->members_destroyed&&!first->shell_record->partial_constructor&&first->ledger&&
            first->ledger->fixed_scope_original_ended(first->native_first.slot,first->native_first.serial);
        first->shell_ending_observation_returned=true;
        if(!first->shell_ending_known)return false;
        if(first->elected_edge_issued) {
            first->elected_edge_extraction_attempted=true;
            first->objects.template get<3>()=std::move(native->terminal_abandonment.owner_original);
            first->elected_edge_extraction_returned=true;first->elected_edge_reset_attempted=true;first->objects.template get<3>().reset();
            if(!native->creator_current())return false;first->elected_edge_reset_returned=true;
        }
        first->returned=true;
        const bool known=terminal_receiving_payloads_ended(owner);
        owner.schedule_collection(); // After real tails/reset returns, outside every source/IO leaf.
        return known;
    }catch(...) {
        if(native->creator_current()) {
            if(native->receiving_ending_attempted&&!native->receiving_ending_original) {
                native->receiving_allocation_returned=true;native->receiving_allocation_exception_returned=true;
            }
            if(!native->receiving_first_failure)native->receiving_first_failure=std::current_exception();
            if(first&&!first->first_failure)first->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_receiving_payloads_ended(const ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto& native=state_;if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->receiving_ending_original||
        !native->receiving_allocation_returned||native->receiving_allocation_exception_returned||native->receiving_first_failure)return false;
    const auto& first=*native->receiving_ending_original;
    return first.process==ordinary_launch::current_process()&&first.thread==std::this_thread::get_id()&&first.state_original==native.get()&&
        first.connection==this&&first.owner==&owner&&first.owner_original.lock().get()==&owner&&first.preparation_returned&&!first.first_failure&&
        first.resources_ended&&first.environment_sources_ended&&first.io_payloads_ended&&first.shell_binding_attempted&&first.shell_binding_returned&&
        first.shell_extraction_attempted&&first.shell_extraction_returned&&first.shell_reset_attempted&&first.shell_reset_returned&&
        first.shell_ending_observation_attempted&&first.shell_ending_observation_returned&&first.shell_ending_known&&
        first.native_record&&first.native_record->members_destroyed&&!first.native_record->partial_constructor&&
        first.shell_record&&first.shell_record->members_destroyed&&!first.shell_record->partial_constructor&&
        (first.elected_edge_issued?(first.elected_reason==static_cast<unsigned>(state::terminal_reason::receiving_owner_stop)&&
            !first.elected_failure_observed&&!first.elected_edge_not_issued_known&&first.elected_edge_extraction_attempted&&
            first.elected_edge_extraction_returned&&first.elected_edge_reset_attempted&&first.elected_edge_reset_returned):
            (first.elected_reason==static_cast<unsigned>(state::terminal_reason::database_failure)&&first.elected_failure_observed&&
             first.elected_edge_not_issued_known&&!first.elected_edge_extraction_attempted&&!first.elected_edge_extraction_returned&&
             !first.elected_edge_reset_attempted&&!first.elected_edge_reset_returned))&&first.returned;
}

bool ordinary_target_staging_connection::terminal_receiving_payloads_selected()const noexcept {
    const auto& native=state_;return native&&native->creator_current()&&native->receiving_ending_attempted;
}
