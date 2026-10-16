// The final ticket contains typed first custody until actual destruction, then
// only bounded original-result DATA. No source, target or completion factory.
struct ordinary_target_staging_connection::terminal_collection_record final {
    using custody=native_retirement_storage<std::array<std::shared_ptr<ordinary_target_staging_connection>,3>,
        std::array<std::shared_ptr<ordinary_target_staging_io>,3>,
        std::array<std::shared_ptr<const ordinary_unpublished_group_target>,3>,
        std::shared_ptr<ordinary_target_schema_roots_ending>,std::shared_ptr<ordinary_resource_reservation<true>>>;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const ordinary_group_target_receiving_owner* const owner;
    const ordinary_target_staging_connection* const connection;
    const void* const connection_state;const void* const io;const void* const target;const void* const charge;
    const void* const schema_history;const std::size_t slot;const std::uint64_t serial;
    std::size_t peak_bytes=0;
    custody objects;
    std::array<ordinary_target_terminal_allocation::request,8> payload_requests{},collection_requests{};
    std::size_t payload_predecessor_bytes=0,io_augmented_bytes=0;
    bool allocation_history_known=false;
    std::shared_ptr<ordinary_target_scope_root_record> facade_record,state_record,target_record;
    std::shared_ptr<const ordinary_target_staging_io::terminal_retirement_record> io_record;
    std::array<const void*,9> source_cells{};
    std::array<bool,9> extraction_attempted{},extraction_returned{},reset_attempted{},reset_returned{};
    bool preparation_returned=false,io_registry_detach_attempted=false,io_registry_detach_returned=false;
    bool main_registry_detach_attempted=false,main_registry_detach_returned=false;
    bool facade_tail_observed=false,state_tail_observed=false,io_ending_observed=false,target_tail_observed=false;
    bool registry_return_attempted=false,registry_returned=false;
    bool schema_history_extraction_attempted=false,schema_history_extraction_returned=false;
    bool schema_history_reset_attempted=false,schema_history_reset_returned=false;
    bool tail_records_reset_attempted=false,tail_records_reset_returned=false;
    bool charge_extraction_attempted=false,charge_extraction_returned=false,charge_reset_attempted=false,charge_reset_returned=false;
    bool returned=false;std::exception_ptr first_failure;
    terminal_collection_record(const state& native,const ordinary_target_staging_connection& facade,
        const ordinary_group_target_receiving_owner& receiving,const void* schema,const void* token)
        :owner(&receiving),connection(&facade),connection_state(&native),io(native.io.get()),target(native.target.get()),
         charge(token),schema_history(schema),slot(native.registry_slot),serial(native.registry_serial){}
};

bool ordinary_target_staging_connection::retire_terminal_collection(ordinary_group_target_receiving_owner& owner,
    std::shared_ptr<terminal_collection_record>& output,
    bool& selected,std::exception_ptr& allocation_failure,
    std::shared_ptr<ordinary_target_staging_connection>& owner_connection,std::shared_ptr<ordinary_target_staging_connection>& request_connection,
    std::shared_ptr<ordinary_target_staging_io>& owner_io,std::shared_ptr<ordinary_target_staging_io>& request_io,
    std::shared_ptr<const ordinary_unpublished_group_target>& owner_target,std::shared_ptr<const ordinary_unpublished_group_target>& request_target,
    std::shared_ptr<ordinary_target_schema_roots_ending>& schema_history,std::shared_ptr<ordinary_resource_reservation<true>>& charge)noexcept {
    const auto process=ordinary_launch::current_process();
    static_assert(sizeof(terminal_collection_record)<=2048);
    if(selected)return terminal_collection_ended(output,&owner); // Unknown first attempt is never retried.
    auto* facade=owner_connection.get();
    if(!facade||!facade->terminal_receiving_payloads_ended(owner)||!owner_io||!owner_target||!schema_history||!charge)return false;
    std::shared_ptr<terminal_collection_record> first;
    try {
        {
            // This entire live preflight ends before the first facade reset.
            // No local strong facade/state/target aliases are manufactured.
            const auto& native=facade->state_;
            if(!native||!native->creator_current()||native.use_count()!=1||!native->receiving_ending_original)return false;
            const auto& prior=*native->receiving_ending_original;
            if(prior.owner!=&owner||prior.charge_original!=charge.get()||prior.io!=owner_io.get()||prior.target!=owner_target.get()||
               native->io!=owner_io||native->target!=owner_target||prior.scope_history!=native->scope_roots_original.get()||
               native->scope_roots_original->predecessor.roots!=schema_history.get()||
               owner_connection!=request_connection||owner_io!=request_io||owner_target!=request_target)return false;
            const auto same_control=[](const auto& left,const auto& right) {
                return left&&right&&left.get()==right.get()&&!left.owner_before(right)&&!right.owner_before(left);
            };
            if(!same_control(owner_connection,request_connection)||!same_control(owner_io,request_io)||
               !same_control(owner_target,request_target)||!same_control(owner_target,native->target))return false;
            // Two exact caller cells plus the real MAIN registry facade. IO
            // still has its original Conn-state reference until Conn dies.
            if(owner_connection.use_count()!=3||owner_io.use_count()!=4||owner_target.use_count()!=5)return false;
            auto& owners=registry();
            {std::lock_guard lock(owners.mutex);
                if(native->registry_slot>=owners.originals.size()||!owners.occupied[native->registry_slot]||
                   owners.serials[native->registry_slot]!=native->registry_serial||
                   !same_control(owner_connection,owners.originals[native->registry_slot])||
                   !same_control(owner_target,owners.targets[native->registry_slot]))return false;}
            const auto tail_current=[](const auto& tail,const void* original,unsigned kind) {
                return tail.process_==ordinary_launch::current_process()&&tail.thread_==std::this_thread::get_id()&&
                    tail.original_==original&&tail.kind_==kind&&tail.construction_returned_&&!tail.first_;
            };
            if(!tail_current(facade->terminal_receiving_tail_,facade,5)||!tail_current(native->terminal_receiving_tail_,native.get(),6)||
               !tail_current(owner_target->terminal_receiving_tail_,owner_target.get(),9))return false;
            const auto augmented_peak=owner_io->terminal_payload_peak_bytes(*facade,owner);
            if(!augmented_peak||augmented_peak<prior.peak_bytes||augmented_peak>ordinary_history_limits::owner_bytes)return false;
            auto& allocations=native->collection_allocations;
            // First immutable selection precedes reservation or allocation.
            // Q's actual augmented peak includes its still-live IO ticket.
            selected=true;
            if(!allocations.begin(augmented_peak,charge.get())||
               !allocations.reserve_fields(1,terminal_collection_record::custody::allocation_bytes()))return false;
            output=std::allocate_shared<terminal_collection_record>(ordinary_target_terminal_allocation::allocator<terminal_collection_record>{allocations,0},
                *native,*facade,owner,schema_history.get(),charge.get());
            first=output;allocations.fields_constructor_returned(1);
            using record_charge=decltype(ordinary_target_scope_root_record::charge);
            if(!allocations.reserve_fields(3,record_charge::allocation_bytes()))return false;
            first->facade_record=std::allocate_shared<ordinary_target_scope_root_record>(
                ordinary_target_terminal_allocation::allocator<ordinary_target_scope_root_record>{allocations,2},facade,5,facade->terminal_receiving_tail_.construction_returned_,charge);
            allocations.fields_constructor_returned(3);
            if(!allocations.reserve_fields(5,record_charge::allocation_bytes()))return false;
            first->state_record=std::allocate_shared<ordinary_target_scope_root_record>(
                ordinary_target_terminal_allocation::allocator<ordinary_target_scope_root_record>{allocations,4},native.get(),6,native->terminal_receiving_tail_.construction_returned_,charge);
            allocations.fields_constructor_returned(5);
            if(!allocations.reserve_fields(7,record_charge::allocation_bytes()))return false;
            first->target_record=std::allocate_shared<ordinary_target_scope_root_record>(
                ordinary_target_terminal_allocation::allocator<ordinary_target_scope_root_record>{allocations,6},owner_target.get(),9,owner_target->terminal_receiving_tail_.construction_returned_,charge);
            allocations.fields_constructor_returned(7);
            if(!allocations.finish(8))return false;first->peak_bytes=allocations.total_bytes;
            first->payload_requests=native->receiving_allocations.requests;
            first->payload_predecessor_bytes=native->receiving_allocations.predecessor_bytes;
            first->collection_requests=allocations.requests;first->io_augmented_bytes=augmented_peak;
            first->allocation_history_known=native->receiving_allocations.sealed&&allocations.sealed;

            first->io_record=owner_io->terminal_retirement_original(*facade,owner);
            if(!first->objects.in_creator_process()||!first->io_record)return false;
            first->source_cells={&owner_connection,&request_connection,nullptr,&owner_io,&request_io,nullptr,&owner_target,&request_target,nullptr};
            if(!facade->terminal_receiving_tail_.bind(first->facade_record)||!native->terminal_receiving_tail_.bind(first->state_record)||
               !owner_target->terminal_receiving_tail_.bind(first->target_record))return false;
            first->preparation_returned=true;
            first->io_registry_detach_attempted=true;
            first->io_registry_detach_returned=owner_io->detach_terminal_registry(*facade,owner,first->objects.template get<1>()[2]);
            if(!first->io_registry_detach_returned)return false;
            // Slot 5 is the separately owned IO registry producer. Its opaque
            // ticket proves the actual move; no inaccessible cell address is invented.
            // MAIN reservation remains occupied/charged. Move only its exact
            // facade/target references, never a destructor under the leaf.
            first->main_registry_detach_attempted=true;
            {std::lock_guard lock(owners.mutex);const auto at=first->slot;
                if(!owners.occupied[at]||owners.serials[at]!=first->serial||owners.originals[at].get()!=facade||owners.targets[at].get()!=first->target)return false;
                first->source_cells[2]=&owners.originals[at];first->source_cells[8]=&owners.targets[at];
                first->extraction_attempted[2]=first->extraction_attempted[8]=true;
                first->objects.template get<0>()[2]=std::move(owners.originals[at]);
                first->objects.template get<2>()[2]=std::move(owners.targets[at]);
                first->extraction_returned[2]=first->extraction_returned[8]=true;}
            first->main_registry_detach_returned=true;
            first->extraction_attempted[5]=first->extraction_returned[5]=true; // Exact IO producer already returned this move.
        }
        const auto local=[&] {return first->process==ordinary_launch::current_process()&&first->thread==std::this_thread::get_id()&&first->objects.in_creator_process();};
        if(!local())return false;
        auto& connections=first->objects.template get<0>();auto& ios=first->objects.template get<1>();auto& targets=first->objects.template get<2>();
        first->extraction_attempted[0]=true;connections[0]=std::move(owner_connection);first->extraction_returned[0]=true;
        first->extraction_attempted[1]=true;connections[1]=std::move(request_connection);first->extraction_returned[1]=true;
        for(std::size_t at=0;at<3;++at) {
            first->reset_attempted[at]=true;connections[at].reset();if(!local())return false;first->reset_returned[at]=true;
        }
        // The facade and its state are dead here. All subsequent observations
        // use independently retained exact first tails/tickets, not live APIs.
        first->facade_tail_observed=first->facade_record->members_destroyed&&!first->facade_record->partial_constructor;
        first->state_tail_observed=first->state_record->members_destroyed&&!first->state_record->partial_constructor;
        if(!first->facade_tail_observed||!first->state_tail_observed)return false;
        if(owner_io.use_count()!=3||owner_target.use_count()!=4)return false;
        first->extraction_attempted[3]=true;ios[0]=std::move(owner_io);first->extraction_returned[3]=true;
        first->extraction_attempted[4]=true;ios[1]=std::move(request_io);first->extraction_returned[4]=true;
        for(std::size_t at=0;at<3;++at) {
            first->reset_attempted[3+at]=true;ios[at].reset();if(!local())return false;first->reset_returned[3+at]=true;
        }
        first->io_ending_observed=ordinary_target_staging_io::terminal_retirement_ended(first->io_record,first->connection,&owner);
        if(!first->io_ending_observed||owner_target.use_count()!=3)return false;
        first->extraction_attempted[6]=true;targets[0]=std::move(owner_target);first->extraction_returned[6]=true;
        first->extraction_attempted[7]=true;targets[1]=std::move(request_target);first->extraction_returned[7]=true;
        for(std::size_t at=0;at<3;++at) {
            first->reset_attempted[6+at]=true;targets[at].reset();if(!local())return false;first->reset_returned[6+at]=true;
        }
        first->target_tail_observed=first->target_record->members_destroyed&&!first->target_record->partial_constructor;
        if(!first->target_tail_observed)return false;
        first->registry_return_attempted=true;
        auto& owners=registry();
        {std::lock_guard lock(owners.mutex);const auto at=first->slot;
            if(at>=owners.originals.size()||!owners.occupied[at]||owners.serials[at]!=first->serial||
               owners.originals[at]||owners.targets[at]||!owners.charged)return false;
            // Positive actual Conn/IO/target endings above authorize this exact
            // slot debit. Empty cells alone can never reach this producer.
            owners.occupied[at]=false;--owners.charged;first->registry_returned=true;}
        if(schema_history.get()!=first->schema_history||schema_history.use_count()!=1)return false;
        first->schema_history_extraction_attempted=true;first->objects.template get<3>()=std::move(schema_history);first->schema_history_extraction_returned=true;
        first->schema_history_reset_attempted=true;first->objects.template get<3>().reset();if(!local())return false;first->schema_history_reset_returned=true;
        // Drop the real first banks/tickets BEFORE the SAME receiving charge.
        // The surviving ticket retains scalar DATA only, under Owner lifetime.
        first->tail_records_reset_attempted=true;
        first->facade_record.reset();if(!local())return false;first->state_record.reset();if(!local())return false;
        first->target_record.reset();if(!local())return false;first->io_record.reset();if(!local())return false;
        first->tail_records_reset_returned=true;
        if(charge.get()!=first->charge||charge.use_count()!=1)return false;
        first->charge_extraction_attempted=true;first->objects.template get<4>()=std::move(charge);first->charge_extraction_returned=true;
        first->charge_reset_attempted=true;first->objects.template get<4>().reset();if(!local())return false;first->charge_reset_returned=true;
        first->returned=true;return terminal_collection_ended(first,&owner);
    }catch(...) {
        if(ordinary_launch::current_process()==process) {
            if(!allocation_failure)allocation_failure=std::current_exception();
            if(first&&!first->first_failure)first->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_collection_ended(const std::shared_ptr<terminal_collection_record>& first,
    const ordinary_group_target_receiving_owner* owner)noexcept {
    if(!first||first->process!=ordinary_launch::current_process()||first->thread!=std::this_thread::get_id()||first->owner!=owner||
       !first->preparation_returned||first->first_failure||!first->objects.in_creator_process()||!first->allocation_history_known||
       !first->io_registry_detach_attempted||!first->io_registry_detach_returned||!first->main_registry_detach_attempted||!first->main_registry_detach_returned||
       !first->facade_tail_observed||!first->state_tail_observed||!first->io_ending_observed||!first->target_tail_observed||
       !first->registry_return_attempted||!first->registry_returned||!first->schema_history_extraction_attempted||!first->schema_history_extraction_returned||
       !first->schema_history_reset_attempted||!first->schema_history_reset_returned||!first->tail_records_reset_attempted||!first->tail_records_reset_returned||
       !first->charge_extraction_attempted||!first->charge_extraction_returned||!first->charge_reset_attempted||!first->charge_reset_returned||!first->returned)return false;
    const auto requested_total=[](const auto& requests,std::size_t count,std::size_t base,std::size_t& total) {
        if(!base||base>ordinary_history_limits::owner_bytes)return false;total=base;
        for(std::size_t at=0;at<requests.size();++at) {
            const auto& request=requests[at];
            if(at>=count){if(request.reserved)return false;continue;}
            if(!request.reserved||!request.bytes||request.exception_returned||
               (request.fixed_fields?!request.containing_constructor_returned:(!request.entered||!request.returned))||
               request.bytes>ordinary_history_limits::owner_bytes-total)return false;
            total+=request.bytes;
        }
        return true;
    };
    std::size_t payload_total=0,final_total=0;
    if(!requested_total(first->payload_requests,7,first->payload_predecessor_bytes,payload_total)||
       first->io_augmented_bytes<payload_total||!requested_total(first->collection_requests,8,first->io_augmented_bytes,final_total)||
       final_total!=first->peak_bytes)return false;
    for(std::size_t at=0;at<9;++at)if((at!=5&&!first->source_cells[at])||!first->extraction_attempted[at]||!first->extraction_returned[at]||
        !first->reset_attempted[at]||!first->reset_returned[at])return false;
    return true;
}
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
std::array<std::uint64_t,28> ordinary_target_staging_connection::qualification_terminal_collection_preflight(
    const ordinary_group_target_receiving_owner& owner,
    const std::shared_ptr<ordinary_target_staging_connection>& connection,const std::shared_ptr<ordinary_target_staging_connection>& requested,
    const std::shared_ptr<ordinary_target_staging_io>& io,const std::shared_ptr<ordinary_target_staging_io>& requested_io,
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,const std::shared_ptr<const ordinary_unpublished_group_target>& requested_target,
    const std::shared_ptr<ordinary_target_schema_roots_ending>& schema,const std::shared_ptr<ordinary_resource_reservation<true>>& charge,
    bool selected,const std::shared_ptr<terminal_collection_record>& ticket,const std::exception_ptr& failure)noexcept {
    std::array<std::uint64_t,28> out{};static_assert(sizeof(out)<=512);
    try {
        if(!connection||!connection->state_||!io||!target||!schema||!charge)return out;
        const auto& native=connection->state_;
        if(!native->creator_current()||!native->receiving_ending_original)return out;
        const auto& prior=*native->receiving_ending_original;
        const auto same=[](const auto& a,const auto& b){return a&&b&&a.get()==b.get()&&!a.owner_before(b)&&!b.owner_before(a);};
        const auto tail=[](const auto& value,const void* original,unsigned kind){return
            value.process_==ordinary_launch::current_process()&&value.thread_==std::this_thread::get_id()&&
            value.original_==original&&value.kind_==kind&&value.construction_returned_&&!value.first_;};
        auto& owners=registry();bool registered=false;
        {std::lock_guard lock(owners.mutex);const auto at=native->registry_slot;
            if(at>=owners.originals.size())return out;
            registered=owners.occupied[at]&&owners.serials[at]==native->registry_serial&&
                same(connection,owners.originals[at])&&same(target,owners.targets[at]);
            out[5]=reinterpret_cast<std::uintptr_t>(&owners.originals[at]);
            out[26]=reinterpret_cast<std::uintptr_t>(&owners.targets[at]);out[10]=owners.charged;}
        const auto augmented=io->terminal_payload_peak_bytes(*connection,owner);
        const auto& allocations=native->collection_allocations;
        out[0]=terminal_scope_fact_flags({true,native->creator_current(),connection->terminal_receiving_payloads_ended(owner),
            native.use_count()==1,
            prior.owner==&owner&&prior.charge_original==charge.get()&&prior.io==io.get()&&prior.target==target.get()&&
                prior.scope_history==native->scope_roots_original.get()&&native->scope_roots_original&&
                native->scope_roots_original->predecessor.roots==schema.get(),
            same(connection,requested),same(io,requested_io)&&same(io,native->io),
            same(target,requested_target)&&same(target,native->target),registered,
            tail(connection->terminal_receiving_tail_,connection.get(),5)&&tail(native->terminal_receiving_tail_,native.get(),6)&&
                tail(target->terminal_receiving_tail_,target.get(),9),
            augmented&&augmented>=prior.peak_bytes&&augmented<=ordinary_history_limits::owner_bytes,
            connection.use_count()==4,io.use_count()==4,target.use_count()==5,allocations.current(),
            !selected&&!ticket&&!failure});
        out[1]=reinterpret_cast<std::uintptr_t>(connection.get());out[2]=reinterpret_cast<std::uintptr_t>(native.get());
        out[3]=reinterpret_cast<std::uintptr_t>(&connection);out[4]=reinterpret_cast<std::uintptr_t>(&requested);
        out[6]=connection.use_count();out[7]=native.use_count();out[8]=native->registry_slot;out[9]=native->registry_serial;
        out[11]=reinterpret_cast<std::uintptr_t>(target.get());out[12]=reinterpret_cast<std::uintptr_t>(io.get());
        out[13]=reinterpret_cast<std::uintptr_t>(charge.get());out[14]=reinterpret_cast<std::uintptr_t>(schema.get());
        out[15]=io.use_count();out[16]=target.use_count();
        for(std::size_t at=0;at<allocations.requests.size();++at) {
            const auto& value=allocations.requests[at];
            if(value.bytes||value.reserved||value.fixed_fields||value.entered||value.returned||value.exception_returned||value.containing_constructor_returned)
                out[17]|=std::uint64_t{1}<<at;
            if(value.reserved)out[27]|=std::uint64_t{1}<<at;
        }
        out[18]=allocations.selected;out[19]=allocations.sealed;out[20]=allocations.total_bytes;out[21]=allocations.predecessor_bytes;
        out[22]=reinterpret_cast<std::uintptr_t>(allocations.charge);out[23]=selected;out[24]=reinterpret_cast<std::uintptr_t>(ticket.get());out[25]=bool(failure);
    }catch(...) {return {};}
    return out;
}
#endif
std::array<std::uint64_t,10> ordinary_target_staging_connection::qualification_terminal_collection_observation(
    const std::shared_ptr<terminal_collection_record>& first)noexcept {
    std::array<std::uint64_t,10> out{};
    if(!first||first->process!=ordinary_launch::current_process()||first->thread!=std::this_thread::get_id())return out;
    out[0]=terminal_scope_fact_flags({true,first->preparation_returned,first->io_registry_detach_attempted,first->io_registry_detach_returned,
        first->main_registry_detach_attempted,first->main_registry_detach_returned,first->facade_tail_observed,first->state_tail_observed,
        first->io_ending_observed,first->target_tail_observed,first->registry_return_attempted,first->registry_returned,
        first->schema_history_extraction_attempted,first->schema_history_extraction_returned,first->schema_history_reset_attempted,
        first->schema_history_reset_returned,first->tail_records_reset_attempted,first->tail_records_reset_returned,
        first->charge_extraction_attempted,first->charge_extraction_returned,first->charge_reset_attempted,first->charge_reset_returned,
        first->returned,bool(first->first_failure)});
    for(std::size_t at=0;at<9;++at) {
        if(first->extraction_attempted[at]&&first->extraction_returned[at])out[1]|=std::uint64_t{1}<<at;
        if(first->reset_attempted[at]&&first->reset_returned[at])out[2]|=std::uint64_t{1}<<at;
    }
    out[3]=first->peak_bytes;out[4]=first->slot;out[5]=first->serial;
    out[6]=reinterpret_cast<std::uintptr_t>(first->connection);out[7]=reinterpret_cast<std::uintptr_t>(first->connection_state);
    out[8]=reinterpret_cast<std::uintptr_t>(first->io);out[9]=reinterpret_cast<std::uintptr_t>(first->target);
    return out;
}
#endif
