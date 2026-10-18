// After actual schema state and completed programme first history. The actual
// wrapper's const state member ends naturally; no payload graph moves to DATA.
struct ordinary_target_schema_root_record final {
    const void* const original;
    const unsigned kind;
    const bool constructor_returned;
    bool binding_attempted=false,binding_returned=false,members_destroyed=false,partial_constructor=false;
    ordinary_target_schema_root_record(const void* actual,unsigned selected,bool constructed)
        :original(actual),kind(selected),constructor_returned(constructed){}
};
ordinary_target_schema_root_tail::ordinary_target_schema_root_tail(const void* actual,unsigned kind)noexcept
    :original_(actual),kind_(kind){}
bool ordinary_target_schema_root_tail::bind(const std::shared_ptr<ordinary_target_schema_root_record>& first)noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!construction_returned_||first_||
       !first||first->original!=original_||first->kind!=kind_||!first->constructor_returned||first->binding_attempted)return false;
    first->binding_attempted=true;first_=first;first->binding_returned=true;return true;
}
ordinary_target_schema_root_tail::~ordinary_target_schema_root_tail()noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!first_||
       first_->original!=original_||first_->kind!=kind_)return;
    first_->partial_constructor=!construction_returned_;first_->members_destroyed=true;
}
struct ordinary_target_schema_roots_ending final {
    using programme_history=ordinary_group_target_schema_original::state::terminal_programmes_ending;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const void* const receiving_state;
    const ordinary_target_staging_connection* const connection;
    const ordinary_group_target_receiving_owner* const owner;
    const std::weak_ptr<ordinary_group_target_receiving_owner> owner_original;
    const void* const request_original;
    const void* const request_state;
    const ordinary_group_target_schema_original* const schema;
    const void* const state_original;
    const ordinary_launch::deadline end;
    const void* const scope;
    const std::uint64_t generation;
    const std::array<const void*,8> source_originals; // application/archive/target/IO/Conn/expectation/declarations/request-state DATA only.
    const std::int32_t version;
    const std::exception_ptr source_first_failure;
    const std::shared_ptr<programme_history> programmes; // Existing <=2KiB first history; all actual programme payloads already ended.
    native_retirement_storage<std::shared_ptr<ordinary_group_target_schema_original>> objects;
    std::array<std::shared_ptr<ordinary_target_schema_root_record>,2> records{};
    std::array<bool,2> source_extraction_attempted{},source_extraction_returned{};
    bool preparation_returned=false,reset_attempted=false,reset_returned=false,returned=false;
    bool ending_observation_attempted=false,ending_observation_returned=false,ending_known=false;
    std::exception_ptr first_failure;
    ordinary_target_schema_roots_ending(const void* receiving,const ordinary_target_staging_connection& conn,
        const ordinary_group_target_receiving_owner& actual,std::weak_ptr<ordinary_group_target_receiving_owner> weak,
        const void* request,const void* request_data,const ordinary_group_target_schema_original& input,
        const ordinary_group_target_schema_original::state& data,const void* cleanup,std::uint64_t gen)
        :receiving_state(receiving),connection(&conn),owner(&actual),owner_original(std::move(weak)),
         request_original(request),request_state(request_data),schema(&input),state_original(&data),end(data.end),scope(cleanup),generation(gen),
         source_originals{data.application.get(),data.archive.get(),data.target.get(),data.io.get(),data.connection.get(),
             data.expectation.get(),data.declarations,request_data},version(data.version),source_first_failure(data.first_failure),programmes(data.programmes_original){}
};
bool ordinary_target_staging_connection::retire_terminal_schema_roots(ordinary_group_target_receiving_owner& owner)noexcept {
    const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return false;
    {std::lock_guard lock(receiving->leaf);
        if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||!receiving->stop||
           !receiving->cleanup_entered||receiving->startup_active||receiving->callbacks||receiving->queued||
           receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return false;}
    if(receiving->schema_roots_attempted)return terminal_schema_roots_ended(owner);
    const auto request=receiving->request.lock();
    if(!request||!request->state_||request->state_->process!=creator())return false;
    const void* scope=nullptr;std::uint64_t generation=0;
    if(!terminal_schema_programmes_ended(owner)||!terminal_raw_objects_context_known(owner,scope,generation))return false;
    std::shared_ptr<ordinary_target_schema_roots_ending> first;
    try {
        { // Actual payload references MUST leave lexical scope before the final wrapper reset.
            const auto& schema=receiving->schema;const auto& request_data=request->state_;
            if(!schema||request_data->target_schema!=schema||schema.use_count()!=2||!schema->state_)return false;
            const auto& data=schema->state_;
            if(data.use_count()!=1||data->process!=creator()||data->connection.get()!=this||data->end!=receiving->end||
               data->request.lock()!=request||!data->programmes_original||
               data->programmes_original->source_first_failure!=data->first_failure)return false;
            const auto actual_tail=[&](const auto& tail,const void* actual,unsigned kind) {
                return tail.process_==ordinary_launch::current_process()&&tail.thread_==std::this_thread::get_id()&&
                    tail.original_==actual&&tail.kind_==kind&&tail.construction_returned_&&!tail.first_;
            };
            if(!actual_tail(schema->terminal_schema_root_,schema.get(),0)||!actual_tail(data->terminal_schema_root_,data.get(),1))return false;
            static_assert(sizeof(ordinary_target_schema_root_record)<=128&&sizeof(ordinary_target_schema_roots_ending)<=1024);
            receiving->schema_roots_attempted=true; // First selection before actual storage/record allocation.
            auto* allocated=new ordinary_target_schema_roots_ending(receiving.get(),*this,owner,
                owner.terminal_retirement_wake_original(*this),request.get(),request_data.get(),*schema,*data,scope,generation);
            receiving->schema_roots_original=std::shared_ptr<ordinary_target_schema_roots_ending>(allocated);
            receiving->schema_roots_allocation_returned=true;first=receiving->schema_roots_original;
            first->records[0]=std::make_shared<ordinary_target_schema_root_record>(schema.get(),0,schema->terminal_schema_root_.construction_returned_);
            first->records[1]=std::make_shared<ordinary_target_schema_root_record>(data.get(),1,data->terminal_schema_root_.construction_returned_);
            if(!first->objects.in_creator_process()||schema.use_count()!=2||data.use_count()!=1||
               !schema->terminal_schema_root_.bind(first->records[0])||!data->terminal_schema_root_.bind(first->records[1]))return false;
            first->preparation_returned=true;
            static_assert(noexcept(first->objects.template get<0>()=std::move(receiving->schema)));
            first->source_extraction_attempted[0]=true;first->objects.template get<0>()=std::move(receiving->schema);
            first->source_extraction_returned[0]=true;
            if(receiving->process!=creator()||!first->objects.in_creator_process())return false;
            if(request_data->target_schema.get()!=first->schema)return false;
            first->source_extraction_attempted[1]=true;request_data->target_schema.reset();
            if(receiving->process!=creator()||!first->objects.in_creator_process())return false;
            first->source_extraction_returned[1]=true;
        } // Neither a shared schema/state local nor a reference into its member survives into reset.
        if(receiving->process!=creator()||first->process!=ordinary_launch::current_process()||
           first->thread!=std::this_thread::get_id()||first->owner_original.lock().get()!=&owner||
           receiving->schema_roots_original!=first||!first->objects.in_creator_process())return false;
        first->reset_attempted=true;
        if(!first->objects.template release<0>()||receiving->process!=creator())return false;
        first->reset_returned=true;
        first->ending_observation_attempted=true;
        first->ending_known=terminal_schema_roots_first_history_known(owner); // Actual first post-reset whole-history result.
        first->ending_observation_returned=true;first->returned=true;
        const bool ended=terminal_schema_roots_ended(owner);
        if(ended)owner.schedule_collection(); // Post state-tail, wrapper-tail and whole reset return, outside every leaf.
        return ended;
    }catch(...) {
        if(receiving->process==creator()) {
            if(receiving->schema_roots_attempted&&!receiving->schema_roots_original) {
                receiving->schema_roots_allocation_returned=true;receiving->schema_roots_allocation_exception_returned=true;
            }
            if(!receiving->schema_roots_first_failure)receiving->schema_roots_first_failure=std::current_exception();
            if(first&&!first->first_failure)first->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_schema_roots_first_history_known(const ordinary_group_target_receiving_owner& owner)const noexcept {
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return false;
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||!receiving->stop||
               !receiving->cleanup_entered||receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return false;}
        if(!receiving->schema_roots_attempted||!receiving->schema_roots_allocation_returned||receiving->schema_roots_allocation_exception_returned||
           receiving->schema_roots_first_failure||!receiving->schema_roots_original)return false;
        const auto& first=*receiving->schema_roots_original;
        const void* scope=nullptr;std::uint64_t generation=0;
        if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||first.receiving_state!=receiving.get()||
           first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner||first.end!=receiving->end||
           !first.schema||!first.state_original||!first.request_original||!first.request_state||!first.objects.in_creator_process()||
           !first.preparation_returned||!first.reset_attempted||!first.reset_returned||first.first_failure||
           !terminal_raw_objects_context_known(owner,scope,generation)||scope!=first.scope||generation!=first.generation)return false;
        for(std::size_t at=0;at<2;++at) {
            const auto& record=first.records[at];
            if(!record||record->original!=(at==0?static_cast<const void*>(first.schema):first.state_original)||record->kind!=at||
               !record->constructor_returned||!record->binding_attempted||!record->binding_returned||!record->members_destroyed||record->partial_constructor||
               !first.source_extraction_attempted[at]||!first.source_extraction_returned[at])return false;
        }
        const auto& programmes=first.programmes;
        if(!programmes||programmes->process!=ordinary_launch::current_process()||programmes->thread!=std::this_thread::get_id()||
           programmes->schema!=first.schema||programmes->connection!=this||programmes->owner!=&owner||
           programmes->owner_original.lock().get()!=&owner||programmes->end!=first.end||programmes->scope!=first.scope||
           programmes->generation!=first.generation||programmes->source_first_failure!=first.source_first_failure||
           !programmes->objects.in_creator_process()||!programmes->preparation_returned||!programmes->returned||programmes->first_failure||
           !terminal_programme_history_known(owner,first.schema,programmes->source_originals,programmes->source_failures))return false;
        bool any=false;
        for(std::size_t at=0;at<4;++at) {
            const auto& record=programmes->records[at];
            if(!record) {if(programmes->issuance[at]||programmes->source_originals[at]||programmes->source_failures[at])return false;continue;}
            any=true;
            if(!programmes->issuance[at]||record->original!=programmes->source_originals[at]||record->schema!=first.schema||record->kind!=at||
               record->first_failure!=programmes->source_failures[at]||!record->constructor_returned||!record->binding_attempted||!record->binding_returned||
               !record->extraction_attempted||!record->extraction_returned||!record->reset_attempted||!record->reset_returned||
               !record->members_destroyed||record->partial_constructor||record->generation_entered_observed!=(at!=0)||record->validation_observed!=(at==1)||
               (at>=2?record->projection_original!=programmes->source_originals[1]:record->projection_original!=nullptr))return false;
            for(std::size_t payload=0;payload<4;++payload)
                if(record->payload_originals[payload]?record->payload_owner_counts[payload]!=1:record->payload_owner_counts[payload]!=0)return false;
        }
        return any; // Actual schema wrapper/state endings only; no scope/owner/charge/registry/refund join.
    }catch(...){return false;}
}

bool ordinary_target_staging_connection::terminal_schema_roots_ended(const ordinary_group_target_receiving_owner& owner)const noexcept {
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return false;
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
               receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return false;}
        if(!receiving->schema_roots_attempted||!receiving->schema_roots_original)return false;
        const auto& first=*receiving->schema_roots_original;
        if(!first.returned||!first.ending_observation_attempted||!first.ending_observation_returned||!first.ending_known)return false;
        return terminal_schema_roots_first_history_known(owner); // Same first facts only; unknown first result never repairs.
    }catch(...){return false;}
}

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
ordinary_target_programme_observation ordinary_target_staging_connection::qualification_programme_after_schema_observation(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    ordinary_target_programme_observation out;
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return out;
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
               receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return out;}
        if(!receiving->schema_roots_attempted||!receiving->schema_roots_original)return out;
        const auto& roots=*receiving->schema_roots_original;const auto& first=roots.programmes;
        if(roots.process!=ordinary_launch::current_process()||roots.thread!=std::this_thread::get_id()||
           roots.receiving_state!=receiving.get()||roots.connection!=this||roots.owner!=&owner||
           roots.owner_original.lock().get()!=&owner||!first)return out;
        out.creator_worker=true;out.receiving_owner_bound=true;out.schema_original=reinterpret_cast<std::uintptr_t>(roots.schema);
        out.attempted=true;out.allocation_returned=true;out.history_checked=true;out.history_known=terminal_schema_roots_ended(owner);
        out.result=out.history_known?1:2;out.first_failure=bool(first->first_failure);
        out.preparation_returned=first->preparation_returned;out.returned=first->returned;
        out.source_failure_observed=true;out.source_failure=bool(first->source_first_failure);
        const auto bit=[](auto& value,unsigned at,bool actual){if(actual)value.flags|=std::uint64_t{1}<<at;};
        const auto address=[](const void* value){return reinterpret_cast<std::uintptr_t>(value);};
            for(std::size_t at=0;at<4;++at) {
                auto& value=out.objects[at];bit(value,0,true);bit(value,1,first->issuance[at]);
                const auto& record=first->records[at];if(!record)continue;
                value.original=address(record->original);value.projection_original=address(record->projection_original);
                value.bytes=record->bytes;value.items=record->items;value.cells=record->cells;value.bindings=record->bindings;
                bit(value,2,true);bit(value,3,true);bit(value,4,record->constructor_returned);bit(value,6,true);bit(value,7,bool(record->first_failure));
                bit(value,8,record->binding_attempted);bit(value,9,record->binding_returned);
                bit(value,10,record->extraction_attempted);bit(value,11,record->extraction_returned);
                bit(value,12,record->reset_attempted);bit(value,13,record->reset_returned);
                bit(value,14,record->members_destroyed);bit(value,15,record->partial_constructor);
                bit(value,16,record->generation_entered_observed);bit(value,17,record->generation_entered);
                bit(value,18,true);bit(value,19,record->generation_returned);bit(value,20,record->validation_observed);
                bit(value,21,record->validation_entered);bit(value,22,record->validation_returned);bit(value,23,at>=2);
                bit(value,24,record->constructor_returned&&record->binding_attempted&&record->binding_returned&&
                    record->extraction_attempted&&record->extraction_returned&&record->reset_attempted&&record->reset_returned&&
                    record->members_destroyed&&!record->partial_constructor);
                if(at==1) {
                    out.projection_payloads_observed=true;
                    for(std::size_t payload=0;payload<4;++payload) {
                        out.projection_payloads[payload]=address(record->payload_originals[payload]);
                        out.projection_payload_owner_counts[payload]=record->payload_owner_counts[payload];
                    }
                }
            }
        return out; // Only actual retained first DATA; never read ended schema/state/programme originals.
    }catch(...){return {};}
}
#endif

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
ordinary_target_schema_root_observation ordinary_target_staging_connection::qualification_schema_root_observation(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    ordinary_target_schema_root_observation out;
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return out;
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
               receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return out;}
        const auto address=[](const void* value){return reinterpret_cast<std::uintptr_t>(value);};
        const auto bit=[](auto& value,unsigned at,bool actual){if(actual)value.flags|=std::uint64_t{1}<<at;};
        out.creator_worker=true;out.receiving_owner_bound=true;out.receiving_state_original=address(receiving.get());out.connection_original=address(this);
        out.attempted=receiving->schema_roots_attempted;out.allocation_returned=receiving->schema_roots_allocation_returned;
        out.allocation_exception_returned=receiving->schema_roots_allocation_exception_returned;out.first_failure=bool(receiving->schema_roots_first_failure);
        if(receiving->schema_roots_attempted) {
            out.result=2;out.history_checked=true;out.history_known=terminal_schema_roots_ended(owner);
            const auto& held=receiving->schema_roots_original;if(!held)return out;
            const auto& first=*held;
            if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||
               first.receiving_state!=receiving.get()||first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner)return {};
            out.originals_observed=true;out.request_original=address(first.request_original);out.request_state_original=address(first.request_state);
            out.preparation_returned=first.preparation_returned;out.reset_attempted=first.reset_attempted;out.reset_returned=first.reset_returned;
            out.ending_observation_attempted=first.ending_observation_attempted;out.ending_observation_returned=first.ending_observation_returned;
            out.ending_known=first.ending_known;out.returned=first.returned;out.first_failure=out.first_failure||bool(first.first_failure);
            out.source_failure_observed=true;out.source_failure=bool(first.source_first_failure);if(out.history_known)out.result=1;
            for(std::size_t at=0;at<2;++at) {
                out.sources[at]=(first.source_extraction_attempted[at]?1u:0u)|(first.source_extraction_returned[at]?2u:0u);
                const auto& record=first.records[at];if(!record)continue;
                auto& value=out.roots[at];value.original=address(record->original);
                bit(value,0,true);bit(value,1,true);bit(value,2,record->constructor_returned);
                bit(value,4,record->binding_attempted);bit(value,5,record->binding_returned);bit(value,6,record->members_destroyed);bit(value,7,record->partial_constructor);
            }
            return out; // Same first DATA only after ANY attempt; no live schema or state access.
        }
        const auto request=receiving->request.lock();const auto& schema=receiving->schema;
        if(!request||!request->state_||request->state_->process!=creator()||!schema||!schema->state_||request->state_->target_schema!=schema)return out;
        const auto& data=schema->state_;
        if(data->process!=creator()||data->connection.get()!=this||data->end!=receiving->end||data->request.lock()!=request)return out;
        const auto actual=[&](const auto& tail,const void* object,unsigned kind) {
            return tail.process_==ordinary_launch::current_process()&&tail.thread_==std::this_thread::get_id()&&tail.original_==object&&tail.kind_==kind;
        };
        if(!actual(schema->terminal_schema_root_,schema.get(),0)||!actual(data->terminal_schema_root_,data.get(),1))return {};
        out.originals_observed=true;out.request_original=address(request.get());out.request_state_original=address(request->state_.get());
        out.source_failure_observed=true;out.source_failure=bool(data->first_failure);
        out.roots[0].original=address(schema.get());out.roots[0].owner_count=schema.use_count();
        out.roots[1].original=address(data.get());out.roots[1].owner_count=data.use_count();
        for(std::size_t at=0;at<2;++at) {
            auto& value=out.roots[at];bit(value,0,true);bit(value,1,true);bit(value,3,true);
            bit(value,2,at==0?schema->terminal_schema_root_.construction_returned_:data->terminal_schema_root_.construction_returned_);
        }
        return out;
    }catch(...){return {};}
}
#endif

bool ordinary_target_staging_connection::terminal_scope_predecessor_snapshot(
    const ordinary_group_target_receiving_owner& owner,ordinary_target_scope_predecessor_fact& output)const noexcept {
    if(!terminal_schema_roots_ended(owner))return false;
    const auto receiving=owner.state_;const auto& first=receiving->schema_roots_original;
    if(!first)return false;
    output={receiving.get(),first.get(),first->schema,first->state_original,first->scope,first->generation,
        sizeof(ordinary_group_target_receiving_owner::state)+receiving->endpoint.capacity()+receiving->model_profile.capacity()+
        receiving->token.capacity()+receiving->model_request.capacity()+receiving->history_request.capacity()+
        sizeof(ordinary_group_target_receiving_owner::state::input_capture_storage)+
        16*(sizeof(ordinary_group_target_receiving_owner::state::capture_return_record)+64)+
        sizeof(ordinary_target_schema_roots_ending)+2*(sizeof(ordinary_target_schema_root_record)+64)+
        sizeof(ordinary_target_schema_roots_ending::programme_history)+4*(sizeof(ordinary_target_programme_record)+64),first->source_first_failure};
    return terminal_scope_predecessor_matches(owner,output);
}
bool ordinary_target_staging_connection::terminal_scope_predecessor_matches(
    const ordinary_group_target_receiving_owner& owner,const ordinary_target_scope_predecessor_fact& saved)const noexcept {
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator()||saved.receiving!=receiving.get())return false;
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||!receiving->stop||
               !receiving->cleanup_entered||receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return false;}
        const auto& first=receiving->schema_roots_original;
        if(!receiving->schema_roots_attempted||!receiving->schema_roots_allocation_returned||receiving->schema_roots_allocation_exception_returned||
           receiving->schema_roots_first_failure||!first||first.get()!=saved.roots||first->schema!=saved.schema||
           first->state_original!=saved.state||first->scope!=saved.scope||first->generation!=saved.generation||
           first->source_first_failure!=saved.first_failure||first->process!=ordinary_launch::current_process()||first->thread!=std::this_thread::get_id()||
           first->connection!=this||first->owner!=&owner||first->owner_original.lock().get()!=&owner||first->end!=receiving->end||
           !first->preparation_returned||!first->reset_attempted||!first->reset_returned||!first->returned||first->first_failure||
           !first->ending_observation_attempted||!first->ending_observation_returned||!first->ending_known||!first->objects.in_creator_process())return false;
        for(std::size_t at=0;at<2;++at) {
            const auto& record=first->records[at];
            if(!record||record->original!=(at==0?saved.schema:saved.state)||record->kind!=at||!record->constructor_returned||
               !record->binding_attempted||!record->binding_returned||!record->members_destroyed||record->partial_constructor||
               !first->source_extraction_attempted[at]||!first->source_extraction_returned[at])return false;
        }
        return true; // Immutable real predecessor first result, never old RAW/scope/live programme revalidation.
    }catch(...){return false;}
}
