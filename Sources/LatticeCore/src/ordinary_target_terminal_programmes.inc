// Included in the real receiver TU after the actual schema state. Programme
// payloads die here; only bounded original first facts remain in this bank.
struct ordinary_target_programme_record final {
    friend class ordinary_target_staging_connection;
    friend class ordinary_target_programme_tail;
    const void* const original;
    const ordinary_group_target_schema_original* const schema;
    const unsigned kind;
    const std::exception_ptr first_failure;
    const void* const projection_original;
    const std::array<const void*,4> payload_originals;
    const std::array<std::size_t,4> payload_owner_counts;
    const std::size_t bytes,items,cells,bindings;
    const bool constructor_returned,generation_entered_observed,generation_entered,generation_returned;
    const bool validation_observed,validation_entered,validation_returned;
    bool binding_attempted=false,binding_returned=false;
    bool extraction_attempted=false,extraction_returned=false,reset_attempted=false,reset_returned=false;
    bool members_destroyed=false,partial_constructor=false;
private:
    ordinary_target_programme_record(const void* object,const ordinary_group_target_schema_original* owner,unsigned selected,
        std::exception_ptr error,const void* projection,std::array<const void*,4> payloads,
        std::array<std::size_t,4> counts,std::size_t retained,std::size_t size,std::size_t cell_count,std::size_t bind_count,
        bool constructed,bool entered_observed,bool entered,bool returned,bool validated,bool validation_start,bool validation_end)
        :original(object),schema(owner),kind(selected),first_failure(std::move(error)),projection_original(projection),
         payload_originals(payloads),payload_owner_counts(counts),bytes(retained),items(size),cells(cell_count),bindings(bind_count),
         constructor_returned(constructed),generation_entered_observed(entered_observed),generation_entered(entered),generation_returned(returned),
         validation_observed(validated),validation_entered(validation_start),validation_returned(validation_end){}
};
ordinary_target_programme_tail::ordinary_target_programme_tail(const void* original,
    const ordinary_group_target_schema_original* schema,unsigned kind)noexcept
    :original_(original),schema_(schema),kind_(kind){}
bool ordinary_target_programme_tail::bind(const std::shared_ptr<ordinary_target_programme_record>& actual)noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||first_||
       !construction_returned_||!actual||actual->original!=original_||actual->schema!=schema_||actual->kind!=kind_||
       !actual->constructor_returned||actual->binding_attempted)return false;
    actual->binding_attempted=true;first_=actual;actual->binding_returned=true;return true;
}
ordinary_target_programme_tail::~ordinary_target_programme_tail()noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!first_||
       first_->original!=original_||first_->schema!=schema_||first_->kind!=kind_)return;
    first_->partial_constructor=!construction_returned_;
    first_->members_destroyed=true; // First member runs after all actual programme members.
}
struct ordinary_group_target_schema_original::state::terminal_programmes_ending final {
    using custody=native_retirement_storage<std::shared_ptr<catalog_program>,std::shared_ptr<projection_program>,
        std::shared_ptr<import_suspension_program>,std::shared_ptr<row_import_program>>;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const std::weak_ptr<state> original;
    const std::weak_ptr<ordinary_group_target_schema_original> schema_original;
    const ordinary_group_target_schema_original* const schema;
    const ordinary_target_staging_connection* const connection;
    const std::weak_ptr<ordinary_group_target_receiving_owner> owner_original;
    const ordinary_group_target_receiving_owner* const owner;
    const ordinary_launch::deadline end;
    const void* const scope;
    const std::uint64_t generation;
    const std::exception_ptr source_first_failure;
    const std::array<bool,4> issuance;
    const std::array<const void*,4> source_originals;
    const std::array<std::exception_ptr,4> source_failures;
    custody objects; // All four empty owning cells exist before binding/extraction.
    std::array<std::shared_ptr<ordinary_target_programme_record>,4> records{};
    bool preparation_returned=false,returned=false;
    std::exception_ptr first_failure;
    terminal_programmes_ending(const std::shared_ptr<state>& data,
        const std::shared_ptr<ordinary_group_target_schema_original>& actual,
        const ordinary_target_staging_connection& conn,const ordinary_group_target_receiving_owner& receiving,
        const void* cleanup,std::uint64_t gen,std::weak_ptr<ordinary_group_target_receiving_owner> actual_owner,std::array<bool,4> issued,
        std::array<const void*,4> programmes,std::array<std::exception_ptr,4> failures)
        :original(data),schema_original(actual),schema(actual.get()),connection(&conn),
         owner_original(std::move(actual_owner)),owner(&receiving),end(data->end),
         scope(cleanup),generation(gen),source_first_failure(data->first_failure),issuance(issued),
         source_originals(programmes),source_failures(std::move(failures)){}
};
bool ordinary_target_staging_connection::retire_terminal_schema_programmes(
    ordinary_group_target_receiving_owner& owner)noexcept {
    const auto receiving=owner.state_;
    if(!receiving||receiving->process!=creator())return false;
    if(receiving->schema_roots_attempted)return terminal_schema_roots_ended(owner);
    {std::lock_guard lock(receiving->leaf);
        if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
           !receiving->stop||!receiving->cleanup_entered||receiving->connection.get()!=this)return false;}
    const auto& schema=receiving->schema;
    if(!schema||!schema->state_||schema->state_->process!=creator())return false;
    const auto data=schema->state_;
    if(data->programmes_attempted)return terminal_schema_programmes_ended(owner); // Same first bank only.
    const void* scope=nullptr;std::uint64_t generation=0;
    if(data->connection.get()!=this||data->end!=receiving->end||!terminal_raw_objects_ended(owner)||
       !terminal_raw_objects_context_known(owner,scope,generation))return false;
    const std::array<bool,4> issued{data->catalog_preparation_attempted,data->projection_preparation_attempted,
        data->import_suspension_attempted,data->row_import_attempted};
    const std::array<const void*,4> originals{data->catalog.get(),data->projection.get(),data->import_suspension.get(),data->row_import.get()};
    const std::array<std::exception_ptr,4> failures{data->catalog?data->catalog->first_failure:std::exception_ptr{},
        data->projection?data->projection->first_failure:std::exception_ptr{},
        data->import_suspension?data->import_suspension->first_failure:std::exception_ptr{},
        data->row_import?data->row_import->first_failure:std::exception_ptr{}};
    for(std::size_t at=0;at<4;++at)if(issued[at]!=bool(originals[at]))return false;
    if(!terminal_programme_originals_known(owner,*schema,originals,failures))return false;
    const auto actual=[&](const auto& programme,unsigned kind,std::size_t expected) {
        return !programme||(programme.use_count()==expected&&programme->original.lock().get()==schema.get()&&
            programme->terminal_programme_.process_==ordinary_launch::current_process()&&
            programme->terminal_programme_.thread_==std::this_thread::get_id()&&
            programme->terminal_programme_.original_==programme.get()&&programme->terminal_programme_.schema_==schema.get()&&
            programme->terminal_programme_.kind_==kind&&programme->terminal_programme_.construction_returned_&&!programme->terminal_programme_.first_);
    };
    const auto projection_owners=std::size_t(1)+bool(data->import_suspension)+bool(data->row_import);
    if(!actual(data->catalog,0,1)||!actual(data->projection,1,projection_owners)||
       !actual(data->import_suspension,2,1)||!actual(data->row_import,3,1)||
       (data->import_suspension&&data->import_suspension->projection!=data->projection)||
       (data->row_import&&data->row_import->projection!=data->projection))return false;
    if(data->projection) {
        const auto sole=[](const auto& payload){return !payload||payload.use_count()==1;};
        if(!sole(data->projection->plan)||!sole(data->projection->logical)||
           !sole(data->projection->metadata)||!sole(data->projection->internal))return false;
    }
    static_assert(sizeof(ordinary_target_programme_record)<=512&&
        sizeof(ordinary_group_target_schema_original::state::terminal_programmes_ending)<=2048,
        "terminal programme history is bounded original DATA, not replacement payload");
    data->programmes_attempted=true; // Seals existing generators before any first allocation/effect.
    try {
        auto* allocated=new ordinary_group_target_schema_original::state::terminal_programmes_ending(
            data,schema,*this,owner,scope,generation,owner.terminal_retirement_wake_original(*this),issued,originals,failures);
        data->programmes_original=std::shared_ptr<ordinary_group_target_schema_original::state::terminal_programmes_ending>(allocated);
        data->programmes_allocation_returned=true;
        auto& first=*data->programmes_original;
        const auto bound=[&] {
            const void* current_scope=nullptr;std::uint64_t current_generation=0;
            return data->process==creator()&&first.process==ordinary_launch::current_process()&&
                first.thread==std::this_thread::get_id()&&first.original.lock()==data&&first.schema_original.lock()==schema&&
                first.schema==schema.get()&&first.connection==this&&first.owner==&owner&&first.owner_original.lock().get()==&owner&&
                first.end==data->end&&first.source_first_failure==data->first_failure&&first.objects.in_creator_process()&&
                terminal_raw_objects_context_known(owner,current_scope,current_generation)&&current_scope==first.scope&&
                current_generation==first.generation&&terminal_programme_originals_known(owner,*schema,first.source_originals,first.source_failures);
        };
        if(!bound())return false;
        const auto prepare=[&]<std::size_t At>(const auto& programme) {
            if(!programme)return;
            std::array<const void*,4> payloads{};std::array<std::size_t,4> counts{};
            const void* projection=nullptr;std::size_t items=0,cells=0,bindings=0;
            bool entered_observed=false,entered=false,validation_observed=false,validation_entered=false,validation_returned=false;
            if constexpr(requires {programme->projection;})projection=programme->projection.get();
            if constexpr(requires {programme->operations;})items=programme->operations.size();
            if constexpr(requires {programme->tables;})items=programme->tables.size();
            if constexpr(requires {programme->cells;})cells=programme->cells;
            if constexpr(requires {programme->bindings;})bindings=programme->bindings;
            if constexpr(requires {programme->generation_entered;}){entered_observed=true;entered=programme->generation_entered;}
            if constexpr(requires {programme->validation_entered;}) {
                validation_observed=true;validation_entered=programme->validation_entered;validation_returned=programme->validation_returned;
                payloads={programme->plan.get(),programme->logical.get(),programme->metadata.get(),programme->internal.get()};
                counts={std::size_t(programme->plan.use_count()),std::size_t(programme->logical.use_count()),
                    std::size_t(programme->metadata.use_count()),std::size_t(programme->internal.use_count())};
            }
            first.records[At]=std::shared_ptr<ordinary_target_programme_record>(new ordinary_target_programme_record(
                programme.get(),schema.get(),At,programme->first_failure,projection,payloads,counts,programme->bytes,items,cells,bindings,
                programme->terminal_programme_.construction_returned_,entered_observed,entered,programme->generation_returned,
                validation_observed,validation_entered,validation_returned));
        };
        prepare.template operator()<0>(data->catalog);prepare.template operator()<1>(data->projection);
        prepare.template operator()<2>(data->import_suspension);prepare.template operator()<3>(data->row_import);
        const auto bind=[&]<std::size_t At>(const auto& programme) {
            return !programme||(bound()&&programme->terminal_programme_.bind(first.records[At]));
        };
        if(!bind.template operator()<0>(data->catalog)||!bind.template operator()<1>(data->projection)||
           !bind.template operator()<2>(data->import_suspension)||!bind.template operator()<3>(data->row_import))return false;
        first.preparation_returned=true;
        const auto take=[&]<std::size_t At>(auto& programme) {
            const auto& record=first.records[At];if(!record)return !first.issuance[At];
            if(!bound()||programme.get()!=record->original)return false;
            static_assert(noexcept(first.objects.template get<At>()=std::move(programme)));
            record->extraction_attempted=true;first.objects.template get<At>()=std::move(programme);
            record->extraction_returned=true;return true;
        };
        if(!take.template operator()<0>(data->catalog)||!take.template operator()<1>(data->projection)||
           !take.template operator()<2>(data->import_suspension)||!take.template operator()<3>(data->row_import))return false;
        const auto reset=[&]<std::size_t At>() {
            const auto& record=first.records[At];if(!record)return !first.issuance[At];
            if(!bound())return false;record->reset_attempted=true;
            if(!first.objects.template release<At>()||!bound())return false;
            record->reset_returned=true;
            return record->members_destroyed&&!record->partial_constructor;
        };
        // These two real programmes own the only internal projection aliases.
        if(!reset.template operator()<3>()||!reset.template operator()<2>()||
           !reset.template operator()<0>()||!reset.template operator()<1>())return false;
        first.returned=true;
        const bool ended=terminal_schema_programmes_ended(owner);
        if(ended)owner.schedule_collection(); // Actual post-tail/whole-reset wake, outside all leaves, once only.
        return ended;
    }catch(...) {
        if(data->process==creator()) {
            if(!data->programmes_original){data->programmes_allocation_exception_returned=true;data->programmes_allocation_returned=true;}
            if(!data->programmes_first_failure)data->programmes_first_failure=std::current_exception();
            if(data->programmes_original&&!data->programmes_original->first_failure)data->programmes_original->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_schema_programmes_ended(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    try {
        const auto receiving=owner.state_;if(!receiving||receiving->process!=creator())return false;
        if(receiving->schema_roots_attempted)return terminal_schema_roots_ended(owner);
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
               !receiving->stop||!receiving->cleanup_entered||receiving->connection.get()!=this)return false;}
        const auto& schema=receiving->schema;
        const auto data=schema?schema->state_:nullptr;
        if(!data||data->process!=creator()||!data->programmes_attempted||!data->programmes_allocation_returned||
           data->programmes_allocation_exception_returned||data->programmes_first_failure||!data->programmes_original)return false;
        const auto& first=*data->programmes_original;
        const void* scope=nullptr;std::uint64_t generation=0;
        if(first.process!=ordinary_launch::current_process()||first.thread!=std::this_thread::get_id()||
           first.original.lock()!=data||first.schema_original.lock()!=schema||first.schema!=schema.get()||
           first.connection!=this||first.owner!=&owner||first.owner_original.lock().get()!=&owner||first.end!=data->end||
           first.source_first_failure!=data->first_failure||!first.objects.in_creator_process()||!first.preparation_returned||
           !first.returned||first.first_failure||!terminal_raw_objects_context_known(owner,scope,generation)||
           scope!=first.scope||generation!=first.generation||
           !terminal_programme_originals_known(owner,*schema,first.source_originals,first.source_failures))return false;
        bool any=false;
        for(std::size_t at=0;at<4;++at) {
            const auto& record=first.records[at];
            if(!record) {if(first.issuance[at]||first.source_originals[at]||first.source_failures[at])return false;continue;}
            any=true;
            if(!first.issuance[at]||record->original!=first.source_originals[at]||record->schema!=schema.get()||record->kind!=at||
               record->first_failure!=first.source_failures[at]||!record->constructor_returned||
               !record->binding_attempted||!record->binding_returned||!record->extraction_attempted||!record->extraction_returned||
               !record->reset_attempted||!record->reset_returned||!record->members_destroyed||record->partial_constructor)return false;
            if(record->generation_entered_observed!=(at!=0)||record->validation_observed!=(at==1)||
               (at>=2?record->projection_original!=first.source_originals[1]:record->projection_original!=nullptr))return false;
            for(std::size_t payload=0;payload<4;++payload)
                if(record->payload_originals[payload]?record->payload_owner_counts[payload]!=1:record->payload_owner_counts[payload]!=0)return false;
        }
        return any; // Programme member endings only; no scope/owner/registry/refund result.
    }catch(...){return false;}
}

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
ordinary_target_programme_observation ordinary_target_staging_connection::qualification_programme_observation(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    ordinary_target_programme_observation out;
    try {
        const auto receiving=owner.state_;
        if(!receiving||receiving->process!=creator())return out;
        if(receiving->schema_roots_attempted)return qualification_programme_after_schema_observation(owner);
        {std::lock_guard lock(receiving->leaf);
            if(!receiving->worker_entered||receiving->creator_thread!=std::this_thread::get_id()||
               receiving->connection.get()!=this||receiving->receiving_original.lock().get()!=&owner)return out;}
        const auto& schema=receiving->schema;
        const auto data=schema?schema->state_:nullptr;
        if(!data||data->process!=creator()||data->connection.get()!=this||data->end!=receiving->end)return out;
        out.creator_worker=true;out.receiving_owner_bound=true;out.schema_original=reinterpret_cast<std::uintptr_t>(schema.get());
        out.attempted=data->programmes_attempted;out.allocation_returned=data->programmes_allocation_returned;
        out.allocation_exception_returned=data->programmes_allocation_exception_returned;out.first_failure=bool(data->programmes_first_failure);
        const auto bit=[](auto& value,unsigned at,bool actual){if(actual)value.flags|=std::uint64_t{1}<<at;};
        const auto address=[](const void* value){return reinterpret_cast<std::uintptr_t>(value);};
        if(data->programmes_attempted) {
            out.result=2;out.history_checked=true;out.history_known=terminal_schema_programmes_ended(owner);
            const auto& first=data->programmes_original;if(!first)return out;
            if(first->original.lock()!=data||first->schema_original.lock()!=schema||first->owner!=&owner||
               first->owner_original.lock().get()!=&owner||first->connection!=this)return {};
            out.first_failure=out.first_failure||bool(first->first_failure);out.preparation_returned=first->preparation_returned;out.returned=first->returned;
            out.source_failure_observed=true;out.source_failure=bool(first->source_first_failure);
            if(out.history_known)out.result=1;
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
            return out; // After ANY attempt, never read a live programme or infer an empty source.
        }
        out.source_failure_observed=true;out.source_failure=bool(data->first_failure);
        const std::array<bool,4> issued{data->catalog_preparation_attempted,data->projection_preparation_attempted,
            data->import_suspension_attempted,data->row_import_attempted};
        const auto live=[&]<std::size_t At>(const auto& programme) {
            auto& value=out.objects[At];bit(value,0,true);bit(value,1,issued[At]);if(!programme)return true;
            const auto& tail=programme->terminal_programme_;
            if(programme->original.lock().get()!=schema.get()||tail.process_!=ordinary_launch::current_process()||
               tail.thread_!=std::this_thread::get_id()||tail.original_!=programme.get()||tail.schema_!=schema.get()||tail.kind_!=At)return false;
            value.original=address(programme.get());value.owner_count=programme.use_count();value.bytes=programme->bytes;
            bit(value,2,true);bit(value,3,true);bit(value,4,tail.construction_returned_);bit(value,5,true);bit(value,6,true);bit(value,7,bool(programme->first_failure));
            if constexpr(requires {programme->projection;}){value.projection_original=address(programme->projection.get());bit(value,23,true);}
            if constexpr(requires {programme->operations;})value.items=programme->operations.size();
            if constexpr(requires {programme->tables;})value.items=programme->tables.size();
            if constexpr(requires {programme->cells;})value.cells=programme->cells;
            if constexpr(requires {programme->bindings;})value.bindings=programme->bindings;
            if constexpr(requires {programme->generation_entered;}){bit(value,16,true);bit(value,17,programme->generation_entered);}
            bit(value,18,true);bit(value,19,programme->generation_returned);
            if constexpr(requires {programme->validation_entered;}) {
                bit(value,20,true);bit(value,21,programme->validation_entered);bit(value,22,programme->validation_returned);
                out.projection_payloads_observed=true;
                out.projection_payloads={address(programme->plan.get()),address(programme->logical.get()),address(programme->metadata.get()),address(programme->internal.get())};
                out.projection_payload_owner_counts={std::uint64_t(programme->plan.use_count()),std::uint64_t(programme->logical.use_count()),
                    std::uint64_t(programme->metadata.use_count()),std::uint64_t(programme->internal.use_count())};
            }
            return true;
        };
        if(!live.template operator()<0>(data->catalog)||!live.template operator()<1>(data->projection)||
           !live.template operator()<2>(data->import_suspension)||!live.template operator()<3>(data->row_import))return {};
        return out;
    }catch(...){return {};}
}
#endif
