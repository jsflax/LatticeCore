// Actual shared RAW facade/state endings, after their original physical close
// and all Native users. No syscall, payload relocation or admission/refund.
struct ordinary_target_raw_object_record final {
    friend class ordinary_target_staging_connection;
    friend class ordinary_target_raw_object_tail;
    struct call_fact {
        const void* original=nullptr;
        std::int64_t result=-1;int error=0;
        bool attempted=false,entered=false,returned=false;
    };
    struct call_bank {std::array<call_fact,300> facts{};std::size_t count=0;};
    struct physical_fact {
        const void* original=nullptr;
        std::uint64_t device=0,inode=0,links=0,uid=0,mode=0;
        std::int64_t size=0,blocks=0,result=-1;int error=0;
        bool attempted=false,entered=false,returned=false;
    };
    const void* const facade_original;
    const void* const state_original;
    const unsigned kind;
    const call_bank calls;
    const std::array<physical_fact,6> physical;
    const std::uint64_t extent_charge;
    const std::exception_ptr source_first_failure;
    const bool facade_constructor_returned,state_constructor_returned;
    bool facade_binding_attempted=false,facade_binding_returned=false;
    bool state_binding_attempted=false,state_binding_returned=false;
    bool facade_members_destroyed=false,state_members_destroyed=false;
    bool facade_partial_constructor=false,state_partial_constructor=false;
private:
    ordinary_target_raw_object_record(const void* facade,const void* state,unsigned selected,
        call_bank actual,std::array<physical_fact,6> native_physical,std::uint64_t original_extent,
        std::exception_ptr error,bool facade_ctor,bool state_ctor)
        :facade_original(facade),state_original(state),kind(selected),calls(std::move(actual)),physical(std::move(native_physical)),extent_charge(original_extent),
         source_first_failure(std::move(error)),facade_constructor_returned(facade_ctor),state_constructor_returned(state_ctor){}
};
ordinary_target_raw_object_tail::ordinary_target_raw_object_tail(const void* object,const void* state,unsigned kind)noexcept
    :object_(object),state_(state),kind_(kind){}
bool ordinary_target_raw_object_tail::bind(const std::shared_ptr<ordinary_target_raw_object_record>& actual)noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!actual||first_||
       !construction_returned_||kind_/2!=actual->kind||state_!=actual->state_original||
       object_!=(kind_%2?actual->state_original:actual->facade_original))return false;
    auto& entered=kind_%2?actual->state_binding_attempted:actual->facade_binding_attempted;
    auto& returned=kind_%2?actual->state_binding_returned:actual->facade_binding_returned;
    if(entered)return false;entered=true;first_=actual;returned=true;return true;
}
ordinary_target_raw_object_tail::~ordinary_target_raw_object_tail()noexcept {
    if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!first_||
       first_->kind!=kind_/2||state_!=first_->state_original||
       object_!=(kind_%2?first_->state_original:first_->facade_original))return;
    if(kind_%2){first_->state_partial_constructor=!construction_returned_;first_->state_members_destroyed=true;}
    else {first_->facade_partial_constructor=!construction_returned_;first_->facade_members_destroyed=true;}
}
struct ordinary_target_staging_connection::state::terminal_raw_objects_ending final {
    using custody=native_retirement_storage<std::shared_ptr<ordinary_target_history_spool>,
        std::shared_ptr<ordinary_target_history_page_append>,std::shared_ptr<ordinary_target_history_page_append>,
        std::shared_ptr<ordinary_target_history_page_reader>,std::shared_ptr<ordinary_target_history_page_reader>>;
    const std::weak_ptr<state> original;
    const ordinary_target_staging_connection* const facade;
    const ordinary_group_target_receiving_owner* const owner;
    const std::weak_ptr<ordinary_group_target_receiving_owner> owner_original;
    const ::lattice::database* const shell;
    const void* const target;
    const void* const scope;
    const std::uint64_t generation;
    const ordinary_launch::deadline end;
    const terminal_raw_retirement_result raw_result;
    const std::exception_ptr allocation_first_failure;
    const bool spool_attempted;
    custody objects;
    std::array<std::shared_ptr<ordinary_target_raw_object_record>,5> records{};
    std::array<bool,5> source_issued{},extraction_attempted{},extraction_returned{},reset_attempted{},reset_returned{};
    bool preparation_returned=false,returned=false;
    std::exception_ptr first_failure;
    terminal_raw_objects_ending(const std::shared_ptr<state>& actual,const ordinary_target_staging_connection& connection,
        const ordinary_group_target_receiving_owner& receiving)
        :original(actual),facade(&connection),owner(&receiving),owner_original(receiving.terminal_retirement_wake_original(connection)),shell(actual->shell.get()),target(actual->target.get()),
         scope(actual->terminal_scope.scope_original.get()),generation(actual->terminal_scope.generation),end(actual->end),
         raw_result(actual->raw_ending_result),allocation_first_failure(actual->history_spool_allocation.exception_original),
         spool_attempted(actual->history_spool_attempted){}
};
bool ordinary_target_staging_connection::terminal_raw_objects_binding_known(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    try {
        const auto native=state_;if(!native||!native->creator_current()||!native->shell||
           native->facade.lock().get()!=this||!native->raw_objects_attempted||!native->raw_objects_original)return false;
        const auto& first=*native->raw_objects_original;
        return first.original.lock()==native&&first.facade==this&&first.owner==&owner&&first.owner_original.lock().get()==&owner&&first.shell==native->shell.get()&&
            first.target==native->target.get()&&first.end==native->end&&first.generation==native->terminal_scope.generation&&
            terminal_scope_root_identity_matches(owner,first.scope,first.generation)&&first.objects.in_creator_process()&&
            first.raw_result==native->raw_ending_result&&first.spool_attempted==native->history_spool_attempted&&
            owner.terminal_retirement_wake_original(*this).lock().get()==&owner&&
            terminal_scope_history_binding_known(*native->shell);
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_raw_objects_history_known(const ::lattice::database& actual)const noexcept {
    try {
        const auto native=state_;if(!native||!native->creator_current()||native->shell.get()!=&actual||
           !native->raw_objects_attempted||!native->raw_objects_allocation_returned||
           native->raw_objects_allocation_exception_returned||native->raw_objects_first_failure||!native->raw_objects_original)return false;
        const auto& first=*native->raw_objects_original;
        const auto owner=first.owner_original.lock();
        if(!owner||owner.get()!=first.owner||!terminal_raw_objects_binding_known(*owner)||!first.preparation_returned||!first.returned||first.first_failure)return false;
        if(first.raw_result==terminal_raw_retirement_result::not_issued) {
            for(std::size_t at=0;at<5;++at)if(first.records[at]||first.source_issued[at]||first.extraction_attempted[at]||
                first.extraction_returned[at]||first.reset_attempted[at]||first.reset_returned[at])return false;
            return true; // Actual original not-issued DATA, never object destruction.
        }
        if(first.raw_result!=terminal_raw_retirement_result::joined||!first.source_issued[0])return false;
        for(std::size_t at=0;at<5;++at) {
            const auto& record=first.records[at];
            if(!first.source_issued[at]) {
                if(record||first.extraction_attempted[at]||first.extraction_returned[at]||first.reset_attempted[at]||first.reset_returned[at])return false;
                continue;
            }
            if(!record||!record->facade_original||!record->state_original||record->kind!=(at==0?0:at<3?1:2)||
               !record->facade_constructor_returned||!record->state_constructor_returned||
               !record->facade_binding_attempted||!record->facade_binding_returned||
               !record->state_binding_attempted||!record->state_binding_returned||
               !record->facade_members_destroyed||!record->state_members_destroyed||
               record->facade_partial_constructor||record->state_partial_constructor||
               !first.extraction_attempted[at]||!first.extraction_returned[at]||!first.reset_attempted[at]||!first.reset_returned[at]||
               record->calls.count>record->calls.facts.size())return false;
            for(std::size_t call=0;call<record->calls.count;++call) {
                const auto& value=record->calls.facts[call];
                if(!value.original||(value.entered&&(!value.attempted||!value.returned))||(!value.entered&&value.returned))return false;
            }
        }
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto& spool=*first.records[0];
        const auto zero=[](const auto& call)noexcept {return call.original&&call.attempted&&call.entered&&call.returned&&call.result==0;};
        if(spool.calls.count<10||!spool.calls.facts[0].attempted||!spool.calls.facts[0].entered||!spool.calls.facts[0].returned||
           spool.calls.facts[0].result<0||!zero(spool.calls.facts[5])||!zero(spool.calls.facts[6])||!zero(spool.calls.facts[7]))return false;
        const auto& physical=spool.physical;
        const auto directory=[&](const auto& value) {
            return zero(value)&&S_ISDIR(value.mode)&&value.uid==::geteuid()&&(value.mode&07777)==0700&&
                value.device==native->member_device&&value.inode==native->member_inode;
        };
        const auto file=[&](const auto& value,std::uint64_t links) {
            const auto& original=physical[1];
            return zero(value)&&S_ISREG(value.mode)&&value.uid==::geteuid()&&(value.mode&07777)==0600&&value.links==links&&
                value.device==original.device&&value.inode==original.inode&&value.size==original.size&&value.blocks==original.blocks&&
                value.size>=0&&value.blocks>=0&&std::uint64_t(value.size)<=spool.extent_charge&&std::uint64_t(value.blocks)<=spool.extent_charge/512;
        };
        if(!directory(physical[0])||!directory(physical[3])||!file(physical[1],1)||!file(physical[2],1)||!file(physical[4],0)||
           !physical[5].original||!physical[5].attempted||!physical[5].entered||!physical[5].returned||physical[5].result!=-1||physical[5].error!=ENOENT)return false;
#else
        return false;
#endif
        return true; // This shared RAW family only, no owner charge or registry refund.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::retire_terminal_raw_objects(const ordinary_group_target_receiving_owner& owner)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||!native->shell)return false;
    if(native->raw_objects_attempted)return terminal_raw_objects_ended(owner); // First facts only; never retry.
    // Real predecessor history includes all Native users of append/read aliases.
    (void)retire_terminal_native_statics(owner);
    if(!terminal_native_statics_ended(owner)||!terminal_raw_ending_after_scope_known(*native->shell)||
       owner.terminal_retirement_wake_original(*this).lock().get()!=&owner)return false;
    const auto& spool=native->history_spool;
    if(native->raw_ending_result==terminal_raw_retirement_result::joined) {
        if(!spool||spool.use_count()!=1||!spool->state_||spool->state_.use_count()!=1)return false;
        const auto& data=*spool->state_;
        const auto sole=[&](const auto& selected,const auto& other) {
            return !selected||(selected->state_&&selected->state_.use_count()==1&&
                selected.use_count()==(selected==other?2:1));
        };
        if(!sole(data.page_append,data.page_observation)||!sole(data.page_observation,data.page_append)||
           !sole(data.page_reader,data.read_observation)||!sole(data.read_observation,data.page_reader))return false;
    }else if(native->raw_ending_result!=terminal_raw_retirement_result::not_issued||spool)return false;
    native->raw_objects_attempted=true; // Before any first bank allocation/binding/extraction.
    try {
        auto* actual=new state::terminal_raw_objects_ending(native,*this,owner);
        native->raw_objects_original=std::shared_ptr<state::terminal_raw_objects_ending>(actual);
        native->raw_objects_allocation_returned=true;
        auto& first=*native->raw_objects_original;
        if(!terminal_raw_objects_binding_known(owner))return false;
        if(first.raw_result==terminal_raw_retirement_result::not_issued) {
            first.preparation_returned=true;first.returned=true;return false;
        }
        auto& data=*spool->state_;
        const auto record=[&](const auto& source,unsigned kind) {
            if(!source)return std::shared_ptr<ordinary_target_raw_object_record>{};
            const auto& phase=*source->state_;
            ordinary_target_raw_object_record::call_bank calls;
            std::array<ordinary_target_raw_object_record::physical_fact,6> physical{};
            std::uint64_t extent=0;
            const auto add=[&](const auto& call) {
                if(calls.count==calls.facts.size())birth_fail("terminal RAW first call history bound exhausted");
                calls.facts[calls.count++]={&call,call.result,call.error,call.attempted,call.entered,call.returned};
            };
            const auto snapshot=[&](const auto& value){add(value.directory);add(value.held);add(value.named);};
#ifdef LATTICE_TARGET_MAIN_POSIX
            if constexpr(requires {phase.header_reads;}) {
                static_assert(10+9*3+4*ordinary_target_history_spool::state::header_read_limit<=300);
                add(phase.open);add(phase.allocate);add(phase.resize);add(phase.file_sync);add(phase.directory_sync);
                add(phase.unlink);add(phase.unlink_directory_sync);add(phase.close);add(phase.header_write);add(phase.header_sync);
                snapshot(phase.before);snapshot(phase.allocated);snapshot(phase.sized);snapshot(phase.durable);
                snapshot(phase.ending_before);snapshot(phase.ending_unlinked);snapshot(phase.abandoned_birth);
                snapshot(phase.header_written);snapshot(phase.header_durable);
                for(std::size_t at=0;at<phase.header_read_count;++at){add(phase.header_reads[at]);snapshot(phase.header_reads[at].after);}
                const auto physical_first=[&](const auto& call,std::size_t at) {
                    const auto& value=call.value;
                    physical[at]={&call,std::uint64_t(value.st_dev),std::uint64_t(value.st_ino),std::uint64_t(value.st_nlink),
                        std::uint64_t(value.st_uid),std::uint64_t(value.st_mode),std::int64_t(value.st_size),std::int64_t(value.st_blocks),
                        call.result,call.error,call.attempted,call.entered,call.returned};
                };
                physical_first(phase.abandoned_birth.directory,0);physical_first(phase.abandoned_birth.held,1);physical_first(phase.abandoned_birth.named,2);
                physical_first(phase.ending_unlinked.directory,3);physical_first(phase.ending_unlinked.held,4);physical_first(phase.ending_unlinked.named,5);
                extent=phase.extent_charge;
            }else if constexpr(requires {phase.input_pin;}) {
                static_assert(2+6+4*ordinary_target_history_page_append::state::read_limit<=300);
                add(phase.write);add(phase.sync);snapshot(phase.written);snapshot(phase.durable);
                for(std::size_t at=0;at<phase.read_count;++at){add(phase.reads[at]);snapshot(phase.reads[at].after);}
            }else {
                static_assert(3+4*ordinary_target_history_page_reader::state::read_limit<=300);
                snapshot(phase.before);for(std::size_t at=0;at<phase.read_count;++at){add(phase.reads[at]);snapshot(phase.reads[at].after);}
            }
#else
            birth_fail("terminal RAW object retirement unsupported");
#endif
            return std::shared_ptr<ordinary_target_raw_object_record>(new ordinary_target_raw_object_record(source.get(),source->state_.get(),kind,
                std::move(calls),std::move(physical),extent,phase.first_failure,source->terminal_raw_tail_.construction_returned_,phase.terminal_raw_tail_.construction_returned_));
        };
        first.source_issued={true,bool(data.page_append),bool(data.page_observation),bool(data.page_reader),bool(data.read_observation)};
        first.records[0]=record(spool,0);
        first.records[1]=record(data.page_append,1);
        first.records[2]=data.page_observation&&data.page_observation==data.page_append?first.records[1]:record(data.page_observation,1);
        first.records[3]=record(data.page_reader,2);
        first.records[4]=data.read_observation&&data.read_observation==data.page_reader?first.records[3]:record(data.read_observation,2);
        const auto bind=[&](const auto& source,std::size_t at,bool alias=false) {
            if(!source)return true;if(alias)return true;
            return terminal_raw_objects_binding_known(owner)&&source->terminal_raw_tail_.bind(first.records[at])&&source->state_->terminal_raw_tail_.bind(first.records[at]);
        };
        if(!bind(spool,0)||!bind(data.page_append,1)||!bind(data.page_observation,2,data.page_observation==data.page_append)||
           !bind(data.page_reader,3)||!bind(data.read_observation,4,data.read_observation==data.page_reader))return false;
        first.preparation_returned=true;
        const auto take=[&]<std::size_t At>(auto& source) {
            if(!first.source_issued[At])return true;
            if(!terminal_raw_objects_binding_known(owner))return false;
            first.extraction_attempted[At]=true;first.objects.template get<At>()=std::move(source);
            first.extraction_returned[At]=true;return true;
        };
        // Children leave the actual spool state before its own last reset.
        if(!take.template operator()<1>(data.page_append)||!take.template operator()<2>(data.page_observation)||
           !take.template operator()<3>(data.page_reader)||!take.template operator()<4>(data.read_observation)||
           !take.template operator()<0>(native->history_spool))return false;
        const auto reset=[&]<std::size_t At>() {
            if(!first.source_issued[At])return true;
            if(!terminal_raw_objects_binding_known(owner))return false;
            first.reset_attempted[At]=true;
            if(!first.objects.template release<At>()||!terminal_raw_objects_binding_known(owner))return false;
            first.reset_returned[At]=true;return true;
        };
        // Exact internal predecessor aliases release before the primary alias.
        if(!reset.template operator()<2>()||!reset.template operator()<1>()||!reset.template operator()<4>()||
           !reset.template operator()<3>()||!reset.template operator()<0>())return false;
        first.returned=true;return terminal_raw_objects_ended(owner);
    }catch(...) {
        if(native->creator_current()) {
            if(!native->raw_objects_original){native->raw_objects_allocation_exception_returned=true;native->raw_objects_allocation_returned=true;}
            if(!native->raw_objects_first_failure)native->raw_objects_first_failure=std::current_exception();
            if(native->raw_objects_original&&!native->raw_objects_original->first_failure)native->raw_objects_original->first_failure=std::current_exception();
        }
        return false;
    }
}
bool ordinary_target_staging_connection::terminal_raw_objects_ended(const ordinary_group_target_receiving_owner& owner)const noexcept {
    const auto native=state_;
    return native&&native->creator_current()&&native->shell&&terminal_raw_objects_binding_known(owner)&&
        native->raw_objects_original->raw_result==terminal_raw_retirement_result::joined&&
        terminal_raw_objects_history_known(*native->shell);
}

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
ordinary_target_raw_object_observation ordinary_target_staging_connection::qualification_raw_object_observation(
    const ordinary_group_target_receiving_owner& owner)const noexcept {
    ordinary_target_raw_object_observation out;
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||!native->shell||
           owner.terminal_retirement_wake_original(*this).lock().get()!=&owner)return out;
        out.creator_worker=true;out.receiving_owner_bound=true;
        out.attempted=native->raw_objects_attempted;out.allocation_returned=native->raw_objects_allocation_returned;
        out.allocation_exception_returned=native->raw_objects_allocation_exception_returned;out.first_failure=bool(native->raw_objects_first_failure);
        const auto bit=[](auto& value,unsigned at,bool actual){if(actual)value.flags|=std::uint64_t{1}<<at;};
        const auto address=[](const void* original){return reinterpret_cast<std::uintptr_t>(original);};
        if(native->raw_objects_attempted) {
            out.result=2;out.history_checked=true;out.history_known=terminal_raw_objects_history_known(*native->shell);
            const auto& first=native->raw_objects_original;if(!first)return out;
            if(first->owner!=&owner||first->owner_original.lock().get()!=&owner||first->original.lock()!=native)return {};
            out.preparation_returned=first->preparation_returned;out.returned=first->returned;out.first_failure=out.first_failure||bool(first->first_failure);
            out.not_issued_observed=first->returned;out.not_issued=first->raw_result==terminal_raw_retirement_result::not_issued;
            if(out.history_known)out.result=out.not_issued?0:1;
            for(std::size_t at=0;at<5;++at) {
                const auto& record=first->records[at];if(!record)continue;
                auto& value=out.objects[at];value.original=address(record->facade_original);value.state_original=address(record->state_original);
                value.kind=std::uint8_t(record->kind);value.alias_of=std::uint8_t(at);
                for(std::size_t old=0;old<at;++old)if(first->records[old]==record){value.alias_of=std::uint8_t(old);break;}
                bit(value,0,first->source_issued[at]);bit(value,1,true);bit(value,2,record->facade_constructor_returned);
                bit(value,3,true);bit(value,4,record->state_constructor_returned);bit(value,6,true);bit(value,7,bool(record->source_first_failure));
                bit(value,8,record->facade_binding_attempted);bit(value,9,record->facade_binding_returned);
                bit(value,10,record->state_binding_attempted);bit(value,11,record->state_binding_returned);
                bit(value,12,first->extraction_attempted[at]);bit(value,13,first->extraction_returned[at]);
                bit(value,14,first->reset_attempted[at]);bit(value,15,first->reset_returned[at]);
                bit(value,16,record->facade_members_destroyed);bit(value,17,record->state_members_destroyed);
                bit(value,18,record->facade_partial_constructor);bit(value,19,record->state_partial_constructor);
                bit(value,20,true);bool calls_known=record->calls.count<=record->calls.facts.size();
                for(std::size_t call=0;calls_known&&call<record->calls.count;++call) {
                    const auto& actual=record->calls.facts[call];
                    calls_known=actual.original&&(!actual.entered||(actual.attempted&&actual.returned))&&(actual.entered||!actual.returned);
                }
                bit(value,21,calls_known);
                bool ended=record->facade_constructor_returned&&record->state_constructor_returned&&record->facade_binding_attempted&&
                    record->facade_binding_returned&&record->state_binding_attempted&&record->state_binding_returned&&
                    record->facade_members_destroyed&&record->state_members_destroyed&&!record->facade_partial_constructor&&!record->state_partial_constructor;
                for(std::size_t alias=0;alias<5;++alias)if(first->records[alias]==record)
                    ended=ended&&first->source_issued[alias]&&first->extraction_attempted[alias]&&first->extraction_returned[alias]&&
                        first->reset_attempted[alias]&&first->reset_returned[alias];
                bit(value,22,ended);
            }
            if(first->records[0]&&first->records[0]->calls.count>=8) {
                const auto& close=first->records[0]->calls.facts[7];
                out.physical_close_observed=close.attempted&&close.entered&&close.returned;
                out.physical_close_result=close.result;out.physical_close_error=close.error;
            }
            return out; // Never live source fallback after any attempted first bank.
        }
        out.not_issued_observed=native->raw_ending_attempted&&native->raw_ending_returned;
        out.not_issued=out.not_issued_observed&&native->raw_ending_result==terminal_raw_retirement_result::not_issued;
        const auto& spool=native->history_spool;if(!spool)return out;
        const auto& data=spool->state_;if(!data||!data->creator_current()||data->connection_identity!=this||
            data->connection.lock()!=native->facade.lock()||data->receiver_identity!=&owner||data->receiver.lock().get()!=&owner)return {};
        const auto live=[&](const auto& source,std::size_t at,unsigned kind) {
            if(!source)return;const auto& phase=source->state_;if(!phase)return;
            auto& value=out.objects[at];value.original=address(source.get());value.state_original=address(phase.get());
            value.kind=std::uint8_t(kind);value.alias_of=std::uint8_t(at);
            for(std::size_t old=0;old<at;++old)if(out.objects[old].original==value.original){value.alias_of=std::uint8_t(old);break;}
            value.owner_count=source.use_count();value.state_owner_count=phase.use_count();
            bit(value,0,true);bit(value,1,true);bit(value,2,source->terminal_raw_tail_.construction_returned_);
            bit(value,3,true);bit(value,4,phase->terminal_raw_tail_.construction_returned_);bit(value,5,true);
            bit(value,6,true);bit(value,7,bool(phase->first_failure));bit(value,20,true);
            if constexpr(requires {phase->header_reads;})bit(value,21,source->abandonment_inputs_known());
            else bit(value,21,source->cleanup_known());
        };
        live(spool,0,0);live(data->page_append,1,1);live(data->page_observation,2,1);live(data->page_reader,3,2);live(data->read_observation,4,2);
        out.physical_close_observed=data->close.attempted&&data->close.entered&&data->close.returned;
        out.physical_close_result=data->close.result;out.physical_close_error=data->close.error;return out;
    }catch(...){return {};}
}
#endif
