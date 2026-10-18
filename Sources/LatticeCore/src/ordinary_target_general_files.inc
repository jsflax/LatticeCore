// Included only after the actual Conn, callback/native evidence, RAW states
// and first RAW ending producers. General files-only disposition: no VFS,
// payload/lease, cleanup leave, owner join, admission or credit is issued.
bool ordinary_target_staging_connection::terminal_raw_ending_historical_known(
    const ::lattice::database& actual)const noexcept {
    try {
        if(state_&&state_->creator_current()&&state_->raw_objects_attempted)return false; // No active/live RAW fallback after any object attempt.
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||
           native->shell.get()!=&actual||!terminal_cleanup_pending(actual)||
           !native->raw_ending_attempted||!native->raw_ending_returned)return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||native->last_scope!=scope||!scope->cleanup||scope->left||
           !scope->creator_binding(actual)||scope->generation!=native->scope_generation)return false;
        if(native->raw_ending_result==terminal_raw_retirement_result::not_issued) {
            const auto& allocation=native->history_spool_allocation;
            return (!native->history_spool_attempted&&!native->history_spool&&!allocation.attempted)||
                (native->history_spool_attempted&&allocation.attempted&&allocation.returned&&
                 allocation.exception_returned&&allocation.exception_original&&!native->history_spool);
            // Issuance DATA only. The separate actual member census must
            // still refuse any unexpected RAW or other namespace entry.
        }
        if(native->raw_ending_result!=terminal_raw_retirement_result::joined||
           !native->history_spool_attempted||!native->history_spool)return false;
        const auto spool=native->history_spool;const auto data=spool->state_;
        if(!data||!data->creator_current()||data->connection.lock().get()!=this||
           data->connection_identity!=this||data->target!=native->target||
           data->member!=native->member_fd||data->member_device!=native->member_device||
           data->member_inode!=native->member_inode||!data->abandoned_selected||
           data->abandoned_scope_original!=scope.get()||data->abandoned_generation!=scope->generation||
           !data->ending_attempted||!data->ending_joined||!data->abandoned_birth_known||
           data->fd>=0||!data->unlinked||!spool->abandonment_inputs_known())return false;
        const auto returned_zero=[](const auto& call)noexcept {
            return call.attempted&&call.entered&&call.returned&&call.result==0;
        };
        if(!data->open.attempted||!data->open.entered||!data->open.returned||data->open.result<0||
           !returned_zero(data->unlink)||!returned_zero(data->unlink_directory_sync)||!returned_zero(data->close))return false;
        const auto& birth=data->abandoned_birth;
        const auto& gone=data->ending_unlinked;
        if(!birth.attempted||!birth.returned||!returned_zero(birth.directory)||!returned_zero(birth.held)||
           !returned_zero(birth.named)||!gone.attempted||!gone.returned||!returned_zero(gone.directory)||
           !returned_zero(gone.held)||!gone.named.attempted||!gone.named.entered||!gone.named.returned||
           gone.named.result!=-1||gone.named.error!=ENOENT)return false;
        const auto same=[&](const struct stat& value,nlink_t links)noexcept {
            const auto& first=birth.held.value;
            return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&(value.st_mode&07777)==0600&&
                value.st_nlink==links&&value.st_size>=0&&value.st_blocks>=0&&
                value.st_dev==first.st_dev&&value.st_ino==first.st_ino&&
                value.st_size==first.st_size&&value.st_blocks==first.st_blocks&&
                std::uint64_t(value.st_size)<=data->extent_charge&&std::uint64_t(value.st_blocks)<=data->extent_charge/512;
        };
        if(!same(birth.held.value,1)||!same(birth.named.value,1)||!same(gone.held.value,0)||
           !data->directory_same(birth.directory.value)||!data->directory_same(gone.directory.value))return false;
        std::size_t slot=0;
        for(const auto& append:{data->page_append,data->page_observation}) {
            const auto& record=data->abandoned_inputs[slot++];
            if(!append){if(record.original||record.attempted||record.returned||record.joined)return false;continue;}
            const auto phase=append->state_;
            if(record.original!=append.get()||!record.attempted||!record.returned||!record.joined||
               !phase||phase->input_pin||phase->decoder_charge||!append->cleanup_known())return false;
        }
        for(const auto& reader:{data->page_reader,data->read_observation}) {
            const auto& record=data->abandoned_inputs[slot++];
            if(!reader){if(record.original||record.attempted||record.returned||record.joined)return false;continue;}
            const auto phase=reader->state_;
            if(record.original!=reader.get()||!record.attempted||!record.returned||!record.joined||
               !phase||!phase->buffer.empty()||phase->buffer.capacity()||phase->decoder_charge||phase->scope_pin||
               !reader->cleanup_known())return false;
        }
        return true; // Historical RAW first receipts only; no live-MAIN gate or eight-slot reuse.
#else
        (void)actual;return false;
#endif
    }catch(...){return false;}
}

bool ordinary_target_staging_connection::terminal_general_native_known(
    const ::lattice::database& actual)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||native->shell.get()!=&actual||
           !native->target||!native->io||!native->cleanup_attempted||native->abandoned_main.selected||
           !terminal_cleanup_pending(actual))return false;
        const auto scope=native->active_scope.lock();
        return scope&&scope.get()==native->scope_identity&&scope==native->last_scope&&scope->cleanup&&!scope->left&&
            scope->creator_binding(actual)&&scope->generation==native->scope_generation&&scope->close_observed&&
            scope->close_rc==SQLITE_OK&&scope->environment&&scope->ledger&&
            !native->settling_scope&&!native->settling_callback_phase&&
            actual.target_staging_native_cleanup_known_(*this)&&
            native->io->terminal_environment_historical_known(*this,scope->environment)&&
            terminal_raw_ending_historical_known(actual);
    }catch(...){return false;}
}

bool ordinary_target_staging_connection::terminal_general_files_context_known()const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!native->shell||!terminal_general_native_known(*native->shell))return false;
        const auto scope=native->active_scope.lock();const auto& files=native->general_files;
        const auto& issued=native->terminal_namespace;
        if(!files.attempted||!files.selected||files.failed||!files.count||files.count>files.files.size()||
           files.scope_original!=scope.get()||files.generation!=scope->generation||
           (!files.returned&&!files.producer_frame)||
           !issued.attempted||!issued.claim_returned||!issued.claimed||issued.failed||
           issued.scope_original!=scope.get()||issued.generation!=scope->generation||
           issued.binding.member!=native->member_fd||issued.binding.member_device!=native->member_device||
           issued.binding.member_inode!=native->member_inode||
           !native->target->terminal_namespace_historical_current(this,issued.binding))return false;
        const auto known=[](const state::disposition_call& call)noexcept {
            return (!call.entered||call.attempted)&&(!call.entered||call.returned)&&(!call.returned||call.entered);
        };
        const auto namespace_known=[&](const auto& check)noexcept {
            const bool observing=files.producer_frame&&files.observing_namespace==&check;
            if(check.attempted&&(!check.returned||!check.joined)&&
               !(observing&&!check.returned&&!check.joined))return false;
            return known(check.stage)&&known(check.root)&&known(check.root_named)&&known(check.member)&&known(check.member_named);
        };
        const auto scan_known=[&](const state::general_member_scan& scan)noexcept {
            if(!known(scan.open)||!known(scan.inspect)||!known(scan.convert)||!known(scan.close)||
               scan.read_count>scan.reads.size())return false;
            for(std::size_t at=0;at<scan.read_count;++at)if(!known(scan.reads[at].call))return false;
            return true;
        };
        if(files.observing_namespace) {
            bool issued=files.observing_namespace==&files.initial_namespace||files.observing_namespace==&files.final_namespace;
            for(std::size_t at=0;at<files.count;++at)for(const auto& check:files.files[at].namespace_checks)
                issued=issued||files.observing_namespace==&check;
            if(!files.producer_frame||!issued||!files.observing_namespace->attempted||
               files.observing_namespace->returned||files.observing_namespace->joined)return false;
        }
        if(!namespace_known(files.initial_namespace)||!namespace_known(files.final_namespace)||
           !scan_known(files.initial_census)||!scan_known(files.journals_census)||!scan_known(files.empty_census)||
           !known(files.prior_directory_sync))return false;
        for(std::size_t at=0;at<files.count;++at) {
            const auto& receipt=files.files[at];const auto file=receipt.original;
            if(!file||!file->cookie||file->borrowed||file->borrower||file->write_pending||
               file->lock_level!=SQLITE_LOCK_NONE||file->pending_byte||file->pending_write||file->reserved_byte||
               file->shared_range||file->exclusive_range||
               (file->close_attempted&&(!file->close_returned||file->close_rc!=0||file->fd>=0))||
               !known(receipt.held)||!known(receipt.named)||!known(receipt.unlink)||!known(receipt.unlinked)||
               !known(receipt.absent)||!known(receipt.directory_sync)||!known(receipt.close)||
               !scan_known(receipt.before)||!scan_known(receipt.after))return false;
            for(const auto& check:receipt.namespace_checks)if(!namespace_known(check))return false;
            if(at==0) {
                if(file!=native->resources[0]||file->journal||state::resource_slot(file->cookie)!=0||
                   !native->bound||!native->birth_verified||!file->birth)return false;
                if(file->fd>=0) {
                    if(file->fd!=native->main_fd||file->close_attempted)return false;
                    if(file->unlinked) {
                        if(file->named||!receipt.unlink.returned||receipt.unlink.result)return false;
                    }else if(!file->named||receipt.unlink.entered)return false;
                }else if(native->main_fd>=0||!file->unlinked||file->named||!receipt.close.attempted||
                         !receipt.close.entered||!receipt.close.returned||receipt.close.result)return false;
            }else if(!file->journal||state::resource_slot(file->cookie)==0||
                     state::resource_slot(file->cookie)>state::maximum_journals||!(file->cookie>>7))return false;
            for(std::size_t previous=0;previous<at;++previous) {
                const auto other=files.files[previous].original;
                if(!other||other==file||other->cookie==file->cookie)return false;
                if(other->fd>=0&&file->fd>=0&&other->fd==file->fd)return false;
                if(other->fd>=0&&file->fd>=0&&other->device&&other->inode&&file->device&&file->inode&&
                   other->device==file->device&&other->inode==file->inode)return false;
            }
        }
        const auto owned=[&](const std::shared_ptr<state::resource>& file)noexcept {
            if(!file)return false;
            for(std::size_t at=0;at<files.count;++at)if(files.files[at].original==file)return true;
            return false;
        };
        for(std::size_t at=0;at<native->resources.size();++at)if(const auto file=native->resources[at]) {
            if(!owned(file)||state::resource_slot(file->cookie)!=at||file->journal!=(at!=0))return false;
        }
        const auto bank=scope->ledger;
        if(!bank||bank->count>bank->calls.size())return false;
        for(std::size_t at=0;at<bank->count;++at)if(!owned(bank->calls[at].original))return false;
        const auto prior=scope->cleanup_predecessor;
        if(!prior||prior->retired_journal_count>prior->retired_journals.size())return false;
        if(prior->retired_journal_count&&(!prior->left||!prior->leave_returned||!prior->leave_joined))return false;
        for(std::size_t at=0;at<prior->retired_journal_count;++at) {
            const auto file=prior->retired_journals[at];
            if(!file||!file->journal||!owned(file))return false;
            for(std::size_t old=0;old<at;++old)if(prior->retired_journals[old]==file)return false;
        }
        for(const auto& phase:{scope,prior})for(const auto& callback:phase->callback_originals)
            if(callback&&callback->first_resource&&!owned(callback->first_resource))return false;
        return true; // Actual same active general scope, even after its MAIN first close.
    }catch(...){return false;}
}

int ordinary_target_staging_connection::terminal_general_file_kind(
    const ordinary_target_staging_io& io,const void* resource_original)const noexcept {
    try {
        const auto native=state_;
        if(!native||native->io.get()!=&io||!resource_original||!terminal_general_files_context_known())return -1;
        for(std::size_t at=0;at<native->general_files.count;++at) {
            const auto& file=native->general_files.files[at].original;
            if(file.get()==resource_original)return file->journal?1:0;
        }
        return -1; // Identity comparison DATA from the actual private IO observer, never an input grant.
    }catch(...){return -1;}
}

bool ordinary_target_staging_connection::terminal_general_files_ended(const ::lattice::database& actual)const noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto native=state_;
        if(!native||native->shell.get()!=&actual||!terminal_general_files_context_known())return false;
        const auto& files=native->general_files;
        if(!files.returned||!files.disposed||files.failed||files.producer_frame||files.observing_namespace)return false;
        const auto zero=[](const state::disposition_call& call)noexcept {
            return call.attempted&&call.entered&&call.returned&&call.result==0;
        };
        const auto checked=[&](const auto& check)noexcept {
            return check.attempted&&check.returned&&check.joined&&zero(check.stage)&&zero(check.root)&&
                zero(check.root_named)&&zero(check.member)&&zero(check.member_named);
        };
        const auto empty=[&](const state::general_member_scan& scan)noexcept {
            if(!scan.joined||!scan.terminal||scan.entries||scan.main_seen||scan.journal_seen||
               scan.descriptor>=0||scan.directory||!scan.open.attempted||!scan.open.entered||
               !scan.open.returned||scan.open.result<0||!zero(scan.inspect)||!zero(scan.convert)||!zero(scan.close)||
               !scan.read_count||scan.read_count>scan.reads.size())return false;
            const auto& final=scan.reads[scan.read_count-1];
            return final.call.attempted&&final.call.entered&&final.call.returned&&final.call.result==0&&final.call.error==0;
        };
        if(!checked(files.initial_namespace)||!checked(files.final_namespace)||!empty(files.empty_census)||
           !zero(files.prior_directory_sync)||native->main_fd>=0||native->named_journal)return false;
        const auto scope=native->active_scope.lock();const auto bank=scope->ledger;
        const auto physical=[&](const state::general_file_disposition& receipt,const struct stat& value,nlink_t links)noexcept {
            const auto file=receipt.original;const auto& first=receipt.held.value;
            const auto high=file->journal?native->journal_high:native->main_high;
            const auto cap=native->io->terminal_descriptor_extent_limit(*this,file.get());
            if(cap<=0||high<0||value.st_size<0||value.st_blocks<0||value.st_size>high||value.st_size>cap)return false;
            const auto ceiling=std::uint64_t(cap);const auto blocks=ceiling/512+(ceiling%512!=0);
            return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&(value.st_mode&07777)==0600&&
                value.st_nlink==links&&std::uint64_t(value.st_blocks)<=blocks&&
                value.st_dev==first.st_dev&&value.st_ino==first.st_ino&&value.st_size==first.st_size&&value.st_blocks==first.st_blocks&&
                ((!file->birth&&!file->device&&!file->inode)||
                 (std::uint64_t(value.st_dev)==file->device&&std::uint64_t(value.st_ino)==file->inode&&value.st_size==file->size));
        };
        const auto retained_call=[&](const state::native_call* original)noexcept {
            if(!original)return false;
            for(std::size_t at=0;at<bank->count;++at)if(&bank->calls[at]==original)return true;
            return false;
        };
        for(std::size_t at=0;at<files.count;++at) {
            const auto& receipt=files.files[at];const auto file=receipt.original;
            if(!receipt.attempted||!receipt.ended||!file||file->fd>=0||file->named)return false;
            if(receipt.not_issued) {
                const auto* call=receipt.prior_open;
                if(!file->journal||file->birth||file->unlinked||file->borrow_attempted||file->close_attempted||
                   file->unlink_attempted||!file->birth_attempted||!retained_call(call)||call->original!=file||
                   call->kind!=state::action::open_journal||call->fd!=native->member_fd||
                   call->entered!=file->birth_entered||call->returned!=file->birth_returned||
                   (call->entered&&(call->result>=0||call->result!=file->fd)))return false;
            }else if(receipt.prior_closed) {
                if(!file->journal||!file->birth||!file->unlinked||!file->unlink_attempted||
                   !file->close_attempted||!file->close_returned||file->close_rc!=0)return false;
            }else {
                if(!file->unlinked||!file->unlink_attempted||!file->close_attempted||!file->close_returned||file->close_rc!=0||
                   !zero(receipt.held)||!zero(receipt.unlinked)||!physical(receipt,receipt.unlinked.value,0)||
                   !receipt.absent.attempted||!receipt.absent.entered||!receipt.absent.returned||
                   receipt.absent.result!=-1||receipt.absent.error!=ENOENT||!zero(receipt.directory_sync)||!zero(receipt.close))return false;
                if(receipt.prior_unlink) {
                    if(!retained_call(receipt.prior_unlink)||receipt.prior_unlink->original!=file||
                       receipt.prior_unlink->kind!=state::action::unlink||receipt.prior_unlink->result!=0)return false;
                    if(!physical(receipt,receipt.held.value,0))return false;
                }else if(!receipt.birth_known||!zero(receipt.named)||!zero(receipt.unlink)||
                         !physical(receipt,receipt.held.value,1)||!physical(receipt,receipt.named.value,1)||
                         !checked(receipt.namespace_checks[1]))return false;
                for(std::size_t n:{std::size_t(0),std::size_t(2),std::size_t(3),std::size_t(4)})
                    if(!checked(receipt.namespace_checks[n]))return false;
            }
        }
        return true; // Actual first files disposition only, while SAME cleanup remains active.
#else
        (void)actual;return false;
#endif
    }catch(...){return false;}
}

ordinary_target_staging_connection::terminal_files_retirement_result
ordinary_target_staging_connection::retire_terminal_files(const ::lattice::database& actual)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||native->shell.get()!=&actual)return terminal_files_retirement_result::retained_debt;
    auto& files=native->general_files;
    if(files.attempted)return terminal_general_files_ended(actual)?terminal_files_retirement_result::disposed:
        terminal_files_retirement_result::retained_debt; // Observe first facts only; never restart unknown effects.
    files.attempted=true;
    struct actual_producer_frame final {
        state::general_files_disposition& original;
        explicit actual_producer_frame(state::general_files_disposition& receipt):original(receipt){original.producer_frame=this;}
        actual_producer_frame(const actual_producer_frame&)=delete;
        actual_producer_frame& operator=(const actual_producer_frame&)=delete;
        ~actual_producer_frame()noexcept {original.observing_namespace=nullptr;original.producer_frame=nullptr;}
    } frame(files); // SAME creator lexical body owns every new first receipt.
    const auto fail=[&]()noexcept {files.failed=true;files.returned=true;return terminal_files_retirement_result::retained_debt;};
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(!terminal_general_native_known(actual)||!prepare_terminal_namespace())return fail();
        const auto scope=native->active_scope.lock();const auto prior=scope->cleanup_predecessor;
        files.scope_original=scope.get();files.generation=scope->generation;
        if(!prior||prior->retired_journal_count>prior->retired_journals.size()||
           native->journal_count>state::maximum_journals)return fail();
        const auto collect=[&](const std::shared_ptr<state::resource>& original) {
            if(!original)return true;
            for(std::size_t at=0;at<files.count;++at)if(files.files[at].original==original)return true;
            if(files.count==files.files.size())return false;
            files.files[files.count++].original=original;return true;
        };
        if(!collect(native->resources[0])||files.count!=1)return fail();
        for(std::size_t at=1;at<native->resources.size();++at)if(!collect(native->resources[at]))return fail();
        for(std::size_t at=0;at<prior->retired_journal_count;++at)if(!collect(prior->retired_journals[at]))return fail();
        // A current/per-predecessor union over the actual 64-journal bound.
        // A longer union refuses, rather than admitting an all-history array.
        files.selected=true;
        if(!terminal_general_files_context_known())return fail();
        const auto& binding=native->terminal_namespace.binding;
        const auto record=[](state::disposition_call& call,int result,int error)noexcept {
            call.result=result;call.error=error;call.returned=true;
        };
        const auto inspect=[&](state::disposition_call& call,int fd) {
            if(call.attempted||fd<0||!terminal_general_files_context_known())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int result=::fstat(fd,&call.value);const int error=result?errno:0;record(call,result,error);return result==0;
        };
        const auto named=[&](state::disposition_call& call,int parent,const char* leaf) {
            if(call.attempted||parent<0||!leaf||!terminal_general_files_context_known())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int result=::fstatat(parent,leaf,&call.value,AT_SYMLINK_NOFOLLOW);
            const int error=result?errno:0;record(call,result,error);return result==0;
        };
        const auto directory=[](const struct stat& value,std::uint64_t device,std::uint64_t inode,bool private_mode)noexcept {
            return S_ISDIR(value.st_mode)&&value.st_uid==::geteuid()&&
                (private_mode?(value.st_mode&07777)==0700:(value.st_mode&0022)==0)&&
                std::uint64_t(value.st_dev)==device&&std::uint64_t(value.st_ino)==inode;
        };
        const auto namespace_check=[&](state::terminal_namespace_disposition::namespace_check& check) {
            if(check.attempted||files.observing_namespace||!terminal_general_files_context_known())return false;
            check.attempted=true;files.observing_namespace=&check;
            struct actual_observation_frame final {
                state::general_files_disposition& original;
                explicit actual_observation_frame(state::general_files_disposition& receipt):original(receipt){}
                actual_observation_frame(const actual_observation_frame&)=delete;
                actual_observation_frame& operator=(const actual_observation_frame&)=delete;
                ~actual_observation_frame()noexcept {original.observing_namespace=nullptr;}
            } observation(files);
            const bool captured=inspect(check.stage,binding.stage)&&inspect(check.root,binding.root)&&
                named(check.root_named,binding.stage,binding.root_leaf->c_str())&&inspect(check.member,binding.member)&&
                named(check.member_named,binding.root,binding.member_leaf->c_str());
            check.returned=true;
            check.joined=captured&&directory(check.stage.value,binding.stage_device,binding.stage_inode,false)&&
                directory(check.root.value,binding.root_device,binding.root_inode,true)&&
                directory(check.root_named.value,binding.root_device,binding.root_inode,true)&&
                directory(check.member.value,binding.member_device,binding.member_inode,true)&&
                directory(check.member_named.value,binding.member_device,binding.member_inode,true);
            return check.joined;
        };
        const auto close_scan=[&](state::general_member_scan& scan) {
            if(scan.close.attempted||!native->creator_current()||scan.descriptor<0)return false;
            scan.close.attempted=true;errno=0;scan.close.entered=true;
            const int result=scan.directory ? ::closedir(scan.directory) : ::close(scan.descriptor);
            const int error=result?errno:0;record(scan.close,result,error);
            if(!result){scan.directory=nullptr;scan.descriptor=-1;}return result==0;
        };
        const auto census=[&](state::general_member_scan& scan) {
            if(scan.open.attempted||!terminal_general_files_context_known())return false;
            const auto main=files.files[0].original;
            const bool expect_main=main->named&&!main->unlinked;
            bool expect_journal=false;
            if(native->named_journal) {
                const auto at=*native->named_journal;
                if(!at||at>=native->resources.size()||!native->resources[at]||
                   !native->resources[at]->journal||!native->resources[at]->named||native->resources[at]->unlinked)return false;
                expect_journal=true;
            }
            scan.open.attempted=true;errno=0;scan.open.entered=true;
            const int fd=::openat(binding.member,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
            const int error=fd<0?errno:0;scan.descriptor=fd;record(scan.open,fd,error);
            if(fd<0)return false;
            const bool identity=inspect(scan.inspect,fd)&&directory(scan.inspect.value,binding.member_device,binding.member_inode,true);
            if(!identity){(void)close_scan(scan);return false;}
            if(!terminal_general_files_context_known()){(void)close_scan(scan);return false;}
            scan.convert.attempted=true;errno=0;scan.convert.entered=true;
            scan.directory=::fdopendir(fd);const int convert_error=scan.directory?0:errno;
            record(scan.convert,scan.directory?0:-1,convert_error);
            if(!scan.directory){(void)close_scan(scan);return false;}
            bool exact=true;
            while(exact&&scan.read_count<scan.reads.size()) {
                if(!terminal_general_files_context_known()){exact=false;break;}
                auto& observed=scan.reads[scan.read_count++];observed.call.attempted=true;errno=0;observed.call.entered=true;
                const auto* entry=::readdir(scan.directory);const int read_error=errno;
                record(observed.call,entry?1:0,read_error);
                if(!entry){scan.terminal=read_error==0;exact=scan.terminal;break;}
                const auto length=::strnlen(entry->d_name,observed.name.size());
                observed.name_bytes=length;
                if(length==observed.name.size()){exact=false;break;}
                std::memcpy(observed.name.data(),entry->d_name,length+1);
                if(std::strcmp(observed.name.data(),".")==0||std::strcmp(observed.name.data(),"..")==0)continue;
                ++scan.entries;
                if(std::strcmp(observed.name.data(),main_leaf)==0&&expect_main&&!scan.main_seen)scan.main_seen=true;
                else if(std::strcmp(observed.name.data(),"store.sqlite-journal")==0&&expect_journal&&!scan.journal_seen)scan.journal_seen=true;
                else exact=false; // RAW, replacement/extra/duplicate/WAL/SHM or unknown name refuses.
            }
            const bool closed=close_scan(scan);
            scan.joined=exact&&scan.terminal&&closed&&scan.main_seen==expect_main&&scan.journal_seen==expect_journal&&
                scan.entries==std::size_t(expect_main)+std::size_t(expect_journal);
            return scan.joined;
        };
        const auto bank_call=[&](const std::shared_ptr<state::resource>& original,state::action kind,bool entered,bool returned) {
            const auto bank=scope->ledger;
            const state::native_call* found=nullptr;
            for(std::size_t at=0;at<bank->count;++at) {
                const auto& call=bank->calls[at];
                if(call.original!=original||call.kind!=kind)continue;
                if(found)return static_cast<const state::native_call*>(nullptr);
                if(!call.attempted||call.entered!=entered||call.returned!=returned||
                   call.resource_index!=state::resource_slot(original->cookie))return static_cast<const state::native_call*>(nullptr);
                found=&call;
            }
            return found;
        };
        if(!namespace_check(files.initial_namespace)||!census(files.initial_census))return fail();
        const auto file_same=[&](const state::general_file_disposition& receipt,const struct stat& value,nlink_t links) {
            const auto& original=*receipt.original;const auto& first=receipt.held.value;
            const auto high=original.journal?native->journal_high:native->main_high;
            const auto ceiling=native->io->terminal_descriptor_extent_limit(*this,receipt.original.get());
            if(ceiling<0||high<0||value.st_size<0||value.st_blocks<0||value.st_size>high||value.st_size>ceiling)return false;
            const auto cap=std::uint64_t(ceiling);const auto block_limit=cap/512+(cap%512!=0);
            for(std::size_t other=0;other<files.count;++other) {
                const auto& observed=files.files[other];
                if(observed.original==receipt.original||!observed.held.attempted||!observed.held.returned||observed.held.result)continue;
                if(observed.held.value.st_dev==value.st_dev&&observed.held.value.st_ino==value.st_ino)return false;
            }
            return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&(value.st_mode&07777)==0600&&
                value.st_nlink==links&&std::uint64_t(value.st_blocks)<=block_limit&&
                value.st_dev==first.st_dev&&value.st_ino==first.st_ino&&value.st_size==first.st_size&&value.st_blocks==first.st_blocks&&
                ((!original.birth&&!original.device&&!original.inode)||
                 (std::uint64_t(value.st_dev)==original.device&&std::uint64_t(value.st_ino)==original.inode))&&
                ((!original.birth&&!original.device&&!original.inode)||value.st_size==original.size);
        };
        const auto dispose=[&](std::size_t at) {
            auto& receipt=files.files[at];const auto original=receipt.original;
            if(receipt.attempted||!terminal_general_files_context_known())return false;
            receipt.attempted=true;
            if(original->journal&&!original->birth&&!original->named&&!original->unlinked&&original->fd<0&&
               !original->borrow_attempted&&!original->close_attempted&&!original->unlink_attempted&&original->birth_attempted) {
                if(!original->birth_entered&&!original->birth_returned) {
                    const auto call=bank_call(original,state::action::open_journal,false,false);
                    if(!call||call->fd!=native->member_fd)return false;
                    receipt.prior_open=call;receipt.not_issued=true;
                }
                else if(original->birth_entered&&original->birth_returned) {
                    const auto call=bank_call(original,state::action::open_journal,true,true);
                    if(!call||call->fd!=native->member_fd||call->result>=0||call->result!=original->fd)return false;
                    receipt.prior_open=call;receipt.not_issued=true;
                }else return false;
                receipt.ended=true;return true; // DATA only; initial/final census supplies actual name absence.
            }
            if(original->fd<0) {
                if(!original->journal||!original->birth||!original->unlinked||original->named||
                   !original->unlink_attempted||!original->close_attempted||!original->close_returned||original->close_rc!=0||
                   !original->device||!original->inode||original->size<0||original->size>native->journal_high||
                   (original->unlink_sync_required&&(!original->unlink_sync_returned||original->unlink_sync_rc!=0)))return false;
                bool retired=false;
                for(std::size_t old=0;old<prior->retired_journal_count;++old)retired=retired||prior->retired_journals[old]==original;
                if(retired&&(!prior->left||!prior->leave_returned||!prior->leave_joined))return false;
                receipt.prior_closed=true;receipt.ended=true;return true; // Actual old first-close DATA, never another close.
            }
            const char* leaf=original->journal?"store.sqlite-journal":main_leaf;
            if(!namespace_check(receipt.namespace_checks[0])||!census(receipt.before)||!inspect(receipt.held,original->fd))return false;
            if(original->unlinked) {
                if(!original->journal||original->named||!original->birth||!original->unlink_attempted||native->named_journal||
                   !file_same(receipt,receipt.held.value,0))return false;
                const auto call=bank_call(original,state::action::unlink,true,true);
                if(!call||call->result!=0||call->fd!=native->member_fd)return false;
                receipt.prior_unlink=call;
            }else {
                if(!original->named||original->unlink_attempted)return false;
                if(original->journal) {
                    const auto slot=state::resource_slot(original->cookie);
                    if(!native->named_journal||*native->named_journal!=slot||native->resources[slot]!=original||
                       !original->birth_attempted||!original->birth_entered||!original->birth_returned)return false;
                    if(!original->birth) {
                        const auto call=bank_call(original,state::action::open_journal,true,true);
                        if(!call||call->result!=original->fd||call->fd!=native->member_fd)return false;
                        receipt.prior_open=call;
                    }
                }
                if(!file_same(receipt,receipt.held.value,1)||!named(receipt.named,binding.member,leaf)||
                   !file_same(receipt,receipt.named.value,1))return false;
                receipt.birth_known=true; // Terminal owned postimage, never original birth/work success.
                if(!namespace_check(receipt.namespace_checks[1])||!terminal_general_files_context_known())return false;
                receipt.unlink.attempted=true;original->unlink_attempted=true;errno=0;receipt.unlink.entered=true;
                const int removed=::unlinkat(binding.member,leaf,0);const int error=removed?errno:0;record(receipt.unlink,removed,error);
                if(removed)return false;
                original->unlinked=true;original->named=false;
                if(original->journal)native->named_journal.reset();
            }
            if(!inspect(receipt.unlinked,original->fd)||!file_same(receipt,receipt.unlinked.value,0))return false;
            const bool present=named(receipt.absent,binding.member,leaf);
            if(present||!receipt.absent.attempted||!receipt.absent.entered||!receipt.absent.returned||
               receipt.absent.result!=-1||receipt.absent.error!=ENOENT||!census(receipt.after)||
               !namespace_check(receipt.namespace_checks[2])||!terminal_general_files_context_known())return false;
            receipt.directory_sync.attempted=true;errno=0;receipt.directory_sync.entered=true;
            const int synced=::fsync(binding.member);const int sync_error=synced?errno:0;record(receipt.directory_sync,synced,sync_error);
            if(synced||!namespace_check(receipt.namespace_checks[3])||!terminal_general_files_context_known())return false;
            if(original->close_attempted)return false; // Unknown/failed first close is never retried.
            receipt.close.attempted=true;original->close_attempted=true;errno=0;receipt.close.entered=true;
            const int closed=::close(original->fd);const int close_error=closed?errno:0;record(receipt.close,closed,close_error);
            original->close_rc=closed;original->close_errno=close_error;original->close_returned=true;
            if(closed)return false;
            original->fd=-1;if(!original->journal)native->main_fd=-1;
            if(!namespace_check(receipt.namespace_checks[4])||!terminal_general_files_context_known())return false;
            receipt.ended=true;return true;
        };
        for(std::size_t at=1;at<files.count;++at)if(!dispose(at))return fail();
        if(native->named_journal||!census(files.journals_census)||!terminal_general_files_context_known())return fail();
        // A separate terminal sync also proves durable absence for prior
        // already-closed journals whose original xDelete did not request sync.
        files.prior_directory_sync.attempted=true;errno=0;files.prior_directory_sync.entered=true;
        const int synced=::fsync(binding.member);const int sync_error=synced?errno:0;record(files.prior_directory_sync,synced,sync_error);
        if(synced||!dispose(0)||!census(files.empty_census)||!namespace_check(files.final_namespace)||
           !terminal_general_files_context_known())return fail();
        for(std::size_t at=0;at<files.count;++at)if(!files.files[at].ended)return fail();
        files.disposed=true;files.returned=true;return terminal_files_retirement_result::disposed;
        // Files only. No native_files_joined, VFS unregister, scope leave,
        // payload/lease/worker/owner success, ACK/adoption, refund or retry.
#else
        (void)actual;return fail();
#endif
    }catch(...){if(!files.first_failure)files.first_failure=std::current_exception();return fail();}
}
