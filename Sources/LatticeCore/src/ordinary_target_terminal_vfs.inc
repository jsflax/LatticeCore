// Included after the untouched GENERAL file producer. Separate terminal
// VFS first ending and read-only history while THIS cleanup remains active.
// No ordinary gate, constructor grant, deadline or remaining join is changed.
bool ordinary_target_staging_connection::terminal_general_native_after_vfs_known(
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
            native->io->terminal_environment_after_vfs_known(*this,scope->environment)&&
            terminal_raw_ending_historical_known(actual);
    }catch(...){return false;}
}

bool ordinary_target_staging_connection::terminal_general_files_context_after_vfs_known()const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!native->shell||!terminal_general_native_after_vfs_known(*native->shell))return false;
        const auto scope=native->active_scope.lock();const auto& files=native->general_files;
        const auto& issued=native->terminal_namespace;
        if(!files.returned||!files.disposed||files.producer_frame||files.observing_namespace)return false;
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

int ordinary_target_staging_connection::terminal_general_file_kind_after_vfs(
    const ordinary_target_staging_io& io,const void* resource_original)const noexcept {
    try {
        const auto native=state_;
        if(!native||native->io.get()!=&io||!resource_original||!terminal_general_files_context_after_vfs_known())return -1;
        for(std::size_t at=0;at<native->general_files.count;++at) {
            const auto& file=native->general_files.files[at].original;
            if(file.get()==resource_original)return file->journal?1:0;
        }
        return -1; // Identity comparison DATA from the actual private IO observer, never an input grant.
    }catch(...){return -1;}
}

bool ordinary_target_staging_connection::terminal_general_files_after_vfs_known(const ::lattice::database& actual)const noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto native=state_;
        if(!native||native->shell.get()!=&actual||!terminal_general_files_context_after_vfs_known())return false;
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
            const auto cap=native->io->terminal_descriptor_extent_limit_after_vfs(*this,file.get());
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

bool ordinary_target_staging_connection::terminal_vfs_scope_matches(const ordinary_target_staging_io& io,
    const void* scope_original,const void* environment_original,std::uint64_t generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||native->io.get()!=&io||
           !native->shell||!terminal_cleanup_pending(*native->shell)||native->abandoned_main.selected)return false;
        const auto scope=native->active_scope.lock();const auto& first=native->terminal_vfs;
        return scope&&scope.get()==native->scope_identity&&scope==native->last_scope&&scope->cleanup&&!scope->left&&
            scope->creator_binding(*native->shell)&&scope->generation==native->scope_generation&&
            first.attempted&&first.scope_original==scope.get()&&first.generation==scope->generation&&
            first.environment_original==scope->environment&&scope_original==scope.get()&&
            environment_original==scope->environment.get()&&generation==scope->generation;
        // Actual private identity DATA; independent Native/call/provider/files
        // receipts are still required by each consuming producer/observer.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_vfs_files_before_known(const ordinary_target_staging_io& io,
    const void* scope_original,const void* environment_original,std::uint64_t generation)const noexcept {
    try {
        const auto native=state_;
        return native&&native->creator_current()&&native->shell&&
            terminal_vfs_scope_matches(io,scope_original,environment_original,generation)&&
            terminal_general_files_ended(*native->shell);
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_vfs_files_after_known(const ordinary_target_staging_io& io,
    const void* scope_original,const void* environment_original,std::uint64_t generation)const noexcept {
    try {
        const auto native=state_;
        return native&&native->creator_current()&&native->shell&&
            terminal_vfs_scope_matches(io,scope_original,environment_original,generation)&&
            terminal_general_files_after_vfs_known(*native->shell);
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::terminal_vfs_ended(const ::lattice::database& actual)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->shell.get()!=&actual||!native->io)return false;
        const auto& first=native->terminal_vfs;
        return first.attempted&&first.returned&&first.result==terminal_vfs_retirement_result::disposed&&
            terminal_general_files_after_vfs_known(actual)&&
            native->io->terminal_registration_ended(*this,first.environment_original,first.scope_original,first.generation);
    }catch(...){return false;}
}
ordinary_target_staging_connection::terminal_vfs_retirement_result
ordinary_target_staging_connection::retire_terminal_vfs(const ::lattice::database& actual)noexcept {
    const auto native=state_;
    if(!native||!native->creator_current()||native->shell.get()!=&actual)return terminal_vfs_retirement_result::retained_debt;
    auto& first=native->terminal_vfs;
    if(first.attempted)return terminal_vfs_ended(actual)?terminal_vfs_retirement_result::disposed:
        terminal_vfs_retirement_result::retained_debt; // Observe first effects only, never retry.
    first.attempted=true;
    try {
        if(!native->io||!terminal_general_files_ended(actual)){first.returned=true;return first.result;}
        const auto scope=native->active_scope.lock();
        first.scope_original=scope.get();first.generation=scope->generation;first.environment_original=scope->environment;
        const bool disposed=native->io->retire_terminal_registration(*this,first.environment_original,first.scope_original,first.generation);
        if(disposed&&terminal_general_files_after_vfs_known(actual))first.result=terminal_vfs_retirement_result::disposed;
        first.returned=true;return first.result; // Actual VFS-only result, not cleanup/native/lease/owner join.
    }catch(...){if(!first.first_failure)first.first_failure=std::current_exception();first.returned=true;return first.result;}
}
