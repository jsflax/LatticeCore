// Included after actual Conn, descriptor/call evidence, spool and reader
// states. Private exact early RAW disposition; no ordinary cleanup gate is
// bypassed and no MAIN/journal/VFS/scope/owner success is supplied.
bool ordinary_target_staging_connection::terminal_namespace_binding_admissible(
    const ordinary_unpublished_group_target* target)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||
           native->target.get()!=target||!target||!native->shell||!native->io||
           !terminal_cleanup_pending(*native->shell)||native->abandoned_main.selected)return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||native->last_scope!=scope||!scope->cleanup||scope->left||
           !scope->creator_binding(*native->shell)||scope->generation!=native->scope_generation||
           !scope->environment||!native->io->terminal_environment_ended(*this,scope->environment))return false;
        const auto main=native->resources[0];
        if(!main||!native->bound||!native->birth_verified||!main->birth||!main->named||main->unlinked||
           main->close_attempted||main->fd<0||main->fd!=native->main_fd)return false;
        for(const auto& file:native->resources)if(file&&(file->borrowed||file->borrower||file->write_pending||
            file->lock_level!=SQLITE_LOCK_NONE||file->pending_byte||file->pending_write||file->reserved_byte||
            file->shared_range||file->exclusive_range||
            (file->close_attempted&&(!file->close_returned||file->close_rc!=0||file->fd>=0))||
            (file->birth&&file->fd<0&&(!file->close_attempted||!file->close_returned||file->close_rc!=0))))return false;
        return true; // Namespace binding prerequisite, not a deletion/credit grant.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::prepare_terminal_namespace()noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!terminal_namespace_binding_admissible(native->target.get()))return false;
        auto& receipt=native->terminal_namespace;
        const auto scope=native->active_scope.lock();
        if(receipt.attempted)return receipt.claim_returned&&receipt.claimed&&!receipt.failed&&
            receipt.scope_original==scope.get()&&receipt.generation==scope->generation;
        receipt.scope_original=scope.get();receipt.generation=scope->generation;
        receipt.attempted=true;
        receipt.claimed=native->target->claim_terminal_namespace(this,receipt.binding);
        receipt.claim_returned=true;
        if(!receipt.claimed){receipt.failed=true;return false;}
        const auto& binding=receipt.binding;
        if(binding.stage<0||binding.root<0||binding.member!=native->member_fd||!binding.root_leaf||!binding.member_leaf||
           binding.member_device!=native->member_device||binding.member_inode!=native->member_inode) {
            receipt.failed=true;return false;
        }
        return true; // Physical namespace checks are still separate actual calls.
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::history_spool_abandonment_admissible(
    const ordinary_target_history_spool::abandoned_ending_scope& ending)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||ending.connection_!=this||!ending.spool_||
           native->history_spool.get()!=ending.spool_||!native->history_spool_attempted||
           !terminal_namespace_binding_admissible(native->target.get()))return false;
        const auto scope=native->active_scope.lock();
        const auto& receipt=native->terminal_namespace;
        const auto data=ending.spool_->state_;
        if(receipt.check_count>receipt.checks.size())return false;
        for(std::size_t at=0;at<receipt.check_count;++at) {
            const auto& check=receipt.checks[at];
            if(!check.attempted||!check.returned||!check.joined)return false;
        }
        return scope&&scope.get()==ending.cleanup_scope_&&scope->generation==ending.generation_&&
            receipt.attempted&&receipt.claim_returned&&receipt.claimed&&!receipt.failed&&
            receipt.scope_original==scope.get()&&receipt.generation==scope->generation&&
            ending.namespace_original_==&receipt&&data&&data->creator_current()&&
            data->connection.lock().get()==this&&data->connection_identity==this&&data->target==native->target&&
            data->member==receipt.binding.member&&data->member_device==receipt.binding.member_device&&
            data->member_inode==receipt.binding.member_inode&&ending.spool_->abandonment_inputs_known();
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::recheck_abandoned_raw_namespace(
    const ordinary_target_history_spool::abandoned_ending_scope& ending)const noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(!history_spool_abandonment_admissible(ending))return false;
        const auto native=state_;auto& receipt=native->terminal_namespace;
        if(receipt.check_count==receipt.checks.size()){receipt.failed=true;return false;}
        auto& check=receipt.checks[receipt.check_count++];check.attempted=true;
        const auto record=[](state::disposition_call& call,int result,int error)noexcept {
            call.result=result;call.error=error;call.returned=true;
        };
        const auto inspect=[&](state::disposition_call& call,int fd) {
            if(call.attempted||!native->creator_current())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int result=::fstat(fd,&call.value);const int error=result?errno:0;record(call,result,error);return result==0;
        };
        const auto named=[&](state::disposition_call& call,int parent,const char* leaf) {
            if(call.attempted||!native->creator_current())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int result=::fstatat(parent,leaf,&call.value,AT_SYMLINK_NOFOLLOW);
            const int error=result?errno:0;record(call,result,error);return result==0;
        };
        const auto directory=[](const struct stat& value,std::uint64_t device,std::uint64_t inode,bool private_mode) {
            return S_ISDIR(value.st_mode)&&value.st_uid==::geteuid()&&
                (private_mode?(value.st_mode&07777)==0700:(value.st_mode&0022)==0)&&
                std::uint64_t(value.st_dev)==device&&std::uint64_t(value.st_ino)==inode;
        };
        const auto& binding=receipt.binding;
        const bool captured=inspect(check.stage,binding.stage)&&inspect(check.root,binding.root)&&
            named(check.root_named,binding.stage,binding.root_leaf->c_str())&&inspect(check.member,binding.member)&&
            named(check.member_named,binding.root,binding.member_leaf->c_str());
        check.returned=true;
        check.joined=captured&&directory(check.stage.value,binding.stage_device,binding.stage_inode,false)&&
            directory(check.root.value,binding.root_device,binding.root_inode,true)&&
            directory(check.root_named.value,binding.root_device,binding.root_inode,true)&&
            directory(check.member.value,binding.member_device,binding.member_inode,true)&&
            directory(check.member_named.value,binding.member_device,binding.member_inode,true);
        if(!check.joined)receipt.failed=true;
        return check.joined;
#else
        (void)ending;return false;
#endif
    }catch(...) {
        const auto native=state_;
        if(native&&native->creator_current())native->terminal_namespace.failed=true;
        return false;
    }
}
ordinary_target_staging_connection::terminal_raw_retirement_result
ordinary_target_staging_connection::retire_terminal_raw(const ::lattice::database& actual)noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->shell.get()!=&actual||
           !terminal_namespace_binding_admissible(native->target.get()))return terminal_raw_retirement_result::retained_debt;
        if(native->raw_ending_attempted)return native->raw_ending_result; // No new claim, token or syscall.
        native->raw_ending_attempted=true;
        if(!native->history_spool_attempted&&!native->history_spool) {
            native->raw_ending_result=terminal_raw_retirement_result::not_issued;
            native->raw_ending_returned=true;return native->raw_ending_result;
        }
        const auto& allocation=native->history_spool_allocation;
        if(allocation.attempted&&allocation.returned&&allocation.exception_returned&&allocation.exception_original&&
           !native->history_spool) {
            // Actual allocation-only producer ended before ANY raw Native
            // birth. This DATA proves no issuance, never name absence or cleanup.
            native->raw_ending_result=terminal_raw_retirement_result::not_issued;
            native->raw_ending_returned=true;return native->raw_ending_result;
        }
        if(!allocation.attempted||!allocation.returned||allocation.exception_returned||!native->history_spool||
           !prepare_terminal_namespace()) {
            native->raw_ending_returned=true;return native->raw_ending_result;
        }
        const auto scope=native->active_scope.lock();
        const ordinary_target_history_spool::abandoned_ending_scope ending(*this,*native->history_spool,
            scope.get(),scope->generation,&native->terminal_namespace);
        const bool joined=native->history_spool->retire_abandoned(ending);
        native->raw_ending_result=joined?terminal_raw_retirement_result::joined:terminal_raw_retirement_result::retained_debt;
        native->raw_ending_returned=true;return native->raw_ending_result;
    }catch(...){return terminal_raw_retirement_result::retained_debt;}
}
bool ordinary_target_history_spool::abandonment_inputs_known()const noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        const auto data=state_;
        if(!data||!data->creator_current())return false;
        const auto known=[](const state::call& call)noexcept {
            return (!call.entered||call.attempted)&&(!call.entered||call.returned)&&(!call.returned||call.entered);
        };
        const auto snapshot_known=[&](const state::snapshot& snapshot)noexcept {
            if(!snapshot.attempted)return !snapshot.returned&&!snapshot.directory.attempted&&
                !snapshot.held.attempted&&!snapshot.named.attempted&&known(snapshot.directory)&&known(snapshot.held)&&known(snapshot.named);
            return snapshot.returned&&snapshot.directory.attempted&&snapshot.directory.entered&&snapshot.directory.returned&&
                snapshot.held.attempted&&snapshot.held.entered&&snapshot.held.returned&&
                snapshot.named.attempted&&snapshot.named.entered&&snapshot.named.returned&&
                known(snapshot.directory)&&known(snapshot.held)&&known(snapshot.named);
        };
        if(!known(data->open)||!known(data->allocate)||!known(data->resize)||!known(data->file_sync)||
           !known(data->directory_sync)||!known(data->unlink)||!known(data->unlink_directory_sync)||!known(data->close)||
           !snapshot_known(data->abandoned_birth)||!snapshot_known(data->ending_before)||!snapshot_known(data->ending_unlinked)||
           !snapshot_known(data->before)||!snapshot_known(data->allocated)||
           !snapshot_known(data->sized)||!snapshot_known(data->durable)||!known(data->header_write)||!known(data->header_sync)||
           !snapshot_known(data->header_written)||!snapshot_known(data->header_durable)||
           data->header_read_count>data->header_reads.size()||data->header_borrowed||
           (data->header_attempted&&!data->header_operation_returned))return false;
        for(std::size_t at=0;at<data->header_read_count;++at) {
            const auto& read=data->header_reads[at];
            if(!known(read)||!snapshot_known(read.after)||(read.entered&&(!read.requested||
                read.offset>data->extent_charge||read.requested>data->extent_charge-read.offset)))return false;
        }
        if((data->page_append_attempted&&!data->page_append_operation_returned)||
           (data->read_attempted&&!data->read_operation_returned))return false;
        for(const auto& append:{data->page_append,data->page_observation})if(append) {
            const auto phase=append->state_;
            if(!phase||phase->read_count>phase->reads.size()||!append->cleanup_known()||phase->facade!=this||phase->spool.lock()!=data||
               phase->spool_identity!=data.get()||phase->connection_identity!=data->connection_identity||
               phase->receiver_identity!=data->receiver_identity||phase->fd!=data->open.result||
               phase->device!=data->device||phase->inode!=data->inode||
               phase->offset>data->extent_charge||phase->length>data->extent_charge-phase->offset||
               !known(phase->write)||!known(phase->sync)||!snapshot_known(phase->written)||!snapshot_known(phase->durable)||
               (phase->input_pin&&phase->input_pin.get()!=phase->page_identity)||
               bool(phase->input_pin)!=bool(phase->decoder_charge))return false;
            for(std::size_t at=0;at<phase->read_count;++at) {
                const auto& read=phase->reads[at];
                if(!known(read)||!snapshot_known(read.after)||(read.entered&&(!read.requested||
                    read.relative>phase->length||read.requested>phase->length-read.relative)))return false;
            }
        }
        for(const auto& reader:{data->page_reader,data->read_observation})if(reader) {
            const auto phase=reader->state_;
            if(!phase||phase->read_count>phase->reads.size()||!reader->cleanup_known()||phase->facade!=this||phase->spool.lock()!=data||
               phase->spool_identity!=data.get()||phase->connection_identity!=data->connection_identity||
               phase->receiver_identity!=data->receiver_identity||phase->fd!=data->open.result||
               phase->device!=data->device||phase->inode!=data->inode||
               !snapshot_known(phase->before))return false;
            for(std::size_t at=0;at<phase->read_count;++at) {
                const auto& read=phase->reads[at];
                if(!known(read)||!snapshot_known(read.after)||(read.entered&&(!read.requested||
                    read.relative>phase->span.length||read.requested>phase->span.length-read.relative)))return false;
            }
        }
        return true; // Known calls/borrows only; native/decode and namespace proofs remain independent.
#else
        return false;
#endif
    }catch(...){return false;}
}
bool ordinary_target_history_spool::retire_abandoned(const abandoned_ending_scope& ending)noexcept {
    const auto data=state_;
    const auto* original=ending.connection_;
    if(!data||!data->creator_current()||!original||ending.spool_!=this||
       !original->history_spool_abandonment_admissible(ending))return false;
    if(data->ending_attempted)return data->abandoned_selected&&data->abandoned_scope_original==ending.cleanup_scope_&&
        data->abandoned_generation==ending.generation_&&data->ending_joined; // First facts, no retry.
    data->ending_attempted=true;data->abandoned_selected=true;
    data->abandoned_scope_original=ending.cleanup_scope_;data->abandoned_generation=ending.generation_;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(data->fd<0||!data->open.attempted||!data->open.entered||!data->open.returned||
           data->open.result!=data->fd||data->unlinked||data->unlink.attempted||data->close.attempted||
           !original->recheck_abandoned_raw_namespace(ending))return false;
        // A separate terminal birth/postimage, derived from this actual
        // exclusive-open held descriptor. All earlier entered calls must have
        // returned; a new stat never resolves an earlier unknown call.
        data->capture(data->abandoned_birth);
        const auto& birth=data->abandoned_birth;
        const auto& identity=birth.held.value;
        const auto file_same=[&](const struct stat& value,nlink_t links)noexcept {
            return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&(value.st_mode&07777)==0600&&
                value.st_nlink==links&&value.st_size>=0&&value.st_blocks>=0&&
                std::uint64_t(value.st_dev)==std::uint64_t(identity.st_dev)&&
                std::uint64_t(value.st_ino)==std::uint64_t(identity.st_ino)&&
                value.st_size==identity.st_size&&value.st_blocks==identity.st_blocks&&
                std::uint64_t(value.st_size)<=data->extent_charge&&std::uint64_t(value.st_blocks)<=data->extent_charge/512;
        };
        const auto snapshot_same=[&](const state::snapshot& snapshot,bool named) {
            return snapshot.returned&&snapshot.directory.returned&&!snapshot.directory.result&&
                data->directory_same(snapshot.directory.value)&&snapshot.held.returned&&!snapshot.held.result&&
                file_same(snapshot.held.value,named?1:0)&&snapshot.named.returned&&
                (named?!snapshot.named.result&&file_same(snapshot.named.value,1):
                       snapshot.named.result==-1&&snapshot.named.error==ENOENT);
        };
        if(!snapshot_same(birth,true))return false;
        if(data->before.held.returned&&!data->before.held.result&&
           (identity.st_dev!=data->before.held.value.st_dev||identity.st_ino!=data->before.held.value.st_ino))return false;
        data->abandoned_birth_known=true; // Does not change born/ready/error/old identity.
        data->capture(data->ending_before);
        if(!snapshot_same(data->ending_before,true)||!original->recheck_abandoned_raw_namespace(ending))return false;
        data->unlink.attempted=true;errno=0;data->unlink.entered=true;
        const int removed=::unlinkat(data->member,state::leaf,0);const int unlink_error=removed?errno:0;
        state::returned(data->unlink,removed,unlink_error);
        if(removed)return false;data->unlinked=true;
        data->capture(data->ending_unlinked);
        if(!snapshot_same(data->ending_unlinked,false)||!original->recheck_abandoned_raw_namespace(ending))return false;
        data->unlink_directory_sync.attempted=true;errno=0;data->unlink_directory_sync.entered=true;
        const int synced=::fsync(data->member);const int sync_error=synced?errno:0;
        state::returned(data->unlink_directory_sync,synced,sync_error);
        if(synced||!original->recheck_abandoned_raw_namespace(ending))return false;
        data->close.attempted=true;errno=0;data->close.entered=true;
        const int closed=::close(data->fd);const int close_error=closed?errno:0;
        state::returned(data->close,closed,close_error);
        if(closed)return false; // First unknown/EINTR/failed close is never repeated.
        data->fd=-1;
        if(!original->recheck_abandoned_raw_namespace(ending))return false;
        std::size_t input_slot=0;
        for(const auto& append:{data->page_append,data->page_observation}) {
            auto& receipt=data->abandoned_inputs[input_slot++];
            if(!append)continue;
            if(!original->history_spool_abandonment_admissible(ending))return false;
            receipt.original=append.get();receipt.attempted=true;
            receipt.joined=append->release_input_after_cleanup();receipt.returned=true;
            if(!receipt.joined)return false;
        }
        for(const auto& reader:{data->page_reader,data->read_observation}) {
            auto& receipt=data->abandoned_inputs[input_slot++];
            if(!reader)continue;
            if(!original->history_spool_abandonment_admissible(ending))return false;
            receipt.original=reader.get();receipt.attempted=true;
            receipt.joined=reader->release_input_after_cleanup();receipt.returned=true;
            if(!receipt.joined)return false;
        }
        if(!original->history_spool_abandonment_admissible(ending))return false;
        data->ending_joined=true;return true; // RAW only; all other target/credit endings remain debt.
#else
        return false;
#endif
    }catch(...){return false;}
}
