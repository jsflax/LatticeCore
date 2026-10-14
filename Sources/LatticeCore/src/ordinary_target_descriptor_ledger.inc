// Included only inside the private connection translation unit/namespace.
// Private fixed phase banks. Actual call/resource originals are monotonic; no
// importer/caller quota, replay, public native handle or recovery authority.
struct ordinary_target_staging_connection::database_scope::scope_state {
    std::weak_ptr<state> original;
    ::lattice::database* shell=nullptr;
    std::shared_ptr<database_retirement_state> retirement;
    std::uint64_t generation=0;
    std::shared_ptr<state::call_bank> ledger;
    std::shared_ptr<scope_state> cleanup_predecessor;
    std::shared_ptr<ordinary_target_staging_io::environmental_phase> environment;
    std::array<std::shared_ptr<state::resource>,state::maximum_journals> retired_journals{};
    std::size_t retired_journal_count=0;
    // Admitted once with the actual phase. No bank/heap allocation per pager callback.
    std::array<std::optional<state::callback_original>,64> callback_originals{};
    std::size_t active_callback_slot=64,settling_callback_slot=64;
    std::uint64_t active_callback_generation=0,settling_callback_generation=0,settling_callback_high_generation=0;
    bool callback_credit_sealed=false;
    bool cleanup=false,schema=false,catalog=false,history_header=false,history_page=false,projection=false,history_chain=false,import_suspension=false,row_import=false,ending=false,published=false,left=false,failed=false;
    std::weak_ptr<ordinary_group_target_schema_original> schema_original;
    bool schema_begin_observed=false,schema_commit_observed=false,schema_rollback_observed=false;
    int schema_begin_rc=SQLITE_MISUSE,schema_commit_rc=SQLITE_MISUSE,schema_rollback_rc=SQLITE_MISUSE;
    std::size_t schema_finalizations=0;
    bool schema_finalization_failed=false;
    bool leave_returned=false,leave_joined=false;
    bool open_observed=false,close_observed=false,native_files_joined=false,abandonment_environment_joined=false;
    sqlite3* native=nullptr;int open_rc=SQLITE_MISUSE,close_rc=SQLITE_MISUSE;
    std::array<int,20> configurations{};std::size_t configuration_count=0;
    void require(const ::lattice::database& actual)const {
        const auto owner=original.lock();
        if(!owner||!owner->creator_current()||!published||left||shell!=&actual||
           owner->shell.get()!=&actual||owner->scope_identity!=this||
           owner->active_scope.lock().get()!=this||!retirement||retirement!=actual.retirement_||
           retirement->owning_wrapper_moved()||retirement->raw_handle_escaped())
            birth_fail("target database scope exact original unavailable");
        if(!cleanup&&!ending) {
            owner->require_local(owner->end);
            owner->io->require_same_target(owner->target,owner->end);
            const auto original=owner->facade.lock();
            if(!original||owner->target->staging_member_descriptor(original.get(),owner->end)!=owner->member_fd)
                birth_fail("target database scope member original unavailable");
        }
    }
    bool creator_binding(const ::lattice::database& actual)const noexcept {
        const auto owner=original.lock();
        return owner&&owner->creator_current()&&published&&shell==&actual&&owner->shell.get()==&actual&&
            retirement&&retirement==actual.retirement_&&owner->scope_identity==this&&
            owner->active_scope.lock().get()==this;
    }
};
void ordinary_target_staging_connection::state::publish_scope(const std::shared_ptr<database_scope::scope_state>& scope) {
    if(!creator_current()||!scope||scope->original.lock().get()!=this||scope->published||
       scope->shell!=shell.get()||!scope->retirement||scope->retirement!=shell->retirement_||
       !active_scope.expired()||scope_identity||scope_generation==UINT64_MAX)
        birth_fail("target phase original publication unavailable");
    if(scope->cleanup) {
        // SAME prior ledger, including unknown/failed calls; no reset or new ending reserve.
        if(!last_scope||!last_scope->ledger)birth_fail("target cleanup lacks original native bank");
        scope->cleanup_predecessor=last_scope;scope->ledger=last_scope->ledger;scope->environment=last_scope->environment;
        scope->callback_credit_sealed=true; // Cleanup never recycles the predecessor's failed/unknown bank.
    } else {
        if(descriptor_uncertain||(last_scope&&(!last_scope->left||!last_scope->leave_returned||
            !last_scope->leave_joined||last_scope->failed)))
            birth_fail("target next phase lacks actual prior ending");
        scope->ledger=std::make_shared<call_bank>(scope.get());
    }
    scope->generation=++scope_generation;
    // Strong custody and exact scope identity BEFORE environmental preparation or native effects.
    last_scope=scope;active_scope=scope;scope_identity=scope.get();scope->published=true;
    const auto actual=facade.lock();
    if(!actual)birth_fail("target phase facade original unavailable");
    io->begin_descriptor_phase(*actual,scope->environment);
}
std::size_t ordinary_target_staging_connection::state::resource_slot(std::uint64_t cookie) {
    const auto low=cookie&127u;
    if(!cookie||!low||low>1+maximum_journals)birth_fail("target native resource cookie unavailable");
    return static_cast<std::size_t>(low-1);
}
void ordinary_target_staging_connection::state::require_role(bool mutation)const {
    if(!creator_current())birth_fail("target descriptor creator unavailable");
    const auto scope=active_scope.lock();
    if(!scope||scope.get()!=scope_identity||!shell||!scope->published||scope->left||scope->shell!=shell.get()||
       scope->retirement!=shell->retirement_||(scope->schema&&scope->native!=shell->db_)||scope->retirement->owning_wrapper_moved()||scope->retirement->raw_handle_escaped())
        birth_fail("target descriptor actual database scope unavailable");
    if(scope->cleanup) {if(mutation)birth_fail("target cleanup cannot authorize mutation");return;}
    // The distinct admitted projection owns only a read snapshot and actual
    // shared-lock/unlock endings. It cannot issue writes, journal births,
    // deletion or exclusive/reserved lock escalation even while current.
    if((scope->projection||scope->history_chain)&&mutation)birth_fail("target projection cannot authorize file mutation");
    if(scope->schema&&scope->ending) {
        // ONLY the already admitted fixed schema transaction's native ending.
        // No new SQL, journal birth, lock escalation or high-water increase.
        if(mutation&&(!scope->schema_begin_observed||scope->schema_begin_rc!=SQLITE_DONE||
           (scope->schema_commit_observed&&scope->schema_commit_rc==SQLITE_DONE)))
            birth_fail("target schema mutation ending lacks its genuine uncommitted BEGIN");
        if(descriptor_uncertain)birth_fail("target schema ending retains unknown descriptor debt");
        return;
    }
    if(scope->schema) {
        const auto original=scope->schema_original.lock();const auto actual=facade.lock();
        if(!original||!actual)birth_fail("target schema actual original unavailable");
        original->require_connection(*actual,end);
    }
    require_local(end);
    if(descriptor_uncertain||scope->failed)birth_fail("target descriptor holds uncertain original");
    io->require_same_target(target,end);
}
ordinary_target_staging_connection::state::native_call&
ordinary_target_staging_connection::state::reserve(action kind,std::size_t index,int fd,std::int64_t requested) {
    if(!creator_current())birth_fail("target native result creator unavailable");
    const auto scope=active_scope.lock();
    if(!scope||scope.get()!=scope_identity||scope->left)birth_fail("target native result scope missing");
    const auto limit=(scope->cleanup||(scope->schema&&scope->ending))?maximum_calls:normal_calls;
    const auto bank=scope->ledger;
    if(!bank||(!scope->cleanup&&bank->issuer!=scope.get())||index>=resources.size()||!resources[index]||
       bank->count>=limit){descriptor_uncertain=true;birth_fail("target fixed phase native ledger exhausted");}
    auto& record=bank->calls[bank->count++];record.kind=kind;record.resource_index=index;record.fd=fd;
    record.original=resources[index];record.requested=requested;record.attempted=true;
    if(scope->active_callback_slot<scope->callback_originals.size()) {
        const auto& callback=scope->callback_originals[scope->active_callback_slot];
        if(!callback||callback->generation!=scope->active_callback_generation) {
            descriptor_uncertain=true;birth_fail("target native call callback original changed");
        }
        record.callback_slot=scope->active_callback_slot;record.callback_generation=callback->generation;
    }
    return record;
}
void ordinary_target_staging_connection::state::returned(native_call& call,std::int64_t rc,int error)noexcept {
    if(!creator_current())return; // No inherited/foreign-thread mutable result join.
    if(!call.attempted||!call.entered||call.returned){descriptor_uncertain=true;return;}
    call.result=rc;call.error=error;call.returned=true; // First native result precedes all diagnostic joins.
    const auto scope=active_scope.lock();
    if(!scope||scope.get()!=scope_identity||call.callback_slot>=scope->callback_originals.size())return;
    auto& callback=scope->callback_originals[call.callback_slot];
    if(!callback||callback->generation!=call.callback_generation||
       scope->active_callback_slot!=call.callback_slot||scope->active_callback_generation!=call.callback_generation) {
        descriptor_uncertain=true;scope->callback_credit_sealed=true;return;
    }
    if(!callback->first_resource)callback->first_resource=call.original;
    // These are issued by this actual Native return, not an input rc/grant.
    if(rc<0&&!callback->first_negative)callback->first_negative.emplace(call);
    if(call.kind==action::read&&rc==0&&!callback->first_short_read)callback->first_short_read.emplace(call);
}
// Actual VFS/file dispatch owns these stack scopes. Inline originals remain
// attached to the same phase; native/callback generations cannot be replayed.
void ordinary_target_staging_connection::begin_descriptor_callback(descriptor_callback_kind kind,
    std::uint64_t cookie,sqlite3_file* borrower,descriptor_callback_scope& retained) {
    const auto native=state_;
    if(!native||!native->creator_current()||retained.original_||retained.phase_)
        birth_fail("target callback actual original unavailable");
    native->require_role(false);
    const auto phase=native->active_scope.lock();
    if(!phase||phase.get()!=native->scope_identity||!phase->published||phase->left||
       !phase->ledger||native->callback_generation==UINT64_MAX)
        birth_fail("target callback phase/generation unavailable");
    std::size_t slot=0;
    while(slot<phase->callback_originals.size()&&phase->callback_originals[slot])++slot;
    if(slot==phase->callback_originals.size()) {
        phase->callback_credit_sealed=true;phase->failed=true;
        birth_fail("target fixed callback-original storage exhausted");
    }
    std::shared_ptr<state::resource> file;
    if(cookie) {
        const auto index=state::resource_slot(cookie);file=native->resources[index];
        if(!file||file->cookie!=cookie||!file->birth||!file->borrowed||file->borrower!=borrower||
           file->close_attempted)birth_fail("target callback input resource differs");
    }
    const auto issued=++native->callback_generation;
    auto& original=phase->callback_originals[slot];
    original.emplace(kind,issued,cookie,borrower,phase->active_callback_slot,phase->active_callback_generation);
    original->first_resource=std::move(file);
    retained.original_=native;retained.phase_=phase;retained.slot_=slot;retained.generation_=issued;
    phase->active_callback_slot=slot;phase->active_callback_generation=issued;
}
ordinary_target_staging_connection::descriptor_callback_scope::~descriptor_callback_scope() { (void)leave(); }
void ordinary_target_staging_connection::descriptor_callback_scope::record_body_return(int rc)noexcept {
    const auto native=original_;const auto phase=phase_;
    if(!native||!native->creator_current()||!phase||slot_>=phase->callback_originals.size())return;
    auto& original=phase->callback_originals[slot_];
    if(!original||original->generation!=generation_||original->body_returned||
       phase->active_callback_slot!=slot_||phase->active_callback_generation!=generation_) {
        native->descriptor_uncertain=true;phase->callback_credit_sealed=true;return;
    }
    original->body_result=rc;original->body_returned=true; // Real provider body has returned.
}
void ordinary_target_staging_connection::descriptor_callback_scope::record_body_exception_return(int rc)noexcept {
    const auto native=original_;const auto phase=phase_;
    if(!native||!native->creator_current()||!phase||slot_>=phase->callback_originals.size())return;
    auto& original=phase->callback_originals[slot_];
    if(!original||original->generation!=generation_||original->body_returned||
       phase->active_callback_slot!=slot_||phase->active_callback_generation!=generation_) {
        native->descriptor_uncertain=true;phase->callback_credit_sealed=true;return;
    }
    // Called only by the actual VFS/file dispatch catch AFTER body locals have
    // unwound. This is distinct from its ordinary normal return and keeps debt.
    original->body_result=rc;original->body_exception_returned=true;original->body_returned=true;
    native->descriptor_uncertain=true;phase->callback_credit_sealed=true;phase->failed=true;
}
std::uint64_t ordinary_target_staging_connection::active_descriptor_callback_generation()const {
    const auto native=state_;if(!native||!native->creator_current())birth_fail("target callback creator unavailable");
    const auto phase=native->active_scope.lock();
    if(!phase||phase.get()!=native->scope_identity||phase->active_callback_slot>=phase->callback_originals.size())
        birth_fail("target active callback original unavailable");
    const auto& original=phase->callback_originals[phase->active_callback_slot];
    if(!original||original->generation!=phase->active_callback_generation||original->leave_attempted)
        birth_fail("target active callback identity changed");
    return original->generation;
}
std::uint64_t ordinary_target_staging_connection::settling_descriptor_callback_generation()const {
    const auto native=state_;if(!native||!native->creator_current())birth_fail("target callback settlement creator unavailable");
    const auto phase=native->settling_callback_phase;
    if(!phase||native->active_scope.lock()!=phase||phase.get()!=native->scope_identity||!phase->published||
       phase->left||phase->callback_credit_sealed||!phase->settling_callback_generation||
       phase->active_callback_slot!=phase->callback_originals.size()||phase->active_callback_generation||
       phase->settling_callback_slot>=phase->callback_originals.size())
        birth_fail("target callback settlement lacks actual outer ending");
    const auto& original=phase->callback_originals[phase->settling_callback_slot];
    if(!original||original->generation!=phase->settling_callback_generation||!original->body_returned||
       !original->leave_attempted||!original->leave_returned||!original->leave_joined||
       original->previous_slot!=phase->callback_originals.size())
        birth_fail("target callback settlement outer original changed");
    return original->generation;
}
std::uint64_t ordinary_target_staging_connection::settling_descriptor_callback_high_generation()const {
    const auto first=settling_descriptor_callback_generation();
    const auto native=state_;const auto phase=native->settling_callback_phase;
    if(phase->settling_callback_high_generation<first||
       phase->settling_callback_high_generation!=native->callback_generation)
        birth_fail("target callback settlement issued generation changed");
    return phase->settling_callback_high_generation; // Actual completed nested chain, not an input bound.
}
bool ordinary_target_staging_connection::descriptor_callback_scope::leave()noexcept {
    const auto native=original_;const auto phase=phase_;
    if(!native||!native->creator_current()||!phase||slot_>=phase->callback_originals.size())return false;
    auto& held=phase->callback_originals[slot_];
    if(!held||held->generation!=generation_)return false;
    if(held->leave_attempted)return held->leave_returned&&held->leave_joined;
    held->leave_attempted=true;
    try {
        if(native->active_scope.lock()!=phase||phase.get()!=native->scope_identity||phase->left||
           phase->active_callback_slot!=slot_||phase->active_callback_generation!=generation_)
            birth_fail("target callback ending identity changed");
        const bool outer=held->previous_slot==phase->callback_originals.size();
        // Restore the real nested stack, even when a body failed. No failure
        // is reissued or converted to a reusable bank by an outer success.
        phase->active_callback_slot=held->previous_slot;
        phase->active_callback_generation=held->previous_generation;
        bool known=held->body_returned;
        const auto kind=held->kind;const int rc=held->body_result;
        if(known) {
            if(kind==descriptor_callback_kind::sector)known=rc>0&&rc<=65536;
            else if(kind==descriptor_callback_kind::device)known=rc==0;
            else if(kind==descriptor_callback_kind::randomness||kind==descriptor_callback_kind::sleep)known=rc>=0;
            else if(kind==descriptor_callback_kind::control)known=rc==SQLITE_OK||rc==SQLITE_NOTFOUND;
            else if(kind==descriptor_callback_kind::read&&rc==SQLITE_IOERR_SHORT_READ) {
                const auto& first=held->first_short_read;
                known=first&&first->entered&&first->returned&&first->result==0&&
                    first->kind==state::action::read&&first->callback_generation==generation_&&
                    first->original==held->first_resource&&held->cookie&&
                    first->original&&first->original->cookie==held->cookie;
            } else if(kind==descriptor_callback_kind::lock&&rc==SQLITE_BUSY) {
                const auto& first=held->first_negative;
#ifdef LATTICE_TARGET_MAIN_POSIX
                known=first&&first->entered&&first->returned&&first->result<0&&
                    first->kind==state::action::lock&&(first->error==EACCES||first->error==EAGAIN)&&
                    first->callback_generation==generation_&&first->original==held->first_resource&&
                    held->cookie&&first->original&&first->original->cookie==held->cookie;
#else
                known=false;
#endif
            } else known=rc==SQLITE_OK;
        }
        if(!known||native->descriptor_uncertain) {
            phase->callback_credit_sealed=true;phase->failed=true;
            held->leave_returned=true;held->leave_joined=false;return false;
        }
        const bool retain=held->first_negative.has_value()||held->first_short_read.has_value();
        held->retained_semantic=retain;
        if(!outer) {
            held->leave_returned=true;held->leave_joined=true;
            if(!retain)held.reset(); // Body locals have ended; Native records stay until outer quiescence.
            return true;
        }
        const auto actual=native->facade.lock();
        if(!actual||!native->io->callbacks_joined(*actual)) {
            phase->callback_credit_sealed=true;held->leave_returned=true;held->leave_joined=false;return false;
        }
        const auto bank=phase->ledger;
        if(!bank)birth_fail("target callback native bank unavailable");
        for(std::size_t n=0;n<bank->count;++n) {
            const auto& call=bank->calls[n];
            if(!call.attempted||!call.entered||!call.returned) {
                phase->callback_credit_sealed=true;held->leave_returned=true;held->leave_joined=false;return false;
            }
            if(call.result<0) {
                // Only genuine BUSY lock semantics can retire a negative
                // temporary record. EINTR/other negatives retain the bank.
                const auto index=call.callback_slot;
                bool busy=false;
#ifdef LATTICE_TARGET_MAIN_POSIX
                if(index<phase->callback_originals.size())if(const auto& issuer=phase->callback_originals[index])
                    busy=issuer->generation==call.callback_generation&&issuer->body_returned&&
                        issuer->kind==descriptor_callback_kind::lock&&issuer->body_result==SQLITE_BUSY&&
                        issuer->first_negative&&call.kind==state::action::lock&&
                        (call.error==EACCES||call.error==EAGAIN)&&call.original==issuer->first_resource;
#endif
                if(!busy){phase->callback_credit_sealed=true;break;}
            }
        }
        held->leave_returned=true;held->leave_joined=true;
        if(!phase->callback_credit_sealed) {
            // Issued only by this real outer RAII leave, after count return.
            // No leaf spans record/capture destruction or provider body code.
            phase->settling_callback_slot=slot_;phase->settling_callback_generation=generation_;
            phase->settling_callback_high_generation=native->callback_generation;
            native->settling_callback_phase=phase;
            try {native->io->settle_descriptor_callback(*actual);}
            catch(...){native->settling_callback_phase.reset();phase->settling_callback_slot=64;
                phase->settling_callback_generation=0;phase->settling_callback_high_generation=0;throw;}
            native->settling_callback_phase.reset();phase->settling_callback_slot=64;
            phase->settling_callback_generation=0;phase->settling_callback_high_generation=0;
            for(std::size_t n=0;n<bank->count;++n)bank->calls[n]=state::native_call{};
            bank->count=0;
        }
        if(!retain)held.reset();
        return true; // Callback ending only; not COMMIT/VM/database-scope leave.
    }catch(...) {
        native->descriptor_uncertain=true;phase->callback_credit_sealed=true;phase->failed=true;
        if(held){held->leave_returned=true;held->leave_joined=false;}
        return false;
    }
}
ordinary_target_staging_connection::state::resource&
ordinary_target_staging_connection::state::loan(std::uint64_t id,sqlite3_file* borrower) {
    require_role(false);
    const auto index=resource_slot(id);
    const auto original=resources[index];
    if(!original||original->cookie!=id)birth_fail("target descriptor generation differs from original");
    auto& value=*original;
    if(!value.birth||value.fd<0||!value.borrowed||value.borrower!=borrower||value.close_attempted)
        birth_fail("target descriptor borrower differs from original");
    return value;
}
void ordinary_target_staging_connection::state::inspect(resource& file,std::size_t index,bool permit_growth) {
#ifdef LATTICE_TARGET_MAIN_POSIX
    auto& call=reserve(action::inspect,index,file.fd);struct stat actual{};errno=0;
    call.entered=true;const int rc=::fstat(file.fd,&actual);const int error=rc?errno:0;returned(call,rc,error);
    const auto high=file.journal?journal_high:main_high;
    if(rc||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0600||
       actual.st_nlink!=(file.unlinked?0:1)||actual.st_size<0||actual.st_size>high||
       std::uint64_t(actual.st_dev)!=file.device||std::uint64_t(actual.st_ino)!=file.inode||
       (!permit_growth&&actual.st_size!=file.size)) {
        descriptor_uncertain=true;birth_fail("target native descriptor identity/extent unavailable");
    }
    file.size=actual.st_size;
#else
    (void)file;(void)index;(void)permit_growth;birth_fail("target descriptor unsupported");
#endif
}
int ordinary_target_staging_connection::state::file_lock(resource& file,std::size_t index,
    int command,short type,std::int64_t start,std::int64_t length) {
#ifdef LATTICE_TARGET_MAIN_POSIX
    static_assert(sizeof(off_t)>=8,"target descriptor requires 64 bit offsets");
    const auto scope=active_scope.lock();
    const bool admitted_read_lock=scope&&scope.get()==scope_identity&&(scope->projection||scope->history_chain)&&!scope->ending&&
        !file.journal&&command==F_SETLK&&type==F_RDLCK&&
        ((start==0x40000000LL&&length==1)||(start==0x40000002LL&&length==510));
    require_role(command!=F_GETLK&&type!=F_UNLCK&&!admitted_read_lock);
    auto& call=reserve(type==F_UNLCK?action::unlock:action::lock,index,file.fd,start);
    struct flock lock{};lock.l_type=type;lock.l_whence=SEEK_SET;lock.l_start=static_cast<off_t>(start);lock.l_len=static_cast<off_t>(length);
    errno=0;call.entered=true;const int rc=::fcntl(file.fd,command,&lock);const int error=rc<0?errno:0;returned(call,rc,error);
    if(rc<0) {
        if(command==F_SETLK&&(error==EACCES||error==EAGAIN))return SQLITE_BUSY;
        descriptor_uncertain=true;return type==F_UNLCK?SQLITE_IOERR_UNLOCK:SQLITE_IOERR_LOCK;
    }
    if(command==F_SETLK) {
        constexpr std::int64_t pending=0x40000000LL,reserved=pending+1,shared=pending+2,count=510;
        if(type==F_UNLCK) {
            if(start==pending){file.pending_byte=false;file.pending_write=false;if(length>=2)file.reserved_byte=false;
                if(length>=count+2){file.shared_range=false;file.exclusive_range=false;}}
        } else if(start==pending) {file.pending_byte=true;file.pending_write=type==F_WRLCK;}
        else if(start==reserved)file.reserved_byte=true;
        else if(start==shared){file.shared_range=true;file.exclusive_range=type==F_WRLCK;}
        file.lock_level=file.exclusive_range?SQLITE_LOCK_EXCLUSIVE:file.pending_write?SQLITE_LOCK_PENDING:
            file.reserved_byte?SQLITE_LOCK_RESERVED:file.shared_range?SQLITE_LOCK_SHARED:SQLITE_LOCK_NONE;
    } // Actual native lock ownership is retained before any later current join.
    return SQLITE_OK;
#else
    (void)file;(void)index;(void)command;(void)type;(void)start;(void)length;return SQLITE_IOERR_LOCK;
#endif
}
ordinary_target_staging_connection::database_scope::database_scope(std::shared_ptr<scope_state> value):held_(std::move(value)){}
ordinary_target_staging_connection::database_scope::~database_scope(){if(held_&&held_->shell)(void)leave(*held_->shell);}
void ordinary_target_staging_connection::database_scope::require_current(const ::lattice::database& actual)const {
    if(!held_)birth_fail("target database scope missing");
    // Same real shell/retirement/registry; full original current for a new
    // constructor operation. Cleanup renews no mutation or target authority.
    const auto native=held_->original.lock();
    if(!native||!held_->creator_binding(actual)||held_->left)birth_fail("target database scope original missing");
    native->require_role(false);
}
void ordinary_target_staging_connection::database_scope::record_sqlite_open(const ::lattice::database& actual,sqlite3* ptr,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||held_->cleanup||held_->schema||held_->open_observed){if(held_)held_->failed=true;return;}
    held_->native=ptr;held_->open_rc=rc;held_->open_observed=true;if(rc!=SQLITE_OK)held_->failed=true;
}
void ordinary_target_staging_connection::database_scope::record_configuration_result(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||held_->cleanup||held_->schema||!held_->open_observed||held_->configuration_count==held_->configurations.size()) {
        if(held_)held_->failed=true;return;
    }
    held_->configurations[held_->configuration_count++]=rc;if(rc!=SQLITE_OK)held_->failed=true;
}
void ordinary_target_staging_connection::database_scope::record_sqlite_close(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||!held_->cleanup||held_->close_observed){if(held_)held_->failed=true;return;}
    held_->close_rc=rc;held_->close_observed=true;if(rc!=SQLITE_OK)held_->failed=true;
}
bool ordinary_target_staging_connection::database_scope::checked_native_close_known(const ::lattice::database& actual)const noexcept {
    try {
        const auto native=held_?held_->original.lock():nullptr;
        // Immutable same-scope first close only. This observes neither VM,
        // callback, file, VFS, scope leave nor namespace cleanup completion.
        // It remains observable after the same scope's actual leave; no new
        // native operation or renewal of live target authority is admitted.
        return native&&native->creator_current()&&held_->published&&held_->cleanup&&
            held_->shell==&actual&&native->shell.get()==&actual&&native->last_scope==held_&&
            held_->generation&&held_->generation==native->scope_generation&&held_->retirement&&
            held_->retirement==actual.retirement_&&!held_->retirement->owning_wrapper_moved()&&
            !held_->retirement->raw_handle_escaped()&&held_->close_observed&&held_->close_rc==SQLITE_OK&&
            held_->retirement->state()==database_retirement_state::phase::closed&&
            held_->retirement->checked_close_result()==SQLITE_OK;
    }catch(...){return false;}
}
std::uint64_t ordinary_target_staging_connection::terminal_environment_ending_generation(
    const ordinary_target_staging_io& io,const void* phase_original,std::uint64_t environment_generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->io.get()!=&io||!phase_original||
           !terminal_descriptor_calls_known(environment_generation))return 0;
        // Pointer is comparison data from the actual private IO producer,
        // never a phase factory, copied admission or cleanup permission.
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||native->last_scope!=scope||!scope->cleanup||scope->left||
           !native->shell||!scope->creator_binding(*native->shell)||scope->generation!=native->scope_generation||
           scope->environment.get()!=phase_original)return 0;
        return scope->generation; // Actual original ending generation; no new deadline/role.
    }catch(...){return 0;}
}
bool ordinary_target_staging_connection::end_terminal_environment(const ::lattice::database& actual)noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||!terminal_cleanup_pending(actual)||!native->io)return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||native->last_scope!=scope||!scope->cleanup||scope->left||
           scope->generation!=native->scope_generation||!scope->creator_binding(actual)||
           !scope->close_observed||scope->close_rc!=SQLITE_OK||!scope->environment)return false;
        // SAME retained cleanup environmental original. Neither success here
        // nor its first result enables namespace/VFS/payload/scope/owner ending.
        return native->io->end_terminal_environment(*this,scope->environment);
    }catch(...){return false;}
}

bool ordinary_target_staging_connection::database_scope::leave(const ::lattice::database& actual)noexcept {
    if(!held_)return false;
    if(held_->left){const auto original=held_->original.lock();
        return original&&original->creator_current()&&held_->shell==&actual&&held_->retirement==actual.retirement_&&
            held_->leave_returned&&held_->leave_joined;}
    try {
        if(!held_->creator_binding(actual))return false;
        const auto native=held_->original.lock();
        if(held_->cleanup&&native->terminal_abandonment.attempted)return false; // Defer SAME scope; do not rewrite a failed leave.
        const auto original=native->facade.lock();
        const bool callbacks=original&&native->io->callbacks_joined(*original);
        if(!callbacks||held_->active_callback_slot!=held_->callback_originals.size()||held_->active_callback_generation)return false;
        for(const auto& file:native->resources)if(file&&file->write_pending)return false;
        const auto bank=held_->ledger;if(!bank)return false;
        for(std::size_t i=0;i<bank->count;++i)
            if(bank->calls[i].entered&&!bank->calls[i].returned)return false;
        const bool okay=!held_->failed&&!native->descriptor_uncertain&&
            (held_->cleanup?held_->close_observed&&held_->close_rc==SQLITE_OK&&held_->native_files_joined:
             held_->schema?held_->schema_commit_observed&&held_->schema_commit_rc==SQLITE_DONE&&
                 (held_->catalog?actual.target_catalog_native_ending_():held_->history_header?actual.target_history_header_native_ending_():held_->history_page?actual.target_history_page_native_ending_():held_->projection?actual.target_history_projection_native_ending_():held_->history_chain?actual.target_history_chain_native_ending_():held_->import_suspension?actual.target_import_suspension_native_ending_():held_->row_import?actual.target_row_import_native_ending_():held_->schema_finalizations==10)&&
                 !held_->schema_finalization_failed&&!held_->ending:
             held_->open_observed&&held_->open_rc==SQLITE_OK&&held_->configuration_count>0);
        if(!held_->cleanup&&!held_->ending) {
            try{native->require_role(false);}catch(...){held_->failed=true;}
        }
        bool joined=okay&&!held_->failed;
        if(joined&&!held_->cleanup) {
            const auto& main=*native->resources[0];
            if(main.lock_level!=SQLITE_LOCK_NONE||main.pending_byte||main.pending_write||main.reserved_byte||
               main.shared_range||main.exclusive_range||main.write_pending||native->named_journal)joined=false;
            for(std::size_t i=1;joined&&i<=native->journal_count;++i)if(const auto file=native->resources[i]) {
                if(!file->birth||!file->birth_returned||file->borrowed||file->borrower||!file->borrow_attempted||
                   file->write_pending||file->lock_level!=SQLITE_LOCK_NONE||file->pending_byte||file->pending_write||
                   file->reserved_byte||file->shared_range||file->exclusive_range||file->named||!file->unlinked||
                   !file->unlink_attempted||!file->close_attempted||!file->close_returned||file->close_rc!=0||file->fd>=0||
                   (file->sync_attempted&&(!file->sync_returned||file->sync_rc!=0))||
                   (file->unlink_sync_required&&(!file->unlink_sync_returned||file->unlink_sync_rc!=0))||
                   held_->retired_journal_count==held_->retired_journals.size())joined=false;
            }
        }
        if(joined&&!held_->abandonment_environment_joined) {
            native->settling_scope=held_; // Only this real leave can issue the private environmental settlement.
            try{native->io->settle_descriptor_phase(*original,held_->environment);}
            catch(...){held_->failed=true;joined=false;}
            native->settling_scope.reset();
        }
        if(joined&&!held_->cleanup)for(std::size_t i=1;i<=native->journal_count;++i)if(native->resources[i]) {
            held_->retired_journals[held_->retired_journal_count++]=std::move(native->resources[i]);
            // Historical cookie/first native results stay in this actual scope and its call records.
        }
        held_->leave_joined=joined;held_->leave_returned=true;
        held_->left=true;native->active_scope.reset();native->scope_identity=nullptr;
        return held_->leave_joined;
    }catch(...){return false;}
}
void ordinary_target_staging_connection::begin_database_constructor(::lattice::database& actual,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);auto native=state_;
    if(retained||native->constructor_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !actual.retirement_||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
        birth_fail("target actual shell constructor already claimed or unavailable");
    native->constructor_attempted=true;
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;scope->retirement=actual.retirement_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope)); // Before fallible current/diagnostic delivery.
    native->publish_scope(scope);
    retained->require_current(actual);
#ifdef LATTICE_TARGET_MAIN_POSIX
    auto& joined=native->reserve(state::action::directory_sync,0,native->member_fd);
    retained->require_current(actual);errno=0;joined.entered=true;
    const int rc=::fsync(native->member_fd);const int error=rc?errno:0;native->returned(joined,rc,error);
    if(rc){native->descriptor_uncertain=true;birth_fail("target MAIN namespace sync refused");}
    retained->require_current(actual);
#endif
}
bool ordinary_target_staging_connection::begin_database_cleanup(::lattice::database& actual,
    std::unique_ptr<database_scope>& retained)noexcept {
    try {
        auto native=state_;
        if(!native||!native->creator_current()||retained||native->cleanup_attempted||native->shell.get()!=&actual||
           !actual.retirement_||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())return false;
        native->cleanup_attempted=true;
        if(auto old=native->active_scope.lock()) {
            if(old.get()!=native->scope_identity||old->shell!=&actual||old->cleanup||
               !native->io->callbacks_joined(*this))return false;
            native->last_scope=old; // Preserve exact unknown/failed bank before cleanup replaces the active role.
            old->left=true;native->active_scope.reset();native->scope_identity=nullptr;
        }
        auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
        scope->retirement=actual.retirement_;scope->cleanup=true;
        retained=std::unique_ptr<database_scope>(new database_scope(scope));
        native->publish_scope(scope);
        retained->require_current(actual);return true;
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::descriptor_callback_admissible()const noexcept {
    try{if(!state_)return false;state_->require_role(false);return true;}catch(...){return false;}
}

int ordinary_target_staging_connection::descriptor_open(bool journal,const char* path,sqlite3_file* borrower,
    int flags,int* actual_flags,std::uint64_t& issued)noexcept {
    issued=0;
    try {
        auto native=state_;if(!native||!path||!borrower||!actual_flags)return SQLITE_CANTOPEN;
        native->require_role(true);
        if(native->active_scope.lock()->ending)return SQLITE_CANTOPEN; // No ending resource birth.
        const auto expected=native->fixed_main_path+(journal?"-journal":"");
        const int type=journal?SQLITE_OPEN_MAIN_JOURNAL:SQLITE_OPEN_MAIN_DB;
        constexpr int forbidden=SQLITE_OPEN_READONLY|SQLITE_OPEN_URI|SQLITE_OPEN_MEMORY|SQLITE_OPEN_DELETEONCLOSE|
            SQLITE_OPEN_TEMP_DB|SQLITE_OPEN_TEMP_JOURNAL|SQLITE_OPEN_SUBJOURNAL|SQLITE_OPEN_SUPER_JOURNAL|SQLITE_OPEN_WAL;
        if(expected!=path||(flags&forbidden)||!(flags&SQLITE_OPEN_READWRITE)||!(flags&type))return SQLITE_CANTOPEN;
        std::size_t index=0;
        if(!journal) {
            auto& value=*native->resources[0];
            if(!value.birth||value.fd<0||value.borrow_attempted||value.borrowed||value.close_attempted)return SQLITE_CANTOPEN;
            native->inspect(value,0,false);native->require_role(true);
        } else {
#ifdef LATTICE_TARGET_MAIN_POSIX
            if(!(flags&SQLITE_OPEN_CREATE)||native->named_journal||
               native->journal_generation==(UINT64_MAX>>7))return SQLITE_CANTOPEN;
            for(std::size_t old=1;old<=native->journal_count;++old)if(const auto prior=native->resources[old]) {
                if(prior->fd>=0||!prior->unlinked||!prior->close_returned||prior->close_rc!=0)return SQLITE_CANTOPEN;
            }
            for(index=1;index<native->resources.size()&&native->resources[index];++index) {}
            if(index==native->resources.size())return SQLITE_CANTOPEN;
            const auto generation=++native->journal_generation; // Never reset, including allocation/birth failure.
            auto original=std::make_shared<state::resource>((generation<<7)|(index+1));
            native->resources[index]=original;native->journal_count=std::max(native->journal_count,index);
            auto& value=*original;value.journal=true;value.birth_attempted=true;
            auto& call=native->reserve(state::action::open_journal,index,native->member_fd);
            native->require_role(true); // Last admission before the creating syscall.
            errno=0;value.birth_entered=true;call.entered=true;const int fd=::openat(native->member_fd,"store.sqlite-journal",O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
            const int error=fd<0?errno:0;value.fd=fd;value.birth_returned=true;native->returned(call,fd,error);
            if(fd<0){native->descriptor_uncertain=true;return SQLITE_CANTOPEN;}
            native->named_journal=index;value.named=true;
            auto& observed=native->reserve(state::action::inspect,index,fd);struct stat birth{};errno=0;
            observed.entered=true;const int rc=::fstat(fd,&birth);const int stat_error=rc?errno:0;native->returned(observed,rc,stat_error);
            if(!rc){value.device=std::uint64_t(birth.st_dev);value.inode=std::uint64_t(birth.st_ino);value.size=birth.st_size;}
            if(rc||!S_ISREG(birth.st_mode)||birth.st_uid!=::geteuid()||(birth.st_mode&07777)!=0600||birth.st_nlink!=1||birth.st_size!=0) {
                native->descriptor_uncertain=true;return SQLITE_CANTOPEN;
            }
            value.birth=true;
            native->require_role(true);
            auto& synced=native->reserve(state::action::directory_sync,index,native->member_fd);
            native->require_role(true);errno=0;synced.entered=true;const int sync_rc=::fsync(native->member_fd);const int sync_error=sync_rc?errno:0;
            native->returned(synced,sync_rc,sync_error);
            if(sync_rc){native->descriptor_uncertain=true;return SQLITE_IOERR_DIR_FSYNC;}
            native->require_role(true);
#else
            return SQLITE_CANTOPEN;
#endif
        }
        auto& value=*native->resources[index];value.borrow_attempted=true;value.borrowed=true;value.borrower=borrower;
        issued=value.cookie;*actual_flags=flags;return SQLITE_OK;
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_CANTOPEN;}
}
int ordinary_target_staging_connection::descriptor_read(std::uint64_t id,sqlite3_file* borrower,
    void* output,int bytes,sqlite3_int64 offset)noexcept {
    if(!output||bytes<0||offset<0||offset>std::numeric_limits<std::int64_t>::max()-bytes)return SQLITE_IOERR_READ;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);const auto index=state::resource_slot(id);
        native->inspect(file,index,false);int done=0;
        while(done<bytes) {
            native->require_role(false);auto& call=native->reserve(state::action::read,index,file.fd,offset+done);
            errno=0;call.entered=true;const auto rc=::pread(file.fd,static_cast<char*>(output)+done,static_cast<std::size_t>(bytes-done),static_cast<off_t>(offset+done));
            const int error=rc<0?errno:0;native->returned(call,rc,error);
            if(rc<0){if(error==EINTR)continue;native->descriptor_uncertain=true;return SQLITE_IOERR_READ;}
            if(rc==0){std::memset(static_cast<char*>(output)+done,0,static_cast<std::size_t>(bytes-done));
                native->require_role(false);return SQLITE_IOERR_SHORT_READ;}
            done+=static_cast<int>(rc);native->require_role(false);
        }
        return SQLITE_OK;
#else
        (void)id;(void)borrower;return SQLITE_IOERR_READ;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_READ;}
}
int ordinary_target_staging_connection::descriptor_write(std::uint64_t id,sqlite3_file* borrower,
    const void* input,int bytes,sqlite3_int64 offset)noexcept {
    if(!input||bytes<0||offset<0||offset>std::numeric_limits<std::int64_t>::max()-bytes)return SQLITE_IOERR_WRITE;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);const auto index=state::resource_slot(id);
        native->require_role(true);native->inspect(file,index,false);
        const auto extent=offset+bytes;
        if(!native->io->charge_descriptor(*this,file.journal,extent))return SQLITE_FULL;
        auto& high=file.journal?native->journal_high:native->main_high;high=std::max(high,extent);
        file.write_pending=true;int done=0;
        while(done<bytes) {
            native->require_role(true);auto& call=native->reserve(state::action::write,index,file.fd,offset+done);
            errno=0;call.entered=true;const auto rc=::pwrite(file.fd,static_cast<const char*>(input)+done,static_cast<std::size_t>(bytes-done),static_cast<off_t>(offset+done));
            const int error=rc<0?errno:0;native->returned(call,rc,error);
            if(rc<0&&error==EINTR)continue;
            if(rc<=0){native->descriptor_uncertain=true;return SQLITE_IOERR_WRITE;}
            done+=static_cast<int>(rc);
        }
        native->inspect(file,index,true);file.write_pending=false;
        native->require_role(true);return SQLITE_OK;
#else
        (void)id;(void)borrower;return SQLITE_IOERR_WRITE;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_WRITE;}
}
int ordinary_target_staging_connection::descriptor_truncate(std::uint64_t id,sqlite3_file* borrower,sqlite3_int64 bytes)noexcept {
    if(bytes<0)return SQLITE_IOERR_TRUNCATE;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);const auto index=state::resource_slot(id);
        native->require_role(true);native->inspect(file,index,false);
        if(!native->io->charge_descriptor(*this,file.journal,bytes))return SQLITE_FULL;
        auto& high=file.journal?native->journal_high:native->main_high;high=std::max(high,std::int64_t(bytes));
        file.write_pending=true;auto& call=native->reserve(state::action::truncate,index,file.fd,bytes);
        native->require_role(true);errno=0;call.entered=true;const int rc=::ftruncate(file.fd,static_cast<off_t>(bytes));const int error=rc?errno:0;
        native->returned(call,rc,error);if(rc){native->descriptor_uncertain=true;return SQLITE_IOERR_TRUNCATE;}
        native->inspect(file,index,true);
        if(file.size!=bytes){native->descriptor_uncertain=true;return SQLITE_IOERR_TRUNCATE;}
        file.write_pending=false;native->require_role(true);return SQLITE_OK;
#else
        (void)id;(void)borrower;return SQLITE_IOERR_TRUNCATE;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_TRUNCATE;}
}
int ordinary_target_staging_connection::descriptor_sync(std::uint64_t id,sqlite3_file* borrower,int flags)noexcept {
    (void)flags; // FULL/DATAONLY never weaken this fixed profile's actual fsync.
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);native->require_role(true);
        auto& call=native->reserve(state::action::sync,state::resource_slot(id),file.fd);
        native->require_role(true);file.sync_attempted=true;errno=0;call.entered=true;const int rc=::fsync(file.fd);const int error=rc?errno:0;native->returned(call,rc,error);
        if(!file.sync_returned){file.sync_rc=rc;file.sync_returned=true;}
        if(rc){native->descriptor_uncertain=true;return SQLITE_IOERR_FSYNC;}
        native->require_role(true);return SQLITE_OK;
#else
        (void)id;(void)borrower;return SQLITE_IOERR_FSYNC;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_FSYNC;}
}
int ordinary_target_staging_connection::descriptor_size(std::uint64_t id,sqlite3_file* borrower,sqlite3_int64* bytes)noexcept {
    if(!bytes)return SQLITE_IOERR_FSTAT;
    try{auto native=state_;auto& file=native->loan(id,borrower);native->inspect(file,state::resource_slot(id),false);
        native->require_role(false);*bytes=file.size;return SQLITE_OK;
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_FSTAT;}
}
int ordinary_target_staging_connection::descriptor_lock(std::uint64_t id,sqlite3_file* borrower,int requested)noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);const auto index=state::resource_slot(id);
        if(file.journal||requested<SQLITE_LOCK_SHARED||requested>SQLITE_LOCK_EXCLUSIVE)return SQLITE_IOERR_LOCK;
        const auto scope=native->active_scope.lock();
        if(scope&&(scope->projection||scope->history_chain)) {
            if(requested!=SQLITE_LOCK_SHARED)return SQLITE_IOERR_LOCK;
            native->require_role(false);
        }else native->require_role(true);
        if(requested<=file.lock_level)return SQLITE_OK;
        if(native->active_scope.lock()->ending)return SQLITE_IOERR_LOCK; // No ending lock escalation.
        constexpr std::int64_t pending=0x40000000LL,reserved=pending+1,shared=pending+2,count=510;
        if(file.lock_level==SQLITE_LOCK_NONE) {
            int rc=native->file_lock(file,index,F_SETLK,F_RDLCK,pending,1);if(rc!=SQLITE_OK)return rc;
            rc=native->file_lock(file,index,F_SETLK,F_RDLCK,shared,count);
            const int released=native->file_lock(file,index,F_SETLK,F_UNLCK,pending,1);
            if(released!=SQLITE_OK)return released;if(rc!=SQLITE_OK)return rc;file.lock_level=SQLITE_LOCK_SHARED;
        }
        if(requested==SQLITE_LOCK_RESERVED) {
            const int rc=native->file_lock(file,index,F_SETLK,F_WRLCK,reserved,1);if(rc!=SQLITE_OK)return rc;
            file.lock_level=SQLITE_LOCK_RESERVED;
        } else if(requested>=SQLITE_LOCK_PENDING) {
            if(file.lock_level<SQLITE_LOCK_PENDING) {
                const int rc=native->file_lock(file,index,F_SETLK,F_WRLCK,pending,1);if(rc!=SQLITE_OK)return rc;
                file.lock_level=SQLITE_LOCK_PENDING;
            }
            if(requested==SQLITE_LOCK_EXCLUSIVE) {
                const int rc=native->file_lock(file,index,F_SETLK,F_WRLCK,shared,count);if(rc!=SQLITE_OK)return rc;
                file.lock_level=SQLITE_LOCK_EXCLUSIVE;
            }
        }
        native->require_role(!(scope&&(scope->projection||scope->history_chain)));return SQLITE_OK;
#else
        (void)id;(void)borrower;(void)requested;return SQLITE_IOERR_LOCK;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_LOCK;}
}
int ordinary_target_staging_connection::descriptor_unlock(std::uint64_t id,sqlite3_file* borrower,int requested)noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);const auto index=state::resource_slot(id);
        if(file.journal||!(requested==SQLITE_LOCK_NONE||requested==SQLITE_LOCK_SHARED)||requested>file.lock_level)return SQLITE_IOERR_UNLOCK;
        constexpr std::int64_t pending=0x40000000LL,reserved=pending+1,shared=pending+2,count=510;
        if(requested==SQLITE_LOCK_SHARED&&file.lock_level>SQLITE_LOCK_SHARED) {
            const int rc=native->file_lock(file,index,F_SETLK,F_RDLCK,shared,count);if(rc!=SQLITE_OK)return rc;
            const int released=native->file_lock(file,index,F_SETLK,F_UNLCK,pending,2);if(released!=SQLITE_OK)return released;
        } else if(requested==SQLITE_LOCK_NONE) {
            const int rc=native->file_lock(file,index,F_SETLK,F_UNLCK,pending,count+2);if(rc!=SQLITE_OK)return rc;
        }
        file.lock_level=requested;native->require_role(false);return SQLITE_OK;
#else
        (void)id;(void)borrower;(void)requested;return SQLITE_IOERR_UNLOCK;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_UNLOCK;}
}
int ordinary_target_staging_connection::descriptor_reserved(std::uint64_t id,sqlite3_file* borrower,int* result)noexcept {
    if(!result)return SQLITE_IOERR_CHECKRESERVEDLOCK;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;auto& file=native->loan(id,borrower);if(file.journal)return SQLITE_IOERR_CHECKRESERVEDLOCK;
        if(file.lock_level>=SQLITE_LOCK_RESERVED){*result=1;return SQLITE_OK;}
        auto& call=native->reserve(state::action::reserved,0,file.fd);
        native->require_role(false);struct flock lock{};lock.l_type=F_WRLCK;lock.l_whence=SEEK_SET;
        lock.l_start=0x40000001LL;lock.l_len=1;errno=0;call.entered=true;const int rc=::fcntl(file.fd,F_GETLK,&lock);const int error=rc?errno:0;
        native->returned(call,rc,error);if(rc){native->descriptor_uncertain=true;return SQLITE_IOERR_CHECKRESERVEDLOCK;}
        native->require_role(false);*result=lock.l_type==F_UNLCK?0:1;return SQLITE_OK;
#else
        (void)id;(void)borrower;return SQLITE_IOERR_CHECKRESERVEDLOCK;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_CHECKRESERVEDLOCK;}
}
int ordinary_target_staging_connection::descriptor_close(std::uint64_t id,sqlite3_file* borrower)noexcept {
    try {
        auto native=state_;auto& file=native->loan(id,borrower);
        if(!file.journal&&(file.lock_level!=SQLITE_LOCK_NONE||file.pending_byte||file.reserved_byte||file.shared_range)) {
            const int rc=descriptor_unlock(id,borrower,SQLITE_LOCK_NONE);if(rc!=SQLITE_OK)return rc;
        }
        native->require_role(false);if(file.write_pending)return SQLITE_IOERR_CLOSE;
        file.borrowed=false;file.borrower=nullptr; // Borrow return is not native fd close or a refund.
        return SQLITE_OK;
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_CLOSE;}
}
int ordinary_target_staging_connection::descriptor_control(std::uint64_t id,sqlite3_file* borrower,int operation,void* argument)noexcept {
    try {
        auto native=state_;auto& file=native->loan(id,borrower);
        if(operation==SQLITE_FCNTL_LOCKSTATE&&argument){*static_cast<int*>(argument)=file.lock_level;return SQLITE_OK;}
#ifdef SQLITE_FCNTL_HAS_MOVED
        if(operation==SQLITE_FCNTL_HAS_MOVED&&argument) {
            // This provider owns the actual fd. No foreign VFS HAS_MOVED
            // result is substituted for journal descriptor custody.
            native->inspect(file,state::resource_slot(id),false);native->require_role(false);
            *static_cast<int*>(argument)=0;return SQLITE_OK;
        }
#endif
        if(operation==SQLITE_FCNTL_SIZE_HINT)return SQLITE_OK; // Optional preallocation hint; never allocate.
#ifdef SQLITE_FCNTL_POWERSAFE_OVERWRITE
        if(operation==SQLITE_FCNTL_POWERSAFE_OVERWRITE&&argument){*static_cast<int*>(argument)=0;return SQLITE_OK;}
#endif
        return SQLITE_NOTFOUND; // No native fd, JOURNAL_POINTER, mmap, WAL or parent-control escape.
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR;}
}
int ordinary_target_staging_connection::descriptor_remove(const char* path,int sync_directory)noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        auto native=state_;if(!native||!path)return SQLITE_IOERR_DELETE;native->require_role(true);
        if(native->fixed_main_path+"-journal"!=path||!native->named_journal)return SQLITE_IOERR_DELETE;
        const auto index=*native->named_journal;auto& file=*native->resources[index];
        if(!file.birth||!file.named||file.unlinked||file.unlink_attempted||file.borrowed||file.lock_level!=SQLITE_LOCK_NONE)return SQLITE_IOERR_DELETE;
        native->inspect(file,index,false);native->require_role(true);
        if(native->active_scope.lock()->ending) {
            // No expired catalog/path observation is reused as namespace proof.
            // Before unlink, match the actual named resource to the SAME fd birth.
            auto& named=native->reserve(state::action::inspect,index,native->member_fd);
            struct stat actual{};errno=0;named.entered=true;
            const int rc=::fstatat(native->member_fd,"store.sqlite-journal",&actual,AT_SYMLINK_NOFOLLOW);
            const int error=rc?errno:0;native->returned(named,rc,error);
            if(rc||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0600||
               actual.st_nlink!=1||std::uint64_t(actual.st_dev)!=file.device||std::uint64_t(actual.st_ino)!=file.inode||
               actual.st_size!=file.size) {native->descriptor_uncertain=true;return SQLITE_IOERR_DELETE;}
        }
        auto& call=native->reserve(state::action::unlink,index,native->member_fd);file.unlink_attempted=true;
        native->require_role(true);errno=0;call.entered=true;const int rc=::unlinkat(native->member_fd,"store.sqlite-journal",0);const int error=rc?errno:0;
        native->returned(call,rc,error);
        if(rc){native->descriptor_uncertain=true;return SQLITE_IOERR_DELETE;}
        file.unlinked=true;file.named=false;native->named_journal.reset();
        // The unlinked fd stays owned. Retained native nlink0 and exact name
        // absence are checked by the nonrecursive census before another span.
        native->inspect(file,index,false);native->require_role(true);
        file.unlink_sync_required=sync_directory!=0;
        if(sync_directory) {
            auto& sync=native->reserve(state::action::directory_sync,index,native->member_fd);
            native->require_role(true);errno=0;sync.entered=true;const int result=::fsync(native->member_fd);const int sync_error=result?errno:0;
            native->returned(sync,result,sync_error);file.unlink_sync_rc=result;file.unlink_sync_returned=true;
            if(result){native->descriptor_uncertain=true;return SQLITE_IOERR_DIR_FSYNC;}
            native->require_role(true);
        }
        // A returned unlinked journal is physically closed before a new
        // journal can be issued. Its inode differs from MAIN: this never
        // closes a second MAIN fd or drops the original MAIN POSIX locks.
        auto& close=native->reserve(state::action::close,index,file.fd);file.close_attempted=true;
        errno=0;close.entered=true;const int closed=::close(file.fd);const int close_error=closed?errno:0;
        file.close_rc=closed;file.close_errno=close_error;file.close_returned=true;native->returned(close,closed,close_error);
        if(closed){native->descriptor_uncertain=true;return SQLITE_IOERR_CLOSE;}
        file.fd=-1;return SQLITE_OK;
#else
        (void)path;(void)sync_directory;return SQLITE_IOERR_DELETE;
#endif
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_DELETE;}
}
int ordinary_target_staging_connection::descriptor_access(const char* path,int flags,int* present)noexcept {
    if(!path||!present)return SQLITE_IOERR_ACCESS;
    try {
        auto native=state_;if(!native)return SQLITE_IOERR_ACCESS;native->require_role(false);
        const auto main=native->fixed_main_path;
        if(flags!=SQLITE_ACCESS_EXISTS&&flags!=SQLITE_ACCESS_READ&&flags!=SQLITE_ACCESS_READWRITE)return SQLITE_IOERR_ACCESS;
        if(main==path) {*present=1;return SQLITE_OK;}
        if(main+"-journal"==path) {*present=native->named_journal?1:0;return SQLITE_OK;}
        // WAL/SHM are forbidden by the whole owned member census, not hidden
        // as an absence fallback. No other path gets a provider probe.
        if(main+"-wal"==path||main+"-shm"==path){*present=0;return SQLITE_OK;}
        return SQLITE_IOERR_ACCESS;
    }catch(...){if(state_&&state_->creator_current())state_->descriptor_uncertain=true;return SQLITE_IOERR_ACCESS;}
}
int ordinary_target_staging_connection::descriptor_full_path(const char* path,int capacity,char* output)noexcept {
    if(!path||!output||capacity<=0)return SQLITE_CANTOPEN;
    try {
        auto native=state_;if(!native)return SQLITE_CANTOPEN;native->require_role(false);
        const auto main=native->fixed_main_path;
        if(main!=path&&main+"-journal"!=path)return SQLITE_CANTOPEN;
        const auto bytes=std::strlen(path);
        if(bytes>=static_cast<std::size_t>(capacity))return SQLITE_CANTOPEN;
        std::memcpy(output,path,bytes+1);return SQLITE_OK;
    }catch(...){return SQLITE_CANTOPEN;}
}
bool ordinary_target_staging_connection::database_scope::abandon_unpublished_main(const ::lattice::database& actual)noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(!held_||!held_->cleanup||!held_->creator_binding(actual)||held_->left||held_->failed||
           !held_->close_observed||held_->close_rc!=SQLITE_OK||held_->retirement->state()!=database_retirement_state::phase::closed||
           held_->retirement->checked_close_result()!=SQLITE_OK||held_->retirement->raw_handle_escaped()||
           held_->retirement->owning_wrapper_moved())return false;
        const auto native=held_->original.lock();
        if(!native||!native->creator_current())return false;
        auto& receipt=native->abandoned_main;
        if(!receipt.selected)return false;
        if(receipt.attempted)return receipt.joined; // First facts only; no retry after unknown effects.
        receipt.attempted=true;
        const auto original=native->facade.lock();
        const auto bank=held_->ledger;
        if(!original||!native->target||!bank||native->descriptor_uncertain||native->named_journal||
           native->history_spool_attempted||native->history_spool||native->schema_attempted||native->catalog_attempted||
           native->history_header_attempted||native->history_page_scopes||native->row_import_attempted||
           native->import_suspension_attempted||native->history_chain_attempted||native->projection_attempted||
           !native->io->callbacks_joined(*original)||held_->active_callback_slot!=held_->callback_originals.size()||
           held_->active_callback_generation)return false;
        for(const auto& file:native->resources)if(file&&(file->borrowed||file->borrower||file->write_pending||
            file->lock_level!=SQLITE_LOCK_NONE||file->pending_byte||file->pending_write||file->reserved_byte||
            file->shared_range||file->exclusive_range))return false;
        for(std::size_t i=0;i<bank->count;++i)if(bank->calls[i].entered&&!bank->calls[i].returned)return false;
        for(const auto& callback:held_->callback_originals)if(callback&&
            (!callback->body_returned||!callback->leave_returned||!callback->leave_joined))return false;
        // Settle the SAME cleanup environmental bank before any namespace
        // deletion. Its first results remain in the retained scope. No native
        // callback can enter after checked SQLite close; leave observes this
        // ending instead of attempting a second settlement.
        native->settling_scope=held_;
        try {native->io->settle_descriptor_phase(*original,held_->environment);}
        catch(...){native->settling_scope.reset();held_->failed=true;return false;}
        native->settling_scope.reset();held_->abandonment_environment_joined=true;
        receipt.original=native->resources[0];
        if(!receipt.original||!receipt.original->birth||!receipt.original->named||receipt.original->unlinked||
           receipt.original->close_attempted||receipt.original->fd<0||receipt.original->fd!=native->main_fd||
           !native->birth_verified||!native->bound)return false;
        receipt.claim_joined=native->target->claim_staging_abandonment(original.get(),receipt.binding);
        receipt.claim_returned=true;
        const auto& binding=receipt.binding;
        if(!receipt.claim_joined||binding.member!=native->member_fd||!binding.root_leaf||!binding.member_leaf||
           binding.member_device!=native->member_device||binding.member_inode!=native->member_inode)return false;
        const auto record=[](state::disposition_call& call,int rc,int error)noexcept {
            call.result=rc;call.error=error;call.returned=true;
        };
        const auto inspect=[&](state::disposition_call& call,int fd) {
            if(call.attempted||!native->creator_current())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int rc=::fstat(fd,&call.value);const int error=rc?errno:0;record(call,rc,error);return rc==0;
        };
        const auto named=[&](state::disposition_call& call,int parent,const char* leaf) {
            if(call.attempted||!native->creator_current())return false;
            call.attempted=true;errno=0;call.entered=true;
            const int rc=::fstatat(parent,leaf,&call.value,AT_SYMLINK_NOFOLLOW);
            const int error=rc?errno:0;record(call,rc,error);return rc==0;
        };
        const auto directory=[](const struct stat& value,std::uint64_t device,std::uint64_t inode,bool private_mode) {
            return S_ISDIR(value.st_mode)&&value.st_uid==::geteuid()&&
                (private_mode?(value.st_mode&07777)==0700:(value.st_mode&0022)==0)&&
                std::uint64_t(value.st_dev)==device&&std::uint64_t(value.st_ino)==inode;
        };
        const auto file_same=[&](const struct stat& value,nlink_t links) {
            const auto& file=*receipt.original;
            return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&(value.st_mode&07777)==0600&&
                value.st_nlink==links&&value.st_size==file.size&&value.st_size>=0&&value.st_size<=native->main_high&&
                std::uint64_t(value.st_dev)==file.device&&std::uint64_t(value.st_ino)==file.inode;
        };
        if(!inspect(receipt.stage,binding.stage)||!directory(receipt.stage.value,binding.stage_device,binding.stage_inode,false)||
           !inspect(receipt.root,binding.root)||!directory(receipt.root.value,binding.root_device,binding.root_inode,true)||
           !named(receipt.root_named,binding.stage,binding.root_leaf->c_str())||
           !directory(receipt.root_named.value,binding.root_device,binding.root_inode,true)||
           !inspect(receipt.member,binding.member)||!directory(receipt.member.value,binding.member_device,binding.member_inode,true)||
           !named(receipt.member_named,binding.root,binding.member_leaf->c_str())||
           !directory(receipt.member_named.value,binding.member_device,binding.member_inode,true))return false;
        const auto census=[&](state::disposition_scan& scan,bool expect_main) {
            if(scan.open.attempted||!native->creator_current())return false;
            scan.open.attempted=true;errno=0;scan.open.entered=true;
            const int fd=::openat(binding.member,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
            const int open_error=fd<0?errno:0;scan.descriptor=fd;record(scan.open,fd,open_error);
            if(fd<0)return false;
            const bool identity=inspect(scan.inspect,fd)&&
                directory(scan.inspect.value,binding.member_device,binding.member_inode,true);
            scan.convert.attempted=true;errno=0;scan.convert.entered=true;
            scan.directory=::fdopendir(fd);const int convert_error=scan.directory?0:errno;
            record(scan.convert,scan.directory?0:-1,convert_error);
            if(!scan.directory) {
                scan.close.attempted=true;errno=0;scan.close.entered=true;
                const int rc=::close(fd);const int error=rc?errno:0;record(scan.close,rc,error);
                if(!rc)scan.descriptor=-1;return false;
            }
            bool exact=identity;
            // Fixed nonrecursive census: no more than MAIN plus one refusal
            // entry is observed. It never traverses history or follows a name.
            while(exact) {
                scan.read_attempted=true;errno=0;const auto* entry=::readdir(scan.directory);
                scan.read_returned=true;scan.read_error=errno;
                if(!entry){scan.terminal=scan.read_error==0;exact=scan.terminal;break;}
                if(std::strcmp(entry->d_name,".")==0||std::strcmp(entry->d_name,"..")==0)continue;
                ++scan.entries;
                if(expect_main&&scan.entries==1&&std::strcmp(entry->d_name,main_leaf)==0)scan.main_seen=true;
                else exact=false;
            }
            scan.close.attempted=true;errno=0;scan.close.entered=true;
            const int rc=::closedir(scan.directory);const int error=rc?errno:0;record(scan.close,rc,error);
            if(!rc){scan.directory=nullptr;scan.descriptor=-1;}
            return exact&&scan.terminal&&rc==0&&(expect_main?scan.main_seen&&scan.entries==1:scan.entries==0);
        };
        if(!census(receipt.before,true)||!inspect(receipt.main,receipt.original->fd)||!file_same(receipt.main.value,1)||
           !named(receipt.main_named,binding.member,main_leaf)||!file_same(receipt.main_named.value,1))return false;
        // Separate private abandonment authority, never require_role(true) or
        // SQLite xDelete: those remain unable to delete MAIN during cleanup.
        if(!native->creator_current())return false;
        receipt.unlink.attempted=true;receipt.original->unlink_attempted=true;errno=0;receipt.unlink.entered=true;
        const int removed=::unlinkat(binding.member,main_leaf,0);const int unlink_error=removed?errno:0;
        record(receipt.unlink,removed,unlink_error);
        if(removed)return false;
        receipt.original->unlinked=true;receipt.original->named=false;
        if(!inspect(receipt.unlinked,receipt.original->fd)||!file_same(receipt.unlinked.value,0))return false;
        const bool name_exists=named(receipt.absent,binding.member,main_leaf);
        if(name_exists||!receipt.absent.returned||receipt.absent.result!=-1||receipt.absent.error!=ENOENT||
           !census(receipt.after,false)||
           !named(receipt.root_named_after,binding.stage,binding.root_leaf->c_str())||
           !directory(receipt.root_named_after.value,binding.root_device,binding.root_inode,true)||
           !named(receipt.member_named_after,binding.root,binding.member_leaf->c_str())||
           !directory(receipt.member_named_after.value,binding.member_device,binding.member_inode,true))return false;
        if(!native->creator_current())return false;
        receipt.directory_sync.attempted=true;errno=0;receipt.directory_sync.entered=true;
        const int synced=::fsync(binding.member);const int sync_error=synced?errno:0;
        record(receipt.directory_sync,synced,sync_error);
        if(synced)return false;
        if(!native->creator_current())return false;
        receipt.close.attempted=true;receipt.original->close_attempted=true;errno=0;receipt.close.entered=true;
        const int closed=::close(receipt.original->fd);const int close_error=closed?errno:0;
        record(receipt.close,closed,close_error);
        receipt.original->close_rc=closed;receipt.original->close_errno=close_error;receipt.original->close_returned=true;
        if(closed)return false; // EINTR/unknown first close keeps debt; never retried.
        receipt.original->fd=-1;native->main_fd=-1;
        receipt.joined=true;return true; // MAIN namespace only; no VFS/worker ending or credit refund.
#else
        (void)actual;return false;
#endif
    }catch(...){if(held_)held_->failed=true;return false;}
}
bool ordinary_target_staging_connection::database_scope::join_native_files(const ::lattice::database& actual)noexcept {
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(!held_||!held_->cleanup||!held_->creator_binding(actual)||held_->left||
           !held_->close_observed||held_->close_rc!=SQLITE_OK||held_->retirement->state()!=database_retirement_state::phase::closed||
           held_->retirement->checked_close_result()!=SQLITE_OK)return false;
        const auto native=held_->original.lock();const auto original=native->facade.lock();
        if(native->terminal_abandonment.attempted)return false; // No ordinary descriptor close after general election.
        if(!original||!native->io->callbacks_joined(*original))return false;
        for(std::size_t i=0;i<=native->journal_count;++i) {
            if(!native->resources[i])continue;auto& file=*native->resources[i];
            if(file.borrowed||file.borrower||file.lock_level!=SQLITE_LOCK_NONE||file.pending_byte||file.reserved_byte||file.shared_range||file.write_pending)return false;
        }
        const auto bank=held_->ledger;if(!bank)return false;
        for(std::size_t i=0;i<bank->count;++i)if(bank->calls[i].entered&&!bank->calls[i].returned)return false;
        // Actual SQLite checked-close plus borrower/lock return is required
        // before closing any retained fd. close_v2 cannot supply this witness.
        bool known=true;
        for(std::size_t i=0;i<=native->journal_count;++i) {
            if(!native->resources[i])continue;auto& file=*native->resources[i];if(file.fd<0)continue;
            if(i==0&&native->abandoned_main.selected)continue; // Explicit disposition retains MAIN until exact unlink proof.
            if(file.close_attempted){known=known&&file.close_returned&&file.close_rc==0;continue;}
            auto& call=native->reserve(state::action::close,i,file.fd);file.close_attempted=true;
            errno=0;call.entered=true;const int rc=::close(file.fd);const int error=rc?errno:0;
            file.close_rc=rc;file.close_errno=error;file.close_returned=true;native->returned(call,rc,error);
            if(rc){known=false;native->descriptor_uncertain=true;continue;} // EINTR/unknown fd ending is never retried.
            file.fd=-1;if(i==0)native->main_fd=-1;
        }
        known=known&&!native->descriptor_uncertain;
        if(!known)return false;
        if(native->abandoned_main.selected&&!abandon_unpublished_main(actual))return false;
        if(!native->io->retire_descriptor_registration(*original))return false;
        held_->native_files_joined=true;return true;
#else
        (void)actual;return false;
#endif
    }catch(...){if(held_)held_->failed=true;return false;}
}

bool ordinary_target_staging_connection::descriptor_retirement_admissible()const noexcept {
    try {
        const auto native=state_;if(!native||!native->creator_current())return false;
        if(native->terminal_abandonment.attempted)return false; // Separate terminal VFS ending remains required.
        if(native->abandoned_main.selected&&!native->abandoned_main.joined)return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||!scope->cleanup||scope->left||!scope->shell||
           !scope->creator_binding(*scope->shell)||!scope->close_observed||scope->close_rc!=SQLITE_OK||
           scope->retirement->state()!=database_retirement_state::phase::closed||
           scope->retirement->checked_close_result()!=SQLITE_OK)return false;
        for(std::size_t i=0;i<=native->journal_count;++i) {
            if(!native->resources[i])continue;const auto& file=*native->resources[i];
            if(file.fd>=0||file.borrowed||file.borrower||file.lock_level!=SQLITE_LOCK_NONE||file.pending_byte||file.reserved_byte||file.shared_range||
               (file.birth&&(!file.close_returned||file.close_rc!=0)))return false;
        }
        return !native->descriptor_uncertain;
    }catch(...){return false;}
}

void ordinary_target_staging_connection::begin_database_schema(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_schema_original>& original,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);auto native=state_;
    if(!original||retained||native->schema_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity)
        birth_fail("target schema actual one-shot scope unavailable");
    original->require_connection(*this,end);
    actual.require_target_fixed_constructor_(*this);
    native->schema_attempted=true; // Native ending capacity already resides in SAME fixed ledger.
    auto scope=std::make_shared<database_scope::scope_state>();
    scope->original=native;scope->shell=&actual;scope->retirement=actual.retirement_;
    scope->schema=true;scope->schema_original=original;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope)); // Before post-current/error conversion.
    native->publish_scope(scope);
    retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_constructor_ending(
    const ::lattice::database& actual,sqlite3* native)const {
    const auto original=held_?held_->original.lock():nullptr;
    if(!original||!original->creator_current()||held_->schema||held_->cleanup||!held_->left||
       !held_->leave_returned||!held_->leave_joined||held_->shell!=&actual||held_->retirement!=actual.retirement_||
       !held_->open_observed||held_->open_rc!=SQLITE_OK||held_->native!=native||!native)
        birth_fail("target schema lacks SAME actual constructor native ending");
}
void ordinary_target_staging_connection::database_scope::require_schema_work(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||held_->ending||held_->cleanup||held_->left)
        birth_fail("target schema generated work is sealed");
    require_current(actual);
}
void ordinary_target_staging_connection::database_scope::begin_schema_ending(const ::lattice::database& actual) {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||held_->left||held_->cleanup)
        birth_fail("target schema ending lacks its actual admitted scope");
    const auto native=held_->original.lock();const auto original=native->facade.lock();
    if(!original||!native->io->callbacks_joined(*original))
        birth_fail("target schema ending overlaps an unreturned file callback");
    held_->ending=true; // SAME original unwind; never reset/rearm or accept new generated work.
}
void ordinary_target_staging_connection::database_scope::record_schema_begin(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||held_->schema_begin_observed){if(held_)held_->failed=true;return;}
    held_->schema_begin_observed=true;held_->schema_begin_rc=rc;
}
void ordinary_target_staging_connection::database_scope::record_schema_commit(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||held_->ending||held_->schema_commit_observed){if(held_)held_->failed=true;return;}
    held_->schema_commit_observed=true;held_->schema_commit_rc=rc;
}
void ordinary_target_staging_connection::database_scope::record_schema_rollback(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->ending||held_->schema_rollback_observed||
       (held_->schema_commit_observed&&held_->schema_commit_rc==SQLITE_DONE)){if(held_)held_->failed=true;return;}
    held_->schema_rollback_observed=true;held_->schema_rollback_rc=rc;
}
void ordinary_target_staging_connection::database_scope::record_schema_finalized(const ::lattice::database& actual,int rc)noexcept {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||held_->schema_finalizations>=(held_->catalog?4164u:held_->history_page?160u:held_->projection?388u:held_->history_chain?8u:held_->import_suspension?16387u:held_->row_import?136u:32u)){if(held_)held_->failed=true;return;}
    ++held_->schema_finalizations;if(rc!=SQLITE_OK)held_->schema_finalization_failed=true;
}
std::uint64_t ordinary_target_staging_connection::active_descriptor_phase_generation()const {
    const auto native=state_;if(!native||!native->creator_current())birth_fail("target phase creator unavailable");
    const auto scope=native->active_scope.lock();
    if(!scope||scope.get()!=native->scope_identity||scope->left||!scope->published||!native->shell||
       !scope->creator_binding(*native->shell)||!scope->generation)birth_fail("target phase actual original unavailable");
    return scope->generation;
}
std::uint64_t ordinary_target_staging_connection::settling_descriptor_phase_generation()const {
    const auto native=state_;if(!native||!native->creator_current())birth_fail("target settlement creator unavailable");
    const auto scope=native->settling_scope;
    if(!scope||native->active_scope.lock()!=scope||scope.get()!=native->scope_identity||
       scope->left||!scope->published||!native->shell||!scope->creator_binding(*native->shell)||!scope->generation)
        birth_fail("target settlement lacks actual leave original");
    return scope->generation;
}
bool ordinary_target_staging_connection::descriptor_environment_phase_admissible(std::uint64_t generation)const noexcept {
    try {
        const auto native=state_;if(!native||!native->creator_current())return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||scope->left||!scope->published||!native->shell||
           !scope->creator_binding(*native->shell))return false;
        if(scope->generation==generation)return true;
        const auto prior=scope->cleanup_predecessor;
        return scope->cleanup&&prior&&prior->generation==generation&&!prior->leave_joined&&
            prior->ledger==scope->ledger&&prior->environment==scope->environment;
    }catch(...){return false;}
}
bool ordinary_target_staging_connection::descriptor_schema_ending_admissible()const noexcept {
    const auto native=state_;if(!native||!native->creator_current())return false;
    const auto scope=native->active_scope.lock();
    return scope&&scope.get()==native->scope_identity&&scope->schema&&scope->ending&&!scope->left&&scope->published&&
        native->shell&&scope->creator_binding(*native->shell)&&!native->descriptor_uncertain;
}

void ordinary_target_staging_connection::database_scope::require_metadata_ending(
    const ::lattice::database& actual,sqlite3* native)const {
    const auto owner=held_?held_->original.lock():nullptr;
    if(!owner||!owner->creator_current()||!held_->schema||held_->catalog||held_->cleanup||held_->ending||
       !held_->left||!held_->leave_returned||!held_->leave_joined||held_->failed||held_->shell!=&actual||
       held_->retirement!=actual.retirement_||held_->native!=native||!native||
       !held_->schema_begin_observed||held_->schema_begin_rc!=SQLITE_DONE||
       !held_->schema_commit_observed||held_->schema_commit_rc!=SQLITE_DONE||
       held_->schema_finalizations!=10||held_->schema_finalization_failed)
        birth_fail("target catalog lacks SAME actual metadata native ending");
}
void ordinary_target_staging_connection::begin_database_catalog(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_schema_original>& original,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);auto native=state_;
    if(!original||retained||native->catalog_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity)
        birth_fail("target catalog actual one-shot scope unavailable");
    original->require_catalog(*this,end);actual.require_target_metadata_ending_(*this,*original);
    native->catalog_attempted=true; // Distinct role; constructor/metadata histories are never reset.
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->catalog=true;scope->schema_original=original;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope));
    native->publish_scope(scope);
    retained->require_current(actual);
}

void ordinary_target_staging_connection::database_scope::require_catalog_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->catalog||held_->cleanup||held_->left)
        birth_fail("target catalog requires its distinct SAME actual role");
}

void ordinary_target_staging_connection::database_scope::require_catalog_ending(const ::lattice::database& actual,sqlite3* native)const {
    const auto owner=held_?held_->original.lock():nullptr;
    if(!owner||!owner->creator_current()||!held_->schema||!held_->catalog||held_->history_header||held_->cleanup||held_->ending||
       !held_->left||!held_->leave_returned||!held_->leave_joined||held_->failed||held_->shell!=&actual||
       held_->retirement!=actual.retirement_||held_->native!=native||!native||
       !held_->schema_begin_observed||held_->schema_begin_rc!=SQLITE_DONE||
       !held_->schema_commit_observed||held_->schema_commit_rc!=SQLITE_DONE||held_->schema_finalization_failed)
        birth_fail("GROUP history lacks SAME actual completed catalog scope");
}
void ordinary_target_staging_connection::begin_database_history_header(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);auto native=state_;
    if(!owner||retained||native->history_header_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity)
        birth_fail("GROUP history header distinct original scope unavailable");
    owner->require_header_original(*this);const auto schema=owner->require_header_schema(*this);
    actual.require_target_catalog_ending_(*this,*schema);
    native->history_header_attempted=true; // No constructor/schema/catalog rearm or replacement.
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->history_header=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope));
    native->publish_scope(scope);
    retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_history_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->history_header||held_->catalog||held_->cleanup||held_->left)
        birth_fail("GROUP history header needs its distinct SAME original role");
}

void ordinary_target_staging_connection::begin_database_history_page(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);auto native=state_;
    if(!receiving||retained||native->history_chain_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity||
       native->history_page_scopes==UINT64_MAX)birth_fail("GROUP distinct page actual scope/capacity unavailable");
    (void)receiving->require_page_original(*this);const auto schema=receiving->require_header_schema(*this);
    actual.require_target_history_header_ending_(*this,*receiving);
    ++native->history_page_scopes; // Actual newly issued page scope, no prior role reset or unknown replay.
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;scope->retirement=actual.retirement_;
    scope->schema=true;scope->history_page=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope));
    native->publish_scope(scope);retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_history_page_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->history_page||held_->history_header||held_->catalog||held_->cleanup||held_->left)
        birth_fail("GROUP page requires its distinct SAME Native original scope");
}

void ordinary_target_staging_connection::begin_database_projection(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);const auto native=state_;
    if(!receiving||retained||native->projection_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity)
        birth_fail("GROUP projection distinct original read scope unavailable");
    actual.require_target_history_complete_(*this,*receiving);
    const auto schema=receiving->require_header_schema(*this);schema->require_projection(*this,end);
    native->projection_attempted=true; // No catalog/header/page/constructor rearming.
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->projection=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope)); // Hold before post-current/diagnostics.
    native->publish_scope(scope);retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_projection_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->projection||held_->history_header||
       held_->history_page||held_->catalog||held_->cleanup||held_->left)
        birth_fail("GROUP projection lacks its distinct SAME Native read role");
}

void ordinary_target_staging_connection::begin_database_history_chain(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);const auto native=state_;
    if(!receiving||retained||!native->history_chain_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity||native->history_chain_scopes==UINT64_MAX)
        birth_fail("GROUP cold-chain distinct original read scope unavailable");
    actual.require_target_history_projection_ending_(*this,*receiving);
    actual.require_target_history_chain_issued_(*this,*receiving);
    const auto schema=receiving->require_header_schema(*this);schema->require_projection(*this,end);
    ++native->history_chain_scopes; // Genuine next phase only after previous actual ending, not a reset.
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->history_chain=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope));
    native->publish_scope(scope);retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_history_chain_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->history_chain||held_->history_header||
       held_->history_page||held_->catalog||held_->projection||held_->cleanup||held_->left)
        birth_fail("GROUP cold-chain lacks its distinct SAME Native read role");
}

void ordinary_target_staging_connection::begin_database_import_suspension(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);const auto native=state_;
    if(!receiving||retained||!native->import_suspension_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity)
        birth_fail("GROUP import suspension distinct original writer unavailable");
    actual.require_target_history_chain_ending_(*this,*receiving);
    actual.require_target_import_suspension_issued_(*this,*receiving);
    const auto schema=receiving->require_header_schema(*this);schema->require_import_suspension(*this,end);
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->import_suspension=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope)); // Held before native effects and postchecks.
    native->publish_scope(scope);retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_import_suspension_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->import_suspension||held_->history_header||
       held_->history_page||held_->catalog||held_->projection||held_->history_chain||held_->cleanup||held_->left)
        birth_fail("GROUP import suspension lacks its distinct SAME Native writer role");
}

void ordinary_target_staging_connection::begin_database_row_import(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& receiving,
    std::unique_ptr<database_scope>& retained,ordinary_launch::deadline end) {
    require_original(end);const auto native=state_;
    if(!receiving||retained||!native->row_import_attempted||native->cleanup_attempted||native->shell.get()!=&actual||
       !native->active_scope.expired()||native->scope_identity||native->row_import_scopes==UINT64_MAX)
        birth_fail("GROUP bounded row import distinct writer unavailable");
    actual.require_target_import_suspension_ending_(*this,*receiving);
    actual.require_target_row_import_issued_(*this,*receiving);
    const auto schema=receiving->require_header_schema(*this);schema->require_row_import(*this,end);
    ++native->row_import_scopes;
    auto scope=std::make_shared<database_scope::scope_state>();scope->original=native;scope->shell=&actual;
    scope->retirement=actual.retirement_;scope->schema=true;scope->row_import=true;scope->schema_original=schema;scope->native=actual.db_;
    retained=std::unique_ptr<database_scope>(new database_scope(scope));
    native->publish_scope(scope);retained->require_current(actual);
}
void ordinary_target_staging_connection::database_scope::require_row_import_role(const ::lattice::database& actual)const {
    if(!held_||!held_->creator_binding(actual)||!held_->schema||!held_->row_import||held_->history_header||held_->history_page||
       held_->catalog||held_->projection||held_->history_chain||held_->import_suspension||held_->cleanup||held_->left)
        birth_fail("GROUP row import lacks its distinct SAME Native writer role");
}

void ordinary_target_staging_connection::require_history_spool_observation(const ordinary_target_history_spool& spool,
    const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_history_header_ending(receiving,end);const auto native=state_;
    if(!native||!native->creator_current()||!native->history_spool_attempted||native->history_spool.get()!=&spool||
       native->member_fd<0||!native->shell||native->cleanup_attempted||native->descriptor_uncertain||native->first_failure)
        birth_fail("GROUP history observation lost SAME Conn/spool/header ending");
    const auto scope=native->active_scope.lock();
    if(!scope) {
        if(native->scope_identity)birth_fail("GROUP history observation scope identity unresolved");
        return; // Pure historical observation with no active SQL consumer.
    }
    const auto schema=receiving.require_header_schema(*this);
    const unsigned roles=unsigned(scope->projection)+unsigned(scope->history_chain)+unsigned(scope->import_suspension)+unsigned(scope->row_import);
    if(scope.get()!=native->scope_identity||native->last_scope!=scope||scope->original.lock()!=native||
       scope->generation!=native->scope_generation||!scope->creator_binding(*native->shell)||!scope->schema||roles!=1||
       scope->catalog||scope->history_header||scope->history_page||scope->cleanup||scope->ending||scope->left||scope->failed||
       scope->schema_finalization_failed||scope->native!=native->shell->db_||scope->schema_original.lock()!=schema||
       !scope->ledger||scope->ledger->issuer!=scope.get())
        birth_fail("GROUP history observation requires SAME actual distinct consumer scope");
    scope->require(*native->shell);schema->require_connection(*this,end);
    // No BEGIN/COMMIT boolean grants an operation here: only retained earlier
    // header/raw/SQL postimages may be observed. A raw read needs its separate
    // actual BEGIN-active chain/import borrower permission and Native ledger.
}
const ordinary_history_descriptor& ordinary_target_staging_connection::history_spool_header_observation(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& receiving,ordinary_launch::deadline end)const {
    require_history_spool_observation(spool,receiving,end);return receiving.require_header_original(*this);
}
