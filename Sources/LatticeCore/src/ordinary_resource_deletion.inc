// Included only in the actual resource issuer TU. This is a filesystem
// operation, not a decoded boolean/receipt authorizing a later Swift unlink.
struct store_deletion_operation::implementation {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();
    const deadline end;
    resource_root* root=nullptr;
    descriptor root_entry,member,entry,physical;
    std::string name,path;
    std::vector<std::uint8_t> member_bytes;
    file_identity main_identity,stub_identity;
    bool marker=false,checkpointed=false,attempted=false,finished=false;
    store_deletion_outcome actual;
    std::exception_ptr first_native;
    explicit implementation(deadline original):end(original){}
    void remember(store_deletion_stage stage,int code)noexcept {
        if(actual.first==store_deletion_stage::none){actual.first=stage;actual.first_code=code;}
    }
    [[noreturn]] static void foreign(){
        store_deletion_outcome actual;actual.first=store_deletion_stage::foreign_return;
        actual.foreign_return=true;throw store_deletion_failure(actual);
    }
    void issuer()const {if(creator!=pid())foreign();}
    // Preadmitted fixed custody, before any open can publish a Native handle.
    // Foreign destruction abandons the raw DATA without a Native API call.
    struct admin_handle final {
        const std::uint64_t creator=pid();
        sqlite3* value=nullptr;
        store_physical_open_original original;
        implementation* owner=nullptr;
        explicit admin_handle(implementation& actual)noexcept:owner(&actual){}
        void close()noexcept {
            if(creator!=pid()||!value)return;
            auto* actual=std::exchange(value,nullptr);
            const int closed=store_physical_close_owned(original,actual);
            if(creator!=pid())return;
            owner->actual.close=closed;owner->actual.close_returned=true;
            if(closed!=SQLITE_OK){
                owner->remember(store_deletion_stage::admin_close,closed);
                const int fallback=store_physical_close_v2_owned(original,actual);
                if(creator!=pid())return;
                owner->actual.fallback=fallback;owner->actual.fallback_returned=true;
                if(fallback!=SQLITE_OK)owner->remember(store_deletion_stage::admin_fallback,fallback);
            }
        }
        ~admin_handle()noexcept{close();}
        admin_handle(const admin_handle&)=delete;
    };
    void check()const {
        if(creator!=pid()||!root)resource_fail();bound(end);root->check(end);
        if(read_record(member.value,"member.v1",end)!=member_bytes)resource_fail();
    }
    bool exists(const std::string& leaf_name,struct stat& value)const {
        if(::fstatat(root->generation_fd.value,leaf_name.c_str(),&value,AT_SYMLINK_NOFOLLOW)==0)return true;
        if(errno!=ENOENT)resource_fail();return false;
    }
    bool record_exists(const char* record)const {
        struct stat value{};if(::fstatat(member.value,record,&value,AT_SYMLINK_NOFOLLOW)==0)return true;
        if(errno!=ENOENT)resource_fail();return false;
    }
    json binding()const {
        return {{"format","lattice-resource-delete-v1"},{"epoch",root->epoch},{"name",name},
            {"memberSHA256",digest_string(member_bytes)},{"parent",identity_json(root->generation_id)},
            {"main",identity_json(main_identity)},{"logicalStub",identity_json(stub_identity)}};
    }
    void exact_record(const char* record,const json& expected)const {
        if(read_record(member.value,record,end)!=bytes(expected))resource_fail();
    }
    void publish_once(const char* record,const json& expected)const {
        check();publish_record(member.value,record,expected,creator,end);
    }
    void main_name(bool may_be_absent)const {
        struct stat value{};
        if(!exists(name,value)){if(!may_be_absent)resource_fail();return;}
        if(!S_ISREG(value.st_mode)||value.st_uid!=::geteuid()||value.st_nlink!=1||
           (value.st_mode&0022)||id(value)!=main_identity)resource_fail();
    }
    void stub_name(bool may_be_absent)const {
        struct stat value{};
        if(::fstatat(root->logical_fd.value,name.c_str(),&value,AT_SYMLINK_NOFOLLOW)){
            if(errno==ENOENT&&may_be_absent)return;resource_fail();
        }
        if(!S_ISREG(value.st_mode)||value.st_uid!=::geteuid()||value.st_nlink!=1||
           value.st_size||(value.st_mode&07777)||id(value)!=stub_identity)resource_fail();
    }
#endif
};
store_deletion_operation::store_deletion_operation(implementation* actual)noexcept:impl_(actual){}
store_deletion_operation::~store_deletion_operation()noexcept {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid())delete impl_;
#endif
}
store_deletion_operation::store_deletion_operation(store_deletion_operation&& other)noexcept
    :impl_(std::exchange(other.impl_,nullptr)){}
store_deletion_operation store_deletion_operation::begin(const std::string& path,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();auto* installed=factory_installation(end);
    if(!installed||resource_memory_path(path))resource_fail();
    auto [root,name]=select(*installed,path,end);
    auto* state=new implementation(end);
    if(creator!=pid())resource_fail();
    try {
        state->root=root;state->name=name;state->path=root->generation+"/"+name;
        auto root_entry=regular_control(root->control_fd.value,"root.entry.lock",root->root_entry_id);
        lock(root_entry.value,LOCK_EX,end);root->check(end);
        const std::vector<std::uint8_t> key(name.begin(),name.end());const auto slot=digest_string(key);leaf(slot);
        descriptor member(::openat(root->control_fd.value,slot.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto folder=metadata(member.value);struct stat named{};
        if(!S_ISDIR(folder.st_mode)||folder.st_uid!=::geteuid()||(folder.st_mode&07777)!=0700||
           ::fstatat(root->control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(folder,named))resource_fail();
        auto original=read_record(member.value,"member.v1",end);const auto record=parse(original);
        if(text(record,"format")!="lattice-resource-member-v1"||text(record,"epoch",64)!=root->epoch||
           text(record,"name",255)!=name||identity_json(record.at("parent"))!=root->generation_id)resource_fail();
        auto entry=regular_control(member.value,"entry.lock",identity_json(record.at("entry")));
        lock(entry.value,LOCK_EX,end);if(read_record(member.value,"member.v1",end)!=original)resource_fail();
        auto physical=regular_control(member.value,"physical.lock",identity_json(record.at("physical")));
        state->member_bytes=std::move(original);state->main_identity=identity_json(record.at("main"));
        state->stub_identity=identity_json(record.at("logicalStub"));
        state->root_entry.~descriptor();new(&state->root_entry)descriptor(std::move(root_entry));
        state->member.~descriptor();new(&state->member)descriptor(std::move(member));
        state->entry.~descriptor();new(&state->entry)descriptor(std::move(entry));
        state->physical.~descriptor();new(&state->physical)descriptor(std::move(physical));
        const auto active=parse(read_record(state->member.value,"state.v1",end));
        if(text(active,"phase",32)!="active"||text(active,"epoch",64)!=root->epoch||
           identity_json(active.at("main"))!=state->main_identity)resource_fail();
        state->marker=state->record_exists("deletion.v1");
        if(state->marker)state->exact_record("deletion.v1",state->binding());
        state->checkpointed=state->record_exists("deletion-checkpoint.v1");
        if(state->checkpointed){if(!state->marker)resource_fail();state->exact_record("deletion-checkpoint.v1",state->binding());}
        state->main_name(state->marker&&state->checkpointed);state->stub_name(state->marker&&state->checkpointed);
        state->check();return store_deletion_operation(state);
    } catch(...) {if(creator==pid())delete state;throw;}
#else
    (void)path;(void)end;resource_fail();
#endif
}
const std::string& store_deletion_operation::actual_path()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid())resource_fail();impl_->check();return impl_->path;
#else
    resource_fail();
#endif
}
store_deletion_outcome store_deletion_operation::outcome()const noexcept {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid())return impl_->actual;
#endif
    store_deletion_outcome refused;refused.first=store_deletion_stage::foreign_return;
    refused.foreign_return=true;return refused;
}
void store_deletion_operation::finish(){
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid())resource_fail();auto& state=*impl_;state.check();
    if(state.attempted)resource_fail();state.attempted=true;
    auto current=store_deletion_stage::physical_wait;
    try {
    // Cooperating handles keep independent physical SH until the complete
    // original API/VFS/Native tails ended; process death also closes those
    // kernel leases. No wrapper count, path cache or SQLITE_OK fiction is used.
    try{lock(state.physical.value,LOCK_EX,state.end);state.check();}
    catch(...){state.issuer();state.remember(store_deletion_stage::physical_wait,0);
        state.first_native=std::current_exception();throw store_deletion_failure(state.actual,state.first_native);}
    if(!state.checkpointed){
        current=store_deletion_stage::admin_open;
        state.main_name(false);state.stub_name(false);
        // Dup only the CONTROL description already holding EX. It remains
        // locked by the operation after the actual admin original ends.
        descriptor lease(::fcntl(state.physical.value,F_DUPFD_CLOEXEC,0));
        auto physical=store_physical_lifetime::adopt_lease_(lease.value);
        if(state.creator!=pid())resource_fail();lease.release();
        implementation::admin_handle actual(state); // before the actual open
        const auto original_end=state.end;
        auto progress=[](void* value)noexcept->int {
            return std::chrono::steady_clock::now()>=*static_cast<const deadline*>(value);
        };
        const auto* lower=sqlite3_vfs_find(nullptr);struct stat expected{};
        if(!state.exists(state.name,expected)||id(expected)!=state.main_identity)resource_fail();
        int frames=-2,checkpointed=-2;
        try{
            state.issuer();
            const int opened=store_physical_vfs::open_(physical,const_cast<sqlite3_vfs*>(lower),state.path,expected,
                &actual.value,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,&actual.original);
            state.issuer();state.actual.open=opened;state.actual.open_returned=true;
            if(opened!=SQLITE_OK||!actual.value)state.remember(store_deletion_stage::admin_open,opened==SQLITE_OK?SQLITE_CANTOPEN:opened);
            else {
                state.issuer();const int configured=sqlite3_busy_timeout(actual.value,0);state.issuer();
                if(configured!=SQLITE_OK){state.actual.work=configured;state.actual.work_returned=true;
                    state.remember(store_deletion_stage::admin_work,configured);}
                else {
                    state.issuer();sqlite3_progress_handler(actual.value,1000,progress,const_cast<deadline*>(&original_end));state.issuer();
                    int work=store_physical_exec(actual.value,"PRAGMA schema_version;",nullptr,nullptr,nullptr);state.issuer();
                    if(work==SQLITE_OK){work=store_physical_wal_checkpoint(actual.value,"main",SQLITE_CHECKPOINT_TRUNCATE,&frames,&checkpointed);state.issuer();}
                    state.actual.work=work;state.actual.work_returned=true;
                    if(work!=SQLITE_OK)state.remember(store_deletion_stage::admin_work,work);
                    // No inherited raw handler reset after any actual callback.
                    state.issuer();sqlite3_progress_handler(actual.value,0,nullptr,nullptr);state.issuer();
                }
            }
        }catch(...){
            state.issuer();if(!state.first_native)state.first_native=std::current_exception();
            state.remember(state.actual.open_returned?store_deletion_stage::admin_work:store_deletion_stage::admin_open,0);
        }
        state.issuer();actual.close();state.issuer();
        const bool ended=store_physical_lifetime::wait_until_(physical,state.end);state.issuer();
        state.actual.ending_observed=true;state.actual.physical_ended=ended;
        if(!ended)state.remember(store_deletion_stage::admin_ending,0);
        if(!((frames==-1&&checkpointed==-1)||(frames>=0&&frames==checkpointed)))
            state.remember(store_deletion_stage::admin_work,SQLITE_BUSY);
        if(state.first_native||state.actual.first!=store_deletion_stage::none||
           state.actual.open!=SQLITE_OK||state.actual.work!=SQLITE_OK||state.actual.close!=SQLITE_OK||!ended)
            throw store_deletion_failure(state.actual,state.first_native);
        state.check();state.main_name(false);state.stub_name(false);
        // Publish a durable fence before ANY namespace removal. An interrupted
        // operation cannot be learned as an ordinary active/openable member.
        state.publish_once("deletion.v1",state.binding());state.marker=true;
        state.publish_once("deletion-checkpoint.v1",state.binding());state.checkpointed=true;
    }
    current=store_deletion_stage::filesystem;
    state.check();state.main_name(true);state.stub_name(true);
    // Full bounded census before the first unlink. Only actual SQLite sibling
    // names are considered; no extension substitution or recursive deletion.
    std::array<std::pair<std::string,std::optional<struct stat>>,4> files;
    std::size_t at=0;
    for(const auto* suffix:{"-wal","-shm","-journal",""}){
        auto name=state.name+suffix;struct stat actual{};std::optional<struct stat> captured;
        if(state.exists(name,actual)){
            if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||(actual.st_mode&0022)||
               (!*suffix&&id(actual)!=state.main_identity))resource_fail();captured=actual;
        }
        files[at++]={std::move(name),captured};
    }
    for(const auto& [name,captured]:files){
        state.check();if(!captured)continue;struct stat actual{};
        if(!state.exists(name,actual)||!same(*captured,actual)||state.creator!=pid())resource_fail();
        state.actual.namespace_attempted=true;
        if(::unlinkat(state.root->generation_fd.value,name.c_str(),0)){
            state.remember(store_deletion_stage::filesystem,errno);throw store_deletion_failure(state.actual,state.first_native);}
        state.issuer();state.actual.namespace_effect=true;
        if(::fsync(state.root->generation_fd.value)){
            state.remember(store_deletion_stage::filesystem,errno);throw store_deletion_failure(state.actual,state.first_native);}
    }
    state.check();state.stub_name(true);struct stat stub{};
    if(::fstatat(state.root->logical_fd.value,state.name.c_str(),&stub,AT_SYMLINK_NOFOLLOW)==0){
        if(id(stub)!=state.stub_identity||state.creator!=pid())resource_fail();
        state.actual.namespace_attempted=true;
        if(::unlinkat(state.root->logical_fd.value,state.name.c_str(),0)){
            state.remember(store_deletion_stage::filesystem,errno);throw store_deletion_failure(state.actual,state.first_native);}
        state.issuer();state.actual.namespace_effect=true;
        if(::fsync(state.root->logical_fd.value)){
            state.remember(store_deletion_stage::filesystem,errno);throw store_deletion_failure(state.actual,state.first_native);}
    }else if(errno!=ENOENT)resource_fail();
    // An absent retry name may follow a completed unlink whose parent fsync
    // failed. Establish both actual parent durability facts on EVERY valid
    // completion path before publishing deletion-complete.
    state.check();state.issuer();
    if(::fsync(state.root->generation_fd.value)||::fsync(state.root->logical_fd.value)){
        state.remember(store_deletion_stage::filesystem,errno);throw store_deletion_failure(state.actual,state.first_native);}
    state.check();
    for(const auto& [name,captured]:files){struct stat absent{};if(state.exists(name,absent))resource_fail();}
    state.stub_name(true);
    struct stat absent_stub{};
    if(::fstatat(state.root->logical_fd.value,state.name.c_str(),&absent_stub,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
    state.publish_once("deletion-complete.v1",state.binding());state.finished=true;
    // Locks remain held through this actual Core operation return/destruction.
    }catch(const store_deletion_failure&){throw;}
    catch(...){state.issuer();if(!state.first_native)state.first_native=std::current_exception();
        state.remember(current,0);throw store_deletion_failure(state.actual,state.first_native);}
#else
    resource_fail();
#endif
}
