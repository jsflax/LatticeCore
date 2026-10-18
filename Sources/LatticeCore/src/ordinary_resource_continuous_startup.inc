// Included in lattice::detail after the issuer's private helper namespace.
struct store_resource_continuous_startup::implementation final {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();const std::thread::id builder=std::this_thread::get_id();
    installation* installed=nullptr;resource_root* root=nullptr;
    std::string path,parent_path;file_identity parent_id;
    std::chrono::steady_clock::time_point end;
    descriptor parent;std::optional<resource_creation_original> original;
    bool claimed=false;
    implementation(installation& actual,resource_root& selected,std::string target,
        std::chrono::steady_clock::time_point deadline)
        :installed(&actual),root(&selected),path(std::move(target)),
         parent_path(path.substr(0,path.rfind('/'))),end(deadline) {
        parent_path=parent_path.substr(0,parent_path.rfind('/'));
    }
    void check(bool require_zero)const {
        if(creator!=pid()||builder!=std::this_thread::get_id()||!installed||!root||!original||
           original->creator!=creator||original->root!=root||resource_continuous_ending_debt.load(std::memory_order_acquire))resource_fail();
        bound(end);if(factory_installation(end)!=installed)resource_fail();require_runtime_code(*installed);core_code_current(*installed,end);
        root->check(end);actual_directory(parent.value,parent_path,parent_id,true);
        const auto& held=*original;const std::string name(held.name.data(),held.name_size);
        if(name!="store.sqlite"||path!=root->generation+"/"+name||held.record_size==0||held.record_size>held.record.size())resource_fail();
        const auto slot=digest_string(std::vector<std::uint8_t>(name.begin(),name.end()));
        struct stat folder{},main{},stub{},entry{},physical{},named{};
        if(::fstat(held.directory,&folder)||!S_ISDIR(folder.st_mode)||folder.st_uid!=::geteuid()||
           (folder.st_mode&07777)!=0700||id(folder)!=held.directory_id||
           ::fstatat(root->control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(folder,named)||
           ::fstat(held.entry,&entry)||id(entry)!=held.entry_id||
           ::fstatat(held.directory,"entry.lock",&named,AT_SYMLINK_NOFOLLOW)||!same(entry,named)||
           ::fstat(held.physical,&physical)||id(physical)!=held.physical_id||
           ::fstatat(held.directory,"physical.lock",&named,AT_SYMLINK_NOFOLLOW)||!same(physical,named))resource_fail();
        for(const auto* file:{&entry,&physical})if(!S_ISREG(file->st_mode)||file->st_uid!=::geteuid()||
            file->st_nlink!=1||file->st_size||(file->st_mode&07777)!=0600)resource_fail();
        if(::fstatat(root->generation_fd.value,name.c_str(),&main,AT_SYMLINK_NOFOLLOW)||!S_ISREG(main.st_mode)||
           main.st_uid!=::geteuid()||main.st_nlink!=1||(main.st_mode&07777)!=0600||id(main)!=held.main_id||
           (require_zero&&main.st_size)||::fstatat(root->logical_fd.value,name.c_str(),&stub,AT_SYMLINK_NOFOLLOW)||
           !S_ISREG(stub.st_mode)||stub.st_uid!=::geteuid()||stub.st_nlink!=1||stub.st_size||
           (stub.st_mode&07777)||id(stub)!=held.stub_id)resource_fail();
        const auto record=read_record(held.directory,"member.v1",end);
        if(record.size()!=held.record_size||!std::equal(record.begin(),record.end(),held.record.begin()))resource_fail();
        const auto state=parse(read_record(held.directory,"state.v1",end));
        if(text(state,"phase",32)!="active"||text(state,"epoch",64)!=root->epoch||identity_json(state.at("main"))!=held.main_id)resource_fail();
        if(::fstatat(held.directory,"deletion.v1",&named,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
        for(const auto* suffix:{"-wal","-shm","-journal"}) {
            if(::fstatat(root->generation_fd.value,(name+suffix).c_str(),&named,AT_SYMLINK_NOFOLLOW)==0) {
                if(require_zero||!S_ISREG(named.st_mode)||named.st_uid!=::geteuid()||named.st_nlink!=1||(named.st_mode&0022))resource_fail();
            }else if(errno!=ENOENT)resource_fail();
        }
        root->check(end);if(creator!=pid())resource_fail();
    }
    bool settle()noexcept {
        if(creator!=pid())return false;
        if(original&&!original->settle()){parent.release();return false;}
        original.reset();return resource_continuous_close(parent.value,creator);
    }
    ~implementation()noexcept{(void)settle();}
#endif
};
#ifdef LATTICE_RESOURCE_POSIX
namespace {
void resource_continuous_fd_budget(installation& installed,deadline end) {
#if defined(__APPLE__) && TARGET_OS_OSX
    if(installed.creator!=pid())resource_fail();bound(end);struct rlimit limit{};
    if(::getrlimit(RLIMIT_NOFILE,&limit))resource_fail();std::array<proc_fdinfo,1024> files{};
    const int n=::proc_pidinfo(::getpid(),PROC_PIDLISTFDS,0,files.data(),sizeof(files));
    if(n<=0||n%sizeof(proc_fdinfo)||static_cast<std::size_t>(n)>=sizeof(files))resource_fail();
    const auto count=static_cast<std::size_t>(n)/sizeof(proc_fdinfo);
    const auto capacity=std::min<std::uint64_t>(1024,static_cast<std::uint64_t>(limit.rlim_cur));
    std::size_t depth=0;
    for(const auto& image:installed.record.at("roles").at(installed.selected_role).at("codeImages")) {
        const auto path=text(image,"path");depth=std::max(depth,static_cast<std::size_t>(std::count(path.begin(),path.end(),'/')));
    }
    // One sequential token: member/entry/physical/parent4 + continuous dirs2;
    // actual resource/open/record validation scopes <=18, Native/VFS opening
    // scopes <=12, root/caller boundary slack12; image walker depth is separate.
    const auto headroom=48+depth;
    if(capacity<headroom||count>capacity-headroom)resource_fail();
    for(std::size_t at=0;at<count;++at)if(files[at].proc_fd>=capacity-headroom)resource_fail();
#else
    (void)installed;(void)end;resource_fail();
#endif
}
}
#endif
store_resource_continuous_startup::store_resource_continuous_startup(implementation* actual)noexcept:impl_(actual){}
store_resource_continuous_startup::~store_resource_continuous_startup()noexcept {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid()) {
        auto* held=std::exchange(impl_,nullptr);
        // Failed/unknown endings retain the capsule/remaining guards until
        // process exit. No destructor labels an uncertain close as complete.
        if(held->settle())delete held;
    }
#endif
}
store_resource_continuous_startup::store_resource_continuous_startup(store_resource_continuous_startup&& prior)noexcept
    :impl_(std::exchange(prior.impl_,nullptr)){}
store_resource_continuous_startup store_resource_continuous_startup::acquire_(const std::string& requested,deadline end) {
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();auto* installed=factory_installation(end);if(!installed)return {};
    auto [root,name]=select(*installed,requested,end);
    const auto container=root->logical.substr(root->logical.rfind('/')+1),generation=root->generation.substr(root->generation.rfind('/')+1);
    const json rule={{"names",json::array({"store.sqlite"})},{"suffixes",json::array()}};
    if(name!="store.sqlite"||!container.ends_with(".lattice-continuous")||container.size()==19||
       (!generation.ends_with(".lattice-continuous")||generation.size()<=19)||root->rule!=rule)resource_fail();
    require_runtime_code(*installed);core_code_current(*installed,end);resource_continuous_fd_budget(*installed,end);
    static_assert(sizeof(implementation)+8194<32768);
    auto* state=new implementation(*installed,*root,root->generation+"/"+name,end);
    if(creator!=pid())resource_fail();
    try {
        auto parent=absolute_directory(state->parent_path,true);state->parent_id=id(metadata(parent.value));
        state->parent.~descriptor();new(&state->parent)descriptor(std::move(parent));
        auto issued=issue_new_member(*root,name,end);
        if(creator!=pid())resource_fail();
        if(issued) {
            // Keep the ACTUAL creator's descriptions; convert EX to SH only
            // after all producer durability/identity checks have returned.
            lock(issued->physical,LOCK_SH,end);lock(issued->entry,LOCK_SH,end);
            state->original.emplace(std::move(*issued));
        }else {
            auto active=active_member(*root,name,end);auto physical=regular_control(active.directory.value,"physical.lock",active.physical_id);
            lock(physical.value,LOCK_SH,end);const auto raw=read_record(active.directory.value,"member.v1",end);const auto record=parse(raw);
            state->original.emplace(*root,name,resource_creation_original::source::active,
                std::move(active.directory),std::move(active.entry),std::move(physical),raw,
                identity_json(record.at("main")),identity_json(record.at("logicalStub")));
        }
        state->check(state->original->provenance!=resource_creation_original::source::active);
        return store_resource_continuous_startup(state);
    }catch(...) {if(creator==pid()&&state->settle())delete state;throw;}
#else
    (void)requested;(void)end;return {};
#endif
}
bool store_resource_continuous_startup::managed_()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator!=pid())resource_fail();
#endif
    return impl_!=nullptr;
}
const std::string& store_resource_continuous_startup::actual_path_()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid())resource_fail();impl_->check(false);return impl_->path;
#else
    resource_fail();
#endif
}
store_resource_continuous_startup::first_start store_resource_continuous_startup::claim_container_(
    const std::string& path,int parent,int container) {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||impl_->claimed||!impl_->original)resource_fail();auto& held=*impl_;
    const bool fresh=held.original->provenance!=resource_creation_original::source::active;
    held.check(fresh);
    if(path!=held.path||id(metadata(parent))!=held.parent_id||id(metadata(container))!=held.root->generation_id)resource_fail();
    held.claimed=true;
    if(held.original->provenance==resource_creation_original::source::initial)return first_start::created;
    if(held.original->provenance==resource_creation_original::source::recreated)return first_start::recreated;
    return first_start::existing;
#else
    (void)path;(void)parent;(void)container;resource_fail();
#endif
}
void store_resource_continuous_startup::before_native_open_()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_)return;
    if(impl_->creator!=pid()||!impl_->claimed)resource_fail();
    impl_->check(impl_->original->provenance!=resource_creation_original::source::active);
    resource_continuous_fd_budget(*impl_->installed,impl_->end);
#endif
}
void store_resource_continuous_startup::finish_native_open_(const std::string& path,std::uint64_t device,std::uint64_t inode) {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_)return;
    if(impl_->creator!=pid()||!impl_->claimed)resource_fail();auto* held=impl_;
    held->check(false);
    if(path!=held->path||held->original->main_id!=file_identity{device,inode})resource_fail();
    impl_=nullptr;
    if(!held->settle())resource_fail(); // unknown guard ending; capsule retained
    delete held;
#else
    (void)path;(void)device;(void)inode;
#endif
}
