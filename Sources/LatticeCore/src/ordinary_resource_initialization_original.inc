// Included only in the genuine normal issuer TU. The private payload owns the
// ACTUAL create/recreate descriptions, never reconstructed READY freshness.
struct store_resource_initialization_original::implementation final {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();const std::thread::id builder=std::this_thread::get_id();
    installation* installed;resource_root* root;
    std::string path,name,slot;const file_identity main_id;
    const std::chrono::steady_clock::time_point end;
    std::optional<resource_creation_original> original;
    const ::lattice::database* owner=nullptr;
    const observer_delivery_open_startup* open=nullptr;
    const store_physical_open_original* physical=nullptr;
    sqlite3* native=nullptr; // comparison only; actual Native original is separate
    bool attempted=false,ending_unknown=false;
    implementation(installation& selected,resource_root& actual,std::string target,
        const struct stat& expected,std::chrono::steady_clock::time_point deadline,
        std::optional<resource_creation_original>&& issued)
        :installed(&selected),root(&actual),path(std::move(target)),name(path.substr(path.rfind('/')+1)),
         slot(digest_string(std::vector<std::uint8_t>(name.begin(),name.end()))),main_id(id(expected)),end(deadline),original(std::move(issued)){}
    void check_before_native_()const {
        if(creator!=pid()||builder!=std::this_thread::get_id()||!installed||!root||ending_unknown||
           resource_continuous_ending_debt.load(std::memory_order_acquire))resource_fail();
        bound(end);if(factory_installation(end)!=installed)resource_fail();
        require_runtime_code(*installed);core_code_current(*installed,end);root->check(end);
        leaf(name);if(path!=root->generation+"/"+name)resource_fail();
        auto member=active_member(*root,name,end);
        if(id(member.main)!=main_id)resource_fail();
        if(original) {
            const auto& held=*original;
            if(held.creator!=creator||held.root!=root||held.provenance==resource_creation_original::source::active||
               name!=std::string_view(held.name.data(),held.name_size)||held.main_id!=main_id||
               held.directory_id!=member.member_identity||held.record_size==0||held.record_size>held.record.size()||
               member.main.st_size)resource_fail();
            const auto record=read_record(held.directory,"member.v1",end);
            if(record.size()!=held.record_size||!std::equal(record.begin(),record.end(),held.record.begin()))resource_fail();
            struct stat entry{},physical{},named{};
            if(::fstat(held.entry,&entry)||id(entry)!=held.entry_id||
               ::fstatat(held.directory,"entry.lock",&named,AT_SYMLINK_NOFOLLOW)||!same(entry,named)||
               ::fstat(held.physical,&physical)||id(physical)!=held.physical_id||
               ::fstatat(held.directory,"physical.lock",&named,AT_SYMLINK_NOFOLLOW)||!same(physical,named))resource_fail();
            for(const auto* file:{&entry,&physical})if(!S_ISREG(file->st_mode)||file->st_uid!=::geteuid()||
                file->st_nlink!=1||file->st_size||(file->st_mode&07777)!=0600)resource_fail();
        }
        root->check(end);if(creator!=pid())resource_fail();
    }
    bool settle()noexcept {
        if(creator!=pid()||ending_unknown)return false;
        if(original&&!original->settle()){ending_unknown=true;return false;}
        original.reset();return true;
    }
    ~implementation()noexcept{(void)settle();}
#endif
};
store_resource_initialization_original::store_resource_initialization_original(implementation* value,std::chrono::steady_clock::time_point end)noexcept
    :impl_(value),creator_(database_native_process()),builder_(std::this_thread::get_id()),end_(end){}
store_resource_initialization_original::store_resource_initialization_original(store_resource_initialization_original&& prior)noexcept
    :impl_(std::exchange(prior.impl_,nullptr)),creator_(std::exchange(prior.creator_,0)),builder_(prior.builder_),end_(prior.end_){}
store_resource_initialization_original::~store_resource_initialization_original()noexcept {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid()){auto* held=std::exchange(impl_,nullptr);if(held->settle())delete held;}
#endif
}
bool store_resource_initialization_original::transport_current_()const noexcept {
    if(creator_!=database_native_process()||builder_!=std::this_thread::get_id()||
       end_==std::chrono::steady_clock::time_point::max()||end_<=std::chrono::steady_clock::time_point{})return false;
#ifdef LATTICE_RESOURCE_POSIX
    return !impl_||(impl_->creator==creator_&&impl_->builder==builder_&&!impl_->ending_unknown);
#else
    return !impl_;
#endif
}
std::chrono::steady_clock::time_point store_resource_initialization_original::original_deadline_()const {
    if(!transport_current_())resource_fail();bound(end_);return end_;
}
void store_resource_initialization_original::transfer_to_(store_resource_initialization_original& recipient) {
    if(!transport_current_()||recipient.creator_||recipient.impl_)resource_fail();bound(end_);
    recipient.impl_=std::exchange(impl_,nullptr);recipient.creator_=std::exchange(creator_,0);
    recipient.builder_=builder_;recipient.end_=end_;
}
void store_resource_initialization_original::finish_unselected_open_(const ::lattice::database& actual,
    const store_physical_open_original& physical) {
    if(!actual.creator_current_()||!transport_current_())resource_fail();if(!impl_)return;
#ifdef LATTICE_RESOURCE_POSIX
    auto* held=impl_;
    if(!held->attempted||held->owner||held->open||held->physical||held->native||
       actual.delivery_startup_||!physical.matches_open_(actual.db_)||held->path!=actual.path_)resource_fail();
    if(!held->settle())resource_fail();impl_=nullptr;delete held;
#else
    (void)actual;(void)physical;resource_fail();
#endif
}
void store_resource_initialization_original::bind_open_(::lattice::database& actual,
    const observer_delivery_open_startup& open,const store_physical_open_original& physical) {
    if(!actual.creator_current_()||!transport_current_())resource_fail();if(!impl_)return;
#ifdef LATTICE_RESOURCE_POSIX
    auto& held=*impl_;
    if(!held.attempted||held.owner||held.open||held.physical||held.native||
       (held.original&&actual.mode_!=::lattice::database::open_mode::read_write)||actual.is_closed()||
       actual.delivery_startup_.get()!=&open||!open.pending_for_(actual)||
       !physical.matches_open_(actual.db_)||actual.raw_handle_escaped_.load(std::memory_order_acquire)||
       actual.path_!=held.path||open.original_deadline_()>held.end)resource_fail();
    // The exact VFS producer already checked its real opened MAIN against this
    // original's expectedstat, and bound this SAME preadmitted physical original.
    // No current-address lookup or fresh claim is synthesized at this seam.
    held.owner=&actual;held.open=&open;held.physical=&physical;held.native=actual.db_;
#else
    (void)actual;(void)open;(void)physical;resource_fail();
#endif
}
namespace {
class normal_creation_identity_error final:public std::exception {
    int code_,error_;
public:
    normal_creation_identity_error(int code,int error)noexcept:code_(code),error_(error){}
    const char* what()const noexcept override{return "actual normal creation identity observation failed";}
    int actual_code()const noexcept{return code_;}int actual_errno()const noexcept{return error_;}
};
}
const store_resource_initialization_original* store_resource_initialization_original::fresh_for_(
    const ::lattice::database& actual,const observer_delivery_open_startup& open)const {
#ifdef LATTICE_RESOURCE_POSIX
    if(!actual.creator_current_())resource_fail();if(!impl_)return nullptr;
    if(!transport_current_())resource_fail();const auto& held=*impl_;
    if(held.owner!=&actual||held.open!=&open||held.physical!=&actual.physical_open_original_||
       held.native!=actual.db_||!held.physical->matches_open_(held.native)||
       &actual.normal_initialization_original_!=this||actual.delivery_startup_.get()!=&open||
       !open.pending_for_(actual)||actual.is_closed()||actual.raw_handle_escaped_.load(std::memory_order_acquire)||
       std::chrono::steady_clock::now()>=held.end||resource_continuous_ending_debt.load(std::memory_order_acquire))resource_fail();
    if(!held.original)return nullptr;const auto& original=*held.original;
    if(original.creator!=held.creator||original.root!=held.root||
       original.provenance==resource_creation_original::source::active||original.main_id!=held.main_id)resource_fail();
    // Revalidation in the real WRITE uses fixed, prospectively charged stat
    // storage and retained actual descriptions only: no Image walk, lock wait,
    // record/parser allocation, profile issuance or renewed deadline here.
    struct observations {struct stat logical{},generation{},control{},member{},entry{},physical{},main{},stub{},named{};};
    open.claim_workspace_(const_cast<::lattice::database&>(actual),sizeof(observations));observations data;
    const auto observe=[](int rc){const int error=errno;if(rc)throw normal_creation_identity_error(rc,error);};
    observe(::fstat(held.root->logical_fd.value,&data.logical));observe(::fstat(held.root->generation_fd.value,&data.generation));
    observe(::fstat(held.root->control_fd.value,&data.control));observe(::fstat(original.directory,&data.member));
    observe(::fstat(original.entry,&data.entry));observe(::fstat(original.physical,&data.physical));
    observe(::fstatat(held.root->generation_fd.value,held.name.c_str(),&data.main,AT_SYMLINK_NOFOLLOW));
    observe(::fstatat(held.root->logical_fd.value,held.name.c_str(),&data.stub,AT_SYMLINK_NOFOLLOW));
    observe(::lstat(held.root->logical.c_str(),&data.named));if(!same(data.logical,data.named))resource_fail();
    observe(::lstat(held.root->generation.c_str(),&data.named));if(!same(data.generation,data.named))resource_fail();
    observe(::lstat(held.root->control.c_str(),&data.named));if(!same(data.control,data.named))resource_fail();
    observe(::fstatat(held.root->control_fd.value,held.slot.c_str(),&data.named,AT_SYMLINK_NOFOLLOW));if(!same(data.member,data.named))resource_fail();
    if(!S_ISDIR(data.logical.st_mode)||id(data.logical)!=held.root->logical_id||!S_ISDIR(data.generation.st_mode)||
       id(data.generation)!=held.root->generation_id||!S_ISDIR(data.control.st_mode)||id(data.control)!=held.root->control_id||
       !S_ISDIR(data.member.st_mode)||data.member.st_uid!=::geteuid()||(data.member.st_mode&07777)!=0700||id(data.member)!=original.directory_id||
       !S_ISREG(data.entry.st_mode)||id(data.entry)!=original.entry_id||!S_ISREG(data.physical.st_mode)||id(data.physical)!=original.physical_id||
       !S_ISREG(data.main.st_mode)||data.main.st_uid!=::geteuid()||data.main.st_nlink!=1||(data.main.st_mode&07777)!=0600||id(data.main)!=original.main_id||
       !S_ISREG(data.stub.st_mode)||data.stub.st_uid!=::geteuid()||data.stub.st_nlink!=1||data.stub.st_size||(data.stub.st_mode&07777)||id(data.stub)!=original.stub_id)
        resource_fail();
    for(const auto* directory:{&data.generation,&data.control})if(directory->st_uid!=::geteuid()||(directory->st_mode&07777)!=0700)resource_fail();
    for(const auto* file:{&data.entry,&data.physical})if(file->st_uid!=::geteuid()||file->st_nlink!=1||file->st_size||(file->st_mode&07777)!=0600)resource_fail();
    observe(::fstatat(original.directory,"entry.lock",&data.named,AT_SYMLINK_NOFOLLOW));if(!same(data.entry,data.named))resource_fail();
    observe(::fstatat(original.directory,"physical.lock",&data.named,AT_SYMLINK_NOFOLLOW));if(!same(data.physical,data.named))resource_fail();
    if(std::chrono::steady_clock::now()>=held.end||held.creator!=pid())resource_fail();
    return this;
#else
    (void)actual;(void)open;return nullptr;
#endif
}
void store_resource_initialization_original::finish_initialization_(const ::lattice::database& actual,
    const observer_delivery_open_startup& open) {
#ifdef LATTICE_RESOURCE_POSIX
    if(fresh_for_(actual,open)!=this)resource_fail();auto* held=impl_;
    // Caller invokes only after its actual known successful catalog COMMIT.
    // The independently retained Native/VFS physical original remains owner.
    if(!held->settle())resource_fail();impl_=nullptr;delete held;
#else
    (void)actual;(void)open;resource_fail();
#endif
}
