#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "store_physical_vfs.hpp"
#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <new>

namespace lattice::detail {
struct store_physical_vfs::wrapper {
    sqlite3_file file{};
    store_physical_vfs* owner=nullptr;
    store_physical_lifetime::pin held;
    store_physical_lifetime::file_original issued;
    std::array<store_physical_lifetime::pin,maximum_members-1> shared_pins{};
    std::array<store_physical_lifetime::file_original,maximum_members-1> shared_files{};
    std::size_t shared_count=0;
    sqlite3_file* inner=nullptr;
    bool main=false,close_entered=false;
};
namespace {
#ifdef SQLITE_OPEN_NOFOLLOW
constexpr int physical_no_follow_flag=SQLITE_OPEN_NOFOLLOW;
#else
constexpr int physical_no_follow_flag=0; // constructor refuses before registration
#endif
#ifdef SQLITE_OPEN_SUPER_JOURNAL
constexpr int physical_super_journal_flag=SQLITE_OPEN_SUPER_JOURNAL;
#else
constexpr int physical_super_journal_flag=SQLITE_OPEN_MASTER_JOURNAL;
#endif
// SQLite allocates szOsFile bytes; no allocation is performed by xOpen.
constexpr std::size_t aligned_prefix(std::size_t size)noexcept {
    return (size+alignof(sqlite3_file)-1)&~(alignof(sqlite3_file)-1);
}
}
bool store_physical_vfs::current_()const noexcept {
    return creator_==database_native_process()&&held_.current();
}
store_physical_lifetime::api_return* store_physical_vfs::vfs_returns::at_(std::size_t i)noexcept {
    return std::launder(reinterpret_cast<store_physical_lifetime::api_return*>(storage_+i*sizeof(store_physical_lifetime::api_return)));
}
store_physical_vfs::vfs_returns::vfs_returns(const wrapper& actual,bool closing)noexcept {
    const auto enter=[&](const store_physical_lifetime::pin& pin){
        auto* call=::new(static_cast<void*>(storage_+count_*sizeof(store_physical_lifetime::api_return)))
            store_physical_lifetime::api_return(pin,true,closing);
        ++count_;if(!call->entered())entered_=false;
    };
    enter(actual.held);
    for(std::size_t i=0;i<actual.shared_count&&entered_;++i)enter(actual.shared_pins[i]);
}
store_physical_vfs::vfs_returns::~vfs_returns()noexcept {while(count_)at_(--count_)->~api_return();}
bool store_physical_vfs::capture_shared_(wrapper& actual)noexcept {
    if(!current_()||root_!=this||actual.owner!=this)return false;
    try{std::lock_guard lock(members_leaf_);
        for(auto* member:members_)if(member&&member->attach_entered_){
            if(actual.shared_count==actual.shared_pins.size()||!member->current_())return false;
            actual.shared_pins[actual.shared_count]=store_physical_lifetime::pin(member->held_);
            if(!actual.shared_pins[actual.shared_count].current())return false;
            ++actual.shared_count;
        }
        return true;
    }catch(...){return false;}
}
store_physical_vfs::store_physical_vfs(const store_physical_lifetime::pin& held,sqlite3_vfs* lower,
    std::string path,const struct stat& expected,std::uint64_t creator)
    :creator_(creator),held_(held),lower_(lower),path_(std::move(path)),
     wal_(path_+"-wal"),journal_(path_+"-journal"),
     name_("lattice-physical-"+std::to_string(creator_)+"-"+std::to_string(reinterpret_cast<std::uintptr_t>(this))),
     expected_(expected) {
    static_assert(alignof(wrapper)<=alignof(sqlite3_file));
    if(!current_()||!lower_||lower_->iVersion<1||!lower_->zName||
       lower_->szOsFile<int(sizeof(sqlite3_file))||
       std::size_t(lower_->szOsFile)>std::size_t(std::numeric_limits<int>::max())-aligned_prefix(sizeof(wrapper))||
       !S_ISREG(expected_.st_mode)||expected_.st_nlink!=1||path_.empty()||path_.find('\0')!=std::string::npos)
        throw db_error("physical VFS requires its actual issued file original");
    const std::filesystem::path selected(path_);
    if(!selected.is_absolute()||selected.lexically_normal()!=selected)
        throw db_error("physical VFS requires issued absolute filename");
#ifndef SQLITE_OPEN_NOFOLLOW
    throw db_error("physical VFS requires supported native no-follow open");
#endif
    vfs_=*lower_;vfs_.pNext=nullptr;vfs_.pAppData=this;vfs_.zName=name_.c_str();
    vfs_.szOsFile=int(aligned_prefix(sizeof(wrapper)))+lower_->szOsFile;
    vfs_.iVersion=std::min(lower_->iVersion,3);vfs_.xOpen=open_file_;
    install_forwarders_();
}
int store_physical_vfs::open_(const store_physical_lifetime::pin& held,sqlite3_vfs* lower,
    const std::string& path,const struct stat& expected,sqlite3** actual,int flags,store_physical_open_original* issued) {
    if(!actual||!held.current())throw db_error("physical open lacks its issued original");
    *actual=nullptr;
    if(issued&&!store_physical_api::can_issue_open_(*issued))return SQLITE_MISUSE;
    const auto creator=database_native_process();
    auto* owner=new store_physical_vfs(held,lower,path,expected,creator);
    // No inherited Native/registry call or automatic owner destruction after
    // an allocator crossed fork. The child abandons this preadmitted capsule.
    if(!owner->current_())throw db_error("physical VFS creator changed during allocation");
    try{owner->original_=store_physical_api::prepare_(held,owner,complete_);}
    catch(...){if(owner->current_())delete owner;throw;}
    if(!owner->current_())throw db_error("physical VFS creator changed during original admission");
    if(issued&&!store_physical_api::issue_open_(*issued,owner->original_))
        throw db_error("physical VFS owning producer admission refused");
    const int registered=sqlite3_vfs_register(&owner->vfs_,0);
    if(!owner->current_())throw db_error("physical VFS creator changed during registration");
    if(registered!=SQLITE_OK)throw db_error("physical VFS registration unproved"); // retain unknown registration/lease capsule
    owner->registered_=true;
    auto original=store_physical_api::capture_issued_(owner->original_);
    if(original.inherited||!original.value)throw db_error("physical VFS original pin unavailable"); // retain registration debt
    int result=SQLITE_MISUSE;
    {
        store_physical_lifetime::api_return returned(held);
        if(!returned.entered())throw db_error("physical open API pin unavailable");
        store_physical_lifetime::native_open_entered_(held);
        result=sqlite3_open_v2(path.c_str(),actual,flags|physical_no_follow_flag,owner->name_.c_str());
        if(!owner->current_()){*actual=nullptr;throw db_error("physical open returned in inherited process");}
        // Publish the genuine open return even on failure/partial handle, so
        // subsequent owned cleanup pins THIS original before Native entry.
        owner->native_=*actual;
        const bool transported=!issued||store_physical_api::open_returned_(*issued,owner->original_,*actual);
        const bool published=store_physical_api::publish_(owner->original_,*actual,owner,complete_);
        store_physical_lifetime::native_open_returned_(held,*actual!=nullptr);
        if(!transported||!published){
            // The actual owning record remains the cleanup original even if
            // registry publication failed. Unknown publication is permanent
            // physical debt; successful partial cleanup cannot erase it.
            store_physical_lifetime::cleanup_returned_(held,SQLITE_MISUSE);
            throw db_error("physical open registration publication unproved");
        }
        if(result==SQLITE_OK){
            sqlite3_file* main=nullptr;
            if(!*actual||sqlite3_file_control(*actual,"main",SQLITE_FCNTL_FILE_POINTER,&main)!=SQLITE_OK||!main||
               (main->pMethods!=&methods_(1)&&main->pMethods!=&methods_(2)&&main->pMethods!=&methods_(3))||
               reinterpret_cast<wrapper*>(main)->owner!=owner||!reinterpret_cast<wrapper*>(main)->main)
                result=SQLITE_CANTOPEN;
        }
    } // C API returned, including its WAL/delete tail, before possible completion.
    return result;
}
void store_physical_vfs::complete_(void* cookie)noexcept {
    auto* member=static_cast<store_physical_vfs*>(cookie);
    if(!member||!member->current_()||!store_physical_lifetime::observe_(member->held_).physical_ended)return;
    auto* root=member->root_;if(!root||!root->current_())return;
    bool last=false,child=member!=root;
    try{
        {std::lock_guard lock(root->members_leaf_);
         if(!root->originals_)return;
         if(child){
             auto at=std::find(root->members_.begin(),root->members_.end(),member);
             if(at==root->members_.end())return;*at=nullptr;
         }else{if(root->primary_ended_)return;root->primary_ended_=true;root->sealed_=true;}
         --root->originals_;last=root->originals_==0&&root->primary_ended_;}
        // No member/registry/Native leaf remains. A child has its own physical
        // original; retiring it must never unregister the live MAIN's VFS.
        if(child)delete member;
        if(!last)return;
        if(root->registered_){
            const int rc=sqlite3_vfs_unregister(&root->vfs_);
            if(!root->current_()||rc!=SQLITE_OK)return; // retain registration debt
            root->registered_=false;
        }
        delete root;
    }catch(...){/* Keep unproved registration/member ending custody. */}
}
store_physical_vfs* store_physical_vfs::selected_(const char* name,bool main,bool wal,bool journal)noexcept {
    if(!current_()||root_!=this)return nullptr;
    try{std::lock_guard lock(members_leaf_);
        const auto matches=[&](store_physical_vfs* member){
            return member&&name&&((main&&member->path_==name)||(wal&&member->wal_==name)||(journal&&member->journal_==name));};
        if(matches(this))return this;
        for(auto* member:members_)if(matches(member)){
            if(member->attach_entered_)return member;
            return nullptr; // reservation was not entered by its actual producer
        }
        return !main&&!wal&&!journal?this:nullptr;
    }catch(...){return nullptr;}
}
store_physical_vfs* store_physical_vfs::enroll_(sqlite3* actual,const store_physical_lifetime::pin& held,
    const std::string& path,const struct stat& expected,const std::string& alias,std::int64_t token,int attached_limit){
    if(!current_()||root_!=this||!actual||actual!=native_||!held.current()||token<=0||alias.empty()||
       alias.find('\0')!=std::string::npos||path==path_)throw db_error("resource attachment requires distinct actual target");
    // The genuine producer read this actual connection limit inside its
    // bounded Native attachment_sql_scope, before any resource acquisition.
    // No unbounded raw SQLite mutex wait is introduced by this VFS metadata.
    // The fixed API frame set reserves one slot for MAIN.
    if(attached_limit<0||attached_limit>=static_cast<int>(maximum_members))
        throw db_error("resource attachment limit exceeds its actual original frame set");
    const auto creator=creator_;
    auto* member=new store_physical_vfs(held,lower_,path,expected,creator);
    if(!current_()||!member->current_())throw db_error("resource attachment allocation changed creator");
    try{
        member->root_=this;member->alias_=alias;member->token_=token;member->native_=actual;
        member->original_=store_physical_api::prepare_(held,member,complete_);
    }catch(...){if(current_())delete member;throw;}
    if(!current_())throw db_error("resource attachment admission changed creator");
    const auto primary_state=store_physical_lifetime::observe_(held_);
    std::size_t slot=maximum_members;
    {std::lock_guard lock(members_leaf_);
        if(!sealed_&&!primary_ended_&&primary_state.current&&!primary_state.connection_retired&&!primary_state.unknown){
            bool duplicate=false;
            for(auto* at:members_)if(at&&(at->path_==path||at->alias_==alias))duplicate=true;
            if(!duplicate){for(std::size_t i=0;i<members_.size();++i)if(!members_[i]){slot=i;break;}
                if(slot<members_.size()){members_[slot]=member;++originals_;}}
        }
    }
    if(slot>=members_.size()){
        store_physical_api::abandon_unpublished_(member->original_);member->original_=nullptr;delete member;
        throw db_error("resource attachment current member capacity or identity refused");
    }
    // Publication failure keeps this exact reserved capsule/lease. It must not
    // disappear from a later whole-connection close census as fabricated clean.
    if(!store_physical_api::publish_member_(member->original_,actual,member,complete_))
        throw db_error("resource attachment original publication unproved");
    return member;
}
store_physical_vfs* store_physical_vfs::find_member_(sqlite3* actual,const std::string& path,
    const std::string& alias,std::int64_t token){
    if(!current_()||root_!=this||actual!=native_||token<=0)throw db_error("resource detach actual recipient changed");
    std::lock_guard lock(members_leaf_);
    if(sealed_||primary_ended_)throw db_error("resource detach recipient retired");
    for(auto* member:members_)if(member&&member->path_==path&&member->alias_==alias&&member->token_==token&&member->attach_returned_)
        return member;
    throw db_error("resource detach original membership unavailable");
}
bool store_physical_vfs::member_entered_(bool detach)noexcept {
    if(!current_()||root_==this||!root_||!root_->current_())return false;
    try{std::lock_guard lock(root_->members_leaf_);
        if(root_->sealed_||root_->primary_ended_)return false;
        if(detach){if(!attach_returned_||detach_active_)return false;detach_active_=true;}
        else{if(attach_entered_||attach_returned_)return false;attach_entered_=true;}
    }catch(...){return false;}
    if(!detach)store_physical_lifetime::attachment_entered_(held_);
    return true;
}
bool store_physical_vfs::member_returned_(bool detach,bool entered,bool clean)noexcept {
    if(!current_()||root_==this||!root_||!root_->current_())return false;
    try{
        {std::lock_guard lock(root_->members_leaf_);
         if(detach){if(!entered||!detach_active_)return false;detach_active_=false;}
         else{if(attach_returned_)return false;attach_returned_=true;}}
        if(detach)store_physical_lifetime::detachment_returned_(held_,clean);
        else store_physical_lifetime::attachment_returned_(held_,entered,clean);
        const auto actual=store_physical_lifetime::observe_(held_);
        return actual.current&&!actual.unknown&&(!clean||(detach?actual.detach_returned:actual.attach_returned));
    }catch(...){store_physical_lifetime::cleanup_returned_(held_,SQLITE_IOERR);return false;}
}
void store_physical_vfs::seal_()noexcept {
    if(!current_()||root_!=this)return;
    try{std::lock_guard lock(members_leaf_);sealed_=true;}catch(...){store_physical_lifetime::cleanup_returned_(held_,SQLITE_IOERR);}
}
int store_physical_vfs::open_file_(sqlite3_vfs* native,const char* name,sqlite3_file* file,int flags,int* out)noexcept {
    auto* root=static_cast<store_physical_vfs*>(native->pAppData);file->pMethods=nullptr;
    if(!root||!root->current_())return SQLITE_CANTOPEN;
    const bool main=(flags&SQLITE_OPEN_MAIN_DB)!=0,wal=(flags&SQLITE_OPEN_WAL)!=0;
    const bool journal=(flags&SQLITE_OPEN_MAIN_JOURNAL)!=0;
    const bool shared=!(main||wal||journal); // includes actual SQLite super-journal/temporary files
    auto* owner=root->selected_(name,main,wal,journal);
    if(!owner||!owner->current_())return SQLITE_CANTOPEN;
    auto* wrapped=::new(static_cast<void*>(file))wrapper{};
    wrapped->owner=owner;wrapped->held=store_physical_lifetime::pin(owner->held_);wrapped->main=main;
    // Shared commit/temporary files are one real VFS file with custody in every
    // current participating store original. No target lease may retire while
    // that file or its actual return tail still exists, regardless of mode.
    if(shared&&!root->capture_shared_(*wrapped)){wrapped->~wrapper();return SQLITE_CANTOPEN;}
    wrapped->inner=reinterpret_cast<sqlite3_file*>(reinterpret_cast<unsigned char*>(file)+aligned_prefix(sizeof(wrapper)));
    std::memset(wrapped->inner,0,std::size_t(owner->lower_->szOsFile));
    const auto kind=main?store_physical_lifetime::file_kind::main:wal?store_physical_lifetime::file_kind::wal:
        journal?store_physical_lifetime::file_kind::journal:store_physical_lifetime::file_kind::temporary;
    wrapped->issued=store_physical_lifetime::file_open_entered_(wrapped->held,kind);
    if(!wrapped->issued.serial_){wrapped->~wrapper();return SQLITE_CANTOPEN;}
    std::size_t admitted=0;
    for(;admitted<wrapped->shared_count;++admitted){
        wrapped->shared_files[admitted]=store_physical_lifetime::file_open_entered_(wrapped->shared_pins[admitted],kind);
        if(!wrapped->shared_files[admitted].serial_)break;
    }
    if(admitted!=wrapped->shared_count){
        store_physical_lifetime::file_open_returned_(wrapped->held,wrapped->issued,false);
        for(std::size_t i=0;i<admitted;++i)store_physical_lifetime::file_open_returned_(wrapped->shared_pins[i],wrapped->shared_files[i],false);
        wrapped->~wrapper();return SQLITE_CANTOPEN;
    }
    vfs_returns call(*wrapped);
    if(!call.entered()){
        store_physical_lifetime::file_open_returned_(wrapped->held,wrapped->issued,false);
        for(std::size_t i=0;i<wrapped->shared_count;++i)store_physical_lifetime::file_open_returned_(wrapped->shared_pins[i],wrapped->shared_files[i],false);
        wrapped->~wrapper();return SQLITE_CANTOPEN;
    }
    int rc=owner->lower_->xOpen(owner->lower_,name,wrapped->inner,flags|physical_no_follow_flag,out);
    if(!owner->current_())return SQLITE_CANTOPEN; // inherited capsule abandoned
    const bool methods=wrapped->inner->pMethods!=nullptr;
    if(rc==SQLITE_OK&&!methods){
        // Invalid success cannot prove the lower resource absent. Retain the
        // physical lease as cleanup debt even if no close method was supplied.
        store_physical_lifetime::cleanup_returned_(wrapped->held,SQLITE_IOERR);
        for(std::size_t i=0;i<wrapped->shared_count;++i)store_physical_lifetime::cleanup_returned_(wrapped->shared_pins[i],SQLITE_IOERR);
        rc=SQLITE_CANTOPEN;
    }
    if(rc==SQLITE_OK&&main){
        struct stat named{};int moved=1;
        if(::lstat(name,&named)!=0||!S_ISREG(named.st_mode)||named.st_nlink!=1||
           named.st_dev!=owner->expected_.st_dev||named.st_ino!=owner->expected_.st_ino||
           named.st_uid!=owner->expected_.st_uid||
           wrapped->inner->pMethods->xFileControl(wrapped->inner,SQLITE_FCNTL_HAS_MOVED,&moved)!=SQLITE_OK||moved)
            rc=SQLITE_CANTOPEN;
    }
    if(rc==SQLITE_OK&&methods&&(flags&physical_super_journal_flag)){
        bool recorded=false;
        try{std::lock_guard lock(root->members_leaf_);
            if(root->super_journal_opens_!=UINT64_MAX){++root->super_journal_opens_;recorded=true;}
        }catch(...){}
        if(!recorded){store_physical_lifetime::cleanup_returned_(wrapped->held,SQLITE_IOERR);rc=SQLITE_CANTOPEN;}
    }
    store_physical_lifetime::file_open_returned_(wrapped->held,wrapped->issued,methods);
    for(std::size_t i=0;i<wrapped->shared_count;++i)store_physical_lifetime::file_open_returned_(wrapped->shared_pins[i],wrapped->shared_files[i],methods);
    if(methods)wrapped->file.pMethods=&methods_(std::min(wrapped->inner->pMethods->iVersion,3));
    else wrapped->~wrapper();
    return rc;
}
int store_physical_vfs::close_file_(sqlite3_file* file)noexcept {
    auto* wrapped=reinterpret_cast<wrapper*>(file);
    if(!wrapped->owner||!wrapped->owner->current_()||wrapped->close_entered)return SQLITE_IOERR_CLOSE;
    vfs_returns call(*wrapped,true);
    if(!call.entered())return SQLITE_IOERR_CLOSE;
    wrapped->close_entered=true;
    const int rc=wrapped->inner->pMethods->xClose(wrapped->inner);
    if(!wrapped->owner->current_())return rc; // no inherited record/leaf/destruction
    store_physical_lifetime::file_close_returned_(wrapped->held,wrapped->issued,rc);
    for(std::size_t i=0;i<wrapped->shared_count;++i)store_physical_lifetime::file_close_returned_(wrapped->shared_pins[i],wrapped->shared_files[i],rc);
    wrapped->file.pMethods=nullptr;wrapped->~wrapper();
    return rc;
}
bool store_physical_vfs::identity_layer_(sqlite3_file* file,sqlite3_vfs* observed,
    sqlite3_file*& inner,sqlite3_vfs*& lower)const noexcept {
    if(!current_()||root_!=this||observed!=&vfs_||!file||
       (file->pMethods!=&methods_(1)&&file->pMethods!=&methods_(2)&&file->pMethods!=&methods_(3)))return false;
    const auto* wrapped=reinterpret_cast<const wrapper*>(file);
    if(!wrapped->owner||!wrapped->owner->current_()||wrapped->owner->root_!=this||!wrapped->main||
       wrapped->close_entered||!wrapped->issued.returned_||!wrapped->issued.methods_||wrapped->issued.closed_)return false;
    // Membership is an actual issued pointer in this connection's live roster,
    // never a matching filename or caller-provided VFS name.
    try{std::lock_guard lock(members_leaf_);
        if(wrapped->owner!=this&&std::find(members_.begin(),members_.end(),wrapped->owner)==members_.end())return false;
    }catch(...){return false;}
    inner=wrapped->inner;lower=lower_;return true;
}
const sqlite3_io_methods& store_physical_vfs::methods_(int version)noexcept {
    static const std::array<sqlite3_io_methods,3> all=[] {
        std::array<sqlite3_io_methods,3> output{};
        for(int i=0;i<3;++i){auto& m=output[i];m.iVersion=i+1;m.xClose=close_file_;
            m.xRead=[](sqlite3_file* f,void* p,int n,sqlite3_int64 at)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xRead(w->inner,p,n,at):SQLITE_IOERR_READ;};
            m.xWrite=[](sqlite3_file* f,const void* p,int n,sqlite3_int64 at)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xWrite(w->inner,p,n,at):SQLITE_IOERR_WRITE;};
            m.xTruncate=[](sqlite3_file* f,sqlite3_int64 n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xTruncate(w->inner,n):SQLITE_IOERR_TRUNCATE;};
            m.xSync=[](sqlite3_file* f,int n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xSync(w->inner,n):SQLITE_IOERR_FSYNC;};
            m.xFileSize=[](sqlite3_file* f,sqlite3_int64* n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xFileSize(w->inner,n):SQLITE_IOERR_FSTAT;};
            m.xLock=[](sqlite3_file* f,int n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xLock(w->inner,n):SQLITE_IOERR_LOCK;};
            m.xUnlock=[](sqlite3_file* f,int n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xUnlock(w->inner,n):SQLITE_IOERR_UNLOCK;};
            m.xCheckReservedLock=[](sqlite3_file* f,int* n)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xCheckReservedLock(w->inner,n):SQLITE_IOERR_CHECKRESERVEDLOCK;};
            m.xFileControl=[](sqlite3_file* f,int op,void* arg)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_())return SQLITE_IOERR;
                if(op==SQLITE_FCNTL_VFS_POINTER){if(!arg)return SQLITE_MISUSE;*static_cast<sqlite3_vfs**>(arg)=&w->owner->root_->vfs_;return SQLITE_OK;}
                return w->inner->pMethods->xFileControl(w->inner,op,arg);};
            m.xSectorSize=[](sqlite3_file* f)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xSectorSize(w->inner):0;};
            m.xDeviceCharacteristics=[](sqlite3_file* f)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xDeviceCharacteristics(w->inner):0;};
            if(i>=1){
                m.xShmMap=[](sqlite3_file* f,int p,int n,int x,void volatile** out)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_()||!w->inner->pMethods->xShmMap)return SQLITE_IOERR_SHMMAP;
                    const int rc=w->inner->pMethods->xShmMap(w->inner,p,n,x,out);if(!w->owner->current_())return rc;
                    store_physical_lifetime::shm_map_returned_(w->held,w->issued,rc==SQLITE_OK&&out&&*out,rc);for(std::size_t j=0;j<w->shared_count;++j)store_physical_lifetime::shm_map_returned_(w->shared_pins[j],w->shared_files[j],rc==SQLITE_OK&&out&&*out,rc);return rc;};
                m.xShmLock=[](sqlite3_file* f,int p,int n,int x)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()&&w->inner->pMethods->xShmLock?w->inner->pMethods->xShmLock(w->inner,p,n,x):SQLITE_IOERR_SHMLOCK;};
                m.xShmBarrier=[](sqlite3_file* f){auto* w=reinterpret_cast<wrapper*>(f);if(w->owner->current_()&&w->inner->pMethods->xShmBarrier)w->inner->pMethods->xShmBarrier(w->inner);};
                m.xShmUnmap=[](sqlite3_file* f,int x)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_()||!w->inner->pMethods->xShmUnmap)return SQLITE_IOERR_SHMOPEN;
                    const int rc=w->inner->pMethods->xShmUnmap(w->inner,x);if(!w->owner->current_())return rc;
                    store_physical_lifetime::shm_unmap_returned_(w->held,w->issued,rc);for(std::size_t j=0;j<w->shared_count;++j)store_physical_lifetime::shm_unmap_returned_(w->shared_pins[j],w->shared_files[j],rc);return rc;};
            }
            if(i>=2){
                m.xFetch=[](sqlite3_file* f,sqlite3_int64 o,int n,void** out)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_())return SQLITE_IOERR_MMAP;
                    if(!w->inner->pMethods->xFetch){if(out)*out=nullptr;return SQLITE_OK;}
                    const int rc=w->inner->pMethods->xFetch(w->inner,o,n,out);if(!w->owner->current_())return rc;
                    store_physical_lifetime::fetch_returned_(w->held,w->issued,rc==SQLITE_OK&&out&&*out,rc);for(std::size_t j=0;j<w->shared_count;++j)store_physical_lifetime::fetch_returned_(w->shared_pins[j],w->shared_files[j],rc==SQLITE_OK&&out&&*out,rc);return rc;};
                m.xUnfetch=[](sqlite3_file* f,sqlite3_int64 o,void* p)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_())return SQLITE_IOERR_MMAP;
                    if(!w->inner->pMethods->xUnfetch)return SQLITE_OK;
                    const int rc=w->inner->pMethods->xUnfetch(w->inner,o,p);if(!w->owner->current_())return rc;
                    store_physical_lifetime::unfetch_returned_(w->held,w->issued,p==nullptr,rc);for(std::size_t j=0;j<w->shared_count;++j)store_physical_lifetime::unfetch_returned_(w->shared_pins[j],w->shared_files[j],p==nullptr,rc);return rc;};
            }
        }return output;
    }();
    return all[version<1?0:version>3?2:version-1];
}
void store_physical_vfs::install_forwarders_()noexcept {
    vfs_.xDelete=[](sqlite3_vfs* v,const char* p,int sync)->int {auto* root=static_cast<store_physical_vfs*>(v->pAppData);if(!root->current_())return SQLITE_IOERR_DELETE;
        auto* o=root->selected_(p,true,true,true);const bool shared=o==nullptr;if(!o)o=root;
        wrapper actual;actual.owner=o;actual.held=store_physical_lifetime::pin(o->held_);
        if(shared&&!root->capture_shared_(actual))return SQLITE_IOERR_DELETE;
        vfs_returns call(actual,true);if(!call.entered())return SQLITE_IOERR_DELETE;
        const int rc=o->lower_->xDelete(o->lower_,p,sync);if(!o->current_())return rc;
        store_physical_lifetime::cleanup_returned_(o->held_,rc);
        for(std::size_t j=0;j<actual.shared_count;++j)store_physical_lifetime::cleanup_returned_(actual.shared_pins[j],rc);return rc;};
    vfs_.xAccess=[](sqlite3_vfs* v,const char* p,int flags,int* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xAccess(o->lower_,p,flags,out):SQLITE_IOERR_ACCESS;};
    vfs_.xFullPathname=[](sqlite3_vfs* v,const char* p,int n,char* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xFullPathname(o->lower_,p,n,out):SQLITE_CANTOPEN;};
    vfs_.xDlOpen=[](sqlite3_vfs* v,const char* p)->void* {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()&&o->lower_->xDlOpen?o->lower_->xDlOpen(o->lower_,p):nullptr;};
    vfs_.xDlError=[](sqlite3_vfs* v,int n,char* out){auto* o=static_cast<store_physical_vfs*>(v->pAppData);if(o->current_()&&o->lower_->xDlError)o->lower_->xDlError(o->lower_,n,out);};
    vfs_.xDlSym=[](sqlite3_vfs* v,void* h,const char* p)->void(*)(void) {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()&&o->lower_->xDlSym?o->lower_->xDlSym(o->lower_,h,p):nullptr;};
    vfs_.xDlClose=[](sqlite3_vfs* v,void* h){auto* o=static_cast<store_physical_vfs*>(v->pAppData);if(o->current_()&&o->lower_->xDlClose)o->lower_->xDlClose(o->lower_,h);};
    vfs_.xRandomness=[](sqlite3_vfs* v,int n,char* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xRandomness(o->lower_,n,out):0;};
    vfs_.xSleep=[](sqlite3_vfs* v,int n)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xSleep(o->lower_,n):0;};
    vfs_.xCurrentTime=[](sqlite3_vfs* v,double* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xCurrentTime(o->lower_,out):SQLITE_IOERR;};
    vfs_.xGetLastError=[](sqlite3_vfs* v,int n,char* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()&&o->lower_->xGetLastError?o->lower_->xGetLastError(o->lower_,n,out):0;};
    if(vfs_.iVersion>=2&&lower_->xCurrentTimeInt64)vfs_.xCurrentTimeInt64=[](sqlite3_vfs* v,sqlite3_int64* out)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xCurrentTimeInt64(o->lower_,out):SQLITE_IOERR;};
    if(vfs_.iVersion>=3){
        if(lower_->xSetSystemCall)vfs_.xSetSystemCall=[](sqlite3_vfs* v,const char* p,sqlite3_syscall_ptr f)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xSetSystemCall(o->lower_,p,f):SQLITE_NOTFOUND;};
        if(lower_->xGetSystemCall)vfs_.xGetSystemCall=[](sqlite3_vfs* v,const char* p)->sqlite3_syscall_ptr {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xGetSystemCall(o->lower_,p):nullptr;};
        if(lower_->xNextSystemCall)vfs_.xNextSystemCall=[](sqlite3_vfs* v,const char* p)->const char* {auto* o=static_cast<store_physical_vfs*>(v->pAppData);return o->current_()?o->lower_->xNextSystemCall(o->lower_,p):nullptr;};
    }
}
}

#endif
