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
    const std::string& path,const struct stat& expected,sqlite3** actual,int flags) {
    if(!actual||!held.current())throw db_error("physical open lacks its issued original");
    *actual=nullptr;
    const auto creator=database_native_process();
    auto* owner=new store_physical_vfs(held,lower,path,expected,creator);
    // No inherited Native/registry call or automatic owner destruction after
    // an allocator crossed fork. The child abandons this preadmitted capsule.
    if(!owner->current_())throw db_error("physical VFS creator changed during allocation");
    try{owner->original_=store_physical_api::prepare_(held);}
    catch(...){if(owner->current_())delete owner;throw;}
    if(!owner->current_())throw db_error("physical VFS creator changed during original admission");
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
        const bool published=store_physical_api::publish_(owner->original_,*actual,owner,complete_);
        store_physical_lifetime::native_open_returned_(held,*actual!=nullptr);
        if(!published)throw db_error("physical open registration publication unproved"); // no unregister/release claim
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
    auto* owner=static_cast<store_physical_vfs*>(cookie);
    if(!owner||!owner->current_())return;
    // Called only from the exact original's last API-return ticket, after
    // actual file/API ending. Never from xClose or a Native/metadata leaf.
    if(!store_physical_lifetime::observe_(owner->held_).physical_ended)return;
    if(owner->registered_){
        const int rc=sqlite3_vfs_unregister(&owner->vfs_);
        if(!owner->current_()||rc!=SQLITE_OK)return; // registration debt retained
        owner->registered_=false;
    }
    delete owner;
}
int store_physical_vfs::open_file_(sqlite3_vfs* native,const char* name,sqlite3_file* file,int flags,int* out)noexcept {
    auto* owner=static_cast<store_physical_vfs*>(native->pAppData);file->pMethods=nullptr;
    if(!owner||!owner->current_())return SQLITE_CANTOPEN;
    const bool main=(flags&SQLITE_OPEN_MAIN_DB)!=0,wal=(flags&SQLITE_OPEN_WAL)!=0;
    const bool journal=(flags&SQLITE_OPEN_MAIN_JOURNAL)!=0;
    // This initial supported resource profile has one actual MAIN and denies
    // ATTACH; each genuine reader/keeper receives a separate issued original.
    if((main&&(!name||owner->path_!=name))||(wal&&(!name||owner->wal_!=name))||
       (journal&&(!name||owner->journal_!=name))||(flags&physical_super_journal_flag))return SQLITE_CANTOPEN;
    auto* wrapped=::new(static_cast<void*>(file))wrapper{};
    wrapped->owner=owner;wrapped->held=store_physical_lifetime::pin(owner->held_);wrapped->main=main;
    wrapped->inner=reinterpret_cast<sqlite3_file*>(reinterpret_cast<unsigned char*>(file)+aligned_prefix(sizeof(wrapper)));
    std::memset(wrapped->inner,0,std::size_t(owner->lower_->szOsFile));
    const auto kind=main?store_physical_lifetime::file_kind::main:wal?store_physical_lifetime::file_kind::wal:
        journal?store_physical_lifetime::file_kind::journal:store_physical_lifetime::file_kind::temporary;
    wrapped->issued=store_physical_lifetime::file_open_entered_(wrapped->held,kind);
    if(!wrapped->issued.serial_){wrapped->~wrapper();return SQLITE_CANTOPEN;}
    store_physical_lifetime::api_return call(wrapped->held,true);
    if(!call.entered()){store_physical_lifetime::file_open_returned_(wrapped->held,wrapped->issued,false);wrapped->~wrapper();return SQLITE_CANTOPEN;}
    int rc=owner->lower_->xOpen(owner->lower_,name,wrapped->inner,flags|physical_no_follow_flag,out);
    if(!owner->current_())return SQLITE_CANTOPEN; // inherited capsule abandoned
    const bool methods=wrapped->inner->pMethods!=nullptr;
    if(rc==SQLITE_OK&&!methods){
        // Invalid success cannot prove the lower resource absent. Retain the
        // physical lease as cleanup debt even if no close method was supplied.
        store_physical_lifetime::cleanup_returned_(wrapped->held,SQLITE_IOERR);
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
    store_physical_lifetime::file_open_returned_(wrapped->held,wrapped->issued,methods);
    if(methods)wrapped->file.pMethods=&methods_(std::min(wrapped->inner->pMethods->iVersion,3));
    else wrapped->~wrapper();
    return rc;
}
int store_physical_vfs::close_file_(sqlite3_file* file)noexcept {
    auto* wrapped=reinterpret_cast<wrapper*>(file);
    if(!wrapped->owner||!wrapped->owner->current_()||wrapped->close_entered)return SQLITE_IOERR_CLOSE;
    store_physical_lifetime::api_return call(wrapped->held,true,true);
    if(!call.entered())return SQLITE_IOERR_CLOSE;
    wrapped->close_entered=true;
    const int rc=wrapped->inner->pMethods->xClose(wrapped->inner);
    if(!wrapped->owner->current_())return rc; // no inherited record/leaf/destruction
    store_physical_lifetime::file_close_returned_(wrapped->held,wrapped->issued,rc);
    wrapped->file.pMethods=nullptr;wrapped->~wrapper();
    return rc;
}
bool store_physical_vfs::identity_layer_(sqlite3_file* file,sqlite3_vfs* observed,
    sqlite3_file*& inner,sqlite3_vfs*& lower)const noexcept {
    if(!current_()||observed!=&vfs_||!file||
       (file->pMethods!=&methods_(1)&&file->pMethods!=&methods_(2)&&file->pMethods!=&methods_(3)))return false;
    const auto* wrapped=reinterpret_cast<const wrapper*>(file);
    if(wrapped->owner!=this||!wrapped->main||wrapped->close_entered||!wrapped->issued.returned_||
       !wrapped->issued.methods_||wrapped->issued.closed_)return false;
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
                if(op==SQLITE_FCNTL_VFS_POINTER){if(!arg)return SQLITE_MISUSE;*static_cast<sqlite3_vfs**>(arg)=&w->owner->vfs_;return SQLITE_OK;}
                return w->inner->pMethods->xFileControl(w->inner,op,arg);};
            m.xSectorSize=[](sqlite3_file* f)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xSectorSize(w->inner):0;};
            m.xDeviceCharacteristics=[](sqlite3_file* f)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()?w->inner->pMethods->xDeviceCharacteristics(w->inner):0;};
            if(i>=1){
                m.xShmMap=[](sqlite3_file* f,int p,int n,int x,void volatile** out)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_()||!w->inner->pMethods->xShmMap)return SQLITE_IOERR_SHMMAP;
                    const int rc=w->inner->pMethods->xShmMap(w->inner,p,n,x,out);if(!w->owner->current_())return rc;
                    store_physical_lifetime::shm_map_returned_(w->held,w->issued,rc==SQLITE_OK&&out&&*out,rc);return rc;};
                m.xShmLock=[](sqlite3_file* f,int p,int n,int x)->int {auto* w=reinterpret_cast<wrapper*>(f);return w->owner->current_()&&w->inner->pMethods->xShmLock?w->inner->pMethods->xShmLock(w->inner,p,n,x):SQLITE_IOERR_SHMLOCK;};
                m.xShmBarrier=[](sqlite3_file* f){auto* w=reinterpret_cast<wrapper*>(f);if(w->owner->current_()&&w->inner->pMethods->xShmBarrier)w->inner->pMethods->xShmBarrier(w->inner);};
                m.xShmUnmap=[](sqlite3_file* f,int x)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_()||!w->inner->pMethods->xShmUnmap)return SQLITE_IOERR_SHMOPEN;
                    const int rc=w->inner->pMethods->xShmUnmap(w->inner,x);if(!w->owner->current_())return rc;
                    store_physical_lifetime::shm_unmap_returned_(w->held,w->issued,rc);return rc;};
            }
            if(i>=2){
                m.xFetch=[](sqlite3_file* f,sqlite3_int64 o,int n,void** out)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_())return SQLITE_IOERR_MMAP;
                    if(!w->inner->pMethods->xFetch){if(out)*out=nullptr;return SQLITE_OK;}
                    const int rc=w->inner->pMethods->xFetch(w->inner,o,n,out);if(!w->owner->current_())return rc;
                    store_physical_lifetime::fetch_returned_(w->held,w->issued,rc==SQLITE_OK&&out&&*out,rc);return rc;};
                m.xUnfetch=[](sqlite3_file* f,sqlite3_int64 o,void* p)->int {auto* w=reinterpret_cast<wrapper*>(f);if(!w->owner->current_())return SQLITE_IOERR_MMAP;
                    if(!w->inner->pMethods->xUnfetch)return SQLITE_OK;
                    const int rc=w->inner->pMethods->xUnfetch(w->inner,o,p);if(!w->owner->current_())return rc;
                    store_physical_lifetime::unfetch_returned_(w->held,w->issued,p==nullptr,rc);return rc;};
            }
        }return output;
    }();
    return all[version<1?0:version>3?2:version-1];
}
void store_physical_vfs::install_forwarders_()noexcept {
    vfs_.xDelete=[](sqlite3_vfs* v,const char* p,int sync)->int {auto* o=static_cast<store_physical_vfs*>(v->pAppData);if(!o->current_())return SQLITE_IOERR_DELETE;
        store_physical_lifetime::api_return call(o->held_,true,true);if(!call.entered())return SQLITE_IOERR_DELETE;
        const int rc=o->lower_->xDelete(o->lower_,p,sync);if(!o->current_())return rc;
        store_physical_lifetime::cleanup_returned_(o->held_,rc);return rc;};
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
