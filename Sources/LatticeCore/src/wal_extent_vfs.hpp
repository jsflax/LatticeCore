#pragma once
#include "lattice/db.hpp"
#include <lattice/store_physical_api.hpp>
#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <new>
#include <optional>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>

namespace lattice::detail {
namespace ordinary_installation {class historical_wal_checkpoint;}
struct wal_extent_slot_registry {
    const std::uint64_t process=database_native_process();
    struct slot {std::uint64_t serial=0;bool charged=false;std::shared_ptr<wal_extent_vfs> original;};
    std::mutex leaf;std::array<slot,64> slots{};std::uint64_t next=0;
};
extern const std::uint64_t wal_extent_registry_process;
extern wal_extent_slot_registry* const wal_extent_registry;
// A caller-selected logical WAL requested-extent limit, not a physical disk,
// MAIN/SHM, whole-process, or unenrolled-writer guarantee. Default opens do not
// select this owner. A first unknown connection/file close retains registration.
class wal_extent_vfs final : public std::enable_shared_from_this<wal_extent_vfs> {
    friend class ::lattice::database;
    friend struct wal_extent_test_access;
    friend class ordinary_cohort_attachment_access;
    friend class ordinary_installation::historical_wal_checkpoint;
    // Issued only by the actual retained historical namespace's private engine.
    // This is not an ordinary writer policy, attach ticket or renewable grant.
    struct file_record;
    struct checkpoint_policy {
        struct stat main{},wal{},shm{};
        // Private original-census absence, never a caller permit or a normal writer policy.
        bool shm_absent=false;
        std::string shm_path;
        sqlite3_int64 main_ceiling=0,wal_read_ceiling=0;
        const std::thread::id worker=std::this_thread::get_id();
        std::chrono::steady_clock::time_point end;
    };
    const std::optional<checkpoint_policy> checkpoint_;
    bool checkpoint_initializing_=false,checkpoint_active_=false;
    sqlite3_int64 checkpoint_main_reads_=0,checkpoint_wal_reads_=0;
    std::uint64_t checkpoint_authorizer_calls_=0;
    bool checkpoint_configuring_exclusive_=false,checkpoint_exclusive_current_=false;
    std::uint64_t checkpoint_exclusive_control_calls_=0;
    static int checkpoint_exclusive_authorizer(void* context,int action,const char* one,const char* two,
        const char* database,const char* origin)noexcept {
        auto* owner=static_cast<wal_extent_vfs*>(context);
        if(owner&&owner->current()&&owner->checkpoint_&&owner->checkpoint_->shm_absent&&
           owner->checkpoint_effect_current()&&owner->checkpoint_configuring_exclusive_&&
           owner->checkpoint_exclusive_control_calls_==0&&action==SQLITE_PRAGMA&&one&&two&&database&&!origin&&
           std::strcmp(one,"locking_mode")==0&&std::strcmp(two,"EXCLUSIVE")==0&&std::strcmp(database,"main")==0){
            ++owner->checkpoint_exclusive_control_calls_;return SQLITE_OK;}
        return checkpoint_authorizer(context,action,one,two,database,origin);
    }
    static int checkpoint_authorizer(void* context,int,const char*,const char*,const char*,const char*)noexcept {
        auto* owner=static_cast<wal_extent_vfs*>(context);
        if(owner&&owner->current()&&owner->checkpoint_&&owner->checkpoint_->worker==std::this_thread::get_id()&&
           owner->checkpoint_authorizer_calls_!=std::numeric_limits<std::uint64_t>::max())++owner->checkpoint_authorizer_calls_;
        return SQLITE_DENY;
    }

    bool checkpoint_identity()const noexcept {
        if(!checkpoint_)return true;
        const auto same=[](const std::string& path,const struct stat& expected,sqlite3_int64 ceiling)noexcept {
            struct stat actual{};
            return ::lstat(path.c_str(),&actual)==0&&S_ISREG(actual.st_mode)&&actual.st_nlink==1&&
                actual.st_uid==expected.st_uid&&actual.st_dev==expected.st_dev&&actual.st_ino==expected.st_ino&&
                (actual.st_mode&07777)==(expected.st_mode&07777)&&actual.st_size>=0&&
                actual.st_size<=ceiling;
        };
        if(checkpoint_->shm_absent){
            struct stat unexpected{};
            return same(path_,checkpoint_->main,checkpoint_->main_ceiling)&&same(wal_path_,checkpoint_->wal,checkpoint_->wal.st_size)&&
                ::lstat(checkpoint_->shm_path.c_str(),&unexpected)!=0&&errno==ENOENT;
        }
        return same(path_,checkpoint_->main,checkpoint_->main_ceiling)&&same(wal_path_,checkpoint_->wal,checkpoint_->wal.st_size)&&
            same(checkpoint_->shm_path,checkpoint_->shm,checkpoint_->shm.st_size);
    }
    bool checkpoint_effect_current()const noexcept {
        return !checkpoint_||(process_==database_native_process()&&checkpoint_->worker==std::this_thread::get_id()&&
            std::chrono::steady_clock::now()<checkpoint_->end&&checkpoint_identity());
    }
    int checkpoint_refuse(file_record& r,int rc)noexcept;
    static constexpr std::size_t maximum_files=256;
    static constexpr std::size_t maximum_members=64;
    struct member_record {
        std::string path,wal_path,alias;
        sqlite3_int64 limit=0;
        std::uint64_t serial=0,device=0,inode=0;
        std::int64_t token=0;
        bool enrolled=false,primary=false,main_entered=false,main_returned=false;
        bool attach_entered=false,attach_returned=false,attach_clean=false;
        bool detach_entered=false,detach_returned=false,detach_clean=false,ended=false;
        int main_result=SQLITE_MISUSE;
        std::uint64_t writes=0,truncates=0,refused_growth=0;
        sqlite3_int64 maximum_delegated_end=0;
        int first_error=SQLITE_OK;
    };
    struct file_record {
        const sqlite3_file* original=nullptr;
        sqlite3_int64 extent=0,limit=0;
        std::size_t member=0;std::uint64_t member_serial=0;
        bool reusable_after_member_end=false;
        bool wal=false,open_returned=false,has_methods=false,in_flight=false;
        bool close_entered=false,close_returned=false;
        int flags=0,open_result=SQLITE_MISUSE,close_result=SQLITE_MISUSE,first_error=SQLITE_OK;
    };
    struct wrapper {sqlite3_file file;wal_extent_vfs* owner;file_record* record;sqlite3_file* inner;};
    static constexpr std::size_t prefix=(sizeof(wrapper)+alignof(sqlite3_file)-1)&~(alignof(sqlite3_file)-1);
    static_assert(alignof(wrapper)<=alignof(sqlite3_file));
    const std::uint64_t process_=database_native_process();
    const std::size_t slot_;const std::uint64_t serial_;const sqlite3_int64 limit_;
    sqlite3_vfs* const base_;sqlite3_vfs vfs_{};
    const std::string path_,wal_path_,name_;
    std::mutex leaf_;std::array<file_record,maximum_files> files_{};std::size_t issued_=0;
    std::uint64_t write_calls_=0,truncate_calls_=0,refused_growth_=0;
    sqlite3_int64 maximum_delegated_end_=0;
    int first_error_=SQLITE_OK,unregister_result_=SQLITE_MISUSE,native_open_result_=SQLITE_MISUSE;
    int checked_close_result_=SQLITE_MISUSE,fallback_close_result_=SQLITE_MISUSE;
    bool registered_=false,open_returned_=false,close_returned_=false,ending_known_=false;
    bool opens_sealed_=false,native_close_entered_=false,checked_close_observed_=false,main_entered_=false;
    bool registration_entered_=false,registration_returned_=false,unregister_entered_=false,unregister_returned_=false;
    std::array<member_record,maximum_members> members_{};
    std::size_t issued_members_=1;std::uint64_t next_member_serial_=1;
    std::array<bool,maximum_members> oversized_original_{};
    sqlite3* native_original_=nullptr;

    // This ticket is issued only by the same private ordinary typed binding.
    // It retains the actual owner through the synchronous first call/unwind.
    class member_operation {
        std::shared_ptr<wal_extent_vfs> owner_;
        const std::size_t slot_;const std::uint64_t serial_;const bool detach_;
        bool entered_=false,returned_=false;
    public:
        member_operation(std::shared_ptr<wal_extent_vfs> owner,std::size_t slot,std::uint64_t serial,bool detach)noexcept
            :owner_(std::move(owner)),slot_(slot),serial_(serial),detach_(detach){}
        member_operation(const member_operation&)=delete;
        member_operation& operator=(const member_operation&)=delete;
        member_operation(member_operation&& other)noexcept
            :owner_(std::move(other.owner_)),slot_(other.slot_),serial_(other.serial_),detach_(other.detach_),
             entered_(other.entered_),returned_(other.returned_){}
        ~member_operation(){if(owner_&&!returned_)owner_->member_call_returned(slot_,serial_,detach_,entered_,false);}
        void enter(){
            if(!owner_||entered_||!owner_->member_call_entered(slot_,serial_,detach_))
                throw db_error("bounded WAL typed member first-call binding changed");
            entered_=true;
        }
        void complete(){
            if(!owner_||!entered_||returned_)throw db_error("bounded WAL typed member return changed");
            if(!owner_->member_call_returned(slot_,serial_,detach_,true,true))
                throw db_error("bounded WAL typed member actual return proof unavailable");
            returned_=true;
        }
    };
    bool member_call_entered(std::size_t slot,std::uint64_t serial,bool detach)noexcept {
        if(!current())return false;std::lock_guard lock(leaf_);
        if(!registered_||opens_sealed_||native_close_entered_||slot>=issued_members_)return false;
        auto& m=members_[slot];if(!m.enrolled||m.primary||m.ended||m.serial!=serial)return false;
        if(detach){if(m.detach_entered)return false;m.detach_entered=true;}
        else {if(m.attach_entered)return false;m.attach_entered=true;}
        return true;
    }
    bool member_call_returned(std::size_t slot,std::uint64_t serial,bool detach,bool entered,bool clean)noexcept {
        if(!current())return false;std::lock_guard lock(leaf_);
        if(slot>=issued_members_)return false;auto& m=members_[slot];
        if(!m.enrolled||m.primary||m.serial!=serial||m.ended)return false;
        if(!entered){
            // Exact private ticket never called the first synchronous helper;
            // xOpen cannot consume this reserved member before attach_entered.
            if(!m.attach_entered&&!detach){m.ended=true;return true;}
            return false;
        }
        if(!detach){
            if(!m.attach_entered||m.attach_returned)return false;
            m.attach_returned=true;m.attach_clean=clean&&m.main_returned&&m.main_result==SQLITE_OK;
            return !clean||m.attach_clean;
        }
        if(!m.detach_entered||m.detach_returned)return false;m.detach_returned=true;m.detach_clean=clean;
        if(!clean)return true;
        // Known DETACH helper return AND every actual original file first
        // close, including the issued MAIN, permits clean member reuse.
        bool main_original=false;
        for(std::size_t i=0;i<issued_;++i){const auto& f=files_[i];
            if(f.member!=slot||f.member_serial!=serial)continue;
            if((f.flags&SQLITE_OPEN_MAIN_DB)&&f.open_returned&&f.open_result==SQLITE_OK&&f.has_methods)
                main_original=true;
            if(!f.open_returned||f.in_flight||(f.has_methods&&(!f.close_returned||f.close_result!=SQLITE_OK)))return false;
        }
        if(!main_original||!m.main_returned||m.main_result!=SQLITE_OK)return false;
        m.ended=true;
        for(std::size_t i=0;i<issued_;++i){auto& f=files_[i];
            if(f.member==slot&&f.member_serial==serial&&f.close_returned&&f.close_result==SQLITE_OK&&f.first_error==SQLITE_OK)
                f.reusable_after_member_end=true;
        }
        return true;
    }
    member_operation enroll_attachment(sqlite3* native,const std::string& path,const std::string& alias,
        const physical_store_identity& expected,sqlite3_int64 limit,std::int64_t token) {
        if(!current()||limit<=0||token<=0||path!=expected.filename||alias.empty()||alias.find('\0')!=std::string::npos)
            throw db_error("bounded WAL typed target policy/path changed");
        const std::filesystem::path selected(path);
        if(!selected.is_absolute()||selected.lexically_normal()!=selected||
           std::filesystem::canonical(selected.parent_path())!=selected.parent_path())
            throw db_error("bounded WAL typed target requires canonical native path");
        auto held=weak_from_this().lock();if(!held||held.get()!=this)throw db_error("bounded WAL typed owner custody unavailable");
        member_record prepared;prepared.path=path;prepared.wal_path=path+"-wal";prepared.alias=alias;
        prepared.limit=limit;prepared.device=expected.device;prepared.inode=expected.inode;prepared.token=token;prepared.enrolled=true;
        std::size_t slot=maximum_members;std::uint64_t serial=0;bool duplicate=false;
        {std::lock_guard lock(leaf_);
            if(registered_&&!opens_sealed_&&!native_close_entered_&&native_original_==native&&native_open_result_==SQLITE_OK&&
               next_member_serial_!=std::numeric_limits<std::uint64_t>::max()){
                for(std::size_t i=0;i<issued_members_;++i)if(members_[i].enrolled&&!members_[i].ended&&
                    (members_[i].path==path||members_[i].alias==alias))duplicate=true;
                if(!duplicate){
                    for(std::size_t i=1;i<issued_members_;++i)if(members_[i].ended){slot=i;break;}
                    if(slot==maximum_members&&issued_members_<maximum_members)slot=issued_members_++;
                    if(slot<maximum_members){serial=++next_member_serial_;prepared.serial=serial;members_[slot]=std::move(prepared);oversized_original_[slot]=false;}
                }
            }
        }
        if(slot==maximum_members)throw db_error("bounded WAL typed member unavailable or capacity exhausted");
        return member_operation(std::move(held),slot,serial,false);
    }
    member_operation detach_attachment(sqlite3* native,const std::string& path,const std::string& alias,std::int64_t token) {
        if(!current())throw db_error("bounded WAL typed detach inherited");
        auto held=weak_from_this().lock();if(!held||held.get()!=this)throw db_error("bounded WAL typed owner custody unavailable");
        std::size_t slot=maximum_members;std::uint64_t serial=0;
        {std::lock_guard lock(leaf_);
            if(registered_&&!opens_sealed_&&!native_close_entered_&&native_original_==native)
                for(std::size_t i=1;i<issued_members_;++i){const auto& m=members_[i];
                    if(m.enrolled&&!m.ended&&m.path==path&&m.alias==alias&&m.token==token&&!m.detach_entered){slot=i;serial=m.serial;break;}}
        }
        if(slot==maximum_members)throw db_error("bounded WAL typed detach original unavailable");
        return member_operation(std::move(held),slot,serial,true);
    }
    bool matches_primary_policy(sqlite3* native,const std::string& path,sqlite3_int64 limit)noexcept {
        if(!current())return false;std::lock_guard lock(leaf_);
        return registered_&&!opens_sealed_&&!native_close_entered_&&native_original_==native&&
            native_open_result_==SQLITE_OK&&path_==path&&limit_==limit;
    }
    void refused(file_record& r)noexcept {
        ++refused_growth_;if(r.member<issued_members_&&members_[r.member].serial==r.member_serial)++members_[r.member].refused_growth;
        fault(r,SQLITE_FULL);
    }

    static wrapper* get(sqlite3_file* f)noexcept{return reinterpret_cast<wrapper*>(f);}
    static wal_extent_vfs* get(sqlite3_vfs* v)noexcept{return static_cast<wal_extent_vfs*>(v->pAppData);}
    bool current()const noexcept{return process_==database_native_process();}
    void fault(file_record& r,int rc)noexcept {
        if(rc==SQLITE_OK)return;if(r.first_error==SQLITE_OK)r.first_error=rc;if(first_error_==SQLITE_OK)first_error_=rc;
        if(r.member<issued_members_&&members_[r.member].serial==r.member_serial&&members_[r.member].first_error==SQLITE_OK)
            members_[r.member].first_error=rc;
    }
    // Inspect the real existing length before each requested growth. SQLite's
    // WAL write lock still owns cross-connection serialization; this is not an
    // assertion that an old/default-VFS writer is excluded by this connection.
    int reserve(wrapper& w,sqlite3_int64 end,bool truncating)noexcept {
        if(end<0||end>w.record->limit){std::lock_guard lock(leaf_);refused(*w.record);return SQLITE_FULL;}
        {std::lock_guard lock(leaf_);
            if(!registered_||unregister_entered_||w.record->in_flight||w.record->close_entered||
               (opens_sealed_&&!(truncating&&end==0&&native_close_entered_)))return SQLITE_IOERR;
            w.record->in_flight=true;}
        sqlite3_int64 actual=0;const int rc=w.inner->pMethods->xFileSize(w.inner,&actual);
        {std::lock_guard lock(leaf_);
            if(rc!=SQLITE_OK||actual<0||(actual>w.record->limit&&!(truncating&&end<=w.record->limit))) {
                const int failure=rc!=SQLITE_OK?rc:(actual<0?SQLITE_IOERR_FSTAT:SQLITE_FULL);
                if(failure==SQLITE_FULL)refused(*w.record);else fault(*w.record,failure);w.record->in_flight=false;return failure;
            }
            w.record->extent=actual;}
        return SQLITE_OK;
    }
    static int write_file(sqlite3_file* f,const void* bytes,int count,sqlite3_int64 offset)noexcept {
        auto* w=get(f);auto& s=*w->owner;if(!s.current())return SQLITE_IOERR_WRITE;
        if(s.checkpoint_){
            if(!s.checkpoint_effect_current()||!s.checkpoint_active_||w->record->wal||
               (s.checkpoint_->shm_absent&&!s.checkpoint_exclusive_current_))
                return s.checkpoint_refuse(*w->record,SQLITE_READONLY);
            if(count<0||offset<0||offset>s.checkpoint_->main_ceiling||
               static_cast<sqlite3_int64>(count)>s.checkpoint_->main_ceiling-offset)
                return s.checkpoint_refuse(*w->record,SQLITE_FULL);
            const int rc=w->inner->pMethods->xWrite(w->inner,bytes,count,offset);
            if(rc!=SQLITE_OK)return s.checkpoint_refuse(*w->record,rc);
            return s.checkpoint_identity()?rc:s.checkpoint_refuse(*w->record,SQLITE_IOERR_WRITE);
        }
        if(!w->record->wal)return w->inner->pMethods->xWrite(w->inner,bytes,count,offset);
        if(count<0||offset<0||offset>w->record->limit||static_cast<sqlite3_int64>(count)>w->record->limit-offset) {
            std::lock_guard lock(s.leaf_);s.refused(*w->record);return SQLITE_FULL;
        }
        const auto end=offset+count;const int admitted=s.reserve(*w,end,false);if(admitted!=SQLITE_OK)return admitted;
        {std::lock_guard lock(s.leaf_);++s.write_calls_;s.maximum_delegated_end_=std::max(s.maximum_delegated_end_,end);
            auto& m=s.members_[w->record->member];++m.writes;m.maximum_delegated_end=std::max(m.maximum_delegated_end,end);}
        const int rc=w->inner->pMethods->xWrite(w->inner,bytes,count,offset);
        {std::lock_guard lock(s.leaf_);s.fault(*w->record,rc);w->record->in_flight=false;}
        return rc;
    }
    static int truncate_file(sqlite3_file* f,sqlite3_int64 length)noexcept {
        auto* w=get(f);auto& s=*w->owner;if(!s.current())return SQLITE_IOERR_TRUNCATE;
        if(s.checkpoint_){
            if(!s.checkpoint_effect_current()||!s.checkpoint_active_||length<0||
               (s.checkpoint_->shm_absent&&!s.checkpoint_exclusive_current_)||
               (!w->record->wal&&length>s.checkpoint_->main_ceiling)||(w->record->wal&&length!=0))
                return s.checkpoint_refuse(*w->record,SQLITE_READONLY);
            if(!w->record->wal){
                const int rc=w->inner->pMethods->xTruncate(w->inner,length);
                if(rc!=SQLITE_OK)return s.checkpoint_refuse(*w->record,rc);
                return s.checkpoint_identity()?rc:s.checkpoint_refuse(*w->record,SQLITE_IOERR_TRUNCATE);
            }
        }
        if(!w->record->wal)return w->inner->pMethods->xTruncate(w->inner,length);
        const int admitted=s.reserve(*w,length,true);if(admitted!=SQLITE_OK)return admitted;
        {std::lock_guard lock(s.leaf_);++s.truncate_calls_;s.maximum_delegated_end_=std::max(s.maximum_delegated_end_,length);
            auto& m=s.members_[w->record->member];++m.truncates;m.maximum_delegated_end=std::max(m.maximum_delegated_end,length);}
        const int rc=w->inner->pMethods->xTruncate(w->inner,length);
        {std::lock_guard lock(s.leaf_);s.fault(*w->record,rc);if(rc==SQLITE_OK)w->record->extent=length;w->record->in_flight=false;}
        return rc;
    }
    static int control_file(sqlite3_file* f,int op,void* argument)noexcept {
        auto* w=get(f);if(!w->owner->current())return SQLITE_IOERR;
        if(w->owner->checkpoint_) {
            // No allocation hints or writable mmap escape the private phase.
            if(op==SQLITE_FCNTL_SIZE_HINT||op==SQLITE_FCNTL_CHUNK_SIZE)return SQLITE_OK;
            if(op==SQLITE_FCNTL_MMAP_SIZE){if(!argument)return SQLITE_MISUSE;*static_cast<sqlite3_int64*>(argument)=0;return SQLITE_OK;}
        }
        if(w->record->wal) {
            // These are optional hints. Never delegate allocation or chunk
            // rounding: either could grow beyond the checked xWrite endpoint.
            if(op==SQLITE_FCNTL_SIZE_HINT) {
                if(!argument||*static_cast<sqlite3_int64*>(argument)<0||*static_cast<sqlite3_int64*>(argument)>w->record->limit) {
                    std::lock_guard lock(w->owner->leaf_);w->owner->refused(*w->record);return SQLITE_FULL;
                }
                return SQLITE_OK;
            }
            if(op==SQLITE_FCNTL_CHUNK_SIZE)return SQLITE_OK;
        }
        return w->inner->pMethods->xFileControl(w->inner,op,argument);
    }
    static int close_file(sqlite3_file* f)noexcept {
        auto* w=get(f);if(!w->owner->current())return SQLITE_IOERR_CLOSE;
        auto keep=w->owner->weak_from_this().lock();if(!keep)return SQLITE_IOERR_CLOSE;auto& s=*keep;
        {std::lock_guard lock(s.leaf_);if(w->record->original!=f||w->record->in_flight||w->record->close_entered)return SQLITE_IOERR_CLOSE;w->record->close_entered=true;}
        const int rc=w->inner->pMethods->xClose(w->inner);
        {std::lock_guard lock(s.leaf_);w->record->close_returned=true;w->record->close_result=rc;s.fault(*w->record,rc);}
        w->file.pMethods=nullptr;s.settle_registration();return rc;
    }
    static int open_file(sqlite3_vfs* v,const char* name,sqlite3_file* f,int flags,int* actual)noexcept {
        auto* s=get(v);f->pMethods=nullptr;if(!s->current())return SQLITE_CANTOPEN;
        auto keep=s->weak_from_this().lock();if(!keep)return SQLITE_CANTOPEN;
        const bool main=(flags&SQLITE_OPEN_MAIN_DB)!=0,wal=(flags&SQLITE_OPEN_WAL)!=0;
        // Uncontrolled temporary/journal files contain no owner pointer. This
        // slice does not claim their extents are WAL, MAIN, SHM or temp quota.
        if(!main&&!wal)return s->checkpoint_?SQLITE_CANTOPEN:s->base_->xOpen(s->base_,name,f,flags,actual);
        if(s->checkpoint_&&(!s->checkpoint_effect_current()||!(flags&SQLITE_OPEN_READWRITE)))return SQLITE_CANTOPEN;
        if(!name)return SQLITE_CANTOPEN;
        file_record* record=nullptr;std::size_t member=maximum_members;std::uint64_t member_serial=0;
        {std::lock_guard lock(s->leaf_);
            if(!s->registered_||!s->registration_returned_||s->opens_sealed_||s->unregister_entered_)return SQLITE_CANTOPEN;
            for(std::size_t i=0;i<s->issued_members_;++i){auto& m=s->members_[i];
                if(!m.enrolled||m.ended||(!m.primary&&!m.attach_entered)||m.detach_returned)continue;
                if((main&&m.path==name)||(wal&&m.wal_path==name)){
                    if((main&&m.main_entered)||(wal&&(!m.main_returned||m.main_result!=SQLITE_OK)))return SQLITE_CANTOPEN;
                    member=i;member_serial=m.serial;break;
                }
            }
            if(member==maximum_members)return SQLITE_CANTOPEN;
            // Primary clean closes retain001 behavior. Attached originals are
            // reusable only AFTER their exact DETACH + first-close ending.
            for(std::size_t i=0;i<s->issued_;++i)if(s->files_[i].close_returned&&s->files_[i].close_result==SQLITE_OK&&
                s->files_[i].first_error==SQLITE_OK&&(s->files_[i].member==0||s->files_[i].reusable_after_member_end)){record=&s->files_[i];break;}
            if(!record&&s->issued_<maximum_files)record=&s->files_[s->issued_++];
            if(!record){if(s->first_error_==SQLITE_OK)s->first_error_=SQLITE_FULL;return SQLITE_FULL;}
            *record=file_record{};record->original=f;record->wal=wal;record->flags=flags;
            record->member=member;record->member_serial=member_serial;record->limit=s->members_[member].limit;
            if(main){s->members_[member].main_entered=true;if(member==0)s->main_entered_=true;}
        }
        auto* w=::new(static_cast<void*>(f))wrapper{};w->owner=s;w->record=record;
        w->inner=reinterpret_cast<sqlite3_file*>(reinterpret_cast<unsigned char*>(f)+prefix);
        std::memset(w->inner,0,static_cast<std::size_t>(s->base_->szOsFile));
        #ifdef SQLITE_OPEN_NOFOLLOW
        const int native=s->base_->xOpen(s->base_,name,w->inner,(s->checkpoint_?(flags&~SQLITE_OPEN_CREATE):flags)|SQLITE_OPEN_NOFOLLOW,actual);
#else
        const int native=SQLITE_CANTOPEN;
#endif
        int returned=native;sqlite3_int64 size=0;
        if(native==SQLITE_OK&&s->checkpoint_&&!s->checkpoint_identity())returned=SQLITE_CANTOPEN;
        if(native==SQLITE_OK) {
            if(!w->inner->pMethods)returned=SQLITE_CANTOPEN;
            else {
                struct stat named{};int moved=1;
                if(::lstat(name,&named)!=0||!S_ISREG(named.st_mode)||named.st_nlink!=1||
                   w->inner->pMethods->xFileControl(w->inner,SQLITE_FCNTL_HAS_MOVED,&moved)!=SQLITE_OK||moved)returned=SQLITE_CANTOPEN;
                if(main&&returned==SQLITE_OK&&member!=0){const auto& m=s->members_[member];
                    if(static_cast<std::uint64_t>(named.st_dev)!=m.device||static_cast<std::uint64_t>(named.st_ino)!=m.inode)returned=SQLITE_CANTOPEN;}
                if(wal&&returned==SQLITE_OK){returned=w->inner->pMethods->xFileSize(w->inner,&size);if(returned==SQLITE_OK&&(size<0||size>record->limit))returned=SQLITE_FULL;}
            }
        }
        {std::lock_guard lock(s->leaf_);record->open_returned=true;record->open_result=native;record->has_methods=w->inner->pMethods!=nullptr;record->extent=size;s->fault(*record,returned);
            if(main){s->members_[member].main_returned=true;s->members_[member].main_result=returned;}
            if(wal&&size>record->limit)s->oversized_original_[member]=true;}
        if(w->inner->pMethods)w->file.pMethods=&methods(std::min(w->inner->pMethods->iVersion,3));
        s->settle_registration();return returned;
    }
    void release_ended_slot()noexcept {
        if(!current()||wal_extent_registry_process!=database_native_process())return;
        auto keep=weak_from_this().lock();if(!keep)return;
        {std::lock_guard lock(leaf_);if(!ending_known_||registered_||!unregister_returned_||unregister_result_!=SQLITE_OK)return;}
        auto& registry=*wal_extent_registry;if(registry.process!=process_)return;
        std::lock_guard lock(registry.leaf);auto& slot=registry.slots[slot_];
        if(slot.charged&&slot.serial==serial_&&slot.original.get()==this){slot.original.reset();slot.charged=false;}
    }
    void settle_registration()noexcept {
        if(!current())return;auto keep=weak_from_this().lock();if(!keep)return;
        {std::lock_guard lock(leaf_);
            if(!registered_||ending_known_||unregister_entered_||!opens_sealed_||!open_returned_||
               (native_original_&&(!native_close_entered_||!checked_close_observed_||!close_returned_||checked_close_result_!=SQLITE_OK)))return;
            for(std::size_t i=0;i<issued_;++i){const auto& f=files_[i];if(!f.open_returned||f.in_flight||(f.has_methods&&(!f.close_returned||f.close_result!=SQLITE_OK)))return;}
            unregister_entered_=true;}
        const int rc=sqlite3_vfs_unregister(&vfs_);
        {std::lock_guard lock(leaf_);unregister_returned_=true;unregister_result_=rc;if(rc!=SQLITE_OK){if(first_error_==SQLITE_OK)first_error_=rc;return;}registered_=false;ending_known_=true;}
        release_ended_slot();
    }
    bool native_open_returned(sqlite3* actual,int result)noexcept {
        if(!current())return false;
        {std::lock_guard lock(leaf_);if(open_returned_)return false;native_original_=actual;open_returned_=true;native_open_result_=result;if(!actual)opens_sealed_=true;}
        if(!actual){settle_registration();return result!=SQLITE_OK;}if(result!=SQLITE_OK)return true;
        sqlite3_file* f=nullptr;
        if(!store_physical_main_for_lower(actual,&vfs_,f))return false;
        if((!f&&sqlite3_file_control(actual,"main",SQLITE_FCNTL_FILE_POINTER,&f)!=SQLITE_OK)||!f||
           (f->pMethods!=&methods(1)&&f->pMethods!=&methods(2)&&f->pMethods!=&methods(3)))return false;
        return get(f)->owner==this;
    }
    void native_close_entered(sqlite3* original)noexcept {
        if(!current())return;std::lock_guard lock(leaf_);if(open_returned_&&native_original_==original){opens_sealed_=true;native_close_entered_=true;}
    }
    void native_close_returned(sqlite3* original,int checked,int fallback)noexcept {
        if(!current())return;
        {std::lock_guard lock(leaf_);if(native_original_!=original||!native_close_entered_||checked_close_observed_)return;
            checked_close_observed_=true;checked_close_result_=checked;fallback_close_result_=fallback;
            if(checked!=SQLITE_OK){if(first_error_==SQLITE_OK)first_error_=checked;return;}close_returned_=true;}
        settle_registration();
    }
    sqlite3_vfs* original_vfs_for_identity(sqlite3* native,const std::string& schema,sqlite3_vfs* observed)noexcept {
        if(!current())return nullptr;
        sqlite3_file* f=nullptr;
        observed=store_physical_identity_vfs(native,schema.c_str(),observed,f);
        if(observed!=&vfs_)return nullptr;
        if((!f&&sqlite3_file_control(native,schema.c_str(),SQLITE_FCNTL_FILE_POINTER,&f)!=SQLITE_OK)||!f||
           (f->pMethods!=&methods(1)&&f->pMethods!=&methods(2)&&f->pMethods!=&methods(3)))return nullptr;
        auto* w=get(f);if(w->owner!=this)return nullptr;
        std::lock_guard lock(leaf_);if(!registered_||opens_sealed_||native_original_!=native||native_open_result_!=SQLITE_OK)return nullptr;
        bool issued=false;for(std::size_t i=0;i<issued_;++i)if(w->record==&files_[i])issued=true;
        if(!issued||w->record->original!=f||w->record->wal||!(w->record->flags&SQLITE_OPEN_MAIN_DB)||w->record->close_entered||w->record->open_result!=SQLITE_OK)return nullptr;
        return base_;
    }
    wal_extent_vfs(const std::string& path,sqlite3_int64 limit,std::size_t slot,std::uint64_t serial,const checkpoint_policy* checkpoint)
        :checkpoint_(checkpoint?std::optional<checkpoint_policy>(*checkpoint):std::nullopt),slot_(slot),serial_(serial),limit_(limit),base_(sqlite3_vfs_find(nullptr)),path_(path),wal_path_(path+"-wal"),
         name_("lattice-wal-extent-"+std::to_string(serial)+"-"+std::to_string(reinterpret_cast<std::uintptr_t>(this))) {
        if(!base_||!base_->zName||(std::strcmp(base_->zName,"unix")!=0&&std::strcmp(base_->zName,"unix-excl")!=0)||base_->iVersion<1||
           base_->szOsFile<static_cast<int>(sizeof(sqlite3_file))||limit<=0||static_cast<std::size_t>(base_->szOsFile)>static_cast<std::size_t>(std::numeric_limits<int>::max())-prefix)
            throw db_error("bounded WAL requires original unix VFS and a positive explicit extent");
        // The missing-SHM fixed-control mechanism is qualified separately on
        // normal unix. Do not silently substitute unix-excl/global defaults.
        if(checkpoint_&&checkpoint_->shm_absent&&std::strcmp(base_->zName,"unix")!=0)
            throw db_error("historical absent-SHM checkpoint requires original unix VFS");
#ifndef SQLITE_OPEN_NOFOLLOW
        throw db_error("bounded WAL requires SQLite no-follow support");
#endif
        const std::filesystem::path selected(path);
        if(!selected.is_absolute()||path.find('\0')!=std::string::npos||selected.lexically_normal()!=selected||
           std::filesystem::canonical(selected.parent_path())!=selected.parent_path())throw db_error("bounded WAL requires a canonical native file parent");
        members_[0].path=path_;members_[0].wal_path=wal_path_;members_[0].limit=limit_;
        members_[0].serial=1;members_[0].enrolled=true;members_[0].primary=true;
        vfs_=*base_;vfs_.pNext=nullptr;vfs_.pAppData=this;vfs_.zName=name_.c_str();vfs_.szOsFile=static_cast<int>(prefix)+base_->szOsFile;
        vfs_.iVersion=std::min(base_->iVersion,3);vfs_.xOpen=open_file;install_forwarders();
    }
    static std::shared_ptr<wal_extent_vfs> create(const std::string& path,sqlite3_int64 limit,const checkpoint_policy* checkpoint=nullptr) {
        if(wal_extent_registry_process!=database_native_process())throw db_error("bounded WAL registry inherited");
        auto& registry=*wal_extent_registry;if(registry.process!=database_native_process())throw db_error("bounded WAL creator required");
        std::size_t index=registry.slots.size();std::uint64_t serial=0;
        {std::lock_guard lock(registry.leaf);if(registry.next==std::numeric_limits<std::uint64_t>::max())throw db_error("bounded WAL serial exhausted");
            for(std::size_t i=0;i<registry.slots.size();++i)if(!registry.slots[i].charged){index=i;serial=++registry.next;registry.slots[i].charged=true;registry.slots[i].serial=serial;break;}}
        if(index==registry.slots.size())throw db_error("bounded WAL registered/debt capacity exhausted");
        struct admission {std::size_t index;std::uint64_t serial;bool transferred=false;
            ~admission(){if(transferred||wal_extent_registry_process!=database_native_process())return;auto& r=*wal_extent_registry;std::lock_guard lock(r.leaf);auto& slot=r.slots[index];if(slot.charged&&slot.serial==serial&&!slot.original)slot.charged=false;}} held{index,serial};
        auto owner=std::shared_ptr<wal_extent_vfs>(new wal_extent_vfs(path,limit,index,serial,checkpoint));
        {std::lock_guard lock(registry.leaf);registry.slots[index].original=owner;held.transferred=true;}
        {std::lock_guard lock(owner->leaf_);owner->registration_entered_=true;}
        const int rc=sqlite3_vfs_register(&owner->vfs_,0);
        {std::lock_guard lock(owner->leaf_);owner->registration_returned_=true;if(rc==SQLITE_OK)owner->registered_=true;else owner->first_error_=rc;}
        if(rc!=SQLITE_OK)throw db_error("bounded WAL registration unknown");return owner;
    }
    static const sqlite3_io_methods& methods(int version)noexcept;
    void install_forwarders()noexcept;
public:
    static bool requires_attachment_guard(sqlite3* native)noexcept {
        if(!native||wal_extent_registry_process!=database_native_process())return false;
        auto& registry=*wal_extent_registry;if(registry.process!=database_native_process())return false;
        std::lock_guard registry_lock(registry.leaf);
        for(const auto& slot:registry.slots)if(slot.charged&&slot.original) {
            auto& owner=*slot.original;
            std::lock_guard owner_lock(owner.leaf_);
            if(owner.registered_&&owner.open_returned_&&owner.native_open_result_==SQLITE_OK&&
               !owner.native_close_entered_&&!owner.opens_sealed_&&owner.native_original_==native)return true;
        }
        return false;
    }
    struct observation {
        sqlite3_int64 limit=0,maximum_delegated_end=0;
        std::uint64_t writes=0,truncates=0,refused_growth=0,member_serial=0;
        bool current_creator=false,registered=false,ending_known=false,close_observed=false;
        int first_error=SQLITE_MISUSE,checked_close=SQLITE_MISUSE,fallback_close=SQLITE_MISUSE;
    };
    observation inspect()noexcept {
        observation out;if(!current())return out;std::lock_guard lock(leaf_);
        out.limit=limit_;out.maximum_delegated_end=maximum_delegated_end_;out.writes=write_calls_;out.truncates=truncate_calls_;out.refused_growth=refused_growth_;
        out.current_creator=true;out.registered=registered_;out.ending_known=ending_known_;out.close_observed=checked_close_observed_;
        out.first_error=first_error_;out.checked_close=checked_close_result_;out.fallback_close=fallback_close_result_;return out;
    }
    observation inspect_member(const std::string& path)noexcept {
        observation out;if(!current())return out;std::lock_guard lock(leaf_);
        for(std::size_t i=0;i<issued_members_;++i){const auto& m=members_[i];if(!m.enrolled||m.path!=path)continue;
            out.limit=m.limit;out.member_serial=m.serial;out.maximum_delegated_end=m.maximum_delegated_end;out.writes=m.writes;out.truncates=m.truncates;
            out.refused_growth=m.refused_growth;out.first_error=m.first_error;out.current_creator=true;
            out.registered=registered_;out.ending_known=m.ended;return out;}
        return out;
    }
    const char* name()const noexcept{return name_.c_str();}
};
inline int wal_extent_vfs::checkpoint_refuse(file_record& record,int rc)noexcept {
    std::lock_guard lock(leaf_);fault(record,rc);return rc;
}
inline const sqlite3_io_methods& wal_extent_vfs::methods(int version)noexcept {
    static constexpr std::array<sqlite3_io_methods,3> all=[]() constexpr {
        std::array<sqlite3_io_methods,3> out{};
        for(int at=0;at<3;++at) {
            auto& value=out[at];value.iVersion=at+1;
            value.xClose=close_file;value.xWrite=write_file;value.xTruncate=truncate_file;value.xFileControl=control_file;
            value.xRead=[](sqlite3_file* file,void* bytes,int count,sqlite3_int64 offset)->int {
                auto* w=get(file);auto& s=*w->owner;
                if(!s.current())return SQLITE_IOERR_READ;
                if(s.checkpoint_){
                    if(!s.checkpoint_effect_current()||count<0||offset<0||
                       (!s.checkpoint_initializing_&&!s.checkpoint_active_&&(w->record->wal||offset!=0||count>4096)))
                        return s.checkpoint_refuse(*w->record,SQLITE_IOERR_READ);
                    const auto ceiling=w->record->wal?s.checkpoint_->wal_read_ceiling:sqlite3_int64(8*1024*1024);
                    {std::lock_guard lock(s.leaf_);auto& consumed=w->record->wal?s.checkpoint_wal_reads_:s.checkpoint_main_reads_;
                        if(consumed>ceiling||static_cast<sqlite3_int64>(count)>ceiling-consumed){s.fault(*w->record,SQLITE_IOERR_READ);return SQLITE_IOERR_READ;}
                        consumed+=count;}
                }
                return w->inner->pMethods->xRead(w->inner,bytes,count,offset);};
            value.xSync=[](sqlite3_file* file,int flags)->int {
                auto* w=get(file);return w->owner->current()?w->inner->pMethods->xSync(w->inner,flags):SQLITE_IOERR_FSYNC;};
            value.xFileSize=[](sqlite3_file* file,sqlite3_int64* size)->int {
                auto* w=get(file);return w->owner->current()?w->inner->pMethods->xFileSize(w->inner,size):SQLITE_IOERR_FSTAT;};
            value.xLock=[](sqlite3_file* file,int lock)->int {
                auto* w=get(file);auto& s=*w->owner;
                if(!s.current())return SQLITE_IOERR_LOCK;
                if(s.checkpoint_&&s.checkpoint_->shm_absent){
                    if(!s.checkpoint_effect_current()||(!s.checkpoint_initializing_&&!s.checkpoint_active_))
                        return s.checkpoint_refuse(*w->record,SQLITE_IOERR_LOCK);
                    const int rc=w->inner->pMethods->xLock(w->inner,lock);
                    if(rc!=SQLITE_OK)s.checkpoint_exclusive_current_=false;
                    else if(!w->record->wal&&lock==SQLITE_LOCK_EXCLUSIVE)s.checkpoint_exclusive_current_=true;
                    return rc;
                }
                return s.current()?w->inner->pMethods->xLock(w->inner,lock):SQLITE_IOERR_LOCK;};
            value.xUnlock=[](sqlite3_file* file,int lock)->int {
                auto* w=get(file);auto& s=*w->owner;
                if(!s.current())return SQLITE_IOERR_UNLOCK;
                if(s.checkpoint_&&s.checkpoint_->shm_absent){
                    // Clear before the real transition, including failure.
                    // An ever-held EXCLUSIVE bit cannot authorize later writes.
                    if(s.checkpoint_->worker!=std::this_thread::get_id())return SQLITE_IOERR_UNLOCK;
                    s.checkpoint_exclusive_current_=false;
                }
                return s.current()?w->inner->pMethods->xUnlock(w->inner,lock):SQLITE_IOERR_UNLOCK;};
            value.xCheckReservedLock=[](sqlite3_file* file,int* result)->int {
                auto* w=get(file);return w->owner->current()?w->inner->pMethods->xCheckReservedLock(w->inner,result):SQLITE_IOERR_CHECKRESERVEDLOCK;};
            value.xSectorSize=[](sqlite3_file* file)->int {
                auto* w=get(file);return w->owner->current()?w->inner->pMethods->xSectorSize(w->inner):0;};
            value.xDeviceCharacteristics=[](sqlite3_file* file)->int {
                auto* w=get(file);return w->owner->current()?w->inner->pMethods->xDeviceCharacteristics(w->inner):0;};
            if(at>=1) {
                value.xShmMap=[](sqlite3_file* file,int page,int size,int extend,void volatile** result)->int {
                    auto* w=get(file);auto& s=*w->owner;
                    if(!s.current())return SQLITE_IOERR_SHMMAP;
                    if(s.checkpoint_){
                        if(s.checkpoint_->shm_absent){if(result)*result=nullptr;return s.checkpoint_refuse(*w->record,SQLITE_IOERR_SHMMAP);}
                        if(!s.checkpoint_effect_current()||(!s.checkpoint_active_&&!s.checkpoint_initializing_)||page<0||size<=0||
                           static_cast<sqlite3_int64>(page)+1> s.checkpoint_->shm.st_size/static_cast<sqlite3_int64>(size))
                            return s.checkpoint_refuse(*w->record,SQLITE_IOERR_SHMSIZE);
                    }
                    return s.current()&&w->inner->pMethods->xShmMap?
                        w->inner->pMethods->xShmMap(w->inner,page,size,extend,result):SQLITE_IOERR_SHMMAP;};
                value.xShmLock=[](sqlite3_file* file,int offset,int count,int flags)->int {
                    auto* w=get(file);if(!w->owner->current())return SQLITE_IOERR_SHMLOCK;
                    if(w->owner->checkpoint_&&w->owner->checkpoint_->shm_absent)return w->owner->checkpoint_refuse(*w->record,SQLITE_IOERR_SHMLOCK);
                    return w->owner->current()&&w->inner->pMethods->xShmLock?
                        w->inner->pMethods->xShmLock(w->inner,offset,count,flags):SQLITE_IOERR_SHMLOCK;};
                value.xShmBarrier=[](sqlite3_file* file) {
                    auto* w=get(file);if(!w->owner->current())return;
                    if(w->owner->checkpoint_&&w->owner->checkpoint_->shm_absent){w->owner->checkpoint_refuse(*w->record,SQLITE_IOERR_SHMLOCK);return;}
                    if(w->owner->current()&&w->inner->pMethods->xShmBarrier)w->inner->pMethods->xShmBarrier(w->inner);};
                value.xShmUnmap=[](sqlite3_file* file,int remove)->int {
                    auto* w=get(file);if(!w->owner->current())return SQLITE_IOERR_SHMOPEN;
                    if(w->owner->checkpoint_&&w->owner->checkpoint_->shm_absent)return w->owner->checkpoint_refuse(*w->record,SQLITE_IOERR_SHMOPEN);
                    return w->owner->current()&&w->inner->pMethods->xShmUnmap?
                        w->inner->pMethods->xShmUnmap(w->inner,w->owner->checkpoint_?0:remove):SQLITE_IOERR_SHMOPEN;};
            }
            if(at>=2) {
                value.xFetch=[](sqlite3_file* file,sqlite3_int64 offset,int size,void** result)->int {
                    auto* w=get(file);if(!w->owner->current())return SQLITE_IOERR_MMAP;
                    if(w->owner->checkpoint_||!w->inner->pMethods->xFetch){*result=nullptr;return SQLITE_OK;}
                    return w->inner->pMethods->xFetch(w->inner,offset,size,result);};
                value.xUnfetch=[](sqlite3_file* file,sqlite3_int64 offset,void* value)->int {
                    auto* w=get(file);if(!w->owner->current())return SQLITE_IOERR_MMAP;
                    return w->inner->pMethods->xUnfetch?w->inner->pMethods->xUnfetch(w->inner,offset,value):SQLITE_OK;};
            }
        }
        return out;
    }();
    return all[static_cast<std::size_t>(std::max(1,std::min(version,3))-1)];
}
inline void wal_extent_vfs::install_forwarders()noexcept {
    vfs_.xDelete=[](sqlite3_vfs* v,const char* name,int sync)->int {
        auto* s=get(v);if(!s->current())return SQLITE_IOERR_DELETE;
        if(s->checkpoint_)return SQLITE_IOERR_DELETE;
        {std::lock_guard lock(s->leaf_);if(name)for(std::size_t i=0;i<s->issued_members_;++i)
            if(s->oversized_original_[i]&&s->members_[i].wal_path==name)return SQLITE_IOERR_DELETE;}
        return s->base_->xDelete(s->base_,name,sync);};
    vfs_.xAccess=[](sqlite3_vfs* v,const char* name,int flags,int* result)->int {
        auto* s=get(v);return s->current()?s->base_->xAccess(s->base_,name,flags,result):SQLITE_IOERR_ACCESS;};
    vfs_.xFullPathname=[](sqlite3_vfs* v,const char* name,int size,char* out)->int {
        auto* s=get(v);return s->current()?s->base_->xFullPathname(s->base_,name,size,out):SQLITE_CANTOPEN;};
    vfs_.xDlOpen=[](sqlite3_vfs* v,const char* name)->void* {
        auto* s=get(v);return s->current()&&s->base_->xDlOpen?s->base_->xDlOpen(s->base_,name):nullptr;};
    vfs_.xDlError=[](sqlite3_vfs* v,int size,char* out) {
        auto* s=get(v);if(s->current()&&s->base_->xDlError)s->base_->xDlError(s->base_,size,out);else if(size>0)out[0]=0;};
    vfs_.xDlSym=[](sqlite3_vfs* v,void* library,const char* name)->void(*)(void) {
        auto* s=get(v);return s->current()&&s->base_->xDlSym?s->base_->xDlSym(s->base_,library,name):nullptr;};
    vfs_.xDlClose=[](sqlite3_vfs* v,void* library) {
        auto* s=get(v);if(s->current()&&s->base_->xDlClose)s->base_->xDlClose(s->base_,library);};
    vfs_.xRandomness=[](sqlite3_vfs* v,int size,char* out)->int {
        auto* s=get(v);return s->current()?s->base_->xRandomness(s->base_,size,out):0;};
    vfs_.xSleep=[](sqlite3_vfs* v,int microseconds)->int {
        auto* s=get(v);return s->current()?s->base_->xSleep(s->base_,microseconds):0;};
    vfs_.xCurrentTime=[](sqlite3_vfs* v,double* out)->int {
        auto* s=get(v);return s->current()?s->base_->xCurrentTime(s->base_,out):SQLITE_IOERR;};
    vfs_.xGetLastError=[](sqlite3_vfs* v,int size,char* out)->int {
        auto* s=get(v);return s->current()&&s->base_->xGetLastError?s->base_->xGetLastError(s->base_,size,out):0;};
    if(vfs_.iVersion>=2) vfs_.xCurrentTimeInt64=[](sqlite3_vfs* v,sqlite3_int64* out)->int {
        auto* s=get(v);if(!s->current())return SQLITE_IOERR;
        if(s->base_->xCurrentTimeInt64)return s->base_->xCurrentTimeInt64(s->base_,out);
        double value=0;const int rc=s->base_->xCurrentTime(s->base_,&value);if(rc==SQLITE_OK)*out=static_cast<sqlite3_int64>(value*86400000.0);return rc;};
    if(vfs_.iVersion>=3) {
        vfs_.xSetSystemCall=[](sqlite3_vfs* v,const char* name,sqlite3_syscall_ptr call)->int {
            auto* s=get(v);return s->current()&&!s->checkpoint_&&s->base_->xSetSystemCall?s->base_->xSetSystemCall(s->base_,name,call):SQLITE_NOTFOUND;};
        vfs_.xGetSystemCall=[](sqlite3_vfs* v,const char* name)->sqlite3_syscall_ptr {
            auto* s=get(v);return s->current()&&s->base_->xGetSystemCall?s->base_->xGetSystemCall(s->base_,name):nullptr;};
        vfs_.xNextSystemCall=[](sqlite3_vfs* v,const char* name)->const char* {
            auto* s=get(v);return s->current()&&s->base_->xNextSystemCall?s->base_->xNextSystemCall(s->base_,name):nullptr;};
    }
}
struct wal_extent_test_access {
    static std::shared_ptr<wal_extent_vfs> original(database& db) {
        db.require_creator_();return db.wal_extent_vfs_;
    }
    static bool attachment_guard_active(const std::shared_ptr<wal_extent_vfs>& original) noexcept {
        if(!original||!original->current())return false;
        sqlite3* native=nullptr;
        {std::lock_guard lock(original->leaf_);native=original->native_original_;}
        // Only pass original identity DATA to lookup; never dereference a closed
        // Native handle or infer resource retirement from this live guard.
        return wal_extent_vfs::requires_attachment_guard(native);
    }
    static int file_call(database& db,int operation,sqlite3_int64 value) {
        db.require_creator_();auto held=db.wal_extent_vfs_;if(!held||!db.db_)return SQLITE_NOTFOUND;
        store_physical_native_scope physical_call(db);
        if(!physical_call.entered())return SQLITE_MISUSE;
        auto* mutex=sqlite3_db_mutex(db.db_);sqlite3_mutex_enter(mutex);
        struct leave {sqlite3_mutex* value;~leave(){sqlite3_mutex_leave(value);}} unlock{mutex};
        sqlite3_file* actual=nullptr;
        {std::lock_guard lock(held->leaf_);
            for(std::size_t i=0;i<held->issued_;++i){auto& file=held->files_[i];
                if(file.wal&&file.open_returned&&file.open_result==SQLITE_OK&&file.has_methods&&!file.close_entered){actual=const_cast<sqlite3_file*>(file.original);break;}}
        }
        if(!actual)return SQLITE_NOTFOUND;
        if(operation==SQLITE_FCNTL_SIZE_HINT)return actual->pMethods->xFileControl(actual,operation,&value);
        if(operation==SQLITE_FCNTL_CHUNK_SIZE){int chunk=static_cast<int>(value);return actual->pMethods->xFileControl(actual,operation,&chunk);}
        return actual->pMethods->xTruncate(actual,value);
    }
};
} // namespace lattice::detail
