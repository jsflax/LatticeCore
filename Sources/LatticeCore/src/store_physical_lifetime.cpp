#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "store_physical_lifetime.hpp"
#include "store_physical_api.hpp"
#include <lattice/db.hpp>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <unistd.h>

namespace lattice::detail {
struct store_physical_lifetime::state {
    const std::uint64_t creator;
    std::atomic<std::uint64_t> pins{1};
    std::atomic<bool> unknown{false};
    std::atomic<bool> observer_policy_unsettled{false};
    std::mutex leaf;
    std::condition_variable changed;
    int lease=-1;
    bool open_entered=false,open_returned=false,connection_retired=false,native_sealed=false;
    bool attached_original=false,attach_returned=false,detach_returned=false;
    bool releasing=false,physical_ended=false,lease_close_returned=false;
    std::uint64_t active_api=0,active_native=0,active_vfs=0,builders=0,files=0,next_file=0;
    int first_native_close=-1,fallback_close=-1,first_file_close_error=0,lease_close_error=0,first_cleanup_error=0;
    bool main_entered=false;
    state(int actual,std::uint64_t issued_creator):creator(issued_creator),lease(actual){}
    ~state()noexcept=default; // Every descriptor ending is observed before delete.
    bool current()const noexcept{return creator==database_native_process();}
};
thread_local store_physical_lifetime::api_return* store_physical_lifetime::api_return::current_=nullptr;

bool store_physical_lifetime::retain_(state* original)noexcept {
    if(!original||!original->current())return false;
    auto count=original->pins.load(std::memory_order_relaxed);
    while(count&&count!=UINT64_MAX){
        if(original->pins.compare_exchange_weak(count,count+1,std::memory_order_acquire,std::memory_order_relaxed))return true;
    }
    original->unknown.store(true,std::memory_order_release);return false;
}
void store_physical_lifetime::release_(state* original)noexcept {
    // A child abandons the actual lease/metadata capsule; its inherited OS
    // descriptors remain until real child exit, not wrapper destruction.
    if(!original||!original->current())return;
    if(original->pins.fetch_sub(1,std::memory_order_acq_rel)!=1)return;
    // Unknown/pending Native resources quarantine the physical SH lease until
    // process exit even if every facade goes away. No false clean destructor.
    if(original->open_entered&&!original->physical_ended){
        original->pins.store(1,std::memory_order_release);return;
    }
    if(original->lease>=0){
        const int lease=std::exchange(original->lease,-1);
        errno=0;const int rc=::close(lease);const int error=rc<0?errno:0;
        if(!original->current())return;
        original->lease_close_returned=true;original->lease_close_error=error;
        if(rc!=0){
            original->unknown.store(true,std::memory_order_release);
            original->pins.store(1,std::memory_order_release);return;
        }
    }
    delete original;
}
store_physical_lifetime::pin::pin(const pin& other)noexcept {
    if(retain_(other.value_))value_=other.value_;
}
store_physical_lifetime::pin& store_physical_lifetime::pin::operator=(pin&& other)noexcept {
    if(this!=&other){auto* old=value_;value_=std::exchange(other.value_,nullptr);release_(old);}return *this;
}
store_physical_lifetime::pin::~pin()noexcept {release_(value_);}
bool store_physical_lifetime::pin::current()const noexcept {return value_&&value_->current();}
store_physical_lifetime::pin store_physical_lifetime::adopt_lease_(int actual) {
    if(actual<0)throw db_error("physical original lacks its acquired lease");
    // Issuer retains descriptor custody if allocation fails. The actual
    // issuer must recheck its creator after allocation and before effects.
    const auto creator=database_native_process();
    return pin(new state(actual,creator));
}
void store_physical_lifetime::settle_(state* original)noexcept {
    if(!original||!original->current())return;
    int lease=-1;
    try {
        {std::lock_guard lock(original->leaf);
         if(original->unknown.load(std::memory_order_acquire)||original->releasing||original->physical_ended||
            !original->open_returned||!original->connection_retired||original->active_api||original->active_native||original->active_vfs||
            original->builders||original->files||original->first_file_close_error||original->first_cleanup_error)return;
         original->releasing=true;lease=std::exchange(original->lease,-1);}
        if(lease<0){original->unknown.store(true,std::memory_order_release);return;}
        errno=0;const int rc=::close(lease);const int error=rc<0?errno:0; // never a MAIN descriptor
        if(!original->current())return;
        {std::lock_guard lock(original->leaf);
         original->lease_close_returned=true;original->lease_close_error=error;
         if(rc==0)original->physical_ended=true;
         else original->unknown.store(true,std::memory_order_release);}
        original->changed.notify_all();
    }catch(...){original->unknown.store(true,std::memory_order_release);}
}
bool store_physical_lifetime::api_return::contains_(state* actual)noexcept {
    for(auto* frame=current_;frame;frame=frame->previous_)
        if(frame->entered_&&!frame->vfs_&&frame->held_.value_==actual)return true;
    return false;
}
store_physical_lifetime::api_return::api_return(const pin& actual,bool vfs,bool closing_vfs)noexcept
    :held_(actual),vfs_(vfs) {
    if(!held_.current())return;
    auto& original=*held_.value_;
    try {
        std::lock_guard lock(original.leaf);
        auto& count=vfs_?original.active_vfs:original.active_api;
        if(count==UINT64_MAX||original.physical_ended||original.releasing){original.unknown.store(true,std::memory_order_release);return;}
        ++count;entered_=true;
        // An untracked external/raw SQLite return cannot prove a complete
        // ending. Preserve its real cleanup call, but quarantine this lease.
        if(closing_vfs&&!contains_(&original))original.unknown.store(true,std::memory_order_release);
    }catch(...){original.unknown.store(true,std::memory_order_release);return;}
    // VFS frames participate in the SAME actual nesting observation. They
    // remain excluded by contains_ from SQL continuation/admission authority.
    previous_=current_;current_=this;
}
store_physical_lifetime::api_return::~api_return()noexcept {
    if(!entered_||!held_.current())return;
    auto* original=held_.value_;
    if(current_!=this){original->unknown.store(true,std::memory_order_release);return;}
    current_=previous_;
    try {
        {std::lock_guard lock(original->leaf);
         auto& count=vfs_?original->active_vfs:original->active_api;
         if(!count){original->unknown.store(true,std::memory_order_release);return;}--count;}
        settle_(original);original->changed.notify_all();
    }catch(...){original->unknown.store(true,std::memory_order_release);}
    // Own held-pin tail and all counter/leaf work precede a cross-original
    // completion/drain. No API/Native/VFS stack may still be active there.
    held_=pin{};
    store_physical_api::returned_tail();
}
bool store_physical_lifetime::native_scope_entered_(const pin& actual,bool continuation)noexcept {
    if(!actual.current())return false;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        // A known ending/seal is ordinary refusal, not unknown cleanup debt.
        // Only an actual still-live TLS frame for THIS original can continue.
        if(s.physical_ended||s.releasing||(s.native_sealed&&!continuation))return false;
        if(s.active_native==UINT64_MAX){s.unknown.store(true,std::memory_order_release);return false;}
        ++s.active_native;return true;
    }catch(...){s.unknown.store(true,std::memory_order_release);return false;}
}
bool store_physical_lifetime::seal_native_and_wait_(const pin& actual,std::chrono::steady_clock::time_point end)noexcept {
    if(!actual.current())return false;auto& s=*actual.value_;
    if(api_return::contains_(&s))return false; // actual same-call reentry, no selfwait
    try{
        std::unique_lock lock(s.leaf);s.native_sealed=true;
        return s.changed.wait_until(lock,end,[&]{return s.active_native==0||s.unknown.load(std::memory_order_acquire);})&&
            !s.unknown.load(std::memory_order_acquire)&&s.active_native==0;
    }catch(...){s.unknown.store(true,std::memory_order_release);return false;}
}
bool store_physical_lifetime::observer_policy_unsettled_(const pin& actual)noexcept {
    return !actual.current()||actual.value_->observer_policy_unsettled.load(std::memory_order_acquire);
}
void store_physical_lifetime::note_observer_policy_unsettled_(const pin& actual)noexcept {
    if(actual.current())actual.value_->observer_policy_unsettled.store(true,std::memory_order_release);
}
void store_physical_lifetime::native_scope_returned_(const pin& actual)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{{std::lock_guard lock(s.leaf);
        if(!s.active_native){s.unknown.store(true,std::memory_order_release);return;}--s.active_native;}
        settle_(&s);s.changed.notify_all();
    }catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::native_open_entered_(const pin& actual)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);if(s.open_entered)s.unknown.store(true,std::memory_order_release);else s.open_entered=true;}
    catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::native_open_returned_(const pin& actual,bool present)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);if(!s.open_entered||s.open_returned)s.unknown.store(true,std::memory_order_release);
        else{s.open_returned=true;if(!present)s.connection_retired=true;}}
    catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
void store_physical_lifetime::native_close_returned_(const pin& actual,bool deferred,int result)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(!s.open_returned){s.unknown.store(true,std::memory_order_release);return;}
        auto& recorded=deferred?s.fallback_close:s.first_native_close;
        if(recorded!=-1){s.unknown.store(true,std::memory_order_release);return;}
        recorded=result;
        // First BUSY stays BUSY. A genuine later close_v2 return plus all real
        // file/API endings is independent physical proof, never a rewritten rc.
        if(result==SQLITE_OK)s.connection_retired=true;
        else if(deferred||result!=SQLITE_BUSY)s.unknown.store(true,std::memory_order_release);
    }catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
void store_physical_lifetime::attachment_entered_(const pin& actual)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(s.open_entered||s.open_returned||s.attached_original)s.unknown.store(true,std::memory_order_release);
        else{s.attached_original=true;s.open_entered=true;}}
    catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::attachment_returned_(const pin& actual,bool entered,bool clean)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(s.open_returned||s.attach_returned||s.main_entered&&!entered){s.unknown.store(true,std::memory_order_release);return;}
        if(!entered){
            // The private producer never entered its literal ATTACH. No VFS
            // call was eligible; this is a no-effect reservation retirement.
            if(s.open_entered||s.builders||s.files){s.unknown.store(true,std::memory_order_release);return;}
            s.attached_original=true;s.open_entered=true;s.connection_retired=true;
        }else if(!s.attached_original||!s.open_entered){s.unknown.store(true,std::memory_order_release);return;}
        s.attach_returned=true;s.open_returned=true;
        // Failed entered ATTACH is retained until real connection close; no
        // alias absence or synthetic successful close is inferred from error.
        (void)clean;
    }catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
void store_physical_lifetime::detachment_returned_(const pin& actual,bool clean)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(!s.attached_original||!s.attach_returned||s.detach_returned){s.unknown.store(true,std::memory_order_release);return;}
        if(clean){s.detach_returned=true;s.connection_retired=true;}
        // A failed DETACH leaves the actual original current. A subsequent
        // correctly admitted retry or whole-connection close can still end it.
    }catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
store_physical_lifetime::file_original store_physical_lifetime::file_open_entered_(const pin& actual,file_kind kind)noexcept {
    file_original result;if(!actual.current())return result;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(s.next_file==UINT64_MAX||s.builders==UINT64_MAX||s.releasing||s.physical_ended||
           (kind==file_kind::main&&s.main_entered)||
           (s.connection_retired&&!api_return::contains_(&s))){
            s.unknown.store(true,std::memory_order_release);return result;}
        result.owner_=&s;result.serial_=++s.next_file;result.kind_=kind;++s.builders;
        if(kind==file_kind::main)s.main_entered=true;
    }catch(...){s.unknown.store(true,std::memory_order_release);}return result;
}
void store_physical_lifetime::file_open_returned_(const pin& actual,file_original& issued,bool methods)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.serial_||issued.returned_||!s.builders||s.files==UINT64_MAX){s.unknown.store(true,std::memory_order_release);return;}
        --s.builders;issued.returned_=true;issued.methods_=methods;
        if(methods)++s.files;else issued.closed_=true;
    }catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
void store_physical_lifetime::file_close_returned_(const pin& actual,file_original& issued,int result)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.serial_||!issued.returned_||!issued.methods_||issued.closed_||!s.files){s.unknown.store(true,std::memory_order_release);return;}
        issued.closed_=true;
        if(issued.shm_mapped_||issued.fetches_)s.unknown.store(true,std::memory_order_release);
        if(result!=SQLITE_OK){if(!s.first_file_close_error)s.first_file_close_error=result;return;}
        --s.files;
    }catch(...){s.unknown.store(true,std::memory_order_release);}settle_(&s);
}
void store_physical_lifetime::shm_map_returned_(const pin& actual,file_original& issued,bool mapped,int rc)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.returned_||issued.closed_){s.unknown.store(true,std::memory_order_release);return;}
        if(rc!=SQLITE_OK)s.unknown.store(true,std::memory_order_release);
        else if(mapped)issued.shm_mapped_=true;
    }catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::shm_unmap_returned_(const pin& actual,file_original& issued,int rc)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.returned_||issued.closed_){s.unknown.store(true,std::memory_order_release);return;}
        if(rc==SQLITE_OK)issued.shm_mapped_=false;
        else if(!s.first_cleanup_error)s.first_cleanup_error=rc;
    }catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::fetch_returned_(const pin& actual,file_original& issued,bool mapped,int rc)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.returned_||issued.closed_){s.unknown.store(true,std::memory_order_release);return;}
        if(rc!=SQLITE_OK)s.unknown.store(true,std::memory_order_release);
        else if(mapped){if(issued.fetches_==UINT64_MAX)s.unknown.store(true,std::memory_order_release);else ++issued.fetches_;}
    }catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::unfetch_returned_(const pin& actual,file_original& issued,bool all,int rc)noexcept {
    if(!actual.current())return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);
        if(issued.owner_!=&s||!issued.returned_||issued.closed_){s.unknown.store(true,std::memory_order_release);return;}
        if(rc!=SQLITE_OK){if(!s.first_cleanup_error)s.first_cleanup_error=rc;return;}
        // NULL is SQLite's request to discard an already-unpinned mapping; it
        // cannot manufacture settlement of still-outstanding fetched pages.
        if(all){if(issued.fetches_)s.unknown.store(true,std::memory_order_release);}
        else if(!issued.fetches_)s.unknown.store(true,std::memory_order_release);else --issued.fetches_;
    }catch(...){s.unknown.store(true,std::memory_order_release);}
}
void store_physical_lifetime::cleanup_returned_(const pin& actual,int rc)noexcept {
    if(!actual.current()||rc==SQLITE_OK)return;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);if(!s.first_cleanup_error)s.first_cleanup_error=rc;}
    catch(...){s.unknown.store(true,std::memory_order_release);}
}
store_physical_lifetime::observation store_physical_lifetime::observe_(const pin& actual)noexcept {
    observation out;if(!actual.current())return out;auto& s=*actual.value_;
    try{std::lock_guard lock(s.leaf);out.current=true;out.open_returned=s.open_returned;out.connection_retired=s.connection_retired;out.native_sealed=s.native_sealed;
        out.attached_original=s.attached_original;out.attach_returned=s.attach_returned;out.detach_returned=s.detach_returned;
        out.physical_ended=s.physical_ended;out.unknown=s.unknown.load(std::memory_order_acquire);out.lease_close_returned=s.lease_close_returned;
        out.active_api=s.active_api;out.active_native=s.active_native;out.active_vfs=s.active_vfs;out.builders=s.builders;out.files=s.files;
        out.first_native_close=s.first_native_close;out.fallback_close=s.fallback_close;
        out.first_file_close_error=s.first_file_close_error;out.lease_close_error=s.lease_close_error;out.first_cleanup_error=s.first_cleanup_error;}
    catch(...){out.unknown=true;}return out;
}
bool store_physical_lifetime::wait_until_(const pin& actual,std::chrono::steady_clock::time_point end)noexcept {
    if(!actual.current())return false;auto& s=*actual.value_;
    if(api_return::contains_(&s))return false; // cannot await this actual calling API tail
    try{std::unique_lock lock(s.leaf);return s.changed.wait_until(lock,end,[&]{return s.physical_ended||s.unknown.load(std::memory_order_acquire);})&&s.physical_ended;}
    catch(...){return false;}
}
} // namespace lattice::detail

#endif
