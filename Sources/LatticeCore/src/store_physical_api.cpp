#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "store_physical_api.hpp"
#include <lattice/db.hpp>
#include <atomic>
#include <mutex>

namespace lattice::detail {
struct store_physical_api::original {
    const std::uint64_t creator=database_native_process();
    store_physical_lifetime::pin held;
    original* next=nullptr;
    sqlite3* actual=nullptr; // lookup DATA only; never dereferenced or invoked
    std::uint64_t references=1;
    bool listed=false,current=false,completion_claimed=false,completion_returned=false;
    void* completion_cookie=nullptr;
    void(*completion)(void*)noexcept=nullptr;
    explicit original(const store_physical_lifetime::pin& pin):held(pin){}
};
namespace {
const std::uint64_t physical_api_creator=database_native_process();
std::mutex physical_api_leaf;
std::atomic<bool> physical_api_used{false};

}
store_physical_api::original* store_physical_api::head_=nullptr;
store_physical_api::original* store_physical_api::prepare_(const store_physical_lifetime::pin& held) {
    if(!held.current()||physical_api_creator!=database_native_process())
        throw db_error("physical API original requires its actual creator");
    auto* value=new original(held);
    if(value->creator!=database_native_process())
        throw db_error("physical API original changed process during admission"); // child abandons capsule
    if(!held.current()){delete value;throw db_error("physical API original lost its lease during admission");}
    return value;
}
bool store_physical_api::publish_(original* issued,sqlite3* actual,void* cookie,void(*completed)(void*)noexcept)noexcept {
    if(!issued||issued->creator!=database_native_process()||physical_api_creator!=database_native_process())return false;
    try {
        std::lock_guard lock(physical_api_leaf);
        if(issued->listed||!issued->held.current()||!completed)return false;
        // Only the actual new SQLite open return can supersede an address.
        // Old cleanup frames already carry their immutable original pin.
        for(auto* at=head_;at;at=at->next)
            if(at->actual==actual)at->current=false;
        issued->actual=actual;issued->completion_cookie=cookie;issued->completion=completed;
        issued->listed=true;issued->current=actual!=nullptr;
        issued->next=head_;head_=issued;
        physical_api_used.store(true,std::memory_order_release);return true;
    }catch(...){return false;} // Issuer retains unproved registration/lease custody.
}
void store_physical_api::abandon_unpublished_(original* issued)noexcept {
    if(!issued||issued->creator!=database_native_process())return;
    if(!issued->listed)delete issued; // called outside Native/VFS/registry leaves
}
bool store_physical_api::foreign_protected_()noexcept {
    return physical_api_creator!=database_native_process()&&physical_api_used.load(std::memory_order_acquire);
}
store_physical_api::ticket store_physical_api::capture_issued_(original* actual)noexcept {
    ticket result;result.creator=database_native_process();
    if(!actual||actual->creator!=result.creator||physical_api_creator!=result.creator){result.inherited=true;return result;}
    try{std::lock_guard lock(physical_api_leaf);
        if(actual->references==UINT64_MAX){result.inherited=true;return result;}
        ++actual->references;result.value=actual;
    }catch(...){result.inherited=true;}
    return result;
}
store_physical_api::ticket store_physical_api::capture_(sqlite3* actual)noexcept {
    ticket result;result.creator=database_native_process();
    if(physical_api_creator!=result.creator){result.inherited=physical_api_used.load(std::memory_order_acquire);return result;}
    try {
        std::lock_guard lock(physical_api_leaf);
        for(auto* at=head_;at;at=at->next)
            if(at->current&&at->actual==actual){
                if(at->references==UINT64_MAX){result.inherited=true;return result;}
                ++at->references;result.value=at;break;
            }
    }catch(...){result.inherited=true;}
    return result;
}
void store_physical_api::release_(ticket& held)noexcept {
    auto* actual=held.value;held.value=nullptr;
    if(!actual||held.creator!=database_native_process()||physical_api_creator!=database_native_process())return;
    // Observe actual DATA outside the registry leaf. The ticket itself keeps
    // this original alive throughout the independent snapshot and callback.
    const auto ended=store_physical_lifetime::observe_(actual->held).physical_ended;
    bool complete=false,destroy=false;
    try {
        {std::lock_guard lock(physical_api_leaf);
         if(ended&&actual->listed&&!actual->completion_claimed){
             auto** cursor=&head_;
             while(*cursor&&*cursor!=actual)cursor=&(*cursor)->next;
             if(*cursor==actual){*cursor=actual->next;actual->listed=false;actual->current=false;
                 actual->completion_claimed=true;--actual->references;complete=true;}
         }}
        // Exact fixed implementation callback only, after real C API return
        // and outside every metadata/Native/VFS leaf. Never user callbacks.
        if(complete&&actual->completion)actual->completion(actual->completion_cookie);
        if(held.creator!=database_native_process())return;
        {std::lock_guard lock(physical_api_leaf);
         if(complete)actual->completion_returned=true;
         if(!actual->references)return; // Impossible reference debt: retain capsule.
         --actual->references;
         destroy=actual->references==0&&actual->completion_returned;}
        if(destroy)delete actual;
    }catch(...){/* Preserve the actual registry/lease ending debt. */}
}
store_physical_api::ticket::~ticket()noexcept {store_physical_api::release_(*this);}
int store_physical_api::finalize(sqlite3_stmt* statement)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!statement)return sqlite3_finalize(nullptr);
    auto actual=capture_(sqlite3_db_handle(statement));
    if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return sqlite3_finalize(statement); // unresourced original unchanged
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_finalize(statement); // actual tail destructor runs AFTER this C API returns
}
int store_physical_api::step(sqlite3_stmt* statement,bool reset)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!statement)return reset?sqlite3_reset(nullptr):sqlite3_step(nullptr);
    auto actual=capture_(sqlite3_db_handle(statement));if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return reset?sqlite3_reset(statement):sqlite3_step(statement);
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    return reset?sqlite3_reset(statement):sqlite3_step(statement);
}
int store_physical_api::exec(sqlite3* owner,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return sqlite3_exec(owner,sql,callback,data,error);
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_exec(owner,sql,callback,data,error);
}
int store_physical_api::checkpoint(sqlite3* owner,const char* schema,int mode,int* log,int* done)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return sqlite3_wal_checkpoint_v2(owner,schema,mode,log,done);
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_wal_checkpoint_v2(owner,schema,mode,log,done);
}
int store_physical_api::blob_close(sqlite3_blob* blob,sqlite3* owner)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!blob)return sqlite3_blob_close(nullptr);
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return sqlite3_blob_close(blob);
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_blob_close(blob);
}
int store_physical_api::close(sqlite3* owner,bool deferred)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    if(!actual.value)return deferred?sqlite3_close_v2(owner):sqlite3_close(owner);
    store_physical_lifetime::api_return returned(actual.value->held);
    if(!returned.entered())return SQLITE_MISUSE;
    const int result=deferred?sqlite3_close_v2(owner):sqlite3_close(owner);
    store_physical_lifetime::native_close_returned_(actual.value->held,deferred,result);
    return result;
}
void store_physical_api::unresourced_open_returned(sqlite3* actual)noexcept {
    if(!actual||physical_api_creator!=database_native_process())return;
    try{std::lock_guard lock(physical_api_leaf);
        for(auto* at=head_;at;at=at->next)
            if(at->actual==actual)at->current=false;
    }catch(...){/* No resource grant is created by this denial-only observation. */}
}
int store_physical_finalize(sqlite3_stmt* actual)noexcept{return store_physical_api::finalize(actual);}
int store_physical_step(sqlite3_stmt* actual)noexcept{return store_physical_api::step(actual,false);}
int store_physical_reset(sqlite3_stmt* actual)noexcept{return store_physical_api::step(actual,true);}
int store_physical_exec(sqlite3* actual,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept{return store_physical_api::exec(actual,sql,callback,data,error);}
int store_physical_wal_checkpoint(sqlite3* actual,const char* schema,int mode,int* log,int* done)noexcept{return store_physical_api::checkpoint(actual,schema,mode,log,done);}
int store_physical_blob_close(sqlite3_blob* actual,sqlite3* owner)noexcept{return store_physical_api::blob_close(actual,owner);}
int store_physical_close(sqlite3* actual)noexcept{return store_physical_api::close(actual,false);}
int store_physical_close_v2(sqlite3* actual)noexcept{return store_physical_api::close(actual,true);}
void store_physical_unresourced_open_returned(sqlite3* actual)noexcept{store_physical_api::unresourced_open_returned(actual);}
}

#else
#include <lattice/store_physical_api.hpp>
namespace lattice::detail {
int store_physical_finalize(sqlite3_stmt* p)noexcept{return sqlite3_finalize(p);}
int store_physical_step(sqlite3_stmt* p)noexcept{return sqlite3_step(p);}
int store_physical_reset(sqlite3_stmt* p)noexcept{return sqlite3_reset(p);}
int store_physical_exec(sqlite3* p,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept{return sqlite3_exec(p,sql,callback,data,error);}
int store_physical_wal_checkpoint(sqlite3* p,const char* schema,int mode,int* log,int* done)noexcept{return sqlite3_wal_checkpoint_v2(p,schema,mode,log,done);}
int store_physical_blob_close(sqlite3_blob* p,sqlite3*)noexcept{return sqlite3_blob_close(p);}
int store_physical_close(sqlite3* p)noexcept{return sqlite3_close(p);}
int store_physical_close_v2(sqlite3* p)noexcept{return sqlite3_close_v2(p);}
void store_physical_unresourced_open_returned(sqlite3*)noexcept{}
}
#endif
