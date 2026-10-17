#pragma once
#include "../../Sources/LatticeCore/src/database_open_test_probe.hpp"
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <type_traits>

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
namespace readonly_open_observation {
// Test-only, finite storage. Callbacks neither format/log nor retain this TLS
// sink in a VFS/file. A returned database may outlive the observation scope.
struct Record {
    const char* operation=nullptr;
    int result=0,observed_errno=0;
    bool result_available=true,output_available=false,path_available=false,truncated=false;
    std::array<sqlite3_int64,4> inputs{};
    sqlite3_int64 output=0;
    std::array<char,1024> text{};
    size_t text_size=0;
};
struct Sink {
    std::array<Record,64> records{};
    size_t count=0;
    bool overflow=false,file_layer_unavailable=false;
};
inline thread_local Sink* current_sink=nullptr;
inline thread_local bool scope_active=false;
inline void append(Record& record,const char* text,size_t maximum=std::numeric_limits<size_t>::max()) noexcept {
    if(!text)return;
    size_t i=0;
    while(i<maximum&&record.text_size<record.text.size()) {
        if(!text[i])return;
        record.text[record.text_size++]=text[i++];
    }
    record.truncated=true;
}
inline Record* record(const char* operation,const char* path,int rc,int saved_errno,
                      std::array<sqlite3_int64,4> inputs={}) noexcept {
    auto* sink=current_sink;if(!sink)return nullptr;
    if(sink->count==sink->records.size()){sink->overflow=true;return nullptr;}
    auto& value=sink->records[sink->count++];value.operation=operation;value.result=rc;
    value.observed_errno=saved_errno;value.inputs=inputs;value.path_available=path!=nullptr;append(value,path);return &value;
}
inline void unavailable_file() noexcept {if(current_sink)current_sink->file_layer_unavailable=true;}

struct FileObservation {
    const sqlite3_io_methods* parent_methods;
    sqlite3_io_methods methods;
    char path[1024];
    bool path_truncated,path_available;
};
static_assert(std::is_trivial_v<FileObservation> && std::is_standard_layout_v<FileObservation>);
static_assert(alignof(FileObservation)<=8);
using Symbol=void(*)(void);
struct Context {
    sqlite3_vfs vfs{};
    sqlite3_vfs* parent=nullptr;
    size_t offset=0;
    bool initialized=false;
};
// Trivial static storage has no teardown that can invalidate a retained reader.
inline Context& context() noexcept {static Context value;return value;}
inline std::mutex& registration_mutex(){static std::mutex value;return value;}
inline Context& self(sqlite3_vfs* vfs) noexcept {return *static_cast<Context*>(vfs->pAppData);}
inline FileObservation& metadata(sqlite3_file* file) noexcept {
    return *std::launder(reinterpret_cast<FileObservation*>(reinterpret_cast<unsigned char*>(file)+context().offset));
}
inline bool install_methods(sqlite3_file* file,FileObservation& observation) noexcept;

template<class F> inline auto parent_call(sqlite3_file* file,F function,bool closing=false) noexcept {
    auto& observation=metadata(file);const auto* methods=observation.parent_methods;
    file->pMethods=methods;
    if constexpr(std::is_void_v<decltype(function(*methods))>) {
        function(*methods);const int saved=errno;
        if(!closing){if(file->pMethods==methods)file->pMethods=&observation.methods;else install_methods(file,observation);}
        errno=saved;
    } else {
        const auto result=function(*methods);const int saved=errno;
        if(!closing){if(file->pMethods==methods)file->pMethods=&observation.methods;else install_methods(file,observation);}
        errno=saved;return result;
    }
}
inline Record* file_record(sqlite3_file* file,const char* operation,int rc,int saved,
                           std::array<sqlite3_int64,4> inputs={}) noexcept {
    auto& observation=metadata(file);auto* value=record(operation,observation.path,rc,saved,inputs);
    if(value){value->truncated|=observation.path_truncated;value->path_available=observation.path_available;}
    return value;
}
inline int close_file(sqlite3_file* file) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xClose(file);},true),saved=errno;
    file_record(file,"close",rc,saved);errno=saved;return rc;
}
inline int read_file(sqlite3_file* file,void* buffer,int amount,sqlite3_int64 offset) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xRead(file,buffer,amount,offset);}),saved=errno;
    file_record(file,"read",rc,saved,{amount,offset});errno=saved;return rc;
}
inline int write_file(sqlite3_file* file,const void* buffer,int amount,sqlite3_int64 offset) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xWrite(file,buffer,amount,offset);}),saved=errno;
    file_record(file,"write",rc,saved,{amount,offset});errno=saved;return rc;
}
inline int truncate_file(sqlite3_file* file,sqlite3_int64 size) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xTruncate(file,size);}),saved=errno;
    file_record(file,"truncate",rc,saved,{size});errno=saved;return rc;
}
inline int sync_file(sqlite3_file* file,int flags) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xSync(file,flags);}),saved=errno;
    file_record(file,"sync",rc,saved,{flags});errno=saved;return rc;
}
inline int size_file(sqlite3_file* file,sqlite3_int64* size) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xFileSize(file,size);}),saved=errno;
    if(auto* value=file_record(file,"size",rc,saved);value&&rc==SQLITE_OK&&size){value->output_available=true;value->output=*size;}
    errno=saved;return rc;
}
inline int lock_file(sqlite3_file* file,int level) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xLock(file,level);}),saved=errno;
    file_record(file,"lock",rc,saved,{level});errno=saved;return rc;
}
inline int unlock_file(sqlite3_file* file,int level) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xUnlock(file,level);}),saved=errno;
    file_record(file,"unlock",rc,saved,{level});errno=saved;return rc;
}
inline int reserved_file(sqlite3_file* file,int* locked) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xCheckReservedLock(file,locked);}),saved=errno;
    if(auto* value=file_record(file,"reservedLock",rc,saved);value&&rc==SQLITE_OK&&locked){value->output_available=true;value->output=*locked;}
    errno=saved;return rc;
}
inline int control_file(sqlite3_file* file,int operation,void* argument) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xFileControl(file,operation,argument);}),saved=errno;
    // No interpretation of arbitrary opcode-specific input/output memory.
    file_record(file,"fileControl",rc,saved,{operation});errno=saved;return rc;
}
inline int sector_file(sqlite3_file* file) noexcept {
    const int result=parent_call(file,[&](const auto& m){return m.xSectorSize(file);}),saved=errno;
    if(auto* value=file_record(file,"sectorSize",0,saved)){value->result_available=false;value->output_available=true;value->output=result;}
    errno=saved;return result;
}
inline int characteristics_file(sqlite3_file* file) noexcept {
    const int result=parent_call(file,[&](const auto& m){return m.xDeviceCharacteristics(file);}),saved=errno;
    if(auto* value=file_record(file,"deviceCharacteristics",0,saved)){value->result_available=false;value->output_available=true;value->output=result;}
    errno=saved;return result;
}
inline int shm_map(sqlite3_file* file,int page,int size,int extend,void volatile** out) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xShmMap(file,page,size,extend,out);}),saved=errno;
    if(auto* value=file_record(file,"shmMap",rc,saved,{page,size,extend});value&&rc==SQLITE_OK&&out){value->output_available=true;value->output=*out!=nullptr;}
    errno=saved;return rc;
}
inline int shm_lock(sqlite3_file* file,int offset,int count,int flags) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xShmLock(file,offset,count,flags);}),saved=errno;
    file_record(file,"shmLock",rc,saved,{offset,count,flags});errno=saved;return rc;
}
inline void shm_barrier(sqlite3_file* file) noexcept {
    parent_call(file,[&](const auto& m){m.xShmBarrier(file);});const int saved=errno;
    if(auto* value=file_record(file,"shmBarrier",0,saved))value->result_available=false;
    errno=saved;
}
inline int shm_unmap(sqlite3_file* file,int remove) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xShmUnmap(file,remove);}),saved=errno;
    file_record(file,"shmUnmap",rc,saved,{remove});errno=saved;return rc;
}
inline int fetch_file(sqlite3_file* file,sqlite3_int64 offset,int amount,void** out) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xFetch(file,offset,amount,out);}),saved=errno;
    if(auto* value=file_record(file,"fetch",rc,saved,{offset,amount});value&&rc==SQLITE_OK&&out){value->output_available=true;value->output=*out!=nullptr;}
    errno=saved;return rc;
}
inline int unfetch_file(sqlite3_file* file,sqlite3_int64 offset,void* pointer) noexcept {
    const int rc=parent_call(file,[&](const auto& m){return m.xUnfetch(file,offset,pointer);}),saved=errno;
    file_record(file,"unfetch",rc,saved,{offset});errno=saved;return rc;
}
inline bool install_methods(sqlite3_file* file,FileObservation& observation) noexcept {
    const auto* m=file->pMethods;
    if(!m)return false; // Preserve the parent's failed-open/close postimage.
    if(m->iVersion<1||m->iVersion>3||!m->xClose||!m->xRead||!m->xWrite||!m->xTruncate||!m->xSync||
       !m->xFileSize||!m->xLock||!m->xUnlock||!m->xCheckReservedLock||!m->xFileControl||!m->xSectorSize||!m->xDeviceCharacteristics) {
        unavailable_file();return false; // Parent table remains intact, with no retry/error injection.
    }
    observation.parent_methods=m;auto& out=observation.methods;out={};out.iVersion=m->iVersion;
    out.xClose=close_file;out.xRead=read_file;out.xWrite=write_file;out.xTruncate=truncate_file;
    out.xSync=sync_file;out.xFileSize=size_file;out.xLock=lock_file;out.xUnlock=unlock_file;
    out.xCheckReservedLock=reserved_file;out.xFileControl=control_file;out.xSectorSize=sector_file;out.xDeviceCharacteristics=characteristics_file;
    if(m->iVersion>=2){out.xShmMap=m->xShmMap?shm_map:nullptr;out.xShmLock=m->xShmLock?shm_lock:nullptr;
        out.xShmBarrier=m->xShmBarrier?shm_barrier:nullptr;out.xShmUnmap=m->xShmUnmap?shm_unmap:nullptr;}
    if(m->iVersion>=3){out.xFetch=m->xFetch?fetch_file:nullptr;out.xUnfetch=m->xUnfetch?unfetch_file:nullptr;}
    file->pMethods=&out;return true;
}
inline int open_file(sqlite3_vfs* vfs,const char* path,sqlite3_file* file,int flags,int* out_flags) noexcept {
    auto& c=self(vfs);FileObservation* observation=nullptr;
    auto* trailing=reinterpret_cast<unsigned char*>(file)+c.offset;
    if(reinterpret_cast<uintptr_t>(trailing)%alignof(FileObservation)==0) {
        observation=::new(static_cast<void*>(trailing)) FileObservation{};
        observation->path_available=path!=nullptr;
        if(path){size_t n=0;while(n+1<sizeof(observation->path)&&path[n]){observation->path[n]=path[n];++n;}
            observation->path[n]=0;observation->path_truncated=path[n]!=0;}
    } else unavailable_file(); // Parent prefix still has its original alignment and capacity.
    const int rc=c.parent->xOpen(c.parent,path,file,flags,out_flags),saved=errno;
    if(auto* value=record("open",path,rc,saved,{flags});value&&rc==SQLITE_OK&&out_flags){value->output_available=true;value->output=*out_flags;}
    if(observation)install_methods(file,*observation);
    errno=saved;return rc;
}
inline int access_file(sqlite3_vfs* vfs,const char* path,int flags,int* exists) noexcept {
    auto* p=self(vfs).parent;const int rc=p->xAccess(p,path,flags,exists),saved=errno;
    if(auto* value=record("access",path,rc,saved,{flags});value&&rc==SQLITE_OK&&exists){value->output_available=true;value->output=*exists;}
    errno=saved;return rc;
}
inline int full_path(sqlite3_vfs* vfs,const char* path,int capacity,char* out) noexcept {
    auto* p=self(vfs).parent;const int rc=p->xFullPathname(p,path,capacity,out),saved=errno;
    bool success=rc==SQLITE_OK;
#ifdef SQLITE_OK_SYMLINK
    success=success||rc==SQLITE_OK_SYMLINK;
#endif
    if(auto* value=record("fullPathname",path,rc,saved,{capacity});value&&success&&out&&capacity>0){
        value->output_available=true;append(*value," -> ");append(*value,out,static_cast<size_t>(capacity));}
    errno=saved;return rc;
}
inline bool initialize(Context& c,sqlite3_vfs* parent) noexcept {
    if(c.initialized)return c.parent==parent;
    if(!parent||parent->iVersion<1||parent->iVersion>3||parent->szOsFile<static_cast<int>(sizeof(sqlite3_file))||
       parent->mxPathname<=0||!parent->xOpen||!parent->xDelete||!parent->xAccess||!parent->xFullPathname||
       !parent->xRandomness||!parent->xSleep||!parent->xCurrentTime)return false;
    constexpr auto alignment=alignof(FileObservation);const auto size=static_cast<size_t>(parent->szOsFile);
    if(size>static_cast<size_t>(std::numeric_limits<int>::max())-(alignment-1)-sizeof(FileObservation))return false;
    c.parent=parent;c.offset=(size+alignment-1)&~(alignment-1);auto& v=c.vfs;
    v.iVersion=parent->iVersion;v.szOsFile=static_cast<int>(c.offset+sizeof(FileObservation));v.mxPathname=parent->mxPathname;
    v.zName="lattice-copied-readonly-observation-v1";v.pAppData=&c;
    v.xOpen=open_file;v.xAccess=access_file;v.xFullPathname=full_path;
    v.xDelete=[](sqlite3_vfs* v,const char* path,int sync)noexcept{auto* p=self(v).parent;const int rc=p->xDelete(p,path,sync),saved=errno;record("delete",path,rc,saved,{sync});errno=saved;return rc;};
    if(parent->xDlOpen)v.xDlOpen=[](sqlite3_vfs* v,const char* path)noexcept{auto* p=self(v).parent;return p->xDlOpen(p,path);};
    if(parent->xDlError)v.xDlError=[](sqlite3_vfs* v,int n,char* out)noexcept{auto* p=self(v).parent;p->xDlError(p,n,out);};
    if(parent->xDlSym)v.xDlSym=[](sqlite3_vfs* v,void* handle,const char* name)noexcept->Symbol{auto* p=self(v).parent;return p->xDlSym(p,handle,name);};
    if(parent->xDlClose)v.xDlClose=[](sqlite3_vfs* v,void* handle)noexcept{auto* p=self(v).parent;p->xDlClose(p,handle);};
    v.xRandomness=[](sqlite3_vfs* v,int n,char* out)noexcept{auto* p=self(v).parent;return p->xRandomness(p,n,out);};
    v.xSleep=[](sqlite3_vfs* v,int micros)noexcept{auto* p=self(v).parent;return p->xSleep(p,micros);};
    v.xCurrentTime=[](sqlite3_vfs* v,double* out)noexcept{auto* p=self(v).parent;return p->xCurrentTime(p,out);};
    if(parent->xGetLastError)v.xGetLastError=[](sqlite3_vfs* v,int n,char* out)noexcept{auto* p=self(v).parent;return p->xGetLastError(p,n,out);};
    if(parent->iVersion>=2&&parent->xCurrentTimeInt64)v.xCurrentTimeInt64=[](sqlite3_vfs* v,sqlite3_int64* out)noexcept{auto* p=self(v).parent;return p->xCurrentTimeInt64(p,out);};
    if(parent->iVersion>=3){
        if(parent->xSetSystemCall)v.xSetSystemCall=[](sqlite3_vfs* v,const char* name,sqlite3_syscall_ptr f)noexcept{auto* p=self(v).parent;return p->xSetSystemCall(p,name,f);};
        if(parent->xGetSystemCall)v.xGetSystemCall=[](sqlite3_vfs* v,const char* name)noexcept{auto* p=self(v).parent;return p->xGetSystemCall(p,name);};
        if(parent->xNextSystemCall)v.xNextSystemCall=[](sqlite3_vfs* v,const char* name)noexcept{auto* p=self(v).parent;return p->xNextSystemCall(p,name);};
    }
    c.initialized=true;return true;
}

class Scope {
    Sink sink;
    lattice::detail::database_open_test_hooks::selector selector;
    lattice::detail::database_open_test_hooks::selector* previous_selector=nullptr;
    Sink* previous_sink=nullptr;
    bool previous_scope_active=false;
    std::unique_lock<std::mutex> registration;
    sqlite3_vfs* parent=nullptr;
    bool registered=false,armed=false,finished=false,available=false,cleanup_ok=true;
    int registration_result=SQLITE_OK,unregistration_result=SQLITE_OK;
    const char* reason="notAttempted";
    void remove_registration() noexcept {
        if(registered){auto& c=context();unregistration_result=sqlite3_vfs_unregister(&c.vfs);registered=false;
            cleanup_ok=unregistration_result==SQLITE_OK&&sqlite3_vfs_find(c.vfs.zName)==nullptr&&sqlite3_vfs_find(nullptr)==parent;}
        if(registration.owns_lock())registration.unlock();
    }
public:
    explicit Scope(const std::string& path) noexcept
        :selector{path.c_str(),SQLITE_OPEN_FULLMUTEX|SQLITE_OPEN_READONLY|SQLITE_OPEN_URI,nullptr} {
        const int saved=errno;
        previous_selector=lattice::detail::database_open_test_hooks::current;previous_sink=current_sink;
        previous_scope_active=scope_active;scope_active=true;
        // Even an unavailable nested observation uses the original default
        // once, rather than accidentally consuming an outer matching selector.
        lattice::detail::database_open_test_hooks::current=nullptr;current_sink=nullptr;armed=true;
        // std::mutex::try_lock is not safe when this thread already owns it.
        if(previous_scope_active){reason="nestedScope";errno=saved;return;}
        try {
            registration=std::unique_lock<std::mutex>(registration_mutex(),std::try_to_lock);
            auto& c=context();parent=sqlite3_vfs_find(nullptr);
            if(!registration.owns_lock()){reason="registrationBusy";errno=saved;return;}
            if(!initialize(c,parent)){reason="unsupportedOrChangedParent";errno=saved;return;}
            if(sqlite3_vfs_find(c.vfs.zName)){reason="nameOccupied";errno=saved;return;}
            registration_result=sqlite3_vfs_register(&c.vfs,0);
            if(registration_result!=SQLITE_OK){reason="registrationFailed";errno=saved;return;}
            registered=true;
            if(sqlite3_vfs_find(nullptr)!=parent||sqlite3_vfs_find(c.vfs.zName)!=&c.vfs){
                reason="registryInterference";remove_registration();errno=saved;return;}
            selector.vfs=c.vfs.zName;lattice::detail::database_open_test_hooks::current=&selector;current_sink=&sink;
            available=true;reason="armed";
        } catch(...) {reason="setupException";remove_registration();} // Original open still executes once.
        errno=saved;
    }
    Scope(const Scope&)=delete;
    Scope& operator=(const Scope&)=delete;
    ~Scope() noexcept {finish();}
    void finish() noexcept {
        if(finished)return;const int saved=errno;finished=true;
        if(armed){lattice::detail::database_open_test_hooks::current=previous_selector;current_sink=previous_sink;scope_active=previous_scope_active;armed=false;}
        remove_registration();errno=saved;
    }
    std::string failure_report() const {
        size_t parent_name_bytes=0;const auto* parent_name=parent?parent->zName:nullptr;
        if(parent_name)while(parent_name_bytes<128&&parent_name[parent_name_bytes])++parent_name_bytes;
        const auto label=[](const char* value){size_t n=0;if(value)while(n<128&&value[n])++n;return value?std::string(value,n):std::string();};
        nlohmann::json events=nlohmann::json::array();
        for(size_t i=0;i<sink.count;++i){const auto& value=sink.records[i];events.push_back({
            {"operation",value.operation},{"resultAvailable",value.result_available},{"result",value.result},
            {"observedErrno",value.observed_errno},{"inputs",value.inputs},{"outputAvailable",value.output_available},
            {"output",value.output},{"pathAvailable",value.path_available},{"pathBytes",std::string(value.text.data(),value.text_size)},{"truncated",value.truncated}});}
        return " [readonly-open-vfs="+nlohmann::json{{"available",available},{"selected",selector.consumed},{"availabilityReason",reason},
            {"registrationResult",registration_result},{"unregistrationResult",unregistration_result},{"cleanupOK",cleanup_ok},
            {"parent",parent_name?std::string(parent_name,parent_name_bytes):std::string()},{"parentNameBoundReached",parent_name_bytes==128},
            {"parentVersion",parent?parent->iVersion:0},{"requestedFlags",selector.flags},
            {"sqliteVersion",label(sqlite3_libversion())},{"sqliteSourceID",label(sqlite3_sourceid())},
            {"fileLayerUnavailable",sink.file_layer_unavailable},{"overflow",sink.overflow},{"events",events}}.dump()+"]";
    }
};
}
#endif
