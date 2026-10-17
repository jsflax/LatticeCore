// Included only by the target-only private VFS guard. Protected callbacks and
// their original producers remain unchanged. No parent file method is used.
    template<class Body> static int vfs_call(sqlite3_vfs* raw,int failure,callback_kind kind,Body&& body)noexcept {
        auto* owner=from(raw);callback_scope call(owner,kind);if(!call)return failure;
        try{const int rc=body(*owner);call.record_body_return(rc);return rc;}
        catch(...){call.original.record_body_exception_return(failure);owner->mark_uncertain();return failure;} // Actual caught body return; original debt stays held.
    }
    template<class Body> static int file_call(sqlite3_file* raw,int failure,callback_kind kind,Body&& body)noexcept {
        if(!raw)return failure;auto* file=wrapper(raw);
        callback_scope call(file->owner,kind,file->resource,raw);
        if(!call||!file->resource||!call.connection)return failure;
        try {
            const int rc=body(*file->owner,*call.connection,*file);
            call.record_body_return(rc);return rc; // First real body return before later joins.
        }catch(...){call.original.record_body_exception_return(failure);file->owner->mark_uncertain();return failure;}
    }
    std::shared_ptr<ordinary_target_staging_connection> actual_connection()const {
        const auto owner=original.lock();const auto held=owner?owner->connection.lock():nullptr;
        if(!held||owner->connection_identity!=held.get())throw std::runtime_error("Target VFS connection original unavailable");
        return held;
    }
    static int open(sqlite3_vfs* raw,const char* path,sqlite3_file* output,int flags,int* actual_flags)noexcept {
        if(!output)return SQLITE_CANTOPEN;
        auto* file=wrapper(output);*file=file_wrapper{};
        return vfs_call(raw,SQLITE_CANTOPEN,callback_kind::open,[&](state& owner) {
            const bool main=(flags&SQLITE_OPEN_MAIN_DB)!=0,journal=(flags&SQLITE_OPEN_MAIN_JOURNAL)!=0;
            if(main==journal||!path||!actual_flags)return SQLITE_CANTOPEN;
            const auto connection=owner.actual_connection();std::uint64_t issued=0;
            const int rc=connection->descriptor_open(journal,path,output,flags,actual_flags,issued);
            if(rc!=SQLITE_OK)return rc;
            file->owner=&owner;file->resource=issued;file->base.pMethods=&io_methods;
            return SQLITE_OK; // Same actual resource loan, never a new parent fd.
        });
    }
    static int close(sqlite3_file* raw)noexcept {
        return file_call(raw,SQLITE_IOERR_CLOSE,callback_kind::close,[&](state&,auto& owner,file_wrapper& file) {
            const int rc=owner.descriptor_close(file.resource,raw);
            if(rc==SQLITE_OK){file.resource=0;file.base.pMethods=nullptr;}return rc;
        });
    }
    static int read(sqlite3_file* raw,void* output,int bytes,sqlite3_int64 offset)noexcept {
        return file_call(raw,SQLITE_IOERR_READ,callback_kind::read,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_read(file.resource,raw,output,bytes,offset);});
    }
    static int write(sqlite3_file* raw,const void* input,int bytes,sqlite3_int64 offset)noexcept {
        return file_call(raw,SQLITE_IOERR_WRITE,callback_kind::write,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_write(file.resource,raw,input,bytes,offset);});
    }
    static int truncate(sqlite3_file* raw,sqlite3_int64 bytes)noexcept {
        return file_call(raw,SQLITE_IOERR_TRUNCATE,callback_kind::truncate,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_truncate(file.resource,raw,bytes);});
    }
    static int sync(sqlite3_file* raw,int flags)noexcept {
        return file_call(raw,SQLITE_IOERR_FSYNC,callback_kind::sync,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_sync(file.resource,raw,flags);});
    }
    static int file_size(sqlite3_file* raw,sqlite3_int64* bytes)noexcept {
        return file_call(raw,SQLITE_IOERR_FSTAT,callback_kind::size,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_size(file.resource,raw,bytes);});
    }
    static int lock(sqlite3_file* raw,int level)noexcept {
        return file_call(raw,SQLITE_IOERR_LOCK,callback_kind::lock,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_lock(file.resource,raw,level);});
    }
    static int unlock(sqlite3_file* raw,int level)noexcept {
        return file_call(raw,SQLITE_IOERR_UNLOCK,callback_kind::unlock,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_unlock(file.resource,raw,level);});
    }
    static int check_reserved(sqlite3_file* raw,int* result)noexcept {
        return file_call(raw,SQLITE_IOERR_CHECKRESERVEDLOCK,callback_kind::reserved,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_reserved(file.resource,raw,result);});
    }
    static int file_control(sqlite3_file* raw,int operation,void* argument)noexcept {
        return file_call(raw,SQLITE_IOERR,callback_kind::control,[&](state&,auto& owner,file_wrapper& file){return owner.descriptor_control(file.resource,raw,operation,argument);});
    }
    static int sector_size(sqlite3_file* raw)noexcept {
        return file_call(raw,0,callback_kind::sector,[&](state&,auto& owner,file_wrapper& file) {
            sqlite3_int64 bytes=0;
            if(owner.descriptor_size(file.resource,raw,&bytes)!=SQLITE_OK)return 0;
            // Conservative fixed alignment, with no IOCAP atomic/powersafe
            // assertions. Linked filesystem durability remains qualification.
            return int(managed_fixed_file_ceiling::maximum_sector_size);
        });
    }
    static int device_characteristics(sqlite3_file* raw)noexcept {
        return file_call(raw,0,callback_kind::device,[](state&,auto&,file_wrapper&){return 0;});
    }
    static int shm_map(sqlite3_file* raw,int,int,int,void volatile** output)noexcept {
        if(output)*output=nullptr;return file_call(raw,SQLITE_IOERR_SHMMAP,callback_kind::unsupported,[](state& owner,auto&,file_wrapper&){owner.unsupported_refused=true;return SQLITE_IOERR_SHMMAP;});
    }
    static int shm_lock(sqlite3_file* raw,int,int,int)noexcept {
        return file_call(raw,SQLITE_IOERR_SHMLOCK,callback_kind::unsupported,[](state& owner,auto&,file_wrapper&){owner.unsupported_refused=true;return SQLITE_IOERR_SHMLOCK;});
    }
    static void shm_barrier(sqlite3_file* raw)noexcept {
        (void)file_call(raw,SQLITE_IOERR_SHMLOCK,callback_kind::unsupported,[](state& owner,auto&,file_wrapper&){owner.unsupported_refused=true;return SQLITE_IOERR_SHMLOCK;});
    }
    static int shm_unmap(sqlite3_file* raw,int)noexcept {
        return file_call(raw,SQLITE_IOERR_SHMOPEN,callback_kind::unsupported,[](state& owner,auto&,file_wrapper&){owner.unsupported_refused=true;return SQLITE_IOERR_SHMOPEN;});
    }
    static int fetch(sqlite3_file* raw,sqlite3_int64,int,void** output)noexcept {
        if(output)*output=nullptr;return file_call(raw,SQLITE_IOERR_MMAP,callback_kind::optional,[](state&,auto&,file_wrapper&){return SQLITE_OK;});
    }
    static int unfetch(sqlite3_file* raw,sqlite3_int64,void* address)noexcept {
        return file_call(raw,SQLITE_IOERR_MMAP,callback_kind::optional,[&](state&,auto&,file_wrapper&){return address?SQLITE_IOERR_MMAP:SQLITE_OK;});
    }
    static int remove(sqlite3_vfs* raw,const char* path,int sync_directory)noexcept {
        return vfs_call(raw,SQLITE_IOERR_DELETE,callback_kind::remove,[&](state& owner){return owner.actual_connection()->descriptor_remove(path,sync_directory);});
    }
    static int access(sqlite3_vfs* raw,const char* path,int flags,int* result)noexcept {
        if(result)*result=0;return vfs_call(raw,SQLITE_IOERR_ACCESS,callback_kind::access,[&](state& owner){return owner.actual_connection()->descriptor_access(path,flags,result);});
    }
    static int full_path(sqlite3_vfs* raw,const char* path,int bytes,char* output)noexcept {
        return vfs_call(raw,SQLITE_CANTOPEN,callback_kind::path,[&](state& owner){return owner.actual_connection()->descriptor_full_path(path,bytes,output);});
    }
    static void* dl_open(sqlite3_vfs*,const char*)noexcept{return nullptr;}
    static void dl_error(sqlite3_vfs*,int bytes,char* output)noexcept {
        if(output&&bytes>0){constexpr char message[]="Target staging extensions unavailable";
            const auto copied=std::min(sizeof(message)-1,std::size_t(bytes-1));std::memcpy(output,message,copied);output[copied]='\0';}
    }
    using dynamic_symbol=void(*)(void);
    static dynamic_symbol dl_symbol(sqlite3_vfs*,void*,const char*)noexcept{return nullptr;}
    static void dl_close(sqlite3_vfs*,void*)noexcept{}
    environmental_call& begin_environment(int kind,int argument) {
        const auto actual=actual_connection();const auto phase=environment;
        if(!route()||!phase||!phase->creator_current()||phase->connection!=actual.get()||phase->settled||
           !actual->descriptor_environment_phase_admissible(phase->generation)||phase->count==phase->calls.size()) {
            mark_uncertain();throw std::runtime_error("Target environmental phase ledger unavailable");
        }
        auto& call=phase->calls[phase->count++];call.kind=kind;call.argument=argument;
        call.callback_generation=actual->active_descriptor_callback_generation();call.attempted=true;return call;
    }
    void end_environment(environmental_call& call,int result)noexcept {
        if(!current()||!call.attempted||!call.entered||call.returned){mark_uncertain();return;}
        call.result=result;call.returned=true;
    }
    static int randomness(sqlite3_vfs* raw,int bytes,char* output)noexcept {
        if(!output||bytes<0)return 0;
        return vfs_call(raw,0,callback_kind::randomness,[&](state& owner){auto& call=owner.begin_environment(1,bytes);
            call.entered=true;const int rc=owner.parent->xRandomness(owner.parent,bytes,output);owner.end_environment(call,rc);
            if(!owner.route())return 0;return rc;});
    }
    static int sleep(sqlite3_vfs* raw,int microseconds)noexcept {
        if(microseconds<0)return 0;
        return vfs_call(raw,0,callback_kind::sleep,[&](state& owner){auto& call=owner.begin_environment(2,microseconds);
            call.entered=true;const int rc=owner.parent->xSleep(owner.parent,microseconds);owner.end_environment(call,rc);
            if(!owner.route())return 0;return rc;});
    }
    static int current_time(sqlite3_vfs* raw,double* output)noexcept {
        if(!output)return SQLITE_ERROR;
        return vfs_call(raw,SQLITE_ERROR,callback_kind::time,[&](state& owner){auto& call=owner.begin_environment(3,0);
            call.entered=true;const int rc=owner.parent->xCurrentTime(owner.parent,output);owner.end_environment(call,rc);
            if(!owner.route())return SQLITE_ERROR;return rc;});
    }
    static int last_error(sqlite3_vfs*,int bytes,char* output)noexcept {
        if(output&&bytes>0)output[0]='\0';return 0; // No parent file/error context exists.
    }
    static int current_time64(sqlite3_vfs* raw,sqlite3_int64* output)noexcept {
        if(!output)return SQLITE_ERROR;
        return vfs_call(raw,SQLITE_ERROR,callback_kind::time,[&](state& owner){auto& call=owner.begin_environment(4,0);
            if(owner.parent->iVersion>=2&&owner.parent->xCurrentTimeInt64) {
                call.entered=true;const int rc=owner.parent->xCurrentTimeInt64(owner.parent,output);owner.end_environment(call,rc);
                if(!owner.route())return SQLITE_ERROR;return rc;
            }
            double days=0;call.entered=true;const int rc=owner.parent->xCurrentTime(owner.parent,&days);owner.end_environment(call,rc);
            if(!owner.route()||rc!=SQLITE_OK)return SQLITE_ERROR;
            const double ms=days*86400000.0;
            if(!(ms>=0&&ms<double(std::numeric_limits<sqlite3_int64>::max())))return SQLITE_ERROR;
            *output=static_cast<sqlite3_int64>(ms);return SQLITE_OK;
        });
    }
    static int set_system_call(sqlite3_vfs*,const char*,sqlite3_syscall_ptr)noexcept{return SQLITE_NOTFOUND;}
    static sqlite3_syscall_ptr get_system_call(sqlite3_vfs*,const char*)noexcept{return nullptr;}
    static const char* next_system_call(sqlite3_vfs*,const char*)noexcept{return nullptr;}
    static const sqlite3_io_methods io_methods;
