#pragma once
#include "ordinary_historical_generation.hpp"
#include "ordinary_wal_policy.hpp"
#include <algorithm>
#include <array>
#include <cerrno>
#include <exception>
#include <set>
#include <string_view>
#include <stdexcept>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "wal_extent_vfs.hpp"
#include <fcntl.h>
#include <unistd.h>
#endif

namespace lattice::detail::ordinary_installation {
struct historical_wal_checkpoint_test_access;
// Private post-retirement engine. Only the two genuine retained-generation
// issuers call drain, on the original controller thread and original deadline.
// It exposes neither a native handle nor a path/policy/Boolean permit to apps.
class historical_wal_checkpoint final {
    friend class engram_namespace_retirement;
    friend struct historical_wal_checkpoint_test_access;
    using deadline=ordinary_launch::deadline;
    static constexpr std::int64_t ordinary_limit=ordinary_managed_wal_limit_bytes;
    [[noreturn]] static void fail(const char* text){
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        throw db_error(text);
#else
        throw std::runtime_error(text);
#endif
    }
    static void bound(deadline end){if(std::chrono::steady_clock::now()>=end)fail("historical WAL checkpoint original deadline expired");}
    static bool builtin_literal(const char* actual,std::string_view upper)noexcept {
        if(!actual)return false;
        for(const unsigned char wanted:upper){unsigned char value=static_cast<unsigned char>(*actual++);
            if(value>='a'&&value<='z')value=static_cast<unsigned char>(value-'a'+'A');
            if(value!=wanted)return false;}
        return *actual=='\0';
    }
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    static file_identity identity(const struct stat& s){return {static_cast<std::uint64_t>(s.st_dev),static_cast<std::uint64_t>(s.st_ino)};}
    static struct stat stat_at(int parent,const std::string& path){
        struct stat s{};if(::fstatat(parent,path.c_str(),&s,AT_SYMLINK_NOFOLLOW)!=0)fail("historical WAL census member unavailable");return s;
    }
    static void exact_regular(const struct stat& actual,const retained_historical_generation::member& original){
        if(original.directory||!S_ISREG(actual.st_mode)||actual.st_nlink!=1||actual.st_uid!=::geteuid()||
           (actual.st_mode&07022)||actual.st_size<0||identity(actual)!=original.identity||
           (actual.st_mode&07777)!=original.mode)fail("historical WAL census member changed");
    }
    static std::string generation_path(int generation,const file_identity& expected){
        struct stat held{};if(generation<0||::fstat(generation,&held)!=0||!S_ISDIR(held.st_mode)||
           held.st_uid!=::geteuid()||(held.st_mode&07022)||identity(held)!=expected)fail("historical WAL generation changed");
        std::array<char,4097> path{};
#if defined(__APPLE__)
        if(::fcntl(generation,F_GETPATH,path.data())!=0)fail("historical WAL generation locator unavailable");
        const auto count=::strnlen(path.data(),path.size());
#else
        const std::string link="/proc/self/fd/"+std::to_string(generation);
        const auto returned=::readlink(link.c_str(),path.data(),path.size()-1);
        if(returned<=0)fail("historical WAL generation locator unavailable");
        const auto count=static_cast<std::size_t>(returned);
#endif
        if(!count||count>=path.size()-1||path[0]!='/'||std::string_view(path.data(),count).ends_with(" (deleted)"))
            fail("historical WAL generation locator invalid");
        std::string result(path.data(),count);const std::filesystem::path selected(result);
        if(selected.lexically_normal()!=selected||std::filesystem::canonical(selected)!=selected)
            fail("historical WAL generation locator noncanonical");
        struct stat named{};if(::lstat(result.c_str(),&named)!=0||!S_ISDIR(named.st_mode)||identity(named)!=expected)
            fail("historical WAL generation locator changed");
        return result;
    }
    static bool group_main(std::string_view leaf){
        if(!leaf.starts_with("group-"))return false;
        const auto suffix=leaf.substr(42);
        if(suffix!=".sqlite"&&suffix!=".sqlite.revoked")return false;
        for(std::size_t at=6;at<42;++at){const unsigned char c=leaf[at];
            if(at==14||at==19||at==24||at==29){if(c!='-')return false;}
            else if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))return false;}
        return true;
    }
    static bool main_name(const std::string& relative){
        if(relative=="memory.sqlite"||relative=="sync/memory-synced.sqlite")return true;
        return relative.starts_with("sync/")&&relative.size()>=49&&group_main(std::string_view(relative).substr(5));
    }
    // Mechanism shared only with real-file tests. This does not issue a retained
    // generation, prove old-process retirement, or authorize ordinary writers.
    static void checkpoint_one(const std::string& path,const struct stat& main,const struct stat& wal,
        const struct stat& shm,deadline end){
        bound(end);
        wal_extent_vfs::checkpoint_policy policy;policy.main=main;policy.wal=wal;policy.shm=shm;
        policy.shm_path=path+"-shm";policy.end=end;
        if(main.st_size<0||wal.st_size<0||shm.st_size<0||
           main.st_size>std::numeric_limits<sqlite3_int64>::max()-wal.st_size||
           wal.st_size>(std::numeric_limits<sqlite3_int64>::max()-1024*1024)/2)
            fail("historical WAL checkpoint original extent arithmetic overflow");
        policy.main_ceiling=main.st_size+wal.st_size;
        // At most two complete original WAL passes plus bounded header reads;
        // MAIN reads have a separate cumulative 8MiB ceiling in the VFS.
        // These are finite requested-read budgets, not physical memory bounds.
        policy.wal_read_ceiling=wal.st_size*2+1024*1024;
        const auto limit=std::max<sqlite3_int64>(ordinary_limit,wal.st_size);
        auto owner=wal_extent_vfs::create(path,limit,&policy);
        sqlite3* native=nullptr;std::exception_ptr first;
        int result=SQLITE_MISUSE;bool opening_returned=false;
        try{
            if(!owner->checkpoint_effect_current())fail("historical WAL checkpoint original identity changed");
            // No CREATE, URI, schema changes, PRAGMA, SQL statement,
            // extension API, database wrapper or optimization is invoked.
#ifdef SQLITE_OPEN_NOFOLLOW
            result=sqlite3_open_v2(path.c_str(),&native,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX|SQLITE_OPEN_NOFOLLOW,owner->name());
#else
            fail("historical WAL checkpoint no-follow support unavailable");
#endif
            opening_returned=true;
            if(!owner->native_open_returned(native,result)||result!=SQLITE_OK||!native)
                fail("historical WAL checkpoint native open failed");
            sqlite3_file* original_main=nullptr;std::array<unsigned char,32> header{};
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_FILE_POINTER,&original_main)!=SQLITE_OK||!original_main||
               !original_main->pMethods||original_main->pMethods->xRead(original_main,header.data(),static_cast<int>(header.size()),0)!=SQLITE_OK||
               std::memcmp(header.data(),"SQLite format 3\0",16)!=0||header[18]!=2||header[19]!=2)
                fail("historical oversized WAL is not the original WAL-mode main");
            bound(end);
            if(sqlite3_set_authorizer(native,wal_extent_vfs::checkpoint_authorizer,owner.get())!=SQLITE_OK||
               sqlite3_busy_handler(native,[](void*,int){return 0;},nullptr)!=SQLITE_OK||
               sqlite3_wal_autocheckpoint(native,0)!=SQLITE_OK)
                fail("historical WAL checkpoint SQL/refill denial unavailable");
            sqlite3_limit(native,SQLITE_LIMIT_ATTACHED,0);
            int observed=0;
#ifdef SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
            if(sqlite3_db_config(native,SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,1,&observed)!=SQLITE_OK||observed!=1)
                fail("historical WAL checkpoint close policy unavailable");
#else
            fail("historical WAL checkpoint close policy unsupported");
#endif
            int persist=1;
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_PERSIST_WAL,&persist)!=SQLITE_OK)
                fail("historical WAL checkpoint sidecar preservation unavailable");
            persist=-1;
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_PERSIST_WAL,&persist)!=SQLITE_OK||persist!=1)
                fail("historical WAL checkpoint sidecar preservation not observed");
            bound(end);
            // Fixed built-in metadata C API initializes the real pager/WAL.
            // No SQL text, statement, unknown table/module or app callback is
            // supplied. Deny-all authorizer custody belongs to the same banked
            // VFS owner, so a first BUSY close cannot leave a stack context.
            const char* type=nullptr;const char* collation=nullptr;
            int not_null=-1,primary_key=-1,autoincrement=-1;
            owner->checkpoint_initializing_=true;
            result=sqlite3_table_column_metadata(native,"main","sqlite_schema","type",&type,&collation,&not_null,&primary_key,&autoincrement);
            owner->checkpoint_initializing_=false;
            if(result!=SQLITE_OK||owner->checkpoint_authorizer_calls_!=0||!builtin_literal(type,"TEXT")||
               !builtin_literal(collation,"BINARY")||not_null!=0||primary_key!=0||autoincrement!=0)
                fail("historical WAL checkpoint fixed metadata initialization failed");
            bound(end);
            if(!owner->checkpoint_effect_current())fail("historical WAL checkpoint custody changed before truncate");
            owner->checkpoint_active_=true;
            int log=-1,checkpointed=-1;
            result=sqlite3_wal_checkpoint_v2(native,"main",SQLITE_CHECKPOINT_TRUNCATE,&log,&checkpointed);
            owner->checkpoint_active_=false;
            if(result!=SQLITE_OK||log<0||checkpointed<0||log!=checkpointed)
                fail("historical WAL checkpoint first TRUNCATE did not complete");
            struct stat after{};
            if(!owner->checkpoint_identity()||::lstat((path+"-wal").c_str(),&after)!=0||after.st_size!=0)
                fail("historical WAL checkpoint original WAL did not truncate");
            bound(end);
        }catch(...){first=std::current_exception();}
        owner->checkpoint_initializing_=false;owner->checkpoint_active_=false;
        // Cleanup is still attempted after expiry, but cannot create late
        // success. No close_v2/zombie fallback can repair first-close debt.
        if(native){
            owner->native_close_entered(native);
            const int closed=sqlite3_close(native);
            owner->native_close_returned(native,closed,SQLITE_MISUSE);
            if(closed!=SQLITE_OK&&!first){try{fail("historical WAL checkpoint first native close failed");}catch(...){first=std::current_exception();}}
        }else if(!opening_returned){
            owner->native_open_returned(nullptr,SQLITE_CANTOPEN);
        }
        const auto ended=owner->inspect();
        if(!first){try{
            if(!ended.current_creator||!ended.ending_known||ended.registered||!ended.close_observed||
               ended.checked_close!=SQLITE_OK||ended.first_error!=SQLITE_OK||owner->checkpoint_authorizer_calls_!=0||!owner->checkpoint_identity())
                fail("historical WAL checkpoint actual native/file/VFS ending unavailable");
            struct stat after{};
            if(::lstat((path+"-wal").c_str(),&after)!=0||after.st_size!=0)
                fail("historical WAL checkpoint WAL changed during close");
            bound(end);
        }catch(...){first=std::current_exception();}}
        if(first)std::rethrow_exception(first);
    }
    // WAL-index requested region storage only. This is NOT a SQLite-wide
    // allocator/schema/checkpoint-workspace/RSS limit. A valid WAL has a 32-byte
    // header and frames of at least 24+512 bytes. Region 0 maps 4062 frames;
    // subsequent 32KiB regions map 4096. Admit one spare region and both old/new
    // pointer arrays before native open. No global SQLite heap configuration.
    static constexpr std::uint64_t heap_index_budget=8*1024*1024;
    static void require_heap_index_budget(sqlite3_int64 bytes){
        if(bytes<32)fail("historical absent-SHM original WAL extent invalid");
        const auto frames=(static_cast<std::uint64_t>(bytes)-32)/536;
        const auto later=frames>4062?(frames-4062+4095)/4096:0;
        if(later>heap_index_budget/32768)fail("historical absent-SHM WAL-index region budget exceeded");
        const auto regions=std::uint64_t(2)+later;
        const auto requested=regions*32768+regions*2*sizeof(void*);
        if(requested>heap_index_budget)fail("historical absent-SHM WAL-index region budget exceeded");
    }
    static void checkpoint_without_shm(const std::string& path,const struct stat& main,const struct stat& wal,
        deadline end){
        bound(end);
        require_heap_index_budget(wal.st_size);
        wal_extent_vfs::checkpoint_policy policy;policy.main=main;policy.wal=wal;policy.shm_absent=true;
        policy.shm_path=path+"-shm";policy.end=end;
        if(main.st_size<0||wal.st_size<0||
           main.st_size>std::numeric_limits<sqlite3_int64>::max()-wal.st_size||
           wal.st_size>(std::numeric_limits<sqlite3_int64>::max()-1024*1024)/2)
            fail("historical WAL checkpoint original extent arithmetic overflow");
        policy.main_ceiling=main.st_size+wal.st_size;
        // At most two complete original WAL passes plus bounded header reads;
        // MAIN reads have a separate cumulative 8MiB ceiling in the VFS.
        // These are finite requested-read budgets, not physical memory bounds.
        policy.wal_read_ceiling=wal.st_size*2+1024*1024;
        const auto limit=std::max<sqlite3_int64>(ordinary_limit,wal.st_size);
        auto owner=wal_extent_vfs::create(path,limit,&policy);
        sqlite3* native=nullptr;std::exception_ptr first;
        int result=SQLITE_MISUSE;bool opening_returned=false;sqlite3_stmt* control=nullptr;
        try{
            if(!owner->checkpoint_effect_current())fail("historical WAL checkpoint original identity changed");
            // No CREATE/URI/app SQL/schema change/extension/wrapper. The only
            // statement is the fixed private EXCLUSIVE control below.
#ifdef SQLITE_OPEN_NOFOLLOW
            result=sqlite3_open_v2(path.c_str(),&native,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX|SQLITE_OPEN_NOFOLLOW,owner->name());
#else
            fail("historical WAL checkpoint no-follow support unavailable");
#endif
            opening_returned=true;
            if(!owner->native_open_returned(native,result)||result!=SQLITE_OK||!native)
                fail("historical WAL checkpoint native open failed");
            sqlite3_file* original_main=nullptr;std::array<unsigned char,32> header{};
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_FILE_POINTER,&original_main)!=SQLITE_OK||!original_main||
               !original_main->pMethods||original_main->pMethods->xRead(original_main,header.data(),static_cast<int>(header.size()),0)!=SQLITE_OK||
               std::memcmp(header.data(),"SQLite format 3\0",16)!=0||header[18]!=2||header[19]!=2)
                fail("historical oversized WAL is not the original WAL-mode main");
            bound(end);
            if(sqlite3_set_authorizer(native,wal_extent_vfs::checkpoint_authorizer,owner.get())!=SQLITE_OK||
               sqlite3_busy_handler(native,[](void*,int){return 0;},nullptr)!=SQLITE_OK||
               sqlite3_wal_autocheckpoint(native,0)!=SQLITE_OK)
                fail("historical WAL checkpoint SQL/refill denial unavailable");
            sqlite3_limit(native,SQLITE_LIMIT_ATTACHED,0);
            int observed=0;
#ifdef SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
            if(sqlite3_db_config(native,SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,1,&observed)!=SQLITE_OK||observed!=1)
                fail("historical WAL checkpoint close policy unavailable");
#else
            fail("historical WAL checkpoint close policy unsupported");
#endif
            int persist=1;
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_PERSIST_WAL,&persist)!=SQLITE_OK)
                fail("historical WAL checkpoint sidecar preservation unavailable");
            persist=-1;
            if(sqlite3_file_control(native,"main",SQLITE_FCNTL_PERSIST_WAL,&persist)!=SQLITE_OK||persist!=1)
                fail("historical WAL checkpoint sidecar preservation not observed");
            bound(end);
            // Set EXCLUSIVE before first pager/WAL access. Exact action/args
            // and one result are observed; the context belongs to this banked
            // owner even if a later first close is BUSY/unknown.
            owner->checkpoint_configuring_exclusive_=true;
            if(sqlite3_set_authorizer(native,wal_extent_vfs::checkpoint_exclusive_authorizer,owner.get())!=SQLITE_OK)
                fail("historical absent-SHM exclusive control denial unavailable");
            static constexpr char exclusive[]="PRAGMA main.locking_mode=EXCLUSIVE";
            const char* tail=nullptr;
            result=sqlite3_prepare_v2(native,exclusive,static_cast<int>(sizeof(exclusive)),&control,&tail);
            if(result!=SQLITE_OK||!control||!tail||*tail!='\0'||owner->checkpoint_exclusive_control_calls_!=1||
               owner->checkpoint_authorizer_calls_!=0||sqlite3_column_count(control)!=1||sqlite3_step(control)!=SQLITE_ROW)
                fail("historical absent-SHM fixed exclusive control failed");
            if(sqlite3_column_type(control,0)!=SQLITE_TEXT)
                fail("historical absent-SHM exclusive control original type changed");
            const auto* mode=sqlite3_column_text(control,0);
            if(sqlite3_column_bytes(control,0)!=9||!mode||
               std::memcmp(mode,"exclusive",9)!=0||sqlite3_step(control)!=SQLITE_DONE)
                fail("historical absent-SHM exclusive control not observed");
            {auto* original=control;control=nullptr;
                if(sqlite3_finalize(original)!=SQLITE_OK)fail("historical absent-SHM first control finalize failed");}
            owner->checkpoint_configuring_exclusive_=false;
            if(sqlite3_set_authorizer(native,wal_extent_vfs::checkpoint_authorizer,owner.get())!=SQLITE_OK||
               !owner->checkpoint_effect_current())fail("historical absent-SHM deny-all restoration failed");
            bound(end);
            // Fixed built-in metadata C API initializes the real pager/WAL.
            // No SQL text, statement, unknown table/module or app callback is
            // supplied. Deny-all authorizer custody belongs to the same banked
            // VFS owner, so a first BUSY close cannot leave a stack context.
            const char* type=nullptr;const char* collation=nullptr;
            int not_null=-1,primary_key=-1,autoincrement=-1;
            owner->checkpoint_initializing_=true;
            result=sqlite3_table_column_metadata(native,"main","sqlite_schema","type",&type,&collation,&not_null,&primary_key,&autoincrement);
            owner->checkpoint_initializing_=false;
            if(result!=SQLITE_OK||!owner->checkpoint_exclusive_current_||owner->checkpoint_authorizer_calls_!=0||!builtin_literal(type,"TEXT")||
               !builtin_literal(collation,"BINARY")||not_null!=0||primary_key!=0||autoincrement!=0)
                fail("historical WAL checkpoint fixed metadata initialization failed");
            bound(end);
            if(!owner->checkpoint_effect_current())fail("historical WAL checkpoint custody changed before truncate");
            owner->checkpoint_active_=true;
            int log=-1,checkpointed=-1;
            result=sqlite3_wal_checkpoint_v2(native,"main",SQLITE_CHECKPOINT_TRUNCATE,&log,&checkpointed);
            owner->checkpoint_active_=false;
            if(result!=SQLITE_OK||log<0||checkpointed<0||log!=checkpointed)
                fail("historical WAL checkpoint first TRUNCATE did not complete");
            struct stat after{};
            if(!owner->checkpoint_identity()||::lstat((path+"-wal").c_str(),&after)!=0||after.st_size!=0)
                fail("historical WAL checkpoint original WAL did not truncate");
            bound(end);
        }catch(...){first=std::current_exception();}
        owner->checkpoint_initializing_=false;owner->checkpoint_active_=false;owner->checkpoint_configuring_exclusive_=false;
        // First statement cleanup is checked even on refusal; no retry repairs
        // an unknown finalize/close and no stack authorizer context escapes.
        if(native){
            const int restored=sqlite3_set_authorizer(native,wal_extent_vfs::checkpoint_authorizer,owner.get());
            if(restored!=SQLITE_OK&&!first){try{fail("historical absent-SHM cleanup denial restoration failed");}catch(...){first=std::current_exception();}}
        }
        if(control){auto* original=control;control=nullptr;const int finalized=sqlite3_finalize(original);
            if(finalized!=SQLITE_OK&&!first){try{fail("historical absent-SHM first control finalize failed");}catch(...){first=std::current_exception();}}}
        // Cleanup is still attempted after expiry, but cannot create late
        // success. No close_v2/zombie fallback can repair first-close debt.
        if(native){
            owner->native_close_entered(native);
            const int closed=sqlite3_close(native);
            owner->native_close_returned(native,closed,SQLITE_MISUSE);
            if(closed!=SQLITE_OK&&!first){try{fail("historical WAL checkpoint first native close failed");}catch(...){first=std::current_exception();}}
        }else if(!opening_returned){
            owner->native_open_returned(nullptr,SQLITE_CANTOPEN);
        }
        const auto ended=owner->inspect();
        if(!first){try{
            if(!ended.current_creator||!ended.ending_known||ended.registered||!ended.close_observed||
               ended.checked_close!=SQLITE_OK||ended.first_error!=SQLITE_OK||owner->checkpoint_authorizer_calls_!=0||owner->checkpoint_exclusive_control_calls_!=1||!owner->checkpoint_identity())
                fail("historical WAL checkpoint actual native/file/VFS ending unavailable");
            struct stat after{};
            if(::lstat((path+"-wal").c_str(),&after)!=0||after.st_size!=0)
                fail("historical WAL checkpoint WAL changed during close");
            bound(end);
        }catch(...){first=std::current_exception();}}
        if(first)std::rethrow_exception(first);
    }
#endif
    static void drain(int generation,const retained_historical_generation::observation& original,
        deadline end,const std::function<void(deadline)>& verify){
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        bound(end);if(!verify)fail("historical WAL retained verifier unavailable");verify(end);
        if(original.members.empty()||original.members.size()>256)fail("historical WAL original census unavailable");
        const auto root=generation_path(generation,original.generation);
        std::set<std::string> names;std::set<std::pair<std::uint64_t,std::uint64_t>> identities;
        std::size_t mains=0;bool primary=false;
        for(const auto& member:original.members){
            bound(end);
            if(!names.insert(member.relative_name).second||!identities.emplace(member.identity.device,member.identity.inode).second)
                fail("historical WAL original census aliased");
            // Full actual verifier owns the allowed spelling/census. Never
            // derive membership from groups.json, a live role, or paired plan.
            if(member.directory)continue;
            exact_regular(stat_at(generation,member.relative_name),member);
            if(main_name(member.relative_name)){if(++mains>64)fail("historical WAL original main census exceeds bound");
                if(member.relative_name=="memory.sqlite")primary=true;}
        }
        if(!primary)fail("historical WAL original primary absent");
        // Complete first observation/validation before any SQLite effects.
        struct work {std::string path;struct stat main{},wal{},shm{};bool shm_absent=false;};
        std::vector<work> pending;pending.reserve(mains);
        for(const auto& member:original.members){
            if(member.directory||!main_name(member.relative_name))continue;
            const auto find=[&](const std::string& suffix)->const retained_historical_generation::member*{
                const auto at=std::find_if(original.members.begin(),original.members.end(),[&](const auto& value){return value.relative_name==member.relative_name+suffix;});
                return at==original.members.end()?nullptr:&*at;};
            const auto* original_wal=find("-wal");if(!original_wal)continue;
            work item;item.path=root+"/"+member.relative_name;item.main=stat_at(generation,member.relative_name);
            item.wal=stat_at(generation,original_wal->relative_name);exact_regular(item.wal,*original_wal);
            if(item.wal.st_size<=ordinary_limit)continue;
            // Only absence in the genuine full ORIGINAL census selects the
            // separate heap-index mechanism. A present but lost/malformed SHM
            // is never reinterpreted as absent. Hot journal remains refused.
            const auto* original_shm=find("-shm");
            if(find("-journal"))fail("historical oversized WAL has original rollback journal");
            if(original_shm){
                item.shm=stat_at(generation,original_shm->relative_name);exact_regular(item.shm,*original_shm);
                if(item.shm.st_size<32768)fail("historical oversized WAL original SHM unavailable");
            }else{
                struct stat unexpected{};
                if(::fstatat(generation,(member.relative_name+"-shm").c_str(),&unexpected,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)
                    fail("historical originally absent SHM appeared");
                require_heap_index_budget(item.wal.st_size);item.shm_absent=true;
            }
            pending.push_back(std::move(item));
        }
        verify(end);bound(end);
        for(const auto& item:pending){
            verify(end);bound(end);
            if(item.shm_absent)checkpoint_without_shm(item.path,item.main,item.wal,end);
            else checkpoint_one(item.path,item.main,item.wal,item.shm,end);
            verify(end);bound(end);
        }
        verify(end);bound(end);
#else
        (void)generation;(void)original;(void)end;(void)verify;fail("historical WAL checkpoint platform unsupported");
#endif
    }
};
}
