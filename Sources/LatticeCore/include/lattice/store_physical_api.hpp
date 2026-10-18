#pragma once
#include <sqlite3.h>
#include <chrono>
#include <array>
#include <cstdint>
#include <cstddef>
#include <memory>

namespace lattice {class database;}
namespace lattice::detail {
class store_physical_api;
class database_initialization_budget;
class store_deletion_operation;
struct store_physical_vfs_test_access;
class store_physical_open_original;
class store_resource_initialization_original;
// Status-only DATA retained by actual SQLite-owned policy/UDF cookies. This
// carries the SAME physical-open original, never a second debt identity. No
// Native pointer, SQL scope, cleanup, close or physical-ending grant exists.
class store_physical_policy_original final {
    friend class store_physical_api;
    friend class ::lattice::database;
    friend struct store_physical_vfs_test_access;
    void* original_=nullptr;
    std::uint64_t creator_=0;
    static store_physical_policy_original capture_(const store_physical_open_original&)noexcept;
public:
    store_physical_policy_original()noexcept;
    store_physical_policy_original(store_physical_policy_original&&)noexcept;
    store_physical_policy_original& operator=(store_physical_policy_original&&)noexcept;
    ~store_physical_policy_original()noexcept;
    store_physical_policy_original(const store_physical_policy_original&)=delete;
    store_physical_policy_original& operator=(const store_physical_policy_original&)=delete;
    bool observer_policy_unsettled()const noexcept;
    void note_observer_policy_unsettled()noexcept;
};
// One actual open's owning DATA identity. Issued by its actual producer before
// Native open; the actual return binds the comparison before publication/SQL. It owns no Native pointer
// or callback and remains provenance after close; it never adopts a later
// connection merely because its allocator recycled the same sqlite3* value.
class store_physical_open_original final {
    friend class database_initialization_budget;
    friend class store_resource_initialization_original;
    friend class store_physical_api;
    friend class ::lattice::database;
    friend class store_deletion_operation;
    friend struct store_physical_vfs_test_access;
    void* original_=nullptr;
    sqlite3* actual_=nullptr; // comparison DATA only, never dereferenced
    std::uint64_t creator_=0;
    bool bound_=false,legacy_=false,native_retired_=false;
    // A separate closed target producer, never normal Host/SH/policy custody.
    // Its existing database_scope/custom VFS/exclusive ledger owns ending.
    bool exclusive_target_=false,exclusive_open_attempted_=false;
    bool issue_exclusive_target_(std::uint64_t captured_creator)noexcept;
    int open_exclusive_target_(const char*,sqlite3**,int,const char*,
        std::chrono::steady_clock::time_point original_end)noexcept;
    bool retain_initialization_budget_(const std::shared_ptr<const database_initialization_budget>&)noexcept;
    bool matches_open_(sqlite3*)const noexcept;
    bool seal_and_wait_(std::chrono::steady_clock::time_point)const noexcept;
public:
    store_physical_open_original()noexcept;
    store_physical_open_original(store_physical_open_original&&)noexcept;
    store_physical_open_original& operator=(store_physical_open_original&&)noexcept;
    ~store_physical_open_original()noexcept;
    store_physical_open_original(const store_physical_open_original&)=delete;
    store_physical_open_original& operator=(const store_physical_open_original&)=delete;
    // Denial-only VM policy DATA. Missing/foreign original is never healthy.
    // This debt does not rewrite physical-close facts or block cleanup.
    bool observer_policy_unsettled()const noexcept;
    void note_observer_policy_unsettled()noexcept;
};
// Internal lexical Native-caller ending pin. Fixed DATA only, no allocation,
// SQLite pointer ownership, user callback, SQL grant or public SDK lease.
// It must be declared before the actual Native-lock holder, so its ending
// follows that holder's real release. C API scopes remain separate and nested.
class store_physical_native_scope final {
    friend class store_physical_api;
    friend class store_physical_cleanup_scope;
    std::array<void*,64> originals_{};
    store_physical_native_scope* previous_=nullptr;
    static thread_local store_physical_native_scope* current_;
    bool registered_=false;
    std::size_t count_=0;
    std::uint64_t creator_=0,entered_mask_=0;
    bool entered_=true,cleanup_only_=false;
    store_physical_native_scope()noexcept=default;
public:
    explicit store_physical_native_scope(sqlite3*)noexcept;
    // Internal actual-wrapper route; captures only its already-bound original.
    explicit store_physical_native_scope(const ::lattice::database&)noexcept;
    store_physical_native_scope(const store_physical_open_original&,sqlite3* actual_comparison)noexcept;
    store_physical_native_scope(const store_physical_open_original*,sqlite3* actual_comparison)noexcept;
    ~store_physical_native_scope()noexcept;
    store_physical_native_scope(const store_physical_native_scope&)=delete;
    store_physical_native_scope& operator=(const store_physical_native_scope&)=delete;
    bool entered()const noexcept{return entered_;}
    void reset()noexcept;
};
// Closed cleanup ending custody only. It survives exact callback-slot removal,
// mutex release and close return after admission is sealed. It NEVER lends a
// continuation grant to a nested/public SQL entry. Captured immutable originals
// remain creator fenced; no address lookup or policy-DATA conversion occurs.
class store_physical_cleanup_scope final {
    store_physical_native_scope held_;
public:
    explicit store_physical_cleanup_scope(const ::lattice::database&)noexcept;
    store_physical_cleanup_scope(const store_physical_open_original&,sqlite3* actual_comparison)noexcept;
    store_physical_cleanup_scope(const store_physical_cleanup_scope&)=delete;
    store_physical_cleanup_scope& operator=(const store_physical_cleanup_scope&)=delete;
    bool entered()const noexcept{return held_.entered();}
    void reset()noexcept{held_.reset();}
};
// Return ONLY an already-owned Native mutex from its actual immutable open
// original. No acquire/SQL/close authority is issued. Callers must keep that
// exact owner and its lexical Native frame alive before entry through return.
// Inherited/retired originals abandon this POD guard without Native access.
class store_physical_mutex_return final {
    const store_physical_open_original* original_=nullptr;
    sqlite3_mutex* mutex_=nullptr;
public:
    store_physical_mutex_return(const ::lattice::database&,sqlite3_mutex*)noexcept;
    store_physical_mutex_return(const store_physical_open_original&,sqlite3_mutex*)noexcept;
    ~store_physical_mutex_return()noexcept;
    store_physical_mutex_return(const store_physical_mutex_return&)=delete;
    store_physical_mutex_return& operator=(const store_physical_mutex_return&)=delete;
    void reset()noexcept;
};
// Source-internal return-scope adapters. They preserve actual SQLite results;
// an argument is never turned into SQL/ownership authority. No public SDK API.
int store_physical_open_v2(const char*,sqlite3**,int,const char*)noexcept;
// The caller supplies its original entry deadline; lower open/cleanup retains
// independently issued original custody, never a renewed deletion deadline.
int store_physical_open_v2_at(const char*,sqlite3**,int,const char*,
    std::chrono::steady_clock::time_point original_end)noexcept;
// Owning producers carry the exact preadmitted original through partial/error
// returns. These private implementation adapters never find it by a recycled
// sqlite3* address. Cleanup carries the same original, including first BUSY.
int store_physical_open_v2_owned_at(const char*,sqlite3**,int,const char*,
    std::chrono::steady_clock::time_point,store_physical_open_original&)noexcept;
int store_physical_close_owned(store_physical_open_original&,sqlite3*)noexcept;
int store_physical_close_v2_owned(store_physical_open_original&,sqlite3*)noexcept;
// DATA peeling of only our actual issued wrapper. The caller must already
// hold its genuine Native connection scope; no descriptor or SQL authority is
// created. Unresourced connections preserve their original pointer route.
bool store_physical_main_for_lower(sqlite3*,sqlite3_vfs* expected_lower,sqlite3_file*& actual_inner)noexcept;
sqlite3_vfs* store_physical_identity_vfs(sqlite3*,const char* schema,sqlite3_vfs* observed,sqlite3_file*& actual_inner)noexcept;
// Creator denial precedes ANY inherited Native mutex/API access. These DATA
// checks do not manufacture an open original or confer raw access permission.
bool store_physical_process_current()noexcept;
bool store_physical_is_enrolled(sqlite3*)noexcept;
int store_physical_prepare_v2(sqlite3*,const char*,int,sqlite3_stmt**,const char**)noexcept;
int store_physical_prepare_v3(sqlite3*,const char*,int,unsigned int,sqlite3_stmt**,const char**)noexcept;
int store_physical_clear_bindings(sqlite3_stmt*)noexcept;
int store_physical_create_function(sqlite3*,const char*,int,int,void*,
    void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*))noexcept;
int store_physical_create_function_v2(sqlite3*,const char*,int,int,void*,
    void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*),void(*)(void*))noexcept;
// Same guarded v2 call, plus DATA witnessing whether the real SQLite API was
// entered. Pre-Native refusal retains its existing result/destruction contract
// and must not be mislabeled an actual SQLite registration error.
int store_physical_create_function_v2_checked(sqlite3*,const char*,int,int,void*,
    void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*,int,sqlite3_value**),
    void(*)(sqlite3_context*),void(*)(void*),bool* actual_native_entered)noexcept;
int store_physical_finalize(sqlite3_stmt*)noexcept;
int store_physical_step(sqlite3_stmt*)noexcept;
int store_physical_reset(sqlite3_stmt*)noexcept;
int store_physical_exec(sqlite3*,const char*,int(*)(void*,int,char**,char**),void*,char**)noexcept;
int store_physical_wal_checkpoint(sqlite3*,const char*,int,int*,int*)noexcept;
int store_physical_blob_close(sqlite3_blob*,sqlite3* actual_owner)noexcept;
int store_physical_close(sqlite3*)noexcept;
int store_physical_close_v2(sqlite3*)noexcept;
// Actual successful/unpublished unresourced open producer only. Supersedes
// stale address associations, never creates a physical resource permission.
void store_physical_unresourced_open_returned(sqlite3*)noexcept;
}
