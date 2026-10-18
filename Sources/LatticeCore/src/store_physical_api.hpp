#pragma once
#include "store_physical_lifetime.hpp"
#include <lattice/store_physical_api.hpp>
#include <array>
#include <cstddef>
#include <new>
#include <string>
namespace lattice {struct physical_store_identity;}

namespace lattice::detail {
class store_physical_vfs;
class ordinary_cohort_attachment_access;
class store_physical_api final {
    friend class ::lattice::database;
    friend class database_initialization_budget;
    friend class store_physical_cleanup_scope;
    friend class store_physical_mutex_return;
    friend class store_physical_vfs;
    friend class ordinary_cohort_attachment_access;
    friend class store_physical_native_scope;
    friend class store_physical_open_original;
    friend class store_physical_policy_original;
    friend struct store_physical_vfs_test_access;
    struct original;
    static original* head_;
    // One preallocated node/retirement reference per actual admitted original;
    // no new payload, allocation, service or raw-address work item is issued.
    static thread_local original* returned_tails_;
    // Last-owner DELETE custody for a preallocated setup budget which has
    // not necessarily reached any physical-open original yet.
    static thread_local database_initialization_budget* returned_budgets_;
    static void retire_initialization_budget_(database_initialization_budget*)noexcept;
    static thread_local bool draining_tails_;
    static thread_local std::uint64_t native_return_work_;
    static bool managed_return_here_()noexcept;
    static void handoff_returned_tail_(original*)noexcept;
    static void finish_returned_tail_(original*)noexcept;
    // Same finite typed-member profile as the existing bounded-WAL owner.
    // The typed target producer checks the actual SQLite attachment limit
    // before publication, reserving one frame for the primary original.
    // This fixed DATA frame storage is not a queue/RSS quota.
    static constexpr std::size_t maximum_originals=64;
    struct ticket {
        original* value=nullptr; // primary original, or first cleanup original
        std::array<original*,maximum_originals> values{};
        std::size_t count=0;
        std::uint64_t creator=0;
        bool inherited=false;
        ticket()noexcept=default;
        ticket(const ticket&)=delete;
        ticket& operator=(const ticket&)=delete;
        ticket(ticket&& other)noexcept:value(std::exchange(other.value,nullptr)),values(other.values),
            count(std::exchange(other.count,0)),creator(other.creator),inherited(other.inherited){}
        ~ticket()noexcept;
    };
    class api_scope final {
        alignas(store_physical_lifetime::api_return) std::byte storage_[maximum_originals*sizeof(store_physical_lifetime::api_return)];
        std::size_t count_=0;bool entered_=true;
        const std::uint64_t creator_;
        api_scope* previous_=nullptr;
        bool registered_=false;
        static thread_local api_scope* current_;
        friend class store_physical_api;
        store_physical_lifetime::api_return* at_(std::size_t i)noexcept {
            return std::launder(reinterpret_cast<store_physical_lifetime::api_return*>(storage_+i*sizeof(store_physical_lifetime::api_return)));
        }
    public:
        // Empty observation is issued at an ACTUAL legacy C-call boundary,
        // never around earlier resource/domain admission or waiting.
        api_scope()noexcept;
        explicit api_scope(const ticket&)noexcept;
        ~api_scope()noexcept;
        api_scope(const api_scope&)=delete;
        bool entered()const noexcept{return entered_;}
    };
    // Preallocated BEFORE SQLite open. Only the actual lifetime VFS can bind
    // this to the genuine open return and its already-issued physical lease.
    static original* prepare_(const store_physical_lifetime::pin&,void*,void(*)(void*)noexcept);
    static bool publish_(original*,sqlite3*,void*,void(*)(void*)noexcept)noexcept;
    static bool publish_member_(original*,sqlite3*,void*,void(*)(void*)noexcept)noexcept;
    static ticket capture_issued_(original*)noexcept;
    static void abandon_unpublished_(original*)noexcept;
    static ticket capture_(sqlite3*)noexcept;
    static ticket capture_open_(const store_physical_open_original&,sqlite3*)noexcept;
    static bool retain_initialization_budget_(store_physical_open_original&,
        const std::shared_ptr<const database_initialization_budget>&)noexcept;
    static bool can_issue_open_(const store_physical_open_original&)noexcept;
    static bool issue_open_(store_physical_open_original&,original*)noexcept;
    static bool issue_legacy_open_(store_physical_open_original&)noexcept;
    static bool open_returned_(store_physical_open_original&,original*,sqlite3*)noexcept;
    static ticket capture_cleanup_(const store_physical_open_original&,sqlite3*)noexcept;
    static void release_open_(store_physical_open_original&)noexcept;
    static void capture_policy_(const store_physical_open_original&,store_physical_policy_original&)noexcept;
    static void release_policy_(store_physical_policy_original&)noexcept;
    static bool observer_policy_unsettled_(const store_physical_policy_original&)noexcept;
    static void note_observer_policy_unsettled_(const store_physical_policy_original&)noexcept;
    static bool seal_open_(const store_physical_open_original&,std::chrono::steady_clock::time_point)noexcept;
    static bool observer_policy_unsettled_(const store_physical_open_original&)noexcept;
    static void note_observer_policy_unsettled_(const store_physical_open_original&)noexcept;
    static void enter_captured_native_(ticket&&,store_physical_native_scope&)noexcept;
    static void enter_database_native_(const ::lattice::database&,store_physical_native_scope&)noexcept;
    static void enter_database_cleanup_(const ::lattice::database&,store_physical_native_scope&)noexcept;
    static void enter_cleanup_scope_(const store_physical_open_original&,sqlite3*,store_physical_native_scope&)noexcept;
    static void enter_native_scope_(const store_physical_open_original&,sqlite3*,store_physical_native_scope&)noexcept;
    static bool native_continuation_(original*)noexcept;
    static bool native_custody_here_(original*)noexcept;
    static store_physical_vfs* actual_owner_(const ticket&)noexcept;
    static void enter_native_scope_(sqlite3*,store_physical_native_scope&)noexcept;
    static void leave_native_scope_(store_physical_native_scope&)noexcept;
    static void release_(ticket&)noexcept;
    static void release_original_(original*,std::uint64_t)noexcept;
    static bool foreign_protected_()noexcept;
    class attachment_operation final {
        struct implementation;
        implementation* impl_=nullptr;
        explicit attachment_operation(implementation*)noexcept;
        friend class store_physical_api;
    public:
        attachment_operation(attachment_operation&&)noexcept;
        attachment_operation(const attachment_operation&)=delete;
        ~attachment_operation()noexcept;
        void enter();
        void complete();
    };
    static attachment_operation enroll_attachment(sqlite3*,const std::string&,const std::string&,
        const ::lattice::physical_store_identity&,std::int64_t,int actual_attached_limit,std::chrono::steady_clock::time_point);
    static attachment_operation retire_attachment(sqlite3*,const std::string&,const std::string&,std::int64_t);
public:
    static int open(const char*,sqlite3**,int,const char*,std::chrono::steady_clock::time_point,store_physical_open_original* issued=nullptr)noexcept;
    static int open_initializing_(const char*,sqlite3**,int,const char*,std::chrono::steady_clock::time_point,
        store_physical_open_original&,store_resource_initialization_original&,store_resource_initialization_original*);
    static bool process_current()noexcept;
    static void return_mutex_(const store_physical_open_original&,sqlite3_mutex*)noexcept;
    static const store_physical_open_original* mutex_owner_(const ::lattice::database&)noexcept;
    static inline thread_local std::uint64_t* mutex_return_observation_=nullptr;
    struct returned_tail_observation {std::uint64_t completed=0,destroyed=0,budget_enqueued=0,budget_delete_calls=0;};
    static inline thread_local returned_tail_observation* returned_tail_observation_=nullptr;
    // Internal return hook, denial-only: no caller can force a drain beneath
    // either genuine Native/API/VFS stack or the return counter work itself.
    static void returned_tail()noexcept;
    static bool enrolled(sqlite3*)noexcept;
    static bool main_for_lower(sqlite3*,sqlite3_vfs*,sqlite3_file*&)noexcept;
    static sqlite3_vfs* identity_vfs(sqlite3*,const char*,sqlite3_vfs*,sqlite3_file*&)noexcept;
    static int prepare(sqlite3*,const char*,int,unsigned int,sqlite3_stmt**,const char**,bool)noexcept;
    static int clear_bindings(sqlite3_stmt*)noexcept;
    static int create_function(sqlite3*,const char*,int,int,void*,void(*)(sqlite3_context*,int,sqlite3_value**),
        void(*)(sqlite3_context*,int,sqlite3_value**),void(*)(sqlite3_context*),void(*)(void*),bool,bool* actual_native_entered=nullptr)noexcept;
    static int finalize(sqlite3_stmt*)noexcept;
    static int step(sqlite3_stmt*,bool reset)noexcept;
    static int exec(sqlite3*,const char*,int(*)(void*,int,char**,char**),void*,char**)noexcept;
    static int checkpoint(sqlite3*,const char*,int,int*,int*)noexcept;
    static int blob_close(sqlite3_blob*,sqlite3*)noexcept;
    static int close(sqlite3*,bool deferred)noexcept;
    static int close_owned(store_physical_open_original&,sqlite3*,bool deferred)noexcept;
    static void unresourced_open_returned(sqlite3*)noexcept;
};
}
