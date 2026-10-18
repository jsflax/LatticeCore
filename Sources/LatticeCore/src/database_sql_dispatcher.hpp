#pragma once
#include <lattice/store_physical_api.hpp>
#include "database_sql_request.hpp"
#include <array>
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <sqlite3.h>
namespace lattice { class database; }
namespace lattice::detail {
enum class database_handler_slot : unsigned char {checked_read,catalog,projection_capture,checkpoint,owned_test,count};
enum class database_authorizer_slot : unsigned char {
    canonical,local_producer,projection_capture,observer_delivery,owned_test_restriction,count
};
// Source-private fixed role table. Only actual database producers bind slots;
// no SQL text, TLS trust bit, arbitrary owning callback or replacement owner.
// Existing callback userdata custody is unchanged. Slots are cleared by exact
// context before that owner retires; retirement marks the entire cookie inactive.
class database_sql_request;
class database_sql_dispatcher final {
    friend class database_sql_request;
    friend class database_initialization_budget;
    friend class observer_delivery_sql_runtime;
    friend class database_sql_request::binding;
    friend class ::lattice::database;
    friend struct database_sql_dispatch_test_access;
    friend struct database_sql_request_test_access;
    using callback=int(*)(void*,int,const char*,const char*,const char*,const char*) noexcept;
    struct slot { callback call=nullptr;void* context=nullptr; };
    static constexpr std::size_t count=std::size_t(database_authorizer_slot::count);
    const uint64_t creator_;
    sqlite3* const native_; // comparison/callback route only; never closed here
    store_physical_policy_original original_; // same actual open's DATA only
    std::array<slot,count> slots_{};
    std::atomic<bool> active_{false};
    // Actual first publication refusal and any separate Native expiration
    // failure, SQLite-mutex guarded. Permanent debt is never made healthy by
    // a later successful clear or by statement/weak-owner ending.
    int publication_failure_=SQLITE_OK,expiration_failure_=SQLITE_OK;
    std::atomic<unsigned> references_{1}; // one actual wrapper DATA reference
    std::atomic<bool> sqlite_reference_{false};
    using progress_callback=int(*)(void*)noexcept;
    using busy_callback=int(*)(void*,int)noexcept;
    struct progress_slot {progress_callback call=nullptr;void* context=nullptr;};
    struct busy_slot {busy_callback call=nullptr;void* context=nullptr;};
    static constexpr std::size_t handler_count=std::size_t(database_handler_slot::count);
    std::array<progress_slot,handler_count> progress_{};
    std::array<busy_slot,handler_count> busy_{};
    bool in_callback_=false;
    // Same-thread, privately issued DATA only. This does not retain a Native
    // owner or grant a SQL purpose; it denies reentry on the actual callback
    // cookie before a new request touches Native or waits for credit.
    struct callback_frame {
        const uint64_t creator;
        const database_sql_dispatcher* const cookie;
        callback_frame* const previous;
        explicit callback_frame(const database_sql_dispatcher&)noexcept;
        ~callback_frame()noexcept;
    };
    static thread_local callback_frame* callbacks_;
    static bool callback_active_for_(const database&)noexcept;
    static bool initialization_admits_(database&,sqlite3_stmt*)noexcept;
    static int progress(void*)noexcept;
    static int busy(void*,int)noexcept;
    bool progress_override_=false,numeric_override_=false,numeric_known_=false;
    int numeric_timeout_=0;
    bool numeric_sampling_=false;
    void refresh_progress_()noexcept;
    int refresh_busy_(bool restoring=false)noexcept;
    bool capture_numeric_()noexcept;
    static int set_progress(database&,database_handler_slot,progress_callback,void*)noexcept;
    static int clear_progress(database&,database_handler_slot,void*)noexcept;
    static int set_busy(database&,database_handler_slot,busy_callback,void*)noexcept;
    static int clear_busy(database&,database_handler_slot,void*,int)noexcept;
    bool request_cleanup_=false;
    database_sql_request* request_=nullptr; // exact current prepare/step; SQLite mutex guarded
    std::atomic<unsigned>* destroyed_observer_=nullptr; // nil in production; DATA-only test counter
    database_sql_dispatcher(uint64_t,sqlite3*,store_physical_policy_original&&)noexcept;
    static int authorize(void*,int,const char*,const char*,const char*,const char*)noexcept;
    static void inaccessible(sqlite3_context*,int,sqlite3_value**)noexcept;
    static void destroy(void*)noexcept;
    static void sqlite_destroy(void*)noexcept;
    void release_()noexcept;
    static int expired_authorize(void*,int,const char*,const char*,const char*,const char*)noexcept;
    static int install(database&)noexcept;
    static int bind(database&,database_authorizer_slot,callback,void*)noexcept;
    static int clear(database&,database_authorizer_slot,void*)noexcept;
    int retire(database&)noexcept;
public:
    database_sql_dispatcher(const database_sql_dispatcher&)=delete;
    database_sql_dispatcher& operator=(const database_sql_dispatcher&)=delete;
};
}
