#pragma once
#include <chrono>
#include <cstdint>
#include <utility>

namespace lattice::detail {
class store_resource_open;
class store_physical_vfs;
class store_physical_api;
class store_deletion_operation;
struct store_physical_lifetime_test_access;

// Private physical-lifetime DATA only. No SQLite pointer, callback, SQL permit,
// pathname authority or ownership of a Native facade is retained here.
class store_physical_lifetime final {
    friend class store_resource_open;
    friend class store_physical_vfs;
    friend class store_physical_api;
    friend class store_deletion_operation;
    friend struct store_physical_lifetime_test_access;
    struct state;
    static bool retain_(state*) noexcept;
    static void release_(state*) noexcept;
    static void settle_(state*) noexcept;
public:
    class pin final {
        friend class store_physical_lifetime;
        friend class store_physical_vfs;
        friend class store_physical_api;
        friend class store_deletion_operation;
        state* value_=nullptr;
        explicit pin(state* value)noexcept:value_(value){}
    public:
        pin()noexcept=default;
        pin(const pin& other)noexcept;
        pin(pin&& other)noexcept:value_(std::exchange(other.value_,nullptr)){}
        pin& operator=(const pin&)=delete;
        pin& operator=(pin&& other)noexcept;
        ~pin()noexcept;
        bool current()const noexcept;
    };
    enum class file_kind : std::uint8_t { main, wal, journal, temporary };
    class file_original final {
        friend class store_physical_lifetime;
        friend class store_physical_vfs;
        state* owner_=nullptr;
        std::uint64_t serial_=0;
        file_kind kind_=file_kind::main;
        bool returned_=false,methods_=false,closed_=false,shm_mapped_=false;
        std::uint64_t fetches_=0;
    public:
        file_original()noexcept=default;
    };
    struct observation {
        bool current=false,open_returned=false,connection_retired=false;
        bool physical_ended=false,unknown=false,lease_close_returned=false;
        std::uint64_t active_api=0,active_vfs=0,builders=0,files=0;
        int first_native_close=-1,fallback_close=-1,first_file_close_error=0;
        int lease_close_error=0,first_cleanup_error=0;
    };
    // Constructor is private to the genuine resource-open issuer. The passed
    // descriptor is an ALREADY ACQUIRED independent physical SH lease, never
    // MAIN and never the process-lived generation_busy lease.
private:
    static pin adopt_lease_(int owned_lease);
    static void native_open_entered_(const pin&)noexcept;
    static void native_open_returned_(const pin&,bool actual_handle_present)noexcept;
    static void native_close_returned_(const pin&,bool deferred,int actual_result)noexcept;
    static file_original file_open_entered_(const pin&,file_kind)noexcept;
    static void file_open_returned_(const pin&,file_original&,bool has_actual_methods)noexcept;
    static void file_close_returned_(const pin&,file_original&,int actual_result)noexcept;
    static void shm_map_returned_(const pin&,file_original&,bool,int)noexcept;
    static void shm_unmap_returned_(const pin&,file_original&,int)noexcept;
    static void fetch_returned_(const pin&,file_original&,bool,int)noexcept;
    static void unfetch_returned_(const pin&,file_original&,bool,int)noexcept;
    static void cleanup_returned_(const pin&,int)noexcept;
    static observation observe_(const pin&)noexcept;
    static bool wait_until_(const pin&,std::chrono::steady_clock::time_point)noexcept;

    // Exact owned C API frame. Retires only AFTER actual close/finalize/blob
    // close/backup-finish return; VFS xClose alone is too early for WAL unlink.
    class api_return final {
        friend class store_physical_lifetime;
        friend class store_physical_vfs;
        friend class store_physical_api;
        friend struct store_physical_lifetime_test_access;
        pin held_;
        api_return* previous_=nullptr;
        bool entered_=false,vfs_=false;
        static thread_local api_return* current_;
        explicit api_return(const pin&,bool vfs=false,bool closing_vfs=false)noexcept;
        static bool contains_(state*)noexcept;
    public:
        api_return(const api_return&)=delete;
        api_return& operator=(const api_return&)=delete;
        ~api_return()noexcept;
        bool entered()const noexcept{return entered_;}
    };
};
} // namespace lattice::detail
