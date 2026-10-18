#pragma once
#include "store_physical_api.hpp"
#include <lattice/db.hpp>
#include <string>
#include <array>
#include <mutex>
#include <sys/stat.h>

namespace lattice::detail {
class store_resource_open;
struct store_physical_vfs_test_access;
// One actual connection's outer lifetime VFS. The real resource issuer alone
// supplies its already-held independent SH lease and physical MAIN identity.
// This class issues no resource epoch, SQL grant, deletion permit or raw route.
class store_physical_vfs final {
    friend class store_resource_open;
    friend class store_deletion_operation;
    friend class store_physical_api;
    friend class ::lattice::database;
    friend struct store_physical_vfs_test_access;
    struct wrapper;
    static constexpr std::size_t maximum_members=64;
    store_physical_vfs* root_=this;
    mutable std::mutex members_leaf_;
    std::array<store_physical_vfs*,maximum_members-1> members_{};
    std::size_t originals_=1;
    std::uint64_t super_journal_opens_=0; // actual lower xOpen successes; passive component DATA only
    bool primary_ended_=false,sealed_=false;
    sqlite3* native_=nullptr; // DATA identity only, never owns a Native facade
    std::string alias_;std::int64_t token_=0;
    bool attach_entered_=false,attach_returned_=false,detach_active_=false;
    const std::uint64_t creator_;
    store_physical_lifetime::pin held_;
    store_physical_api::original* original_=nullptr;
    sqlite3_vfs* const lower_;
    sqlite3_vfs vfs_{};
    const std::string path_,wal_,journal_,name_;
    const struct stat expected_;
    bool registered_=false;
    store_physical_vfs(const store_physical_lifetime::pin&,sqlite3_vfs*,std::string,const struct stat&,std::uint64_t);
    ~store_physical_vfs()noexcept=default; // actual unregister happens before delete
    bool current_()const noexcept;
    bool capture_shared_(wrapper&)noexcept;
    class vfs_returns final {
        alignas(store_physical_lifetime::api_return) std::byte storage_[maximum_members*sizeof(store_physical_lifetime::api_return)];
        std::size_t count_=0;bool entered_=true;
        store_physical_lifetime::api_return* at_(std::size_t)noexcept;
    public:
        explicit vfs_returns(const wrapper&,bool closing=false)noexcept;
        ~vfs_returns()noexcept;
        bool entered()const noexcept{return entered_;}
    };
    static const sqlite3_io_methods& methods_(int)noexcept;
    static int open_file_(sqlite3_vfs*,const char*,sqlite3_file*,int,int*)noexcept;
    static int close_file_(sqlite3_file*)noexcept;
    static void complete_(void*)noexcept;
    void install_forwarders_()noexcept;
    store_physical_vfs* selected_(const char*,bool main,bool wal,bool journal)noexcept;
    store_physical_vfs* enroll_(sqlite3*,const store_physical_lifetime::pin&,const std::string&,
        const struct stat&,const std::string&,std::int64_t,int actual_attached_limit);
    store_physical_vfs* find_member_(sqlite3*,const std::string&,const std::string&,std::int64_t);
    bool member_entered_(bool detach)noexcept;
    bool member_returned_(bool detach,bool entered,bool clean)noexcept;
    void seal_()noexcept;

    // Called only by the actual resource-open issuer; no public path/flag can
    // construct this VFS or choose a pre-existing original by pointer address.
    static int open_(const store_physical_lifetime::pin&,sqlite3_vfs*,const std::string&,
        const struct stat&,sqlite3**,int,store_physical_open_original* issued=nullptr);
    // DATA peel of our genuine issued MAIN; never casts a private unixFile.
    bool identity_layer_(sqlite3_file*,sqlite3_vfs* observed,sqlite3_file*& inner,
        sqlite3_vfs*& lower)const noexcept;
};
}
