#pragma once
#include "store_physical_api.hpp"
#include <lattice/db.hpp>
#include <string>
#include <sys/stat.h>

namespace lattice::detail {
class store_resource_open;
struct store_physical_vfs_test_access;
// One actual connection's outer lifetime VFS. The real resource issuer alone
// supplies its already-held independent SH lease and physical MAIN identity.
// This class issues no resource epoch, SQL grant, deletion permit or raw route.
class store_physical_vfs final {
    friend class store_resource_open;
    friend class ::lattice::database;
    friend struct store_physical_vfs_test_access;
    struct wrapper;
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
    static const sqlite3_io_methods& methods_(int)noexcept;
    static int open_file_(sqlite3_vfs*,const char*,sqlite3_file*,int,int*)noexcept;
    static int close_file_(sqlite3_file*)noexcept;
    static void complete_(void*)noexcept;
    void install_forwarders_()noexcept;
    // Called only by the actual resource-open issuer; no public path/flag can
    // construct this VFS or choose a pre-existing original by pointer address.
    static int open_(const store_physical_lifetime::pin&,sqlite3_vfs*,const std::string&,
        const struct stat&,sqlite3**,int);
    // DATA peel of our genuine issued MAIN; never casts a private unixFile.
    bool identity_layer_(sqlite3_file*,sqlite3_vfs* observed,sqlite3_file*& inner,
        sqlite3_vfs*& lower)const noexcept;
};
}
