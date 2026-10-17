#pragma once
#include "store_physical_lifetime.hpp"
#include <lattice/store_physical_api.hpp>

namespace lattice::detail {
class store_physical_vfs;
class store_physical_api final {
    friend class store_physical_vfs;
    struct original;
    static original* head_;
    struct ticket {
        original* value=nullptr;
        std::uint64_t creator=0;
        bool inherited=false;
        ticket()noexcept=default;
        ticket(const ticket&)=delete;
        ticket& operator=(const ticket&)=delete;
        ticket(ticket&& other)noexcept:value(std::exchange(other.value,nullptr)),creator(other.creator),inherited(other.inherited){}
        ~ticket()noexcept;
    };
    // Preallocated BEFORE SQLite open. Only the actual lifetime VFS can bind
    // this to the genuine open return and its already-issued physical lease.
    static original* prepare_(const store_physical_lifetime::pin&);
    static bool publish_(original*,sqlite3*,void*,void(*)(void*)noexcept)noexcept;
    static ticket capture_issued_(original*)noexcept;
    static void abandon_unpublished_(original*)noexcept;
    static ticket capture_(sqlite3*)noexcept;
    static void release_(ticket&)noexcept;
    static bool foreign_protected_()noexcept;
public:
    static int finalize(sqlite3_stmt*)noexcept;
    static int step(sqlite3_stmt*,bool reset)noexcept;
    static int exec(sqlite3*,const char*,int(*)(void*,int,char**,char**),void*,char**)noexcept;
    static int checkpoint(sqlite3*,const char*,int,int*,int*)noexcept;
    static int blob_close(sqlite3_blob*,sqlite3*)noexcept;
    static int close(sqlite3*,bool deferred)noexcept;
    static void unresourced_open_returned(sqlite3*)noexcept;
};
}
