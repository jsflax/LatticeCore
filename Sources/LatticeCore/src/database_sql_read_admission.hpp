#pragma once
#include "observer_delivery_store.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
namespace lattice { class database; }
namespace lattice::detail {
class observer_delivery_connection;
class database_sql_request;
struct database_sql_read_admission_test_access;
// Structured payload/count policy, NOT an allocator/RSS or VM memory limit.
// Zero bytes/duration resolve once from the actual bound profile at enrollment.
struct database_sql_read_policy {
    uint64_t workspace_bytes=0,columns=4096,rows=65536;
    std::chrono::milliseconds maximum_duration{0};
};
class database_sql_read_binding;
class database_sql_read_profile final {
    const uint64_t creator_;
    const void* const publisher_; // compare-only actual issuer, never a lookup
    const observer_delivery_store store_;
    const observer_delivery_store::client client_;
    const database_sql_read_policy policy_;
    const std::chrono::milliseconds maximum_admission_wait_;
    mutable std::atomic<bool> published_{false};
    database_sql_read_profile(uint64_t,const void*,observer_delivery_store,
        observer_delivery_store::client,database_sql_read_policy,std::chrono::milliseconds);
    static void destroy(const database_sql_read_profile*)noexcept;
    static std::shared_ptr<const database_sql_read_profile> create(uint64_t,const void*,
        observer_delivery_store,observer_delivery_store::client,database_sql_read_policy,
        std::chrono::milliseconds);
    void publish()const noexcept;
    friend class observer_delivery_connection;
    friend class observer_delivery_startup;
    friend class observer_delivery_open_startup;
    friend class database_sql_read_binding;
    friend class database_sql_request;
    friend struct database_sql_read_admission_test_access;
};
// One binding per ACTUAL database object/open. It does not own that object or
// any Native connection. Different reader originals share only profile DATA.
class database_sql_read_binding final {
    const uint64_t creator_;
    const database* const owner_;
    const std::shared_ptr<const database_sql_read_profile> profile_;
    database_sql_read_binding(uint64_t,const database*,std::shared_ptr<const database_sql_read_profile>);
    static void destroy(const database_sql_read_binding*)noexcept;
    static std::shared_ptr<const database_sql_read_binding> install(database&,
        const std::shared_ptr<const database_sql_read_profile>&);
    static void remove(database&,const std::shared_ptr<const database_sql_read_binding>&)noexcept;
    bool current(const database&)const noexcept;
    friend class observer_delivery_connection;
    friend class observer_delivery_startup;
    friend class observer_delivery_open_startup;
    friend class database_sql_request;
    friend struct database_sql_read_admission_test_access;
};
}
