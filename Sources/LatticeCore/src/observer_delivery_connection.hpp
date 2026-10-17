#pragma once
#include "observer_delivery_plan.hpp"
#include "observer_delivery_sql_runtime.hpp"
#include <chrono>
#include <memory>
#include <lattice/recovery_schema.hpp>

namespace lattice {class lattice_db;}
namespace lattice::detail {
class observer_delivery_factory;
struct observer_delivery_connection_test_access;
// Actual connection-side producer. This remains UNSELECTED/UNINTEGRATED:
// the eventual bounded factory must first close legacy registration/admission
// and provide the reviewed resource/registration profile. No bool enables it.
class observer_delivery_connection final {
    struct activation_key {
    private:
        activation_key()=default;
        friend class observer_delivery_factory;
        friend struct observer_delivery_connection_test_access;
    };
public:
    struct policy {
        observer_delivery_domain::limits domain;
        observer_delivery_meter_budget original;
        std::chrono::milliseconds maximum_admission_wait{0};
    };
private:
    const uint64_t creator_;
    lattice_db* const owner_;
    database* const writer_;
    const recovery_owner_schema catalog_;
    const policy policy_;
    const observer_delivery_store store_;
    const observer_delivery_store::client client_;
    std::shared_ptr<observer_delivery_sql_runtime> runtime_;
    observer_delivery_connection(uint64_t,lattice_db&,database&,recovery_owner_schema,policy,
        observer_delivery_store,observer_delivery_store::client);
    static void destroy(const observer_delivery_connection*)noexcept;
    static void enroll(lattice_db&,policy,activation_key);
    friend class observer_delivery_factory;
    friend class observer_delivery_write;
    friend class observer_delivery_update;
    friend class observer_delivery_begin;
    friend struct observer_delivery_connection_test_access;
    void validate_row(const observer_delivery_values::row_view&)const;
    void validate_plan(const observer_delivery_plan&)const;
    observer_delivery_plan prepare(const observer_delivery_values::cursor&)const;
    observer_delivery_plan prepare(std::span<const observer_delivery_values::row_view>)const;
    observer_delivery_plan prepare(const observer_delivery_values::cursor&,
        const std::shared_ptr<const observer_delivery_original>&)const;
    observer_delivery_plan prepare(std::span<const observer_delivery_values::row_view>,
        const std::shared_ptr<const observer_delivery_original>&)const;
    bool current(const lattice_db&,const database&)const noexcept;
};
}
