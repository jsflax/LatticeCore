#pragma once
#include "ordinary_managed_cohort_link.hpp"
#include <atomic>
#include <memory>
#include <string>

namespace lattice { class lattice_db; }
namespace lattice::detail {
namespace managed_ipc_connection_private {
struct connection_record;
bool release_record(const std::shared_ptr<connection_record>&,ordinary_launch::deadline);
}
class ordinary_export_access;
class ordinary_managed_ipc_access;
class ordinary_managed_ipc_role;
class ordinary_managed_ipc_delivery;
// A retained actual primary owner and the controller's one-shot paired link.
// This is transport/physical custody only. Only the real daemon product
// producer can subsequently issue a request; a peer JSON object cannot.
class ordinary_managed_peer_owner final {
    friend class ordinary_export_access;
    friend struct managed_ipc_connection_private::connection_record;
    friend bool managed_ipc_connection_private::release_record(const std::shared_ptr<managed_ipc_connection_private::connection_record>&,ordinary_launch::deadline);
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_role;
    friend class ordinary_managed_ipc_delivery;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_peer_owner(std::unique_ptr<implementation>);
    void validate_lifetime()const;
    void validate(ordinary_launch::deadline)const;
    const ordinary_managed_cohort_link_facts& facts()const;
    int descriptor()const;
    void request_stop()noexcept;
    ordinary_managed_cohort_link_settlement settle(ordinary_launch::deadline)noexcept;
public:
    ~ordinary_managed_peer_owner();
    ordinary_managed_peer_owner(const ordinary_managed_peer_owner&)=delete;
    ordinary_managed_peer_owner& operator=(const ordinary_managed_peer_owner&)=delete;
};
}
