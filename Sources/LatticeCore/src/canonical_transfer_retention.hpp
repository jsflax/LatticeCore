#pragma once
#include "recovery_writer_access.hpp"
#include <cstdint>
#include <memory>
#include <optional>

namespace lattice::detail {
class canonical_writer_adapter;
// Private, unadvertised source retention only. These limits do not reserve
// spool bytes or confer peer/route/authentication, READY or serving authority.
struct canonical_retention_limits {
    int64_t attempts, duration_ms;
    bool operator==(const canonical_retention_limits&) const = default;
};
class canonical_retention_ticket {
    friend class canonical_writer_adapter;
    std::weak_ptr<void> session_;
    int64_t incarnation_, id_, base_, deadline_;
    std::optional<int64_t> requested_base_;
    canonical_retention_ticket(std::weak_ptr<void> session, int64_t incarnation,
        int64_t id, int64_t base, int64_t deadline, std::optional<int64_t> requested)
        : session_(std::move(session)), incarnation_(incarnation), id_(id), base_(base), deadline_(deadline), requested_base_(requested) {}
public:
    int64_t protected_base() const noexcept { return base_; }
};
struct canonical_retention_result {
    // Committed with a postcommit_error still owns its durable reservation.
    // Unsettled/ownership_lost never mean durable absence or permit replay.
    recovery_install_result settlement;
    std::optional<canonical_retention_ticket> reservation;
};
}
