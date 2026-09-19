#pragma once
#include "recovery_witness.hpp"
#include <lattice/lattice.hpp>

namespace lattice::detail {
struct recovery_refresh_state;
// Private deterministic engine. The public-header forwarding methods exist
// for the portable SDK bridge; no sync dispatcher/capability is activated.
struct recovery_refresh_access {
    static uint64_t subscribe(lattice_db&, std::function<void()>);
    static void unsubscribe(lattice_db&, uint64_t);
    static void subscriptions_changed(lattice_db&);
    static void request(lattice_db&) noexcept;
private:
    static std::shared_ptr<recovery_refresh_state> state(lattice_db&, bool create);
    static void deliver(lattice_db&, const std::shared_ptr<recovery_refresh_state>&);
};
} // namespace lattice::detail
