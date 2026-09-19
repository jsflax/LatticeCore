#pragma once
#include "recovery_witness.hpp"
#include <lattice/lattice.hpp>

namespace lattice::detail {
struct recovery_refresh_state;
struct recovery_refresh_prepared;
struct recovery_refresh_worker;
struct recovery_refresh_test_access;
// Private deterministic engine. The public-header forwarding methods exist
// for the portable SDK bridge; no sync dispatcher/capability is activated.
struct recovery_refresh_access {
    friend struct recovery_refresh_worker;
    friend struct recovery_refresh_test_access;
    static uint64_t subscribe(lattice_db&, std::function<void()>);
    static void unsubscribe(lattice_db&, uint64_t);
    static void subscriptions_changed(lattice_db&);
    static void request(lattice_db&) noexcept;
    static notification_token observe_managed(lattice_db&, const std::string&, int64_t,
        const std::string&, std::vector<std::string>, managed_observation_state::callback);
private:
    static std::shared_ptr<recovery_refresh_state> state(lattice_db&, bool create);
    static void process(const std::shared_ptr<recovery_refresh_state>&) noexcept;
    static void signal(const std::shared_ptr<recovery_refresh_state>&) noexcept;
    static std::optional<recovery_refresh_prepared> prepare(lattice_db&, const std::shared_ptr<recovery_refresh_state>&);
    static void deliver(lattice_db&, const std::shared_ptr<recovery_refresh_state>&, const recovery_refresh_prepared&);
};
// Per-owner deterministic preparation only. Configure before subscribing;
// production never calls this and receives the real worker plus periodic retry.
struct recovery_refresh_test_access {
    static void use_manual_preparation(lattice_db&);
};
} // namespace lattice::detail
