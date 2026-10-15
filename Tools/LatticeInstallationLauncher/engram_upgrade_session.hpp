#pragma once
#include "../../Sources/LatticeCore/src/ordinary_owned_launch.hpp"
#include <memory>
#include "../../Sources/LatticeCore/src/ordinary_historical_generation.hpp"

namespace lattice::detail::ordinary_installation {
// Dedicated native upgrade process only. The installer publishes a fixed
// account binding before replacing old images. This retained object consumes
// that binding and owns actual namespace/process resources; it is deliberately
// not constructible from a caller's manifest, PID list or completion Boolean.
class retained_engram_upgrade_session final {
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit retained_engram_upgrade_session(std::shared_ptr<implementation>);
public:
    static retained_engram_upgrade_session open_staged(ordinary_launch::deadline);
    ~retained_engram_upgrade_session();
    retained_engram_upgrade_session(retained_engram_upgrade_session&&) noexcept;
    retained_engram_upgrade_session& operator=(retained_engram_upgrade_session&&) noexcept;
    retained_engram_upgrade_session(const retained_engram_upgrade_session&) = delete;
    retained_engram_upgrade_session& operator=(const retained_engram_upgrade_session&) = delete;
    // Durable deny, exact qualified-role retirement/re-observation, durable
    // completion journal and private same-session preservation handoff. The
    // resulting generation is preserved-unadopted: no database is opened and
    // no ordinary/configured recovery admission is issued by this operation.
    void retire_and_preserve(ordinary_launch::deadline);
    // Same real retirement/exchange as above, followed by a move-only retained
    // handoff. This is not a DB open/profile grant; the fixed managed launcher
    // must still establish a new child cohort and actual enrollment exchange.
    retained_historical_generation retire_and_retain(ordinary_launch::deadline);
    // Fixed candidate runtime only. No caller role/path/filter or ready bit.
    // The new child receiver and post-COMMIT profile remain separate gates.
    int run_managed_recovery(ordinary_launch::deadline);
    // Fixed memory + memory-sync pair; both actual phase2 receivers precede closure.
    int run_managed_cohort_recovery(ordinary_launch::deadline);
};
}
