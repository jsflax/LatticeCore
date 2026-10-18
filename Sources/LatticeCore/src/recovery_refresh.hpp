#pragma once
#include "recovery_witness.hpp"
#include <lattice/lattice.hpp>

namespace lattice::detail {
struct recovery_refresh_state;
struct recovery_refresh_prepared;
struct recovery_refresh_worker;
struct recovery_refresh_test_access;
struct registry_depth_failure_observation {
    uint64_t holds_before = 0, holds_after = 0;
    bool callback_entered = false, caught_bad_alloc = false, depth_publication_reached = false;
};
struct automatic_maintenance_snapshot {
    bool enabled = false, running = false;
    size_t pause_count = 0;
    uint64_t attempts = 0, quiet_returns = 0, deferred = 0, completed = 0;
};
// Private deterministic engine. The public-header forwarding methods exist
// for the portable SDK bridge; no sync dispatcher/capability is activated.
struct recovery_refresh_access {
    friend struct recovery_refresh_worker;
    friend struct recovery_refresh_test_access;
    static uint64_t subscribe(lattice_db&, std::function<void()>);
    static void unsubscribe(lattice_db&, uint64_t);
    static void subscriptions_changed(lattice_db&);
    static void request(lattice_db&) noexcept;
    static bool enable_automatic_maintenance(lattice_db&);
    static notification_token observe_managed(lattice_db&, const std::string&, int64_t,
        const std::string&, std::vector<std::string>, managed_observation_state::callback);
private:
    struct statement_guard;
    static bool settled(database&, sqlite3*, std::shared_ptr<const physical_store_identity>* identity = nullptr);
    static std::shared_ptr<recovery_refresh_state> state(lattice_db&, bool create);
    static void process(const std::shared_ptr<recovery_refresh_state>&) noexcept;
    static void maintain(const std::shared_ptr<recovery_refresh_state>&) noexcept;
    enum class maintenance_result { quiet, deferred, completed };
    static maintenance_result maintain_owner(lattice_db&);
    static bool retire_due_generations(lattice_db&, bool pressure, bool& work);
    static std::shared_ptr<database> test_keeper(lattice_db&, uint64_t);
    static registry_depth_failure_observation test_registry_depth_failure(lattice_db&, bool);
    static void signal(const std::shared_ptr<recovery_refresh_state>&) noexcept;
    static std::optional<recovery_refresh_prepared> prepare(lattice_db&, const std::shared_ptr<recovery_refresh_state>&);
    static void deliver(lattice_db&, const std::shared_ptr<recovery_refresh_state>&, const recovery_refresh_prepared&);
};
// Per-owner deterministic preparation only. Configure before subscribing;
// production never calls this and receives the real worker plus periodic retry.
struct recovery_refresh_test_access {
    static void use_manual_preparation(lattice_db&);
    // Synchronous deterministic probe for tests configured as manual. Counts
    // every writer inspection in this call, without installing a user callback.
    // A prepared reader may publish, but this seam does not deliver/ack it.
    // The test must serialize requests and retain its owner for the whole call.
    static bool prepare_once(lattice_db&, size_t& writer_inspections);
    // Inert private fixture controls, never called by production. Null pause
    // means ONLY a genuinely running automatic turn; every other refusal
    // throws. A held token fences new turns without clearing any obligation.
    static std::shared_ptr<void> pause_automatic_maintenance(lattice_db&);
    static automatic_maintenance_snapshot automatic_snapshot(lattice_db&);
    // Thread-local synthetic bad_alloc in the next fresh enrollment. This is
    // a transaction rollback mechanism, not simulated OS resource exhaustion.
    static std::shared_ptr<void> fail_next_automatic_enrollment();
    // The actual retained keeper, solely for real SQLite fault injection.
    // Does not open a connection, issue a generation, or synthesize an ending.
    static std::shared_ptr<database> actual_read_keeper(lattice_db&, uint64_t);
    // Injects bad_alloc at the real notify-held/TLS-publication edge, not an
    // OS allocation failure. Reports actual held-counter and callback facts.
    static registry_depth_failure_observation registry_depth_failure(lattice_db&, bool creator_bound);
};
} // namespace lattice::detail
