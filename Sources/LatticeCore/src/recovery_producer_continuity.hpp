#pragma once
#include "recovery_local_producer.hpp"

namespace lattice { class swift_lattice_ref; }
namespace lattice::detail {
class recovery_receiver_controller;
class recovery_receiver_route;
class recovery_reconciliation_descriptor;
class receiver_source_binding;
class sync_callback_lifetime;
enum class recovery_continuous_receiver_profile : int64_t { disabled=0, full_canonical_v2=2 };
// Private opt-in storage/route policy. These spellings do not authenticate a
// source or namespace. The actual accepted application/TLS issuer is separate.
struct recovery_continuous_contribution {
    recovery_obligation_profile profile;
    std::vector<std::string> models;
    std::vector<uint8_t> incoming_grant_claim;
};
struct recovery_continuous_route_policy {
    std::string sync_id, endpoint;
    bool operator==(const recovery_continuous_route_policy&) const = default;
};
struct recovery_continuous_policy {
    std::vector<recovery_continuous_contribution> contributions;
    std::vector<recovery_continuous_route_policy> routes;
    recovery_obligation_producer_discovery_limits limits{};
    // Counts are admission caps, not a queue or a promise to buffer writes.
    size_t owners=0, physical_routes=0, operations=0, frozen_entries=0;
    uint64_t frozen_bytes=0;
    recovery_continuous_receiver_profile receiver=recovery_continuous_receiver_profile::disabled;
};
class recovery_continuous_producer;
class recovery_continuous_route;
class recovery_continuous_work;
struct recovery_continuous_admission;
struct recovery_continuous_state;
class verified_unsent_set {
    friend class recovery_continuous_producer;
    std::shared_ptr<lattice_db> owner_;
    std::weak_ptr<lattice_db> controller_owner_;
    std::weak_ptr<recovery_continuous_state> state_;
    int64_t incarnation_=0,barrier_=0;
    std::vector<recovery_obligation_snapshot> journals_;
    std::string policy_digest_;
    std::vector<std::string> originals_;
    verified_unsent_set()=default;
public:
    verified_unsent_set(const verified_unsent_set&)=default;
    verified_unsent_set(verified_unsent_set&&) noexcept=default;
    verified_unsent_set& operator=(const verified_unsent_set&)=default;
    verified_unsent_set& operator=(verified_unsent_set&&) noexcept=default;
    // Read-only local provenance. Neither these rows nor this object's
    // existence authorize source receipts, an install, ACK, or a remote peer.
    const std::vector<recovery_obligation_snapshot>& frozen_journals()const noexcept{return journals_;}
    const std::vector<std::string>& canonical_originals()const noexcept{return originals_;}
};
class recovery_continuous_barrier {
    friend class recovery_continuous_producer;
    std::shared_ptr<lattice_db> owner_;
    std::weak_ptr<recovery_continuous_state> state_;
    int64_t incarnation_=0,barrier_=0,attempt_=0;
    recovery_continuous_barrier()=default;
public:
    recovery_continuous_barrier(const recovery_continuous_barrier&)=default;
};
struct recovery_continuous_open_result {
    recovery_install_result settlement;
    std::shared_ptr<lattice_db> owner;
};
struct recovery_continuous_quiescence {
    recovery_install_result settlement;
    bool waiting=false;
    std::optional<recovery_continuous_barrier> barrier;
    std::optional<verified_unsent_set> unsent;
};
// Internal counted cells are constructed only at the actual native WSS route
// and retained through preparation/handoff. Destruction settles local work; it
// never clears durable claims or establishes remote cancellation.
class recovery_continuous_work {
    friend class recovery_continuous_producer;
    std::shared_ptr<recovery_continuous_state> state_;
    std::shared_ptr<lattice_db> owner_;
    uint64_t route_=0, physical_=0;
    int64_t incarnation_=0;
    std::shared_ptr<const recovery_reconciliation_descriptor> reconciliation_;
    std::shared_ptr<recovery_continuous_route> restricted_route_;
    recovery_continuous_work()=default;
public:
    ~recovery_continuous_work();
    recovery_continuous_work(const recovery_continuous_work&)=delete;
    recovery_continuous_work& operator=(const recovery_continuous_work&)=delete;
};
class recovery_continuous_route {
    friend class recovery_continuous_producer;
    std::shared_ptr<recovery_continuous_state> state_;
    std::weak_ptr<lattice_db> owner_;
    uint64_t route_=0;
    int64_t incarnation_=0;
    recovery_continuous_route()=default;
public:
    ~recovery_continuous_route();
    recovery_continuous_route(const recovery_continuous_route&)=delete;
    recovery_continuous_route& operator=(const recovery_continuous_route&)=delete;
};
class recovery_continuous_producer {
    friend class ::lattice::swift_lattice_ref;
    friend struct recovery_continuous_admission;
    // Closed recipe: only native and Swift retained-owner factories construct it.
    // Captures immutable declarations, never an owner or application callback.
    struct owner_recipe {
        std::function<std::shared_ptr<lattice_db>(const configuration&,const std::shared_ptr<recovery_continuous_admission>&)> construct;
        std::function<void(const std::shared_ptr<lattice_db>&)> publish_pointer;
    };
    static recovery_continuous_open_result open_owned(const configuration&,const recovery_continuous_policy&,
        std::shared_ptr<const owner_recipe>);
    friend class recovery_local_producer_adapter;
    friend class recovery_export_adapter;
    friend class recovery_export_route;
    friend class recovery_server_export_endpoint;
    friend class ::lattice::lattice_db;
    friend class ::lattice::database;
    friend class ::lattice::synchronizer_base;
    friend class recovery_receiver_controller;
    friend class recovery_receiver_route;
    friend class recovery_unknown_reconciliation;
    friend class receive_install_store;
    friend class recovery_obligation_store;
    friend class recovery_obligation_producer_store;
    friend std::shared_ptr<database> open_continuous_writer(const configuration&,const std::shared_ptr<recovery_continuous_admission>&);
    friend void require_continuous_legacy_export_absent(database&);
    friend void require_continuous_raw_handle_absent(database&);
    static void classify_open(database&,bool private_admission,bool writable);
    static std::shared_ptr<database> open_writer(const configuration&,
        const std::shared_ptr<recovery_continuous_admission>&);
    static std::shared_ptr<recovery_continuous_route> admit_route(
        std::shared_ptr<lattice_db>,const sync_config&,bool injected);
    static std::shared_ptr<recovery_continuous_work> admit_work(
        const std::shared_ptr<recovery_continuous_route>&,std::shared_ptr<lattice_db>,uint64_t);
    static void verify_work(const std::shared_ptr<recovery_continuous_work>&,lattice_db&,database&);
    static std::shared_ptr<recovery_continuous_work> admit_reconciliation_work(
        const std::shared_ptr<const recovery_reconciliation_descriptor>&,
        const std::shared_ptr<recovery_continuous_route>&,std::shared_ptr<lattice_db>,uint64_t physical);
    static recovery_install_result reconciliation_export_owned(std::shared_ptr<lattice_db>,
        const std::shared_ptr<recovery_continuous_work>&,const std::vector<std::string>& ordered_originals,
        const std::function<void(database&)>&);
    static recovery_install_result export_owned(std::shared_ptr<lattice_db>,
        const std::shared_ptr<recovery_continuous_work>&,const std::function<void(database&)>&);
    static void setup_configured_route(lattice_db&);
    static bool attached(const lattice_db&) noexcept;
    static bool shared_install_domains(const lattice_db&)noexcept;
    static void require_no_continuous_export(database&);
    static void require_no_continuous_route(lattice_db&);
    static recovery_install_result enroll(std::shared_ptr<lattice_db>);
    static std::shared_ptr<recovery_receiver_route> attach_receiver(
        const std::shared_ptr<recovery_continuous_route>&,std::shared_ptr<lattice_db>,
        const std::shared_ptr<receiver_source_binding>&,const std::shared_ptr<owned_platform_sync_transport>&,
        const std::shared_ptr<scheduler>&,const std::shared_ptr<sync_callback_lifetime>&);
    static recovery_install_result controller_owned(const recovery_receiver_controller&,
        std::shared_ptr<lattice_db>,const std::function<void(database&)>&);
    static void controller_park_proof(verified_unsent_set&);
    static int64_t controller_next_attempt_owned(std::shared_ptr<lattice_db>);
    static const recovery_owner_schema& controller_catalog(const lattice_db&)noexcept;
    static void controller_installed_owned(const recovery_receiver_controller&,
        const verified_unsent_set&);
    static void controller_resume_owned(const recovery_receiver_controller&,
        std::shared_ptr<lattice_db>,int64_t barrier,int64_t attempt);
    static void controller_transition_reconcile_owned(const recovery_receiver_controller&,std::shared_ptr<lattice_db>,
        int64_t phase,int64_t barrier,int64_t attempt,int64_t next_phase,int64_t next_barrier,int64_t next_attempt);
    static void controller_publish_reconcile(const recovery_receiver_controller&,std::shared_ptr<lattice_db>,int64_t barrier,int64_t attempt);
    static void controller_publish_resume(const recovery_receiver_controller&,
        std::shared_ptr<lattice_db>,int64_t barrier,int64_t attempt);
public:
    // Actual retained unpublished-owner factory. No borrowed/no-op shared
    // owner may turn constructor setup into the owned enrollment transaction.
    static recovery_continuous_open_result open(const configuration&,const recovery_continuous_policy&);
    static recovery_continuous_quiescence begin(std::shared_ptr<lattice_db>,int64_t logical_attempt);
    // Nonblocking: waiting means admitted work still exists. Caller resumes
    // on its existing scheduler; no wait under SQLite/registry/owner locks.
    static recovery_continuous_quiescence finish(const recovery_continuous_barrier&);
    // Authoritative same-generation inspection, including restart with an
    // already closed durable barrier. Does not reopen admission.
    static recovery_continuous_quiescence inspect(std::shared_ptr<lattice_db>);
    static recovery_install_result cancel(const recovery_continuous_barrier&);
    // Revalidate actual frozen Q, producer stamps, continuous coverage and
    // physical lifetime under the same owned WRITE as a future consumer.
    static void verify_for_owned_write(const verified_unsent_set&);
    // Producer primitives alone grant neither source authority nor recovery.
    // The explicit receiver profile activates only through the real controller.
    static constexpr bool source_authentication_capability=false;
    static constexpr bool automatic_recovery_capability=false;
};
// Read-only/constructor guards. Absence never grants a capability.
void require_continuous_path_unowned(const std::string&);
void require_continuous_legacy_export_absent(database&);
void require_continuous_raw_handle_absent(database&);
} // namespace lattice::detail
