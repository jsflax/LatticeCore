#pragma once
#include "recovery_local_producer.hpp"

namespace lattice { class swift_lattice_ref; }
namespace lattice::detail {
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
};
class recovery_continuous_producer;
class recovery_continuous_route;
class recovery_continuous_work;
struct recovery_continuous_admission;
struct recovery_continuous_state;
class verified_unsent_set {
    friend class recovery_continuous_producer;
    std::shared_ptr<lattice_db> owner_;
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
    static recovery_install_result export_owned(std::shared_ptr<lattice_db>,
        const std::shared_ptr<recovery_continuous_work>&,const std::function<void(database&)>&);
    static void setup_configured_route(lattice_db&);
    static bool attached(const lattice_db&) noexcept;
    static void require_no_continuous_export(database&);
    static void require_no_continuous_route(lattice_db&);
    static recovery_install_result enroll(std::shared_ptr<lattice_db>);
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
    static constexpr bool source_authentication_capability=false;
    static constexpr bool automatic_recovery_capability=false;
};
// Read-only/constructor guards. Absence never grants a capability.
void require_continuous_path_unowned(const std::string&);
void require_continuous_legacy_export_absent(database&);
void require_continuous_raw_handle_absent(database&);
} // namespace lattice::detail
