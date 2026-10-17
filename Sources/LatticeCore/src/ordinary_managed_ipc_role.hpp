#pragma once
#include <memory>
#include <chrono>
#include <functional>
#include <cstdint>
#include <string>
#include <utility>
#include "ordinary_managed_cohort_link.hpp"

namespace lattice { class lattice_db; class swift_lattice_ref; class swift_managed_product_request; class swift_managed_product_route; }
namespace lattice::detail {
class ordinary_managed_profile;
class ordinary_managed_peer_owner;
class ordinary_managed_product_credentials;
class ordinary_managed_ipc_local_route;
class managed_directory_completion;
class ordinary_export_access;
struct ordinary_managed_product_state;
class ordinary_managed_ipc_access;
class ordinary_managed_wss_admission;
class ordinary_managed_wss_handle;
class ordinary_managed_ipc_endpoint_access;
class ordinary_managed_ipc_attempt;
class ordinary_managed_ipc_workers;
class ordinary_managed_ipc_endpoint;
class ordinary_managed_ipc_delivery;

// Operation names select an admission check. They cannot issue a role or
// change its lineage, partition, source, recipient or removal permissions.
enum class ordinary_managed_ipc_operation { receive, send, publication };

class ordinary_managed_ipc_role final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_delivery;
    const std::shared_ptr<ordinary_managed_peer_owner>& peer_owner()const;
    struct implementation;
    std::shared_ptr<const implementation> impl_;
    explicit ordinary_managed_ipc_role(std::shared_ptr<const implementation>);
public:
    ~ordinary_managed_ipc_role();
    ordinary_managed_ipc_role(const ordinary_managed_ipc_role&) = delete;
    ordinary_managed_ipc_role& operator=(const ordinary_managed_ipc_role&) = delete;
    // Live native admission: actual peer/cohort and retained profile lifetime.
    // Durable projection/lineage checks still occur in the actual owned SQL
    // turn before constructing output or publishing input. No SQL/foreign
    // callback is run by this check, and no leaf is held across socket work.
    void validate(ordinary_managed_ipc_operation) const;
};

// Move-only actual connected-descriptor custody. There is intentionally no
// public fd-taking constructor, DTO conversion or ordinary_context conversion.
// The attempt retains this whole object until its real socket/read/write/
// callback users settle; unproved retirement must keep that owner charged.
class ordinary_managed_ipc_connection final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_attempt;
    // Called only by the actual attempt collector after its real operation/
    // callback settlement and owner-worker joins. Destruction alone requests
    // stop and leaves the already registered native owner charged.
    // Also verifies the issuer-owned actual worker/join registry. False means
    // incomplete retirement and retains all custody; errors never become a
    // successful release. No caller-supplied joined/complete Boolean exists.
    bool release_after_settlement(std::chrono::steady_clock::time_point = std::chrono::steady_clock::time_point::max());
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_ipc_connection(std::unique_ptr<implementation>);
public:
    ~ordinary_managed_ipc_connection();
    ordinary_managed_ipc_connection(ordinary_managed_ipc_connection&&) noexcept;
    ordinary_managed_ipc_connection& operator=(ordinary_managed_ipc_connection&&) noexcept;
    ordinary_managed_ipc_connection(const ordinary_managed_ipc_connection&) = delete;
    ordinary_managed_ipc_connection& operator=(const ordinary_managed_ipc_connection&) = delete;
    // Borrow only while retaining this connection; never close it separately.
    int descriptor() const;
    // Sticky issuer admission closure/cancellation only; no release, join or
    // synthetic callback completion. Safe without allocation during teardown.
    void request_stop() noexcept;
    const ordinary_managed_ipc_role& role() const;
    void validate(ordinary_managed_ipc_operation) const;
};

// Paired with one issued connection. Its two worker reservations are made
// before endpoint callback construction; this is a count limit, not a bound on
// arbitrary std::function captures. The concrete endpoint must separately own
// and bound its callback state before handing it here.
class ordinary_managed_ipc_worker_owner final {
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_endpoint_access;
    void bind_endpoint(std::shared_ptr<ordinary_managed_ipc_endpoint>);
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_ipc_worker_owner(std::unique_ptr<implementation>);
public:
    ~ordinary_managed_ipc_worker_owner();
    ordinary_managed_ipc_worker_owner(ordinary_managed_ipc_worker_owner&&) noexcept;
    ordinary_managed_ipc_worker_owner& operator=(ordinary_managed_ipc_worker_owner&&) noexcept;
    ordinary_managed_ipc_worker_owner(const ordinary_managed_ipc_worker_owner&)=delete;
    ordinary_managed_ipc_worker_owner& operator=(const ordinary_managed_ipc_worker_owner&)=delete;
    // Exactly one actual thread per slot. Failed creation keeps an explicit
    // unstarted fact; retained callback destruction occurs outside leaves.
    // Return is not join: the wrapper destroys the actual callback before
    // publishing returned, and only join() records successful native joining.
    void start_reader(std::function<void()>&&);
    void start_writer(std::function<void()>&&);
    void request_stop() noexcept;
    // Actual creator-process check precedes any mutex or fd access. Refuses
    // self-join and returns false on the unchanged deadline without detaching
    // or releasing the record. Joins the real owned std::thread handles only.
    bool join(std::chrono::steady_clock::time_point deadline);
};
class ordinary_managed_ipc_issued final {
    friend class ordinary_managed_ipc_access;
    ordinary_managed_ipc_issued(ordinary_managed_ipc_connection&&,ordinary_managed_ipc_worker_owner&&);
public:
    ordinary_managed_ipc_connection connection;
    ordinary_managed_ipc_worker_owner workers;
    ordinary_managed_ipc_issued(ordinary_managed_ipc_issued&&) noexcept;
    ordinary_managed_ipc_issued& operator=(ordinary_managed_ipc_issued&&) noexcept;
    ordinary_managed_ipc_issued(const ordinary_managed_ipc_issued&)=delete;
    ordinary_managed_ipc_issued& operator=(const ordinary_managed_ipc_issued&)=delete;
};

// Issuance implementation is owned with managed enrollment. Only the actual
// internal endpoint path can call it, before allocating a child/attempt. It
// must validate two actual profiles, actual peer credentials/endpoint custody
// and the product-issued route/partition. An arbitrary fd and enum cannot do
// this. Concrete issuer entrypoints are added with those retained types rather
// than accepting a temporary weak substitute in this initial consumer API.
class ordinary_managed_ipc_access final {
    friend struct ordinary_managed_peer_test_access;
    friend class ordinary_managed_peer_owner;
    friend class ordinary_managed_ipc_delivery;
    // Fixed peer transport creation. Only an actual enrolled primary owner can
    // claim its controller-issued link; this grants no product contribution.
    static std::shared_ptr<ordinary_managed_ipc_endpoint> start_peer_endpoint(std::shared_ptr<lattice_db>,std::chrono::steady_clock::time_point);
    static void bind_peer_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
        std::shared_ptr<const ordinary_managed_ipc_local_route>,std::chrono::steady_clock::time_point);
    static void submit_peer_observation(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
        std::shared_ptr<const ordinary_managed_ipc_local_route>,const std::string&,const std::string&,std::chrono::steady_clock::time_point);
    static ordinary_managed_ipc_issued make_cohort_connection(std::shared_ptr<lattice_db>,
        std::chrono::steady_clock::time_point);
    static std::shared_ptr<ordinary_managed_cohort_link> claim_cohort_link(
        const ordinary_managed_profile&,lattice_db&,database&,ordinary_launch::deadline);
    static ordinary_managed_cohort_link_facts cohort_link_facts(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline);
    static int cohort_link_descriptor(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline);
    static void validate_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline);
    static void stop_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&)noexcept;
    static ordinary_managed_cohort_link_settlement settle_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline)noexcept;
    friend class ::lattice::swift_lattice_ref;
    friend class ::lattice::swift_managed_product_request;
    friend class ::lattice::swift_managed_product_route;
    friend class managed_directory_completion;
    friend class ordinary_export_access;
    friend class ordinary_managed_wss_admission;
    // Only a real locally issued product route can create this connected pair.
    // Remote controller peer links require a distinct issuer and cannot enter
    // this same-process owner/profile path.
    static std::pair<ordinary_managed_ipc_issued,ordinary_managed_ipc_issued> make_local_connections(
        std::shared_ptr<const ordinary_managed_ipc_local_route>,std::chrono::steady_clock::time_point);
    static void validate_local_route(const ordinary_managed_ipc_local_route&,
        std::chrono::steady_clock::time_point);
    static std::shared_ptr<ordinary_managed_wss_handle> start_wss(const ordinary_managed_ipc_local_route&,const std::shared_ptr<ordinary_managed_product_credentials>&,const std::string&);
    static void validate_wss(const ordinary_managed_product_state&,const std::shared_ptr<lattice_db>&,std::chrono::steady_clock::time_point);
    static std::shared_ptr<ordinary_managed_product_state> begin(
        std::shared_ptr<lattice_db> hub,std::shared_ptr<lattice_db> spoke,
        std::shared_ptr<ordinary_managed_product_credentials>,std::uint8_t,const std::string&);
    static std::shared_ptr<const ordinary_managed_ipc_local_route> publish(
        const std::shared_ptr<ordinary_managed_product_state>&);
    static std::uint64_t cookie(const std::shared_ptr<ordinary_managed_product_state>&) noexcept;
    static void cancel(const std::shared_ptr<ordinary_managed_product_state>&) noexcept;
    static bool current(const std::shared_ptr<ordinary_managed_product_state>&) noexcept;
    static std::string principal(const ordinary_managed_ipc_local_route&);
    static bool route_current(const ordinary_managed_ipc_local_route&) noexcept;

    friend class ::lattice::lattice_db;
    friend class ordinary_managed_ipc_endpoint_access;
    // Lifetime-only paired-owner construction. The actual role/descriptor
    // issuer must already own the reserved record; this creates no authority.
    static ordinary_managed_ipc_worker_owner make_workers(std::shared_ptr<ordinary_managed_ipc_workers>);
};
}
