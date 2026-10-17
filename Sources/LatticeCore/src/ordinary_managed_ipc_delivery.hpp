#pragma once
#include "ordinary_ipc_frame_io.hpp"
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <cstdint>
#include <mutex>
#include <atomic>

namespace lattice::detail {
class ordinary_managed_ipc_role;
class ordinary_managed_ipc_access;
class ordinary_managed_ipc_endpoint;
class ordinary_managed_peer_owner;
class ordinary_managed_ipc_local_route;
class ordinary_managed_ipc_endpoint_access;
class ordinary_export_access;

// Closed fixed upstream-observation output. Actual requests retain their
// accepted original until an exact recipient outcome; no public byte factory.
// This implementation does not grant partition materialization or convergence.
class ordinary_managed_ipc_output final {
    friend class ordinary_managed_ipc_delivery;
    friend class ordinary_managed_ipc_endpoint;
    void validate_before_send(const ordinary_managed_ipc_role&,ordinary_ipc_deadline);
    ordinary_ipc_deadline deadline(ordinary_ipc_deadline)const;
    struct state;
    std::unique_ptr<state> state_;
    explicit ordinary_managed_ipc_output(std::unique_ptr<state>);
public:
    // Moves transfer the unique payload and leave the source empty. Destroying
    // that empty shell cannot invoke an owner/callback or release live storage.
    ~ordinary_managed_ipc_output();
    ordinary_managed_ipc_output(ordinary_managed_ipc_output&&) noexcept;
    ordinary_managed_ipc_output& operator=(ordinary_managed_ipc_output&&) noexcept;
    ordinary_managed_ipc_output(const ordinary_managed_ipc_output&)=delete;
    ordinary_managed_ipc_output& operator=(const ordinary_managed_ipc_output&)=delete;
    std::span<const uint8_t> frame() const;
};

// The constructor is reserved for the actual two-profile/product-route issuer.
// No arbitrary receive/producer callbacks, fd, channel string or wire flag can
// substitute for it. The fixed peer implementation stages exact accepted
// observations only; partition materialization is a separate gated contract.
class ordinary_managed_ipc_delivery final {
    friend class ordinary_export_access; // fixed private resource regression only
    friend class ordinary_managed_ipc_access;
    friend class ordinary_managed_ipc_endpoint;
    friend class ordinary_managed_ipc_output;
    friend class ordinary_managed_ipc_endpoint_access;
    static std::unique_ptr<ordinary_managed_ipc_delivery> for_peer(const std::shared_ptr<ordinary_managed_peer_owner>&);
    void bind_route(std::shared_ptr<const ordinary_managed_ipc_local_route>,ordinary_ipc_deadline);
    void submit(std::shared_ptr<const ordinary_managed_ipc_local_route>,const std::string&,const std::string&,ordinary_ipc_deadline);
    ordinary_managed_ipc_output output(std::uint8_t,std::int64_t,std::size_t,std::size_t,const std::string&,std::span<const std::uint8_t>);
    void validate_output(const ordinary_managed_ipc_role&,ordinary_managed_ipc_output&,ordinary_ipc_deadline);
    struct state;
    std::unique_ptr<state> state_;
    explicit ordinary_managed_ipc_delivery(std::unique_ptr<state>);
    // There is at most one prepare/output_written worker and one concurrent
    // consume worker. The implementation must serialize its actual durable
    // turns without retaining endpoint leaves across SQL or foreign callbacks.
    // Pending durable requests precede fresh classification. A policy mismatch
    // retains its request; local visibility removal is never a fleet DELETE.
    std::optional<ordinary_managed_ipc_output> prepare_output(
        const ordinary_managed_ipc_role&, ordinary_ipc_deadline);
    // Native role + owned-SQL admission chooses the closed protocol path.
    // Imported lineage comes from the accepted source association, not the
    // local profile. Filtered completeness cannot choose whole-scope apply.
    std::optional<ordinary_managed_ipc_output> consume_input(
        const ordinary_managed_ipc_role&, std::span<const uint8_t>, ordinary_ipc_deadline);
    // Transmission cursor only; suppress repeat emission on this attempt until
    // its actual reply or native retry boundary, avoiding a busy resend loop.
    // Neither request handling nor repair debt is
    // cleared here; exact version ACK and partition receipt have separate turns.
    void output_written(const ordinary_managed_ipc_role&, ordinary_managed_ipc_output&);
    // Bounds idle waiting by any already admitted request. Returning a later
    // caller slice never changes a request's original absolute expiry.
    ordinary_ipc_deadline wait_deadline(ordinary_ipc_deadline);
    void request_stop() noexcept;
    // Actual observer unregister/join and frame/owner capture retirement, not
    // a caller Boolean. False or failure retains this complete owner.
    bool retire(ordinary_ipc_deadline);
public:
    ~ordinary_managed_ipc_delivery();
    ordinary_managed_ipc_delivery(const ordinary_managed_ipc_delivery&)=delete;
    ordinary_managed_ipc_delivery& operator=(const ordinary_managed_ipc_delivery&)=delete;
};
}
