#pragma once
#include "ordinary_managed_peer_owner.hpp"
#include "ordinary_managed_upstream_capture.hpp"
#include "ordinary_managed_contribution_ledger.hpp"
#include <span>
#include <vector>

namespace lattice::detail {
class ordinary_managed_ipc_local_route;
class ordinary_managed_ipc_delivery;
// One actual daemon-issued request. This immutable body can only be produced
// from the authenticated route and its actual accepted-original capture. It is
// retained until an exact recipient result or unresolved debt handoff; a socket
// write is never handling, projection, repair or ACK completion.
class ordinary_managed_peer_request final {
    friend class ordinary_export_access;
    friend class ordinary_managed_ipc_delivery;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_peer_request(std::unique_ptr<implementation>);
public:
    ~ordinary_managed_peer_request();
    ordinary_managed_peer_request(const ordinary_managed_peer_request&)=delete;
};
// Staging is passive even after parsing/hash verification. Only the fixed
// peer delivery state machine, with the actual issued opposite image/link and
// one-shot request correlation, may request its final owned observation turn.
class ordinary_managed_peer_staged final {
    friend class ordinary_export_access;
    friend class ordinary_managed_ipc_delivery;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_peer_staged(std::unique_ptr<implementation>);
public:
    ~ordinary_managed_peer_staged();
    ordinary_managed_peer_staged(const ordinary_managed_peer_staged&)=delete;
};
}
