#pragma once
#include "ordinary_managed_ipc_attempt.hpp"
#include "ordinary_managed_ipc_delivery.hpp"
#include <condition_variable>
#include <mutex>
#include <optional>

namespace lattice::detail {
class ordinary_managed_ipc_endpoint_access;

// The issuer owns the only construction path. Its registry retains this exact
// endpoint before either concrete worker starts. Public aliases are neither a
// role factory nor evidence of worker/descriptor retirement.
class ordinary_managed_ipc_endpoint final {
    friend class ordinary_managed_ipc_endpoint_access;
    struct state;
    std::unique_ptr<state> state_;
    ordinary_managed_ipc_endpoint(ordinary_managed_ipc_issued&&,
        std::unique_ptr<ordinary_managed_ipc_delivery>&&, ordinary_ipc_deadline);
    void reader() noexcept;
    void writer() noexcept;
    void record_failure(const ordinary_ipc_result&) noexcept;
public:
    ~ordinary_managed_ipc_endpoint();
    ordinary_managed_ipc_endpoint(const ordinary_managed_ipc_endpoint&)=delete;
    ordinary_managed_ipc_endpoint& operator=(const ordinary_managed_ipc_endpoint&)=delete;
    // A bounded, payload-free hint. This cannot choose a partition or clear
    // durable debt. The actual writer rechecks its queue under the issued role.
    void notify_changed() noexcept;
    void request_stop() noexcept;
    // Must be driven by the actual native owner off both endpoint workers.
    // On false, the issuer registry still owns endpoint, workers and descriptor.
    bool retire(ordinary_ipc_deadline) noexcept;
    struct observation {
        bool stopping=false, reader_returned=false, writer_returned=false;
        bool response_pending=false, delivery_retired=false, collected=false;
        ordinary_ipc_result first_failure;
        std::exception_ptr cleanup_error;
    };
    observation observe() const noexcept;
};

class ordinary_managed_ipc_endpoint_access final {
    friend class ordinary_managed_ipc_access;
    // A supplies the issued pair only after actual peer/profile/product-route
    // admission. The concrete delivery object has its own closed constructor.
    // An absolute finite operation deadline comes from that actual owner;
    // this component does not add a new or longer timeout.
    static void bind_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
        std::shared_ptr<const ordinary_managed_ipc_local_route>,ordinary_ipc_deadline);
    static void submit(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
        std::shared_ptr<const ordinary_managed_ipc_local_route>,const std::string&,const std::string&,ordinary_ipc_deadline);
    static std::shared_ptr<ordinary_managed_ipc_endpoint> start(
        ordinary_managed_ipc_issued&&,
        std::unique_ptr<ordinary_managed_ipc_delivery>&&, ordinary_ipc_deadline);
};
}
