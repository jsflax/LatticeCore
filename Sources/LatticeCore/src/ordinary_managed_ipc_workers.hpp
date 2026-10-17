#pragma once
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>

namespace lattice::detail {
class ordinary_managed_ipc_endpoint;
class ordinary_managed_ipc_access;
// Private lifetime accounting only. Reserving this record does not issue an
// IPC role, descriptor, database capability, partition or source authority.
// The actual endpoint issuer must reserve before allocating its callbacks,
// retain the returned record with both issued owners, and release only after
// the attempt's separate reader/writer/callback/descriptor settlement.
class ordinary_managed_ipc_workers final {
    friend class ordinary_managed_ipc_access;
#ifdef LATTICE_MANAGED_IPC_WORKER_QUALIFICATION
    friend struct ordinary_managed_ipc_worker_leaf_fixture;
#endif
    struct state;
    std::shared_ptr<state> state_;
    explicit ordinary_managed_ipc_workers(std::shared_ptr<state>);
    void claim_worker_owner();
public:
    static constexpr std::size_t maximum_owners = 64;
    enum class slot { reader, writer };
    struct observation {
        bool stopping = false;
        bool starting[2]{};
        bool started[2]{};
        bool returned[2]{};
        bool joined[2]{};
        bool released = false;
        bool inherited = false;
        std::exception_ptr first_error;
        std::exception_ptr join_error;
    };
    static std::shared_ptr<ordinary_managed_ipc_workers> reserve();
    ~ordinary_managed_ipc_workers();
    ordinary_managed_ipc_workers(const ordinary_managed_ipc_workers&) = delete;
    ordinary_managed_ipc_workers& operator=(const ordinary_managed_ipc_workers&) = delete;
    void start(slot, std::function<void()>&&);
    // Fixed endpoint type only; no arbitrary finalizer/retirement callback.
    // Issuer calls before either worker starts. Its registry then retains the
    // concrete coordinator even when every public wrapper is dropped.
    void bind_endpoint(std::shared_ptr<ordinary_managed_ipc_endpoint>);
    void request_stop() noexcept;
    // No detach, callback-thread join or callback invocation under the leaf.
    // Waits for actual callback destruction, then joins real thread handles.
    // A deadline overrun is false even if native join subsequently completed;
    // the truthful joined fact remains available to a later collector.
    bool join(std::chrono::steady_clock::time_point deadline);
    // Does NOT close a descriptor or settle an attempt. The actual issuer
    // calls this only after the independent attempt/descriptor release checks.
    // An uncertain join or any unjoined worker retains the registered owner
    // slot. Settled operation failure remains in first_error after release.
    bool release_if_joined();
    observation observe() const;
};
}
