#pragma once
#include "ordinary_ipc_frame_io.hpp"
#include "ordinary_managed_ipc_role.hpp"
#include <functional>
#include <memory>

namespace lattice::detail {
// A single issued endpoint. No reconnect, path-based constructor, substitute
// role, internal worker, detached task or cross-attempt descriptor reuse.
class ordinary_managed_ipc_attempt final {
    struct state;
    std::shared_ptr<state> state_;
    static void stop(const std::shared_ptr<state>&) noexcept;
public:
    using receive_callback = std::function<void(std::span<const uint8_t>, const ordinary_managed_ipc_role&)>;
    struct observation {
        bool stopping = false, reader = false, writer = false, callback = false;
        bool collecting = false, collected = false;
        size_t terminal_calls = 0;
        ordinary_ipc_result first_failure;
        int shutdown_error = 0;
        std::exception_ptr cleanup_error;
    };
    explicit ordinary_managed_ipc_attempt(ordinary_managed_ipc_connection&&);
    ~ordinary_managed_ipc_attempt();
    ordinary_managed_ipc_attempt(const ordinary_managed_ipc_attempt&) = delete;
    ordinary_managed_ipc_attempt& operator=(const ordinary_managed_ipc_attempt&) = delete;

    // One read and one write may coexist. A second same-direction call is
    // explicitly busy without accepting or copying any payload. The real
    // owner retains its durable pending request for a later admitted call.
    // Keep this wrapper alive through each invocation. Payload/callback are
    // borrowed synchronously; callbacks run off all attempt leaves. Durable
    // application must revalidate lineage in its actual owned SQL turn.
    // Occupies the real reader admission while waiting, but consumes no bytes.
    // Only this path may return idle without retiring the exact stream.
    ordinary_ipc_result wait_input(ordinary_ipc_deadline) noexcept;
    ordinary_ipc_result receive(ordinary_ipc_deadline, const receive_callback&) noexcept;
    ordinary_ipc_result send(std::span<const uint8_t>, ordinary_ipc_deadline) noexcept;
    void request_stop() noexcept;

    // Local zero-call observation is necessary but not sufficient. The real
    // issuer independently verifies its tracked worker joins before releasing
    // the descriptor/role. False preserves custody, including callback-reentry
    // and unfinished joins. No caller-supplied completion Boolean exists.
    // True means actual custody released, not that an earlier I/O, callback or
    // cleanup error succeeded; those remain in observe(). The actual owner
    // must retain/settle borrowed callback captures in its tracked worker lane.
    bool collect() noexcept;
    bool collect(ordinary_ipc_deadline) noexcept;
    observation observe() const noexcept;
};
}
