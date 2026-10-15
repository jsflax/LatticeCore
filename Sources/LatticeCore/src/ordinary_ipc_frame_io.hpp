#pragma once
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <span>
#include <vector>

namespace lattice::detail {
// Socket mechanics only. A borrowed descriptor is not a managed role, a
// projection grant, an endpoint identity, or proof of retirement.
inline constexpr size_t ordinary_ipc_maximum_frame_bytes = 4 * 1024 * 1024;
using ordinary_ipc_deadline = std::chrono::steady_clock::time_point;
enum class ordinary_ipc_code {
    complete, stopped, timeout, peer_closed, socket_error, oversized,
    admission_refused, busy, allocation_error, callback_error, invalid_deadline, idle
};
enum class ordinary_ipc_phase { admission, header, payload, publication, complete };
struct ordinary_ipc_result {
    ordinary_ipc_code code = ordinary_ipc_code::complete;
    ordinary_ipc_phase phase = ordinary_ipc_phase::admission;
    size_t header_bytes = 0, payload_bytes = 0, declared_bytes = 0;
    int system_error = 0;
    std::exception_ptr exception;
    bool succeeded() const noexcept { return code == ordinary_ipc_code::complete; }
};
struct ordinary_ipc_frame {
    ordinary_ipc_result result;
    std::vector<uint8_t> bytes;
};
// Synchronous, borrowed, no-copy admission hook. The managed attempt always
// supplies its retained role check; mechanics tests can exercise raw sockets
// without manufacturing a managed connection or any authority.
struct ordinary_ipc_io_check { void* context = nullptr; void (*call)(void*) = nullptr; };

// The caller owns the descriptor exclusively and retains it throughout each
// call. Cancellation sets stopped and shuts down (not closes) this descriptor.
// There are no internal workers, queues, callbacks or descriptor ownership.
ordinary_ipc_result ordinary_ipc_prepare_socket(int descriptor) noexcept;
// Bounded no-byte readiness only. idle proves no byte was consumed by this
// call, not role renewal, request completion, or a successful frame. The
// subsequent admitted frame receives its own single immutable deadline.
ordinary_ipc_result ordinary_ipc_wait_input(int descriptor, const std::atomic<bool>& stopped,
    ordinary_ipc_deadline deadline, ordinary_ipc_io_check check = {}) noexcept;
ordinary_ipc_frame ordinary_ipc_read_frame(int descriptor, const std::atomic<bool>& stopped,
    ordinary_ipc_deadline deadline, ordinary_ipc_io_check check = {}) noexcept;
ordinary_ipc_result ordinary_ipc_write_frame(int descriptor, std::span<const uint8_t> bytes,
    const std::atomic<bool>& stopped, ordinary_ipc_deadline deadline,
    ordinary_ipc_io_check check = {}) noexcept;
}
