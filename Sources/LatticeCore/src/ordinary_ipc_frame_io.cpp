#include "ordinary_ipc_frame_io.hpp"
#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

namespace lattice::detail {
namespace {
using clock = std::chrono::steady_clock;
bool admitted(ordinary_ipc_result& result, const std::atomic<bool>& stopped,
              ordinary_ipc_deadline deadline, ordinary_ipc_io_check check) noexcept {
    if (stopped.load(std::memory_order_acquire)) { result.code = ordinary_ipc_code::stopped; return false; }
    if (deadline == ordinary_ipc_deadline::max()) { result.code = ordinary_ipc_code::invalid_deadline; return false; }
    if (clock::now() >= deadline) { result.code = ordinary_ipc_code::timeout; return false; }
    if (check.call) {
        try { check.call(check.context); }
        catch (...) { result.code = ordinary_ipc_code::admission_refused; result.exception = std::current_exception(); return false; }
    }
    // The native check can race retirement or consume the remaining time.
    if (stopped.load(std::memory_order_acquire)) { result.code = ordinary_ipc_code::stopped; return false; }
    if (clock::now() >= deadline) { result.code = ordinary_ipc_code::timeout; return false; }
    return true;
}
bool ready(int descriptor, short events, ordinary_ipc_result& result,
           const std::atomic<bool>& stopped, ordinary_ipc_deadline deadline,
           ordinary_ipc_io_check check) noexcept {
    for (;;) {
        if (!admitted(result, stopped, deadline, check)) return false;
        const auto remaining = deadline - clock::now();
        if (remaining <= clock::duration::zero()) { result.code = ordinary_ipc_code::timeout; return false; }
        // Round up: an early timer wake cannot become a successful read/write.
        const auto milliseconds = std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
        pollfd socket{descriptor, events, 0};
        const int rc = ::poll(&socket, 1, int(std::min<int64_t>(milliseconds, INT_MAX)));
        if (rc < 0) {
            const int error = errno;
            if (error == EINTR) continue; // Next attempt rechecks the same deadline.
            result.code = ordinary_ipc_code::socket_error; result.system_error = error; return false;
        }
        if (!rc) continue;
        if (socket.revents & POLLNVAL) {
            result.code = ordinary_ipc_code::socket_error; result.system_error = EBADF; return false;
        }
        // HUP may accompany unread bytes. The following real syscall decides
        // EOF/error; no positive I/O is inferred from a poll flag.
        return admitted(result, stopped, deadline, check);
    }
}
bool transfer(int descriptor, uint8_t* data, size_t size, bool sending,
              size_t& progress, ordinary_ipc_result& result,
              const std::atomic<bool>& stopped, ordinary_ipc_deadline deadline,
              ordinary_ipc_io_check check) noexcept {
    while (progress < size) {
        if (!admitted(result, stopped, deadline, check)) return false;
        ssize_t amount;
        if (sending) {
#ifdef MSG_NOSIGNAL
            amount = ::send(descriptor, data + progress, size - progress, MSG_NOSIGNAL);
#else
            // prepare_socket requires the platform's SO_NOSIGPIPE contract.
            amount = ::send(descriptor, data + progress, size - progress, 0);
#endif
        } else amount = ::recv(descriptor, data + progress, size - progress, 0);
        if (amount > 0) { progress += size_t(amount); continue; }
        if (!amount) {
            result.code = stopped.load(std::memory_order_acquire) ? ordinary_ipc_code::stopped : ordinary_ipc_code::peer_closed;
            return false;
        }
        const int error = errno;
        if (error == EINTR) continue;
        if (error == EAGAIN || error == EWOULDBLOCK) {
            if (!ready(descriptor, sending ? POLLOUT : POLLIN, result, stopped, deadline, check)) return false;
            continue;
        }
        result.code = stopped.load(std::memory_order_acquire) ? ordinary_ipc_code::stopped : ordinary_ipc_code::socket_error;
        result.system_error = error; return false;
    }
    return true;
}
}
ordinary_ipc_result ordinary_ipc_wait_input(int descriptor, const std::atomic<bool>& stopped,
    ordinary_ipc_deadline deadline, ordinary_ipc_io_check check) noexcept {
    ordinary_ipc_result result;
    // Expired/unbounded admission is not normal idle and cannot hide a stale
    // original operation deadline. Nothing here calls recv or allocates bytes.
    if(!admitted(result,stopped,deadline,check))return result;
    for(;;){
        if(stopped.load(std::memory_order_acquire)){result.code=ordinary_ipc_code::stopped;return result;}
        if(check.call){try{check.call(check.context);}catch(...){result.code=ordinary_ipc_code::admission_refused;result.exception=std::current_exception();return result;}}
        if(stopped.load(std::memory_order_acquire)){result.code=ordinary_ipc_code::stopped;return result;}
        const auto remaining=deadline-clock::now();
        if(remaining<=clock::duration::zero()){result.code=ordinary_ipc_code::idle;return result;}
        const auto milliseconds=std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
        pollfd socket{descriptor,POLLIN,0};
        const int rc=::poll(&socket,1,int(std::min<int64_t>(milliseconds,INT_MAX)));
        if(rc<0){const auto error=errno;if(error==EINTR)continue;
            result.code=ordinary_ipc_code::socket_error;result.system_error=error;return result;}
        if(!rc)continue; // same finite slice; no new authority/deadline
        if(socket.revents&POLLNVAL){result.code=ordinary_ipc_code::socket_error;result.system_error=EBADF;return result;}
        // HUP/error readiness is deliberately left for the subsequent real
        // frame read. This call never invents EOF or positive consumption.
        if(!admitted(result,stopped,deadline,check))return result;
        return result;
    }
}
ordinary_ipc_result ordinary_ipc_prepare_socket(int descriptor) noexcept {
    ordinary_ipc_result result;
    auto failed = [&](int error) { result.code = ordinary_ipc_code::socket_error; result.system_error = error; return result; };
    int kind = 0; socklen_t length = sizeof(kind);
    if (::getsockopt(descriptor, SOL_SOCKET, SO_TYPE, &kind, &length) != 0) return failed(errno);
    if (length != sizeof(kind) || kind != SOCK_STREAM) return failed(EPROTOTYPE);
    sockaddr_storage peer{}; length = sizeof(peer);
    if (::getpeername(descriptor, reinterpret_cast<sockaddr*>(&peer), &length) != 0) return failed(errno);
    if (peer.ss_family != AF_UNIX) return failed(EAFNOSUPPORT);
    const int flags = ::fcntl(descriptor, F_GETFL);
    if (flags < 0) return failed(errno);
    if (::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0) return failed(errno);
    const int fdflags = ::fcntl(descriptor, F_GETFD);
    if (fdflags < 0) return failed(errno);
    if (::fcntl(descriptor, F_SETFD, fdflags | FD_CLOEXEC) != 0) return failed(errno);
#ifdef SO_NOSIGPIPE
    const int on = 1;
    if (::setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on)) != 0) return failed(errno);
#elif !defined(MSG_NOSIGNAL)
    return failed(ENOTSUP);
#endif
    return result;
}
ordinary_ipc_frame ordinary_ipc_read_frame(int descriptor, const std::atomic<bool>& stopped,
    ordinary_ipc_deadline deadline, ordinary_ipc_io_check check) noexcept {
    ordinary_ipc_frame frame;
    auto& result = frame.result;
    std::array<uint8_t, 4> header{}; result.phase = ordinary_ipc_phase::header;
    if (!transfer(descriptor, header.data(), header.size(), false, result.header_bytes, result, stopped, deadline, check)) return frame;
    result.declared_bytes = (uint32_t(header[0]) << 24) | (uint32_t(header[1]) << 16) | (uint32_t(header[2]) << 8) | uint32_t(header[3]);
    result.phase = ordinary_ipc_phase::payload;
    if (result.declared_bytes > ordinary_ipc_maximum_frame_bytes) { result.code = ordinary_ipc_code::oversized; return frame; }
    if (!admitted(result, stopped, deadline, check)) return frame;
    try { frame.bytes.resize(result.declared_bytes); }
    catch (...) { result.code = ordinary_ipc_code::allocation_error; result.exception = std::current_exception(); return frame; }
    if (!transfer(descriptor, frame.bytes.data(), frame.bytes.size(), false, result.payload_bytes, result, stopped, deadline, check)) return frame;
    result.phase = ordinary_ipc_phase::publication;
    if (!admitted(result, stopped, deadline, check)) return frame;
    result.phase = ordinary_ipc_phase::complete; return frame;
}
ordinary_ipc_result ordinary_ipc_write_frame(int descriptor, std::span<const uint8_t> bytes,
    const std::atomic<bool>& stopped, ordinary_ipc_deadline deadline, ordinary_ipc_io_check check) noexcept {
    ordinary_ipc_result result; result.declared_bytes = bytes.size();
    if (bytes.size() > ordinary_ipc_maximum_frame_bytes) { result.code = ordinary_ipc_code::oversized; return result; }
    if (!admitted(result, stopped, deadline, check)) return result;
    const auto length = uint32_t(bytes.size());
    std::array<uint8_t, 4> header{uint8_t(length >> 24), uint8_t(length >> 16), uint8_t(length >> 8), uint8_t(length)};
    result.phase = ordinary_ipc_phase::header;
    if (!transfer(descriptor, header.data(), header.size(), true, result.header_bytes, result, stopped, deadline, check)) return result;
    result.phase = ordinary_ipc_phase::payload;
    // send does not modify bytes; transfer uses a pointer only to share the
    // progress/EOF machinery with recv and never writes in its sending arm.
    if (!transfer(descriptor, const_cast<uint8_t*>(bytes.data()), bytes.size(), true, result.payload_bytes, result, stopped, deadline, check)) return result;
    result.phase = ordinary_ipc_phase::publication;
    if (!admitted(result, stopped, deadline, check)) return result;
    result.phase = ordinary_ipc_phase::complete; return result;
}
}
#else
namespace lattice::detail {
ordinary_ipc_result ordinary_ipc_wait_input(int,const std::atomic<bool>&,ordinary_ipc_deadline,ordinary_ipc_io_check) noexcept {
    ordinary_ipc_result result;result.code=ordinary_ipc_code::admission_refused;result.system_error=ENOTSUP;return result;
}
ordinary_ipc_result ordinary_ipc_prepare_socket(int) noexcept {
    ordinary_ipc_result result; result.code = ordinary_ipc_code::socket_error; result.system_error = ENOTSUP; return result;
}
ordinary_ipc_frame ordinary_ipc_read_frame(int, const std::atomic<bool>&, ordinary_ipc_deadline, ordinary_ipc_io_check) noexcept {
    ordinary_ipc_frame frame; frame.result = ordinary_ipc_prepare_socket(-1); return frame;
}
ordinary_ipc_result ordinary_ipc_write_frame(int, std::span<const uint8_t>, const std::atomic<bool>&,
    ordinary_ipc_deadline, ordinary_ipc_io_check) noexcept { return ordinary_ipc_prepare_socket(-1); }
}
#endif
