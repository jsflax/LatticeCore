#include "lattice/ipc.hpp"
#include "lattice/log.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <arpa/inet.h>  // htonl / ntohl

#ifdef __APPLE__
#include <pwd.h>
#endif

namespace lattice {

// ============================================================================
// Channel → socket path resolution
// ============================================================================

std::string resolve_ipc_socket_path(const std::string& channel) {
#ifdef __APPLE__
    // macOS: ~/Library/Caches/Lattice/ipc/<channel>.sock
    const char* home = getenv("HOME");
    if (!home) {
        struct passwd* pw = getpwuid(getuid());
        home = pw ? pw->pw_dir : "/tmp";
    }
    std::string dir = std::string(home) + "/Library/Caches/Lattice/ipc";
#else
    // Linux: $XDG_RUNTIME_DIR/lattice/<channel>.sock
    // Fallback: /tmp/lattice-<uid>/<channel>.sock
    const char* runtime_dir = getenv("XDG_RUNTIME_DIR");
    std::string dir;
    if (runtime_dir && runtime_dir[0] != '\0') {
        dir = std::string(runtime_dir) + "/lattice";
    } else {
        dir = "/tmp/lattice-" + std::to_string(getuid());
    }
#endif
    std::filesystem::create_directories(dir);
    return dir + "/" + channel + ".sock";
}

// ============================================================================
// Length-prefix framing
// ============================================================================

bool write_length_prefixed(int fd, const void* data, uint32_t length) {
    uint32_t net_len = htonl(length);
    // Write the 4-byte length header
    const uint8_t* hdr = reinterpret_cast<const uint8_t*>(&net_len);
    size_t written = 0;
    while (written < 4) {
        ssize_t n = ::write(fd, hdr + written, 4 - written);
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    // Write the payload
    const uint8_t* payload = static_cast<const uint8_t*>(data);
    written = 0;
    while (written < length) {
        ssize_t n = ::write(fd, payload + written, length - written);
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

std::vector<uint8_t> read_length_prefixed(int fd) {
    // Read 4-byte length header
    uint32_t net_len = 0;
    uint8_t* hdr = reinterpret_cast<uint8_t*>(&net_len);
    size_t received = 0;
    while (received < 4) {
        ssize_t n = ::read(fd, hdr + received, 4 - received);
        if (n <= 0) return {};  // EOF or error
        received += static_cast<size_t>(n);
    }

    uint32_t length = ntohl(net_len);
    if (length == 0) return {};
    // Sanity limit: 256 MB (matches WebSocket max frame size)
    if (length > (1u << 28)) return {};

    // Read payload
    std::vector<uint8_t> buf(length);
    received = 0;
    while (received < length) {
        ssize_t n = ::read(fd, buf.data() + received, length - received);
        if (n <= 0) return {};  // EOF or error
        received += static_cast<size_t>(n);
    }
    return buf;
}

// ============================================================================
// ipc_socket_client implementation
// ============================================================================

ipc_socket_client::ipc_socket_client(const std::string& socket_path)
    : socket_path_(socket_path) {}

ipc_socket_client::ipc_socket_client(int accepted_fd)
    : fd_(accepted_fd) {
    // fd is valid but state stays closed — connect() will start the read loop.
    // This ensures the synchronizer can set callbacks before messages are processed.
}

ipc_socket_client::~ipc_socket_client() {
    should_stop_ = true;
    close_fd();
    if (read_thread_.joinable()) {
        read_thread_.join();
    }
}

void ipc_socket_client::connect(const std::string& /*url*/,
                                 const std::map<std::string, std::string>& /*headers*/) {
    if (state_ == transport_state::open || state_ == transport_state::connecting) return;

    // If fd_ is already valid (server-accepted connection), just start the read loop.
    if (fd_ >= 0) {
        state_ = transport_state::open;
        start_read_loop();
        if (on_open_) on_open_();
        return;
    }

    // Client-side: connect to the server socket
    state_ = transport_state::connecting;

    int sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        std::string err = "ipc: socket() failed: " + std::string(strerror(errno));
        LOG_DEBUG("ipc", "%s", err.c_str());
        state_ = transport_state::closed;
        if (on_error_) on_error_(err);
        return;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::string err = "ipc: connect() failed: " + std::string(strerror(errno));
        LOG_DEBUG("ipc", "%s", err.c_str());
        ::close(sock);
        state_ = transport_state::closed;
        if (on_error_) on_error_(err);
        return;
    }

    fd_ = sock;
    state_ = transport_state::open;

    start_read_loop();

    if (on_open_) on_open_();
}

void ipc_socket_client::disconnect() {
    bool was_open = (state_ != transport_state::closed && state_ != transport_state::closing);

    if (was_open) {
        state_ = transport_state::closing;
        should_stop_ = true;
        close_fd();
    }

    // Always join the read thread if joinable — even if the read loop already
    // exited on its own (connection lost). A joinable std::thread that isn't
    // joined before destruction calls std::terminate().
    if (read_thread_.joinable()) {
        read_thread_.join();
    }

    if (was_open) {
        state_ = transport_state::closed;
        if (on_close_) on_close_(1000, "Normal closure");
    }
}

transport_state ipc_socket_client::state() const {
    return state_.load();
}

void ipc_socket_client::send(const transport_message& message) {
    int fd = fd_;
    if (fd < 0 || state_ != transport_state::open) return;

    bool ok;
    if (message.msg_type == transport_message::type::text) {
        std::string text = message.as_string();
        ok = write_length_prefixed(fd, text.data(), static_cast<uint32_t>(text.size()));
    } else {
        ok = write_length_prefixed(fd, message.data.data(), static_cast<uint32_t>(message.data.size()));
    }

    if (!ok) {
        LOG_DEBUG("ipc", "send failed, disconnecting");
        // Don't call disconnect() from here — the read loop will detect the broken pipe.
    }
}

void ipc_socket_client::set_on_open(on_open_handler handler) { on_open_ = std::move(handler); }
void ipc_socket_client::set_on_message(on_message_handler handler) { on_message_ = std::move(handler); }
void ipc_socket_client::set_on_error(on_error_handler handler) { on_error_ = std::move(handler); }
void ipc_socket_client::set_on_close(on_close_handler handler) { on_close_ = std::move(handler); }

void ipc_socket_client::start_read_loop() {
    // Safety: if called from the read thread itself (e.g., during reconnection),
    // detach the old thread before replacing it. If called from another thread, join.
    if (read_thread_.joinable()) {
        if (read_thread_.get_id() == std::this_thread::get_id()) {
            read_thread_.detach();
        } else {
            read_thread_.join();
        }
    }

    should_stop_ = false;
    read_thread_ = std::thread([this]() {
        while (!should_stop_) {
            auto payload = read_length_prefixed(fd_);
            if (payload.empty()) {
                if (should_stop_) break;
                // Connection lost
                LOG_DEBUG("ipc", "read_length_prefixed returned empty, connection lost");
                state_ = transport_state::closed;
                if (on_close_) on_close_(1006, "Connection lost");
                return;
            }
            // IPC uses text framing (JSON) — same as the WSS protocol
            transport_message msg;
            msg.msg_type = transport_message::type::text;
            msg.data = std::move(payload);
            if (on_message_) on_message_(msg);
        }
    });
}

void ipc_socket_client::close_fd() {
    int fd = fd_;
    fd_ = -1;
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
}

// ============================================================================
// ipc_server implementation
// ============================================================================

ipc_server::ipc_server(const std::string& socket_path)
    : socket_path_(socket_path) {}

ipc_server::~ipc_server() {
    stop();
}

void ipc_server::start(accept_callback callback) {
    if (is_listening_) return;

    // Remove stale socket file if it exists
    ::unlink(socket_path_.c_str());

    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error("ipc_server: socket() failed: " + std::string(strerror(errno)));
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        int err = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw std::runtime_error("ipc_server: bind() failed: " + std::string(strerror(err)));
    }

    if (::listen(listen_fd_, 8) < 0) {
        int err = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::unlink(socket_path_.c_str());
        throw std::runtime_error("ipc_server: listen() failed: " + std::string(strerror(err)));
    }

    is_listening_ = true;
    should_stop_ = false;

    accept_thread_ = std::thread([this, cb = std::move(callback)]() {
        LOG_DEBUG("ipc_server", "Accept thread started on %s", socket_path_.c_str());
        while (!should_stop_) {
            int client_fd = ::accept(listen_fd_, nullptr, nullptr);
            if (client_fd < 0) {
                if (should_stop_ || errno == EBADF || errno == EINVAL) break;
                LOG_DEBUG("ipc_server", "accept() error: %s", strerror(errno));
                continue;
            }
            LOG_DEBUG("ipc_server", "Accepted client fd=%d", client_fd);
            auto client = std::make_unique<ipc_socket_client>(client_fd);
            cb(std::move(client));
        }
        LOG_DEBUG("ipc_server", "Accept thread exiting");
    });
}

void ipc_server::stop() {
    if (!is_listening_ && listen_fd_ < 0) return;

    should_stop_ = true;
    is_listening_ = false;

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // Clean up the socket file
    ::unlink(socket_path_.c_str());
}

// ============================================================================
// ipc_endpoint implementation (auto-negotiated role)
// ============================================================================

ipc_endpoint::ipc_endpoint(const std::string& channel)
    : channel_(channel)
    , socket_path_(resolve_ipc_socket_path(channel)) {}

ipc_endpoint::~ipc_endpoint() {
    stop();
}

void ipc_endpoint::start(transport_ready_callback callback) {
    // Try to bind as server first (atomic at filesystem level)
    int sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        throw std::runtime_error("ipc_endpoint: socket() failed: " + std::string(strerror(errno)));
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    // Don't unlink first — if bind fails with EADDRINUSE, we connect as client
    int bind_result = ::bind(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));

    if (bind_result == 0) {
        // We got the bind — become the server
        ::close(sock);  // We'll let ipc_server create its own socket
        // But we already created the file, so unlink it for the server to re-bind cleanly
        ::unlink(socket_path_.c_str());

        server_ = std::make_unique<ipc_server>(socket_path_);
        server_->start([cb = callback](std::unique_ptr<ipc_socket_client> client) {
            // Hand the transport to the synchronizer. It will set callbacks
            // then call connect(), which starts the read loop for accepted-fd clients.
            cb(std::move(client));
        });
        LOG_DEBUG("ipc_endpoint", "Channel '%s': acting as server on %s",
                  channel_.c_str(), socket_path_.c_str());
    } else if (errno == EADDRINUSE) {
        // Socket already exists — connect as client
        ::close(sock);

        client_ = std::make_unique<ipc_socket_client>(socket_path_);
        // connect() is called by the synchronizer — we just hand it off
        callback(std::move(client_));
        LOG_DEBUG("ipc_endpoint", "Channel '%s': acting as client to %s",
                  channel_.c_str(), socket_path_.c_str());
    } else {
        int err = errno;
        ::close(sock);
        throw std::runtime_error("ipc_endpoint: bind() failed: " + std::string(strerror(err)));
    }
}

void ipc_endpoint::stop() {
    if (server_) {
        server_->stop();
        server_.reset();
    }
    // client_ ownership was transferred via callback; nothing to do here
}

} // namespace lattice
