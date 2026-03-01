#pragma once

#ifdef __cplusplus

#include "network.hpp"
#include <string>
#include <functional>
#include <memory>
#include <thread>
#include <atomic>
#include <vector>

namespace lattice {

/// Resolve a channel name to a platform-specific Unix domain socket path.
/// macOS: ~/Library/Caches/Lattice/ipc/<channel>.sock
/// Linux: $XDG_RUNTIME_DIR/lattice/<channel>.sock (fallback: /tmp/lattice-<uid>/<channel>.sock)
std::string resolve_ipc_socket_path(const std::string& channel);

// ============================================================================
// Length-prefix framing helpers
// ============================================================================

/// Write a length-prefixed frame to a file descriptor.
/// Format: [4 bytes big-endian length][payload]
/// Returns true on success.
bool write_length_prefixed(int fd, const void* data, uint32_t length);

/// Read a length-prefixed frame from a file descriptor.
/// Returns the payload, or empty vector on error/disconnect.
std::vector<uint8_t> read_length_prefixed(int fd);

// ============================================================================
// IPC Socket Client — implements sync_transport over Unix domain socket
// ============================================================================

class ipc_socket_client : public sync_transport {
public:
    /// Construct with a socket path (for client-initiated connections).
    explicit ipc_socket_client(const std::string& socket_path);

    /// Construct by wrapping an already-accepted file descriptor (server side).
    explicit ipc_socket_client(int accepted_fd);

    ~ipc_socket_client() override;

    // Non-copyable
    ipc_socket_client(const ipc_socket_client&) = delete;
    ipc_socket_client& operator=(const ipc_socket_client&) = delete;

    // sync_transport interface
    void connect(const std::string& url,
                 const std::map<std::string, std::string>& headers = {}) override;
    void disconnect() override;
    transport_state state() const override;
    void send(const transport_message& message) override;

    void set_on_open(on_open_handler handler) override;
    void set_on_message(on_message_handler handler) override;
    void set_on_error(on_error_handler handler) override;
    void set_on_close(on_close_handler handler) override;

private:
    std::string socket_path_;
    int fd_ = -1;
    std::atomic<transport_state> state_{transport_state::closed};

    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;

    std::thread read_thread_;
    std::atomic<bool> should_stop_{false};

    void start_read_loop();
    void close_fd();
};

// ============================================================================
// IPC Server — listens on a Unix domain socket, accepts connections
// ============================================================================

class ipc_server {
public:
    /// Callback invoked for each accepted client connection.
    using accept_callback = std::function<void(std::unique_ptr<ipc_socket_client>)>;

    explicit ipc_server(const std::string& socket_path);
    ~ipc_server();

    // Non-copyable
    ipc_server(const ipc_server&) = delete;
    ipc_server& operator=(const ipc_server&) = delete;

    /// Start listening and accepting connections in a background thread.
    /// The callback is invoked once per accepted client.
    void start(accept_callback callback);

    /// Stop accepting, close the listen socket.
    void stop();

    /// Returns true if the server is currently listening.
    bool is_listening() const { return is_listening_.load(); }

    /// The socket path this server is bound to.
    const std::string& socket_path() const { return socket_path_; }

private:
    std::string socket_path_;
    int listen_fd_ = -1;
    std::atomic<bool> is_listening_{false};
    std::atomic<bool> should_stop_{false};
    std::thread accept_thread_;
};

// ============================================================================
// IPC Endpoint — auto-negotiated role (try bind, fallback to connect)
// ============================================================================

/// Represents one side of an IPC sync channel.
/// On start(), tries to bind+listen. If the socket is already bound (EADDRINUSE),
/// falls back to connecting as a client.
class ipc_endpoint {
public:
    explicit ipc_endpoint(const std::string& channel);
    ~ipc_endpoint();

    // Non-copyable
    ipc_endpoint(const ipc_endpoint&) = delete;
    ipc_endpoint& operator=(const ipc_endpoint&) = delete;

    /// Start the endpoint. Calls transport_ready once a connected transport is available.
    /// The callback receives an ipc_socket_client implementing sync_transport.
    using transport_ready_callback = std::function<void(std::unique_ptr<ipc_socket_client>)>;
    void start(transport_ready_callback callback);

    /// Stop the endpoint (disconnects client, stops server).
    void stop();

    /// The channel name.
    const std::string& channel() const { return channel_; }

    /// Whether this endpoint ended up as the server.
    bool is_server() const { return server_ != nullptr; }

private:
    std::string channel_;
    std::string socket_path_;
    std::unique_ptr<ipc_server> server_;
    std::unique_ptr<ipc_socket_client> client_;
};

} // namespace lattice

#endif // __cplusplus
