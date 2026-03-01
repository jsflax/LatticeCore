#pragma once

#ifdef __cplusplus

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <optional>
#include <map>

namespace lattice {

// Type aliases for Swift interop (Swift can't use templated types directly)
using HeadersMap = std::map<std::string, std::string>;
using ByteVector = std::vector<uint8_t>;

// ============================================================================
// HTTP Client Interface
// ============================================================================
//
// Abstract interface for HTTP operations. Platform-specific implementations:
// - Apple: URLSession
// - Android: OkHttp
// - Generic: libcurl, asio, etc.

struct http_response {
    int status_code = 0;
    std::map<std::string, std::string> headers;
    std::vector<uint8_t> body;

    bool is_success() const { return status_code >= 200 && status_code < 300; }
    std::string body_string() const {
        return std::string(body.begin(), body.end());
    }
};

struct http_request {
    std::string method = "GET";
    std::string url;
    std::map<std::string, std::string> headers;
    std::vector<uint8_t> body;

    void set_body(const std::string& s) {
        body = std::vector<uint8_t>(s.begin(), s.end());
    }

    void set_json_body(const std::string& json) {
        set_body(json);
        headers["Content-Type"] = "application/json";
    }
};

class http_client {
public:
    virtual ~http_client() = default;

    // Synchronous request (blocks until complete)
    virtual http_response send(const http_request& request) = 0;

    // Asynchronous request
    using completion_handler = std::function<void(http_response)>;
    virtual void send_async(const http_request& request, completion_handler handler) = 0;
};

// ============================================================================
// Sync Transport Interface
// ============================================================================
//
// Abstract interface for bidirectional sync connections.
// Implementations: WebSocket (Apple URLSession, NIO), Unix domain socket (IPC),
// or any transport providing connect/disconnect/send/receive semantics.

enum class transport_state {
    connecting,
    open,
    closing,
    closed
};

struct transport_message {
    enum class type { text, binary };
    type msg_type = type::binary;
    std::vector<uint8_t> data;

    std::string as_string() const {
        return std::string(data.begin(), data.end());
    }

    static transport_message from_string(const std::string& s) {
        transport_message msg;
        msg.msg_type = type::text;
        msg.data = std::vector<uint8_t>(s.begin(), s.end());
        return msg;
    }

    static transport_message from_binary(const std::vector<uint8_t>& d) {
        transport_message msg;
        msg.msg_type = type::binary;
        msg.data = d;
        return msg;
    }
};

class sync_transport {
public:
    virtual ~sync_transport() = default;

    // Connection lifecycle
    virtual void connect(const std::string& url,
                        const std::map<std::string, std::string>& headers = {}) = 0;
    virtual void disconnect() = 0;
    virtual transport_state state() const = 0;

    // Send message
    virtual void send(const transport_message& message) = 0;

    // Event callbacks
    using on_open_handler = std::function<void()>;
    using on_message_handler = std::function<void(const transport_message&)>;
    using on_error_handler = std::function<void(const std::string& error)>;
    using on_close_handler = std::function<void(int code, const std::string& reason)>;

    virtual void set_on_open(on_open_handler handler) = 0;
    virtual void set_on_message(on_message_handler handler) = 0;
    virtual void set_on_error(on_error_handler handler) = 0;
    virtual void set_on_close(on_close_handler handler) = 0;
};

// ============================================================================
// Generic Sync Transport - for external injection of implementations
// ============================================================================

class generic_sync_transport: public sync_transport {
public:
    // C function pointer types using void* for C++ types (portable, Swift-safe).
    // url_ptr/headers_ptr/message_ptr are opaque pointers to C++ objects;
    // implementations cast back via helper methods.
    using connect_fn_ptr = void (*)(void* user_data, const void* url_ptr, const void* headers_ptr);
    using disconnect_fn_ptr = void (*)(void* user_data);
    using state_fn_ptr = transport_state (*)(void* user_data);
    using send_fn_ptr = void (*)(void* user_data, const void* message_ptr);

    // Helpers: cast opaque pointers back to C++ types
    static const std::string& cast_url(const void* p) { return *static_cast<const std::string*>(p); }
    static const HeadersMap& cast_headers(const void* p) { return *static_cast<const HeadersMap*>(p); }
    static const transport_message& cast_message(const void* p) { return *static_cast<const transport_message*>(p); }

private:
    void* user_data_;
    connect_fn_ptr connect_;
    disconnect_fn_ptr disconnect_;
    state_fn_ptr state_;
    send_fn_ptr send_;

    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;

public:
    // C function pointer constructor (primary, works from Swift on all platforms)
    generic_sync_transport(void* user_data,
                             connect_fn_ptr connect,
                             disconnect_fn_ptr disconnect,
                             state_fn_ptr state,
                             send_fn_ptr send)
    : user_data_(user_data)
    , connect_(connect)
    , disconnect_(disconnect)
    , state_(state)
    , send_(send)
    {
    }

    ~generic_sync_transport() = default;

    void connect(const std::string& url,
                 const std::map<std::string, std::string>& headers = {}) override {
        if (connect_) connect_(user_data_, &url, &headers);
    }

    void disconnect() override {
        if (disconnect_) disconnect_(user_data_);
    }

    transport_state state() const override {
        if (state_) return state_(user_data_);
        return transport_state::closed;
    }

    void send(const transport_message& message) override {
        if (send_) send_(user_data_, &message);
    }

    void set_on_open(on_open_handler handler) override { on_open_ = handler; }
    void set_on_message(on_message_handler handler) override { on_message_ = handler; }
    void set_on_error(on_error_handler handler) override { on_error_ = handler; }
    void set_on_close(on_close_handler handler) override { on_close_ = handler; }

    void trigger_on_open() { if (on_open_) on_open_(); }
    void trigger_on_message(const transport_message& msg) {
        if (on_message_)
            on_message_(msg);
    }
    void trigger_on_error(const std::string& error) { if (on_error_) on_error_(error); }
    void trigger_on_close(int code, const std::string& reason) { if (on_close_) on_close_(code, reason); }
};

using UniqueSyncTransport = std::unique_ptr<sync_transport>;

// ============================================================================
// Factory for creating platform-specific clients
// ============================================================================

class network_factory {
public:
    virtual ~network_factory() = default;

    virtual std::unique_ptr<http_client> create_http_client() = 0;
    virtual std::unique_ptr<sync_transport> create_sync_transport() = 0;
};

// Global factory registration (set by platform layer)
void set_network_factory(std::shared_ptr<network_factory> factory);
std::shared_ptr<network_factory> get_network_factory();

// ============================================================================
// Generic network factory - for external injection
// ============================================================================

class generic_network_factory : public network_factory {
public:
    // C function pointer types (portable, Swift-safe on all platforms)
    using create_http_fn_ptr = http_client* (*)(void* user_data);
    using create_transport_fn_ptr = sync_transport* (*)(void* user_data);

private:
    void* user_data_;
    create_http_fn_ptr http_fn_;
    create_transport_fn_ptr ws_fn_;
    void (*destroy_fn_)(void*);

public:
    // C function pointer constructor (primary, works from Swift on all platforms)
    generic_network_factory(void* user_data,
                            create_http_fn_ptr http_fn,
                            create_transport_fn_ptr ws_fn,
                            void (*destroy_fn)(void*) = nullptr)
        : user_data_(user_data)
        , http_fn_(http_fn)
        , ws_fn_(ws_fn)
        , destroy_fn_(destroy_fn)
    {}

    ~generic_network_factory() override {
        if (destroy_fn_ && user_data_) {
            destroy_fn_(user_data_);
        }
    }

    std::unique_ptr<http_client> create_http_client() override {
        if (http_fn_) {
            return std::unique_ptr<http_client>(http_fn_(user_data_));
        }
        return nullptr;
    }

    std::unique_ptr<sync_transport> create_sync_transport() override {
        if (ws_fn_) {
            return std::unique_ptr<sync_transport>(ws_fn_(user_data_));
        }
        return nullptr;
    }
};

inline void register_generic_network_factory(void* user_data,
                                             generic_network_factory::create_http_fn_ptr http_fn,
                                             generic_network_factory::create_transport_fn_ptr ws_fn,
                                             void (*destroy_fn)(void*) = nullptr) {
    auto factory = std::make_shared<generic_network_factory>(user_data, http_fn, ws_fn, destroy_fn);
    set_network_factory(factory);
}

// ============================================================================
// Null/Mock implementations for testing
// ============================================================================

class null_http_client : public http_client {
public:
    http_response send(const http_request&) override {
        return http_response{503, {}, {}};  // Service Unavailable
    }

    void send_async(const http_request&, completion_handler handler) override {
        if (handler) handler(http_response{503, {}, {}});
    }
};

class mock_sync_transport : public sync_transport {
public:
    void connect(const std::string& url,
                const std::map<std::string, std::string>& headers = {}) override {
        url_ = url;
        state_ = transport_state::open;
        if (on_open_) on_open_();
    }

    void disconnect() override {
        state_ = transport_state::closed;
        if (on_close_) on_close_(1000, "Normal closure");
    }

    transport_state state() const override { return state_; }

    void send(const transport_message& message) override {
        sent_messages_.push_back(message);
    }

    void set_on_open(on_open_handler handler) override { on_open_ = handler; }
    void set_on_message(on_message_handler handler) override { on_message_ = handler; }
    void set_on_error(on_error_handler handler) override { on_error_ = handler; }
    void set_on_close(on_close_handler handler) override { on_close_ = handler; }

    // Test helpers
    void simulate_message(const transport_message& msg) {
        if (on_message_) on_message_(msg);
    }

    void simulate_error(const std::string& error) {
        if (on_error_) on_error_(error);
    }

    const std::vector<transport_message>& get_sent_messages() const {
        return sent_messages_;
    }

    void clear_sent_messages() { sent_messages_.clear(); }

private:
    std::string url_;
    transport_state state_ = transport_state::closed;
    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;
    std::vector<transport_message> sent_messages_;
};

class mock_network_factory : public network_factory {
public:
    std::unique_ptr<http_client> create_http_client() override {
        return std::make_unique<null_http_client>();
    }

    std::unique_ptr<sync_transport> create_sync_transport() override {
        auto client = std::make_unique<mock_sync_transport>();
        last_websocket_ = client.get();
        return client;
    }

    // Access last created websocket for testing
    mock_sync_transport* last_websocket() { return last_websocket_; }

private:
    mock_sync_transport* last_websocket_ = nullptr;
};

} // namespace lattice

#endif // __cplusplus
