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
// WebSocket Client Interface
// ============================================================================
//
// Abstract interface for WebSocket connections. Used for real-time sync.
// Platform-specific implementations:
// - Apple: URLSessionWebSocketTask
// - Android: OkHttp WebSocket
// - Generic: libwebsockets, Beast, etc.

enum class websocket_state {
    connecting,
    open,
    closing,
    closed
};

struct websocket_message {
    enum class type { text, binary };
    type msg_type = type::binary;
    std::vector<uint8_t> data;

    std::string as_string() const {
        return std::string(data.begin(), data.end());
    }

    static websocket_message from_string(const std::string& s) {
        websocket_message msg;
        msg.msg_type = type::text;
        msg.data = std::vector<uint8_t>(s.begin(), s.end());
        return msg;
    }

    static websocket_message from_binary(const std::vector<uint8_t>& d) {
        websocket_message msg;
        msg.msg_type = type::binary;
        msg.data = d;
        return msg;
    }
};

class websocket_client {
public:
    virtual ~websocket_client() = default;

    // Connection lifecycle
    virtual void connect(const std::string& url,
                        const std::map<std::string, std::string>& headers = {}) = 0;
    virtual void disconnect() = 0;
    virtual websocket_state state() const = 0;

    // Send message
    virtual void send(const websocket_message& message) = 0;

    // Event callbacks
    using on_open_handler = std::function<void()>;
    using on_message_handler = std::function<void(const websocket_message&)>;
    using on_error_handler = std::function<void(const std::string& error)>;
    using on_close_handler = std::function<void(int code, const std::string& reason)>;

    virtual void set_on_open(on_open_handler handler) = 0;
    virtual void set_on_message(on_message_handler handler) = 0;
    virtual void set_on_error(on_error_handler handler) = 0;
    virtual void set_on_close(on_close_handler handler) = 0;
};

// ============================================================================
// Generic WebSocket Client - for external injection of implementations
// ============================================================================

#ifdef __BLOCKS__
// Block-based version for Apple platforms (Swift interop)
class generic_websocket_client: public websocket_client {
public:
    using connect_block = void (^)(void* user_data,
                                   const std::string& url,
                                   const std::map<std::string, std::string>& headers);
    using disconnect_block = void (^)(void* user_data);
    using state_block = websocket_state (^)(void* user_data);
    using send_block = void (^)(void* user_data, const websocket_message& message);

private:
    void* user_data_;
    connect_block connect_;
    disconnect_block disconnect_;
    state_block state_;
    send_block send_;

    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;

public:
    generic_websocket_client(void* user_data,
                             connect_block connect,
                             disconnect_block disconnect,
                             state_block state,
                             send_block send)
    : user_data_(user_data)
    , connect_(connect)
    , disconnect_(disconnect)
    , state_(state)
    , send_(send)
    {
    }

    ~generic_websocket_client() = default;

    void connect(const std::string& url,
                 const std::map<std::string, std::string>& headers = {}) override {
        if (connect_) connect_(user_data_, url, headers);
    }

    void disconnect() override {
        if (disconnect_) disconnect_(user_data_);
    }

    websocket_state state() const override {
        if (state_) return state_(user_data_);
        return websocket_state::closed;
    }

    void send(const websocket_message& message) override {
        if (send_) send_(user_data_, message);
    }

    void set_on_open(on_open_handler handler) override { on_open_ = handler; }
    void set_on_message(on_message_handler handler) override { on_message_ = handler; }
    void set_on_error(on_error_handler handler) override { on_error_ = handler; }
    void set_on_close(on_close_handler handler) override { on_close_ = handler; }

    void trigger_on_open() { if (on_open_) on_open_(); }
    void trigger_on_message(const websocket_message& msg) {
        if (on_message_)
            on_message_(msg);
    }
    void trigger_on_error(const std::string& error) { if (on_error_) on_error_(error); }
    void trigger_on_close(int code, const std::string& reason) { if (on_close_) on_close_(code, reason); }
};

#else
// std::function-based version for non-Apple platforms (WASM, Linux, etc.)
class generic_websocket_client: public websocket_client {
public:
    using connect_fn = std::function<void(const std::string& url,
                                          const std::map<std::string, std::string>& headers)>;
    using disconnect_fn = std::function<void()>;
    using state_fn = std::function<websocket_state()>;
    using send_fn = std::function<void(const websocket_message& message)>;

private:
    connect_fn connect_;
    disconnect_fn disconnect_;
    state_fn state_;
    send_fn send_;

    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;

public:
    generic_websocket_client(connect_fn connect,
                             disconnect_fn disconnect,
                             state_fn state,
                             send_fn send)
    : connect_(std::move(connect))
    , disconnect_(std::move(disconnect))
    , state_(std::move(state))
    , send_(std::move(send))
    {
    }

    ~generic_websocket_client() = default;

    void connect(const std::string& url,
                 const std::map<std::string, std::string>& headers = {}) override {
        if (connect_) connect_(url, headers);
    }

    void disconnect() override {
        if (disconnect_) disconnect_();
    }

    websocket_state state() const override {
        if (state_) return state_();
        return websocket_state::closed;
    }

    void send(const websocket_message& message) override {
        if (send_) send_(message);
    }

    void set_on_open(on_open_handler handler) override { on_open_ = handler; }
    void set_on_message(on_message_handler handler) override { on_message_ = handler; }
    void set_on_error(on_error_handler handler) override { on_error_ = handler; }
    void set_on_close(on_close_handler handler) override { on_close_ = handler; }

    void trigger_on_open() { if (on_open_) on_open_(); }
    void trigger_on_message(const websocket_message& msg) { if (on_message_) on_message_(msg); }
    void trigger_on_error(const std::string& error) { if (on_error_) on_error_(error); }
    void trigger_on_close(int code, const std::string& reason) { if (on_close_) on_close_(code, reason); }
};
#endif

using UniqueWebsocketClient = std::unique_ptr<websocket_client>;

// ============================================================================
// Factory for creating platform-specific clients
// ============================================================================

class network_factory {
public:
    virtual ~network_factory() = default;

    virtual std::unique_ptr<http_client> create_http_client() = 0;
    virtual std::unique_ptr<websocket_client> create_websocket_client() = 0;
};

// Global factory registration (set by platform layer)
void set_network_factory(std::shared_ptr<network_factory> factory);
std::shared_ptr<network_factory> get_network_factory();

// ============================================================================
// Generic network factory - for external injection
// ============================================================================

#ifdef __BLOCKS__
// Block-based version for Apple platforms (Swift interop)
class generic_network_factory : public network_factory {
public:
    using create_http_block = http_client* (^)(void* user_data);
    using create_websocket_block = websocket_client* (^)(void* user_data);

private:
    void* user_data_;
    create_http_block http_block_;
    create_websocket_block ws_block_;
    void (*destroy_fn_)(void*);

public:
    generic_network_factory(void* user_data,
                            create_http_block http_block,
                            create_websocket_block ws_block,
                            void (*destroy_fn)(void*) = nullptr)
        : user_data_(user_data)
        , http_block_(http_block)
        , ws_block_(ws_block)
        , destroy_fn_(destroy_fn)
    {}

    ~generic_network_factory() override {
        if (destroy_fn_ && user_data_) {
            destroy_fn_(user_data_);
        }
    }

    std::unique_ptr<http_client> create_http_client() override {
        if (http_block_) {
            return std::unique_ptr<http_client>(http_block_(user_data_));
        }
        return nullptr;
    }

    std::unique_ptr<websocket_client> create_websocket_client() override {
        if (ws_block_) {
            return std::unique_ptr<websocket_client>(ws_block_(user_data_));
        }
        return nullptr;
    }
};

inline void register_generic_network_factory(void* user_data,
                                             generic_network_factory::create_http_block http_block,
                                             generic_network_factory::create_websocket_block ws_block,
                                             void (*destroy_fn)(void*) = nullptr) {
    auto factory = std::make_shared<generic_network_factory>(user_data, http_block, ws_block, destroy_fn);
    set_network_factory(factory);
}

#else
// std::function-based version for non-Apple platforms (WASM, Linux, etc.)
class generic_network_factory : public network_factory {
public:
    using create_http_fn = std::function<std::unique_ptr<http_client>()>;
    using create_websocket_fn = std::function<std::unique_ptr<websocket_client>()>;

private:
    create_http_fn http_fn_;
    create_websocket_fn ws_fn_;

public:
    generic_network_factory(create_http_fn http_fn,
                            create_websocket_fn ws_fn)
        : http_fn_(std::move(http_fn))
        , ws_fn_(std::move(ws_fn))
    {}

    ~generic_network_factory() override = default;

    std::unique_ptr<http_client> create_http_client() override {
        if (http_fn_) {
            return http_fn_();
        }
        return nullptr;
    }

    std::unique_ptr<websocket_client> create_websocket_client() override {
        if (ws_fn_) {
            return ws_fn_();
        }
        return nullptr;
    }
};

inline void register_generic_network_factory(generic_network_factory::create_http_fn http_fn,
                                             generic_network_factory::create_websocket_fn ws_fn) {
    auto factory = std::make_shared<generic_network_factory>(std::move(http_fn), std::move(ws_fn));
    set_network_factory(factory);
}
#endif

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

class mock_websocket_client : public websocket_client {
public:
    void connect(const std::string& url,
                const std::map<std::string, std::string>& headers = {}) override {
        url_ = url;
        state_ = websocket_state::open;
        if (on_open_) on_open_();
    }

    void disconnect() override {
        state_ = websocket_state::closed;
        if (on_close_) on_close_(1000, "Normal closure");
    }

    websocket_state state() const override { return state_; }

    void send(const websocket_message& message) override {
        sent_messages_.push_back(message);
    }

    void set_on_open(on_open_handler handler) override { on_open_ = handler; }
    void set_on_message(on_message_handler handler) override { on_message_ = handler; }
    void set_on_error(on_error_handler handler) override { on_error_ = handler; }
    void set_on_close(on_close_handler handler) override { on_close_ = handler; }

    // Test helpers
    void simulate_message(const websocket_message& msg) {
        if (on_message_) on_message_(msg);
    }

    void simulate_error(const std::string& error) {
        if (on_error_) on_error_(error);
    }

    const std::vector<websocket_message>& get_sent_messages() const {
        return sent_messages_;
    }

    void clear_sent_messages() { sent_messages_.clear(); }

private:
    std::string url_;
    websocket_state state_ = websocket_state::closed;
    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;
    std::vector<websocket_message> sent_messages_;
};

class mock_network_factory : public network_factory {
public:
    std::unique_ptr<http_client> create_http_client() override {
        return std::make_unique<null_http_client>();
    }

    std::unique_ptr<websocket_client> create_websocket_client() override {
        auto client = std::make_unique<mock_websocket_client>();
        last_websocket_ = client.get();
        return client;
    }

    // Access last created websocket for testing
    mock_websocket_client* last_websocket() { return last_websocket_; }

private:
    mock_websocket_client* last_websocket_ = nullptr;
};

} // namespace lattice

#endif // __cplusplus
