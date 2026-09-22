#pragma once

#ifdef __cplusplus

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <optional>
#include <map>
#include <mutex>
#include <atomic>
#include <limits>
#include <stdexcept>

namespace lattice {

struct scheduler;

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

    /// Whether this transport can re-establish its connection by calling
    /// connect() again after a loss. True for endpoint dialers (WSS clients,
    /// IPC socket dialers); false for server-accepted connections, which are
    /// replaced by a fresh transport on the next accept.
    virtual bool supports_reconnect() const { return true; }

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

// Platform callbacks must not retain a pointer to the native transport. This
// value owns only its callback cell and the exact connect attempt. It is safe
// to retain after the transport is deleted; stale events then do nothing.
// This is lifetime provenance, NOT authenticated source/recovery authority.
namespace detail { struct platform_transport_test_access; struct platform_attempt_owner_test_access; class sync_callback_lifetime; class receiver_source_binding; class receiver_upload_view; }
class owned_platform_sync_transport;
class platform_tls_test_driver; // observation-only test-support product
class platform_transport_callbacks {
    // Installed only by the separate trusted SDK system-TLS adapter factory.
    // This is a retained platform verifier, never an application source grant.
    struct tls_provider {
        void* context;
        int32_t (*verify)(void*,const void*,const void*);
        void (*release)(void*);
        tls_provider(void* c,int32_t(*v)(void*,const void*,const void*),void(*r)(void*))
            :context(c),verify(v),release(r){}
        ~tls_provider(){if(release)release(context);}
    };
    struct handlers {
        std::function<void(const platform_transport_callbacks&)> open;
        std::function<void(const platform_transport_callbacks&, const transport_message&)> message;
        std::function<void(const platform_transport_callbacks&, const std::string&)> error;
        std::function<void(const platform_transport_callbacks&, int, const std::string&)> close;
    };
    struct cell {
        std::mutex mutex;
        uint64_t generation = 0;
        // A denial fence published only with the endpoint transition under
        // mutex. Owner admission reads it without taking this endpoint lock.
        std::atomic<uint64_t> owner_attempt{0};
        std::atomic<uint64_t> live_owner_attempt{0};
        std::atomic<uint64_t> verified_tls_attempt{0};
        std::shared_ptr<tls_provider> system_tls; // immutable after construction
        std::shared_ptr<const std::string> dial_url;
        transport_state phase = transport_state::closed;
        bool retired = false;
        handlers callbacks;
        // Private source-test rendezvous; normally empty. Never runs under mutex.
        std::function<void()> before_error_delivery;
        std::function<void()> before_message_delivery;
    };
    std::shared_ptr<cell> cell_;
    uint64_t generation_ = 0;
    platform_transport_callbacks(std::shared_ptr<cell> state, uint64_t generation)
        : cell_(std::move(state)), generation_(generation) {}
    bool current_locked() const {
        return !cell_->retired && generation_ != 0 && cell_->generation == generation_ &&
            (cell_->phase == transport_state::connecting || cell_->phase == transport_state::open);
    }
    friend class owned_platform_sync_transport;
    friend struct detail::platform_transport_test_access;
    friend struct detail::platform_attempt_owner_test_access;
    friend class detail::sync_callback_lifetime;
    friend class detail::receiver_source_binding;
    friend class detail::receiver_upload_view;
    friend class platform_tls_test_driver;
    bool current_system_tls_for_owner()const noexcept {
        return cell_&&generation_&&cell_->verified_tls_attempt.load(std::memory_order_acquire)==generation_&&
            current_attempt_for_owner(false);
    }
    // Called only while the owner admission leaf is held. The atomic fence
    // avoids nesting endpoint and owner locks (including capture-copy paths).
    // A terminal callback has already closed its phase; identity, not phase,
    // distinguishes it from a replaced or retired physical attempt.
    bool current_attempt_for_owner(bool terminal) const {
        if (!cell_) return false;
        const auto current = terminal ? cell_->owner_attempt.load(std::memory_order_acquire)
                                      : cell_->live_owner_attempt.load(std::memory_order_acquire);
        return generation_ != 0 && current == generation_;
    }
public:
    platform_transport_callbacks() = default;
    bool is_current() const {
        if (!cell_) return false;
        std::lock_guard<std::mutex> lock(cell_->mutex);
        return current_locked();
    }
    bool matches(const platform_transport_callbacks& other) const {
        return cell_ && cell_ == other.cell_ && generation_ != 0 && generation_ == other.generation_;
    }
    // Admission linearizes under the cell mutex; admitted work may finish
    // after a concurrent disconnect. No foreign callback runs under this lock.
    // The synchronizer's separate owner-lifetime admission protects its body.
    bool trigger_on_open() const {
        if (!cell_) return false;
        decltype(handlers::open) callback;
        std::shared_ptr<tls_provider> verifier;
        std::shared_ptr<const std::string> url;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (!current_locked() || cell_->phase != transport_state::connecting) return false;
            verifier=cell_->system_tls;url=cell_->dial_url;
        }
        // Retention outlives concurrent native deletion. The SDK verifies the
        // same actual task/socket and callback endpoint, outside native locks.
        bool verified=false;
        if(verifier&&url){try{verified=verifier->verify(verifier->context,this,url.get())==1;}catch(...){}}
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (!current_locked() || cell_->phase != transport_state::connecting) return false;
            callback = cell_->callbacks.open;
            cell_->verified_tls_attempt.store(verified?generation_:0,std::memory_order_release);
            cell_->phase = transport_state::open;
        }
        if (callback) callback(*this);
        return true;
    }
    bool trigger_on_message(const transport_message& message) const {
        if (!cell_) return false;
        decltype(handlers::message) callback;
        std::function<void()> before_delivery;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (!current_locked() || cell_->phase != transport_state::open) return false;
            callback = cell_->callbacks.message;
            before_delivery = cell_->before_message_delivery;
        }
        if (before_delivery) before_delivery();
        if (callback) callback(*this, message);
        return true;
    }
    bool trigger_on_error(const std::string& error) const {
        if (!cell_) return false;
        decltype(handlers::error) callback;
        std::function<void()> before_delivery;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (!current_locked()) return false;
            callback = cell_->callbacks.error;
            before_delivery = cell_->before_error_delivery;
            cell_->phase = transport_state::closed;
            cell_->live_owner_attempt.store(0, std::memory_order_release);
            cell_->verified_tls_attempt.store(0, std::memory_order_release);
        }
        if (before_delivery) before_delivery();
        if (callback) callback(*this, error);
        return true;
    }
    bool trigger_on_close(int code, const std::string& reason) const {
        if (!cell_) return false;
        decltype(handlers::close) callback;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (!current_locked()) return false;
            callback = cell_->callbacks.close;
            cell_->phase = transport_state::closed;
            cell_->live_owner_attempt.store(0, std::memory_order_release);
            cell_->verified_tls_attempt.store(0, std::memory_order_release);
        }
        if (callback) callback(*this, code, reason);
        return true;
    }
};

// Constructed by the native factory below, never copied into Swift-allocated
// storage. Native delete releases the platform retain exactly once. Existing
// generic/custom transports keep their original API and trust semantics.
class owned_platform_sync_transport final : public sync_transport {
public:
    using connect_fn_ptr = void (*)(void*, const void*, const void*, const void*);
    using disconnect_fn_ptr = void (*)(void*);
    using send_fn_ptr = void (*)(void*, const void*, const void*);
    using destroy_fn_ptr = void (*)(void*);
private:
    std::shared_ptr<platform_transport_callbacks::cell> cell_ =
        std::make_shared<platform_transport_callbacks::cell>();
    void* user_data_;
    connect_fn_ptr connect_;
    disconnect_fn_ptr disconnect_;
    send_fn_ptr send_;
    destroy_fn_ptr destroy_;
public:
    owned_platform_sync_transport(void* user_data, connect_fn_ptr connect,
        disconnect_fn_ptr disconnect, send_fn_ptr send, destroy_fn_ptr destroy)
        : user_data_(user_data), connect_(connect), disconnect_(disconnect), send_(send), destroy_(destroy) {}
    // Trusted SDK/native-host system-TLS boundary (callable C++ surface).
    // A native host can install a verifier here; it must be trusted. Generic
    // legacy factory callers never install
    // this provider. Both separately retained contexts are consumed on failure.
    // The trusted SDK implementation must verify real system TLS on the exact
    // Attempt; this factory does not accept a source descriptor or a grant.
    static sync_transport* make_system_tls_adapter(void* user_data,connect_fn_ptr connect,
        disconnect_fn_ptr disconnect,send_fn_ptr send,destroy_fn_ptr destroy,
        void* verification_context,int32_t(*verify)(void*,const void*,const void*),
        void(*release_verification)(void*))noexcept {
        std::shared_ptr<platform_transport_callbacks::tls_provider> provider;
        try {
            if(!verify||!release_verification||!destroy)throw std::invalid_argument("system TLS adapter requires owned verification");
            provider=std::make_shared<platform_transport_callbacks::tls_provider>(verification_context,verify,release_verification);
            auto* result=new owned_platform_sync_transport(user_data,connect,disconnect,send,destroy);
            result->cell_->system_tls=std::move(provider);
            return result;
        }catch(...){
            if(!provider&&release_verification)release_verification(verification_context);
            if(destroy)destroy(user_data);
            return nullptr;
        }
    }
    owned_platform_sync_transport(const owned_platform_sync_transport&) = delete;
    owned_platform_sync_transport& operator=(const owned_platform_sync_transport&) = delete;
    ~owned_platform_sync_transport() override {
        platform_transport_callbacks::handlers retired_callbacks;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            cell_->retired = true;
            cell_->phase = transport_state::closed;
            cell_->owner_attempt.store(0, std::memory_order_release);
            cell_->live_owner_attempt.store(0, std::memory_order_release);
            cell_->verified_tls_attempt.store(0, std::memory_order_release);
            std::swap(retired_callbacks, cell_->callbacks);
        }
        // Release captures and platform resources outside the cell lock.
        // Platform destruction must be nonthrowing and must not join itself.
        if (destroy_) destroy_(user_data_);
    }
private:
    void connect_impl(const std::string& url, const HeadersMap& headers,
                      std::optional<platform_transport_callbacks::handlers> replacement) {
        platform_transport_callbacks endpoint;
        // Generic transports retain no URL or trust callback. A malformed or
        // oversized system-adapter URL remains legacy/non-authoritative.
        std::shared_ptr<const std::string> bound_url;
        if(cell_->system_tls&&url.size()<=8192)bound_url=std::make_shared<const std::string>(url);
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (cell_->retired || cell_->generation == std::numeric_limits<uint64_t>::max())
                throw std::overflow_error("platform transport attempt exhausted");
            if (replacement) std::swap(cell_->callbacks, *replacement);
            ++cell_->generation;
            cell_->verified_tls_attempt.store(0,std::memory_order_release);
            cell_->dial_url.swap(bound_url); // displaced URL releases off lock
            cell_->phase = transport_state::connecting;
            endpoint = platform_transport_callbacks(cell_, cell_->generation);
            cell_->owner_attempt.store(cell_->generation, std::memory_order_release);
            cell_->live_owner_attempt.store(cell_->generation, std::memory_order_release);
        }
        // Any displaced handler captures outlive the lock and are released
        // only after this call. Copied old handlers retain their old lifecycle.
        if (connect_) connect_(user_data_, &url, &headers, &endpoint);
    }
public:
    void connect(const std::string& url, const HeadersMap& headers = {}) override {
        connect_impl(url, headers, std::nullopt);
    }
    // The actual synchronizer supplies handlers that capture its lifecycle.
    // Publish the complete handler set atomically with the new dial attempt.
    void connect_with_handlers(const std::string& url, const HeadersMap& headers,
        on_open_handler open, on_message_handler message, on_error_handler error, on_close_handler close) {
        connect_with_attempt_handlers(url, headers,
            [open=std::move(open)](const platform_transport_callbacks&) { if (open) open(); },
            [message=std::move(message)](const platform_transport_callbacks&, const transport_message& value) { if (message) message(value); },
            [error=std::move(error)](const platform_transport_callbacks&, const std::string& value) { if (error) error(value); },
            [close=std::move(close)](const platform_transport_callbacks&, int code, const std::string& reason) { if (close) close(code,reason); });
    }
    // Preserve the actual endpoint identity through the independent owner
    // admission. Automatic redials can share an owner lifecycle, never this
    // monotonically identified native attempt. Compatibility APIs above keep
    // their original callback signatures and do not confer owner admission.
    void connect_with_attempt_handlers(const std::string& url, const HeadersMap& headers,
        std::function<void(const platform_transport_callbacks&)> open,
        std::function<void(const platform_transport_callbacks&, const transport_message&)> message,
        std::function<void(const platform_transport_callbacks&, const std::string&)> error,
        std::function<void(const platform_transport_callbacks&, int, const std::string&)> close) {
        connect_impl(url, headers, platform_transport_callbacks::handlers{
            std::move(open), std::move(message), std::move(error), std::move(close)});
    }
    void disconnect() override {
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            cell_->phase = transport_state::closed;
            cell_->owner_attempt.store(0, std::memory_order_release);
            cell_->live_owner_attempt.store(0, std::memory_order_release);
            cell_->verified_tls_attempt.store(0, std::memory_order_release);
        }
        if (disconnect_) disconnect_(user_data_);
    }
    transport_state state() const override {
        std::lock_guard<std::mutex> lock(cell_->mutex);
        return cell_->phase;
    }
    // Keeps a control send on the exact admitted socket. The SDK still checks
    // this endpoint after the lock is released, including replacement races.
    bool send_to_attempt(const platform_transport_callbacks& endpoint,const transport_message& message) {
        {std::lock_guard<std::mutex> lock(cell_->mutex);
            if(endpoint.cell_!=cell_||!endpoint.current_locked()||cell_->phase!=transport_state::open)return false;}
        if(!send_)return false;send_(user_data_,&message,&endpoint);return true;
    }
    void send(const transport_message& message) override {
        platform_transport_callbacks endpoint;
        {
            std::lock_guard<std::mutex> lock(cell_->mutex);
            if (cell_->retired || cell_->phase != transport_state::open) return;
            endpoint = platform_transport_callbacks(cell_, cell_->generation);
        }
        if (send_) send_(user_data_, &message, &endpoint);
    }
    void set_on_open(on_open_handler handler) override {
        decltype(platform_transport_callbacks::handlers::open) replacement =
            [handler=std::move(handler)](const platform_transport_callbacks&) { if (handler) handler(); };
        std::lock_guard<std::mutex> lock(cell_->mutex);
        cell_->callbacks.open.swap(replacement);
    }
    void set_on_message(on_message_handler handler) override {
        decltype(platform_transport_callbacks::handlers::message) replacement =
            [handler=std::move(handler)](const platform_transport_callbacks&, const transport_message& value) { if (handler) handler(value); };
        std::lock_guard<std::mutex> lock(cell_->mutex);
        cell_->callbacks.message.swap(replacement);
    }
    void set_on_error(on_error_handler handler) override {
        decltype(platform_transport_callbacks::handlers::error) replacement =
            [handler=std::move(handler)](const platform_transport_callbacks&, const std::string& value) { if (handler) handler(value); };
        std::lock_guard<std::mutex> lock(cell_->mutex);
        cell_->callbacks.error.swap(replacement);
    }
    void set_on_close(on_close_handler handler) override {
        decltype(platform_transport_callbacks::handlers::close) replacement =
            [handler=std::move(handler)](const platform_transport_callbacks&, int code, const std::string& reason) { if (handler) handler(code,reason); };
        std::lock_guard<std::mutex> lock(cell_->mutex);
        cell_->callbacks.close.swap(replacement);
    }
};

// Consumes the platform retain on both success and allocation failure. The
// caller gives the resulting native pointer directly to unique_ptr ownership.
inline sync_transport* make_owned_platform_sync_transport(void* user_data,
    owned_platform_sync_transport::connect_fn_ptr connect,
    owned_platform_sync_transport::disconnect_fn_ptr disconnect,
    owned_platform_sync_transport::send_fn_ptr send,
    owned_platform_sync_transport::destroy_fn_ptr destroy) noexcept {
    try {
        return new owned_platform_sync_transport(user_data, connect, disconnect, send, destroy);
    } catch (...) {
        if (destroy) destroy(user_data);
        return nullptr;
    }
}

// Called only by the SDK's closed system-TLS adapters. This software boundary
// is distinct from the generic factory above and never constructs a grant.
inline sync_transport* make_system_tls_platform_sync_transport(void* user_data,
    owned_platform_sync_transport::connect_fn_ptr connect,
    owned_platform_sync_transport::disconnect_fn_ptr disconnect,
    owned_platform_sync_transport::send_fn_ptr send,
    owned_platform_sync_transport::destroy_fn_ptr destroy,
    void* verification_context,int32_t(*verify)(void*,const void*,const void*),
    void(*release_verification)(void*))noexcept {
    return owned_platform_sync_transport::make_system_tls_adapter(user_data,connect,disconnect,send,destroy,
        verification_context,verify,release_verification);
}

// ============================================================================
// Factory for creating platform-specific clients
// ============================================================================

class network_factory {
public:
    virtual ~network_factory() = default;

    virtual std::unique_ptr<http_client> create_http_client() = 0;
    virtual std::unique_ptr<sync_transport> create_sync_transport() = 0;

    // Called with the synchronizer's actual scheduler, including lazy setup
    // and handoff to another owner. Platforms that bind transport callbacks
    // to an owner can override this; existing factories keep their behavior.
    virtual std::unique_ptr<sync_transport> create_sync_transport(
        std::shared_ptr<scheduler> /*owner_scheduler*/) {
        return create_sync_transport();
    }
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

// Calls may arrive from a synchronizer's pacer and its caller at once.
// Snapshots own their bytes; callbacks run after unlocking so they may reenter.
// The caller must still keep the transport alive until all calls finish.
class mock_sync_transport : public sync_transport {
public:
    void connect(const std::string& url,
                const std::map<std::string, std::string>& headers = {}) override {
        on_open_handler callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            url_ = url;
            state_ = transport_state::open;
            callback = on_open_;
        }
        if (callback) callback();
    }

    void disconnect() override {
        on_close_handler callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            state_ = transport_state::closed;
            callback = on_close_;
        }
        if (callback) callback(1000, "Normal closure");
    }

    transport_state state() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_;
    }

    void send(const transport_message& message) override {
        std::lock_guard<std::mutex> lock(mutex_);
        sent_messages_.push_back(message);
    }

    void set_on_open(on_open_handler handler) override {
        std::lock_guard<std::mutex> lock(mutex_);
        on_open_.swap(handler);
    }
    void set_on_message(on_message_handler handler) override {
        std::lock_guard<std::mutex> lock(mutex_);
        on_message_.swap(handler);
    }
    void set_on_error(on_error_handler handler) override {
        std::lock_guard<std::mutex> lock(mutex_);
        on_error_.swap(handler);
    }
    void set_on_close(on_close_handler handler) override {
        std::lock_guard<std::mutex> lock(mutex_);
        on_close_.swap(handler);
    }

    // Test helpers. A concurrent replacement does not cancel an already-copied
    // callback; its captures must remain valid until that callback completes.
    void simulate_message(const transport_message& msg) {
        on_message_handler callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = on_message_;
        }
        if (callback) callback(msg);
    }

    void simulate_error(const std::string& error) {
        on_error_handler callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = on_error_;
        }
        if (callback) callback(error);
    }

    std::vector<transport_message> get_sent_messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return sent_messages_;
    }

    void clear_sent_messages() {
        std::lock_guard<std::mutex> lock(mutex_);
        sent_messages_.clear();
    }

private:
    mutable std::mutex mutex_;
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
