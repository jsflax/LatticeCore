#pragma once
#include <lattice/configured_platform.hpp>
#include "configured_attempt_custody.hpp"

namespace lattice::detail {
class configured_retirement_registry;
class configured_platform_bridge : public std::enable_shared_from_this<configured_platform_bridge> {
    mutable std::mutex mutex_;
    platform_retirement_receipt receipt_;
    std::shared_ptr<configured_attempt_custody> custody_;
    void* context_=nullptr;
    owned_platform_sync_transport::connect_fn_ptr connect_;
    owned_platform_sync_transport::disconnect_fn_ptr disconnect_;
    owned_platform_sync_transport::send_fn_ptr send_;
    owned_platform_sync_transport::destroy_fn_ptr destroy_;
    configured_tls_verify_fn_ptr verify_;
    configured_retirement_request_fn_ptr request_;
    bool claimed_=false,closing_=false,collected_=false;
    void* admitted_context()const;
public:
    configured_platform_bridge(platform_retirement_receipt,
        std::shared_ptr<configured_attempt_custody>,void*,
        owned_platform_sync_transport::connect_fn_ptr,
        owned_platform_sync_transport::disconnect_fn_ptr,
        owned_platform_sync_transport::send_fn_ptr,
        owned_platform_sync_transport::destroy_fn_ptr,
        configured_tls_verify_fn_ptr,configured_retirement_request_fn_ptr);
    ~configured_platform_bridge();
    bool valid()const noexcept;
    bool matches(const platform_retirement_receipt&)const noexcept;
    bool claim()noexcept;
    void connect(const void*,const void*,const void*);
    void disconnect();
    void send(const void*,const void*);
    int32_t verify(const void*,const void*) noexcept;
    void request(platform_retirement_receipt);
    // Registry invokes only after actual native/adapter completion, command
    // zero, and request callback return. Releases foreign retain off all leaves.
    bool collect()noexcept;
};
struct configured_platform_access {
    static std::shared_ptr<configured_retirement_registry> registry(const platform_retirement_receipt& r){return r.registry_;}
    static configured_transport_registration registration(std::shared_ptr<configured_platform_bridge> p){return configured_transport_registration(std::move(p));}
    static std::shared_ptr<configured_platform_bridge> bridge(const configured_transport_registration& r){return r.bridge_;}
};
}
